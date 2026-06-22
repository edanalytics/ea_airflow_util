# AirflowDBT uses Airflow 1.x syntax when defining Hooks and Operators.
# These warnings clog up the scheduler and should be hidden until the package is updated.
import warnings
warnings.filterwarnings("ignore", module="airflow_dbt", category=DeprecationWarning)

from datetime import datetime
from typing import Optional

from airflow.models.param import Param
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

from airflow_dbt.operators.dbt_operator import DbtRunOperator, DbtSeedOperator, DbtTestOperator

from ea_airflow_util.dags.ea_custom_dag import EACustomDAG
from ea_airflow_util.callables.dbt import check_package_lock_hash, update_package_lock_hash
from ea_airflow_util.callables.variable import check_variable, update_variable
from ea_airflow_util.providers.dbt.operators.dbt import DbtRunOperationOperator


class RunDbtDag:
    """
    :param environment:
    :param dbt_repo_path:
    :param dbt_target_name:
    :param dbt_bin_path:
    :param full_refresh: -- default to False
    :param full_refresh_schedule: -- default to None
    :param opt_swap: -- default to False
    :param opt_dest_schema: -- default to None
    :param opt_swap_target: -- default to opt_dest_schema

    """
    params_dict = {
        "force": Param(
            default=False,
            type="boolean",
            description="If true, run DBT regardless of the state of the DBT incrementer variable"
        ),
    }

    def __init__(self,
        environment: str,
    
        # required dbt paths and target
        dbt_repo_path  : str,
        dbt_target_name: str,
        dbt_bin_path   : str,

        # default to optional
        full_refresh: bool = False,
        full_refresh_schedule: Optional[str] = None,
        track_package_lock: bool = False,

        seed_vars: Optional[dict] = None,
        run_vars: Optional[dict] = None,
        test_vars: Optional[dict] = None,

        opt_swap: bool = False,
        opt_dest_schema: Optional[str] = None,
        opt_swap_target: Optional[str] = None,

        upload_artifacts: bool = False,
        dbt_incrementer_var: str = None,
        trigger_dags_on_run_success: Optional[list] = None,

        **kwargs
    ):
        self.environment = environment
        
        # dbt paths
        self.dbt_repo_path = dbt_repo_path
        self.dbt_target_name = dbt_target_name
        self.dbt_bin_path = dbt_bin_path

        # full refreshes schedules
        self.full_refresh = full_refresh
        self.full_refresh_schedule = full_refresh_schedule
        self.track_package_lock = track_package_lock

        # run-time vars
        self.seed_vars = seed_vars
        self.run_vars = run_vars
        self.test_vars = run_vars

        # bluegreen
        self.opt_swap        = opt_swap
        self.opt_dest_schema = opt_dest_schema
        self.opt_swap_target = opt_swap_target or self.opt_dest_schema

        # DBT Artifacts
        self.upload_artifacts = upload_artifacts

        # Dynamic runs via variables
        self.dbt_incrementer_var = dbt_incrementer_var

        self.dag = EACustomDAG(
            params=self.params_dict,
            user_defined_macros= {
                'environment': self.environment,
            },
            **kwargs
        )

        # Build operators to check the value of the DBT var at the start and reset it at the end.
        if self.dbt_incrementer_var:
            self.dbt_var_check_operator = PythonOperator(
                task_id='check_dbt_variable',
                python_callable=check_variable,
                op_kwargs={
                    'var': self.dbt_incrementer_var,
                    'condition': lambda x: int(x) > 0,
                    'force': "{{ params.force }}"
                },
                dag=self.dag
            )

            self.dbt_var_reset_operator = PythonOperator(
                task_id='reset_dbt_variable',
                python_callable=update_variable,
                op_kwargs={
                    'var': self.dbt_incrementer_var,
                    'value': 0,
                },
                trigger_rule='none_skipped',
                dag=self.dag
            )

        else:
            self.dbt_var_check_operator = None
            self.dbt_var_reset_operator = None

        # Build optional operator to trigger downstream DAG when `dbt run` succeeds.
        if trigger_dags_on_run_success:
            self.external_dags = []

            for external_dag_id in trigger_dags_on_run_success:
                self.external_dags.append(
                    TriggerDagRunOperator(
                        task_id=f"trigger_{external_dag_id}",
                        trigger_dag_id=external_dag_id,
                        wait_for_completion=False,  # Keep running DBT DAG while downstream DAG runs.
                        trigger_rule='all_success',
                    ))
        else:
            self.external_dags = None

    
    # build function for tasks
    def build_dbt_run(self, on_success_callback=None, **kwargs):
        """
        four tasks defined here: 

        dbt seed: 
        dbt run:
        dbt test:
        dbt swap: bluegreen step, not required

        """
        # set a logic to force a full refresh 
        day = datetime.today().weekday()
        dag_conf_full_refresh = kwargs.get('dag_run', {}).get('conf', {}).get('full_refresh') or False
        if self.full_refresh_schedule == day or dag_conf_full_refresh:
           self.full_refresh = True

        with TaskGroup(
            group_id="Run DBT",
            prefix_group_id=False,
            parent_group=None,
            dag=self.dag
        ) as dbt_task_group:

            dbt_seed = DbtSeedOperator(
                task_id= f'dbt_seed_{self.environment}',
                dir    = self.dbt_repo_path,
                target = self.dbt_target_name,
                dbt_bin= self.dbt_bin_path,
                trigger_rule='all_success',
                full_refresh=True,
                vars=self.seed_vars,
                dag=self.dag
            )

            dbt_test = DbtTestOperator(
                task_id= f'dbt_test_{self.environment}',
                dir    = self.dbt_repo_path,
                target = self.dbt_target_name,
                dbt_bin= self.dbt_bin_path,
                trigger_rule='none_failed_min_one_success' if self.track_package_lock else 'all_success',
                vars=self.test_vars,
                dag=self.dag
            )

            if self.track_package_lock:
                full_refresh_task_id = f'dbt_run_full_refresh_{self.environment}'
                incremental_task_id  = f'dbt_run_{self.environment}'

                # BranchPythonOperator returns one of the two task IDs below.
                # Airflow runs that task and skips the other, so exactly one dbt run executes.
                pkg_lock_check = BranchPythonOperator(
                    task_id=f'check_pkg_lock_{self.environment}',
                    python_callable=check_package_lock_hash,
                    op_kwargs={
                        'dbt_repo_path'        : self.dbt_repo_path,
                        'environment'          : self.environment,
                        'full_refresh_task_id' : full_refresh_task_id,
                        'incremental_task_id'  : incremental_task_id,
                        # Passes through any config-based full_refresh flag.
                        'force_full_refresh'   : self.full_refresh,
                        # Checked at task runtime so the day is evaluated when the DAG actually runs.
                        'full_refresh_schedule': self.full_refresh_schedule,
                    },
                    dag=self.dag,
                )

                dbt_run = DbtRunOperator(
                    task_id= incremental_task_id,
                    dir    = self.dbt_repo_path,
                    target = self.dbt_target_name,
                    dbt_bin= self.dbt_bin_path,
                    full_refresh=False,
                    vars=self.run_vars,
                    dag=self.dag,
                )

                dbt_run_full_refresh = DbtRunOperator(
                    task_id= full_refresh_task_id,
                    dir    = self.dbt_repo_path,
                    target = self.dbt_target_name,
                    dbt_bin= self.dbt_bin_path,
                    full_refresh=True,
                    vars=self.run_vars,
                    dag=self.dag,
                )

                # Stores the hash after a successful run so the next run can compare against it.
                pkg_lock_update = PythonOperator(
                    task_id=f'update_pkg_lock_{self.environment}',
                    python_callable=update_package_lock_hash,
                    op_kwargs={
                        'dbt_repo_path': self.dbt_repo_path,
                        'environment'  : self.environment,
                    },
                    # one of the two dbt_run tasks will always be skipped by the branch operator.
                    trigger_rule='one_success',
                    dag=self.dag,
                )

                # dbt_test uses none_failed_min_one_success so it runs even though one of the
                # two dbt_run tasks will always be skipped by the branch operator.
                dbt_seed >> pkg_lock_check >> [dbt_run, dbt_run_full_refresh] >> pkg_lock_update >> dbt_test

                dbt_run_operators = [dbt_run, dbt_run_full_refresh]

            else:
                dbt_run = DbtRunOperator(
                    task_id= f'dbt_run_{self.environment}',
                    dir    = self.dbt_repo_path,
                    target = self.dbt_target_name,
                    dbt_bin= self.dbt_bin_path,
                    full_refresh=self.full_refresh,
                    vars=self.run_vars,
                    dag=self.dag
                )

                dbt_seed >> dbt_run >> dbt_test

                dbt_run_operators = [dbt_run]


            # bluegreen operator
            if self.opt_swap:
                dbt_swap = DbtRunOperationOperator(
                    task_id= f'dbt_swap_{self.environment}',
                    dir    = self.dbt_repo_path,
                    target = self.dbt_target_name,
                    dbt_bin= self.dbt_bin_path,
                    op_name= 'swap_schemas',
                    arguments={
                        "dest_schema": self.opt_dest_schema,
                    },
                    on_success_callback=on_success_callback,
                    dag=self.dag
                )

                # Schema swaps only apply to tables, not views.
                dbt_rerun_views_swap = DbtRunOperator(
                    task_id=f'dbt_rerun_views_{self.opt_swap_target}',
                    dir=self.dbt_repo_path,
                    target=self.opt_swap_target,
                    dbt_bin=self.dbt_bin_path,
                    models="config.materialized:view",
                    full_refresh=self.full_refresh,
                    dag=self.dag
                )

                # Rerun the original target also to allow comparison after swap.
                dbt_rerun_views = DbtRunOperator(
                    task_id=f'dbt_rerun_views_{self.environment}',
                    dir=self.dbt_repo_path,
                    target=self.dbt_target_name,
                    dbt_bin=self.dbt_bin_path,
                    models="config.materialized:view",
                    full_refresh=self.full_refresh,
                    dag=self.dag
                )

                dbt_test >> dbt_swap >> [dbt_rerun_views_swap, dbt_rerun_views]


            # Upload run artifacts to Snowflake
            if self.upload_artifacts:
                dbt_build_artifact_tables = DbtRunOperator(
                    task_id=f'dbt_build_artifact_tables_{self.environment}',
                    dir=self.dbt_repo_path,
                    target=self.dbt_target_name,
                    dbt_bin=self.dbt_bin_path,
                    select="package:dbt_artifacts",
                    dag=self.dag
                )

                dbt_build_artifact_tables >> dbt_seed

            # Trigger downstream DAG when `dbt run` succeeds
            if self.external_dags:
                dbt_run_operators >> self.external_dags

        # Apply the DBT variable operators if defined.
        if self.dbt_incrementer_var:
            self.dbt_var_check_operator >> dbt_task_group >> self.dbt_var_reset_operator
