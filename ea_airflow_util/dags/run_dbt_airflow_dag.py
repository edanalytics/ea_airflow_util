from datetime import datetime
from functools import partial
from typing import Optional

import ea_airflow_util.dags.dag_util.slack_callbacks as slack_callbacks

from airflow import DAG
from airflow.utils.task_group import TaskGroup

from airflow_dbt.operators.dbt_operator import DbtRunOperator, DbtSeedOperator, DbtTestOperator

from .operators.dbt_operators import DbtRunOperationOperator
from .operators.variable import build_variable_check_operator, build_variable_update_operator


class RunDbtDag():
    """
    params: environment 
    params: dbt_repo_path 
    params: dbt_target_name 
    params: dbt_bin_path 
    params: full_refresh -- default to False
    params: full_refresh_schedule -- default to None
    params: opt_dest_schema -- default to None
    params: opt_swap -- default to False 
    
    """
    def __init__(self,
        environment: str,
    
        # required dbt paths and target
        dbt_repo_path  : str,
        dbt_target_name: str,
        dbt_bin_path   : str,

        # default to optional
        full_refresh: bool = False,
        full_refresh_schedule: Optional[str] = None,

        opt_dest_schema: Optional[str] = None,
        opt_swap: bool = False,

        upload_artifacts: bool = False,

        slack_conn_id: Optional[str] = None,
        dbt_incrementer_var: str = None,

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

        # bluegreen 
        self.opt_dest_schema = opt_dest_schema
        self.opt_swap        = opt_swap

        # DBT Artifacts
        self.upload_artifacts = upload_artifacts

        # Slack alerting
        self.slack_conn_id = slack_conn_id

        # Dynamic runs via variables
        self.dbt_incrementer_var = dbt_incrementer_var

        self.dag = self.initialize_dag(**kwargs)

        # Build operators to check the value of the DBT var at the start and reset it at the end.
        if self.dbt_incrementer_var:
            self.dbt_var_check_operator = build_variable_check_operator(
                self.dbt_incrementer_var, lambda x: x > 0,
                task_id='check_dbt_variable', dag=self.dag
            )

            self.dbt_var_reset_operator = build_variable_update_operator(
                self.dbt_incrementer_var, 0,
                task_id='reset_dbt_variable', trigger_rule='all_done', dag=self.dag
            )

        else:
            self.dbt_var_check_operator = None
            self.dbt_var_reset_operator = None


    # create DAG 
    def initialize_dag(self, dag_id, schedule_interval, default_args, **kwargs):
        """
        :param dag_id:
        :param schedule_interval:
        :param default_args:
        :param catchup:
        :user_defined_macros:
        """
        # If a Slack connection has been defined, add the failure callback to the default_args.
        if self.slack_conn_id:
            slack_failure_callback = partial(slack_callbacks.slack_alert_failure, http_conn_id=self.slack_conn_id)
            default_args['on_failure_callback'] = slack_failure_callback

        return DAG(
            dag_id=dag_id,
            schedule_interval=schedule_interval,
            default_args=default_args,
            catchup=False,
            user_defined_macros= {
                'environment': self.environment,
            }
        )


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
        if self.full_refresh_schedule == day or "{{ dag_run.conf['full_refresh'] }}":
           self.full_refresh = True

        with TaskGroup(
            group_id="Run DBT",
            prefix_group_id=False,
            parent_group=None,
            dag=self.dag
        ) as dbt_task_group:

            # open question: does full refresh seed necessarily need to be scheduled?
            dbt_seed = DbtSeedOperator(
                task_id= f'dbt_seed_{self.environment}',
                dir    = self.dbt_repo_path,
                target = self.dbt_target_name,
                dbt_bin= self.dbt_bin_path,
                trigger_rule='all_success',
                full_refresh=True,
                dag=self.dag
            )

            #
            dbt_run = DbtRunOperator(
                task_id= f'dbt_run_{self.environment}',
                dir    = self.dbt_repo_path,
                target = self.dbt_target_name,
                dbt_bin= self.dbt_bin_path,
                full_refresh=self.full_refresh,
                dag=self.dag
            )

            dbt_test = DbtTestOperator(
                task_id= f'dbt_test_{self.environment}',
                dir    = self.dbt_repo_path,
                target = self.dbt_target_name,
                dbt_bin= self.dbt_bin_path,
                dag=self.dag
            )

            dbt_seed >> dbt_run >> dbt_test


            # bluegreen operator
            if self.opt_swap:
                dbt_swap = DbtRunOperationOperator(
                    task_id= f'dbt_swap_{self.environment}',
                    dir    = self.dbt_repo_path,
                    target = self.dbt_target_name,
                    dbt_bin= self.dbt_bin_path,
                    op_name= 'swap_schemas',
                    vars   = "{dest_schema = self.opt_dest_schema}",

                    on_success_callback=on_success_callback,
                    dag=self.dag
                )

                dbt_test >> dbt_swap


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

        # Apply the DBT variable operators if defined.
        if self.dbt_incrementer_var:
            self.dbt_var_check_operator >> dbt_task_group >> self.dbt_var_reset_operator
