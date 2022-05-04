from datetime import datetime
from typing import Optional

from airflow import DAG
from airflow_dbt.operators.dbt_operator import DbtRunOperator, DbtSeedOperator, DbtTestOperator
from .operators.dbt_operators import DbtRunOperationOperator


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

        self.dag = self.initialize_dag(**kwargs)


    # create DAG 
    def initialize_dag(self, dag_id, schedule_interval, default_args, **kwargs):
        """
        :param dag_id:
        :param schedule_interval:
        :param default_args:
        :param catchup:
        :user_defined_macros:
        """
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

        # open question: does full refresh seed necessarily need to be scheduled?    
        dbt_seed = DbtSeedOperator(
            task_id= f'dbt_seed_{self.environment}',
            dir    = self.dbt_repo_path,
            target = self.dbt_target_name,
            dbt_bin= self.dbt_bin_path,
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

            dbt_seed >> dbt_run >> dbt_test >> dbt_swap

        else:
            dbt_seed >> dbt_run >> dbt_test

