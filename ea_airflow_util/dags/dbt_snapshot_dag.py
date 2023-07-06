from functools import partial
from typing import Optional

import ea_airflow_util.dags.dag_util.slack_callbacks as slack_callbacks

from airflow import DAG
from airflow_dbt.operators.dbt_operator import DbtSnapshotOperator


class DbtSnapshotDag():
    """
    params: dbt_repo_path 
    params: dbt_target_name 
    params: dbt_bin_path 
    
    """
    def __init__(self,
        # required dbt paths and target
        dbt_repo_path  : str,
        dbt_target_name: str,
        dbt_bin_path   : str,

        slack_conn_id: Optional[str] = None,

        **kwargs
    ):
        # dbt paths
        self.dbt_repo_path = dbt_repo_path
        self.dbt_target_name = dbt_target_name
        self.dbt_bin_path = dbt_bin_path

        # Slack alerting
        self.slack_conn_id = slack_conn_id

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
            }
        )

    def dbt_snapshot_run(self, on_success_callback=None, **kwargs):

        dbt_snapshot_task = DbtSnapshotOperator(
            task_id= f'dbt_snapshot',
            dir    = self.dbt_repo_path,
            target = self.dbt_target_name,
            dbt_bin= self.dbt_bin_path,
            dag=self.dag
        )

        dbt_snapshot_task
