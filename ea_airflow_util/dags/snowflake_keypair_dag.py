from typing import List, Optional

from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from ea_airflow_util.dags.ea_custom_dag import EACustomDAG
from ea_airflow_util.callables import snowflake_keypair


class SnowflakeKeypairRotationDag:
    def __init__(
        self,
        *,
        key_rotator_conn_id: str,
        snowflake_users: List[str],
        key_dir: str = "/efs/snowflake_keys",
        **kwargs
    ) -> None:
        self.key_rotator_conn_id = key_rotator_conn_id
        self.snowflake_users = snowflake_users
        self.key_dir = key_dir

        params = {
            "snowflake_users": Param(self.snowflake_users, type="array", description="Subset which users to rotate."),
        }

        self.dag = EACustomDAG(params=params, **kwargs)
        self.build_snowflake_keypair_rotation_dag()

    def rotate_keypair_wrapper(key_rotator_conn_id: str, snowflake_user: str, output_dir: str, **context):
        """
        Simple wrapper to raise a skip exception when the user is not specified in the DAG params. 
        """
        if snowflake_user not in context['params']['snowflake_users']:
            raise AirflowSkipException(f"Snowflake user `{snowflake_user}` was not specified in DAG params for rotation. Skipping...")
        
        return snowflake_keypair.rotate_keypair(
            key_rotator_conn_id=key_rotator_conn_id,
            snowflake_user=snowflake_user,
            output_dir=output_dir
        )

    def build_snowflake_keypair_rotation_dag(self, **kwargs) -> None:
        for snowflake_user in self.snowflake_users:
            PythonOperator(
                task_id=f"rotate_keypair_{snowflake_user}",
                python_callable=self.rotate_keypair_wrapper,
                op_kwargs={
                    "key_rotator_conn_id": self.key_rotator_conn_id,
                    "snowflake_user": snowflake_user,
                    "output_dir": self.key_dir
                },
                dag=self.dag,
            )

        return