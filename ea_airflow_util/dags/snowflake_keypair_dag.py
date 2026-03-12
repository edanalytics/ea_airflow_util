from typing import List, Optional

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

        self.dag = EACustomDAG(**kwargs)
        self.build_snowflake_keypair_rotation_dag()

    def build_snowflake_keypair_rotation_dag(self, **kwargs) -> None:
        for snowflake_user in self.snowflake_users:
            PythonOperator(
                task_id=f"rotate_keypair_{snowflake_user}",
                python_callable=snowflake_keypair.rotate_keypair,
                op_kwargs={
                    "key_rotator_conn_id": self.key_rotator_conn_id,
                    "snowflake_user": snowflake_user,
                    "output_dir": self.key_dir
                },
                dag=self.dag,
            )

        return