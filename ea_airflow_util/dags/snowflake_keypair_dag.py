# ea_airflow_util/dags/snowflake_keypair_rotation_dag.py

from typing import List, Optional, Dict

from airflow.operators.python import PythonOperator

from ea_airflow_util.dags.ea_custom_dag import EACustomDAG
from ea_airflow_util.callables import snowflake_keypair


class SnowflakeKeypairRotationDag:
    """
    """
    def __init__(self,
        *,
        key_rotator_conn_id: str,

        key_names: List[str],  # e.g. ["key_rotator", "loader","transformer"]
        snowflake_users: Dict[str, str],  # e.g. {"loader": "loader_prod", ...}
        test_conn_ids: Optional[Dict[str, str]] = None,  # e.g. {"key_rotator": "snowflake_key_rotator", "loader": "snowflake"}

        key_dir: str = "/efs/snowflake_keys",
        do_test: bool = True,
        **kwargs
    ) -> None:
        self.key_rotator_conn_id = key_rotator_conn_id

        self.key_names = key_names
        self.snowflake_users = snowflake_users
        self.test_conn_ids = test_conn_ids or {}

        self.key_dir = key_dir
        self.do_test = do_test

        self.dag = EACustomDAG(**kwargs)
        self.build_snowflake_keypair_rotation_dag()


    def build_snowflake_keypair_rotation_dag(self, **kwargs):

        rotate_tasks = []
        for key_name in self.key_names:
            snowflake_user = self.snowflake_users[key_name]
            test_conn_id = self.test_conn_ids.get(key_name)

            rotate_tasks.append(
                PythonOperator(
                    task_id="rotate_keypair_" + key_name,
                    python_callable=snowflake_keypair.rotate_keypair,
                    op_kwargs={
                        "key_rotator_conn_id": self.key_rotator_conn_id,
                        "snowflake_user": snowflake_user,
                        "key_name": key_name,
                        "output_dir": self.key_dir,
                        "test_conn_id": test_conn_id,
                        "do_test": self.do_test and test_conn_id is not None,
                    },
                    dag=self.dag
                )
            )

        rotate_tasks

        return