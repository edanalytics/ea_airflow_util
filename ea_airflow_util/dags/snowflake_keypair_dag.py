import logging
from typing import List

from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param

from ea_airflow_util.dags.ea_custom_dag import EACustomDAG
from ea_airflow_util.callables import snowflake_keypair


class SnowflakeKeypairRotationDag:
    def __init__(
        self,
        *,
        key_rotator_conn_id: str,
        snowflake_users: List[str] = (),  # Primary use-case to hardcode accounts to rotate
        snowflake_user_prefix: str | None = None,  # Secondary use-case to infer accounts by prefix
        key_dir: str = "/efs/snowflake_keys",
        **kwargs
    ) -> None:
        # Raise an error immediately if both or neither users and prefixes are passed in the same DAG init.
        if bool(snowflake_users) == bool(snowflake_user_prefix):
            raise ValueError("SnowflakeKeypairRotationDag arguments `snowflake_users` and `snowflake_user_prefix` are mutually-exclusive and required!")
        
        self.key_rotator_conn_id = key_rotator_conn_id
        self.key_dir = key_dir
        self.snowflake_users = snowflake_users
        self.snowflake_user_prefix = snowflake_user_prefix

        # Note that the users to rotate do not have to appear in this list if they are inferred by prefix! 
        params = {
            "dryrun": Param(False, type="boolean", description="Run as a dryrun without cycling any credentials."),
            "snowflake_users": Param(self.snowflake_users, type="array", description="Subset which users to rotate."),
        }

        self.dag = EACustomDAG(params=params, **kwargs)
        self.build_snowflake_keypair_rotation_dag()

    def build_snowflake_keypair_rotation_dag(self, **kwargs) -> None:

        @task(dag=self.dag)
        def list_snowflake_users_by_prefix(key_rotator_conn_id: str, snowflake_user_prefix: str):
            """
            Simple method to retrieve user list from Snowflake.
            """
            from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
            hook = SnowflakeHook(snowflake_conn_id=key_rotator_conn_id)

            users_df = hook.get_pandas_df(f"SHOW USERS LIKE '{snowflake_user_prefix}%'")
            users = list(users_df['name'])
            if not users:
                raise ValueError(f"No users found with name like '{snowflake_user_prefix}'!")
            
            return users

        @task(dag=self.dag, map_index_template="{{ task.op_kwargs['snowflake_user'] }}")
        def rotate_keypair_wrapper(snowflake_user: str, key_rotator_conn_id: str, output_dir: str, **context):
            """
            Simple wrapper to raise a skip exception when the user is not specified in the DAG params. 
            """
            specified_users = context['params']['snowflake_users']
            if specified_users and snowflake_user not in specified_users:
                raise AirflowSkipException(f"Snowflake user `{snowflake_user}` was not specified in DAG params for rotation. Skipping...")
            
            if context['params']['dryrun']:
                logging.info(f"DRYRUN: Snowflake user {snowflake_user} would be rotated during this run!")
                return
            
            return snowflake_keypair.rotate_keypair(
                key_rotator_conn_id=key_rotator_conn_id,
                snowflake_user=snowflake_user,
                output_dir=output_dir
            )
        
        if self.snowflake_user_prefix:
            users_to_rotate = list_snowflake_users_by_prefix(self.key_rotator_conn_id, self.snowflake_user_prefix)
        else:
            users_to_rotate = self.snowflake_users

        (rotate_keypair_wrapper
            .override(task_id='rotate_snowflake_user_keypairs')
            .partial(key_rotator_conn_id=self.key_rotator_conn_id, output_dir=self.key_dir)
            .expand(snowflake_user=users_to_rotate)
        )
        return