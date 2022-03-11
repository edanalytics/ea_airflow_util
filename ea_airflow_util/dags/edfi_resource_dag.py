import os
from typing import Optional

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from .dag_util.base_dag import BaseDAG
from .dag_util.edfi_util import get_deletes_name
from .dag_util.xcom_util import xcom_pull_template
from edfi_api import EdFiToS3Operator, SnowflakeChangeVersionOperator, SnowflakeDeleteOperator
from edfi_api import camel_to_snake


class EdfiResourceDAG(BaseDAG):
    """

    """
    def __init__(self,
        *,
        tenant_code: str,
        api_year   : int,

        edfi_conn_id     : str,
        s3_conn_id       : str,
        snowflake_conn_id: str,

        pool     : str,
        page_size: int,
        tmp_dir  : str,

        # How many of these are predefined in the connections?
        snowflake_database  : str,
        snowflake_schema    : str,
        snowflake_stage     : str,
        change_version_table: str,

        **kwargs
    ) -> None:
        self.tenant_code = tenant_code
        self.api_year    = api_year

        self.edfi_conn_id = edfi_conn_id
        self.s3_conn_id   = s3_conn_id
        self.snowflake_conn_id = snowflake_conn_id

        self.pool      = pool
        self.page_size = page_size
        self.tmp_dir   = tmp_dir

        self.snowflake_database   = snowflake_database
        self.snowflake_schema     = snowflake_schema
        self.snowflake_stage      = snowflake_stage
        self.change_version_table = change_version_table

        self.dag = self.initialize_dag(**kwargs)


    def initialize_dag(self,
            dag_id: str,
            schedule_interval: str,
            default_args: dict,
            catchup: bool = False,

            **kwargs
    ) -> DAG:
        """

        :param dag_id:
        :param schedule_interval:
        :param default_args:
        :param catchup:
        :return:
        """
        return DAG(
            dag_id=dag_id,
            schedule_interval=schedule_interval,
            default_args=default_args,
            catchup=catchup,
            user_defined_macros= {  # Note: none of these UDMs are currently used.
                'tenant_code': self.tenant_code,
                'api_year'   : self.api_year,
            }
        )


    def build_resource_branch(self,
            resource : str,
            namespace: str = 'ed-fi',
            *,
            deletes  : bool = False,
            table    : Optional[str] = None,
    ) -> None:
        """
        Pulling an EdFi resource/descriptor requires knowing its camelCased name and namespace.
        Deletes are optionally specified.
        Specify `table` to overwrite the final Snowflake table location.

        TODO:
            - Add schoolYear query parameter to `query_params`, but only if these conditions apply:
                * ODS configuration is multi-year (tenant-level condition)
                * resource allows schoolYear filtering (resource-level condition) (and edfi-version condition?)
                - Note: some resources allow you to filter on school year to emulate api-year.
            - Skip S3-to-Snowflake if EdFi-to-S3 skipped (don't want to run if only delete ran)

        :param resource :
        :param namespace:
        :param deletes  :
        :param table    : Overwrite the table to output the rows to (exception case for descriptors).
        :return:
        """
        # Snowflake tables and Airflow tasks use snake_cased resources for readability.
        snake_resource = camel_to_snake(resource)

        if deletes:
            snake_resource = get_deletes_name(snake_resource)  # Singular logic for defining 'deletes' labelling.

        # Define all tasks here for easier editing later.
        get_change_version_id    =  "get_latest_change_version"
        edfi_to_s3_task_id       = f"pull_{snake_resource}"
        delete_snowflake_task_id = f"delete_existing_{snake_resource}"
        s3_to_snowflake_task_id  = f"copy_into_snowflake_{snake_resource}"


        ### SNOWFLAKE CHANGE OPERATOR
        get_change_version = SnowflakeChangeVersionOperator(
            task_id= get_change_version_id,

            edfi_conn_id     = self.edfi_conn_id,
            snowflake_conn_id= self.snowflake_conn_id,

            tenant_code= self.tenant_code,
            api_year   = self.api_year,
            database   = self.snowflake_database,
            schema     = self.snowflake_schema,
            change_version_table= self.change_version_table,

            dag=self.dag
        )


        ### EDFI TO S3
        edfi_query_params = {
            'minChangeVersion': xcom_pull_template('get_change_version', key='prev_change_version'),
            'maxChangeVersion': xcom_pull_template('get_change_version', key='max_change_version'),
        }

        s3_destination_key = os.path.join(
            self.tenant_code, str(self.api_year), '{{ ds_nodash }}', '{{ ts_nodash }}', f'{snake_resource}.json'
        )

        edfi_pull = EdFiToS3Operator(
            task_id= edfi_to_s3_task_id,

            edfi_conn_id    = self.edfi_conn_id,
            page_size       = self.page_size,
            resource        = resource,
            api_namespace   = namespace,
            api_get_deletes = deletes,
            query_parameters= edfi_query_params,

            pool      = self.pool,
            tmp_dir   = self.tmp_dir,
            s3_conn_id= self.s3_conn_id,
            s3_destination_key= s3_destination_key,

            trigger_rule='all_done',
            dag=self.dag
        )


        ### DELETE FROM SNOWFLAKE
        delete_existing = SnowflakeDeleteOperator(
            task_id= delete_snowflake_task_id,

            edfi_conn_id= self.edfi_conn_id,
            tenant_code = self.tenant_code,
            api_year    = self.api_year,

            snowflake_conn_id= self.snowflake_conn_id,
            database= self.snowflake_database,
            schema  = self.snowflake_schema,
            table   = table or snake_resource,  # Use the provided table name, or default to resource.

            trigger_rule='all_done',
            dag=self.dag
        )


        ### S3 TO SNOWFLAKE
        snowflake_copy_into_template_table_path = 'snowflake_copy_into_template_table.sql'

        copy_into_snowflake = SnowflakeOperator(
            task_id= s3_to_snowflake_task_id,

            snowflake_conn_id= self.snowflake_conn_id,
            sql   = snowflake_copy_into_template_table_path,
            params= {
                'database'   : self.snowflake_database,
                'schema'     : self.snowflake_schema,
                'table'      : table or snake_resource,  # Use the provided table name, or default to resource.
                'stage'      : self.snowflake_stage,
                's3_key'     : xcom_pull_template(task_ids=edfi_to_s3_task_id),
                'resource'   : snake_resource,
            },

            trigger_rule='none_failed',
            dag=self.dag
        )


        get_change_version >> edfi_pull >> delete_existing >> copy_into_snowflake
