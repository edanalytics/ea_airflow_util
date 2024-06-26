import logging
import os

from typing import Optional

from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.helpers import chain


from ea_airflow_util.dags.ea_custom_dag import EACustomDAG
from ea_airflow_util.callables.airflow import xcom_pull_template
from ea_airflow_util.providers.aws.operators.s3 import LoopS3FileTransformOperator


class S3ToSnowflakeDag:
    """
    This DAG transfers data from an S3 bucket location into the Snowflake raw data lake. It should be used when data sources
    are not available from an Ed-Fi ODS but need to be brought into the data warehouse.

    """
    def __init__(self,
        *,
        tenant_code: str,
        api_year: int,

        snowflake_conn_id: str,
        database: str,
        schema: str,

        data_source: str,
        resource_names: str,
        transform_script: str,

        s3_source_conn_id: str,
        s3_dest_conn_id: str,
        s3_dest_file_extension: str,
        is_manual_upload: bool = False,

        pool: str,
        full_replace: bool = False,  #TODO once on latest version of airflow, use dagrun parameter to allow full_replace runs even if not set here at dag level

        do_delete_from_source: bool = True,
        **kwargs
    ) -> None:
        self.tenant_code = tenant_code
        self.api_year = api_year

        self.snowflake_conn_id = snowflake_conn_id
        self.database = database
        self.schema = schema

        self.data_source = data_source
        self.resource_names = resource_names
        self.transform_script = transform_script      
        self.do_delete_from_source = do_delete_from_source
        
        self.s3_source_conn_id = s3_source_conn_id
        self.s3_dest_conn_id = s3_dest_conn_id
        self.s3_dest_file_extension = s3_dest_file_extension
        self.is_manual_upload = is_manual_upload

        self.full_replace = full_replace
        self.pool = pool

        self.dag = EACustomDAG(**kwargs)

    
    def build_s3_to_snowflake_dag(self, **kwargs):

        for resource_name in self.resource_names:

            # different source prefix depending on whether the upload to external bucket is manual or not
            if self.is_manual_upload:
                s3_source_prefix = os.path.join(
                    self.tenant_code, self.data_source,
                    resource_name, str(self.api_year), 
                    '{{ ds_nodash }}'
                ) 
            else:
                s3_source_prefix = os.path.join(
                    self.tenant_code, self.data_source,
                    str(self.api_year), '{{ ds_nodash }}',
                    resource_name
                )
            
            datalake_prefix = os.path.join(
                self.tenant_code, str(self.api_year),
                '{{ ds_nodash }}', '{{ ts_nodash }}',
                resource_name
            )

            ## List the s3 files from the source bucket
            list_s3_objects = S3ListOperator(
                task_id=f'list_s3_objects_{resource_name}',
                bucket='{{ conn.%s.schema }}' % self.s3_source_conn_id,  # Pass bucket as Jinja template to avoid Hook during DAG-init
                prefix=s3_source_prefix,
                delimiter='',
                aws_conn_id=self.s3_source_conn_id,
                dag=self.dag
            )

            ## Transfer from source to dest bucket, and run transform script
            if self.s3_dest_conn_id:
                transfer_s3_to_s3 = LoopS3FileTransformOperator(
                    task_id=f'transfer_s3_to_s3_{resource_name}',
                    source_s3_keys=xcom_pull_template(list_s3_objects.task_id),
                    dest_s3_prefix=datalake_prefix,
                    transform_script=self.transform_script,
                    source_aws_conn_id=self.s3_source_conn_id,
                    dest_aws_conn_id=self.s3_dest_conn_id,
                    dest_s3_file_extension=self.s3_dest_file_extension,
                    replace=True,
                    dag=self.dag
                )
            else:
                transfer_s3_to_s3 = None

            ## Copy data from dest bucket (data lake stage) to snowflake raw table
            copy_to_raw = PythonOperator(
                task_id=f'copy_to_raw_{resource_name}',
                python_callable=self.copy_from_datalake_to_raw,
                op_kwargs={
                    'resource_name': resource_name,
                    'datalake_prefix': datalake_prefix,
                    'full_replace': self.full_replace
                },
                dag=self.dag
            )

            ## Delete data from source bucket
            if self.s3_dest_conn_id and self.do_delete_from_source:
                delete_from_source = PythonOperator(
                    task_id=f'delete_from_source_{resource_name}',
                    python_callable=self.delete_from_source,
                    op_kwargs={
                        's3_source_keys'  : xcom_pull_template(list_s3_objects.task_id)
                    },
                    dag=self.dag
                )
            else:
                delete_from_source = None

            ### Default route: List -> Transfer -> Copy -> Delete from source
            task_order = (
                list_s3_objects,
                transfer_s3_to_s3,
                copy_to_raw,
                delete_from_source
            )

            chain(*filter(None, task_order))  # Chain all defined operators into task-order.


    def copy_from_datalake_to_raw(self, resource_name, datalake_prefix, full_replace):
        """
        Copy raw data from data lake to data warehouse, including object metadata.
        """

        delete_sql = f'''
            delete from {self.database}.{self.schema}.{self.data_source}__{resource_name}
            where tenant_code = '{self.tenant_code}'
              and api_year = '{self.api_year}'
        '''

        date_regex = "\\\\d{8}"
        ts_regex = "\\\\d{8}T\\\\d{6}"

        
        logging.info(f"Copying from data lake to raw: {datalake_prefix}")
        copy_sql = f'''
            copy into {self.database}.{self.schema}.{self.data_source}__{resource_name}
                (tenant_code, api_year, pull_date, pull_timestamp, file_row_number, filename, name, v)
            from (
                select
                    '{self.tenant_code}' as tenant_code,
                    '{self.api_year}' as api_year,
                    TO_DATE(REGEXP_SUBSTR(metadata$filename, '{date_regex}'), 'YYYYMMDD') AS pull_date,
                    TO_TIMESTAMP(REGEXP_SUBSTR(metadata$filename, '{ts_regex}'), 'YYYYMMDDTHH24MISS') AS pull_timestamp,
                    metadata$file_row_number as file_row_number,
                    metadata$filename as filename,
                    '{resource_name}' as name,
                    t.$1 as v
                from @{self.database}.util.airflow_stage/{datalake_prefix}/
                (file_format => 'json_default') t
            ) FORCE=TRUE
        '''

        # Commit the copy query to Snowflake
        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)

        if full_replace:
            cursor_log_delete = snowflake_hook.run(sql=delete_sql)
            logging.info(cursor_log_delete)

        cursor_log_copy = snowflake_hook.run(sql=copy_sql)

        #TODO look into ways to return copy metadata (n rows copied, n failures, etc.) right now it just says "1 row affected"
        logging.info(cursor_log_copy)


    def delete_from_source(self, s3_source_keys):
        """
        Delete the object from the source bucket.
        """
        s3_source_hook = S3Hook(aws_conn_id=self.s3_source_conn_id)

        logging.info('Deleting file from source s3')

        s3_source_hook.delete_objects(bucket=s3_source_hook.get_connection(self.s3_source_conn_id).schema, keys=s3_source_keys)
