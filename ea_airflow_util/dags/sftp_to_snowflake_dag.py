import os
import logging

from functools import partial
from typing import Callable, Optional

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from ea_airflow_util.hooks.sftp_hook import SFTPHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.helpers import chain

import ea_airflow_util.dags.dag_util.slack_callbacks as slack_callbacks
from .dag_util.xcom_util import xcom_pull_template


class SFTPToSnowflakeDag():
    """
    This DAG transfers data from an SFTP source into the Snowflake raw data lake. It should be used when data sources
    are not available from an Ed-Fi ODS but need to be brought into the data warehouse.

    """
    def __init__(self,

        tenant_code: str,
        api_year: int,

        sftp_conn_id: str,
        sftp_filepath: str,
        local_path: str,
        
        snowflake_conn_id: str,
        database: str,
        schema: str,

        data_source: str,
        resource_names: str,
        transform_script: str,
        transform_script_args: str,                      
                    
        s3_conn_id: str,
        s3_dest_file_extension: str,

        #TODO once on latest version of airflow, use dagrun parameter to allow full_replace runs even if not set here at dag level
        full_replace: bool,

        slack_conn_id: str,
        pool: str,

        file_patterns: str = None,
        do_delete_from_source: bool = True,   

        **kwargs
    ) -> None:
        self.tenant_code = tenant_code
        self.api_year = api_year

        self.sftp_conn_id = sftp_conn_id
        self.sftp_filepath = sftp_filepath
        self.local_path = local_path
        self.file_patterns = file_patterns

        self.snowflake_conn_id = snowflake_conn_id
        self.database = database
        self.schema = schema

        self.data_source = data_source
        self.resource_names = resource_names
        self.transform_script = transform_script 
        self.transform_script_args = transform_script_args

        self.do_delete_from_source = do_delete_from_source

        self.s3_conn_id = s3_conn_id
        self.s3_dest_file_extension = s3_dest_file_extension

        self.full_replace = full_replace

        self.slack_conn_id = slack_conn_id
        self.pool = pool

        self.dag = self.initialize_dag(**kwargs)


    def initialize_dag(self,
        dag_id: str,
        schedule_interval: str,
        default_args: dict,
        **kwargs
    ) -> DAG:

        # If a Slack connection has been defined, add the failure callback to the default_args.
        if self.slack_conn_id:
            slack_failure_callback = partial(slack_callbacks.slack_alert_failure, http_conn_id=self.slack_conn_id)
            default_args['on_failure_callback'] = slack_failure_callback

            # Define an SLA-miss callback as well.
            slack_sla_miss_callback = partial(slack_callbacks.slack_alert_sla_miss, http_conn_id=self.slack_conn_id)
        else:
            slack_sla_miss_callback = None

        return DAG(
            dag_id=dag_id,
            schedule_interval=schedule_interval,
            default_args=default_args,
            catchup=False,
            render_template_as_native_obj=True,
            max_active_runs=1,
            sla_miss_callback=slack_sla_miss_callback,
            **kwargs
        )

    
    def build_sftp_to_snowflake_dag(self, **kwargs):

        for resource_name, file_pattern in zip(self.resource_names, self.file_patterns):

            ## Copy data from SFTP to disk
            sftp_to_local = PythonOperator(
                task_id=f'sftp_to_local_{resource_name}',
                python_callable=self.sftp_to_local_filepath,
                op_kwargs={
                    'resource_name': resource_name,
                    'file_pattern': file_pattern
                },
                pool=self.pool,
                dag=self.dag
            )

            ## Optional Python preprocessing step
            if self.transform_script:
                csv_filename = os.path.join(self.local_path, resource_name + '.csv')
                json_filename = os.path.join(self.local_path, resource_name + '.json')

                python_preprocess = BashOperator(
                    task_id=f'preprocess_python_{resource_name}',
                    bash_command=f'python {self.transform_script} {csv_filename} {json_filename}',
                    pool=self.pool,
                    dag=self.dag
                )
            else:
                python_preprocess = None

            ## Copy from disk to S3 data lake stage and optionally delete local data
            datalake_prefix = os.path.join(
                self.tenant_code, str(self.api_year),
                '{{ ds_nodash }}', '{{ ts_nodash }}',
                resource_name
            )

            local_to_s3 = PythonOperator(
                task_id=f'local_to_s3_{resource_name}',
                python_callable=self.local_filepath_to_s3,
                op_kwargs={
                    'local_filepath': os.path.join(self.local_path, resource_name + '.json'), #TO DO!!
                    's3_destination_key': os.path.join(datalake_prefix, resource_name + '.json'),
                    'remove_local_filepath': False,
                },
                provide_context=True,
                pool=self.pool,
                dag=self.dag
            )

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

            ### Default route: SFTP to local -> Transform -> Local to S3 -> Copy to raw -> Delete from source
            task_order = (
                sftp_to_local,
                python_preprocess,
                local_to_s3,
                copy_to_raw
            )

            chain(*filter(None, task_order))  # Chain all defined operators into task-order.


    def sftp_to_local_filepath(self, resource_name, file_pattern):

        sftp_hook = SFTPHook(self.sftp_conn_id)

        # If a directory, retrieve all files 
        # TO DO: allow for nested directories
        if sftp_hook.isdir(self.sftp_filepath):

            if file_pattern:
                file_list = sftp_hook.get_files_by_pattern(self.sftp_filepath, file_pattern)
            
            else:
                file_list = sftp_hook.list_directory(self.sftp_filepath, self.sftp_filepath)

            for file in file_list:
                full_path = os.path.join(self.sftp_filepath, self.sftp_filepath, file)
                #Need to fix this - can't use resource_name for all files in directory
                local_full_path = os.path.join(self.local_path, resource_name + '.csv') #TO DO: determine based on file type

                os.makedirs(os.path.dirname(local_full_path), exist_ok=True)

                sftp_hook.retrieve_file(
                    remote_full_path=full_path,
                    local_full_path=local_full_path
                )

        # Otherwise, retrieve the single file
        else:
            local_full_path = os.path.join(self.local_path, resource_name + '.csv') #TO DO: determine based on file type

            os.makedirs(os.path.dirname(local_full_path), exist_ok=True)

            sftp_hook.retrieve_file(
                remote_full_path=sftp_filepath,
                local_full_path=local_full_path
            )   


    def local_filepath_to_s3(self, local_filepath, s3_destination_key, remove_local_filepath = False):

        try:
            s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)
            s3_bucket = s3_hook.get_connection(self.s3_conn_id).schema

            # If a directory, upload all files to S3.
            if os.path.isdir(local_filepath):
                for root, dirs, files in os.walk(local_filepath):
                    for file in files:
                        full_path = os.path.join(root, file)
                        s3_full_path = os.path.join(s3_destination_key, file)

                        s3_hook.load_file(
                            filename=full_path,
                            bucket_name=s3_bucket,
                            key=s3_full_path,
                            encrypt=True,
                            replace=True
                        )

            # Otherwise, upload the single file
            else:
                s3_hook.load_file(
                    filename=local_filepath,
                    bucket_name=s3_bucket,
                    key=s3_destination_key,
                    encrypt=True,
                    replace=True
                )

        # Regardless, delete the local files if specified.
        finally:
            if remove_local_filepath:
                logging.info(f"Removing temporary files written to `{local_filepath}`")
                try:
                    if os.path.isdir(local_filepath):
                        os.rmdir(local_filepath)
                    else:
                        os.remove(local_filepath)
                except FileNotFoundError:
                    pass

        return s3_destination_key
    
    
    def copy_from_datalake_to_raw(self, resource_name, datalake_prefix, full_replace):
        """
        Copy raw data from data lake to data warehouse, including object metadata.
        """

        delete_sql = f'''
            delete from {self.database}.{self.schema}.{self.data_source}__{resource_name}
            where tenant_code = '{self.tenant_code}'
              and api_year = '{self.api_year}'
        '''

        logging.info(f"Copying from data lake to raw: {datalake_prefix}")
        copy_sql = f'''
            copy into {self.database}.{self.schema}.{self.data_source}__{resource_name}
                (tenant_code, api_year, pull_date, pull_timestamp, file_row_number, filename, name, v)
            from (
                select
                    '{self.tenant_code}' as tenant_code,
                    '{self.api_year}' as api_year,
                    to_date(split_part(metadata$filename, '/', 3), 'YYYYMMDD') as pull_date,
                    to_timestamp(split_part(metadata$filename, '/', 4), 'YYYYMMDDTHH24MISS') as pull_timestamp,
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