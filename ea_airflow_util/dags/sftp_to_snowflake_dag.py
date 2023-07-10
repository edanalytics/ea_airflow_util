import os
import logging
import shutil
from functools import partial
from typing import Optional

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from ea_airflow_util.hooks.sftp_hook import SFTPHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowSkipException

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
        data_source: str,
        resource_name: str,

        sftp_conn_id: str,
        sftp_filepath: str,
        file_pattern: str,

        local_base_path: str,

        s3_conn_id: str,
        s3_dest_file_extension: str,  
        
        snowflake_conn_id: str,
        database: str,
        schema: str,                   
        full_replace: bool, #TODO once on latest version of airflow, use dagrun parameter to allow full_replace runs even if not set here at dag level

        slack_conn_id: str,
        pool: str,

        transform_script: Optional[str] = None,
        do_delete_from_local: Optional[bool] = False,

        **kwargs
    ) -> None:
        self.tenant_code = tenant_code
        self.api_year = api_year
        self.data_source = data_source
        self.resource_name = resource_name

        self.sftp_conn_id = sftp_conn_id
        self.sftp_filepath = sftp_filepath
        self.file_pattern = file_pattern

        self.local_base_path = local_base_path

        self.s3_conn_id = s3_conn_id
        self.s3_dest_file_extension = s3_dest_file_extension

        self.snowflake_conn_id = snowflake_conn_id
        self.database = database
        self.schema = schema
        self.full_replace = full_replace

        self.slack_conn_id = slack_conn_id
        self.pool = pool

        self.transform_script = transform_script 
        self.do_delete_from_local = do_delete_from_local

        self.dag = self.initialize_dag(**kwargs)


    def initialize_dag(self,
        dag_id: str,
        schedule_interval: str,
        default_args: dict,
        **kwargs
    ) -> DAG:
        """

        :param dag_id:
        :param schedule_interval:
        :param default_args:
        :param kwargs:
        :return:
        """
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
        
    
    def build_sftp_to_snowflake_dag(self):
        
        ## Create local directories for raw and transformed data
        create_local_dir = PythonOperator(
            task_id=f'create_local_dir_{self.resource_name}',
            python_callable=self.create_local_directories,
            pool=self.pool,
            dag=self.dag
        )

        parent_dir = xcom_pull_template(create_local_dir.task_id)
        raw_dir = os.path.join(xcom_pull_template(parent_dir), 'raw')
        processed_dir = os.path.join(xcom_pull_template(parent_dir), 'processed')

        ## Copy data from SFTP to local raw directory
        sftp_to_local = PythonOperator(
            task_id=f'sftp_to_local_{self.resource_name}',
            python_callable=self.sftp_to_local_filepath,
            op_kwargs={
                'local_path': raw_dir
            },
            pool=self.pool,
            dag=self.dag
        )

        ## If a transformation script was provided, define the bash command to call it with the source and destination directories as arguments
        ## Otherwise, call a function to skip this task and use the raw directory as the source for the loading step
        if self.transform_script:
            transform_bash_command = f'python {self.transform_script} {raw_dir} {processed_dir}'
            source_dir = processed_dir
        else:
            transform_bash_command = self.skip_transform_step
            source_dir = raw_dir

        python_transformation = BashOperator(
            task_id=f'python_transformation_{self.resource_name}',
            bash_command=transform_bash_command,
            pool=self.pool,
            dag=self.dag
        )

        ## Copy from disk to S3 data lake stage and optionally delete local data
        datalake_prefix = os.path.join(
            self.tenant_code, str(self.api_year),
            '{{ ds_nodash }}', '{{ ts_nodash }}',
            self.resource_name
        )

        local_to_s3 = PythonOperator(
            task_id=f'local_to_s3_{self.resource_name}',
            python_callable=self.local_filepath_to_s3,
            op_kwargs={
                'local_filepath': source_dir,
                's3_destination_key': datalake_prefix,
                'parent_to_delete': parent_dir
            },
            provide_context=True,
            pool=self.pool,
            dag=self.dag
        )

        ## Copy data from dest bucket (data lake stage) to snowflake raw table
        copy_to_raw = PythonOperator(
            task_id=f'copy_to_raw_{self.resource_name}',
            python_callable=self.copy_from_datalake_to_raw,
            op_kwargs={
                'datalake_prefix': datalake_prefix
            },
            pool=self.pool,
            dag=self.dag
        )

        create_local_dir >> sftp_to_local >> python_transformation >> local_to_s3 >> copy_to_raw


    def create_local_directories(self):
        """
        Creates subdirectories for raw and processed data at a provided local path.

        :param local_path:     
        :return:
        """        
        local_path = os.path.join(self.local_base_path, self.tenant_code, self.resource_name)
        subdirs = ['raw', 'processed']

        for dir_name in subdirs:
            os.makedirs(os.path.join(local_path, dir_name), exist_ok=True)

        return local_path
    

    def sftp_to_local_filepath(self, local_path):
        """
        Copies a file or directory from an SFTP to a local directory. If a file pattern has been 
        copies only matching files. 

        :param local_path:     
        :return:
        """        
        sftp_hook = SFTPHook(self.sftp_conn_id)

        # If a directory, retrieve all files or only those which match a provided file pattern
        # TODO allow for nested directories
        if sftp_hook.isdir(self.sftp_filepath):

            if self.file_pattern:
                file_list = sftp_hook.get_files_by_pattern(self.sftp_filepath, self.file_pattern)
            
            else:
                file_list = sftp_hook.list_directory(self.sftp_filepath, self.sftp_filepath)

            for file in file_list:
                full_path = os.path.join(self.sftp_filepath, self.sftp_filepath, file)
                local_full_path = os.path.join(local_path, file)

                os.makedirs(os.path.dirname(local_full_path), exist_ok=True)

                sftp_hook.retrieve_file(
                    remote_full_path=full_path,
                    local_full_path=local_full_path
                )

        # Otherwise, retrieve the single file
        else:
            local_full_path = os.path.join(local_path, file)

            os.makedirs(os.path.dirname(local_full_path), exist_ok=True)

            sftp_hook.retrieve_file(
                remote_full_path=self.sftp_filepath,
                local_full_path=local_full_path
            )   


    def skip_transform_step():
        """
        Used to bypass the optional Python transformation step with a SkipException.
            
        :return:
        """ 
        raise AirflowSkipException
    

    def local_filepath_to_s3(self, local_filepath, s3_destination_key, parent_to_delete):
        """
        Copies data from a file or directory to S3 and deletes local files if specified.

        :param local_filepath:
        :param s3_destination_key:       
        :return:
        """ 
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
            if self.do_delete_from_local:
                logging.info(f"Removing temporary files written to `{parent_to_delete}`")
                try:
                    shutil.rmtree(parent_to_delete)
                except FileNotFoundError:
                    pass

        return s3_destination_key
    
    
    def copy_from_datalake_to_raw(self, datalake_prefix):
        """
        Copy raw data from data lake to data warehouse, including object metadata.
        
        :param datalake_prefix:    
        :return:
        """
        delete_sql = f'''
            delete from {self.database}.{self.schema}.{self.data_source}__{self.resource_name}
            where tenant_code = '{self.tenant_code}'
              and api_year = '{self.api_year}'
        '''

        logging.info(f"Copying from data lake to raw: {datalake_prefix}")
        copy_sql = f'''
            copy into {self.database}.{self.schema}.{self.data_source}__{self.resource_name}
                (tenant_code, api_year, pull_date, pull_timestamp, file_row_number, filename, name, v)
            from (
                select
                    '{self.tenant_code}' as tenant_code,
                    '{self.api_year}' as api_year,
                    to_date(split_part(metadata$filename, '/', 3), 'YYYYMMDD') as pull_date,
                    to_timestamp(split_part(metadata$filename, '/', 4), 'YYYYMMDDTHH24MISS') as pull_timestamp,
                    metadata$file_row_number as file_row_number,
                    metadata$filename as filename,
                    '{self.resource_name}' as name,
                    t.$1 as v
                from @{self.database}.util.airflow_stage/{datalake_prefix}/
                (file_format => 'json_default') t
            ) FORCE=TRUE
        '''

        # Commit the copy query to Snowflake
        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)

        if self.full_replace:
            cursor_log_delete = snowflake_hook.run(sql=delete_sql)
            logging.info(cursor_log_delete)

        cursor_log_copy = snowflake_hook.run(sql=copy_sql)

        #TODO look into ways to return copy metadata (n rows copied, n failures, etc.) right now it just says "1 row affected"
        logging.info(cursor_log_copy)