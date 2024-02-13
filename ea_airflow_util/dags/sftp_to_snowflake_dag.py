import os
import logging
import shutil

from functools import partial
from typing import Optional

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.task_group import TaskGroup

from ea_airflow_util.callables import slack


class SFTPToSnowflakeDag:
    """
    This DAG transfers data from an SFTP source into the Snowflake raw data lake. It should be used when data sources
    are not available from an Ed-Fi ODS but need to be brought into the data warehouse.

    """
    def __init__(self,
        s3_conn_id: str,
        snowflake_conn_id: str,
        database: str,
        schema: str,                   

        pool: str,
        do_delete_from_local: Optional[bool] = False,
        slack_conn_id: Optional[str] = None,

        #These parameters can be passed on initialization or when calling the build_tenant_year_resource_taskgroup function, depending on where they are specified in the config
        domain: Optional[str] = None,
        sftp_conn_id: Optional[str] = None,
        sftp_filepath: Optional[str] = None,
        file_pattern: Optional[str] = None,
        local_base_path: Optional[str] = None,
        transform_script: Optional[str] = None,
        full_replace: Optional[bool] = False,  #TODO once on latest version of airflow, use dagrun parameter to allow full_replace runs even if not set here at dag level

        **kwargs
    ) -> None:
        self.s3_conn_id = s3_conn_id
        self.snowflake_conn_id = snowflake_conn_id
        self.database = database
        self.schema = schema

        self.slack_conn_id = slack_conn_id
        self.pool = pool
 
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
        :return:
        """
        # If a Slack connection has been defined, add the failure callback to the default_args.
        if self.slack_conn_id:
            slack_failure_callback = partial(slack.slack_alert_failure, http_conn_id=self.slack_conn_id)
            default_args['on_failure_callback'] = slack_failure_callback

            # Define an SLA-miss callback as well.
            slack_sla_miss_callback = partial(slack.slack_alert_sla_miss, http_conn_id=self.slack_conn_id)
        else:
            slack_sla_miss_callback = None

        return DAG(
            dag_id=dag_id,
            schedule_interval=schedule_interval,
            default_args=default_args,
            catchup=False,
            user_defined_macros={
                'slack_conn_id': self.slack_conn_id,
            },
            render_template_as_native_obj=True,
            max_active_runs=1,
            sla_miss_callback=slack_sla_miss_callback,
            **kwargs
        )
        
    
    def build_tenant_year_resource_taskgroup(self,
        tenant_code: str,
        api_year: int,
        resource_name: str,
        domain: str,
        
        sftp_conn_id: str,
        sftp_filepath: str,
        file_pattern: str,
        local_base_path: str,
        full_replace: bool,
        transform_script: Optional[str] = None,
        
        **kwargs
    ):

        taskgroup_grain = f"{tenant_code}_{api_year}_{resource_name}"

        with TaskGroup(
            group_id=taskgroup_grain,
            dag=self.dag
        ) as tenant_year_task_group:
        
            ## Create local directories for raw and transformed data
            local_date_path = os.path.join(local_base_path, tenant_code, str(api_year), '{{ ds_nodash }}', '{{ ts_nodash }}', resource_name)

            create_local_dir = PythonOperator(
                task_id=f'{taskgroup_grain}_create_local_dir',
                python_callable=self.create_local_directories,
                op_kwargs={
                    'local_date_path': local_date_path
                },
                pool=self.pool,
                dag=self.dag
            )

            raw_dir = os.path.join(local_date_path, 'raw')
            processed_dir = os.path.join(local_date_path, 'processed')

            ## Copy data from SFTP to local raw directory
            sftp_to_local = PythonOperator(
                task_id=f'{taskgroup_grain}_sftp_to_local',
                python_callable=self.sftp_to_local_filepath,
                op_kwargs={
                    'local_path': raw_dir,
                    'sftp_conn_id': sftp_conn_id,
                    'sftp_filepath': sftp_filepath,
                    'file_pattern': file_pattern
                },
                pool=self.pool,
                dag=self.dag
            )

            ## If a transformation script was provided, define the bash command to call it with the source and destination directories as arguments
            ## Otherwise, use a bash exit code 99 to skip this task and use the raw directory as the source for the loading step
            ##
            ## This step uses a BashOperator to allow for the transformation script to be customized and stored in a separate repo.
            ## The PythonOperator requires the callable to be imported from a package/module
            if transform_script:
                transform_bash_command = f'python {transform_script} {raw_dir} {processed_dir}'
                source_dir = processed_dir
            else:
                transform_bash_command = 'exit 99'
                source_dir = raw_dir

            python_transformation = BashOperator(
                task_id=f'{taskgroup_grain}_python_transformation',
                bash_command=transform_bash_command,
                pool=self.pool,
                dag=self.dag
            )

            ## Copy from disk to S3 data lake stage
            datalake_prefix = os.path.join(tenant_code, str(api_year))
            datalake_date_path = os.path.join(datalake_prefix, '{{ ds_nodash }}', '{{ ts_nodash }}', resource_name)

            local_to_s3 = PythonOperator(
                task_id=f'{taskgroup_grain}_local_to_s3',
                python_callable=self.local_filepath_to_s3,
                op_kwargs={
                    'local_filepath': source_dir,
                    's3_destination_key': datalake_date_path
                },
                pool=self.pool,
                dag=self.dag
            )

            ## Copy data from dest bucket (data lake stage) to snowflake raw table
            copy_to_raw = PythonOperator(
                task_id=f'{taskgroup_grain}_copy_to_raw',
                python_callable=self.copy_from_datalake_to_raw,
                op_kwargs={
                    'datalake_date_path': datalake_date_path,
                    'domain': domain,
                    'tenant_code': tenant_code,
                    'api_year': api_year,
                    'resource_name': resource_name,
                    'full_replace': full_replace
                },
                pool=self.pool,
                dag=self.dag
            )

            ## Optionally delete local copies 
            delete_local = PythonOperator(
                task_id=f'{taskgroup_grain}_delete_from_local',
                python_callable=self.delete_from_local,
                op_kwargs={
                    'parent_to_delete': local_date_path
                },
                pool=self.pool,
                dag=self.dag
            )

            create_local_dir >> sftp_to_local >> python_transformation >> local_to_s3 >> copy_to_raw >> delete_local

        return tenant_year_task_group


    def create_local_directories(self, local_date_path):
        """
        Creates subdirectories for raw and processed data at a provided local path.

        :param local_path:     
        :return:
        """        
        subdirs = ['raw', 'processed']

        for dir_name in subdirs:
            os.makedirs(os.path.join(local_date_path, dir_name), exist_ok=True)

        return local_date_path
    

    def sftp_to_local_filepath(self, local_path, sftp_conn_id, sftp_filepath, file_pattern):
        """
        Copies a file or directory from an SFTP to a local directory. If a file pattern has been 
        copies only matching files. 

        :param local_path:
        :param sftp_conn_id
        :param sftp_filepath
        :param file_pattern
        :return:
        """        
        sftp_hook = SFTPHook(sftp_conn_id)

        # If a directory, retrieve all files or only those which match a provided file pattern
        # TODO allow for nested directories
        if sftp_hook.isdir(sftp_filepath):

            if file_pattern:
                file_list = sftp_hook.get_files_by_pattern(sftp_filepath, file_pattern)
            
            else:
                file_list = sftp_hook.list_directory(sftp_filepath, sftp_filepath)

            for file in file_list:
                full_path = os.path.join(sftp_filepath, sftp_filepath, file)
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
    

    def local_filepath_to_s3(self, local_filepath, s3_destination_key):
        """
        Copies data from a file or directory to S3.

        :param local_filepath:
        :param s3_destination_key:       
        :return:
        """ 
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

        return s3_destination_key
    
    
    def copy_from_datalake_to_raw(self, datalake_date_path, domain, tenant_code, api_year, resource_name, full_replace):
        """
        Copy raw data from data lake to data warehouse, including object metadata.
        
        :param datalake_date_path:  
        :param domain
        :param tenant_code
        :param api_year
        :param resource_name
        :param full_replace
        :return:
        """
        delete_sql = f'''
            delete from {self.database}.{self.schema}.{domain}__{resource_name}
            where tenant_code = '{tenant_code}'
              and api_year = '{api_year}'
        '''

        logging.info(f"Copying from data lake to raw: {datalake_date_path}")
        copy_sql = f'''
            copy into {self.database}.{self.schema}.{domain}__{resource_name}
                (tenant_code, api_year, pull_date, pull_timestamp, file_row_number, filename, name, v)
            from (
                select
                    '{tenant_code}' as tenant_code,
                    '{api_year}' as api_year,
                    to_date(split_part(metadata$filename, '/', 3), 'YYYYMMDD') as pull_date,
                    to_timestamp(split_part(metadata$filename, '/', 4), 'YYYYMMDDTHH24MISS') as pull_timestamp,
                    metadata$file_row_number as file_row_number,
                    metadata$filename as filename,
                    '{resource_name}' as name,
                    t.$1 as v
                from @{self.database}.util.airflow_stage/{datalake_date_path}/
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


    def delete_from_local(self, parent_to_delete):
        """
        Deletes local files 
        
        :param parent_to_delete:    
        :return:
        """        
        if self.do_delete_from_local:
            logging.info(f"Removing temporary files written to `{parent_to_delete}`")
            try:
                shutil.rmtree(parent_to_delete)
            except FileNotFoundError:
                pass
        
        else:
            raise AirflowSkipException