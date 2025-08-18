import os
import logging

from typing import Callable

from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from ea_airflow_util.callables import jsonl, s3
from ea_airflow_util.callables.airflow import skip_if_not_in_params_list
from ea_airflow_util.callables.airflow import xcom_pull_template
from ea_airflow_util.callables.sharefile import sharefile_copy_file
from ea_airflow_util.providers.sharefile.transfers.sharefile_to_disk import SharefileToDiskOperator
from ea_airflow_util.providers.aws.operators.s3 import S3ToSnowflakeOperator
from ea_airflow_util.dags.ea_custom_dag import EACustomDAG


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

class SharefileToSnowflakeDag:
    """
    A class to build an Airflow DAG to extract data from flat files on ShareFile
    and load them to tables in Snowflake. 

    Parameters:
        - dag_id (str): ID of the DAG to be created.
        - airflow_default_args (dict): Default arguments to pass to the DAG.
        - schedule_interval (str or timedelta, optional): Airflow schedule
            interval for the DAG.
        - file_sources (list[dict]): A list of dictionaries, one for each file
            source (i.e. Sharefile file or folder) to load, containing the
            following key-value pairs:
            
            name (str): A name for the Sharefile file source.
            sharefile_path (str): A file or folder path in Sharefile.
            local_path (str): A file path to stage the file in locally.
            snowflake_table (str): A Snowflake table name to load the Sharefile
                source file(s) to.
            metadata_fields (dict, optional): A mapping of metadata field names
                to values to include in the target Snowflake table.

        - sharefile_conn_id (str): Airflow connection ID for ShareFile.
        - delete_remote (bool, optional): If True, deletes the original file
            from ShareFile after download.
        - local_base_path (str): Base local path for downloading files.
        - s3_conn_id (str): Airflow connection ID for AWS S3.
        - s3_bucket (str): S3 bucket where to stage files.
        - delete_local (bool, optional): If True, deletes the local file(s)
            after loading to S3.
        - snowflake_conn_id (str): Airflow connection ID for Snowflake.
        - database (str): Snowflake database name.
        - schema (str): Snowflake schema name.
        - full_refresh (bool): If True, performs a full refresh load in
            Snowflake.        
        - **kwargs: Additional arguments passed to Airflow operators.
    """
    def __init__(
        self,
        dag_id: str,
        airflow_default_args: dict,
        file_sources: list[dict],
        sharefile_conn_id: str,
        local_base_path: str,
        s3_conn_id: str,
        s3_bucket: str,
        snowflake_conn_id: str,
        database: str,
        schema: str,
        schedule_interval = None,
        delete_remote: bool = False,
        delete_local: bool = False,
        full_refresh: bool = False,
        **kwargs
    ):
        self.dag_id = dag_id
        self.airflow_default_args = airflow_default_args
        self.file_sources = file_sources
        self.schedule_interval = schedule_interval

        self.sharefile_conn_id = sharefile_conn_id
        self.delete_remote = delete_remote

        self.local_base_path = local_base_path

        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.delete_local = delete_local

        self.snowflake_conn_id = snowflake_conn_id
        self.database = database
        self.schema = schema
        self.full_refresh = full_refresh

        self.logger = logging.getLogger(self.__class__.__name__)

        self.params_dict = {
            "file_sources": Param(
                default=[file_source['name'] for file_source in self.file_sources],
                examples=[file_source['name'] for file_source in self.file_sources],
                type="array",
                description="Newline-separated list of file sources to pull from ShareFile",
            ),
        }

        self.dag = EACustomDAG(
            dag_id=self.dag_id, 
            default_args=self.airflow_default_args, 
            schedule_interval=self.schedule_interval,
            params=self.params_dict,
            catchup=False
        )

    def build_sharefile_to_snowflake_dag(self, **kwargs):
        """
        Builds the DAG with tasks for each file source, including:
        - Conditional execution based on DAG parameters.
        - Downloading from ShareFile to local disk (supports both folder and
            single file paths).
        - Optional transformation from CSV to JSONL.
        - Upload to S3.
        - Load into Snowflake using the default or a custom operator.

        Returns:
            airflow.DAG: The constructed Airflow DAG instance.
        """
        for file_source in self.file_sources:
            
            # One task group per file source
            with TaskGroup(
                group_id=f"{file_source['name']}_to_snowflake",
                dag=self.dag
            ) as file_source_task_group:

                check_source_in_params = PythonOperator(
                    task_id=f"{file_source['name']}_check_in_params",
                    python_callable=skip_if_not_in_params_list,
                    op_kwargs={
                        'param_name': "file_sources", 
                        'value': file_source['name'],
                    },
                    dag=self.dag
                )

                sharefile_to_disk = SharefileToDiskOperator(
                    task_id=f"{file_source['name']}_to_disk",
                    sharefile_conn_id=self.sharefile_conn_id,
                    sharefile_path=file_source['sharefile_path'],
                    local_path=file_source['local_path'],
                    delete_remote=self.delete_remote,
                    dag=self.dag
                )
        
                transform_to_jsonl = PythonOperator(
                    task_id=f"{file_source['name']}_to_jsonl",
                    python_callable=jsonl.translate_csv_file_to_jsonl,
                    op_kwargs={
                        'local_path': xcom_pull_template(sharefile_to_disk),
                        'output_path': None,
                        # Subsequent tasks break if csv's are retained
                        'delete_csv': True,
                    },
                    dag=self.dag
                )
                
                disk_to_s3 = PythonOperator(
                    task_id=f"{file_source['name']}_to_s3",
                    python_callable=s3.disk_to_s3,
                    op_kwargs={
                        'local_path': xcom_pull_template(transform_to_jsonl),
                        's3_conn_id': self.s3_conn_id,
                        'bucket': self.s3_bucket,
                        'base_dir': self.local_base_path,
                        'delete_local': self.delete_local,
                    },
                    dag=self.dag
                )
                
                s3_to_snowflake = S3ToSnowflakeOperator(
                    task_id=f"{file_source['name']}_to_snowflake",
                    snowflake_conn_id=self.snowflake_conn_id,
                    database=self.database,
                    schema=self.schema,
                    table_name=file_source['snowflake_table'],
                    custom_metadata_columns=file_source['metadata_fields'],
                    s3_destination_key=xcom_pull_template(disk_to_s3),
                    full_refresh=self.full_refresh,
                    dag=self.dag
                )

                # Move processed files to specific Sharefile location
                move_to_processed = PythonOperator(
                    task_id=f'{file_source['name']}_to_processed',
                    python_callable=sharefile_copy_file,
                    op_kwargs={
                        'sharefile_conn_id': self.sharefile_conn_id,
                        'sharefile_path': file_source['sharefile_path'],
                        'sharefile_dest_dir': file_source['processed_dir'],
                        'delete_source': True,
                    },
                    dag=self.dag
                )

                (
                    check_source_in_params
                    >> sharefile_to_disk
                    >> transform_to_jsonl
                    >> disk_to_s3
                    >> s3_to_snowflake
                    >> move_to_processed
                )

        return self.dag