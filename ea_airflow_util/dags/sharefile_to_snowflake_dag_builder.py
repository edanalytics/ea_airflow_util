from datetime import datetime
import logging
import os

from airflow import DAG
from airflow.models import Param
from airflow.operators.python import PythonOperator

from edu_edfi_airflow.callables import s3

from ea_airflow_util.callables.airflow import skip_if_not_in_params_list
from ea_airflow_util.callables import jsonl, snowflake
from ea_airflow_util.providers.sharefile.transfers.sharefile_to_disk import SharefileToDiskOperator
from ea_airflow_util.providers.aws.operators.s3 import S3ToSnowflakeOperator
from ea_airflow_util.callables.airflow import xcom_pull_template
from ea_airflow_util.providers.sharefile.hooks.sharefile import SharefileHook

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from ea_airflow_util import EACustomDAG

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

class SharefileTransferToSnowflakeDagBuilder:
    """
    This class is responsible for creating an Apache Airflow DAG that automates the process of transferring
    files from ShareFile to Snowflake. 

    Parameters:
        - dag_id (str): ID of the DAG to be created.
        - airflow_default_args (dict): Default arguments to pass to the DAG.
        - file_sources (dict): A mapping of file source names to their ShareFile paths.
        - local_base_path (str): Base local path for downloading files.
        - sharefile_conn_id (str): Airflow connection ID for ShareFile.
        - base_s3_destination_key (str): Base S3 key (path prefix) for uploads.
        - s3_conn_id (str): Airflow connection ID for AWS S3.
        - snowflake_conn_id (str): Airflow connection ID for Snowflake.
        - database (str): Snowflake database name.
        - schema (str): Snowflake schema name.
        - full_refresh (bool): If True, performs a full refresh load in Snowflake.
        - schedule_interval (str or timedelta, optional): Airflow schedule interval for the DAG.
        - transform_csv_to_jsonl (bool, optional): If True, converts downloaded CSV files to JSONL format.
        - delete_remote (bool, optional): If True, deletes the original file from ShareFile after download.
        - delete_local_csv (bool, optional): If True, deletes the CSV after transforming to JSONL.
        - transfer_s3_to_snowflake (BaseOperator, optional): Custom operator for the S3 to Snowflake transfer step.
        - **kwargs: Additional arguments passed to Airflow operators.
    """
    def __init__(self,
                dag_id: str,
                airflow_default_args: dict,
                file_sources: dict,
                local_base_path: str,
                sharefile_conn_id: str,
                base_s3_destination_key: str,
                s3_conn_id: str,
                snowflake_conn_id: str,
                database: str,
                schema: str,
                full_refresh: bool,
                schedule_interval = None,
                transform_csv_to_jsonl: bool = False, 
                delete_remote: bool = False,
                delete_local_csv: bool = False,
                transfer_s3_to_snowflake = None,
                **kwargs
                ):
        self.dag_id = dag_id
        self.airflow_default_args = airflow_default_args
        self.file_sources = file_sources
        self.schedule_interval = schedule_interval

        self.local_base_path = local_base_path
        self.transform_csv_to_jsonl = transform_csv_to_jsonl

        self.sharefile_conn_id = sharefile_conn_id
        self.delete_remote = delete_remote
        
        self.delete_local_csv = delete_local_csv

        self.base_s3_destination_key = base_s3_destination_key
        self.s3_conn_id = s3_conn_id

        self.snowflake_conn_id = snowflake_conn_id
        self.database = database
        self.schema = schema
        self.full_refresh = full_refresh
        self.transfer_s3_to_snowflake = transfer_s3_to_snowflake

        self.logger = logging.getLogger(self.__class__.__name__)

        self.params_dict = {
            "file_sources": Param(
                default=list(self.file_sources.keys()) if self.file_sources else [],
                examples=list(self.file_sources.keys()),
                type="array",
                description="Newline-separated list of file sources to pull from ShareFile",
                ),
                }

        self.dag = EACustomDAG(dag_id=self.dag_id, 
                               default_args=self.airflow_default_args, 
                               schedule_interval=self.schedule_interval,
                               params=self.params_dict,
                               catchup=False
                               )
    
    def build_sharefile_to_snowflake_dag(self, **kwargs):
        """
        Builds the DAG with tasks for each file source, including:
        - Conditional execution based on DAG parameters.
        - Downloading from ShareFile to local disk (supports both folder and single file paths).
        - Optional transformation from CSV to JSONL.
        - Upload to S3.
        - Load into Snowflake using the default or a custom operator.

        Returns:
            airflow.DAG: The constructed Airflow DAG instance.
        """
        for file, details in self.file_sources.items():

            check_if_file_in_param = PythonOperator(
                task_id=f"check_{file}",
                python_callable=skip_if_not_in_params_list,
                op_kwargs={
                    'param_name': "file_sources", 
                    'value': file
                    },
                dag=self.dag,
                **kwargs
                )

            transfer_sharefile_to_disk = SharefileToDiskOperator(
                task_id=f"transfer_{file}_to_disk",
                sharefile_conn_id=self.sharefile_conn_id,
                sharefile_path=details['sharefile_path'],
                local_path=os.path.join(self.local_base_path, '{{ds_nodash}}', '{{ts_nodash}}', file),
                delete_remote=self.delete_remote,
                dag=self.dag,
                **kwargs
                )
            
            transform_to_jsonl = PythonOperator(
                task_id=f"transform_{file}_to_jsonl",
                python_callable=jsonl.translate_csv_file_to_jsonl,
                op_kwargs={
                    'local_path': xcom_pull_template(transfer_sharefile_to_disk),
                    'output_path': None,
                    'delete_csv': self.delete_local_csv,
                    'metadata_dict': {'file_source': file},
                },
                dag=self.dag,
                **kwargs
                )
            
            transfer_disk_to_s3 = PythonOperator(
                task_id=f"transfer_{file}_to_s3",
                python_callable=s3.local_filepath_to_s3,
                op_kwargs={
                    'local_filepath': xcom_pull_template(transfer_sharefile_to_disk),
                    's3_destination_key': f"{self.base_s3_destination_key}/" + '{{ds_nodash}}' + "/" + '{{ts_nodash}}' + f"/{file}",
                    's3_conn_id': self.s3_conn_id
                },
                dag=self.dag,
                **kwargs
                )
            
            if not self.transfer_s3_to_snowflake:
                transfer_s3_to_snowflake = S3ToSnowflakeOperator(
                    task_id=f"{file}_s3_to_snowflake",
                    snowflake_conn_id=self.snowflake_conn_id,
                    database=self.database,
                    schema=self.schema,
                    table_name=file,
                    s3_destination_key=self.base_s3_destination_key,
                    full_refresh=self.full_refresh,
                    dag=self.dag,
                    **kwargs
                    )
            else: 
                transfer_s3_to_snowflake = transfer_s3_to_snowflake
            
            if self.transform_csv_to_jsonl:
                check_if_file_in_param >> transfer_sharefile_to_disk >> transform_to_jsonl >> transfer_disk_to_s3 >> transfer_s3_to_snowflake
            else:
                check_if_file_in_param >> transfer_sharefile_to_disk >> transfer_disk_to_s3 >> transfer_s3_to_snowflake

        return self.dag