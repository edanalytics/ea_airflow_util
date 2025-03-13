import os
from datetime import datetime

from airflow import DAG
from airflow.models import Param
from airflow.operators.python import PythonOperator

from ea_airflow_util.callables.airflow import skip_if_not_in_params_list
from ea_airflow_util.callables import jsonl, snowflake
from ea_airflow_util.providers.sharefile.transfers.sharefile_to_disk import SharefileToDiskOperator
from edu_edfi_airflow.callables import s3
from ea_airflow_util.providers.aws.operators.s3 import S3ToSnowflakeOperator
from ea_airflow_util.callables.airflow import xcom_pull_template

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from ea_airflow_util import EACustomDAG


class SharefileTransferToSnowflakeDagBuilder:
    """
    This class is responsible for creating an Apache Airflow DAG that automates the process of transferring
    files from ShareFile to Snowflake. 

    Attributes:
        dag_id (str): The ID for the Airflow DAG.
        airflow_default_args (dict): Default arguments passed to the DAG.
        file_sources (list): List of file sources to be processed.
        schedule_interval (str or None): The schedule interval for the DAG.
    """

    def __init__(self,
                dag_id: str,
                airflow_default_args: dict,
                file_sources: dict,
                schedule_interval = None,

                local_base_path: str,
                transform_csv_to_jsonl: bool = False, 

                # sharefile_path: str,
                sharefile_conn_id: str,
                delete_remote: bool = False,
                
                delete_local_csv: bool = False,

                base_s3_destination_key: str,
                s3_conn_id: str,

                snowflake_conn_id: str,
                database: str,
                schema: str,
                full_refresh: bool,
                ):
        self.dag_id = dag_id
        self.airflow_default_args = airflow_default_args
        self.file_sources = file_sources
        self.schedule_interval = schedule_interval

        self.local_base_path = local_base_path
        self.transform_csv_to_jsonl = transform_csv_to_jsonl

        # self.sharefile_path = sharefile_path
        self.sharefile_conn_id = sharefile_conn_id
        self.delete_remote = delete_remote
        
        self.delete_local_csv = delete_local_csv

        self.base_s3_destination_key = base_s3_destination_key
        self.s3_conn_id = s3_conn_id

        self.snowflake_conn_id = snowflake_conn_id
        self.database = database
        self.schema = schema
        self.full_refresh = full_refresh

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

        for file, details in self.file_sources.items():

            check_if_file_in_param = PythonOperator(
                task_id=f"check_{file}",
                python_callable=skip_if_not_in_params_list,
                op_kwargs={
                    'param_name': "file_sources", 
                    'value': file
                    },
                dag=self.dag
                )
            
            transfer_sharefile_to_disk = SharefileToDiskOperator(
                task_id=f"transfer_{file}_to_disk",
                sharefile_conn_id=self.sharefile_conn_id,
                sharefile_path=details['sharefile_path'],
                local_path=os.path.join(self.local_base_path, '{{ds_nodash}}', '{{ts_nodash}}', file),
                delete_remote=self.delete_remote,
                dag=self.dag
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
                dag=self.dag
                )
            
            transfer_disk_to_s3 = PythonOperator(
                task_id=f"transfer_{file}_to_s3",
                python_callable=s3.local_filepath_to_s3,
                op_kwargs={
                    'local_filepath': xcom_pull_template(transfer_sharefile_to_disk),
                    's3_destination_key': f"{self.base_s3_destination_key}/" + '{{ds_nodash}}' + "/" + '{{ts_nodash}}' + f"/{file}",
                    's3_conn_id': self.s3_conn_id
                },
                dag=self.dag
                )
            
            transfer_s3_to_snowflake = S3ToSnowflakeOperator(
                task_id=f"{file}_s3_to_snowflake",
                snowflake_conn_id=self.snowflake_conn_id,
                database=self.database,
                schema=self.schema,
                table_name=file,
                s3_destination_key=self.base_s3_destination_key,
                full_refresh=self.full_refresh,
                dag=self.dag
                )
            
            if self.transform_csv_to_jsonl:
                check_if_file_in_param >> transfer_sharefile_to_disk >> transform_to_jsonl >> transfer_disk_to_s3 >> transfer_s3_to_snowflake
            else:
                check_if_file_in_param >> transfer_sharefile_to_disk >> transfer_disk_to_s3 >> transfer_s3_to_snowflake

        return self.dag