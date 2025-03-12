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
                dag_id,
                airflow_default_args_dict, 
                file_sources_dict, 
                schedule_interval = None
    ):
        self.dag_id = dag_id
        self.airflow_default_args = airflow_default_args_dict
        self.file_sources = file_sources_dict
        self.schedule_interval = schedule_interval

        self.params_dict = {
            "file_sources": Param(
                default=list(self.file_sources.keys()),
                examples=list(self.file_sources.keys()),
                type="array",
                description="Newline-separated list of file sources to pull from ShareFile",
            ),
        }

        self.dag = EACustomDAG(dag_id=self.dag_id, 
                        params=self.params_dict,
                       default_args=self.airflow_default_args, 
                       schedule_interval=self.schedule_interval
                       )
    
    def build_structured_path(self, base_path, file, separator="/"):
        """
        Constructs a structured path using the current date and time.

        Args:
            base_path (str): The root directory or base key where files should be stored.
            file (str): The filename.
            separator (str, optional): The separator to use for the path. Defaults to '/' for directories
                                       and can be set to an appropriate separator for other storage systems
                                       (e.g., AWS S3 uses '/').
        
        Returns:
            str: The fully structured path where the file will be stored.
        """
        # Get the execution date and time
        execution_date = datetime.now().strftime('%Y%m%d') 
        execution_time = datetime.now().strftime('%H%M%S') 
        
        # Construct the full path (e.g., "/tmp/sharefile/20250312/143500/file.csv" OR "s3://bucket/key/20250312/143500/file.csv")
        structured_path = f"{base_path}{separator}{execution_date}{separator}{execution_time}{separator}{file}"
        
        return structured_path

    def check_if_file_in_params(self, file):
        """
        Checks if the specified file exists in the provided Airflow parameters list.

        Args:
            file (str): The name of the file to check in the parameters.

        Returns:
            PythonOperator: The Airflow task that checks if the file is in the parameters list.
        """
        return PythonOperator(
            task_id=f"check_{file}",
            python_callable=skip_if_not_in_params_list,
            op_kwargs={
                'param_name': "file_sources", 
                'value': file
                },
            dag=self.dag
        )

    def transfer_sharefile_to_disk(self, file, sharefile_conn_id, 
                                    sharefile_path, local_base_path, delete_remote):
        """
        Transfers a file from ShareFile to a local disk.

        Args:
            file (str): The name of the file to transfer.
            sharefile_conn_id (str): The Airflow connection ID for ShareFile.
            sharefile_path (str): The file path in ShareFile from where the file will be fetched.
            base_local_path (str): The root directory where files should be stored.
            delete_remote (bool): Flag indicating whether to delete the remote file after transfer.

        Returns:
            SharefileToDiskOperator: The Airflow task to transfer the file from ShareFile to local disk.
        """
        structured_local_path = self.build_structured_path(local_base_path, file)

        os.makedirs(os.path.dirname(structured_local_path), exist_ok=True)

        return SharefileToDiskOperator(
            task_id=f"transfer_{file}_to_disk",
            sharefile_conn_id=sharefile_conn_id,
            sharefile_path=sharefile_path,
            local_path=structured_local_path,
            delete_remote=delete_remote,
            dag=self.dag
        )

    def transform_to_jsonl(self, file, local_path, delete_csv
                           ) -> PythonOperator:
        """
        Transforms a CSV file to JSONL format.

        Args:
            file (str): The name of the file to transform.
            local_path (str): The local file path of the CSV file to be transformed.
            delete_csv (bool): Flag indicating whether to delete the CSV file after transformation.

        Returns:
            PythonOperator: The Airflow task that transforms the CSV to JSONL.
        """
        return PythonOperator(
            task_id=f"transform_{file}_to_jsonl",
            python_callable=jsonl.translate_csv_file_to_jsonl,
            op_kwargs={
                'local_path': local_path,
                'output_path': None,
                'delete_csv': delete_csv,
                'metadata_dict': {'file_source': file},
            },
            dag=self.dag
        )

    def transfer_disk_to_s3(self, file, local_path, base_s3_key, s3_conn_id
                            ) -> PythonOperator:
        """
        Transfers a file from local disk to Amazon S3.

        Args:
            file (str): The name of the file to transfer.
            local_path (str): The local directory path where the files are saved.
            s3_conn_id (str): The Airflow connection ID for AWS S3.

        Returns:
            PythonOperator: The Airflow task to transfer the file from local disk to S3.
        """
        structured_s3_key = self.build_structured_path(base_s3_key, file, separator="/")

        return PythonOperator(
            task_id=f"transfer_{file}_to_s3",
            python_callable=s3.local_filepath_to_s3,
            op_kwargs={
                'local_filepath': local_path,
                's3_destination_key': structured_s3_key,
                's3_conn_id': s3_conn_id
            },
            dag=self.dag
        )

    def transfer_s3_to_snowflake(self, file, snowflake_conn_id, database, 
                                 schema, s3_destination_key, full_refresh):
        """
        Transfers a file from Amazon S3 to Snowflake.

        Args:
            file (str): The name of the file to transfer.
            snowflake_conn_id (str): The Airflow connection ID for Snowflake.
            database (str): The Snowflake database where the data will be loaded.
            schema (str): The Snowflake schema where the data will be loaded.
            full_refresh (bool): Flag to determine whether to perform a full refresh of the data.

        Returns:
            S3ToSnowflakeOperator: The Airflow task to transfer the file from S3 to Snowflake.
        """
        return S3ToSnowflakeOperator(
            task_id=f"{file}_s3_to_snowflake",
            snowflake_conn_id=snowflake_conn_id,
            database=database,
            schema=schema,
            table_name=file,
            s3_destination_key=s3_destination_key,
            full_refresh=full_refresh,
            dag=self.dag
        )