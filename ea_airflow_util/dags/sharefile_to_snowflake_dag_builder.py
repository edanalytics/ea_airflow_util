import os

from airflow import DAG
from airflow.models import Param
from airflow.operators.python import PythonOperator

from util import io_helpers

from ea_airflow_util.callables.airflow import skip_if_not_in_params_list, xcom_pull_template
from ea_airflow_util.callables import jsonl, snowflake
from ea_airflow_util.providers.sharefile.transfers.sharefile_to_disk import SharefileToDiskOperator
from edu_edfi_airflow.callables import s3
from ea_airflow_util.providers.aws.operators.s3 import S3ToSnowflakeOperator

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

class SharefileTransferToSnowflakeDagBuilder:
    """
    This class is responsible for creating an Apache Airflow DAG that automates the process of transferring
    files from ShareFile to Snowflake. It defines the steps to handle file transfers, transformations, and
    data loading operations. The DAG is dynamically built based on provided parameters such as file sources
    and schedule interval. Each method in this class corresponds to a specific step in the workflow.

    Attributes:
        dag_id (str): The ID for the Airflow DAG.
        airflow_default_args (dict): Default arguments passed to the DAG.
        file_sources (list): List of file sources to be processed.
        schedule_interval (str or None): The schedule interval for the DAG.
    """
    def __init__(self,
                dag_id,
                airflow_default_args, 
                file_sources, 
                schedule_interval = None
    ):
        self.dag_id = dag_id
        self.airflow_default_args = airflow_default_args
        self.file_sources = file_sources
        self.schedule_interval = schedule_interval

        self.dag = None
        self._initialize_dag()
    
    def _initialize_dag(self):
        """
        Initializes the DAG with the provided configuration and sets up the parameters.

        This method creates a DAG object and configures its parameters, including the list of file sources.
        The file sources are passed as an Airflow parameter, allowing the workflow to be flexible in handling 
        multiple sources.

        This is an internal method used to initialize the DAG.
        """
        params = {
            "file_sources": Param(
                default=list(self.file_sources, {}),
                examples=list(self.file_sources, {}),
                type="list",
                description="Newline-separated list of file sources to pull from ShareFile",
            ),
        }

        self.dag = DAG(
            dag_id=self.dag_id,
            default_args=self.airflow_default_args,
            schedule_interval=self.schedule_interval,
            params=params,
            catchup=False,
        )

    def check_if_file_in_params(self, file):
        """
        Checks if the specified file exists in the provided Airflow parameters list.

        This method creates a PythonOperator task that calls a custom function `skip_if_not_in_params_list` 
        to check if the given file exists in the parameters defined in the DAG.

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
                                    sharefile_path, local_path, delete_remote):
        """
        Transfers a file from ShareFile to a local disk.

        This method creates a SharefileToDiskOperator task that handles the actual transfer of a file from 
        ShareFile to a specified local disk location. The file is downloaded from ShareFile and saved 
        locally, and the option to delete the remote file after transfer is provided.

        Args:
            file (str): The name of the file to transfer.
            sharefile_conn_id (str): The Airflow connection ID for ShareFile.
            sharefile_path (str): The file path in ShareFile from where the file will be fetched.
            local_path (str): The local directory path where the file will be saved.
            delete_remote (bool): Flag indicating whether to delete the remote file after transfer.

        Returns:
            SharefileToDiskOperator: The Airflow task to transfer the file from ShareFile to local disk.
        """
        return SharefileToDiskOperator(
            task_id=f"transfer_{file}_to_disk",
            sharefile_conn_id=sharefile_conn_id,
            sharefile_path=sharefile_path,
            local_path=local_path,
            delete_remote=delete_remote,
            dag=self.dag
        )

    def transform_to_jsonl(self, file, local_path, delete_csv):
        """
        Transforms a CSV file to JSONL format.

        This method creates a PythonOperator task to invoke a transformation function that converts a CSV 
        file into JSONL format. The option to delete the original CSV file after transformation is also 
        supported.

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

    def transfer_disk_to_s3(self, file):
        """
        Transfers a file from local disk to Amazon S3.

        This method creates a PythonOperator task that uploads a file from the local disk to an S3 bucket. 
        The file will be stored in a specific location defined by the S3 destination key.

        Args:
            file (str): The name of the file to transfer.

        Returns:
            PythonOperator: The Airflow task to transfer the file from local disk to S3.
        """
        return PythonOperator(
            task_id=f"transfer_{file}_to_s3",
            python_callable=s3.local_filepath_to_s3,
            op_kwargs={
                'local_filepath': self._generate_file_path(file, 'disk'),
                's3_destination_key': f"ea_research/{file}",
                's3_conn_id': self.dag_configs['s3_conn_id']
            },
            dag=self.dag
        )

    def transfer_s3_to_snowflake(self, file, snowflake_conn_id, database, schema,
                                 full_refresh):
        """
        Transfers a file from Amazon S3 to Snowflake.

        This method creates an S3ToSnowflakeOperator task that loads a file from S3 into a Snowflake table.
        It supports full refresh functionality, which replaces the existing data with the new data.

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
            s3_destination_key=f"ea_research/{file}",
            full_refresh=full_refresh,
            dag=self.dag
        )

    # def build_dag(self):
    #     """Builds the complete DAG, setting up tasks and their dependencies."""
    #     for file, file_configs in self.file_sources_dict.get(self.run_type, {}).items():
    #         # Task for checking if file is in parameters list
    #         check_task = self._check_if_file_in_params(file)
            
    #         # Task for transferring from ShareFile to Disk
    #         transfer_task = self._transfer_sharefile_to_disk(file, file_configs)
            
    #         # Task for transforming from CSV to JSONL
    #         transform_task = self._transform_to_jsonl(transfer_task, file)
            
    #         # Task for transferring from Disk to S3
    #         transfer_to_s3_task = self._transfer_disk_to_s3(file)
            
    #         # Task for transferring from S3 to Snowflake
    #         s3_to_snowflake_task = self._transfer_s3_to_snowflake(file)

    #         # Set task dependencies in a linear flow
    #         check_task >> transfer_task >> transform_task >> transfer_to_s3_task >> s3_to_snowflake_task

    #     return self.dag

