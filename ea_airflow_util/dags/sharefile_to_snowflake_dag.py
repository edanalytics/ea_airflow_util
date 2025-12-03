from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from ea_airflow_util.callables import jsonl, s3, ea_csv
from ea_airflow_util.callables.airflow import xcom_pull_template
from ea_airflow_util.callables.sharefile import sharefile_copy_file
from ea_airflow_util.providers.sharefile.transfers.sharefile_to_disk import SharefileToDiskOperator
from ea_airflow_util.providers.aws.operators.s3 import S3ToSnowflakeOperator
from ea_airflow_util.dags.ea_custom_dag import EACustomDAG

from airflow.exceptions import AirflowException


class SharefileToSnowflakeDag:
    """
    A class to build an Airflow DAG to extract data from flat files (csv and
    txt) on ShareFile to tables in a Snowflake database schema. 

    Parameters:
    - sharefile_conn_id (str): A Sharefile connection ID.
    - local_base_path (str): A base local path for downloading files.
    - s3_conn_id (str): An Airflow connection ID for AWS S3.
    - s3_bucket (str): An S3 bucket where to stage files.
    - snowflake_conn_id (str): An Airflow connection ID for Snowflake.
    - snowflake_database (str): A Snowflake database name.
    - snowflake_schema (str): A Snowflake schema name.
    - **kwargs: Additional arguments to pass to the Airflow DAG.
    """

    def __init__(
        self,

        sharefile_conn_id: str,

        local_base_path: str,

        s3_conn_id: str,
        s3_bucket: str,

        snowflake_conn_id: str,
        snowflake_database: str,
        snowflake_schema: str,

        **kwargs
    ) -> None:
        self.sharefile_conn_id = sharefile_conn_id

        self.local_base_path = local_base_path

        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket

        self.snowflake_conn_id = snowflake_conn_id
        self.snowflake_database = snowflake_database
        self.snowflake_schema = snowflake_schema

        self.dag = EACustomDAG(**kwargs)

        self.sharefile_task_groups = []

        # Only preprocessors that produce csv files are supported since the 
        # csv_to_jsonl operator expects csv files.
        self.valid_preprocessors = {name: obj for name, obj in vars(ea_csv).items() if callable(obj)}
    
    def build_task_group(
        self,
        group_id,
        sharefile_source_path,
        local_rel_path,
        snowflake_table,
        sharefile_processed_path=None,
        preprocessor=None,
        preprocessor_kwargs=None,
        custom_metadata={},
        full_refresh=False,
        csv_encoding='utf-8',
        **kwargs
    ):
        """Builds a task group to load data from csv and txt files in a
        Sharefile directory to a Snowflake table.
        
        Note that the arguments specified here are relative to the class
        arguments provided at instantiation. For example, the sharefile_path
        argument is relative to the sharefile_conn_id specified at the class
        level.

        Parameters:
        - group_id (str): A name for the Airflow task group.
        - sharefile_source_path (str): A Sharefile path to extract data from.
        - sharefile_processed_path (str): A Sharefile path to move files to
            after they have been processed. If None, do not move processed
            files. Default is None.
        - local_rel_path (str): A local relative path to stage data in with 
            respect to the class's local_base_path. This is also used to
            determine the staging S3 destination relative to the class's S3
            bucket.
        - snowflake_table (str): A Snowflake table name to write data to.
        - preprocessor (callable): A function to preprocess the files before
            loading them into Snowflake. Default is None.
        - preprocessor_kwargs (dict): A dictionary of keyword arguments to pass
            to the preprocessor function. Default is None.
        - custom_metadata (dict): A mapping of metadata field names to values
            to include in the target Snowflake table.
        - full_refresh (bool): If True, performs a full refresh load in
            Snowflake. Default is False.
        - csv_encoding (str): Optional encoding to use for csv files. Default is
            'utf-8'.
        - **kwargs: Additional keyword arguments to pass to the task group.
        """

        with TaskGroup(group_id=group_id, dag=self.dag, **kwargs) as task_group:

            sharefile_to_disk = SharefileToDiskOperator(
                task_id=f"sharefile_to_disk",
                sharefile_conn_id=self.sharefile_conn_id,
                sharefile_path=sharefile_source_path,
                local_path=f'{self.local_base_path}/{local_rel_path}',
                delete_remote=False,
                dag=self.dag
            )

            if preprocessor is not None and preprocessor_kwargs is not None:
                
                if preprocessor not in self.valid_preprocessors:
                    raise AirflowException(f"Invalid file preprocessor '{preprocessor}' provided. Valid preprocessors are: {list(self.valid_preprocessors.keys())}")

                # Create a copy of preprocessor_kwargs to avoid mutating the original dict
                # and ensure proper template rendering in op_kwargs
                op_kwargs = preprocessor_kwargs.copy()
                op_kwargs['path_in'] = xcom_pull_template(sharefile_to_disk)

                preprocess_files = PythonOperator(
                    task_id=f"preprocess_files",
                    python_callable=self.valid_preprocessors[preprocessor],
                    op_kwargs=op_kwargs,
                    dag=self.dag
                )
            else:
                # If no preprocessor is provided, use a dummy operator that
                # just passes the files through
                preprocess_files = PythonOperator(
                    task_id=f"preprocess_files",
                    python_callable=lambda x: x,
                    op_kwargs={'x': xcom_pull_template(sharefile_to_disk)},
                    dag=self.dag
                )

            csv_to_jsonl = PythonOperator(
                task_id=f"csv_to_jsonl",
                python_callable=jsonl.translate_csv_file_to_jsonl,
                op_kwargs={
                    'local_path': xcom_pull_template(preprocess_files),
                    'output_path': None,
                    # S3 to Snowflake task breaks if non-jsonl file retained
                    'delete_csv': True,
                    'csv_encoding': csv_encoding,
                },
                dag=self.dag
            )
            
            disk_to_s3 = PythonOperator(
                task_id=f"disk_to_s3",
                python_callable=s3.disk_to_s3,
                op_kwargs={
                    'local_path': xcom_pull_template(csv_to_jsonl),
                    's3_conn_id': self.s3_conn_id,
                    'bucket': self.s3_bucket,
                    'base_dir': self.local_base_path,
                    'delete_local': False,
                },
                dag=self.dag
            )
            
            s3_to_snowflake = S3ToSnowflakeOperator(
                task_id=f"s3_to_snowflake",
                snowflake_conn_id=self.snowflake_conn_id,
                database=self.snowflake_database,
                schema=self.snowflake_schema,
                table_name=snowflake_table,
                custom_metadata_columns=custom_metadata,
                s3_destination_key=xcom_pull_template(disk_to_s3),
                full_refresh=full_refresh,
                dag=self.dag
            )
            
            if sharefile_processed_path is None:
                (
                    sharefile_to_disk
                    >> preprocess_files
                    >> csv_to_jsonl
                    >> disk_to_s3
                    >> s3_to_snowflake
                )
            else:
                # Move processed files to specific Sharefile location
                move_to_processed = PythonOperator(
                    task_id=f'move_to_processed',
                    python_callable=sharefile_copy_file,
                    op_kwargs={
                        'sharefile_conn_id': self.sharefile_conn_id,
                        'sharefile_path': sharefile_source_path,
                        'sharefile_dest_dir': sharefile_processed_path,
                        'delete_source': True,
                    },
                    dag=self.dag
                )
                (
                    sharefile_to_disk
                    >> preprocess_files
                    >> csv_to_jsonl
                    >> disk_to_s3
                    >> s3_to_snowflake
                    >> move_to_processed
                )

            self.sharefile_task_groups.append(task_group)
