import os
from typing import List

from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow_dbt.operators.dbt_operator import DbtRunOperator

from ea_airflow_util.callables.airflow import xcom_pull_template
from ea_airflow_util.callables import jsonl, s3, sharefile
from ea_airflow_util.dags.ea_custom_dag import EACustomDAG
from ea_airflow_util.providers.sharefile.transfers.sharefile_to_disk import SharefileToDiskOperator
from ea_airflow_util.providers.aws.operators.s3 import S3ToSnowflakeOperator


class LoadSharefileCustomUsersDag:
    """

    """    
    def __init__(self,
        *,
        tenant_codes: List[str],
        sharefile_conn_id: str,
        sharefile_base_path: str,
        tmp_dir: str,
        colnames: List[str],

        s3_conn_id: str,
        s3_base_dir: str,
        s3_bucket: str,

        snowflake_conn_id: str,
        snowflake_database: str,
        snowflake_schema: str,
        snowflake_table: str,

        dbt_repo_path: str,
        dbt_target_name: str,
        dbt_bin_path: str,

        heimdall_dag_id: str = None,
        delete_remote: bool = False,
        **kwargs
    ) -> None:
        self.tenant_codes = tenant_codes
        self.sharefile_conn_id = sharefile_conn_id
        self.sharefile_base_path = sharefile_base_path
        self.tmp_dir = tmp_dir
        self.colnames = colnames

        self.s3_conn_id = s3_conn_id
        self.s3_base_dir = s3_base_dir
        self.s3_bucket = s3_bucket

        self.snowflake_conn_id = snowflake_conn_id
        self.snowflake_database = snowflake_database
        self.snowflake_schema = snowflake_schema
        self.snowflake_table = snowflake_table

        self.dbt_repo_path = dbt_repo_path
        self.dbt_target_name = dbt_target_name
        self.dbt_bin_path = dbt_bin_path

        self.heimdall_dag_id = heimdall_dag_id
        self.delete_remote = delete_remote

        self.dag = EACustomDAG(**kwargs)
        
    def build_sharefile_custom_users_dag(self, **kwargs):

        extract_load_taskgroups = []
        for tenant in self.tenant_codes:

            with TaskGroup(
                group_id=tenant,
                dag=self.dag
            ) as tenant_task_group:

                # Checks that the folder exists and contains a file that has been uploaded or modified since the last successful run
                # If the dag is reset and previous runs are lost, all files will be loaded. Models have deduplication to handle this
                # Skips if no new files are found
                # Fails if more than one file is found (according to PS desired bahavior)
                check_for_new_files = PythonOperator(
                    task_id         = f"check_for_new_files",
                    python_callable = sharefile.check_for_new_files,
                    op_kwargs       = {
                        'sharefile_conn_id': self.sharefile_conn_id,
                        'sharefile_path'   : os.path.join(self.sharefile_base_path, tenant),
                        'expected_files'   : 1,
                        'updated_after'    : '{{prev_data_interval_start_success}}'
                    },
                    dag=self.dag
                )

                ### ShareFile to Disk
                sharefile_to_disk = SharefileToDiskOperator(
                    task_id           = f"{tenant}_sharefile_to_disk",
                    sharefile_conn_id = self.sharefile_conn_id,
                    sharefile_path    = os.path.join(self.sharefile_base_path, tenant),
                    local_path        = os.path.join(self.tmp_dir, self.s3_base_dir, '{{ds_nodash}}', '{{ts_nodash}}', tenant),
                    delete_remote     = self.delete_remote,
                    retries           = 3,
                    dag=self.dag
                )

                ### Translate CSV to JSONL
                transform_to_jsonl = PythonOperator(
                    task_id         = f"{tenant}_csv_to_jsonl",
                    python_callable = jsonl.translate_csv_file_to_jsonl,
                    op_kwargs       = {
                        'local_path'   : xcom_pull_template(sharefile_to_disk),
                        'output_path'  : None,
                        'to_snake_case': True,
                        'delete_csv'   : True,
                    },
                    dag=self.dag
                )

                ### Disk to S3
                disk_to_s3 = PythonOperator(
                    task_id         = f"{tenant}_disk_to_s3",
                    python_callable = s3.disk_to_s3,
                    op_kwargs       = {
                        's3_conn_id'  : self.s3_conn_id,
                        'bucket'      : self.s3_bucket,
                        'base_dir'    : self.tmp_dir,
                        'local_path'  : xcom_pull_template(transform_to_jsonl),
                        'delete_local': True,
                        'expected_col_names': self.colnames,
                    },
                    dag=self.dag
                )

                ### S3 to Snowflake
                s3_to_snowflake = S3ToSnowflakeOperator(
                    task_id                 = f"{tenant}_s3_to_snowflake",
                    snowflake_conn_id       = self.snowflake_conn_id,
                    database                = self.snowflake_database,
                    schema                  = self.snowflake_schema,
                    table_name              = self.snowflake_table,
                    custom_metadata_columns = {'tenant_code': f"'{tenant}'"},
                    s3_destination_key      = xcom_pull_template(disk_to_s3),
                    full_refresh            = False,
                    dag=self.dag
                )

                check_for_new_files >> sharefile_to_disk >> transform_to_jsonl >> disk_to_s3 >> s3_to_snowflake

                extract_load_taskgroups.append(tenant_task_group)

        ### Partial dbt run to update the custom user security table
        run_dbt_security_table = DbtRunOperator(
            task_id      = 'run_dbt_security_table',
            dir          = self.dbt_repo_path,
            target       = self.dbt_target_name,
            dbt_bin      = self.dbt_bin_path,
            models       = '+stg_auth__heimdall_custom_users+',
            trigger_rule = 'all_done',
            dag=self.dag
        )

        ### Trigger the Heimdall dag (prod only)
        if self.heimdall_dag_id is not None:
            trigger_heimdall_dag = TriggerDagRunOperator(
                task_id             = 'trigger_heimdall_dag',
                trigger_dag_id      = self.heimdall_dag_id,
                wait_for_completion = False, 
                trigger_rule        = 'all_done',
                dag=self.dag
            )

            extract_load_taskgroups >> run_dbt_security_table >> trigger_heimdall_dag

        else:
            extract_load_taskgroups >> run_dbt_security_table