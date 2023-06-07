import logging
from functools import partial

from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.helpers import chain
from .dag_util.xcom_util import xcom_pull_template
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator

import ea_airflow_util.dags.dag_util.slack_callbacks as slack_callbacks
from .operators.loop_s3_file_transform_operator import LoopS3FileTransformOperator

from airflow import DAG

import os

class S3ToSnowflakeDag():
    """
    This DAG transfers data from an S3 bucket location into the Snowflake raw data lake. It should be used when data sources
    are not available from an Ed-Fi ODS but need to be brought into the data warehouse.

    """
    def __init__(self,
        *,
        tenant_code: str,
        api_year: int,

        database: str,
        schema: str,
        data_source: str,
        resource_names: str,
        do_delete_from_source: bool = True,

        s3_source_conn_id : str,
        s3_dest_conn_id : str,
        s3_dest_file_extension: str,
        transform_script: str,
        slack_conn_id : str,
        snowflake_conn_id : str,
        pool     : str,

        **kwargs
    ) -> None:
        self.tenant_code = tenant_code
        self.api_year = api_year

        self.data_source = data_source
        self.resource_names = resource_names
        self.do_delete_from_source = do_delete_from_source
        
        self.s3_source_conn_id = s3_source_conn_id
        self.s3_dest_conn_id = s3_dest_conn_id
        self.s3_dest_file_extension = s3_dest_file_extension
        self.transform_script = transform_script
        self.snowflake_conn_id = snowflake_conn_id
        self.database = database
        self.schema = schema
        self.slack_conn_id = slack_conn_id
        self.pool = pool

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
        )

    def copy_from_datalake_to_raw(self, resource_name, datalake_prefix):
        """
        Copy raw data from data lake to data warehouse, including object metadata.
        """
        
        logging.info(f"Copying from data lake to raw: {datalake_prefix}")
        # TODO: should we have FORCE=TRUE? this is useful if data have been deleted from raw & want to re-load
        # if so, add this line `on_error='continue', FORCE = TRUE`
        sql = f'''
            copy into {self.database}.{self.schema}.{self.data_source}__{resource_name} (tenant_code, api_year, pull_date, pull_timestamp, file_row_number, filename, name, v)
            from (
                select
                    split_part(metadata$filename, '/', 1) as tenant_code,
                    split_part(metadata$filename, '/', 2) as api_year,
                    to_date(split_part(metadata$filename, '/', 3), 'YYYYMMDD') as pull_date,
                    to_timestamp(split_part(metadata$filename, '/', 4), 'YYYYMMDDTHH24MISS') as pull_timestamp,
                    metadata$file_row_number as file_row_number,
                    metadata$filename as filename,
                    '{resource_name}' as name,
                    t.$1 as v
                from @{self.database}.util.airflow_stage/{datalake_prefix}/
                (file_format => 'json_default') t
            )
        '''
            
        copy_to_snowflake_raw = SnowflakeOperator(
        task_id=f'copy_to_snowflake_raw',
        snowflake_conn_id=self.snowflake_conn_id,
        sql=sql
        )
        #todo ask jay if this operator within operator makes sense?
        #todo how to get log to show actual rows copied? right now it says "1 row affected" no matter what
        copy_to_snowflake_raw.execute(dict())

    def delete_from_source(self, s3_source_hook, s3_source_bucket, s3_source_keys):
        """
        Delete the object from the source bucket.
        """
        logging.info('Deleting file from source s3')
        # TODO should we delete the full dated folder afterward? or leave it there as empty record that data were once there?
        s3_source_hook.delete_objects(bucket=s3_source_bucket, keys=s3_source_keys)

    def build_s3_to_snowflake_dag(self,
                                  **kwargs):

        for resource_name in self.resource_names:

            s3_source_prefix = os.path.join(self.tenant_code,
                                            self.data_source,
                                            str(self.api_year),
                                            '{{ ds_nodash }}',
                                            resource_name)
            s3_source_hook = S3Hook(aws_conn_id=self.s3_source_conn_id)
            s3_source_bucket = s3_source_hook.get_connection(self.s3_source_conn_id).schema   

            datalake_prefix = os.path.join(self.tenant_code, 
                                        str(self.api_year), 
                                        '{{ ds_nodash }}',
                                        '{{ ts_nodash }}',
                                        resource_name)
            
            list_s3_objects = S3ListOperator(
                task_id=f'list_s3_objects_{resource_name}',
                bucket=s3_source_bucket,
                prefix=s3_source_prefix,
                delimiter='',
                aws_conn_id=self.s3_source_conn_id,
                dag=self.dag
            )

            if self.s3_dest_conn_id:
                transfer_s3_to_s3 = LoopS3FileTransformOperator(
                    task_id=f'transfer_s3_to_s3_{resource_name}',
                    source_s3_keys= xcom_pull_template(f'list_s3_objects_{resource_name}'),
                    dest_s3_prefix=datalake_prefix,
                    transform_script=self.transform_script,
                    source_aws_conn_id=self.s3_source_conn_id,
                    dest_aws_conn_id=self.s3_dest_conn_id,
                    # TODO: should this be configurable by resource? currently it's not
                    dest_s3_file_extension=self.s3_dest_file_extension,
                    # TODO: should this always be true? if false, and you are running an identically named file to a previous run (in same directory), the new file will be ignored
                    # this most commonly impacts testing, will rarely occur in real time, unless you do mulitple dag runs per day?
                    replace=True,
                    dag=self.dag)
            else:
                transfer_s3_to_s3 = None

            copy_to_raw = PythonOperator(
                task_id=f'copy_to_raw_{resource_name}',
                python_callable=self.copy_from_datalake_to_raw,
                op_kwargs={
                    'resource_name': resource_name,
                    'datalake_prefix': datalake_prefix
                },
                dag=self.dag
            )

            # TODO make optional
            # only delete from source if we transferred data to a different bucket
            if self.s3_dest_conn_id and self.do_delete_from_source:
                delete_from_source = PythonOperator(
                    task_id=f'delete_from_source_{resource_name}',
                    python_callable=self.delete_from_source,
                    op_kwargs={
                        's3_source_hook': s3_source_hook,
                        's3_source_bucket': s3_source_bucket,
                        's3_source_keys': xcom_pull_template(f'list_s3_objects_{resource_name}')
                    },
                    dag=self.dag
                )
            else:
                delete_from_source = None

            ### Default route: List -> Transfer -> Copy -> Delete from source
            task_order = (
                list_s3_objects,
                transfer_s3_to_s3,
                copy_to_raw,
                delete_from_source
            )

            chain(*filter(None, task_order))  # Chain all defined operators into task-order.