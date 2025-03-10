import os

from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.utils.helpers import chain


from ea_airflow_util.dags.ea_custom_dag import EACustomDAG
from ea_airflow_util.callables.airflow import xcom_pull_template
from ea_airflow_util.callables import s3
from ea_airflow_util.providers.aws.operators.s3 import LoopS3FileTransformOperator, S3ToSnowflakeOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

class S3ToSnowflakeDag:
    """
    This DAG transfers data from an S3 bucket location into the Snowflake raw data lake. It should be used when data sources
    are not available from an Ed-Fi ODS but need to be brought into the data warehouse.

    """
    def __init__(self,
        *,
        tenant_code: str,
        api_year: int,

        snowflake_conn_id: str,
        database: str,
        schema: str,

        data_source: str,
        resource_names: str,
        transform_script: str,
        prefix_the_source_name: bool = True,

        s3_source_conn_id: str,
        s3_dest_conn_id: str,
        s3_dest_file_extension: str,
        is_manual_upload: bool = False,

        pool: str,
        full_replace: bool = False,  #TODO once on latest version of airflow, use dagrun parameter to allow full_replace runs even if not set here at dag level

        do_delete_from_source: bool = True,
        use_s3_sensor: bool = False,  # New parameter to enable/disable sensor
        **kwargs
    ) -> None:
        self.tenant_code = tenant_code
        self.api_year = api_year

        self.snowflake_conn_id = snowflake_conn_id
        self.database = database
        self.schema = schema

        self.data_source = data_source
        self.resource_names = resource_names
        self.transform_script = transform_script      
        self.do_delete_from_source = do_delete_from_source
        self.prefix_the_source_name = prefix_the_source_name
        
        self.s3_source_conn_id = s3_source_conn_id
        self.s3_dest_conn_id = s3_dest_conn_id
        self.s3_dest_file_extension = s3_dest_file_extension
        self.is_manual_upload = is_manual_upload

        self.full_replace = full_replace
        self.pool = pool

        self.use_s3_sensor = use_s3_sensor

        self.dag = EACustomDAG(**kwargs)

    
    def build_s3_to_snowflake_dag(self, **kwargs):

        for resource_name in self.resource_names:

            # different source prefix depending on whether the upload to external bucket is manual or not
            if self.is_manual_upload:
                s3_source_prefix = os.path.join(
                    self.tenant_code, self.data_source,
                    resource_name, str(self.api_year), 
                    '{{ ds_nodash }}'
                ) 
            else:
                s3_source_prefix = os.path.join(
                    self.tenant_code, self.data_source,
                    str(self.api_year), '{{ ds_nodash }}',
                    resource_name
                )
            
            datalake_prefix = os.path.join(
                self.tenant_code, str(self.api_year),
                '{{ ds_nodash }}', '{{ ts_nodash }}',
                resource_name
            )

            # Conditionally add S3KeySensor
            if self.use_s3_sensor:
                s3_key_sensor = S3KeySensor(
                    task_id=f'wait_for_s3_file_{resource_name}',
                    bucket_key=f'{s3_source_prefix}/*',
                    bucket_name='{{ conn.%s.schema }}' % self.s3_source_conn_id,
                    wildcard_match=True,
                    aws_conn_id=self.s3_source_conn_id,
                    poke_interval=60,
                    timeout=600,
                    dag=self.dag
                )
            else:
                s3_key_sensor = None

            # List S3 objects
            list_s3_objects = S3ListOperator(
                task_id=f'list_s3_objects_{resource_name}',
                bucket='{{ conn.%s.schema }}' % self.s3_source_conn_id,  # Pass bucket as Jinja template to avoid Hook during DAG-init
                prefix=s3_source_prefix,
                delimiter='',
                aws_conn_id=self.s3_source_conn_id,
                dag=self.dag
            )

            ## Transfer from source to dest bucket, and run transform script
            if self.s3_dest_conn_id:
                transfer_s3_to_s3 = LoopS3FileTransformOperator(
                    task_id=f'transfer_s3_to_s3_{resource_name}',
                    source_s3_keys=xcom_pull_template(list_s3_objects.task_id),
                    dest_s3_prefix=datalake_prefix,
                    transform_script=self.transform_script if self.transform_script else None,
                    select_expression=None if self.transform_script else "SELECT * FROM S3Object",
                    source_aws_conn_id=self.s3_source_conn_id,
                    dest_aws_conn_id=self.s3_dest_conn_id,
                    dest_s3_file_extension=self.s3_dest_file_extension,
                    replace=True,
                    dag=self.dag
                )
            else:
                transfer_s3_to_s3 = None

            ## Copy data from dest bucket (data lake stage) to snowflake raw table
            copy_to_raw = S3ToSnowflakeOperator(
                task_id=f'copy_to_raw_{resource_name}',
                snowflake_conn_id=self.snowflake_conn_id,
                database=self.database,
                schema=self.schema,
                table_name=f'{self.data_source}__{resource_name}' if self.prefix_the_source_name else f'{resource_name}',
                custom_metadata_columns={
                    'tenant_code': f"'{self.tenant_code}'",
                    'api_year': f"'{self.api_year}'",
                    'name': f"'{resource_name}'"
                },
                s3_destination_key=datalake_prefix,
                full_refresh=self.full_replace,
                delete_where=f"where tenant_code = '{self.tenant_code}' and api_year = '{self.api_year}'",
                dag=self.dag
            )

            ## Delete data from source bucket
            if self.s3_dest_conn_id and self.do_delete_from_source:
                delete_from_source = PythonOperator(
                    task_id=f'delete_from_source_{resource_name}',
                    python_callable=s3.delete_from_s3,
                    op_kwargs={
                        's3_conn_id': self.s3_source_conn_id,
                        's3_keys_to_delete': xcom_pull_template(list_s3_objects.task_id)
                    },
                    dag=self.dag
                )
            else:
                delete_from_source = None

            # Define task execution order
            ## Default route: Sensor (optional) -> List -> Transfer -> Copy -> Delete from source
            task_order = (
                s3_key_sensor,
                list_s3_objects,
                transfer_s3_to_s3,
                copy_to_raw,
                delete_from_source
            )

            chain(*filter(None, task_order))  # Chain all defined operators into task-order.
