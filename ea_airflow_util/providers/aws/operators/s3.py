import os
import pathlib
import sys

from typing import Any, List, Optional, Sequence, Union

from airflow.exceptions import AirflowSkipException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.operators.s3 import S3FileTransformOperator, S3CopyObjectOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.decorators import apply_defaults


class LoopS3FileTransformOperator(S3FileTransformOperator):
    """
    This operator extends Airflow's built-in S3FileTransformOperator to iterate over multiple files.
    In addition, the new `dest_s3_file_extension` argument provides greater transparency in output type.
    """

    template_fields = ('source_s3_keys', 'dest_s3_prefix', 'transform_script')

    def __init__(self,
        source_s3_keys: Optional[List[str]] = None,
        dest_s3_prefix: Optional[str] = None,
        dest_s3_file_extension: Optional[str] = None,

        *,
        select_expression: Optional[str] = None,
        transform_script: Optional[str] = None,
        script_args: Optional[Sequence[str]] = None,

        source_aws_conn_id: str = 'aws_default',
        source_verify: Optional[Union[bool, str]] = None,

        dest_aws_conn_id: str = 'aws_default',
        dest_verify: Optional[Union[bool, str]] = None,

        replace: bool = False,

        **kwargs
    ):
        # We need S3FileTransformOperator's execute, but cannot use its init due to differing datatypes.
        BaseOperator.__init__(self, **kwargs)

        self.source_aws_conn_id = source_aws_conn_id
        self.source_s3_keys = source_s3_keys
        self.source_verify = source_verify

        self.dest_aws_conn_id = dest_aws_conn_id
        self.dest_s3_prefix = dest_s3_prefix or ''
        self.dest_s3_file_extension = dest_s3_file_extension
        self.dest_verify = dest_verify

        self.select_expression = select_expression
        self.transform_script = transform_script
        self.script_args = script_args or []

        self.replace = replace
        self.output_encoding = sys.getdefaultencoding()


    def execute(self, context):
        """
        Loop over source and destination keys, using the S3FileTransformOperator's execute for each.

        Technically, monkey-patching class attributes in execute is a coding faux pas, but it allows us to utilize
        S3FileTransformOperator's execute as is without rebuilding it here from scratch.
        """
        # Remove directories from the listing (must be done here to give XComs time to parse.
        self.source_s3_keys = list(filter(lambda key: not key.endswith('/'), self.source_s3_keys))  # Remove directories

        # Skip prematurely if no work to be done.
        if not self.source_s3_keys:
            raise AirflowSkipException(
                "No files found in source S3 bucket to transfer"
            )

        transferred_keys = []

        for source_s3_key in self.source_s3_keys:

            # Monkey-patch the missing `source_s3_key` and `dest_s3_key` arguments used in super's execute.
            self.source_s3_key = source_s3_key

            source_file_extension = pathlib.Path(self.source_s3_key).suffix
            if not self.dest_s3_file_extension:
                self.dest_s3_file_extension = source_file_extension

            self.dest_s3_key = os.path.join(
                self.dest_s3_prefix,
                source_s3_key.replace(source_file_extension, self.dest_s3_file_extension).split('/')[-1]
            )

            super().execute(context)
            transferred_keys.append(self.dest_s3_key)

        return transferred_keys
    

class LoopS3CopyOperator(BaseOperator):
    """
    This operator loops over multiple source S3 keys and copies each file to the destination S3 location.
    """

    template_fields = ('source_s3_keys', 'dest_s3_prefix')

    def __init__(self,
        source_s3_keys: Optional[List[str]] = None,
        dest_s3_prefix: Optional[str] = None,
        source_aws_conn_id: str = 'aws_default',
        source_verify: Optional[Union[bool, str]] = None,
        dest_aws_conn_id: str = 'aws_default',
        dest_verify: Optional[Union[bool, str]] = None,
        replace: bool = False,
        **kwargs
    ):
        super().__init__(**kwargs)

        self.source_aws_conn_id = source_aws_conn_id
        self.source_s3_keys = source_s3_keys
        self.source_verify = source_verify

        self.dest_aws_conn_id = dest_aws_conn_id
        self.dest_s3_prefix = dest_s3_prefix or ''
        self.dest_verify = dest_verify

        self.replace = replace

    def execute(self, context):
        """
        Loop over source and destination keys, using the S3CopyObjectOperator for each.
        """

        # Remove directories from the listing
        self.source_s3_keys = list(filter(lambda key: not key.endswith('/'), self.source_s3_keys))

        if not self.source_s3_keys:
            raise AirflowSkipException("No files found in source S3 bucket to copy")

        copied_keys = []

        for source_s3_key in self.source_s3_keys:
            dest_s3_key = os.path.join(self.dest_s3_prefix, os.path.basename(source_s3_key))

            copy_task = S3CopyObjectOperator(
                task_id=f"copy_s3_{os.path.basename(source_s3_key)}",
                source_bucket_key=source_s3_key,
                dest_bucket_key=dest_s3_key,
                source_bucket_name=None,  # Should be set dynamically in DAG
                dest_bucket_name=None,  # Should be set dynamically in DAG
                aws_conn_id=self.source_aws_conn_id,
                replace=self.replace
            )
            copy_task.execute(context)
            copied_keys.append(dest_s3_key)

        return copied_keys

class S3ToSnowflakeOperator(BaseOperator):
    """
    Copy JSON files saved to S3 to Snowflake raw tables.

    The optional custom_metadata_columns param takes a dictionary in the format {alias: value}
    """
    template_fields = ('s3_destination_key', 's3_destination_dir', 's3_destination_filename',)

    @apply_defaults
    def __init__(self,
        *,
        snowflake_conn_id: str,
        database: str,
        schema: str,
        table_name: str,
        custom_metadata_columns: Optional[dict] = None,

        s3_destination_key: Optional[str] = None,
        s3_destination_dir: Optional[str] = None,
        s3_destination_filename: Optional[str] = None,

        full_refresh: bool = False,
        delete_where: Optional[str] = None,
        xcom_return: Optional[Any] = None,
        **kwargs
    ) -> None:
        super(S3ToSnowflakeOperator, self).__init__(**kwargs)

        self.snowflake_conn_id = snowflake_conn_id
        self.database = database
        self.schema = schema
        self.table_name = table_name
        self.custom_metadata_columns = custom_metadata_columns

        self.s3_destination_key = s3_destination_key
        self.s3_destination_dir = s3_destination_dir
        self.s3_destination_filename = s3_destination_filename

        self.full_refresh = full_refresh
        self.delete_where = delete_where
        self.xcom_return = xcom_return


    def execute(self, context):
        """

        :param context:
        :return:
        """
        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)

        ### Optionally set destination key by concatting separate args for dir and filename
        if not self.s3_destination_key:
            if not (self.s3_destination_dir and self.s3_destination_filename):
                raise ValueError(
                    f"Argument `s3_destination_key` has not been specified, and `s3_destination_dir` or `s3_destination_filename` is missing."
                )
            self.s3_destination_key = os.path.join(self.s3_destination_dir, self.s3_destination_filename)

        ### Extract column name and select statement string from custom metadata dictionary
        if self.custom_metadata_columns:
            column_names_str = ", ".join(self.custom_metadata_columns.keys()) + ","
            metadata_columns = ", ".join(
                f"{value} as {alias}"
                for alias, value in self.custom_metadata_columns.items()
            ) + ","
        else:
            column_names_str = None
            metadata_columns = None

        ### Build the SQL queries to be passed into `Hook.run()`.
        qry_delete = f"""
            DELETE FROM {self.database}.{self.schema}.{self.table_name}
            {self.delete_where}
        """

        # Brackets in regex conflict with string formatting.
        date_regex = "\\\\d{8}"
        ts_regex   = "\\\\d{8}T\\\\d{6}"

        qry_copy_into = f"""
            COPY INTO {self.database}.{self.schema}.{self.table_name}
                (pull_date, pull_timestamp, file_row_number, filename, {column_names_str} v)
            FROM (
                SELECT
                    TO_DATE(REGEXP_SUBSTR(metadata$filename, '{date_regex}'), 'YYYYMMDD') AS pull_date,
                    TO_TIMESTAMP(REGEXP_SUBSTR(metadata$filename, '{ts_regex}'), 'YYYYMMDDTHH24MISS') AS pull_timestamp,
                    metadata$file_row_number AS file_row_number,
                    metadata$filename AS filename,
                    {metadata_columns}
                    t.$1 AS v
                FROM @{self.database}.util.airflow_stage/{self.s3_destination_key}
                (file_format => 'json_default') t
            )
            force = true;
        """

        ### Commit the update queries to Snowflake.
        if self.full_refresh:
            snowflake_hook.run(
                sql=[qry_delete, qry_copy_into],
                autocommit=False
            )
        else:
            snowflake_hook.run(
                sql=qry_copy_into
            )

        return self.xcom_return
