import os
import pathlib
import sys

from typing import List, Optional, Sequence, Union

from airflow.exceptions import AirflowSkipException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.operators.s3 import S3FileTransformOperator


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
