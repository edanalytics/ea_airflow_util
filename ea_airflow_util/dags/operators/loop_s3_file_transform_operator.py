import logging
import os
import pathlib
import subprocess
import sys

from tempfile import NamedTemporaryFile
from typing import List, Optional, Union

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoopS3FileTransformOperator(BaseOperator):
    """
    Copies data from a source S3 location to a temporary location on the
    local filesystem. Runs a transformation on this file as specified by
    the transformation script and uploads the output to a destination S3
    location.

    The locations of the source and the destination files in the local
    filesystem is provided as first and second arguments to the
    transformation script. The transformation script is expected to read the
    data from source, transform it and write the output to the local
    destination file. The operator then takes over control and uploads the
    local destination file to S3.

    S3 Select is also available to filter the source contents. Users can
    omit the transformation script if S3 Select expression is specified.

    :param source_s3_keys: The list of keys to be retrieved from S3. (templated)
    :type source_s3_key: list
    :param source_aws_conn_id: source s3 connection
    :type source_aws_conn_id: str
    :param source_verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
             (unless use_ssl is False), but SSL certificates will not be
             verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
             You can specify this argument if you want to use a different
             CA cert bundle than the one used by botocore.

        This is also applicable to ``dest_verify``.
    :type source_verify: bool or str
    :param dest_aws_conn_id: destination s3 connection
    :type dest_aws_conn_id: str
    :param replace: Replace dest S3 key if it already exists
    :type replace: bool
    :param transform_script: location of the executable transformation script
    :type transform_script: str
    :param select_expression: S3 Select expression
    :type select_expression: str
    """
    template_fields = ('source_s3_keys', 'dest_s3_prefix', 'transform_script')

    @apply_defaults
    def __init__(self,
        source_s3_keys: Optional[List[str]] = None,
        dest_s3_prefix: Optional[str] = None,
        dest_s3_file_extension: Optional[str] = None,

        *,
        transform_script: Optional[str] = None,
        select_expression: Optional[str] = None,

        source_aws_conn_id: str = 'aws_default',
        source_verify: Optional[Union[bool, str]] = None,

        dest_aws_conn_id: str = 'aws_default',
        dest_verify: Optional[Union[bool, str]] = None,

        replace: bool = False,

        **kwargs
    ):
        super(LoopS3FileTransformOperator, self).__init__(**kwargs)

        self.source_aws_conn_id = source_aws_conn_id
        self.source_s3_keys = source_s3_keys
        self.source_verify = source_verify

        self.dest_aws_conn_id = dest_aws_conn_id
        self.dest_s3_prefix = dest_s3_prefix or ''
        self.dest_s3_file_extension = dest_s3_file_extension
        self.dest_verify = dest_verify

        self.transform_script = transform_script
        self.select_expression = select_expression

        self.replace = replace
        self.output_encoding = sys.getdefaultencoding()


    def execute(self, context):

        ###
        if not self.transform_script and not self.select_expression:
            raise AirflowException(
                "Either `transform_script` or `select_expression` must be specified"
            )

        if not self.source_s3_keys:
            raise AirflowSkipException(
                "No files found in source S3 bucket to transfer"
            )

        ###
        source_s3 = S3Hook(
            aws_conn_id=self.source_aws_conn_id,
            verify=self.source_verify
        )
        dest_s3 = S3Hook(
            aws_conn_id=self.dest_aws_conn_id,
            verify=self.dest_verify
        )

        ###
        transferred_keys = []

        for source_s3_key in self.source_s3_keys:
            logging.info(f"Downloading source S3 file: {source_s3_key}")

            #
            if not source_s3.check_for_key(source_s3_key):
                logging.warning(f"{source_s3_key} does not exist")
                continue
            elif source_s3_key.endswith('/'):
                self.log.info(f"{source_s3_key} is a directory, ignoring transfer")
                continue
            source_s3_key_object = source_s3.get_key(source_s3_key)

            #
            with NamedTemporaryFile("wb") as f_source, NamedTemporaryFile("wb") as f_dest:
                logging.info(
                    f"Dumping S3 file {source_s3_key} contents to local file {f_source.name}"
                )

                if self.select_expression:
                    content = source_s3.select_key(
                        key=source_s3_key,
                        expression=self.select_expression
                    )
                    f_source.write(content.encode("utf-8"))
                else:
                    source_s3_key_object.download_fileobj(Fileobj=f_source)

                f_source.flush()  #

                ###
                if self.transform_script:
                    process = subprocess.Popen(
                        [self.transform_script, f_source.name, f_dest.name],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        close_fds=True
                    )

                    logging.info("Output:")
                    for line in iter(process.stdout.readline, b''):
                        logging.info(line.decode(self.output_encoding).rstrip())

                    process.wait()  #

                    if process.returncode > 0:
                        raise AirflowException(
                            f"Transform script failed: {process.returncode}"
                        )
                    else:
                        logging.info(
                            f"Transform script successful. Output temporarily located at {f_dest.name}"
                        )

                ###
                logging.info("Uploading transformed file to S3")

                source_file_extension = pathlib.Path(source_s3_key).suffix

                if not self.dest_s3_file_extension:
                    self.dest_s3_file_extension = source_file_extension

                dest_s3_key = os.path.join(
                    self.dest_s3_prefix,
                    source_s3_key.replace(source_file_extension, self.dest_s3_file_extension).split('/')[-1]
                )
                f_dest.flush()  #

                dest_s3.load_file(
                    filename=f_dest.name,
                    key=dest_s3_key,
                    replace=self.replace
                )

                logging.info("Upload successful")
                transferred_keys.append(dest_s3_key)
        
        return transferred_keys
