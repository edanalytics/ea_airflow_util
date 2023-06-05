from tempfile import NamedTemporaryFile
import subprocess
import sys
import os
import pathlib

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoopS3FileTransformOperator(BaseOperator):
    template_fields = ('source_s3_keys', 'dest_s3_prefix', 'transform_script')
    """
    Copies data from a source S3 location to a temporary location on the
    local filesystem. Runs a transformation on this file as specified by
    the transformation script and uploads the output to a destination S3
    location.

    The locations of the source and the destination files in the local
    filesystem is provided as an first and second arguments to the
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


    @apply_defaults
    def __init__(
            self,
            source_s3_keys = [],
            dest_s3_prefix = '',
            dest_s3_file_extension=None,
            transform_script=None,
            select_expression=None,
            source_aws_conn_id='aws_default',
            source_verify=None,
            dest_aws_conn_id='aws_default',
            dest_verify=None,
            replace=False,
            *args, **kwargs):
        super(LoopS3FileTransformOperator, self).__init__(*args, **kwargs)
        self.source_s3_keys = source_s3_keys
        self.dest_s3_prefix = dest_s3_prefix
        self.dest_s3_file_extension = dest_s3_file_extension
        self.source_aws_conn_id = source_aws_conn_id
        self.source_verify = source_verify
        self.dest_aws_conn_id = dest_aws_conn_id
        self.dest_verify = dest_verify
        self.replace = replace
        self.transform_script = transform_script
        self.select_expression = select_expression
        self.output_encoding = sys.getdefaultencoding()

    def execute(self, context):
        if self.transform_script is None and self.select_expression is None:
            raise AirflowException(
                "Either transform_script or select_expression must be specified")

        if not self.source_s3_keys:
            raise AirflowSkipException(
                "No files found in source S3 bucket to transfer"
            )

        source_s3 = S3Hook(aws_conn_id=self.source_aws_conn_id,
                           verify=self.source_verify)
        dest_s3 = S3Hook(aws_conn_id=self.dest_aws_conn_id,
                         verify=self.dest_verify)

        transferred_keys = []

        for source_s3_key in self.source_s3_keys:

            self.log.info("Downloading source S3 file %s", source_s3_key)
            if not source_s3.check_for_key(source_s3_key):
                # raise AirflowException(
                #     "The source key {0} does not exist".format(source_s3_key))
                self.log.info(f"{source_s3_key} does not exist")
                continue
            elif source_s3_key.endswith('/'):
                self.log.info(f"{source_s3_key} is a directory, ignoring transfer")
                continue
            source_s3_key_object = source_s3.get_key(source_s3_key)

            with NamedTemporaryFile("wb") as f_source, NamedTemporaryFile("wb") as f_dest:
                self.log.info(
                    "Dumping S3 file %s contents to local file %s",
                    source_s3_key, f_source.name
                )

                if self.select_expression is not None:
                    content = source_s3.select_key(
                        key=source_s3_key,
                        expression=self.select_expression
                    )
                    f_source.write(content.encode("utf-8"))
                else:
                    source_s3_key_object.download_fileobj(Fileobj=f_source)
                f_source.flush()

                if self.transform_script is not None:
                    process = subprocess.Popen(
                        [self.transform_script, f_source.name, f_dest.name],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        close_fds=True
                    )

                    self.log.info("Output:")
                    for line in iter(process.stdout.readline, b''):
                        self.log.info(line.decode(self.output_encoding).rstrip())

                    process.wait()

                    if process.returncode > 0:
                        raise AirflowException(
                            "Transform script failed: {0}".format(process.returncode)
                        )
                    else:
                        self.log.info(
                            "Transform script successful. Output temporarily located at %s",
                            f_dest.name
                        )


                source_file_extension = pathlib.Path(source_s3_key).suffix
                if not self.dest_s3_file_extension:
                    self.dest_s3_file_extension = source_file_extension
                dest_s3_key = os.path.join(self.dest_s3_prefix,
                                           source_s3_key.replace(source_file_extension,self.dest_s3_file_extension).split('/')[-1]
                                           )

                self.log.info("Uploading transformed file to S3")
                f_dest.flush()
                dest_s3.load_file(
                    filename=f_dest.name,
                    key=dest_s3_key,
                    replace=self.replace
                )
                self.log.info("Upload successful")

                transferred_keys.append(dest_s3_key)
        
        return transferred_keys