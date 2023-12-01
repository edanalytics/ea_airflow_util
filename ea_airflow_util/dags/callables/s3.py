import csv
import gzip
import json
import logging
import os

from typing import List, Optional

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from ea_airflow_util.dags.dag_util import slack_callbacks


def disk_to_s3(
    s3_conn_id: str,
    local_path: str,
    base_dir: str,
    bucket: str,
    delete_local: bool,
    expected_col_names: Optional[List[str]] = None,
    extra_dir_to_local: Optional[str] = None,
    **context
):
    # use hook to make connection to s3
    s3_hook = S3Hook(s3_conn_id)

    # get local path from xcom
    if local_path is None:
        local_path = context.get('templates_dict', {}).get('local_path')

    # extra dir to local will be an additional path added on to local path
    if extra_dir_to_local is not None:
        local_path = os.path.join(local_path, extra_dir_to_local)

    # starting failed count
    failed_count = 0
    n_files = 0

    # loop and find all the files
    for root, _, files in os.walk(local_path):

        # loop over the files in this directory (since there could potentially be multiple?)
        for file in files:

            # if the file is empty, do nothing
            if os.stat(os.path.join(root, file)).st_size == 0:
                continue

            full_local_path = os.path.join(root, file)
            key = full_local_path.replace(base_dir + '/', '')

            # file validation by checking headers
            if expected_col_names:

                if os.path.basename(full_local_path).endswith('.gz'):
                    with gzip.open(full_local_path, 'rt') as csvfile:
                        reader = csv.reader(csvfile)
                        header = next(reader)
                elif os.path.basename(full_local_path).endswith('.jsonl'):
                    with open(full_local_path, 'r') as jsonfile:
                        line = jsonfile.readline()
                        header = list(json.loads(line).keys())
                else:
                    with open(full_local_path, 'r') as csvfile:
                        reader = csv.reader(csvfile)
                        header = next(reader)

                header = [col.lower().replace('\ufeff', '').strip('"') for col in header]
                expected_col_names = [col.lower().strip('"') for col in expected_col_names]

                # we want to allow for different orders (by using set) for json files
                # because this is json, we are allowing extra columns in the files
                # but as long as all of the expected cols are in there, it's allowed
                # TODO: not sure if we want to explicitly set endwith = '.gz' or '.csv' or do not 'jsonl'
                if (
                    (expected_col_names == header and not os.path.basename(full_local_path).endswith('.jsonl'))
                    or (set(expected_col_names) == set(header) and os.path.basename(full_local_path).endswith('.jsonl'))
                    or (all(elem in header for elem in expected_col_names) and os.path.basename(full_local_path).endswith('.jsonl'))
                ):
                    logging.info(f'{full_local_path} has correct column names')
                else:
                    logging.error(f'{full_local_path} has unexpected column configuration')
                    file_type = local_path.split('/')[-1]
                    slack_callbacks.slack_alert_file_format_failure(file_type, full_local_path, expected_col_names, header)
                    # update s3 key
                    ds_nodash = context.get('templates_dict').get('ds_nodash')
                    key = os.path.join('failed_files', ds_nodash, file_type, file)

                    # fail the task if they all fail to upload
                    failed_count += 1

            # use s3 hook to upload from local file
            if os.path.basename(full_local_path).endswith('.gz'):
                try:
                    s3_hook.load_file(
                        full_local_path, bucket, key,
                        ExtraArgs={'ServerSideEncryption': 'AES256', 'ContentEncoding': 'gzip'}
                    )
                except Exception as error:
                    logging.error(error)
                    slack_callbacks.slack_alert_s3_upload_failure(full_local_path, key, error)
                    # if we've already counted this task as 'failing' bc of incorrect format, don't count again
                    if not key.startswith('failed_files'):
                        failed_count += 1
            else:
                try:
                    s3_hook.load_file(full_local_path, key, bucket, replace=True, encrypt=True)
                except Exception as error:
                    logging.error(error)
                    slack_callbacks.slack_alert_s3_upload_failure(full_local_path, key, error)
                    # if we've already counted this task as 'failing' bc of incorrect format, don't count again
                    if not key.startswith('failed_files'):
                        failed_count += 1

            # if delete_local is true, then delete the file
            if delete_local:
                os.remove(full_local_path)

            n_files += 1

    if failed_count == n_files:
        raise AirflowException

    # return the key
    return local_path.replace(base_dir + '/', '')