import logging
import os
import pathlib
import re
import requests
import uuid

from typing import List

from airflow.exceptions import AirflowSkipException
from airflow.exceptions import AirflowException

from ea_airflow_util.providers.sharefile.hooks.sharefile import SharefileHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def list_sharefile_objects(sharefile_conn_id: str, remote_dir: str) -> List[str]:
    sharefile_hook = SharefileHook(sharefile_conn_id)
    sharefile_dir_id = sharefile_hook.get_path_id(remote_dir)

    sharefile_objects = sharefile_hook.find_files(sharefile_dir_id)
    if not sharefile_objects:
        logging.info(f"No files found in remote directory: `{remote_dir}`")
        raise AirflowSkipException

    return [object['ParentName'] for object in sharefile_objects]


def sharefile_to_disk(sharefile_conn_id, sharefile_path, local_path, ds_nodash, ts_nodash, delete_remote=False, file_pattern=None):
    """
    Transfers all files from a ShareFile folder to a local date-stamped directory,
    optionally deleting the remote copy.

    :param sharefile_conn_id: reference to a specific ShareFile connection
    :type sharefile_conn_id: string
    :param sharefile_path: The root directory to transfer, such as '/CORE_Data_System'
    :type sharefile_path: string
    :param local_path: local path to stream sharefile files to
    :type local_path: string
    :param delete_remote: Optionally delete original file on ShareFile
    :type delete_remote: bool
    """

    # use hook to make connection
    sf_hook = SharefileHook(sharefile_conn_id)
    sf_hook.get_conn()

    # get the item id of the remote path, find all files within that path (up to 1000)
    try:
        base_path_id = sf_hook.get_path_id(sharefile_path)
        remote_files = sf_hook.find_files(base_path_id)
    except requests.exceptions.HTTPError as err:
        raise AirflowSkipException(
            f"{err.response.status_code} {err.response.text}: {sharefile_path}"
        )

    # check whether we found anything
    if len(remote_files) == 0:
        logging.info("No files on FTP")
        raise AirflowSkipException

    # extract relevant file details
    files = []
    for res in remote_files:
        file_details = {'file_name': res['FileName'],
                        'size': res['Size'],
                        'hash': res['MD5'],
                        'parent_id': res['ParentID'],
                        'file_path': res['ParentSemanticPath'],
                        'file_path_no_base': res['ParentSemanticPath'].replace(sharefile_path, ''),
                        'item_id': res['ItemID']}

        files.append(file_details)

    # for all files, move to local
    date_path = os.path.join(local_path, str(ds_nodash), str(ts_nodash))
    num_successes = 0

    for file in files:

        # if a file pattern was specified, skip files which do not match
        remote_file = os.path.join(file['file_path'], file['file_name'])
        if file_pattern is not None:
            re_pattern = re.compile(file_pattern)
            if not re.search(re_pattern, file['file_name']):
                logging.info("File does not match pattern, skipping file: " + remote_file)
                continue

        logging.info("Attempting to get file: " + remote_file)

        # lower filename and replace spaces with underscores
        file['file_name'] = file['file_name'].lower().replace(' ', '_')

        # check to see if there is other metadata needed in local path and if not, add filename to local path
        if file['parent_id'] == base_path_id:
            full_local_path = os.path.join(date_path, file['file_name'])
        else:
            full_local_path = os.path.join(date_path, file['file_path_no_base'], file['file_name'])

        # create dir (works if there is a file name or not)
        os.makedirs(os.path.dirname(full_local_path), exist_ok=True)

        # download the file and hash it
        try:
            sf_hook.download_to_disk(item_id=file['item_id'], local_path=full_local_path)

            if delete_remote:
                sf_hook.delete(file['item_id'])

            num_successes += 1

        except Exception as err:
            logging.error(f'Failed to get file with message: {err}')
            continue

    if num_successes == 0:
        raise AirflowException(f"Failed transfer from ShareFile to local: no files transferred successfully!")

    return local_path


def disk_to_sharefile(sf_conn_id: str, sf_folder_path: str, local_path: str):
    """Post a file or the contents of a directory to the specified Sharefile folder"""
    sf_hook = SharefileHook(sf_conn_id )

    sf_folder_id = sf_hook.folder_id_from_path(sf_folder_path)
    if sf_folder_id is None:
        raise AirflowException(f"failed to find Sharefile folder {sf_folder_path}")

    local_path = pathlib.Path(local_path)
    if local_path.is_dir():
        # upload all files in directory
        for filepath in local_path.iterdir():
            sf_hook.upload_file(sf_folder_id, filepath)
    else:
        sf_hook.upload_file(sf_folder_id, local_path)


def s3_to_sharefile(s3_conn_id: str, s3_key: str, sf_conn_id: str, sf_folder_path: str):
    """Copy a single file from S3 to Sharefile"""

    s3_hook = S3Hook(s3_conn_id)
    s3_creds = s3_hook.get_connection(s3_hook.aws_conn_id)
    s3_bucket = s3_creds.schema

    downloaded_file = s3_hook.download_file(s3_key, s3_bucket, preserve_file_name=True)

    disk_to_sharefile(sf_conn_id, sf_folder_path, downloaded_file)

    # claen up the disk
    os.remove(downloaded_file)