import logging
import os
import pathlib
import pytz
import re
import requests
import uuid

from datetime import datetime
from typing import List, Optional

from airflow.exceptions import AirflowException, AirflowSkipException, AirflowFailException

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


def sharefile_to_disk(
    sharefile_conn_id: str,
    sharefile_path: str,
    local_path: str,
    ds_nodash: Optional[str] = None,  # Deprecated
    ts_nodash: Optional[str] = None,  # Deprecated
    delete_remote: bool = False,
    recursive: bool = True,
    file_pattern: Optional[str] = None,
    **kwargs
):
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

    ### Connect to ShareFile and determine whether files are present.
    # use hook to make connection
    sf_hook = SharefileHook(sharefile_conn_id)
    sf_hook.get_conn()

    # get the item id of the remote path, find all files within that path
    try:
        base_path_id = sf_hook.get_path_id(sharefile_path)

        # The default approach finds files in all subdirectories recursively.
        if recursive:
            remote_files = sf_hook.find_files(base_path_id)
        
        # `get_children` returns only top-level items, but with a different payload schema.
        else:
            remote_children = sf_hook.get_children(base_path_id)
            remote_files = []

            for res in remote_children:

                # Folders are returned alongside items and must be filtered.
                if res['odata.type'].endswith('Folder'):
                    continue

                res['ParentID'] = base_path_id
                res['ParentSemanticPath'] = sharefile_path
                res['ItemID'] = res['Id']
                remote_files.append(res)

    except requests.exceptions.HTTPError as err:
        raise AirflowSkipException(
            f"{err.response.status_code} {err.response.text}: {sharefile_path}"
        )

    # check whether we found anything
    if len(remote_files) == 0:
        logging.info("No files on FTP")
        raise AirflowSkipException

    ### Iterate and download the files locally, filtering on a pattern if specified.
    # for all files, move to local
    num_successes = 0

    for res in remote_files:

        file_name = res['FileName']
        parent_id = res['ParentID']
        file_path = res['ParentSemanticPath']
        file_path_no_base = res['ParentSemanticPath'].replace(sharefile_path, '')
        item_id = res['ItemID']
        # size = res['Size']  # Not used for anything downstream
        # hash = res['MD5']

        # if a file pattern was specified, skip files which do not match
        remote_file = os.path.join(file_path, file_name)
        if file_pattern and not re.search(file_pattern, file_name):
            logging.info("File does not match pattern, skipping file: " + remote_file)
            continue

        logging.info("Attempting to get file: " + remote_file)

        # check to see if there is other metadata needed in local path and if not, add filename to local path
        file_name = file_name.lower().replace(' ', '_')  # lower filename and replace spaces with underscores

        if parent_id == base_path_id:
            full_local_path = os.path.join(local_path, file_name)
        else:
            full_local_path = os.path.join(local_path, file_path_no_base, file_name)

        # create dir (works if there is a file name or not)
        os.makedirs(os.path.dirname(full_local_path), exist_ok=True)

        # download the file and hash it
        try:
            sf_hook.download_to_disk(item_id=item_id, local_path=full_local_path)

            if delete_remote:
                logging.info(f'delete_remote set to True; removing {remote_file} from ShareFile...')
                sf_hook.delete(item_id)

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

    sf_folder_id = sf_hook.get_path_id(sf_folder_path)
    if sf_folder_id is None:
        raise AirflowException(f"failed to find Sharefile folder {sf_folder_path}")

    local_path = pathlib.Path(local_path)
    if local_path.is_dir():
        # upload all files in directory
        for filepath in local_path.iterdir():
            sf_hook.upload_file(sf_folder_id, filepath)
    else:
        sf_hook.upload_file(sf_folder_id, local_path)


def s3_to_sharefile(s3_conn_id: str, s3_key: str, sf_conn_id: str, sf_folder_path: str, dest_filename: str = None):
    """Copy a single file from S3 to Sharefile"""

    s3_hook = S3Hook(s3_conn_id)
    s3_creds = s3_hook.get_connection(s3_hook.aws_conn_id)
    s3_bucket = s3_creds.schema

    downloaded_file = s3_hook.download_file(s3_key, s3_bucket, preserve_file_name=True)

    # If a destination filename is provided, rename the downloaded file
    if dest_filename:
        # Get the directory of the downloaded file
        download_dir = os.path.dirname(downloaded_file)
        # Define the new path with the new filename
        dest_file_path = os.path.join(download_dir, dest_filename)
        # Rename the downloaded file
        os.rename(downloaded_file, dest_file_path)
        # Update the downloaded_file variable to the new file path
        downloaded_file = dest_file_path

    disk_to_sharefile(sf_conn_id, sf_folder_path, downloaded_file)

    # claen up the disk
    os.remove(downloaded_file)

def check_for_new_files(sharefile_conn_id: str, sharefile_path: str, num_expected_files: Optional[int] = None, updated_after: Optional[datetime] = None):
    """
    Checks a ShareFile folder for files

    :param sharefile_conn_id: reference to a specific ShareFile connection
    :type sharefile_conn_id: string
    :param sharefile_path: The root directory to transfer, such as '/CORE_Data_System'
    :type sharefile_path: string
    :param num_expected_files: Number of files expected to be found at the Sharefile path; fails if different
    :type num_expected_files: int
    :param updated_after: Checks whether any files have been added since this date; raises a skip exception if not
    :type updated_after: datetime
    """

    # use hook to make connection
    sf_hook = SharefileHook(sharefile_conn_id)
    sf_hook.get_conn()

    # get the item id of the remote path, find all files within that path (up to 1000)
    try: 
        path_id = sf_hook.get_path_id(sharefile_path)
        sf_all_files = sf_hook.find_files(path_id)
    except: 
        logging.info(f"Folder not found: {sharefile_path}")
        raise AirflowSkipException

    # skip if no files found
    if len(sf_all_files) == 0:
        logging.info(f"No files found in '{sharefile_path}'.")
        raise AirflowSkipException

    # fail if other than expected number of files is found
    if num_expected_files is not None and len(sf_all_files) != num_expected_files:
        logging.info(f"{len(sf_all_files)} files found in the Sharefile folder '{sharefile_path}'. Expected {num_expected_files}.")
        raise AirflowFailException

    # check how many files have been added since the last successful run 
    new_file_count = 0

    for file in sf_all_files:
        file_updated_time = datetime.strptime(file['CreationDate'], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=pytz.UTC)
        # updated_after will be a None string if airflow cannot find a previous successful dag run
        if updated_after is None or updated_after == 'None' or file_updated_time >= updated_after:
            new_file_count += 1

    if new_file_count == 0:
        logging.info(f"No new files in '{sharefile_path}' since last run.")
        raise AirflowSkipException


def sharefile_copy_file(sharefile_conn_id: str, sharefile_path: str, sharefile_dest_dir: str, delete_source: bool = False):
    """
    Copy a single filepath to a directory on ShareFile.
    """
    sf_hook = SharefileHook(sharefile_conn_id)

    # Find internal IDs for source and destination paths.
    if not (sharefile_source_id := sf_hook.get_path_id(sharefile_path)):
        raise AirflowException(f"Failed to find Sharefile source path `{sharefile_path}`!")
    
    if not (sharefile_dest_id := sf_hook.get_path_id(sharefile_dest_dir)):
        raise AirflowException(f"Failed to find Sharefile destination directory `{sharefile_dest_dir}`!")
    
    logging.info(f"Copying file `{sharefile_path}` to directory `{sharefile_dest_dir}`...")
    sf_hook.copy_file(sharefile_source_id, sharefile_dest_id)

    # Optionally delete the file in its original locations (i.e., a MOVE instead of a COPY).
    if delete_source:
        logging.info(f"Deleting file `{sharefile_path}`...")
        sf_hook.delete_file(sharefile_source_id)
    