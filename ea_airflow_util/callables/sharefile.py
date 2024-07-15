import logging
import os
import re
import requests

from datetime import datetime
from typing import List

from airflow.exceptions import AirflowException, AirflowSkipException, AirflowFailException

from ea_airflow_util.providers.sharefile.hooks.sharefile import SharefileHook

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

    return date_path


def check_for_new_files(sharefile_conn_id: str, sharefile_path: str, expected_files: int, updated_after: datetime):
    """
    Checks a ShareFile folder for files

    :param sharefile_conn_id: reference to a specific ShareFile connection
    :type sharefile_conn_id: string
    :param sharefile_path: The root directory to transfer, such as '/CORE_Data_System'
    :type sharefile_path: string
    :param expected_files: Number of files expected to be found at the Sharefile path; fails if different
    :type expected_files: int
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
    if len(sf_all_files) != expected_files:
        logging.info(f"{len(sf_all_files)} files found in the Sharefile folder '{sharefile_path}'. Expected {expected_files}.")
        raise AirflowFailException

    # check how many files have been added since the last successful run 
    new_file_count = 0

    for file in sf_all_files:
        if updated_after == 'None' or file['CreationDate'] >= updated_after:
            new_file_count += 1

    if new_file_count == 0:
        logging.info(f"No new files in '{sharefile_path}' since last run.")
        raise AirflowSkipException