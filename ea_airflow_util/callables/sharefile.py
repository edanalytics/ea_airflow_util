import logging
import os
import re
import requests

from typing import List, Optional

from airflow.exceptions import AirflowSkipException
from airflow.exceptions import AirflowException

from ea_airflow_util.providers.sharefile.hooks.sharefile import SharefileHook

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
    ds_nodash: Optional[str] = None,
    ts_nodash: Optional[str] = None,
    delete_remote: bool = False,
    file_pattern: Optional[str] = None
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

    ### Iterate and download the files locally, filtering on a pattern if specified.
    # Set a date path, including optional elements if passed as args.
    date_path = os.path.join(*filter(None, [local_path, ds_nodash, ts_nodash]))

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
            full_local_path = os.path.join(date_path, file_name)
        else:
            full_local_path = os.path.join(date_path, file_path_no_base, file_name)

        # create dir (works if there is a file name or not)
        os.makedirs(os.path.dirname(full_local_path), exist_ok=True)

        # download the file and hash it
        try:
            sf_hook.download_to_disk(item_id=item_id, local_path=full_local_path)

            if delete_remote:
                sf_hook.delete(item_id)

            num_successes += 1

        except Exception as err:
            logging.error(f'Failed to get file with message: {err}')
            continue

    if num_successes == 0:
        raise AirflowException(f"Failed transfer from ShareFile to local: no files transferred successfully!")

    return date_path