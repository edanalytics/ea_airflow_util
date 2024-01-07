import logging

from typing import List

from airflow.exceptions import AirflowSkipException

from ea_airflow_util.providers.sharefile.hooks.sharefile import SharefileHook

def list_sharefile_objects(sharefile_conn_id: str, remote_dir: str) -> List[str]:
    sharefile_hook = SharefileHook(sharefile_conn_id)
    sharefile_dir_id = sharefile_hook.get_path_id(remote_dir)

    sharefile_objects = sharefile_hook.find_files(sharefile_dir_id)
    if not sharefile_objects:
        logging.info(f"No files found in remote directory: `{remote_dir}`")
        raise AirflowSkipException

    return [object['ParentName'] for object in sharefile_objects]
