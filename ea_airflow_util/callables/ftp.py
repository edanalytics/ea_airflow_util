import logging
import os

from typing import Optional, Tuple, Union

from ea_airflow_util.callables import slack
from ea_airflow_util.providers.sftp.hooks.sftp import SFTPHook


def download_all(
    ftp_conn_id: str,
    remote_dir: str,
    local_dir: str,
    startswith: Optional[Union[Tuple[str], str]] = (),
    endswith: Optional[Union[Tuple[str], str]] = (),
    **context
):
    """
    Download all files from an FTP to disk, optionally filtering on file extension endings
    """
    # Ensure local directory exists before downloading to it.
    os.makedirs(local_dir, exist_ok=True)

    # `str.startswith()` requires a string or a tuple object; genericize all inputs to tuples.
    if startswith and isinstance(startswith, str):
        startswith = [startswith]
    startswith = tuple(startswith)

    # `str.endswith()` requires a string or a tuple object; genericize all inputs to tuples.
    if endswith and isinstance(endswith, str):
        endswith = [endswith]
    endswith = tuple(endswith)

    # Connect and download all selected files.
    hook = SFTPHook(ftp_conn_id)

    _, _has_extension = os.path.splitext(remote_dir)  # Overload to support files and directories.
    if _has_extension:
        files_to_download = [remote_dir]
    else:
        files_to_download = [
            file for file in hook.list_directory(remote_dir)
            if (not startswith or file.startswith(startswith))
            and (not endswith or file.endswith(endswith))
        ]
    logging.info(f"Found {len(files_to_download)} files to download from remote directory `{remote_dir}`.")

    for file in files_to_download:
        if file == remote_dir:  # File passed as remote_dir
            remote_path = remote_dir
            local_path = os.path.join(local_dir, os.path.basename(file).lower().replace(' ', '_'))
        else:
            remote_path = os.path.join(remote_dir, file)
            local_path  = os.path.join(local_dir, file.lower().replace(' ', '_'))

        try:
            hook.retrieve_large_file(remote_path, local_path)
        except Exception as err:
            logging.error(err)

            if slack_conn_id := context["dag"].user_defined_macros.get("slack_conn_id"):
                slack.slack_alert_download_failure(
                    context=context, http_conn_id=slack_conn_id,
                    remote_path=remote_path, local_path=local_path, error=err
                )

            os.remove(local_path)  # this exception (when sftp connection closes) will create a ghost file
