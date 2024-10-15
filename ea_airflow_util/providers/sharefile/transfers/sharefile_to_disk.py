import os

from airflow.models import BaseOperator
from airflow.exceptions import AirflowSkipException
from airflow.exceptions import AirflowException

from ea_airflow_util.callables import slack
from ea_airflow_util.providers.sharefile.hooks.sharefile import SharefileHook


# TODO: will need to modify this code when we decide on the final folder structure for client folders

class SharefileToDiskOperator(BaseOperator):
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
    :param most_recent_file: If we expect a single file at a given path and want the most recent version of it
    :type most_recent_file: bool

    """

    template_fields = ('local_path',)

    def __init__(self,
        sharefile_conn_id: str,
        sharefile_path: str,
        local_path: str,
        delete_remote: bool = False,
        most_recent_file: bool = False,
        *args, **kwargs
    ):
        super(SharefileToDiskOperator, self).__init__(*args, **kwargs)
        self.sharefile_conn_id = sharefile_conn_id
        self.sharefile_path = sharefile_path
        self.local_path = local_path
        self.delete_remote = delete_remote
        self.most_recent_file = most_recent_file

    def execute(self, **context):
        # use hook to make connection
        sf_hook = SharefileHook(sharefile_conn_id=self.sharefile_conn_id)
        sf_hook.get_conn()

        # get the item id of the remote path, find all files within that path (up to 1000)
        base_path_id = sf_hook.get_path_id(self.sharefile_path)
        remote_files = sf_hook.find_files(base_path_id)

        # check whether we found anything
        if len(remote_files) == 0:
            self.log.info("No files on FTP")
            raise AirflowSkipException

        # extract relevant file details
        files = []
        for res in remote_files:
            file_details = {
                'file_name': res['FileName'],
                'size': res['Size'],
                'parent_id': res['ParentID'],
                'file_path_no_base': res['ParentSemanticPath'].replace(self.sharefile_path, ''),
                'file_path_ftp': res['ParentSemanticPath'],
                'item_id': res['ItemID']
            }


            files.append(file_details)

        if self.most_recent_file:
            # of files found in directory, find the one with the most recent edit timestamp
            max_timestamp = None
            chosen_file = []
            for res in files:
                # seem to be cases where search is out of date and returns items that don't exist
                try:
                    item_info = sf_hook.item_info(res['item_id'])
                except:
                    # if the item fails to fetch item info, it probably doesn't exist, so can't be most recent
                    continue
                # grab last modified, compare to current max known
                item_last_modified = item_info['ProgenyEditDate']
                if max_timestamp is None or item_last_modified > max_timestamp:
                    max_timestamp = item_last_modified
                    chosen_file = [res]
            # overwrite files list with singular chosen item
            files = chosen_file


        # for all files, move to local
        num_successes = 0

        for file in files:

            remote_file = os.path.join(file['file_path_ftp'], file['file_name'])
            self.log.info("Attempting to get file " + remote_file)

            # lower filename and replace spaces with underscores
            file['file_name'] = file['file_name'].lower().replace(' ', '_')

            # check to see if there is other metadata needed in local path and if not, add filename to local path
            if file['parent_id'] == base_path_id:
                full_local_path = os.path.join(self.local_path, file['file_name'])
            else:
                full_local_path = os.path.join(self.local_path, file['file_path_no_base'], file['file_name'])

            # create dir (works if there is a file name or not)
            os.makedirs(os.path.dirname(full_local_path), exist_ok=True)

            # download the file
            try:
                sf_hook.download_to_disk(item_id=file['item_id'], local_path=full_local_path)

                if self.delete_remote:
                    sf_hook.delete(file['item_id'])

                num_successes += 1

            except Exception as err:
                self.log.error(f'Failed to get file with message: {err}')

                if slack_conn_id := context["dag"].user_defined_macros.get("slack_conn_id"):
                    slack.slack_alert_download_failure(
                        context=context, http_conn_id=slack_conn_id,
                        remote_path=remote_file, local_path=full_local_path, error=err
                    )

                continue

        if num_successes == 0:
            raise AirflowException("Failed transfer from ShareFile to local: no files transferred successfully!")

        return self.local_path

