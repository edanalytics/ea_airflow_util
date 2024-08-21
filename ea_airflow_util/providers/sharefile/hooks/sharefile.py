import io
from pathlib import Path

import requests

from airflow.hooks.base import BaseHook


# Note: consider making each file use its own session with context handler
# may be necessary if we experience timeouts or token expiration

class SharefileHook(BaseHook):
    """
    Interact with ShareFile servers.
    Note that the connection in Airflow must be configured in an unusual way:
    - Host should be the API endpoint
    - Schema should be the authentication URL
    - Login/Password are filled out as normal
    - Extra should be a dictionary structured as follows:
        {"grant_type": "password", "client_id": client_id, "client_secret": client_secret}
    :param sharefile_conn_id: connection that has the base API url i.e https://www.google.com/
        and optional authentication credentials. Default headers can also be specified in
        the Extra field in json format.
    :type http_conn_id: str
    """

    def __init__(self, sharefile_conn_id='sharefile_default'):
        self.sharefile_conn_id = sharefile_conn_id
        self.base_url = None
        self.session = None

    def get_conn(self, headers={'Content-Type':'application/x-www-form-urlencoded'}):
        # pull connection details from airflow
        conn = self.get_connection(self.sharefile_conn_id)
        # extract extras
        params = conn.extra_dejson

        # we store the auth url in the schema since it is different from the api url
        auth_url = conn.schema

        # append username and pw to the other auth parameters
        params['username'] = conn.login
        params['password'] = conn.password

        # request authorization, extract token
        response = requests.post(auth_url, data=params, headers=headers)
        if response.status_code != 200:
            self.log.error('Failed to authenticate with status {}'.format(response.status_code))
            response.raise_for_status()
        token = response.json()['access_token']

        # create our request header and session
        req_header = {'Authorization': 'Bearer {}'.format(token)}
        session = requests.Session()
        session.headers.update(req_header)

        # put the api base url into the hook
        self.base_url = conn.host

        # put the session in the hook for use by other functions, and return it for direct use
        self.session = session
        return session

    def download(self, item_id, local_path):
        # establish a session if we don't already have one
        if not self.session:
            self.get_conn()

        dl_path = '/Items({item_id})/Download'
        dl_url = self.base_url + dl_path

        dl_response = self.session.get(dl_url.format(item_id=item_id))

        if dl_response.status_code == 200:
            with open(local_path, 'wb') as f:
                f.write(dl_response.content)
        else:
            self.log.error('Item request failed with status code: {}'.format(dl_response.status_code))
            dl_response.raise_for_status()

    def upload_file(self, folder_id, local_file):
        # establish a session if we don't already have one
        if not self.session:
            self.get_conn()

        # referencing sharefile's upload logic here
        # https://api.sharefile.com/samples/python
        upload_config_uri = f"{self.base_url}/Items({folder_id})/Upload"
        upload_config_resp = self.session.get(upload_config_uri)
        upload_config = upload_config_resp.json()
    
        upload_uri = ""
        try:
            upload_uri = upload_config["ChunkUri"]
        except KeyError:
            # most likely an invalid or nonexistent folder ID
            raise AirflowException(f"Failed to get upload link for {local_file}; Reason: {upload_config['reason']}; Message: {upload_config['message']['value']}")

        files = {Path(local_file).name: open(local_file, 'rb')}
        resp = self.session.post(upload_uri, files=files)
        resp.raise_for_status()


    def folder_id_from_path(self, folder_path):
        """Given a complete Sharefile folder path, returns the ID of the leaf folder. If the path is not found, returns `None`"""
        # Sharefile's documentation doesn't explicitly describe how "allshared" is supposed to be used,
        #    but it appears to be a stand-in for "root." By searching for all folders underneath "allshared"
        #    we should get a list of every folder in the hierarchy (including nested ones)
        # ref: https://api.sharefile.com/samples/python
        folders = self.find_folders("allshared")

        result = None
        for folder in folders:
            # special handling for folders just under th root
            test_path = f"{folder['ParentSemanticPath']}/{folder['DisplayName']}" if folder['ParentSemanticPath'] != '/' else f"/{folder['DisplayName']}"
            if test_path == folder_path:
                result = folder["ItemID"]
                break

        return result

    def delete(self, item_id):
        # establish a session if we don't already have one
        if not self.session:
            self.get_conn()

        item_path = '/Items({})'.format(item_id)

        response = self.session.delete(self.base_url + item_path)
        if response.status_code != 204:
            self.log.error('Delete failed with response code {}'.format(response.status_code))
            response.raise_for_status()

    def get_path_id(self, path):
        if not self.session:
            self.get_conn()

        path_string = '/Items/ByPath?path={}'.format(path)
        response = self.session.get(self.base_url + path_string)

        if response.status_code == 200:
            return response.json()['Id']
        else:
            self.log.error('Failed to get id for path {}'.format(path))
            response.raise_for_status()

    def item_info(self, id):
        if not self.session:
            self.get_conn()

        response = self.session.get(self.base_url + f'/Items({id})')

        # do we need to check response.json()['TimedOut']?
        if response.status_code != 200:
            self.log.error(f'Getting item info failed for id {id}')
            response.raise_for_status()

        results = response.json()

        return results

    def find_folders(self, folder_id):
        return self._find_items(folder_id, "Folder")

    def find_files(self, folder_id):
        return self._find_items(folder_id, "File")

    ## this method started returning inconsistent results
    # specifically: the parentSemanticPath would sometimes be IDs rather than names
    # hence we switched to the below simplesearch method
    def _find_items(self, folder_id, item_type):
        if not self.session:
            self.get_conn()
        qry = {
            "Query": {
                "ItemType": item_type,
                "ParentID": folder_id
            },
            "Paging": {
                "Count": 1000,
                "Skip": 0
            },
            "TimeoutInSeconds": 15
        }

        response = self.session.post(self.base_url + '/Items/AdvancedSimpleSearch', json=qry)

        # do we need to check response.json()['TimedOut']?
        if response.status_code != 200:
            self.log.error('Search failed')
            response.raise_for_status()

        results = response.json()['Results']

        return results

    def get_access_controls(self, item_id):
        # establish a session if we don't already have one
        if not self.session:
            self.get_conn()

        # creating path of item to get access controls info
        # base url = 'https://edanalytics.sf-api.com/sf/v3'
        ac_path = f'/Items({item_id})/AccessControls'
        ac_url = self.base_url + ac_path

        # pull out response from access controls
        response = self.session.get(ac_url)

        # if not error, return value (not sure if we want to subset to this)
        if response.status_code == 200:
            return response.json()['value']
        else:
            self.log.error('Access controls request failed with status code: {}'.format(response.status_code))
            response.raise_for_status()

    def get_user(self, user_id):
        # establish a session if we don't already have one
        if not self.session:
            self.get_conn()

        # creating path of user id to get user info
        # base url = 'https://edanalytics.sf-api.com/sf/v3'
        user_path =  f'/Users({user_id})'
        user_url = self.base_url + user_path

        # pull out response from users
        response = self.session.get(user_url)

        # if not error, return value (not sure if we want to subset to this)
        if response.status_code == 200:
            return response.json()
        else:
            self.log.error('Users request failed with status code: {}'.format(response.status_code))
            response.raise_for_status()

    def get_children(self, item_id):
        # establish a session if we don't already have one
        if not self.session:
            self.get_conn()

        # creating path of item to get children info
        # base url = 'https://edanalytics.sf-api.com/sf/v3'
        child_path = f'/Items({item_id})/Children?includeDeleted=false'
        child_url = self.base_url + child_path

        # pull out response from children
        response = self.session.get(child_url)

        # if not error, return value (not sure if we want to subset to this)
        if response.status_code == 200:
            return response.json()['value']
        else:
            self.log.error('Get children request failed with status code: {}'.format(response.status_code))
            response.raise_for_status()

    def file_to_memory(self, item_id):
        """
        load the file into memory

        :param path: Path to file
        :return: An io.BytesIO object containing file.
        """

        # establish a session if we don't already have one
        if not self.session:
            self.get_conn()

        file_path = '/Items({item_id})/Download'
        file_url = self.base_url + file_path

        dl_response = self.session.get(file_url.format(item_id=item_id))

        if dl_response.status_code == 200:
            flo = io.BytesIO(dl_response.content)
            # with io.BytesIO() as flo:
            #     flo.write(dl_response.content)
        else:
            self.log.error('Item request failed with status code: {}'.format(dl_response.status_code))
            dl_response.raise_for_status()

        return flo

    def download_to_disk(self, item_id, local_path):
        # establish a session if we don't already have one
        if not self.session:
            self.get_conn()
        dl_path = f'/Items({item_id})/Download'
        dl_url = self.base_url + dl_path
        # could we return this as a file object and stream straight to S3?
        with self.session.get(dl_url, stream=True) as r:
            r.raise_for_status()
            with open(local_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)