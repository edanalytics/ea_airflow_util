#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import stat
import pysftp
import datetime
import os
import io
import glob
import fnmatch
import itertools
import paramiko
from collections import defaultdict
from airflow.providers.ssh.hooks.ssh import SSHHook


class SFTPHook(SSHHook):
    """
    This hook is inherited from SSH hook. Please refer to SSH hook for the input
    arguments.

    Interact with SFTP. Aims to be interchangeable with FTPHook.

    :Pitfalls::

        - In contrast with FTPHook describe_directory only returns size, type and
          modify. It doesn't return unix.owner, unix.mode, perm, unix.group and
          unique.
        - retrieve_file and store_file only take a local full path and not a
           buffer.
        - If no mode is passed to create_directory it will be created with 777
          permissions.

    Errors that may occur throughout but should be handled downstream.
    """

    def __init__(self, ftp_conn_id='sftp_default', *args, **kwargs):
        kwargs['ssh_conn_id'] = ftp_conn_id
        super(SFTPHook, self).__init__(*args, **kwargs)

        self.conn = None
        self.private_key_pass = None

        # Fail for unverified hosts, unless this is explicitly allowed
        self.no_host_key_check = False

        if self.ssh_conn_id is not None:
            conn = self.get_connection(self.ssh_conn_id)
            if conn.extra is not None:
                extra_options = conn.extra_dejson
                if 'private_key_pass' in extra_options:
                    self.private_key_pass = extra_options.get('private_key_pass', None)

                # For backward compatibility
                # TODO: remove in Airflow 2.1
                import warnings
                if 'ignore_hostkey_verification' in extra_options:
                    warnings.warn(
                        'Extra option `ignore_hostkey_verification` is deprecated.'
                        'Please use `no_host_key_check` instead.'
                        'This option will be removed in Airflow 2.1',
                        DeprecationWarning,
                        stacklevel=2,
                    )
                    self.no_host_key_check = str(
                        extra_options['ignore_hostkey_verification']
                    ).lower() == 'true'

                if 'no_host_key_check' in extra_options:
                    self.no_host_key_check = str(
                        extra_options['no_host_key_check']).lower() == 'true'

                if 'private_key' in extra_options:
                    warnings.warn(
                        'Extra option `private_key` is deprecated.'
                        'Please use `key_file` instead.'
                        'This option will be removed in Airflow 2.1',
                        DeprecationWarning,
                        stacklevel=2,
                    )
                    self.key_file = extra_options.get('private_key')

    def get_conn(self):
        """
        Returns an SFTP connection object
        """
        if self.conn is None:
            cnopts = pysftp.CnOpts()
            if self.no_host_key_check:
                cnopts.hostkeys = None
            cnopts.compression = self.compress
            conn_params = {
                'host': self.remote_host,
                'port': self.port,
                'username': self.username,
                'cnopts': cnopts
            }
            if self.password and self.password.strip():
                conn_params['password'] = self.password
            if self.key_file:
                conn_params['private_key'] = self.key_file
            if self.private_key_pass:
                conn_params['private_key_pass'] = self.private_key_pass

            self.conn = pysftp.Connection(**conn_params)
        return self.conn


    def close_conn(self):
        """
        Closes the connection. An error will occur if the
        connection wasnt ever opened.
        """
        conn = self.conn
        conn.close()
        self.conn = None


    def describe_directory(self, path):
        """
        Returns a dictionary of {filename: {attributes}} for all files
        on the remote system (where the MLSD command is supported).

        :param path: full path to the remote directory
        :type path: str
        """
        conn = self.get_conn()
        flist = conn.listdir_attr(path)
        files = {}
        for f in flist:
            modify = datetime.datetime.fromtimestamp(
                f.st_mtime).strftime('%Y%m%d%H%M%S')
            files[f.filename] = {
                'size': f.st_size,
                'type': 'dir' if stat.S_ISDIR(f.st_mode) else 'file',
                'modify': modify}
        return files


    def list_directory(self, path):
        """
        Returns a list of files on the remote system.
        This method supports wildcards. If multiple folders match the wildcard, files in each are listed.

        :param path: full path to the remote directory to list
        :type path: str
        """
        conn = self.get_conn()
        
        # If the path has wildcards, a different approach is required.
        if not glob.has_magic(path):
            return conn.listdir(path)

        # Split the folderpath into parts, and iterate subfolders with wildcards as needed.
        # e.g., "/exports/sc-*/Current_Year" -> expand "/exports/sc-*", then list "CurrentYear" in each subfolder
        dynamic_paths = ["/"]

        for path_part in path.split("/"):
            if glob.has_magic(path_part):
                wildcard_matches = []

                for incremental_path in dynamic_paths:

                    wildcard_path = os.path.join(incremental_path, path_part)

                    # List the subfolders at the level of the wildcard.
                    for subdir in conn.listdir(incremental_path):
                        potential_match = os.path.join(incremental_path, subdir)

                        # Filter to only those that match the wildcard.
                        if fnmatch.fnmatch(potential_match, wildcard_path):
                            wildcard_matches.append(potential_match)

                dynamic_paths = wildcard_matches 
            
            else:
                dynamic_paths = [
                    os.path.join(incremental_path, path_part)
                    for incremental_path in dynamic_paths
                ]

        return list(itertools.chain(*map(conn.listdir, dynamic_paths)))


    def create_directory(self, path, mode=777):
        """
        Creates a directory on the remote system.

        :param path: full path to the remote directory to create
        :type path: str
        :param mode: int representation of octal mode for directory
        """
        conn = self.get_conn()
        conn.mkdir(path, mode)


    def delete_directory(self, path):
        """
        Deletes a directory on the remote system.

        :param path: full path to the remote directory to delete
        :type path: str
        """
        conn = self.get_conn()
        conn.rmdir(path)


    def retrieve_file(self, remote_full_path, local_full_path):
        """
        Transfers the remote file to a local location.
        If local_full_path is a string path, the file will be put
        at that location

        :param remote_full_path: full path to the remote file
        :type remote_full_path: str
        :param local_full_path: full path to the local file
        :type local_full_path: str
        """
        conn = self.get_conn()
        self.log.info('Retrieving file from FTP: %s', remote_full_path)
        conn.get(remote_full_path, local_full_path)
        self.log.info('Finished retrieving file from FTP: %s', remote_full_path)


    def store_file(self, remote_full_path, local_full_path):
        """
        Transfers a local file to the remote location.
        If local_full_path_or_buffer is a string path, the file will be read
        from that location

        :param remote_full_path: full path to the remote file
        :type remote_full_path: str
        :param local_full_path: full path to the local file
        :type local_full_path: str
        """
        conn = self.get_conn()
        conn.put(local_full_path, remote_full_path)


    def delete_file(self, path):
        """
        Removes a file on the FTP Server

        :param path: full path to the remote file
        :type path: str
        """
        conn = self.get_conn()
        conn.remove(path)


    def get_mod_time(self, path):
        conn = self.get_conn()
        ftp_mdtm = conn.stat(path).st_mtime
        return datetime.datetime.fromtimestamp(ftp_mdtm).strftime('%Y%m%d%H%M%S')


    def path_exists(self, path):
        """
        Returns True if a remote entity exists

        :param path: full path to the remote file or directory
        :type path: str
        """
        conn = self.get_conn()
        return conn.exists(path)

    def walk_files(self, path='.', files=None):
        """
        Traverse remote directories on the FTP, finding all files within

        :param path: Path to traverse to find files
        :param files: Placeholder for recursion, do not use
        :return: A defaultdict. For each parent path, a list of files within (ignoring empty dirs)
        """
        conn = self.get_conn()
        # on the first pass create files dict
        if files is None:
            files = defaultdict(list)
        # loop over list of SFTPAttributes (files with modes)
        for attr in conn.listdir_attr(path):
            if stat.S_ISDIR(attr.st_mode):
                # If the file is a directory, recurse it
                self.walk_files(os.path.join(path, attr.filename), files)
            else:
                # if the file is a file, add it to our dict
                files[path].append(attr.filename)
        return files

    def file_to_local_memory(self, path):
        """
        load the file into memory

        :param path: Path to file
        :return: An io.BytesIO object containing file.
        """
        conn = self.get_conn()

        paramiko.sftp_file.SFTPFile.MAX_REQUEST_SIZE = 1024

        flo = io.BytesIO()
        conn.getfo(path, flo)
        return flo

    def describe_directory_walk(self, path, files=None):
        """
        Returns a dictionary of {filename: {attributes}} for all files
        on the remote system (where the MLSD command is supported).

        :param path: full path to the remote directory
        :type path: str
        """
        conn = self.get_conn()
        flist = conn.listdir_attr(path)

        # on the first pass create files dict
        if files is None:
            files = {}

        for f in flist:
            if stat.S_ISDIR(f.st_mode):
                # If the file is a directory, recurse it
                self.describe_directory_walk(os.path.join(path, f.filename), files)
            else:
                modify = datetime.datetime.fromtimestamp(
                    f.st_mtime).strftime('%Y%m%d%H%M%S')
                files[os.path.join(path, f.filename)] = {
                    'size': f.st_size,
                    'type': 'file',
                    'modify': modify}
        return files

    def retrieve_large_file(self, remote_full_path, local_full_path):
        """
        Method to download files that are too big for paramiko to handle correctly.
        Paramiko (and by extension, pysftp, which wraps it) has a problem with large files on certain servers
        The code here comes from the issue on this topic here:
        https://github.com/paramiko/paramiko/issues/151

        Method signature is the same as retrieve_file

        :param remote_full_path: full path to the remote file
        :type remote_full_path: str
        :param local_full_path: full path to the local file
        :type local_full_path: str
        """
        conn_details = self.get_connection(self.ssh_conn_id)
        tr = paramiko.Transport((conn_details.host, 22))
        tr.default_window_size = paramiko.common.MAX_WINDOW_SIZE // 2
        # tr.default_max_packet_size = paramiko.common.MAX_WINDOW_SIZE
        tr.packetizer.REKEY_BYTES = pow(2, 40)
        tr.packetizer.REKEY_PACKETS = pow(2, 40)
        tr.connect(username=conn_details.login, password=conn_details.password)

        with paramiko.SFTPClient.from_transport(tr) as sftp:
            sftp.get_channel().in_window_size = 2097152
            sftp.get_channel().out_window_size = 2097152
            sftp.get_channel().in_max_packet_size = 2097152
            sftp.get_channel().out_max_packet_size = 2097152

            self.log.info('Retrieving file from FTP: %s', remote_full_path)
            sftp.get(remote_full_path, local_full_path)
            self.log.info('Finished retrieving file from FTP: %s', remote_full_path)
