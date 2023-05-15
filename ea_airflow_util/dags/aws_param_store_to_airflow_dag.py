import logging
import re

from collections import defaultdict
from typing import Optional

import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import Connection

from .dag_util.ssm_parameter_store import SSMParameterStore


class ConnectionKwargs:
    """ Class for storing connection pieces from ParamStore """
    def __init__(self):
        self.__kwargs = {}  # Keep kwargs hidden from logging

    def add_kwarg(self, key, value):
        """
        Add key to connection kwargs, translating as necessary.
        """
        if key == 'key':
            self.__kwargs['login'] = value
        elif key == 'secret':
            self.__kwargs['password'] = value
        elif key == 'url':
            self.__kwargs['host'] = value
        else:
            self.__kwargs[key] = value

    def to_conn(self, conn_id: str) -> dict:
        """
        Convert connection pieces into a JSON connection.
        """
        if self.__kwargs.keys() < {"host", "login", "password"}:
            raise Exception(
                f"Connection is missing one or more required fields."
            )

        return Connection(conn_id=conn_id, conn_type='http', **self.__kwargs)


class AWSParamStoreToAirflowDAG:
    """
    Build Airflow connections based on key, secret, and url parameters in AWS SystemsManager ParameterStore.

    The presumed structure of parameters are as follows:
    {ssm_prefix}/{tenant_code}/key
    {ssm_prefix}/{tenant_code}/secret
    {ssm_prefix}/{tenant_code}/url

    Argument `prefix_year_mapping` maps prefixes to API years of data.
    Optional argument `tenant_mapping` provides tenant_code naming-fixes when they misalign in ParameterStore.

    """
    def __init__(self,
        region_name: str,

        *,
        connection_mapping: Optional[dict] = None,
        prefix_year_mapping: Optional[dict] = None,
        tenant_mapping: Optional[str] = None,

        overwrite: bool = False,
        join_numbers: bool = False,

        **kwargs
    ):
        self.region_name = region_name
        self.overwrite = overwrite
        self.join_numbers = join_numbers

        self.connection_mapping  = connection_mapping or {}
        self.prefix_year_mapping = prefix_year_mapping or {}
        self.tenant_mapping      = tenant_mapping or {}

        self.connection_kwargs = defaultdict(ConnectionKwargs)
        self.dag = self.build_dag(**kwargs)


    def build_dag(self,
        dag_id: str,
        default_args: dict,
        **kwargs
    ):
        """

        :param dag_id:
        :param default_args:
        :return:
        """
        @task
        def upload_connections_from_paramstore(**context):
            """
            Iterate ParamStore across prefix-mappings and collect connection kwargs.
            Iterate kwargs into connections and attempt import.

            Note: This method mutates self.connection_kwargs. This object resets between tasks, so all logic is unified.
            """
            ### Iterate ParamStore across prefix-mappings and collect connection kwargs.
            if not (self.connection_mapping or self.prefix_year_mapping):
                raise AirflowFailException(
                    "Neither arguments `connection_mapping` nor `prefix_year_mapping` have been defined."
                )

            if self.connection_mapping:
                self.build_kwargs_from_connection_mapping()

            if self.prefix_year_mapping:
                self.build_kwargs_from_prefix_year_mapping()

            ### Iterate kwargs into connections and attempt import.
            if not self.connection_kwargs:
                raise AirflowSkipException(
                    "No connections were found using specified arguments!"
                )

            self.upload_connection_kwargs_to_airflow()


        with DAG(
            dag_id=dag_id,
            default_args=default_args,
            catchup=False,
            **kwargs
        ) as dag:
            upload_connections_from_paramstore()

        return dag


    def build_kwargs_from_connection_mapping(self):
        """
        Populate the connection_kwargs via an explicit connection-mapping.
        {ssm_prefix}/{param_type}
        """
        for ssm_prefix, conn_id in self.connection_mapping.items():
            param_store = SSMParameterStore(prefix=ssm_prefix, region_name=self.region_name)

            # Add each param to the connection kwargs dictionary.
            for param_type in param_store.keys():
                self.connection_kwargs[conn_id].add_kwarg(param_type, param_store[param_type])


    def build_kwargs_from_prefix_year_mapping(self):
        """
        Populate the connection_kwargs via prefix_year- and tenant-mappings.
        # {ssm_prefix}/{tenant_code}/{param_type}
        """
        for ssm_prefix, api_year in self.prefix_year_mapping.items():
            param_store = SSMParameterStore(prefix=ssm_prefix, region_name=self.region_name)

            # Add each param-combination to the connection kwargs dictionary.
            for param_name in param_store.keys():
                logging.info(param_name)
                try:
                    tenant_code, param_type = param_name.replace(ssm_prefix, "").strip('/').split('/')
                except:
                    logging.warning(
                        f"Parameter {param_name} does not match expected shape and will be skipped."
                    )
                    continue

                # Translate the tenant-code if provided in the mapping.
                if tenant_code in self.tenant_mapping:
                    tenant_code = self.tenant_mapping[tenant_code]
                else:
                    # Replace dashes and spaces with underscores.
                    tenant_code = tenant_code.replace('-', '_').replace(' ', '_')

                    # Remove underscores between district name and number, if specified.
                    if self.join_numbers:
                        tenant_code = re.sub(r"^(.*)_(\d+)$", r"\1\2", tenant_code)

                # Standardize the connection ID.
                conn_id = f"edfi_{tenant_code}_{api_year}"
                self.connection_kwargs[conn_id].add_kwarg(param_type, param_store[param_name])


    def upload_connection_kwargs_to_airflow(self):
        """
        Attempt to upload connections to Airflow, warning if already present or incomplete.
        https://stackoverflow.com/questions/51863881
        """
        # Establish a connection and begin upload.
        session = airflow.settings.Session()

        for conn_id, conn_kwargs in self.connection_kwargs.items():

            # Verify whether the connection already exists in Airflow, and continue if not overwriting.
            if session.query(Connection).filter(Connection.conn_id == conn_id).first():

                if not self.overwrite:
                    logging.warning(
                        f"Failed to import `{conn_id}`: Connection already exists!"
                    )
                    continue

            # Try to convert the kwargs into a connection, erroring if missing a required field.
            try:
                conn = conn_kwargs.to_conn(conn_id)
            except Exception as err:
                logging.warning(
                    f"Failed to import `{conn_id}`: {err}"
                )
                continue

            # Add the connection
            session.add(conn)
            session.commit()

            logging.info(
                f"Connection {conn_id} was added."
            )
