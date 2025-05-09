import itertools
import logging
import re

from typing import Iterator, Optional

import airflow
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.models import Connection, Param

from ea_airflow_util.dags.ea_custom_dag import EACustomDAG
from ea_airflow_util.callables.ssm import SSMParameterStore


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
            logging.debug(f"Ignoring unexpected parameter key: {key}")

    def to_conn(self, conn_id: str) -> dict:
        """
        Convert connection pieces into a JSON connection.
        """
        conn_keys = {"host", "login", "password"}
        param_keys = self.__kwargs.keys()

        if param_keys < conn_keys:
            raise ValueError(
                f"Connection is missing one or more required fields: {conn_keys.difference(param_keys)}"
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
    params_dict = {
        "force": Param(
            default=False,
            type="boolean",
            description="If true, recreate connection if it already exists."
        ),
    }

    def __init__(self,
        region_name: str,

        *,
        connection_mapping: Optional[dict] = None,
        prefix_year_mapping: Optional[dict] = None,
        tenant_mapping: Optional[str] = None,

        join_numbers: bool = True,
        **kwargs
    ):
        self.region_name = region_name
        self.join_numbers = join_numbers

        self.connection_mapping  = connection_mapping or {}
        self.prefix_year_mapping = prefix_year_mapping or {}
        self.tenant_mapping      = tenant_mapping or {}

        self.session = airflow.settings.Session()
        self.dag = self.build_dag(**kwargs)


    def build_dag(self, **kwargs):
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
            
            overwrite: bool = context['params']['force']

            for conn_id, conn_kwargs in itertools.chain(
                self.build_kwargs_from_connection_mapping(),
                self.build_kwargs_from_prefix_year_mapping()
            ):
                try:
                    self.upload_connection_kwargs_to_airflow(conn_id, conn_kwargs, overwrite=overwrite)
                except NameError:  # Internal-declared error
                    logging.info(f"Skipping existing connection: `{conn_id}`")
                except Exception as err:
                    logging.warning(f"Failed to import `{conn_id}`: {err}")


        with EACustomDAG(params=self.params_dict, **kwargs) as dag:
            upload_connections_from_paramstore()

        return dag


    def build_kwargs_from_connection_mapping(self):
        """
        Populate the connection_kwargs via an explicit connection-mapping.
        {ssm_prefix}/{param_type}
        """
        ### Iterate prefixes and associated params to collect connection kwargs, then yield.
        for ssm_prefix, conn_id in self.connection_mapping.items():
            param_store = SSMParameterStore(prefix=ssm_prefix, region_name=self.region_name)

            try:
                conn_kwargs = ConnectionKwargs()

                for param_type in param_store.keys():
                    param_value = param_store[param_type]
                    conn_kwargs.add_kwarg(param_type, param_value)

            except ValueError:
                logging.warning(
                    f"Parameters for prefix {ssm_prefix} do not match expected shape and will be skipped."
                )
                continue

            yield conn_id, conn_kwargs


    def build_kwargs_from_prefix_year_mapping(self) -> Iterator[ConnectionKwargs]:
        """
        Populate the connection_kwargs via prefix_year- and tenant-mappings.
        # {ssm_prefix}/{tenant_code}/{param_type}
        """
        for ssm_prefix, api_year in self.prefix_year_mapping.items():
            param_store = SSMParameterStore(prefix=ssm_prefix, region_name=self.region_name)

            ### Iterate tenant codes and associated params to collect connection kwargs, then yield.
            for tenant_code in param_store.keys():
                try:
                    conn_kwargs = ConnectionKwargs()

                    for param_type in param_store[tenant_code].keys():
                        param_value = param_store[tenant_code][param_type]
                        conn_kwargs.add_kwarg(param_type, param_value)

                except ValueError:
                    logging.warning(
                        f"Parameters for prefix `{ssm_prefix}/{tenant_code}` do not match expected shape and will be skipped."
                    )
                    continue

                ### Standardize the connection ID.
                # Translate the tenant-code if provided in the mapping.
                # Note: This mutates `tenant_code`, so it must be placed after the ParamStore collect.
                if tenant_code in self.tenant_mapping:
                    tenant_code = self.tenant_mapping[tenant_code]
                else:
                    # Replace dashes and spaces with underscores.
                    tenant_code = tenant_code.replace('-', '_').replace(' ', '_')

                    # Remove underscores between district name and number, if specified.
                    if self.join_numbers:
                        tenant_code = re.sub(r"^(.*)_(\d+)$", r"\1\2", tenant_code)

                conn_id = f"edfi_{tenant_code}_{api_year}"
                yield conn_id, conn_kwargs


    def upload_connection_kwargs_to_airflow(self, conn_id: str, conn_kwargs: ConnectionKwargs, overwrite: bool = False):
        """
        Attempt to upload connections to Airflow, warning if already present or incomplete.
        https://stackoverflow.com/questions/51863881
        """
        # Verify whether the connection already exists in Airflow, and continue if not overwriting.
        if self.session.query(Connection).filter(Connection.conn_id == conn_id).first():
            if overwrite:
                self.session.delete(conn_id)
                self.session.commit()
            else:
                raise NameError("Connection already exists!")

        # Try to convert the kwargs into a connection, erroring if missing a required field.
        conn = conn_kwargs.to_conn(conn_id)
        self.session.add(conn)
        self.session.commit()

        logging.info(
            f"Successful import: Connection `{conn_id}` added to Airflow."
        )
