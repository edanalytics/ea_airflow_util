from ea_airflow_util.dags.ea_custom_dag import EACustomDAG

from ea_airflow_util.dags.airflow_db_clean_dag import AirflowDBCleanDAG
from ea_airflow_util.dags.aws_param_store_to_airflow_dag import AWSParamStoreToAirflowDAG
from ea_airflow_util.dags.run_dbt_airflow_dag import RunDbtDag
from ea_airflow_util.dags.update_dbt_docs_dag import UpdateDbtDocsDag
from ea_airflow_util.dags.s3_to_snowflake_dag import S3ToSnowflakeDag
from ea_airflow_util.dags.dbt_snapshot_dag import DbtSnapshotDag
from ea_airflow_util.dags.sftp_to_snowflake_dag import SFTPToSnowflakeDag

from ea_airflow_util.callables.airflow import xcom_pull_template
from ea_airflow_util.callables import slack as slack_callbacks
from ea_airflow_util.callables.variable import check_variable, update_variable

from ea_airflow_util.providers.aws.operators.s3 import LoopS3FileTransformOperator
from ea_airflow_util.providers.dbt.operators.dbt import DbtRunOperationOperator
from ea_airflow_util.providers.sftp.hooks.sftp import SFTPHook
from ea_airflow_util.providers.sharefile.hooks.sharefile import SharefileHook
from ea_airflow_util.providers.sharefile.transfers.sharefile_to_disk import SharefileToDiskOperator


# Reroute deprecated module pathing.
# Using this SO as inspiration: https://stackoverflow.com/a/72244240
import importlib
import sys
from types import ModuleType
from typing import Optional

class LazyModule(ModuleType):
    def __init__(self, name: str, mod_name: str, child_mapping: Optional[dict] = None):
        super().__init__(name)
        self.__mod_name = mod_name
        self.__child_mapping = child_mapping or {}

    def __getattr__(self, attr):
        if "_lazy_module" not in self.__dict__:
            self._lazy_module = importlib.import_module(self.__mod_name, package="ea_airflow_util")

        if attr in self.__child_mapping:
            return getattr(self._lazy_module, self.__child_mapping[attr])
        else:
            return getattr(self._lazy_module, attr)


rename_mapping = {
    "ea_airflow_util.dags.dag_util": {
        "name": "ea_airflow_util.callables",
        "child_mapping": {
            "xcom_util": "airflow",
            "s3_to_postgres": "sql",
            "slack_callbacks": "slack",
            "snowflake_to_disk": "snowflake",
            "ssm_parameter_store": "ssm",
        }
    },
    "ea_airflow_util.dags.callables": {
        "name": "ea_airflow_util.callables",
        "child_mapping": {
            "variable": "variable"
        }
    },
    "ea_airflow_util.dags.operators": {
        "name": "ea_airflow_util.providers.dbt.operators.dbt",
        "child_mapping": {
            "dbt_operators": "dbt.operators.dbt",
            "loop_s3_file_transform_operator": "aws.operators.s3"
        }
    },
}

for old_path, new_path_metadata in rename_mapping.items():
    new_path = new_path_metadata['name']
    child_mapping = new_path_metadata['child_mapping']
    sys.modules[old_path] = LazyModule(old_path, new_path, child_mapping=child_mapping)

    for old_child, new_child in child_mapping.items():
        full_old_path = '.'.join([old_path, old_child])
        full_new_path = '.'.join([new_path, new_child])
        sys.modules[full_old_path] = LazyModule(full_old_path, full_new_path)
