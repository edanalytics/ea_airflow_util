from ea_airflow_util.dags.aws_param_store_to_airflow_dag import AWSParamStoreToAirflowDAG
from ea_airflow_util.dags.run_dbt_airflow_dag import RunDbtDag
from ea_airflow_util.dags.update_dbt_docs_dag import UpdateDbtDocsDag
from ea_airflow_util.dags.s3_to_snowflake_dag import S3ToSnowflakeDag
from ea_airflow_util.dags.dbt_snapshot_dag import DbtSnapshotDag
from ea_airflow_util.dags.sftp_to_snowflake_dag import SFTPToSnowflakeDag

from ea_airflow_util.callables.variable import check_variable, update_variable


# Reroute deprecated module pathing.
# Using this SO as inspiration: https://stackoverflow.com/a/72244240
import importlib
import sys
from types import ModuleType

class LazyModule(ModuleType):
    def __init__(self, name, mod_name):
        super().__init__(name)
        self.__mod_name = mod_name

    def __getattr__(self, attr):
        if "_lazy_module" not in self.__dict__:
            self._lazy_module = importlib.import_module(self.__mod_name, package="ea_airflow_util")
        return self._lazy_module.__getattr__(attr)

rename_mapping = {
    "ea_airflow_util.dags.dag_util.xcom_util"          : "ea_airflow_util.callables.airflow",
    "ea_airflow_util.dags.dag_util.s3_to_postgres"     : "ea_airflow_util.callables.s3",
    "ea_airflow_util.dags.dag_util.slack_callbacks"    : "ea_airflow_util.callables.slack",
    "ea_airflow_util.dags.dag_util.snowflake_to_disk"  : "ea_airflow_util.callables.snowflake",
    "ea_airflow_util.dags.dag_util.ssm_parameter_store": "ea_airflow_util.callables.ssm",
    "ea_airflow_util.dags.callables.variable"          : "ea_airflow_util.callables.variable",
    "ea_airflow_util.dags.operators.dbt_operators"     : "ea_airflow_util.providers.dbt.operators.dbt",
    "ea_airflow_util.dags.operators.loop_s3_file_transform_operator": "ea_airflow_util.providers.aws.operators.s3",
}

for old_path, new_path in rename_mapping.items():
    sys.modules[old_path] = LazyModule(old_path, new_path)
