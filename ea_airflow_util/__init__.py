import sys

from ea_airflow_util.dags.aws_param_store_to_airflow_dag import AWSParamStoreToAirflowDAG
from ea_airflow_util.dags.run_dbt_airflow_dag import RunDbtDag
from ea_airflow_util.dags.update_dbt_docs_dag import UpdateDbtDocsDag
from ea_airflow_util.dags.s3_to_snowflake_dag import S3ToSnowflakeDag
from ea_airflow_util.dags.dbt_snapshot_dag import DbtSnapshotDag
from ea_airflow_util.dags.sftp_to_snowflake_dag import SFTPToSnowflakeDag

from ea_airflow_util.callables.variable import check_variable, update_variable


# Reroute deprecated module pathing.
# Using this SO as inspiration: https://stackoverflow.com/a/72244240
from ea_airflow_util.providers.dbt.operators import dbt as dbt_operators
sys.modules['ea_airflow_util.dags.operators.dbt_operators'] = dbt_operators

from ea_airflow_util.callables import airflow as airflow_callables
sys.modules['ea_airflow_util.dags.callables.xcom_util'] = airflow_callables

from ea_airflow_util.callables import variable as variable_callables
sys.modules['ea_airflow_util.dags.callables.variable'] = variable_callables

from ea_airflow_util.callables import s3 as s3_callables
sys.modules['ea_airflow_util.dags.callables.dag_util.s3_to_postgres'] = s3_callables

from ea_airflow_util.callables import slack as slack_callbacks
sys.modules['ea_airflow_util.dags.dag_util.slack_callbacks'] = slack_callbacks

from ea_airflow_util.callables import snowflake as snowflake_callables
sys.modules['ea_airflow_util.dags.dag_util.snowflake_to_disk'] = snowflake_callables

