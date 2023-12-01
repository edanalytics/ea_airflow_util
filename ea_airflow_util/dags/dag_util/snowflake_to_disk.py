import warnings

warnings.warn(
    "The 'ea_airflow_util.dags.callables.dag_util.snowflake_to_disk' module is deprecated and will be removed in a future update."
    "Please update your code to use the new location: 'ea_airflow_util.callables.snowflake'",
    DeprecationWarning
)

from ea_airflow_util.callables.snowflake import snowflake_to_disk
