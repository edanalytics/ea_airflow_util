import warnings

warnings.warn(
    "The 'ea_airflow_util.dags.callables.dag_util.s3_to_postgres' module is deprecated and will be removed in a future update."
    "Please update your code to use the new location: 'ea_airflow_util.callables.s3'",
    DeprecationWarning
)

from ea_airflow_util.callables.s3 import s3_to_postgres
