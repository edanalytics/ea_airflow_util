import warnings

warnings.warn(
    "The 'ea_airflow_util.dags.dag_util.slack_callbacks' module is deprecated and will be removed in a future update."
    "Please update your code to use the new location: 'ea_airflow_util.callables.slack'",
    DeprecationWarning
)

from ea_airflow_util.callables.slack import *
