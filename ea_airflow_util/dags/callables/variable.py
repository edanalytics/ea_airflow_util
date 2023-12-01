import warnings

warnings.warn(
    "The 'ea_airflow_util.dags.callables.variable' module is deprecated and will be removed in a future update."
    "Please update your code to use the new location: 'ea_airflow_util.callables.airflow'",
    DeprecationWarning
)

from ea_airflow_util.callables.airflow import check_variable, update_variable
