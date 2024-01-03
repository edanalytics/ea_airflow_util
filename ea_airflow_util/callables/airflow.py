import logging

from airflow.exceptions import AirflowSkipException


def skip_if_not_in_params_list(param_name: str, value: str, **context):
    """
    Raise an AirflowSkipException if the specified value is not defined in the specified param.
    """
    if value not in context['params'][param_name]:
        logging.info(f"Value not specified in parameter `{value}`. Skipping task.")
        raise AirflowSkipException
