import logging

from typing import Union

from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowSkipException


def xcom_pull_template(
    task_ids: Union[str, BaseOperator],
    key: str = 'return_value'
) -> str:
    """
    Generate a Jinja template to pull a particular xcom key from a task_id
    :param task_ids: An upstream task to pull xcom from
    :param key: The key to retrieve. Default: return_value
    :return: A formatted Jinja string for the xcom pull
    """
    # If an Airflow operator is provided, automatically infer its task_id.
    if isinstance(task_ids, BaseOperator):
        task_ids = task_ids.task_id

    xcom_string = f"ti.xcom_pull(task_ids='{task_ids}', key='{key}')"
    return '{{ ' + xcom_string + ' }}'

def skip_if_not_in_params_list(param_name: str, value: str, **context):
    """
    Raise an AirflowSkipException if the specified value is not defined in the specified param.
    """
    if value not in context['params'][param_name]:
        logging.info(f"Value not specified in parameter `{value}`. Skipping task.")
        raise AirflowSkipException
