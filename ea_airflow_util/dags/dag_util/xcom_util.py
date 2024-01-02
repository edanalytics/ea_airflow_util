from typing import Union

from airflow.models.baseoperator import BaseOperator


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
