import logging
from typing import Callable, Union

from airflow.exceptions import AirflowSkipException
from airflow.models import Variable


def update_variable(var: str, value: Union[Callable, int, str], **kwargs):
    """
    Update a specified Airflow variable.

    Argument `new_val` can be static or a lambda for dynamic updates.
    """
    # If a lambda is provided, retrieve the current value to pass into the lambda.
    if callable(value):
        current_val = Variable.get(var)
        return_val = value(current_val)
        Variable.update(var, return_val)

    # Otherwise, set the static value.
    else:
        Variable.update(var, value)


def check_variable(var: str, condition: Callable, **kwargs):
    """
    Return a Python operator that skips if the retrieved variable does not meet the provided condition.
    """
    current_val = Variable.get(var)

    if condition(current_val):
        logging.info(
            f"Variable `{var}` meets provided condition. Marking task as successful."
        )
    else:
        raise AirflowSkipException(
            f"Variable `{var}` does not meet provided condition. Marking task as skipped."
        )
