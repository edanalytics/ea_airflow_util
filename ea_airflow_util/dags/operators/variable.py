import logging
from typing import Callable, Union

from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.python import PythonOperator


def build_variable_update_operator(var: str, new_val: Union[Callable, int, str], **kwargs):
    """
    Return a Python operator that updates a specified Airflow variable.

    Argument `new_val` can be static or a lambda for dynamic updates.
    """
    def _update_variable():
        """  """
        # If a lambda is provided, retrieve the current value to pass into the lambda.
        if callable(new_val):
            current_val = Variable.get(var)
            return_val  = new_val(current_val)
            Variable.update(var, return_val)

        # Otherwise, set the static value.
        else:
            Variable.update(var, new_val)

    return PythonOperator(
        python_callable=_update_variable,
        **kwargs
    )


def build_variable_check_operator(var: str, condition: Callable, **kwargs):
    """
    Return a Python operator that skips if the retrieved variable does not meet the provided condition.
    """
    def _check_variable():
        """  """
        current_val = Variable.get(var)

        if condition(current_val):
            logging.info(
                f"Variable `{var}` meets provided condition. Marking task as successful."
            )
        else:
            raise AirflowSkipException(
                f"Variable `{var}` does not meet provided condition. Marking task as skipped."
            )

    return PythonOperator(
        python_callable=_check_variable,
        **kwargs
    )