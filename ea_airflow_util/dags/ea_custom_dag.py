import copy
import datetime
import inspect

from airflow import DAG
from functools import partial
from typing import Optional

from ea_airflow_util.callables import slack


class EACustomDAG(DAG):

    def __init__(self,
        *args,
        slack_conn_id: Optional[str] = None,
        default_args: Optional[dict] = None,
        user_defined_macros: Optional[dict] = None,

        # Universal args that are static for EA-use.
        max_active_runs: int = 1,
        render_template_as_native_obj: bool = True,
        catchup: bool = False,
        start_date: Optional[str] = None,
        **kwargs
    ):
        """

        """
        # Force args instantiation if not already defined.
        default_args = copy.deepcopy(default_args or {})
        user_defined_macros = copy.deepcopy(user_defined_macros or {})

        # If a Slack connection has been defined, add failure callback to the default_args and SLA-miss callback.
        if slack_conn_id:
            slack_failure_callback = partial(slack.slack_alert_failure, http_conn_id=slack_conn_id)
            default_args['on_failure_callback'] = slack_failure_callback

            slack_sla_miss_callback = partial(slack.slack_alert_sla_miss, http_conn_id=slack_conn_id)

        else:
            slack_sla_miss_callback = None

        # Add slack_conn_id to UDMs for dynamic task-alerting.
        user_defined_macros['slack_conn_id'] = slack_conn_id

        # Fix bug where start_date is not passed through tasks.
        start_date = start_date or default_args.get('start_date')
        if isinstance(start_date, str):
            start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')

        super().__init__(
            *args,
            start_date=start_date,
            catchup=catchup,
            render_template_as_native_obj=render_template_as_native_obj,
            max_active_runs=max_active_runs,
            default_args=default_args,
            user_defined_macros=user_defined_macros,
            sla_miss_callback=slack_sla_miss_callback,
            **self.subset_kwargs_to_class(DAG, kwargs)  # Remove kwargs not expected in DAG.
        )


    @staticmethod
    def subset_kwargs_to_class(class_: object, kwargs: dict) -> dict:
        """
        Helper function to remove unexpected arguments from kwargs,
        based on the actual arguments of the class.

        :param class_:
        :param kwargs:
        :return:
        """
        class_parameters = list(inspect.signature(class_).parameters.keys())
        return {
            arg: val for arg, val in kwargs.items()
            if arg in class_parameters
        }