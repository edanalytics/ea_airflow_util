import textwrap

from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator


def _execute_slack_message(context: dict, http_conn_id: str, message: str, **kwargs):
    """
    This class runs the SlackWebhookOperator with prebuilt messages.
    https://airflow.apache.org/docs/apache-airflow-providers-slack/stable/_api/airflow/providers/slack/operators/slack_webhook/index.html

    Kwargs in init are passed to SlackWebhookOperator.
    """
    return SlackWebhookOperator(
        http_conn_id=http_conn_id,
        message=message,
        **kwargs
    ).execute(context)


def slack_alert_failure(context, http_conn_id, **kwargs):
    """  """
    message = textwrap.dedent("""
        :red_circle: Task Failed. 
        *Task*: {{ ti.task_id }}
        *Dag*: {{ ti.dag_id }}
        *Execution Time*: {{ dag_run.logical_date }}
        *Log Url*: {{ ti.log_url }}
    """)
    return _execute_slack_message(context, http_conn_id=http_conn_id, message=message, **kwargs)


def slack_alert_success(context, http_conn_id, **kwargs):
    """  """
    message = textwrap.dedent("""
        :heavy_check_mark: Task Succeeded. 
        *Task*: {{ ti.task_id }}
        *Dag*: {{ ti.dag_id }}
        *Execution Time*: {{ dag_run.logical_date }}
    """)
    return _execute_slack_message(context, http_conn_id=http_conn_id, message=message, **kwargs)
