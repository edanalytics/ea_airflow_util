import textwrap

from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook


def _execute_slack_message(http_conn_id: str, message: str, **kwargs):
    """
    This class runs the SlackWebhookOperator with prebuilt messages.
    https://airflow.apache.org/docs/apache-airflow-providers-slack/stable/_api/airflow/providers/slack/operators/slack_webhook/index.html

    Kwargs in init are passed to SlackWebhookOperator.
    """
    return SlackWebhookHook(
        http_conn_id=http_conn_id,
        message=message,
        **kwargs
    ).execute()

def slack_alert_failure(context, http_conn_id, **kwargs):
    """  """
    message = textwrap.dedent(f"""
        :red_circle: Task Failed. 
        *Task*: { context['ti'].task_id }
        *Dag*: { context['ti'].dag_id }
        *Execution Time*: { context['dag_run'].logical_date }
        *Log Url*: { context['ti'].log_url }
    """)
    return _execute_slack_message(http_conn_id=http_conn_id, message=message, **kwargs)

def slack_alert_success(context, http_conn_id, **kwargs):
    """  """
    message = textwrap.dedent(f"""
        :heavy_check_mark: Task Succeeded. 
        *Task*: { context['ti'].task_id }
        *Dag*: { context['ti'].dag_id }
        *Execution Time*: { context['dag_run'].logical_date }
    """)
    return _execute_slack_message(http_conn_id=http_conn_id, message=message, **kwargs)

def slack_alert_sla_miss(
    http_conn_id,
    dag, task_list, blocking_task_list, slas, blocking_tis,
    **kwargs
):
    """
    Inspired by this StackOverflow: https://stackoverflow.com/questions/64040649
    Note: SLA callbacks require 5 arguments be provided. We only use `slas` to build the message.

    """
    dag_id = slas[0].dag_id
    task_id = slas[0].task_id
    execution_date = slas[0].execution_date.isoformat()

    message = textwrap.dedent(f"""
        :sos: *SLA has been missed.*
        *Task*: {task_id}
        *Dag*: {dag_id}
        *Execution Time*: {execution_date}
    """)
    return _execute_slack_message(http_conn_id=http_conn_id, message=message, **kwargs)

def slack_alert_download_failure(context, http_conn_id, remote_path: str, local_path: str, error: str, **kwargs):
    """  """
    message = textwrap.dedent(f"""
        :red_circle: File did not download
        *Remote Path*: {remote_path}
        *Local Path*: {local_path}
        *Error*: {error}
        *Task*: {context['ti'].task_id}
        *Dag*: {context['ti'].dag_id}
        *Execution Time*: {context['dag_run'].logical_date}
        *Log Url*: {context['ti'].log_url}
    """)
    return _execute_slack_message(http_conn_id=http_conn_id, message=message, **kwargs)

def slack_alert_s3_upload_failure(context, http_conn_id: str, local_path: str, file_key: str, **kwargs):
    """  """
    message = textwrap.dedent(f"""
        :red_circle: File did not upload to S3
        *File Path*: {local_path}
        *File Key*: {file_key}
        *Task*: { context['ti'].task_id }
        *Dag*: { context['ti'].dag_id }
        *Execution Time*: { context['dag_run'].logical_date }
        *Log Url*: { context['ti'].log_url }
    """)
    return _execute_slack_message(http_conn_id=http_conn_id, message=message, **kwargs)

def slack_alert_insert_failure(context, http_conn_id, file_key: str, table: str, error: str, **kwargs):
    """  """
    message = textwrap.dedent(f"""
        :red_circle: File did not insert to database
        *File Key*: {file_key}
        *Dest Table*: {table}
        *Error*: {error}
        *Task*: {context['ti'].task_id}
        *Dag*: {context['ti'].dag_id}
        *Execution Time*: {context['dag_run'].logical_date}
        *Log Url*: {context['ti'].log_url}
    """)
    return _execute_slack_message(http_conn_id=http_conn_id, message=message, **kwargs)

def slack_alert_file_format_failure(
    context,
    http_conn_id: str,
    local_path: str,
    file_type: str,
    cols_expected: list,
    cols_found: list,
    **kwargs
):
    """  """
    message = textwrap.dedent(f"""
        :red_circle: File did not match expected spec
        *File Path*: {local_path}
        *File Type*: {file_type}
        *Exp. Cols*: {cols_expected}
        *Found Cols*: {cols_found}
        *Task*: { context['ti'].task_id }
        *Dag*: { context['ti'].dag_id }
        *Execution Time*: { context['dag_run'].logical_date }
        *Log Url*: { context['ti'].log_url }
    """)
    return _execute_slack_message(http_conn_id=http_conn_id, message=message, **kwargs)

def slack_alert_match_spec_failure(context, http_conn_id, local_path: str, error: str, **kwargs):
    """  """
    message = textwrap.dedent(f"""
        :red_circle: File did not match file spec
        *File Path*: {local_path}
        *Error*: {error}
        *Task*: {context['ti'].task_id}
        *Dag*: {context['ti'].dag_id}
        *Execution Time*: {context['dag_run'].logical_date}
        *Log Url*: {context['ti'].log_url}
    """)
    return _execute_slack_message(http_conn_id=http_conn_id, message=message, **kwargs)
