import textwrap

from airflow.contrib.hooks.slack_webhook_hook import SlackWebhookHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator


MESSAGES = {
    "failure": """
        :red_circle: Task Failed. 
        *Task*: {{{{ ti.task_id }}}}
        *Dag*: {{{{ ti.dag_id }}}}
        *Execution Time*: {{{{ dag_run.logical_date }}}}
        *Log Url*: {{{{ ti.log_url }}}} 
    """,

    "success": """
        :heavy_check_mark: Task Succeeded. 
        *Task*: {{{{ ti.task_id }}}}
        *Dag*: {{{{ ti.dag_id }}}}
        *Execution Time*: {{{{ dag_run.logical_date }}}}
    """,

    "edfi_pull_failure": """
        :exclamation: Resource did not pull from Ed-Fi ODS:
        *Ed-Fi ODS*: {edfi_conn_id}
        *Resource*: {resource}
        *File Path*: {full_local_path}
        *Error*: {error}
    """,

    "s3_upload_failure": """
        :exclamation: File did not upload to s3:
        *File Path*: {full_local_path}
        *File Key*: {file_key}
        *Error*: {error}
    """,

    "snowflake_copy_failure": """
        :exclamation: File did not copy into Snowflake:
        *File Key*: {file_key}
        *Dest Table*: {dest_table}
        *Error*: {error}
    """,
}

def _get_slack_message(message_type: str, **kwargs):
    return textwrap.dedent(
        MESSAGES[message_type].format(**kwargs)
    )

def _get_slack_alert_hook(http_conn_id: str, message_type: str, *args, **kwargs):
    """
    This class runs the SlackWebhookOperator with prebuilt messages.
    https://airflow.apache.org/docs/apache-airflow/1.10.13/_modules/airflow/contrib/hooks/slack_webhook_hook.html

    Args and kwargs in init are passed to both SlackWebhookHook and to format the message string.
    Because of this, make sure string format keywords do not collide with named operator kwargs.
    """
    message = _get_slack_message(message_type, **kwargs)

    return SlackWebhookHook(
        http_conn_id=http_conn_id,
        message=message,
        *args, **kwargs
    )

def _get_slack_alert_operator(http_conn_id: str, message_type: str, *args, **kwargs):
    """
    This class runs the SlackWebhookOperator with prebuilt messages.
    https://airflow.apache.org/docs/apache-airflow/1.10.13/_modules/airflow/contrib/operators/slack_webhook_operator.html

    Args and kwargs in init are passed to both SlackWebhookOperator and to format the message string.
    Because of this, make sure string format keywords do not collide with named operator kwargs.
    """
    message = _get_slack_message(message_type, **kwargs)

    return SlackWebhookOperator(
        task_id=message_type,
        http_conn_id=http_conn_id,
        message=message,
        *args, **kwargs
    )



def slack_alert_failure(http_conn_id, *args, **kwargs):
    return _get_slack_alert_hook(http_conn_id, message_type='failure', *args, **kwargs).execute()

def slack_alert_success(http_conn_id, *args, **kwargs):
    return _get_slack_alert_hook(http_conn_id, message_type='success', *args, **kwargs).execute()

def slack_alert_edfi_pull_failure(http_conn_id, *args, **kwargs):
    return _get_slack_alert_hook(http_conn_id, message_type='edfi_pull_failure', *args, **kwargs).execute()

def slack_alert_task_s3_upload_failure(http_conn_id, *args, **kwargs):
    return _get_slack_alert_hook(http_conn_id, message_type='s3_upload_failure', *args, **kwargs).execute()

def slack_alert_task_snowflake_copy_failure(http_conn_id, *args, **kwargs):
    return _get_slack_alert_hook(http_conn_id, message_type='snowflake_copy_failure', *args, **kwargs).execute()



def task_slack_alert_failure(context, http_conn_id, *args, **kwargs):
    return _get_slack_alert_operator(http_conn_id, message_type='failure', *args, **kwargs).execute(context)

def task_slack_alert_success(context, http_conn_id, *args, **kwargs):
    return _get_slack_alert_operator(http_conn_id, message_type='success', *args, **kwargs).execute(context)

def task_slack_alert_edfi_pull_failure(context, http_conn_id, *args, **kwargs):
    return _get_slack_alert_operator(http_conn_id, message_type='edfi_pull_failure', *args, **kwargs).execute(context)

def task_slack_alert_task_s3_upload_failure(context, http_conn_id, *args, **kwargs):
    return _get_slack_alert_operator(http_conn_id, message_type='s3_upload_failure', *args, **kwargs).execute(context)

def task_slack_alert_task_snowflake_copy_failure(context, http_conn_id, *args, **kwargs):
    return _get_slack_alert_operator(http_conn_id, message_type='snowflake_copy_failure', *args, **kwargs).execute(context)
