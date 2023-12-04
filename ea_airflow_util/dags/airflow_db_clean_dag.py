import datetime
import logging
import subprocess

from functools import partial
from typing import Optional

from airflow.models.param import Param
from airflow.models import DAG
from airflow.operators.python import PythonOperator

import ea_airflow_util.dags.dag_util.slack_callbacks as slack_callbacks


class AirflowDBCleanDAG:
    """
    Delete data older than a specified retention period from all relevant Airflow backend tables.
    """

    params_dict = {
        "retention_days": Param(
            default=0,
            type="integer",
            description="How many days of data should be retained? (i.e., current-date - N days)"
        ),
        "dry_run": Param(
            default=False,
            type="boolean",
            description="If true, perform a dry-run without deleting any records."
        ),
        "verbose": Param(
            default=False,
            type="boolean",
            description="If true, verbose log what is being deleted."
        ),
    }

    def __init__(self,
        retention_days: int = 30,
        dry_run: bool = False,
        verbose: bool = False,
        slack_conn_id: Optional[str] = None,
        *args, **kwargs
    ):
        self.retention_days: int = retention_days
        self.dry_run: bool = dry_run
        self.verbose: bool = verbose
        self.slack_conn_id: Optional[str] = slack_conn_id

        self.dag = self.initialize_dag(*args, **kwargs)

    def initialize_dag(self, default_args, *args, **kwargs):
        """

        """
        # If a Slack connection has been defined, add the failure callback to the default_args.
        if self.slack_conn_id:
            slack_failure_callback = partial(slack_callbacks.slack_alert_failure, http_conn_id=self.slack_conn_id)
            default_args['on_failure_callback'] = slack_failure_callback

        dag = DAG(
            *args,
            catchup=False,
            params=self.params_dict,
            render_template_as_native_obj=True,
            **kwargs
        )

        PythonOperator(
            task_id="airflow_db_clean",
            python_callable=self.cli_airflow_db_clean,
            dag=dag
        )

        return dag

    def cli_airflow_db_clean(self, **context):
        """

        """
        # Override DAG arguments with params if specified.
        retention_days = context['params']['retention_days'] or self.retention_days
        dry_run = context['params']['dry_run'] or self.dry_run
        verbose = context['params']['verbose'] or self.verbose

        if retention_days < 30:
            raise Exception("The specified number of days to retain is less than one month!")

        max_date = datetime.datetime.now() - datetime.timedelta(retention_days)
        logging.info(f"Checking Airflow database for data older than {max_date} ({retention_days} days ago)")

        if dry_run:
            logging.info("This is a dry-run! Set `dry_run=False` in DAG arguments or params to complete the deletion.")

        # Use `subprocess` instead of BashOperator because it's easier to override DAG-args with params.
        cli_command_args = [
            "airflow", "db", "clean",
            f"--clean-before-timestamp '{max_date}'",
            "--dry-run" if dry_run else "",
            "--verbose" if verbose else "",
            "--yes",
        ]

        result = subprocess.run(cli_command_args)
        result.check_returncode()
        return result.returncode
