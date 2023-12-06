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
    def __init__(self,
        retention_days: int = 30,
        dry_run: bool = False,
        verbose: bool = False,
        slack_conn_id: Optional[str] = None,

        # Generic DAG arguments
        *args, default_args: dict = {}, **kwargs
    ):
        params_dict = {
            "retention_days": Param(
                default=retention_days,
                type="integer",
                description="How many days of data should be retained? (i.e., current-date - N days)"
            ),
            "dry_run": Param(
                default=dry_run,
                type="boolean",
                description="If true, perform a dry-run without deleting any records."
            ),
            "verbose": Param(
                default=verbose,
                type="boolean",
                description="If true, verbose log what is being deleted."
            ),
        }

        # If a Slack connection has been defined, add the failure callback to the default_args.
        if slack_conn_id:
            slack_failure_callback = partial(slack_callbacks.slack_alert_failure, http_conn_id=slack_conn_id)
            default_args['on_failure_callback'] = slack_failure_callback

        self.dag = DAG(
            *args,
            params=params_dict,
            default_args=default_args,
            catchup=False,
            render_template_as_native_obj=True,
            **kwargs
        )

        # TODO: One operation per table, or one operation overall?
        PythonOperator(
            task_id="airflow_db_clean",
            python_callable=self.cli_airflow_db_clean,
            dag=self.dag
        )

    @staticmethod
    def cli_airflow_db_clean(**context):
        """

        """
        # Gather param values from context (allows cleaner overrides via "Run DAG w/ config").
        retention_days = context['params']['retention_days']
        dry_run = context['params']['dry_run']
        verbose = context['params']['verbose']

        if retention_days < 30:
            raise Exception("The specified number of days to retain is less than one month!")

        max_date = (datetime.datetime.now() - datetime.timedelta(retention_days)).date()
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

        result = subprocess.run(" ".join(cli_command_args), shell=True, capture_output=True, text=True)
        logging.info(result.stdout)

        if result.stderr:
            logging.warning(result.stderr)

        result.check_returncode()
