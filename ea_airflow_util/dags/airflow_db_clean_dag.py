import datetime
import logging
import subprocess

from typing import Optional

from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from ea_airflow_util import EACustomDAG


class AirflowDBCleanDAG:
    """
    Delete data older than a specified retention period from all relevant Airflow backend tables.

    See official documentation:
        https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env-variables-ref.html#clean
    """
    MIN_RETENTION_DAYS: int = 30  # Raise an error if fewer than 30 days are specified by the user.

    def __init__(self,
        retention_days: int = 90,
        dry_run: bool = False,
        verbose: bool = False,
        slack_conn_id: Optional[str] = None,

        # Generic DAG arguments
        *args, **kwargs
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

        self.dag = EACustomDAG(
            *args,
            params=params_dict,
            slack_conn_id=slack_conn_id,
            **kwargs
        )

        # TODO: One operation per table, or one operation overall?
        PythonOperator(
            task_id="airflow_db_clean",
            python_callable=self.cli_airflow_db_clean,
            dag=self.dag
        )

    
    def cli_airflow_db_clean(self, **context):
        """
        Use a wrapper Python method instead of BashOperator for additional logging and easier command construction.
        """
        # Gather param values from context (allows cleaner overrides via "Run DAG w/ config").
        retention_days = context['params']['retention_days']
        dry_run = context['params']['dry_run']
        verbose = context['params']['verbose']

        if retention_days < self.MIN_RETENTION_DAYS:
            raise Exception("The specified number of days to retain is less than one month!")

        max_date = (datetime.datetime.now() - datetime.timedelta(retention_days)).date()
        logging.info(f"Checking Airflow database for data older than {max_date} ({retention_days} days ago)")

        if dry_run:
            logging.info("This is a dry-run! Set `dry_run=False` in DAG arguments or params to complete the deletion.")

        # Run the command against the Airflow CLI and output to logs.
        cli_command_args = [
            "airflow", "db", "clean",
            f"--clean-before-timestamp '{max_date}'",
            "--dry-run" if dry_run else "",
            "--verbose" if verbose else "",
            "--skip-archive",
            "--yes",
        ]

        result = subprocess.run(" ".join(cli_command_args), shell=True, capture_output=True, text=True)
        logging.info(result.stdout)

        if result.stderr:
            logging.warning(result.stderr)

        result.check_returncode()
