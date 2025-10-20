import logging
import os

from ea_airflow_util import EACustomDAG

from airflow.decorators import task


class RotateLogsDAG:
    """
    A DAG for rotating Airflow logs.

    Currently, this DAG only deletes empty directories in the Airflow logs
    folder. It does not save off logs or delete logs at this time. See Jira item
    https://edanalytics.atlassian.net/browse/STAD-198

    If located somewhere other than `$AIRFLOW_HOME/logs`, set the `logs_dir`
    parameter to the path to the Airflow logs root directory.

    Example entry in an airflow_config.yml file:

    ```yaml
    rotate_logs_dag:
        schedule_interval: "@weekly"
        default_args: *default_task_args
        logs_dir: /home/airflow/airflow/logs
    ```
    """

    def __init__(self, *, logs_dir: str = None, **kwargs) -> None:
        self.logs_dir = logs_dir or os.path.join(os.environ["AIRFLOW_HOME"], "logs")
        self.dag = self.build_dag(**kwargs)

    def get_empty_directories(self) -> list[str]:
        empty_dirs = []
        for root, dirs, files in os.walk(self.logs_dir):
            # The answer to "What does it mean for a directory to be empty"
            # could be more complex. This check works for Dag/Task directories.
            if not dirs and not files:
                empty_dirs.append(root)
        return empty_dirs

    def build_dag(self, **kwargs):
        @task
        def delete_empty_directories():
            paths = self.get_empty_directories()
            logging.info(f"Deleting {len(paths)} empty directories in {self.logs_dir}")

            for path in paths:
                logging.info(f"Removing empty directory {path}")
                os.rmdir(path)

        with EACustomDAG(**kwargs) as dag:
            delete_empty_directories()

        return dag
