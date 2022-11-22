import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


class CleanAirflowDAG:
    """

    """
    def __init__(self,
        tmp_dir: str,
        rds_days_kept: int,
        **kwargs
    ):
        """

        """
        self.tmp_dir = tmp_dir
        self.rds_days_kept = rds_days_kept

        self.dag = self.initialize_dag(**kwargs)


    def initialize_dag(self,
        dag_id,
        schedule_interval,
        default_args,
        **kwargs
    ):
        """
        :param dag_id:
        :param schedule_interval:
        :param default_args:
        """
        with DAG(
            dag_id=dag_id,
            schedule_interval=schedule_interval,
            default_args=default_args,
            **kwargs
        ) as dag:

            # Clean up empty folders left behind in tmp storage.
            # "-mindepth 1": do not delete root directory
            # "-type d"    : find directories
            clean_tmp_dir = BashOperator(
                task_id="clean_tmp_dir",
                bash_command = f"""
                    find "{self.tmp_dir}" -mindepth 1 -type d -delete -empty
                """
            )

            # Delete RDS logs greater than specified time.
            # "--yes": Do not prompt to confirm.
            first_date = datetime.date.today() - datetime.timedelta(days=self.rds_days_kept)

            delete_rds_logs = BashOperator(
                task_id="delete_rds_logs",
                bash_command = f"""
                    airflow db clean --clean-before-timestamp '{first_date}' --yes
                """
            )


        return dag
