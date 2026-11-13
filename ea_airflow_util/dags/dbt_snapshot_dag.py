from providers.ea_dbt.operators.ea_dbt import dbt_task

from ea_airflow_util.dags.ea_custom_dag import EACustomDAG


class DbtSnapshotDag:
    """
    :param dbt_repo_path:
    :param dbt_target_name:
    :param dbt_bin_path:
    :param slack_conn_id:
    """

    def __init__(
        self, dbt_repo_path: str, dbt_target_name: str, dbt_bin_path: str, **kwargs
    ):
        # dbt paths
        self.dbt_repo_path = dbt_repo_path
        self.dbt_target_name = dbt_target_name
        self.dbt_bin_path = dbt_bin_path
        self.dag = EACustomDAG(**kwargs)

    def dbt_snapshot_run(self):
        dbt_task.override(task_id="dbt_snapshot")(
            "snapshot", project_dir=self.dbt_repo_path, target=self.dbt_target_name
        )
