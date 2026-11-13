from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import TaskGroup
from providers.ea_dbt.operators.ea_dbt import dbt_task

from ea_airflow_util.dags.ea_custom_dag import EACustomDAG


class RunDbtDag:
    """
    :param environment:
    :param dbt_repo_path:
    :param dbt_target_name:
    :param full_refresh: -- default to False
    :param full_refresh_schedule: -- default to None
    :param opt_swap: -- default to False
    :param opt_dest_schema: -- default to None
    :param opt_swap_target: -- default to opt_dest_schema

    """

    def __init__(
        self,
        environment: str,
        dbt_repo_path: str,
        dbt_target_name: str,
        full_refresh: bool = False,
        seed_vars: dict | None = None,
        run_vars: dict | None = None,
        test_vars: dict | None = None,
        upload_artifacts: bool = False,
        trigger_dags_on_run_success: list | None = None,
        **kwargs,
    ):

        self.environment = environment

        # dbt paths
        self.dbt_repo_path = dbt_repo_path
        self.dbt_target_name = dbt_target_name

        self.full_refresh = full_refresh

        # run-time vars
        self.seed_vars = seed_vars
        self.run_vars = run_vars
        self.test_vars = test_vars

        # DBT Artifacts
        self.upload_artifacts = upload_artifacts

        self.dag = EACustomDAG(
            params=self.params_dict,
            user_defined_macros={
                "environment": self.environment,
            },
            **kwargs,
        )

        # Build optional operator to trigger downstream DAG when `dbt run` succeeds.
        if trigger_dags_on_run_success:
            self.external_dags = []

            for external_dag_id in trigger_dags_on_run_success:
                self.external_dags.append(
                    TriggerDagRunOperator(
                        task_id=f"trigger_{external_dag_id}",
                        trigger_dag_id=external_dag_id,
                        wait_for_completion=False,  # Keep running DBT DAG while downstream DAG runs.
                        trigger_rule="all_success",
                    )
                )
        else:
            self.external_dags = None

    def build_dbt_run(self, **kwargs):
        self.full_refresh = self.full_refresh or kwargs.get("dag_run", {}).get(
            "conf", {}
        ).get("full_refresh", False)

        with TaskGroup(
            group_id="RunDBT", prefix_group_id=False, parent_group=None, dag=self.dag
        ):
            dbt_base_task = dbt_task.partial(
                project_dir=self.dbt_repo_path,
                profiles_dir="/home/airflow/.dbt",
                target=self.dbt_target_name,
            )

            def tid(cmd):
                return f"dbt_{cmd}_{self.environment}"

            dbt_debug = dbt_base_task.override(task_id=tid("debug"))("debug")
            dbt_seed = dbt_base_task.override(task_id=tid("seed"))(
                "seed", vars=self.seed_vars
            )
            dbt_run = dbt_base_task.override(task_id=tid("run"))(
                "run", vars=self.run_vars
            )
            dbt_test = dbt_base_task.override(task_id=tid("test"))(
                "test", vars=self.test_vars
            )

            if self.upload_artifacts:
                dbt_artifacts = dbt_base_task.override(
                    task_id=tid("build_artifact_tables")
                )("run", select="package:dbt_artifacts")
                pre_run = dbt_debug >> dbt_artifacts >> dbt_seed
            else:
                pre_run = dbt_debug >> dbt_seed

            pre_run >> dbt_run >> dbt_test

            # Trigger downstream DAG when `dbt run` succeeds
            if self.external_dags:
                dbt_run >> self.external_dags
