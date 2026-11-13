from airflow.sdk import task
from airflow.sdk.exceptions import AirflowFailException


@task
def dbt_task(
    cmd: str | list[str],
    project_dir: str,
    profiles_dir: str,
    target: str,
    select: str | list[str] = "*",
    vars: dict | None = None,
) -> None:
    from dbt.cli.main import dbtRunner, dbtRunnerResult

    if isinstance(cmd, str):
        cmd = [cmd]

    # dbt_kwargs = dbt_kwargs or {}

    runner = dbtRunner()
    result: dbtRunnerResult = runner.invoke(
        cmd,
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        target=target,
        select=select,
        vars=vars or {},
    )

    if not result.success:
        # dbtRunnerResult.exception is populated only for unhandled exceptions,
        # and is None otherwise.
        raise AirflowFailException() from result.exception
