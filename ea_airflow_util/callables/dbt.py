import hashlib
import logging
import pathlib
import subprocess

from airflow.models import Variable


def _compute_fingerprint(dbt_repo_path: str) -> str:
    """
    Combines two signals into a single fingerprint string:
      - MD5 of package-lock.yml       — detects external package version changes
      - git hash of the dbt directory  — detects changes to models, macros, seeds, etc.

    dbt_repo_path is expected to be the dbt subdirectory (e.g. .../stadium_txexchange/dbt),
    so we walk up one level to find the git root and filter the log to that subdirectory.
    """
    dbt_path = pathlib.Path(dbt_repo_path)
    repo_root = str(dbt_path.parent)
    dbt_dir   = dbt_path.name

    lock_hash = hashlib.md5((dbt_path / 'package-lock.yml').read_bytes()).hexdigest()

    git_hash = subprocess.check_output(
        ['git', '-C', repo_root, 'log', '-1', '--format=%H', '--', dbt_dir]
    ).decode().strip()

    return f"{lock_hash}:{git_hash}"


def check_package_lock_hash(
    dbt_repo_path: str,
    environment: str,
    full_refresh_task_id: str,
    incremental_task_id: str,
    force_full_refresh: bool = False,
    full_refresh_schedule: int = None,
    **context,
) -> str:
    """BranchPythonOperator callable. Returns the task_id to execute next."""
    from datetime import datetime

    if force_full_refresh:
        logging.info("Full refresh forced via config. Skipping fingerprint check.")
        return full_refresh_task_id

    # full_refresh_schedule is a weekday integer (0=Mon … 6=Sun), matching datetime.weekday().
    if full_refresh_schedule is not None and datetime.today().weekday() == full_refresh_schedule:
        logging.info(f"Scheduled full refresh day (weekday={full_refresh_schedule}). Skipping fingerprint check.")
        return full_refresh_task_id

    var_key = f'dbt_fingerprint_{environment}'
    current = _compute_fingerprint(dbt_repo_path)

    try:
        stored = Variable.get(var_key)
    except KeyError:
        stored = None

    if current != stored:
        logging.info(f"dbt code changed ({stored!r} → {current!r}). Running full refresh.")
        return full_refresh_task_id

    logging.info(f"dbt code unchanged ({current!r}). Running incrementally.")
    return incremental_task_id


def update_package_lock_hash(dbt_repo_path: str, environment: str, **context):
    """Store current dbt fingerprint after a successful run."""
    var_key = f'dbt_fingerprint_{environment}'
    current = _compute_fingerprint(dbt_repo_path)
    Variable.set(var_key, current)
    logging.info(f"Updated {var_key} to {current!r}.")
