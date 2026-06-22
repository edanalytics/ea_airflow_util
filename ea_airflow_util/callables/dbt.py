import hashlib
import logging
import pathlib

from airflow.models import Variable


def _compute_package_lock_hash(dbt_repo_path: str) -> str:
    lock_file = pathlib.Path(dbt_repo_path) / 'package-lock.yml'
    return hashlib.md5(lock_file.read_bytes()).hexdigest()


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
        logging.info("Full refresh forced via config. Skipping hash check.")
        return full_refresh_task_id

    # full_refresh_schedule is a weekday integer (0=Mon … 6=Sun), matching datetime.weekday().
    if full_refresh_schedule is not None and datetime.today().weekday() == full_refresh_schedule:
        logging.info(f"Scheduled full refresh day (weekday={full_refresh_schedule}). Skipping hash check.")
        return full_refresh_task_id

    var_key = f'dbt_pkg_lock_hash_{environment}'
    current_hash = _compute_package_lock_hash(dbt_repo_path)

    try:
        stored_hash = Variable.get(var_key)
    except KeyError:
        stored_hash = None

    if current_hash != stored_hash:
        logging.info(f"package-lock.yml changed ({stored_hash!r} → {current_hash!r}). Running full refresh.")
        return full_refresh_task_id

    logging.info(f"package-lock.yml unchanged ({current_hash!r}). Running incrementally.")
    return incremental_task_id


def update_package_lock_hash(dbt_repo_path: str, environment: str, **context):
    """Store current package-lock.yml hash after a successful run."""
    var_key = f'dbt_pkg_lock_hash_{environment}'
    current_hash = _compute_package_lock_hash(dbt_repo_path)
    Variable.set(var_key, current_hash)
    logging.info(f"Updated {var_key} to {current_hash!r}.")
