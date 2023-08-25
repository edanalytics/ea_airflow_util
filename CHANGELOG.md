# ea_airflow_util v0.2.1
## New features
- Add `SFTPToSnowflakeDag` for copying files from SFTP to Snowflake


# ea_airflow_util v0.2.0
## New features
- Refactor `AWSParamStoreToAirflowDAG` to use (key, secret, url) standard for saving Airflow credentials
- Add optional Airflow variable check at start of `RunDbtDag` to only trigger DAG if variable is truthy, and to reset the variable after each run

## Under the hood
- Turn off `airflow_dbt` deprecation-warnings that clog scheduler logs
- Refactor `RunDbtDag` to include a DBT task-group
