# ea_airflow_util v0.2.6
## Fixes
- All DAGs use `EACustomDAG` to standardize initialization.

# ea_airflow_util v0.2.5
## New features
- Add a dag generator for cleaning up the Airflow database
- Make header case handling optional in `snowflake_to_disk`

# ea_airflow_util v0.2.4
## Fixes
- Fix `s3_dir_to_postgres` utility function

# ea_airflow_util v0.2.3
## New features
- Add `s3_dir_to_postgres` utility function

# ea_airflow_util v0.2.2
## New features
- Add `snowflake_to_disk` utility function

# ea_airflow_util v0.2.1
## New features
- Add `LoopS3FileTransformOperator` and `S3ToSnowflakeDag` for copying files from S3 to Snowflake
- Add `SFTPToSnowflakeDag` for copying files from SFTP to Snowflake

# ea_airflow_util v0.2.0
## New features
- Refactor `AWSParamStoreToAirflowDAG` to use (key, secret, url) standard for saving Airflow credentials
- Add optional Airflow variable check at start of `RunDbtDag` to only trigger DAG if variable is truthy, and to reset the variable after each run

## Under the hood
- Turn off `airflow_dbt` deprecation-warnings that clog scheduler logs
- Refactor `RunDbtDag` to include a DBT task-group
