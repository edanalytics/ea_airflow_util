# ea_airflow_util v0.3.7
## Fixes
- Fix change in interface in `SlackWebhookHook` instantiation in Slack callables.


# ea_airflow_util v0.3.6
## New Features
- Add `recursive` flag to `sharefile_to_disk()` callable (default `True`). When set to `False`, only top-level files are copied using an alternative API method.

## Under the hood
- Add pagination to `SharefileHook._find_items()` and downstream-dependent methods.
- Use `SharefileHook` helper method in `sharefile_to_disk()` callable to reduce API calls to map filepaths to internal API IDs.

## Fixes
- Fix bug where full-refresh DAG config is always set to true in `RunDbtDag`.
- Fix bug in `S3ToSnowflakeOperator` where optional arguments being undefined resulted in malformed SQL statements.


# ea_airflow_util v0.3.5
## New features
- Add optional `most_recent_file` flag to `SharefileToDiskOperator` to extract the most recent version of a singleton file from a path.

## Under the hood
- Log a warning message when `SharefileHook.folder_id_from_path()` finds no files for a given path.
- Change logic in callable `sql.s3_dir_to_postgres()` to raise an AirflowException if any copy of S3 key fails, instead of only when all fail.


# ea_airflow_util v0.3.4
## Fixes
- Handle duplicate search results in sharefile.

# ea_airflow_util v0.3.3
## Under the hood
- Add `dest_filename` argument to `s3_to_sharefile` callable to optionally override filename
- Add `LoadSharefileCustomUsersDag` to top-level package import path

## Fixes
- Run DAG setup method during `LoadSharefileCustomUsersDag` initialization


# ea_airflow_util v0.3.2
## New features
- `AWSParamStoreToAirflowDAG` allows more flexibility when passing Parameter Store paths. Use `{tenant_code}` when the tenant is in the middle of the path, instead of the end.
- Add `s3_to_sharefile` and `disk_to_sharefile` callables
- Add methods to the `SharefileHook`
- Add ShareFile callable `check_for_new_files()` to assert expectations in ShareFile directory
- Add `S3ToSnowflakeOperator` to S3 operators
- Add `LoadSharefileCustomUsersDag` to automate Heimdall user creation from uploaded authenticated users files in ShareFile

## Under the hood
- Code and error-handling improved in callable `sharefile_to_disk`. Arguments `ds_nodash` and `ts_nodash` are deprecated.
- Refactor `SFTPToSnowflakeDAG` and `S3ToSnowflakeDAG` to use new `S3ToSnowflakeOperator`
- Update callable `ftp.download_all` to accept either a remote directory or file.


# ea_airflow_util v0.3.1
## New features
- Boolean argument `is_manual_upload` in `S3ToSnowflakeDag` rearranges S3 source pathing to easier structure for partners

## Under the hood
- Copy statement in `S3ToSnowflakeDag` uses regex instead of string-splitting to infer pull-date and pull-timestamp

## Fixes
- Fix bug in `EACustomDAG` where `default_args` were not passed to DAG super init.


# ea_airflow_util v0.3.0
## New features
- Migrate FTP, ShareFile, casing, and ZIP utilities from Rally into `ea_airflow_util`
- New `EACustomDAG` factory to streamline DAG instantiation moving forward

## Under the hood
- Move all Python callables out from `/airflow/dags/dag_util` into `/airflow/callables`
  - Note that all original imports are still valid and are secretly rerouted in `__init__.py`.
- Overload `callables.airflow.xcom_pull_template` to accept a task ID string or an Airflow `Operator`
- All DAGs use `EACustomDAG` to standardize initialization

# ea_airflow_util v0.2.6
## New features
- Add optional `trigger_dags_on_run_success` argument to `RunDbtDag` to trigger a list of external DAGs upon completion of `dbt run`.

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
