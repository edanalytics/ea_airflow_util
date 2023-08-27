# Overview
`ea_airflow_util` contains additional Airflow functionality used within EDU that falls outside the scope of `edfi_airflow`


## RunDbtDag
`RunDbtDag` is an Airflow DAG that completes a full DBT run with optional post-run behavior.
Seed tables are fully refreshed, all models are run, and all tests are tested.
This emulates the behavior of a `dbt build` call, but with more control over parameters and failure states.

If all tests succeed, schemas are optionally swapped (e.g. from `rc` to `prod`).
Additionally, DBT artifacts are optionally uploaded using the [Brooklyn Data dbt_artifacts](https://github.com/brooklyn-data/dbt_artifacts) `upload_dbt_artifacts_v2` operation.

<details>
<summary>Arguments:</summary>

-----

| Argument              | Description                                                                                            |
|-----------------------|--------------------------------------------------------------------------------------------------------|
| environment           | environment name for the DAG label                                                                     |
| dbt_repo_path         | path to the project `/dbt` folder                                                                      |
| dbt_target_name       | name of the DBT target to select                                                                       |
| dbt_bin_path          | path to the environment `/dbt` folder                                                                  |
| full_refresh          | boolean flag for whether to apply the `--full-refresh` flag to incremental models (default `False`)    |
| full_refresh_schedule | Cron schedule for when to automatically kick off a full refresh run                                    |
| opt_dest_schema       | optional destination schema to swap target schema with if `opt_swap=True`                              |
| opt_swap              | boolean flag for whether to swap target schema with `opt_dest_schema` after each run (default `False`) |
| upload_artifacts      | boolean flag for whether to upload DBT artifacts at the end of the run (default `False`)               |
| slack_conn_id         | Slack webhook Airflow connection ID for sending run errors to a Slack channel                          |

Additional DAG arguments (e.g. `default_args`) can be passed as kwargs.

-----

</details>

![RunDbtDag](./images/RunDbtDag.png)



## UpdateDbtDocsDag
`UpdateDbtDocsDag` is an Airflow DAG that generates the three [DBT docs](https://docs.getdbt.com/reference/commands/cmd-docs) metadata files and uploads them to a bucket on AWS S3.
If an AWS Cloudfront instance is pointed to this S3 bucket, a static website is built that is identical to the one generated by `dbt docs generate`.

<details>
<summary>Arguments:</summary>

-----

| Argument            | Description                                                                                       |
|---------------------|---------------------------------------------------------------------------------------------------|
| dbt_repo_path       | path to the project `/dbt` folder                                                                 |
| dbt_target_name     | name of the DBT target to select                                                                  |
| dbt_bin_path        | path to the environment `/dbt` folder                                                             |
| dbt_docs_s3_conn_id | S3 Airflow connection ID where S3 bucket to upload DBT documentations files is defined in `schema` |

Additional DAG arguments (e.g. `default_args`) can be passed as kwargs.

-----

</details>

![UpdateDbtDocsDag](./images/UpdateDbtDocsDag.png)



## AWSParamStoreToAirflowDAG
The Cloud Engineering and Integration team saves Ed-Fi ODS credentials as parameters in AWS Systems Manager Parameter Store.
Each Stadium implementation has a shared SSM-prefix, which is further delineated by tenant-code and/or API year.
There are three parameters associated with each ODS-connection:
```text
{SSM_PREFIX}/{TENANT_CODE}/key
{SSM_PREFIX}/{TENANT_CODE}/secret
{SSM_PREFIX}/{TENANT_CODE}/url
```

<details>
<summary>Arguments:</summary>

-----

| Argument            | Description                                                                                                     |
|---------------------|-----------------------------------------------------------------------------------------------------------------|
| region_name         | AWS region where parameters are stored                                                                          |
| connection_mapping  | Optional one-to-one mapping between Parameter Store prefixes and ODS credentials                                |
| prefix_year_mapping | Optional mapping between a shared SSM-prefix and a given Ed-Fi year for dynamic connections                     |
| tenant_mapping      | Optional mapping between tenant-code name in Parameter Store and its identity in Stadium in dynamic connections |
| join_numbers        | Optional boolean flag to strip underscores between district and number in dynamic connections (default `True`)  |

Additional DAG arguments (e.g. `default_args`) can be passed as kwargs.

There are three types of mappings that can be defined in the Parameter Store DAG.
Arguments `connection_mapping` and `prefix_year_mapping` are mutually-exclusive.
Argument `tenant_mapping` is optional, and is only applied if `prefix_year_mapping` is defined.

In Stadium implementations with fewer tenants, it is suggested to manually map the `{SSM_PREFIX}/{TENANT_CODE}` strings to their Ed-Fi connection name in Airflow using `connection_mapping`.
For example:
```python
connection_mapping = {
    '/startingblocks/api/2122/sc-state': 'edfi_scde_2022',
    '/startingblocks/api/2223/sc-state': 'edfi_scde_2023',
    '/startingblocks/api/sc/state-2324': 'edfi_scde_2024',
}
```

In Stadium implementations with many tenants, an explicit one-to-one mapping between prefixes and connections may be untenable.
In cases like these, the `prefix_year_mapping` argument maps shared SSM-prefixes to API years and dynamically builds Airflow credentials.
For example:
```python
prefix_year_mapping = {
    '/startingblocks/api/districts-2122': 2022,
    '/startingblocks/api/sc/districts-2223': 2023,
}
```

Connection pieces between the prefixes and `url`, `key`, and `secret` are assumed to be tenant-codes, and connections are built dynamically.
Some standardization is always applied to inferred tenant-codes: spaces and dashes are converted to underscores.

However, in the case that the dynamically-inferred tenant-code does not match its identity in Stadium, the `tenant_mapping` can be used to force a match.
For example:
```python
tenant_mapping = {
    'fortmill': 'fort_mill',
    'york-4'  : 'fort_mill',
}
```

Using the example `prefix_year_mapping` and `tenant_mapping` defined above on the following Parameter Store keys will create a single Airflow connection: `edfi_fort_mill_2023`.
```text
/startingblocks/api/sc/districts-2223/fortmill/url
/startingblocks/api/sc/districts-2223/fortmill/key
/startingblocks/api/sc/districts-2223/fortmill/secret
```

Finally, there is an optional boolean argument `join_numbers` that is turned on by default.
When true, dynamically-inferred tenant-codes are standardized further to remove underscores between district name and code.
For example, `york_1` becomes `york1`.

-----

</details>



## Slack Callbacks
This package also contains several callback functions which can be used with Slack webhooks to alert at task failures or successes, or when SLAs are missed.
Each function takes the Slack Airflow connection ID as their primary argument.
The contents of the callback messages are filled automatically via the DAG run context.

Airflow callbacks only accept expected arguments, not kwargs.
Because these custom Slack callback functions expect the additional argument `http_conn_id`, this argument must be filled before applying the callbacks to the DAG.
This can be done using the `functools.partial()` function, as follows:
```python
from functools import partial

on_failure_callback = partial(slack_alert_failure , http_conn_id=HTTP_CONN_ID)
on_success_callback = partial(slack_alert_success , http_conn_id=HTTP_CONN_ID)
sla_miss_callback   = partial(slack_alert_sla_miss, http_conn_id=HTTP_CONN_ID)
```

There are three Slack callbacks currently included in this package:

### slack_alert_failure
>🔴 Task Failed.  
**Task**: {task_id}  
**Dag**: {dag_id}  
**Execution Time**: {logical_date}  
**Log Url**: {log_url}  

### slack_alert_success
>✔ Task Succeeded.  
**Task**: {task_id}  
**Dag**: {dag_id}  
**Execution Time**: {logical_date}  
**Log Url**: {log_url}  

### slack_alert_sla_miss
>🆘 **SLA has been missed.**  
**Task**: {task_id}  
**Dag**: {dag_id}  
**Execution Time**: {logical_date}  

Note, due to different definitions of task-failure/success callbacks and SLA callbacks, `Log Url` is unavailable in SLA callback messages.
This will be investigated further and patched in a future update.
