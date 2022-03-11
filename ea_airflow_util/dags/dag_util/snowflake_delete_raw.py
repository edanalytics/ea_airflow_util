import logging

from airflow.exceptions import AirflowSkipException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from edfi_api.providers.util import is_full_refresh
from edfi_api.util import get_deletes_name


def delete_all_raw(
        snowflake_conn_id,
        tenant_code,
        api_year,
        database,
        schema,
        resource,
        deletes=False,
        **kwargs
):
    # Full-replace must be explicitly specified to run.
    full_refresh = is_full_refresh(kwargs)

    # TODO: Find a way to make full-refresh mandatory if EdFi 2.
    if not full_refresh:
        raise AirflowSkipException

    #
    if deletes:
        resource = get_deletes_name(resource)

    full_table_path = f"{database}.{schema}.{resource}"

    logging.info(
        f"Deleting all data in `{full_table_path}`."
    )

    #
    snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    snowflake_conn = snowflake_hook.get_conn()

    delete_query = f"""
        delete from {full_table_path}
        where tenant_code = '{tenant_code}'
        and api_year = '{api_year}'
    """
    with snowflake_conn.cursor() as cur:
        cur.execute(delete_query)

    logging.info(
        f"Delete from `{full_table_path}` successful."
    )
