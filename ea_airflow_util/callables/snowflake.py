import csv
import logging
import pathlib

from typing import Optional

from airflow.models.connection import Connection
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector import DictCursor

from ea_airflow_util.callables import s3
from ea_airflow_util.callables import slack


def snowflake_to_disk(
    snowflake_conn_id: str,
    query: str,
    local_path: str,
    sep: str = ',',
    quote_char: str = '"',
    chunk_size: int = 1000,
    **context
):
    hook = SnowflakeHook(snowflake_conn_id)
    conn = hook.get_conn()

    with open(local_path, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=sep,
                                quotechar=quote_char, quoting=csv.QUOTE_MINIMAL)
        # fetch and write header
        meta = conn.cursor().describe(query)
        header = [x[0].lower() for x in meta]
        csv_writer.writerow(header)

        # run query, chunked
        cur = conn.cursor().execute(query)
        while True:
            if not (res := cur.fetchmany(chunk_size)):
                break

            for row in res:
                csv_writer.writerow(row)

    conn.close()
    return local_path


### Imported from Rally
def _run_table_clear_query(
    snowflake_conn: Connection,
    dest_table: str,
    truncate: bool = False,
    delete_source_orgs: Optional[set] = None,
):
    """
    Isolated logic for truncating or deleting from a table.
    """
    if truncate and delete_source_orgs:
        raise ValueError(f'!!! Only specify one of (truncate, delete) during Snowflake import to `{dest_table}`!')

    if truncate and not delete_source_orgs:
        logging.info(f'Truncating table `{dest_table}`')
        with snowflake_conn.cursor() as cur:
            cur.execute(f"truncate {dest_table};")

    elif delete_source_orgs and not truncate:
        # create comma-seperated string of source org set
        delete_source_orgs = "('" + "','".join(delete_source_orgs) + "')"
        logging.info(f'Deleting {delete_source_orgs} from `{dest_table}`')

        with snowflake_conn.cursor() as cur:
            delete_qry = f"""
                delete from {dest_table} 
                where source_org in {delete_source_orgs}
            """
            cur.execute(delete_qry)


def _run_table_import_query(
    # S3 parameters
    s3_hook: S3Hook,
    s3_key: str,

    # Postgres parameters
    snowflake_conn: Connection,
    dest_table: str,
    stage: str,
    column_customization: str,
    column_customization_dtype: str,
    metadata: str,
    file_format: str,

    # Meta-parameters
    slack_on_failure: bool,
    row_hash: bool,
    **context
):
    """
    Isolated logic for completing an import of data from S3 to Postgres.
    """
    # Collect the credentials to pass to the copy query.
    # (This is completed here to avoid passing sensitive information between functions.)
    s3_creds = s3_hook.get_connection(s3_hook.aws_conn_id)

    # TODO: the key in this method is a single file, not a directory.
    # could consider splitting into multiple files in disk to s3
    # then use pattern recognition in copy statements (non-row hashing)
    # to take advantage of parallel operations (see below)
    # https://docs.snowflake.com/en/user-guide/data-load-considerations-load.html#options-for-selecting-staged-data-files

    # creating various column strings for the complicated merge
    if file_format == 'json_default':
        # TODO: is source org always the directory before the file itself
        select_cols_str = ", ".join(
            f"${num}::variant {name}"
            for num, name in enumerate(column_customization.split(","), start=1)
        )

    else:
        # TODO: is source org always the directory before the file itself
        select_cols_str = ", ".join(
            f"${num}::{column_customization_dtype} {name}"
            for num, name in enumerate(column_customization.split(","), start=1)
        )

    select_cols_str += ", " + (
        metadata
            .replace('source_org', "split_part(metadata$filename, '/', -2) as source_org")
            .replace('file_path', 'metadata$filename as file_path')
    )

    # add a forward slash to the end of the s3_key in order to properly find files
    if not s3_key.endswith(('csv', '.gz', 'jsonl')):
        s3_key = s3_key + '/'

    # pull out the raw db from the dest table to use correct db for external stage (s3)
    raw_db = dest_table.split('.', 1)[0]

    # Build a copy query SQL string (logic differs if row hashing is used).
    if row_hash:
        # todo: will this need to be split(", ") ((include a space?))
        hashed_cols_str = ", ".join(
            f"${num}::{column_customization_dtype}"
            for num, name in enumerate(column_customization.split(","), start=1)
        )

        # more necessary column strings because nothing can be easy
        insert_cols_string = ", ".join([column_customization, metadata, 'row_md5_hash'])

        values_cols_str = "{}, {}, source.row_md5_hash".format(
            ", ".join(f"source.{col}" for col in column_customization.split(",")),
            ", ".join(f"source.{col}" for col in metadata.split(","))
        )

        # todo: change the external path?
        copy_query = f"""
            merge into {dest_table} raw
            using (
                select
                    {select_cols_str},
                    md5(array_to_string(array_construct({hashed_cols_str}), ',')) as row_md5_hash
                from @{raw_db}.public.{stage}/{s3_key} (file_format => {file_format})
            ) source
            on raw.row_md5_hash = source.row_md5_hash
            when not matched then
                insert (
                    {insert_cols_string}
                ) values (
                    {values_cols_str}
                );
        """

    else:
        copy_query = f"""
            copy into {dest_table} ({column_customization}, {metadata})
            from (
                select {select_cols_str}
                from @{raw_db}.public.{stage}/{s3_key} (file_format => {file_format})
            )
                force = true
                on_error = skip_file;
        """

    # Perform the actual execution of the copy query.
    logging.info(f'Beginning insert to `{dest_table}`')
    logging.info(f'Using query: `{copy_query}`')
    with snowflake_conn.cursor(DictCursor) as cur:
        try:
            cur.execute(copy_query)
            return_value = cur.rowcount if row_hash else cur.fetchone()
            # logging if successful
            if row_hash:
                logging.info(f'''Number of rows inserted to `{dest_table}`: {return_value}''')
                # TODO: this will offer the same info as the previous line
                # ^ want to capture any errors for files
                logging.info(cur.fetchone())
            else:
                # TODO: capture errors for individual files (will not fully error bc we set on error skip file)
                logging.info(return_value)
        except Exception as e:
            logging.error(e)

            if slack_on_failure:
                slack.slack_alert_insert_failure(
                    context=context, http_conn_id="TODO",
                    file_key=s3_key, dest_table=dest_table, error=str(e).splitlines()[0]
                )

    snowflake_conn.commit()


def import_s3_to_snowflake(
    # S3 parameters
    s3_conn_id: str,
    s3_bucket: str,
    s3_key: str,

    # Postgres parameters
    snowflake_conn_id: str,
    dest_table: str,
    stage: str,
    column_customization: str = None,
    column_customization_dtype: str = "string",

    # Table clear parameters
    truncate: bool = False,
    delete: bool = False,
    metadata: str = None,

    # Meta-parameters
    slack_on_failure: bool = True,
    row_hash: bool = False,
    **context
):
    # Establish connections to S3 and to Postgres.
    s3_hook = S3Hook(s3_conn_id)
    hook = SnowflakeHook(snowflake_conn_id)
    snowflake_conn = hook.get_conn()

    s3_subkeys = s3._list_s3_keys(s3_hook, s3_bucket, s3_key)

    # output all the keys to be loaded
    # each on a newline
    keys_output = '\n'.join([
        f"{key}" for key in s3_subkeys
    ])
    logging.info(f'Attempting to load these files: {keys_output}')

    # check what type of file (this will determine file format for snowflake loading)
    # TODO: should I check across all of the files? Not really necessary
    file_format = None
    if s3_subkeys[0].endswith(('.csv', '.gz')):
        file_format = 'csv_enclosed'
    if s3_subkeys[0].endswith('.jsonl'):
        file_format = 'json_default'

    delete_source_orgs = None
    if delete:
        # Extract the source_org from the S3 folder structure, and apply to SQL queries if necessary.
        delete_source_orgs = set([pathlib.PurePath(key).parent.name for key in s3_subkeys])

        logging.info(f'Deleting source_orgs = {delete_source_orgs}')

    # Apply table truncations or deletes if specified.
    _run_table_clear_query(
        snowflake_conn,
        dest_table,
        truncate=truncate,
        delete_source_orgs=delete_source_orgs
    )

    _run_table_import_query(
        s3_hook=s3_hook,
        s3_key=s3_key,

        snowflake_conn=snowflake_conn,
        dest_table=dest_table,
        stage=stage,
        column_customization=column_customization,
        column_customization_dtype=column_customization_dtype,
        metadata=metadata,
        file_format=file_format,

        slack_on_failure=slack_on_failure,
        row_hash=row_hash,
        **context
    )
