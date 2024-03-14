import datetime
import logging
import json
import os
import uuid

from ea_airflow_util.callables import s3

from typing import List, Optional, Union

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


def type_converter(o):
    if isinstance(o, uuid.UUID):
        return o.__str__()
    elif isinstance(o, datetime.datetime):
        return o.__str__()
    else:
        return o

# MySQL
def mssql_to_disk(conn_string: str, tables: Union[str, List[str]], local_path: str, **context):
    """

    """
    # Sanitize input arguments and prepare environment for write.
    if isinstance(tables, str):
        tables = (tables,)

    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    with MsSqlHook(mssql_conn_id=conn_string).get_conn() as conn, open(local_path, "w") as writer:

        # Iterate tables and union to disk.
        for table in tables:
            num_rows = 0

            with conn.cursor(as_dict=True) as cursor:
                cursor.execute(f"SELECT * FROM {table}")

                for json_record in cursor:
                    writer.write(json.dumps(json_record, default=type_converter) + '\n')
                    num_rows += 1

            logging.info(f'Pulled {num_rows} rows from {table} to disk: {local_path}')

    return local_path

# Postgres
def s3_to_postgres(
    pg_conn_id: str,
    s3_conn_id: str,
    dest_table: str,
    column_customization: Optional[str],
    options: str,
    s3_key: str,
    s3_region: str,
    truncate: bool = False,
    delete_qry: Optional[str] = None,
    metadata_qry: Optional[str] = None,
    **context
):
    if column_customization is None:
        column_customization = ''
    if options is None:
        options = ''
    if s3_key is None:
        s3_key = context.get('templates_dict').get('s3_key')

    hook = PostgresHook(pg_conn_id)
    conn = hook.get_conn()

    # temporary: pull and use creds until IAM role set up
    s3_hook = S3Hook(s3_conn_id)
    s3_creds = s3_hook.get_connection(s3_hook.aws_conn_id)
    s3_bucket = s3_creds.schema

    # make sure content is > 0 bytes, if not, skip this
    if (s3_hook.get_key(key=s3_key, bucket_name=s3_bucket).get()['ContentLength']) == 0:
        print("File at this s3 key is empty.")
        raise AirflowSkipException

    if truncate and not delete_qry:
        with conn.cursor() as cur:
            logging.info('Truncating table')
            cur.execute(f'truncate table {dest_table};')
    elif not truncate and delete_qry:
        with conn.cursor() as cur:
            logging.info('Deleting from table')
            cur.execute(delete_qry)
    elif truncate and delete_qry:
        raise ValueError('Only specify one of truncate, delete_qry')

    copy_qry = f"""
        select aws_s3.table_import_from_s3(
        '{dest_table}',
        '{column_customization}',
        '{options}',
        aws_commons.create_s3_uri('{s3_bucket}', '{s3_key}', '{s3_region}'),
        aws_commons.create_aws_credentials('{s3_creds.login}', '{s3_creds.password}', '')
        );
    """

    logging.info('Beginning insert')
    with conn.cursor() as cur:
        cur.execute(copy_qry)
        ret_value = cur.fetchone()

    if metadata_qry:
        with conn.cursor() as cur:
            cur.execute(metadata_qry.format(s3_key=s3_key))

    conn.commit()
    logging.info(ret_value)


def s3_dir_to_postgres(
    pg_conn_id: str,
    s3_conn_id: str,
    dest_table: str,
    column_customization: Optional[str],
    options: str,
    s3_key: str,
    s3_region: str,
    truncate: bool = False,
    delete_s3_dir: bool = False,
    metadata_qry: Optional[str] = None,
    **context
):
    s3_hook = S3Hook(s3_conn_id)
    s3_creds = s3_hook.get_connection(s3_hook.aws_conn_id)
    s3_bucket = s3_creds.schema
    s3_keys = s3.list_s3_keys(s3_hook, s3_bucket, s3_key)

    if truncate:
        conn = PostgresHook(pg_conn_id).get_conn()
        with conn.cursor() as cur:
            logging.info('Truncating table')
            cur.execute(f'truncate table {dest_table};')
            conn.commit()

    failed_count = 0
    for key in s3_keys:
        logging.info(f'Loading key: {key}')
        try:
            s3_to_postgres(
                pg_conn_id=pg_conn_id,
                s3_conn_id=s3_conn_id,
                dest_table=dest_table,
                column_customization=column_customization,
                options=options,
                s3_key=key,
                s3_region=s3_region,
                truncate=False,
                delete_qry=None,
                metadata_qry=metadata_qry
            )
        except Exception as err:
            logging.error(err)
            failed_count += 1
            continue

    if failed_count == len(s3_keys):
        raise AirflowException('All inserts failed')

    logging.info(f'Loaded {len(s3_keys) - failed_count} of {len(s3_keys)} keys.')

    if delete_s3_dir:
        s3_hook.delete_objects(s3_bucket, s3_keys)