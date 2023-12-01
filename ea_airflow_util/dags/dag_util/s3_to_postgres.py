import logging

from airflow.exceptions import AirflowSkipException, AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

def _list_s3_keys(
    s3_hook: S3Hook,
    s3_bucket: str,
    s3_key: str,
):
    """
    List the keys at a specified S3 location, then filter out directories.
    """
    subkeys = s3_hook.list_keys(s3_bucket, s3_key)

    if subkeys is None or len(subkeys) == 0:
        raise AirflowSkipException

    # Remove directories and return.
    return [subkey for subkey in subkeys if not subkey.endswith("/")]


def s3_to_postgres(
    pg_conn_id,
    s3_conn_id,
    dest_table,
    column_customization,
    options,
    s3_key,
    s3_region,
    truncate=False,
    delete_qry=None,
    metadata_qry=None,
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
    pg_conn_id,
    s3_conn_id,
    dest_table,
    column_customization,
    options,
    s3_key,
    s3_region,
    truncate=False,
    delete_s3_dir=False,
    metadata_qry=None,
    **context
):
    s3_hook = S3Hook(s3_conn_id)
    s3_creds = s3_hook.get_connection(s3_hook.aws_conn_id)
    s3_bucket = s3_creds.schema
    s3_keys = _list_s3_keys(s3_hook, s3_bucket, s3_key)

    if truncate:
        hook = PostgresHook(pg_conn_id)
        conn = hook.get_conn()
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
        except Exception as e:
            logging.error(e)
            failed_count += 1
            continue

    if failed_count == len(s3_keys):
        raise AirflowException('All inserts failed')
    else:
        logging.info(f'Loaded {len(s3_keys) - failed_count} of {len(s3_keys)} keys.')
    
    if delete_s3_dir:
        s3_hook.delete_objects(s3_bucket, s3_keys)
