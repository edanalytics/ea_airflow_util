from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
from airflow.exceptions import AirflowSkipException, AirflowException
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def s3_import(
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
