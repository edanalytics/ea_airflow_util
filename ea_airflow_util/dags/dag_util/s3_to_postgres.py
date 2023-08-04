import logging

from airflow.exceptions import AirflowSkipException, AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.hooks.sql import fetch_one_handler


def s3_import(
    pg_conn_id: str,
    s3_conn_id: str,

    dest_table: str,
    column_customization: str,
    options: str,

    s3_key: str,
    s3_region: str,

    truncate: bool = False,
    delete_qry: str = None,
    metadata_qry: str = None,

    **context
):
    column_customization = column_customization or ''
    options = options or ''
    s3_key = s3_key or context['templates_dict'].get('s3_key')

    # temporary: pull and use creds until IAM role set up
    s3_hook = S3Hook(s3_conn_id)
    s3_creds = s3_hook.get_connection(s3_hook.aws_conn_id)

    # make sure content is > 0 bytes, if not, skip this
    s3_bucket = s3_creds.schema
    if (s3_hook.get_key(key=s3_key, bucket_name=s3_bucket).get()['ContentLength']) == 0:
        logging.info("File at this s3 key is empty.")
        raise AirflowSkipException

    # Make sure only one data-reset query is provided.
    if truncate and delete_qry:
        raise ValueError('Only specify one of truncate, delete_qry')

    # Build optional delete/truncate query followed by insert.
    conn = PostgresHook(pg_conn_id).get_conn()
    queries_to_run = []

    if truncate:
        logging.info('Truncating table')
        queries_to_run.append(f"truncate table {dest_table};")

    if delete_qry:
        logging.info('Deleting from table')
        queries_to_run.append(delete_qry)

    copy_qry = f"""
        select aws_s3.table_import_from_s3(
            '{dest_table}',
            '{column_customization}',
            '{options}',
            aws_commons.create_s3_uri('{s3_bucket}', '{s3_key}', '{s3_region}'),
            aws_commons.create_aws_credentials('{s3_creds.login}', '{s3_creds.password}', '')
        );
    """
    queries_to_run.append(copy_qry)

    logging.info('Beginning insert')
    ret_value = conn.run(
        sql=queries_to_run,
        autocommit=False,
        handler=fetch_one_handler,
        return_last=True
    )

    # Run optional metadata query before returning the fetched insert output.
    if metadata_qry:
        conn.run(metadata_qry.format(s3_key=s3_key))

    logging.info(ret_value)
