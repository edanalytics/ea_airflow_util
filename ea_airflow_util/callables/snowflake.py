import csv
import logging
import pathlib

from typing import Optional

from airflow.models.connection import Connection
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector import DictCursor

from ea_airflow_util.callables import s3, slack


def snowflake_to_disk(
    snowflake_conn_id: str,
    query: str,
    local_path: str,
    sep: str = ',',
    quote_char: str = '"',
    lower_header: bool = True,
    chunk_size: int = 1000,
    **context
):
    conn = SnowflakeHook(snowflake_conn_id).get_conn()

    with open(local_path, 'w', newline='') as csv_file:
        csv_writer = csv.writer(
            csv_file, delimiter=sep, quotechar=quote_char, quoting=csv.QUOTE_MINIMAL
        )
        # fetch and write header
        meta = conn.cursor().describe(query)
        header = [x[0] for x in meta]
        if lower_header:
            header = [x.lower() for x in header]
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
