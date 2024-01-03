import datetime
import logging
import json
import os
import uuid

from typing import List, Union

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


def type_converter(o):
    if isinstance(o, uuid.UUID):
        return o.__str__()
    elif isinstance(o, datetime.datetime):
        return o.__str__()
    else:
        return o

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
