from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import csv

def snowflake_to_disk(
        snowflake_conn_id,
        query,
        local_path,
        sep=',',
        quote_char='"',
        lower_header=True,
        chunk_size=1000,
        **context
        ):
    hook = SnowflakeHook(snowflake_conn_id)
    conn = hook.get_conn()

    with open(local_path, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=sep, 
                                quotechar=quote_char, quoting=csv.QUOTE_MINIMAL)
        # fetch and write header
        meta = conn.cursor().describe(query)
        header = [x[0] for x in meta]
        if lower_header:
            header = [x.lower() for x in header]
        csv_writer.writerow(header)

        # run query, chunked
        cur = conn.cursor().execute(query)
        while True:
            res = cur.fetchmany(chunk_size)
            if len(res) == 0:
                break
            for row in res:
                csv_writer.writerow(row)
    conn.close()

    return(local_path)
