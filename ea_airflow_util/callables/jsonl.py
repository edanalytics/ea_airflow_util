#!/usr/bin/env python3

'''
Note: You'll need to make this file executable in whatever environment you use it in. Run this in bash 
`chmod +x /home/airflow/airflow/dags/util/jsonl_executable.py`
'''


import sys
import os
import csv
import logging
import json

from airflow.exceptions import AirflowException, AirflowSkipException
from typing import Optional, Iterator

from ea_airflow_util.callables import casing

def serialize_json_records_to_disk(
    json_records : Iterator[dict],
    output_path  : str,
    mode         : str = "w",
    metadata_dict: Optional[dict] = None,
    to_snake_case: bool = False,
    **kwargs
):
    """
    Write an iterator of dictionaries to an output path as JSON lines.
    The write mode defaults to w(rite).
    `metadata_dict` is optional additional fields to be injected into each record upon write.
    If `to_snake_case is True`, force the record field names to snake_case.
    """
    # Create the directory structure if missing.
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Write to the output path record-by-record.
    total_records = 0

    with open(output_path, mode) as writer:
        
        json_records = list(json_records)
        logging.info(f"Number of records parsed from CSV: {len(json_records)}")
        logging.info(f"First record: {json_records[0] if json_records else 'None'}")

        for json_record in json_records:

            # Augment the data with external metadata if specified.
            if metadata_dict is not None:
                json_record.update(metadata_dict)

            # Force the fields to snake_case if specified.
            if to_snake_case:
                json_record = casing.record_to_snake_case(json_record)

            writer.write( json.dumps(json_record) + '\n' )
            total_records += 1
        logging.info(f"Current Record: {total_records}")

    if total_records == 0:
        raise AirflowSkipException(
            f"No data found to write to `{output_path}`!"
        )
    else:
        logging.info(
            f"{total_records} JSON lines written to local path `{output_path}`."
        )

def translate_csv_file_to_jsonl(
    local_path: str,
    output_path: str,
    delete_csv: bool = False,
    metadata_dict: Optional[dict] = None,
    to_snake_case: bool = False,
    **kwargs
):  
    logging.info(f"Local Path Used: {local_path}")
    logging.info(f"Output Path Used: {output_path}")

    if os.path.isdir(local_path) and output_path is not None:
        raise AirflowException("Trying to write multiple files to one output file.")

    if os.path.isfile(local_path):
        files = [os.path.basename(local_path)]
        root = os.path.dirname(local_path)
        logging.info(f"root: {root}, files: {files}")
    else:
        for root, _, files in os.walk(local_path):
            logging.info(f"root: {root}, files: {files}")
    for file in files:
        full_local_path = os.path.join(root, file)
        output_path_new = output_path or full_local_path.replace('.csv', '.jsonl')
        logging.info(f"Full Local Path within for loop: {full_local_path}")

        try:
            with open(full_local_path, 'r') as reader:
                json_records = csv.DictReader(reader)
                serialize_json_records_to_disk(json_records, output_path_new, "w", metadata_dict, to_snake_case, **kwargs)
        except UnicodeDecodeError:
            with open(full_local_path, 'r', encoding='latin1', errors='backslashreplace') as reader:
                json_records = csv.DictReader(reader)
                serialize_json_records_to_disk(json_records, output_path_new, "w", metadata_dict, to_snake_case, **kwargs)

        if delete_csv:
            os.remove(full_local_path)
            logging.info(f"Deleted {full_local_path}")

if __name__ == "__main__":
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    translate_csv_file_to_jsonl(input_path, output_path)