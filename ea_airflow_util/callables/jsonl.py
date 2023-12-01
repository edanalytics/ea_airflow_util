import csv
import json
import logging
import os

from typing import Iterator, Optional

from airflow.exceptions import AirflowException, AirflowSkipException

from ea_airflow_util.callables import snake_case


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

        for json_record in json_records:

            # Augment the data with external metadata if specified.
            if metadata_dict is not None:
                json_record.update(metadata_dict)

            # Force the fields to snake_case if specified.
            if to_snake_case:
                json_record = snake_case.record_to_snake_case(json_record)

            writer.write( json.dumps(json_record) + '\n' )

            total_records += 1

    if total_records == 0:
        raise AirflowSkipException(
            f"No data found to write to `{output_path}`!"
        )
    else:
        logging.info(
            f"{total_records} JSON lines written to local path `{output_path}`."
        )

def translate_csv_file_to_jsonl(
    csv_path   : str = None,
    output_path: str = None,
    delete_csv : bool = False,
    metadata_dict: Optional[dict] = None,
    to_snake_case: bool = False,
    **kwargs
):
    """
    Main for transforming CSV data to JSON lines (for consistency downstream).
    Records are collected from the CSV as a generator and written one-by-one to the output file.
    Optional **kwargs for `serialize_json_records_to_disk` can be specified.
    If `delete_csv is True`, delete the CSV source path.
    """

    # note: csv path was initially intended to be a full path to a file
    # it can now be a directory in case there are multiple files

    # get local path from xcom
    if csv_path is None:
        csv_path = kwargs.get('templates_dict').get('local_path')

    # error if csv path is a directory but output path is not null (the loop will overwrite)
    if os.path.isdir(csv_path) and output_path is not None:
        raise AirflowException(
            f"Trying to write multiple files to one output file. You will overwrite your output"
        )

    # loop and find all the files
    for root, _, files in os.walk(csv_path):
        # loop over the files in this directory (since there could potentially be multiple)
        for file in files:

            # create full local name
            full_local_path = os.path.join(root, file)

            # create output path
            if output_path is None:
                output_path_new = full_local_path.replace('.csv', '.jsonl')
            else:
                output_path_new = output_path

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
                logging.info(
                    f"Local path `{full_local_path}` deleted."
                )

    # this is slightly awkward bc the output path is being created within the above loop
    # but technically if the csv path ends with a file and is not a directory, there will only be one output path anyway
    if csv_path.endswith('.csv'):
        return output_path
    else:
        return csv_path
