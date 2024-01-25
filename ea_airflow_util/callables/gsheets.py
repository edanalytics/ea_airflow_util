import gspread
import logging
import os
import random
import time

from typing import Optional

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.exceptions import AirflowException
from gspread.exceptions import APIError, WorksheetNotFound

from ea_airflow_util.callables import jsonl


#####################################################
# Functions for pulling Google Sheets from the API. #
#####################################################
def get_google_spreadsheet_by_url(
    google_cloud_client: gspread.Client,
    google_sheets_url  : str,
    maximum_backoff_sec: int = 600,
)-> gspread.Spreadsheet:
    """
    Call the Google Sheets API and retrieve a Spreadsheet based on a given URL.
    If API Rate Limit has been reached, use Truncated exponential backoff strategy to retry.
    (See `https://cloud.google.com/storage/docs/exponential-backoff` to learn more.)

    Code inspired by GitLab documentation:
    https://gitlab.com/gitlab-data/analytics/-/blob/master/extract/sheetload/google_sheets_client.py

    Return a gspread Spreadsheet.
    """
    n = 0
    while maximum_backoff_sec > (2 ** n):
        try:
            return google_cloud_client.open_by_url(google_sheets_url)

        except APIError as gspread_error:

            # If we have hit the API too often, wait and try again.
            if gspread_error.response.status_code == 429:

                # Wait a certain number of seconds, determined by TEB and the number of attempts at calling the API.
                wait_sec = (2 ** n) + (random.randint(0, 1000) / 1000)
                logging.info(
                    f"Received API rate limit error. Wait for {wait_sec} seconds before trying again."
                )
                time.sleep(wait_sec)
                n += 1

            # For any other error, raise an Exception.
            else:
                raise AirflowException(
                    "Google API limits have been hit! Wait for an extended period before reattempting Google pulls."
                )

    # If we are waiting too long with no success, give up.
    # JK: How do we want to deal with this? Raise an error? Silently fail?
    else:
        logging.error(f"Max retries exceeded, giving up on `{google_sheets_url}`")


########################################################
# Functions for parsing the pulled Google spreadsheet. #
########################################################

def parse_google_worksheet(
    worksheet   : gspread.Worksheet,
    iter_records: bool,
)-> dict:
    """
    Parse a gspread worksheet and retrieve the relevant data.
    This method is isolated to simplify updates to the data collected.

    Return a dictionary of metadata and records of the worksheet.
    If `iter_records is True`, return the records as an iterator to save memory.
    """
    # Specify whether to return all records in memory at once, or as an iterator.
    if iter_records:
        worksheet_records = iter(worksheet.get_all_records())
    else:
        worksheet_records = worksheet.get_all_records()

    return {
        # Spreadsheet metadata
        "spreadsheet_id"      : worksheet.spreadsheet.id,
        # "spreadsheet_timezone": worksheet.spreadsheet._properties["timeZone"],
        "spreadsheet_title"   : worksheet.spreadsheet.title,
        "spreadsheet_url"     : worksheet.spreadsheet.url,

        # Worksheet metadata
        "worksheet_col_count": worksheet.col_count,
        "worksheet_id"       : worksheet.id,
        "worksheet_index"    : worksheet._properties["index"],
        "worksheet_title"    : worksheet.title,
        "worksheet_url"      : worksheet.url,
        "worksheet_row_count": worksheet.row_count,

        # Worksheet content
        "worksheet_records": worksheet_records,
    }


def get_worksheet_from_google_spreadsheet(
    spreadsheet: gspread.Spreadsheet,
    sheet_index: Optional[int] = None,
    sheet_name : Optional[str] = None,
)-> gspread.Worksheet:
    """
    Parse a Google spreadsheet and return a specific worksheet by index or name.

    If neither is specified, retrieve the zeroth worksheet.
    """
    # If no sheet index or name is specified, default to returning the 0th sheet.
    if sheet_index is None and sheet_name is None:
        sheet_index = 0

    # Verify that both parameters have not been filled. (We may want to change this behavior later.)
    if sheet_index is not None and sheet_name is not None:
        raise KeyError(
            f"Both sheet index `{sheet_index}' and sheet name `{sheet_name}` were specified. Limit to one or the other."
        )

    # Collect the worksheet by index.
    if sheet_index is not None:
        if (worksheet := spreadsheet.get_worksheet(sheet_index)) is not None:
            return worksheet
        else:
            raise KeyError(
                f"No worksheet found at index '{sheet_index}' in spreadsheet '{spreadsheet.title}'!"
            )

    # Collect the worksheet by name.
    if sheet_name is not None:
        try:
            return spreadsheet.worksheet(sheet_name)
        except WorksheetNotFound:
            raise KeyError(
                f"No worksheet found with title '{sheet_name}' in spreadsheet '{spreadsheet.title}'!"
            )


##############################################
# Functions for writing survey data to disk. #
##############################################

def get_and_serialize_google_survey_url_to_jsonl(
    gcp_conn_id: str,
    survey_url : str,
    output_dir: str,
    **kwargs
)-> str:
    """
    Main for retrieving data from the Google surveys config list.

    Records are collected as a generator and written one-by-one to the output file.
    Optional **kwargs for `serialize_json_records_to_disk` can be specified.
    """
    # Instantiate a Google client for retrieving the sheet contents.
    credentials = GoogleBaseHook(gcp_conn_id).get_credentials()
    gcp_client = gspread.authorize(credentials)

    # Retrieve the spreadsheet from the Google API.
    spreadsheet = get_google_spreadsheet_by_url(gcp_client, survey_url)
    logging.info(
        f"Collected survey from url `{survey_url}`."
    )

    # Notify whether there is more than one worksheet in the spreadsheet.
    if (num_worksheets := len(spreadsheet.worksheets())) > 1:
        logging.warning(
            f"There are {num_worksheets} worksheets present at `{survey_url}`, but only one was collected!"
        )

    # Collect the relevant data from the 0th worksheet in the spreadsheet.
    worksheet = get_worksheet_from_google_spreadsheet(spreadsheet, sheet_index=0)
    worksheet_data = parse_google_worksheet(worksheet, iter_records=True)

    worksheet_title = worksheet_data["worksheet_title"]
    logging.info(
        f"Building DataFrame from worksheet `{worksheet_title}`."
    )

    # Write the worksheet records to disk as JSON lines.
    worksheet_records = worksheet_data["worksheet_records"]
    output_path = os.path.join(output_dir, "surveys_sheet0.jsonl")

    jsonl.serialize_json_records_to_disk(worksheet_records, output_path, **kwargs)

    return output_dir
