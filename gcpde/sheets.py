"""Google Sheets client."""

from typing import Optional

import gspread
from google.auth.credentials import Credentials as GoogleCredentials
from gspread import Spreadsheet, Worksheet
from loguru import logger

from gcpde.types import ListJsonType


def _open_document(
    document_id: str,
    json_key: Optional[dict[str, str]] = None,
    credentials: Optional[GoogleCredentials] = None,
) -> Spreadsheet:
    if json_key is None and credentials is None:
        raise ValueError(
            "You must provide either a json_key or credentials to connect to sheets."
        )
    gc = (
        gspread.authorize(credentials)
        if credentials
        else gspread.service_account_from_dict(json_key)  # type: ignore[arg-type]
    )
    return gc.open_by_key(document_id)


def _open_sheet(
    sheet_name: str,
    document_id: str,
    json_key: Optional[dict[str, str]] = None,
    credentials: Optional[GoogleCredentials] = None,
) -> Worksheet:
    """Open a sheet from a document."""
    spreadsheet = _open_document(
        document_id=document_id, json_key=json_key, credentials=credentials
    )
    worksheet: Worksheet = spreadsheet.worksheet(sheet_name)
    return worksheet


def _get_worksheets(
    document_id: str,
    json_key: Optional[dict[str, str]] = None,
    credentials: Optional[GoogleCredentials] = None,
) -> list[str]:
    return [
        ws.title
        for ws in _open_document(
            document_id=document_id, json_key=json_key, credentials=credentials
        ).worksheets()
    ]


def replace_from_records(
    document_id: str,
    sheet_name: str,
    records: ListJsonType,
    columns: list[str],
    json_key: Optional[dict[str, str]] = None,
    credentials: Optional[GoogleCredentials] = None,
) -> None:
    """Replace a target document content with new data.

    Args:
        document_id: id for the document (can be retrieved from the url).
        sheet_name: name of the sheet to replace with new records.
        records: list of records to add to the document.
            Prefer to use only strings and numbers. datetime for example will not
            serialize.
        columns: name of the columns expected in the given records.
        json_key: json key with gcp credentials.
        credentials: google-auth credentials to connect to gcp.

    """
    logger.info(f"Document '{document_id}' update started ...")
    sheet = _open_sheet(
        sheet_name=sheet_name,
        document_id=document_id,
        json_key=json_key,
        credentials=credentials,
    )
    sheet.clear()
    records_as_row = [[r[c] for c in columns] for r in records]
    sheet.update(values=[columns] + records_as_row, range_name="A1")
    logger.info("Document update finished!")


def read_sheet(
    document_id: str,
    sheet_name: str,
    json_key: Optional[dict[str, str]] = None,
    head: int = 1,
    expected_headers: Optional[list[str]] = None,
    credentials: Optional[GoogleCredentials] = None,
) -> list[dict[str, str | None]]:
    """Get all the data in a sheet from a document.

    Args:
        document_id: id for the document (can be retrieved from the url).
        sheet_name: name of the sheet to read in the document.
        json_key: json key with gcp credentials.
        head: (optional) Determines which row to use as keys,
            starting from 1 following the numeration of the spreadsheet.
        expected_headers: (optional) List of expected headers, they must be unique.
        credentials: google-auth credentials to connect to gcp.

    Returns:
        List of records as json dict.

    """
    logger.info(f"Reading sheet '{sheet_name}' from document '{document_id}' ...")
    sheet = _open_sheet(
        sheet_name=sheet_name,
        document_id=document_id,
        json_key=json_key,
        credentials=credentials,
    )
    records: ListJsonType = sheet.get_all_records(
        head=head, expected_headers=expected_headers
    )
    logger.info("Records successfully retrieve from document!")
    return [
        {key: (str(value) or None) for key, value in record.items()}
        for record in records
    ]


def read_sheets(
    document_id: str,
    json_key: Optional[dict[str, str]] = None,
    sheet_names: list[str] | None = None,
    credentials: Optional[GoogleCredentials] = None,
) -> dict[str, list[dict[str, str | None]]]:
    """Get all data from all sheets in a document.

    Args:
        document_id: id for the document (can be retrieved from the url).
        json_key: json key with gcp credentials.
        sheet_names: list of sheet names to read in the document.
        credentials: google-auth credentials to connect to gcp.

    Returns:
        Dict with each key being the sheet_name and the values as a list of records as
        json dict for each sheet.

    """
    sheet_names = sheet_names or _get_worksheets(
        document_id=document_id, json_key=json_key, credentials=credentials
    )
    return {
        sheet_name: read_sheet(
            document_id=document_id,
            sheet_name=sheet_name,
            json_key=json_key,
            credentials=credentials,
        )
        for sheet_name in sheet_names
    }
