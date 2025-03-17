"""Google Sheets client."""

from typing import Iterator, Sequence

import gspread
from gspread import Spreadsheet, Worksheet
from loguru import logger

from gcpde.types import ListJsonType


def _open_document(document_id: str, json_key: dict[str, str]) -> Spreadsheet:
    gc = gspread.service_account_from_dict(json_key)
    return gc.open_by_key(document_id)


def _open_sheet(
    sheet_name: str, document_id: str, json_key: dict[str, str]
) -> Worksheet:
    """Open a sheet from a document."""
    spreadsheet = _open_document(document_id=document_id, json_key=json_key)
    worksheet: Worksheet = spreadsheet.worksheet(sheet_name)
    return worksheet


def _get_worksheets(document_id: str, json_key: dict[str, str]) -> list[str]:
    return [
        ws.title
        for ws in _open_document(
            document_id=document_id, json_key=json_key
        ).worksheets()
    ]


def replace_from_records(
    document_id: str,
    sheet_name: str,
    records: ListJsonType,
    columns: list[str],
    json_key: dict[str, str],
) -> None:
    """Replace a target document content with new data.

    Args:
        document_id: id for the document (can be retrieved from the url).
        sheet_name: name of the sheet to replace with new records.
        records: list of records to add to the document.
        columns: name of the columns expected in the given records.
        json_key: json key with gcp credentials.

    """
    logger.info(f"Document '{document_id}' update started ...")
    sheet = _open_sheet(
        sheet_name=sheet_name, document_id=document_id, json_key=json_key
    )
    sheet.clear()

    def generate_rows() -> Iterator[Sequence[str | None]]:
        yield columns
        for row in records:
            yield [row.get(c) for c in columns]

    sheet.update(values=generate_rows(), range_name="A1")
    logger.info("Document update finished!")


def read_sheet(
    document_id: str,
    sheet_name: str,
    json_key: dict[str, str],
) -> list[dict[str, str | None]]:
    """Get all the data in a sheet from a document.

    Args:
        document_id: id for the document (can be retrieved from the url).
        sheet_name: name of the sheet to read in the document.
        json_key: json key with gcp credentials.

    Returns:
        List of records as json dict.

    """
    logger.info(f"Reading sheet '{sheet_name}' from document '{document_id}' ...")
    sheet = _open_sheet(
        sheet_name=sheet_name, document_id=document_id, json_key=json_key
    )
    records: ListJsonType = sheet.get_all_records()
    logger.info("Records successfully retrieve from document!")
    return [
        {key: (str(value) or None) for key, value in record.items()}
        for record in records
    ]


def read_sheets(
    document_id: str,
    json_key: dict[str, str],
    sheet_names: list[str] | None = None,
) -> dict[str, list[dict[str, str | None]]]:
    """Get all data from all sheets in a document.

    Args:
        document_id: id for the document (can be retrieved from the url).
        sheet_names: list of sheet names to read in the document.
        json_key: json key with gcp credentials.

    Returns:
        Dict with each key being the sheet_name and the values as a list of records as
        json dict for each sheet.

    """
    sheet_names = sheet_names or _get_worksheets(
        document_id=document_id, json_key=json_key
    )
    return {
        sheet_name: read_sheet(
            document_id=document_id, sheet_name=sheet_name, json_key=json_key
        )
        for sheet_name in sheet_names
    }
