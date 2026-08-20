"""Google Sheets client."""

import logging
from typing import Callable, Optional, TypeVar

import gspread
import tenacity
from google.auth.credentials import Credentials as GoogleCredentials
from google.auth.credentials import Scoped
from gspread import Spreadsheet, Worksheet
from gspread.exceptions import APIError, WorksheetNotFound
from loguru import logger

from gcpde.types import ListJsonType

_SHEETS_SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

_TRANSIENT_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})

_T = TypeVar("_T")


def _is_transient_api_error(exception: BaseException) -> bool:
    """Tell whether a gspread error is worth retrying.

    Args:
        exception: exception raised by a gspread call.

    Returns:
        True for rate limiting and server-side errors, False otherwise. Client
        errors such as 401/403/404 are deterministic and are never retried.
    """
    if not isinstance(exception, APIError):
        return False
    status_code = getattr(exception.response, "status_code", None)
    return status_code in _TRANSIENT_STATUS_CODES


@tenacity.retry(
    retry=tenacity.retry_if_exception(_is_transient_api_error),
    wait=tenacity.wait_exponential(min=1, max=30),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
    before_sleep=tenacity.before_sleep_log(
        logger=logger,
        log_level=logging.WARNING,
    ),
)
def _call_api(operation: Callable[[], _T]) -> _T:
    """Run a single Google Sheets API call, retrying transient failures.

    Wrap only leaf calls with this: nesting two retried calls multiplies the
    number of attempts.

    Args:
        operation: zero-argument callable performing one API round trip.

    Returns:
        Whatever the operation returns.
    """
    return operation()


def _open_document(
    document_id: str,
    json_key: Optional[dict[str, str]] = None,
    credentials: Optional[GoogleCredentials] = None,
) -> Spreadsheet:
    if not json_key and credentials is None:
        raise ValueError(
            "You must provide either a json_key or credentials to connect to sheets."
        )
    if credentials is not None:
        needs_scopes = isinstance(credentials, Scoped) and not (
            credentials.scopes
            and set(credentials.scopes)
            & {
                "https://www.googleapis.com/auth/spreadsheets",
                "https://spreadsheets.google.com/feeds",
            }
        )
        if needs_scopes:
            assert isinstance(credentials, Scoped)
            _creds: GoogleCredentials = credentials.with_scopes(  # type: ignore[no-untyped-call]
                _SHEETS_SCOPES
            )
        elif not isinstance(credentials, Scoped):
            logger.warning(
                "Credentials do not support scoping. Ensure they were obtained with "
                "the required Google Sheets scopes: "
                "https://www.googleapis.com/auth/spreadsheets"
            )
            _creds = credentials
        else:
            _creds = credentials
        gc = gspread.authorize(_creds)
    else:
        gc = gspread.service_account_from_dict(json_key)  # type: ignore[arg-type]
    return _call_api(lambda: gc.open_by_key(document_id))


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
    worksheet: Worksheet = _call_api(lambda: spreadsheet.worksheet(sheet_name))
    return worksheet


def _get_worksheets(
    document_id: str,
    json_key: Optional[dict[str, str]] = None,
    credentials: Optional[GoogleCredentials] = None,
) -> list[str]:
    spreadsheet = _open_document(
        document_id=document_id, json_key=json_key, credentials=credentials
    )
    return [ws.title for ws in _call_api(spreadsheet.worksheets)]


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


def replace_or_create_from_records(
    document_id: str,
    sheet_name: str,
    records: ListJsonType,
    columns: list[str],
    min_rows: int = 100,
    json_key: dict[str, str] | None = None,
    credentials: GoogleCredentials | None = None,
) -> Worksheet:
    """Replace worksheet content, creating and resizing the worksheet as needed.

    Args:
        document_id: ID for the document (can be retrieved from the URL).
        sheet_name: Name of the worksheet to replace or create.
        records: Records to write to the worksheet.
        columns: Column names and order to use when writing records.
        min_rows: Minimum number of rows the worksheet should contain.
        json_key: JSON key with GCP credentials.
        credentials: Google-auth credentials to connect to GCP.

    Returns:
        The created or updated worksheet.
    """
    logger.info(f"Document '{document_id}' update started ...")
    spreadsheet = _open_document(
        document_id=document_id, json_key=json_key, credentials=credentials
    )
    required_rows = len(records) + 1

    try:
        worksheet = spreadsheet.worksheet(sheet_name)
    except WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=sheet_name,
            rows=max(min_rows, required_rows),
            cols=len(columns),
        )
    else:
        worksheet.resize(
            rows=max(worksheet.row_count, min_rows, required_rows),
            cols=max(worksheet.col_count, len(columns)),
        )
    worksheet.clear()
    worksheet.update(
        values=[columns]
        + [[record[column] for column in columns] for record in records],
        range_name="A1",
    )
    logger.info("Document update finished!")
    return worksheet


def list_worksheets(
    document_id: str,
    json_key: dict[str, str] | None = None,
    credentials: GoogleCredentials | None = None,
) -> list[Worksheet]:
    """List all worksheets in a Google Sheets document.

    Args:
        document_id: ID for the document (can be retrieved from the URL).
        json_key: JSON key with GCP credentials.
        credentials: Google-auth credentials to connect to GCP.

    Returns:
        All worksheets in the document.
    """
    spreadsheet = _open_document(
        document_id=document_id, json_key=json_key, credentials=credentials
    )
    return _call_api(spreadsheet.worksheets)


def delete_worksheet(
    document_id: str,
    sheet_name: str,
    json_key: dict[str, str] | None = None,
    credentials: GoogleCredentials | None = None,
) -> None:
    """Delete a worksheet from a Google Sheets document.

    Args:
        document_id: ID for the document (can be retrieved from the URL).
        sheet_name: Name of the worksheet to delete.
        json_key: JSON key with GCP credentials.
        credentials: Google-auth credentials to connect to GCP.

    Raises:
        gspread.exceptions.WorksheetNotFound: If the worksheet does not exist.
    """
    spreadsheet = _open_document(
        document_id=document_id, json_key=json_key, credentials=credentials
    )
    spreadsheet.del_worksheet(spreadsheet.worksheet(sheet_name))


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
    records: ListJsonType = _call_api(
        lambda: sheet.get_all_records(head=head, expected_headers=expected_headers)
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
