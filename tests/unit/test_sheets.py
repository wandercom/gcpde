from unittest import mock

import pytest
from google.auth.credentials import Scoped

from gcpde import sheets

MockWorksheet = mock.Mock("gspread.worksheet.Worksheet", autospec=True)
MockSpreadsheet = mock.Mock("gspread.spreadsheet.Spreadsheet", autospec=True)

MOCK_JSON_KEY = {"type": "service_account"}


@mock.patch("gspread.service_account_from_dict", autospec=True)
def test__open_sheet(mock_service_account_from_dict: mock.Mock):
    # act
    _ = sheets._open_sheet(
        sheet_name="sheet_name", document_id="document_id", json_key=MOCK_JSON_KEY
    )

    # assert
    mock_service_account_from_dict.assert_called_once_with(MOCK_JSON_KEY)


def test__open_document_raises_without_credentials():
    with pytest.raises(ValueError, match="json_key or credentials"):
        sheets._open_document(document_id="document_id")


@mock.patch("gspread.authorize", autospec=True)
def test__open_document_scoped_credentials_no_scopes(mock_authorize: mock.Mock):
    """Scoped credentials with no scopes should have Sheets scopes added."""
    mock_creds = mock.MagicMock(spec=Scoped)
    mock_creds.scopes = None
    scoped_creds = mock.MagicMock()
    mock_creds.with_scopes.return_value = scoped_creds

    sheets._open_document(document_id="document_id", credentials=mock_creds)

    mock_creds.with_scopes.assert_called_once_with(sheets._SHEETS_SCOPES)
    mock_authorize.assert_called_once_with(scoped_creds)


@mock.patch("gspread.authorize", autospec=True)
def test__open_document_scoped_credentials_with_sheets_scope(mock_authorize: mock.Mock):
    """Scoped credentials that already have a Sheets scope should be used as-is."""
    mock_creds = mock.MagicMock(spec=Scoped)
    mock_creds.scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    sheets._open_document(document_id="document_id", credentials=mock_creds)

    mock_creds.with_scopes.assert_not_called()
    mock_authorize.assert_called_once_with(mock_creds)


@mock.patch("gspread.authorize", autospec=True)
def test__open_document_non_scoped_credentials_logs_warning(mock_authorize: mock.Mock):
    """Non-Scoped credentials cannot be re-scoped; a warning should be emitted."""
    from google.auth.credentials import Credentials as GoogleCredentials

    mock_creds = mock.MagicMock(spec=GoogleCredentials)

    with mock.patch("gcpde.sheets.logger") as mock_logger:
        sheets._open_document(document_id="document_id", credentials=mock_creds)
        mock_logger.warning.assert_called_once()

    mock_authorize.assert_called_once_with(mock_creds)


@mock.patch("gcpde.sheets._open_sheet", autospec=True)
def test_replace_from_records(mock_open_sheet: mock.Mock):
    # arrange
    mock_worksheet = MockWorksheet()
    mock_open_sheet.return_value = mock_worksheet

    # act
    sheets.replace_from_records(
        document_id="document_id",
        sheet_name="sheet_name",
        records=[{"col": "value"}],
        columns=["col"],
        json_key=MOCK_JSON_KEY,
    )

    # assert
    mock_open_sheet.assert_called_once()
    mock_worksheet.clear.assert_called_once()
    kwargs = mock_worksheet.update.call_args.kwargs
    assert list(kwargs["values"]) == [["col"], ["value"]]
    assert kwargs["range_name"] == "A1"


@mock.patch("gcpde.sheets._open_sheet", autospec=True)
def test_read_sheet(mock_open_sheet: mock.Mock):
    mock_worksheet = MockWorksheet()
    mock_open_sheet.return_value = mock_worksheet
    mock_worksheet.get_all_records.return_value = [{"col": "value"}, {"col": ""}]

    result = sheets.read_sheet(
        document_id="document_id",
        sheet_name="sheet_name",
        json_key=MOCK_JSON_KEY,
    )

    mock_open_sheet.assert_called_once()
    assert result == [{"col": "value"}, {"col": None}]


@mock.patch("gcpde.sheets._open_document", autospec=True)
def test_read_sheets(mock_open_document: mock.Mock):
    # arrange
    mock_spreadsheet = MockSpreadsheet()
    mock_open_document.return_value = mock_spreadsheet

    mock_worksheet = MockWorksheet()
    mock_spreadsheet.worksheet.return_value = mock_worksheet
    mock_worksheet.get_all_records.return_value = [{"key": 123}]

    mock_worksheet.title = "sheet"
    mock_spreadsheet.worksheets.return_value = [mock_worksheet]

    # act
    output = sheets.read_sheets(
        document_id="document_id",
        json_key=MOCK_JSON_KEY,
    )

    # assert
    assert output == {"sheet": [{"key": "123"}]}
