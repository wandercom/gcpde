from unittest import mock

from gcpde import sheets

MockWorksheet = mock.Mock("gspread.worksheet.Worksheet", autospec=True)
MockSpreadsheet = mock.Mock("gspread.spreadsheet.Spreadsheet", autospec=True)


@mock.patch("gspread.service_account_from_dict", autospec=True)
def test__open_sheet(mock_service_account_from_dict: mock.Mock):
    # act
    _ = sheets._open_sheet(
        sheet_name="sheet_name", document_id="document_id", json_key={}
    )

    # assert
    mock_service_account_from_dict.assert_called_once_with({})


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
        json_key={},
    )

    # assert
    mock_open_sheet.assert_called_once()
    mock_worksheet.clear.assert_called_once()
    kwargs = mock_worksheet.update.call_args.kwargs
    assert list(kwargs["values"]) == [["col"], ["value"]]
    assert kwargs["range_name"] == "A1"


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
        json_key={},
    )

    # assert
    assert output == {"sheet": [{"key": "123"}]}
