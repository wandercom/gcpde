from unittest.mock import Mock

import pandas as pd
import pytest
from google.api_core.exceptions import NotFound

from gcpde import bq


class TestBigQueryClient:
    dataset = "my_dataset"
    table = "my_table"

    @pytest.fixture
    def bq_client(self) -> bq.BigQueryClient:
        client = bq.BigQueryClient(client=Mock(autospec=True))
        client.client.project = "my_project"
        client.client.insert_rows_json.return_value = []
        return client

    def test_check_table(self, bq_client: bq.BigQueryClient):
        # act
        result = bq_client.check_table(dataset=self.dataset, table=self.table)

        # assert
        assert result is True

    def test_check_table_not_found(self, bq_client: bq.BigQueryClient):
        # arrange
        bq_client.client.get_table.side_effect = NotFound("")

        # act
        result = bq_client.check_table(dataset=self.dataset, table=self.table)

        # assert
        assert result is False

    def test_create_table(self, bq_client: bq.BigQueryClient):
        # arrange
        schema = [{"name": "column1", "type": "STRING"}]

        # act
        bq_client.create_table(dataset=self.dataset, table=self.table, schema=schema)

        # assert
        bq_client.client.create_table.assert_called_once()

    def test_delete_table(self, bq_client: bq.BigQueryClient):
        # act
        bq_client.delete_table(dataset=self.dataset, table=self.table)

        # assert
        bq_client.client.delete_table.assert_called_once()

    def test_push_rows(self, bq_client: bq.BigQueryClient):
        # arrange
        records = [{"column1": "value1"}]

        # act
        bq_client.insert(dataset=self.dataset, table=self.table, records=records)

        # assert
        bq_client.client.insert_rows_json.assert_called_once()

    def test_query(self, bq_client: bq.BigQueryClient):
        # assert
        query = "SELECT * FROM my_table"
        query_job = Mock()
        query_job.result.return_value = []
        bq_client.client.query.return_value = query_job

        # act
        result = bq_client.query(query)

        # assert
        assert result == []
        bq_client.client.query.assert_called_once()
        query_job.result.assert_called_once()

    def test_run_command(self, bq_client: bq.BigQueryClient):
        # assert
        command = "create table a as select * from b"

        # act
        bq_client.run_command(command_sql=command)

        # assert
        bq_client.client.query.assert_called_once_with(command)


def test_create_table_from_records():
    # arrange
    dataset, table = "my-dataset", "my-table"
    mock_client = Mock(spec_set=bq.BigQueryClient)
    input_records = [
        {"id": 1},
        {"id": 2, "json_col": {"col1": 1}},
        {"id": 3, "json_col": {"col2": True}},
        {"json_col": {"col3": "abc"}},
    ]
    target_schema = [
        {
            "name": "id",
            "type": "INTEGER",
            "mode": "NULLABLE",
        },
        {
            "name": "json_col",
            "type": "RECORD",
            "mode": "NULLABLE",
            "fields": [
                {
                    "name": "col1",
                    "type": "INTEGER",
                    "mode": "NULLABLE",
                },
                {
                    "name": "col2",
                    "type": "BOOLEAN",
                    "mode": "NULLABLE",
                },
                {
                    "name": "col3",
                    "type": "STRING",
                    "mode": "NULLABLE",
                },
            ],
        },
    ]

    # act
    bq.create_table_from_records(
        dataset=dataset,
        table=table,
        records=input_records,
        overwrite=True,
        client=mock_client,
    )

    # assert
    table_tmp = table + "_tmp"
    mock_client.delete_table.assert_called_with(dataset=dataset, table=table_tmp)
    assert mock_client.delete_table.call_count == 2
    mock_client.create_table.assert_called_once_with(
        dataset=dataset,
        table=table_tmp,
        schema=target_schema,
    )
    mock_client.insert.assert_called_once_with(
        dataset=dataset, table=table_tmp, records=input_records
    )
    mock_client.run_command.assert_called_once()


def test_create_table_from_records_overwrite_false():
    # arrange
    dataset, table = "my-dataset", "my-table"
    mock_client = Mock(spec_set=bq.BigQueryClient)
    input_records = [{"id": 1}]
    target_schema = [
        {
            "name": "id",
            "type": "INTEGER",
            "mode": "NULLABLE",
        }
    ]

    # act
    bq.create_table_from_records(
        dataset="my-dataset",
        table="my-table",
        records=input_records,
        overwrite=False,
        client=mock_client,
    )

    # assert
    mock_client.delete_table.assert_not_called()
    mock_client.create_table.assert_called_once_with(
        dataset=dataset,
        table=table,
        schema=target_schema,
    )
    mock_client.insert.assert_called_once_with(
        dataset=dataset, table=table, records=input_records
    )


def test_select():
    # arrange
    mock_client = Mock(spec_set=bq.BigQueryClient)
    target = [{"col": "value"}]
    mock_client.query.return_value = target

    # act
    output = bq.select(query="select * from table", client=mock_client)

    # assert
    assert output == target


def test_delete_not_exist():
    """Add coverage to flow on delete_table when table does not exist."""
    # arrange
    mock_client = Mock(spec_set=bq.BigQueryClient)
    mock_client.check_table.return_value = False

    # act
    bq.delete_table(dataset="my-dataset", table="my-table", client=mock_client)


def test__create_schema_from_records_exception():
    # arrange
    input_records = [{"a": True}, {"a": "TRUE"}]

    # act and assert
    with pytest.raises(bq.BigQueryClientException):
        bq._create_schema_from_records(records=input_records)


def test_create_table_from_records_no_records():
    """Add coverage to case where there are no records to insert."""
    bq.create_table_from_records(dataset="dataset", table="table", records=[])


def test_insert_rows_json_fail():
    # arrange
    client = bq.BigQueryClient(client=Mock(autospec=True))
    client.client.insert_rows_json.return_value = [
        {
            "index": 0,
            "errors": [
                {
                    "reason": "invalid",
                    "location": "dates",
                    "debugInfo": "",
                    "message": "Array specified for non-repeated field: dates.",
                }
            ],
        }
    ]

    def mock_create_table_reference(dataset: str, table: str):
        return Mock(autospec=True)

    client._create_table_reference = mock_create_table_reference
    records = [{"column1": "value1"}]
    dataset = "my_dateset"
    table = "my_table"

    # act
    with pytest.raises(ValueError):
        client.insert(dataset=dataset, table=table, records=records)


def test_query_to_df():
    mock_client = Mock(spec_set=bq.BigQueryClient)
    mock_client.query.return_value = [{"col": "value"}]

    # act
    df = bq.query_to_df(query="select * from table", client=mock_client)

    # assert
    assert df.equals(pd.DataFrame([{"col": "value"}]))


def test_create_table_from_query():
    # arrange
    mock_client = Mock(spec_set=bq.BigQueryClient)
    mock_client.run_command.return_value = None

    # act
    bq.create_table_from_query(
        query="select * from table",
        dataset="dataset",
        table="table",
        client=mock_client,
        timeout=10,
    )

    # assert
    mock_client.run_command.assert_called_once_with(
        command_sql="create or replace table dataset.table as select * from table",
        timeout=10,
    )
