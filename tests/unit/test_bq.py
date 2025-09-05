from unittest.mock import Mock, patch

import pandas as pd
import pytest
from google.api_core.exceptions import NotFound
from google.cloud.bigquery import DatasetReference, SchemaField, TableReference

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

    def test_get_table(self, bq_client: bq.BigQueryClient):
        # arrange
        dataset_ref = DatasetReference(bq_client.client.project, self.dataset)
        expected_table_ref = TableReference(dataset_ref, self.table)

        # act
        result = bq_client.get_table(dataset=self.dataset, table=self.table)

        # assert
        bq_client.client.get_table.assert_called_once_with(table=expected_table_ref)
        assert result is not None


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
        SchemaField.from_api_repr(
            {
                "name": "id",
                "type": "INTEGER",
                "mode": "NULLABLE",
            }
        ),
        SchemaField.from_api_repr(
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
            }
        ),
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
        SchemaField.from_api_repr(
            {
                "name": "id",
                "type": "INTEGER",
                "mode": "NULLABLE",
            }
        )
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


@patch("gcpde.bq.delete_table")
@patch("gcpde.bq.create_table_from_records")
def test_upsert_table_from_records(mock_create_table, mock_delete_table):
    # arrange
    mock_client = Mock(spec_set=bq.BigQueryClient)
    table_tmp = "table_tmp"
    table = "table"
    dataset = "dataset"

    table_mock = Mock(schema=[])
    mock_client.get_table.return_value = table_mock

    schema_json = [
        {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
    ]
    table_mock.schema = [SchemaField.from_api_repr(field) for field in schema_json]

    command_sql = (
        "MERGE INTO dataset.table AS target "
        "USING dataset.table_tmp AS source "
        "ON source.id = target.id "
        "WHEN MATCHED THEN "
        "UPDATE SET id = source.id, name = source.name "
        "WHEN NOT MATCHED THEN "
        "INSERT (id, name) "
        "VALUES (id, name)"
    )

    # act
    bq.upsert_table_from_records(
        dataset=dataset,
        table=table,
        records=[{"id": 1, "name": "test"}, {"id": 2, "name": "test2"}],
        key_field="id",
        client=mock_client,
        insert_chunk_size=None,
    )

    # assert
    mock_create_table.assert_called_once_with(
        dataset=dataset,
        table=table_tmp,
        records=[{"id": 1, "name": "test"}, {"id": 2, "name": "test2"}],
        overwrite=True,
        json_key=None,
        client=mock_client,
        chunk_size=None,
        schema=table_mock.schema,
    )

    mock_delete_table.assert_called_once_with(
        dataset=dataset, table=table_tmp, client=mock_client
    )

    mock_client.run_command.assert_called_with(command_sql=command_sql)

    for call in mock_delete_table.call_args_list:
        assert call.kwargs.get("table") != table


@patch("gcpde.bq.delete_table")
@patch("gcpde.bq.create_table_from_records")
def test_upsert_table_from_records_no_records(mock_create_table, mock_delete_table):
    # arrange
    mock_client = Mock(spec_set=bq.BigQueryClient)

    # act
    bq.upsert_table_from_records(
        dataset="dataset", table="table", records=[], key_field="id", client=mock_client
    )

    # assert
    mock_create_table.assert_not_called()
    mock_delete_table.assert_not_called()


@patch("gcpde.bq.delete_table")
def test_upsert_table_from_records_schema_mismatch(mock_delete_table):
    # arrange
    mock_client = Mock(spec_set=bq.BigQueryClient)

    table_mock = Mock()
    temp_table_mock = Mock()
    table_mock.schema = [{"name": "uuid", "type": "STRING", "mode": "NULLABLE"}]
    temp_table_mock.schema = [{"name": "id", "type": "INTEGER", "mode": "NULLABLE"}]
    mock_client.get_table = (
        lambda dataset, table: table_mock if table == "table" else temp_table_mock
    )

    schema_json = [{"name": "uuid", "type": "STRING", "mode": "NULLABLE"}]
    table_mock.schema = [SchemaField.from_api_repr(field) for field in schema_json]

    # act/assert
    with pytest.raises(bq.BigQuerySchemaMismatchException):
        bq.upsert_table_from_records(
            dataset="dataset",
            table="table",
            records=[{"id": 1}],
            key_field="id",
            client=mock_client,
            use_target_schema=False,
        )

    mock_delete_table.call_count == 2


@patch("gcpde.bq.create_table_from_records")
def test_upsert_table_from_records_missing_target_table(mock_create_table):
    # arrange
    mock_client = Mock(spec_set=bq.BigQueryClient)
    mock_client.get_table.side_effect = NotFound("")

    # act
    bq.upsert_table_from_records(
        dataset="dataset",
        table="table",
        records=[{"id": 1}],
        key_field="id",
        client=mock_client,
    )

    # assert
    mock_create_table.assert_called_once_with(
        dataset="dataset",
        table="table",
        records=[{"id": 1}],
        overwrite=False,
        json_key=None,
        client=mock_client,
        chunk_size=None,
    )


def test_big_query_schema_mismatch_exception():
    # arrange
    source_schema = [{"name": "id"}]
    target_schema = [{"name": "id"}]

    # act
    exception = bq.BigQuerySchemaMismatchException(
        message="message", source_schema=source_schema, target_schema=target_schema
    )

    # assert
    assert (
        str(exception)
        == "message\nSource schema: [{'name': 'id'}]\nTarget schema: [{'name': 'id'}]"
    )


def test_get_schema_from_json():
    # arrange
    schema_json = [
        {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "name", "type": "STRING", "mode": "REQUIRED"},
    ]

    # act
    result = bq.get_schema_from_json(schema_json)

    # assert
    assert len(result) == 2
    assert result[0].name == "id"
    assert result[0].field_type == "INTEGER"
    assert result[0].mode == "NULLABLE"
    assert result[1].name == "name"
    assert result[1].field_type == "STRING"
    assert result[1].mode == "REQUIRED"
