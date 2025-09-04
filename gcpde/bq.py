"""BigQuery client using the official Google Cloud BigQuery package."""

import logging
from typing import Sequence

import funcy
import pandas as pd
import tenacity
from bigquery_schema_generator.generate_schema import SchemaGenerator
from google.api_core.exceptions import BadRequest, NotFound
from google.cloud import bigquery
from google.cloud.exceptions import Conflict
from google.oauth2.service_account import Credentials
from loguru import logger

from gcpde.types import ListJsonType

FIVE_MINUTES = 1 * 60 * 5


class BigQueryClient:
    """BigQuery client using the official Google Cloud BigQuery package.

    Attributes:
        client: Official BigQuery client.

    """

    def __init__(
        self,
        json_key: dict[str, str] | None = None,
        credentials: Credentials | None = None,
        client: bigquery.Client | None = None,
    ):
        self.client = client or bigquery.Client(
            credentials=credentials
            or Credentials.from_service_account_info(info=json_key)  # type: ignore[no-untyped-call]
        )

    def _create_table_reference(
        self, dataset: str, table: str
    ) -> bigquery.TableReference:
        dataset_ref = bigquery.DatasetReference(self.client.project, dataset)
        table_ref: bigquery.TableReference = dataset_ref.table(table)
        return table_ref

    def check_table(self, dataset: str, table: str) -> bool:
        """Check if a table exists on bigquery.

        Args:
            dataset: dataset name.
            table: table name.

        Returns:
            True if the table exists, False otherwise.

        """
        try:
            self.get_table(dataset, table)
            return True
        except NotFound:
            return False

    def get_table(self, dataset: str, table: str) -> bigquery.Table:
        """Get a table from bigquery.

        Args:
            dataset: dataset name.
            table: table name.
        """
        table_ref = self._create_table_reference(dataset, table)
        return self.client.get_table(table=table_ref)

    def create_table(
        self,
        dataset: str,
        table: str,
        schema: list[dict[str, str]],
    ) -> None:
        """Create a new table on bigquery.

        Please provide one the both arguments: schema or schema_from_records.

        Args:
            dataset: dataset name.
            table: table name.
            schema: json dict schema for the table.
                https://cloud.google.com/bigquery/docs/schemas#creating_a_json_schema_file

        Raises:
            google.cloud.exceptions.Conflict: if the table already exists.

        """
        table_ref = self._create_table_reference(dataset, table)
        table_obj = bigquery.Table(
            table_ref=table_ref,
            schema=[bigquery.SchemaField.from_api_repr(field) for field in schema],
        )
        self.client.create_table(table_obj)

    def delete_table(self, dataset: str, table: str) -> None:
        """Delete a table from bigquery.

        Args:
            dataset: dataset name.
            table: table name.

        Raises:
            google.api_core.exceptions.NotFound: if the table does not exist.

        """
        table_ref = self._create_table_reference(dataset, table)
        self.client.delete_table(table_ref)

    def insert(
        self,
        dataset: str,
        table: str,
        records: ListJsonType,
    ) -> None:
        """Push rows to bigquery.

        Args:
            dataset: dataset name.
            table: table name.
            records: list of json records.

        Raises:
            google.api_core.exceptions.NotFound: if table doesn't exist.
            google.api_core.exceptions.BadRequest: if the records are not valid.
                Like empty list.

        """
        table_ref = self._create_table_reference(dataset, table)
        results: Sequence[dict] = self.client.insert_rows_json(  # type: ignore
            table=table_ref,
            json_rows=records,
            ignore_unknown_values=False,
            skip_invalid_rows=False,
        )
        errors = [r["errors"] for r in results if "errors" in r and r["errors"]]
        if errors:
            raise ValueError(f"Found errors when inserting rows: {errors}")

    def query(self, query: str, timeout: int = FIVE_MINUTES) -> ListJsonType:
        """Query bigquery.

        Args:
            query: query to be executed.
            timeout: timeout in seconds. Default is 5 minutes.

        Returns:
            Records from query.

        """
        job_config = bigquery.QueryJobConfig(use_legacy_sql=False)
        query_job = self.client.query(query, job_config=job_config)
        results = query_job.result(timeout=timeout)
        return [dict(row) for row in results]

    def run_command(self, command_sql: str, timeout: int = FIVE_MINUTES) -> None:
        """Run a command on bigquery.

        We have a different function just for running commands because according to
        the official documentation: "If the query is a special query that produces no
        results, e.g. a DDL query, an ``_EmptyRowIterator`` instance is returned."

        Args:
            command_sql: command to be executed.
            timeout: timeout in seconds. Default is 5 minutes.

        Raises:
            BigQueryClientException if the operation is not successful.

        """
        self.client.query(command_sql).result(timeout=timeout)


class BigQueryClientException(Exception):
    """Base exception for connection or command errors."""


class BigQuerySchemaMismatchException(Exception):
    """Exception for schema mismatch."""

    def __init__(
        self,
        message: str,
        source_schema: list[bigquery.SchemaField],
        target_schema: list[bigquery.SchemaField],
    ):
        super().__init__(message)
        self.message = message
        self.source_schema = source_schema
        self.target_schema = target_schema

    def __str__(self) -> str:
        return (
            f"{self.message}\n"
            f"Source schema: {self.source_schema}\n"
            f"Target schema: {self.target_schema}"
        )


def delete_table(
    dataset: str,
    table: str,
    json_key: dict[str, str] | None = None,
    client: BigQueryClient | None = None,
) -> None:
    """Delete a table from bigquery.

    Args:
        dataset: dataset name.
        table: table name.
        json_key: json key with gcp credentials.
        client: client to connect to gcp.

    """
    logger.info(f"Deleting table {dataset}.{table}...")
    client = client or BigQueryClient(json_key=json_key or {})
    exist = client.check_table(dataset=dataset, table=table)
    if not exist:
        logger.warning("No table to delete!")
        return
    client.delete_table(dataset=dataset, table=table)
    logger.info("Table deleted!")
    return


def _create_schema_from_records(records: ListJsonType) -> list[dict[str, str]]:
    generator = SchemaGenerator(
        input_format="dict",
        keep_nulls=True,
        quoted_values_are_strings=True,
        preserve_input_sort_order=True,
    )
    logger.debug("Deducing schema from records...")
    schema_map, error_logs = generator.deduce_schema(input_data=records)
    if error_logs:
        raise BigQueryClientException(
            f"Can't infer schema from records, error: {error_logs}"
        )
    output: list[dict[str, str]] = generator.flatten_schema(schema_map)
    logger.debug("Schema generator complete!")
    return output


@tenacity.retry(
    retry=tenacity.retry_if_exception_type(Conflict),
    wait=tenacity.wait_exponential(min=1),
    stop=tenacity.stop_after_attempt(3),
    before_sleep=tenacity.before_sleep_log(
        logger=logger,  # type: ignore[arg-type]
        log_level=logging.WARNING,
    ),
)
def create_table(
    dataset: str,
    table: str,
    schema: list[dict[str, str]] | None = None,
    schema_from_records: ListJsonType | None = None,
    json_key: dict[str, str] | None = None,
    client: BigQueryClient | None = None,
) -> None:
    """Create a new table on bigquery.

    Please provide one the both arguments: schema or schema_from_records.

    Args:
        dataset: dataset name.
        table: table name.
        schema: json dict schema for the table.
            https://cloud.google.com/bigquery/docs/schemas#creating_a_json_schema_file
        schema_from_records: infer schema from a records sample.
        json_key: json key with gcp credentials.
        client: client to connect to gcp.

    Raises:
        google.cloud.exceptions.Conflict: if the table already exists.

    """
    logger.info(f"Creating table {dataset}.{table}...")
    client = client or BigQueryClient(json_key=json_key or {})
    schema = schema or _create_schema_from_records(records=schema_from_records or [])
    client.create_table(
        dataset=dataset,
        table=table,
        schema=schema,
    )
    logger.info("Table created!")


@tenacity.retry(
    retry=tenacity.retry_if_exception_type(exception_types=(BadRequest, NotFound)),
    wait=tenacity.wait_exponential(min=1),
    stop=tenacity.stop_after_attempt(10),
    before_sleep=tenacity.before_sleep_log(
        logger=logger,  # type: ignore[arg-type]
        log_level=logging.WARNING,
    ),
)
def _insert_chunk(
    dataset: str,
    table: str,
    records: ListJsonType,
    client: BigQueryClient,
) -> None:
    client.insert(dataset=dataset, table=table, records=records)
    logger.debug(f"Chunk of size {len(records)} successfully inserted.")


def insert(
    dataset: str,
    table: str,
    records: ListJsonType,
    chunk_size: int | None = None,
    json_key: dict[str, str] | None = None,
    client: BigQueryClient | None = None,
) -> None:
    """Insert records on a bigquery table.

    It automatically split the input data into smaller chunks, as the GCP API has a size
    limit, which can break if trying to insert too much data into a single request.

    Args:
        dataset: dataset name.
        table: table name.
        records: collection of records to insert.
        chunk_size: chunk size number to send to GCP API. 1000 by default.
        json_key: json key with gcp credentials.
        client: client to connect to gcp.

    Raises:
        google.api_core.exceptions.NotFound: if table doesn't exist.
        google.api_core.exceptions.BadRequest: if the records are not valid.
            Like empty list.

    """
    logger.info(f"Inserting {len(records)} records into {dataset}.{table}...")
    client = client or BigQueryClient(json_key=json_key or {})
    for chunk in funcy.chunks(chunk_size or 1000, records):
        _insert_chunk(dataset=dataset, table=table, records=chunk, client=client)
    logger.info("Insert operation completed!")


def create_or_replace_table_as(
    dataset: str,
    output_table: str,
    source_table: str,
    json_key: dict[str, str] | None = None,
    client: BigQueryClient | None = None,
) -> None:
    """Create or replace a table as another table.

    Args:
        dataset: dataset name.
        output_table: table name.
        source_table: table name.
        json_key: json key with gcp credentials.
        client: client to connect to BigQuery.

    Raises:
        BigQueryClientException if the operation is not successful.

    """
    client = client or BigQueryClient(json_key=json_key or {})
    command_sql = (
        f"create or replace table {dataset}.{output_table} as "
        f"select * from {dataset}.{source_table}"
    )
    logger.info(f"Running `{command_sql}`...")
    client.run_command(command_sql=command_sql)
    logger.info("Command executed!")


def upsert_table_from_records(
    dataset: str,
    table: str,
    records: ListJsonType,
    key_field: str,
    json_key: dict[str, str] | None = None,
    insert_chunk_size: int | None = None,
    client: BigQueryClient | None = None,
) -> None:
    """Upsert records into a table.

    This function performs an upsert (update/insert) operation by:
    1. Creating a temporary table with the new records
    2. using MERGE statement to update/insert records
    3. Cleaning up temporary table

    Args:
        dataset: dataset name.
        table: table name.
        records: records to be upserted.
        key_field: field used to match records for update.
        json_key: json key with gcp credentials.
        insert_chunk_size: chunk size for batch inserts.
        client: client to connect to BigQuery.

    Raises:
        BigQuerySchemaMismatchException: if schema of new records doesn't match table.
        BigQueryClientException: if schema cannot be inferred from records.
    """
    if not records:
        logger.warning("No records to create a table from! (empty collection given)")
        return

    client = client or BigQueryClient(json_key=json_key or {})
    tmp_table = table + "_tmp"

    create_table_from_records(
        dataset=dataset,
        table=tmp_table,
        records=records,
        overwrite=True,
        json_key=json_key,
        client=client,
        chunk_size=insert_chunk_size,
    )

    tmp_table_schema_bq = client.get_table(dataset, tmp_table).schema
    table_schema_bq = client.get_table(dataset, table).schema

    if table_schema_bq != tmp_table_schema_bq:
        logger.info("Cleaning up temporary table...")
        delete_table(dataset=dataset, table=tmp_table, client=client)

        raise BigQuerySchemaMismatchException(
            message="New data schema does not match table schema",
            source_schema=table_schema_bq,
            target_schema=tmp_table_schema_bq,
        )

    update_statement = ", ".join(
        [f"{field.name} = source.{field.name}" for field in table_schema_bq]
    )
    table_fields = ", ".join([field.name for field in table_schema_bq])

    merge_command_sql = (
        f"MERGE INTO {dataset}.{table} AS target "
        f"USING {dataset}.{tmp_table} AS source "
        f"ON source.{key_field} = target.{key_field} "
        f"WHEN MATCHED THEN "
        f"UPDATE SET {update_statement} "
        f"WHEN NOT MATCHED THEN "
        f"INSERT ({table_fields}) "
        f"VALUES ({table_fields})"
    )

    logger.info(f"Running `{merge_command_sql}`...")
    client.run_command(command_sql=merge_command_sql)
    logger.info("Command executed!")

    logger.info("Cleaning up temporary table...")
    delete_table(dataset=dataset, table=tmp_table, client=client)


def replace_table(
    dataset: str,
    table: str,
    records: ListJsonType,
    schema: list[dict[str, str]] | None = None,
    chunk_size: int | None = None,
    json_key: dict[str, str] | None = None,
    client: BigQueryClient | None = None,
) -> None:
    """Low down-time table replacement strategy."""
    client = client or BigQueryClient(json_key=json_key or {})

    tmp_table = table + "_tmp"
    delete_table(dataset=dataset, table=tmp_table, client=client)
    create_table(dataset=dataset, table=tmp_table, schema=schema, client=client)
    insert(
        dataset=dataset,
        table=tmp_table,
        records=records,
        client=client,
        chunk_size=chunk_size,
    )
    create_or_replace_table_as(
        dataset=dataset,
        output_table=table,
        source_table=tmp_table,
        client=client,
    )
    delete_table(dataset=dataset, table=tmp_table, client=client)


def create_table_from_records(
    dataset: str,
    table: str,
    records: ListJsonType,
    overwrite: bool = False,
    json_key: dict[str, str] | None = None,
    client: BigQueryClient | None = None,
    chunk_size: int | None = None,
) -> None:
    """Create or replace a table from a collection of records.

    Args:
        dataset: dataset name.
        table: table name.
        records: collection of records to insert.
        overwrite: delete a table if exists first.
        json_key: json key with gcp credentials.
        client: client to connect to gcp.
        chunk_size: chunk size number to send to GCP API.

    """
    if not records:
        logger.warning("No records to create a table from! (empty collection given)")
        return
    schema = _create_schema_from_records(records=records or [])

    client = client or BigQueryClient(json_key=json_key or {})
    if overwrite:
        replace_table(
            dataset=dataset,
            table=table,
            records=records,
            schema=schema,
            client=client,
            chunk_size=chunk_size,
        )
        return

    create_table(dataset=dataset, table=table, schema=schema, client=client)
    insert(
        dataset=dataset,
        table=table,
        records=records,
        client=client,
        chunk_size=chunk_size,
    )


def create_table_from_query(
    query: str,
    dataset: str,
    table: str,
    json_key: dict[str, str] | None = None,
    client: BigQueryClient | None = None,
    timeout: int = FIVE_MINUTES,
) -> None:
    """Create a table from a query.

    Args:
        query: query to create a table from. should be a select statement.
        dataset: dataset name.
        table: table name.
        json_key: json key with gcp credentials.
        client: client to connect to gcp.
        timeout: timeout in seconds. Default is 5 minutes.

    Raises:
        BigQueryClientException if the operation is not successful.

    """
    client = client or BigQueryClient(json_key=json_key or {})
    create_or_replace_query = f"create or replace table {dataset}.{table} as {query}"
    client.run_command(command_sql=create_or_replace_query, timeout=timeout)


def select(
    query: str,
    timeout: int = 10,
    json_key: dict[str, str] | None = None,
    client: BigQueryClient | None = None,
) -> ListJsonType:
    """Run a select statement query and return its results.

    Args:
        query: select statement to run against BigQuery.
        timeout: seconds to wait for the query to finish.
        json_key: json key with gcp credentials.
        client: client to connect to gcp.

    Returns:
        Records resulted from query.

    """
    logger.info("Query execution started ...")
    client = client or BigQueryClient(json_key=json_key or {})
    records = client.query(query, timeout=timeout)
    logger.info(f"Query execution finished, {len(records)} records returned!")
    return records


def query_to_df(
    query: str,
    json_key: dict[str, str] | None = None,
    client: BigQueryClient | None = None,
) -> pd.DataFrame:
    """Run a query and return its results as a pandas DataFrame.

    Args:
        query: query to run against BigQuery.
        json_key: json key with gcp credentials.
        client: client to connect to gcp.

    Returns:
        A pandas DataFrame with the query results.

    """
    records = select(query=query, json_key=json_key, client=client)
    return pd.DataFrame(records)
