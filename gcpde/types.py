"""Custom types."""

from typing import Any

from google.cloud import bigquery

ListJsonType = list[dict[str, Any]]

BigQuerySchema = list[bigquery.SchemaField]

PagedQueryResult = tuple[ListJsonType, str | None]
"""A page of query results and an optional next page token."""
