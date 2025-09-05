"""Custom types."""

from typing import Any

from google.cloud import bigquery

ListJsonType = list[dict[str, Any]]

BigQuerySchema = list[bigquery.SchemaField]
