"""Google Cloud Storage client using the official Google Cloud Storage package."""

import asyncio
import io
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Protocol, Tuple

import tenacity
from aiohttp import ClientResponseError, ClientSession, ClientTimeout
from gcloud.aio.storage import Storage as AsyncStorageClient
from google.cloud.storage import Client as StorageClient
from google.oauth2 import service_account
from loguru import logger
from pydantic import BaseModel

from gcpde.base import get_utc_now
from gcpde.types import ListJsonType


class GCSDownloadedFile(BaseModel):
    """GCS downloaded file."""

    file_bytes: bytes
    path: str = ""


class GCSHMACKeyConfig(BaseModel):
    """GCS HMAC Key configuration."""

    access_key: str
    secret: str


def _apply_zero_padding(value: int) -> str:
    return str(value) if value > 9 else f"0{value}"


class DateTimePartitions(BaseModel):
    """Datetime partition values for gcs path."""

    year: int
    month: int
    day: int
    hour: int

    def __str__(self) -> str:
        """ISO datatime format."""
        month = _apply_zero_padding(self.month)
        day = _apply_zero_padding(self.day)
        hour = _apply_zero_padding(self.hour)
        return f"{self.year}-{month}-{day}T{hour}:00"


def _build_file_path(
    dataset: str, version: str, datetime_partition: DateTimePartitions
) -> str:
    partitions_path = (
        f"version={version}/"
        f"year={datetime_partition.year}/"
        f"month={datetime_partition.month}/"
        f"day={datetime_partition.day}"
    )
    return f"{dataset}/{partitions_path}/"


class BuildFileNameProtocol(Protocol):
    """Protocol for building file names."""

    def __call__(self, *args: Any, **kwargs: Any) -> str:
        """Build a file name with any number of arguments.

        Returns:
            str: The generated file name
        """
        ...  # pragma: no cover


def _build_file_name(dataset: str, datetime_partition: DateTimePartitions) -> str:
    return f"{dataset}__{datetime_partition}.jsonl"


def _get_gcs_client(
    json_key: Dict[str, str], client: Optional[StorageClient] = None
) -> StorageClient:
    credentials = service_account.Credentials.from_service_account_info(info=json_key)  # type: ignore[no-untyped-call]
    storage_client = (client or StorageClient)(credentials=credentials)
    return storage_client


def _upload_file(
    content: str | bytes,
    bucket_name: str,
    file_name: str,
    client: StorageClient,
    content_type: str = "application/json",
) -> None:
    client.get_bucket(bucket_name).blob(file_name).upload_from_string(
        content, content_type=content_type
    )


def upload_file(
    content: str | bytes,
    bucket_name: str,
    file_name: str,
    json_key: Optional[Dict[str, str]] = None,
    client: Optional[StorageClient] = None,
    content_type: str = "application/json",
) -> None:
    """Upload a file to gcs.

    Args:
        content: content to upload.
        bucket_name: the name of the dataset.
        file_name: the name of the file with full path.
        json_key: auth to connect to gcp.
        client: client to user to make the API requests.
        content_type: content type of the file.

    """
    _check_auth_args(json_key=json_key, client=client)
    _upload_file(
        content=content,
        bucket_name=bucket_name,
        file_name=file_name,
        client=client or _get_gcs_client(json_key=json_key or {}),
        content_type=content_type,
    )


def add_records_to_dataset(
    bucket_name: str,
    json_str_records: List[str],
    dataset: str,
    version: str = "1",
    datetime_partition: Optional[DateTimePartitions] = None,
    json_key: Optional[Dict[str, str]] = None,
    client: Optional[StorageClient] = None,
    build_file_name: Optional[BuildFileNameProtocol] = None,
) -> None:
    """Add a jsonl file to gcs using our file path conventions.

    Args:
        json_str_records: list of records as serialized json strings.
        dataset: identification for the dataset.
        version: schema version of the dataset.
        datetime_partition: datetime partition values for gcs path.
        bucket_name: temporal partitioning for the object path.
        json_key: json key with gcp credentials.
        client: google storage client to use for connecting to the API.
        build_file_name: optional callable that generates the file name.
            If not provided, defaults to internal _build_file_name method.

    """
    _check_auth_args(json_key=json_key, client=client)
    if not json_str_records:
        logger.warning("No records to add! (empty collection given)")
        return
    logger.info(
        f"Adding {len(json_str_records)} records on {dataset} dataset "
        f"version={version} on gcs..."
    )
    ts_now = get_utc_now()
    datetime_partition = datetime_partition or DateTimePartitions(
        year=ts_now.year, month=ts_now.month, day=ts_now.day, hour=ts_now.hour
    )

    file_path = _build_file_path(
        dataset=dataset, version=version, datetime_partition=datetime_partition
    )
    file_name = (
        build_file_name()
        if build_file_name
        else _build_file_name(dataset=dataset, datetime_partition=datetime_partition)
    )

    jsonl_file_str = "\n".join(json_str_records)

    _upload_file(
        content=jsonl_file_str,
        bucket_name=bucket_name,
        file_name=file_path + file_name,
        client=client or _get_gcs_client(json_key=json_key or {}),
    )
    logger.info(f"File {file_name} created successfully!")


@tenacity.retry(
    retry=tenacity.retry_if_exception_type(
        exception_types=(
            ClientResponseError,
            asyncio.TimeoutError,
            asyncio.CancelledError,
        )
    ),
    wait=tenacity.wait_exponential(max=5),
    stop=tenacity.stop_after_attempt(7),
    before_sleep=tenacity.before_sleep_log(
        logger=logger,  # type: ignore[arg-type]
        log_level=logging.WARNING,
    ),
)
async def _async_download_file(
    bucket_name: str,
    file_path: str,
    client: AsyncStorageClient,
    timeout: int,
) -> GCSDownloadedFile:
    logger.debug(f"Downloading file {file_path} ...")
    file = await client.download(
        bucket=bucket_name, object_name=file_path, timeout=timeout
    )
    logger.debug(f"Downloading file {file_path} completed!")
    gcs_file = GCSDownloadedFile(file_bytes=file, path=file_path)
    return gcs_file


async def _async_copy_file_from_bucket_to_bucket(
    source_bucket_name: str,
    source_prefix: str,
    destination_bucket_name: str,
    destination_prefix: str,
    timeout: int = 300,
    client: Optional[AsyncStorageClient] = None,
) -> None:
    logger.debug(
        f"Copying file from {source_bucket_name} to {destination_bucket_name}..."
    )
    session_timeout = ClientTimeout(total=None, sock_connect=timeout, sock_read=timeout)

    async with ClientSession(timeout=session_timeout) as session:
        storage_client = client or AsyncStorageClient(session=session)
        await storage_client.copy(
            bucket=source_bucket_name,
            object_name=source_prefix,
            destination_bucket=destination_bucket_name,
            new_name=destination_prefix,
            timeout=timeout,
        )

    logger.debug(
        f"Copying files from {source_bucket_name} to "
        f"{destination_bucket_name} completed!"
    )


async def _async_download_files(
    bucket_name: str,
    file_paths: List[str],
    client: AsyncStorageClient,
    timeout: int,
) -> list[GCSDownloadedFile]:
    tasks = [
        _async_download_file(
            bucket_name=bucket_name,
            file_path=file_path,
            client=client,
            timeout=timeout,
        )
        for file_path in file_paths
    ]
    downloaded_files_w_path: list[GCSDownloadedFile] = await asyncio.gather(*tasks)
    return downloaded_files_w_path


async def _async_download_files_handling_auth(
    bucket_name: str,
    file_paths: List[str],
    json_key: Optional[Dict[str, str]],
    client: Optional[AsyncStorageClient],
    timeout: int,
) -> list[GCSDownloadedFile]:
    _check_auth_args(json_key=json_key, client=client)
    session_timeout = ClientTimeout(total=None, sock_connect=timeout, sock_read=timeout)
    async with ClientSession(timeout=session_timeout) as session:
        client = client or AsyncStorageClient(
            service_file=io.StringIO(json.dumps(json_key)),
            session=session,
        )
        return await _async_download_files(
            bucket_name=bucket_name,
            file_paths=file_paths,
            client=client,
            timeout=timeout,
        )


def download_files(
    bucket_name: str,
    file_paths: List[str],
    json_key: Optional[Dict[str, str]] = None,
    client: Optional[AsyncStorageClient] = None,
    timeout: int = 300,
) -> list[GCSDownloadedFile]:
    """Download files from gcs using our file path conventions."""
    return asyncio.run(
        _async_download_files_handling_auth(
            bucket_name=bucket_name,
            file_paths=file_paths,
            json_key=json_key,
            client=client,
            timeout=timeout,
        )
    )


def _deserialize_jsonl_files(files: list[GCSDownloadedFile]) -> ListJsonType:
    records = []
    for file in files:
        for record in file.file_bytes.splitlines():
            records.append(json.loads(record))
    return records


def _check_auth_args(
    json_key: Optional[Dict[str, str]], client: Optional[AsyncStorageClient]
) -> None:
    if not json_key and not client:
        raise ValueError(
            "You must provide either a json_key or a client to connect to gcs."
        )


async def _async_list_files(
    bucket_name: str,
    prefix: str,
    client: AsyncStorageClient,
    api_params: Optional[Dict[str, Any]] = None,
    updated_after: Optional[datetime] = None,
    updated_before: Optional[datetime] = None,
) -> List[str]:
    extra_api_params = api_params or {}
    items = []
    next_page_token = None
    while True:
        params = {"prefix": prefix, "delimiter": "/", **extra_api_params}
        if next_page_token:
            params["pageToken"] = next_page_token

        search_result = await client.list_objects(
            bucket=bucket_name,
            params=params,
        )
        items.extend(search_result["items"])
        next_page_token = search_result.get("nextPageToken")

        if not next_page_token:
            break

    if updated_after:
        updated_after = updated_after.replace(tzinfo=timezone.utc)
        items = [
            item
            for item in items
            if datetime.fromisoformat(item["updated"]) >= updated_after
        ]
    if updated_before:
        updated_before = updated_before.replace(tzinfo=timezone.utc)
        items = [
            item
            for item in items
            if datetime.fromisoformat(item["updated"]) <= updated_before
        ]

    file_paths = [item["name"] for item in items if not item["name"].endswith("/")]
    return file_paths


async def _async_list_files_handling_auth(
    bucket_name: str,
    prefix: str,
    json_key: Optional[Dict[str, str]],
    client: Optional[AsyncStorageClient],
    timeout: int,
    api_params: Optional[Dict[str, Any]] = None,
    updated_after: Optional[datetime] = None,
) -> List[str]:
    _check_auth_args(json_key=json_key, client=client)
    session_timeout = ClientTimeout(total=None, sock_connect=timeout, sock_read=timeout)
    async with ClientSession(timeout=session_timeout) as session:
        client = client or AsyncStorageClient(
            service_file=io.StringIO(json.dumps(json_key)),
            session=session,
        )
        return await _async_list_files(
            bucket_name=bucket_name,
            prefix=prefix,
            client=client,
            api_params=api_params,
            updated_after=updated_after,
        )


def list_files(
    bucket_name: str,
    prefix: str,
    json_key: Optional[Dict[str, str]] = None,
    client: Optional[AsyncStorageClient] = None,
    timeout: int = 300,
    api_params: Optional[Dict[str, Any]] = None,
    updated_after: Optional[datetime] = None,
) -> List[str]:
    """List files on gcs from a given prefix.

    Args:
        bucket_name: the name of the dataset.
        prefix: prefix to filter the files.
        json_key: auth to connect to gcp.
        client: client to user to make the API requests.
        timeout: timeout for the API requests.
        api_params: parameters for the API request (ref. https://cloud.google.com/storage/docs/json_api/v1/objects/list).
        updated_after: filter files updated after this datetime.
    """
    logger.info(f"Listing files from {prefix} on {bucket_name} bucket...")
    file_paths = asyncio.run(
        _async_list_files_handling_auth(
            bucket_name=bucket_name,
            prefix=prefix,
            json_key=json_key,
            client=client,
            timeout=timeout,
            api_params=api_params,
            updated_after=updated_after,
        )
    )
    logger.info(f"{len(file_paths)} files found.")
    return file_paths


def _get_latest_path_files(file_paths: List[str]) -> List[str]:
    latest_partition_values: Tuple[int, ...] = sorted(
        {
            tuple(map(int, partitions_dict.values()))
            for partitions_dict in [
                dict(
                    partition.split("=")
                    for partition in [
                        section for section in path.split("/") if "=" in section
                    ]
                )
                for path in file_paths
            ]
        },
        reverse=True,
    ).pop(0)
    return [
        file_path
        for file_path in file_paths
        if "version={}/year={}/month={}/day={}".format(*latest_partition_values)
        in file_path
    ]


async def _async_get_dataset(
    version: str,
    dataset: Optional[str],
    bucket_name: str,
    latest_partition_only: bool,
    json_key: Optional[Dict[str, str]],
    client: Optional[AsyncStorageClient],
    timeout: int,
) -> ListJsonType:
    _check_auth_args(json_key=json_key, client=client)
    session_timeout = ClientTimeout(total=None, sock_connect=timeout, sock_read=timeout)
    async with ClientSession(timeout=session_timeout) as session:
        client = client or AsyncStorageClient(
            service_file=io.StringIO(json.dumps(json_key)),
            session=session,
        )
        file_paths = await _async_list_files(
            bucket_name=bucket_name,
            prefix=f"{dataset}/version={version}" if dataset else f"version={version}",
            client=client,
        )
        target_file_paths = (
            _get_latest_path_files(file_paths=file_paths)
            if latest_partition_only
            else file_paths
        )
        logger.debug(f"{len(target_file_paths)} files found in path.")
        downloaded_files = await _async_download_files(
            bucket_name=bucket_name,
            file_paths=target_file_paths,
            client=client,
            timeout=timeout,
        )
        json_dict_results = _deserialize_jsonl_files(files=downloaded_files)
        return json_dict_results


def get_dataset(
    bucket_name: str,
    dataset: Optional[str] = None,
    version: str = "1",
    latest_partition_only: bool = False,
    json_key: Optional[Dict[str, str]] = None,
    client: Optional[AsyncStorageClient] = None,
    timeout: int = 300,
) -> ListJsonType:
    """Get a dataset from gcs using our file path conventions.

    Args:
        dataset: identification for the dataset.
        version: the schema version of the dataset.
        bucket_name: the name of the dataset.
        latest_partition_only: if True, return only the latest partition data.
        json_key: auth to connect to gcp.
        client: client to user to make the API requests.
        timeout: timeout for the API requests.

    """
    logger.info(f"Reading everything from {dataset} dataset version={version}...")
    records = asyncio.run(
        _async_get_dataset(
            dataset=dataset,
            version=version,
            bucket_name=bucket_name,
            latest_partition_only=latest_partition_only,
            json_key=json_key,
            client=client,
            timeout=timeout,
        )
    )
    logger.info(f"Collection finished! {len(records)} records retrieved.")
    return records


def copy_files_from_bucket_to_bucket(
    source_bucket_name: str,
    source_prefix: str,
    destination_bucket_name: str,
    destination_prefix: str,
    client: Optional[AsyncStorageClient] = None,
    timeout: int = 300,
) -> None:
    """Copy files from one bucket to another.

    Args:
        source_bucket_name: the name of the source bucket.
        source_prefix: the prefix of the source files.
        destination_bucket_name: the name of the destination bucket.
        destination_prefix: the prefix of the destination files.
        client: the client to use to make the API requests.
        timeout: timeout for the API requests.

    """
    asyncio.run(
        _async_copy_file_from_bucket_to_bucket(
            source_bucket_name=source_bucket_name,
            source_prefix=source_prefix,
            destination_bucket_name=destination_bucket_name,
            destination_prefix=destination_prefix,
            client=client,
            timeout=timeout,
        )
    )
