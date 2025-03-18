import datetime
from unittest import mock

import pytest
import time_machine
from gcloud.aio.storage import Storage as AsyncStorageClient
from google.cloud.storage import Blob, Bucket, Client

from gcpde import gcs


def test_upload_file():
    # arrange
    mock_client = mock.Mock(spec_set=Client)
    mock_bucket = mock.Mock(spec_set=Bucket)
    mock_blob = mock.Mock(spec_set=Blob)

    mock_client.get_bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob

    # act
    gcs.upload_file(
        content='{"id": "1"}\n{"id": "2"}',
        bucket_name="my-bucket",
        file_name="path/to/file.jsonl",
        client=mock_client,
    )

    # assert
    mock_client.get_bucket.assert_called_with("my-bucket")
    mock_bucket.blob.assert_called_with("path/to/file.jsonl")
    mock_blob.upload_from_string.assert_called_with(
        '{"id": "1"}\n{"id": "2"}', content_type="application/json"
    )


@time_machine.travel(
    destination=datetime.datetime(2022, 1, 1, tzinfo=datetime.timezone.utc), tick=False
)
@mock.patch("gcpde.gcs._upload_file", autospec=True)
def test_add_records_to_dataset(mock_upload_file: mock.Mock):
    # arrange
    mock_client = mock.Mock(spec_set=Client)

    def custom_filename_builder(
        dataset: str, datetime_partition: gcs.DateTimePartitions
    ) -> str:
        return f"custom_{dataset}_{datetime_partition.year}.jsonl"

    # act
    gcs.add_records_to_dataset(
        bucket_name="my-bucket",
        json_str_records=['{"id": "1"}', '{"id": "2"}'],
        dataset="dataset",
        version="1",
        client=mock_client,
        build_file_name=custom_filename_builder,
    )

    # assert
    mock_upload_file.assert_called_with(
        content='{"id": "1"}\n{"id": "2"}',
        bucket_name="my-bucket",
        file_name="dataset/version=1/year=2022/month=1/day=1/custom_dataset_2022.jsonl",
        client=mock_client,
    )


@mock.patch(
    "google.oauth2.service_account.Credentials.from_service_account_info", autospec=True
)
@mock.patch("google.cloud.storage.client.Client", autospec=True)
def test__get_gcs_client(
    mock_client: mock.Mock, mock_from_service_account_info: mock.Mock
):
    # act
    client = gcs._get_gcs_client(json_key={"auth": "key"}, client=mock_client)

    # assert
    mock_from_service_account_info.assert_called_with({"auth": "key"})
    assert isinstance(client, Client)


@mock.patch("gcpde.gcs.AsyncStorageClient", autospec=True)
@mock.patch("gcloud.aio.storage.bucket.Bucket.list_blobs", autospec=True)
def test_get_dataset(mock_list_blobs: mock.AsyncMock, mock_client: mock.Mock):
    # arrange
    async def mock_list_blobs_se(*args, **kwargs):
        return [
            "my-dataset/version=1/year=2022/month=1/day=1/"
            "my-dataset__2022-01-01T00:00.jsonl",
            "my-dataset/version=1/year=2022/month=1/day=1/"
            "my-dataset__2022-01-01T01:00.jsonl",
            "my-dataset/version=1/year=2022/month=1/day=1/"
            "my-dataset__2022-01-01T03:00.jsonl",
        ]

    mock_list_blobs.side_effect = mock_list_blobs_se

    async def mock_download(**kwargs):
        return b'{"id": "1"}\n{"id": "2"}'

    mock_client.download.side_effect = mock_download

    # act
    output_records = gcs.get_dataset(
        bucket_name="my-bucket",
        dataset="my-dataset",
        json_key={"auth": "key"},
        client=mock_client,
    )

    # assert
    assert output_records == [{"id": "1"}, {"id": "2"}] * 3


def test__get_latest_path_files():
    # arrange
    file_paths = [
        "dataset/version=1/year=2022/month=12/day=10/file_1.jsonl",
        "dataset/version=1/year=2022/month=12/day=10/file_2.jsonl",
        "dataset/version=1/year=2022/month=12/day=9/file_1.jsonl",
        "dataset/version=1/year=2022/month=9/day=1/file_1.jsonl",
    ]
    target_file_paths = [
        "dataset/version=1/year=2022/month=12/day=10/file_1.jsonl",
        "dataset/version=1/year=2022/month=12/day=10/file_2.jsonl",
    ]

    # act
    output_file_paths = gcs._get_latest_path_files(file_paths=file_paths)

    # assert
    assert output_file_paths == target_file_paths


def test_download_files():
    # arrange
    mock_client = mock.Mock(spec_set=AsyncStorageClient)

    async def _mock_download(**kwargs):
        return b'{"id": "1"}\n{"id": "2"}'

    mock_client.download.side_effect = _mock_download

    # act
    output = gcs.download_files(
        bucket_name="my-bucket",
        file_paths=[
            "dataset/version=1/year=2022/month=12/day=10/file_1.jsonl",
            "dataset/version=1/year=2022/month=12/day=10/file_2.jsonl",
        ],
        client=mock_client,
    )

    # assert
    count = 1
    for file in output:
        assert file.file_bytes == b'{"id": "1"}\n{"id": "2"}'
        assert (
            file.path
            == f"dataset/version=1/year=2022/month=12/day=10/file_{count}.jsonl"
        )
        count += 1


@mock.patch("gcpde.gcs._async_list_files", autospec=True)
def test_list_files(mock__async_list_files: mock.AsyncMock):
    # arrange
    mock_client = mock.Mock(spec_set=AsyncStorageClient)

    async def _mock_list_files(**kwargs):
        return [
            "dataset/version=1/year=2022/month=12/day=10/file_1.jsonl",
            "dataset/version=1/year=2022/month=12/day=10/file_2.jsonl",
        ]

    mock__async_list_files.side_effect = _mock_list_files

    # act
    output = gcs.list_files(
        bucket_name="my-bucket",
        prefix="dataset/version=1/year=2022/month=12/day=10/",
        client=mock_client,
    )

    # assert
    assert output == [
        "dataset/version=1/year=2022/month=12/day=10/file_1.jsonl",
        "dataset/version=1/year=2022/month=12/day=10/file_2.jsonl",
    ]


def test__check_auth_args_exception():
    # arrange
    json_key = None
    client = None

    # act and assert
    with pytest.raises(ValueError):
        gcs._check_auth_args(json_key=json_key, client=client)


def test_add_records_to_dataset_no_records():
    """Add coverage for the case where there are no records are passed to function."""
    gcs.add_records_to_dataset(
        bucket_name="my-bucket",
        json_str_records=[],
        dataset="dataset",
        json_key={"auth": "data"},
    )


@pytest.mark.asyncio
@mock.patch("aiohttp.ClientSession")
async def test__async_copy_file_from_bucket_to_bucket_no_client(mock_client_session):
    # arrange
    mock_session = mock.AsyncMock()
    mock_storage_client = mock.AsyncMock(spec=AsyncStorageClient)

    # Mock the copy method specifically
    mock_storage_client.copy = mock.AsyncMock()

    mock_client_session.return_value.__aenter__.return_value = mock_session

    with mock.patch(
        "gcpde.gcs.AsyncStorageClient",
        return_value=mock_storage_client,
    ):
        # act
        await gcs._async_copy_file_from_bucket_to_bucket(
            source_bucket_name="source_bucket",
            source_prefix="source_prefix",
            destination_bucket_name="destination_bucket",
            destination_prefix="destination_prefix",
            timeout=300,
            client=None,
        )

    # assert
    mock_storage_client.copy.assert_awaited_once_with(
        bucket="source_bucket",
        object_name="source_prefix",
        destination_bucket="destination_bucket",
        new_name="destination_prefix",
        timeout=300,
    )


@mock.patch("gcpde.gcs._async_copy_file_from_bucket_to_bucket")
@mock.patch("asyncio.run")
def test_copy_files_from_bucket_to_bucket(
    mock_asyncio_run: mock.Mock,
    mock_async_copy: mock.Mock,
):
    # arrange
    mock_client = mock.Mock(spec_set=AsyncStorageClient)

    # act
    gcs.copy_files_from_bucket_to_bucket(
        source_bucket_name="source_bucket",
        source_prefix="source_prefix",
        destination_bucket_name="destination_bucket",
        destination_prefix="destination_prefix",
        client=mock_client,
    )

    # assert
    mock_asyncio_run.assert_called_once()
    mock_async_copy.assert_called_once_with(
        source_bucket_name="source_bucket",
        source_prefix="source_prefix",
        destination_bucket_name="destination_bucket",
        destination_prefix="destination_prefix",
        client=mock_client,
        timeout=300,
    )
