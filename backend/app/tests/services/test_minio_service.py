import json
import logging
from unittest.mock import MagicMock, call, patch  # Added call

import pytest
from minio.error import S3Error
from minio.notificationconfig import NotificationConfig

from app.core.config import Settings
from app.services import minio_service


# Fixture for a logger instance
@pytest.fixture
def logger_mock():
    return MagicMock(spec=logging.Logger)


# Fixture for mock application settings
@pytest.fixture
def mock_settings():
    settings = MagicMock(spec=Settings)
    settings.MINIO_URL_INTERNAL = "http://minio:9000"
    settings.MINIO_ROOT_USER = "testuser"
    settings.MINIO_ROOT_PASSWORD = "testpassword"
    settings.MINIO_PRIMARY_BUCKET = "uploads"
    settings.MINIO_SECONDARY_BUCKET = "segments"
    settings.MINIO_TERTIARY_BUCKET = "transcripts"
    settings.FRONTEND_ORIGIN = "http://frontend:1234"
    settings.MINIO_KAFKA_NOTIFICATION_ARN = "arn:minio:sqs::PRIMARY:kafka"
    return settings


# Fixture for a mock Minio client
@pytest.fixture
def minio_client_mock():
    client = MagicMock(spec=minio_service.Minio)
    # Configure bucket_exists to return True by default for some tests
    client.bucket_exists.return_value = True
    return client


# Tests for _parse_endpoint
def test_parse_endpoint_http():
    endpoint, secure = minio_service._parse_endpoint("http://localhost:9000")  # pylint: disable=protected-access
    assert endpoint == "localhost:9000"
    assert not secure


def test_parse_endpoint_https():
    endpoint, secure = minio_service._parse_endpoint("https://secure.minio.host")  # pylint: disable=protected-access
    assert endpoint == "secure.minio.host"
    assert secure


def test_parse_endpoint_no_port():
    endpoint, secure = minio_service._parse_endpoint("http://miniohost")  # pylint: disable=protected-access
    assert endpoint == "miniohost"
    assert not secure


def test_parse_endpoint_invalid_url_no_scheme():
    with pytest.raises(ValueError, match="Invalid MINIO_URL_INTERNAL value"):
        minio_service._parse_endpoint("localhost:9000")  # pylint: disable=protected-access


def test_parse_endpoint_invalid_url_no_hostname():
    with pytest.raises(ValueError, match="Invalid MINIO_URL_INTERNAL value"):
        minio_service._parse_endpoint("http://")  # pylint: disable=protected-access


# Tests for get_minio_client
@patch("app.services.minio_service.Minio")
def test_get_minio_client_success(mock_minio_constructor, mock_settings, logger_mock):  # pylint: disable=redefined-outer-name
    endpoint, secure = minio_service._parse_endpoint(mock_settings.MINIO_URL_INTERNAL)  # pylint: disable=protected-access

    client_instance_mock = MagicMock()
    mock_minio_constructor.return_value = client_instance_mock

    client = minio_service.get_minio_client(mock_settings, logger_mock)

    mock_minio_constructor.assert_called_once_with(
        endpoint,
        access_key=mock_settings.MINIO_ROOT_USER,
        secret_key=mock_settings.MINIO_ROOT_PASSWORD,
        secure=secure,
    )
    assert client == client_instance_mock
    logger_mock.debug.assert_called_with(
        f"Minio client configured for endpoint: '{endpoint}', secure: {secure}"
    )


def test_get_minio_client_missing_credentials(mock_settings, logger_mock):  # pylint: disable=redefined-outer-name
    mock_settings.MINIO_ROOT_USER = None
    with pytest.raises(ValueError, match="MinIO credentials not set in settings."):
        minio_service.get_minio_client(mock_settings, logger_mock)
    logger_mock.error.assert_called_once_with(
        "MINIO_ROOT_USER and MINIO_ROOT_PASSWORD environment variables must be set."
    )

    mock_settings.MINIO_ROOT_USER = "user"
    mock_settings.MINIO_ROOT_PASSWORD = None
    with pytest.raises(ValueError, match="MinIO credentials not set in settings."):
        minio_service.get_minio_client(mock_settings, logger_mock)
    # error logger called twice now
    assert logger_mock.error.call_count == 2


# Tests for ensure_bucket_exists
def test_ensure_bucket_exists_creates_bucket(minio_client_mock, logger_mock):  # pylint: disable=redefined-outer-name
    bucket_name = "new-bucket"
    minio_client_mock.bucket_exists.return_value = False

    minio_service.ensure_bucket_exists(minio_client_mock, bucket_name, logger_mock)

    minio_client_mock.bucket_exists.assert_called_once_with(bucket_name)
    minio_client_mock.make_bucket.assert_called_once_with(bucket_name)
    logger_mock.info.assert_called_once_with(
        f"Bucket '{bucket_name}' created successfully."
    )


def test_ensure_bucket_exists_already_exists(minio_client_mock, logger_mock):  # pylint: disable=redefined-outer-name
    bucket_name = "existing-bucket"
    minio_client_mock.bucket_exists.return_value = True  # Default, but explicit here

    minio_service.ensure_bucket_exists(minio_client_mock, bucket_name, logger_mock)

    minio_client_mock.bucket_exists.assert_called_once_with(bucket_name)
    minio_client_mock.make_bucket.assert_not_called()
    logger_mock.info.assert_called_once_with(f"Bucket '{bucket_name}' already exists.")


def test_ensure_bucket_exists_s3error_on_check(minio_client_mock, logger_mock):  # pylint: disable=redefined-outer-name
    bucket_name = "error-bucket"
    s3_error = S3Error(
        code="TestCode",
        message="Test S3 error on check",
        resource=bucket_name,
        request_id="test_req_id",
        host_id="test_host_id",
        response=MagicMock(status=500),
    )
    minio_client_mock.bucket_exists.side_effect = s3_error

    with pytest.raises(S3Error, match="Test S3 error on check"):
        minio_service.ensure_bucket_exists(minio_client_mock, bucket_name, logger_mock)

    logger_mock.error.assert_called_once_with(
        f"Error creating or checking bucket '{bucket_name}': {s3_error}"
    )


def test_ensure_bucket_exists_s3error_on_create(minio_client_mock, logger_mock):  # pylint: disable=redefined-outer-name
    bucket_name = "error-bucket-create"
    minio_client_mock.bucket_exists.return_value = False
    s3_error = S3Error(
        code="TestCodeCreate",
        message="Test S3 error on create",
        resource=bucket_name,
        request_id="test_req_id_create",
        host_id="test_host_id_create",
        response=MagicMock(status=500),
    )
    minio_client_mock.make_bucket.side_effect = s3_error

    with pytest.raises(S3Error, match="Test S3 error on create"):
        minio_service.ensure_bucket_exists(minio_client_mock, bucket_name, logger_mock)

    logger_mock.error.assert_called_once_with(
        f"Error creating or checking bucket '{bucket_name}': {s3_error}"
    )


# Tests for set_public_download_policy
def test_set_public_download_policy_new_policy(minio_client_mock, logger_mock):  # pylint: disable=redefined-outer-name
    bucket_name = "public-bucket"
    minio_client_mock.get_bucket_policy.side_effect = S3Error(
        code="NoSuchBucketPolicy",
        message="No such bucket policy",
        resource=bucket_name,
        request_id="123",
        host_id="abc",
        response=MagicMock(status=404),
    )
    expected_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": ["*"]},
                "Action": ["s3:GetObject"],
                "Resource": [f"arn:aws:s3:::{bucket_name}/*"],
            }
        ],
    }

    minio_service.set_public_download_policy(
        minio_client_mock, bucket_name, logger_mock
    )

    minio_client_mock.get_bucket_policy.assert_called_once_with(bucket_name)
    minio_client_mock.set_bucket_policy.assert_called_once_with(
        bucket_name, json.dumps(expected_policy)
    )
    assert (
        call(f"No existing policy for bucket '{bucket_name}'. Setting new policy.")
        in logger_mock.info.call_args_list
    )
    assert (
        call(f"Public read (download) policy set for bucket '{bucket_name}'.")
        in logger_mock.info.call_args_list
    )


def test_set_public_download_policy_existing_matches(minio_client_mock, logger_mock):  # pylint: disable=redefined-outer-name
    bucket_name = "public-bucket-exists"
    current_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": ["*"]},
                "Action": ["s3:GetObject"],
                "Resource": [f"arn:aws:s3:::{bucket_name}/*"],
            }
        ],
    }
    minio_client_mock.get_bucket_policy.return_value = json.dumps(current_policy)

    minio_service.set_public_download_policy(
        minio_client_mock, bucket_name, logger_mock
    )

    minio_client_mock.get_bucket_policy.assert_called_once_with(bucket_name)
    minio_client_mock.set_bucket_policy.assert_not_called()
    logger_mock.info.assert_called_once_with(
        f"Public read (download) policy for bucket '{bucket_name}' is already correctly set."
    )


def test_set_public_download_policy_existing_different(minio_client_mock, logger_mock):  # pylint: disable=redefined-outer-name
    bucket_name = "public-bucket-diff"
    current_policy = {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Deny"}],
    }  # Different policy
    minio_client_mock.get_bucket_policy.return_value = json.dumps(current_policy)

    expected_policy_json = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": ["*"]},
                    "Action": ["s3:GetObject"],
                    "Resource": [f"arn:aws:s3:::{bucket_name}/*"],
                }
            ],
        }
    )

    minio_service.set_public_download_policy(
        minio_client_mock, bucket_name, logger_mock
    )
    minio_client_mock.set_bucket_policy.assert_called_once_with(
        bucket_name, expected_policy_json
    )
    assert (
        call(f"Public read (download) policy set for bucket '{bucket_name}'.")
        in logger_mock.info.call_args_list
    )


def test_set_public_download_policy_s3error_on_get_other_than_no_such_policy(
    minio_client_mock, logger_mock
):  # pylint: disable=redefined-outer-name
    bucket_name = "public-bucket-error-get"
    s3_error = S3Error(
        code="AccessDenied",
        message="Access Denied",
        resource=bucket_name,
        request_id="123",
        host_id="abc",
        response=MagicMock(status=403),
    )
    minio_client_mock.get_bucket_policy.side_effect = s3_error

    expected_policy_json = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": ["*"]},
                    "Action": ["s3:GetObject"],
                    "Resource": [f"arn:aws:s3:::{bucket_name}/*"],
                }
            ],
        }
    )

    minio_service.set_public_download_policy(
        minio_client_mock, bucket_name, logger_mock
    )

    logger_mock.warning.assert_called_once_with(
        f"Could not retrieve current policy for bucket '{bucket_name}': {s3_error}. Proceeding to set policy."
    )
    minio_client_mock.set_bucket_policy.assert_called_once_with(
        bucket_name, expected_policy_json
    )


def test_set_public_download_policy_s3error_on_set(minio_client_mock, logger_mock):  # pylint: disable=redefined-outer-name
    bucket_name = "public-bucket-error-set"
    minio_client_mock.get_bucket_policy.side_effect = S3Error(
        code="NoSuchBucketPolicy",
        message="No policy",
        resource=bucket_name,
        request_id="1",
        host_id="a",
        response=MagicMock(status=404),
    )
    s3_error_set = S3Error(
        code="InternalError",
        message="Cannot set policy",
        resource=bucket_name,
        request_id="2",
        host_id="b",
        response=MagicMock(status=500),
    )
    minio_client_mock.set_bucket_policy.side_effect = s3_error_set

    with pytest.raises(S3Error, match="Cannot set policy"):
        minio_service.set_public_download_policy(
            minio_client_mock, bucket_name, logger_mock
        )

    logger_mock.error.assert_called_once_with(
        f"Error setting public read policy for bucket '{bucket_name}': {s3_error_set}"
    )


# Tests for set_cors_policy
def test_set_cors_policy_logs_skip_message(minio_client_mock, logger_mock):  # pylint: disable=redefined-outer-name
    bucket_name = "cors-bucket"
    frontend_origin = "http://my.frontend.com"
    minio_service.set_cors_policy(
        minio_client_mock, bucket_name, frontend_origin, logger_mock
    )
    logger_mock.info.assert_called_once_with(
        "Skipping bucket-level CORS. "
        'If you need CORS use `mc admin config set <alias>/ api cors_allow_origin="%s"` '
        "or the MINIO_API_CORS_ALLOW_ORIGIN env-var.",
        frontend_origin,
    )


def test_set_cors_policy_logs_skip_message_no_origin(minio_client_mock, logger_mock):  # pylint: disable=redefined-outer-name
    bucket_name = "cors-bucket"
    minio_service.set_cors_policy(minio_client_mock, bucket_name, None, logger_mock)
    logger_mock.info.assert_called_once_with(
        "Skipping bucket-level CORS. "
        'If you need CORS use `mc admin config set <alias>/ api cors_allow_origin="%s"` '
        "or the MINIO_API_CORS_ALLOW_ORIGIN env-var.",
        "<origin>",
    )


# Tests for set_bucket_notifications
def test_set_bucket_notifications_success(
    minio_client_mock, mock_settings, logger_mock
):  # pylint: disable=redefined-outer-name
    bucket_name = mock_settings.MINIO_PRIMARY_BUCKET
    notification_arn = mock_settings.MINIO_KAFKA_NOTIFICATION_ARN

    minio_service.set_bucket_notifications(
        minio_client_mock, bucket_name, notification_arn, logger_mock
    )

    expected_events = ["s3:ObjectCreated:*"]
    expected_id = f"celery-queue-{bucket_name.replace('.', '-')}-notification"

    # Check that set_bucket_notification was called with a NotificationConfig
    # containing the correct QueueConfig
    args, _ = minio_client_mock.set_bucket_notification.call_args
    assert args[0] == bucket_name
    assert isinstance(args[1], NotificationConfig)

    queue_config_list = args[1].queue_config_list
    assert len(queue_config_list) == 1
    actual_queue_config = queue_config_list[0]

    assert actual_queue_config.queue == notification_arn
    assert actual_queue_config.events == expected_events
    assert actual_queue_config.config_id == expected_id
    assert actual_queue_config.prefix_filter_rule == ""
    assert actual_queue_config.suffix_filter_rule == ""

    logger_mock.info.assert_called_once_with(
        f"Bucket notification set for '{bucket_name}' to ARN '{notification_arn}' for events: {expected_events}."
    )


def test_set_bucket_notifications_no_arn(minio_client_mock, mock_settings, logger_mock):  # pylint: disable=redefined-outer-name
    bucket_name = mock_settings.MINIO_PRIMARY_BUCKET
    mock_settings.MINIO_KAFKA_NOTIFICATION_ARN = None  # No ARN

    minio_service.set_bucket_notifications(
        minio_client_mock,
        bucket_name,
        mock_settings.MINIO_KAFKA_NOTIFICATION_ARN,
        logger_mock,
    )

    minio_client_mock.set_bucket_notification.assert_not_called()
    logger_mock.info.assert_called_once_with(
        f"MINIO_KAFKA_NOTIFICATION_ARN not set for bucket '{bucket_name}'. Skipping notification setup."
    )


def test_set_bucket_notifications_s3error(
    minio_client_mock, mock_settings, logger_mock
):  # pylint: disable=redefined-outer-name
    bucket_name = mock_settings.MINIO_PRIMARY_BUCKET
    notification_arn = mock_settings.MINIO_KAFKA_NOTIFICATION_ARN
    s3_error = S3Error(
        code="NotificationError",
        message="Test S3 error on notification set",
        resource=bucket_name,
        request_id="notify_req",
        host_id="notify_host",
        response=MagicMock(status=500),
    )
    minio_client_mock.set_bucket_notification.side_effect = s3_error

    with pytest.raises(S3Error, match="Test S3 error on notification set"):
        minio_service.set_bucket_notifications(
            minio_client_mock, bucket_name, notification_arn, logger_mock
        )

    logger_mock.error.assert_called_once_with(
        f"Error setting bucket notification for '{bucket_name}' to ARN '{notification_arn}': {s3_error}"
    )


# Tests for download_file_to_tmp
@patch("app.services.minio_service.tempfile.NamedTemporaryFile")
@patch("app.services.minio_service.os.path.exists")
@patch("app.services.minio_service.os.remove")
def test_download_file_to_tmp_success(
    mock_os_remove,
    _mock_os_exists,
    mock_named_temp_file,
    minio_client_mock,
    logger_mock,
):  # pylint: disable=redefined-outer-name
    bucket_name = "dl-bucket"
    object_name = "my_file.txt"

    mock_temp_file_instance = MagicMock()
    mock_temp_file_instance.name = "/tmp/fake_temp_file.txt"
    mock_named_temp_file.return_value = mock_temp_file_instance

    # Simulate file download
    minio_client_mock.fget_object.return_value = None

    result_path = minio_service.download_file_to_tmp(
        minio_client_mock, bucket_name, object_name, logger_mock
    )

    mock_named_temp_file.assert_called_once_with(delete=False, suffix=".txt")
    mock_temp_file_instance.close.assert_called_once()
    minio_client_mock.fget_object.assert_called_once_with(
        bucket_name, object_name, "/tmp/fake_temp_file.txt"
    )
    assert result_path == "/tmp/fake_temp_file.txt"
    logger_mock.info.assert_any_call(
        f"Attempting to download '{object_name}' from bucket '{bucket_name}' to '/tmp/fake_temp_file.txt'..."
    )
    logger_mock.info.assert_any_call(
        f"Successfully downloaded '{object_name}' to '/tmp/fake_temp_file.txt'."
    )
    mock_os_remove.assert_not_called()  # Should not be called on success


@patch("app.services.minio_service.tempfile.NamedTemporaryFile")
@patch("app.services.minio_service.os.path.exists")
@patch("app.services.minio_service.os.remove")
def test_download_file_to_tmp_s3error(
    mock_os_remove, mock_os_exists, mock_named_temp_file, minio_client_mock, logger_mock
):  # pylint: disable=redefined-outer-name
    bucket_name = "dl-bucket-s3err"
    object_name = "my_file.csv"

    mock_temp_file_instance = MagicMock()
    temp_file_path = "/tmp/fake_temp_file.csv"
    mock_temp_file_instance.name = temp_file_path
    mock_named_temp_file.return_value = mock_temp_file_instance

    s3_error = S3Error(
        code="DownloadFailed",
        message="Download failed",
        resource=f"{bucket_name}/{object_name}",
        request_id="dl_req",
        host_id="dl_host",
        response=MagicMock(status=500),
    )
    minio_client_mock.fget_object.side_effect = s3_error
    mock_os_exists.return_value = True  # Assume temp file was created

    result_path = minio_service.download_file_to_tmp(
        minio_client_mock, bucket_name, object_name, logger_mock
    )

    assert result_path is None
    logger_mock.error.assert_called_once_with(
        f"Error downloading '{object_name}' from '{bucket_name}': {s3_error}"
    )
    mock_os_exists.assert_called_once_with(temp_file_path)
    mock_os_remove.assert_called_once_with(temp_file_path)


@patch("app.services.minio_service.tempfile.NamedTemporaryFile")
@patch("app.services.minio_service.os.path.exists")
@patch("app.services.minio_service.os.remove")
def test_download_file_to_tmp_unexpected_error(
    mock_os_remove, mock_os_exists, mock_named_temp_file, minio_client_mock, logger_mock
):  # pylint: disable=redefined-outer-name
    bucket_name = "dl-bucket-unexpect"
    object_name = "another.zip"

    mock_temp_file_instance = MagicMock()
    temp_file_path = "/tmp/fake_temp_file.zip"
    mock_temp_file_instance.name = temp_file_path
    mock_named_temp_file.return_value = mock_temp_file_instance

    unexpected_error = Exception("Something else went wrong")
    minio_client_mock.fget_object.side_effect = unexpected_error
    mock_os_exists.return_value = True

    result_path = minio_service.download_file_to_tmp(
        minio_client_mock, bucket_name, object_name, logger_mock
    )

    assert result_path is None
    logger_mock.error.assert_called_once_with(
        f"An unexpected error occurred during download of '{object_name}': {unexpected_error}"
    )
    mock_os_exists.assert_called_once_with(temp_file_path)
    mock_os_remove.assert_called_once_with(temp_file_path)


@patch("app.services.minio_service.tempfile.NamedTemporaryFile")
@patch("app.services.minio_service.os.path.exists")
@patch("app.services.minio_service.os.remove")
def test_download_file_to_tmp_s3error_temp_file_not_exists(
    mock_os_remove, mock_os_exists, mock_named_temp_file, minio_client_mock, logger_mock
):  # pylint: disable=redefined-outer-name
    bucket_name = "dl-bucket-s3err-no-tmp"
    object_name = "my_file.dat"

    mock_temp_file_instance = MagicMock()
    temp_file_path = "/tmp/fake_temp_file.dat"
    mock_temp_file_instance.name = temp_file_path
    mock_named_temp_file.return_value = mock_temp_file_instance

    s3_error = S3Error(
        code="DownloadFailedNoTmp",
        message="Download failed",  # Message can be the same if code is different or context implies
        resource=f"{bucket_name}/{object_name}",
        request_id="dl_req_notmp",
        host_id="dl_host_notmp",
        response=MagicMock(status=500),
    )
    minio_client_mock.fget_object.side_effect = s3_error
    # Simulate temp file does not exist (e.g. creation failed before this or it was deleted by another process)
    mock_os_exists.return_value = False

    result_path = minio_service.download_file_to_tmp(
        minio_client_mock, bucket_name, object_name, logger_mock
    )

    assert result_path is None
    logger_mock.error.assert_called_once_with(
        f"Error downloading '{object_name}' from '{bucket_name}': {s3_error}"
    )
    mock_os_exists.assert_called_once_with(
        temp_file_path
    )  # os.path.exists is still called
    mock_os_remove.assert_not_called()  # os.remove should not be called if file doesn't exist


# Tests for upload_file_from_tmp
@patch("app.services.minio_service.os.path.exists")
@patch("app.services.minio_service.os.remove")
def test_upload_file_from_tmp_success_cleanup(
    mock_os_remove, mock_os_exists, minio_client_mock, logger_mock
):  # pylint: disable=redefined-outer-name
    local_file_path = "/tmp/local_upload_me.dat"
    bucket_name = "ul-bucket"
    target_object_name = "uploaded_file.dat"

    mock_os_exists.return_value = True  # File exists for upload and cleanup
    minio_client_mock.fput_object.return_value = None  # Simulate successful upload

    success = minio_service.upload_file_from_tmp(
        minio_client_mock,
        local_file_path,
        bucket_name,
        target_object_name,
        logger_mock,
        cleanup_tmp_file=True,
    )

    assert success is True
    mock_os_exists.assert_any_call(
        local_file_path
    )  # Called twice: once at start, once in finally
    minio_client_mock.fput_object.assert_called_once_with(
        bucket_name, target_object_name, local_file_path
    )
    mock_os_remove.assert_called_once_with(local_file_path)
    logger_mock.info.assert_any_call(
        f"Attempting to upload '{local_file_path}' to bucket '{bucket_name}' as '{target_object_name}'..."
    )
    logger_mock.info.assert_any_call(
        f"Successfully uploaded '{local_file_path}' to '{bucket_name}' as '{target_object_name}'."
    )
    logger_mock.info.assert_any_call(f"Cleaned up temporary file: '{local_file_path}'.")


@patch("app.services.minio_service.os.path.exists")
@patch("app.services.minio_service.os.remove")
def test_upload_file_from_tmp_success_no_cleanup(
    mock_os_remove, mock_os_exists, minio_client_mock, logger_mock
):  # pylint: disable=redefined-outer-name
    local_file_path = "/tmp/keep_me.dat"
    bucket_name = "ul-bucket-keep"
    target_object_name = "kept_file.dat"

    mock_os_exists.return_value = True
    minio_client_mock.fput_object.return_value = None

    success = minio_service.upload_file_from_tmp(
        minio_client_mock,
        local_file_path,
        bucket_name,
        target_object_name,
        logger_mock,
        cleanup_tmp_file=False,
    )
    assert success is True
    mock_os_exists.assert_called_once_with(local_file_path)  # Only called at the start
    mock_os_remove.assert_not_called()


@patch("app.services.minio_service.os.path.exists")
def test_upload_file_from_tmp_local_file_not_found(
    mock_os_exists, minio_client_mock, logger_mock
):  # pylint: disable=redefined-outer-name
    local_file_path = "/tmp/i_do_not_exist.dat"
    bucket_name = "ul-bucket-nonexist"
    target_object_name = "non_existent.dat"

    mock_os_exists.return_value = False  # File does not exist

    success = minio_service.upload_file_from_tmp(
        minio_client_mock, local_file_path, bucket_name, target_object_name, logger_mock
    )
    assert success is False
    logger_mock.error.assert_called_once_with(
        f"Local file '{local_file_path}' not found for upload to '{target_object_name}' in '{bucket_name}'."
    )
    minio_client_mock.fput_object.assert_not_called()


@patch("app.services.minio_service.os.path.exists")
@patch("app.services.minio_service.os.remove")
def test_upload_file_from_tmp_s3error(
    mock_os_remove, mock_os_exists, minio_client_mock, logger_mock
):  # pylint: disable=redefined-outer-name
    local_file_path = "/tmp/upload_fail_s3.dat"
    bucket_name = "ul-bucket-s3err"
    target_object_name = "fail_s3.dat"

    mock_os_exists.return_value = True
    s3_error = S3Error(
        code="UploadFailed",
        message="Upload S3 failed",
        resource=f"{bucket_name}/{target_object_name}",
        request_id="ul_req",
        host_id="ul_host",
        response=MagicMock(status=500),
    )
    minio_client_mock.fput_object.side_effect = s3_error

    success = minio_service.upload_file_from_tmp(
        minio_client_mock,
        local_file_path,
        bucket_name,
        target_object_name,
        logger_mock,
        cleanup_tmp_file=True,
    )
    assert success is False
    logger_mock.error.assert_called_once_with(
        f"Error uploading '{local_file_path}' to '{target_object_name}' in '{bucket_name}': {s3_error}"
    )
    mock_os_remove.assert_called_once_with(
        local_file_path
    )  # Cleanup should still happen


@patch("app.services.minio_service.os.path.exists")
@patch("app.services.minio_service.os.remove")
def test_upload_file_from_tmp_unexpected_error(
    mock_os_remove, mock_os_exists, minio_client_mock, logger_mock
):  # pylint: disable=redefined-outer-name
    local_file_path = "/tmp/upload_fail_unexp.dat"
    bucket_name = "ul-bucket-unexperr"
    target_object_name = "fail_unexp.dat"

    mock_os_exists.return_value = True
    unexpected_error = Exception("Unexpected upload issue")
    minio_client_mock.fput_object.side_effect = unexpected_error

    success = minio_service.upload_file_from_tmp(
        minio_client_mock,
        local_file_path,
        bucket_name,
        target_object_name,
        logger_mock,
        cleanup_tmp_file=True,
    )
    assert success is False
    logger_mock.error.assert_called_once_with(
        f"An unexpected error occurred during upload of '{local_file_path}': {unexpected_error}"
    )
    mock_os_remove.assert_called_once_with(local_file_path)


@patch("app.services.minio_service.os.path.exists")
@patch("app.services.minio_service.os.remove")
def test_upload_file_from_tmp_cleanup_oserror(
    mock_os_remove, mock_os_exists, minio_client_mock, logger_mock
):  # pylint: disable=redefined-outer-name
    local_file_path = "/tmp/cleanup_fail.dat"
    bucket_name = "ul-bucket-cleanup-err"
    target_object_name = "cleanup_err.dat"

    mock_os_exists.return_value = True
    minio_client_mock.fput_object.return_value = None  # Upload succeeds
    os_error_on_remove = OSError("Cannot remove file")
    mock_os_remove.side_effect = os_error_on_remove

    success = minio_service.upload_file_from_tmp(
        minio_client_mock,
        local_file_path,
        bucket_name,
        target_object_name,
        logger_mock,
        cleanup_tmp_file=True,
    )
    assert success is True  # Upload itself was successful
    mock_os_remove.assert_called_once_with(local_file_path)
    logger_mock.error.assert_called_once_with(
        f"Error cleaning up temporary file '{local_file_path}': {os_error_on_remove}"
    )
