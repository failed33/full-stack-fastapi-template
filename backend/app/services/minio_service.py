import json
import logging
import os
import tempfile
from urllib.parse import urlparse

from minio import Minio
from minio.error import S3Error
from minio.notificationconfig import NotificationConfig, QueueConfig

from app.core.config import Settings


def _parse_endpoint(url: str) -> tuple[str, bool]:
    """
    Parse a MinIO endpoint URL and return a tuple of (endpoint, secure_connection).
    `endpoint` is the host[:port] string expected by the Minio client,
    and `secure_connection` is True when the scheme is HTTPS, False otherwise.
    """
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.hostname:
        raise ValueError(f"Invalid MINIO_URL_INTERNAL value: {url}")
    secure = parsed.scheme == "https"
    endpoint = parsed.hostname
    if parsed.port:
        endpoint += f":{parsed.port}"
    return endpoint, secure


def get_minio_client(settings: Settings, logger: logging.Logger) -> Minio:
    """
    Creates and returns a Minio client instance based on application settings.
    Parses MINIO_URL_INTERNAL to determine endpoint and security.
    """
    if not all([settings.MINIO_ROOT_USER, settings.MINIO_ROOT_PASSWORD]):
        logger.error(
            "MINIO_ROOT_USER and MINIO_ROOT_PASSWORD environment variables must be set."
        )
        raise ValueError("MinIO credentials not set in settings.")

    endpoint, secure_connection = _parse_endpoint(settings.MINIO_URL_INTERNAL)
    logger.debug(
        f"Minio client configured for endpoint: '{endpoint}', secure: {secure_connection}"
    )
    client = Minio(
        endpoint,
        access_key=settings.MINIO_ROOT_USER,
        secret_key=settings.MINIO_ROOT_PASSWORD,
        secure=secure_connection,
    )
    return client


def ensure_bucket_exists(client: Minio, bucket_name: str, logger: logging.Logger):
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info(f"Bucket '{bucket_name}' created successfully.")
        else:
            logger.info(f"Bucket '{bucket_name}' already exists.")
    except S3Error as e:
        logger.error(f"Error creating or checking bucket '{bucket_name}': {e}")
        raise


def set_public_download_policy(client: Minio, bucket_name: str, logger: logging.Logger):
    policy = {
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
    try:
        current_policy_str = client.get_bucket_policy(bucket_name)
        current_policy = json.loads(current_policy_str) if current_policy_str else {}
        # Basic check to see if it's somewhat similar to avoid unnecessary updates
        if current_policy.get("Statement") == policy.get("Statement"):
            logger.info(
                f"Public read (download) policy for bucket '{bucket_name}' is already correctly set."
            )
            return
    except S3Error as e:
        if e.code == "NoSuchBucketPolicy":
            logger.info(
                f"No existing policy for bucket '{bucket_name}'. Setting new policy."
            )
        else:
            # Log warning but proceed, as setting the policy might fix it.
            logger.warning(
                f"Could not retrieve current policy for bucket '{bucket_name}': {e}. Proceeding to set policy."
            )

    try:
        client.set_bucket_policy(bucket_name, json.dumps(policy))
        logger.info(f"Public read (download) policy set for bucket '{bucket_name}'.")
    except S3Error as e:
        logger.error(
            f"Error setting public read policy for bucket '{bucket_name}': {e}"
        )
        raise


# For MinIO this call is a no-op (the server does not expose PUT Bucket CORS).
def set_cors_policy(
    _client: Minio,
    _bucket_name: str,
    frontend_origin: str | None,
    logger: logging.Logger,
) -> None:
    logger.info(
        "Skipping bucket-level CORS. "
        'If you need CORS use `mc admin config set <alias>/ api cors_allow_origin="%s"` '
        "or the MINIO_API_CORS_ALLOW_ORIGIN env-var.",
        frontend_origin or "<origin>",
    )


def set_bucket_notifications(
    client: Minio,
    bucket_name: str,
    notification_arn: str | None,
    logger: logging.Logger,
):
    if not notification_arn:  # notification_arn can be None from settings
        logger.info(
            f"MINIO_KAFKA_NOTIFICATION_ARN not set for bucket '{bucket_name}'. Skipping notification setup."
        )
        return

    notification_events = ["s3:ObjectCreated:*"]
    queue_config_id = f"celery-queue-{bucket_name.replace('.', '-')}-notification"

    prefix = ""
    suffix = ""

    queue_config = QueueConfig(
        queue=notification_arn,
        events=notification_events,
        prefix_filter_rule=prefix,
        suffix_filter_rule=suffix,
        config_id=queue_config_id,
    )

    notification_config = NotificationConfig(queue_config_list=[queue_config])

    try:
        client.set_bucket_notification(bucket_name, notification_config)
        logger.info(
            f"Bucket notification set for '{bucket_name}' to ARN '{notification_arn}' for events: {notification_events}."
        )
    except S3Error as e:
        logger.error(
            f"Error setting bucket notification for '{bucket_name}' to ARN '{notification_arn}': {e}"
        )
        raise


def download_file_to_tmp(
    client: Minio, bucket_name: str, object_name: str, logger: logging.Logger
) -> str | None:
    """
    Downloads a file from MinIO to a temporary local file.

    Args:
        client: Initialized MinIO client.
        bucket_name: Name of the source bucket.
        object_name: Name of the object (key) in the bucket.
        logger: Logger instance.

    Returns:
        Path to the temporary file, or None if download failed.
    """
    try:
        # Create a temporary file. It's important to delete it manually after use.
        # Suffix can be useful to retain original extension for some tools.
        suffix = os.path.splitext(object_name)[1]
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        temp_file_path = temp_file.name
        temp_file.close()  # Close it so MinIO client can write to it

        logger.info(
            f"Attempting to download '{object_name}' from bucket '{bucket_name}' to '{temp_file_path}'..."
        )
        client.fget_object(bucket_name, object_name, temp_file_path)
        logger.info(f"Successfully downloaded '{object_name}' to '{temp_file_path}'.")
        return temp_file_path
    except S3Error as e:
        logger.error(f"Error downloading '{object_name}' from '{bucket_name}': {e}")
        # Clean up the temp file if it was created and an error occurred
        if "temp_file_path" in locals() and os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        return None
    except Exception as e:
        logger.error(
            f"An unexpected error occurred during download of '{object_name}': {e}"
        )
        if "temp_file_path" in locals() and os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        return None


def upload_file_from_tmp(
    client: Minio,
    local_file_path: str,
    bucket_name: str,
    target_object_name: str,
    logger: logging.Logger,
    cleanup_tmp_file: bool = True,
) -> bool:
    """
    Uploads a local temporary file to MinIO and optionally cleans it up.

    Args:
        client: Initialized MinIO client.
        local_file_path: Path to the local temporary file.
        bucket_name: Name of the target bucket.
        target_object_name: Name of the object (key) to create in the bucket.
        logger: Logger instance.
        cleanup_tmp_file: Whether to delete the local temporary file after upload.

    Returns:
        True if upload was successful, False otherwise.
    """
    if not os.path.exists(local_file_path):
        logger.error(
            f"Local file '{local_file_path}' not found for upload to '{target_object_name}' in '{bucket_name}'."
        )
        return False
    try:
        logger.info(
            f"Attempting to upload '{local_file_path}' to bucket '{bucket_name}' as '{target_object_name}'..."
        )
        client.fput_object(bucket_name, target_object_name, local_file_path)
        logger.info(
            f"Successfully uploaded '{local_file_path}' to '{bucket_name}' as '{target_object_name}'."
        )
        return True
    except S3Error as e:
        logger.error(
            f"Error uploading '{local_file_path}' to '{target_object_name}' in '{bucket_name}': {e}"
        )
        return False
    except Exception as e:
        logger.error(
            f"An unexpected error occurred during upload of '{local_file_path}': {e}"
        )
        return False
    finally:
        if cleanup_tmp_file and os.path.exists(local_file_path):
            try:
                os.remove(local_file_path)
                logger.info(f"Cleaned up temporary file: '{local_file_path}'.")
            except OSError as e:
                logger.error(
                    f"Error cleaning up temporary file '{local_file_path}': {e}"
                )
