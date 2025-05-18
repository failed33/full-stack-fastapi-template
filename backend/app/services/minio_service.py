import json
import logging
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
            f"MINIO_CELERY_NOTIFICATION_ARN not set for bucket '{bucket_name}'. Skipping notification setup."
        )
        return

    notification_events = ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"]
    # Make ID unique per bucket to avoid conflicts if multiple buckets notify the same ARN with different filters
    queue_config_id = f"celery-queue-{bucket_name.replace('.', '-')}-notification"

    queue_config = QueueConfig(
        notification_arn,
        notification_events,
    )
    queue_config.config_id = queue_config_id

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
