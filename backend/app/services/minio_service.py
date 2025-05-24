import json
import logging
import os
import tempfile
from datetime import timedelta
from typing import Any
from urllib import parse
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
    This client is used for internal operations like bucket management and file operations.
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
        region="us-east-1",  # Default region to prevent _get_region network calls
    )
    return client


def get_minio_client_for_presigned_urls(
    settings: Settings, logger: logging.Logger
) -> Minio:
    """
    Creates and returns a Minio client instance for generating presigned URLs.

    This function is deprecated in favor of generate_presigned_put_url which handles
    the public endpoint configuration properly. Consider using that function instead.
    """
    if not all([settings.MINIO_ROOT_USER, settings.MINIO_ROOT_PASSWORD]):
        logger.error(
            "MINIO_ROOT_USER and MINIO_ROOT_PASSWORD environment variables must be set."
        )
        raise ValueError("MinIO credentials not set in settings.")

    # Always use internal endpoint for the client connection
    # The URL rewriting will happen after presigned URL generation
    endpoint, secure_connection = _parse_endpoint(settings.MINIO_URL_INTERNAL)

    logger.debug(
        f"Minio client for presigned URLs configured for internal endpoint: '{endpoint}', secure: {secure_connection}"
    )

    # Add region to prevent potential _get_region calls
    client = Minio(
        endpoint,
        access_key=settings.MINIO_ROOT_USER,
        secret_key=settings.MINIO_ROOT_PASSWORD,
        secure=secure_connection,
        region="us-east-1",  # Default region to prevent network calls
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
    queue_config_id = f"dramatiq-queue-{bucket_name.replace('.', '-')}-notification"

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


def generate_presigned_put_url(
    settings: Settings,
    bucket_name: str,
    object_name: str,
    expires: timedelta,
    logger: logging.Logger,
) -> str:
    """
    Generate a presigned PUT URL using the public endpoint if configured.

    If MINIO_PUBLIC_ENDPOINT is set, creates a MinIO client with the public
    endpoint to generate a URL accessible from outside the Docker network.
    Otherwise, creates a client with the internal endpoint.

    Args:
        settings: Application settings containing MinIO configuration
        bucket_name: Name of the bucket
        object_name: Name of the object
        expires: Expiration time for the presigned URL
        logger: Logger instance

    Returns:
        Presigned URL string
    """
    logger.info(
        f"Attempting to generate presigned PUT URL for bucket='{bucket_name}', object='{object_name}'"
    )
    logger.debug(
        f"Settings: MINIO_PUBLIC_ENDPOINT='{settings.MINIO_PUBLIC_ENDPOINT}', MINIO_URL_INTERNAL='{settings.MINIO_URL_INTERNAL}'"
    )

    if not settings.MINIO_PUBLIC_ENDPOINT:
        logger.error(
            "MINIO_PUBLIC_ENDPOINT is not set. This is required for generating browser-accessible presigned URLs."
        )
        raise ValueError("MINIO_PUBLIC_ENDPOINT is not configured.")

    endpoint_url = settings.MINIO_PUBLIC_ENDPOINT
    logger.info(f"Using public endpoint for presigned URL generation: {endpoint_url}")

    # Parse the endpoint
    try:
        parsed_endpoint = urlparse(endpoint_url)
        if not parsed_endpoint.scheme or not parsed_endpoint.hostname:
            logger.error(f"Invalid public endpoint URL format: {endpoint_url}")
            raise ValueError(f"Invalid public endpoint URL: {endpoint_url}")
    except ValueError as e:
        logger.error(f"ValueError parsing public endpoint_url '{endpoint_url}': {e}")
        raise

    # Create endpoint string for MinIO client (without scheme)
    endpoint_for_minio_client = parsed_endpoint.hostname
    if parsed_endpoint.port:
        endpoint_for_minio_client += f":{parsed_endpoint.port}"

    # Determine if connection should be secure
    secure_connection = parsed_endpoint.scheme == "https"

    # Provide a default region. For MinIO, 'us-east-1' is often used as a placeholder.
    # This is an attempt to prevent the client from trying to fetch the region via a network call.
    region_to_use = "us-east-1"

    logger.info(
        f"MinIO client params for presigned URL: "
        f"endpoint='{endpoint_for_minio_client}', "
        f"access_key_present={settings.MINIO_ROOT_USER is not None}, "
        f"secure={secure_connection}, region='{region_to_use}'"
    )

    # Create a MinIO client with the appropriate endpoint
    try:
        # Instantiate the client that will be used for signing.
        # It uses the public endpoint details because this host must be in the signed URL.
        # The region is provided to hopefully prevent an internal network call to _get_region.
        signing_client = Minio(
            endpoint=endpoint_for_minio_client,
            access_key=settings.MINIO_ROOT_USER,
            secret_key=settings.MINIO_ROOT_PASSWORD,
            secure=secure_connection,
            region=region_to_use,  # Crucial parameter to prevent _get_region call
        )

        logger.info(
            f"MinIO client instantiated. Region set on client: '{signing_client._region if hasattr(signing_client, '_region') else '_region attr not found'}'"
        )

        # Generate the presigned URL
        presigned_url = signing_client.presigned_put_object(
            bucket_name, object_name, expires
        )
        logger.info(f"Successfully generated presigned URL: {presigned_url}")

        return presigned_url
    except S3Error as e:
        logger.error(
            f"S3Error generating presigned URL with endpoint '{endpoint_for_minio_client}': {e}",
            exc_info=True,
        )
        # Check if the error is about invalid arguments specifically for region
        if "region" in str(e).lower() or "argument" in str(e).lower():
            logger.error(
                "The S3Error might be related to the region parameter. Check MinIO client version compatibility."
            )
        raise
    except TypeError as e:
        logger.error(
            f"TypeError during MinIO client instantiation or presigned URL generation (check arguments): {e}",
            exc_info=True,
        )
        if "region" in str(e).lower() or "keyword" in str(e).lower():
            logger.error(
                "The TypeError might be due to an unexpected 'region' keyword argument. Double-check minio-py version and its Minio() constructor."
            )
        raise
    except Exception as e:
        logger.error(
            f"Unexpected error generating presigned URL with endpoint '{endpoint_for_minio_client}': {e}",
            exc_info=True,
        )
        raise


def initiate_multipart_upload(
    client: Minio,
    bucket_name: str,
    object_name: str,
    logger: logging.Logger | None = None,
) -> str:
    """
    Initiate a multipart upload for a large file.

    Args:
        client: Initialized MinIO client
        bucket_name: Name of the bucket
        object_name: Name of the object
        logger: Logger instance

    Returns:
        Upload ID for the multipart upload
    """
    try:
        # MinIO's Python SDK doesn't have a direct initiate_multipart_upload method
        # We need to use the low-level API
        response = client._execute(
            method="POST",
            bucket_name=bucket_name,
            object_name=object_name,
            query={"uploads": ""},
        )

        # Parse the XML response to get upload ID
        from xml.etree import ElementTree as ET

        root = ET.fromstring(response.data.decode())
        upload_id = root.find(
            "./{http://s3.amazonaws.com/doc/2006-03-01/}UploadId"
        ).text

        if logger:
            logger.info(
                f"Initiated multipart upload for '{object_name}' in bucket '{bucket_name}' with upload ID: {upload_id}"
            )

        return upload_id
    except Exception as e:
        if logger:
            logger.error(
                f"Error initiating multipart upload for '{object_name}' in '{bucket_name}': {e}"
            )
        raise


def generate_presigned_part_url(
    settings: Settings,
    bucket_name: str,
    object_name: str,
    upload_id: str,
    part_number: int,
    expires: timedelta,
    logger: logging.Logger,
) -> str:
    """
    Generate a presigned URL for uploading a specific part of a multipart upload.

    Args:
        settings: Application settings
        bucket_name: Name of the bucket
        object_name: Name of the object
        upload_id: The multipart upload ID
        part_number: Part number (1-indexed)
        expires: Expiration time for the presigned URL
        logger: Logger instance

    Returns:
        Presigned URL for uploading the part
    """
    logger.debug(
        f"Generating presigned URL for part {part_number} of upload {upload_id}"
    )

    if not settings.MINIO_PUBLIC_ENDPOINT:
        logger.error(
            "MINIO_PUBLIC_ENDPOINT is required for generating multipart presigned URLs"
        )
        raise ValueError("MINIO_PUBLIC_ENDPOINT is not configured.")

    endpoint_url = settings.MINIO_PUBLIC_ENDPOINT
    logger.debug(f"Using public endpoint for multipart presigned URL: {endpoint_url}")

    parsed_endpoint = urlparse(endpoint_url)
    endpoint_for_client = parsed_endpoint.hostname
    if parsed_endpoint.port:
        endpoint_for_client += f":{parsed_endpoint.port}"
    secure = parsed_endpoint.scheme == "https"

    # Use the same region approach to prevent _get_region network calls
    region_to_use = "us-east-1"

    try:
        client = Minio(
            endpoint=endpoint_for_client,
            access_key=settings.MINIO_ROOT_USER,
            secret_key=settings.MINIO_ROOT_PASSWORD,
            secure=secure,
            region=region_to_use,  # Prevent _get_region call
        )

        # Build the base URL
        scheme = "https" if secure else "http"
        base_url = f"{scheme}://{endpoint_for_client}/{bucket_name}/{parse.quote(object_name, safe='')}"

        # Add query parameters
        query_params = {
            "uploadId": upload_id,
            "partNumber": str(part_number),
        }

        # Generate presigned URL using MinIO's internal signing
        # This uses the presign_v4 method with custom query parameters
        from datetime import datetime, timezone

        from minio.signer import presign_v4

        # Get credentials
        credentials = client._credentials

        # Create the presigned URL
        presigned_url = presign_v4(
            method="PUT",
            url=base_url,
            region=client._region,
            credentials=credentials,
            expires=int(expires.total_seconds()),
            request_datetime=datetime.now(timezone.utc),
            sha256_hash="UNSIGNED-PAYLOAD",
            headers={},
            query_params=query_params,
        )

        logger.info(
            f"Generated presigned URL for part {part_number}: {presigned_url[:70]}..."
        )
        return presigned_url

    except Exception as e:
        logger.error(
            f"Error generating presigned URL for part {part_number}: {e}", exc_info=True
        )
        raise


def complete_multipart_upload(
    client: Minio,
    bucket_name: str,
    object_name: str,
    upload_id: str,
    parts: list[dict[str, Any]],
    logger: logging.Logger | None = None,
) -> dict[str, str]:
    """
    Complete a multipart upload by combining all uploaded parts.

    Args:
        client: Initialized MinIO client
        bucket_name: Name of the bucket
        object_name: Name of the object
        upload_id: The multipart upload ID
        parts: List of part information, each containing 'part_number' and 'etag'
        logger: Logger instance

    Returns:
        Dictionary with 'etag' and 'location' of the completed object
    """
    try:
        # Build XML for complete multipart upload
        from xml.etree import ElementTree as ET

        root = ET.Element("CompleteMultipartUpload")
        for part in sorted(parts, key=lambda x: x["part_number"]):
            part_elem = ET.SubElement(root, "Part")
            ET.SubElement(part_elem, "PartNumber").text = str(part["part_number"])
            ET.SubElement(part_elem, "ETag").text = part["etag"]

        xml_data = ET.tostring(root, encoding="utf-8")

        # Execute the complete multipart upload request
        response = client._execute(
            method="POST",
            bucket_name=bucket_name,
            object_name=object_name,
            query={"uploadId": upload_id},
            content=xml_data,
            headers={"Content-Type": "application/xml"},
        )

        # Parse response
        result_root = ET.fromstring(response.data.decode())
        namespace = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

        result = {
            "etag": result_root.find(".//s3:ETag", namespace).text.strip('"'),
            "location": result_root.find(".//s3:Location", namespace).text,
        }

        if logger:
            logger.info(
                f"Completed multipart upload for '{object_name}' with ETag: {result['etag']}"
            )

        return result

    except Exception as e:
        if logger:
            logger.error(f"Error completing multipart upload for '{object_name}': {e}")
        raise


def abort_multipart_upload(
    client: Minio,
    bucket_name: str,
    object_name: str,
    upload_id: str,
    logger: logging.Logger | None = None,
) -> None:
    """
    Abort a multipart upload and clean up any uploaded parts.

    Args:
        client: Initialized MinIO client
        bucket_name: Name of the bucket
        object_name: Name of the object
        upload_id: The multipart upload ID to abort
        logger: Logger instance
    """
    try:
        client._execute(
            method="DELETE",
            bucket_name=bucket_name,
            object_name=object_name,
            query={"uploadId": upload_id},
        )

        if logger:
            logger.info(f"Aborted multipart upload {upload_id} for '{object_name}'")

    except Exception as e:
        if logger:
            logger.error(f"Error aborting multipart upload {upload_id}: {e}")
        raise
