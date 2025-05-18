import logging

from minio import Minio
from minio.error import S3Error
from tenacity import after_log, before_log, retry, stop_after_attempt, wait_fixed

from app.core.config import Settings, settings
from app.services import minio_service

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Tenacity Settings for Retrying Connection ---
# Reduced MAX_TRIES as Docker Compose health check should handle primary readiness.
# This is a secondary safeguard.
MAX_TRIES = 15  # Try for 15 seconds (adjust as needed)
WAIT_SECONDS = 1


@retry(
    stop=stop_after_attempt(MAX_TRIES),
    wait=wait_fixed(WAIT_SECONDS),
    before=before_log(logger, logging.INFO),
    after=after_log(logger, logging.WARN),
)
def wait_for_minio_service(app_settings: Settings) -> Minio:
    """
    Waits for MinIO to be available and returns a connected client.
    Uses the get_minio_client from minio_service.
    """
    logger.debug(
        f"Attempting to connect to MinIO specified in settings ({app_settings.MINIO_URL_INTERNAL})..."
    )
    try:
        # Use the centralized client getter from the service
        client = minio_service.get_minio_client(app_settings, logger)
        # Perform a lightweight operation to check if MinIO is truly responsive
        client.list_buckets()
        logger.info(
            f"Successfully connected to MinIO at {app_settings.MINIO_URL_INTERNAL}."
        )
        return client
    except S3Error as e:  # Catch S3Error specifically from MinIO operations
        logger.error(f"MinIO connection/check failed with S3Error: {e}. Retrying...")
        raise e
    except Exception as e:  # Catch other potential errors (e.g., network, DNS resolution before client connection)
        logger.error(
            f"An unexpected error occurred while trying to connect/check MinIO: {e}. Retrying..."
        )
        raise e


def initialize_minio_resources(app_settings: Settings) -> None:
    logger.info("Attempting to connect to MinIO and initialize resources...")
    # This call will retry until MinIO is available or timeout
    client = wait_for_minio_service(app_settings)

    logger.info("MinIO is available. Proceeding with resource initialization.")

    # Initialize Uploads Bucket
    minio_service.ensure_bucket_exists(
        client, app_settings.MINIO_UPLOADS_BUCKET, logger
    )

    # Setup notifications for uploads bucket if ARN is configured
    minio_service.set_bucket_notifications(
        client,
        app_settings.MINIO_UPLOADS_BUCKET,
        app_settings.MINIO_CELERY_NOTIFICATION_ARN,
        logger,
    )

    # Initialize Transcripts Bucket
    minio_service.ensure_bucket_exists(
        client, app_settings.MINIO_TRANSCRIPTS_BUCKET, logger
    )
    minio_service.set_public_download_policy(
        client, app_settings.MINIO_TRANSCRIPTS_BUCKET, logger
    )

    logger.info("MinIO resource initialization complete.")


def main() -> None:
    logger.info("Starting MinIO initialization process...")
    try:
        # Pass the global settings object
        initialize_minio_resources(settings)
        logger.info("MinIO initialization process finished successfully.")
    except Exception as e:
        logger.error(f"MinIO initialization process failed: {e}", exc_info=True)
        # Re-raise to ensure prestart.sh (if using set -e) fails if this script does
        raise


if __name__ == "__main__":
    main()
