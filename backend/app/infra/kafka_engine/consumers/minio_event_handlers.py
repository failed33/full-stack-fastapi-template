import json
import logging

from pydantic import ValidationError
from sqlmodel import Session

from app import crud
from app.core.config import settings
from app.core.db import engine
from app.infra.kafka_engine.kafka import router

# Import Pydantic models
from app.schemas.minio_events import MinioEventRecord, MinioNotification

# Remove or comment out the WebSocket manager import if no longer used elsewhere in this file
# from app.websockets.connection_manager import manager

# from app.core.dramatiq_setup import your_actual_dramatiq_actor # Placeholder: Import your Dramatiq actor

logger = logging.getLogger(__name__)


async def _process_minio_record(record: MinioEventRecord):
    """
    Processes a single MinIO event record.
    Uses the object key from the event to find the pre-associated File record
    and updates it to mark upload completion.
    """
    if "s3:ObjectCreated" not in record.eventName:
        logger.debug("Skipping non-ObjectCreated MinIO event: %s", record.eventName)
        return

    bucket_name = record.s3.bucket.name
    object_key = record.s3.object.key
    object_size = record.s3.object.size  # This is the size in bytes
    object_etag = record.s3.object.eTag

    logger.info(
        f"Processing MinIO upload completion event: "
        f"eventName='{record.eventName}', bucket='{bucket_name}', "
        f"object_key='{object_key}', size={object_size}, eTag='{object_etag}'"
    )

    # Create a new session for this operation
    with Session(engine) as session:
        try:
            # Use object key to find the pre-associated File record
            db_file = crud.get_file_by_minio_object_name(
                session=session, minio_object_name=object_key
            )

            if not db_file:
                logger.warning(
                    f"No File record found for minio_object_name: '{object_key}'. "
                    "This could indicate:"
                    "- Pre-association step in upload API failed"
                    "- Unexpected S3 event for untracked upload"
                    "- Object key mismatch between upload API and MinIO event"
                )
                return

            # Check if already processed (handle duplicate events)
            if db_file.upload_status.value == "completed":
                logger.info(
                    f"File {db_file.id} (object_key: '{object_key}') is already marked as completed. "
                    "This might be a duplicate MinIO event. Ignoring."
                )
                return

            # Update the file record with upload completion metadata
            updated_file = crud.update_file_upload_completed_with_metadata(
                session=session,
                file_id=db_file.id,
                size_bytes=object_size,
                s3_etag=object_etag,
            )

            if updated_file:
                logger.info(
                    f"Successfully updated File record ID {updated_file.id}: "
                    f"upload_status=COMPLETED, size={object_size} bytes, "
                    f"eTag='{object_etag}', object_key='{object_key}'"
                )
                logger.info(
                    f"File '{updated_file.original_filename}' (ID: {updated_file.id}) "
                    f"for user {updated_file.user_id} is now ready for processing."
                )
            else:
                logger.error(
                    f"Failed to update File record ID {db_file.id} for object_key '{object_key}'"
                )

        except Exception as db_e:
            logger.error(
                f"Database error while processing MinIO event for object_key '{object_key}': {db_e}",
                exc_info=True,
            )
            # Session will rollback automatically on exception when using 'with Session(engine) as session:'

    # NOTE: FileProcess creation is now user-triggered via the /files/{file_id}/start-process API endpoint
    # This allows users to decide when and what type of processing they want to perform
    logger.debug(
        f"Upload processing complete for object_key: '{object_key}'. "
        "Awaiting user-triggered processing via API."
    )

    # OPTIONAL: Auto-trigger the pipeline for automatic processing
    # Uncomment the following lines if you want automatic processing after upload completion
    #
    # try:
    #     from app.core.dramatiq_setup import get_pipeline_actors
    #     from app.schemas.pipeline_events import FileReadyForConversionEvent
    #
    #     # Create a FileReadyForConversionEvent to trigger the pipeline
    #     pipeline_event = FileReadyForConversionEvent(
    #         trace_id=uuid.uuid4(),  # Generate a new trace ID for this workflow
    #         original_file_id=db_file_id,
    #         user_id=uuid.UUID(user_id_str) if user_id_str else db_file_id,  # Use file_id as fallback
    #         minio_object_name=object_key,
    #         original_filename=original_filename or "unknown"
    #     )
    #
    #     # Send to conversion actor to start the pipeline
    #     actors = get_pipeline_actors()
    #     actors['conversion'].send(pipeline_event.model_dump())
    #     logger.info(f"Auto-triggered pipeline for file {db_file_id}")
    #
    # except Exception as pipeline_e:
    #     logger.error(f"Failed to auto-trigger pipeline for file {db_file_id}: {pipeline_e}")
    #     # Don't re-raise - this is optional functionality


@router.subscriber(settings.MINIO_NOTIFY_KAFKA_TOPIC_PRIMARY)
async def handle_minio_upload_notification(
    message: dict,
):  # FastStream parses JSON to dict
    """
    Handles MinIO object creation notifications from Kafka, parses them using Pydantic,
    and triggers processing for each relevant record (which includes updating the database).
    """
    logger.info(
        "Received Kafka message on topic '%s'. Attempting to parse...",
        settings.MINIO_NOTIFY_KAFKA_TOPIC_PRIMARY,
    )

    try:
        notification = MinioNotification.model_validate(message)
        logger.debug(
            "Successfully parsed MinIO notification. Processing %s record(s).",
            len(notification.Records),
        )
        logger.debug(
            "Full notification payload: %s", notification.model_dump_json(indent=2)
        )

        for record in notification.Records:
            await _process_minio_record(record)

    except ValidationError as ve:
        logger.error(
            "Pydantic Validation Error processing MinIO event from Kafka: %s\n"
            "Problematic message content: %s",
            ve,
            json.dumps(message),
            exc_info=True,
        )
        # Consider moving the message to a Dead Letter Queue (DLQ)
    except Exception as e:
        logger.error(
            "Unexpected error processing MinIO event from Kafka: %s\n" "Message: %s",
            e,
            json.dumps(message),
            exc_info=True,
        )
