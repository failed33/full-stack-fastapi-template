"""
Bridges Kafka messages from MinIO notifications to Server-Sent Event (SSE) streams.

This module consumes MinIO object creation notifications from a Kafka topic,
processes them, and forwards relevant information to connected clients via SSE
streams managed by the `SSEManager`.

Key functionalities:
- Initializes a FastStream `KafkaRouter` to connect to Kafka brokers.
- Defines a unique `subscriber_group_id` for each application instance to ensure
  messages are broadcast to all instances for SSE fan-out.
- Subscribes to the MinIO notification Kafka topic specified in settings.
- Parses incoming Kafka messages, expecting MinIO notification JSON format.
- Validates notifications using the `MinioNotification` Pydantic model.
- Filters for `s3:ObjectCreated:*` events.
- Extracts `user-id` and `file-id` from the S3 object's user metadata.
- Constructs a payload (`ui_update_payload`) containing event details.
- Pushes the payload to the appropriate user via `sse_manager.push_event_to_user()`.
- Includes robust error handling for message decoding and processing.

Configuration:
- Kafka brokers: `settings.KAFKA_BOOTSTRAP`
- Kafka topic: `settings.MINIO_NOTIFY_KAFKA_TOPIC_PRIMARY`
- Application instance ID for group ID: `settings.APP_INSTANCE_ID`

Note:
  The `user-id` and `file-id` in MinIO's S3 object userMetadata are crucial for
  routing SSE events and linking them to database records. Ensure that clients
  uploading objects to MinIO set these metadata keys correctly. Common variations
  like "user-id"/"userid" and "file-id"/"fileid" are checked. MinIO typically
  lowercases metadata keys and may prefix them (e.g., "x-amz-meta-user-id").

"""
# app/infra/kafka_sse_bridge.py
import logging
import uuid
from urllib.parse import unquote

from faststream.kafka.fastapi import KafkaRouter
from pydantic import ValidationError

from app.core.config import settings
from app.schemas.minio_events import MinioNotification
from app.sse_stream.sse_manager import sse_manager

logger = logging.getLogger(__name__)

# Initialize the FastStream KafkaRouter
# It will use Kafka brokers from your settings
kafka_sse_router = KafkaRouter(
    # TODO This is the Kafka bootstrap server that is used in our backend many times. This might not be the best way to do this.
    settings.KAFKA_BOOTSTRAP,
    # Example: if using SASL_SSL
    # security_protocol=settings.KAFKA_SECURITY_PROTOCOL,
    # sasl_mechanism=settings.KAFKA_SASL_MECHANISM,
    # sasl_plain_username=settings.KAFKA_USERNAME,
    # sasl_plain_password=settings.KAFKA_PASSWORD,
)

# Ensure APP_INSTANCE_ID is defined in your settings
subscriber_group_id = f"sse-fan-out-{settings.APP_INSTANCE_ID}-{uuid.uuid4().hex[:4]}"
logger.info(
    f"Initializing Kafka SSE Bridge consumer with group_id: {subscriber_group_id}"
)


@kafka_sse_router.subscriber(
    settings.MINIO_NOTIFY_KAFKA_TOPIC_PRIMARY,
    group_id=subscriber_group_id,
    auto_offset_reset="latest",
    # Add other consumer params as needed, e.g., `auto_commit_interval_ms`
)
async def on_minio_event_for_sse(
    message: dict,
):  # Changed to dict, FastStream will parse JSON
    """
    Consumes MinIO notification messages from Kafka and pushes updates to SSE streams.

    This function processes MinIO events and uses the object key to look up the
    pre-associated File record in the database for reliable user/file identification.
    Falls back to userMetadata if database lookup fails.
    """
    try:
        # FastStream should have already parsed the JSON message into a dict.
        # Validate with Pydantic model using the already parsed message (dict)
        minio_notification = MinioNotification.model_validate(message)
        records = minio_notification.Records

        if not records:
            logger.debug("Received Kafka message with no records for SSE processing.")
            return

        for record in records:
            event_name = record.eventName
            # Filter for relevant events, e.g., s3:ObjectCreated:*
            if not event_name.startswith("s3:ObjectCreated:"):
                logger.debug(
                    f"Skipping event {event_name} as it's not ObjectCreated for SSE."
                )
                continue

            s3_data = record.s3
            object_data = s3_data.object
            encoded_object_key = object_data.key
            # Decode the object key from URL encoding (e.g., %2F to /)
            # This fixes the mismatch between MinIO event keys and database storage
            decoded_object_key = unquote(encoded_object_key)
            logger.debug(
                f"Received object_key: '{encoded_object_key}', decoded to: '{decoded_object_key}'"
            )

            # Primary method: Use object key to look up File record in database
            user_id = None
            file_id = None
            original_filename = None

            try:
                from sqlmodel import Session

                from app import crud
                from app.core.db import engine

                with Session(engine) as session:
                    db_file = crud.get_file_by_minio_object_name(
                        session=session,
                        minio_object_name=decoded_object_key,  # Use decoded key for DB lookup
                    )

                    if db_file:
                        user_id = str(db_file.user_id)
                        file_id = str(db_file.id)
                        original_filename = db_file.original_filename
                        logger.debug(
                            f"Found File record via object key lookup: "
                            f"user_id={user_id}, file_id={file_id}, filename={original_filename}, lookup_key='{decoded_object_key}'"
                        )
                    else:
                        logger.debug(
                            f"No File record found for decoded_object_key: '{decoded_object_key}'"
                        )

            except Exception as db_error:
                logger.warning(
                    f"Database lookup failed for decoded_object_key '{decoded_object_key}': {db_error}",
                    exc_info=True,
                )

            # Fallback method: Use userMetadata if database lookup failed
            if not user_id and object_data.userMetadata:
                user_metadata = object_data.userMetadata
                user_id = user_metadata.get("user-id") or user_metadata.get("userid")
                file_id = user_metadata.get("file-id") or user_metadata.get("fileid")
                original_filename = user_metadata.get("originalfilename")
                logger.debug(
                    f"Using userMetadata fallback: user_id={user_id}, file_id={file_id}, original_filename={original_filename}"
                )

            if user_id:
                # Construct the payload for the UI with enhanced metadata
                ui_update_payload = {
                    "event_type": event_name,  # e.g., "s3:ObjectCreated:Put"
                    "message": f"File '{original_filename or decoded_object_key}' uploaded successfully.",  # Use decoded key
                    "bucket_name": s3_data.bucket.name,
                    "object_key": decoded_object_key,  # Use decoded key in payload
                    "size": object_data.size,
                    "etag": object_data.eTag,
                    "user_id": user_id,
                    "file_id": file_id,  # Database file ID for frontend linking
                    "original_filename": original_filename,
                    "event_time": record.eventTime,
                }
                await sse_manager.push_event_to_user(user_id, ui_update_payload)
                logger.info(
                    f"Processed MinIO event ({event_name}) for SSE to user {user_id}: "
                    f"object={decoded_object_key}, file_id={file_id}, filename={original_filename}"  # Use decoded key
                )
            else:
                logger.warning(
                    f"Could not determine user_id for MinIO event: object={decoded_object_key}. "  # Use decoded key
                    f"Database lookup failed and no (or insufficient) user metadata found. "
                    f"UserMetadata received: {object_data.userMetadata}"
                )

    except ValidationError as ve:
        logger.error(
            f"Pydantic validation error processing Kafka message for SSE: {ve}. Message data: {message}",
            exc_info=True,
        )
    except Exception as e:
        logger.error(
            f"Unexpected error processing MinIO event for SSE from Kafka: {e}. Message: {message}",
            exc_info=True,
        )
