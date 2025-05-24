import logging
import os
import tempfile
import uuid
from typing import Any

import dramatiq
from sqlmodel import Session

from app import crud
from app.core.config import settings
from app.core.db import engine
from app.models import FileProcessCreate, FileSegmentCreate, ProcessStatus, ProcessType
from app.schemas.pipeline_events import (
    ConversionCompletedEvent,
    FileReadyForConversionEvent,
    SegmentCreatedEvent,
    TranscriptionCompletedEvent,
)
from app.services.minio_service import (
    download_file_to_tmp,
    get_minio_client,
    upload_file_from_tmp,
)

# Setup Redis broker for workers when this module is imported
try:
    from app.core.dramatiq_worker_setup import setup_dramatiq_worker_broker

    setup_dramatiq_worker_broker()
    logging.info("Successfully initialized Redis broker for workers")
except ImportError as e:
    # Fallback to regular setup if worker setup is not available
    logging.warning(
        f"Worker-specific Dramatiq setup not available: {e}. Using default broker setup."
    )
    pass
except Exception as e:
    # Log any other setup errors but don't fail completely
    logging.error(
        f"Failed to setup Redis broker for workers: {e}. Tasks may not work correctly."
    )
    pass

logger = logging.getLogger(__name__)


# --- Helper Functions ---


def _create_or_get_file_process(
    session: Session, file_id: uuid.UUID, user_id: uuid.UUID
) -> Any:
    """Create or get a FileProcess for the given file."""
    # Try to find an existing FileProcess for this file
    processes = crud.get_file_processes_by_file(
        session=session, file_id=file_id, limit=1
    )
    if processes:
        return processes[0]

    # Create a new FileProcess if none exists
    process_in = FileProcessCreate(
        file_id=file_id,
        user_id=user_id,
        process_type=ProcessType.TRANSCRIPTION,
        status=ProcessStatus.PROCESSING,
    )
    return crud.create_file_process(session=session, process_in=process_in)


def _upload_string_to_minio(content: str, bucket_name: str, object_name: str) -> bool:
    """Upload a string as a text file to MinIO."""
    try:
        minio_client = get_minio_client(settings, logger)

        # Create a temporary file with the content
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".txt"
        ) as temp_file:
            temp_file.write(content)
            temp_file_path = temp_file.name

        success = upload_file_from_tmp(
            client=minio_client,
            local_file_path=temp_file_path,
            bucket_name=bucket_name,
            target_object_name=object_name,
            logger=logger,
            cleanup_tmp_file=True,
        )
        return success
    except Exception as e:
        logger.error(f"Error uploading string to MinIO: {e}")
        return False


# --- Stage 1: Conversion Actor ---


@dramatiq.actor(queue_name="convert_cpu", time_limit=300000)  # 5 minutes
def conversion_actor(event_data: dict[str, Any]):
    """
    Stage 1: Convert uploaded file to a format suitable for segmentation (e.g., WAV).
    Consumes: FileReadyForConversionEvent
    Produces: ConversionCompletedEvent
    """
    try:
        event = FileReadyForConversionEvent.model_validate(event_data)
        logger.info(
            f"Conversion actor received event for file: {event.original_file_id} (target_hardware: {event.target_hardware})"
        )

        with Session(engine) as session:
            # Create or get FileProcess
            file_process = _create_or_get_file_process(
                session=session, file_id=event.original_file_id, user_id=event.user_id
            )

            crud.update_file_process_status(
                session=session,
                process_id=file_process.id,
                status=ProcessStatus.PROCESSING.value,
                error_message="Conversion started",
            )

            # 1. Download original file from MinIO
            minio_client = get_minio_client(settings, logger)
            downloaded_path = download_file_to_tmp(
                client=minio_client,
                bucket_name=settings.MINIO_PRIMARY_BUCKET,
                object_name=event.minio_object_name,
                logger=logger,
            )

            if not downloaded_path:
                raise Exception(f"Failed to download file: {event.minio_object_name}")

            # 2. Perform file conversion (placeholder - replace with actual conversion logic)
            # For now, we'll just copy the file and assume it's already in the right format
            converted_file_format = "wav"  # Target format
            converted_minio_object_name = f"converted/{event.original_file_id}/{uuid.uuid4()}.{converted_file_format}"

            # TODO: Replace this with actual audio conversion using ffmpeg or similar
            # For now, just upload the original file as "converted"
            success = upload_file_from_tmp(
                client=minio_client,
                local_file_path=downloaded_path,
                bucket_name=settings.MINIO_PRIMARY_BUCKET,
                target_object_name=converted_minio_object_name,
                logger=logger,
                cleanup_tmp_file=True,
                metadata={
                    "original_file_id": str(event.original_file_id),
                    "user_id": str(event.user_id),
                    "conversion_format": converted_file_format,
                },
            )

            if not success:
                raise Exception(
                    f"Failed to upload converted file: {converted_minio_object_name}"
                )

            logger.info(
                f"File converted and uploaded to: {converted_minio_object_name}"
            )

            # 3. Update FileProcess status
            crud.update_file_process_status(
                session=session,
                process_id=file_process.id,
                status=ProcessStatus.PROCESSING.value,
                error_message="Conversion completed, pending segmentation",
            )

        # 4. Send event for next stage (Segmentation)
        segmentation_event = ConversionCompletedEvent(
            trace_id=event.trace_id,
            original_file_id=event.original_file_id,
            user_id=event.user_id,
            converted_file_minio_path=converted_minio_object_name,
            converted_file_format=converted_file_format,
            target_hardware=event.target_hardware,  # Propagate hardware preference
        )

        segmentation_actor.send(segmentation_event.model_dump())
        logger.info(
            f"Sent ConversionCompletedEvent to segmentation_actor for: {event.original_file_id} (target_hardware: {event.target_hardware})"
        )

    except Exception as e:
        logger.error(
            f"Error in conversion_actor for file {event_data.get('original_file_id')}: {e}",
            exc_info=True,
        )

        # Update FileProcess to FAILED
        try:
            with Session(engine) as session:
                processes = crud.get_file_processes_by_file(
                    session=session, file_id=event_data.get("original_file_id"), limit=1
                )
                if processes:
                    crud.update_file_process_status(
                        session=session,
                        process_id=processes[0].id,
                        status=ProcessStatus.FAILED.value,
                        error_message=f"Conversion failed: {str(e)}",
                    )
        except Exception as db_e:
            logger.error(f"Failed to update FileProcess status: {db_e}")

        raise  # Re-raise for Dramatiq's retry/error handling


# --- Stage 2: Segmentation Actor ---


@dramatiq.actor(queue_name="split_cpu", time_limit=600000)  # 10 minutes
def segmentation_actor(event_data: dict[str, Any]):
    """
    Stage 2: Segment the converted audio file into smaller chunks for transcription.
    Consumes: ConversionCompletedEvent
    Produces: SegmentCreatedEvent (multiple events, one per segment)
    """
    try:
        event = ConversionCompletedEvent.model_validate(event_data)
        logger.info(
            f"Segmentation actor received event for file: {event.original_file_id} (target_hardware: {event.target_hardware})"
        )

        with Session(engine) as session:
            # Get the FileProcess
            processes = crud.get_file_processes_by_file(
                session=session, file_id=event.original_file_id, limit=1
            )
            if not processes:
                raise Exception(
                    f"FileProcess not found for file: {event.original_file_id}"
                )

            file_process = processes[0]

            crud.update_file_process_status(
                session=session,
                process_id=file_process.id,
                status=ProcessStatus.PROCESSING.value,
                error_message="Segmentation started",
            )

            # 1. Download converted file from MinIO
            minio_client = get_minio_client(settings, logger)
            downloaded_converted_path = download_file_to_tmp(
                client=minio_client,
                bucket_name=settings.MINIO_PRIMARY_BUCKET,
                object_name=event.converted_file_minio_path,
                logger=logger,
            )

            if not downloaded_converted_path:
                raise Exception(
                    f"Failed to download converted file: {event.converted_file_minio_path}"
                )

            # 2. Perform segmentation (placeholder - replace with actual segmentation logic)
            # For now, we'll create 3 dummy segments
            # TODO: Replace this with actual audio segmentation using pydub or similar
            segments_data = [
                {"index": 0, "start_time": 0, "end_time": 30},
                {"index": 1, "start_time": 30, "end_time": 60},
                {"index": 2, "start_time": 60, "end_time": 90},
            ]

            total_segments = len(segments_data)
            logger.info(
                f"Creating {total_segments} segments for file {event.original_file_id}"
            )

            for seg_meta in segments_data:
                segment_index = seg_meta["index"]
                segment_id = uuid.uuid4()
                segment_minio_object_name = f"segments/{event.original_file_id}/{file_process.id}/{segment_id}.{event.converted_file_format}"

                # 3. Create segment file (placeholder - just copy the original for now)
                # TODO: Replace with actual segment extraction
                success = upload_file_from_tmp(
                    client=minio_client,
                    local_file_path=downloaded_converted_path,
                    bucket_name=settings.MINIO_PRIMARY_BUCKET,
                    target_object_name=segment_minio_object_name,
                    logger=logger,
                    cleanup_tmp_file=(
                        segment_index == total_segments - 1
                    ),  # Only cleanup on last segment
                    metadata={
                        "original_file_id": str(event.original_file_id),
                        "user_id": str(event.user_id),
                        "segment_index": str(segment_index),
                        "file_process_id": str(file_process.id),
                    },
                )

                if not success:
                    raise Exception(
                        f"Failed to upload segment: {segment_minio_object_name}"
                    )

                # 4. Create FileSegment record in DB
                segment_create = FileSegmentCreate(
                    file_process_id=file_process.id,
                    original_file_id=event.original_file_id,
                    user_id=event.user_id,
                    segment_index=segment_index,
                    minio_object_name=segment_minio_object_name,
                    status=ProcessStatus.PENDING,
                )

                segment_db_record = crud.create_file_segment(
                    session=session, segment_in=segment_create
                )
                logger.info(
                    f"Created FileSegment {segment_db_record.id} for file {event.original_file_id}"
                )

                # 5. Send event for Transcription stage for this segment
                transcription_trigger_event = SegmentCreatedEvent(
                    trace_id=event.trace_id,
                    original_file_id=event.original_file_id,
                    user_id=event.user_id,
                    parent_converted_file_minio_path=event.converted_file_minio_path,
                    segment_id=segment_db_record.id,
                    segment_minio_path=segment_minio_object_name,
                    segment_index=segment_index,
                    total_segments=total_segments,
                    target_hardware=event.target_hardware,  # Propagate hardware preference
                )

                # Determine target queue for transcription based on hardware preference
                target_hw = event.target_hardware.lower()
                if target_hw == "gpu":  # Map 'gpu' to 'cuda' for consistency
                    target_hw = "cuda"

                if target_hw not in ["cpu", "cuda", "rocm"]:
                    logger.warning(
                        f"Unsupported target_hardware '{event.target_hardware}', defaulting to CPU queue for segment {segment_db_record.id}."
                    )
                    target_transcription_queue = "transcribe_cpu"
                else:
                    target_transcription_queue = f"transcribe_{target_hw}"

                # Send to the chosen transcription queue
                transcription_actor.send_with_options(
                    args=(transcription_trigger_event.model_dump(),),
                    queue_name=target_transcription_queue,
                )
                logger.info(
                    f"Sent SegmentCreatedEvent for segment {segment_db_record.id} to queue {target_transcription_queue} (target_hardware: {event.target_hardware})"
                )

            crud.update_file_process_status(
                session=session,
                process_id=file_process.id,
                status=ProcessStatus.PROCESSING.value,
                error_message=f"Segmentation completed, {total_segments} segments created, pending transcription",
            )

    except Exception as e:
        logger.error(
            f"Error in segmentation_actor for file {event_data.get('original_file_id')}: {e}",
            exc_info=True,
        )

        # Update FileProcess to FAILED
        try:
            with Session(engine) as session:
                processes = crud.get_file_processes_by_file(
                    session=session, file_id=event_data.get("original_file_id"), limit=1
                )
                if processes:
                    crud.update_file_process_status(
                        session=session,
                        process_id=processes[0].id,
                        status=ProcessStatus.FAILED.value,
                        error_message=f"Segmentation failed: {str(e)}",
                    )
        except Exception as db_e:
            logger.error(f"Failed to update FileProcess status: {db_e}")

        raise


# --- Stage 3: Transcription Actor ---


@dramatiq.actor(time_limit=900000)  # 15 minutes - queue_name specified at send time
def transcription_actor(event_data: dict[str, Any]):
    """
    Stage 3: Transcribe an individual audio segment.
    Consumes: SegmentCreatedEvent
    Produces: TranscriptionCompletedEvent
    """
    try:
        event = SegmentCreatedEvent.model_validate(event_data)
        logger.info(
            f"Transcription actor received event for segment: {event.segment_id} (target_hardware: {event.target_hardware})"
        )

        with Session(engine) as session:
            # Update FileSegment status to PROCESSING
            segment_record = crud.get_file_segment(
                session=session, segment_id=event.segment_id
            )
            if not segment_record:
                raise Exception(f"FileSegment {event.segment_id} not found")

            crud.update_file_segment_status(
                session=session,
                segment_id=event.segment_id,
                status=ProcessStatus.PROCESSING.value,
            )

            # 1. Download audio segment from MinIO
            minio_client = get_minio_client(settings, logger)
            segment_audio_path = download_file_to_tmp(
                client=minio_client,
                bucket_name=settings.MINIO_PRIMARY_BUCKET,
                object_name=event.segment_minio_path,
                logger=logger,
            )

            if not segment_audio_path:
                raise Exception(
                    f"Failed to download segment: {event.segment_minio_path}"
                )

            # 2. Perform transcription (placeholder - replace with actual speech-to-text)
            # TODO: Replace with actual transcription using OpenAI Whisper, Google Speech-to-Text, etc.
            transcription_text_result = f"Placeholder transcription for segment {event.segment_index} of file {event.original_file_id}. This would contain the actual transcribed text from the audio segment."

            # Clean up the temporary audio file
            try:
                os.remove(segment_audio_path)
            except Exception as cleanup_e:
                logger.warning(
                    f"Failed to cleanup temp file {segment_audio_path}: {cleanup_e}"
                )

            # 3. Store transcription result to MinIO
            transcription_output_minio_path = (
                f"transcriptions/{event.original_file_id}/{event.segment_id}.txt"
            )

            success = _upload_string_to_minio(
                content=transcription_text_result,
                bucket_name=settings.MINIO_PRIMARY_BUCKET,
                object_name=transcription_output_minio_path,
            )

            if not success:
                raise Exception(
                    f"Failed to upload transcription: {transcription_output_minio_path}"
                )

            logger.info(
                f"Transcription result for segment {event.segment_id} stored at {transcription_output_minio_path}"
            )

            # 4. Update FileSegment record with result and COMPLETED status
            crud.update_file_segment_transcription_result(
                session=session,
                segment_id=event.segment_id,
                status=ProcessStatus.COMPLETED.value,
                result_path=transcription_output_minio_path,
                summary=transcription_text_result[:100] + "..."
                if len(transcription_text_result) > 100
                else transcription_text_result,
            )

        # 5. Send event for final processing/aggregation
        final_event = TranscriptionCompletedEvent(
            trace_id=event.trace_id,
            original_file_id=event.original_file_id,
            user_id=event.user_id,
            segment_id=event.segment_id,
            transcription_text_minio_path=transcription_output_minio_path,
            transcription_summary=transcription_text_result[:100] + "..."
            if len(transcription_text_result) > 100
            else transcription_text_result,
            target_hardware=event.target_hardware,  # Propagate hardware preference
        )

        final_processing_actor.send(final_event.model_dump())
        logger.info(
            f"Sent TranscriptionCompletedEvent for segment {event.segment_id} (target_hardware: {event.target_hardware})"
        )

    except Exception as e:
        logger.error(
            f"Error in transcription_actor for segment {event_data.get('segment_id')}: {e}",
            exc_info=True,
        )

        # Update FileSegment to FAILED
        try:
            with Session(engine) as session:
                crud.update_file_segment_status(
                    session=session,
                    segment_id=event_data.get("segment_id"),
                    status=ProcessStatus.FAILED.value,
                    error_message=f"Transcription failed: {str(e)}",
                )
        except Exception as db_e:
            logger.error(f"Failed to update FileSegment status: {db_e}")

        raise


# --- Stage 4: Final Processing Actor ---


@dramatiq.actor(queue_name="final_cpu", time_limit=60000)  # 1 minute
def final_processing_actor(event_data: dict[str, Any]):
    """
    Stage 4: Handle completion of individual segment transcriptions and check if all segments are done.
    Consumes: TranscriptionCompletedEvent
    """
    try:
        event = TranscriptionCompletedEvent.model_validate(event_data)
        logger.info(
            f"Final processing actor received event for segment: {event.segment_id}"
        )

        with Session(engine) as session:
            # Get all segments for this file process to check completion status
            processes = crud.get_file_processes_by_file(
                session=session, file_id=event.original_file_id, limit=1
            )
            if not processes:
                logger.warning(
                    f"FileProcess not found for file: {event.original_file_id}"
                )
                return

            file_process = processes[0]
            all_segments = crud.get_file_segments_by_process(
                session=session, process_id=file_process.id
            )

            # Check if all segments are completed
            completed_segments = [
                s for s in all_segments if s.status == ProcessStatus.COMPLETED
            ]
            failed_segments = [
                s for s in all_segments if s.status == ProcessStatus.FAILED
            ]

            logger.info(
                f"File {event.original_file_id}: {len(completed_segments)}/{len(all_segments)} segments completed, {len(failed_segments)} failed"
            )

            # If all segments are either completed or failed, mark the overall process as done
            if len(completed_segments) + len(failed_segments) == len(all_segments):
                if len(failed_segments) == 0:
                    # All segments completed successfully
                    crud.update_file_process_status(
                        session=session,
                        process_id=file_process.id,
                        status=ProcessStatus.COMPLETED.value,
                        error_message=f"All {len(completed_segments)} segments transcribed successfully",
                    )
                    logger.info(f"FileProcess {file_process.id} marked as COMPLETED")
                else:
                    # Some segments failed
                    crud.update_file_process_status(
                        session=session,
                        process_id=file_process.id,
                        status=ProcessStatus.FAILED.value,
                        error_message=f"{len(completed_segments)} segments completed, {len(failed_segments)} segments failed",
                    )
                    logger.warning(
                        f"FileProcess {file_process.id} marked as FAILED due to segment failures"
                    )

    except Exception as e:
        logger.error(
            f"Error in final_processing_actor for segment {event_data.get('segment_id')}: {e}",
            exc_info=True,
        )
        # This actor is non-critical, so we don't re-raise the exception
