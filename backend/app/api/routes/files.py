import logging
import uuid

from fastapi import APIRouter, HTTPException
from pydantic import Field
from sqlmodel import SQLModel

from app import crud
from app.api.deps import CurrentUser, SessionDep

# Import the pipeline components
from app.core.dramatiq_setup import get_pipeline_actors
from app.models import (
    FileProcessCreate,
    FileProcessPublic,
    FileSegmentPublic,
    ProcessStatus,
    ProcessType,
)
from app.schemas.pipeline_events import FileReadyForConversionEvent

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/files", tags=["files"])


class StartProcessRequest(SQLModel):
    """Request model for starting file processing."""

    process_type: ProcessType = ProcessType.TRANSCRIPTION
    target_hardware: str = Field(
        default="cpu", description="Preferred hardware: cpu, cuda, rocm"
    )


@router.post("/{file_id}/start-process", response_model=FileProcessPublic)
def start_file_processing(
    *,
    session: SessionDep,
    current_user: CurrentUser,
    file_id: uuid.UUID,
    request_data: StartProcessRequest,
) -> FileProcessPublic:
    """
    Starts a new processing task for a given file.

    Args:
        session: Database session dependency
        current_user: Authenticated user from JWT token
        file_id: UUID of the file to process
        request_data: Processing request containing process_type

    Returns:
        FileProcessPublic: The created FileProcess record

    Raises:
        HTTPException: 404 if file not found, 403 if unauthorized,
                      400 if upload not complete, 500 if processing failed
    """
    logger.info(
        f"User {current_user.id} attempting to start process type "
        f"'{request_data.process_type.value}' for file {file_id}"
    )

    # 1. Validate file exists and belongs to user
    db_file = crud.get_file(session=session, file_id=file_id)

    if not db_file:
        logger.warning(f"File {file_id} not found for user {current_user.id}.")
        raise HTTPException(status_code=404, detail="File not found")

    if db_file.user_id != current_user.id:
        logger.warning(
            f"User {current_user.id} not authorized to process file {file_id} "
            f"(owner is {db_file.user_id})."
        )
        raise HTTPException(
            status_code=403, detail="Not authorized to process this file"
        )

    # 2. Validate upload is complete
    if not db_file.upload_completed_at:
        logger.warning(f"File {file_id} upload not complete. Cannot start process.")
        raise HTTPException(status_code=400, detail="File upload is not yet complete")

    # 3. Optional: Check for existing active processes to prevent duplicates
    existing_processes = crud.get_file_processes_by_file(
        session=session, file_id=file_id
    )
    for proc in existing_processes:
        if proc.process_type == request_data.process_type and proc.status in [
            ProcessStatus.PENDING,
            ProcessStatus.PROCESSING,
        ]:
            logger.warning(
                f"Process type '{request_data.process_type.value}' already pending/processing "
                f"for file {file_id} (FileProcess ID: {proc.id})"
            )
            raise HTTPException(
                status_code=409,
                detail=f"Process type '{request_data.process_type.value}' is already in progress for this file.",
            )

    # 4. Create FileProcess record
    process_in = FileProcessCreate(
        file_id=db_file.id,
        user_id=current_user.id,
        process_type=request_data.process_type,
        status=ProcessStatus.PENDING,
    )

    try:
        db_file_process = crud.create_file_process(
            session=session, process_in=process_in
        )
        logger.info(
            f"Created FileProcess record ID {db_file_process.id} for File ID {file_id}, "
            f"process type: {request_data.process_type.value}"
        )
    except Exception as e:
        logger.error(
            f"DB error creating FileProcess for File ID {file_id}: {e}", exc_info=True
        )
        raise HTTPException(
            status_code=500, detail="Could not create file process record."
        )

    # 5. Send task to Dramatiq pipeline
    try:
        # Generate trace ID for this workflow
        trace_id = uuid.uuid4()

        # Create pipeline event to trigger processing
        pipeline_event = FileReadyForConversionEvent(
            trace_id=trace_id,
            original_file_id=db_file.id,
            user_id=current_user.id,
            minio_object_name=db_file.minio_object_name,
            original_filename=db_file.original_filename,
            target_hardware=request_data.target_hardware,
        )

        # Send to conversion actor to start the pipeline
        actors = get_pipeline_actors()
        actors["conversion"].send(pipeline_event.model_dump())

        logger.info(
            f"Sent pipeline event to Dramatiq for FileProcess ID {db_file_process.id}, "
            f"File: '{db_file.original_filename}', Type: {request_data.process_type.value}, "
            f"Trace ID: {trace_id}"
        )
    except Exception as e:
        logger.error(
            f"Failed to send pipeline task to Dramatiq for FileProcess ID {db_file_process.id}: {e}",
            exc_info=True,
        )

        # Mark the DB record as FAILED since queuing failed
        try:
            crud.update_file_process_status(
                session=session,
                process_id=db_file_process.id,
                status=ProcessStatus.FAILED,
                error_message="Failed to queue processing task.",
            )
            logger.info(
                f"Marked FileProcess ID {db_file_process.id} as FAILED due to queueing error."
            )
        except Exception as db_update_e:
            logger.error(
                f"Additionally failed to update FileProcess ID {db_file_process.id} status "
                f"after queueing error: {db_update_e}",
                exc_info=True,
            )

        raise HTTPException(
            status_code=500, detail="Could not queue file processing task."
        )

    return db_file_process


@router.get("/{file_id}/processes", response_model=list[FileProcessPublic])
def get_file_processes(
    *,
    session: SessionDep,
    current_user: CurrentUser,
    file_id: uuid.UUID,
    skip: int = 0,
    limit: int = 100,
) -> list[FileProcessPublic]:
    """
    Get all processing records for a specific file.

    Args:
        session: Database session dependency
        current_user: Authenticated user from JWT token
        file_id: UUID of the file to get processes for
        skip: Number of records to skip (pagination)
        limit: Maximum number of records to return (pagination)

    Returns:
        list[FileProcessPublic]: List of FileProcess records for the file

    Raises:
        HTTPException: 404 if file not found, 403 if unauthorized
    """
    # Validate file exists and belongs to user
    db_file = crud.get_file(session=session, file_id=file_id)

    if not db_file:
        raise HTTPException(status_code=404, detail="File not found")

    if db_file.user_id != current_user.id:
        raise HTTPException(
            status_code=403, detail="Not authorized to access this file"
        )

    # Get all processes for this file
    processes = crud.get_file_processes_by_file(
        session=session, file_id=file_id, skip=skip, limit=limit
    )

    return processes


@router.get("/{file_id}/processes/{process_id}", response_model=FileProcessPublic)
def get_file_process(
    *,
    session: SessionDep,
    current_user: CurrentUser,
    file_id: uuid.UUID,
    process_id: uuid.UUID,
) -> FileProcessPublic:
    """
    Get a specific processing record for a file.

    Args:
        session: Database session dependency
        current_user: Authenticated user from JWT token
        file_id: UUID of the file
        process_id: UUID of the specific process

    Returns:
        FileProcessPublic: The requested FileProcess record

    Raises:
        HTTPException: 404 if file/process not found, 403 if unauthorized
    """
    # Validate file exists and belongs to user
    db_file = crud.get_file(session=session, file_id=file_id)

    if not db_file:
        raise HTTPException(status_code=404, detail="File not found")

    if db_file.user_id != current_user.id:
        raise HTTPException(
            status_code=403, detail="Not authorized to access this file"
        )

    # Get the specific process
    db_process = crud.get_file_process(session=session, process_id=process_id)

    if not db_process:
        raise HTTPException(status_code=404, detail="Process not found")

    # Validate the process belongs to the specified file
    if db_process.file_id != file_id:
        raise HTTPException(status_code=404, detail="Process not found for this file")

    return db_process


@router.get(
    "/{file_id}/processes/{process_id}/segments", response_model=list[FileSegmentPublic]
)
def get_process_segments(
    *,
    session: SessionDep,
    current_user: CurrentUser,
    file_id: uuid.UUID,
    process_id: uuid.UUID,
    skip: int = 0,
    limit: int = 100,
) -> list[FileSegmentPublic]:
    """
    Get all segments for a specific file process.

    Args:
        session: Database session dependency
        current_user: Authenticated user from JWT token
        file_id: UUID of the file
        process_id: UUID of the process
        skip: Number of records to skip (pagination)
        limit: Maximum number of records to return (pagination)

    Returns:
        list[FileSegmentPublic]: List of FileSegment records for the process

    Raises:
        HTTPException: 404 if file/process not found, 403 if unauthorized
    """
    # Validate file exists and belongs to user
    db_file = crud.get_file(session=session, file_id=file_id)

    if not db_file:
        raise HTTPException(status_code=404, detail="File not found")

    if db_file.user_id != current_user.id:
        raise HTTPException(
            status_code=403, detail="Not authorized to access this file"
        )

    # Get the process to validate it exists and belongs to this file
    db_process = crud.get_file_process(session=session, process_id=process_id)

    if not db_process:
        raise HTTPException(status_code=404, detail="Process not found")

    if db_process.file_id != file_id:
        raise HTTPException(status_code=404, detail="Process not found for this file")

    # Get all segments for this process
    segments = crud.get_file_segments_by_process(
        session=session, process_id=process_id, skip=skip, limit=limit
    )

    return segments
