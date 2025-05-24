import logging
import uuid
from datetime import timedelta
from typing import Any

from fastapi import APIRouter, HTTPException
from minio.error import S3Error
from pydantic import BaseModel

from app import crud
from app.api.deps import CurrentUser, SessionDep
from app.core.config import settings
from app.models import FileCreate
from app.services.minio_service import (
    abort_multipart_upload,
    complete_multipart_upload,
    generate_presigned_part_url,
    generate_presigned_put_url,  # Just import the function we need
    get_minio_client,
    initiate_multipart_upload,
)

router = APIRouter(prefix="/uploads", tags=["uploads"])
logger = logging.getLogger(__name__)


class PresignedUrlRequest(BaseModel):
    filename: str
    content_type: str | None = None


class PresignedUrlResponse(BaseModel):
    url: str
    object_name: str  # The actual object name in MinIO, might include a path prefix
    file_id: str  # The database file ID for later processing
    # This dictionary will contain all headers the frontend needs to set.
    # Keys are header names (e.g., "Content-Type", "X-Amz-Meta-FileId"),
    # values are the corresponding header values.
    headers_to_set: dict[str, str]


class InitiateMultipartRequest(BaseModel):
    filename: str
    content_type: str | None = None
    file_size_bytes: int | None = None  # Optional, for validation


class InitiateMultipartResponse(BaseModel):
    upload_id: str
    object_name: str
    file_id: str
    recommended_part_size: int  # Recommended size for each part (except possibly the last)
    total_parts: int  # Expected number of parts based on file size


class PresignedPartUrlRequest(BaseModel):
    file_id: str
    upload_id: str
    part_number: int  # 1-indexed


class PresignedPartUrlResponse(BaseModel):
    url: str
    part_number: int
    headers_to_set: dict[str, str]


class CompleteMultipartRequest(BaseModel):
    file_id: str
    upload_id: str
    parts: list[dict[str, Any]]  # Each part should have 'part_number' and 'etag'


class CompleteMultipartResponse(BaseModel):
    file_id: str
    object_name: str
    etag: str
    location: str


@router.post("/presigned-url", response_model=PresignedUrlResponse)
def create_presigned_upload_url(
    request_data: PresignedUrlRequest,
    current_user: CurrentUser,  # Protect endpoint: only logged-in users can get upload URLs
    session: SessionDep,  # Add database session dependency
    # minio_client: Minio = Depends(get_minio_client) # If get_minio_client was a dependency
) -> PresignedUrlResponse:
    """
    Generates a presigned URL for uploading a file directly to MinIO
    and creates a corresponding File record in the database.
    """
    logger.info(
        f"Received request for presigned URL: filename='{request_data.filename}', content_type='{request_data.content_type}' by user ID '{current_user.id}'"
    )
    # You prefix filenames with user ID or a UUID to avoid collisions
    unique_file_key = f"{uuid.uuid4()}_{request_data.filename}"
    object_name_in_bucket = f"user_uploads/{current_user.id}/{unique_file_key}"
    logger.debug(f"Generated object name in bucket: {object_name_in_bucket}")

    # 1. Create File record in database BEFORE generating presigned URL
    file_in = FileCreate(
        original_filename=request_data.filename,
        minio_object_name=object_name_in_bucket,
        file_type=request_data.content_type,
        user_id=current_user.id,
        # size_bytes will be None initially, set when upload completes
        # upload_started_at will be set by model default
        # upload_completed_at will be None initially
    )

    try:
        db_file = crud.create_file(
            session=session, file_in=file_in, user_id=current_user.id
        )
        logger.info(
            f"Created File record with ID: {db_file.id} for object: {object_name_in_bucket}"
        )
    except Exception as e:
        logger.error(
            f"Failed to create File record for {object_name_in_bucket} in database: {e}",
            exc_info=True,
        )
        raise HTTPException(
            status_code=500, detail="Could not create file record in database."
        )

    # 2. Generate presigned URL
    try:
        logger.info(
            f"Calling generate_presigned_put_url for bucket='{settings.MINIO_PRIMARY_BUCKET}', object='{object_name_in_bucket}'"
        )
        # Use the new function that handles public endpoint properly
        presigned_url = generate_presigned_put_url(
            settings=settings,
            bucket_name=settings.MINIO_PRIMARY_BUCKET,
            object_name=object_name_in_bucket,
            expires=timedelta(
                minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES
            ),  # Ensure this is adequate
            logger=logger,
        )
        logger.info(
            f"Successfully obtained presigned URL from service: {presigned_url}"
        )
    except Exception as e:
        logger.error(
            f"Failed to generate presigned URL for {object_name_in_bucket}: {e}",
            exc_info=True,  # This will log the full traceback from generate_presigned_put_url
        )
        # Note: In a production system, you might want to rollback the database record here
        crud.delete_file(
            session=session, file_id=db_file.id
        )  # Good: rollback is present
        logger.info(
            f"Rolled back File record with ID: {db_file.id} due to presigned URL generation failure."
        )
        # Provide more specific error to client if possible, but avoid leaking too much detail
        detail_message = "Could not generate presigned URL."
        if isinstance(e, ValueError):  # e.g. from invalid endpoint URL
            detail_message = (
                f"Could not generate presigned URL: Invalid configuration. {e}"
            )
        elif isinstance(e, S3Error):  # from MinIO client
            detail_message = f"Could not generate presigned URL: Storage error. {e.code if hasattr(e, 'code') else e}"

        raise HTTPException(status_code=500, detail=detail_message)

    # 3. Pass the headers to the browser
    client_headers = {}
    if request_data.content_type:
        client_headers["Content-Type"] = request_data.content_type
    logger.debug(f"Headers to set for client: {client_headers}")

    response = PresignedUrlResponse(
        url=presigned_url,  # Use the presigned URL directly
        object_name=object_name_in_bucket,
        file_id=str(db_file.id),
        headers_to_set=client_headers,
    )
    logger.info(
        f"Returning presigned URL response: URL starts with {presigned_url[:70]}..., object_name='{object_name_in_bucket}'"
    )
    return response


@router.post("/multipart/initiate", response_model=InitiateMultipartResponse)
def initiate_multipart_upload_endpoint(
    request_data: InitiateMultipartRequest,
    current_user: CurrentUser,
    session: SessionDep,
) -> InitiateMultipartResponse:
    """
    Initiate a multipart upload for large files.
    Creates a database record and returns an upload ID for subsequent part uploads.
    """
    logger.info(
        f"Initiating multipart upload for '{request_data.filename}' by user {current_user.id}"
    )

    # Generate unique object name
    unique_file_key = f"{uuid.uuid4()}_{request_data.filename}"
    object_name_in_bucket = f"user_uploads/{current_user.id}/{unique_file_key}"

    # Create file record in database
    file_in = FileCreate(
        original_filename=request_data.filename,
        minio_object_name=object_name_in_bucket,
        file_type=request_data.content_type,
        user_id=current_user.id,
        size_bytes=request_data.file_size_bytes,  # Store expected size if provided
    )

    try:
        db_file = crud.create_file(
            session=session, file_in=file_in, user_id=current_user.id
        )
        logger.info(f"Created File record with ID: {db_file.id}")
    except Exception as e:
        logger.error(f"Failed to create File record: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail="Could not create file record in database."
        )

    # Initiate multipart upload in MinIO
    try:
        minio_client = get_minio_client(settings, logger)
        upload_id = initiate_multipart_upload(
            client=minio_client,
            bucket_name=settings.MINIO_PRIMARY_BUCKET,
            object_name=object_name_in_bucket,
            logger=logger,
        )

        # Calculate recommended part size and total parts
        # MinIO recommends 5MB minimum part size, max 5GB
        # For very large files, we might want larger parts to stay under 10,000 parts limit
        file_size = request_data.file_size_bytes or 0
        if file_size <= 100 * 1024 * 1024:  # 100MB
            part_size = 5 * 1024 * 1024  # 5MB parts
        elif file_size <= 1024 * 1024 * 1024:  # 1GB
            part_size = 10 * 1024 * 1024  # 10MB parts
        elif file_size <= 10 * 1024 * 1024 * 1024:  # 10GB
            part_size = 50 * 1024 * 1024  # 50MB parts
        else:
            part_size = 100 * 1024 * 1024  # 100MB parts for very large files

        total_parts = (file_size + part_size - 1) // part_size if file_size > 0 else 1

        logger.info(
            f"Multipart upload initiated with ID: {upload_id}, recommended part size: {part_size}, total parts: {total_parts}"
        )

        return InitiateMultipartResponse(
            upload_id=upload_id,
            object_name=object_name_in_bucket,
            file_id=str(db_file.id),
            recommended_part_size=part_size,
            total_parts=total_parts,
        )

    except Exception as e:
        logger.error(f"Failed to initiate multipart upload: {e}", exc_info=True)
        # Clean up database record
        crud.delete_file(session=session, file_id=db_file.id)
        raise HTTPException(
            status_code=500, detail=f"Could not initiate multipart upload: {str(e)}"
        )


@router.post("/multipart/part-url", response_model=PresignedPartUrlResponse)
def get_multipart_part_url(
    request_data: PresignedPartUrlRequest,
    current_user: CurrentUser,
    session: SessionDep,
) -> PresignedPartUrlResponse:
    """
    Generate a presigned URL for uploading a specific part of a multipart upload.
    """
    logger.info(
        f"Generating presigned URL for part {request_data.part_number} of upload {request_data.upload_id}"
    )

    # Verify the file belongs to the user
    db_file = crud.get_file(session=session, file_id=request_data.file_id)
    if not db_file or db_file.user_id != current_user.id:
        raise HTTPException(status_code=404, detail="File not found or access denied")

    try:
        presigned_url = generate_presigned_part_url(
            settings=settings,
            bucket_name=settings.MINIO_PRIMARY_BUCKET,
            object_name=db_file.minio_object_name,
            upload_id=request_data.upload_id,
            part_number=request_data.part_number,
            expires=timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES),
            logger=logger,
        )

        return PresignedPartUrlResponse(
            url=presigned_url,
            part_number=request_data.part_number,
            headers_to_set={},  # Part uploads typically don't need special headers
        )

    except Exception as e:
        logger.error(f"Failed to generate part presigned URL: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Could not generate presigned URL for part: {str(e)}",
        )


@router.post("/multipart/complete", response_model=CompleteMultipartResponse)
def complete_multipart_upload_endpoint(
    request_data: CompleteMultipartRequest,
    current_user: CurrentUser,
    session: SessionDep,
) -> CompleteMultipartResponse:
    """
    Complete a multipart upload by combining all uploaded parts.
    """
    logger.info(
        f"Completing multipart upload {request_data.upload_id} for file {request_data.file_id}"
    )

    # Verify the file belongs to the user
    db_file = crud.get_file(session=session, file_id=request_data.file_id)
    if not db_file or db_file.user_id != current_user.id:
        raise HTTPException(status_code=404, detail="File not found or access denied")

    try:
        minio_client = get_minio_client(settings, logger)
        result = complete_multipart_upload(
            client=minio_client,
            bucket_name=settings.MINIO_PRIMARY_BUCKET,
            object_name=db_file.minio_object_name,
            upload_id=request_data.upload_id,
            parts=request_data.parts,
            logger=logger,
        )

        # Update file record to mark upload as complete
        # You might want to add upload_completed_at field to your model
        # crud.update_file_upload_complete(session=session, file_id=db_file.id)

        logger.info(
            f"Multipart upload completed successfully with ETag: {result['etag']}"
        )

        return CompleteMultipartResponse(
            file_id=str(db_file.id),
            object_name=db_file.minio_object_name,
            etag=result["etag"],
            location=result["location"],
        )

    except Exception as e:
        logger.error(f"Failed to complete multipart upload: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Could not complete multipart upload: {str(e)}"
        )


@router.post("/multipart/abort")
def abort_multipart_upload_endpoint(
    file_id: str,
    upload_id: str,
    current_user: CurrentUser,
    session: SessionDep,
) -> dict[str, str]:
    """
    Abort a multipart upload and clean up resources.
    """
    logger.info(f"Aborting multipart upload {upload_id} for file {file_id}")

    # Verify the file belongs to the user
    db_file = crud.get_file(session=session, file_id=file_id)
    if not db_file or db_file.user_id != current_user.id:
        raise HTTPException(status_code=404, detail="File not found or access denied")

    try:
        minio_client = get_minio_client(settings, logger)
        abort_multipart_upload(
            client=minio_client,
            bucket_name=settings.MINIO_PRIMARY_BUCKET,
            object_name=db_file.minio_object_name,
            upload_id=upload_id,
            logger=logger,
        )

        # Optionally delete the file record
        crud.delete_file(session=session, file_id=db_file.id)

        return {
            "status": "aborted",
            "message": f"Multipart upload {upload_id} aborted successfully",
        }

    except Exception as e:
        logger.error(f"Failed to abort multipart upload: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Could not abort multipart upload: {str(e)}"
        )
