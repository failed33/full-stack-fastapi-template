import logging
import uuid
from datetime import timedelta

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.api.deps import CurrentUser  # Optional: if you want to protect this endpoint
from app.core.config import settings
from app.services.minio_service import (
    get_minio_client,  # Assuming get_minio_client is here
)

router = APIRouter(prefix="/uploads", tags=["uploads"])
logger = logging.getLogger(__name__)


class PresignedUrlRequest(BaseModel):
    filename: str
    content_type: str | None = None


class PresignedUrlResponse(BaseModel):
    url: str
    object_name: str  # The actual object name in MinIO, might include a path prefix


@router.post("/presigned-url", response_model=PresignedUrlResponse)
def create_presigned_upload_url(
    request_data: PresignedUrlRequest,
    current_user: CurrentUser,  # Protect endpoint: only logged-in users can get upload URLs
    # minio_client: Minio = Depends(get_minio_client) # If get_minio_client was a dependency
) -> PresignedUrlResponse:
    """
    Generates a presigned URL for uploading a file directly to MinIO.
    """
    try:
        # Use the global settings for MinIO client config within the endpoint
        # This is similar to how db sessions are handled rather than direct dependency injection
        # of the client, to align with typical FastAPI patterns.
        minio_client = get_minio_client(settings, logger)
    except ValueError as e:
        logger.error(f"MinIO client configuration error: {e}")
        raise HTTPException(status_code=500, detail="MinIO client configuration error.")

    # You prefix filenames with user ID or a UUID to avoid collisions
    object_name_in_bucket = (
        f"user_uploads/{current_user.id}/{uuid.uuid4()}_{request_data.filename}"
    )

    try:
        presigned_url = minio_client.presigned_put_object(
            bucket_name=settings.MINIO_PRIMARY_BUCKET,
            object_name=object_name_in_bucket,
            expires=timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES),
        )
        return PresignedUrlResponse(
            url=presigned_url, object_name=object_name_in_bucket
        )
    except Exception as e:
        logger.error(
            f"Failed to generate presigned URL for {object_name_in_bucket}: {e}",
            exc_info=True,
        )
        raise HTTPException(
            status_code=500, detail=f"Could not generate presigned URL: {e}"
        )
