import logging
import uuid

import requests  # For making the HTTP PUT request to MinIO
from fastapi.testclient import TestClient

from app.core.config import settings

# normal_user_token_headers is a pytest fixture, not a direct import from user utils
# from app.tests.utils.user import normal_user_token_headers
# Assuming get_minio_client can be used here for test verification/cleanup client
# If it needs a logger, we can pass one or adapt its usage for tests.
from app.services.minio_service import get_minio_client

logger = logging.getLogger(__name__)


def test_generate_presigned_url_and_upload_file(
    client: TestClient,
    normal_user_token_headers: dict[str, str],
    # db: Session, # db fixture removed as it's unused
) -> None:
    """
    Test generating a presigned URL and then using it to upload a file.
    Also verifies the file exists in MinIO and cleans it up.
    """
    test_filename = f"test_upload_{uuid.uuid4()}.txt"
    file_content = b"This is a test file for MinIO upload."
    content_type = "text/plain"

    # 1. Get a presigned URL from the backend
    presigned_url_request_data = {
        "filename": test_filename,
        "content_type": content_type,
    }
    response = client.post(
        f"{settings.API_V1_STR}/uploads/presigned-url",
        headers=normal_user_token_headers,
        json=presigned_url_request_data,
    )
    assert response.status_code == 200, f"Failed to get presigned URL: {response.text}"
    presigned_url_data = response.json()
    upload_url = presigned_url_data["url"]
    object_name_in_bucket = presigned_url_data["object_name"]

    assert upload_url is not None
    assert object_name_in_bucket.endswith(test_filename)

    # 2. Upload the file to MinIO using the presigned URL
    headers = {}
    # If your presigned URL generation on the backend specified a content_type,
    # MinIO will expect it on the PUT request.
    if content_type:
        headers["Content-Type"] = content_type

    upload_response = requests.put(upload_url, data=file_content, headers=headers)
    # MinIO typically returns 200 OK on successful PUT via presigned URL
    assert (
        upload_response.status_code == 200
    ), f"File upload to MinIO failed with status {upload_response.status_code}: {upload_response.text}"

    # 3. Verify the file exists in MinIO using a direct client (and then clean up)
    minio_verify_client = None
    try:
        minio_verify_client = get_minio_client(settings, logger)

        # Check if object exists
        minio_verify_client.stat_object(
            settings.MINIO_UPLOADS_BUCKET, object_name_in_bucket
        )
        logger.info(f"Successfully verified {object_name_in_bucket} in MinIO.")

    except Exception as e:
        raise AssertionError(f"File verification/cleanup in MinIO failed: {e}")
    finally:
        # 4. Clean up: Remove the test file from MinIO
        if minio_verify_client:
            try:
                minio_verify_client.remove_object(
                    settings.MINIO_UPLOADS_BUCKET, object_name_in_bucket
                )
                logger.info(
                    f"Successfully cleaned up {object_name_in_bucket} from MinIO."
                )
            except Exception as e:
                logger.error(
                    f"Failed to clean up test file {object_name_in_bucket} from MinIO: {e}"
                )
