from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel
from pydantic.networks import EmailStr

from app.api.deps import get_current_active_superuser
from app.core.config import settings
from app.models import Message
from app.utils import generate_test_email, send_email

router = APIRouter(prefix="/utils", tags=["utils"])


class SSEUrlResponse(BaseModel):
    url: str


@router.post(
    "/test-email/",
    dependencies=[Depends(get_current_active_superuser)],
    status_code=201,
)
def test_email(email_to: EmailStr) -> Message:
    """
    Test emails.
    """
    email_data = generate_test_email(email_to=email_to)
    send_email(
        email_to=email_to,
        subject=email_data.subject,
        html_content=email_data.html_content,
    )
    return Message(message="Test email sent")


@router.get("/health-check/")
async def health_check() -> bool:
    return True


@router.get("/sse-url/", response_model=SSEUrlResponse)
async def get_sse_url(request: Request):
    """
    Provides the full SSE URL for client connections.
    """
    host = request.url.hostname
    port = request.url.port
    # Determine SSE scheme (http or https) based on request scheme
    sse_scheme = request.url.scheme  # http or https

    # Construct the path based on API_V1_STR and router prefixes
    # settings.API_V1_STR (e.g., /api/v1)
    # + "/sse" (prefix for sse_router in api_router)
    # + "/stream" (path defined in sse_router itself)

    path_parts = [
        settings.API_V1_STR.strip("/"),
        "sse".strip("/"),  # The prefix for SSE routes (from app.api.main.py)
        "stream".strip("/"),  # The specific path from app.sse_stream.router.py
    ]
    sse_path = "/" + "/".join(part for part in path_parts if part)

    # Construct the base URL part (scheme://host:port)
    base_url_part = f"{sse_scheme}://{host}"
    if port and (
        (sse_scheme == "http" and port != 80) or (sse_scheme == "https" and port != 443)
    ):
        base_url_part += f":{port}"

    sse_full_url = f"{base_url_part}{sse_path}"

    return SSEUrlResponse(url=sse_full_url)
