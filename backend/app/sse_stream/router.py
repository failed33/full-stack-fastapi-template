"""
Defines the API endpoint for Server-Sent Event (SSE) streams.

This module provides the `/stream` GET endpoint that clients can connect to
receive real-time updates from the server using the SSE protocol. It handles
client connections, event generation, and disconnections.

Key functionalities:
- **SSE Endpoint (`/stream`):**
  - Authenticates users via a JWT token passed as a query parameter (`?token=...`).
  - Uses the `get_current_user_ws` dependency for authentication.
  - Returns an `EventSourceResponse` that leverages the `sse_event_stream`
    async generator to send events.
- **Event Stream Generation (`sse_event_stream`):**
  - Manages the lifecycle of an individual SSE connection for a user.
  - Generates a unique `stream_id` for each connection.
  - Creates an `asyncio.Queue` to receive events for this specific stream.
  - Registers the stream with the global `sse_manager`.
  - Sends an initial `connected` event to the client upon successful connection.
  - Continuously listens for events on its queue and yields them to the client.
  - Sends periodic `ping` events to keep the connection alive if no data events occur.
  - Monitors client disconnections using `request.is_disconnected()`.
  - Cleans up by removing the stream from `sse_manager` in a `finally` block.
  - Includes error handling for issues during event processing or stream management.

Authentication Note:
  The `get_current_user_ws` dependency is responsible for validating the JWT token.
  If authentication fails, it should raise an `HTTPException`, preventing the stream
  from being established. The endpoint includes a fallback check for robustness.
"""

# app/sse_stream/router.py
import asyncio
import json
import logging
import uuid

from fastapi import APIRouter, Request
from sse_starlette.sse import EventSourceResponse

# Ensure User model is imported if current_user type hint needs it directly,
# though CurrentUserHttp already implies it.
# Use the new HTTP-specific authentication dependency
from app.sse_stream.deps import CurrentUserHttp  # Or from app.auth.deps if moved

from .sse_manager import sse_manager  # Import the global instance

logger = logging.getLogger(__name__)
router = APIRouter()


async def sse_event_stream(user_id: str, request: Request):
    """
    Asynchronously generates Server-Sent Events for a single client connection.

    This generator is responsible for the lifecycle of an SSE stream for a user.
    It registers with the `SSEManager`, sends an initial connection confirmation,
    then listens on a dedicated queue for events to forward to the client. It also
    sends periodic ping events to maintain connection liveness and handles client
    disconnections gracefully.

    Args:
        user_id (str): The unique identifier of the authenticated user for this stream.
        request (Request): The FastAPI Request object, used to check for client disconnections.

    Yields:
        dict: SSE-formatted events. Can be data events (`{"data": json.dumps(event)}`),
              a connection event (`{"event": "connected", ...}`),
              ping events (`{"event": "ping", "data": ""}`),
              or error events (`{"event": "error", ...}`).
    """
    stream_id = str(uuid.uuid4())
    queue = asyncio.Queue()
    await sse_manager.add_stream(user_id, stream_id, queue)

    # Send an initial acknowledgment/connection confirmation event
    initial_payload = {
        "message": "SSE connection established",
        "user_id": user_id,
        "stream_id": stream_id,
    }
    yield {"event": "connected", "data": json.dumps(initial_payload)}
    logger.info(f"SSE connection established for user {user_id}, stream {stream_id}")

    try:
        while True:
            if await request.is_disconnected():
                logger.info(
                    f"SSE client for user {user_id} (stream {stream_id}) disconnected by client."
                )
                break
            try:
                # Wait for an event from the queue with a timeout for keep-alive
                event = await asyncio.wait_for(queue.get(), timeout=15)
                yield {"data": json.dumps(event)}
                logger.debug(
                    f"Sent event to user {user_id} (stream {stream_id}): {str(event)[:100]}..."
                )
            except asyncio.TimeoutError:
                # Send a keep-alive ping (comment or custom event)
                yield {"event": "ping", "data": ""}  # SSE standard ping format
                logger.debug(f"Sent ping to user {user_id} (stream {stream_id})")
            except Exception as e:  # Catch other errors during queue.get or yielding
                logger.error(
                    f"Error during event processing for user {user_id} (stream {stream_id}): {e}",
                    exc_info=True,
                )
                # Optionally, yield an error event to the client
                yield {
                    "event": "error",
                    "data": json.dumps({"message": "Internal server error"}),
                }
                # Depending on the error, might need to break
                break
    except Exception as e:
        # Catch errors related to stream setup or unexpected issues
        logger.error(
            f"Unhandled error in SSE stream for user {user_id} (stream {stream_id}): {e}",
            exc_info=True,
        )
    finally:
        logger.info(f"Closing SSE stream {stream_id} for user {user_id}.")
        await sse_manager.remove_stream(user_id, stream_id)


@router.get(
    "/stream",
    summary="Subscribe to Server-Sent Events for real-time updates",
    tags=["SSE"],
)
async def sse_updates_endpoint(
    request: Request,
    # Token must be passed as query parameter: /stream?token=YOUR_JWT_TOKEN
    current_user: CurrentUserHttp,  # MODIFIED: Use new dependency. Implicitly Depends(get_current_user_http)
):
    # The get_current_user_http dependency will raise HTTPException if auth fails,
    # so current_user here will be a valid User object.
    # The explicit check `if not current_user ...` can be removed for cleaner code,
    # as the dependency handles the unauthenticated/invalid cases.

    user_id = str(current_user.id)
    return EventSourceResponse(sse_event_stream(user_id=user_id, request=request))
