"""
Manages Server-Sent Events (SSE) connections and facilitates real-time event
distribution to connected clients.

This module introduces the `SSEManager` class, a singleton responsible for orchestrating
SSE streams within the application. It enables multiple clients (e.g., different
browser tabs or devices for the same user) to establish and maintain individual SSE
connections. Each connection is uniquely identified by a stream ID and utilizes an
`asyncio.Queue` to buffer events, ensuring non-blocking operations.

Core Responsibilities:
- Tracking and managing all active SSE streams, organized by user ID and stream ID.
- Providing mechanisms to add new SSE streams when clients connect.
- Offering methods to remove SSE streams upon client disconnection.
- Broadcasting events to all active SSE streams associated with a particular user.
- Leveraging `asyncio.Queue` for efficient, asynchronous event queuing and delivery.
- Incorporating detailed logging for connection management and event propagation
  to aid in monitoring and debugging.

Integration:
A globally accessible instance, `sse_manager`, is instantiated from this module.
Other parts of the application can interact with this instance to:
  - Register new client SSE connections (e.g., upon a user opening a new tab).
  - Deregister client SSE connections (e.g., when a user closes a tab).
  - Dispatch application-specific events or notifications to users in real-time.

Example Workflow:
1. A client establishes an SSE connection with the server.
2. The server-side endpoint handler calls `sse_manager.add_stream()`,
   registering the new stream with a unique user ID and stream ID, along with
   an `asyncio.Queue` for this stream.
3. When an application event relevant to the user occurs (e.g., a background task
   completes), a component calls `sse_manager.push_event_to_user()` with the
   user ID and event payload.
4. The `SSEManager` iterates through all active streams for that user and puts
   the event onto each stream's queue.
5. The server-side endpoint handler for each stream dequeues events and sends them
   to the respective connected client.
6. Upon client disconnection, the endpoint handler calls `sse_manager.remove_stream()`
   to clean up the resources.
"""

# app/sse_stream/sse_manager.py
import asyncio
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)


class SSEManager:
    """
    Manages Server-Sent Event (SSE) streams and distributes events to clients.

    This class provides a centralized mechanism for handling multiple SSE connections
    per user. It uses a nested dictionary structure (`active_streams`) to store
    `asyncio.Queue` instances associated with each user's active streams.

    Attributes:
        active_streams (defaultdict[str, dict[str, asyncio.Queue]]):
            A dictionary where keys are user IDs (str). Each user ID maps to another
            dictionary where keys are unique stream IDs (str) and values are
            `asyncio.Queue` objects. This allows a single user to maintain multiple
            concurrent SSE connections (e.g., from different browser tabs or devices).
            The `defaultdict(dict)` ensures that accessing a new `user_id`
            automatically initializes an empty dictionary for their streams.
        logger (logging.Logger): A logger instance specific to this class, named
            `app.sse_stream.sse_manager.SSEManager`, for detailed logging of
            SSE management activities.
    """

    def __init__(self):
        """Initializes the SSEManager with an empty collection of active streams.
        Sets up a specific logger for the instance.
        """
        # user_id -> {stream_id: asyncio.Queue}
        self.active_streams: defaultdict[str, dict[str, asyncio.Queue]] = defaultdict(
            dict
        )
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    async def add_stream(self, user_id: str, stream_id: str, queue: asyncio.Queue):
        """
        Registers a new SSE stream for a given user.

        This method associates an `asyncio.Queue` with a specific stream ID for a user,
        allowing events to be pushed to this stream.

        Args:
            user_id (str): The unique identifier for the user establishing the stream.
            stream_id (str): A unique identifier for this particular SSE stream (e.g., UUID).
            queue (asyncio.Queue): The queue instance that will be used to send events
                                   to this specific SSE stream.
        """
        self.active_streams[user_id][stream_id] = queue
        self.logger.info(
            f"SSE stream {stream_id} added for user {user_id}. Total streams for user: {len(self.active_streams[user_id])}"
        )

    async def remove_stream(self, user_id: str, stream_id: str):
        """
        Deregisters an SSE stream for a given user.

        If, after removing the stream, the user has no other active streams, the user's
        entire entry is removed from the `active_streams` collection to conserve resources.
        Logs a warning if an attempt is made to remove a non-existent stream.

        Args:
            user_id (str): The unique identifier for the user whose stream is to be removed.
            stream_id (str): The unique identifier of the SSE stream to be removed.
        """
        if user_id in self.active_streams and stream_id in self.active_streams[user_id]:
            del self.active_streams[user_id][stream_id]
            self.logger.info(f"SSE stream {stream_id} removed for user {user_id}.")
            if not self.active_streams[
                user_id
            ]:  # Check if the user's stream dictionary is now empty
                del self.active_streams[user_id]
                self.logger.info(
                    f"User {user_id} has no more active SSE streams. Removed user entry."
                )
        else:
            self.logger.warning(
                f"Attempted to remove non-existent SSE stream {stream_id} for user {user_id}."
            )

    async def push_event_to_user(self, user_id: str, event: dict):
        """
        Pushes an event to all active SSE streams associated with a specific user.

        The method iterates over all registered streams for the given `user_id` and
        asynchronously puts the `event` dictionary into each stream's `asyncio.Queue`.
        Handles cases where a user might have no active streams or if pushing to a
        queue fails.

        Args:
            user_id (str): The unique identifier of the user to whom the event should be sent.
            event (dict): The event payload to be sent. This should be a dictionary
                          that can be serialized (e.g., to JSON) by the SSE endpoint.
        """
        if user_id in self.active_streams:
            # Create a copy of the stream items for safe iteration,
            # as queue operations are async and the underlying dict might change
            # in highly concurrent scenarios (though less likely with GIL and typical usage).
            user_streams = list(self.active_streams[user_id].items())

            if not user_streams:
                self.logger.debug(
                    f"No active SSE streams for user {user_id} when trying to push event."
                )
                return

            self.logger.debug(
                f"Pushing event to {len(user_streams)} streams for user {user_id}: {event.get('event_type', 'N/A')}"
            )
            for stream_id, queue in user_streams:
                try:
                    await queue.put(event)
                    self.logger.debug(
                        f"Event successfully pushed to stream {stream_id} for user {user_id}."
                    )
                except asyncio.QueueFull:
                    self.logger.error(
                        f"SSE queue full for stream {stream_id}, user {user_id}. Event dropped: {event.get('event_type', 'N/A')}",
                        exc_info=True,
                    )
                except Exception as e:
                    self.logger.error(
                        f"Failed to push event to stream {stream_id} for user {user_id}: {e}",
                        exc_info=True,
                    )
        else:
            self.logger.debug(
                f"No active SSE streams entry for user {user_id} to push event: {event.get('event_type', 'N/A')}"
            )


# Global instance, making the SSEManager a singleton for practical purposes within the application.
sse_manager = SSEManager()
