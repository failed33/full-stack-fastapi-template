import logging

import dramatiq
from dramatiq.brokers.redis import RedisBroker
from dramatiq.middleware import AgeLimit, Callbacks, Pipelines, Retries, TimeLimit

from app.core.config import settings

logger = logging.getLogger(__name__)


def setup_dramatiq_worker_broker():
    """
    Configure and set up the Dramatiq broker with Redis for worker processes.
    This is specifically for worker containers that process tasks.
    """
    # Get Redis configuration from environment variables
    redis_host = settings.REDIS_HOST
    redis_port = settings.REDIS_PORT
    redis_db = settings.REDIS_DB

    logger.info(
        f"Setting up Redis broker for workers: {redis_host}:{redis_port}/{redis_db}"
    )

    # Set up Redis broker for Dramatiq workers
    redis_broker = RedisBroker(host=redis_host, port=redis_port, db=redis_db)

    # Configure middleware
    redis_broker.add_middleware(
        AgeLimit(max_age=24 * 60 * 60 * 1000)
    )  # 24 hours max age
    redis_broker.add_middleware(TimeLimit())  # Time limits for actors
    redis_broker.add_middleware(Callbacks())  # Support for callbacks
    redis_broker.add_middleware(Pipelines())  # Support for pipelines
    redis_broker.add_middleware(
        Retries(max_retries=3)
    )  # Retry failed tasks up to 3 times

    # Set as the global broker
    dramatiq.set_broker(redis_broker)

    logger.info(
        f"Dramatiq worker broker configured with Redis at {redis_host}:{redis_port}"
    )
    return redis_broker


def import_worker_actors():
    """Import all Dramatiq actors for worker processes."""
    try:
        # Import pipeline tasks to register actors with the broker
        import importlib.util

        if importlib.util.find_spec("app.tasks.pipeline_tasks"):
            import app.tasks.pipeline_tasks  # noqa: F401

        logger.info("Dramatiq worker actors imported and registered")
    except ImportError as e:
        logger.error(f"Failed to import Dramatiq worker actors: {e}")
        raise
