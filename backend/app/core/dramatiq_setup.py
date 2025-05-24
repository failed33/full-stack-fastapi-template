import logging

import dramatiq
from dramatiq.brokers.redis import RedisBroker
from dramatiq.middleware import AgeLimit, Callbacks, Pipelines, Retries, TimeLimit

from app.core.config import settings

logger = logging.getLogger(__name__)


def setup_dramatiq_broker():
    """
    Configure and set up the Dramatiq broker with appropriate middleware.
    This should be called early in the application lifecycle.
    """
    # Set up Redis broker for Dramatiq
    # Note: For production with Kafka, you would use dramatiq-kafka here instead
    redis_broker = RedisBroker(
        host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_DB
    )

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

    logger.info("Dramatiq broker configured with Redis")
    return redis_broker


# Import all actors to ensure they are registered with Dramatiq
# This import must come after the broker is set up
def import_actors():
    """Import all Dramatiq actors to register them with the broker."""
    try:
        # Import all your actors here so they get registered
        import importlib.util

        if importlib.util.find_spec("app.tasks.pipeline_tasks"):
            import app.tasks.pipeline_tasks  # noqa: F401

        logger.info("Dramatiq actors imported and registered")
    except ImportError as e:
        logger.error(f"Failed to import Dramatiq actors: {e}")
        raise


# For convenience, also import the actors directly
def get_pipeline_actors():
    """Get references to the pipeline actors for external use."""
    from app.tasks.pipeline_tasks import (
        conversion_actor,
        final_processing_actor,
        segmentation_actor,
        transcription_actor,
    )

    return {
        "conversion": conversion_actor,
        "segmentation": segmentation_actor,
        "transcription": transcription_actor,
        "final_processing": final_processing_actor,
    }
