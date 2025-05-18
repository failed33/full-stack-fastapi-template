"""
Dramatiq broker configuration.
Provides a function to set up the Kafka broker and middleware.
"""
from datetime import timedelta

import dramatiq
from dramatiq.middleware import (
    AgeLimit,  # kill very old messages
    Retries,  # automatic retry with back-off
    TimeLimit,  # per-task wall-clock limit
)
from dramatiq_kafka import KafkaBroker

# Prometheus     # /metrics endpoint for Grafana-Prometheus stack
from app.core.config import Settings

# To prevent setting up multiple times if this function is called more than once,
# though dramatiq.set_broker itself is generally safe to call multiple times.
_broker_initialized = False


def setup_broker() -> KafkaBroker:
    """
    Initializes and configures the Dramatiq Kafka broker and sets it globally.
    Returns the configured broker instance.
    This function is designed to be idempotent.
    """
    global _broker_initialized
    # Check if already initialized and if a broker is already globally set
    current_global_broker = dramatiq.get_broker()
    if (
        _broker_initialized
        and current_global_broker is not None
        and isinstance(current_global_broker, KafkaBroker)
    ):
        return current_global_broker

    current_settings = Settings()
    # Renamed variable for clarity within this function's scope
    task_time_limit_ms: int = 60 * 60 * 1000  # 60 minutes in milliseconds

    broker = KafkaBroker(
        bootstrap_servers=current_settings.KAFKA_BOOTSTRAP,
        group_id=current_settings.DRAMATIQ_GROUP_ID,
        # max_poll_records and auto_offset_reset are not directly supported
        # by this KafkaBroker constructor based on documentation and behavior.
        # The underlying kafka-python client will use its defaults.
    )

    # Middleware setup
    broker.add_middleware(AgeLimit(max_age=timedelta(hours=24)))
    broker.add_middleware(TimeLimit(time_limit=task_time_limit_ms))
    broker.add_middleware(Retries(max_retries=3))
    # broker.add_middleware(Prometheus(namespace="tts"))

    dramatiq.set_broker(broker)
    _broker_initialized = True
    return broker
