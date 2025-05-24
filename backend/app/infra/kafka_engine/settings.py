from app.core.config import settings


def get_kafka_config():
    return {
        "bootstrap_servers": settings.KAFKA_BOOTSTRAP,
        # "group_id": settings.DRAMATIQ_GROUP_ID,
        # "max_poll_records": settings.KAFKA_MAX_POLL_RECORDS,
        # "auto_offset_reset": settings.KAFKA_AUTO_OFFSET_RESET,
    }
