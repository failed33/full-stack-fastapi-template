from faststream.kafka.fastapi import KafkaRouter

from app.infra.kafka_engine.settings import get_kafka_config

router = KafkaRouter(**get_kafka_config())
