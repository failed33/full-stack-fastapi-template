import asyncio
import json

import aiokafka

from app.core.config import Settings
from app.task_orchestration.pipe.tasks import split_wav


async def main():
    settings = Settings()
    BOOTSTRAP = settings.KAFKA_BOOTSTRAP
    TOPIC = settings.MINIO_NOTIFY_KAFKA_TOPIC_PRIMARY
    print(f"Dispatcher starting. Kafka: {BOOTSTRAP}, Topic: {TOPIC}")
    consumer = aiokafka.AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8", errors="ignore")),
        group_id="minio-dispatcher",
        auto_offset_reset="earliest",  # Start from beginning if new group or offset is lost
    )
    await consumer.start()
    print("Dispatcher consumer started.")
    try:
        async for msg in consumer:
            print(f"Dispatcher received message: {msg.topic}, offset {msg.offset}")
            try:
                # Ensure Records is present and is a list
                if (
                    "Records" not in msg.value
                    or not isinstance(msg.value["Records"], list)
                    or not msg.value["Records"]
                ):
                    print(
                        f"Skipping message due to missing or invalid Records: {msg.value}"
                    )
                    continue

                rec = msg.value["Records"][0]
                # Validate expected S3 structure
                s3_data = rec.get("s3", {})
                s3_bucket = s3_data.get("bucket", {})
                s3_object = s3_data.get("object", {})

                if not (
                    isinstance(s3_data, dict)
                    and isinstance(s3_bucket, dict)
                    and isinstance(s3_object, dict)
                    and "name" in s3_bucket
                    and "key" in s3_object
                    and "eTag" in s3_object
                ):
                    print(f"Skipping message due to incomplete S3 data: {rec}")
                    continue

                bucket = s3_bucket["name"]
                key = s3_object["key"]
                etag = s3_object["eTag"]
                print(
                    f"Dispatching task for: bucket='{bucket}', key='{key}', etag='{etag}'"
                )
                # fire off the first stage
                split_wav.send(bucket, key, etag=etag)
                print(f"Sent task for {key} to split_wav actor.")
            except Exception as e:
                print(f"Error processing message value {msg.value}: {e}")
                # Potentially log the problematic message or send to a dead-letter queue

    except Exception as e:
        print(f"Dispatcher main loop error: {e}")
    finally:
        print("Dispatcher stopping consumer.")
        await consumer.stop()
        print("Dispatcher consumer stopped.")


if __name__ == "__main__":
    print("Starting MinIO event dispatcher...")
    asyncio.run(main())
