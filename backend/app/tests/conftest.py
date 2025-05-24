from collections.abc import AsyncGenerator, Generator

import pytest
from fastapi.testclient import TestClient
from faststream.kafka import TestKafkaBroker
from sqlmodel import Session, delete

from app.core.config import settings
from app.core.db import engine, init_db
from app.main import app
from app.models import Item, User
from app.tests.utils.user import authentication_token_from_email
from app.tests.utils.utils import get_superuser_token_headers

# Store original states for unpatching
_original_aiokafka_admin_client_start = None
_original_kafka_bootstrap_setting = None
_kafka_bootstrap_was_present = False
_original_aiokafka_producer_start = None
_original_aiokafka_producer_send_and_wait = None


def pytest_configure(_config):
    print("pytest_configure: Applying patches and settings overrides...")
    global \
        _original_aiokafka_admin_client_start, \
        _original_kafka_bootstrap_setting, \
        _kafka_bootstrap_was_present
    global _original_aiokafka_producer_start, _original_aiokafka_producer_send_and_wait

    # Override Kafka bootstrap setting very early
    if hasattr(settings, "KAFKA_BOOTSTRAP"):
        _original_kafka_bootstrap_setting = settings.KAFKA_BOOTSTRAP
        _kafka_bootstrap_was_present = True
    else:
        _kafka_bootstrap_was_present = False
        _original_kafka_bootstrap_setting = None  # It didn't exist
    settings.KAFKA_BOOTSTRAP = "localhost:1"  # Force dummy value
    print(
        f"pytest_configure: settings.KAFKA_BOOTSTRAP set to {settings.KAFKA_BOOTSTRAP}"
    )

    # Patch AIOKafkaAdminClient.start
    from aiokafka.admin import AIOKafkaAdminClient
    from aiokafka.cluster import ClusterMetadata

    _original_aiokafka_admin_client_start = AIOKafkaAdminClient.start
    print(
        f"pytest_configure: Original AIOKafkaAdminClient.start: {_original_aiokafka_admin_client_start}"
    )

    async def mock_admin_start(self_admin_client):
        print(
            f"mock_admin_start: Called for {type(self_admin_client)}. Preventing real start."
        )
        self_admin_client._closed = False
        if hasattr(self_admin_client, "_client"):
            self_admin_client._client._closed = False
            if (
                not hasattr(self_admin_client._client, "cluster")
                or self_admin_client._client.cluster is None
            ):
                if not hasattr(self_admin_client._client, "config") or not isinstance(
                    self_admin_client._client.config, dict
                ):
                    self_admin_client._client.config = {"metadata_max_age_ms": 300000}
                self_admin_client._client.cluster = ClusterMetadata(
                    bootstrap_brokers_changed_callback=lambda: None,
                    error_cb=lambda e: None,
                    metadata_max_age_ms=self_admin_client._client.config.get(
                        "metadata_max_age_ms", 300000
                    ),
                    api_version=self_admin_client._client.api_version,
                )

    AIOKafkaAdminClient.start = mock_admin_start
    print(f"pytest_configure: Patched AIOKafkaAdminClient.start to: {mock_admin_start}")

    # --- Add AIOKafkaProducer patches ---
    from aiokafka import AIOKafkaProducer

    _original_aiokafka_producer_start = AIOKafkaProducer.start
    _original_aiokafka_producer_send_and_wait = AIOKafkaProducer.send_and_wait

    async def mock_producer_start(self_producer):
        print(
            f"mock_producer_start: CALLED for AIOKafkaProducer instance {id(self_producer)}. Preventing real start."
        )
        # Make it look like it started successfully without network ops
        self_producer._closed = False
        # Producers often have a _sender task or similar; ensure it's not problematic
        # For aiokafka, it has _sender, _sender_task, _metadata
        # We'll keep it simple: just mark as not closed.
        # If specific attributes are checked (e.g., metadata), they might need minimal mocking here.
        # For example, if self_producer._metadata.update() is called and _metadata is None:
        if not hasattr(self_producer, "_metadata") or self_producer._metadata is None:
            from aiokafka.cluster import ClusterMetadata

            # Create a basic ClusterMetadata object. Bootstrap servers list might be needed by it.
            # Producer's config usually has 'bootstrap_servers'.
            _bootstrap_servers = self_producer.config.get(
                "bootstrap_servers", "dummy:1234"
            )
            self_producer._metadata = ClusterMetadata(
                metadata_max_age_ms=self_producer.config.get(
                    "metadata_max_age_ms", 300000
                ),
                bootstrap_brokers_changed_callback=lambda: None,  # No-op callback
                api_version=self_producer.config.get("api_version", "auto"),
            )
            # Optionally, pre-populate with a dummy broker if code checks for brokers
            # from aiokafka.structs import BrokerMetadata
            # dummy_broker_node = -1
            # self_producer._metadata.update_metadata(
            #     {'brokers': {(dummy_broker_node, "localhost", 1, None): BrokerMetadata(dummy_broker_node, "localhost", 1, None)},
            #      'topics': {}, 'controller_id': dummy_broker_node, 'cluster_id': "mock_cluster_id"}
            # )

    async def mock_producer_send_and_wait(
        self_producer, topic, value, _key=None, _partition=None, _timestamp_ms=None
    ):
        print(
            f"mock_producer_send_and_wait: CALLED for AIOKafkaProducer {id(self_producer)}."
            f" Topic: {topic}, Value: {value}. Doing nothing."
        )
        from aiokafka.structs import RecordMetadata  # type: ignore

        # topic, partition, topic_partition, offset, timestamp, timestamp_type, checksum, serialized_key_size, serialized_value_size
        return RecordMetadata(topic, 0, (topic, 0), 0, -1, 0, None, 0, 0)  # type: ignore

    AIOKafkaProducer.start = mock_producer_start
    AIOKafkaProducer.send_and_wait = mock_producer_send_and_wait
    print("pytest_configure: Patched AIOKafkaProducer.start and .send_and_wait")
    # --- End AIOKafkaProducer patches ---

    print("pytest_configure: Patches and overrides applied.")


def pytest_unconfigure(_config):
    print("pytest_unconfigure: Restoring original settings and patches...")
    global \
        _original_aiokafka_admin_client_start, \
        _original_kafka_bootstrap_setting, \
        _kafka_bootstrap_was_present
    global _original_aiokafka_producer_start, _original_aiokafka_producer_send_and_wait

    if _original_aiokafka_admin_client_start:
        from aiokafka.admin import AIOKafkaAdminClient

        AIOKafkaAdminClient.start = _original_aiokafka_admin_client_start
        print("pytest_unconfigure: Restored AIOKafkaAdminClient.start")

    if _kafka_bootstrap_was_present:
        settings.KAFKA_BOOTSTRAP = _original_kafka_bootstrap_setting
        print(
            f"pytest_unconfigure: Restored settings.KAFKA_BOOTSTRAP to {_original_kafka_bootstrap_setting}"
        )
    elif hasattr(settings, "KAFKA_BOOTSTRAP"):
        # If we added it and it wasn't there before, remove it
        delattr(settings, "KAFKA_BOOTSTRAP")
        print(
            "pytest_unconfigure: Removed settings.KAFKA_BOOTSTRAP as it was test-specific"
        )

    # --- Restore AIOKafkaProducer patches ---
    if _original_aiokafka_producer_start:
        from aiokafka import AIOKafkaProducer  # Ensure it's imported for unpatching

        AIOKafkaProducer.start = _original_aiokafka_producer_start
        print("pytest_unconfigure: Restored AIOKafkaProducer.start")
    if _original_aiokafka_producer_send_and_wait:
        from aiokafka import AIOKafkaProducer  # Ensure it's imported for unpatching

        AIOKafkaProducer.send_and_wait = _original_aiokafka_producer_send_and_wait
        print("pytest_unconfigure: Restored AIOKafkaProducer.send_and_wait")
    # --- End AIOKafkaProducer patches ---

    print("pytest_unconfigure: Restoration complete.")


@pytest.fixture(scope="session", autouse=True)
def override_minio_for_tests() -> None:
    original_minio_url = settings.MINIO_URL_INTERNAL
    settings.MINIO_URL_INTERNAL = "http://localhost:9000"
    print(
        f"override_minio_for_tests: settings.MINIO_URL_INTERNAL set to {settings.MINIO_URL_INTERNAL}"
    )
    yield
    settings.MINIO_URL_INTERNAL = original_minio_url
    print(
        f"override_minio_for_tests: Restored settings.MINIO_URL_INTERNAL to {original_minio_url}"
    )


@pytest.fixture(scope="module")
async def kafka_test_broker(_pytestconfig) -> AsyncGenerator[TestKafkaBroker, None]:
    print("kafka_test_broker: Setting up TestKafkaBroker...")
    # Import app here to ensure settings are patched first by pytest_configure
    from app.infra.kafka_engine.kafka import router as kafka_router

    print(
        f"kafka_test_broker: Original broker bootstrap_servers from get_kafka_config: {kafka_router.broker._bootstrap_servers}"
    )
    async with TestKafkaBroker(kafka_router.broker) as tb:
        print("kafka_test_broker: TestKafkaBroker active.")
        yield tb
    print("kafka_test_broker: TestKafkaBroker cleaned up.")


@pytest.fixture(scope="module")
def client(_kafka_test_broker: TestKafkaBroker) -> Generator[TestClient, None, None]:
    print("client fixture: Initializing TestClient(app)...")
    with TestClient(app) as c:
        print("client fixture: TestClient active.")
        yield c
    print("client fixture: TestClient closed.")


@pytest.fixture(scope="session", autouse=True)
def db() -> Generator[Session, None, None]:
    with Session(engine) as session:
        init_db(session)
        yield session
        statement = delete(Item)
        session.exec(statement)
        statement = delete(User)
        session.exec(statement)
        session.commit()


@pytest.fixture(scope="module")
def superuser_token_headers(client: TestClient) -> dict[str, str]:
    return get_superuser_token_headers(client)


@pytest.fixture(scope="module")
def normal_user_token_headers(client: TestClient, db: Session) -> dict[str, str]:
    return authentication_token_from_email(
        client=client, email=settings.EMAIL_TEST_USER, db=db
    )
