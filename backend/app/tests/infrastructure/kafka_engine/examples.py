# -----------------------------------------------------------------------------
# Example of how to use the Kafka engine
# -----------------------------------------------------------------------------
# In your application, e.g., app/services/message_service.py
import pytest
from fastapi.testclient import TestClient
from faststream.kafka import TestKafkaBroker

from app.infra.kafka_engine.kafka import router as kafka_router
from app.main import app  # Assuming 'app' is your FastAPI instance

# Store last received message for test assertion
if not hasattr(app.state, "last_kafka_message"):
    app.state.last_kafka_message = None


async def publish_greeting(name: str):
    message = f"Hello, {name}!"
    await kafka_router.broker.publish(message, topic="greetings_topic")
    print(f"Published: {message}")


@kafka_router.subscriber("greetings_topic")
async def greeting_subscriber(message: str):
    app.state.last_kafka_message = message
    print(f"Subscriber received: {message}")


# Ensure your FastAPI app instance (e.g., in app/main.py) has the kafka_router included
# and that this subscriber is registered when the app starts up.
# For testing, the TestClient(app) in your fixtures will ensure the app is initialized.

# -----------------------------------------------------------------------------
# Example of how to test the Kafka engine
# -----------------------------------------------------------------------------

# In your tests, e.g., app/tests/services/test_message_service.py
# Assuming your FastAPI app instance is 'app' from 'app.main'
# and your service functions are in 'app.services.message_service'


@pytest.mark.asyncio
async def test_publish_and_subscribe_greeting(
    _client: TestClient,  # Ensures app is initialized, including Kafka router and subscribers
    _kafka_test_broker: TestKafkaBroker,  # Fixed unused parameter
):
    # The greeting_subscriber is already "listening" via the TestKafkaBroker
    # because it's decorated with @kafka_router.subscriber(...) and the
    # kafka_router.broker has been taken over by kafka_test_broker.

    test_name = "World"
    expected_message = f"Hello, {test_name}!"

    # 1. Call your application code that publishes a message
    await publish_greeting(test_name)

    # 2. Assert that your subscriber was called
    # TestKafkaBroker (via its TestBroker superclass) provides `sub_is_called`
    # You might need to ensure `greeting_subscriber` is correctly referenced.
    # If greeting_subscriber is decorated directly, it might be:
    # await kafka_test_broker.sub_is_called(greeting_subscriber, timeout=5)
    # Or, if it's part of a class or needs specific referencing:
    # For this example, we'll check the side effect.

    # 3. Check the side effect of the subscriber
    # Give a very brief moment for async processing if needed, though often not required with TestBroker
    # await asyncio.sleep(0.01) # Usually not necessary with FastStream's test utilities

    assert app.state.last_kafka_message == expected_message

    # Optional: You can also directly publish using TestKafkaBroker to test a subscriber
    # app.state.last_kafka_message = None # Reset state
    # await kafka_test_broker.publish("Another message", topic="greetings_topic")
    # await asyncio.sleep(0.01) # if needed
    # assert app.state.last_kafka_message == "Another message"

    # Optional: If your publisher 'publish_greeting' was an annotated producer
    # (e.g., @kafka_router.broker.producer("greetings_topic"))
    # you could assert it was called:
    # await kafka_test_broker.pub_is_called(publish_greeting.producer, timeout=5)
