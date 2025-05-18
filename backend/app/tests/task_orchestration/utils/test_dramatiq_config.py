from datetime import timedelta
from unittest.mock import ANY, MagicMock, patch

import dramatiq
import pytest
from dramatiq.middleware import AgeLimit, Retries, TimeLimit
from dramatiq_kafka import KafkaBroker as ActualKafkaBroker

# Assuming your setup_broker function and Settings class are in these locations
# Update: Import the module itself to allow direct modification of its globals for testing
import app.task_orchestration.utils.dramatiq_config as dramatiq_config_module
from app.core.config import Settings
from app.task_orchestration.utils.dramatiq_config import (
    setup_broker,  # _broker_initialized is now accessed via dramatiq_config_module
)


@pytest.fixture(autouse=True)
def reset_dramatiq_state():
    """Ensures a clean state for dramatiq before and after each test."""
    # Before test:
    # Close previous broker if any and it's closeable
    current_broker_before = dramatiq.get_broker()
    if current_broker_before is not None and hasattr(current_broker_before, "close"):
        try:
            current_broker_before.close()
        except Exception:
            pass  # Ignore errors during pre-test cleanup
    dramatiq.set_broker(None)
    dramatiq_config_module._broker_initialized = (
        False  # Directly reset the flag in the imported module
    )

    yield  # Run the test

    # After test:
    # Clean up broker created during the test
    current_broker_after = dramatiq.get_broker()
    if current_broker_after is not None and hasattr(current_broker_after, "close"):
        try:
            current_broker_after.close()
        except Exception:
            pass  # Ignore errors during post-test cleanup
    dramatiq.set_broker(None)
    dramatiq_config_module._broker_initialized = False  # Reset again for hygiene


@pytest.fixture
def mock_settings_values():
    """Provides a dictionary of settings values."""
    return {
        "KAFKA_BOOTSTRAP": "mock_kafka:9092",
        "DRAMATIQ_GROUP_ID": "test-tts-workers-mocked",
        # The following are not directly used by KafkaBroker constructor now, but keep for completeness if Settings needs them
        # 'DRAMATIQ_MAX_POLL_RECORDS': 10,
        # 'DRAMATIQ_AUTO_OFFSET_RESET': "earliest",
    }


@pytest.fixture
def mock_pydantic_settings(mock_settings_values):
    """Provides a mock pydantic Settings object."""
    settings_instance = MagicMock(spec=Settings)
    for key, value in mock_settings_values.items():
        setattr(settings_instance, key, value)
    return settings_instance


@patch("dramatiq_kafka.KafkaBroker", autospec=True)
def test_setup_broker_initialization(MockPatchedKafkaBroker, mock_pydantic_settings):
    """Test that setup_broker initializes and configures a KafkaBroker instance correctly."""
    mock_broker_instance = MockPatchedKafkaBroker.return_value

    with patch(
        "app.task_orchestration.utils.dramatiq_config.Settings",
        return_value=mock_pydantic_settings,
    ):
        broker_returned = setup_broker()

        MockPatchedKafkaBroker.assert_called_once_with(
            bootstrap_servers=mock_pydantic_settings.KAFKA_BOOTSTRAP,
            group_id=mock_pydantic_settings.DRAMATIQ_GROUP_ID,
            # Add other args here if KafkaBroker takes them and they come from settings
        )
        assert broker_returned is mock_broker_instance
        assert dramatiq.get_broker() is mock_broker_instance

        # Check middleware calls
        # We expect 3 middleware to be added
        assert mock_broker_instance.add_middleware.call_count == 3
        mock_broker_instance.add_middleware.assert_any_call(ANY)
        mock_broker_instance.add_middleware.assert_any_call(ANY)
        mock_broker_instance.add_middleware.assert_any_call(ANY)


@patch("dramatiq_kafka.KafkaBroker", autospec=True)
def test_setup_broker_idempotency(MockPatchedKafkaBroker, mock_pydantic_settings):
    """Test that calling setup_broker multiple times returns the same broker instance and doesn't re-initialize."""
    mock_broker_instance = MockPatchedKafkaBroker.return_value

    with patch(
        "app.task_orchestration.utils.dramatiq_config.Settings",
        return_value=mock_pydantic_settings,
    ) as mock_settings_class_factory:
        broker1 = setup_broker()

        # KafkaBroker should be instantiated once
        MockPatchedKafkaBroker.assert_called_once()
        # Settings class should be instantiated once
        mock_settings_class_factory.assert_called_once()

        # Call setup_broker again
        broker2 = setup_broker()

        assert broker1 is broker2
        assert broker1 is mock_broker_instance  # Ensure it's our mock

        # KafkaBroker should still only have been called once
        MockPatchedKafkaBroker.assert_called_once()
        # Settings class should still only have been called once
        mock_settings_class_factory.assert_called_once()
        assert dramatiq.get_broker() is broker1


@patch("dramatiq_kafka.KafkaBroker", autospec=True)
def test_setup_broker_middleware_details(
    MockPatchedKafkaBroker, mock_pydantic_settings
):
    """Test the details of middleware configuration."""
    mock_broker_instance = MockPatchedKafkaBroker.return_value

    with patch(
        "app.task_orchestration.utils.dramatiq_config.Settings",
        return_value=mock_pydantic_settings,
    ):
        setup_broker()

        calls = mock_broker_instance.add_middleware.call_args_list
        assert len(calls) == 3

        # Check for AgeLimit configuration
        age_limit_call = next(
            call for call in calls if isinstance(call[0][0], AgeLimit)
        )
        assert age_limit_call[0][0].max_age == timedelta(hours=24)

        # Check for TimeLimit configuration
        time_limit_call = next(
            call for call in calls if isinstance(call[0][0], TimeLimit)
        )
        expected_time_limit_ms = 60 * 60 * 1000  # 60 minutes
        assert time_limit_call[0][0].time_limit == expected_time_limit_ms

        # Check for Retries (just its presence, specific args are harder to check without more complex mocking)
        retry_call = next(call for call in calls if isinstance(call[0][0], Retries))
        assert retry_call is not None
        # If you need to check Retries(max_retries=3), you'd compare the instance directly
        # e.g. assert retry_call[0][0].max_retries == 3 (if Retries stores it like that)
        # For now, ANY in the earlier test and presence here is a good start.


# This test checks the idempotency logic if a broker is already set (even a different type)
@patch("dramatiq_kafka.KafkaBroker", autospec=True)
def test_setup_broker_when_another_broker_is_already_set(
    MockPatchedKafkaBroker,
):
    # Simulate an existing broker that IS an instance of the actual KafkaBroker type
    # The mock_existing_broker needs to be recognized by `isinstance(..., ActualKafkaBrokerForTypeCheck)`
    mock_existing_broker = MagicMock(spec=ActualKafkaBroker)
    # Crucially, make it behave like an instance of ActualKafkaBroker for isinstance checks
    # This is often done by setting __class__ or relying on spec, but MagicMock with spec handles it.
    # Forcing it this way for clarity in the test, though spec should be enough.
    mock_existing_broker.__class__ = ActualKafkaBroker
    dramatiq.set_broker(mock_existing_broker)

    # Explicitly set _broker_initialized to True to simulate prior setup_broker call
    with patch(
        "app.task_orchestration.utils.dramatiq_config._broker_initialized", True
    ):
        # We also need to ensure that when setup_broker checks dramatiq.get_broker(), it gets our mock_existing_broker
        with patch("dramatiq.get_broker", return_value=mock_existing_broker):
            returned_broker = setup_broker()
            assert returned_broker is mock_existing_broker
            MockPatchedKafkaBroker.assert_not_called()  # New KafkaBroker should not be created


@patch("dramatiq_kafka.KafkaBroker", autospec=True)
def test_setup_broker_reinitializes_if_flag_true_but_no_broker(
    MockPatchedKafkaBroker, mock_pydantic_settings
):
    """Test re-initialization if _broker_initialized is True but no global broker is set."""
    mock_broker_instance = MockPatchedKafkaBroker.return_value
    with patch(
        "app.task_orchestration.utils.dramatiq_config.Settings",
        return_value=mock_pydantic_settings,
    ):
        with patch(
            "app.task_orchestration.utils.dramatiq_config._broker_initialized", True
        ):
            dramatiq.set_broker(None)  # Explicitly no broker set
            returned_broker = setup_broker()
            MockPatchedKafkaBroker.assert_called_once()  # Should create a new one
            assert returned_broker is mock_broker_instance
            assert dramatiq.get_broker() is mock_broker_instance


@patch("dramatiq_kafka.KafkaBroker", autospec=True)
def test_setup_broker_reinitializes_if_flag_true_but_different_broker_type(
    MockPatchedKafkaBroker, mock_pydantic_settings
):
    """Test re-initialization if _broker_initialized is True but a different type of broker is set."""
    mock_broker_instance = MockPatchedKafkaBroker.return_value

    class OtherBrokerType(dramatiq.Broker):  # A genuinely different type
        def declare_queue(self, queue_name):
            pass

        def enqueue(self, message, delay=None):
            pass

        def get_declared_queues(self):
            return set()

        def flush(self, queue_name):
            pass

        def close(self):
            pass

    dramatiq.set_broker(OtherBrokerType())
    with patch(
        "app.task_orchestration.utils.dramatiq_config.Settings",
        return_value=mock_pydantic_settings,
    ):
        with patch(
            "app.task_orchestration.utils.dramatiq_config._broker_initialized", True
        ):
            returned_broker = setup_broker()
            MockPatchedKafkaBroker.assert_called_once()  # Should create a new KafkaBroker
            assert returned_broker is mock_broker_instance
            assert dramatiq.get_broker() is mock_broker_instance


@pytest.mark.integration
def test_setup_broker_connects_to_real_kafka(
    # reset_dramatiq_state, # This is an autouse fixture, no need to list as arg if not used in body
):  # Uses the updated fixture
    """
    Test that setup_broker initializes a KafkaBroker that attempts to connect to a
    real Kafka service as configured in Settings.
    This test expects a Kafka instance to be running and accessible (e.g., kafka:9092).
    """
    # We are NOT patching '...KafkaBroker' or '...Settings'.
    # setup_broker will use the actual Settings class which loads from the environment.
    # The app.core.config.Settings has a default KAFKA_BOOTSTRAP = "kafka:9092"

    try:
        broker_returned = setup_broker()

        assert broker_returned is not None, "setup_broker returned None"
        assert isinstance(
            broker_returned, ActualKafkaBroker
        ), f"Broker is not ActualKafkaBroker, but {type(broker_returned)}"
        assert (
            dramatiq.get_broker() is broker_returned
        ), "Global broker not set correctly"

        # Verify middleware configuration on the real broker instance
        assert (
            len(broker_returned.middleware) == 3
        ), f"Expected 3 middleware, got {len(broker_returned.middleware)}"

        middleware_types = [type(m) for m in broker_returned.middleware]
        assert AgeLimit in middleware_types, "AgeLimit middleware missing"
        assert TimeLimit in middleware_types, "TimeLimit middleware missing"
        assert Retries in middleware_types, "Retries middleware missing"

        age_limit_mw = next(
            m for m in broker_returned.middleware if isinstance(m, AgeLimit)
        )
        assert age_limit_mw.max_age == timedelta(hours=24), "AgeLimit max_age incorrect"

        time_limit_mw = next(
            m for m in broker_returned.middleware if isinstance(m, TimeLimit)
        )
        expected_time_limit_ms = 60 * 60 * 1000  # 60 minutes
        assert (
            time_limit_mw.time_limit == expected_time_limit_ms
        ), "TimeLimit time_limit incorrect"

        retries_mw = next(
            m for m in broker_returned.middleware if isinstance(m, Retries)
        )
        assert hasattr(
            retries_mw, "max_retries"
        ), "Retries middleware does not have max_retries attribute"
        assert retries_mw.max_retries == 3, "Retries max_retries incorrect"

    except Exception as e:
        # If Kafka is not available or configuration is wrong, this test will fail.
        # This is expected for an integration test.
        pytest.fail(f"Failed to setup broker with real Kafka connection: {e}")

    # Cleanup is handled by the autouse reset_dramatiq_state fixture's yield part
