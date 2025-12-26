"""Unit tests for RabbitMQ health checks."""

import sys
from pathlib import Path
import unittest
import time
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime

# Add parent directory to path to allow imports
# test_rabbitmq_health.py is at: db_connections/tests/unit/connectors/rabbitmq/
# We need to go up 6 levels to get to the parent of db_connections
try:
    _file_path = Path(__file__).resolve()
except NameError:
    # __file__ might not be defined in some contexts
    import os

    _file_path = (
        Path(os.getcwd())
        / "tests"
        / "unit"
        / "connectors"
        / "rabbitmq"
        / "test_rabbitmq_health.py"
    )  # noqa: E501

parent_dir = _file_path.parent.parent.parent.parent.parent.parent
parent_dir_str = str(parent_dir)
if parent_dir_str not in sys.path:
    sys.path.insert(0, parent_dir_str)

from db_connections.scr.all_db_connectors.connectors.rabbitmq.config import (  # noqa: E402, E501
    RabbitMQPoolConfig,
)
from db_connections.scr.all_db_connectors.connectors.rabbitmq.health import (  # noqa: E402
    RabbitMQHealthChecker,
)
from db_connections.scr.all_db_connectors.core.health import (  # noqa: E402
    HealthState,
    HealthStatus,
)


class MockRabbitMQConnection:
    """Mock RabbitMQ connection for testing."""

    def __init__(self, healthy=True):
        self.closed = False
        self.healthy = healthy
        self.is_closed = False
        self.is_open = True
        self.server_properties = {
            "product": "RabbitMQ",
            "version": "3.12.0",
            "platform": "Erlang/OTP",
        }

    def is_closing(self):
        """Mock is_closing."""
        return self.closed

    def close(self):
        """Mock close."""
        self.closed = True
        self.is_closed = True
        self.is_open = False

    def channel(self):
        """Mock channel creation."""
        if not self.healthy:
            raise Exception("Connection lost")
        return MockRabbitMQChannel()


class MockRabbitMQChannel:
    """Mock RabbitMQ channel for testing."""

    def __init__(self):
        self.closed = False
        self.is_closed = False
        self.is_open = True

    def queue_declare(self, queue, **kwargs):
        """Mock queue_declare."""
        # Return a mock object with a method attribute that has message_count and consumer_count
        method_mock = Mock(message_count=0, consumer_count=0)
        result_mock = Mock(method=method_mock)
        return result_mock

    def exchange_declare(self, exchange, **kwargs):
        """Mock exchange_declare."""
        pass

    def queue_bind(self, queue, exchange, routing_key, **kwargs):
        """Mock queue_bind."""
        pass

    def basic_publish(self, exchange, routing_key, body, **kwargs):
        """Mock basic_publish."""
        pass

    def close(self):
        """Mock close."""
        self.closed = True


class MockAsyncRabbitMQConnection:
    """Mock Async RabbitMQ connection for testing."""

    def __init__(self, healthy=True):
        self.closed = False
        self.healthy = healthy
        self.is_closed = False
        self.is_open = True
        self.server_properties = {"product": "RabbitMQ", "version": "3.12.0"}

    def is_closing(self):
        """Mock is_closing."""
        return self.closed

    async def close(self):
        """Mock close."""
        self.closed = True
        self.is_closed = True
        self.is_open = False

    async def channel(self):
        """Mock channel creation."""
        if not self.healthy:
            raise Exception("Connection lost")
        return MockAsyncRabbitMQChannel()


class MockAsyncRabbitMQChannel:
    """Mock Async RabbitMQ channel for testing."""

    def __init__(self):
        self.closed = False
        self.is_closed = False
        self.is_open = True

    async def queue_declare(self, queue, **kwargs):
        """Mock queue_declare."""
        return Mock(queue="test_queue", message_count=0, consumer_count=0)

    async def close(self):
        """Mock close."""
        self.closed = True


class TestRabbitMQHealthCheckerInit(unittest.TestCase):
    """Test RabbitMQHealthChecker initialization."""

    def setUp(self):
        """Set up test fixtures."""
        self.rabbitmq_config = RabbitMQPoolConfig(
            host="localhost",
            port=5672,
        )

    def test_init(self):
        """Test health checker initialization."""
        checker = RabbitMQHealthChecker(self.rabbitmq_config)

        self.assertEqual(checker.config, self.rabbitmq_config)
        self.assertIsNone(checker._last_check_time)
        self.assertIsNone(checker._last_status)


class TestRabbitMQHealthCheckerConnection(unittest.TestCase):
    """Test connection health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.rabbitmq_config = RabbitMQPoolConfig(
            host="localhost",
            port=5672,
        )
        self.health_checker = RabbitMQHealthChecker(self.rabbitmq_config)
        self.mock_rabbitmq_connection = MockRabbitMQConnection(healthy=True)

    def test_check_health_healthy(self):
        """Test health check on healthy connection."""
        result = self.health_checker.check_health(self.mock_rabbitmq_connection)

        self.assertIsInstance(result, HealthStatus)
        self.assertEqual(result.state, HealthState.HEALTHY)
        self.assertIsNotNone(result.response_time_ms)
        self.assertGreaterEqual(result.response_time_ms, 0)

    def test_check_health_unhealthy(self):
        """Test health check on unhealthy connection."""
        bad_conn = MockRabbitMQConnection(healthy=False)

        result = self.health_checker.check_health(bad_conn)

        self.assertIsInstance(result, HealthStatus)
        self.assertEqual(result.state, HealthState.UNHEALTHY)
        self.assertIn("failed", result.message.lower())
        self.assertIsNotNone(result.details)
        self.assertIn("error", result.details)

    def test_check_health_with_closed_connection(self):
        """Test health check with closed connection."""
        conn = MockRabbitMQConnection()
        conn.close()

        result = self.health_checker.check_health(conn)

        # Should be unhealthy due to closed connection
        self.assertEqual(result.state, HealthState.UNHEALTHY)

    def test_check_health_with_slow_response(self):
        """Test health check with slow response."""
        conn = MockRabbitMQConnection()

        # Simulate slow response by patching time
        with patch("time.time", side_effect=[0, 1.5]):  # 1.5 second delay
            result = self.health_checker.check_health(conn)

        # Should be degraded or unhealthy due to slow response
        self.assertIn(result.state, [HealthState.DEGRADED, HealthState.UNHEALTHY])


class TestRabbitMQHealthCheckerQueue(unittest.TestCase):
    """Test queue health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.rabbitmq_config = RabbitMQPoolConfig(
            host="localhost",
            port=5672,
        )
        self.health_checker = RabbitMQHealthChecker(self.rabbitmq_config)
        self.mock_rabbitmq_connection = MockRabbitMQConnection()

    def test_check_queue_status(self):
        """Test queue status check."""
        conn = MockRabbitMQConnection()
        channel = conn.channel()

        status = self.health_checker.check_queue_status(channel, "test_queue")

        self.assertIsInstance(status, dict)
        self.assertIn("queue", status)
        self.assertIn("message_count", status)
        self.assertIn("consumer_count", status)

    def test_check_queue_status_error(self):
        """Test queue status check with error."""
        channel = Mock()
        channel.queue_declare.side_effect = Exception("Error")

        status = self.health_checker.check_queue_status(channel, "test_queue")

        # Should return None on error
        self.assertIsNone(status)


class TestRabbitMQHealthCheckerExchange(unittest.TestCase):
    """Test exchange health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.rabbitmq_config = RabbitMQPoolConfig(
            host="localhost",
            port=5672,
        )
        self.health_checker = RabbitMQHealthChecker(self.rabbitmq_config)
        self.mock_rabbitmq_connection = MockRabbitMQConnection()

    def test_check_exchange_status(self):
        """Test exchange status check."""
        conn = MockRabbitMQConnection()
        channel = conn.channel()

        status = self.health_checker.check_exchange_status(channel, "test_exchange")

        self.assertIsInstance(status, dict)

    def test_check_exchange_status_error(self):
        """Test exchange status check with error."""
        channel = Mock()
        channel.exchange_declare.side_effect = Exception("Error")

        status = self.health_checker.check_exchange_status(channel, "test_exchange")

        # Should return None on error
        self.assertIsNone(status)


class TestRabbitMQHealthCheckerServerInfo(unittest.TestCase):
    """Test server info checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.rabbitmq_config = RabbitMQPoolConfig(
            host="localhost",
            port=5672,
        )
        self.health_checker = RabbitMQHealthChecker(self.rabbitmq_config)
        self.mock_rabbitmq_connection = MockRabbitMQConnection()

    def test_check_server_info(self):
        """Test server info check."""
        conn = MockRabbitMQConnection()

        info = self.health_checker.check_server_info(conn)

        self.assertIsInstance(info, dict)
        self.assertIn("product", info)
        self.assertIn("version", info)

    def test_check_server_info_error(self):
        """Test server info check with error."""
        conn = Mock()
        conn.server_properties = None

        info = self.health_checker.check_server_info(conn)

        # Should return None on error
        self.assertIsNone(info)


class TestAsyncRabbitMQHealthChecker(unittest.IsolatedAsyncioTestCase):
    """Test async health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.rabbitmq_config = RabbitMQPoolConfig(
            host="localhost",
            port=5672,
        )
        self.health_checker = RabbitMQHealthChecker(self.rabbitmq_config)
        self.mock_async_rabbitmq_connection = MockAsyncRabbitMQConnection(healthy=True)

    async def test_async_check_health_healthy(self):
        """Test async health check on healthy connection."""
        result = await self.health_checker.async_check_health(
            self.mock_async_rabbitmq_connection
        )

        self.assertIsInstance(result, HealthStatus)
        self.assertEqual(result.state, HealthState.HEALTHY)
        self.assertIsNotNone(result.response_time_ms)

    async def test_async_check_health_unhealthy(self):
        """Test async health check on unhealthy connection."""
        bad_conn = MockAsyncRabbitMQConnection(healthy=False)

        result = await self.health_checker.async_check_health(bad_conn)

        self.assertEqual(result.state, HealthState.UNHEALTHY)
        self.assertIn("failed", result.message.lower())


class TestHealthStatusProperties(unittest.TestCase):
    """Test HealthStatus properties."""

    def test_health_status_properties(self):
        """Test HealthStatus properties."""
        status = HealthStatus(
            state=HealthState.HEALTHY,
            message="All good",
            checked_at=datetime.now(),
            response_time_ms=10.5,
        )

        self.assertTrue(status.is_healthy)
        self.assertFalse(status.is_unhealthy)
        self.assertFalse(status.is_degraded)

    def test_health_status_degraded_properties(self):
        """Test degraded HealthStatus properties."""
        status = HealthStatus(
            state=HealthState.DEGRADED,
            message="Slow",
            checked_at=datetime.now(),
        )

        self.assertFalse(status.is_healthy)
        self.assertFalse(status.is_unhealthy)
        self.assertTrue(status.is_degraded)

    def test_health_status_unhealthy_properties(self):
        """Test unhealthy HealthStatus properties."""
        status = HealthStatus(
            state=HealthState.UNHEALTHY,
            message="Failed",
            checked_at=datetime.now(),
        )

        self.assertFalse(status.is_healthy)
        self.assertTrue(status.is_unhealthy)
        self.assertFalse(status.is_degraded)


class TestHealthCheckerResponseTime(unittest.TestCase):
    """Test response time tracking."""

    def setUp(self):
        """Set up test fixtures."""
        self.rabbitmq_config = RabbitMQPoolConfig(
            host="localhost",
            port=5672,
        )
        self.health_checker = RabbitMQHealthChecker(self.rabbitmq_config)
        self.mock_rabbitmq_connection = MockRabbitMQConnection()

    def test_response_time_recorded(self):
        """Test that response time is recorded."""
        result = self.health_checker.check_health(self.mock_rabbitmq_connection)

        self.assertIsNotNone(result.response_time_ms)
        self.assertGreater(result.response_time_ms, 0)

    def test_response_time_on_error(self):
        """Test response time is recorded even on error."""
        bad_conn = MockRabbitMQConnection(healthy=False)

        result = self.health_checker.check_health(bad_conn)

        self.assertIsNotNone(result.response_time_ms)
        self.assertGreaterEqual(result.response_time_ms, 0)


class TestRabbitMQHealthCheckerComprehensive(unittest.TestCase):
    """Test comprehensive health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.rabbitmq_config = RabbitMQPoolConfig(
            host="localhost",
            port=5672,
        )
        self.health_checker = RabbitMQHealthChecker(self.rabbitmq_config)
        self.mock_rabbitmq_connection = MockRabbitMQConnection()

    def test_comprehensive_check(self):
        """Test comprehensive health check."""
        result = self.health_checker.comprehensive_check(self.mock_rabbitmq_connection)

        self.assertIsInstance(result, dict)
        self.assertIn("connection", result)
        self.assertIn("server", result)
        self.assertIn("timestamp", result)

        self.assertIsInstance(result["connection"], HealthStatus)
        self.assertIsInstance(result["timestamp"], datetime)


if __name__ == "__main__":
    unittest.main()
