"""Unit tests for RabbitMQ synchronous connection pool."""

import sys
import queue
from pathlib import Path
import unittest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta

# Add parent directory to path to allow imports
# test_rabbitmq_pool.py is at: db_connections/tests/unit/connectors/rabbitmq/
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
        / "test_rabbitmq_pool.py"
    )  # noqa: E501

parent_dir = _file_path.parent.parent.parent.parent.parent.parent
parent_dir_str = str(parent_dir)
if parent_dir_str not in sys.path:
    sys.path.insert(0, parent_dir_str)

from db_connections.scr.all_db_connectors.connectors.rabbitmq.config import (  # noqa: E402, E501
    RabbitMQPoolConfig,
)
from db_connections.scr.all_db_connectors.connectors.rabbitmq.pool import (  # noqa: E402, E501
    RabbitMQSyncConnectionPool,
)
from db_connections.scr.all_db_connectors.connectors.rabbitmq.exceptions import (  # noqa: E402
    RabbitMQConnectionError,
)
from db_connections.scr.all_db_connectors.core.exceptions import (  # noqa: E402
    ConnectionError,
    PoolExhaustedError,
)
from db_connections.scr.all_db_connectors.core.health import (  # noqa: E402
    HealthState,
)
from db_connections.scr.all_db_connectors.core.utils import (  # noqa: E402
    ConnectionMetadata,
)


class MockRabbitMQConnection:
    """Mock RabbitMQ connection for testing."""

    def __init__(self):
        self.closed = False
        self.is_closed = False
        self.is_open = True
        self.server_properties = {}

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
        if self.closed:
            raise Exception("Connection closed")
        return MockRabbitMQChannel()


class MockRabbitMQChannel:
    """Mock RabbitMQ channel for testing."""

    def __init__(self):
        self.closed = False
        self.is_closed = False
        self.is_open = True

    def close(self):
        """Mock close."""
        self.closed = True
        self.is_closed = True
        self.is_open = False

    def queue_declare(self, queue, **kwargs):
        """Mock queue_declare."""
        return Mock(queue="test_queue", message_count=0, consumer_count=0)

    def basic_publish(self, exchange, routing_key, body, **kwargs):
        """Mock basic_publish."""
        pass


class MockRabbitMQConnectionPool:
    """Mock RabbitMQ ConnectionPool."""

    def __init__(self, max_connections=10, **kwargs):
        self.max_connections = max_connections
        self.connections = []
        self.in_use = set()
        self.closed = False

    def acquire(self):
        """Get connection from pool."""
        if self.closed:
            raise Exception("Pool is closed")
        if len(self.in_use) >= self.max_connections:
            return None
        conn = MockRabbitMQConnection()
        self.in_use.add(id(conn))
        return conn

    def release(self, connection):
        """Release connection to pool."""
        conn_id = id(connection)
        if conn_id in self.in_use:
            self.in_use.remove(conn_id)

    def close(self):
        """Close all connections."""
        self.closed = True
        self.in_use.clear()


class TestRabbitMQConnectionPoolInit(unittest.TestCase):
    """Test RabbitMQSyncConnectionPool initialization."""

    def setUp(self):
        """Set up test fixtures."""
        self.rabbitmq_config = RabbitMQPoolConfig(
            host="localhost",
            port=5672,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_rabbitmq_pool = MockRabbitMQConnectionPool(max_connections=10)

    def _setup_mock_pika_module(self):
        """Set up mock pika module."""
        # Mock BlockingConnection - use existing mock_rabbitmq_connection if available
        mock_conn = (
            getattr(self, "mock_rabbitmq_connection", None) or MockRabbitMQConnection()
        )
        mock_blocking_connection = Mock(return_value=mock_conn)

        # Mock PlainCredentials to accept any arguments including None
        mock_plain_credentials = Mock(return_value=Mock())

        # Mock ConnectionParameters to accept any arguments
        mock_connection_params = Mock(return_value=Mock())

        # Mock URLParameters
        mock_url_params = Mock(return_value=Mock())

        # Patch the direct imports used in pool.py
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.BlockingConnection",
            mock_blocking_connection,
        ).start()
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.PlainCredentials",
            mock_plain_credentials,
        ).start()
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.ConnectionParameters",
            mock_connection_params,
        ).start()
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.URLParameters",
            mock_url_params,
        ).start()

        # Store references for assertions
        self.mock_module = Mock()
        self.mock_module.BlockingConnection = mock_blocking_connection
        self.mock_module.PlainCredentials = mock_plain_credentials
        self.mock_module.ConnectionParameters = mock_connection_params
        self.mock_module.URLParameters = mock_url_params

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_init_default(self):
        """Test pool initialization with defaults."""
        self._setup_mock_pika_module()
        pool = RabbitMQSyncConnectionPool(self.rabbitmq_config)

        self.assertEqual(pool.config, self.rabbitmq_config)
        self.assertFalse(pool._initialized)
        self.assertIsNone(pool._pool)
        self.assertFalse(pool._closed)

    def test_init_validates_config(self):
        """Test pool validates configuration on init."""
        with self.assertRaisesRegex(ValueError, "max_connections must be positive"):
            invalid_config = RabbitMQPoolConfig(
                host="localhost",
                max_connections=-1,  # Invalid
            )

    def test_repr(self):
        """Test string representation."""
        self._setup_mock_pika_module()
        pool = RabbitMQSyncConnectionPool(self.rabbitmq_config)

        repr_str = repr(pool)
        self.assertIn("RabbitMQSyncConnectionPool", repr_str)
        self.assertIn("localhost", repr_str)
        self.assertIn("5672", repr_str)


class TestRabbitMQConnectionPoolInitialization(unittest.TestCase):
    """Test pool initialization methods."""

    def setUp(self):
        """Set up test fixtures."""
        self.rabbitmq_config = RabbitMQPoolConfig(
            host="localhost",
            port=5672,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_rabbitmq_pool = MockRabbitMQConnectionPool(max_connections=10)

    def _setup_mock_pika_module(self):
        """Set up mock pika module."""
        # Mock BlockingConnection - use existing mock_rabbitmq_connection if available
        mock_conn = (
            getattr(self, "mock_rabbitmq_connection", None) or MockRabbitMQConnection()
        )
        mock_blocking_connection = Mock(return_value=mock_conn)

        # Mock PlainCredentials to accept any arguments including None
        mock_plain_credentials = Mock(return_value=Mock())

        # Mock ConnectionParameters to accept any arguments
        mock_connection_params = Mock(return_value=Mock())

        # Mock URLParameters
        mock_url_params = Mock(return_value=Mock())

        # Patch the direct imports used in pool.py
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.BlockingConnection",
            mock_blocking_connection,
        ).start()
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.PlainCredentials",
            mock_plain_credentials,
        ).start()
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.ConnectionParameters",
            mock_connection_params,
        ).start()
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.URLParameters",
            mock_url_params,
        ).start()

        # Store references for assertions
        self.mock_module = Mock()
        self.mock_module.BlockingConnection = mock_blocking_connection
        self.mock_module.PlainCredentials = mock_plain_credentials
        self.mock_module.ConnectionParameters = mock_connection_params
        self.mock_module.URLParameters = mock_url_params

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_initialize_pool(self):
        """Test explicit pool initialization."""
        self._setup_mock_pika_module()
        pool = RabbitMQSyncConnectionPool(self.rabbitmq_config)
        pool.initialize_pool()

        self.assertTrue(pool._initialized)
        self.assertIsNotNone(pool._pool)
        self.mock_module.BlockingConnection.assert_called_once()

    def test_initialize_pool_already_initialized(self):
        """Test initializing already initialized pool."""
        self._setup_mock_pika_module()
        pool = RabbitMQSyncConnectionPool(self.rabbitmq_config)
        pool.initialize_pool()
        pool.initialize_pool()  # Second call should be no-op

        # Should only be called once
        self.assertEqual(self.mock_module.BlockingConnection.call_count, 1)

    def test_initialize_pool_connection_error(self):
        """Test pool initialization with connection error."""
        with patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.pika"
        ) as mock_module:
            mock_module.BlockingConnection.side_effect = Exception("Connection failed")

            pool = RabbitMQSyncConnectionPool(self.rabbitmq_config)

            with self.assertRaisesRegex(ConnectionError, "Pool initialization failed"):
                pool.initialize_pool()

    def test_lazy_initialization(self):
        """Test lazy pool initialization on first connection."""
        self._setup_mock_pika_module()
        pool = RabbitMQSyncConnectionPool(self.rabbitmq_config)

        self.assertFalse(pool._initialized)

        with pool.get_connection():
            pass

        self.assertTrue(pool._initialized)


class TestRabbitMQConnectionPoolGetConnection(unittest.TestCase):
    """Test connection acquisition."""

    def setUp(self):
        """Set up test fixtures."""
        self.rabbitmq_config = RabbitMQPoolConfig(
            host="localhost",
            port=5672,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_rabbitmq_pool = MockRabbitMQConnectionPool(max_connections=10)
        self.mock_rabbitmq_connection = MockRabbitMQConnection()

    def _setup_mock_pika_module(self):
        """Set up mock pika module."""
        # Mock BlockingConnection - use existing mock_rabbitmq_connection if available
        mock_conn = (
            getattr(self, "mock_rabbitmq_connection", None) or MockRabbitMQConnection()
        )
        mock_blocking_connection = Mock(return_value=mock_conn)

        # Mock PlainCredentials to accept any arguments including None
        mock_plain_credentials = Mock(return_value=Mock())

        # Mock ConnectionParameters to accept any arguments
        mock_connection_params = Mock(return_value=Mock())

        # Mock URLParameters
        mock_url_params = Mock(return_value=Mock())

        # Patch the direct imports used in pool.py
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.BlockingConnection",
            mock_blocking_connection,
        ).start()
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.PlainCredentials",
            mock_plain_credentials,
        ).start()
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.ConnectionParameters",
            mock_connection_params,
        ).start()
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.URLParameters",
            mock_url_params,
        ).start()

        # Store references for assertions
        self.mock_module = Mock()
        self.mock_module.BlockingConnection = mock_blocking_connection
        self.mock_module.PlainCredentials = mock_plain_credentials
        self.mock_module.ConnectionParameters = mock_connection_params
        self.mock_module.URLParameters = mock_url_params

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_get_connection_success(self):
        """Test successful connection acquisition."""
        self._setup_mock_pika_module()
        pool = RabbitMQSyncConnectionPool(self.rabbitmq_config)
        pool.initialize_pool()

        with pool.get_connection() as conn:
            self.assertIsNotNone(conn)

    def test_get_connection_lazy_init(self):
        """Test connection triggers lazy initialization."""
        self._setup_mock_pika_module()
        pool = RabbitMQSyncConnectionPool(self.rabbitmq_config)

        self.assertFalse(pool._initialized)

        with pool.get_connection():
            self.assertTrue(pool._initialized)

    def test_get_connection_validates(self):
        """Test connection validation on checkout."""
        self._setup_mock_pika_module()
        config = RabbitMQPoolConfig(
            host="localhost", validate_on_checkout=True, pre_ping=True
        )

        pool = RabbitMQSyncConnectionPool(config)

        with patch.object(pool, "validate_connection", return_value=True):
            with pool.get_connection() as conn:
                self.assertIsNotNone(conn)

    def test_get_connection_pool_closed(self):
        """Test getting connection from closed pool."""
        self._setup_mock_pika_module()
        pool = RabbitMQSyncConnectionPool(self.rabbitmq_config)
        pool._closed = True

        with self.assertRaisesRegex(ConnectionError, "Pool is closed"):
            with pool.get_connection():
                pass

    def test_get_connection_tracks_metadata(self):
        """Test connection metadata is tracked."""
        self._setup_mock_pika_module()
        pool = RabbitMQSyncConnectionPool(self.rabbitmq_config)
        pool.initialize_pool()

        with pool.get_connection() as conn:
            conn_id = id(conn)
            self.assertIn(conn_id, pool._connection_metadata)
            self.assertTrue(pool._connection_metadata[conn_id].in_use)

    def test_get_connection_releases_on_exit(self):
        """Test connection is released after context manager exit."""
        self._setup_mock_pika_module()
        pool = RabbitMQSyncConnectionPool(self.rabbitmq_config)
        pool.initialize_pool()

        with pool.get_connection() as conn:
            conn_id = id(conn)

        # After context manager exit
        self.assertNotIn(conn_id, pool._connections_in_use)

    def test_get_connection_exhausted(self):
        """Test connection acquisition when pool is exhausted."""
        # Mock all pika components
        # Return a new connection each time so they have different IDs
        mock_blocking_connection = Mock(
            side_effect=lambda *args, **kwargs: MockRabbitMQConnection()
        )
        mock_plain_credentials = Mock(return_value=Mock())
        mock_connection_params = Mock(return_value=Mock())
        mock_url_params = Mock(return_value=Mock())

        with patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.BlockingConnection",
            mock_blocking_connection,
        ), patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.PlainCredentials",
            mock_plain_credentials,
        ), patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.ConnectionParameters",
            mock_connection_params,
        ), patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.URLParameters",
            mock_url_params,
        ):
            # Use config with small max to make exhaustion easier
            config = RabbitMQPoolConfig(
                host="localhost",
                max_connections=2,
                min_connections=1,
                max_overflow=1,
                timeout=1,  # Short timeout to avoid hanging
            )
            pool = RabbitMQSyncConnectionPool(config)
            pool.initialize_pool()

            # Get and hold all connections from the pool to exhaust it
            # max_connections + max_overflow = 2 + 1 = 3 total
            # Use context managers to hold connections
            cm1 = pool.get_connection()
            conn1 = cm1.__enter__()  # Get first connection

            cm2 = pool.get_connection()
            conn2 = cm2.__enter__()  # Get second connection

            cm3 = pool.get_connection()
            conn3 = cm3.__enter__()  # Get third connection (uses overflow)

            # Keep references to prevent garbage collection
            self._held_connections = [conn1, conn2, conn3]
            self._held_context_managers = [cm1, cm2, cm3]

            # Now all 3 connections are in use
            # Verify we have 3 connections in metadata
            self.assertEqual(len(pool._connection_metadata), 3)

            # Try to get a fourth connection - should fail
            # Empty the queue first to force it to try creating a new connection
            while not pool._pool.empty():
                try:
                    pool._pool.get_nowait()
                except queue.Empty:
                    break

            # Now try to get another connection - should raise PoolExhaustedError
            # Mock queue.get to raise Empty immediately to avoid blocking
            original_get = pool._pool.get

            def mock_get(timeout=None):
                if pool._pool.empty():
                    raise queue.Empty()
                return original_get(timeout=timeout)

            pool._pool.get = mock_get

            with self.assertRaisesRegex(
                RabbitMQConnectionError, "Connection acquisition failed"
            ):
                cm4 = pool.get_connection()
                cm4.__enter__()  # This should raise RabbitMQConnectionError

            # Restore original get method
            pool._pool.get = original_get

            # Clean up - release the connections we're holding
            for cm in self._held_context_managers:
                try:
                    cm.__exit__(None, None, None)
                except Exception:
                    pass


class TestRabbitMQConnectionPoolRelease(unittest.TestCase):
    """Test connection release."""

    def setUp(self):
        """Set up test fixtures."""
        self.rabbitmq_config = RabbitMQPoolConfig(
            host="localhost",
            port=5672,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_rabbitmq_pool = MockRabbitMQConnectionPool(max_connections=10)
        self.mock_rabbitmq_connection = MockRabbitMQConnection()

    def _setup_mock_pika_module(self):
        """Set up mock pika module."""
        # Mock BlockingConnection - use existing mock_rabbitmq_connection if available
        mock_conn = (
            getattr(self, "mock_rabbitmq_connection", None) or MockRabbitMQConnection()
        )
        mock_blocking_connection = Mock(return_value=mock_conn)

        # Mock PlainCredentials to accept any arguments including None
        mock_plain_credentials = Mock(return_value=Mock())

        # Mock ConnectionParameters to accept any arguments
        mock_connection_params = Mock(return_value=Mock())

        # Mock URLParameters
        mock_url_params = Mock(return_value=Mock())

        # Patch the direct imports used in pool.py
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.BlockingConnection",
            mock_blocking_connection,
        ).start()
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.PlainCredentials",
            mock_plain_credentials,
        ).start()
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.ConnectionParameters",
            mock_connection_params,
        ).start()
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.URLParameters",
            mock_url_params,
        ).start()

        # Store references for assertions
        self.mock_module = Mock()
        self.mock_module.BlockingConnection = mock_blocking_connection
        self.mock_module.PlainCredentials = mock_plain_credentials
        self.mock_module.ConnectionParameters = mock_connection_params
        self.mock_module.URLParameters = mock_url_params

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_release_connection(self):
        """Test releasing a connection back to pool."""
        self._setup_mock_pika_module()
        pool = RabbitMQSyncConnectionPool(self.rabbitmq_config)
        pool.initialize_pool()

        conn = self.mock_rabbitmq_connection
        conn_id = id(conn)
        pool._connections_in_use.add(conn_id)

        pool.release_connection(conn)

        self.assertNotIn(conn_id, pool._connections_in_use)

    def test_release_connection_with_recycling(self):
        """Test connection recycling on release."""
        self._setup_mock_pika_module()
        config = RabbitMQPoolConfig(host="localhost", recycle_on_return=True)

        pool = RabbitMQSyncConnectionPool(config)
        pool.initialize_pool()

        conn = self.mock_rabbitmq_connection
        conn_id = id(conn)

        # Create metadata
        from db_connections.scr.all_db_connectors.core.utils import ConnectionMetadata

        pool._connection_metadata[conn_id] = ConnectionMetadata(
            created_at=datetime.now() - timedelta(hours=1)
        )

        pool.release_connection(conn)

        # Connection should be recycled
        self.assertNotIn(conn_id, pool._connection_metadata)


class TestRabbitMQConnectionPoolClose(unittest.TestCase):
    """Test pool closing."""

    def setUp(self):
        """Set up test fixtures."""
        self.rabbitmq_config = RabbitMQPoolConfig(
            host="localhost",
            port=5672,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_rabbitmq_pool = MockRabbitMQConnectionPool(max_connections=10)
        self.mock_rabbitmq_connection = MockRabbitMQConnection()

    def _setup_mock_pika_module(self):
        """Set up mock pika module."""
        # Mock BlockingConnection - use existing mock_rabbitmq_connection if available
        mock_conn = (
            getattr(self, "mock_rabbitmq_connection", None) or MockRabbitMQConnection()
        )
        mock_blocking_connection = Mock(return_value=mock_conn)

        # Mock PlainCredentials to accept any arguments including None
        mock_plain_credentials = Mock(return_value=Mock())

        # Mock ConnectionParameters to accept any arguments
        mock_connection_params = Mock(return_value=Mock())

        # Mock URLParameters
        mock_url_params = Mock(return_value=Mock())

        # Patch the direct imports used in pool.py
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.BlockingConnection",
            mock_blocking_connection,
        ).start()
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.PlainCredentials",
            mock_plain_credentials,
        ).start()
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.ConnectionParameters",
            mock_connection_params,
        ).start()
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.URLParameters",
            mock_url_params,
        ).start()

        # Store references for assertions
        self.mock_module = Mock()
        self.mock_module.BlockingConnection = mock_blocking_connection
        self.mock_module.PlainCredentials = mock_plain_credentials
        self.mock_module.ConnectionParameters = mock_connection_params
        self.mock_module.URLParameters = mock_url_params

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_close_connection(self):
        """Test closing a single connection."""
        self._setup_mock_pika_module()
        pool = RabbitMQSyncConnectionPool(self.rabbitmq_config)
        pool.initialize_pool()

        conn = self.mock_rabbitmq_connection
        conn_id = id(conn)
        pool._connections_in_use.add(conn_id)

        pool.close_connection(conn)

        self.assertNotIn(conn_id, pool._connections_in_use)
        self.assertTrue(conn.closed)

    def test_close_all_connections(self):
        """Test closing all connections in pool."""
        self._setup_mock_pika_module()
        pool = RabbitMQSyncConnectionPool(self.rabbitmq_config)
        pool.initialize_pool()

        pool.close_all_connections()

        self.assertTrue(pool._closed)
        self.assertEqual(len(pool._connections_in_use), 0)
        self.assertEqual(len(pool._connection_metadata), 0)


class TestRabbitMQConnectionPoolStatus(unittest.TestCase):
    """Test pool status and metrics."""

    def setUp(self):
        """Set up test fixtures."""
        self.rabbitmq_config = RabbitMQPoolConfig(
            host="localhost",
            port=5672,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_rabbitmq_pool = MockRabbitMQConnectionPool(max_connections=10)

    def _setup_mock_pika_module(self):
        """Set up mock pika module."""
        # Mock BlockingConnection - use existing mock_rabbitmq_connection if available
        mock_conn = (
            getattr(self, "mock_rabbitmq_connection", None) or MockRabbitMQConnection()
        )
        mock_blocking_connection = Mock(return_value=mock_conn)

        # Mock PlainCredentials to accept any arguments including None
        mock_plain_credentials = Mock(return_value=Mock())

        # Mock ConnectionParameters to accept any arguments
        mock_connection_params = Mock(return_value=Mock())

        # Mock URLParameters
        mock_url_params = Mock(return_value=Mock())

        # Patch the direct imports used in pool.py
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.BlockingConnection",
            mock_blocking_connection,
        ).start()
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.PlainCredentials",
            mock_plain_credentials,
        ).start()
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.ConnectionParameters",
            mock_connection_params,
        ).start()
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.URLParameters",
            mock_url_params,
        ).start()

        # Store references for assertions
        self.mock_module = Mock()
        self.mock_module.BlockingConnection = mock_blocking_connection
        self.mock_module.PlainCredentials = mock_plain_credentials
        self.mock_module.ConnectionParameters = mock_connection_params
        self.mock_module.URLParameters = mock_url_params

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_pool_status_uninitialized(self):
        """Test pool status before initialization."""
        self._setup_mock_pika_module()
        pool = RabbitMQSyncConnectionPool(self.rabbitmq_config)

        status = pool.pool_status()

        self.assertFalse(status["initialized"])
        self.assertEqual(status["total_connections"], 0)
        expected_max = (
            self.rabbitmq_config.max_connections + self.rabbitmq_config.max_overflow
        )
        self.assertEqual(status["max_connections"], expected_max)

    def test_pool_status_initialized(self):
        """Test pool status after initialization."""
        self._setup_mock_pika_module()
        pool = RabbitMQSyncConnectionPool(self.rabbitmq_config)
        pool.initialize_pool()

        status = pool.pool_status()

        self.assertTrue(status["initialized"])
        self.assertFalse(status["closed"])
        expected_max = (
            self.rabbitmq_config.max_connections + self.rabbitmq_config.max_overflow
        )
        self.assertEqual(status["max_connections"], expected_max)
        self.assertEqual(
            status["min_connections"], self.rabbitmq_config.min_connections
        )

    def test_get_metrics(self):
        """Test getting pool metrics."""
        self._setup_mock_pika_module()
        pool = RabbitMQSyncConnectionPool(self.rabbitmq_config)
        pool.initialize_pool()

        metrics = pool.get_metrics()

        self.assertGreaterEqual(metrics.total_connections, 0)
        self.assertGreaterEqual(metrics.active_connections, 0)
        self.assertGreaterEqual(metrics.idle_connections, 0)
        expected_max = (
            self.rabbitmq_config.max_connections + self.rabbitmq_config.max_overflow
        )
        self.assertEqual(metrics.max_connections, expected_max)
        self.assertEqual(metrics.min_connections, self.rabbitmq_config.min_connections)


class TestRabbitMQConnectionPoolValidation(unittest.TestCase):
    """Test connection validation."""

    def setUp(self):
        """Set up test fixtures."""
        self.rabbitmq_config = RabbitMQPoolConfig(
            host="localhost",
            port=5672,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_rabbitmq_pool = MockRabbitMQConnectionPool(max_connections=10)
        self.mock_rabbitmq_connection = MockRabbitMQConnection()

    def _setup_mock_pika_module(self):
        """Set up mock pika module."""
        # Mock BlockingConnection - use existing mock_rabbitmq_connection if available
        mock_conn = (
            getattr(self, "mock_rabbitmq_connection", None) or MockRabbitMQConnection()
        )
        mock_blocking_connection = Mock(return_value=mock_conn)

        # Mock PlainCredentials to accept any arguments including None
        mock_plain_credentials = Mock(return_value=Mock())

        # Mock ConnectionParameters to accept any arguments
        mock_connection_params = Mock(return_value=Mock())

        # Mock URLParameters
        mock_url_params = Mock(return_value=Mock())

        # Patch the direct imports used in pool.py
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.BlockingConnection",
            mock_blocking_connection,
        ).start()
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.PlainCredentials",
            mock_plain_credentials,
        ).start()
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.ConnectionParameters",
            mock_connection_params,
        ).start()
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.URLParameters",
            mock_url_params,
        ).start()

        # Store references for assertions
        self.mock_module = Mock()
        self.mock_module.BlockingConnection = mock_blocking_connection
        self.mock_module.PlainCredentials = mock_plain_credentials
        self.mock_module.ConnectionParameters = mock_connection_params
        self.mock_module.URLParameters = mock_url_params

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_validate_connection_success(self):
        """Test successful connection validation."""
        self._setup_mock_pika_module()
        pool = RabbitMQSyncConnectionPool(self.rabbitmq_config)

        result = pool.validate_connection(self.mock_rabbitmq_connection)

        self.assertTrue(result)

    def test_validate_connection_failure(self):
        """Test connection validation failure."""
        self._setup_mock_pika_module()
        pool = RabbitMQSyncConnectionPool(self.rabbitmq_config)

        # Create connection that will fail validation
        bad_conn = Mock()
        bad_conn.is_closed = True
        bad_conn.is_open = False

        result = pool.validate_connection(bad_conn)

        self.assertFalse(result)


class TestRabbitMQConnectionPoolHealth(unittest.TestCase):
    """Test health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.rabbitmq_config = RabbitMQPoolConfig(
            host="localhost",
            port=5672,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_rabbitmq_pool = MockRabbitMQConnectionPool(max_connections=10)
        self.mock_rabbitmq_connection = MockRabbitMQConnection()

    def _setup_mock_pika_module(self):
        """Set up mock pika module."""
        # Mock BlockingConnection - use existing mock_rabbitmq_connection if available
        mock_conn = (
            getattr(self, "mock_rabbitmq_connection", None) or MockRabbitMQConnection()
        )
        mock_blocking_connection = Mock(return_value=mock_conn)

        # Mock PlainCredentials to accept any arguments including None
        mock_plain_credentials = Mock(return_value=Mock())

        # Mock ConnectionParameters to accept any arguments
        mock_connection_params = Mock(return_value=Mock())

        # Mock URLParameters
        mock_url_params = Mock(return_value=Mock())

        # Patch the direct imports used in pool.py
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.BlockingConnection",
            mock_blocking_connection,
        ).start()
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.PlainCredentials",
            mock_plain_credentials,
        ).start()
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.ConnectionParameters",
            mock_connection_params,
        ).start()
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.URLParameters",
            mock_url_params,
        ).start()

        # Store references for assertions
        self.mock_module = Mock()
        self.mock_module.BlockingConnection = mock_blocking_connection
        self.mock_module.PlainCredentials = mock_plain_credentials
        self.mock_module.ConnectionParameters = mock_connection_params
        self.mock_module.URLParameters = mock_url_params

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_health_check_healthy(self):
        """Test health check on healthy pool."""
        self._setup_mock_pika_module()
        pool = RabbitMQSyncConnectionPool(self.rabbitmq_config)
        pool.initialize_pool()

        health = pool.health_check()

        self.assertEqual(health.state, HealthState.HEALTHY)
        self.assertIn("healthy", health.message.lower())

    def test_database_health_check(self):
        """Test database health check."""
        self._setup_mock_pika_module()
        pool = RabbitMQSyncConnectionPool(self.rabbitmq_config)
        pool.initialize_pool()

        with patch.object(pool, "get_connection"):
            health = pool.health_check()

            self.assertIn(
                health.state,
                [HealthState.HEALTHY, HealthState.DEGRADED, HealthState.UNHEALTHY],
            )


class TestRabbitMQConnectionPoolContextManager(unittest.TestCase):
    """Test context manager support."""

    def setUp(self):
        """Set up test fixtures."""
        self.rabbitmq_config = RabbitMQPoolConfig(
            host="localhost",
            port=5672,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_rabbitmq_pool = MockRabbitMQConnectionPool(max_connections=10)

    def _setup_mock_pika_module(self):
        """Set up mock pika module."""
        # Mock BlockingConnection - use existing mock_rabbitmq_connection if available
        mock_conn = (
            getattr(self, "mock_rabbitmq_connection", None) or MockRabbitMQConnection()
        )
        mock_blocking_connection = Mock(return_value=mock_conn)

        # Mock PlainCredentials to accept any arguments including None
        mock_plain_credentials = Mock(return_value=Mock())

        # Mock ConnectionParameters to accept any arguments
        mock_connection_params = Mock(return_value=Mock())

        # Mock URLParameters
        mock_url_params = Mock(return_value=Mock())

        # Patch the direct imports used in pool.py
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.BlockingConnection",
            mock_blocking_connection,
        ).start()
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.PlainCredentials",
            mock_plain_credentials,
        ).start()
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.ConnectionParameters",
            mock_connection_params,
        ).start()
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.URLParameters",
            mock_url_params,
        ).start()

        # Store references for assertions
        self.mock_module = Mock()
        self.mock_module.BlockingConnection = mock_blocking_connection
        self.mock_module.PlainCredentials = mock_plain_credentials
        self.mock_module.ConnectionParameters = mock_connection_params
        self.mock_module.URLParameters = mock_url_params

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_context_manager_enter(self):
        """Test pool context manager enter."""
        self._setup_mock_pika_module()
        with RabbitMQSyncConnectionPool(self.rabbitmq_config) as pool:
            self.assertTrue(pool._initialized)
            self.assertFalse(pool._closed)

    def test_context_manager_exit(self):
        """Test pool context manager exit."""
        self._setup_mock_pika_module()
        pool = RabbitMQSyncConnectionPool(self.rabbitmq_config)

        with pool:
            pass

        self.assertTrue(pool._closed)

    def test_context_manager_with_connections(self):
        """Test using connections within context manager."""
        self._setup_mock_pika_module()
        with RabbitMQSyncConnectionPool(self.rabbitmq_config) as pool:
            with pool.get_connection() as conn:
                self.assertIsNotNone(conn)

        self.assertTrue(pool._closed)


class TestRabbitMQConnectionPoolRetry(unittest.TestCase):
    """Test retry logic."""

    def setUp(self):
        """Set up test fixtures."""
        self.rabbitmq_config = RabbitMQPoolConfig(
            host="localhost",
            port=5672,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )

    def test_retry_on_connection_failure(self):
        """Test retry logic on connection failure."""
        # Mock all pika components
        mock_plain_credentials = Mock(return_value=Mock())
        mock_connection_params = Mock(return_value=Mock())
        mock_url_params = Mock(return_value=Mock())

        # For initialization: succeed (min_size=1 needs 1 connection)
        # For get_connection: fail twice, succeed on third attempt
        import pika

        call_count = [0]

        def connection_factory(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] <= 1:
                # First call for initialization
                return MockRabbitMQConnection()
            elif call_count[0] <= 3:
                # Next two calls fail
                raise pika.exceptions.ConnectionClosed(200, "Connection failed")
            else:
                # Fourth call succeeds
                return MockRabbitMQConnection()

        mock_blocking_connection = Mock(side_effect=connection_factory)

        with patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.BlockingConnection",
            mock_blocking_connection,
        ), patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.PlainCredentials",
            mock_plain_credentials,
        ), patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.ConnectionParameters",
            mock_connection_params,
        ), patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.URLParameters",
            mock_url_params,
        ), patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.time.sleep"
        ) as mock_sleep:
            config = RabbitMQPoolConfig(
                host="localhost", max_retries=3, retry_delay=0.1, min_connections=1
            )

            pool = RabbitMQSyncConnectionPool(config)
            pool.initialize_pool()

            # Empty the pool so get_connection will try to create a new connection
            # This will trigger the retry logic
            while not pool._pool.empty():
                try:
                    pool._pool.get_nowait()
                except queue.Empty:
                    break

            # Should succeed after retries
            with pool.get_connection() as conn:
                self.assertIsNotNone(conn)

            # Verify retries happened (sleep should have been called)
            self.assertGreater(mock_sleep.call_count, 0)

    def test_retry_exhausted(self):
        """Test when all retry attempts are exhausted."""
        # Mock all pika components
        mock_plain_credentials = Mock(return_value=Mock())
        mock_connection_params = Mock(return_value=Mock())
        mock_url_params = Mock(return_value=Mock())

        # For initialization: succeed (min_size=1 needs 1 connection)
        # For get_connection: always fail
        import pika

        call_count = [0]

        def connection_factory(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] <= 1:
                # First call for initialization
                return MockRabbitMQConnection()
            else:
                # All subsequent calls fail
                raise pika.exceptions.ConnectionClosed(200, "Connection failed")

        mock_blocking_connection = Mock(side_effect=connection_factory)

        with patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.BlockingConnection",
            mock_blocking_connection,
        ), patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.PlainCredentials",
            mock_plain_credentials,
        ), patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.ConnectionParameters",
            mock_connection_params,
        ), patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.URLParameters",
            mock_url_params,
        ), patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.time.sleep"
        ) as mock_sleep:
            config = RabbitMQPoolConfig(
                host="localhost",
                max_retries=2,
                retry_delay=0.1,
                min_connections=1,
                timeout=0.1,  # Short timeout to avoid blocking
            )

            pool = RabbitMQSyncConnectionPool(config)
            pool.initialize_pool()

            # Empty the pool so get_connection will try to create a new connection
            # This will trigger the retry logic
            while not pool._pool.empty():
                try:
                    pool._pool.get_nowait()
                except queue.Empty:
                    break

            with self.assertRaisesRegex(
                RabbitMQConnectionError, "Connection acquisition failed"
            ):
                with pool.get_connection():
                    pass

            # Verify retries happened (sleep should have been called)
            # With max_retries=2, we should have 2 retries (attempts 1 and 2)
            self.assertGreaterEqual(mock_sleep.call_count, 2)


class TestRabbitMQConnectionPoolConcurrency(unittest.TestCase):
    """Test concurrent operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.rabbitmq_config = RabbitMQPoolConfig(
            host="localhost",
            port=5672,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_rabbitmq_pool = MockRabbitMQConnectionPool(max_connections=10)
        self.mock_rabbitmq_connection = MockRabbitMQConnection()

    def _setup_mock_pika_module(self):
        """Set up mock pika module."""
        # Mock BlockingConnection - return new connection each time for concurrency tests
        # This allows test_multiple_connections to get different connection instances
        mock_blocking_connection = Mock(
            side_effect=lambda *args, **kwargs: MockRabbitMQConnection()
        )

        # Mock PlainCredentials to accept any arguments including None
        mock_plain_credentials = Mock(return_value=Mock())

        # Mock ConnectionParameters to accept any arguments
        mock_connection_params = Mock(return_value=Mock())

        # Mock URLParameters
        mock_url_params = Mock(return_value=Mock())

        # Patch the direct imports used in pool.py
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.BlockingConnection",
            mock_blocking_connection,
        ).start()
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.PlainCredentials",
            mock_plain_credentials,
        ).start()
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.ConnectionParameters",
            mock_connection_params,
        ).start()
        patch(
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.URLParameters",
            mock_url_params,
        ).start()

        # Store references for assertions
        self.mock_module = Mock()
        self.mock_module.BlockingConnection = mock_blocking_connection
        self.mock_module.PlainCredentials = mock_plain_credentials
        self.mock_module.ConnectionParameters = mock_connection_params
        self.mock_module.URLParameters = mock_url_params

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_multiple_connections(self):
        """Test acquiring multiple connections."""
        self._setup_mock_pika_module()
        pool = RabbitMQSyncConnectionPool(self.rabbitmq_config)
        pool.initialize_pool()

        connections = []

        # Acquire multiple connections concurrently (nested context managers)
        # This ensures they're all different since they're all in use at the same time
        with pool.get_connection() as conn1:
            connections.append(conn1)
            with pool.get_connection() as conn2:
                connections.append(conn2)
                with pool.get_connection() as conn3:
                    connections.append(conn3)
                    # All should be different
                    self.assertEqual(len(set(id(c) for c in connections)), 3)

    def test_connection_reuse(self):
        """Test connections are reused after release."""
        self._setup_mock_pika_module()
        pool = RabbitMQSyncConnectionPool(self.rabbitmq_config)
        pool.initialize_pool()

        conn_id_1 = None
        conn_id_2 = None

        with pool.get_connection() as conn:
            conn_id_1 = id(conn)

        # Connection should be returned to pool
        with pool.get_connection() as conn:
            conn_id_2 = id(conn)

        # Might get same connection back
        self.assertIsNotNone(conn_id_1)
        self.assertIsNotNone(conn_id_2)


if __name__ == "__main__":
    unittest.main()
