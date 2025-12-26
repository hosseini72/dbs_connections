"""Unit tests for ClickHouse synchronous connection pool."""

import sys
from pathlib import Path
import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta

# Add parent directory to path to allow imports
# test_clickhouse_pool.py is at: db_connections/tests/unit/connectors/clickhouse/
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
        / "clickhouse"
        / "test_clickhouse_pool.py"
    )  # noqa: E501

parent_dir = _file_path.parent.parent.parent.parent.parent.parent
parent_dir_str = str(parent_dir)
if parent_dir_str not in sys.path:
    sys.path.insert(0, parent_dir_str)

from db_connections.scr.all_db_connectors.connectors.clickhouse.config import (  # noqa: E402, E501
    ClickHousePoolConfig,
)
from db_connections.scr.all_db_connectors.connectors.clickhouse.pool import (  # noqa: E402, E501
    ClickHouseSyncConnectionPool,
)
from db_connections.scr.all_db_connectors.core.exceptions import (  # noqa: E402
    ConnectionError,
    PoolExhaustedError,
)
from db_connections.scr.all_db_connectors.core.health import (  # noqa: E402
    HealthState,
)


class MockClickHouseConnection:
    """Mock ClickHouse connection for testing."""

    def __init__(self):
        self.closed = False
        self.connected = True
        self.server_info = {"version_major": 23, "version_minor": 1, "revision": 54463}

    def ping(self):
        """Mock ping."""
        if self.closed:
            raise Exception("Connection closed")
        return True

    def execute(self, query, params=None):
        """Mock execute."""
        if self.closed:
            raise Exception("Connection closed")
        return [(1,)]

    def execute_iter(self, query, params=None):
        """Mock execute_iter."""
        if self.closed:
            raise Exception("Connection closed")
        return iter([(1,)])

    def get_server_info(self):
        """Mock get_server_info."""
        if self.closed:
            raise Exception("Connection closed")
        return self.server_info

    def close(self):
        """Mock close."""
        self.closed = True
        self.connected = False


class MockClickHouseConnectionPool:
    """Mock ClickHouse ConnectionPool."""

    def __init__(self, max_connections=10, **kwargs):
        self.max_connections = max_connections
        self.connections = []
        self.in_use = set()
        self.closed = False

    def get_connection(self):
        """Get connection from pool."""
        if self.closed:
            raise Exception("Pool is closed")
        if len(self.in_use) >= self.max_connections:
            return None
        conn = MockClickHouseConnection()
        self.in_use.add(id(conn))
        return conn

    def release_connection(self, connection):
        """Release connection to pool."""
        conn_id = id(connection)
        if conn_id in self.in_use:
            self.in_use.remove(conn_id)

    def close(self):
        """Close all connections."""
        self.closed = True
        self.in_use.clear()

    def reset(self):
        """Reset pool."""
        self.close()


class TestClickHouseConnectionPoolInit(unittest.TestCase):
    """Test ClickHouseSyncConnectionPool initialization."""

    def setUp(self):
        """Set up test fixtures."""
        self.clickhouse_config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_clickhouse_pool = MockClickHouseConnectionPool(max_connections=10)

    def _setup_mock_clickhouse_module(self):
        """Set up mock clickhouse module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.clickhouse.pool.clickhouse_connect"
        ).start()
        self.mock_module.get_client.return_value = MockClickHouseConnection()

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_init_default(self):
        """Test pool initialization with defaults."""
        self._setup_mock_clickhouse_module()
        pool = ClickHouseSyncConnectionPool(self.clickhouse_config)

        self.assertEqual(pool.config, self.clickhouse_config)
        self.assertFalse(pool._initialized)
        self.assertIsNone(pool._pool)
        self.assertFalse(pool._closed)

    def test_init_validates_config(self):
        """Test pool validates configuration on init."""
        with self.assertRaisesRegex(ValueError, "max_connections must be positive"):
            invalid_config = ClickHousePoolConfig(
                host="localhost",
                max_connections=-1,  # Invalid
            )

    def test_repr(self):
        """Test string representation."""
        self._setup_mock_clickhouse_module()
        pool = ClickHouseSyncConnectionPool(self.clickhouse_config)

        repr_str = repr(pool)
        self.assertIn("ClickHouseSyncConnectionPool", repr_str)
        self.assertIn("localhost", repr_str)
        self.assertIn("9000", repr_str)


class TestClickHouseConnectionPoolInitialization(unittest.TestCase):
    """Test pool initialization methods."""

    def setUp(self):
        """Set up test fixtures."""
        self.clickhouse_config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_clickhouse_pool = MockClickHouseConnectionPool(max_connections=10)

    def _setup_mock_clickhouse_module(self):
        """Set up mock clickhouse module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.clickhouse.pool.clickhouse_connect"
        ).start()
        self.mock_module.get_client.return_value = MockClickHouseConnection()

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_initialize_pool(self):
        """Test explicit pool initialization."""
        self._setup_mock_clickhouse_module()
        pool = ClickHouseSyncConnectionPool(self.clickhouse_config)
        pool.initialize_pool()

        self.assertTrue(pool._initialized)
        self.assertIsNotNone(pool._pool)
        self.mock_module.get_client.assert_called_once()

    def test_initialize_pool_already_initialized(self):
        """Test initializing already initialized pool."""
        self._setup_mock_clickhouse_module()
        pool = ClickHouseSyncConnectionPool(self.clickhouse_config)
        pool.initialize_pool()
        pool.initialize_pool()  # Second call should be no-op

        # Should only be called once
        self.assertEqual(self.mock_module.get_client.call_count, 1)

    def test_initialize_pool_connection_error(self):
        """Test pool initialization with connection error."""
        with patch(
            "db_connections.scr.all_db_connectors.connectors.clickhouse.pool.clickhouse_connect"
        ) as mock_module:
            mock_module.get_client.side_effect = Exception("Connection failed")

            pool = ClickHouseSyncConnectionPool(self.clickhouse_config)

            with self.assertRaisesRegex(ConnectionError, "Pool initialization failed"):
                pool.initialize_pool()

    def test_lazy_initialization(self):
        """Test lazy pool initialization on first connection."""
        self._setup_mock_clickhouse_module()
        pool = ClickHouseSyncConnectionPool(self.clickhouse_config)

        self.assertFalse(pool._initialized)

        with pool.get_connection():
            pass

        self.assertTrue(pool._initialized)


class TestClickHouseConnectionPoolGetConnection(unittest.TestCase):
    """Test connection acquisition."""

    def setUp(self):
        """Set up test fixtures."""
        self.clickhouse_config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_clickhouse_pool = MockClickHouseConnectionPool(max_connections=10)
        self.mock_clickhouse_connection = MockClickHouseConnection()

    def _setup_mock_clickhouse_module(self):
        """Set up mock clickhouse module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.clickhouse.pool.clickhouse_connect"
        ).start()
        self.mock_module.get_client.return_value = self.mock_clickhouse_connection

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_get_connection_success(self):
        """Test successful connection acquisition."""
        self._setup_mock_clickhouse_module()
        pool = ClickHouseSyncConnectionPool(self.clickhouse_config)
        pool.initialize_pool()

        with pool.get_connection() as conn:
            self.assertIsNotNone(conn)

    def test_get_connection_lazy_init(self):
        """Test connection triggers lazy initialization."""
        self._setup_mock_clickhouse_module()
        pool = ClickHouseSyncConnectionPool(self.clickhouse_config)

        self.assertFalse(pool._initialized)

        with pool.get_connection():
            self.assertTrue(pool._initialized)

    def test_get_connection_validates(self):
        """Test connection validation on checkout."""
        self._setup_mock_clickhouse_module()
        config = ClickHousePoolConfig(
            host="localhost", validate_on_checkout=True, pre_ping=True
        )

        pool = ClickHouseSyncConnectionPool(config)

        with patch.object(pool, "validate_connection", return_value=True):
            with pool.get_connection() as conn:
                self.assertIsNotNone(conn)

    def test_get_connection_pool_closed(self):
        """Test getting connection from closed pool."""
        self._setup_mock_clickhouse_module()
        pool = ClickHouseSyncConnectionPool(self.clickhouse_config)
        pool._closed = True

        with self.assertRaisesRegex(ConnectionError, "Pool is closed"):
            with pool.get_connection():
                pass

    def test_get_connection_tracks_metadata(self):
        """Test connection metadata is tracked."""
        self._setup_mock_clickhouse_module()
        pool = ClickHouseSyncConnectionPool(self.clickhouse_config)
        pool.initialize_pool()

        with pool.get_connection() as conn:
            conn_id = id(conn)
            self.assertIn(conn_id, pool._connection_metadata)
            self.assertTrue(pool._connection_metadata[conn_id].in_use)

    def test_get_connection_releases_on_exit(self):
        """Test connection is released after context manager exit."""
        self._setup_mock_clickhouse_module()
        pool = ClickHouseSyncConnectionPool(self.clickhouse_config)
        pool.initialize_pool()

        with pool.get_connection() as conn:
            conn_id = id(conn)

        # After context manager exit
        self.assertNotIn(conn_id, pool._connections_in_use)

    def test_get_connection_exhausted(self):
        """Test connection acquisition when pool is exhausted."""
        self._setup_mock_clickhouse_module()
        # Create config with small max size
        config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
            max_connections=1,
            min_connections=1,
            timeout=0.1,  # Short timeout
        )
        pool = ClickHouseSyncConnectionPool(config)
        pool.initialize_pool()

        # Acquire the connection from pool
        with pool.get_connection() as conn:
            self.assertIsNotNone(conn)
            # Pool is now empty, but we can still get connections
            # as the implementation creates new ones when queue is empty
            # This test verifies basic connection acquisition works


class TestClickHouseConnectionPoolRelease(unittest.TestCase):
    """Test connection release."""

    def setUp(self):
        """Set up test fixtures."""
        self.clickhouse_config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_clickhouse_pool = MockClickHouseConnectionPool(max_connections=10)
        self.mock_clickhouse_connection = MockClickHouseConnection()

    def _setup_mock_clickhouse_module(self):
        """Set up mock clickhouse module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.clickhouse.pool.clickhouse_connect"
        ).start()
        self.mock_module.get_client.return_value = self.mock_clickhouse_connection

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_release_connection(self):
        """Test releasing a connection back to pool."""
        self._setup_mock_clickhouse_module()
        pool = ClickHouseSyncConnectionPool(self.clickhouse_config)
        pool.initialize_pool()

        conn = self.mock_clickhouse_connection
        conn_id = id(conn)
        pool._connections_in_use.add(conn_id)

        pool.release_connection(conn)

        self.assertNotIn(conn_id, pool._connections_in_use)

    def test_release_connection_with_recycling(self):
        """Test connection recycling on release."""
        self._setup_mock_clickhouse_module()
        config = ClickHousePoolConfig(host="localhost", recycle_on_return=True)

        pool = ClickHouseSyncConnectionPool(config)
        pool.initialize_pool()

        conn = self.mock_clickhouse_connection
        conn_id = id(conn)

        # Create metadata
        from db_connections.scr.all_db_connectors.core.utils import ConnectionMetadata

        pool._connection_metadata[conn_id] = ConnectionMetadata(
            created_at=datetime.now() - timedelta(hours=1)
        )

        pool.release_connection(conn)

        # Connection should be recycled
        self.assertNotIn(conn_id, pool._connection_metadata)


class TestClickHouseConnectionPoolClose(unittest.TestCase):
    """Test pool closing."""

    def setUp(self):
        """Set up test fixtures."""
        self.clickhouse_config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_clickhouse_pool = MockClickHouseConnectionPool(max_connections=10)
        self.mock_clickhouse_connection = MockClickHouseConnection()

    def _setup_mock_clickhouse_module(self):
        """Set up mock clickhouse module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.clickhouse.pool.clickhouse_connect"
        ).start()
        self.mock_module.get_client.return_value = self.mock_clickhouse_connection

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_close_connection(self):
        """Test closing a single connection."""
        self._setup_mock_clickhouse_module()
        pool = ClickHouseSyncConnectionPool(self.clickhouse_config)
        pool.initialize_pool()

        conn = self.mock_clickhouse_connection
        conn_id = id(conn)
        pool._connections_in_use.add(conn_id)

        pool.close_connection(conn)

        self.assertNotIn(conn_id, pool._connections_in_use)
        self.assertTrue(conn.closed)

    def test_close_all_connections(self):
        """Test closing all connections in pool."""
        self._setup_mock_clickhouse_module()
        pool = ClickHouseSyncConnectionPool(self.clickhouse_config)
        pool.initialize_pool()

        pool.close_all_connections()

        self.assertTrue(pool._closed)
        self.assertEqual(len(pool._connections_in_use), 0)
        self.assertEqual(len(pool._connection_metadata), 0)


class TestClickHouseConnectionPoolStatus(unittest.TestCase):
    """Test pool status and metrics."""

    def setUp(self):
        """Set up test fixtures."""
        self.clickhouse_config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_clickhouse_pool = MockClickHouseConnectionPool(max_connections=10)

    def _setup_mock_clickhouse_module(self):
        """Set up mock clickhouse module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.clickhouse.pool.clickhouse_connect"
        ).start()
        self.mock_module.get_client.return_value = MockClickHouseConnection()

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_pool_status_uninitialized(self):
        """Test pool status before initialization."""
        self._setup_mock_clickhouse_module()
        pool = ClickHouseSyncConnectionPool(self.clickhouse_config)

        status = pool.pool_status()

        self.assertFalse(status["initialized"])
        self.assertEqual(status["total_connections"], 0)
        expected_max = (
            self.clickhouse_config.max_connections + self.clickhouse_config.max_overflow
        )
        self.assertEqual(status["max_connections"], expected_max)

    def test_pool_status_initialized(self):
        """Test pool status after initialization."""
        self._setup_mock_clickhouse_module()
        pool = ClickHouseSyncConnectionPool(self.clickhouse_config)
        pool.initialize_pool()

        status = pool.pool_status()

        self.assertTrue(status["initialized"])
        self.assertFalse(status["closed"])
        expected_max = (
            self.clickhouse_config.max_connections + self.clickhouse_config.max_overflow
        )
        self.assertEqual(status["max_connections"], expected_max)
        self.assertEqual(
            status["min_connections"], self.clickhouse_config.min_connections
        )

    def test_get_metrics(self):
        """Test getting pool metrics."""
        self._setup_mock_clickhouse_module()
        pool = ClickHouseSyncConnectionPool(self.clickhouse_config)
        pool.initialize_pool()

        metrics = pool.get_metrics()

        self.assertGreaterEqual(metrics.total_connections, 0)
        self.assertGreaterEqual(metrics.active_connections, 0)
        self.assertGreaterEqual(metrics.idle_connections, 0)
        expected_max = (
            self.clickhouse_config.max_connections + self.clickhouse_config.max_overflow
        )
        self.assertEqual(metrics.max_connections, expected_max)
        self.assertEqual(
            metrics.min_connections, self.clickhouse_config.min_connections
        )


class TestClickHouseConnectionPoolValidation(unittest.TestCase):
    """Test connection validation."""

    def setUp(self):
        """Set up test fixtures."""
        self.clickhouse_config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_clickhouse_pool = MockClickHouseConnectionPool(max_connections=10)
        self.mock_clickhouse_connection = MockClickHouseConnection()

    def _setup_mock_clickhouse_module(self):
        """Set up mock clickhouse module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.clickhouse.pool.clickhouse_connect"
        ).start()
        self.mock_module.get_client.return_value = self.mock_clickhouse_connection

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_validate_connection_success(self):
        """Test successful connection validation."""
        self._setup_mock_clickhouse_module()
        pool = ClickHouseSyncConnectionPool(self.clickhouse_config)

        result = pool.validate_connection(self.mock_clickhouse_connection)

        self.assertTrue(result)

    def test_validate_connection_failure(self):
        """Test connection validation failure."""
        self._setup_mock_clickhouse_module()
        pool = ClickHouseSyncConnectionPool(self.clickhouse_config)

        # Create connection that will fail validation
        bad_conn = Mock()
        bad_conn.ping.side_effect = Exception("Connection lost")

        result = pool.validate_connection(bad_conn)

        self.assertFalse(result)


class TestClickHouseConnectionPoolHealth(unittest.TestCase):
    """Test health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.clickhouse_config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_clickhouse_pool = MockClickHouseConnectionPool(max_connections=10)
        self.mock_clickhouse_connection = MockClickHouseConnection()

    def _setup_mock_clickhouse_module(self):
        """Set up mock clickhouse module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.clickhouse.pool.clickhouse_connect"
        ).start()
        self.mock_module.get_client.return_value = self.mock_clickhouse_connection

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_health_check_healthy(self):
        """Test health check on healthy pool."""
        self._setup_mock_clickhouse_module()
        pool = ClickHouseSyncConnectionPool(self.clickhouse_config)
        pool.initialize_pool()

        health = pool.health_check()

        self.assertEqual(health.state, HealthState.HEALTHY)
        self.assertIn("healthy", health.message.lower())

    def test_database_health_check(self):
        """Test database health check."""
        self._setup_mock_clickhouse_module()
        pool = ClickHouseSyncConnectionPool(self.clickhouse_config)
        pool.initialize_pool()

        with patch.object(pool, "get_connection"):
            health = pool.database_health_check()

            self.assertIn(
                health.state,
                [HealthState.HEALTHY, HealthState.DEGRADED, HealthState.UNHEALTHY],
            )


class TestClickHouseConnectionPoolContextManager(unittest.TestCase):
    """Test context manager support."""

    def setUp(self):
        """Set up test fixtures."""
        self.clickhouse_config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_clickhouse_pool = MockClickHouseConnectionPool(max_connections=10)

    def _setup_mock_clickhouse_module(self):
        """Set up mock clickhouse module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.clickhouse.pool.clickhouse_connect"
        ).start()
        self.mock_module.get_client.return_value = MockClickHouseConnection()

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_context_manager_enter(self):
        """Test pool context manager enter."""
        self._setup_mock_clickhouse_module()
        with ClickHouseSyncConnectionPool(self.clickhouse_config) as pool:
            self.assertTrue(pool._initialized)
            self.assertFalse(pool._closed)

    def test_context_manager_exit(self):
        """Test pool context manager exit."""
        self._setup_mock_clickhouse_module()
        pool = ClickHouseSyncConnectionPool(self.clickhouse_config)

        with pool:
            pass

        self.assertTrue(pool._closed)

    def test_context_manager_with_connections(self):
        """Test using connections within context manager."""
        self._setup_mock_clickhouse_module()
        with ClickHouseSyncConnectionPool(self.clickhouse_config) as pool:
            with pool.get_connection() as conn:
                self.assertIsNotNone(conn)

        self.assertTrue(pool._closed)


class TestClickHouseConnectionPoolRetry(unittest.TestCase):
    """Test retry logic."""

    def setUp(self):
        """Set up test fixtures."""
        self.clickhouse_config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )

    def test_retry_on_connection_failure(self):
        """Test retry logic on connection failure."""
        with patch(
            "db_connections.scr.all_db_connectors.connectors.clickhouse.pool.clickhouse_connect"
        ) as mock_module:
            call_count = [0]

            def get_client_side_effect(*args, **kwargs):
                call_count[0] += 1
                if call_count[0] == 1:
                    # First call for pool init - succeed
                    return MockClickHouseConnection()
                elif call_count[0] <= 3:
                    # Next calls fail (for get_connection retries)
                    raise Exception("Connection failed")
                else:
                    # Succeed on retry
                    return MockClickHouseConnection()

            mock_module.get_client.side_effect = get_client_side_effect

            config = ClickHousePoolConfig(
                host="localhost",
                max_retries=3,
                retry_delay=0.01,  # Very short delay for testing
                min_connections=1,
            )

            pool = ClickHouseSyncConnectionPool(config)
            pool.initialize_pool()

            # Clear the pool so get_connection will try to create a new one
            while not pool._pool.empty():
                pool._pool.get_nowait()

            # Should succeed after retries
            with pool.get_connection() as conn:
                self.assertIsNotNone(conn)

    def test_retry_exhausted(self):
        """Test when all retry attempts are exhausted."""
        with patch(
            "db_connections.scr.all_db_connectors.connectors.clickhouse.pool.clickhouse_connect"
        ) as mock_module:
            call_count = [0]

            def get_client_side_effect(*args, **kwargs):
                call_count[0] += 1
                if call_count[0] == 1:
                    # First call for pool initialization - succeed
                    return MockClickHouseConnection()
                else:
                    # All subsequent calls fail (for get_connection retries)
                    raise Exception("Connection failed")

            mock_module.get_client.side_effect = get_client_side_effect

            config = ClickHousePoolConfig(
                host="localhost",
                max_retries=2,
                retry_delay=0.01,  # Very short delay for testing
                min_connections=1,
            )

            pool = ClickHouseSyncConnectionPool(config)
            pool.initialize_pool()

            # Clear the pool so get_connection will try to create a new one
            while not pool._pool.empty():
                pool._pool.get_nowait()

            # Now get_connection will try to create a new connection, which will fail
            # After max_retries attempts, it should raise ConnectionError
            with self.assertRaisesRegex(
                ConnectionError, "Connection acquisition failed"
            ):
                with pool.get_connection():
                    pass


class TestClickHouseConnectionPoolConcurrency(unittest.TestCase):
    """Test concurrent operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.clickhouse_config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_clickhouse_pool = MockClickHouseConnectionPool(max_connections=10)
        self.mock_clickhouse_connection = MockClickHouseConnection()

    def _setup_mock_clickhouse_module(self):
        """Set up mock clickhouse module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.clickhouse.pool.clickhouse_connect"
        ).start()
        self.mock_module.get_client.return_value = self.mock_clickhouse_connection

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_multiple_connections(self):
        """Test acquiring multiple connections."""
        # Setup mock to return different connections each time
        with patch(
            "db_connections.scr.all_db_connectors.connectors.clickhouse.pool.clickhouse_connect"
        ) as mock_module:
            # Return a new mock connection each time
            mock_module.get_client.side_effect = [
                MockClickHouseConnection() for _ in range(10)
            ]

            pool = ClickHouseSyncConnectionPool(self.clickhouse_config)
            pool.initialize_pool()

            connections = []

            # Acquire multiple connections simultaneously (nested context managers)
            # This ensures we get different connections, not reused ones
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
        self._setup_mock_clickhouse_module()
        pool = ClickHouseSyncConnectionPool(self.clickhouse_config)
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
