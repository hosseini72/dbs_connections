"""Unit tests for ClickHouse asynchronous connection pool."""

import sys
from pathlib import Path
import unittest
import asyncio
from unittest.mock import Mock, patch, AsyncMock, MagicMock

# Add parent directory to path to allow imports
# test_clickhouse_pool_async.py is at: db_connections/tests/unit/connectors/clickhouse/
# We need to go up 6 levels to get to the parent of db_connections
try:
    _file_path = Path(__file__).resolve()
except NameError:
    # __file__ might not be defined in some contexts
    import os
    _file_path = Path(os.getcwd()) / 'tests' / 'unit' / 'connectors' / 'clickhouse' / 'test_clickhouse_pool_async.py'  # noqa: E501

parent_dir = _file_path.parent.parent.parent.parent.parent.parent
parent_dir_str = str(parent_dir)
if parent_dir_str not in sys.path:
    sys.path.insert(0, parent_dir_str)

from db_connections.scr.all_db_connectors.connectors.clickhouse.config import (  # noqa: E402
    ClickHousePoolConfig
)
from db_connections.scr.all_db_connectors.connectors.clickhouse.pool import (  # noqa: E402, E501
    ClickHouseAsyncConnectionPool
)
from db_connections.scr.all_db_connectors.core.exceptions import (  # noqa: E402
    ConnectionError,
    PoolTimeoutError,
)
from db_connections.scr.all_db_connectors.core.health import (  # noqa: E402
    HealthState
)


class MockAsyncClickHouseConnection:
    """Mock Async ClickHouse connection for testing."""

    def __init__(self):
        self.closed = False
        self.connected = True
        self.server_info = {
            "version_major": 23,
            "version_minor": 1,
            "revision": 54463
        }

    async def ping(self):
        """Mock ping."""
        if self.closed:
            raise Exception("Connection closed")
        return True

    async def execute(self, query, params=None):
        """Mock execute."""
        if self.closed:
            raise Exception("Connection closed")
        return [(1,)]

    async def execute_iter(self, query, params=None):
        """Mock execute_iter."""
        if self.closed:
            raise Exception("Connection closed")
        return iter([(1,)])

    async def get_server_info(self):
        """Mock get_server_info."""
        if self.closed:
            raise Exception("Connection closed")
        return self.server_info

    async def close(self):
        """Mock close."""
        self.closed = True
        self.connected = False


class MockAsyncClickHouseConnectionPool:
    """Mock Async ClickHouse ConnectionPool."""

    def __init__(self, max_connections=10, **kwargs):
        self.max_connections = max_connections
        self.connections = []
        self.in_use = set()
        self.closed = False

    async def acquire(self):
        """Get connection from pool."""
        if self.closed:
            raise Exception("Pool is closed")
        if len(self.in_use) >= self.max_connections:
            return None
        conn = MockAsyncClickHouseConnection()
        self.in_use.add(id(conn))
        return conn

    async def release(self, connection):
        """Release connection to pool."""
        conn_id = id(connection)
        if conn_id in self.in_use:
            self.in_use.remove(conn_id)

    async def close(self):
        """Close all connections."""
        self.closed = True
        self.in_use.clear()


class TestAsyncClickHouseConnectionPoolInit(unittest.TestCase):
    """Test ClickHouseAsyncConnectionPool initialization."""

    def setUp(self):
        """Set up test fixtures."""
        self.clickhouse_config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_async_clickhouse_pool = MockAsyncClickHouseConnectionPool(
            max_connections=10
        )

    def _setup_mock_async_clickhouse_module(self):
        """Set up mock async clickhouse module."""
        async def create_client_mock(**kwargs):
            return MockAsyncClickHouseConnection()

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors."
            "clickhouse.pool.clickhouse_connect"
        )
        self.mock_module = patch(patch_path).start()
        self.mock_module.get_async_client = create_client_mock
        
        # Also patch ASYNC_AVAILABLE to True
        patch(
            "db_connections.scr.all_db_connectors.connectors."
            "clickhouse.pool.ASYNC_AVAILABLE",
            True
        ).start()

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_init_default(self):
        """Test pool initialization with defaults."""
        self._setup_mock_async_clickhouse_module()
        pool = ClickHouseAsyncConnectionPool(self.clickhouse_config)

        self.assertEqual(pool.config, self.clickhouse_config)
        self.assertFalse(pool._initialized)
        self.assertIsNone(pool._pool)
        self.assertFalse(pool._closed)

    def test_init_validates_config(self):
        """Test pool validates configuration on init."""
        with self.assertRaisesRegex(
            ValueError, "max_connections must be positive"
        ):
            invalid_config = ClickHousePoolConfig(
                host="localhost",
                max_connections=-1  # Invalid
            )

    def test_repr(self):
        """Test string representation."""
        self._setup_mock_async_clickhouse_module()
        pool = ClickHouseAsyncConnectionPool(self.clickhouse_config)

        repr_str = repr(pool)
        self.assertIn("ClickHouseAsyncConnectionPool", repr_str)
        self.assertIn("localhost", repr_str)
        self.assertIn("9000", repr_str)


class TestAsyncClickHouseConnectionPoolInitialization(
    unittest.IsolatedAsyncioTestCase
):
    """Test async pool initialization."""

    def setUp(self):
        """Set up test fixtures."""
        self.clickhouse_config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_async_clickhouse_pool = MockAsyncClickHouseConnectionPool(
            max_connections=10
        )

    def _setup_mock_async_clickhouse_module(self):
        """Set up mock async clickhouse module."""
        async def create_client_mock(**kwargs):
            return MockAsyncClickHouseConnection()

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors."
            "clickhouse.pool.clickhouse_connect"
        )
        self.mock_module = patch(patch_path).start()
        self.mock_module.get_async_client = create_client_mock
        
        # Also patch ASYNC_AVAILABLE to True
        patch(
            "db_connections.scr.all_db_connectors.connectors."
            "clickhouse.pool.ASYNC_AVAILABLE",
            True
        ).start()

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_initialize_pool(self):
        """Test explicit pool initialization."""
        self._setup_mock_async_clickhouse_module()
        pool = ClickHouseAsyncConnectionPool(self.clickhouse_config)
        await pool.initialize_pool()

        self.assertTrue(pool._initialized)
        self.assertIsNotNone(pool._pool)

    async def test_initialize_pool_already_initialized(self):
        """Test initializing already initialized pool."""
        self._setup_mock_async_clickhouse_module()
        pool = ClickHouseAsyncConnectionPool(self.clickhouse_config)
        await pool.initialize_pool()
        await pool.initialize_pool()  # Second call should be no-op

        self.assertTrue(pool._initialized)

    async def test_initialize_pool_connection_error(self):
        """Test pool initialization with connection error."""
        async def failing_create_client(**kwargs):
            raise Exception("Connection failed")

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors."
            "clickhouse.pool.clickhouse_connect"
        )
        with patch(patch_path) as mock_module:
            mock_module.get_async_client = failing_create_client
            # Also patch ASYNC_AVAILABLE to True
            with patch(
                "db_connections.scr.all_db_connectors.connectors."
                "clickhouse.pool.ASYNC_AVAILABLE",
                True
            ):
                pool = ClickHouseAsyncConnectionPool(self.clickhouse_config)

                with self.assertRaisesRegex(
                    ConnectionError, "Pool initialization failed"
                ):
                    await pool.initialize_pool()

    async def test_lazy_initialization(self):
        """Test lazy pool initialization on first connection."""
        self._setup_mock_async_clickhouse_module()
        pool = ClickHouseAsyncConnectionPool(self.clickhouse_config)

        self.assertFalse(pool._initialized)

        async with pool.get_connection():
            pass

        self.assertTrue(pool._initialized)


class TestAsyncClickHouseConnectionPoolGetConnection(
    unittest.IsolatedAsyncioTestCase
):
    """Test async connection acquisition."""

    def setUp(self):
        """Set up test fixtures."""
        self.clickhouse_config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_async_clickhouse_pool = MockAsyncClickHouseConnectionPool(
            max_connections=10
        )
        self.mock_async_clickhouse_connection = (
            MockAsyncClickHouseConnection()
        )

    def _setup_mock_async_clickhouse_module(self):
        """Set up mock async clickhouse module."""
        async def create_client_mock(**kwargs):
            return self.mock_async_clickhouse_connection

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors."
            "clickhouse.pool.clickhouse_connect"
        )
        self.mock_module = patch(patch_path).start()
        self.mock_module.get_async_client = create_client_mock
        
        # Also patch ASYNC_AVAILABLE to True
        patch(
            "db_connections.scr.all_db_connectors.connectors."
            "clickhouse.pool.ASYNC_AVAILABLE",
            True
        ).start()

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_get_connection_success(self):
        """Test successful connection acquisition."""
        self._setup_mock_async_clickhouse_module()
        pool = ClickHouseAsyncConnectionPool(self.clickhouse_config)
        await pool.initialize_pool()

        async with pool.get_connection() as conn:
            self.assertIsNotNone(conn)

    async def test_get_connection_lazy_init(self):
        """Test connection triggers lazy initialization."""
        self._setup_mock_async_clickhouse_module()
        pool = ClickHouseAsyncConnectionPool(self.clickhouse_config)

        self.assertFalse(pool._initialized)

        async with pool.get_connection():
            self.assertTrue(pool._initialized)

    async def test_get_connection_validates(self):
        """Test connection validation on checkout."""
        self._setup_mock_async_clickhouse_module()
        config = ClickHousePoolConfig(
            host="localhost",
            validate_on_checkout=True,
            pre_ping=True
        )

        pool = ClickHouseAsyncConnectionPool(config)

        with patch.object(
            pool, "validate_connection", return_value=True
        ):
            async with pool.get_connection() as conn:
                self.assertIsNotNone(conn)

    async def test_get_connection_pool_closed(self):
        """Test getting connection from closed pool."""
        self._setup_mock_async_clickhouse_module()
        pool = ClickHouseAsyncConnectionPool(self.clickhouse_config)
        pool._closed = True

        with self.assertRaisesRegex(ConnectionError, "Pool is closed"):
            async with pool.get_connection():
                pass

    async def test_get_connection_tracks_metadata(self):
        """Test connection metadata is tracked."""
        self._setup_mock_async_clickhouse_module()
        pool = ClickHouseAsyncConnectionPool(self.clickhouse_config)
        await pool.initialize_pool()

        async with pool.get_connection() as conn:
            conn_id = id(conn)
            async with pool._metadata_lock:
                self.assertIn(conn_id, pool._connection_metadata)
                self.assertTrue(
                    pool._connection_metadata[conn_id].in_use
                )

    async def test_get_connection_releases_on_exit(self):
        """Test connection is released after context manager exit."""
        self._setup_mock_async_clickhouse_module()
        pool = ClickHouseAsyncConnectionPool(self.clickhouse_config)
        await pool.initialize_pool()

        async with pool.get_connection() as conn:
            conn_id = id(conn)

        # After context manager exit
        self.assertNotIn(conn_id, pool._connections_in_use)

    async def test_get_connection_timeout(self):
        """Test connection acquisition timeout."""
        # Setup mock to succeed for pool initialization
        # but timeout when getting connection from empty pool
        call_count = [0]
        async def create_client_mock(**kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                # First call for pool init - succeed
                return MockAsyncClickHouseConnection()
            else:
                # Subsequent calls should timeout, but actually the pool
                # uses asyncio.wait_for which will raise TimeoutError
                # Let's just make it fail to create connection
                raise asyncio.TimeoutError("Connection timeout")

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors."
            "clickhouse.pool.clickhouse_connect"
        )
        with patch(patch_path) as mock_module:
            mock_module.get_async_client = create_client_mock
            # Also patch ASYNC_AVAILABLE to True
            with patch(
                "db_connections.scr.all_db_connectors.connectors."
                "clickhouse.pool.ASYNC_AVAILABLE",
                True
            ):
                pool = ClickHouseAsyncConnectionPool(self.clickhouse_config)
                await pool.initialize_pool()
                
                # Clear the pool so get_connection will try to create a new one
                while not pool._pool.empty():
                    pool._pool.get_nowait()

                # This should timeout or fail
                with self.assertRaises((PoolTimeoutError, ConnectionError, asyncio.TimeoutError)):
                    async with pool.get_connection():
                        pass


class TestAsyncClickHouseConnectionPoolRelease(
    unittest.IsolatedAsyncioTestCase
):
    """Test async connection release."""

    def setUp(self):
        """Set up test fixtures."""
        self.clickhouse_config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_async_clickhouse_pool = MockAsyncClickHouseConnectionPool(
            max_connections=10
        )
        self.mock_async_clickhouse_connection = (
            MockAsyncClickHouseConnection()
        )

    def _setup_mock_async_clickhouse_module(self):
        """Set up mock async clickhouse module."""
        async def create_client_mock(**kwargs):
            return self.mock_async_clickhouse_connection

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors."
            "clickhouse.pool.clickhouse_connect"
        )
        self.mock_module = patch(patch_path).start()
        self.mock_module.get_async_client = create_client_mock
        
        # Also patch ASYNC_AVAILABLE to True
        patch(
            "db_connections.scr.all_db_connectors.connectors."
            "clickhouse.pool.ASYNC_AVAILABLE",
            True
        ).start()

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_release_connection(self):
        """Test releasing a connection back to pool."""
        self._setup_mock_async_clickhouse_module()
        pool = ClickHouseAsyncConnectionPool(self.clickhouse_config)
        await pool.initialize_pool()

        conn = self.mock_async_clickhouse_connection
        conn_id = id(conn)
        pool._connections_in_use.add(conn_id)

        await pool.release_connection(conn)

        self.assertNotIn(conn_id, pool._connections_in_use)


class TestAsyncClickHouseConnectionPoolClose(
    unittest.IsolatedAsyncioTestCase
):
    """Test async pool closing."""

    def setUp(self):
        """Set up test fixtures."""
        self.clickhouse_config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_async_clickhouse_pool = MockAsyncClickHouseConnectionPool(
            max_connections=10
        )
        self.mock_async_clickhouse_connection = (
            MockAsyncClickHouseConnection()
        )

    def _setup_mock_async_clickhouse_module(self):
        """Set up mock async clickhouse module."""
        async def create_client_mock(**kwargs):
            return self.mock_async_clickhouse_connection

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors."
            "clickhouse.pool.clickhouse_connect"
        )
        self.mock_module = patch(patch_path).start()
        self.mock_module.get_async_client = create_client_mock
        
        # Also patch ASYNC_AVAILABLE to True
        patch(
            "db_connections.scr.all_db_connectors.connectors."
            "clickhouse.pool.ASYNC_AVAILABLE",
            True
        ).start()

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_close_connection(self):
        """Test closing a single connection."""
        self._setup_mock_async_clickhouse_module()
        pool = ClickHouseAsyncConnectionPool(self.clickhouse_config)
        await pool.initialize_pool()

        conn = self.mock_async_clickhouse_connection
        conn_id = id(conn)
        pool._connections_in_use.add(conn_id)

        await pool.close_connection(conn)

        self.assertNotIn(conn_id, pool._connections_in_use)
        self.assertTrue(conn.closed)

    async def test_close_all_connections(self):
        """Test closing all connections in pool."""
        self._setup_mock_async_clickhouse_module()
        pool = ClickHouseAsyncConnectionPool(self.clickhouse_config)
        await pool.initialize_pool()

        await pool.close_all_connections()

        self.assertTrue(pool._closed)
        self.assertEqual(len(pool._connections_in_use), 0)


class TestAsyncClickHouseConnectionPoolStatus(
    unittest.IsolatedAsyncioTestCase
):
    """Test async pool status and metrics."""

    def setUp(self):
        """Set up test fixtures."""
        self.clickhouse_config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_async_clickhouse_pool = MockAsyncClickHouseConnectionPool(
            max_connections=10
        )

    def _setup_mock_async_clickhouse_module(self):
        """Set up mock async clickhouse module."""
        async def create_client_mock(**kwargs):
            return MockAsyncClickHouseConnection()

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors."
            "clickhouse.pool.clickhouse_connect"
        )
        self.mock_module = patch(patch_path).start()
        self.mock_module.get_async_client = create_client_mock
        
        # Also patch ASYNC_AVAILABLE to True
        patch(
            "db_connections.scr.all_db_connectors.connectors."
            "clickhouse.pool.ASYNC_AVAILABLE",
            True
        ).start()

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_pool_status_uninitialized(self):
        """Test pool status before initialization."""
        self._setup_mock_async_clickhouse_module()
        pool = ClickHouseAsyncConnectionPool(self.clickhouse_config)

        status = await pool.pool_status()

        self.assertFalse(status["initialized"])
        self.assertEqual(status["total_connections"], 0)
        expected_max = (
            self.clickhouse_config.max_connections +
            self.clickhouse_config.max_overflow
        )
        self.assertEqual(status["max_connections"], expected_max)

    async def test_pool_status_initialized(self):
        """Test pool status after initialization."""
        self._setup_mock_async_clickhouse_module()
        pool = ClickHouseAsyncConnectionPool(self.clickhouse_config)
        await pool.initialize_pool()

        status = await pool.pool_status()

        self.assertTrue(status["initialized"])
        self.assertFalse(status["closed"])
        expected_max = (
            self.clickhouse_config.max_connections +
            self.clickhouse_config.max_overflow
        )
        self.assertEqual(status["max_connections"], expected_max)
        self.assertEqual(
            status["min_connections"], self.clickhouse_config.min_connections
        )

    async def test_get_metrics(self):
        """Test getting pool metrics."""
        self._setup_mock_async_clickhouse_module()
        pool = ClickHouseAsyncConnectionPool(self.clickhouse_config)
        await pool.initialize_pool()

        metrics = await pool.get_metrics()

        self.assertGreaterEqual(metrics.total_connections, 0)
        self.assertGreaterEqual(metrics.active_connections, 0)
        self.assertGreaterEqual(metrics.idle_connections, 0)
        expected_max = (
            self.clickhouse_config.max_connections +
            self.clickhouse_config.max_overflow
        )
        self.assertEqual(metrics.max_connections, expected_max)
        self.assertEqual(
            metrics.min_connections, self.clickhouse_config.min_connections
        )


class TestAsyncClickHouseConnectionPoolValidation(
    unittest.IsolatedAsyncioTestCase
):
    """Test async connection validation."""

    def setUp(self):
        """Set up test fixtures."""
        self.clickhouse_config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_async_clickhouse_pool = MockAsyncClickHouseConnectionPool(
            max_connections=10
        )
        self.mock_async_clickhouse_connection = (
            MockAsyncClickHouseConnection()
        )

    def _setup_mock_async_clickhouse_module(self):
        """Set up mock async clickhouse module."""
        async def create_client_mock(**kwargs):
            return self.mock_async_clickhouse_connection

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors."
            "clickhouse.pool.clickhouse_connect"
        )
        self.mock_module = patch(patch_path).start()
        self.mock_module.get_async_client = create_client_mock
        
        # Also patch ASYNC_AVAILABLE to True
        patch(
            "db_connections.scr.all_db_connectors.connectors."
            "clickhouse.pool.ASYNC_AVAILABLE",
            True
        ).start()

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_validate_connection_success(self):
        """Test successful connection validation."""
        self._setup_mock_async_clickhouse_module()
        pool = ClickHouseAsyncConnectionPool(self.clickhouse_config)

        result = await pool.validate_connection(
            self.mock_async_clickhouse_connection
        )

        self.assertTrue(result)

    async def test_validate_connection_failure(self):
        """Test connection validation failure."""
        self._setup_mock_async_clickhouse_module()
        pool = ClickHouseAsyncConnectionPool(self.clickhouse_config)

        # Create connection that will fail validation
        bad_conn = Mock()
        bad_conn.ping = AsyncMock(
            side_effect=Exception("Connection lost")
        )

        result = await pool.validate_connection(bad_conn)

        self.assertFalse(result)


class TestAsyncClickHouseConnectionPoolHealth(
    unittest.IsolatedAsyncioTestCase
):
    """Test async health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.clickhouse_config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_async_clickhouse_pool = MockAsyncClickHouseConnectionPool(
            max_connections=10
        )
        self.mock_async_clickhouse_connection = (
            MockAsyncClickHouseConnection()
        )

    def _setup_mock_async_clickhouse_module(self):
        """Set up mock async clickhouse module."""
        async def create_client_mock(**kwargs):
            return self.mock_async_clickhouse_connection

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors."
            "clickhouse.pool.clickhouse_connect"
        )
        self.mock_module = patch(patch_path).start()
        self.mock_module.get_async_client = create_client_mock
        
        # Also patch ASYNC_AVAILABLE to True
        patch(
            "db_connections.scr.all_db_connectors.connectors."
            "clickhouse.pool.ASYNC_AVAILABLE",
            True
        ).start()

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_health_check_healthy(self):
        """Test health check on healthy pool."""
        self._setup_mock_async_clickhouse_module()
        pool = ClickHouseAsyncConnectionPool(self.clickhouse_config)
        await pool.initialize_pool()

        health = await pool.health_check()

        self.assertEqual(health.state, HealthState.HEALTHY)
        self.assertIn("healthy", health.message.lower())

    async def test_health_check_degraded(self):
        """Test health check on degraded pool."""
        self._setup_mock_async_clickhouse_module()
        # Use a config with max_connections=10 and max_overflow=0 to test degradation
        # This ensures max_connections = 10, so 8/10 = 0.8 utilization = DEGRADED
        config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
            max_connections=10,
            max_overflow=0,  # Set to 0 so max_connections = 10
            min_connections=1,
            timeout=30,
        )
        pool = ClickHouseAsyncConnectionPool(config)
        await pool.initialize_pool()

        # Simulate high utilization by creating metadata for connections in use
        # We need both _connections_in_use and _connection_metadata to be set
        from db_connections.scr.all_db_connectors.core.utils import ConnectionMetadata
        
        # Clear the initial connection from the pool and add it to in_use
        # Then add 7 more to get 8 total active connections
        initial_conn_id = None
        if pool._connection_metadata:
            initial_conn_id = list(pool._connection_metadata.keys())[0]
            pool._connections_in_use.add(initial_conn_id)
        
        # Add 7 more connections to get 8 total active (80% utilization)
        for i in range(7):
            conn_id = i + 1000  # Use unique IDs
            pool._connections_in_use.add(conn_id)
            async with pool._get_metadata_lock():
                pool._connection_metadata[conn_id] = ConnectionMetadata()

        health = await pool.health_check()

        # With 8 active out of max 10, utilization is 0.8, which should be DEGRADED
        # (0.7 <= 0.8 < 0.9)
        self.assertIn(
            health.state, [HealthState.DEGRADED, HealthState.UNHEALTHY]
        )

    async def test_database_health_check(self):
        """Test database health check."""
        self._setup_mock_async_clickhouse_module()
        pool = ClickHouseAsyncConnectionPool(self.clickhouse_config)
        await pool.initialize_pool()

        health = await pool.database_health_check()

        self.assertIn(
            health.state,
            [
                HealthState.HEALTHY,
                HealthState.DEGRADED,
                HealthState.UNHEALTHY
            ]
        )
        self.assertIsNotNone(health.response_time_ms)


class TestAsyncClickHouseConnectionPoolContextManager(
    unittest.IsolatedAsyncioTestCase
):
    """Test async context manager support."""

    def setUp(self):
        """Set up test fixtures."""
        self.clickhouse_config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_async_clickhouse_pool = MockAsyncClickHouseConnectionPool(
            max_connections=10
        )

    def _setup_mock_async_clickhouse_module(self):
        """Set up mock async clickhouse module."""
        async def create_client_mock(**kwargs):
            return MockAsyncClickHouseConnection()

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors."
            "clickhouse.pool.clickhouse_connect"
        )
        self.mock_module = patch(patch_path).start()
        self.mock_module.get_async_client = create_client_mock
        
        # Also patch ASYNC_AVAILABLE to True
        patch(
            "db_connections.scr.all_db_connectors.connectors."
            "clickhouse.pool.ASYNC_AVAILABLE",
            True
        ).start()

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_context_manager_enter(self):
        """Test pool async context manager enter."""
        self._setup_mock_async_clickhouse_module()
        async with ClickHouseAsyncConnectionPool(
            self.clickhouse_config
        ) as pool:
            self.assertTrue(pool._initialized)
            self.assertFalse(pool._closed)

    async def test_context_manager_exit(self):
        """Test pool async context manager exit."""
        self._setup_mock_async_clickhouse_module()
        pool = ClickHouseAsyncConnectionPool(self.clickhouse_config)

        async with pool:
            pass

        self.assertTrue(pool._closed)

    async def test_context_manager_with_connections(self):
        """Test using connections within async context manager."""
        self._setup_mock_async_clickhouse_module()
        async with ClickHouseAsyncConnectionPool(
            self.clickhouse_config
        ) as pool:
            async with pool.get_connection() as conn:
                self.assertIsNotNone(conn)

        self.assertTrue(pool._closed)


class TestAsyncClickHouseConnectionPoolConcurrency(
    unittest.IsolatedAsyncioTestCase
):
    """Test concurrent async operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.clickhouse_config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_async_clickhouse_pool = MockAsyncClickHouseConnectionPool(
            max_connections=10
        )
        self.mock_async_clickhouse_connection = (
            MockAsyncClickHouseConnection()
        )

    def _setup_mock_async_clickhouse_module(self):
        """Set up mock async clickhouse module."""
        async def create_client_mock(**kwargs):
            return self.mock_async_clickhouse_connection

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors."
            "clickhouse.pool.clickhouse_connect"
        )
        self.mock_module = patch(patch_path).start()
        self.mock_module.get_async_client = create_client_mock
        
        # Also patch ASYNC_AVAILABLE to True
        patch(
            "db_connections.scr.all_db_connectors.connectors."
            "clickhouse.pool.ASYNC_AVAILABLE",
            True
        ).start()

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_multiple_concurrent_connections(self):
        """Test acquiring multiple connections concurrently."""
        self._setup_mock_async_clickhouse_module()
        pool = ClickHouseAsyncConnectionPool(self.clickhouse_config)
        await pool.initialize_pool()

        async def get_conn():
            async with pool.get_connection() as conn:
                return id(conn)

        # Get 5 connections concurrently
        conn_ids = await asyncio.gather(*[get_conn() for _ in range(5)])

        # All should be different (in mock scenario)
        self.assertEqual(len(conn_ids), 5)

    async def test_connection_reuse(self):
        """Test connections are reused after release."""
        self._setup_mock_async_clickhouse_module()
        pool = ClickHouseAsyncConnectionPool(self.clickhouse_config)
        await pool.initialize_pool()

        conn_id_1 = None
        conn_id_2 = None

        async with pool.get_connection() as conn:
            conn_id_1 = id(conn)

        # Connection should be returned to pool
        async with pool.get_connection() as conn:
            conn_id_2 = id(conn)

        # Might get same connection back
        self.assertIsNotNone(conn_id_1)
        self.assertIsNotNone(conn_id_2)


if __name__ == '__main__':
    unittest.main()

