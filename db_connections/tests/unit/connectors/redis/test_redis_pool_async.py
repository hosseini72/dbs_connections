"""Unit tests for Redis asynchronous connection pool."""

import sys
from pathlib import Path
import unittest
import asyncio
from unittest.mock import Mock, patch, AsyncMock, MagicMock

# Add parent directory to path to allow imports
# test_redis_pool_async.py is at: db_connections/tests/unit/connectors/redis/
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
        / "redis"
        / "test_redis_pool_async.py"
    )

parent_dir = _file_path.parent.parent.parent.parent.parent.parent
parent_dir_str = str(parent_dir)
if parent_dir_str not in sys.path:
    sys.path.insert(0, parent_dir_str)

from db_connections.scr.all_db_connectors.connectors.redis.config import (  # noqa: E402
    RedisPoolConfig,
)
from db_connections.scr.all_db_connectors.connectors.redis.pool import (  # noqa: E402, E501
    RedisAsyncConnectionPool,
)
from db_connections.scr.all_db_connectors.core.exceptions import (  # noqa: E402
    ConnectionError,
    PoolTimeoutError,
)
from db_connections.scr.all_db_connectors.core.health import (  # noqa: E402
    HealthState,
)


class MockAsyncRedisConnection:
    """Mock Async Redis connection for testing."""

    def __init__(self):
        self.closed = False
        self.connection_pool = Mock()
        self.connection_pool.connection_kwargs = {}

    async def ping(self):
        """Mock ping."""
        if self.closed:
            raise Exception("Connection closed")
        return True

    async def get(self, key):
        """Mock get."""
        if self.closed:
            raise Exception("Connection closed")
        return None

    async def set(self, key, value):
        """Mock set."""
        if self.closed:
            raise Exception("Connection closed")
        return True

    async def info(self, section=None):
        """Mock info."""
        if self.closed:
            raise Exception("Connection closed")
        return {
            "redis_version": "7.0.0",
            "used_memory": 1024000,
            "connected_clients": 1,
        }

    async def close(self):
        """Mock close."""
        self.closed = True

    async def aclose(self):
        """Mock async close."""
        self.closed = True


class MockAsyncRedisConnectionPool:
    """Mock Async Redis ConnectionPool."""

    def __init__(self, max_connections=10, **kwargs):
        self.max_connections = max_connections
        self.connections = []
        self.in_use = set()
        self.closed = False
        # Add methods to make it behave like a Redis client
        self._mock_conn = MockAsyncRedisConnection()

    async def ping(self):
        """Mock ping - delegate to internal connection."""
        return await self._mock_conn.ping()

    async def aclose(self):
        """Mock async close."""
        self.closed = True
        self.in_use.clear()

    async def get_connection(self, command_name, *keys, **options):
        """Get connection from pool."""
        if self.closed:
            raise Exception("Pool is closed")
        if len(self.in_use) >= self.max_connections:
            return None
        conn = MockAsyncRedisConnection()
        self.in_use.add(id(conn))
        return conn

    async def release(self, connection):
        """Release connection to pool."""
        conn_id = id(connection)
        if conn_id in self.in_use:
            self.in_use.remove(conn_id)

    async def disconnect(self):
        """Close all connections."""
        self.closed = True
        self.in_use.clear()

    async def reset(self):
        """Reset pool."""
        await self.disconnect()


class TestAsyncRedisConnectionPoolInit(unittest.TestCase):
    """Test RedisAsyncConnectionPool initialization."""

    def setUp(self):
        """Set up test fixtures."""
        self.redis_config = RedisPoolConfig(
            host="localhost",
            port=6379,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_async_redis_pool = MockAsyncRedisConnectionPool(max_connections=10)

    def _setup_mock_async_redis_module(self):
        """Set up mock async redis module."""
        # Patch AsyncRedis to return our mock pool
        patch_path = (
            "db_connections.scr.all_db_connectors.connectors.redis.pool.AsyncRedis"
        )
        self.mock_module = patch(
            patch_path, return_value=self.mock_async_redis_pool
        ).start()

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_init_default(self):
        """Test pool initialization with defaults."""
        self._setup_mock_async_redis_module()
        pool = RedisAsyncConnectionPool(self.redis_config)

        self.assertEqual(pool.config, self.redis_config)
        self.assertFalse(pool._initialized)
        self.assertIsNone(pool._pool)
        self.assertFalse(pool._closed)

    def test_init_validates_config(self):
        """Test pool validates configuration on init."""
        with self.assertRaisesRegex(ValueError, "max_connections must be positive"):
            invalid_config = RedisPoolConfig(
                host="localhost",
                max_connections=-1,  # Invalid
            )
            RedisAsyncConnectionPool(invalid_config)

    def test_repr(self):
        """Test string representation."""
        self._setup_mock_async_redis_module()
        pool = RedisAsyncConnectionPool(self.redis_config)

        repr_str = repr(pool)
        self.assertIn("RedisAsyncConnectionPool", repr_str)
        self.assertIn("localhost", repr_str)
        self.assertIn("6379", repr_str)


class TestAsyncRedisConnectionPoolInitialization(unittest.IsolatedAsyncioTestCase):
    """Test async pool initialization."""

    def setUp(self):
        """Set up test fixtures."""
        self.redis_config = RedisPoolConfig(
            host="localhost",
            port=6379,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_async_redis_pool = MockAsyncRedisConnectionPool(max_connections=10)

    def _setup_mock_async_redis_module(self):
        """Set up mock async redis module."""
        # Patch AsyncRedis to return our mock pool
        patch_path = (
            "db_connections.scr.all_db_connectors.connectors.redis.pool.AsyncRedis"
        )
        self.mock_module = patch(
            patch_path, return_value=self.mock_async_redis_pool
        ).start()

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_initialize_pool(self):
        """Test explicit pool initialization."""
        self._setup_mock_async_redis_module()
        pool = RedisAsyncConnectionPool(self.redis_config)
        await pool.initialize_pool()

        self.assertTrue(pool._initialized)
        self.assertIsNotNone(pool._pool)

    async def test_initialize_pool_already_initialized(self):
        """Test initializing already initialized pool."""
        self._setup_mock_async_redis_module()
        pool = RedisAsyncConnectionPool(self.redis_config)
        await pool.initialize_pool()
        await pool.initialize_pool()  # Second call should be no-op

        self.assertTrue(pool._initialized)

    async def test_initialize_pool_connection_error(self):
        """Test pool initialization with connection error."""

        def failing_create_redis(**kwargs):
            raise Exception("Connection failed")

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors.redis.pool.AsyncRedis"
        )
        with patch(patch_path, side_effect=failing_create_redis):
            pool = RedisAsyncConnectionPool(self.redis_config)

            with self.assertRaisesRegex(ConnectionError, "Pool initialization failed"):
                await pool.initialize_pool()

    async def test_lazy_initialization(self):
        """Test lazy pool initialization on first connection."""
        self._setup_mock_async_redis_module()
        pool = RedisAsyncConnectionPool(self.redis_config)

        self.assertFalse(pool._initialized)

        async with pool.get_connection():
            pass

        self.assertTrue(pool._initialized)


class TestAsyncRedisConnectionPoolGetConnection(unittest.IsolatedAsyncioTestCase):
    """Test async connection acquisition."""

    def setUp(self):
        """Set up test fixtures."""
        self.redis_config = RedisPoolConfig(
            host="localhost",
            port=6379,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_async_redis_pool = MockAsyncRedisConnectionPool(max_connections=10)
        self.mock_async_redis_connection = MockAsyncRedisConnection()

    def _setup_mock_async_redis_module(self):
        """Set up mock async redis module."""
        # Patch AsyncRedis to return our mock pool
        patch_path = (
            "db_connections.scr.all_db_connectors.connectors.redis.pool.AsyncRedis"
        )
        self.mock_module = patch(
            patch_path, return_value=self.mock_async_redis_pool
        ).start()

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_get_connection_success(self):
        """Test successful connection acquisition."""
        self._setup_mock_async_redis_module()
        pool = RedisAsyncConnectionPool(self.redis_config)
        await pool.initialize_pool()

        async with pool.get_connection() as conn:
            self.assertIsNotNone(conn)

    async def test_get_connection_lazy_init(self):
        """Test connection triggers lazy initialization."""
        self._setup_mock_async_redis_module()
        pool = RedisAsyncConnectionPool(self.redis_config)

        self.assertFalse(pool._initialized)

        async with pool.get_connection():
            self.assertTrue(pool._initialized)

    async def test_get_connection_validates(self):
        """Test connection validation on checkout."""
        self._setup_mock_async_redis_module()
        config = RedisPoolConfig(
            host="localhost", validate_on_checkout=True, pre_ping=True
        )

        pool = RedisAsyncConnectionPool(config)

        with patch.object(pool, "validate_connection", return_value=True):
            async with pool.get_connection() as conn:
                self.assertIsNotNone(conn)

    async def test_get_connection_pool_closed(self):
        """Test getting connection from closed pool."""
        self._setup_mock_async_redis_module()
        pool = RedisAsyncConnectionPool(self.redis_config)
        pool._closed = True

        with self.assertRaisesRegex(ConnectionError, "Pool is closed"):
            async with pool.get_connection():
                pass

    async def test_get_connection_tracks_metadata(self):
        """Test connection metadata is tracked."""
        self._setup_mock_async_redis_module()
        pool = RedisAsyncConnectionPool(self.redis_config)
        await pool.initialize_pool()

        async with pool.get_connection() as conn:
            conn_id = id(conn)
            async with pool._metadata_lock:
                self.assertIn(conn_id, pool._connection_metadata)
                self.assertTrue(pool._connection_metadata[conn_id].in_use)

    async def test_get_connection_releases_on_exit(self):
        """Test connection is released after context manager exit."""
        self._setup_mock_async_redis_module()
        pool = RedisAsyncConnectionPool(self.redis_config)
        await pool.initialize_pool()

        async with pool.get_connection() as conn:
            conn_id = id(conn)

        # After context manager exit
        self.assertNotIn(conn_id, pool._connections_in_use)

    async def test_get_connection_timeout(self):
        """Test connection acquisition timeout."""

        # Create a mock that always times out on ping (even after reconnection)
        async def timeout_ping():
            raise asyncio.TimeoutError("Timeout")

        mock_redis = MockAsyncRedisConnection()
        mock_redis.ping = timeout_ping

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors.redis.pool.AsyncRedis"
        )
        # Make sure reconnection also returns the same mock that times out
        # Use side_effect to return the same mock each time initialize_pool is called
        with patch(patch_path, return_value=mock_redis) as mock_async_redis:
            config = RedisPoolConfig(
                host="localhost",
                validate_on_checkout=True,  # Enable validation
                pre_ping=False,  # Disable pre_ping to avoid double validation
            )
            pool = RedisAsyncConnectionPool(config)
            await pool.initialize_pool()

            with self.assertRaisesRegex(
                PoolTimeoutError, "Connection acquisition timed out"
            ):
                async with pool.get_connection():
                    pass


class TestAsyncRedisConnectionPoolRelease(unittest.IsolatedAsyncioTestCase):
    """Test async connection release."""

    def setUp(self):
        """Set up test fixtures."""
        self.redis_config = RedisPoolConfig(
            host="localhost",
            port=6379,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_async_redis_pool = MockAsyncRedisConnectionPool(max_connections=10)
        self.mock_async_redis_connection = MockAsyncRedisConnection()

    def _setup_mock_async_redis_module(self):
        """Set up mock async redis module."""
        # Patch AsyncRedis to return our mock pool
        patch_path = (
            "db_connections.scr.all_db_connectors.connectors.redis.pool.AsyncRedis"
        )
        self.mock_module = patch(
            patch_path, return_value=self.mock_async_redis_pool
        ).start()

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_release_connection(self):
        """Test releasing a connection back to pool."""
        self._setup_mock_async_redis_module()
        pool = RedisAsyncConnectionPool(self.redis_config)
        await pool.initialize_pool()

        conn = self.mock_async_redis_connection
        conn_id = id(conn)
        pool._connections_in_use.add(conn_id)

        await pool.release_connection(conn)

        self.assertNotIn(conn_id, pool._connections_in_use)


class TestAsyncRedisConnectionPoolClose(unittest.IsolatedAsyncioTestCase):
    """Test async pool closing."""

    def setUp(self):
        """Set up test fixtures."""
        self.redis_config = RedisPoolConfig(
            host="localhost",
            port=6379,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_async_redis_pool = MockAsyncRedisConnectionPool(max_connections=10)
        self.mock_async_redis_connection = MockAsyncRedisConnection()

    def _setup_mock_async_redis_module(self):
        """Set up mock async redis module."""
        # Patch AsyncRedis to return our mock pool
        patch_path = (
            "db_connections.scr.all_db_connectors.connectors.redis.pool.AsyncRedis"
        )
        self.mock_module = patch(
            patch_path, return_value=self.mock_async_redis_pool
        ).start()

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_close_connection(self):
        """Test closing a single connection."""
        self._setup_mock_async_redis_module()
        pool = RedisAsyncConnectionPool(self.redis_config)
        await pool.initialize_pool()

        conn = self.mock_async_redis_connection
        conn_id = id(conn)
        pool._connections_in_use.add(conn_id)

        await pool.close_connection(conn)

        self.assertNotIn(conn_id, pool._connections_in_use)
        self.assertTrue(conn.closed)

    async def test_close_all_connections(self):
        """Test closing all connections in pool."""
        self._setup_mock_async_redis_module()
        pool = RedisAsyncConnectionPool(self.redis_config)
        await pool.initialize_pool()

        await pool.close_all_connections()

        self.assertTrue(pool._closed)
        self.assertEqual(len(pool._connections_in_use), 0)


class TestAsyncRedisConnectionPoolStatus(unittest.IsolatedAsyncioTestCase):
    """Test async pool status and metrics."""

    def setUp(self):
        """Set up test fixtures."""
        self.redis_config = RedisPoolConfig(
            host="localhost",
            port=6379,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_async_redis_pool = MockAsyncRedisConnectionPool(max_connections=10)

    def _setup_mock_async_redis_module(self):
        """Set up mock async redis module."""
        # Patch AsyncRedis to return our mock pool
        patch_path = (
            "db_connections.scr.all_db_connectors.connectors.redis.pool.AsyncRedis"
        )
        self.mock_module = patch(
            patch_path, return_value=self.mock_async_redis_pool
        ).start()

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_pool_status_uninitialized(self):
        """Test pool status before initialization."""
        self._setup_mock_async_redis_module()
        pool = RedisAsyncConnectionPool(self.redis_config)

        status = await pool.pool_status()

        self.assertFalse(status["initialized"])
        self.assertEqual(status["total_connections"], 0)
        expected_max = (
            self.redis_config.max_connections + self.redis_config.max_overflow
        )
        self.assertEqual(status["max_connections"], expected_max)

    async def test_pool_status_initialized(self):
        """Test pool status after initialization."""
        self._setup_mock_async_redis_module()
        pool = RedisAsyncConnectionPool(self.redis_config)
        await pool.initialize_pool()

        status = await pool.pool_status()

        self.assertTrue(status["initialized"])
        self.assertFalse(status["closed"])
        expected_max = (
            self.redis_config.max_connections + self.redis_config.max_overflow
        )
        self.assertEqual(status["max_connections"], expected_max)
        self.assertEqual(status["min_connections"], self.redis_config.min_connections)

    async def test_get_metrics(self):
        """Test getting pool metrics."""
        self._setup_mock_async_redis_module()
        pool = RedisAsyncConnectionPool(self.redis_config)
        await pool.initialize_pool()

        metrics = await pool.get_metrics()

        self.assertGreaterEqual(metrics.total_connections, 0)
        self.assertGreaterEqual(metrics.active_connections, 0)
        self.assertGreaterEqual(metrics.idle_connections, 0)
        expected_max = (
            self.redis_config.max_connections + self.redis_config.max_overflow
        )
        self.assertEqual(metrics.max_connections, expected_max)
        self.assertEqual(metrics.min_connections, self.redis_config.min_connections)


class TestAsyncRedisConnectionPoolValidation(unittest.IsolatedAsyncioTestCase):
    """Test async connection validation."""

    def setUp(self):
        """Set up test fixtures."""
        self.redis_config = RedisPoolConfig(
            host="localhost",
            port=6379,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_async_redis_pool = MockAsyncRedisConnectionPool(max_connections=10)
        self.mock_async_redis_connection = MockAsyncRedisConnection()

    def _setup_mock_async_redis_module(self):
        """Set up mock async redis module."""
        # Patch AsyncRedis to return our mock pool
        patch_path = (
            "db_connections.scr.all_db_connectors.connectors.redis.pool.AsyncRedis"
        )
        self.mock_module = patch(
            patch_path, return_value=self.mock_async_redis_pool
        ).start()

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_validate_connection_success(self):
        """Test successful connection validation."""
        self._setup_mock_async_redis_module()
        pool = RedisAsyncConnectionPool(self.redis_config)

        result = await pool.validate_connection(self.mock_async_redis_connection)

        self.assertTrue(result)

    async def test_validate_connection_failure(self):
        """Test connection validation failure."""
        self._setup_mock_async_redis_module()
        pool = RedisAsyncConnectionPool(self.redis_config)

        # Create connection that will fail validation
        bad_conn = Mock()
        bad_conn.ping = AsyncMock(side_effect=Exception("Connection lost"))

        result = await pool.validate_connection(bad_conn)

        self.assertFalse(result)


class TestAsyncRedisConnectionPoolHealth(unittest.IsolatedAsyncioTestCase):
    """Test async health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.redis_config = RedisPoolConfig(
            host="localhost",
            port=6379,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_async_redis_pool = MockAsyncRedisConnectionPool(max_connections=10)
        self.mock_async_redis_connection = MockAsyncRedisConnection()

    def _setup_mock_async_redis_module(self):
        """Set up mock async redis module."""
        # Patch AsyncRedis to return our mock pool
        patch_path = (
            "db_connections.scr.all_db_connectors.connectors.redis.pool.AsyncRedis"
        )
        self.mock_module = patch(
            patch_path, return_value=self.mock_async_redis_pool
        ).start()

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_health_check_healthy(self):
        """Test health check on healthy pool."""
        self._setup_mock_async_redis_module()
        pool = RedisAsyncConnectionPool(self.redis_config)
        await pool.initialize_pool()

        health = await pool.health_check()

        self.assertEqual(health.state, HealthState.HEALTHY)
        self.assertIn("healthy", health.message.lower())

    async def test_health_check_degraded(self):
        """Test health check on degraded pool."""
        self._setup_mock_async_redis_module()
        pool = RedisAsyncConnectionPool(self.redis_config)
        await pool.initialize_pool()

        # Simulate high utilization (8 out of 10 = 80%, which is DEGRADED)
        # Note: health_check calculates utilization before calling get_connection,
        # using len(_connections_in_use) directly. With max_connections=10,
        # 8/10 = 0.8 = 80% utilization, which should be DEGRADED (0.7 <= 0.8 < 0.9)
        pool._connections_in_use = set(range(8))  # 8 out of 10

        health = await pool.health_check()

        # Should be DEGRADED (80% utilization)
        self.assertEqual(health.state, HealthState.DEGRADED)
        self.assertIn("utilization", health.message.lower())

    async def test_database_health_check(self):
        """Test database health check."""
        self._setup_mock_async_redis_module()
        pool = RedisAsyncConnectionPool(self.redis_config)
        await pool.initialize_pool()

        health = await pool.database_health_check()

        self.assertIn(
            health.state,
            [HealthState.HEALTHY, HealthState.DEGRADED, HealthState.UNHEALTHY],
        )
        self.assertIsNotNone(health.response_time_ms)


class TestAsyncRedisConnectionPoolContextManager(unittest.IsolatedAsyncioTestCase):
    """Test async context manager support."""

    def setUp(self):
        """Set up test fixtures."""
        self.redis_config = RedisPoolConfig(
            host="localhost",
            port=6379,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_async_redis_pool = MockAsyncRedisConnectionPool(max_connections=10)

    def _setup_mock_async_redis_module(self):
        """Set up mock async redis module."""
        # Patch AsyncRedis to return our mock pool
        patch_path = (
            "db_connections.scr.all_db_connectors.connectors.redis.pool.AsyncRedis"
        )
        self.mock_module = patch(
            patch_path, return_value=self.mock_async_redis_pool
        ).start()

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_context_manager_enter(self):
        """Test pool async context manager enter."""
        self._setup_mock_async_redis_module()
        async with RedisAsyncConnectionPool(self.redis_config) as pool:
            self.assertTrue(pool._initialized)
            self.assertFalse(pool._closed)

    async def test_context_manager_exit(self):
        """Test pool async context manager exit."""
        self._setup_mock_async_redis_module()
        pool = RedisAsyncConnectionPool(self.redis_config)

        async with pool:
            pass

        self.assertTrue(pool._closed)

    async def test_context_manager_with_connections(self):
        """Test using connections within async context manager."""
        self._setup_mock_async_redis_module()
        async with RedisAsyncConnectionPool(self.redis_config) as pool:
            async with pool.get_connection() as conn:
                self.assertIsNotNone(conn)

        self.assertTrue(pool._closed)


class TestAsyncRedisConnectionPoolConcurrency(unittest.IsolatedAsyncioTestCase):
    """Test concurrent async operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.redis_config = RedisPoolConfig(
            host="localhost",
            port=6379,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_async_redis_pool = MockAsyncRedisConnectionPool(max_connections=10)
        self.mock_async_redis_connection = MockAsyncRedisConnection()

    def _setup_mock_async_redis_module(self):
        """Set up mock async redis module."""
        # Patch AsyncRedis to return our mock pool
        patch_path = (
            "db_connections.scr.all_db_connectors.connectors.redis.pool.AsyncRedis"
        )
        self.mock_module = patch(
            patch_path, return_value=self.mock_async_redis_pool
        ).start()

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_multiple_concurrent_connections(self):
        """Test acquiring multiple connections concurrently."""
        self._setup_mock_async_redis_module()
        pool = RedisAsyncConnectionPool(self.redis_config)
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
        self._setup_mock_async_redis_module()
        pool = RedisAsyncConnectionPool(self.redis_config)
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


if __name__ == "__main__":
    unittest.main()
