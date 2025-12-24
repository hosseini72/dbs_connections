"""Unit tests for Redis synchronous connection pool."""

import sys
from pathlib import Path
import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta

# Add parent directory to path to allow imports
# test_redis_pool.py is at: db_connections/tests/unit/connectors/redis/
# We need to go up 6 levels to get to the parent of db_connections
try:
    _file_path = Path(__file__).resolve()
except NameError:
    # __file__ might not be defined in some contexts
    import os
    _file_path = Path(os.getcwd()) / 'tests' / 'unit' / 'connectors' / 'redis' / 'test_redis_pool.py'

parent_dir = _file_path.parent.parent.parent.parent.parent.parent
parent_dir_str = str(parent_dir)
if parent_dir_str not in sys.path:
    sys.path.insert(0, parent_dir_str)

from db_connections.scr.all_db_connectors.connectors.redis.config import (  # noqa: E402, E501
    RedisPoolConfig
)
from db_connections.scr.all_db_connectors.connectors.redis.pool import (  # noqa: E402, E501
    RedisSyncConnectionPool
)
from db_connections.scr.all_db_connectors.core.exceptions import (  # noqa: E402
    ConnectionError,
    PoolExhaustedError,
)
from db_connections.scr.all_db_connectors.core.health import (  # noqa: E402
    HealthState
)


class MockRedisConnection:
    """Mock Redis connection for testing."""

    def __init__(self):
        self.closed = False
        self.connection_pool = Mock()
        self.connection_pool.connection_kwargs = {}

    def ping(self):
        """Mock ping."""
        if self.closed:
            raise Exception("Connection closed")
        return True

    def get(self, key):
        """Mock get."""
        if self.closed:
            raise Exception("Connection closed")
        return None

    def set(self, key, value):
        """Mock set."""
        if self.closed:
            raise Exception("Connection closed")
        return True

    def info(self, section=None):
        """Mock info."""
        if self.closed:
            raise Exception("Connection closed")
        return {
            "redis_version": "7.0.0",
            "used_memory": 1024000,
            "connected_clients": 1,
        }

    def close(self):
        """Mock close."""
        self.closed = True


class MockRedisConnectionPool:
    """Mock Redis ConnectionPool."""

    def __init__(self, max_connections=10, **kwargs):
        self.max_connections = max_connections
        self.connections = []
        self.in_use = set()
        self.closed = False

    def get_connection(self, command_name, *keys, **options):
        """Get connection from pool."""
        if self.closed:
            raise Exception("Pool is closed")
        if len(self.in_use) >= self.max_connections:
            return None
        conn = MockRedisConnection()
        self.in_use.add(id(conn))
        return conn

    def release(self, connection):
        """Release connection to pool."""
        conn_id = id(connection)
        if conn_id in self.in_use:
            self.in_use.remove(conn_id)

    def disconnect(self):
        """Close all connections."""
        self.closed = True
        self.in_use.clear()

    def reset(self):
        """Reset pool."""
        self.disconnect()


class TestRedisConnectionPoolInit(unittest.TestCase):
    """Test RedisSyncConnectionPool initialization."""

    def setUp(self):
        """Set up test fixtures."""
        self.redis_config = RedisPoolConfig(
            host="localhost",
            port=6379,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_redis_pool = MockRedisConnectionPool(max_connections=10)

    def _setup_mock_redis_module(self):
        """Set up mock redis module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.redis.pool.redis"
        ).start()
        self.mock_module.ConnectionPool.return_value = self.mock_redis_pool

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_init_default(self):
        """Test pool initialization with defaults."""
        self._setup_mock_redis_module()
        pool = RedisSyncConnectionPool(self.redis_config)

        self.assertEqual(pool.config, self.redis_config)
        self.assertFalse(pool._initialized)
        self.assertIsNone(pool._pool)
        self.assertFalse(pool._closed)

    def test_init_validates_config(self):
        """Test pool validates configuration on init."""
        with self.assertRaisesRegex(
            ValueError, "max_connections must be positive"
        ):
            invalid_config = RedisPoolConfig(
                host="localhost",
                max_connections=-1  # Invalid
            )
            # Config creation raises the error, but we test pool creation too
            RedisSyncConnectionPool(invalid_config)

    def test_repr(self):
        """Test string representation."""
        self._setup_mock_redis_module()
        pool = RedisSyncConnectionPool(self.redis_config)

        repr_str = repr(pool)
        self.assertIn("RedisSyncConnectionPool", repr_str)
        self.assertIn("localhost", repr_str)
        self.assertIn("6379", repr_str)


class TestRedisConnectionPoolInitialization(unittest.TestCase):
    """Test pool initialization methods."""

    def setUp(self):
        """Set up test fixtures."""
        self.redis_config = RedisPoolConfig(
            host="localhost",
            port=6379,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_redis_pool = MockRedisConnectionPool(max_connections=10)

    def _setup_mock_redis_module(self):
        """Set up mock redis module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.redis.pool.redis"
        ).start()
        self.mock_module.ConnectionPool.return_value = self.mock_redis_pool

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_initialize_pool(self):
        """Test explicit pool initialization."""
        self._setup_mock_redis_module()
        pool = RedisSyncConnectionPool(self.redis_config)
        pool.initialize_pool()

        self.assertTrue(pool._initialized)
        self.assertIsNotNone(pool._pool)
        self.mock_module.ConnectionPool.assert_called_once()

    def test_initialize_pool_already_initialized(self):
        """Test initializing already initialized pool."""
        self._setup_mock_redis_module()
        pool = RedisSyncConnectionPool(self.redis_config)
        pool.initialize_pool()
        pool.initialize_pool()  # Second call should be no-op

        # Should only be called once
        self.assertEqual(
            self.mock_module.ConnectionPool.call_count, 1
        )

    def test_initialize_pool_connection_error(self):
        """Test pool initialization with connection error."""
        with patch(
            "db_connections.scr.all_db_connectors.connectors.redis.pool.redis"
        ) as mock_module:
            mock_module.ConnectionPool.side_effect = Exception(
                "Connection failed"
            )

            pool = RedisSyncConnectionPool(self.redis_config)

            with self.assertRaisesRegex(
                ConnectionError, "Pool initialization failed"
            ):
                pool.initialize_pool()

    def test_lazy_initialization(self):
        """Test lazy pool initialization on first connection."""
        self._setup_mock_redis_module()
        pool = RedisSyncConnectionPool(self.redis_config)

        self.assertFalse(pool._initialized)

        with pool.get_connection():
            pass

        self.assertTrue(pool._initialized)


class TestRedisConnectionPoolGetConnection(unittest.TestCase):
    """Test connection acquisition."""

    def setUp(self):
        """Set up test fixtures."""
        self.redis_config = RedisPoolConfig(
            host="localhost",
            port=6379,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_redis_pool = MockRedisConnectionPool(max_connections=10)
        self.mock_redis_connection = MockRedisConnection()

    def _setup_mock_redis_module(self):
        """Set up mock redis module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.redis.pool.redis"
        ).start()
        self.mock_module.ConnectionPool.return_value = self.mock_redis_pool
        self.mock_module.Redis.return_value = self.mock_redis_connection

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_get_connection_success(self):
        """Test successful connection acquisition."""
        self._setup_mock_redis_module()
        pool = RedisSyncConnectionPool(self.redis_config)
        pool.initialize_pool()

        with pool.get_connection() as conn:
            self.assertIsNotNone(conn)

    def test_get_connection_lazy_init(self):
        """Test connection triggers lazy initialization."""
        self._setup_mock_redis_module()
        pool = RedisSyncConnectionPool(self.redis_config)

        self.assertFalse(pool._initialized)

        with pool.get_connection():
            self.assertTrue(pool._initialized)

    def test_get_connection_validates(self):
        """Test connection validation on checkout."""
        self._setup_mock_redis_module()
        config = RedisPoolConfig(
            host="localhost",
            validate_on_checkout=True,
            pre_ping=True
        )

        pool = RedisSyncConnectionPool(config)

        with patch.object(
            pool, "validate_connection", return_value=True
        ):
            with pool.get_connection() as conn:
                self.assertIsNotNone(conn)

    def test_get_connection_pool_closed(self):
        """Test getting connection from closed pool."""
        self._setup_mock_redis_module()
        pool = RedisSyncConnectionPool(self.redis_config)
        pool._closed = True

        with self.assertRaisesRegex(ConnectionError, "Pool is closed"):
            with pool.get_connection():
                pass

    def test_get_connection_tracks_metadata(self):
        """Test connection metadata is tracked."""
        self._setup_mock_redis_module()
        pool = RedisSyncConnectionPool(self.redis_config)
        pool.initialize_pool()

        with pool.get_connection() as conn:
            conn_id = id(conn)
            self.assertIn(conn_id, pool._connection_metadata)
            self.assertTrue(
                pool._connection_metadata[conn_id].in_use
            )

    def test_get_connection_releases_on_exit(self):
        """Test connection is released after context manager exit."""
        self._setup_mock_redis_module()
        pool = RedisSyncConnectionPool(self.redis_config)
        pool.initialize_pool()

        with pool.get_connection() as conn:
            conn_id = id(conn)

        # After context manager exit
        self.assertNotIn(conn_id, pool._connections_in_use)

    def test_get_connection_exhausted(self):
        """Test connection acquisition when pool is exhausted."""
        with patch(
            "db_connections.scr.all_db_connectors.connectors.redis.pool.redis"
        ) as mock_module:
            mock_pool = MagicMock()
            mock_pool.get_connection.return_value = None
            mock_module.ConnectionPool.return_value = mock_pool

            pool = RedisSyncConnectionPool(self.redis_config)
            pool.initialize_pool()

            with self.assertRaisesRegex(
                PoolExhaustedError, "No connections available"
            ):
                with pool.get_connection():
                    pass


class TestRedisConnectionPoolRelease(unittest.TestCase):
    """Test connection release."""

    def setUp(self):
        """Set up test fixtures."""
        self.redis_config = RedisPoolConfig(
            host="localhost",
            port=6379,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_redis_pool = MockRedisConnectionPool(max_connections=10)
        self.mock_redis_connection = MockRedisConnection()

    def _setup_mock_redis_module(self):
        """Set up mock redis module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.redis.pool.redis"
        ).start()
        self.mock_module.ConnectionPool.return_value = self.mock_redis_pool

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_release_connection(self):
        """Test releasing a connection back to pool."""
        self._setup_mock_redis_module()
        pool = RedisSyncConnectionPool(self.redis_config)
        pool.initialize_pool()

        conn = self.mock_redis_connection
        conn_id = id(conn)
        pool._connections_in_use.add(conn_id)

        pool.release_connection(conn)

        self.assertNotIn(conn_id, pool._connections_in_use)

    def test_release_connection_with_recycling(self):
        """Test connection recycling on release."""
        self._setup_mock_redis_module()
        config = RedisPoolConfig(
            host="localhost",
            recycle_on_return=True
        )

        pool = RedisSyncConnectionPool(config)
        pool.initialize_pool()

        conn = self.mock_redis_connection
        conn_id = id(conn)

        # Create metadata
        from db_connections.scr.all_db_connectors.core.utils import ConnectionMetadata
        pool._connection_metadata[conn_id] = ConnectionMetadata(
            created_at=datetime.now() - timedelta(hours=1)
        )

        pool.release_connection(conn)

        # Connection should be recycled
        self.assertNotIn(conn_id, pool._connection_metadata)


class TestRedisConnectionPoolClose(unittest.TestCase):
    """Test pool closing."""

    def setUp(self):
        """Set up test fixtures."""
        self.redis_config = RedisPoolConfig(
            host="localhost",
            port=6379,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_redis_pool = MockRedisConnectionPool(max_connections=10)
        self.mock_redis_connection = MockRedisConnection()

    def _setup_mock_redis_module(self):
        """Set up mock redis module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.redis.pool.redis"
        ).start()
        self.mock_module.ConnectionPool.return_value = self.mock_redis_pool

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_close_connection(self):
        """Test closing a single connection."""
        self._setup_mock_redis_module()
        pool = RedisSyncConnectionPool(self.redis_config)
        pool.initialize_pool()

        conn = self.mock_redis_connection
        conn_id = id(conn)
        pool._connections_in_use.add(conn_id)

        pool.close_connection(conn)

        self.assertNotIn(conn_id, pool._connections_in_use)
        self.assertTrue(conn.closed)

    def test_close_all_connections(self):
        """Test closing all connections in pool."""
        self._setup_mock_redis_module()
        pool = RedisSyncConnectionPool(self.redis_config)
        pool.initialize_pool()

        pool.close_all_connections()

        self.assertTrue(pool._closed)
        self.assertEqual(len(pool._connections_in_use), 0)
        self.assertEqual(len(pool._connection_metadata), 0)


class TestRedisConnectionPoolStatus(unittest.TestCase):
    """Test pool status and metrics."""

    def setUp(self):
        """Set up test fixtures."""
        self.redis_config = RedisPoolConfig(
            host="localhost",
            port=6379,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_redis_pool = MockRedisConnectionPool(max_connections=10)

    def _setup_mock_redis_module(self):
        """Set up mock redis module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.redis.pool.redis"
        ).start()
        self.mock_module.ConnectionPool.return_value = self.mock_redis_pool

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_pool_status_uninitialized(self):
        """Test pool status before initialization."""
        self._setup_mock_redis_module()
        pool = RedisSyncConnectionPool(self.redis_config)

        status = pool.pool_status()

        self.assertFalse(status["initialized"])
        self.assertEqual(status["total_connections"], 0)
        expected_max = (
            self.redis_config.max_connections +
            self.redis_config.max_overflow
        )
        self.assertEqual(status["max_connections"], expected_max)

    def test_pool_status_initialized(self):
        """Test pool status after initialization."""
        self._setup_mock_redis_module()
        pool = RedisSyncConnectionPool(self.redis_config)
        pool.initialize_pool()

        status = pool.pool_status()

        self.assertTrue(status["initialized"])
        self.assertFalse(status["closed"])
        expected_max = (
            self.redis_config.max_connections +
            self.redis_config.max_overflow
        )
        self.assertEqual(status["max_connections"], expected_max)
        self.assertEqual(
            status["min_connections"], self.redis_config.min_connections
        )

    def test_get_metrics(self):
        """Test getting pool metrics."""
        self._setup_mock_redis_module()
        pool = RedisSyncConnectionPool(self.redis_config)
        pool.initialize_pool()

        metrics = pool.get_metrics()

        self.assertGreaterEqual(metrics.total_connections, 0)
        self.assertGreaterEqual(metrics.active_connections, 0)
        self.assertGreaterEqual(metrics.idle_connections, 0)
        expected_max = (
            self.redis_config.max_connections +
            self.redis_config.max_overflow
        )
        self.assertEqual(metrics.max_connections, expected_max)
        self.assertEqual(
            metrics.min_connections, self.redis_config.min_connections
        )


class TestRedisConnectionPoolValidation(unittest.TestCase):
    """Test connection validation."""

    def setUp(self):
        """Set up test fixtures."""
        self.redis_config = RedisPoolConfig(
            host="localhost",
            port=6379,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_redis_pool = MockRedisConnectionPool(max_connections=10)
        self.mock_redis_connection = MockRedisConnection()

    def _setup_mock_redis_module(self):
        """Set up mock redis module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.redis.pool.redis"
        ).start()
        self.mock_module.ConnectionPool.return_value = self.mock_redis_pool

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_validate_connection_success(self):
        """Test successful connection validation."""
        self._setup_mock_redis_module()
        pool = RedisSyncConnectionPool(self.redis_config)

        result = pool.validate_connection(self.mock_redis_connection)

        self.assertTrue(result)

    def test_validate_connection_failure(self):
        """Test connection validation failure."""
        self._setup_mock_redis_module()
        pool = RedisSyncConnectionPool(self.redis_config)

        # Create connection that will fail validation
        bad_conn = Mock()
        bad_conn.ping.side_effect = Exception("Connection lost")

        result = pool.validate_connection(bad_conn)

        self.assertFalse(result)


class TestRedisConnectionPoolHealth(unittest.TestCase):
    """Test health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.redis_config = RedisPoolConfig(
            host="localhost",
            port=6379,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_redis_pool = MockRedisConnectionPool(max_connections=10)
        self.mock_redis_connection = MockRedisConnection()

    def _setup_mock_redis_module(self):
        """Set up mock redis module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.redis.pool.redis"
        ).start()
        self.mock_module.ConnectionPool.return_value = self.mock_redis_pool

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_health_check_healthy(self):
        """Test health check on healthy pool."""
        self._setup_mock_redis_module()
        pool = RedisSyncConnectionPool(self.redis_config)
        pool.initialize_pool()

        health = pool.health_check()

        self.assertEqual(health.state, HealthState.HEALTHY)
        self.assertIn("healthy", health.message.lower())

    def test_database_health_check(self):
        """Test database health check."""
        self._setup_mock_redis_module()
        pool = RedisSyncConnectionPool(self.redis_config)
        pool.initialize_pool()

        with patch.object(pool, "get_connection"):
            health = pool.database_health_check()

            self.assertIn(
                health.state,
                [
                    HealthState.HEALTHY,
                    HealthState.DEGRADED,
                    HealthState.UNHEALTHY
                ]
            )


class TestRedisConnectionPoolContextManager(unittest.TestCase):
    """Test context manager support."""

    def setUp(self):
        """Set up test fixtures."""
        self.redis_config = RedisPoolConfig(
            host="localhost",
            port=6379,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_redis_pool = MockRedisConnectionPool(max_connections=10)

    def _setup_mock_redis_module(self):
        """Set up mock redis module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.redis.pool.redis"
        ).start()
        self.mock_module.ConnectionPool.return_value = self.mock_redis_pool

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_context_manager_enter(self):
        """Test pool context manager enter."""
        self._setup_mock_redis_module()
        with RedisSyncConnectionPool(self.redis_config) as pool:
            self.assertTrue(pool._initialized)
            self.assertFalse(pool._closed)

    def test_context_manager_exit(self):
        """Test pool context manager exit."""
        self._setup_mock_redis_module()
        pool = RedisSyncConnectionPool(self.redis_config)

        with pool:
            pass

        self.assertTrue(pool._closed)

    def test_context_manager_with_connections(self):
        """Test using connections within context manager."""
        self._setup_mock_redis_module()
        with RedisSyncConnectionPool(self.redis_config) as pool:
            with pool.get_connection() as conn:
                self.assertIsNotNone(conn)

        self.assertTrue(pool._closed)


class TestRedisConnectionPoolRetry(unittest.TestCase):
    """Test retry logic."""

    def setUp(self):
        """Set up test fixtures."""
        self.redis_config = RedisPoolConfig(
            host="localhost",
            port=6379,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )

    def test_retry_on_connection_failure(self):
        """Test retry logic on connection failure."""
        with patch(
            "db_connections.scr.all_db_connectors.connectors.redis.pool.redis"
        ) as mock_module:
            mock_pool = MagicMock()

            # Fail twice, succeed on third attempt
            import redis
            mock_pool.get_connection.side_effect = [
                redis.ConnectionError("Connection failed"),
                redis.ConnectionError("Connection failed"),
                Mock(),  # Success
            ]

            mock_module.ConnectionPool.return_value = mock_pool

            config = RedisPoolConfig(
                host="localhost",
                max_retries=3,
                retry_delay=0.1
            )

            pool = RedisSyncConnectionPool(config)
            pool.initialize_pool()

            # Should succeed after retries
            with pool.get_connection() as conn:
                self.assertIsNotNone(conn)

    def test_retry_exhausted(self):
        """Test when all retry attempts are exhausted."""
        with patch(
            "db_connections.scr.all_db_connectors.connectors.redis.pool.redis"
        ) as mock_module:
            mock_pool = MagicMock()

            import redis
            mock_pool.get_connection.side_effect = redis.ConnectionError(
                "Connection failed"
            )

            mock_module.ConnectionPool.return_value = mock_pool

            config = RedisPoolConfig(
                host="localhost",
                max_retries=2,
                retry_delay=0.1
            )

            pool = RedisSyncConnectionPool(config)
            pool.initialize_pool()

            with self.assertRaisesRegex(
                ConnectionError, "Connection acquisition failed"
            ):
                with pool.get_connection():
                    pass


class TestRedisConnectionPoolConcurrency(unittest.TestCase):
    """Test concurrent operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.redis_config = RedisPoolConfig(
            host="localhost",
            port=6379,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_redis_pool = MockRedisConnectionPool(max_connections=10)
        self.mock_redis_connection = MockRedisConnection()

    def _setup_mock_redis_module(self):
        """Set up mock redis module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.redis.pool.redis"
        ).start()
        self.mock_module.ConnectionPool.return_value = self.mock_redis_pool

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_multiple_connections(self):
        """Test acquiring multiple connections."""
        self._setup_mock_redis_module()
        pool = RedisSyncConnectionPool(self.redis_config)
        pool.initialize_pool()

        connections = []

        # Acquire multiple connections
        for _ in range(3):
            with pool.get_connection() as conn:
                connections.append(conn)

        # All should be different
        self.assertEqual(len(set(id(c) for c in connections)), 3)

    def test_connection_reuse(self):
        """Test connections are reused after release."""
        self._setup_mock_redis_module()
        pool = RedisSyncConnectionPool(self.redis_config)
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


if __name__ == '__main__':
    unittest.main()

