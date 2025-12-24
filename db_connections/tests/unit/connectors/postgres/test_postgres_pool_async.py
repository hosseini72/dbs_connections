"""Unit tests for PostgreSQL asynchronous connection pool."""

import sys
from pathlib import Path
import unittest
import asyncio
from unittest.mock import Mock, patch, AsyncMock, MagicMock

# Add parent directory to path to allow imports
# test_postgres_pool_async.py is at: db_connections/tests/unit/connectors/postgres/
# We need to go up 6 levels to get to the parent of db_connections
try:
    _file_path = Path(__file__).resolve()
except NameError:
    # __file__ might not be defined in some contexts
    import os
    _file_path = Path(os.getcwd()) / 'tests' / 'unit' / 'connectors' / 'postgres' / 'test_postgres_pool_async.py'

parent_dir = _file_path.parent.parent.parent.parent.parent.parent
parent_dir_str = str(parent_dir)
if parent_dir_str not in sys.path:
    sys.path.insert(0, parent_dir_str)

from db_connections.scr.all_db_connectors.connectors.postgres.config import (  # noqa: E402
    PostgresPoolConfig
)
from db_connections.scr.all_db_connectors.connectors.postgres.pool_async import (  # noqa: E402, E501
    AsyncPostgresConnectionPool
)
from db_connections.scr.all_db_connectors.core.exceptions import (  # noqa: E402
    ConnectionError,
    PoolTimeoutError,
)
from db_connections.scr.all_db_connectors.core.health import (  # noqa: E402
    HealthState
)


class MockAsyncpgTransaction:
    """Mock asyncpg transaction."""
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False


class MockAsyncpgConnection:
    """Mock asyncpg connection for testing."""
    
    def __init__(self):
        self.closed = False
    
    async def fetch(self, query, *args):
        """Mock fetch."""
        return [{"id": 1, "name": "test"}]
    
    async def fetchval(self, query, *args):
        """Mock fetchval."""
        if "SELECT 1" in query:
            return 1
        elif "version" in query.lower():
            return "PostgreSQL 15.0"
        elif "count" in query.lower():
            return 5
        return "test_value"
    
    async def fetchrow(self, query, *args):
        """Mock fetchrow."""
        return {"id": 1, "name": "test"}
    
    async def execute(self, query, *args):
        """Mock execute."""
        return "INSERT 0 1"
    
    async def executemany(self, query, args):
        """Mock executemany."""
        pass
    
    async def close(self):
        """Mock close."""
        self.closed = True
    
    def transaction(self):
        """Mock transaction context manager."""
        return MockAsyncpgTransaction()


class MockTransactionContext:
    """Mock transaction context."""

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False


class MockAsyncpgPool:
    """Mock asyncpg Pool."""

    def __init__(self, min_size, max_size):
        self.min_size = min_size
        self.max_size = max_size
        self._size = min_size
        self._idle_size = min_size
        self.closed = False

    async def acquire(self, timeout=None):
        """Acquire connection from pool."""
        return MockAsyncpgConnection()

    async def release(self, connection, timeout=None):
        """Release connection to pool."""
        pass

    async def close(self):
        """Close pool."""
        self.closed = True

    def get_size(self):
        """Get pool size."""
        return self._size

    def get_idle_size(self):
        """Get idle connections."""
        return self._idle_size


class TestAsyncPostgresConnectionPoolInit(unittest.TestCase):
    """Test AsyncPostgresConnectionPool initialization."""

    def setUp(self):
        """Set up test fixtures."""
        self.postgres_config = PostgresPoolConfig(
            host="localhost",
            port=5432,
            database="test_db",
            user="test_user",
            password="test_pass",
            min_size=2,
            max_size=10,
            timeout=30,
        )
        self.mock_asyncpg_pool = MockAsyncpgPool(
            min_size=2, max_size=10
        )

    def _setup_mock_asyncpg_module(self):
        """Set up mock asyncpg module."""
        async def create_pool_mock(**kwargs):
            return self.mock_asyncpg_pool

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors."
            "postgres.pool_async.asyncpg"
        )
        self.mock_module = patch(patch_path).start()
        self.mock_module.create_pool = create_pool_mock

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_init_default(self):
        """Test pool initialization with defaults."""
        self._setup_mock_asyncpg_module()
        pool = AsyncPostgresConnectionPool(self.postgres_config)

        self.assertEqual(pool.config, self.postgres_config)
        self.assertFalse(pool._initialized)
        self.assertIsNone(pool._pool)
        self.assertFalse(pool._closed)

    def test_init_validates_config(self):
        """Test pool validates configuration on init."""
        invalid_config = PostgresPoolConfig(
            host="localhost",
            database="test_db",
            user="test_user",
            max_size=-1  # Invalid
        )

        with self.assertRaisesRegex(
            ValueError, "max_size must be positive"
        ):
            AsyncPostgresConnectionPool(invalid_config)

    def test_repr(self):
        """Test string representation."""
        self._setup_mock_asyncpg_module()
        pool = AsyncPostgresConnectionPool(self.postgres_config)

        repr_str = repr(pool)
        self.assertIn("AsyncPostgresConnectionPool", repr_str)
        self.assertIn("localhost", repr_str)
        self.assertIn("test_db", repr_str)


class TestAsyncPostgresConnectionPoolInitialization(
    unittest.IsolatedAsyncioTestCase
):
    """Test async pool initialization."""

    def setUp(self):
        """Set up test fixtures."""
        self.postgres_config = PostgresPoolConfig(
            host="localhost",
            port=5432,
            database="test_db",
            user="test_user",
            password="test_pass",
            min_size=2,
            max_size=10,
            timeout=30,
        )
        self.mock_asyncpg_pool = MockAsyncpgPool(
            min_size=2, max_size=10
        )

    def _setup_mock_asyncpg_module(self):
        """Set up mock asyncpg module."""
        async def create_pool_mock(**kwargs):
            return self.mock_asyncpg_pool

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors."
            "postgres.pool_async.asyncpg"
        )
        self.mock_module = patch(patch_path).start()
        self.mock_module.create_pool = create_pool_mock

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_initialize_pool(self):
        """Test explicit pool initialization."""
        self._setup_mock_asyncpg_module()
        pool = AsyncPostgresConnectionPool(self.postgres_config)
        await pool.initialize_pool()

        self.assertTrue(pool._initialized)
        self.assertIsNotNone(pool._pool)

    async def test_initialize_pool_already_initialized(self):
        """Test initializing already initialized pool."""
        self._setup_mock_asyncpg_module()
        pool = AsyncPostgresConnectionPool(self.postgres_config)
        await pool.initialize_pool()
        await pool.initialize_pool()  # Second call should be no-op

        self.assertTrue(pool._initialized)

    async def test_initialize_pool_connection_error(self):
        """Test pool initialization with connection error."""
        async def failing_create_pool(**kwargs):
            raise Exception("Connection failed")

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors."
            "postgres.pool_async.asyncpg"
        )
        with patch(patch_path) as mock_module:
            mock_module.create_pool = failing_create_pool

            pool = AsyncPostgresConnectionPool(self.postgres_config)

            with self.assertRaisesRegex(
                ConnectionError, "Pool initialization failed"
            ):
                await pool.initialize_pool()

    async def test_lazy_initialization(self):
        """Test lazy pool initialization on first connection."""
        self._setup_mock_asyncpg_module()
        pool = AsyncPostgresConnectionPool(self.postgres_config)

        self.assertFalse(pool._initialized)

        async with pool.get_connection():
            pass

        self.assertTrue(pool._initialized)


class TestAsyncPostgresConnectionPoolGetConnection(
    unittest.IsolatedAsyncioTestCase
):
    """Test async connection acquisition."""

    def setUp(self):
        """Set up test fixtures."""
        self.postgres_config = PostgresPoolConfig(
            host="localhost",
            port=5432,
            database="test_db",
            user="test_user",
            password="test_pass",
            min_size=2,
            max_size=10,
            timeout=30,
        )
        self.mock_asyncpg_pool = MockAsyncpgPool(
            min_size=2, max_size=10
        )
        self.mock_asyncpg_connection = MockAsyncpgConnection()

    def _setup_mock_asyncpg_module(self):
        """Set up mock asyncpg module."""
        async def create_pool_mock(**kwargs):
            return self.mock_asyncpg_pool

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors."
            "postgres.pool_async.asyncpg"
        )
        self.mock_module = patch(patch_path).start()
        self.mock_module.create_pool = create_pool_mock

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_get_connection_success(self):
        """Test successful connection acquisition."""
        self._setup_mock_asyncpg_module()
        pool = AsyncPostgresConnectionPool(self.postgres_config)
        await pool.initialize_pool()

        async with pool.get_connection() as conn:
            self.assertIsNotNone(conn)

    async def test_get_connection_lazy_init(self):
        """Test connection triggers lazy initialization."""
        self._setup_mock_asyncpg_module()
        pool = AsyncPostgresConnectionPool(self.postgres_config)

        self.assertFalse(pool._initialized)

        async with pool.get_connection():
            self.assertTrue(pool._initialized)

    async def test_get_connection_validates(self):
        """Test connection validation on checkout."""
        self._setup_mock_asyncpg_module()
        config = PostgresPoolConfig(
            host="localhost",
            database="test_db",
            user="test_user",
            validate_on_checkout=True,
            pre_ping=True
        )

        pool = AsyncPostgresConnectionPool(config)

        with patch.object(
            pool, "validate_connection", return_value=True
        ):
            async with pool.get_connection() as conn:
                self.assertIsNotNone(conn)

    async def test_get_connection_pool_closed(self):
        """Test getting connection from closed pool."""
        self._setup_mock_asyncpg_module()
        pool = AsyncPostgresConnectionPool(self.postgres_config)
        pool._closed = True

        with self.assertRaisesRegex(ConnectionError, "Pool is closed"):
            async with pool.get_connection():
                pass

    async def test_get_connection_tracks_metadata(self):
        """Test connection metadata is tracked."""
        self._setup_mock_asyncpg_module()
        pool = AsyncPostgresConnectionPool(self.postgres_config)
        await pool.initialize_pool()

        async with pool.get_connection() as conn:
            conn_id = id(conn)
            async with pool._get_metadata_lock():
                self.assertIn(conn_id, pool._connection_metadata)
                self.assertTrue(
                    pool._connection_metadata[conn_id].in_use
                )

    async def test_get_connection_releases_on_exit(self):
        """Test connection is released after context manager exit."""
        self._setup_mock_asyncpg_module()
        pool = AsyncPostgresConnectionPool(self.postgres_config)
        await pool.initialize_pool()

        async with pool.get_connection() as conn:
            conn_id = id(conn)

        # After context manager exit
        self.assertNotIn(conn_id, pool._connections_in_use)

    async def test_get_connection_timeout(self):
        """Test connection acquisition timeout."""
        async def timeout_acquire(timeout=None):
            raise asyncio.TimeoutError("Timeout")

        mock_pool = MagicMock()
        mock_pool.acquire = timeout_acquire

        async def create_pool_mock(**kwargs):
            return mock_pool

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors."
            "postgres.pool_async.asyncpg"
        )
        with patch(patch_path) as mock_module:
            mock_module.create_pool = create_pool_mock

            pool = AsyncPostgresConnectionPool(self.postgres_config)
            await pool.initialize_pool()

            with self.assertRaisesRegex(
                PoolTimeoutError, "Connection acquisition timed out"
            ):
                async with pool.get_connection():
                    pass


class TestAsyncPostgresConnectionPoolRelease(
    unittest.IsolatedAsyncioTestCase
):
    """Test async connection release."""

    def setUp(self):
        """Set up test fixtures."""
        self.postgres_config = PostgresPoolConfig(
            host="localhost",
            port=5432,
            database="test_db",
            user="test_user",
            password="test_pass",
            min_size=2,
            max_size=10,
            timeout=30,
        )
        self.mock_asyncpg_pool = MockAsyncpgPool(
            min_size=2, max_size=10
        )
        self.mock_asyncpg_connection = MockAsyncpgConnection()

    def _setup_mock_asyncpg_module(self):
        """Set up mock asyncpg module."""
        async def create_pool_mock(**kwargs):
            return self.mock_asyncpg_pool

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors."
            "postgres.pool_async.asyncpg"
        )
        self.mock_module = patch(patch_path).start()
        self.mock_module.create_pool = create_pool_mock

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_release_connection(self):
        """Test releasing a connection back to pool."""
        self._setup_mock_asyncpg_module()
        pool = AsyncPostgresConnectionPool(self.postgres_config)
        await pool.initialize_pool()

        conn = self.mock_asyncpg_connection
        conn_id = id(conn)
        pool._connections_in_use.add(conn_id)

        await pool.release_connection(conn)

        self.assertNotIn(conn_id, pool._connections_in_use)


class TestAsyncPostgresConnectionPoolClose(
    unittest.IsolatedAsyncioTestCase
):
    """Test async pool closing."""

    def setUp(self):
        """Set up test fixtures."""
        self.postgres_config = PostgresPoolConfig(
            host="localhost",
            port=5432,
            database="test_db",
            user="test_user",
            password="test_pass",
            min_size=2,
            max_size=10,
            timeout=30,
        )
        self.mock_asyncpg_pool = MockAsyncpgPool(
            min_size=2, max_size=10
        )
        self.mock_asyncpg_connection = MockAsyncpgConnection()

    def _setup_mock_asyncpg_module(self):
        """Set up mock asyncpg module."""
        async def create_pool_mock(**kwargs):
            return self.mock_asyncpg_pool

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors."
            "postgres.pool_async.asyncpg"
        )
        self.mock_module = patch(patch_path).start()
        self.mock_module.create_pool = create_pool_mock

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_close_connection(self):
        """Test closing a single connection."""
        self._setup_mock_asyncpg_module()
        pool = AsyncPostgresConnectionPool(self.postgres_config)
        await pool.initialize_pool()

        conn = self.mock_asyncpg_connection
        conn_id = id(conn)
        pool._connections_in_use.add(conn_id)
        
        # Ensure pool.release is awaitable
        pool._pool.release = AsyncMock()

        await pool.close_connection(conn)

        self.assertNotIn(conn_id, pool._connections_in_use)
        # The close() should have been called and set closed = True
        self.assertTrue(conn.closed, "Connection should be marked as closed")

    async def test_close_all_connections(self):
        """Test closing all connections in pool."""
        self._setup_mock_asyncpg_module()
        pool = AsyncPostgresConnectionPool(self.postgres_config)
        await pool.initialize_pool()

        await pool.close_all_connections()

        self.assertTrue(pool._closed)
        self.assertEqual(len(pool._connections_in_use), 0)


class TestAsyncPostgresConnectionPoolStatus(
    unittest.IsolatedAsyncioTestCase
):
    """Test async pool status and metrics."""

    def setUp(self):
        """Set up test fixtures."""
        self.postgres_config = PostgresPoolConfig(
            host="localhost",
            port=5432,
            database="test_db",
            user="test_user",
            password="test_pass",
            min_size=2,
            max_size=10,
            timeout=30,
        )
        self.mock_asyncpg_pool = MockAsyncpgPool(
            min_size=2, max_size=10
        )

    def _setup_mock_asyncpg_module(self):
        """Set up mock asyncpg module."""
        async def create_pool_mock(**kwargs):
            return self.mock_asyncpg_pool

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors."
            "postgres.pool_async.asyncpg"
        )
        self.mock_module = patch(patch_path).start()
        self.mock_module.create_pool = create_pool_mock

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_pool_status_uninitialized(self):
        """Test pool status before initialization."""
        self._setup_mock_asyncpg_module()
        pool = AsyncPostgresConnectionPool(self.postgres_config)

        status = await pool.pool_status()

        self.assertFalse(status["initialized"])
        self.assertEqual(status["total_connections"], 0)
        expected_max = (
            self.postgres_config.max_size +
            self.postgres_config.max_overflow
        )
        self.assertEqual(status["max_connections"], expected_max)

    async def test_pool_status_initialized(self):
        """Test pool status after initialization."""
        self._setup_mock_asyncpg_module()
        pool = AsyncPostgresConnectionPool(self.postgres_config)
        await pool.initialize_pool()

        status = await pool.pool_status()

        self.assertTrue(status["initialized"])
        self.assertFalse(status["closed"])
        expected_max = (
            self.postgres_config.max_size +
            self.postgres_config.max_overflow
        )
        self.assertEqual(status["max_connections"], expected_max)
        self.assertEqual(
            status["min_connections"], self.postgres_config.min_size
        )

    async def test_get_metrics(self):
        """Test getting pool metrics."""
        self._setup_mock_asyncpg_module()
        pool = AsyncPostgresConnectionPool(self.postgres_config)
        await pool.initialize_pool()

        metrics = await pool.get_metrics()

        self.assertGreaterEqual(metrics.total_connections, 0)
        self.assertGreaterEqual(metrics.active_connections, 0)
        self.assertGreaterEqual(metrics.idle_connections, 0)
        expected_max = (
            self.postgres_config.max_size +
            self.postgres_config.max_overflow
        )
        self.assertEqual(metrics.max_connections, expected_max)
        self.assertEqual(
            metrics.min_connections, self.postgres_config.min_size
        )


class TestAsyncPostgresConnectionPoolValidation(
    unittest.IsolatedAsyncioTestCase
):
    """Test async connection validation."""

    def setUp(self):
        """Set up test fixtures."""
        self.postgres_config = PostgresPoolConfig(
            host="localhost",
            port=5432,
            database="test_db",
            user="test_user",
            password="test_pass",
            min_size=2,
            max_size=10,
            timeout=30,
        )
        self.mock_asyncpg_pool = MockAsyncpgPool(
            min_size=2, max_size=10
        )
        self.mock_asyncpg_connection = MockAsyncpgConnection()

    def _setup_mock_asyncpg_module(self):
        """Set up mock asyncpg module."""
        async def create_pool_mock(**kwargs):
            return self.mock_asyncpg_pool

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors."
            "postgres.pool_async.asyncpg"
        )
        self.mock_module = patch(patch_path).start()
        self.mock_module.create_pool = create_pool_mock

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_validate_connection_success(self):
        """Test successful connection validation."""
        self._setup_mock_asyncpg_module()
        pool = AsyncPostgresConnectionPool(self.postgres_config)

        result = await pool.validate_connection(
            self.mock_asyncpg_connection
        )

        self.assertTrue(result)

    async def test_validate_connection_failure(self):
        """Test connection validation failure."""
        self._setup_mock_asyncpg_module()
        pool = AsyncPostgresConnectionPool(self.postgres_config)

        # Create connection that will fail validation
        bad_conn = Mock()
        bad_conn.fetchval = AsyncMock(
            side_effect=Exception("Connection lost")
        )

        result = await pool.validate_connection(bad_conn)

        self.assertFalse(result)


class TestAsyncPostgresConnectionPoolHealth(
    unittest.IsolatedAsyncioTestCase
):
    """Test async health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.postgres_config = PostgresPoolConfig(
            host="localhost",
            port=5432,
            database="test_db",
            user="test_user",
            password="test_pass",
            min_size=2,
            max_size=10,
            timeout=30,
        )
        self.mock_asyncpg_pool = MockAsyncpgPool(
            min_size=2, max_size=10
        )
        self.mock_asyncpg_connection = MockAsyncpgConnection()

    def _setup_mock_asyncpg_module(self):
        """Set up mock asyncpg module."""
        async def create_pool_mock(**kwargs):
            return self.mock_asyncpg_pool

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors."
            "postgres.pool_async.asyncpg"
        )
        self.mock_module = patch(patch_path).start()
        self.mock_module.create_pool = create_pool_mock

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_health_check_healthy(self):
        """Test health check on healthy pool."""
        self._setup_mock_asyncpg_module()
        pool = AsyncPostgresConnectionPool(self.postgres_config)
        await pool.initialize_pool()

        health = await pool.health_check()

        self.assertEqual(health.state, HealthState.HEALTHY)
        self.assertIn("healthy", health.message.lower())

    async def test_health_check_degraded(self):
        """Test health check on degraded pool."""
        self._setup_mock_asyncpg_module()
        pool = AsyncPostgresConnectionPool(self.postgres_config)
        await pool.initialize_pool()

        # Simulate high utilization (12 out of 15 = 80%)
        pool._connections_in_use = set(range(12))

        health = await pool.health_check()

        self.assertIn(
            health.state, [HealthState.DEGRADED, HealthState.UNHEALTHY]
        )

    async def test_database_health_check(self):
        """Test database health check."""
        self._setup_mock_asyncpg_module()
        pool = AsyncPostgresConnectionPool(self.postgres_config)
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


class TestAsyncPostgresConnectionPoolContextManager(
    unittest.IsolatedAsyncioTestCase
):
    """Test async context manager support."""

    def setUp(self):
        """Set up test fixtures."""
        self.postgres_config = PostgresPoolConfig(
            host="localhost",
            port=5432,
            database="test_db",
            user="test_user",
            password="test_pass",
            min_size=2,
            max_size=10,
            timeout=30,
        )
        self.mock_asyncpg_pool = MockAsyncpgPool(
            min_size=2, max_size=10
        )

    def _setup_mock_asyncpg_module(self):
        """Set up mock asyncpg module."""
        async def create_pool_mock(**kwargs):
            return self.mock_asyncpg_pool

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors."
            "postgres.pool_async.asyncpg"
        )
        self.mock_module = patch(patch_path).start()
        self.mock_module.create_pool = create_pool_mock

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_context_manager_enter(self):
        """Test pool async context manager enter."""
        self._setup_mock_asyncpg_module()
        async with AsyncPostgresConnectionPool(
            self.postgres_config
        ) as pool:
            self.assertTrue(pool._initialized)
            self.assertFalse(pool._closed)

    async def test_context_manager_exit(self):
        """Test pool async context manager exit."""
        self._setup_mock_asyncpg_module()
        pool = AsyncPostgresConnectionPool(self.postgres_config)

        async with pool:
            pass

        self.assertTrue(pool._closed)

    async def test_context_manager_with_connections(self):
        """Test using connections within async context manager."""
        self._setup_mock_asyncpg_module()
        async with AsyncPostgresConnectionPool(
            self.postgres_config
        ) as pool:
            async with pool.get_connection() as conn:
                self.assertIsNotNone(conn)

        self.assertTrue(pool._closed)


class TestAsyncPostgresConnectionPoolConcurrency(
    unittest.IsolatedAsyncioTestCase
):
    """Test concurrent async operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.postgres_config = PostgresPoolConfig(
            host="localhost",
            port=5432,
            database="test_db",
            user="test_user",
            password="test_pass",
            min_size=2,
            max_size=10,
            timeout=30,
        )
        self.mock_asyncpg_pool = MockAsyncpgPool(
            min_size=2, max_size=10
        )
        self.mock_asyncpg_connection = MockAsyncpgConnection()

    def _setup_mock_asyncpg_module(self):
        """Set up mock asyncpg module."""
        async def create_pool_mock(**kwargs):
            return self.mock_asyncpg_pool

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors."
            "postgres.pool_async.asyncpg"
        )
        self.mock_module = patch(patch_path).start()
        self.mock_module.create_pool = create_pool_mock

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_multiple_concurrent_connections(self):
        """Test acquiring multiple connections concurrently."""
        self._setup_mock_asyncpg_module()
        pool = AsyncPostgresConnectionPool(self.postgres_config)
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
        self._setup_mock_asyncpg_module()
        pool = AsyncPostgresConnectionPool(self.postgres_config)
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


class TestAsyncPostgresConnectionPoolRetry(
    unittest.IsolatedAsyncioTestCase
):
    """Test async retry logic."""

    def setUp(self):
        """Set up test fixtures."""
        self.postgres_config = PostgresPoolConfig(
            host="localhost",
            port=5432,
            database="test_db",
            user="test_user",
            password="test_pass",
            min_size=2,
            max_size=10,
            timeout=30,
        )

    async def test_retry_on_connection_failure(self):
        """Test retry logic on connection failure."""
        attempt_count = 0
        
        async def acquire_with_retries(timeout=None):
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise Exception("Connection failed")
            return MockAsyncpgConnection()
        
        mock_pool = MagicMock()
        mock_pool.acquire = AsyncMock(side_effect=acquire_with_retries)
        mock_pool.release = AsyncMock()  # Make release awaitable
        
        async def create_pool_mock(**kwargs):
            return mock_pool
        
        with patch("db_connections.scr.all_db_connectors.connectors.postgres.pool_async.asyncpg") as mock_module:
            mock_module.create_pool = create_pool_mock
            
            config = PostgresPoolConfig(
                host="localhost",
                database="test_db",
                user="test_user",
                max_retries=3,
                retry_delay=0.1,
                pre_ping=False,
                validate_on_checkout=False
            )
            
            pool = AsyncPostgresConnectionPool(config)
            await pool.initialize_pool()
            
            # Should succeed after retries
            async with pool.get_connection() as conn:
                self.assertIsNotNone(conn)
            
            self.assertEqual(attempt_count, 3)


class TestAsyncPostgresConnectionPoolTransactions(
    unittest.IsolatedAsyncioTestCase
):
    """Test transaction support."""

    def setUp(self):
        """Set up test fixtures."""
        self.postgres_config = PostgresPoolConfig(
            host="localhost",
            port=5432,
            database="test_db",
            user="test_user",
            password="test_pass",
            min_size=2,
            max_size=10,
            timeout=30,
        )
        self.mock_asyncpg_pool = MockAsyncpgPool(
            min_size=2, max_size=10
        )
        self.mock_asyncpg_connection = MockAsyncpgConnection()

    def _setup_mock_asyncpg_module(self):
        """Set up mock asyncpg module."""
        async def create_pool_mock(**kwargs):
            return self.mock_asyncpg_pool

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors."
            "postgres.pool_async.asyncpg"
        )
        self.mock_module = patch(patch_path).start()
        self.mock_module.create_pool = create_pool_mock

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_transaction_context(self):
        """Test using transaction context."""
        self._setup_mock_asyncpg_module()
        pool = AsyncPostgresConnectionPool(self.postgres_config)
        await pool.initialize_pool()

        async with pool.get_connection() as conn:
            async with conn.transaction():
                # Execute queries in transaction
                result = await conn.fetchval("SELECT 1")
                self.assertEqual(result, 1)


class TestAsyncPostgresConnectionPoolQueries(
    unittest.IsolatedAsyncioTestCase
):
    """Test query execution."""

    def setUp(self):
        """Set up test fixtures."""
        self.postgres_config = PostgresPoolConfig(
            host="localhost",
            port=5432,
            database="test_db",
            user="test_user",
            password="test_pass",
            min_size=2,
            max_size=10,
            timeout=30,
        )
        self.mock_asyncpg_pool = MockAsyncpgPool(
            min_size=2, max_size=10
        )
        self.mock_asyncpg_connection = MockAsyncpgConnection()

    def _setup_mock_asyncpg_module(self):
        """Set up mock asyncpg module."""
        async def create_pool_mock(**kwargs):
            return self.mock_asyncpg_pool

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors."
            "postgres.pool_async.asyncpg"
        )
        self.mock_module = patch(patch_path).start()
        self.mock_module.create_pool = create_pool_mock

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_fetch_query(self):
        """Test fetch query."""
        self._setup_mock_asyncpg_module()
        pool = AsyncPostgresConnectionPool(self.postgres_config)
        await pool.initialize_pool()

        async with pool.get_connection() as conn:
            results = await conn.fetch("SELECT * FROM users")
            self.assertGreaterEqual(len(results), 0)

    async def test_fetchval_query(self):
        """Test fetchval query."""
        self._setup_mock_asyncpg_module()
        pool = AsyncPostgresConnectionPool(self.postgres_config)
        await pool.initialize_pool()

        async with pool.get_connection() as conn:
            result = await conn.fetchval("SELECT 1")
            self.assertEqual(result, 1)

    async def test_execute_query(self):
        """Test execute query."""
        self._setup_mock_asyncpg_module()
        pool = AsyncPostgresConnectionPool(self.postgres_config)
        await pool.initialize_pool()

        async with pool.get_connection() as conn:
            result = await conn.execute(
                "INSERT INTO users (name) VALUES ($1)",
                "test"
            )
            self.assertIsNotNone(result)


if __name__ == '__main__':
    unittest.main()
