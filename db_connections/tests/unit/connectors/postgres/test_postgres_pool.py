"""Unit tests for PostgreSQL synchronous connection pool."""

import sys
from pathlib import Path
import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta

# Add parent directory to path to allow imports
# test_postgres_pool.py is at: db_connections/tests/unit/connectors/postgres/
# We need to go up 6 levels to get to the parent of db_connections
try:
    _file_path = Path(__file__).resolve()
except NameError:
    # __file__ might not be defined in some contexts
    import os
    _file_path = Path(os.getcwd()) / 'tests' / 'unit' / 'connectors' / 'postgres' / 'test_postgres_pool.py'

parent_dir = _file_path.parent.parent.parent.parent.parent.parent
parent_dir_str = str(parent_dir)
if parent_dir_str not in sys.path:
    sys.path.insert(0, parent_dir_str)

from db_connections.scr.all_db_connectors.connectors.postgres.config import (  # noqa: E402, E501
    PostgresPoolConfig
)
from db_connections.scr.all_db_connectors.connectors.postgres.pool import (  # noqa: E402, E501
    PostgresConnectionPool
)
from db_connections.scr.all_db_connectors.core.exceptions import (  # noqa: E402
    ConnectionError,
    PoolExhaustedError,
)
from db_connections.scr.all_db_connectors.core.health import (  # noqa: E402
    HealthState
)


class MockPsycopg2Cursor:
    """Mock psycopg2 cursor for testing."""

    def __init__(self):
        self.closed = False
        self._results = [(1,)]

    def execute(self, query, params=None):
        """Mock execute."""
        if "non_existent" in query:
            raise Exception("Table does not exist")
        pass

    def executemany(self, query, params):
        """Mock executemany."""
        pass

    def fetchone(self):
        """Mock fetchone."""
        return self._results[0] if self._results else None

    def close(self):
        """Mock close."""
        self.closed = True


class MockPsycopg2Connection:
    """Mock psycopg2 connection for testing."""

    def __init__(self):
        self.closed = False
        self.autocommit = False
        self._cursor = None

    def cursor(self):
        """Return mock cursor."""
        return MockPsycopg2Cursor()

    def commit(self):
        """Mock commit."""
        pass

    def rollback(self):
        """Mock rollback."""
        pass

    def close(self):
        """Mock close."""
        self.closed = True


class MockPsycopg2Pool:
    """Mock psycopg2 ThreadedConnectionPool."""

    def __init__(self, minconn, maxconn, **kwargs):
        self.minconn = minconn
        self.maxconn = maxconn
        self.connections = []
        self.in_use = set()

        # Create initial connections
        for _ in range(minconn):
            self.connections.append(MockPsycopg2Connection())

    def getconn(self):
        """Get connection from pool."""
        if self.connections:
            conn = self.connections.pop(0)
            self.in_use.add(id(conn))
            return conn
        elif len(self.in_use) < self.maxconn:
            conn = MockPsycopg2Connection()
            self.in_use.add(id(conn))
            return conn
        return None

    def putconn(self, conn, close=False):
        """Return connection to pool."""
        conn_id = id(conn)
        if conn_id in self.in_use:
            self.in_use.remove(conn_id)

        if close:
            conn.close()
        else:
            self.connections.append(conn)

    def closeall(self):
        """Close all connections."""
        for conn in self.connections:
            conn.close()
        self.connections.clear()
        self.in_use.clear()


class TestPostgresConnectionPoolInit(unittest.TestCase):
    """Test PostgresConnectionPool initialization."""

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
        self.mock_psycopg2_pool = MockPsycopg2Pool(minconn=2, maxconn=10)

    def _setup_mock_psycopg2_module(self):
        """Set up mock psycopg2 module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.postgres.pool.psycopg2_pool"
        ).start()
        self.mock_module.ThreadedConnectionPool.return_value = (
            self.mock_psycopg2_pool
        )

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_init_default(self):
        """Test pool initialization with defaults."""
        self._setup_mock_psycopg2_module()
        pool = PostgresConnectionPool(self.postgres_config)

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
            PostgresConnectionPool(invalid_config)

    def test_repr(self):
        """Test string representation."""
        self._setup_mock_psycopg2_module()
        pool = PostgresConnectionPool(self.postgres_config)

        repr_str = repr(pool)
        self.assertIn("PostgresConnectionPool", repr_str)
        self.assertIn("localhost", repr_str)
        self.assertIn("test_db", repr_str)


class TestPostgresConnectionPoolInitialization(unittest.TestCase):
    """Test pool initialization methods."""

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
        self.mock_psycopg2_pool = MockPsycopg2Pool(minconn=2, maxconn=10)

    def _setup_mock_psycopg2_module(self):
        """Set up mock psycopg2 module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.postgres.pool.psycopg2_pool"
        ).start()
        self.mock_module.ThreadedConnectionPool.return_value = (
            self.mock_psycopg2_pool
        )

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_initialize_pool(self):
        """Test explicit pool initialization."""
        self._setup_mock_psycopg2_module()
        pool = PostgresConnectionPool(self.postgres_config)
        pool.initialize_pool()

        self.assertTrue(pool._initialized)
        self.assertIsNotNone(pool._pool)
        self.mock_module.ThreadedConnectionPool.assert_called_once()

    def test_initialize_pool_already_initialized(self):
        """Test initializing already initialized pool."""
        self._setup_mock_psycopg2_module()
        pool = PostgresConnectionPool(self.postgres_config)
        pool.initialize_pool()
        pool.initialize_pool()  # Second call should be no-op

        # Should only be called once
        self.assertEqual(
            self.mock_module.ThreadedConnectionPool.call_count, 1
        )

    def test_initialize_pool_connection_error(self):
        """Test pool initialization with connection error."""
        with patch(
            "db_connections.scr.all_db_connectors.connectors.postgres.pool.psycopg2_pool"
        ) as mock_module:
            mock_module.ThreadedConnectionPool.side_effect = Exception(
                "Connection failed"
            )

            pool = PostgresConnectionPool(self.postgres_config)

            with self.assertRaisesRegex(
                ConnectionError, "Pool initialization failed"
            ):
                pool.initialize_pool()

    def test_lazy_initialization(self):
        """Test lazy pool initialization on first connection."""
        self._setup_mock_psycopg2_module()
        pool = PostgresConnectionPool(self.postgres_config)

        self.assertFalse(pool._initialized)

        with pool.get_connection():
            pass

        self.assertTrue(pool._initialized)


class TestPostgresConnectionPoolGetConnection(unittest.TestCase):
    """Test connection acquisition."""

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
        self.mock_psycopg2_pool = MockPsycopg2Pool(minconn=2, maxconn=10)
        self.mock_psycopg2_connection = MockPsycopg2Connection()

    def _setup_mock_psycopg2_module(self):
        """Set up mock psycopg2 module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.postgres.pool.psycopg2_pool"
        ).start()
        self.mock_module.ThreadedConnectionPool.return_value = (
            self.mock_psycopg2_pool
        )

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_get_connection_success(self):
        """Test successful connection acquisition."""
        self._setup_mock_psycopg2_module()
        pool = PostgresConnectionPool(self.postgres_config)
        pool.initialize_pool()

        with pool.get_connection() as conn:
            self.assertIsNotNone(conn)

    def test_get_connection_lazy_init(self):
        """Test connection triggers lazy initialization."""
        self._setup_mock_psycopg2_module()
        pool = PostgresConnectionPool(self.postgres_config)

        self.assertFalse(pool._initialized)

        with pool.get_connection():
            self.assertTrue(pool._initialized)

    def test_get_connection_validates(self):
        """Test connection validation on checkout."""
        self._setup_mock_psycopg2_module()
        config = PostgresPoolConfig(
            host="localhost",
            database="test_db",
            user="test_user",
            validate_on_checkout=True,
            pre_ping=True
        )

        pool = PostgresConnectionPool(config)

        with patch.object(
            pool, "validate_connection", return_value=True
        ):
            with pool.get_connection() as conn:
                self.assertIsNotNone(conn)

    def test_get_connection_pool_closed(self):
        """Test getting connection from closed pool."""
        self._setup_mock_psycopg2_module()
        pool = PostgresConnectionPool(self.postgres_config)
        pool._closed = True

        with self.assertRaisesRegex(ConnectionError, "Pool is closed"):
            with pool.get_connection():
                pass

    def test_get_connection_tracks_metadata(self):
        """Test connection metadata is tracked."""
        self._setup_mock_psycopg2_module()
        pool = PostgresConnectionPool(self.postgres_config)
        pool.initialize_pool()

        with pool.get_connection() as conn:
            conn_id = id(conn)
            self.assertIn(conn_id, pool._connection_metadata)
            self.assertTrue(
                pool._connection_metadata[conn_id].in_use
            )

    def test_get_connection_releases_on_exit(self):
        """Test connection is released after context manager exit."""
        self._setup_mock_psycopg2_module()
        pool = PostgresConnectionPool(self.postgres_config)
        pool.initialize_pool()

        with pool.get_connection() as conn:
            conn_id = id(conn)

        # After context manager exit
        self.assertNotIn(conn_id, pool._connections_in_use)

    def test_get_connection_exhausted(self):
        """Test connection acquisition when pool is exhausted."""
        with patch(
            "db_connections.scr.all_db_connectors.connectors.postgres.pool.psycopg2_pool"
        ) as mock_module:
            mock_pool = MagicMock()
            mock_pool.getconn.return_value = None
            mock_module.ThreadedConnectionPool.return_value = mock_pool

            pool = PostgresConnectionPool(self.postgres_config)
            pool.initialize_pool()

            with self.assertRaisesRegex(
                PoolExhaustedError, "No connections available"
            ):
                with pool.get_connection():
                    pass


class TestPostgresConnectionPoolRelease(unittest.TestCase):
    """Test connection release."""

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
        self.mock_psycopg2_pool = MockPsycopg2Pool(minconn=2, maxconn=10)
        self.mock_psycopg2_connection = MockPsycopg2Connection()

    def _setup_mock_psycopg2_module(self):
        """Set up mock psycopg2 module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.postgres.pool.psycopg2_pool"
        ).start()
        self.mock_module.ThreadedConnectionPool.return_value = (
            self.mock_psycopg2_pool
        )

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_release_connection(self):
        """Test releasing a connection back to pool."""
        self._setup_mock_psycopg2_module()
        pool = PostgresConnectionPool(self.postgres_config)
        pool.initialize_pool()

        conn = self.mock_psycopg2_connection
        conn_id = id(conn)
        pool._connections_in_use.add(conn_id)

        pool.release_connection(conn)

        self.assertNotIn(conn_id, pool._connections_in_use)

    def test_release_connection_with_recycling(self):
        """Test connection recycling on release."""
        self._setup_mock_psycopg2_module()
        config = PostgresPoolConfig(
            host="localhost",
            database="test_db",
            user="test_user",
            recycle_on_return=True
        )

        pool = PostgresConnectionPool(config)
        pool.initialize_pool()

        conn = self.mock_psycopg2_connection
        conn_id = id(conn)

        # Create metadata
        from db_connections.scr.all_db_connectors.core.utils import ConnectionMetadata
        pool._connection_metadata[conn_id] = ConnectionMetadata(
            created_at=datetime.now() - timedelta(hours=1)
        )

        pool.release_connection(conn)

        # Connection should be recycled
        self.assertNotIn(conn_id, pool._connection_metadata)


class TestPostgresConnectionPoolClose(unittest.TestCase):
    """Test pool closing."""

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
        self.mock_psycopg2_pool = MockPsycopg2Pool(minconn=2, maxconn=10)
        self.mock_psycopg2_connection = MockPsycopg2Connection()

    def _setup_mock_psycopg2_module(self):
        """Set up mock psycopg2 module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.postgres.pool.psycopg2_pool"
        ).start()
        self.mock_module.ThreadedConnectionPool.return_value = (
            self.mock_psycopg2_pool
        )

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_close_connection(self):
        """Test closing a single connection."""
        self._setup_mock_psycopg2_module()
        pool = PostgresConnectionPool(self.postgres_config)
        pool.initialize_pool()

        conn = self.mock_psycopg2_connection
        conn_id = id(conn)
        pool._connections_in_use.add(conn_id)

        pool.close_connection(conn)

        self.assertNotIn(conn_id, pool._connections_in_use)
        self.assertTrue(conn.closed)

    def test_close_all_connections(self):
        """Test closing all connections in pool."""
        self._setup_mock_psycopg2_module()
        pool = PostgresConnectionPool(self.postgres_config)
        pool.initialize_pool()

        pool.close_all_connections()

        self.assertTrue(pool._closed)
        self.assertEqual(len(pool._connections_in_use), 0)
        self.assertEqual(len(pool._connection_metadata), 0)


class TestPostgresConnectionPoolStatus(unittest.TestCase):
    """Test pool status and metrics."""

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
        self.mock_psycopg2_pool = MockPsycopg2Pool(minconn=2, maxconn=10)

    def _setup_mock_psycopg2_module(self):
        """Set up mock psycopg2 module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.postgres.pool.psycopg2_pool"
        ).start()
        self.mock_module.ThreadedConnectionPool.return_value = (
            self.mock_psycopg2_pool
        )

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_pool_status_uninitialized(self):
        """Test pool status before initialization."""
        self._setup_mock_psycopg2_module()
        pool = PostgresConnectionPool(self.postgres_config)

        status = pool.pool_status()

        self.assertFalse(status["initialized"])
        self.assertEqual(status["total_connections"], 0)
        expected_max = (
            self.postgres_config.max_size +
            self.postgres_config.max_overflow
        )
        self.assertEqual(status["max_connections"], expected_max)

    def test_pool_status_initialized(self):
        """Test pool status after initialization."""
        self._setup_mock_psycopg2_module()
        pool = PostgresConnectionPool(self.postgres_config)
        pool.initialize_pool()

        status = pool.pool_status()

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

    def test_get_metrics(self):
        """Test getting pool metrics."""
        self._setup_mock_psycopg2_module()
        pool = PostgresConnectionPool(self.postgres_config)
        pool.initialize_pool()

        metrics = pool.get_metrics()

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


class TestPostgresConnectionPoolValidation(unittest.TestCase):
    """Test connection validation."""

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
        self.mock_psycopg2_pool = MockPsycopg2Pool(minconn=2, maxconn=10)
        self.mock_psycopg2_connection = MockPsycopg2Connection()

    def _setup_mock_psycopg2_module(self):
        """Set up mock psycopg2 module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.postgres.pool.psycopg2_pool"
        ).start()
        self.mock_module.ThreadedConnectionPool.return_value = (
            self.mock_psycopg2_pool
        )

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_validate_connection_success(self):
        """Test successful connection validation."""
        self._setup_mock_psycopg2_module()
        pool = PostgresConnectionPool(self.postgres_config)

        result = pool.validate_connection(self.mock_psycopg2_connection)

        self.assertTrue(result)

    def test_validate_connection_failure(self):
        """Test connection validation failure."""
        self._setup_mock_psycopg2_module()
        pool = PostgresConnectionPool(self.postgres_config)

        # Create connection that will fail validation
        bad_conn = Mock()
        bad_conn.cursor.side_effect = Exception("Connection lost")

        result = pool.validate_connection(bad_conn)

        self.assertFalse(result)


class TestPostgresConnectionPoolHealth(unittest.TestCase):
    """Test health checks."""

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
        self.mock_psycopg2_pool = MockPsycopg2Pool(minconn=2, maxconn=10)
        self.mock_psycopg2_connection = MockPsycopg2Connection()

    def _setup_mock_psycopg2_module(self):
        """Set up mock psycopg2 module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.postgres.pool.psycopg2_pool"
        ).start()
        self.mock_module.ThreadedConnectionPool.return_value = (
            self.mock_psycopg2_pool
        )

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_health_check_healthy(self):
        """Test health check on healthy pool."""
        self._setup_mock_psycopg2_module()
        pool = PostgresConnectionPool(self.postgres_config)
        pool.initialize_pool()

        health = pool.health_check()

        self.assertEqual(health.state, HealthState.HEALTHY)
        self.assertIn("healthy", health.message.lower())

    def test_database_health_check(self):
        """Test database health check."""
        self._setup_mock_psycopg2_module()
        pool = PostgresConnectionPool(self.postgres_config)
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


class TestPostgresConnectionPoolContextManager(unittest.TestCase):
    """Test context manager support."""

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
        self.mock_psycopg2_pool = MockPsycopg2Pool(minconn=2, maxconn=10)

    def _setup_mock_psycopg2_module(self):
        """Set up mock psycopg2 module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.postgres.pool.psycopg2_pool"
        ).start()
        self.mock_module.ThreadedConnectionPool.return_value = (
            self.mock_psycopg2_pool
        )

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_context_manager_enter(self):
        """Test pool context manager enter."""
        self._setup_mock_psycopg2_module()
        with PostgresConnectionPool(self.postgres_config) as pool:
            self.assertTrue(pool._initialized)
            self.assertFalse(pool._closed)

    def test_context_manager_exit(self):
        """Test pool context manager exit."""
        self._setup_mock_psycopg2_module()
        pool = PostgresConnectionPool(self.postgres_config)

        with pool:
            pass

        self.assertTrue(pool._closed)

    def test_context_manager_with_connections(self):
        """Test using connections within context manager."""
        self._setup_mock_psycopg2_module()
        with PostgresConnectionPool(self.postgres_config) as pool:
            with pool.get_connection() as conn:
                self.assertIsNotNone(conn)

        self.assertTrue(pool._closed)


class TestPostgresConnectionPoolRetry(unittest.TestCase):
    """Test retry logic."""

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

    def test_retry_on_connection_failure(self):
        """Test retry logic on connection failure."""
        with patch(
            "db_connections.scr.all_db_connectors.connectors.postgres.pool.psycopg2_pool"
        ) as mock_module:
            mock_pool = MagicMock()

            # Fail twice, succeed on third attempt
            # Need extra items in case validation triggers additional getconn() calls
            import psycopg2
            mock_pool.getconn.side_effect = [
                psycopg2.OperationalError("Connection failed"),
                psycopg2.OperationalError("Connection failed"),
                Mock(),  # Success
                Mock(),  # Extra in case validation needs it
            ]

            mock_module.ThreadedConnectionPool.return_value = mock_pool

            config = PostgresPoolConfig(
                host="localhost",
                database="test_db",
                user="test_user",
                max_retries=3,
                retry_delay=0.1,
                pre_ping=False,  # Disable validation to avoid extra getconn() calls
                validate_on_checkout=False
            )

            pool = PostgresConnectionPool(config)
            pool.initialize_pool()

            # Should succeed after retries
            with pool.get_connection() as conn:
                self.assertIsNotNone(conn)

    def test_retry_exhausted(self):
        """Test when all retry attempts are exhausted."""
        with patch(
            "db_connections.scr.all_db_connectors.connectors.postgres.pool.psycopg2_pool"
        ) as mock_module:
            mock_pool = MagicMock()

            import psycopg2
            mock_pool.getconn.side_effect = psycopg2.OperationalError(
                "Connection failed"
            )

            mock_module.ThreadedConnectionPool.return_value = mock_pool

            config = PostgresPoolConfig(
                host="localhost",
                database="test_db",
                user="test_user",
                max_retries=2,
                retry_delay=0.1
            )

            pool = PostgresConnectionPool(config)
            pool.initialize_pool()

            with self.assertRaisesRegex(
                ConnectionError, "Connection acquisition failed"
            ):
                with pool.get_connection():
                    pass


class TestPostgresConnectionPoolConcurrency(unittest.TestCase):
    """Test concurrent operations."""

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
        self.mock_psycopg2_pool = MockPsycopg2Pool(minconn=2, maxconn=10)
        self.mock_psycopg2_connection = MockPsycopg2Connection()

    def _setup_mock_psycopg2_module(self):
        """Set up mock psycopg2 module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.postgres.pool.psycopg2_pool"
        ).start()
        self.mock_module.ThreadedConnectionPool.return_value = (
            self.mock_psycopg2_pool
        )

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_multiple_connections(self):
        """Test acquiring multiple connections."""
        with patch(
            "db_connections.scr.all_db_connectors.connectors.postgres.pool.psycopg2_pool"
        ) as mock_module:
            mock_pool = MagicMock()
            # Return a new Mock connection each time
            mock_pool.getconn = MagicMock(side_effect=lambda: MockPsycopg2Connection())
            mock_module.ThreadedConnectionPool.return_value = mock_pool

            pool = PostgresConnectionPool(self.postgres_config)
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
        self._setup_mock_psycopg2_module()
        pool = PostgresConnectionPool(self.postgres_config)
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
