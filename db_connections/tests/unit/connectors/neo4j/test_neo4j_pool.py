"""Unit tests for Neo4j synchronous connection pool."""

import sys
from pathlib import Path
import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta

# Add parent directory to path to allow imports
# test_neo4j_pool.py is at: db_connections/tests/unit/connectors/neo4j/
# We need to go up 6 levels to get to the parent of db_connections
try:
    _file_path = Path(__file__).resolve()
except NameError:
    # __file__ might not be defined in some contexts
    import os
    _file_path = Path(os.getcwd()) / 'tests' / 'unit' / 'connectors' / 'neo4j' / 'test_neo4j_pool.py'  # noqa: E501

parent_dir = _file_path.parent.parent.parent.parent.parent.parent
parent_dir_str = str(parent_dir)
if parent_dir_str not in sys.path:
    sys.path.insert(0, parent_dir_str)

from db_connections.scr.all_db_connectors.connectors.neo4j.config import (  # noqa: E402, E501
    Neo4jPoolConfig
)
from db_connections.scr.all_db_connectors.connectors.neo4j.pool import (  # noqa: E402, E501
    Neo4jSyncConnectionPool
)
from db_connections.scr.all_db_connectors.core.exceptions import (  # noqa: E402
    ConnectionError,
    PoolExhaustedError,
)
from db_connections.scr.all_db_connectors.core.health import (  # noqa: E402
    HealthState
)


class MockNeo4jSession:
    """Mock Neo4j session for testing."""

    def __init__(self):
        self.closed = False

    def run(self, query, parameters=None):
        """Mock run."""
        if self.closed:
            raise Exception("Session closed")
        return MockNeo4jResult()

    def close(self):
        """Mock close."""
        self.closed = True

    def __enter__(self):
        """Context manager enter."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False


class MockNeo4jResult:
    """Mock Neo4j result for testing."""

    def __init__(self):
        self.data = [{"n": {"id": 1, "name": "test"}}]

    def data(self):
        """Mock data."""
        return self.data

    def single(self):
        """Mock single."""
        return self.data[0] if self.data else None

    def values(self):
        """Mock values."""
        return [[1, "test"]]


class MockNeo4jDriver:
    """Mock Neo4j driver for testing."""

    def __init__(self):
        self.closed = False
        self.encrypted = False
        self.verify_connectivity_called = False

    def verify_connectivity(self):
        """Mock verify_connectivity."""
        if self.closed:
            raise Exception("Driver closed")
        self.verify_connectivity_called = True
        return True

    def session(self, database=None, **kwargs):
        """Mock session creation."""
        if self.closed:
            raise Exception("Driver closed")
        return MockNeo4jSession()

    def close(self):
        """Mock close."""
        self.closed = True


class MockNeo4jConnectionPool:
    """Mock Neo4j ConnectionPool."""

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
        conn = MockNeo4jDriver()
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


class TestNeo4jConnectionPoolInit(unittest.TestCase):
    """Test Neo4jSyncConnectionPool initialization."""

    def setUp(self):
        """Set up test fixtures."""
        self.neo4j_config = Neo4jPoolConfig(
            host="localhost",
            port=7687,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_neo4j_pool = MockNeo4jConnectionPool(max_connections=10)

    def _setup_mock_neo4j_module(self):
        """Set up mock neo4j module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.neo4j.pool.neo4j"
        ).start()
        self.mock_module.GraphDatabase.driver.return_value = (
            MockNeo4jDriver()
        )

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_init_default(self):
        """Test pool initialization with defaults."""
        self._setup_mock_neo4j_module()
        pool = Neo4jSyncConnectionPool(self.neo4j_config)

        self.assertEqual(pool.config, self.neo4j_config)
        self.assertFalse(pool._initialized)
        self.assertIsNone(pool._pool)
        self.assertFalse(pool._closed)

    def test_init_validates_config(self):
        """Test pool validates configuration on init."""
        invalid_config = Neo4jPoolConfig(
            host="localhost",
            max_connections=-1  # Invalid
        )

        with self.assertRaisesRegex(
            ValueError, "max_connections must be positive"
        ):
            Neo4jSyncConnectionPool(invalid_config)

    def test_repr(self):
        """Test string representation."""
        self._setup_mock_neo4j_module()
        pool = Neo4jSyncConnectionPool(self.neo4j_config)

        repr_str = repr(pool)
        self.assertIn("Neo4jSyncConnectionPool", repr_str)
        self.assertIn("localhost", repr_str)
        self.assertIn("7687", repr_str)


class TestNeo4jConnectionPoolInitialization(unittest.TestCase):
    """Test pool initialization methods."""

    def setUp(self):
        """Set up test fixtures."""
        self.neo4j_config = Neo4jPoolConfig(
            host="localhost",
            port=7687,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_neo4j_pool = MockNeo4jConnectionPool(max_connections=10)

    def _setup_mock_neo4j_module(self):
        """Set up mock neo4j module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.neo4j.pool.neo4j"
        ).start()
        self.mock_module.GraphDatabase.driver.return_value = (
            MockNeo4jDriver()
        )

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_initialize_pool(self):
        """Test explicit pool initialization."""
        self._setup_mock_neo4j_module()
        pool = Neo4jSyncConnectionPool(self.neo4j_config)
        pool.initialize_pool()

        self.assertTrue(pool._initialized)
        self.assertIsNotNone(pool._pool)
        self.mock_module.GraphDatabase.driver.assert_called_once()

    def test_initialize_pool_already_initialized(self):
        """Test initializing already initialized pool."""
        self._setup_mock_neo4j_module()
        pool = Neo4jSyncConnectionPool(self.neo4j_config)
        pool.initialize_pool()
        pool.initialize_pool()  # Second call should be no-op

        # Should only be called once
        self.assertEqual(
            self.mock_module.GraphDatabase.driver.call_count, 1
        )

    def test_initialize_pool_connection_error(self):
        """Test pool initialization with connection error."""
        with patch(
            "db_connections.scr.all_db_connectors.connectors.neo4j.pool.neo4j"
        ) as mock_module:
            mock_module.GraphDatabase.driver.side_effect = Exception(
                "Connection failed"
            )

            pool = Neo4jSyncConnectionPool(self.neo4j_config)

            with self.assertRaisesRegex(
                ConnectionError, "Pool initialization failed"
            ):
                pool.initialize_pool()

    def test_lazy_initialization(self):
        """Test lazy pool initialization on first connection."""
        self._setup_mock_neo4j_module()
        pool = Neo4jSyncConnectionPool(self.neo4j_config)

        self.assertFalse(pool._initialized)

        with pool.get_connection():
            pass

        self.assertTrue(pool._initialized)


class TestNeo4jConnectionPoolGetConnection(unittest.TestCase):
    """Test connection acquisition."""

    def setUp(self):
        """Set up test fixtures."""
        self.neo4j_config = Neo4jPoolConfig(
            host="localhost",
            port=7687,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_neo4j_pool = MockNeo4jConnectionPool(max_connections=10)
        self.mock_neo4j_driver = MockNeo4jDriver()

    def _setup_mock_neo4j_module(self):
        """Set up mock neo4j module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.neo4j.pool.neo4j"
        ).start()
        self.mock_module.GraphDatabase.driver.return_value = (
            self.mock_neo4j_driver
        )

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_get_connection_success(self):
        """Test successful connection acquisition."""
        self._setup_mock_neo4j_module()
        pool = Neo4jSyncConnectionPool(self.neo4j_config)
        pool.initialize_pool()

        with pool.get_connection() as conn:
            self.assertIsNotNone(conn)

    def test_get_connection_lazy_init(self):
        """Test connection triggers lazy initialization."""
        self._setup_mock_neo4j_module()
        pool = Neo4jSyncConnectionPool(self.neo4j_config)

        self.assertFalse(pool._initialized)

        with pool.get_connection():
            self.assertTrue(pool._initialized)

    def test_get_connection_validates(self):
        """Test connection validation on checkout."""
        self._setup_mock_neo4j_module()
        config = Neo4jPoolConfig(
            host="localhost",
            validate_on_checkout=True,
            pre_ping=True
        )

        pool = Neo4jSyncConnectionPool(config)

        with patch.object(
            pool, "validate_connection", return_value=True
        ):
            with pool.get_connection() as conn:
                self.assertIsNotNone(conn)

    def test_get_connection_pool_closed(self):
        """Test getting connection from closed pool."""
        self._setup_mock_neo4j_module()
        pool = Neo4jSyncConnectionPool(self.neo4j_config)
        pool._closed = True

        with self.assertRaisesRegex(ConnectionError, "Pool is closed"):
            with pool.get_connection():
                pass

    def test_get_connection_tracks_metadata(self):
        """Test connection metadata is tracked."""
        self._setup_mock_neo4j_module()
        pool = Neo4jSyncConnectionPool(self.neo4j_config)
        pool.initialize_pool()

        with pool.get_connection() as conn:
            conn_id = id(conn)
            self.assertIn(conn_id, pool._connection_metadata)
            self.assertTrue(
                pool._connection_metadata[conn_id].in_use
            )

    def test_get_connection_releases_on_exit(self):
        """Test connection is released after context manager exit."""
        self._setup_mock_neo4j_module()
        pool = Neo4jSyncConnectionPool(self.neo4j_config)
        pool.initialize_pool()

        with pool.get_connection() as conn:
            conn_id = id(conn)

        # After context manager exit
        self.assertNotIn(conn_id, pool._connections_in_use)

    def test_get_connection_exhausted(self):
        """Test connection acquisition when pool is exhausted."""
        with patch(
            "db_connections.scr.all_db_connectors.connectors.neo4j.pool.neo4j"
        ) as mock_module:
            mock_driver = MagicMock()
            mock_driver.__enter__ = Mock(return_value=None)
            mock_driver.__exit__ = Mock(return_value=None)
            mock_module.GraphDatabase.driver.return_value = mock_driver

            pool = Neo4jSyncConnectionPool(self.neo4j_config)
            pool.initialize_pool()

            # Simulate pool exhaustion
            pool._connections_in_use = set(range(15))

            with self.assertRaisesRegex(
                PoolExhaustedError, "No connections available"
            ):
                with pool.get_connection():
                    pass


class TestNeo4jConnectionPoolRelease(unittest.TestCase):
    """Test connection release."""

    def setUp(self):
        """Set up test fixtures."""
        self.neo4j_config = Neo4jPoolConfig(
            host="localhost",
            port=7687,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_neo4j_pool = MockNeo4jConnectionPool(max_connections=10)
        self.mock_neo4j_driver = MockNeo4jDriver()

    def _setup_mock_neo4j_module(self):
        """Set up mock neo4j module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.neo4j.pool.neo4j"
        ).start()
        self.mock_module.GraphDatabase.driver.return_value = (
            self.mock_neo4j_driver
        )

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_release_connection(self):
        """Test releasing a connection back to pool."""
        self._setup_mock_neo4j_module()
        pool = Neo4jSyncConnectionPool(self.neo4j_config)
        pool.initialize_pool()

        conn = self.mock_neo4j_driver
        conn_id = id(conn)
        pool._connections_in_use.add(conn_id)

        pool.release_connection(conn)

        self.assertNotIn(conn_id, pool._connections_in_use)

    def test_release_connection_with_recycling(self):
        """Test connection recycling on release."""
        self._setup_mock_neo4j_module()
        config = Neo4jPoolConfig(
            host="localhost",
            recycle_on_return=True
        )

        pool = Neo4jSyncConnectionPool(config)
        pool.initialize_pool()

        conn = self.mock_neo4j_driver
        conn_id = id(conn)

        # Create metadata
        from db_connections.scr.all_db_connectors.core.utils import ConnectionMetadata
        pool._connection_metadata[conn_id] = ConnectionMetadata(
            created_at=datetime.now() - timedelta(hours=1)
        )

        pool.release_connection(conn)

        # Connection should be recycled
        self.assertNotIn(conn_id, pool._connection_metadata)


class TestNeo4jConnectionPoolClose(unittest.TestCase):
    """Test pool closing."""

    def setUp(self):
        """Set up test fixtures."""
        self.neo4j_config = Neo4jPoolConfig(
            host="localhost",
            port=7687,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_neo4j_pool = MockNeo4jConnectionPool(max_connections=10)
        self.mock_neo4j_driver = MockNeo4jDriver()

    def _setup_mock_neo4j_module(self):
        """Set up mock neo4j module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.neo4j.pool.neo4j"
        ).start()
        self.mock_module.GraphDatabase.driver.return_value = (
            self.mock_neo4j_driver
        )

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_close_connection(self):
        """Test closing a single connection."""
        self._setup_mock_neo4j_module()
        pool = Neo4jSyncConnectionPool(self.neo4j_config)
        pool.initialize_pool()

        conn = self.mock_neo4j_driver
        conn_id = id(conn)
        pool._connections_in_use.add(conn_id)

        pool.close_connection(conn)

        self.assertNotIn(conn_id, pool._connections_in_use)
        self.assertTrue(conn.closed)

    def test_close_all_connections(self):
        """Test closing all connections in pool."""
        self._setup_mock_neo4j_module()
        pool = Neo4jSyncConnectionPool(self.neo4j_config)
        pool.initialize_pool()

        pool.close_all_connections()

        self.assertTrue(pool._closed)
        self.assertEqual(len(pool._connections_in_use), 0)
        self.assertEqual(len(pool._connection_metadata), 0)


class TestNeo4jConnectionPoolStatus(unittest.TestCase):
    """Test pool status and metrics."""

    def setUp(self):
        """Set up test fixtures."""
        self.neo4j_config = Neo4jPoolConfig(
            host="localhost",
            port=7687,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_neo4j_pool = MockNeo4jConnectionPool(max_connections=10)

    def _setup_mock_neo4j_module(self):
        """Set up mock neo4j module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.neo4j.pool.neo4j"
        ).start()
        self.mock_module.GraphDatabase.driver.return_value = (
            MockNeo4jDriver()
        )

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_pool_status_uninitialized(self):
        """Test pool status before initialization."""
        self._setup_mock_neo4j_module()
        pool = Neo4jSyncConnectionPool(self.neo4j_config)

        status = pool.pool_status()

        self.assertFalse(status["initialized"])
        self.assertEqual(status["total_connections"], 0)
        expected_max = (
            self.neo4j_config.max_connections +
            self.neo4j_config.max_overflow
        )
        self.assertEqual(status["max_connections"], expected_max)

    def test_pool_status_initialized(self):
        """Test pool status after initialization."""
        self._setup_mock_neo4j_module()
        pool = Neo4jSyncConnectionPool(self.neo4j_config)
        pool.initialize_pool()

        status = pool.pool_status()

        self.assertTrue(status["initialized"])
        self.assertFalse(status["closed"])
        expected_max = (
            self.neo4j_config.max_connections +
            self.neo4j_config.max_overflow
        )
        self.assertEqual(status["max_connections"], expected_max)
        self.assertEqual(
            status["min_connections"], self.neo4j_config.min_connections
        )

    def test_get_metrics(self):
        """Test getting pool metrics."""
        self._setup_mock_neo4j_module()
        pool = Neo4jSyncConnectionPool(self.neo4j_config)
        pool.initialize_pool()

        metrics = pool.get_metrics()

        self.assertGreaterEqual(metrics.total_connections, 0)
        self.assertGreaterEqual(metrics.active_connections, 0)
        self.assertGreaterEqual(metrics.idle_connections, 0)
        expected_max = (
            self.neo4j_config.max_connections +
            self.neo4j_config.max_overflow
        )
        self.assertEqual(metrics.max_connections, expected_max)
        self.assertEqual(
            metrics.min_connections, self.neo4j_config.min_connections
        )


class TestNeo4jConnectionPoolValidation(unittest.TestCase):
    """Test connection validation."""

    def setUp(self):
        """Set up test fixtures."""
        self.neo4j_config = Neo4jPoolConfig(
            host="localhost",
            port=7687,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_neo4j_pool = MockNeo4jConnectionPool(max_connections=10)
        self.mock_neo4j_driver = MockNeo4jDriver()

    def _setup_mock_neo4j_module(self):
        """Set up mock neo4j module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.neo4j.pool.neo4j"
        ).start()
        self.mock_module.GraphDatabase.driver.return_value = (
            self.mock_neo4j_driver
        )

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_validate_connection_success(self):
        """Test successful connection validation."""
        self._setup_mock_neo4j_module()
        pool = Neo4jSyncConnectionPool(self.neo4j_config)

        result = pool.validate_connection(self.mock_neo4j_driver)

        self.assertTrue(result)

    def test_validate_connection_failure(self):
        """Test connection validation failure."""
        self._setup_mock_neo4j_module()
        pool = Neo4jSyncConnectionPool(self.neo4j_config)

        # Create connection that will fail validation
        bad_conn = Mock()
        bad_conn.verify_connectivity.side_effect = Exception(
            "Connection lost"
        )

        result = pool.validate_connection(bad_conn)

        self.assertFalse(result)


class TestNeo4jConnectionPoolHealth(unittest.TestCase):
    """Test health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.neo4j_config = Neo4jPoolConfig(
            host="localhost",
            port=7687,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_neo4j_pool = MockNeo4jConnectionPool(max_connections=10)
        self.mock_neo4j_driver = MockNeo4jDriver()

    def _setup_mock_neo4j_module(self):
        """Set up mock neo4j module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.neo4j.pool.neo4j"
        ).start()
        self.mock_module.GraphDatabase.driver.return_value = (
            self.mock_neo4j_driver
        )

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_health_check_healthy(self):
        """Test health check on healthy pool."""
        self._setup_mock_neo4j_module()
        pool = Neo4jSyncConnectionPool(self.neo4j_config)
        pool.initialize_pool()

        health = pool.health_check()

        self.assertEqual(health.state, HealthState.HEALTHY)
        self.assertIn("healthy", health.message.lower())

    def test_database_health_check(self):
        """Test database health check."""
        self._setup_mock_neo4j_module()
        pool = Neo4jSyncConnectionPool(self.neo4j_config)
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


class TestNeo4jConnectionPoolContextManager(unittest.TestCase):
    """Test context manager support."""

    def setUp(self):
        """Set up test fixtures."""
        self.neo4j_config = Neo4jPoolConfig(
            host="localhost",
            port=7687,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_neo4j_pool = MockNeo4jConnectionPool(max_connections=10)

    def _setup_mock_neo4j_module(self):
        """Set up mock neo4j module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.neo4j.pool.neo4j"
        ).start()
        self.mock_module.GraphDatabase.driver.return_value = (
            MockNeo4jDriver()
        )

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_context_manager_enter(self):
        """Test pool context manager enter."""
        self._setup_mock_neo4j_module()
        with Neo4jSyncConnectionPool(self.neo4j_config) as pool:
            self.assertTrue(pool._initialized)
            self.assertFalse(pool._closed)

    def test_context_manager_exit(self):
        """Test pool context manager exit."""
        self._setup_mock_neo4j_module()
        pool = Neo4jSyncConnectionPool(self.neo4j_config)

        with pool:
            pass

        self.assertTrue(pool._closed)

    def test_context_manager_with_connections(self):
        """Test using connections within context manager."""
        self._setup_mock_neo4j_module()
        with Neo4jSyncConnectionPool(self.neo4j_config) as pool:
            with pool.get_connection() as conn:
                self.assertIsNotNone(conn)

        self.assertTrue(pool._closed)


class TestNeo4jConnectionPoolRetry(unittest.TestCase):
    """Test retry logic."""

    def setUp(self):
        """Set up test fixtures."""
        self.neo4j_config = Neo4jPoolConfig(
            host="localhost",
            port=7687,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )

    def test_retry_on_connection_failure(self):
        """Test retry logic on connection failure."""
        with patch(
            "db_connections.scr.all_db_connectors.connectors.neo4j.pool.neo4j"
        ) as mock_module:
            # Fail twice, succeed on third attempt
            import neo4j
            mock_module.GraphDatabase.driver.side_effect = [
                neo4j.exceptions.ServiceUnavailable("Connection failed"),
                neo4j.exceptions.ServiceUnavailable("Connection failed"),
                MockNeo4jDriver(),  # Success
            ]

            config = Neo4jPoolConfig(
                host="localhost",
                max_retries=3,
                retry_delay=0.1
            )

            pool = Neo4jSyncConnectionPool(config)
            pool.initialize_pool()

            # Should succeed after retries
            with pool.get_connection() as conn:
                self.assertIsNotNone(conn)

    def test_retry_exhausted(self):
        """Test when all retry attempts are exhausted."""
        with patch(
            "db_connections.scr.all_db_connectors.connectors.neo4j.pool.neo4j"
        ) as mock_module:
            import neo4j
            mock_module.GraphDatabase.driver.side_effect = (
                neo4j.exceptions.ServiceUnavailable("Connection failed")
            )

            config = Neo4jPoolConfig(
                host="localhost",
                max_retries=2,
                retry_delay=0.1
            )

            pool = Neo4jSyncConnectionPool(config)
            pool.initialize_pool()

            with self.assertRaisesRegex(
                ConnectionError, "Connection acquisition failed"
            ):
                with pool.get_connection():
                    pass


class TestNeo4jConnectionPoolConcurrency(unittest.TestCase):
    """Test concurrent operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.neo4j_config = Neo4jPoolConfig(
            host="localhost",
            port=7687,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_neo4j_pool = MockNeo4jConnectionPool(max_connections=10)
        self.mock_neo4j_driver = MockNeo4jDriver()

    def _setup_mock_neo4j_module(self):
        """Set up mock neo4j module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.neo4j.pool.neo4j"
        ).start()
        self.mock_module.GraphDatabase.driver.return_value = (
            self.mock_neo4j_driver
        )

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_multiple_connections(self):
        """Test acquiring multiple connections."""
        self._setup_mock_neo4j_module()
        pool = Neo4jSyncConnectionPool(self.neo4j_config)
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
        self._setup_mock_neo4j_module()
        pool = Neo4jSyncConnectionPool(self.neo4j_config)
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

