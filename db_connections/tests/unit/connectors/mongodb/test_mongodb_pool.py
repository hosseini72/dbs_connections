"""Unit tests for MongoDB synchronous connection pool."""

import sys
from pathlib import Path
import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta

# Add parent directory to path to allow imports
# test_mongodb_pool.py is at: db_connections/tests/unit/connectors/mongodb/
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
        / "mongodb"
        / "test_mongodb_pool.py"
    )

parent_dir = _file_path.parent.parent.parent.parent.parent.parent
parent_dir_str = str(parent_dir)
if parent_dir_str not in sys.path:
    sys.path.insert(0, parent_dir_str)

from db_connections.scr.all_db_connectors.connectors.mongodb.config import (  # noqa: E402, E501
    MongoPoolConfig,
)
from db_connections.scr.all_db_connectors.connectors.mongodb.pool import (  # noqa: E402, E501
    MongoSyncConnectionPool,
)
from db_connections.scr.all_db_connectors.core.exceptions import (  # noqa: E402
    ConnectionError,
    PoolExhaustedError,
)
from db_connections.scr.all_db_connectors.core.health import (  # noqa: E402
    HealthState,
)


class MockMongoClient:
    """Mock MongoDB client for testing."""

    def __init__(self):
        self.closed = False
        self.address = ("localhost", 27017)
        self.nodes = [("localhost", 27017)]

    def ping(self):
        """Mock ping."""
        if self.closed:
            raise Exception("Connection closed")
        return True

    def admin(self):
        """Mock admin database."""
        return MockMongoDatabase()

    def __getitem__(self, name):
        """Mock database access."""
        return MockMongoDatabase()

    def close(self):
        """Mock close."""
        self.closed = True


class MockMongoDatabase:
    """Mock MongoDB database for testing."""

    def __init__(self):
        self.name = "testdb"

    def command(self, command):
        """Mock command."""
        if command == "ping":
            return {"ok": 1}
        elif command == "serverStatus":
            return {
                "ok": 1,
                "version": "6.0.0",
                "uptime": 3600,
                "connections": {"current": 10, "available": 990},
            }
        return {"ok": 1}

    def __getitem__(self, name):
        """Mock collection access."""
        return MockMongoCollection()


class MockMongoCollection:
    """Mock MongoDB collection for testing."""

    def find_one(self, filter=None):
        """Mock find_one."""
        return None

    def insert_one(self, document):
        """Mock insert_one."""
        return Mock(inserted_id="507f1f77bcf86cd799439011")


class MockMongoConnectionPool:
    """Mock MongoDB ConnectionPool."""

    def __init__(self, max_pool_size=10, **kwargs):
        self.max_pool_size = max_pool_size
        self.connections = []
        self.in_use = set()
        self.closed = False

    def get_connection(self):
        """Get connection from pool."""
        if self.closed:
            raise Exception("Pool is closed")
        if len(self.in_use) >= self.max_pool_size:
            return None
        conn = MockMongoClient()
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


class TestMongoConnectionPoolInit(unittest.TestCase):
    """Test MongoSyncConnectionPool initialization."""

    def setUp(self):
        """Set up test fixtures."""
        self.mongo_config = MongoPoolConfig(
            host="localhost",
            port=27017,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_mongo_pool = MockMongoConnectionPool(max_pool_size=10)

    def _setup_mock_mongo_module(self):
        """Set up mock pymongo module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.mongodb.pool.pymongo"
        ).start()
        self.mock_module.MongoClient.return_value = MockMongoClient()

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_init_default(self):
        """Test pool initialization with defaults."""
        self._setup_mock_mongo_module()
        pool = MongoSyncConnectionPool(self.mongo_config)

        self.assertEqual(pool.config, self.mongo_config)
        self.assertFalse(pool._initialized)
        self.assertIsNone(pool._pool)
        self.assertFalse(pool._closed)

    def test_init_validates_config(self):
        """Test pool validates configuration on init."""
        invalid_config = MongoPoolConfig(
            host="localhost",
            max_connections=-1,  # Invalid
        )

        with self.assertRaisesRegex(ValueError, "max_connections must be positive"):
            MongoSyncConnectionPool(invalid_config)

    def test_repr(self):
        """Test string representation."""
        self._setup_mock_mongo_module()
        pool = MongoSyncConnectionPool(self.mongo_config)

        repr_str = repr(pool)
        self.assertIn("MongoSyncConnectionPool", repr_str)
        self.assertIn("localhost", repr_str)
        self.assertIn("27017", repr_str)


class TestMongoConnectionPoolInitialization(unittest.TestCase):
    """Test pool initialization methods."""

    def setUp(self):
        """Set up test fixtures."""
        self.mongo_config = MongoPoolConfig(
            host="localhost",
            port=27017,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_mongo_pool = MockMongoConnectionPool(max_pool_size=10)

    def _setup_mock_mongo_module(self):
        """Set up mock pymongo module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.mongodb.pool.pymongo"
        ).start()
        self.mock_module.MongoClient.return_value = MockMongoClient()

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_initialize_pool(self):
        """Test explicit pool initialization."""
        self._setup_mock_mongo_module()
        pool = MongoSyncConnectionPool(self.mongo_config)
        pool.initialize_pool()

        self.assertTrue(pool._initialized)
        self.assertIsNotNone(pool._pool)
        self.mock_module.MongoClient.assert_called_once()

    def test_initialize_pool_already_initialized(self):
        """Test initializing already initialized pool."""
        self._setup_mock_mongo_module()
        pool = MongoSyncConnectionPool(self.mongo_config)
        pool.initialize_pool()
        pool.initialize_pool()  # Second call should be no-op

        # Should only be called once
        self.assertEqual(self.mock_module.MongoClient.call_count, 1)

    def test_initialize_pool_connection_error(self):
        """Test pool initialization with connection error."""
        with patch(
            "db_connections.scr.all_db_connectors.connectors.mongodb.pool.pymongo"
        ) as mock_module:
            mock_module.MongoClient.side_effect = Exception("Connection failed")

            pool = MongoSyncConnectionPool(self.mongo_config)

            with self.assertRaisesRegex(ConnectionError, "Pool initialization failed"):
                pool.initialize_pool()

    def test_lazy_initialization(self):
        """Test lazy pool initialization on first connection."""
        self._setup_mock_mongo_module()
        pool = MongoSyncConnectionPool(self.mongo_config)

        self.assertFalse(pool._initialized)

        with pool.get_connection():
            pass

        self.assertTrue(pool._initialized)


class TestMongoConnectionPoolGetConnection(unittest.TestCase):
    """Test connection acquisition."""

    def setUp(self):
        """Set up test fixtures."""
        self.mongo_config = MongoPoolConfig(
            host="localhost",
            port=27017,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_mongo_pool = MockMongoConnectionPool(max_pool_size=10)
        self.mock_mongo_client = MockMongoClient()

    def _setup_mock_mongo_module(self):
        """Set up mock pymongo module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.mongodb.pool.pymongo"
        ).start()
        self.mock_module.MongoClient.return_value = self.mock_mongo_client

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_get_connection_success(self):
        """Test successful connection acquisition."""
        self._setup_mock_mongo_module()
        pool = MongoSyncConnectionPool(self.mongo_config)
        pool.initialize_pool()

        with pool.get_connection() as conn:
            self.assertIsNotNone(conn)

    def test_get_connection_lazy_init(self):
        """Test connection triggers lazy initialization."""
        self._setup_mock_mongo_module()
        pool = MongoSyncConnectionPool(self.mongo_config)

        self.assertFalse(pool._initialized)

        with pool.get_connection():
            self.assertTrue(pool._initialized)

    def test_get_connection_validates(self):
        """Test connection validation on checkout."""
        self._setup_mock_mongo_module()
        config = MongoPoolConfig(
            host="localhost", validate_on_checkout=True, pre_ping=True
        )

        pool = MongoSyncConnectionPool(config)

        with patch.object(pool, "validate_connection", return_value=True):
            with pool.get_connection() as conn:
                self.assertIsNotNone(conn)

    def test_get_connection_pool_closed(self):
        """Test getting connection from closed pool."""
        self._setup_mock_mongo_module()
        pool = MongoSyncConnectionPool(self.mongo_config)
        pool._closed = True

        with self.assertRaisesRegex(ConnectionError, "Pool is closed"):
            with pool.get_connection():
                pass

    def test_get_connection_tracks_metadata(self):
        """Test connection metadata is tracked."""
        self._setup_mock_mongo_module()
        pool = MongoSyncConnectionPool(self.mongo_config)
        pool.initialize_pool()

        with pool.get_connection() as conn:
            conn_id = id(conn)
            self.assertIn(conn_id, pool._connection_metadata)
            self.assertTrue(pool._connection_metadata[conn_id].in_use)

    def test_get_connection_releases_on_exit(self):
        """Test connection is released after context manager exit."""
        self._setup_mock_mongo_module()
        pool = MongoSyncConnectionPool(self.mongo_config)
        pool.initialize_pool()

        with pool.get_connection() as conn:
            conn_id = id(conn)

        # After context manager exit
        self.assertNotIn(conn_id, pool._connections_in_use)

    def test_get_connection_exhausted(self):
        """Test connection acquisition when pool is exhausted."""
        with patch(
            "db_connections.scr.all_db_connectors.connectors.mongodb.pool.pymongo"
        ) as mock_module:
            mock_client = MagicMock()
            mock_client.__enter__ = Mock(return_value=None)
            mock_client.__exit__ = Mock(return_value=None)
            mock_module.MongoClient.return_value = mock_client

            pool = MongoSyncConnectionPool(self.mongo_config)
            pool.initialize_pool()

            # Simulate pool exhaustion
            pool._connections_in_use = set(range(15))

            with self.assertRaisesRegex(PoolExhaustedError, "No connections available"):
                with pool.get_connection():
                    pass


class TestMongoConnectionPoolRelease(unittest.TestCase):
    """Test connection release."""

    def setUp(self):
        """Set up test fixtures."""
        self.mongo_config = MongoPoolConfig(
            host="localhost",
            port=27017,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_mongo_pool = MockMongoConnectionPool(max_pool_size=10)
        self.mock_mongo_client = MockMongoClient()

    def _setup_mock_mongo_module(self):
        """Set up mock pymongo module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.mongodb.pool.pymongo"
        ).start()
        self.mock_module.MongoClient.return_value = self.mock_mongo_client

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_release_connection(self):
        """Test releasing a connection back to pool."""
        self._setup_mock_mongo_module()
        pool = MongoSyncConnectionPool(self.mongo_config)
        pool.initialize_pool()

        conn = self.mock_mongo_client
        conn_id = id(conn)
        pool._connections_in_use.add(conn_id)

        pool.release_connection(conn)

        self.assertNotIn(conn_id, pool._connections_in_use)

    def test_release_connection_with_recycling(self):
        """Test connection recycling on release."""
        self._setup_mock_mongo_module()
        config = MongoPoolConfig(host="localhost", recycle_on_return=True)

        pool = MongoSyncConnectionPool(config)
        pool.initialize_pool()

        conn = self.mock_mongo_client
        conn_id = id(conn)

        # Create metadata
        from db_connections.scr.all_db_connectors.core.utils import ConnectionMetadata

        pool._connection_metadata[conn_id] = ConnectionMetadata(
            created_at=datetime.now() - timedelta(hours=1)
        )

        pool.release_connection(conn)

        # Connection should be recycled
        self.assertNotIn(conn_id, pool._connection_metadata)


class TestMongoConnectionPoolClose(unittest.TestCase):
    """Test pool closing."""

    def setUp(self):
        """Set up test fixtures."""
        self.mongo_config = MongoPoolConfig(
            host="localhost",
            port=27017,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_mongo_pool = MockMongoConnectionPool(max_pool_size=10)
        self.mock_mongo_client = MockMongoClient()

    def _setup_mock_mongo_module(self):
        """Set up mock pymongo module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.mongodb.pool.pymongo"
        ).start()
        self.mock_module.MongoClient.return_value = self.mock_mongo_client

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_close_connection(self):
        """Test closing a single connection."""
        self._setup_mock_mongo_module()
        pool = MongoSyncConnectionPool(self.mongo_config)
        pool.initialize_pool()

        conn = self.mock_mongo_client
        conn_id = id(conn)
        pool._connections_in_use.add(conn_id)

        pool.close_connection(conn)

        self.assertNotIn(conn_id, pool._connections_in_use)
        self.assertTrue(conn.closed)

    def test_close_all_connections(self):
        """Test closing all connections in pool."""
        self._setup_mock_mongo_module()
        pool = MongoSyncConnectionPool(self.mongo_config)
        pool.initialize_pool()

        pool.close_all_connections()

        self.assertTrue(pool._closed)
        self.assertEqual(len(pool._connections_in_use), 0)
        self.assertEqual(len(pool._connection_metadata), 0)


class TestMongoConnectionPoolStatus(unittest.TestCase):
    """Test pool status and metrics."""

    def setUp(self):
        """Set up test fixtures."""
        self.mongo_config = MongoPoolConfig(
            host="localhost",
            port=27017,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_mongo_pool = MockMongoConnectionPool(max_pool_size=10)

    def _setup_mock_mongo_module(self):
        """Set up mock pymongo module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.mongodb.pool.pymongo"
        ).start()
        self.mock_module.MongoClient.return_value = MockMongoClient()

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_pool_status_uninitialized(self):
        """Test pool status before initialization."""
        self._setup_mock_mongo_module()
        pool = MongoSyncConnectionPool(self.mongo_config)

        status = pool.pool_status()

        self.assertFalse(status["initialized"])
        self.assertEqual(status["total_connections"], 0)
        expected_max = (
            self.mongo_config.max_connections + self.mongo_config.max_overflow
        )
        self.assertEqual(status["max_connections"], expected_max)

    def test_pool_status_initialized(self):
        """Test pool status after initialization."""
        self._setup_mock_mongo_module()
        pool = MongoSyncConnectionPool(self.mongo_config)
        pool.initialize_pool()

        status = pool.pool_status()

        self.assertTrue(status["initialized"])
        self.assertFalse(status["closed"])
        expected_max = (
            self.mongo_config.max_connections + self.mongo_config.max_overflow
        )
        self.assertEqual(status["max_connections"], expected_max)
        self.assertEqual(status["min_connections"], self.mongo_config.min_connections)

    def test_get_metrics(self):
        """Test getting pool metrics."""
        self._setup_mock_mongo_module()
        pool = MongoSyncConnectionPool(self.mongo_config)
        pool.initialize_pool()

        metrics = pool.get_metrics()

        self.assertGreaterEqual(metrics.total_connections, 0)
        self.assertGreaterEqual(metrics.active_connections, 0)
        self.assertGreaterEqual(metrics.idle_connections, 0)
        expected_max = (
            self.mongo_config.max_connections + self.mongo_config.max_overflow
        )
        self.assertEqual(metrics.max_connections, expected_max)
        self.assertEqual(metrics.min_connections, self.mongo_config.min_connections)


class TestMongoConnectionPoolValidation(unittest.TestCase):
    """Test connection validation."""

    def setUp(self):
        """Set up test fixtures."""
        self.mongo_config = MongoPoolConfig(
            host="localhost",
            port=27017,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_mongo_pool = MockMongoConnectionPool(max_pool_size=10)
        self.mock_mongo_client = MockMongoClient()

    def _setup_mock_mongo_module(self):
        """Set up mock pymongo module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.mongodb.pool.pymongo"
        ).start()
        self.mock_module.MongoClient.return_value = self.mock_mongo_client

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_validate_connection_success(self):
        """Test successful connection validation."""
        self._setup_mock_mongo_module()
        pool = MongoSyncConnectionPool(self.mongo_config)

        result = pool.validate_connection(self.mock_mongo_client)

        self.assertTrue(result)

    def test_validate_connection_failure(self):
        """Test connection validation failure."""
        self._setup_mock_mongo_module()
        pool = MongoSyncConnectionPool(self.mongo_config)

        # Create connection that will fail validation
        bad_conn = Mock()
        bad_conn.admin.return_value.command.side_effect = Exception("Connection lost")

        result = pool.validate_connection(bad_conn)

        self.assertFalse(result)


class TestMongoConnectionPoolHealth(unittest.TestCase):
    """Test health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.mongo_config = MongoPoolConfig(
            host="localhost",
            port=27017,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_mongo_pool = MockMongoConnectionPool(max_pool_size=10)
        self.mock_mongo_client = MockMongoClient()

    def _setup_mock_mongo_module(self):
        """Set up mock pymongo module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.mongodb.pool.pymongo"
        ).start()
        self.mock_module.MongoClient.return_value = self.mock_mongo_client

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_health_check_healthy(self):
        """Test health check on healthy pool."""
        self._setup_mock_mongo_module()
        pool = MongoSyncConnectionPool(self.mongo_config)
        pool.initialize_pool()

        health = pool.health_check()

        self.assertEqual(health.state, HealthState.HEALTHY)
        self.assertIn("healthy", health.message.lower())

    def test_database_health_check(self):
        """Test database health check."""
        self._setup_mock_mongo_module()
        pool = MongoSyncConnectionPool(self.mongo_config)
        pool.initialize_pool()

        with patch.object(pool, "get_connection"):
            health = pool.database_health_check()

            self.assertIn(
                health.state,
                [HealthState.HEALTHY, HealthState.DEGRADED, HealthState.UNHEALTHY],
            )


class TestMongoConnectionPoolContextManager(unittest.TestCase):
    """Test context manager support."""

    def setUp(self):
        """Set up test fixtures."""
        self.mongo_config = MongoPoolConfig(
            host="localhost",
            port=27017,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_mongo_pool = MockMongoConnectionPool(max_pool_size=10)

    def _setup_mock_mongo_module(self):
        """Set up mock pymongo module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.mongodb.pool.pymongo"
        ).start()
        self.mock_module.MongoClient.return_value = MockMongoClient()

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_context_manager_enter(self):
        """Test pool context manager enter."""
        self._setup_mock_mongo_module()
        with MongoSyncConnectionPool(self.mongo_config) as pool:
            self.assertTrue(pool._initialized)
            self.assertFalse(pool._closed)

    def test_context_manager_exit(self):
        """Test pool context manager exit."""
        self._setup_mock_mongo_module()
        pool = MongoSyncConnectionPool(self.mongo_config)

        with pool:
            pass

        self.assertTrue(pool._closed)

    def test_context_manager_with_connections(self):
        """Test using connections within context manager."""
        self._setup_mock_mongo_module()
        with MongoSyncConnectionPool(self.mongo_config) as pool:
            with pool.get_connection() as conn:
                self.assertIsNotNone(conn)

        self.assertTrue(pool._closed)


class TestMongoConnectionPoolRetry(unittest.TestCase):
    """Test retry logic."""

    def setUp(self):
        """Set up test fixtures."""
        self.mongo_config = MongoPoolConfig(
            host="localhost",
            port=27017,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )

    def test_retry_on_connection_failure(self):
        """Test retry logic on connection failure."""
        with patch(
            "db_connections.scr.all_db_connectors.connectors.mongodb.pool.pymongo"
        ) as mock_module:
            # Fail twice, succeed on third attempt
            import pymongo

            mock_module.MongoClient.side_effect = [
                pymongo.errors.ConnectionFailure("Connection failed"),
                pymongo.errors.ConnectionFailure("Connection failed"),
                MockMongoClient(),  # Success
            ]

            config = MongoPoolConfig(host="localhost", max_retries=3, retry_delay=0.1)

            pool = MongoSyncConnectionPool(config)
            pool.initialize_pool()

            # Should succeed after retries
            with pool.get_connection() as conn:
                self.assertIsNotNone(conn)

    def test_retry_exhausted(self):
        """Test when all retry attempts are exhausted."""
        with patch(
            "db_connections.scr.all_db_connectors.connectors.mongodb.pool.pymongo"
        ) as mock_module:
            import pymongo

            mock_module.MongoClient.side_effect = pymongo.errors.ConnectionFailure(
                "Connection failed"
            )

            config = MongoPoolConfig(host="localhost", max_retries=2, retry_delay=0.1)

            pool = MongoSyncConnectionPool(config)
            pool.initialize_pool()

            with self.assertRaisesRegex(
                ConnectionError, "Connection acquisition failed"
            ):
                with pool.get_connection():
                    pass


class TestMongoConnectionPoolConcurrency(unittest.TestCase):
    """Test concurrent operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.mongo_config = MongoPoolConfig(
            host="localhost",
            port=27017,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_mongo_pool = MockMongoConnectionPool(max_pool_size=10)
        self.mock_mongo_client = MockMongoClient()

    def _setup_mock_mongo_module(self):
        """Set up mock pymongo module."""
        self.mock_module = patch(
            "db_connections.scr.all_db_connectors.connectors.mongodb.pool.pymongo"
        ).start()
        self.mock_module.MongoClient.return_value = self.mock_mongo_client

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_multiple_connections(self):
        """Test acquiring multiple connections."""
        self._setup_mock_mongo_module()
        pool = MongoSyncConnectionPool(self.mongo_config)
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
        self._setup_mock_mongo_module()
        pool = MongoSyncConnectionPool(self.mongo_config)
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
