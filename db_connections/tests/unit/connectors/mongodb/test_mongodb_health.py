"""Unit tests for MongoDB health checks."""

import sys
from pathlib import Path
import unittest
import time
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime

# Add parent directory to path to allow imports
# test_mongodb_health.py is at: db_connections/tests/unit/connectors/mongodb/
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
        / "test_mongodb_health.py"
    )

parent_dir = _file_path.parent.parent.parent.parent.parent.parent
parent_dir_str = str(parent_dir)
if parent_dir_str not in sys.path:
    sys.path.insert(0, parent_dir_str)

from db_connections.scr.all_db_connectors.connectors.mongodb.config import (  # noqa: E402, E501
    MongoPoolConfig,
)
from db_connections.scr.all_db_connectors.connectors.mongodb.health import (  # noqa: E402
    MongoHealthChecker,
)
from db_connections.scr.all_db_connectors.core.health import (  # noqa: E402
    HealthState,
    HealthStatus,
)


class MockMongoClient:
    """Mock MongoDB client for testing."""

    def __init__(self, healthy=True):
        self.closed = False
        self.healthy = healthy
        self.address = ("localhost", 27017)
        self.nodes = [("localhost", 27017)]
        self._server_status = {
            "ok": 1,
            "version": "6.0.0",
            "uptime": 3600,
            "uptimeMillis": 3600000,
            "connections": {"current": 10, "available": 990, "totalCreated": 1000},
            "network": {"bytesIn": 1024000, "bytesOut": 2048000, "numRequests": 5000},
            "opcounters": {
                "insert": 100,
                "query": 500,
                "update": 50,
                "delete": 10,
                "getmore": 20,
                "command": 200,
            },
            "repl": {
                "setName": "rs0",
                "ismaster": True,
                "secondary": False,
                "primary": "localhost:27017",
            },
            "storageEngine": {"name": "wiredTiger"},
            "mem": {"resident": 512, "virtual": 1024, "mapped": 256},
        }

    def ping(self):
        """Mock ping."""
        if not self.healthy:
            raise Exception("Connection lost")
        return True

    def admin(self):
        """Mock admin database."""
        return MockMongoDatabase(self.healthy, self._server_status)

    def __getitem__(self, name):
        """Mock database access."""
        return MockMongoDatabase(self.healthy, self._server_status)

    def set_server_status(self, key, value):
        """Set server status value for testing."""
        self._server_status[key] = value


class MockMongoDatabase:
    """Mock MongoDB database for testing."""

    def __init__(self, healthy=True, server_status=None):
        self.name = "testdb"
        self.healthy = healthy
        self._server_status = server_status or {}

    def command(self, command):
        """Mock command."""
        if not self.healthy:
            raise Exception("Connection lost")

        if command == "ping":
            return {"ok": 1}
        elif command == "serverStatus":
            return self._server_status
        elif command == "replSetGetStatus":
            return {
                "ok": 1,
                "set": "rs0",
                "members": [
                    {"name": "localhost:27017", "stateStr": "PRIMARY", "health": 1}
                ],
            }
        elif command == "dbStats":
            return {
                "ok": 1,
                "db": "testdb",
                "collections": 5,
                "objects": 1000,
                "dataSize": 1024000,
                "storageSize": 2048000,
                "indexes": 10,
                "indexSize": 512000,
            }
        return {"ok": 1}


class MockAsyncMongoClient:
    """Mock Async MongoDB client for testing."""

    def __init__(self, healthy=True):
        self.closed = False
        self.healthy = healthy
        self.address = ("localhost", 27017)
        self.nodes = [("localhost", 27017)]
        self._server_status = {
            "ok": 1,
            "version": "6.0.0",
            "uptime": 3600,
            "connections": {"current": 10, "available": 990},
        }

    async def ping(self):
        """Mock ping."""
        if not self.healthy:
            raise Exception("Connection lost")
        return True

    def admin(self):
        """Mock admin database."""
        return MockAsyncMongoDatabase(self.healthy, self._server_status)

    def __getitem__(self, name):
        """Mock database access."""
        return MockAsyncMongoDatabase(self.healthy, self._server_status)

    async def close(self):
        """Mock close."""
        self.closed = True


class MockAsyncMongoDatabase:
    """Mock Async MongoDB database for testing."""

    def __init__(self, healthy=True, server_status=None):
        self.name = "testdb"
        self.healthy = healthy
        self._server_status = server_status or {}

    async def command(self, command):
        """Mock command."""
        if not self.healthy:
            raise Exception("Connection lost")

        if command == "ping":
            return {"ok": 1}
        elif command == "serverStatus":
            return self._server_status
        return {"ok": 1}


class TestMongoHealthCheckerInit(unittest.TestCase):
    """Test MongoHealthChecker initialization."""

    def setUp(self):
        """Set up test fixtures."""
        self.mongo_config = MongoPoolConfig(
            host="localhost",
            port=27017,
        )

    def test_init(self):
        """Test health checker initialization."""
        checker = MongoHealthChecker(self.mongo_config)

        self.assertEqual(checker.config, self.mongo_config)
        self.assertIsNone(checker._last_check_time)
        self.assertIsNone(checker._last_status)


class TestMongoHealthCheckerConnection(unittest.TestCase):
    """Test connection health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.mongo_config = MongoPoolConfig(
            host="localhost",
            port=27017,
        )
        self.health_checker = MongoHealthChecker(self.mongo_config)
        self.mock_mongo_client = MockMongoClient(healthy=True)

    def test_check_health_healthy(self):
        """Test health check on healthy connection."""
        result = self.health_checker.check_health(self.mock_mongo_client)

        self.assertIsInstance(result, HealthStatus)
        self.assertEqual(result.state, HealthState.HEALTHY)
        self.assertIsNotNone(result.response_time_ms)
        self.assertGreaterEqual(result.response_time_ms, 0)

    def test_check_health_unhealthy(self):
        """Test health check on unhealthy connection."""
        bad_client = MockMongoClient(healthy=False)

        result = self.health_checker.check_health(bad_client)

        self.assertIsInstance(result, HealthStatus)
        self.assertEqual(result.state, HealthState.UNHEALTHY)
        self.assertIn("failed", result.message.lower())
        self.assertIsNotNone(result.details)
        self.assertIn("error", result.details)

    def test_check_health_with_high_connections(self):
        """Test health check with high connection count."""
        client = MockMongoClient()
        client.set_server_status(
            "connections", {"current": 950, "available": 50, "totalCreated": 10000}
        )

        result = self.health_checker.check_health(client)

        # Should be degraded or unhealthy due to high connections
        self.assertIn(result.state, [HealthState.DEGRADED, HealthState.UNHEALTHY])

    def test_check_health_with_slow_response(self):
        """Test health check with slow response."""
        client = MockMongoClient()

        # Simulate slow response by patching time
        with patch("time.time", side_effect=[0, 1.5]):  # 1.5 second delay
            result = self.health_checker.check_health(client)

        # Should be degraded or unhealthy due to slow response
        self.assertIn(result.state, [HealthState.DEGRADED, HealthState.UNHEALTHY])


class TestMongoHealthCheckerReplication(unittest.TestCase):
    """Test replication health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.mongo_config = MongoPoolConfig(
            host="localhost",
            port=27017,
        )
        self.health_checker = MongoHealthChecker(self.mongo_config)
        self.mock_mongo_client = MockMongoClient()

    def test_check_replication_status_primary(self):
        """Test replication status check on primary."""
        client = MockMongoClient()
        client.set_server_status(
            "repl",
            {
                "setName": "rs0",
                "ismaster": True,
                "secondary": False,
                "primary": "localhost:27017",
            },
        )

        status = self.health_checker.check_replication_status(client)

        self.assertIsInstance(status, dict)
        self.assertTrue(status.get("is_primary"))
        self.assertEqual(status.get("replica_set"), "rs0")

    def test_check_replication_status_secondary(self):
        """Test replication status check on secondary."""
        client = MockMongoClient()
        client.set_server_status(
            "repl",
            {
                "setName": "rs0",
                "ismaster": False,
                "secondary": True,
                "primary": "other:27017",
            },
        )

        status = self.health_checker.check_replication_status(client)

        self.assertIsInstance(status, dict)
        self.assertFalse(status.get("is_primary"))
        self.assertTrue(status.get("is_secondary"))

    def test_check_replication_status_error(self):
        """Test replication status check with error."""
        client = Mock()
        client.admin.return_value.command.side_effect = Exception("Error")

        status = self.health_checker.check_replication_status(client)

        # Should return None or empty dict on error
        self.assertTrue(status is None or status == {})


class TestMongoHealthCheckerDatabase(unittest.TestCase):
    """Test database health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.mongo_config = MongoPoolConfig(
            host="localhost",
            port=27017,
        )
        self.health_checker = MongoHealthChecker(self.mongo_config)
        self.mock_mongo_client = MockMongoClient()

    def test_check_database_stats(self):
        """Test database stats check."""
        client = MockMongoClient()
        db = client["testdb"]

        stats = self.health_checker.check_database_stats(db)

        self.assertIsInstance(stats, dict)
        self.assertIn("collections", stats)
        self.assertIn("objects", stats)
        self.assertIn("dataSize", stats)
        self.assertIn("storageSize", stats)

    def test_check_database_stats_error(self):
        """Test database stats check with error."""
        db = Mock()
        db.command.side_effect = Exception("Error")

        stats = self.health_checker.check_database_stats(db)

        # Should return None or empty dict on error
        self.assertTrue(stats is None or stats == {})


class TestMongoHealthCheckerServerStatus(unittest.TestCase):
    """Test server status checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.mongo_config = MongoPoolConfig(
            host="localhost",
            port=27017,
        )
        self.health_checker = MongoHealthChecker(self.mongo_config)
        self.mock_mongo_client = MockMongoClient()

    def test_check_server_status(self):
        """Test server status check."""
        client = MockMongoClient()

        status = self.health_checker.check_server_status(client)

        self.assertIsInstance(status, dict)
        self.assertIn("version", status)
        self.assertIn("uptime", status)
        self.assertIn("connections", status)

    def test_check_server_status_error(self):
        """Test server status check with error."""
        client = Mock()
        client.admin.return_value.command.side_effect = Exception("Error")

        status = self.health_checker.check_server_status(client)

        # Should return None or empty dict on error
        self.assertTrue(status is None or status == {})


class TestAsyncMongoHealthChecker(unittest.IsolatedAsyncioTestCase):
    """Test async health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.mongo_config = MongoPoolConfig(
            host="localhost",
            port=27017,
        )
        self.health_checker = MongoHealthChecker(self.mongo_config)
        self.mock_async_mongo_client = MockAsyncMongoClient(healthy=True)

    async def test_async_check_health_healthy(self):
        """Test async health check on healthy connection."""
        result = await self.health_checker.async_check_health(
            self.mock_async_mongo_client
        )

        self.assertIsInstance(result, HealthStatus)
        self.assertEqual(result.state, HealthState.HEALTHY)
        self.assertIsNotNone(result.response_time_ms)

    async def test_async_check_health_unhealthy(self):
        """Test async health check on unhealthy connection."""
        bad_client = MockAsyncMongoClient(healthy=False)

        result = await self.health_checker.async_check_health(bad_client)

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
        self.mongo_config = MongoPoolConfig(
            host="localhost",
            port=27017,
        )
        self.health_checker = MongoHealthChecker(self.mongo_config)
        self.mock_mongo_client = MockMongoClient()

    def test_response_time_recorded(self):
        """Test that response time is recorded."""
        result = self.health_checker.check_health(self.mock_mongo_client)

        self.assertIsNotNone(result.response_time_ms)
        self.assertGreater(result.response_time_ms, 0)

    def test_response_time_on_error(self):
        """Test response time is recorded even on error."""
        bad_client = MockMongoClient(healthy=False)

        result = self.health_checker.check_health(bad_client)

        self.assertIsNotNone(result.response_time_ms)
        self.assertGreaterEqual(result.response_time_ms, 0)


class TestMongoHealthCheckerComprehensive(unittest.TestCase):
    """Test comprehensive health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.mongo_config = MongoPoolConfig(
            host="localhost",
            port=27017,
        )
        self.health_checker = MongoHealthChecker(self.mongo_config)
        self.mock_mongo_client = MockMongoClient()

    def test_comprehensive_check(self):
        """Test comprehensive health check."""
        result = self.health_checker.comprehensive_check(self.mock_mongo_client)

        self.assertIsInstance(result, dict)
        self.assertIn("connection", result)
        self.assertIn("server", result)
        self.assertIn("replication", result)
        self.assertIn("timestamp", result)

        self.assertIsInstance(result["connection"], HealthStatus)
        self.assertIsInstance(result["timestamp"], datetime)


if __name__ == "__main__":
    unittest.main()
