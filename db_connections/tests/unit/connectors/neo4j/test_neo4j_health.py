"""Unit tests for Neo4j health checks."""

import sys
from pathlib import Path
import unittest
import time
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime

# Add parent directory to path to allow imports
# test_neo4j_health.py is at: db_connections/tests/unit/connectors/neo4j/
# We need to go up 6 levels to get to the parent of db_connections
try:
    _file_path = Path(__file__).resolve()
except NameError:
    # __file__ might not be defined in some contexts
    import os
    _file_path = Path(os.getcwd()) / 'tests' / 'unit' / 'connectors' / 'neo4j' / 'test_neo4j_health.py'  # noqa: E501

parent_dir = _file_path.parent.parent.parent.parent.parent.parent
parent_dir_str = str(parent_dir)
if parent_dir_str not in sys.path:
    sys.path.insert(0, parent_dir_str)

from db_connections.scr.all_db_connectors.connectors.neo4j.config import (  # noqa: E402, E501
    Neo4jPoolConfig
)
from db_connections.scr.all_db_connectors.connectors.neo4j.health import (  # noqa: E402
    Neo4jHealthChecker,
)
from db_connections.scr.all_db_connectors.core.health import (  # noqa: E402
    HealthState, HealthStatus
)


class MockNeo4jDriver:
    """Mock Neo4j driver for testing."""

    def __init__(self, healthy=True):
        self.closed = False
        self.healthy = healthy
        self.connected = True

    def verify_connectivity(self):
        """Mock verify_connectivity."""
        if not self.healthy:
            raise Exception("Connection lost")
        return True

    def session(self, database=None, **kwargs):
        """Mock session creation."""
        if not self.healthy:
            raise Exception("Connection lost")
        return MockNeo4jSession(self.healthy)

    def close(self):
        """Mock close."""
        self.closed = True
        self.connected = False


class MockNeo4jSession:
    """Mock Neo4j session for testing."""

    def __init__(self, healthy=True):
        self.healthy = healthy
        self.closed = False

    def run(self, query, parameters=None):
        """Mock run."""
        if not self.healthy:
            raise Exception("Connection lost")
        if "RETURN 1" in query:
            return MockNeo4jResult([{"1": 1}])
        elif "CALL dbms.components()" in query:
            return MockNeo4jResult([{
                "name": "Neo4j Kernel",
                "versions": ["5.0.0"],
                "edition": "community"
            }])
        return MockNeo4jResult([])

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

    def __init__(self, data=None):
        self.data = data or []

    def data(self):
        """Mock data."""
        return self.data

    def single(self):
        """Mock single."""
        return self.data[0] if self.data else None

    def values(self):
        """Mock values."""
        return [[item] for item in self.data]


class MockAsyncNeo4jDriver:
    """Mock Async Neo4j driver for testing."""

    def __init__(self, healthy=True):
        self.closed = False
        self.healthy = healthy
        self.connected = True

    async def verify_connectivity(self):
        """Mock verify_connectivity."""
        if not self.healthy:
            raise Exception("Connection lost")
        return True

    def session(self, database=None, **kwargs):
        """Mock session creation."""
        if not self.healthy:
            raise Exception("Connection lost")
        return MockAsyncNeo4jSession(self.healthy)

    async def close(self):
        """Mock close."""
        self.closed = True
        self.connected = False


class MockAsyncNeo4jSession:
    """Mock Async Neo4j session for testing."""

    def __init__(self, healthy=True):
        self.healthy = healthy
        self.closed = False

    async def run(self, query, parameters=None):
        """Mock run."""
        if not self.healthy:
            raise Exception("Connection lost")
        if "RETURN 1" in query:
            return MockAsyncNeo4jResult([{"1": 1}])
        return MockAsyncNeo4jResult([])

    async def close(self):
        """Mock close."""
        self.closed = True

    async def __aenter__(self):
        """Async context manager enter."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
        return False


class MockAsyncNeo4jResult:
    """Mock Async Neo4j result for testing."""

    def __init__(self, data=None):
        self.data = data or []

    async def data(self):
        """Mock data."""
        return self.data

    async def single(self):
        """Mock single."""
        return self.data[0] if self.data else None

    async def values(self):
        """Mock values."""
        return [[item] for item in self.data]


class TestNeo4jHealthCheckerInit(unittest.TestCase):
    """Test Neo4jHealthChecker initialization."""

    def setUp(self):
        """Set up test fixtures."""
        self.neo4j_config = Neo4jPoolConfig(
            host="localhost",
            port=7687,
        )

    def test_init(self):
        """Test health checker initialization."""
        checker = Neo4jHealthChecker(self.neo4j_config)

        self.assertEqual(checker.config, self.neo4j_config)
        self.assertIsNone(checker._last_check_time)
        self.assertIsNone(checker._last_status)


class TestNeo4jHealthCheckerConnection(unittest.TestCase):
    """Test connection health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.neo4j_config = Neo4jPoolConfig(
            host="localhost",
            port=7687,
        )
        self.health_checker = Neo4jHealthChecker(self.neo4j_config)
        self.mock_neo4j_driver = MockNeo4jDriver(healthy=True)

    def test_check_health_healthy(self):
        """Test health check on healthy connection."""
        result = self.health_checker.check_health(
            self.mock_neo4j_driver
        )

        self.assertIsInstance(result, HealthStatus)
        self.assertEqual(result.state, HealthState.HEALTHY)
        self.assertIsNotNone(result.response_time_ms)
        self.assertGreaterEqual(result.response_time_ms, 0)

    def test_check_health_unhealthy(self):
        """Test health check on unhealthy connection."""
        bad_driver = MockNeo4jDriver(healthy=False)

        result = self.health_checker.check_health(bad_driver)

        self.assertIsInstance(result, HealthStatus)
        self.assertEqual(result.state, HealthState.UNHEALTHY)
        self.assertIn("failed", result.message.lower())
        self.assertIsNotNone(result.details)
        self.assertIn("error", result.details)

    def test_check_health_with_closed_connection(self):
        """Test health check with closed connection."""
        driver = MockNeo4jDriver()
        driver.close()

        result = self.health_checker.check_health(driver)

        # Should be unhealthy due to closed connection
        self.assertEqual(result.state, HealthState.UNHEALTHY)

    def test_check_health_with_slow_response(self):
        """Test health check with slow response."""
        driver = MockNeo4jDriver()

        # Simulate slow response by patching time
        with patch('time.time', side_effect=[0, 1.5]):  # 1.5 second delay
            result = self.health_checker.check_health(driver)

        # Should be degraded or unhealthy due to slow response
        self.assertIn(
            result.state,
            [HealthState.DEGRADED, HealthState.UNHEALTHY]
        )


class TestNeo4jHealthCheckerServerInfo(unittest.TestCase):
    """Test server info checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.neo4j_config = Neo4jPoolConfig(
            host="localhost",
            port=7687,
        )
        self.health_checker = Neo4jHealthChecker(self.neo4j_config)
        self.mock_neo4j_driver = MockNeo4jDriver()

    def test_check_server_info(self):
        """Test server info check."""
        driver = MockNeo4jDriver()

        info = self.health_checker.check_server_info(driver)

        self.assertIsInstance(info, dict)

    def test_check_server_info_error(self):
        """Test server info check with error."""
        driver = Mock()
        # Make driver.session not callable to simulate error condition
        driver.session = None

        info = self.health_checker.check_server_info(driver)

        # Should return None when session is not callable
        self.assertIsNone(info)


class TestNeo4jHealthCheckerDatabase(unittest.TestCase):
    """Test database health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.neo4j_config = Neo4jPoolConfig(
            host="localhost",
            port=7687,
        )
        self.health_checker = Neo4jHealthChecker(self.neo4j_config)
        self.mock_neo4j_driver = MockNeo4jDriver()

    def test_check_database_status(self):
        """Test database status check."""
        driver = MockNeo4jDriver()

        status = self.health_checker.check_database_status(driver)

        self.assertIsInstance(status, dict)

    def test_check_database_status_error(self):
        """Test database status check with error."""
        driver = Mock()
        driver.session.side_effect = Exception("Error")

        status = self.health_checker.check_database_status(driver)

        # Should return None on error
        self.assertIsNone(status)


class TestNeo4jHealthCheckerQuery(unittest.TestCase):
    """Test query health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.neo4j_config = Neo4jPoolConfig(
            host="localhost",
            port=7687,
        )
        self.health_checker = Neo4jHealthChecker(self.neo4j_config)
        self.mock_neo4j_driver = MockNeo4jDriver()

    def test_check_query_performance(self):
        """Test query performance check."""
        driver = MockNeo4jDriver()

        performance = self.health_checker.check_query_performance(
            driver, "MATCH (n) RETURN count(n)"
        )

        self.assertIsInstance(performance, dict)

    def test_check_query_performance_error(self):
        """Test query performance check with error."""
        driver = Mock()
        driver.session.side_effect = Exception("Error")

        performance = self.health_checker.check_query_performance(
            driver, "MATCH (n) RETURN count(n)"
        )

        # Should return None on error
        self.assertIsNone(performance)


class TestAsyncNeo4jHealthChecker(unittest.IsolatedAsyncioTestCase):
    """Test async health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.neo4j_config = Neo4jPoolConfig(
            host="localhost",
            port=7687,
        )
        self.health_checker = Neo4jHealthChecker(self.neo4j_config)
        self.mock_async_neo4j_driver = MockAsyncNeo4jDriver(healthy=True)

    async def test_async_check_health_healthy(self):
        """Test async health check on healthy connection."""
        result = await self.health_checker.async_check_health(
            self.mock_async_neo4j_driver
        )

        self.assertIsInstance(result, HealthStatus)
        self.assertEqual(result.state, HealthState.HEALTHY)
        self.assertIsNotNone(result.response_time_ms)

    async def test_async_check_health_unhealthy(self):
        """Test async health check on unhealthy connection."""
        bad_driver = MockAsyncNeo4jDriver(healthy=False)

        result = await self.health_checker.async_check_health(bad_driver)

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
        self.neo4j_config = Neo4jPoolConfig(
            host="localhost",
            port=7687,
        )
        self.health_checker = Neo4jHealthChecker(self.neo4j_config)
        self.mock_neo4j_driver = MockNeo4jDriver()

    def test_response_time_recorded(self):
        """Test that response time is recorded."""
        result = self.health_checker.check_health(
            self.mock_neo4j_driver
        )

        self.assertIsNotNone(result.response_time_ms)
        self.assertGreater(result.response_time_ms, 0)

    def test_response_time_on_error(self):
        """Test response time is recorded even on error."""
        bad_driver = MockNeo4jDriver(healthy=False)

        result = self.health_checker.check_health(bad_driver)

        self.assertIsNotNone(result.response_time_ms)
        self.assertGreaterEqual(result.response_time_ms, 0)


class TestNeo4jHealthCheckerComprehensive(unittest.TestCase):
    """Test comprehensive health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.neo4j_config = Neo4jPoolConfig(
            host="localhost",
            port=7687,
        )
        self.health_checker = Neo4jHealthChecker(self.neo4j_config)
        self.mock_neo4j_driver = MockNeo4jDriver()

    def test_comprehensive_check(self):
        """Test comprehensive health check."""
        result = self.health_checker.comprehensive_check(
            self.mock_neo4j_driver
        )

        self.assertIsInstance(result, dict)
        self.assertIn("connection", result)
        self.assertIn("server", result)
        self.assertIn("timestamp", result)

        self.assertIsInstance(result["connection"], HealthStatus)
        self.assertIsInstance(result["timestamp"], datetime)


if __name__ == '__main__':
    unittest.main()

