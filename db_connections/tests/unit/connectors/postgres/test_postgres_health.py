"""Unit tests for PostgreSQL health checks."""

import sys
from pathlib import Path
import unittest
import time
from unittest.mock import Mock, AsyncMock, MagicMock
from datetime import datetime

# Add parent directory to path to allow imports
# test_postgres_health.py is at: db_connections/tests/unit/connectors/postgres/
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
        / "postgres"
        / "test_postgres_health.py"
    )

parent_dir = _file_path.parent.parent.parent.parent.parent.parent
parent_dir_str = str(parent_dir)
if parent_dir_str not in sys.path:
    sys.path.insert(0, parent_dir_str)

from db_connections.scr.all_db_connectors.connectors.postgres.config import (  # noqa: E402, E501
    PostgresPoolConfig,
)
from db_connections.scr.all_db_connectors.connectors.postgres.health import (  # noqa: E402
    PostgresHealthChecker,
    async_check_connection,
)
from db_connections.scr.all_db_connectors.core.health import (  # noqa: E402
    HealthState,
    HealthStatus,
)


class MockPsycopg2Cursor:
    """Mock psycopg2 cursor for testing."""

    def __init__(self):
        self.closed = False
        self._results = [(1,)]

    def execute(self, query, *args):
        """Mock execute."""
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


class TestPostgresHealthCheckerInit(unittest.TestCase):
    """Test PostgresHealthChecker initialization."""

    def setUp(self):
        """Set up test fixtures."""
        self.postgres_config = PostgresPoolConfig(
            host="localhost",
            port=5432,
            database="test_db",
            user="test_user",
            password="test_pass",
        )
        self.mock_pool = Mock()
        self.mock_pool.config = self.postgres_config
        self.mock_pool.pool_status = Mock(
            return_value={
                "total_connections": 5,
                "active_connections": 2,
                "idle_connections": 3,
                "max_connections": 10,
            }
        )

    def test_init(self):
        """Test health checker initialization."""
        checker = PostgresHealthChecker(self.mock_pool)

        self.assertEqual(checker.pool, self.mock_pool)


class TestPostgresHealthCheckerConnection(unittest.TestCase):
    """Test connection health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.postgres_config = PostgresPoolConfig(
            host="localhost",
            port=5432,
            database="test_db",
            user="test_user",
            password="test_pass",
        )
        self.mock_pool = Mock()
        self.mock_pool.config = self.postgres_config
        self.mock_pool.pool_status = Mock(
            return_value={
                "total_connections": 5,
                "active_connections": 2,
                "idle_connections": 3,
                "max_connections": 10,
            }
        )
        self.health_checker = PostgresHealthChecker(self.mock_pool)
        self.mock_psycopg2_connection = MockPsycopg2Connection()

    def test_check_connection_healthy(self):
        """Test health check on healthy connection."""
        result = self.health_checker.check_connection(self.mock_psycopg2_connection)

        self.assertIsInstance(result, HealthStatus)
        self.assertEqual(result.state, HealthState.HEALTHY)
        self.assertIsNotNone(result.response_time_ms)
        self.assertGreaterEqual(result.response_time_ms, 0)

    def test_check_connection_unhealthy(self):
        """Test health check on unhealthy connection."""
        bad_conn = Mock()
        bad_conn.cursor.side_effect = Exception("Connection lost")

        result = self.health_checker.check_connection(bad_conn)

        self.assertIsInstance(result, HealthStatus)
        self.assertEqual(result.state, HealthState.UNHEALTHY)
        self.assertIn("failed", result.message.lower())
        self.assertIsNotNone(result.details)
        self.assertIn("error", result.details)

    def test_check_connection_wrong_result(self):
        """Test health check with unexpected result."""
        # Create a mock connection with wrong result
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (2,)  # Wrong value
        mock_cursor.execute = Mock()
        mock_cursor.close = Mock()
        mock_conn.cursor.return_value = mock_cursor

        result = self.health_checker.check_connection(mock_conn)

        self.assertEqual(result.state, HealthState.UNHEALTHY)
        self.assertIn("unexpected", result.message.lower())


class TestPostgresHealthCheckerPool(unittest.TestCase):
    """Test pool health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.postgres_config = PostgresPoolConfig(
            host="localhost",
            port=5432,
            database="test_db",
            user="test_user",
            password="test_pass",
        )
        self.mock_pool = Mock()
        self.mock_pool.config = self.postgres_config
        self.mock_pool.pool_status = Mock(
            return_value={
                "total_connections": 5,
                "active_connections": 2,
                "idle_connections": 3,
                "max_connections": 10,
            }
        )
        self.health_checker = PostgresHealthChecker(self.mock_pool)

    def test_check_pool_healthy(self):
        """Test health check on healthy pool."""
        result = self.health_checker.check_pool()

        self.assertIsInstance(result, HealthStatus)
        self.assertEqual(result.state, HealthState.HEALTHY)
        self.assertIsNotNone(result.response_time_ms)
        self.assertIn("total_connections", result.details)
        self.assertIn("utilization_percent", result.details)

    def test_check_pool_degraded(self):
        """Test health check on degraded pool."""
        # Simulate high utilization
        self.mock_pool.pool_status.return_value = {
            "total_connections": 10,
            "active_connections": 8,  # 80% utilization
            "idle_connections": 2,
            "max_connections": 10,
        }

        result = self.health_checker.check_pool()

        self.assertEqual(result.state, HealthState.DEGRADED)
        self.assertIn("high", result.message.lower())

    def test_check_pool_unhealthy(self):
        """Test health check on unhealthy pool."""
        # Simulate very high utilization
        self.mock_pool.pool_status.return_value = {
            "total_connections": 10,
            "active_connections": 10,  # 100% utilization
            "idle_connections": 0,
            "max_connections": 10,
        }

        result = self.health_checker.check_pool()

        self.assertEqual(result.state, HealthState.UNHEALTHY)
        self.assertIn("capacity", result.message.lower())

    def test_check_pool_error(self):
        """Test health check when pool status fails."""
        self.mock_pool.pool_status.side_effect = Exception("Pool error")

        result = self.health_checker.check_pool()

        self.assertEqual(result.state, HealthState.UNHEALTHY)
        self.assertIn("failed", result.message.lower())


class TestPostgresHealthCheckerDatabase(unittest.TestCase):
    """Test database health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.postgres_config = PostgresPoolConfig(
            host="localhost",
            port=5432,
            database="test_db",
            user="test_user",
            password="test_pass",
        )
        self.mock_pool = Mock()
        self.mock_pool.config = self.postgres_config
        self.mock_pool.pool_status = Mock(
            return_value={
                "total_connections": 5,
                "active_connections": 2,
                "idle_connections": 3,
                "max_connections": 10,
            }
        )
        self.health_checker = PostgresHealthChecker(self.mock_pool)
        self.mock_psycopg2_connection = MockPsycopg2Connection()

    def _mock_get_connection_context(self, connection):
        """Helper to mock get_connection as context manager."""
        mock_context = MagicMock()
        mock_context.__enter__ = Mock(return_value=connection)
        mock_context.__exit__ = Mock(return_value=None)
        self.mock_pool.get_connection = Mock(return_value=mock_context)

    def test_check_database_healthy(self):
        """Test health check on healthy database."""
        self._mock_get_connection_context(self.mock_psycopg2_connection)

        result = self.health_checker.check_database()

        self.assertIsInstance(result, HealthStatus)
        self.assertEqual(result.state, HealthState.HEALTHY)
        self.assertIsNotNone(result.response_time_ms)
        self.assertIn("server_version", result.details)

    def test_check_database_slow(self):
        """Test health check on slow database."""
        # Simulate slow response
        import time

        def slow_execute(*args, **kwargs):
            time.sleep(0.2)  # 200ms delay

        mock_cursor = Mock()
        mock_cursor.execute = slow_execute
        mock_cursor.fetchone = Mock(return_value=("PostgreSQL 15.0",))
        mock_cursor.close = Mock()

        # Create mock connection with slow cursor
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor

        # Mock get_connection context manager
        self._mock_get_connection_context(mock_conn)

        result = self.health_checker.check_database()

        # Should be degraded or unhealthy due to slow response
        self.assertIn(result.state, [HealthState.DEGRADED, HealthState.UNHEALTHY])

    def test_check_database_connection_error(self):
        """Test health check when database connection fails."""
        self.mock_pool.get_connection.side_effect = Exception("Connection failed")

        result = self.health_checker.check_database()

        self.assertEqual(result.state, HealthState.UNHEALTHY)
        self.assertIn("failed", result.message.lower())

    def test_check_database_with_details(self):
        """Test database health check includes details."""
        self._mock_get_connection_context(self.mock_psycopg2_connection)

        result = self.health_checker.check_database()

        self.assertIn("server_version", result.details)
        self.assertIn("active_queries", result.details)
        self.assertIn("total_db_connections", result.details)


class TestPostgresHealthCheckerComprehensive(unittest.TestCase):
    """Test comprehensive health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.postgres_config = PostgresPoolConfig(
            host="localhost",
            port=5432,
            database="test_db",
            user="test_user",
            password="test_pass",
        )
        self.mock_pool = Mock()
        self.mock_pool.config = self.postgres_config
        self.mock_pool.pool_status = Mock(
            return_value={
                "total_connections": 5,
                "active_connections": 2,
                "idle_connections": 3,
                "max_connections": 10,
            }
        )
        self.health_checker = PostgresHealthChecker(self.mock_pool)
        self.mock_psycopg2_connection = MockPsycopg2Connection()

    def _mock_get_connection_context(self, connection):
        """Helper to mock get_connection as context manager."""
        mock_context = MagicMock()
        mock_context.__enter__ = Mock(return_value=connection)
        mock_context.__exit__ = Mock(return_value=None)
        self.mock_pool.get_connection = Mock(return_value=mock_context)

    def test_comprehensive_check(self):
        """Test comprehensive health check."""
        self._mock_get_connection_context(self.mock_psycopg2_connection)

        result = self.health_checker.comprehensive_check()

        self.assertIsInstance(result, dict)
        self.assertIn("pool", result)
        self.assertIn("database", result)
        self.assertIn("timestamp", result)

        self.assertIsInstance(result["pool"], HealthStatus)
        self.assertIsInstance(result["database"], HealthStatus)
        self.assertIsInstance(result["timestamp"], datetime)


class TestAsyncCheckConnection(unittest.IsolatedAsyncioTestCase):
    """Test async connection health check."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_asyncpg_connection = MockAsyncpgConnection()

    async def test_async_check_connection_healthy(self):
        """Test async health check on healthy connection."""
        result = await async_check_connection(self.mock_asyncpg_connection)

        self.assertIsInstance(result, HealthStatus)
        self.assertEqual(result.state, HealthState.HEALTHY)
        self.assertIsNotNone(result.response_time_ms)

    async def test_async_check_connection_unhealthy(self):
        """Test async health check on unhealthy connection."""
        bad_conn = Mock()
        bad_conn.fetchval = AsyncMock(side_effect=Exception("Connection lost"))

        result = await async_check_connection(bad_conn)

        self.assertEqual(result.state, HealthState.UNHEALTHY)
        self.assertIn("failed", result.message.lower())

    async def test_async_check_connection_wrong_result(self):
        """Test async health check with unexpected result."""
        conn = Mock()
        conn.fetchval = AsyncMock(return_value=2)  # Wrong value

        result = await async_check_connection(conn)

        self.assertEqual(result.state, HealthState.UNHEALTHY)
        self.assertIn("unexpected", result.message.lower())


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


class TestHealthCheckerUtilization(unittest.TestCase):
    """Test utilization calculations."""

    def setUp(self):
        """Set up test fixtures."""
        self.postgres_config = PostgresPoolConfig(
            host="localhost",
            port=5432,
            database="test_db",
            user="test_user",
            password="test_pass",
        )
        self.mock_pool = Mock()
        self.mock_pool.config = self.postgres_config
        self.mock_pool.pool_status = Mock(
            return_value={
                "total_connections": 5,
                "active_connections": 2,
                "idle_connections": 3,
                "max_connections": 10,
            }
        )
        self.health_checker = PostgresHealthChecker(self.mock_pool)

    def test_zero_max_connections(self):
        """Test health check with zero max connections."""
        self.mock_pool.pool_status.return_value = {
            "total_connections": 0,
            "active_connections": 0,
            "idle_connections": 0,
            "max_connections": 0,
        }

        result = self.health_checker.check_pool()

        # Should handle division by zero gracefully
        self.assertIsInstance(result, HealthStatus)

    def test_utilization_thresholds(self):
        """Test different utilization thresholds."""
        # Test 50% utilization (healthy)
        self.mock_pool.pool_status.return_value = {
            "total_connections": 10,
            "active_connections": 5,
            "idle_connections": 5,
            "max_connections": 10,
        }

        result = self.health_checker.check_pool()
        self.assertEqual(result.state, HealthState.HEALTHY)

        # Test 75% utilization (degraded)
        self.mock_pool.pool_status.return_value = {
            "total_connections": 10,
            "active_connections": 7.5,
            "idle_connections": 2.5,
            "max_connections": 10,
        }

        result = self.health_checker.check_pool()
        self.assertEqual(result.state, HealthState.DEGRADED)

        # Test 95% utilization (unhealthy)
        self.mock_pool.pool_status.return_value = {
            "total_connections": 10,
            "active_connections": 9.5,
            "idle_connections": 0.5,
            "max_connections": 10,
        }

        result = self.health_checker.check_pool()
        self.assertEqual(result.state, HealthState.UNHEALTHY)


class TestHealthCheckerResponseTime(unittest.TestCase):
    """Test response time tracking."""

    def setUp(self):
        """Set up test fixtures."""
        self.postgres_config = PostgresPoolConfig(
            host="localhost",
            port=5432,
            database="test_db",
            user="test_user",
            password="test_pass",
        )
        self.mock_pool = Mock()
        self.mock_pool.config = self.postgres_config
        self.mock_pool.pool_status = Mock(
            return_value={
                "total_connections": 5,
                "active_connections": 2,
                "idle_connections": 3,
                "max_connections": 10,
            }
        )
        self.health_checker = PostgresHealthChecker(self.mock_pool)
        self.mock_psycopg2_connection = MockPsycopg2Connection()

    def _mock_get_connection_context(self, connection):
        """Helper to mock get_connection as context manager."""
        mock_context = MagicMock()
        mock_context.__enter__ = Mock(return_value=connection)
        mock_context.__exit__ = Mock(return_value=None)
        self.mock_pool.get_connection = Mock(return_value=mock_context)

    def test_response_time_recorded(self):
        """Test that response time is recorded."""
        import time

        # Add a small delay to the mock to ensure response time > 0
        original_cursor = self.mock_psycopg2_connection.cursor

        def delayed_cursor():
            time.sleep(0.001)  # 1ms delay
            return original_cursor()

        self.mock_psycopg2_connection.cursor = delayed_cursor
        self._mock_get_connection_context(self.mock_psycopg2_connection)

        result = self.health_checker.check_database()

        self.assertIsNotNone(result.response_time_ms)
        self.assertGreater(result.response_time_ms, 0)

    def test_response_time_on_error(self):
        """Test response time is recorded even on error."""
        self.mock_pool.pool_status.side_effect = Exception("Error")

        result = self.health_checker.check_pool()

        self.assertIsNotNone(result.response_time_ms)
        self.assertGreaterEqual(result.response_time_ms, 0)


if __name__ == "__main__":
    unittest.main()
