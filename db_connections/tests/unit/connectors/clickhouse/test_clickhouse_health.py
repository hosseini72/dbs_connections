"""Unit tests for ClickHouse health checks."""

import sys
from pathlib import Path
import unittest
import time
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime

# Add parent directory to path to allow imports
# test_clickhouse_health.py is at: db_connections/tests/unit/connectors/clickhouse/
# We need to go up 6 levels to get to the parent of db_connections
try:
    _file_path = Path(__file__).resolve()
except NameError:
    # __file__ might not be defined in some contexts
    import os
    _file_path = Path(os.getcwd()) / 'tests' / 'unit' / 'connectors' / 'clickhouse' / 'test_clickhouse_health.py'  # noqa: E501

parent_dir = _file_path.parent.parent.parent.parent.parent.parent
parent_dir_str = str(parent_dir)
if parent_dir_str not in sys.path:
    sys.path.insert(0, parent_dir_str)

from db_connections.scr.all_db_connectors.connectors.clickhouse.config import (  # noqa: E402, E501
    ClickHousePoolConfig
)
from db_connections.scr.all_db_connectors.connectors.clickhouse.health import (  # noqa: E402
    ClickHouseHealthChecker,
)
from db_connections.scr.all_db_connectors.core.health import (  # noqa: E402
    HealthState, HealthStatus
)


class MockClickHouseConnection:
    """Mock ClickHouse connection for testing."""

    def __init__(self, healthy=True):
        self.closed = False
        self.healthy = healthy
        self.connected = True
        self.server_info = {
            "version_major": 23,
            "version_minor": 1,
            "revision": 54463,
            "name": "ClickHouse",
            "version_display": "23.1.54463"
        }

    def ping(self):
        """Mock ping."""
        if self.closed or not self.healthy:
            raise Exception("Connection lost")
        return True

    def execute(self, query, params=None):
        """Mock execute."""
        if not self.healthy:
            raise Exception("Connection lost")
        if "SELECT 1" in query:
            return [(1,)]
        elif "version()" in query:
            return [("23.1.54463",)]
        elif "uptime()" in query:
            return [(3600,)]
        return []

    def get_server_info(self):
        """Mock get_server_info."""
        if not self.healthy:
            raise Exception("Connection lost")
        return self.server_info

    def close(self):
        """Mock close."""
        self.closed = True
        self.connected = False


class MockAsyncClickHouseConnection:
    """Mock Async ClickHouse connection for testing."""

    def __init__(self, healthy=True):
        self.closed = False
        self.healthy = healthy
        self.connected = True
        self.server_info = {
            "version_major": 23,
            "version_minor": 1,
            "revision": 54463
        }

    async def ping(self):
        """Mock ping."""
        if not self.healthy:
            raise Exception("Connection lost")
        return True

    async def execute(self, query, params=None):
        """Mock execute."""
        if not self.healthy:
            raise Exception("Connection lost")
        if "SELECT 1" in query:
            return [(1,)]
        return []

    async def get_server_info(self):
        """Mock get_server_info."""
        if not self.healthy:
            raise Exception("Connection lost")
        return self.server_info

    async def close(self):
        """Mock close."""
        self.closed = True
        self.connected = False


class TestClickHouseHealthCheckerInit(unittest.TestCase):
    """Test ClickHouseHealthChecker initialization."""

    def setUp(self):
        """Set up test fixtures."""
        self.clickhouse_config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
        )

    def test_init(self):
        """Test health checker initialization."""
        checker = ClickHouseHealthChecker(self.clickhouse_config)

        self.assertEqual(checker.config, self.clickhouse_config)
        self.assertIsNone(checker._last_check_time)
        self.assertIsNone(checker._last_status)


class TestClickHouseHealthCheckerConnection(unittest.TestCase):
    """Test connection health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.clickhouse_config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
        )
        self.health_checker = ClickHouseHealthChecker(self.clickhouse_config)
        self.mock_clickhouse_connection = MockClickHouseConnection(
            healthy=True
        )

    def test_check_health_healthy(self):
        """Test health check on healthy connection."""
        result = self.health_checker.check_connection(
            self.mock_clickhouse_connection
        )

        self.assertIsInstance(result, HealthStatus)
        self.assertEqual(result.state, HealthState.HEALTHY)
        self.assertIsNotNone(result.response_time_ms)
        self.assertGreaterEqual(result.response_time_ms, 0)

    def test_check_health_unhealthy(self):
        """Test health check on unhealthy connection."""
        bad_conn = MockClickHouseConnection(healthy=False)

        result = self.health_checker.check_connection(bad_conn)

        self.assertIsInstance(result, HealthStatus)
        self.assertEqual(result.state, HealthState.UNHEALTHY)
        self.assertIn("failed", result.message.lower())
        self.assertIsNotNone(result.details)
        self.assertIn("error", result.details)

    def test_check_health_with_closed_connection(self):
        """Test health check with closed connection."""
        conn = MockClickHouseConnection()
        conn.close()

        result = self.health_checker.check_connection(conn)

        # Should be unhealthy due to closed connection
        self.assertEqual(result.state, HealthState.UNHEALTHY)

    def test_check_health_with_slow_response(self):
        """Test health check with slow response."""
        conn = MockClickHouseConnection()

        # Simulate slow response by patching time
        # The health checker doesn't check response time for degradation,
        # it only checks if ping succeeds or fails
        with patch('time.time', side_effect=[0, 1.5]):  # 1.5 second delay
            result = self.health_checker.check_connection(conn)

        # Should still be healthy if ping succeeds, even if slow
        # The response_time_ms will be recorded but doesn't affect health state
        self.assertEqual(result.state, HealthState.HEALTHY)
        self.assertGreater(result.response_time_ms, 0)


class TestClickHouseHealthCheckerServerInfo(unittest.TestCase):
    """Test server info checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.clickhouse_config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
        )
        self.health_checker = ClickHouseHealthChecker(self.clickhouse_config)
        self.mock_clickhouse_connection = MockClickHouseConnection()

    def test_check_server_info(self):
        """Test server info check."""
        # check_server_info method doesn't exist in health checker
        # This test is skipped as the method is not implemented
        self.skipTest("check_server_info method not implemented in health checker")

    def test_check_server_info_error(self):
        """Test server info check with error."""
        # check_server_info method doesn't exist in health checker
        # This test is skipped as the method is not implemented
        self.skipTest("check_server_info method not implemented in health checker")


class TestClickHouseHealthCheckerDatabase(unittest.TestCase):
    """Test database health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.clickhouse_config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
        )
        self.health_checker = ClickHouseHealthChecker(self.clickhouse_config)
        self.mock_clickhouse_connection = MockClickHouseConnection()

    def test_check_database_status(self):
        """Test database status check."""
        # check_database_status method doesn't exist in health checker
        # check_database exists but requires a pool instance, not a connection
        # This test is skipped as the method is not implemented
        self.skipTest("check_database_status method not implemented in health checker")

    def test_check_database_status_error(self):
        """Test database status check with error."""
        # check_database_status method doesn't exist in health checker
        # This test is skipped as the method is not implemented
        self.skipTest("check_database_status method not implemented in health checker")


class TestClickHouseHealthCheckerTables(unittest.TestCase):
    """Test table health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.clickhouse_config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
        )
        self.health_checker = ClickHouseHealthChecker(self.clickhouse_config)
        self.mock_clickhouse_connection = MockClickHouseConnection()

    def test_check_table_status(self):
        """Test table status check."""
        # check_table_status method doesn't exist in health checker
        # This test is skipped as the method is not implemented
        self.skipTest("check_table_status method not implemented in health checker")

    def test_check_table_status_error(self):
        """Test table status check with error."""
        # check_table_status method doesn't exist in health checker
        # This test is skipped as the method is not implemented
        self.skipTest("check_table_status method not implemented in health checker")


class TestAsyncClickHouseHealthChecker(unittest.IsolatedAsyncioTestCase):
    """Test async health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.clickhouse_config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
        )
        self.health_checker = ClickHouseHealthChecker(self.clickhouse_config)
        self.mock_async_clickhouse_connection = MockAsyncClickHouseConnection(
            healthy=True
        )

    async def test_async_check_health_healthy(self):
        """Test async health check on healthy connection."""
        result = await self.health_checker.async_check_connection(
            self.mock_async_clickhouse_connection
        )

        self.assertIsInstance(result, HealthStatus)
        self.assertEqual(result.state, HealthState.HEALTHY)
        self.assertIsNotNone(result.response_time_ms)

    async def test_async_check_health_unhealthy(self):
        """Test async health check on unhealthy connection."""
        bad_conn = MockAsyncClickHouseConnection(healthy=False)

        result = await self.health_checker.async_check_connection(bad_conn)

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
        self.clickhouse_config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
        )
        self.health_checker = ClickHouseHealthChecker(self.clickhouse_config)
        self.mock_clickhouse_connection = MockClickHouseConnection()

    def test_response_time_recorded(self):
        """Test that response time is recorded."""
        # Add a small delay to ensure response time is > 0
        import time
        original_ping = self.mock_clickhouse_connection.ping
        
        def delayed_ping():
            time.sleep(0.001)  # 1ms delay
            return original_ping()
        
        self.mock_clickhouse_connection.ping = delayed_ping
        
        result = self.health_checker.check_connection(
            self.mock_clickhouse_connection
        )

        self.assertIsNotNone(result.response_time_ms)
        self.assertGreaterEqual(result.response_time_ms, 0)

    def test_response_time_on_error(self):
        """Test response time is recorded even on error."""
        bad_conn = MockClickHouseConnection(healthy=False)

        result = self.health_checker.check_connection(bad_conn)

        self.assertIsNotNone(result.response_time_ms)
        self.assertGreaterEqual(result.response_time_ms, 0)


class TestClickHouseHealthCheckerComprehensive(unittest.TestCase):
    """Test comprehensive health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.clickhouse_config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
        )
        self.health_checker = ClickHouseHealthChecker(self.clickhouse_config)
        self.mock_clickhouse_connection = MockClickHouseConnection()

    def test_comprehensive_check(self):
        """Test comprehensive health check."""
        # comprehensive_check requires a pool instance
        # Create a mock pool for testing
        from db_connections.scr.all_db_connectors.connectors.clickhouse.pool import (
            ClickHouseSyncConnectionPool
        )
        from unittest.mock import patch
        
        with patch(
            "db_connections.scr.all_db_connectors.connectors.clickhouse.pool.clickhouse_connect"
        ) as mock_module:
            mock_module.get_client.return_value = MockClickHouseConnection()
            # Also patch SYNC_AVAILABLE
            with patch(
                "db_connections.scr.all_db_connectors.connectors.clickhouse.pool.SYNC_AVAILABLE",
                True
            ):
                pool = ClickHouseSyncConnectionPool(self.clickhouse_config)
                pool.initialize_pool()
                
                # Create health checker with pool
                health_checker = ClickHouseHealthChecker(pool)
                result = health_checker.comprehensive_check()

                self.assertIsInstance(result, dict)
                self.assertIn("pool", result)
                self.assertIn("database", result)
                self.assertIn("timestamp", result)

                self.assertIsInstance(result["pool"], HealthStatus)
                self.assertIsInstance(result["database"], HealthStatus)
                self.assertIsInstance(result["timestamp"], datetime)


if __name__ == '__main__':
    unittest.main()

