"""Unit tests for Redis health checks."""

import sys
from pathlib import Path
import unittest
import time
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime

# Add parent directory to path to allow imports
# test_redis_health.py is at: db_connections/tests/unit/connectors/redis/
# We need to go up 6 levels to get to the parent of db_connections
try:
    _file_path = Path(__file__).resolve()
except NameError:
    # __file__ might not be defined in some contexts
    import os
    _file_path = Path(os.getcwd()) / 'tests' / 'unit' / 'connectors' / 'redis' / 'test_redis_health.py'

parent_dir = _file_path.parent.parent.parent.parent.parent.parent
parent_dir_str = str(parent_dir)
if parent_dir_str not in sys.path:
    sys.path.insert(0, parent_dir_str)

from db_connections.scr.all_db_connectors.connectors.redis.config import (  # noqa: E402, E501
    RedisPoolConfig
)
from db_connections.scr.all_db_connectors.connectors.redis.health import (  # noqa: E402
    RedisHealthChecker,
)
from db_connections.scr.all_db_connectors.core.health import (  # noqa: E402
    HealthState, HealthStatus
)


class MockRedisConnection:
    """Mock Redis connection for testing."""

    def __init__(self, healthy=True):
        self.closed = False
        self.healthy = healthy
        self._info = {
            "redis_version": "7.0.0",
            "used_memory": 1024000,
            "maxmemory": 10485760,  # 10MB
            "connected_clients": 1,
            "uptime_in_seconds": 3600,
            "role": "master",
            "connected_slaves": 0,
            "mem_fragmentation_ratio": 1.2,
            "rdb_changes_since_last_save": 100,
            "rdb_last_save_time": int(time.time()),
            "aof_enabled": 0,
            "instantaneous_ops_per_sec": 100,
            "rejected_connections": 0,
            "evicted_keys": 0,
            "blocked_clients": 0,
        }

    def ping(self):
        """Mock ping."""
        if not self.healthy:
            raise Exception("Connection lost")
        return True

    def info(self, section=None):
        """Mock info."""
        if not self.healthy:
            raise Exception("Connection lost")
        return self._info.copy()

    def set_info(self, key, value):
        """Set info value for testing."""
        self._info[key] = value


class MockAsyncRedisConnection:
    """Mock Async Redis connection for testing."""

    def __init__(self, healthy=True):
        self.closed = False
        self.healthy = healthy
        self._info = {
            "redis_version": "7.0.0",
            "used_memory": 1024000,
            "maxmemory": 10485760,
            "connected_clients": 1,
            "uptime_in_seconds": 3600,
            "role": "master",
            "connected_slaves": 0,
            "mem_fragmentation_ratio": 1.2,
            "rdb_changes_since_last_save": 100,
            "rdb_last_save_time": int(time.time()),
            "aof_enabled": 0,
            "instantaneous_ops_per_sec": 100,
            "rejected_connections": 0,
            "evicted_keys": 0,
            "blocked_clients": 0,
        }

    async def ping(self):
        """Mock ping."""
        if not self.healthy:
            raise Exception("Connection lost")
        return True

    async def info(self, section=None):
        """Mock info."""
        if not self.healthy:
            raise Exception("Connection lost")
        return self._info.copy()

    def set_info(self, key, value):
        """Set info value for testing."""
        self._info[key] = value


class TestRedisHealthCheckerInit(unittest.TestCase):
    """Test RedisHealthChecker initialization."""

    def setUp(self):
        """Set up test fixtures."""
        self.redis_config = RedisPoolConfig(
            host="localhost",
            port=6379,
        )

    def test_init(self):
        """Test health checker initialization."""
        checker = RedisHealthChecker(self.redis_config)

        self.assertEqual(checker.config, self.redis_config)
        self.assertIsNone(checker._last_check_time)
        self.assertIsNone(checker._last_status)


class TestRedisHealthCheckerConnection(unittest.TestCase):
    """Test connection health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.redis_config = RedisPoolConfig(
            host="localhost",
            port=6379,
        )
        self.health_checker = RedisHealthChecker(self.redis_config)
        self.mock_redis_connection = MockRedisConnection(healthy=True)

    def test_check_health_healthy(self):
        """Test health check on healthy connection."""
        result = self.health_checker.check_health(
            self.mock_redis_connection
        )

        self.assertIsInstance(result, HealthStatus)
        self.assertEqual(result.state, HealthState.HEALTHY)
        self.assertIsNotNone(result.response_time_ms)
        self.assertGreaterEqual(result.response_time_ms, 0)

    def test_check_health_unhealthy(self):
        """Test health check on unhealthy connection."""
        bad_conn = MockRedisConnection(healthy=False)

        result = self.health_checker.check_health(bad_conn)

        self.assertIsInstance(result, HealthStatus)
        self.assertEqual(result.state, HealthState.UNHEALTHY)
        self.assertIn("failed", result.message.lower())
        self.assertIsNotNone(result.details)
        self.assertIn("error", result.details)

    def test_check_health_with_high_memory(self):
        """Test health check with high memory usage."""
        conn = MockRedisConnection()
        # Set memory usage to 95%
        conn.set_info("used_memory", 9961472)  # ~95% of 10MB
        conn.set_info("maxmemory", 10485760)

        result = self.health_checker.check_health(conn)

        # Should be unhealthy due to high memory
        self.assertIn(
            result.state,
            [HealthState.DEGRADED, HealthState.UNHEALTHY]
        )
        self.assertIn("memory", result.message.lower())

    def test_check_health_with_slow_response(self):
        """Test health check with slow response."""
        conn = MockRedisConnection()

        # Simulate slow response by patching perf_counter
        # First call returns 0, second call returns 1.5 (1.5 second delay = 1500ms)
        with patch('time.perf_counter', side_effect=[0, 1.5]):
            result = self.health_checker.check_health(conn)

        # Should be degraded or unhealthy due to slow response (1500ms > 1000ms threshold)
        self.assertIn(
            result.state,
            [HealthState.DEGRADED, HealthState.UNHEALTHY]
        )

    def test_check_health_with_rejected_connections(self):
        """Test health check with rejected connections."""
        conn = MockRedisConnection()
        conn.set_info("rejected_connections", 10)

        result = self.health_checker.check_health(conn)

        # Should be unhealthy
        self.assertEqual(result.state, HealthState.UNHEALTHY)
        self.assertIn("rejected", result.message.lower())


class TestRedisHealthCheckerReplication(unittest.TestCase):
    """Test replication health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.redis_config = RedisPoolConfig(
            host="localhost",
            port=6379,
        )
        self.health_checker = RedisHealthChecker(self.redis_config)
        self.mock_redis_connection = MockRedisConnection()

    def test_check_replication_lag_master(self):
        """Test replication lag check on master."""
        conn = MockRedisConnection()
        conn.set_info("role", "master")

        lag = self.health_checker.check_replication_lag(conn)

        # Should return None for master
        self.assertIsNone(lag)

    def test_check_replication_lag_slave(self):
        """Test replication lag check on slave."""
        conn = MockRedisConnection()
        conn.set_info("role", "slave")
        conn.set_info("master_repl_offset", 1000)
        conn.set_info("slave_repl_offset", 900)

        # Mock info method to return replication info
        def mock_info(section):
            if section == "replication":
                return {
                    "role": "slave",
                    "master_repl_offset": 1000,
                    "slave_repl_offset": 900,
                }
            return conn._info

        conn.info = mock_info

        lag = self.health_checker.check_replication_lag(conn)

        # Should return lag value
        self.assertIsNotNone(lag)

    def test_check_replication_lag_error(self):
        """Test replication lag check with error."""
        conn = Mock()
        conn.info.side_effect = Exception("Error")

        lag = self.health_checker.check_replication_lag(conn)

        # Should return None on error
        self.assertIsNone(lag)


class TestRedisHealthCheckerMemory(unittest.TestCase):
    """Test memory pressure checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.redis_config = RedisPoolConfig(
            host="localhost",
            port=6379,
        )
        self.health_checker = RedisHealthChecker(self.redis_config)
        self.mock_redis_connection = MockRedisConnection()

    def test_check_memory_pressure(self):
        """Test memory pressure check."""
        conn = MockRedisConnection()
        conn.set_info("used_memory", 5242880)  # 5MB
        conn.set_info("used_memory_rss", 6291456)  # 6MB
        conn.set_info("maxmemory", 10485760)  # 10MB
        conn.set_info("mem_fragmentation_ratio", 1.2)
        conn.set_info("evicted_keys", 0)

        def mock_info(section):
            if section == "memory":
                return {
                    "used_memory": 5242880,
                    "used_memory_rss": 6291456,
                    "maxmemory": 10485760,
                    "mem_fragmentation_ratio": 1.2,
                    "evicted_keys": 0,
                }
            return conn._info

        conn.info = mock_info

        result = self.health_checker.check_memory_pressure(conn)

        self.assertIsInstance(result, dict)
        self.assertIn("used_memory_mb", result)
        self.assertIn("used_memory_rss_mb", result)
        self.assertIn("max_memory_mb", result)
        self.assertIn("memory_fragmentation_ratio", result)
        self.assertIn("evicted_keys", result)
        self.assertIn("memory_usage_percent", result)

    def test_check_memory_pressure_error(self):
        """Test memory pressure check with error."""
        conn = Mock()
        conn.info.side_effect = Exception("Error")

        with self.assertRaises(Exception):
            self.health_checker.check_memory_pressure(conn)


class TestRedisHealthCheckerPersistence(unittest.TestCase):
    """Test persistence status checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.redis_config = RedisPoolConfig(
            host="localhost",
            port=6379,
        )
        self.health_checker = RedisHealthChecker(self.redis_config)
        self.mock_redis_connection = MockRedisConnection()

    def test_check_persistence_status(self):
        """Test persistence status check."""
        conn = MockRedisConnection()
        conn.set_info("rdb_bgsave_in_progress", 0)
        conn.set_info("rdb_last_save_time", int(time.time()))
        conn.set_info("rdb_changes_since_last_save", 100)
        conn.set_info("rdb_last_bgsave_status", "ok")
        conn.set_info("aof_enabled", 0)

        def mock_info(section):
            if section == "persistence":
                return {
                    "rdb_bgsave_in_progress": 0,
                    "rdb_last_save_time": int(time.time()),
                    "rdb_changes_since_last_save": 100,
                    "rdb_last_bgsave_status": "ok",
                    "aof_enabled": 0,
                }
            return conn._info

        conn.info = mock_info

        result = self.health_checker.check_persistence_status(conn)

        self.assertIsInstance(result, dict)
        self.assertIn("rdb_enabled", result)
        self.assertIn("rdb_last_save_time", result)
        self.assertIn("rdb_changes_since_last_save", result)
        self.assertIn("rdb_last_status", result)
        self.assertIn("aof_enabled", result)

    def test_check_persistence_status_with_aof(self):
        """Test persistence status check with AOF enabled."""
        conn = MockRedisConnection()
        conn.set_info("aof_enabled", 1)
        conn.set_info("aof_current_size", 1024000)
        conn.set_info("aof_base_size", 512000)
        conn.set_info("aof_last_rewrite_status", "ok")

        def mock_info(section):
            if section == "persistence":
                return {
                    "rdb_bgsave_in_progress": 0,
                    "rdb_last_save_time": int(time.time()),
                    "rdb_changes_since_last_save": 100,
                    "rdb_last_bgsave_status": "ok",
                    "aof_enabled": 1,
                    "aof_current_size": 1024000,
                    "aof_base_size": 512000,
                    "aof_last_rewrite_status": "ok",
                }
            return conn._info

        conn.info = mock_info

        result = self.health_checker.check_persistence_status(conn)

        self.assertTrue(result["aof_enabled"])
        self.assertIn("aof_current_size", result)
        self.assertIn("aof_base_size", result)
        self.assertIn("aof_last_rewrite_status", result)

    def test_check_persistence_status_error(self):
        """Test persistence status check with error."""
        conn = Mock()
        conn.info.side_effect = Exception("Error")

        with self.assertRaises(Exception):
            self.health_checker.check_persistence_status(conn)


class TestRedisHealthCheckerSlowLog(unittest.TestCase):
    """Test slow log checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.redis_config = RedisPoolConfig(
            host="localhost",
            port=6379,
        )
        self.health_checker = RedisHealthChecker(self.redis_config)
        self.mock_redis_connection = MockRedisConnection()

    def test_get_slow_log(self):
        """Test getting slow log."""
        conn = MockRedisConnection()
        conn.slowlog_get = Mock(return_value=[
            {"id": 1, "duration": 1000, "command": "GET key"},
            {"id": 2, "duration": 2000, "command": "SET key value"},
        ])

        result = self.health_checker.get_slow_log(conn, count=10)

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)
        conn.slowlog_get.assert_called_once_with(10)

    def test_get_slow_log_error(self):
        """Test getting slow log with error."""
        conn = Mock()
        conn.slowlog_get.side_effect = Exception("Error")

        result = self.health_checker.get_slow_log(conn, count=10)

        # Should return empty list on error
        self.assertEqual(result, [])


class TestAsyncRedisHealthChecker(unittest.IsolatedAsyncioTestCase):
    """Test async health checks."""

    def setUp(self):
        """Set up test fixtures."""
        self.redis_config = RedisPoolConfig(
            host="localhost",
            port=6379,
        )
        self.health_checker = RedisHealthChecker(self.redis_config)
        self.mock_async_redis_connection = MockAsyncRedisConnection(healthy=True)

    async def test_async_check_health_healthy(self):
        """Test async health check on healthy connection."""
        result = await self.health_checker.async_check_health(
            self.mock_async_redis_connection
        )

        self.assertIsInstance(result, HealthStatus)
        self.assertEqual(result.state, HealthState.HEALTHY)
        self.assertIsNotNone(result.response_time_ms)

    async def test_async_check_health_unhealthy(self):
        """Test async health check on unhealthy connection."""
        bad_conn = MockAsyncRedisConnection(healthy=False)

        result = await self.health_checker.async_check_health(bad_conn)

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
        self.redis_config = RedisPoolConfig(
            host="localhost",
            port=6379,
        )
        self.health_checker = RedisHealthChecker(self.redis_config)
        self.mock_redis_connection = MockRedisConnection()

    def test_response_time_recorded(self):
        """Test that response time is recorded."""
        result = self.health_checker.check_health(
            self.mock_redis_connection
        )

        self.assertIsNotNone(result.response_time_ms)
        self.assertGreater(result.response_time_ms, 0)

    def test_response_time_on_error(self):
        """Test response time is recorded even on error."""
        bad_conn = MockRedisConnection(healthy=False)

        result = self.health_checker.check_health(bad_conn)

        self.assertIsNotNone(result.response_time_ms)
        self.assertGreaterEqual(result.response_time_ms, 0)


if __name__ == '__main__':
    unittest.main()

