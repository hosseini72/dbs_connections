"""Unit tests for Redis configuration."""

import sys
from pathlib import Path
import os
import unittest

# Add parent directory to path to allow imports
# test_redis_config.py is at: db_connections/tests/unit/connectors/redis/
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
        / "redis"
        / "test_redis_config.py"
    )

parent_dir = _file_path.parent.parent.parent.parent.parent.parent
parent_dir_str = str(parent_dir)
if parent_dir_str not in sys.path:
    sys.path.insert(0, parent_dir_str)

from db_connections.scr.all_db_connectors.connectors.redis.config import (  # noqa: E402, E501
    RedisPoolConfig,
)


class TestRedisPoolConfig(unittest.TestCase):
    """Test RedisPoolConfig class."""

    def test_default_values(self):
        """Test default configuration values."""
        config = RedisPoolConfig(host="localhost")

        self.assertEqual(config.host, "localhost")
        self.assertEqual(config.port, 6379)
        self.assertIsNone(config.password)
        self.assertEqual(config.db, 0)
        self.assertEqual(config.timeout, 10)
        self.assertEqual(config.max_connections, 10)
        self.assertEqual(config.min_connections, 1)

    def test_custom_values(self):
        """Test custom configuration values."""
        config = RedisPoolConfig(
            host="redis.example.com",
            port=6380,
            password="secret",
            db=1,
            timeout=30,
            max_connections=20,
            min_connections=5,
            max_idle_time=120,
            max_lifetime=7200,
        )

        self.assertEqual(config.host, "redis.example.com")
        self.assertEqual(config.port, 6380)
        self.assertEqual(config.password, "secret")
        self.assertEqual(config.db, 1)
        self.assertEqual(config.timeout, 30)
        self.assertEqual(config.max_connections, 20)
        self.assertEqual(config.min_connections, 5)
        self.assertEqual(config.max_idle_time, 120)
        self.assertEqual(config.max_lifetime, 7200)

    def test_missing_required_fields(self):
        """Test validation of required fields."""
        with self.assertRaisesRegex(ValueError, "host is required"):
            RedisPoolConfig(host="")

        with self.assertRaisesRegex(ValueError, "host is required"):
            RedisPoolConfig(host=None)

    def test_invalid_port(self):
        """Test validation of port."""
        with self.assertRaisesRegex(ValueError, "port must be between"):
            RedisPoolConfig(host="localhost", port=0)

        with self.assertRaisesRegex(ValueError, "port must be between"):
            RedisPoolConfig(host="localhost", port=65536)

    def test_invalid_db(self):
        """Test validation of database number."""
        with self.assertRaisesRegex(ValueError, "db must be between"):
            RedisPoolConfig(host="localhost", db=-1)

        with self.assertRaisesRegex(ValueError, "db must be between"):
            RedisPoolConfig(host="localhost", db=16)

    def test_invalid_timeout(self):
        """Test validation of timeout."""
        with self.assertRaisesRegex(ValueError, "timeout must be positive"):
            RedisPoolConfig(host="localhost", timeout=-1)

    def test_invalid_max_connections(self):
        """Test validation of max_connections."""
        with self.assertRaisesRegex(ValueError, "max_connections must be positive"):
            RedisPoolConfig(host="localhost", max_connections=0)

    def test_invalid_min_connections(self):
        """Test validation of min_connections."""
        with self.assertRaisesRegex(ValueError, "min_connections must be positive"):
            RedisPoolConfig(host="localhost", min_connections=0)

    def test_min_greater_than_max(self):
        """Test validation that min_connections <= max_connections."""
        with self.assertRaisesRegex(ValueError, "min_connections cannot be greater"):
            RedisPoolConfig(host="localhost", min_connections=20, max_connections=10)

    def test_get_connection_params(self):
        """Test connection parameters generation."""
        config = RedisPoolConfig(
            host="localhost",
            port=6379,
            password="testpass",
            db=1,
            timeout=15,
            socket_timeout=5,
            socket_connect_timeout=3,
        )

        params = config.get_connection_params()

        self.assertEqual(params["host"], "localhost")
        self.assertEqual(params["port"], 6379)
        self.assertEqual(params["password"], "testpass")
        self.assertEqual(params["db"], 1)
        self.assertEqual(params["socket_timeout"], 5)
        self.assertEqual(params["socket_connect_timeout"], 3)

    def test_get_connection_params_no_password(self):
        """Test connection parameters without password."""
        config = RedisPoolConfig(host="localhost", port=6379)

        params = config.get_connection_params()

        self.assertNotIn("password", params)

    def test_get_connection_params_with_ssl(self):
        """Test connection parameters with SSL."""
        config = RedisPoolConfig(
            host="localhost",
            ssl=True,
            ssl_cert_reqs="required",
            ssl_ca_certs="/path/to/ca.crt",
        )

        params = config.get_connection_params()

        self.assertTrue(params["ssl"])
        self.assertEqual(params["ssl_cert_reqs"], "required")
        self.assertEqual(params["ssl_ca_certs"], "/path/to/ca.crt")

    def test_get_connection_url(self):
        """Test connection URL generation."""
        config = RedisPoolConfig(host="localhost", port=6379, password="testpass", db=1)

        url = config.get_connection_url()

        self.assertTrue(url.startswith("redis://"))
        self.assertIn("localhost:6379", url)
        self.assertIn("/1", url)

    def test_get_connection_url_no_password(self):
        """Test connection URL without password."""
        config = RedisPoolConfig(host="localhost", port=6379)

        url = config.get_connection_url()

        self.assertEqual(url, "redis://localhost:6379/0")

    def test_get_connection_url_with_ssl(self):
        """Test connection URL with SSL (rediss://)."""
        config = RedisPoolConfig(host="localhost", port=6379, ssl=True)

        url = config.get_connection_url()

        self.assertTrue(url.startswith("rediss://"))

    def test_from_url(self):
        """Test creating config from URL."""
        url = "redis://user:pass@localhost:6379/1"

        config = RedisPoolConfig.from_url(url)

        self.assertEqual(config.connection_url, url)

    def test_from_url_with_additional_params(self):
        """Test creating config from URL with extra parameters."""
        url = "redis://localhost:6379/0"

        config = RedisPoolConfig.from_url(url, max_connections=20, min_connections=5)

        self.assertEqual(config.connection_url, url)
        self.assertEqual(config.max_connections, 20)
        self.assertEqual(config.min_connections, 5)

    def test_from_env(self):
        """Test creating config from environment variables."""
        # Set environment variables
        os.environ["REDIS_HOST"] = "envhost"
        os.environ["REDIS_PORT"] = "6380"
        os.environ["REDIS_PASSWORD"] = "envpass"
        os.environ["REDIS_DB"] = "2"

        try:
            config = RedisPoolConfig.from_env()

            self.assertEqual(config.host, "envhost")
            self.assertEqual(config.port, 6380)
            self.assertEqual(config.password, "envpass")
            self.assertEqual(config.db, 2)
        finally:
            # Cleanup
            keys = ["HOST", "PORT", "PASSWORD", "DB"]
            for key in keys:
                os.environ.pop(f"REDIS_{key}", None)

    def test_from_env_custom_prefix(self):
        """Test creating config from environment with custom prefix."""
        # Set environment variables
        os.environ["CACHE_HOST"] = "customhost"
        os.environ["CACHE_PORT"] = "6381"
        os.environ["CACHE_DB"] = "3"

        try:
            config = RedisPoolConfig.from_env(prefix="CACHE_")

            self.assertEqual(config.host, "customhost")
            self.assertEqual(config.port, 6381)
            self.assertEqual(config.db, 3)
        finally:
            # Cleanup
            for key in ["HOST", "PORT", "DB"]:
                os.environ.pop(f"CACHE_{key}", None)

    def test_from_env_defaults(self):
        """Test from_env uses defaults for missing vars."""
        # Make sure env vars don't exist
        for key in ["HOST", "PORT", "DB"]:
            os.environ.pop(f"TEST_REDIS_{key}", None)

        config = RedisPoolConfig.from_env(prefix="TEST_REDIS_")

        self.assertEqual(config.host, "localhost")
        self.assertEqual(config.port, 6379)
        self.assertEqual(config.db, 0)

    def test_from_env_empty_url(self):
        """Test from_env with empty REDIS_URL doesn't break."""
        # Set REDIS_URL to empty string
        os.environ["REDIS_URL"] = ""
        os.environ["REDIS_HOST"] = "testhost"
        os.environ["REDIS_PORT"] = "6380"

        try:
            config = RedisPoolConfig.from_env()

            # Empty URL should be ignored, individual params used
            self.assertIsNone(config.connection_string)
            self.assertEqual(config.host, "testhost")
            self.assertEqual(config.port, 6380)

            # Should not raise TypeError when getting params
            params = config.get_connection_params()
            self.assertEqual(params["host"], "testhost")
            self.assertEqual(params["port"], 6380)
        finally:
            # Cleanup
            for key in ["URL", "HOST", "PORT"]:
                os.environ.pop(f"REDIS_{key}", None)

    def test_from_env_valid_url(self):
        """Test from_env with valid REDIS_URL."""
        os.environ["REDIS_URL"] = "redis://user:pass@urlhost:6381/3"

        try:
            config = RedisPoolConfig.from_env()

            # URL should be used
            self.assertEqual(
                config.connection_string, "redis://user:pass@urlhost:6381/3"
            )

            # Should parse URL correctly
            params = config.get_connection_params()
            self.assertEqual(params["host"], "urlhost")
            self.assertEqual(params["port"], 6381)
            self.assertEqual(params["db"], 3)
        finally:
            os.environ.pop("REDIS_URL", None)

    def test_connection_url_overrides(self):
        """Test that connection_url overrides individual params."""
        config = RedisPoolConfig(
            connection_url="redis://user:pass@remote:6380/2",
            host="localhost",  # Should be ignored
            port=6379,  # Should be ignored
        )

        params = config.get_connection_params()

        # URL should be parsed into individual parameters
        self.assertEqual(params["host"], "remote")
        self.assertEqual(params["port"], 6380)
        self.assertEqual(params["db"], 2)
        self.assertEqual(params["username"], "user")
        self.assertEqual(params["password"], "pass")

    def test_decode_responses(self):
        """Test decode_responses configuration."""
        config = RedisPoolConfig(host="localhost", decode_responses=True)

        params = config.get_connection_params()

        self.assertTrue(params["decode_responses"])

    def test_health_check_interval(self):
        """Test health check interval configuration."""
        config = RedisPoolConfig(host="localhost", health_check_interval=30)

        self.assertEqual(config.health_check_interval, 30)

    def test_retry_on_timeout(self):
        """Test retry_on_timeout configuration."""
        config = RedisPoolConfig(host="localhost", retry_on_timeout=True)

        params = config.get_connection_params()

        self.assertTrue(params["retry_on_timeout"])


class TestRedisPoolConfigEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions."""

    def test_special_characters_in_password(self):
        """Test password with special characters."""
        config = RedisPoolConfig(host="localhost", password="p@ssw0rd!#$%")

        params = config.get_connection_params()
        self.assertEqual(params["password"], "p@ssw0rd!#$%")

    def test_ipv6_host(self):
        """Test IPv6 host address."""
        config = RedisPoolConfig(host="::1", port=6379)

        self.assertEqual(config.host, "::1")

    def test_unix_socket_host(self):
        """Test Unix socket host."""
        config = RedisPoolConfig(host="/var/run/redis/redis.sock")

        self.assertEqual(config.host, "/var/run/redis/redis.sock")

    def test_connection_url_with_query_params(self):
        """Test connection URL with query parameters."""
        url = "redis://localhost:6379/0?socket_timeout=5&socket_connect_timeout=3"
        config = RedisPoolConfig.from_url(url)

        self.assertEqual(config.connection_url, url)

    def test_max_idle_time_validation(self):
        """Test max_idle_time validation."""
        with self.assertRaisesRegex(ValueError, "max_idle_time must be positive"):
            RedisPoolConfig(host="localhost", max_idle_time=-1)

    def test_max_lifetime_validation(self):
        """Test max_lifetime validation."""
        with self.assertRaisesRegex(ValueError, "max_lifetime must be positive"):
            RedisPoolConfig(host="localhost", max_lifetime=-1)


if __name__ == "__main__":
    unittest.main()
