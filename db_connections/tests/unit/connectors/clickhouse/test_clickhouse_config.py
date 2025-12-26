"""Unit tests for ClickHouse configuration."""

import sys
from pathlib import Path
import os
import unittest

# Add parent directory to path to allow imports
# test_clickhouse_config.py is at: db_connections/tests/unit/connectors/clickhouse/
# We need to go up 6 levels to get to the parent of db_connections
try:
    _file_path = Path(__file__).resolve()
except NameError:
    # __file__ might not be defined in some contexts
    _file_path = (
        Path(os.getcwd())
        / "tests"
        / "unit"
        / "connectors"
        / "clickhouse"
        / "test_clickhouse_config.py"
    )  # noqa: E501

parent_dir = _file_path.parent.parent.parent.parent.parent.parent
parent_dir_str = str(parent_dir)
if parent_dir_str not in sys.path:
    sys.path.insert(0, parent_dir_str)

from db_connections.scr.all_db_connectors.connectors.clickhouse.config import (  # noqa: E402, E501
    ClickHousePoolConfig,
)


class TestClickHousePoolConfig(unittest.TestCase):
    """Test ClickHousePoolConfig class."""

    def test_default_values(self):
        """Test default configuration values."""
        config = ClickHousePoolConfig(host="localhost")

        self.assertEqual(config.host, "localhost")
        self.assertEqual(config.port, 9000)
        self.assertIsNone(config.username)
        self.assertIsNone(config.password)
        self.assertEqual(config.database, "default")
        self.assertEqual(config.timeout, 30)
        self.assertEqual(config.max_connections, 10)
        self.assertEqual(config.min_connections, 1)

    def test_custom_values(self):
        """Test custom configuration values."""
        config = ClickHousePoolConfig(
            host="clickhouse.example.com",
            port=8123,
            username="admin",
            password="secret",
            database="mydb",
            timeout=60,
            max_connections=20,
            min_connections=5,
            max_idle_time=120,
            max_lifetime=7200,
            secure=True,
            verify=True,
        )

        self.assertEqual(config.host, "clickhouse.example.com")
        self.assertEqual(config.port, 8123)
        self.assertEqual(config.username, "admin")
        self.assertEqual(config.password, "secret")
        self.assertEqual(config.database, "mydb")
        self.assertEqual(config.timeout, 60)
        self.assertEqual(config.max_connections, 20)
        self.assertEqual(config.min_connections, 5)
        self.assertEqual(config.max_idle_time, 120)
        self.assertEqual(config.max_lifetime, 7200)
        self.assertTrue(config.secure)
        self.assertTrue(config.verify)

    def test_missing_required_fields(self):
        """Test validation of required fields."""
        with self.assertRaisesRegex(ValueError, "host is required"):
            ClickHousePoolConfig(host="")

        with self.assertRaisesRegex(ValueError, "host is required"):
            ClickHousePoolConfig(host=None)

    def test_invalid_port(self):
        """Test validation of port."""
        with self.assertRaisesRegex(ValueError, "port must be between"):
            ClickHousePoolConfig(host="localhost", port=0)

        with self.assertRaisesRegex(ValueError, "port must be between"):
            ClickHousePoolConfig(host="localhost", port=65536)

    def test_invalid_timeout(self):
        """Test validation of timeout."""
        with self.assertRaisesRegex(ValueError, "timeout must be positive"):
            ClickHousePoolConfig(host="localhost", timeout=-1)

    def test_invalid_max_connections(self):
        """Test validation of max_connections."""
        with self.assertRaisesRegex(ValueError, "max_connections must be positive"):
            ClickHousePoolConfig(host="localhost", max_connections=0)

    def test_invalid_min_connections(self):
        """Test validation of min_connections."""
        with self.assertRaisesRegex(ValueError, "min_connections must be positive"):
            ClickHousePoolConfig(host="localhost", min_connections=0)

    def test_min_greater_than_max(self):
        """Test validation that min_connections <= max_connections."""
        with self.assertRaisesRegex(ValueError, "min_connections cannot be greater"):
            ClickHousePoolConfig(
                host="localhost", min_connections=20, max_connections=10
            )

    def test_get_connection_params(self):
        """Test connection parameters generation."""
        config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
            username="user",
            password="pass",
            database="testdb",
            timeout=15,
            connect_timeout=10,
            send_receive_timeout=30,
        )

        params = config.get_connection_params()

        self.assertEqual(params["host"], "localhost")
        self.assertEqual(params["port"], 9000)
        self.assertEqual(params["user"], "user")
        self.assertEqual(params["password"], "pass")
        self.assertEqual(params["database"], "testdb")
        self.assertEqual(params["timeout"], 15)
        self.assertEqual(params["connect_timeout"], 10)
        self.assertEqual(params["send_receive_timeout"], 30)

    def test_get_connection_params_no_auth(self):
        """Test connection parameters without authentication."""
        config = ClickHousePoolConfig(host="localhost", port=9000)

        params = config.get_connection_params()

        self.assertNotIn("user", params)
        self.assertNotIn("password", params)

    def test_get_connection_params_with_ssl(self):
        """Test connection parameters with SSL."""
        config = ClickHousePoolConfig(
            host="localhost",
            secure=True,
            verify=True,
            ca_certs="/path/to/ca.crt",
            cert="/path/to/client.crt",
            key="/path/to/client.key",
        )

        params = config.get_connection_params()

        self.assertTrue(params["secure"])
        self.assertTrue(params["verify"])
        self.assertEqual(params["ca_certs"], "/path/to/ca.crt")
        self.assertEqual(params["cert"], "/path/to/client.crt")
        self.assertEqual(params["key"], "/path/to/client.key")

    def test_get_connection_url(self):
        """Test connection URL generation."""
        config = ClickHousePoolConfig(
            host="localhost",
            port=9000,
            username="user",
            password="pass",
            database="testdb",
        )

        url = config.get_connection_url()

        self.assertTrue(url.startswith("clickhouse://"))
        self.assertIn("user:pass", url)
        self.assertIn("localhost:9000", url)
        self.assertIn("/testdb", url)

    def test_get_connection_url_no_auth(self):
        """Test connection URL without authentication."""
        config = ClickHousePoolConfig(host="localhost", port=9000)

        url = config.get_connection_url()

        self.assertEqual(url, "clickhouse://localhost:9000/default")

    def test_get_connection_url_with_ssl(self):
        """Test connection URL with SSL (clickhouses://)."""
        config = ClickHousePoolConfig(host="localhost", port=9440, secure=True)

        url = config.get_connection_url()

        self.assertTrue(url.startswith("clickhouses://"))

    def test_from_url(self):
        """Test creating config from URL."""
        url = "clickhouse://user:pass@localhost:9000/testdb"

        config = ClickHousePoolConfig.from_url(url)

        self.assertEqual(config.connection_url, url)

    def test_from_url_with_additional_params(self):
        """Test creating config from URL with extra parameters."""
        url = "clickhouse://localhost:9000/default"

        config = ClickHousePoolConfig.from_url(
            url, max_connections=20, min_connections=5
        )

        self.assertEqual(config.connection_url, url)
        self.assertEqual(config.max_connections, 20)
        self.assertEqual(config.min_connections, 5)

    def test_from_env(self):
        """Test creating config from environment variables."""
        # Set environment variables
        os.environ["CLICKHOUSE_HOST"] = "envhost"
        os.environ["CLICKHOUSE_PORT"] = "8123"
        os.environ["CLICKHOUSE_USERNAME"] = "envuser"
        os.environ["CLICKHOUSE_PASSWORD"] = "envpass"
        os.environ["CLICKHOUSE_DATABASE"] = "envdb"

        try:
            config = ClickHousePoolConfig.from_env()

            self.assertEqual(config.host, "envhost")
            self.assertEqual(config.port, 8123)
            self.assertEqual(config.username, "envuser")
            self.assertEqual(config.password, "envpass")
            self.assertEqual(config.database, "envdb")
        finally:
            # Cleanup
            keys = ["HOST", "PORT", "USERNAME", "PASSWORD", "DATABASE"]
            for key in keys:
                os.environ.pop(f"CLICKHOUSE_{key}", None)

    def test_from_env_custom_prefix(self):
        """Test creating config from environment with custom prefix."""
        # Set environment variables
        os.environ["CH_HOST"] = "customhost"
        os.environ["CH_PORT"] = "9001"
        os.environ["CH_DATABASE"] = "customdb"

        try:
            config = ClickHousePoolConfig.from_env(prefix="CH_")

            self.assertEqual(config.host, "customhost")
            self.assertEqual(config.port, 9001)
            self.assertEqual(config.database, "customdb")
        finally:
            # Cleanup
            for key in ["HOST", "PORT", "DATABASE"]:
                os.environ.pop(f"CH_{key}", None)

    def test_from_env_defaults(self):
        """Test from_env uses defaults for missing vars."""
        # Make sure env vars don't exist
        for key in ["HOST", "PORT", "DATABASE"]:
            os.environ.pop(f"TEST_CLICKHOUSE_{key}", None)

        config = ClickHousePoolConfig.from_env(prefix="TEST_CLICKHOUSE_")

        self.assertEqual(config.host, "localhost")
        self.assertEqual(config.port, 9000)
        self.assertEqual(config.database, "default")

    def test_connection_url_overrides(self):
        """Test that connection_url overrides individual params."""
        config = ClickHousePoolConfig(
            connection_url="clickhouse://user:pass@remote:8123/remotedb",
            host="localhost",  # Should be ignored
            port=9000,  # Should be ignored
        )

        params = config.get_connection_params()

        self.assertIn("url", params)
        self.assertEqual(params["url"], "clickhouse://user:pass@remote:8123/remotedb")

    def test_compression_configuration(self):
        """Test compression configuration."""
        config = ClickHousePoolConfig(host="localhost", compression=True)

        params = config.get_connection_params()

        self.assertTrue(params["compression"])

    def test_settings_configuration(self):
        """Test settings configuration."""
        config = ClickHousePoolConfig(
            host="localhost",
            settings={"max_execution_time": 300, "max_memory_usage": 10000000000},
        )

        params = config.get_connection_params()

        self.assertIn("settings", params)
        self.assertEqual(params["settings"]["max_execution_time"], 300)

    def test_client_name_configuration(self):
        """Test client name configuration."""
        config = ClickHousePoolConfig(host="localhost", client_name="MyApp")

        params = config.get_connection_params()

        self.assertEqual(params["client_name"], "MyApp")


class TestClickHousePoolConfigEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions."""

    def test_special_characters_in_password(self):
        """Test password with special characters."""
        config = ClickHousePoolConfig(host="localhost", password="p@ssw0rd!#$%")

        params = config.get_connection_params()
        self.assertEqual(params["password"], "p@ssw0rd!#$%")

    def test_ipv6_host(self):
        """Test IPv6 host address."""
        config = ClickHousePoolConfig(host="::1", port=9000)

        self.assertEqual(config.host, "::1")

    def test_connection_url_with_query_params(self):
        """Test connection URL with query parameters."""
        url = "clickhouse://localhost:9000/default?secure=true&verify=true"
        config = ClickHousePoolConfig.from_url(url)

        self.assertEqual(config.connection_url, url)

    def test_max_idle_time_validation(self):
        """Test max_idle_time validation."""
        with self.assertRaisesRegex(ValueError, "max_idle_time must be positive"):
            ClickHousePoolConfig(host="localhost", max_idle_time=-1)

    def test_max_lifetime_validation(self):
        """Test max_lifetime validation."""
        with self.assertRaisesRegex(ValueError, "max_lifetime must be positive"):
            ClickHousePoolConfig(host="localhost", max_lifetime=-1)

    def test_cluster_configuration(self):
        """Test cluster configuration."""
        config = ClickHousePoolConfig(host="localhost", cluster="my_cluster")

        params = config.get_connection_params()
        self.assertEqual(params["cluster"], "my_cluster")

    def test_http_port_vs_native_port(self):
        """Test HTTP port vs native port."""
        config_http = ClickHousePoolConfig(host="localhost", port=8123, use_http=True)

        config_native = ClickHousePoolConfig(
            host="localhost", port=9000, use_http=False
        )

        self.assertTrue(config_http.use_http)
        self.assertFalse(config_native.use_http)

    def test_alt_hosts_configuration(self):
        """Test alternative hosts configuration."""
        config = ClickHousePoolConfig(
            host="localhost", alt_hosts=["host2:9000", "host3:9000"]
        )

        params = config.get_connection_params()
        self.assertIn("alt_hosts", params)
        self.assertEqual(len(params["alt_hosts"]), 2)

    def test_insert_block_size(self):
        """Test insert block size configuration."""
        config = ClickHousePoolConfig(host="localhost", insert_block_size=1048576)

        params = config.get_connection_params()
        self.assertEqual(params["insert_block_size"], 1048576)

    def test_max_block_size(self):
        """Test max block size configuration."""
        config = ClickHousePoolConfig(host="localhost", max_block_size=65536)

        params = config.get_connection_params()
        self.assertEqual(params["max_block_size"], 65536)


if __name__ == "__main__":
    unittest.main()
