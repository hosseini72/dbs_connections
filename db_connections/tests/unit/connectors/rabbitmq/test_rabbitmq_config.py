"""Unit tests for RabbitMQ configuration."""

import sys
from pathlib import Path
import os
import unittest

# Add parent directory to path to allow imports
# test_rabbitmq_config.py is at: db_connections/tests/unit/connectors/rabbitmq/
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
        / "rabbitmq"
        / "test_rabbitmq_config.py"
    )  # noqa: E501

parent_dir = _file_path.parent.parent.parent.parent.parent.parent
parent_dir_str = str(parent_dir)
if parent_dir_str not in sys.path:
    sys.path.insert(0, parent_dir_str)

from db_connections.scr.all_db_connectors.connectors.rabbitmq.config import (  # noqa: E402, E501
    RabbitMQPoolConfig,
)


class TestRabbitMQPoolConfig(unittest.TestCase):
    """Test RabbitMQPoolConfig class."""

    def test_default_values(self):
        """Test default configuration values."""
        config = RabbitMQPoolConfig(host="localhost")

        self.assertEqual(config.host, "localhost")
        self.assertEqual(config.port, 5672)
        self.assertIsNone(config.username)
        self.assertIsNone(config.password)
        self.assertEqual(config.virtual_host, "/")
        self.assertFalse(config.ssl)
        self.assertEqual(config.heartbeat, 600)
        self.assertEqual(config.timeout, 30)
        self.assertEqual(config.max_connections, 10)
        self.assertEqual(config.min_connections, 1)

    def test_custom_values(self):
        """Test custom configuration values."""
        config = RabbitMQPoolConfig(
            host="rabbitmq.example.com",
            port=5673,
            username="admin",
            password="secret",
            virtual_host="my_vhost",
            ssl=True,
            heartbeat=30,
            connection_attempts=5,
            retry_delay=10,
            timeout=60,
            max_connections=20,
            min_connections=5,
            max_idle_time=120,
            max_lifetime=7200,
        )

        self.assertEqual(config.host, "rabbitmq.example.com")
        self.assertEqual(config.port, 5673)
        self.assertEqual(config.username, "admin")
        self.assertEqual(config.password, "secret")
        self.assertEqual(config.virtual_host, "my_vhost")
        self.assertTrue(config.ssl)
        self.assertEqual(config.heartbeat, 30)
        self.assertEqual(config.connection_attempts, 5)
        self.assertEqual(config.retry_delay, 10)
        self.assertEqual(config.timeout, 60)
        self.assertEqual(config.max_connections, 20)
        self.assertEqual(config.min_connections, 5)
        self.assertEqual(config.max_idle_time, 120)
        self.assertEqual(config.max_lifetime, 7200)

    def test_missing_required_fields(self):
        """Test validation of required fields."""
        with self.assertRaisesRegex(ValueError, "host is required"):
            RabbitMQPoolConfig(host="")

        with self.assertRaisesRegex(ValueError, "host is required"):
            RabbitMQPoolConfig(host=None)

    def test_invalid_port(self):
        """Test validation of port."""
        with self.assertRaisesRegex(ValueError, "port must be between"):
            RabbitMQPoolConfig(host="localhost", port=0)

        with self.assertRaisesRegex(ValueError, "port must be between"):
            RabbitMQPoolConfig(host="localhost", port=65536)

    def test_invalid_timeout(self):
        """Test validation of timeout."""
        with self.assertRaisesRegex(ValueError, "timeout must be positive"):
            RabbitMQPoolConfig(host="localhost", timeout=-1)

    def test_invalid_max_connections(self):
        """Test validation of max_connections."""
        with self.assertRaisesRegex(ValueError, "max_connections must be positive"):
            RabbitMQPoolConfig(host="localhost", max_connections=0)

    def test_invalid_min_connections(self):
        """Test validation of min_connections."""
        with self.assertRaisesRegex(ValueError, "min_connections must be positive"):
            RabbitMQPoolConfig(host="localhost", min_connections=0)

    def test_min_greater_than_max(self):
        """Test validation that min_connections <= max_connections."""
        with self.assertRaisesRegex(ValueError, "min_connections cannot be greater"):
            RabbitMQPoolConfig(host="localhost", min_connections=20, max_connections=10)

    def test_invalid_heartbeat(self):
        """Test validation of heartbeat."""
        with self.assertRaisesRegex(ValueError, "heartbeat must be positive"):
            RabbitMQPoolConfig(host="localhost", heartbeat=-1)

    def test_invalid_connection_attempts(self):
        """Test validation of connection_attempts."""
        with self.assertRaisesRegex(ValueError, "connection_attempts must be positive"):
            RabbitMQPoolConfig(host="localhost", connection_attempts=0)

    def test_get_connection_params(self):
        """Test connection parameters generation."""
        config = RabbitMQPoolConfig(
            host="localhost",
            port=5672,
            username="user",
            password="pass",
            virtual_host="vhost",
            heartbeat=30,
            connection_timeout=10,
            socket_timeout=5,
        )

        params = config.get_connection_params()

        self.assertEqual(params["host"], "localhost")
        self.assertEqual(params["port"], 5672)
        self.assertEqual(params["username"], "user")
        self.assertEqual(params["password"], "pass")
        self.assertEqual(params["virtual_host"], "vhost")
        self.assertEqual(params["heartbeat"], 30)
        self.assertEqual(params["connection_timeout"], 10)
        self.assertEqual(params["socket_timeout"], 5)

    def test_get_connection_params_no_auth(self):
        """Test connection parameters without authentication."""
        config = RabbitMQPoolConfig(host="localhost", port=5672)

        params = config.get_connection_params()

        self.assertNotIn("username", params)
        self.assertNotIn("password", params)

    def test_get_connection_params_with_ssl(self):
        """Test connection parameters with SSL."""
        config = RabbitMQPoolConfig(
            host="localhost",
            ssl=True,
            ssl_options={
                "ca_certs": "/path/to/ca.crt",
                "certfile": "/path/to/client.crt",
                "keyfile": "/path/to/client.key",
            },
        )

        params = config.get_connection_params()

        self.assertTrue(params["ssl"])
        self.assertIn("ssl_options", params)
        self.assertEqual(params["ssl_options"]["ca_certs"], "/path/to/ca.crt")

    def test_get_connection_url(self):
        """Test connection URL generation."""
        config = RabbitMQPoolConfig(
            host="localhost",
            port=5672,
            username="user",
            password="pass",
            virtual_host="vhost",
        )

        url = config.get_connection_url()

        self.assertTrue(url.startswith("amqp://"))
        self.assertIn("user:pass", url)
        self.assertIn("localhost:5672", url)
        self.assertIn("/vhost", url)

    def test_get_connection_url_no_auth(self):
        """Test connection URL without authentication."""
        config = RabbitMQPoolConfig(host="localhost", port=5672)

        url = config.get_connection_url()

        self.assertEqual(url, "amqp://localhost:5672/")

    def test_get_connection_url_with_ssl(self):
        """Test connection URL with SSL (amqps://)."""
        config = RabbitMQPoolConfig(host="localhost", port=5671, ssl=True)

        url = config.get_connection_url()

        self.assertTrue(url.startswith("amqps://"))

    def test_from_url(self):
        """Test creating config from URL."""
        url = "amqp://user:pass@localhost:5672/vhost"

        config = RabbitMQPoolConfig.from_url(url)

        self.assertEqual(config.connection_url, url)

    def test_from_url_with_additional_params(self):
        """Test creating config from URL with extra parameters."""
        url = "amqp://localhost:5672/"

        config = RabbitMQPoolConfig.from_url(url, max_connections=20, min_connections=5)

        self.assertEqual(config.connection_url, url)
        self.assertEqual(config.max_connections, 20)
        self.assertEqual(config.min_connections, 5)

    def test_from_env(self):
        """Test creating config from environment variables."""
        # Set environment variables
        os.environ["RABBITMQ_HOST"] = "envhost"
        os.environ["RABBITMQ_PORT"] = "5673"
        os.environ["RABBITMQ_USERNAME"] = "envuser"
        os.environ["RABBITMQ_PASSWORD"] = "envpass"
        os.environ["RABBITMQ_VIRTUAL_HOST"] = "envvhost"

        try:
            config = RabbitMQPoolConfig.from_env()

            self.assertEqual(config.host, "envhost")
            self.assertEqual(config.port, 5673)
            self.assertEqual(config.username, "envuser")
            self.assertEqual(config.password, "envpass")
            self.assertEqual(config.virtual_host, "envvhost")
        finally:
            # Cleanup
            keys = ["HOST", "PORT", "USERNAME", "PASSWORD", "VIRTUAL_HOST"]
            for key in keys:
                os.environ.pop(f"RABBITMQ_{key}", None)

    def test_from_env_custom_prefix(self):
        """Test creating config from environment with custom prefix."""
        # Set environment variables
        os.environ["MQ_HOST"] = "customhost"
        os.environ["MQ_PORT"] = "5674"
        os.environ["MQ_VIRTUAL_HOST"] = "customvhost"

        try:
            config = RabbitMQPoolConfig.from_env(prefix="MQ_")

            self.assertEqual(config.host, "customhost")
            self.assertEqual(config.port, 5674)
            self.assertEqual(config.virtual_host, "customvhost")
        finally:
            # Cleanup
            for key in ["HOST", "PORT", "VIRTUAL_HOST"]:
                os.environ.pop(f"MQ_{key}", None)

    def test_from_env_defaults(self):
        """Test from_env uses defaults for missing vars."""
        # Make sure env vars don't exist
        for key in ["HOST", "PORT", "VIRTUAL_HOST"]:
            os.environ.pop(f"TEST_RABBITMQ_{key}", None)

        config = RabbitMQPoolConfig.from_env(prefix="TEST_RABBITMQ_")

        self.assertEqual(config.host, "localhost")
        self.assertEqual(config.port, 5672)
        self.assertEqual(config.virtual_host, "/")

    def test_connection_url_overrides(self):
        """Test that connection_url overrides individual params."""
        config = RabbitMQPoolConfig(
            connection_url="amqp://user:pass@remote:5673/remotevhost",
            host="localhost",  # Should be ignored
            port=5672,  # Should be ignored
        )

        params = config.get_connection_params()

        self.assertIn("url", params)
        self.assertEqual(params["url"], "amqp://user:pass@remote:5673/remotevhost")

    def test_blocked_connection_timeout(self):
        """Test blocked connection timeout."""
        config = RabbitMQPoolConfig(host="localhost", blocked_connection_timeout=300)

        params = config.get_connection_params()

        self.assertEqual(params["blocked_connection_timeout"], 300)

    def test_channel_max(self):
        """Test channel max configuration."""
        config = RabbitMQPoolConfig(host="localhost", channel_max=100)

        params = config.get_connection_params()

        self.assertEqual(params["channel_max"], 100)

    def test_frame_max(self):
        """Test frame max configuration."""
        config = RabbitMQPoolConfig(host="localhost", frame_max=131072)

        params = config.get_connection_params()

        self.assertEqual(params["frame_max"], 131072)


class TestRabbitMQPoolConfigEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions."""

    def test_special_characters_in_password(self):
        """Test password with special characters."""
        config = RabbitMQPoolConfig(host="localhost", password="p@ssw0rd!#$%")

        params = config.get_connection_params()
        self.assertEqual(params["password"], "p@ssw0rd!#$%")

    def test_ipv6_host(self):
        """Test IPv6 host address."""
        config = RabbitMQPoolConfig(host="::1", port=5672)

        self.assertEqual(config.host, "::1")

    def test_connection_url_with_query_params(self):
        """Test connection URL with query parameters."""
        url = "amqp://localhost:5672/?heartbeat=30&connection_timeout=10"
        config = RabbitMQPoolConfig.from_url(url)

        self.assertEqual(config.connection_url, url)

    def test_max_idle_time_validation(self):
        """Test max_idle_time validation."""
        with self.assertRaisesRegex(ValueError, "max_idle_time must be positive"):
            RabbitMQPoolConfig(host="localhost", max_idle_time=-1)

    def test_max_lifetime_validation(self):
        """Test max_lifetime validation."""
        with self.assertRaisesRegex(ValueError, "max_lifetime must be positive"):
            RabbitMQPoolConfig(host="localhost", max_lifetime=-1)

    def test_ssl_options_dict(self):
        """Test SSL options as dictionary."""
        config = RabbitMQPoolConfig(
            host="localhost",
            ssl=True,
            ssl_options={
                "ca_certs": "/path/to/ca.crt",
                "cert_reqs": "CERT_REQUIRED",
                "certfile": "/path/to/cert.pem",
                "keyfile": "/path/to/key.pem",
            },
        )

        params = config.get_connection_params()
        self.assertIn("ssl_options", params)
        self.assertIsInstance(params["ssl_options"], dict)

    def test_virtual_host_encoding(self):
        """Test virtual host URL encoding."""
        config = RabbitMQPoolConfig(host="localhost", virtual_host="my/vhost")

        url = config.get_connection_url()
        # Virtual host should be URL encoded
        self.assertIn("%2F", url or "my%2Fvhost")

    def test_locale_configuration(self):
        """Test locale configuration."""
        config = RabbitMQPoolConfig(host="localhost", locale="en_US")

        params = config.get_connection_params()
        self.assertEqual(params["locale"], "en_US")

    def test_client_properties(self):
        """Test client properties configuration."""
        config = RabbitMQPoolConfig(
            host="localhost", client_properties={"product": "MyApp", "version": "1.0.0"}
        )

        params = config.get_connection_params()
        self.assertIn("client_properties", params)
        self.assertEqual(params["client_properties"]["product"], "MyApp")


if __name__ == "__main__":
    unittest.main()
