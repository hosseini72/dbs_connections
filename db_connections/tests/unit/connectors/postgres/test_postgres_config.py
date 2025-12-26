"""Unit tests for PostgreSQL configuration."""

import sys
from pathlib import Path
import os
import unittest

# Add parent directory to path to allow imports
# test_postgres_config.py is at: db_connections/tests/unit/connectors/postgres/
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
        / "test_postgres_config.py"
    )

parent_dir = _file_path.parent.parent.parent.parent.parent.parent
parent_dir_str = str(parent_dir)
if parent_dir_str not in sys.path:
    sys.path.insert(0, parent_dir_str)

from db_connections.scr.all_db_connectors.connectors.postgres.config import (  # noqa: E402, E501
    PostgresPoolConfig,
)


class TestPostgresPoolConfig(unittest.TestCase):
    """Test PostgresPoolConfig class."""

    def test_default_values(self):
        """Test default configuration values."""
        config = PostgresPoolConfig(
            host="localhost", database="testdb", user="testuser"
        )

        self.assertEqual(config.host, "localhost")
        self.assertEqual(config.port, 5432)
        self.assertEqual(config.database, "testdb")
        self.assertEqual(config.user, "testuser")
        self.assertIsNone(config.password)
        self.assertEqual(config.sslmode, "prefer")
        self.assertEqual(config.min_size, 1)
        self.assertEqual(config.max_size, 10)
        self.assertEqual(config.timeout, 30)

    def test_custom_values(self):
        """Test custom configuration values."""
        config = PostgresPoolConfig(
            host="db.example.com",
            port=5433,
            database="mydb",
            user="admin",
            password="secret",
            min_size=5,
            max_size=20,
            timeout=60,
            sslmode="require",
        )

        self.assertEqual(config.host, "db.example.com")
        self.assertEqual(config.port, 5433)
        self.assertEqual(config.database, "mydb")
        self.assertEqual(config.user, "admin")
        self.assertEqual(config.password, "secret")
        self.assertEqual(config.min_size, 5)
        self.assertEqual(config.max_size, 20)
        self.assertEqual(config.timeout, 60)
        self.assertEqual(config.sslmode, "require")

    def test_missing_required_fields(self):
        """Test validation of required fields."""
        with self.assertRaisesRegex(ValueError, "host is required"):
            PostgresPoolConfig(host="", database="testdb", user="testuser")

        with self.assertRaisesRegex(ValueError, "database is required"):
            PostgresPoolConfig(host="localhost", database="", user="testuser")

        with self.assertRaisesRegex(ValueError, "user is required"):
            PostgresPoolConfig(host="localhost", database="testdb", user="")

    def test_invalid_sslmode(self):
        """Test validation of SSL mode."""
        with self.assertRaisesRegex(ValueError, "Invalid sslmode"):
            PostgresPoolConfig(
                host="localhost", database="testdb", user="testuser", sslmode="invalid"
            )

    def test_valid_sslmodes(self):
        """Test all valid SSL modes."""
        valid_modes = [
            "disable",
            "allow",
            "prefer",
            "require",
            "verify-ca",
            "verify-full",
        ]

        for mode in valid_modes:
            config = PostgresPoolConfig(
                host="localhost", database="testdb", user="testuser", sslmode=mode
            )
            self.assertEqual(config.sslmode, mode)

    def test_get_connection_params(self):
        """Test connection parameters generation."""
        config = PostgresPoolConfig(
            host="localhost",
            port=5432,
            database="testdb",
            user="testuser",
            password="testpass",
            sslmode="require",
            connect_timeout=15,
        )

        params = config.get_connection_params()

        self.assertEqual(params["host"], "localhost")
        self.assertEqual(params["port"], 5432)
        self.assertEqual(params["database"], "testdb")
        self.assertEqual(params["user"], "testuser")
        self.assertEqual(params["password"], "testpass")
        self.assertEqual(params["sslmode"], "require")
        self.assertEqual(params["connect_timeout"], 15)

    def test_get_connection_params_no_password(self):
        """Test connection parameters without password."""
        config = PostgresPoolConfig(
            host="localhost", database="testdb", user="testuser"
        )

        params = config.get_connection_params()

        self.assertNotIn("password", params)

    def test_get_connection_params_with_server_settings(self):
        """Test connection parameters with server settings."""
        config = PostgresPoolConfig(
            host="localhost",
            database="testdb",
            user="testuser",
            server_settings={"timezone": "UTC", "search_path": "public"},
        )

        params = config.get_connection_params()

        self.assertEqual(params["server_settings"]["timezone"], "UTC")
        self.assertEqual(params["server_settings"]["search_path"], "public")

    def test_get_dsn(self):
        """Test DSN generation."""
        config = PostgresPoolConfig(
            host="localhost",
            port=5432,
            database="testdb",
            user="testuser",
            password="testpass",
        )

        dsn = config.get_dsn()

        self.assertTrue(dsn.startswith("postgresql://"))
        self.assertIn("testuser:testpass", dsn)
        self.assertIn("localhost:5432", dsn)
        self.assertIn("/testdb", dsn)

    def test_get_dsn_no_password(self):
        """Test DSN generation without password."""
        config = PostgresPoolConfig(
            host="localhost", database="testdb", user="testuser"
        )

        dsn = config.get_dsn()

        self.assertEqual(dsn, "postgresql://testuser@localhost:5432/testdb")

    def test_get_dsn_with_ssl(self):
        """Test DSN generation with SSL mode."""
        config = PostgresPoolConfig(
            host="localhost", database="testdb", user="testuser", sslmode="require"
        )

        dsn = config.get_dsn()

        self.assertIn("sslmode=require", dsn)

    def test_from_dsn(self):
        """Test creating config from DSN."""
        dsn = "postgresql://user:pass@localhost:5432/mydb?sslmode=require"

        config = PostgresPoolConfig.from_dsn(dsn)

        self.assertEqual(config.connection_string, dsn)

    def test_from_dsn_with_additional_params(self):
        """Test creating config from DSN with extra parameters."""
        dsn = "postgresql://user:pass@localhost:5432/mydb"

        config = PostgresPoolConfig.from_dsn(dsn, min_size=5, max_size=20)

        self.assertEqual(config.connection_string, dsn)
        self.assertEqual(config.min_size, 5)
        self.assertEqual(config.max_size, 20)

    def test_from_env(self):
        """Test creating config from environment variables."""
        # Set environment variables
        os.environ["POSTGRES_HOST"] = "envhost"
        os.environ["POSTGRES_PORT"] = "5433"
        os.environ["POSTGRES_DATABASE"] = "envdb"
        os.environ["POSTGRES_USER"] = "envuser"
        os.environ["POSTGRES_PASSWORD"] = "envpass"
        os.environ["POSTGRES_SSLMODE"] = "require"

        try:
            config = PostgresPoolConfig.from_env()

            self.assertEqual(config.host, "envhost")
            self.assertEqual(config.port, 5433)
            self.assertEqual(config.database, "envdb")
            self.assertEqual(config.user, "envuser")
            self.assertEqual(config.password, "envpass")
            self.assertEqual(config.sslmode, "require")
        finally:
            # Cleanup
            keys = ["HOST", "PORT", "DATABASE", "USER", "PASSWORD", "SSLMODE"]
            for key in keys:
                os.environ.pop(f"POSTGRES_{key}", None)

    def test_from_env_custom_prefix(self):
        """Test creating config from environment with custom prefix."""
        # Set environment variables
        os.environ["DB_HOST"] = "customhost"
        os.environ["DB_DATABASE"] = "customdb"
        os.environ["DB_USER"] = "customuser"

        try:
            config = PostgresPoolConfig.from_env(prefix="DB_")

            self.assertEqual(config.host, "customhost")
            self.assertEqual(config.database, "customdb")
            self.assertEqual(config.user, "customuser")
        finally:
            # Cleanup
            for key in ["HOST", "DATABASE", "USER"]:
                os.environ.pop(f"DB_{key}", None)

    def test_from_env_defaults(self):
        """Test from_env uses defaults for missing vars."""
        # Make sure env vars don't exist
        for key in ["HOST", "PORT", "DATABASE", "USER"]:
            os.environ.pop(f"TEST_POSTGRES_{key}", None)

        config = PostgresPoolConfig.from_env(prefix="TEST_POSTGRES_")

        self.assertEqual(config.host, "localhost")
        self.assertEqual(config.port, 5432)
        self.assertEqual(config.database, "postgres")
        self.assertEqual(config.user, "postgres")

    def test_connection_string_overrides(self):
        """Test that connection_string overrides individual params."""
        config = PostgresPoolConfig(
            connection_string="postgresql://user:pass@remote:5433/remotedb",
            host="localhost",  # Should be ignored
            database="localdb",  # Should be ignored
        )

        params = config.get_connection_params()

        expected = {"dsn": "postgresql://user:pass@remote:5433/remotedb"}
        self.assertEqual(params, expected)

    def test_application_name(self):
        """Test application name configuration."""
        config = PostgresPoolConfig(
            host="localhost",
            database="testdb",
            user="testuser",
            application_name="MyMicroservice",
        )

        params = config.get_connection_params()

        self.assertEqual(params["application_name"], "MyMicroservice")

    def test_server_settings_initialization(self):
        """Test server_settings is initialized to empty dict."""
        config = PostgresPoolConfig(
            host="localhost", database="testdb", user="testuser"
        )

        self.assertEqual(config.server_settings, {})
        self.assertIsInstance(config.server_settings, dict)

    def test_ssl_certificate_paths(self):
        """Test SSL certificate configuration."""
        config = PostgresPoolConfig(
            host="localhost",
            database="testdb",
            user="testuser",
            sslmode="verify-full",
            sslcert="/path/to/client.crt",
            sslkey="/path/to/client.key",
            sslrootcert="/path/to/ca.crt",
        )

        params = config.get_connection_params()

        self.assertEqual(params["sslcert"], "/path/to/client.crt")
        self.assertEqual(params["sslkey"], "/path/to/client.key")
        self.assertEqual(params["sslrootcert"], "/path/to/ca.crt")

    def test_command_timeout(self):
        """Test command timeout configuration."""
        config = PostgresPoolConfig(
            host="localhost", database="testdb", user="testuser", command_timeout=60
        )

        params = config.get_connection_params()

        self.assertEqual(params["command_timeout"], 60)

    def test_command_timeout_none(self):
        """Test command timeout as None."""
        config = PostgresPoolConfig(
            host="localhost", database="testdb", user="testuser", command_timeout=None
        )

        params = config.get_connection_params()

        self.assertNotIn("command_timeout", params)


class TestPostgresPoolConfigEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions."""

    def test_empty_server_settings(self):
        """Test with empty server settings."""
        config = PostgresPoolConfig(
            host="localhost", database="testdb", user="testuser", server_settings={}
        )

        self.assertEqual(config.server_settings, {})

    def test_connection_string_with_query_params(self):
        """Test connection string with query parameters."""
        dsn = "postgresql://user:pass@host:5432/db?sslmode=require&connect_timeout=10"
        config = PostgresPoolConfig.from_dsn(dsn)

        self.assertEqual(config.connection_string, dsn)

    def test_special_characters_in_password(self):
        """Test password with special characters."""
        config = PostgresPoolConfig(
            host="localhost",
            database="testdb",
            user="testuser",
            password="p@ssw0rd!#$%",
        )

        params = config.get_connection_params()
        self.assertEqual(params["password"], "p@ssw0rd!#$%")

    def test_ipv6_host(self):
        """Test IPv6 host address."""
        config = PostgresPoolConfig(host="::1", database="testdb", user="testuser")

        self.assertEqual(config.host, "::1")

    def test_unix_socket_host(self):
        """Test Unix socket host."""
        config = PostgresPoolConfig(
            host="/var/run/postgresql", database="testdb", user="testuser"
        )

        self.assertEqual(config.host, "/var/run/postgresql")


if __name__ == "__main__":
    unittest.main()
