"""Unit tests for Neo4j configuration."""

import sys
from pathlib import Path
import os
import unittest
from unittest.mock import patch, Mock

# Add parent directory to path to allow imports
# test_neo4j_config.py is at: db_connections/tests/unit/connectors/neo4j/
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
        / "neo4j"
        / "test_neo4j_config.py"
    )  # noqa: E501

parent_dir = _file_path.parent.parent.parent.parent.parent.parent
parent_dir_str = str(parent_dir)
if parent_dir_str not in sys.path:
    sys.path.insert(0, parent_dir_str)

from db_connections.scr.all_db_connectors.connectors.neo4j.config import (  # noqa: E402, E501
    Neo4jPoolConfig,
)


class TestNeo4jPoolConfig(unittest.TestCase):
    """Test Neo4jPoolConfig class."""

    def test_default_values(self):
        """Test default configuration values."""
        config = Neo4jPoolConfig(host="localhost")

        self.assertEqual(config.host, "localhost")
        self.assertEqual(config.port, 7687)
        self.assertIsNone(config.username)
        self.assertIsNone(config.password)
        self.assertEqual(config.database, "neo4j")
        self.assertEqual(config.timeout, 30)
        self.assertEqual(config.max_connections, 10)
        self.assertEqual(config.min_connections, 1)

    def test_custom_values(self):
        """Test custom configuration values."""
        config = Neo4jPoolConfig(
            host="neo4j.example.com",
            port=7688,
            username="admin",
            password="secret",
            database="mydb",
            timeout=60,
            max_connections=20,
            min_connections=5,
            max_idle_time=120,
            max_lifetime=7200,
            encrypted=True,
            trust="TRUST_ALL_CERTIFICATES",
        )

        self.assertEqual(config.host, "neo4j.example.com")
        self.assertEqual(config.port, 7688)
        self.assertEqual(config.username, "admin")
        self.assertEqual(config.password, "secret")
        self.assertEqual(config.database, "mydb")
        self.assertEqual(config.timeout, 60)
        self.assertEqual(config.max_connections, 20)
        self.assertEqual(config.min_connections, 5)
        self.assertEqual(config.max_idle_time, 120)
        self.assertEqual(config.max_lifetime, 7200)
        self.assertTrue(config.encrypted)
        self.assertEqual(config.trust, "TRUST_ALL_CERTIFICATES")

    def test_missing_required_fields(self):
        """Test validation of required fields."""
        with self.assertRaisesRegex(ValueError, "host is required"):
            Neo4jPoolConfig(host="")

        with self.assertRaisesRegex(ValueError, "host is required"):
            Neo4jPoolConfig(host=None)

    def test_invalid_port(self):
        """Test validation of port."""
        with self.assertRaisesRegex(ValueError, "port must be between"):
            Neo4jPoolConfig(host="localhost", port=0)

        with self.assertRaisesRegex(ValueError, "port must be between"):
            Neo4jPoolConfig(host="localhost", port=65536)

    def test_invalid_timeout(self):
        """Test validation of timeout."""
        with self.assertRaisesRegex(ValueError, "timeout must be positive"):
            Neo4jPoolConfig(host="localhost", timeout=-1)

    def test_invalid_max_connections(self):
        """Test validation of max_connections."""
        # Validation is deferred to pool creation, not config creation
        # Create invalid config (should succeed)
        config = Neo4jPoolConfig(host="localhost", max_connections=0)
        # Validation happens when creating pool
        from db_connections.scr.all_db_connectors.connectors.neo4j.pool import (
            Neo4jSyncConnectionPool,
        )

        # Mock neo4j module to avoid import errors
        with patch(
            "db_connections.scr.all_db_connectors.connectors.neo4j.pool.neo4j"
        ) as mock_neo4j:
            mock_neo4j.GraphDatabase = Mock()
            with self.assertRaisesRegex(ValueError, "max_connections must be positive"):
                Neo4jSyncConnectionPool(config)

    def test_invalid_min_connections(self):
        """Test validation of min_connections."""
        # Validation is deferred to pool creation, not config creation
        # Create invalid config (should succeed) - use negative value since validation checks min_size < 0
        config = Neo4jPoolConfig(host="localhost", min_connections=-1)
        # Validation happens when creating pool
        from db_connections.scr.all_db_connectors.connectors.neo4j.pool import (
            Neo4jSyncConnectionPool,
        )

        # Mock neo4j module to avoid import errors
        with patch(
            "db_connections.scr.all_db_connectors.connectors.neo4j.pool.neo4j"
        ) as mock_neo4j:
            mock_neo4j.GraphDatabase = Mock()
            with self.assertRaisesRegex(ValueError, "min_connections must be positive"):
                Neo4jSyncConnectionPool(config)

    def test_min_greater_than_max(self):
        """Test validation that min_connections <= max_connections."""
        # Validation is deferred to pool creation, not config creation
        # Create invalid config (should succeed)
        config = Neo4jPoolConfig(
            host="localhost", min_connections=20, max_connections=10
        )
        # Validation happens when creating pool
        from db_connections.scr.all_db_connectors.connectors.neo4j.pool import (
            Neo4jSyncConnectionPool,
        )

        # Mock neo4j module to avoid import errors
        with patch(
            "db_connections.scr.all_db_connectors.connectors.neo4j.pool.neo4j"
        ) as mock_neo4j:
            mock_neo4j.GraphDatabase = Mock()
            with self.assertRaisesRegex(
                ValueError, "min_connections cannot be greater"
            ):
                Neo4jSyncConnectionPool(config)

    def test_invalid_trust(self):
        """Test validation of trust setting."""
        with self.assertRaisesRegex(ValueError, "Invalid trust"):
            Neo4jPoolConfig(host="localhost", trust="INVALID")

    def test_valid_trust_values(self):
        """Test all valid trust values."""
        valid_trusts = [
            "TRUST_ALL_CERTIFICATES",
            "TRUST_SYSTEM_CA_SIGNED_CERTIFICATES",
            "TRUST_CUSTOM_CA_SIGNED_CERTIFICATES",
        ]

        for trust in valid_trusts:
            config = Neo4jPoolConfig(host="localhost", trust=trust)
            self.assertEqual(config.trust, trust)

    def test_get_connection_params(self):
        """Test connection parameters generation."""
        config = Neo4jPoolConfig(
            host="localhost",
            port=7687,
            username="user",
            password="pass",
            database="testdb",
            encrypted=True,
            trust="TRUST_ALL_CERTIFICATES",
            connection_timeout=10,
            max_retry_time=30,
        )

        params = config.get_connection_params()

        self.assertEqual(params["uri"], "bolt://localhost:7687")
        self.assertEqual(params["auth"], ("user", "pass"))
        self.assertEqual(params["database"], "testdb")
        self.assertTrue(params["encrypted"])
        self.assertEqual(params["trust"], "TRUST_ALL_CERTIFICATES")
        self.assertEqual(params["connection_timeout"], 10)
        self.assertEqual(params["max_retry_time"], 30)

    def test_get_connection_params_no_auth(self):
        """Test connection parameters without authentication."""
        config = Neo4jPoolConfig(host="localhost", port=7687)

        params = config.get_connection_params()

        self.assertNotIn("auth", params)

    def test_get_connection_params_with_ssl(self):
        """Test connection parameters with SSL."""
        config = Neo4jPoolConfig(
            host="localhost",
            encrypted=True,
            trust="TRUST_SYSTEM_CA_SIGNED_CERTIFICATES",
            trusted_certificate="/path/to/ca.crt",
        )

        params = config.get_connection_params()

        self.assertTrue(params["encrypted"])
        self.assertEqual(params["trust"], "TRUST_SYSTEM_CA_SIGNED_CERTIFICATES")
        self.assertEqual(params["trusted_certificate"], "/path/to/ca.crt")

    def test_get_connection_url(self):
        """Test connection URL generation."""
        config = Neo4jPoolConfig(
            host="localhost",
            port=7687,
            username="user",
            password="pass",
            database="testdb",
        )

        url = config.get_connection_url()

        self.assertTrue(url.startswith("bolt://"))
        self.assertIn("user:pass", url)
        self.assertIn("localhost:7687", url)
        self.assertIn("/testdb", url)

    def test_get_connection_url_no_auth(self):
        """Test connection URL without authentication."""
        config = Neo4jPoolConfig(host="localhost", port=7687)

        url = config.get_connection_url()

        self.assertEqual(url, "bolt://localhost:7687/neo4j")

    def test_get_connection_url_with_ssl(self):
        """Test connection URL with SSL (bolt+s://)."""
        config = Neo4jPoolConfig(host="localhost", port=7687, encrypted=True)

        url = config.get_connection_url()

        self.assertTrue(url.startswith("bolt+s://") or "encrypted" in url)

    def test_get_connection_url_neo4j_scheme(self):
        """Test connection URL with neo4j:// scheme."""
        config = Neo4jPoolConfig(host="localhost", port=7687, use_neo4j_scheme=True)

        url = config.get_connection_url()

        self.assertTrue(url.startswith("neo4j://"))

    def test_from_url(self):
        """Test creating config from URL."""
        url = "bolt://user:pass@localhost:7687/testdb"

        config = Neo4jPoolConfig.from_url(url)

        self.assertEqual(config.connection_url, url)

    def test_from_url_with_additional_params(self):
        """Test creating config from URL with extra parameters."""
        url = "bolt://localhost:7687/neo4j"

        config = Neo4jPoolConfig.from_url(url, max_connections=20, min_connections=5)

        self.assertEqual(config.connection_url, url)
        self.assertEqual(config.max_connections, 20)
        self.assertEqual(config.min_connections, 5)

    def test_from_env(self):
        """Test creating config from environment variables."""
        # Set environment variables
        os.environ["NEO4J_HOST"] = "envhost"
        os.environ["NEO4J_PORT"] = "7688"
        os.environ["NEO4J_USERNAME"] = "envuser"
        os.environ["NEO4J_PASSWORD"] = "envpass"
        os.environ["NEO4J_DATABASE"] = "envdb"

        try:
            config = Neo4jPoolConfig.from_env()

            self.assertEqual(config.host, "envhost")
            self.assertEqual(config.port, 7688)
            self.assertEqual(config.username, "envuser")
            self.assertEqual(config.password, "envpass")
            self.assertEqual(config.database, "envdb")
        finally:
            # Cleanup
            keys = ["HOST", "PORT", "USERNAME", "PASSWORD", "DATABASE"]
            for key in keys:
                os.environ.pop(f"NEO4J_{key}", None)

    def test_from_env_custom_prefix(self):
        """Test creating config from environment with custom prefix."""
        # Set environment variables
        os.environ["GRAPH_HOST"] = "customhost"
        os.environ["GRAPH_PORT"] = "7689"
        os.environ["GRAPH_DATABASE"] = "customdb"

        try:
            config = Neo4jPoolConfig.from_env(prefix="GRAPH_")

            self.assertEqual(config.host, "customhost")
            self.assertEqual(config.port, 7689)
            self.assertEqual(config.database, "customdb")
        finally:
            # Cleanup
            for key in ["HOST", "PORT", "DATABASE"]:
                os.environ.pop(f"GRAPH_{key}", None)

    def test_from_env_defaults(self):
        """Test from_env uses defaults for missing vars."""
        # Make sure env vars don't exist
        for key in ["HOST", "PORT", "DATABASE"]:
            os.environ.pop(f"TEST_NEO4J_{key}", None)

        config = Neo4jPoolConfig.from_env(prefix="TEST_NEO4J_")

        self.assertEqual(config.host, "localhost")
        self.assertEqual(config.port, 7687)
        self.assertEqual(config.database, "neo4j")

    def test_connection_url_overrides(self):
        """Test that connection_url overrides individual params."""
        config = Neo4jPoolConfig(
            connection_url="bolt://user:pass@remote:7688/remotedb",
            host="localhost",  # Should be ignored
            port=7687,  # Should be ignored
        )

        params = config.get_connection_params()

        self.assertIn("uri", params)
        self.assertEqual(params["uri"], "bolt://user:pass@remote:7688/remotedb")

    def test_routing_context(self):
        """Test routing context configuration."""
        config = Neo4jPoolConfig(
            host="localhost",
            routing_context={"address": "localhost:7687", "region": "us-east"},
        )

        params = config.get_connection_params()
        self.assertIn("routing_context", params)
        self.assertEqual(params["routing_context"]["region"], "us-east")

    def test_max_transaction_retry_time(self):
        """Test max transaction retry time."""
        config = Neo4jPoolConfig(host="localhost", max_transaction_retry_time=30)

        params = config.get_connection_params()
        self.assertEqual(params["max_transaction_retry_time"], 30)

    def test_user_agent(self):
        """Test user agent configuration."""
        config = Neo4jPoolConfig(host="localhost", user_agent="MyApp/1.0")

        params = config.get_connection_params()
        self.assertEqual(params["user_agent"], "MyApp/1.0")


class TestNeo4jPoolConfigEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions."""

    def test_special_characters_in_password(self):
        """Test password with special characters."""
        config = Neo4jPoolConfig(host="localhost", password="p@ssw0rd!#$%")

        params = config.get_connection_params()
        self.assertEqual(params["auth"][1], "p@ssw0rd!#$%")

    def test_ipv6_host(self):
        """Test IPv6 host address."""
        config = Neo4jPoolConfig(host="::1", port=7687)

        self.assertEqual(config.host, "::1")

    def test_connection_url_with_query_params(self):
        """Test connection URL with query parameters."""
        url = "bolt://localhost:7687/neo4j?encrypted=true&trust=TRUST_ALL_CERTIFICATES"
        config = Neo4jPoolConfig.from_url(url)

        self.assertEqual(config.connection_url, url)

    def test_max_idle_time_validation(self):
        """Test max_idle_time validation."""
        with self.assertRaisesRegex(ValueError, "max_idle_time must be positive"):
            Neo4jPoolConfig(host="localhost", max_idle_time=-1)

    def test_max_lifetime_validation(self):
        """Test max_lifetime validation."""
        with self.assertRaisesRegex(ValueError, "max_lifetime must be positive"):
            Neo4jPoolConfig(host="localhost", max_lifetime=-1)

    def test_bolt_vs_http_scheme(self):
        """Test Bolt vs HTTP scheme."""
        config_bolt = Neo4jPoolConfig(host="localhost", port=7687, use_bolt=True)

        config_http = Neo4jPoolConfig(host="localhost", port=7474, use_http=True)

        url_bolt = config_bolt.get_connection_url()
        url_http = config_http.get_connection_url()

        self.assertTrue(url_bolt.startswith("bolt://"))
        self.assertTrue(url_http.startswith("http://"))

    def test_cluster_routing(self):
        """Test cluster routing configuration."""
        config = Neo4jPoolConfig(
            host="localhost",
            routing=True,
            routing_context={"address": "localhost:7687"},
        )

        params = config.get_connection_params()
        self.assertTrue(params.get("routing", False))

    def test_connection_acquisition_timeout(self):
        """Test connection acquisition timeout."""
        config = Neo4jPoolConfig(host="localhost", connection_acquisition_timeout=60)

        params = config.get_connection_params()
        self.assertEqual(params["connection_acquisition_timeout"], 60)

    def test_max_connection_lifetime(self):
        """Test max connection lifetime."""
        config = Neo4jPoolConfig(host="localhost", max_connection_lifetime=3600)

        params = config.get_connection_params()
        self.assertEqual(params["max_connection_lifetime"], 3600)

    def test_keep_alive(self):
        """Test keep alive configuration."""
        config = Neo4jPoolConfig(host="localhost", keep_alive=True)

        params = config.get_connection_params()
        self.assertTrue(params["keep_alive"])


if __name__ == "__main__":
    unittest.main()
