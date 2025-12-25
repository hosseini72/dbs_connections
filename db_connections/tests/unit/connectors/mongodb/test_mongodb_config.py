"""Unit tests for MongoDB configuration."""

import sys
from pathlib import Path
import os
import unittest

# Add parent directory to path to allow imports
# test_mongodb_config.py is at: db_connections/tests/unit/connectors/mongodb/
# We need to go up 6 levels to get to the parent of db_connections
try:
    _file_path = Path(__file__).resolve()
except NameError:
    # __file__ might not be defined in some contexts
    _file_path = Path(os.getcwd()) / 'tests' / 'unit' / 'connectors' / 'mongodb' / 'test_mongodb_config.py'  # noqa: E501

parent_dir = _file_path.parent.parent.parent.parent.parent.parent
parent_dir_str = str(parent_dir)
if parent_dir_str not in sys.path:
    sys.path.insert(0, parent_dir_str)

from db_connections.scr.all_db_connectors.connectors.mongodb.config import (  # noqa: E402, E501
    MongoPoolConfig
)


class TestMongoPoolConfig(unittest.TestCase):
    """Test MongoPoolConfig class."""

    def test_default_values(self):
        """Test default configuration values."""
        config = MongoPoolConfig(
            host="localhost"
        )
        
        self.assertEqual(config.host, "localhost")
        self.assertEqual(config.port, 27017)
        self.assertIsNone(config.username)
        self.assertIsNone(config.password)
        self.assertIsNone(config.database)
        self.assertEqual(config.timeout, 30)
        self.assertEqual(config.max_connections, 10)
        self.assertEqual(config.min_connections, 1)
    
    def test_custom_values(self):
        """Test custom configuration values."""
        config = MongoPoolConfig(
            host="mongo.example.com",
            port=27018,
            username="admin",
            password="secret",
            database="mydb",
            auth_source="admin",
            auth_mechanism="SCRAM-SHA-256",
            timeout=60,
            max_connections=20,
            min_connections=5,
            max_idle_time=120,
            max_lifetime=7200
        )
        
        self.assertEqual(config.host, "mongo.example.com")
        self.assertEqual(config.port, 27018)
        self.assertEqual(config.username, "admin")
        self.assertEqual(config.password, "secret")
        self.assertEqual(config.database, "mydb")
        self.assertEqual(config.auth_source, "admin")
        self.assertEqual(config.auth_mechanism, "SCRAM-SHA-256")
        self.assertEqual(config.timeout, 60)
        self.assertEqual(config.max_connections, 20)
        self.assertEqual(config.min_connections, 5)
        self.assertEqual(config.max_idle_time, 120)
        self.assertEqual(config.max_lifetime, 7200)

    def test_missing_required_fields(self):
        """Test validation of required fields."""
        with self.assertRaisesRegex(ValueError, "host is required"):
            MongoPoolConfig(host="")
        
        with self.assertRaisesRegex(ValueError, "host is required"):
            MongoPoolConfig(host=None)
    
    def test_invalid_port(self):
        """Test validation of port."""
        with self.assertRaisesRegex(ValueError, "port must be between"):
            MongoPoolConfig(host="localhost", port=0)
        
        with self.assertRaisesRegex(ValueError, "port must be between"):
            MongoPoolConfig(host="localhost", port=65536)
    
    def test_invalid_timeout(self):
        """Test validation of timeout."""
        with self.assertRaisesRegex(ValueError, "timeout must be positive"):
            MongoPoolConfig(host="localhost", timeout=-1)
    
    def test_invalid_max_connections(self):
        """Test that invalid max_connections can be set (validation happens in pool)."""
        # Config creation succeeds (validation deferred to pool)
        config = MongoPoolConfig(host="localhost", max_connections=0)
        self.assertEqual(config.max_connections, 0)
        self.assertEqual(config.max_size, 0)
    
    def test_invalid_min_connections(self):
        """Test that invalid min_connections can be set (validation happens in pool)."""
        # Config creation succeeds (validation deferred to pool)
        config = MongoPoolConfig(host="localhost", min_connections=0)
        self.assertEqual(config.min_connections, 0)
        self.assertEqual(config.min_size, 0)
    
    def test_min_greater_than_max(self):
        """Test that min_connections > max_connections can be set (validation happens in pool)."""
        # Config creation succeeds (validation deferred to pool)
        config = MongoPoolConfig(
            host="localhost",
            min_connections=20,
            max_connections=10
        )
        self.assertEqual(config.min_connections, 20)
        self.assertEqual(config.max_connections, 10)
    
    def test_invalid_auth_mechanism(self):
        """Test validation of auth mechanism."""
        with self.assertRaisesRegex(ValueError, "Invalid auth_mechanism"):
            MongoPoolConfig(
                host="localhost",
                auth_mechanism="INVALID"
            )
    
    def test_valid_auth_mechanisms(self):
        """Test all valid auth mechanisms."""
        valid_mechanisms = [
            "SCRAM-SHA-1",
            "SCRAM-SHA-256",
            "MONGODB-CR",
            "MONGODB-X509",
            "PLAIN",
            "GSSAPI"
        ]
        
        for mechanism in valid_mechanisms:
            config = MongoPoolConfig(
                host="localhost",
                auth_mechanism=mechanism
            )
            self.assertEqual(config.auth_mechanism, mechanism)
    
    def test_get_connection_params(self):
        """Test connection parameters generation."""
        config = MongoPoolConfig(
            host="localhost",
            port=27017,
            username="user",
            password="pass",
            database="testdb",
            auth_source="admin",
            auth_mechanism="SCRAM-SHA-256",
            connect_timeout_ms=5000,
            socket_timeout_ms=10000
        )
        
        params = config.get_connection_params()
        
        self.assertEqual(params["host"], "localhost")
        self.assertEqual(params["port"], 27017)
        self.assertEqual(params["username"], "user")
        self.assertEqual(params["password"], "pass")
        self.assertEqual(params["database"], "testdb")
        self.assertEqual(params["authSource"], "admin")
        self.assertEqual(params["authMechanism"], "SCRAM-SHA-256")
        self.assertEqual(params["connectTimeoutMS"], 5000)
        self.assertEqual(params["socketTimeoutMS"], 10000)
    
    def test_get_connection_params_no_auth(self):
        """Test connection parameters without authentication."""
        config = MongoPoolConfig(
            host="localhost",
            port=27017
        )
        
        params = config.get_connection_params()
        
        self.assertNotIn("username", params)
        self.assertNotIn("password", params)
        self.assertNotIn("authSource", params)
        self.assertNotIn("authMechanism", params)
    
    def test_get_connection_params_with_ssl(self):
        """Test connection parameters with SSL."""
        config = MongoPoolConfig(
            host="localhost",
            ssl=True,
            ssl_cert_reqs="REQUIRED",
            ssl_ca_certs="/path/to/ca.crt",
            ssl_certfile="/path/to/client.crt",
            ssl_keyfile="/path/to/client.key"
        )
        
        params = config.get_connection_params()
        
        self.assertTrue(params["ssl"])
        self.assertEqual(params["ssl_cert_reqs"], "REQUIRED")
        self.assertEqual(params["ssl_ca_certs"], "/path/to/ca.crt")
        self.assertEqual(params["ssl_certfile"], "/path/to/client.crt")
        self.assertEqual(params["ssl_keyfile"], "/path/to/client.key")
    
    def test_get_connection_string(self):
        """Test connection string generation."""
        config = MongoPoolConfig(
            host="localhost",
            port=27017,
            username="user",
            password="pass",
            database="testdb"
        )
        
        uri = config.get_connection_string()
        
        self.assertTrue(uri.startswith("mongodb://"))
        self.assertIn("user:pass", uri)
        self.assertIn("localhost:27017", uri)
        self.assertIn("/testdb", uri)
    
    def test_get_connection_string_no_auth(self):
        """Test connection string without authentication."""
        config = MongoPoolConfig(
            host="localhost",
            port=27017
        )
        
        uri = config.get_connection_string()
        
        self.assertEqual(uri, "mongodb://localhost:27017/")
    
    def test_get_connection_string_with_ssl(self):
        """Test connection string with SSL (mongodb+srv://)."""
        config = MongoPoolConfig(
            host="cluster.mongodb.net",
            ssl=True,
            use_srv=True
        )
        
        uri = config.get_connection_string()
        
        self.assertTrue(uri.startswith("mongodb+srv://"))
    
    def test_get_connection_string_replica_set(self):
        """Test connection string with replica set."""
        config = MongoPoolConfig(
            hosts=["localhost:27017", "localhost:27018", "localhost:27019"],
            replica_set="rs0"
        )
        
        uri = config.get_connection_string()
        
        self.assertIn("replicaSet=rs0", uri)
        self.assertIn("localhost:27017", uri)
        self.assertIn("localhost:27018", uri)
        self.assertIn("localhost:27019", uri)
    
    def test_from_uri(self):
        """Test creating config from URI."""
        uri = "mongodb://user:pass@localhost:27017/testdb?authSource=admin"
        
        config = MongoPoolConfig.from_uri(uri)
        
        self.assertEqual(config.connection_string, uri)
    
    def test_from_uri_with_additional_params(self):
        """Test creating config from URI with extra parameters."""
        uri = "mongodb://localhost:27017/testdb"
        
        config = MongoPoolConfig.from_uri(
            uri,
            max_connections=20,
            min_connections=5
        )
        
        self.assertEqual(config.connection_string, uri)
        self.assertEqual(config.max_connections, 20)
        self.assertEqual(config.min_connections, 5)
    
    def test_from_env(self):
        """Test creating config from environment variables."""
        # Set environment variables
        os.environ["MONGO_HOST"] = "envhost"
        os.environ["MONGO_PORT"] = "27018"
        os.environ["MONGO_USERNAME"] = "envuser"
        os.environ["MONGO_PASSWORD"] = "envpass"
        os.environ["MONGO_DATABASE"] = "envdb"
        os.environ["MONGO_AUTH_SOURCE"] = "admin"
        
        try:
            config = MongoPoolConfig.from_env()
            
            self.assertEqual(config.host, "envhost")
            self.assertEqual(config.port, 27018)
            self.assertEqual(config.username, "envuser")
            self.assertEqual(config.password, "envpass")
            self.assertEqual(config.database, "envdb")
            self.assertEqual(config.auth_source, "admin")
        finally:
            # Cleanup
            keys = ["HOST", "PORT", "USERNAME", "PASSWORD", "DATABASE", "AUTH_SOURCE"]
            for key in keys:
                os.environ.pop(f"MONGO_{key}", None)
    
    def test_from_env_custom_prefix(self):
        """Test creating config from environment with custom prefix."""
        # Set environment variables
        os.environ["DB_HOST"] = "customhost"
        os.environ["DB_PORT"] = "27019"
        os.environ["DB_DATABASE"] = "customdb"
        
        try:
            config = MongoPoolConfig.from_env(prefix="DB_")
            
            self.assertEqual(config.host, "customhost")
            self.assertEqual(config.port, 27019)
            self.assertEqual(config.database, "customdb")
        finally:
            # Cleanup
            for key in ["HOST", "PORT", "DATABASE"]:
                os.environ.pop(f"DB_{key}", None)
    
    def test_from_env_defaults(self):
        """Test from_env uses defaults for missing vars."""
        # Make sure env vars don't exist
        for key in ["HOST", "PORT", "DATABASE"]:
            os.environ.pop(f"TEST_MONGO_{key}", None)
        
        config = MongoPoolConfig.from_env(prefix="TEST_MONGO_")
        
        self.assertEqual(config.host, "localhost")
        self.assertEqual(config.port, 27017)
        self.assertIsNone(config.database)
    
    def test_connection_string_overrides(self):
        """Test that connection_string overrides individual params."""
        config = MongoPoolConfig(
            connection_string="mongodb://user:pass@remote:27018/remotedb",
            host="localhost",  # Should be ignored
            port=27017  # Should be ignored
        )
        
        params = config.get_connection_params()
        
        self.assertIn("uri", params)
        self.assertEqual(params["uri"], "mongodb://user:pass@remote:27018/remotedb")
    
    def test_read_preference(self):
        """Test read preference configuration."""
        config = MongoPoolConfig(
            host="localhost",
            read_preference="secondaryPreferred"
        )
        
        params = config.get_connection_params()
        
        self.assertEqual(params["readPreference"], "secondaryPreferred")
    
    def test_write_concern(self):
        """Test write concern configuration."""
        config = MongoPoolConfig(
            host="localhost",
            w=2,
            wtimeout=5000,
            journal=True
        )
        
        params = config.get_connection_params()
        
        self.assertEqual(params["w"], 2)
        self.assertEqual(params["wtimeout"], 5000)
        self.assertTrue(params["journal"])
    
    def test_read_concern(self):
        """Test read concern configuration."""
        config = MongoPoolConfig(
            host="localhost",
            read_concern_level="majority"
        )
        
        params = config.get_connection_params()
        
        self.assertEqual(params["readConcernLevel"], "majority")
    
    def test_retry_writes(self):
        """Test retry writes configuration."""
        config = MongoPoolConfig(
            host="localhost",
            retry_writes=True
        )
        
        params = config.get_connection_params()
        
        self.assertTrue(params["retryWrites"])
    
    def test_compressors(self):
        """Test compression configuration."""
        config = MongoPoolConfig(
            host="localhost",
            compressors=["snappy", "zlib"]
        )
        
        params = config.get_connection_params()
        
        self.assertEqual(params["compressors"], ["snappy", "zlib"])


class TestMongoPoolConfigEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions."""
    
    def test_special_characters_in_password(self):
        """Test password with special characters."""
        config = MongoPoolConfig(
            host="localhost",
            password="p@ssw0rd!#$%"
        )
        
        params = config.get_connection_params()
        self.assertEqual(params["password"], "p@ssw0rd!#$%")
    
    def test_ipv6_host(self):
        """Test IPv6 host address."""
        config = MongoPoolConfig(
            host="::1",
            port=27017
        )
        
        self.assertEqual(config.host, "::1")
    
    def test_unix_socket_host(self):
        """Test Unix socket host."""
        config = MongoPoolConfig(
            host="/var/run/mongodb/mongodb.sock"
        )
        
        self.assertEqual(config.host, "/var/run/mongodb/mongodb.sock")
    
    def test_connection_string_with_query_params(self):
        """Test connection string with query parameters."""
        uri = "mongodb://localhost:27017/testdb?authSource=admin&ssl=true"
        config = MongoPoolConfig.from_uri(uri)
        
        self.assertEqual(config.connection_string, uri)
    
    def test_max_idle_time_validation(self):
        """Test max_idle_time validation."""
        with self.assertRaisesRegex(ValueError, "max_idle_time must be positive"):
            MongoPoolConfig(host="localhost", max_idle_time=-1)
    
    def test_max_lifetime_validation(self):
        """Test max_lifetime validation."""
        with self.assertRaisesRegex(ValueError, "max_lifetime must be positive"):
            MongoPoolConfig(host="localhost", max_lifetime=-1)
    
    def test_multiple_hosts(self):
        """Test configuration with multiple hosts."""
        config = MongoPoolConfig(
            hosts=["host1:27017", "host2:27017", "host3:27017"]
        )
        
        self.assertEqual(len(config.hosts), 3)
        self.assertIn("host1:27017", config.hosts)
        self.assertIn("host2:27017", config.hosts)
        self.assertIn("host3:27017", config.hosts)
    
    def test_replica_set_configuration(self):
        """Test replica set configuration."""
        config = MongoPoolConfig(
            hosts=["localhost:27017", "localhost:27018"],
            replica_set="rs0",
            read_preference="secondary"
        )
        
        self.assertEqual(config.replica_set, "rs0")
        self.assertEqual(config.read_preference, "secondary")
    
    def test_auth_mechanism_properties(self):
        """Test auth mechanism properties."""
        config = MongoPoolConfig(
            host="localhost",
            auth_mechanism="GSSAPI",
            auth_mechanism_properties={
                "SERVICE_NAME": "mongodb",
                "CANONICALIZE_HOST_NAME": "true"
            }
        )
        
        params = config.get_connection_params()
        self.assertIn("authMechanismProperties", params)
    
    def test_server_selection_timeout(self):
        """Test server selection timeout."""
        config = MongoPoolConfig(
            host="localhost",
            server_selection_timeout_ms=5000
        )
        
        params = config.get_connection_params()
        self.assertEqual(params["serverSelectionTimeoutMS"], 5000)
    
    def test_heartbeat_frequency(self):
        """Test heartbeat frequency."""
        config = MongoPoolConfig(
            host="localhost",
            heartbeat_frequency_ms=10000
        )
        
        params = config.get_connection_params()
        self.assertEqual(params["heartbeatFrequencyMS"], 10000)


if __name__ == '__main__':
    unittest.main()
