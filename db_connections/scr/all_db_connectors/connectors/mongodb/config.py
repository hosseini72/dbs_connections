"""MongoDB-specific configuration."""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Union
from db_connections.scr.all_db_connectors.core import BasePoolConfig


@dataclass
class MongoPoolConfig(BasePoolConfig):
    """Configuration for MongoDB connection pool.

    Extends BasePoolConfig with MongoDB-specific settings.
    """

    # Connection parameters
    host: str = "localhost"
    """MongoDB host address."""

    port: int = 27017
    """MongoDB port."""

    database: Optional[str] = None
    """Database name."""

    username: Optional[str] = None
    """MongoDB username."""

    password: Optional[str] = None
    """MongoDB password."""

    # Connection string alternative
    connection_string: Optional[str] = None
    """Complete MongoDB connection URI (overrides individual parameters if provided).
    
    Format: mongodb://[username:password@]host[:port][/database][?options]
    or: mongodb+srv://[username:password@]host[/database][?options] (for Atlas)
    """

    # Authentication
    auth_source: Optional[str] = None
    """Authentication database (defaults to 'admin' or specified database)."""

    auth_mechanism: Optional[str] = None
    """Authentication mechanism: SCRAM-SHA-1, SCRAM-SHA-256, MONGODB-CR, etc."""

    # Connection behavior
    connect_timeout_ms: int = 20000
    """Connection timeout in milliseconds."""

    socket_timeout_ms: Optional[int] = None
    """Socket timeout in milliseconds (None = no timeout)."""

    server_selection_timeout_ms: int = 30000
    """Server selection timeout in milliseconds."""

    heartbeat_frequency_ms: int = 10000
    """Heartbeat frequency in milliseconds."""

    # Pool settings (aliases for compatibility)
    max_pool_size: Optional[int] = None
    """Maximum pool size (overrides max_size if provided)."""

    min_pool_size: Optional[int] = None
    """Minimum pool size (overrides min_size if provided)."""

    max_idle_time_ms: Optional[int] = None
    """Max idle time in milliseconds (overrides idle_timeout if provided)."""

    # Aliases for test compatibility
    max_connections: Optional[int] = field(default=None, repr=False, init=True)
    """Alias for max_size (for test compatibility)."""

    min_connections: Optional[int] = field(default=None, repr=False, init=True)
    """Alias for min_size (for test compatibility)."""

    max_idle_time: Optional[int] = field(default=None, repr=False, init=True)
    """Alias for idle_timeout in seconds (for test compatibility)."""

    # Multiple hosts support
    hosts: Optional[List[str]] = None
    """List of host:port strings for multiple hosts."""

    # SSL/TLS settings (with ssl alias for compatibility)
    ssl: Optional[bool] = field(default=None, repr=False, init=True)
    """Alias for tls (for test compatibility)."""

    ssl_cert_reqs: Optional[str] = field(default=None, repr=False, init=True)
    """SSL certificate requirements (for test compatibility)."""

    ssl_ca_certs: Optional[str] = field(default=None, repr=False, init=True)
    """Path to CA certificate file (alias for tls_ca_file)."""

    ssl_certfile: Optional[str] = field(default=None, repr=False, init=True)
    """Path to client certificate file (alias for tls_certificate_key_file)."""

    ssl_keyfile: Optional[str] = field(default=None, repr=False, init=True)
    """Path to client private key file (alias for tls_certificate_key_file)."""

    use_srv: bool = False
    """Use SRV record (mongodb+srv://)."""

    # Write concern (direct parameters for compatibility)
    w: Optional[Union[int, str]] = field(default=None, repr=False, init=True)
    """Write concern w parameter."""

    wtimeout: Optional[int] = field(default=None, repr=False, init=True)
    """Write concern timeout in milliseconds."""

    journal: Optional[bool] = field(default=None, repr=False, init=True)
    """Write concern journal parameter."""

    # Auth mechanism properties
    auth_mechanism_properties: Optional[Dict[str, Any]] = None
    """Auth mechanism properties (e.g., for GSSAPI)."""

    wait_queue_timeout_ms: Optional[int] = None
    """Wait queue timeout in milliseconds."""

    wait_queue_multiple: Optional[int] = None
    """Multiplier for wait queue size based on max_pool_size."""

    # Replica set and sharding
    replica_set: Optional[str] = None
    """Replica set name."""

    read_preference: str = "primary"
    """Read preference: primary, primaryPreferred, secondary, secondaryPreferred, nearest."""

    read_concern_level: Optional[str] = None
    """Read concern level: local, majority, linearizable, snapshot."""

    write_concern: Optional[Dict[str, Any]] = None
    """Write concern dictionary (e.g., {'w': 1, 'j': True})."""

    # SSL/TLS settings
    tls: bool = False
    """Enable TLS/SSL."""

    tls_certificate_key_file: Optional[str] = None
    """Path to client certificate and key file."""

    tls_certificate_key_file_password: Optional[str] = None
    """Password for client certificate key file."""

    tls_ca_file: Optional[str] = None
    """Path to CA certificate file."""

    tls_allow_invalid_certificates: bool = False
    """Allow invalid certificates (for testing only)."""

    tls_allow_invalid_hostnames: bool = False
    """Allow invalid hostnames (for testing only)."""

    tls_crl_file: Optional[str] = None
    """Path to certificate revocation list file."""

    # Compression
    compressors: Optional[List[str]] = None
    """List of compressors: snappy, zlib, zstd."""

    zlib_compression_level: Optional[int] = None
    """Zlib compression level (1-9)."""

    # Additional options
    retry_writes: bool = True
    """Enable retryable writes."""

    retry_reads: bool = True
    """Enable retryable reads."""

    direct_connection: bool = False
    """Connect directly to a single host."""

    server_selection_try_once: bool = False
    """Only try to select a server once."""

    # Application metadata
    app_name: Optional[str] = None
    """Application name for connection tracking."""

    # Additional MongoDB client options
    extra_options: Optional[Dict[str, Any]] = None
    """Additional MongoDB client options."""

    def __post_init__(self):
        """Validate configuration after initialization."""
        # Handle hosts parameter (convert to host if single, or use hosts)
        if self.hosts:
            if len(self.hosts) == 1:
                # Single host, extract host and port
                host_port = self.hosts[0]
                if ":" in host_port:
                    self.host, port_str = host_port.rsplit(":", 1)
                    self.port = int(port_str)
                else:
                    self.host = host_port
            # hosts will be used in get_connection_params

        # Sync alias fields with base class fields
        if self.max_connections is not None:
            self.max_size = self.max_connections
            self.max_pool_size = self.max_connections
        elif self.max_pool_size is not None:
            self.max_size = self.max_pool_size
        else:
            self.max_connections = self.max_size
            self.max_pool_size = self.max_size

        if self.min_connections is not None:
            self.min_size = self.min_connections
            self.min_pool_size = self.min_connections
        elif self.min_pool_size is not None:
            self.min_size = self.min_pool_size
        else:
            self.min_connections = self.min_size
            self.min_pool_size = self.min_size

        # Note: Pool size validation (max_connections, min_connections, min <= max)
        # is deferred to validate_pool_config() which is called during pool creation.
        # This allows tests to create invalid configs and verify that pool.__init__
        # validates them with MongoDB-specific error messages.

        # Sync max_idle_time and max_idle_time_ms with idle_timeout
        if self.max_idle_time is not None:
            self.idle_timeout = self.max_idle_time
            self.max_idle_time_ms = int(self.max_idle_time * 1000)
        elif self.max_idle_time_ms is not None:
            # Convert milliseconds to seconds
            self.idle_timeout = self.max_idle_time_ms / 1000
            self.max_idle_time = int(self.idle_timeout)
        else:
            # Convert seconds to milliseconds
            self.max_idle_time_ms = int(self.idle_timeout * 1000)
            self.max_idle_time = int(self.idle_timeout)

        # Handle ssl alias
        if self.ssl is not None:
            self.tls = self.ssl

        # Handle ssl_cert aliases
        if self.ssl_ca_certs:
            self.tls_ca_file = self.ssl_ca_certs
        if self.ssl_certfile:
            self.tls_certificate_key_file = self.ssl_certfile
        if self.ssl_keyfile:
            # If both certfile and keyfile, they should be combined
            # For now, use certfile as the combined file
            if not self.tls_certificate_key_file:
                self.tls_certificate_key_file = self.ssl_keyfile

        # Handle write concern direct parameters
        if self.w is not None or self.wtimeout is not None or self.journal is not None:
            if self.write_concern is None:
                self.write_concern = {}
            if self.w is not None:
                self.write_concern["w"] = self.w
            if self.wtimeout is not None:
                self.write_concern["wtimeout"] = self.wtimeout
            if self.journal is not None:
                self.write_concern["j"] = self.journal

        # Validate connection parameters if not using connection string
        if not self.connection_string:
            if not self.host and not self.hosts:
                raise ValueError("host is required when not using connection_string")

            # Validate port
            if not (1 <= self.port <= 65535):
                raise ValueError("port must be between 1 and 65535")

        # Validate timeout (from base class)
        if self.timeout <= 0:
            raise ValueError("timeout must be positive")

        # Validate max_lifetime (from base class)
        if self.max_lifetime <= 0:
            raise ValueError("max_lifetime must be positive")

        # Note: max_connections, min_connections, and min <= max validation
        # is deferred to validate_pool_config() which is called during pool creation.
        # This allows tests to verify that pool creation validates the config.
        # However, we override validate_pool_config behavior in the pool's __init__
        # to provide MongoDB-specific error messages.

        # Validate max_idle_time
        if self.idle_timeout <= 0:
            raise ValueError("max_idle_time must be positive")

        # Validate read preference
        valid_read_preferences = {
            "primary",
            "primaryPreferred",
            "secondary",
            "secondaryPreferred",
            "nearest",
        }
        if self.read_preference not in valid_read_preferences:
            raise ValueError(
                f"Invalid read_preference: {self.read_preference}. "
                f"Must be one of {valid_read_preferences}"
            )

        # Validate auth mechanism
        if self.auth_mechanism:
            valid_auth_mechanisms = {
                "SCRAM-SHA-1",
                "SCRAM-SHA-256",
                "MONGODB-CR",
                "MONGODB-X509",
                "PLAIN",
                "GSSAPI",
            }
            if self.auth_mechanism not in valid_auth_mechanisms:
                raise ValueError(
                    f"Invalid auth_mechanism: {self.auth_mechanism}. "
                    f"Must be one of {valid_auth_mechanisms}"
                )

        # Validate read concern level
        if self.read_concern_level:
            valid_read_concern_levels = {
                "local",
                "majority",
                "linearizable",
                "snapshot",
            }
            if self.read_concern_level not in valid_read_concern_levels:
                raise ValueError(
                    f"Invalid read_concern_level: {self.read_concern_level}. "
                    f"Must be one of {valid_read_concern_levels}"
                )

        # Validate timeouts
        if self.connect_timeout_ms <= 0:
            raise ValueError("connect_timeout_ms must be positive")

        if self.server_selection_timeout_ms <= 0:
            raise ValueError("server_selection_timeout_ms must be positive")

        if self.socket_timeout_ms is not None and self.socket_timeout_ms <= 0:
            raise ValueError("socket_timeout_ms must be positive if provided")

        # Initialize extra_options if None
        if self.extra_options is None:
            self.extra_options = {}

    def get_connection_params(self) -> Dict[str, Any]:
        """Get connection parameters as a dictionary.

        Returns:
            Dictionary of connection parameters suitable for pymongo.MongoClient/motor.AsyncIOMotorClient.
        """
        params: Dict[str, Any] = {}

        if self.connection_string:
            params["uri"] = self.connection_string
        elif self.hosts:
            # Use hosts list
            params["host"] = self.hosts
        else:
            # Build host list or single host
            if isinstance(self.host, list):
                hosts = [f"{h}:{self.port}" for h in self.host]
                params["host"] = hosts
            else:
                params["host"] = self.host
                params["port"] = self.port

        # Authentication
        if self.username:
            params["username"] = self.username
        if self.password:
            params["password"] = self.password
        if self.auth_source:
            params["authSource"] = self.auth_source
        if self.auth_mechanism:
            params["authMechanism"] = self.auth_mechanism
        if self.auth_mechanism_properties:
            params["authMechanismProperties"] = self.auth_mechanism_properties

        # Database
        if self.database:
            params["database"] = self.database

        # Timeouts (convert to seconds for pymongo/motor)
        params["connectTimeoutMS"] = self.connect_timeout_ms
        if self.socket_timeout_ms:
            params["socketTimeoutMS"] = self.socket_timeout_ms
        params["serverSelectionTimeoutMS"] = self.server_selection_timeout_ms
        params["heartbeatFrequencyMS"] = self.heartbeat_frequency_ms

        # Pool settings
        params["maxPoolSize"] = self.max_pool_size
        params["minPoolSize"] = self.min_pool_size
        params["maxIdleTimeMS"] = self.max_idle_time_ms
        if self.wait_queue_timeout_ms:
            params["waitQueueTimeoutMS"] = self.wait_queue_timeout_ms
        if self.wait_queue_multiple:
            params["waitQueueMultiple"] = self.wait_queue_multiple

        # Replica set and sharding
        if self.replica_set:
            params["replicaSet"] = self.replica_set

        # Read preference
        params["readPreference"] = self.read_preference

        # Read concern
        if self.read_concern_level:
            params["readConcernLevel"] = self.read_concern_level

        # Write concern
        if self.write_concern:
            params["w"] = self.write_concern.get("w", 1)
            if "j" in self.write_concern:
                params["journal"] = self.write_concern["j"]
            if "wtimeout" in self.write_concern:
                params["wtimeout"] = self.write_concern["wtimeout"]
        elif (
            self.w is not None or self.wtimeout is not None or self.journal is not None
        ):
            if self.w is not None:
                params["w"] = self.w
            if self.wtimeout is not None:
                params["wtimeout"] = self.wtimeout
            if self.journal is not None:
                params["journal"] = self.journal

        # TLS/SSL
        if self.tls or (self.ssl is not None and self.ssl):
            params["ssl"] = True
            params["tls"] = True
            if self.ssl_cert_reqs:
                params["ssl_cert_reqs"] = self.ssl_cert_reqs
            if self.tls_certificate_key_file or self.ssl_certfile:
                params["ssl_certfile"] = (
                    self.ssl_certfile or self.tls_certificate_key_file
                )
                params["tlsCertificateKeyFile"] = (
                    self.ssl_certfile or self.tls_certificate_key_file
                )
            if self.ssl_keyfile:
                params["ssl_keyfile"] = self.ssl_keyfile
            if self.tls_certificate_key_file_password:
                params["tlsCertificateKeyFilePassword"] = (
                    self.tls_certificate_key_file_password
                )
            if self.tls_ca_file or self.ssl_ca_certs:
                params["ssl_ca_certs"] = self.ssl_ca_certs or self.tls_ca_file
                params["tlsCAFile"] = self.ssl_ca_certs or self.tls_ca_file
            params["tlsAllowInvalidCertificates"] = self.tls_allow_invalid_certificates
            params["tlsAllowInvalidHostnames"] = self.tls_allow_invalid_hostnames
            if self.tls_crl_file:
                params["tlsCRLFile"] = self.tls_crl_file

        # Compression
        if self.compressors:
            params["compressors"] = self.compressors
        if self.zlib_compression_level:
            params["zlibCompressionLevel"] = self.zlib_compression_level

        # Retry settings
        params["retryWrites"] = self.retry_writes
        params["retryReads"] = self.retry_reads

        # Connection mode
        params["directConnection"] = self.direct_connection
        # Note: serverSelectionTryOnce is not a valid pymongo option - removed

        # Application name
        if self.app_name:
            params["appName"] = self.app_name

        # Merge extra options (extra_options override explicit params)
        if self.extra_options:
            params.update(self.extra_options)

        return params

    def get_connection_string(self) -> str:
        """Build a MongoDB connection URI.

        Returns:
            Connection string in URI format.
        """
        if self.connection_string:
            return self.connection_string

        # Determine protocol
        if self.use_srv:
            protocol = "mongodb+srv"
        elif self.tls or (self.ssl is not None and self.ssl):
            protocol = "mongodb+srv"  # Use srv for SSL
        else:
            protocol = "mongodb"

        # Build auth part
        auth = ""
        if self.username and self.password:
            auth = f"{self.username}:{self.password}@"
        elif self.username:
            auth = f"{self.username}@"

        # Build host part
        if self.hosts:
            hosts = ",".join(self.hosts)
        elif isinstance(self.host, list):
            hosts = ",".join([f"{h}:{self.port}" for h in self.host])
        else:
            hosts = f"{self.host}:{self.port}"

        # Build URI
        if self.database:
            uri = f"{protocol}://{auth}{hosts}/{self.database}"
        else:
            uri = f"{protocol}://{auth}{hosts}/"

        # Add query parameters
        params = []
        if self.auth_source:
            params.append(f"authSource={self.auth_source}")
        if self.auth_mechanism:
            params.append(f"authMechanism={self.auth_mechanism}")
        if self.replica_set:
            params.append(f"replicaSet={self.replica_set}")
        if self.read_preference != "primary":
            params.append(f"readPreference={self.read_preference}")
        if self.tls or (self.ssl is not None and self.ssl):
            params.append("ssl=true")
            params.append("tls=true")

        if params:
            uri += "?" + "&".join(params)

        return uri

    @classmethod
    def from_uri(cls, uri: str, **kwargs) -> "MongoPoolConfig":
        """Create configuration from a MongoDB connection URI.

        Args:
            uri: MongoDB connection URI.
            **kwargs: Additional configuration parameters.

        Returns:
            MongoPoolConfig instance.
        """
        return cls(connection_string=uri, **kwargs)

    @classmethod
    def from_env(cls, prefix: str = "MONGO_") -> "MongoPoolConfig":
        """Create configuration from environment variables.

        Args:
            prefix: Environment variable prefix (default: MONGO_).

        Returns:
            MongoPoolConfig instance.

        Example:
            Set environment variables:
            - MONGO_HOST=localhost
            - MONGO_PORT=27017
            - MONGO_DATABASE=mydb
            - MONGO_USERNAME=myuser
            - MONGO_PASSWORD=secret
            - MONGO_CONNECTION_STRING=mongodb://localhost:27017/mydb
        """
        import os

        return cls(
            host=os.getenv(f"{prefix}HOST", "localhost"),
            port=int(os.getenv(f"{prefix}PORT", "27017")),
            database=os.getenv(f"{prefix}DATABASE"),  # None if not set
            username=os.getenv(f"{prefix}USERNAME"),
            password=os.getenv(f"{prefix}PASSWORD"),
            connection_string=os.getenv(f"{prefix}CONNECTION_STRING"),
            auth_source=os.getenv(f"{prefix}AUTH_SOURCE"),
            replica_set=os.getenv(f"{prefix}REPLICA_SET"),
            tls=os.getenv(f"{prefix}TLS", "false").lower() == "true",
        )
