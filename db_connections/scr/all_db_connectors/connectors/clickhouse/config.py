"""ClickHouse-specific configuration."""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from db_connections.scr.all_db_connectors.core import BasePoolConfig


@dataclass
class ClickHousePoolConfig(BasePoolConfig):
    """Configuration for ClickHouse connection pool.

    Extends BasePoolConfig with ClickHouse-specific settings.
    """

    # Connection parameters
    host: str = "localhost"
    """ClickHouse host address."""

    port: int = 9000
    """ClickHouse port (9000 for native, 8123 for HTTP)."""

    database: str = "default"
    """Database name."""

    username: Optional[str] = None
    """ClickHouse username."""

    password: Optional[str] = None
    """ClickHouse password."""

    # Connection string alternative
    connection_url: Optional[str] = None
    """Complete ClickHouse connection URL (overrides individual parameters if provided).
    
    Format: clickhouse://[username:password@]host[:port][/database][?options]
    or: clickhouses://[username:password@]host[:port][/database][?options] (for SSL)
    """

    # Connection behavior
    timeout: int = 30
    """Query timeout in seconds."""

    connect_timeout: Optional[int] = None
    """Connection timeout in seconds (None = uses timeout value)."""

    send_receive_timeout: Optional[int] = None
    """Send/receive timeout in seconds."""

    # SSL/TLS settings
    secure: bool = False
    """Enable SSL/TLS connection."""

    verify: bool = True
    """Verify SSL certificates."""

    ca_certs: Optional[str] = None
    """Path to CA certificate file."""

    cert: Optional[str] = None
    """Path to client certificate file."""

    key: Optional[str] = None
    """Path to client private key file."""

    # Protocol selection
    use_http: bool = False
    """Use HTTP interface instead of native protocol."""

    # Pool settings (aliases for base class compatibility)
    max_connections: Optional[int] = field(default=None, repr=False, init=True)
    """Maximum number of connections (alias for max_size)."""

    min_connections: Optional[int] = field(default=None, repr=False, init=True)
    """Minimum number of connections (alias for min_size)."""

    max_idle_time: Optional[int] = field(default=None, repr=False, init=True)
    """Max idle time in seconds (alias for idle_timeout)."""

    # ClickHouse-specific settings
    compression: bool = False
    """Enable compression."""

    settings: Optional[Dict[str, Any]] = None
    """ClickHouse query settings."""

    client_name: Optional[str] = None
    """Client name for connection tracking."""

    # Cluster settings
    cluster: Optional[str] = None
    """Cluster name."""

    alt_hosts: Optional[List[str]] = None
    """Alternative hosts for failover."""

    # Performance settings
    insert_block_size: Optional[int] = None
    """Insert block size in bytes."""

    max_block_size: Optional[int] = None
    """Maximum block size for reading."""

    # Connection pooling (for HTTP interface)
    pool_connections: Optional[int] = None
    """Number of connection pools for HTTP."""

    pool_maxsize: Optional[int] = None
    """Maximum number of connections per pool for HTTP."""

    def __post_init__(self):
        """Validate configuration after initialization."""
        # Sync alias fields with base class fields
        if self.max_connections is not None:
            self.max_size = self.max_connections
        else:
            self.max_connections = self.max_size

        if self.min_connections is not None:
            self.min_size = self.min_connections
        else:
            self.min_connections = self.min_size

        if self.max_idle_time is not None:
            self.idle_timeout = self.max_idle_time
        else:
            self.max_idle_time = self.idle_timeout

        # Validate connection parameters if not using connection URL
        if not self.connection_url:
            if not self.host:
                raise ValueError("host is required when not using connection_url")

            # Validate port
            if not (1 <= self.port <= 65535):
                raise ValueError("port must be between 1 and 65535")

        # Validate timeouts
        if self.timeout <= 0:
            raise ValueError("timeout must be positive")

        if self.connect_timeout is not None and self.connect_timeout <= 0:
            raise ValueError("connect_timeout must be positive if provided")

        if self.send_receive_timeout is not None and self.send_receive_timeout <= 0:
            raise ValueError("send_receive_timeout must be positive if provided")

        # Validate max_connections
        if self.max_size <= 0:
            raise ValueError("max_connections must be positive")

        # Validate min_connections
        if self.min_size <= 0:
            raise ValueError("min_connections must be positive")

        # Validate min <= max
        if self.min_size > self.max_size:
            raise ValueError("min_connections cannot be greater than max_connections")

        # Validate max_idle_time
        if self.idle_timeout <= 0:
            raise ValueError("max_idle_time must be positive")

        # Validate max_lifetime
        if self.max_lifetime <= 0:
            raise ValueError("max_lifetime must be positive")

        # Initialize settings if None
        if self.settings is None:
            self.settings = {}

        # Initialize alt_hosts if None
        if self.alt_hosts is None:
            self.alt_hosts = []

    def get_connection_params(self) -> Dict[str, Any]:
        """Get connection parameters as a dictionary.

        Returns:
            Dictionary of connection parameters suitable for clickhouse_connect.get_client().
        """
        params: Dict[str, Any] = {}

        if self.connection_url:
            params["url"] = self.connection_url
        else:
            params["host"] = self.host
            params["port"] = self.port

            if self.username:
                params["user"] = self.username
            if self.password:
                params["password"] = self.password
            if self.database:
                params["database"] = self.database

        # Timeouts
        params["timeout"] = self.timeout
        if self.connect_timeout is not None:
            params["connect_timeout"] = self.connect_timeout
        if self.send_receive_timeout is not None:
            params["send_receive_timeout"] = self.send_receive_timeout

        # SSL/TLS
        if self.secure:
            params["secure"] = True
            params["verify"] = self.verify
            if self.ca_certs:
                params["ca_certs"] = self.ca_certs
            if self.cert:
                params["cert"] = self.cert
            if self.key:
                params["key"] = self.key

        # Protocol
        if self.use_http:
            params["use_http"] = True

        # Compression
        if self.compression:
            params["compression"] = True

        # Settings
        if self.settings:
            params["settings"] = self.settings

        # Client name
        if self.client_name:
            params["client_name"] = self.client_name

        # Cluster
        if self.cluster:
            params["cluster"] = self.cluster

        # Alternative hosts
        if self.alt_hosts:
            params["alt_hosts"] = self.alt_hosts

        # Performance settings
        if self.insert_block_size is not None:
            params["insert_block_size"] = self.insert_block_size
        if self.max_block_size is not None:
            params["max_block_size"] = self.max_block_size

        # HTTP pool settings
        if self.pool_connections is not None:
            params["pool_connections"] = self.pool_connections
        if self.pool_maxsize is not None:
            params["pool_maxsize"] = self.pool_maxsize

        return params

    def get_connection_url(self) -> str:
        """Build a ClickHouse connection URL.

        Returns:
            Connection string in URL format.
        """
        if self.connection_url:
            return self.connection_url

        # Determine protocol
        protocol = "clickhouses" if self.secure else "clickhouse"

        # Build auth part
        auth = ""
        if self.username and self.password:
            auth = f"{self.username}:{self.password}@"
        elif self.username:
            auth = f"{self.username}@"

        # Build URL
        url = f"{protocol}://{auth}{self.host}:{self.port}/{self.database}"

        return url

    @classmethod
    def from_url(cls, url: str, **kwargs) -> "ClickHousePoolConfig":
        """Create configuration from a ClickHouse connection URL.

        Args:
            url: ClickHouse connection URL.
            **kwargs: Additional configuration parameters.

        Returns:
            ClickHousePoolConfig instance.
        """
        return cls(connection_url=url, **kwargs)

    @classmethod
    def from_env(cls, prefix: str = "CLICKHOUSE_") -> "ClickHousePoolConfig":
        """Create configuration from environment variables.

        Args:
            prefix: Environment variable prefix (default: CLICKHOUSE_).

        Returns:
            ClickHousePoolConfig instance.

        Example:
            Set environment variables:
            - CLICKHOUSE_HOST=localhost
            - CLICKHOUSE_PORT=9000
            - CLICKHOUSE_DATABASE=mydb
            - CLICKHOUSE_USERNAME=myuser
            - CLICKHOUSE_PASSWORD=secret
            - CLICKHOUSE_CONNECTION_URL=clickhouse://localhost:9000/mydb
        """
        import os

        return cls(
            host=os.getenv(f"{prefix}HOST", "localhost"),
            port=int(os.getenv(f"{prefix}PORT", "9000")),
            database=os.getenv(f"{prefix}DATABASE", "default"),
            username=os.getenv(f"{prefix}USERNAME"),
            password=os.getenv(f"{prefix}PASSWORD"),
            connection_url=os.getenv(f"{prefix}CONNECTION_URL"),
            secure=os.getenv(f"{prefix}SECURE", "false").lower() == "true",
            use_http=os.getenv(f"{prefix}USE_HTTP", "false").lower() == "true",
        )
