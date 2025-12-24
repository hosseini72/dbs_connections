"""Redis-specific configuration."""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from db_connections.scr.all_db_connectors.core import BasePoolConfig


@dataclass
class RedisPoolConfig(BasePoolConfig):
    """Configuration for Redis connection pool.
    
    Extends BasePoolConfig with Redis-specific settings.
    """
    
    # Connection parameters
    host: str = "localhost"
    """Redis host address."""
    
    port: int = 6379
    """Redis port."""
    
    db: int = 0
    """Redis database number (0-15)."""
    
    password: Optional[str] = None
    """Redis password (if required)."""
    
    username: Optional[str] = None
    """Redis username (for Redis 6+ ACL)."""
    
    # Connection behavior
    timeout: Optional[float] = 10
    """Socket timeout in seconds (default: 10)."""
    
    socket_timeout: Optional[float] = None
    """Socket timeout in seconds (None = uses timeout value)."""
    
    socket_connect_timeout: Optional[float] = None
    """Socket connection timeout in seconds (None = no timeout)."""
    
    socket_keepalive: bool = False
    """Enable TCP keepalive."""
    
    socket_keepalive_options: Optional[Dict[str, Any]] = None
    """TCP keepalive options."""
    
    # SSL/TLS settings
    ssl: bool = False
    """Enable SSL/TLS connection."""
    
    ssl_cert_reqs: str = "required"
    """SSL certificate requirements: none, optional, required."""
    
    ssl_ca_certs: Optional[str] = None
    """Path to CA certificate file."""
    
    ssl_certfile: Optional[str] = None
    """Path to client certificate file."""
    
    ssl_keyfile: Optional[str] = None
    """Path to client private key file."""
    
    ssl_check_hostname: bool = True
    """Verify hostname in SSL certificate."""
    
    # Connection pool settings
    decode_responses: bool = False
    """Decode responses as strings (True) or bytes (False)."""
    
    encoding: str = "utf-8"
    """Encoding for string responses."""
    
    encoding_errors: str = "strict"
    """Error handling for encoding/decoding."""
    
    # Redis-specific settings
    retry_on_timeout: bool = False
    """Retry commands on timeout."""
    
    retry_on_error: Optional[list] = None
    """List of exceptions to retry on."""
    
    health_check_interval: int = 30
    """Interval in seconds between health checks."""
    
    # Connection string alternative
    connection_string: Optional[str] = None
    """Complete Redis connection URL (overrides individual parameters if provided).
    
    Format: redis://[username]:[password]@host:port/db
    or: rediss://[username]:[password]@host:port/db (for SSL)
    """
    
    connection_url: Optional[str] = field(default=None, repr=False, init=True)
    """Alias for connection_string (for test compatibility).
    
    This field is synced with connection_string. You can use either
    connection_string (preferred) or connection_url (for compatibility).
    """
    
    # Aliases for base class fields (for test compatibility)
    max_connections: Optional[int] = field(default=None, repr=False, init=True)
    """Alias for max_size (for test compatibility)."""
    
    min_connections: Optional[int] = field(default=None, repr=False, init=True)
    """Alias for min_size (for test compatibility)."""
    
    max_idle_time: Optional[int] = field(default=None, repr=False, init=True)
    """Alias for idle_timeout (for test compatibility)."""
    
    # Sentinel support
    sentinel_hosts: Optional[list] = None
    """List of Sentinel host:port tuples for high availability."""
    
    sentinel_service_name: Optional[str] = None
    """Service name for Sentinel."""
    
    # Cluster support
    cluster_nodes: Optional[list] = None
    """List of cluster node host:port tuples."""
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        # Sync connection_url and connection_string (whichever is set takes precedence)
        if self.connection_url and not self.connection_string:
            self.connection_string = self.connection_url
        elif self.connection_string:
            # Keep them in sync
            self.connection_url = self.connection_string
        
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
        
        if not self.connection_string:
            # Validate required fields if not using connection string
            if not self.host:
                raise ValueError("host is required when not using connection_string")
            
            # Validate database number
            if not (0 <= self.db <= 15):
                raise ValueError("db must be between 0 and 15")
        
        # Sync timeout and socket_timeout
        if self.socket_timeout is None:
            self.socket_timeout = self.timeout
        
        # Validate timeout
        if self.timeout is not None and self.timeout <= 0:
            raise ValueError("timeout must be positive")
        
        # Validate socket_timeout
        if self.socket_timeout is not None and self.socket_timeout <= 0:
            raise ValueError("socket_timeout must be positive")
        
        # Validate port
        if not (1 <= self.port <= 65535):
            raise ValueError("port must be between 1 and 65535")
        
        # Validate max_connections (alias for max_size)
        if self.max_size <= 0:
            raise ValueError("max_connections must be positive")
        
        # Validate min_connections (alias for min_size)
        if self.min_size <= 0:
            raise ValueError("min_connections must be positive")
        
        # Validate min <= max
        if self.min_size > self.max_size:
            raise ValueError("min_connections cannot be greater than max_connections")
        
        # Validate max_idle_time (alias for idle_timeout)
        if self.idle_timeout <= 0:
            raise ValueError("max_idle_time must be positive")
        
        # Validate max_lifetime
        if self.max_lifetime <= 0:
            raise ValueError("max_lifetime must be positive")
        
        # Validate SSL settings
        if self.ssl:
            valid_cert_reqs = {"none", "optional", "required"}
            if self.ssl_cert_reqs not in valid_cert_reqs:
                raise ValueError(
                    f"Invalid ssl_cert_reqs: {self.ssl_cert_reqs}. "
                    f"Must be one of {valid_cert_reqs}"
                )
        
        # Validate socket timeouts
        if self.socket_timeout is not None and self.socket_timeout <= 0:
            raise ValueError("socket_timeout must be positive")
        
        if self.socket_connect_timeout is not None and self.socket_connect_timeout <= 0:
            raise ValueError("socket_connect_timeout must be positive")
        
        # Initialize socket_keepalive_options if None
        if self.socket_keepalive_options is None:
            self.socket_keepalive_options = {}
        
        # Initialize retry_on_error if None
        if self.retry_on_error is None:
            self.retry_on_error = []
    
    
    
    def get_connection_params(self) -> Dict[str, Any]:
        """Get connection parameters as a dictionary.
        
        Returns:
            Dictionary of connection parameters suitable for redis.Redis/redis.asyncio.Redis.
        """
        if self.connection_string:
            return {"url": self.connection_string}
        
        params: Dict[str, Any] = {
            "host": self.host,
            "port": self.port,
            "db": self.db,
            "decode_responses": self.decode_responses,
            "encoding": self.encoding,
            "encoding_errors": self.encoding_errors,
        }
        
        if self.password:
            params["password"] = self.password
        
        if self.username:
            params["username"] = self.username
        
        # Socket settings
        socket_timeout = self.socket_timeout if self.socket_timeout is not None else self.timeout
        if socket_timeout is not None:
            params["socket_timeout"] = socket_timeout
        if self.socket_connect_timeout is not None:
            params["socket_connect_timeout"] = self.socket_connect_timeout
        if self.socket_keepalive:
            params["socket_keepalive"] = True
            if self.socket_keepalive_options:
                params["socket_keepalive_options"] = self.socket_keepalive_options
        
        # SSL parameters
        if self.ssl:
            params["ssl"] = True
            params["ssl_cert_reqs"] = self.ssl_cert_reqs
            if self.ssl_ca_certs:
                params["ssl_ca_certs"] = self.ssl_ca_certs
            if self.ssl_certfile:
                params["ssl_certfile"] = self.ssl_certfile
            if self.ssl_keyfile:
                params["ssl_keyfile"] = self.ssl_keyfile
            params["ssl_check_hostname"] = self.ssl_check_hostname
        
        # Redis-specific settings
        params["retry_on_timeout"] = self.retry_on_timeout
        if self.retry_on_error:
            params["retry_on_error"] = self.retry_on_error
        
        return params
    
    def get_connection_url(self) -> str:
        """Build a Redis connection URL.
        
        Returns:
            Connection string in URL format.
        """
        if self.connection_string:
            return self.connection_string
        
        # Determine protocol
        protocol = "rediss" if self.ssl else "redis"
        
        # Build auth part
        auth = ""
        if self.username and self.password:
            auth = f"{self.username}:{self.password}@"
        elif self.password:
            auth = f":{self.password}@"
        
        # Build URL
        url = f"{protocol}://{auth}{self.host}:{self.port}/{self.db}"
        
        return url
    
    @classmethod
    def from_url(cls, url: str, **kwargs) -> "RedisPoolConfig":
        """Create configuration from a Redis connection URL.
        
        Args:
            url: Redis connection URL.
            **kwargs: Additional configuration parameters.
        
        Returns:
            RedisPoolConfig instance.
        """
        return cls(connection_string=url, **kwargs)
    
    @classmethod
    def from_env(cls, prefix: str = "REDIS_") -> "RedisPoolConfig":
        """Create configuration from environment variables.
        
        Args:
            prefix: Environment variable prefix (default: REDIS_).
        
        Returns:
            RedisPoolConfig instance.
            
        Example:
            Set environment variables:
            - REDIS_HOST=localhost
            - REDIS_PORT=6379
            - REDIS_DB=0
            - REDIS_PASSWORD=secret
            - REDIS_USERNAME=user
        """
        import os
        
        return cls(
            host=os.getenv(f"{prefix}HOST", "localhost"),
            port=int(os.getenv(f"{prefix}PORT", "6379")),
            db=int(os.getenv(f"{prefix}DB", "0")),
            password=os.getenv(f"{prefix}PASSWORD"),
            username=os.getenv(f"{prefix}USERNAME"),
            connection_string=os.getenv(f"{prefix}URL"),
            ssl=os.getenv(f"{prefix}SSL", "false").lower() == "true",
        )

