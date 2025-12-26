"""RabbitMQ-specific configuration."""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from db_connections.scr.all_db_connectors.core import BasePoolConfig


@dataclass
class RabbitMQPoolConfig(BasePoolConfig):
    """Configuration for RabbitMQ connection pool.

    Extends BasePoolConfig with RabbitMQ-specific settings.
    """

    # Connection parameters
    host: str = "localhost"
    """RabbitMQ host address."""

    port: int = 5672
    """RabbitMQ port (5672 for AMQP, 5671 for AMQPS)."""

    username: Optional[str] = None
    """RabbitMQ username."""

    password: Optional[str] = None
    """RabbitMQ password."""

    virtual_host: str = "/"
    """Virtual host name."""

    # Connection string alternative
    connection_url: Optional[str] = None
    """Complete RabbitMQ connection URL (overrides individual parameters if provided).

    Format: amqp://[username:password@]host[:port][/virtual_host]
    or: amqps://[username:password@]host[:port][/virtual_host] (for SSL)
    """

    # Connection behavior
    timeout: int = 30
    """Connection timeout in seconds."""

    connection_timeout: Optional[int] = None
    """Connection timeout in seconds (None = uses timeout value)."""

    socket_timeout: Optional[int] = None
    """Socket timeout in seconds."""

    heartbeat: int = 600
    """Heartbeat interval in seconds (0 to disable)."""

    connection_attempts: int = 3
    """Maximum number of connection attempts."""

    retry_delay: float = 2.0
    """Delay in seconds between connection attempts."""

    blocked_connection_timeout: Optional[int] = None
    """Timeout in seconds for blocked connections."""

    # SSL/TLS settings
    ssl: bool = False
    """Enable SSL/TLS connection."""

    ssl_options: Optional[Dict[str, Any]] = None
    """SSL options dictionary (ca_certs, certfile, keyfile, etc.)."""

    # Protocol settings
    channel_max: int = 0
    """Maximum number of channels per connection (0 = no limit)."""

    frame_max: int = 131072
    """Maximum frame size in bytes."""

    locale: str = "en_US"
    """Locale for connection."""

    client_properties: Optional[Dict[str, Any]] = None
    """Client properties dictionary for connection identification."""

    # Pool settings (aliases for base class compatibility)
    max_connections: Optional[int] = field(default=None, repr=False, init=True)
    """Maximum number of connections (alias for max_size)."""

    min_connections: Optional[int] = field(default=None, repr=False, init=True)
    """Minimum number of connections (alias for min_size)."""

    max_idle_time: Optional[int] = field(default=None, repr=False, init=True)
    """Max idle time in seconds (alias for idle_timeout)."""

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

        if self.connection_timeout is not None and self.connection_timeout <= 0:
            raise ValueError("connection_timeout must be positive if provided")

        if self.socket_timeout is not None and self.socket_timeout <= 0:
            raise ValueError("socket_timeout must be positive if provided")

        # Validate heartbeat
        if self.heartbeat <= 0:
            raise ValueError("heartbeat must be positive")

        # Validate connection_attempts
        if self.connection_attempts <= 0:
            raise ValueError("connection_attempts must be positive")

        if self.retry_delay < 0:
            raise ValueError("retry_delay must be non-negative")

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

        # Initialize ssl_options if None and ssl is True
        if self.ssl and self.ssl_options is None:
            self.ssl_options = {}

    def get_connection_params(self) -> Dict[str, Any]:
        """Get connection parameters as a dictionary.

        Returns:
            Dictionary of connection parameters suitable for pika connection.
        """
        params: Dict[str, Any] = {}

        if self.connection_url:
            params["url"] = self.connection_url
        else:
            params["host"] = self.host
            params["port"] = self.port

            # Authentication
            if self.username:
                params["username"] = self.username
            if self.password:
                params["password"] = self.password

            # Virtual host
            params["virtual_host"] = self.virtual_host

        # Timeouts
        if self.connection_timeout is not None:
            params["connection_timeout"] = self.connection_timeout
        elif self.timeout:
            params["connection_timeout"] = self.timeout

        if self.socket_timeout is not None:
            params["socket_timeout"] = self.socket_timeout

        # Heartbeat
        if self.heartbeat is not None:
            params["heartbeat"] = self.heartbeat

        # Connection attempts
        if self.connection_attempts:
            params["connection_attempts"] = self.connection_attempts

        if self.retry_delay:
            params["retry_delay"] = self.retry_delay

        # Blocked connection timeout
        if self.blocked_connection_timeout is not None:
            params["blocked_connection_timeout"] = self.blocked_connection_timeout

        # SSL/TLS
        if self.ssl:
            params["ssl"] = True
            if self.ssl_options:
                params["ssl_options"] = self.ssl_options

        # Protocol settings
        if self.channel_max:
            params["channel_max"] = self.channel_max

        if self.frame_max:
            params["frame_max"] = self.frame_max

        if self.locale:
            params["locale"] = self.locale

        # Client properties
        if self.client_properties:
            params["client_properties"] = self.client_properties

        return params

    def get_connection_url(self) -> str:
        """Build a RabbitMQ connection URL.

        Returns:
            Connection string in URL format.
        """
        if self.connection_url:
            return self.connection_url

        # Determine scheme
        scheme = "amqps" if self.ssl else "amqp"

        # Build auth part
        auth = ""
        if self.username and self.password:
            auth = f"{self.username}:{self.password}@"
        elif self.username:
            auth = f"{self.username}@"

        # URL encode virtual host if needed
        vhost = self.virtual_host
        if vhost != "/":
            # Simple URL encoding for common cases
            vhost = vhost.replace("/", "%2F")
            if not vhost.startswith("/"):
                vhost = "/" + vhost

        # Build URL
        url = f"{scheme}://{auth}{self.host}:{self.port}{vhost}"

        return url

    @classmethod
    def from_url(cls, url: str, **kwargs) -> "RabbitMQPoolConfig":
        """Create configuration from a RabbitMQ connection URL.

        Args:
            url: RabbitMQ connection URL.
            **kwargs: Additional configuration parameters.

        Returns:
            RabbitMQPoolConfig instance.
        """
        return cls(connection_url=url, **kwargs)

    @classmethod
    def from_env(cls, prefix: str = "RABBITMQ_") -> "RabbitMQPoolConfig":
        """Create configuration from environment variables.

        Args:
            prefix: Environment variable prefix (default: RABBITMQ_).

        Returns:
            RabbitMQPoolConfig instance.

        Example:
            Set environment variables:
            - RABBITMQ_HOST=localhost
            - RABBITMQ_PORT=5672
            - RABBITMQ_USERNAME=guest
            - RABBITMQ_PASSWORD=guest
            - RABBITMQ_VIRTUAL_HOST=/
            - RABBITMQ_CONNECTION_URL=amqp://localhost:5672/
        """
        import os

        return cls(
            host=os.getenv(f"{prefix}HOST", "localhost"),
            port=int(os.getenv(f"{prefix}PORT", "5672")),
            username=os.getenv(f"{prefix}USERNAME"),
            password=os.getenv(f"{prefix}PASSWORD"),
            virtual_host=os.getenv(f"{prefix}VIRTUAL_HOST", "/"),
            connection_url=os.getenv(f"{prefix}CONNECTION_URL"),
            ssl=os.getenv(f"{prefix}SSL", "false").lower() == "true",
            heartbeat=int(os.getenv(f"{prefix}HEARTBEAT", "600")),
        )
