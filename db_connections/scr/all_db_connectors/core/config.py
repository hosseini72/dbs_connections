from dataclasses import dataclass


@dataclass
class BasePoolConfig:
    """Base configuration for database connection pools."""

    # Conncection pool settings
    max_size: int = 10
    """Maximum number of connections in the pool."""
    max_overflow: int = 5
    """Maximum number of overflow connections beyond the pool size."""
    min_size: int = 1
    """Minimum number of connections in the pool."""
    # Connection timeout settings
    timeout: int = 30
    """Time in seconds to wait for a connection before timing out."""
    connection_timeout: int = 10
    """Time in seconds to wait for a new connection to be established."""
    # Connection recycling settings
    max_lifetime: int = 1800
    """Maximum lifetime of a connection in seconds."""
    idle_timeout: int = 300
    """Time in seconds after which an idle connection is closed."""
    # Connection validation settings
    validate_on_checkout: bool = True
    """Whether to validate connections when they are checked out from the pool."""  # noqa E501
    ping_interval: int = 60
    """Interval in seconds to ping the database to keep connections alive."""
    # Pool behavior settings
    recycle_on_return: bool = False
    """Whether to recycle connections when they are returned to the pool."""
    pre_ping: bool = True
    """Whether to ping the database before using a connection."""
    # retying settings
    max_retries: int = 3
    """Maximum number of retries for acquiring a connection."""
    retry_backoff: int = 5
    """Time in seconds to wait before retrying to acquire a connection."""
    retry_delay: float = 1.0
    """Delay in seconds between retries for acquiring a connection."""
    # Obervation settings
    enable_metrics: bool = False
