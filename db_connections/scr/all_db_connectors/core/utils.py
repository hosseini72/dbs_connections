"""
Utility functions for database connection management.

This module provides helper functions for connection lifecycle management,
including validation, recycling, and retry logic.
"""

import asyncio
import time
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from functools import wraps
from typing import Any, Callable, TypeVar

from .config import BasePoolConfig

T = TypeVar("T")


# ============================================================================
# Connection Lifecycle Utilities
# ============================================================================


def is_connection_expired(
    created_at: datetime,
    max_lifetime_seconds: int,
) -> bool:
    """Check if a connection is expired based on its creation time and max lifetime.

    Args:
        created_at: The creation time of the connection.
        max_lifetime_seconds: The maximum lifetime of the connection in seconds.

    Returns:
        True if the connection is expired, False otherwise.
    """
    expiration_time = created_at + timedelta(seconds=max_lifetime_seconds)
    return datetime.now() >= expiration_time


def is_connection_idle_too_long(last_used: datetime, max_idle_seconds: int) -> bool:
    """Check if a connection has been idle for too long.

    Args:
        last_used: The last used time of the connection.
        max_idle_seconds: The maximum idle time of the connection in seconds.

    Returns:
        True if the connection has been idle for too long, False otherwise.
    """
    idle_expiration_time = last_used + timedelta(seconds=max_idle_seconds)
    return datetime.now() >= idle_expiration_time


def should_recycle_connection(conn_metadata: dict, config: BasePoolConfig) -> bool:
    """Determine if a connection should be recycled based on its metadata.

    Args:
        conn_metadata: Metadata of the connection containing 'created_at', 'last_used',
            and 'in_use' timestamps.
        config: Configuration settings for the connection pool.

    Returns:
        True if the connection should be recycled, False otherwise.
    """
    created_at = conn_metadata.get("created_at")
    last_used = conn_metadata.get("last_used")
    in_use = conn_metadata.get("in_use", False)

    # Don't recycle connections currently in use
    if in_use:
        return False

    # Must have valid timestamps
    if not created_at or not last_used:
        return False

    # Check if forced recycling on return is enabled
    if config.recycle_on_return:
        return True

    # Check if connection has exceeded max lifetime
    if is_connection_expired(created_at, config.max_lifetime):
        return True

    # Check if connection has been idle too long
    if is_connection_idle_too_long(last_used, config.idle_timeout):
        return True

    return False


# ============================================================================
# Retry Logic - Synchronous
# ============================================================================


def retry_on_failure(
    max_retries: int = 3,
    retry_delay: float = 1.0,
    retry_backoff: float = 2.0,
    exceptions: tuple = (Exception,),
):
    """Decorator to retry a function on failure with exponential backoff.

    Args:
        max_retries: Maximum number of retries.
        retry_delay: Initial delay between retries in seconds.
        retry_backoff: Backoff multiplier for delay.
        exceptions: Tuple of exception types to catch and retry on.

    Returns:
        Decorated function with retry logic.

    Example:
        @retry_on_failure(max_retries=3, retry_delay=1.0, retry_backoff=2.0)
        def connect_to_db():
            return create_connection()
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            attempt = 0
            delay = retry_delay
            last_exception = None

            while attempt <= max_retries:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt == max_retries:
                        raise

                    # Log retry attempt (you can add proper logging here)
                    print(
                        f"Attempt {attempt + 1}/{max_retries + 1} failed: {e}. "
                        f"Retrying in {delay}s..."
                    )

                    time.sleep(delay)
                    delay *= retry_backoff
                    attempt += 1

            # This should never be reached, but just in case
            if last_exception:
                raise last_exception

        return wrapper

    return decorator


def acquire_with_retries(
    acquire_func: Callable[[], T],
    max_retries: int = 3,
    retry_delay: float = 1.0,
    retry_backoff: float = 2.0,
) -> T:
    """Acquire a resource with retries on failure.

    Args:
        acquire_func: Function to acquire the resource.
        max_retries: Maximum number of retries.
        retry_delay: Initial delay between retries in seconds.
        retry_backoff: Backoff multiplier for delay.

    Returns:
        The acquired resource.

    Raises:
        Exception: If all retry attempts fail.

    Example:
        conn = acquire_with_retries(
            lambda: create_connection(),
            max_retries=3,
            retry_delay=1.0,
            retry_backoff=2.0
        )
    """

    @retry_on_failure(max_retries, retry_delay, retry_backoff)
    def _acquire():
        return acquire_func()

    return _acquire()


# ============================================================================
# Retry Logic - Asynchronous
# ============================================================================


def async_retry_on_failure(
    max_retries: int = 3,
    retry_delay: float = 1.0,
    retry_backoff: float = 2.0,
    exceptions: tuple = (Exception,),
):
    """Async decorator to retry a function on failure with exponential backoff.

    Args:
        max_retries: Maximum number of retries.
        retry_delay: Initial delay between retries in seconds.
        retry_backoff: Backoff multiplier for delay.
        exceptions: Tuple of exception types to catch and retry on.

    Returns:
        Decorated async function with retry logic.

    Example:
        @async_retry_on_failure(max_retries=3, retry_delay=1.0, retry_backoff=2.0)
        async def connect_to_db():
            return await create_connection()
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            attempt = 0
            delay = retry_delay
            last_exception = None

            while attempt <= max_retries:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt == max_retries:
                        raise

                    # Log retry attempt
                    print(
                        f"Attempt {attempt + 1}/{max_retries + 1} failed: {e}. "
                        f"Retrying in {delay}s..."
                    )

                    await asyncio.sleep(delay)
                    delay *= retry_backoff
                    attempt += 1

            # This should never be reached, but just in case
            if last_exception:
                raise last_exception

        return wrapper

    return decorator


async def async_acquire_with_retries(
    acquire_func: Callable[[], T],
    max_retries: int = 3,
    retry_delay: float = 1.0,
    retry_backoff: float = 2.0,
) -> T:
    """Async acquire a resource with retries on failure.

    Args:
        acquire_func: Async function to acquire the resource.
        max_retries: Maximum number of retries.
        retry_delay: Initial delay between retries in seconds.
        retry_backoff: Backoff multiplier for delay.

    Returns:
        The acquired resource.

    Raises:
        Exception: If all retry attempts fail.

    Example:
        conn = await async_acquire_with_retries(
            lambda: create_async_connection(),
            max_retries=3,
            retry_delay=1.0,
            retry_backoff=2.0
        )
    """

    @async_retry_on_failure(max_retries, retry_delay, retry_backoff)
    async def _acquire():
        return await acquire_func()

    return await _acquire()


# ============================================================================
# Connection Metadata
# ============================================================================


@dataclass
class ConnectionMetadata:
    """Metadata for tracking connection lifecycle and usage.

    Attributes:
        created_at: Timestamp when the connection was created.
        last_used: Timestamp when the connection was last used.
        use_count: Number of times the connection has been used.
        is_valid: Whether the connection is currently valid.
        in_use: Whether the connection is currently being used.
    """

    created_at: datetime = field(default_factory=datetime.now)
    last_used: datetime = field(default_factory=datetime.now)
    use_count: int = 0
    is_valid: bool = True
    in_use: bool = False

    def mark_used(self) -> None:
        """Mark the connection as used and update metadata."""
        self.last_used = datetime.now()
        self.use_count += 1
        self.in_use = True

    def mark_released(self) -> None:
        """Mark the connection as released."""
        self.in_use = False
        self.last_used = datetime.now()

    def mark_invalid(self) -> None:
        """Mark the connection as invalid."""
        self.is_valid = False

    def to_dict(self) -> dict:
        """Convert metadata to dictionary format.

        Returns:
            Dictionary representation of the metadata.
        """
        return {
            "created_at": self.created_at,
            "last_used": self.last_used,
            "use_count": self.use_count,
            "is_valid": self.is_valid,
            "in_use": self.in_use,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ConnectionMetadata":
        """Create ConnectionMetadata from dictionary.

        Args:
            data: Dictionary containing metadata fields.

        Returns:
            ConnectionMetadata instance.
        """
        return cls(
            created_at=data.get("created_at", datetime.now()),
            last_used=data.get("last_used", datetime.now()),
            use_count=data.get("use_count", 0),
            is_valid=data.get("is_valid", True),
            in_use=data.get("in_use", False),
        )


# ============================================================================
# Configuration Validation
# ============================================================================


def validate_pool_config(config: BasePoolConfig) -> None:
    """Validate pool configuration settings.

    Args:
        config: Configuration object to validate.

    Raises:
        ValueError: If configuration is invalid.
    """
    if config.max_size <= 0:
        raise ValueError("max_size must be positive")

    if config.min_size < 0:
        raise ValueError("min_size must be non-negative")

    if config.min_size > config.max_size:
        raise ValueError("min_size cannot exceed max_size")

    if config.timeout <= 0:
        raise ValueError("timeout must be positive")

    if config.connection_timeout is not None and config.connection_timeout <= 0:
        raise ValueError("connection_timeout must be positive")

    if config.max_overflow < 0:
        raise ValueError("max_overflow must be non-negative")

    if config.max_lifetime <= 0:
        raise ValueError("max_lifetime must be positive")

    if config.idle_timeout <= 0:
        raise ValueError("idle_timeout must be positive")

    if config.max_retries < 0:
        raise ValueError("max_retries must be non-negative")

    if config.retry_delay <= 0:
        raise ValueError("retry_delay must be positive")

    if config.retry_backoff <= 0:
        raise ValueError("retry_backoff must be positive")


# ============================================================================
# Connection String Formatting
# ============================================================================


def format_connection_string(
    protocol: str,
    host: str,
    port: int,
    database: str,
    username: str = None,
    password: str = None,
    **kwargs: Any,
) -> str:
    """Format a database connection string.

    Args:
        protocol: Database protocol (e.g., 'postgresql', 'mysql', 'mongodb').
        host: Database host.
        port: Database port.
        database: Database name.
        username: Optional username.
        password: Optional password.
        **kwargs: Additional connection parameters.

    Returns:
        Formatted connection string.

    Example:
        >>> format_connection_string(
        ...     'postgresql', 'localhost', 5432, 'mydb',
        ...     username='user', password='pass', sslmode='require'
        ... )
        'postgresql://user:pass@localhost:5432/mydb?sslmode=require'
    """
    auth = f"{username}:{password}@" if username and password else ""
    base = f"{protocol}://{auth}{host}:{port}/{database}"

    if kwargs:
        params = "&".join(f"{k}={v}" for k, v in kwargs.items())
        return f"{base}?{params}"

    return base


def parse_connection_string(connection_string: str) -> dict:
    """Parse a connection string into components.

    Args:
        connection_string: Connection string to parse.

    Returns:
        Dictionary with parsed components.

    Example:
        >>> parse_connection_string('postgresql://user:pass@localhost:5432/mydb')
        {
            'protocol': 'postgresql',
            'username': 'user',
            'password': 'pass',
            'host': 'localhost',
            'port': 5432,
            'database': 'mydb',
            'params': {}
        }
    """
    # This is a simplified parser - you might want to use urllib.parse for production
    from urllib.parse import urlparse, parse_qs

    parsed = urlparse(connection_string)

    return {
        "protocol": parsed.scheme,
        "username": parsed.username,
        "password": parsed.password,
        "host": parsed.hostname,
        "port": parsed.port,
        "database": parsed.path.lstrip("/"),
        "params": {
            k: v[0] if len(v) == 1 else v for k, v in parse_qs(parsed.query).items()
        },
    }
