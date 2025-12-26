"""Redis-specific exceptions."""

from db_connections.scr.all_db_connectors.core.exceptions import (
    DatabaseError,
    ConnectionError,
    PoolTimeoutError,
    PoolExhaustedError,
    ValidationError,
    HealthCheckError,
)

__all__ = [
    "RedisError",
    "RedisConnectionError",
    "RedisTimeoutError",
    "RedisPoolError",
    "RedisValidationError",
]


class RedisError(DatabaseError):
    """Base exception for all Redis-related errors."""

    pass


class RedisConnectionError(ConnectionError, RedisError):
    """Raised when a Redis connection fails."""

    pass


class RedisTimeoutError(PoolTimeoutError, RedisError):
    """Raised when a Redis operation times out."""

    pass


class RedisPoolError(PoolExhaustedError, RedisError):
    """Raised when Redis pool operations fail."""

    pass


class RedisValidationError(ValidationError, RedisError):
    """Raised when Redis connection validation fails."""

    pass
