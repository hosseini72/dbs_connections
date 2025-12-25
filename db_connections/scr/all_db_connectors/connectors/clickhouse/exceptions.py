"""ClickHouse-specific exceptions."""

from db_connections.scr.all_db_connectors.core.exceptions import (
    DatabaseError,
    ConnectionError,
    PoolTimeoutError,
    PoolExhaustedError,
    QueryError,
    ValidationError,
    HealthCheckError,
)

__all__ = [
    "ClickHouseError",
    "ClickHouseConnectionError",
    "ClickHouseTimeoutError",
    "ClickHousePoolError",
    "ClickHouseQueryError",
    "ClickHouseValidationError",
    "ClickHouseHealthCheckError",
]


class ClickHouseError(DatabaseError):
    """Base exception for all ClickHouse-related errors."""
    pass


class ClickHouseConnectionError(ConnectionError, ClickHouseError):
    """Raised when a ClickHouse connection fails."""
    pass


class ClickHouseTimeoutError(PoolTimeoutError, ClickHouseError):
    """Raised when a ClickHouse operation times out."""
    pass


class ClickHousePoolError(PoolExhaustedError, ClickHouseError):
    """Raised when ClickHouse pool operations fail."""
    pass


class ClickHouseQueryError(QueryError, ClickHouseError):
    """Raised when a ClickHouse query fails."""
    pass


class ClickHouseValidationError(ValidationError, ClickHouseError):
    """Raised when ClickHouse connection validation fails."""
    pass


class ClickHouseHealthCheckError(HealthCheckError, ClickHouseError):
    """Raised when a ClickHouse health check fails."""
    pass

