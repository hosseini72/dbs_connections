"""Neo4j-specific exceptions."""

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
    "Neo4jError",
    "Neo4jConnectionError",
    "Neo4jTimeoutError",
    "Neo4jPoolError",
    "Neo4jQueryError",
    "Neo4jValidationError",
    "Neo4jHealthCheckError",
]


class Neo4jError(DatabaseError):
    """Base exception for all Neo4j-related errors."""

    pass


class Neo4jConnectionError(ConnectionError, Neo4jError):
    """Raised when a Neo4j connection fails."""

    pass


class Neo4jTimeoutError(PoolTimeoutError, Neo4jError):
    """Raised when a Neo4j operation times out."""

    pass


class Neo4jPoolError(PoolExhaustedError, Neo4jError):
    """Raised when Neo4j pool operations fail."""

    pass


class Neo4jQueryError(QueryError, Neo4jError):
    """Raised when a Neo4j query fails."""

    pass


class Neo4jValidationError(ValidationError, Neo4jError):
    """Raised when Neo4j connection validation fails."""

    pass


class Neo4jHealthCheckError(HealthCheckError, Neo4jError):
    """Raised when a Neo4j health check fails."""

    pass
