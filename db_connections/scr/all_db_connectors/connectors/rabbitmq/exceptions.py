"""RabbitMQ-specific exceptions."""

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
    "RabbitMQError",
    "RabbitMQConnectionError",
    "RabbitMQTimeoutError",
    "RabbitMQPoolError",
    "RabbitMQQueryError",
    "RabbitMQValidationError",
    "RabbitMQHealthCheckError",
]


class RabbitMQError(DatabaseError):
    """Base exception for all RabbitMQ-related errors."""

    pass


class RabbitMQConnectionError(ConnectionError, RabbitMQError):
    """Raised when a RabbitMQ connection fails."""

    pass


class RabbitMQTimeoutError(PoolTimeoutError, RabbitMQError):
    """Raised when a RabbitMQ operation times out."""

    pass


class RabbitMQPoolError(PoolExhaustedError, RabbitMQError):
    """Raised when RabbitMQ pool operations fail."""

    pass


class RabbitMQQueryError(QueryError, RabbitMQError):
    """Raised when a RabbitMQ query fails."""

    pass


class RabbitMQValidationError(ValidationError, RabbitMQError):
    """Raised when RabbitMQ connection validation fails."""

    pass


class RabbitMQHealthCheckError(HealthCheckError, RabbitMQError):
    """Raised when a RabbitMQ health check fails."""

    pass
