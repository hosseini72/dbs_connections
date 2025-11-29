"""Core module for database connection management."""


class DatabaseError(Exception):
    """Base class for all database-related errors."""

    pass


class ConnectionError(DatabaseError):
    """Raised when a connection to the database fails."""

    pass


class PoolTimeoutError(DatabaseError):
    """Raised for errors related to the connection pool."""

    pass


class PoolExhaustedError(PoolTimeoutError):
    """Raised when the connection pool is exhausted."""

    pass


class QueryError(DatabaseError):
    """Raised when a database query fails."""

    pass


class ValidationError(DatabaseError):
    """Raised when connection validation fails."""

    pass


class ConfigurationError(DatabaseError):
    """Raised for errors in the database configuration."""

    pass


class HealthCheckError(DatabaseError):
    """Raised when a health check on the connection pool fails."""

    pass


class TransactionError(DatabaseError):
    """Raised when a transaction fails."""

    pass
