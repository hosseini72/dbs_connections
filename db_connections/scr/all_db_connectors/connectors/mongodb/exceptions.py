"""
MongoDB-specific exceptions for connection pools and connectors.
"""

class MongoDBConnectorError(Exception):
    """Base exception for MongoDB connector errors."""
    pass

class PoolTimeoutError(MongoDBConnectorError, TimeoutError):
    """Raised when a pool operation times out (no connection available within specified timeout)."""
    pass

class PoolExhaustedError(MongoDBConnectorError):
    """Raised when the pool is exhausted and no more connections can be made."""
    pass

class ConnectionError(MongoDBConnectorError):
    """Raised when there is a connection error."""
    pass

class MongoAuthenticationError(MongoDBConnectorError):
    """Raised for authentication failures with MongoDB."""
    pass

class ConfigurationError(MongoDBConnectorError, ValueError):
    """Raised for invalid configuration settings."""
    pass

class HealthCheckError(MongoDBConnectorError):
    """Raised when a health check fails."""
    pass

class OperationNotSupportedError(MongoDBConnectorError, NotImplementedError):
    """Raised when an operation is not supported by the current implementation."""
    pass

