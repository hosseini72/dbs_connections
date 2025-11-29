"""
MyCompany Database Connection Management

A unified library for managing database connections across multiple DBMSs.
"""

from .version import __version__
from .config import BasePoolConfig
from .exceptions import (
    DatabaseError,
    ConnectionError,
    PoolTimeoutError,
    PoolExhaustedError,
    QueryError,
    ValidationError,
    ConfigurationError,
    HealthCheckError,
    TransactionError,
)
from .health import HealthState, HealthStatus
from .metrics import PoolMetrics

__all__ = [
    "__version__",
    "BasePoolConfig",
    "DatabaseError",
    "ConnectionError",
    "PoolTimeoutError",
    "PoolExhaustedError",
    "QueryError",
    "ValidationError",
    "ConfigurationError",
    "HealthCheckError",
    "TransactionError",
    "HealthState",
    "HealthStatus",
    "PoolMetrics",
]
