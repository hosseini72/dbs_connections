"""
MongoDB Connector Module

Provides connection pooling and management for MongoDB databases.
Supports both synchronous and asynchronous operations.
"""

from .config import MongoPoolConfig
from .health import MongoHealthChecker

# Import sync pool
try:
    from .pool import MongoSyncConnectionPool

    SYNC_AVAILABLE = True
except ImportError:
    SYNC_AVAILABLE = False
    MongoSyncConnectionPool = None

# Import async pool
try:
    from .pool import MongoAsyncConnectionPool

    ASYNC_AVAILABLE = True
except ImportError:
    ASYNC_AVAILABLE = False
    MongoAsyncConnectionPool = None

__all__ = [
    "MongoPoolConfig",
    "MongoSyncConnectionPool",
    "MongoAsyncConnectionPool",
    "MongoHealthChecker",
]


def check_availability():
    """Check which MongoDB drivers are available.

    Returns:
        Dictionary with availability status.
    """
    return {
        "sync": SYNC_AVAILABLE,
        "async": ASYNC_AVAILABLE,
        "sync_driver": "pymongo" if SYNC_AVAILABLE else None,
        "async_driver": "motor" if ASYNC_AVAILABLE else None,
    }
