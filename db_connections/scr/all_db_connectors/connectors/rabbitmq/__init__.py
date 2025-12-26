"""
RabbitMQ Connector Module

Provides connection pooling and management for RabbitMQ databases.
Supports both synchronous and asynchronous operations.
"""

from .config import RabbitMQPoolConfig
from .health import RabbitMQHealthChecker

# Import sync pool
try:
    from .pool import RabbitMQSyncConnectionPool

    SYNC_AVAILABLE = True
except ImportError:
    SYNC_AVAILABLE = False
    RabbitMQSyncConnectionPool = None

# Import async pool (for now it's in pool.py but not fully implemented)
try:
    from .pool import RabbitMQAsyncConnectionPool

    ASYNC_AVAILABLE = True
except (ImportError, NotImplementedError):
    ASYNC_AVAILABLE = False
    RabbitMQAsyncConnectionPool = None

__all__ = [
    "RabbitMQPoolConfig",
    "RabbitMQSyncConnectionPool",
    "RabbitMQAsyncConnectionPool",
    "RabbitMQHealthChecker",
    "check_availability",
]


def check_availability():
    """Check which RabbitMQ drivers are available.

    Returns:
        Dictionary with availability status.
    """
    return {
        "sync": SYNC_AVAILABLE,
        "async": ASYNC_AVAILABLE,
        "sync_driver": "pika" if SYNC_AVAILABLE else None,
        "async_driver": "aio_pika" if ASYNC_AVAILABLE else None,
    }
