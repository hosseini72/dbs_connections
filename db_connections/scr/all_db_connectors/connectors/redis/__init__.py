"""
Redis Connector Module

Provides connection pooling and management for Redis databases.
Supports both synchronous and asynchronous operations.
"""

from .config import RedisPoolConfig
from .health import RedisHealthChecker

# Import sync pool
try:
    from .pool import RedisSyncConnectionPool

    SYNC_AVAILABLE = True
except ImportError:
    SYNC_AVAILABLE = False
    RedisSyncConnectionPool = None

# Import async pool
try:
    from .pool import RedisAsyncConnectionPool

    ASYNC_AVAILABLE = True
except ImportError:
    ASYNC_AVAILABLE = False
    RedisAsyncConnectionPool = None

__all__ = [
    "RedisPoolConfig",
    "RedisSyncConnectionPool",
    "RedisAsyncConnectionPool",
    "RedisHealthChecker",
    "check_availability",
]


def check_availability():
    """Check which Redis drivers are available.

    Returns:
        Dictionary with availability status.
    """
    return {
        "sync": SYNC_AVAILABLE,
        "async": ASYNC_AVAILABLE,
        "sync_driver": "redis" if SYNC_AVAILABLE else None,
        "async_driver": "redis" if ASYNC_AVAILABLE else None,
    }
