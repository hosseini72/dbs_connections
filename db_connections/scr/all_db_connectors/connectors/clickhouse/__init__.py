"""
ClickHouse Connector Module

Provides connection pooling and management for ClickHouse databases.
Supports both synchronous and asynchronous operations.
"""

from .config import ClickHousePoolConfig
from .health import ClickHouseHealthChecker

# Import sync pool
try:
    from .pool import ClickHouseSyncConnectionPool

    SYNC_AVAILABLE = True
except ImportError:
    SYNC_AVAILABLE = False
    ClickHouseSyncConnectionPool = None

# Import async pool
try:
    from .pool import ClickHouseAsyncConnectionPool

    ASYNC_AVAILABLE = True
except ImportError:
    ASYNC_AVAILABLE = False
    ClickHouseAsyncConnectionPool = None

__all__ = [
    "ClickHousePoolConfig",
    "ClickHouseSyncConnectionPool",
    "ClickHouseAsyncConnectionPool",
    "ClickHouseHealthChecker",
]


def check_availability():
    """Check which ClickHouse drivers are available.

    Returns:
        Dictionary with availability status.
    """
    return {
        "sync": SYNC_AVAILABLE,
        "async": ASYNC_AVAILABLE,
        "sync_driver": "clickhouse-connect" if SYNC_AVAILABLE else None,
        "async_driver": "clickhouse-connect" if ASYNC_AVAILABLE else None,
    }
