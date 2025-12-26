"""
Neo4j Connector Module

Provides connection pooling and management for Neo4j databases.
Supports both synchronous and asynchronous operations.
"""

from .config import Neo4jPoolConfig
from .health import Neo4jHealthChecker

# Import sync pool
try:
    from .pool import Neo4jSyncConnectionPool

    SYNC_AVAILABLE = True
except ImportError:
    SYNC_AVAILABLE = False
    Neo4jSyncConnectionPool = None

# Import async pool (for now it's in pool.py but not fully implemented)
try:
    from .pool import Neo4jAsyncConnectionPool

    ASYNC_AVAILABLE = True
except (ImportError, NotImplementedError):
    ASYNC_AVAILABLE = False
    Neo4jAsyncConnectionPool = None

__all__ = [
    "Neo4jPoolConfig",
    "Neo4jSyncConnectionPool",
    "Neo4jAsyncConnectionPool",
    "Neo4jHealthChecker",
    "check_availability",
]


def check_availability():
    """Check which Neo4j drivers are available.

    Returns:
        Dictionary with availability status.
    """
    return {
        "sync": SYNC_AVAILABLE,
        "async": ASYNC_AVAILABLE,
        "sync_driver": "neo4j" if SYNC_AVAILABLE else None,
        "async_driver": "neo4j" if ASYNC_AVAILABLE else None,
    }
