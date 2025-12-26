"""PostgreSQL connector module.

This module provides synchronous and asynchronous connection pools for PostgreSQL.

Synchronous usage (psycopg2):
    >>> from db_connections.scr.all_db_connectors.connectors.postgres import PostgresConnectionPool, PostgresPoolConfig
    >>>
    >>> config = PostgresPoolConfig(
    ...     host="localhost",
    ...     database="mydb",
    ...     user="user",
    ...     password="pass"
    ... )
    >>>
    >>> with PostgresConnectionPool(config) as pool:
    ...     with pool.get_connection() as conn:
    ...         cursor = conn.cursor()
    ...         cursor.execute("SELECT * FROM users")
    ...         results = cursor.fetchall()

Asynchronous usage (asyncpg):
    >>> from db_connections.scr.all_db_connectors.connectors.postgres import AsyncPostgresConnectionPool, PostgresPoolConfig
    >>>
    >>> config = PostgresPoolConfig(
    ...     host="localhost",
    ...     database="mydb",
    ...     user="user",
    ...     password="pass"
    ... )
    >>>
    >>> async with AsyncPostgresConnectionPool(config) as pool:
    ...     async with pool.get_connection() as conn:
    ...         results = await conn.fetch("SELECT * FROM users")
"""

from .config import PostgresPoolConfig
from .health import PostgresHealthChecker, async_check_connection

# Import sync pool
try:
    from .pool import PostgresConnectionPool

    SYNC_AVAILABLE = True
except ImportError:
    SYNC_AVAILABLE = False
    PostgresConnectionPool = None

# Import async pool
try:
    from .pool_async import AsyncPostgresConnectionPool

    ASYNC_AVAILABLE = True
except ImportError:
    ASYNC_AVAILABLE = False
    AsyncPostgresConnectionPool = None


__all__ = [
    "PostgresPoolConfig",
    "PostgresConnectionPool",
    "AsyncPostgresConnectionPool",
    "PostgresHealthChecker",
    "async_check_connection",
    "SYNC_AVAILABLE",
    "ASYNC_AVAILABLE",
]


def check_availability():
    """Check which PostgreSQL drivers are available.

    Returns:
        Dictionary with availability status.
    """
    return {
        "sync": SYNC_AVAILABLE,
        "async": ASYNC_AVAILABLE,
        "sync_driver": "psycopg2" if SYNC_AVAILABLE else None,
        "async_driver": "asyncpg" if ASYNC_AVAILABLE else None,
    }
