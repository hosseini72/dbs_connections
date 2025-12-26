"""
Database Connections - A unified Python library for managing database connections

This package provides connection pooling and management for multiple database systems
including PostgreSQL, Redis, ClickHouse, MongoDB, RabbitMQ, and Neo4j.
"""

# Import version info
# Import core classes and exceptions
from .scr.all_db_connectors.core import (
    BasePoolConfig,
    ConfigurationError,
    ConnectionError,
    DatabaseError,
    HealthCheckError,
    HealthState,
    HealthStatus,
    PoolExhaustedError,
    PoolMetrics,
    PoolTimeoutError,
    QueryError,
    TransactionError,
    ValidationError,
)
from .scr.all_db_connectors.core.version import __version__

# Import connector availability checkers
try:
    from .scr.all_db_connectors.connectors.postgres import (
        check_availability as _postgres_check,
    )

    postgres_available = lambda: {  # noqa: E731
        "available": _postgres_check()["sync"] or _postgres_check()["async"],
        "details": _postgres_check(),
    }
except ImportError:
    postgres_available = lambda: {  # noqa: E731
        "available": False,
        "reason": "PostgreSQL dependencies not installed",
    }

try:
    from .scr.all_db_connectors.connectors.redis import (
        check_availability as _redis_check,
    )

    redis_available = lambda: {  # noqa: E731
        "available": _redis_check()["sync"] or _redis_check()["async"],
        "details": _redis_check(),
    }
except ImportError:
    redis_available = lambda: {  # noqa: E731
        "available": False,
        "reason": "Redis dependencies not installed",
    }

try:
    from .scr.all_db_connectors.connectors.mongodb import (
        check_availability as _mongodb_check,
    )

    mongodb_available = lambda: {  # noqa: E731
        "available": _mongodb_check()["sync"] or _mongodb_check()["async"],
        "details": _mongodb_check(),
    }
except ImportError:
    mongodb_available = lambda: {  # noqa: E731
        "available": False,
        "reason": "MongoDB dependencies not installed",
    }

try:
    from .scr.all_db_connectors.connectors.clickhouse import (
        check_availability as _clickhouse_check,
    )

    clickhouse_available = lambda: {  # noqa: E731
        "available": _clickhouse_check()["sync"] or _clickhouse_check()["async"],
        "details": _clickhouse_check(),
    }
except ImportError:
    clickhouse_available = lambda: {  # noqa: E731
        "available": False,
        "reason": "ClickHouse dependencies not installed",
    }

try:
    from .scr.all_db_connectors.connectors.rabbitmq import (
        check_availability as _rabbitmq_check,
    )

    rabbitmq_available = lambda: {  # noqa: E731
        "available": _rabbitmq_check()["sync"] or _rabbitmq_check()["async"],
        "details": _rabbitmq_check(),
    }
except ImportError:
    rabbitmq_available = lambda: {  # noqa: E731
        "available": False,
        "reason": "RabbitMQ dependencies not installed",
    }

try:
    from .scr.all_db_connectors.connectors.neo4j import (
        check_availability as _neo4j_check,
    )

    neo4j_available = lambda: {  # noqa: E731
        "available": _neo4j_check()["sync"] or _neo4j_check()["async"],
        "details": _neo4j_check(),
    }
except ImportError:
    neo4j_available = lambda: {  # noqa: E731
        "available": False,
        "reason": "Neo4j dependencies not installed",
    }


def check_all_availability():
    """Check availability of all database connectors."""
    return {
        "postgres": postgres_available(),
        "redis": redis_available(),
        "mongodb": mongodb_available(),
        "clickhouse": clickhouse_available(),
        "rabbitmq": rabbitmq_available(),
        "neo4j": neo4j_available(),
    }


__all__ = [
    "__version__",
    # Core classes
    "BasePoolConfig",
    # Exceptions
    "DatabaseError",
    "ConnectionError",
    "PoolTimeoutError",
    "PoolExhaustedError",
    "QueryError",
    "ValidationError",
    "ConfigurationError",
    "HealthCheckError",
    "TransactionError",
    # Health and metrics
    "HealthState",
    "HealthStatus",
    "PoolMetrics",
    # Availability checkers
    "postgres_available",
    "redis_available",
    "mongodb_available",
    "clickhouse_available",
    "rabbitmq_available",
    "neo4j_available",
    "check_all_availability",
]
