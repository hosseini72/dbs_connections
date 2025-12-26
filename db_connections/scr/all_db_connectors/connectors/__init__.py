"""
Database connector implementations.

Available connectors:
- PostgreSQL (sync & async)
- Redis (sync & async)
- ClickHouse (sync & async)
- MongoDB (sync & async)
- RabbitMQ (sync & async)
- Neo4j (sync & async)
"""

# Import all available connectors
__all__ = []

# PostgreSQL connectors
try:
    from .postgres import (  # noqa: F401
        PostgresConnectionPool,
        AsyncPostgresConnectionPool,
        PostgresPoolConfig,
        check_availability as postgres_check_availability,
    )

    __all__.extend(
        [
            "PostgresConnectionPool",
            "AsyncPostgresConnectionPool",
            "PostgresPoolConfig",
            "postgres_check_availability",
        ]
    )
except ImportError:
    pass

# Redis connectors
try:
    from .redis import (  # noqa: F401
        RedisSyncConnectionPool,
        RedisAsyncConnectionPool,
        RedisPoolConfig,
        check_availability as redis_check_availability,
    )

    __all__.extend(
        [
            "RedisSyncConnectionPool",
            "RedisAsyncConnectionPool",
            "RedisPoolConfig",
            "redis_check_availability",
        ]
    )
except ImportError:
    pass

# ClickHouse connectors
try:
    from .clickhouse import (  # noqa: F401
        ClickHouseSyncConnectionPool,
        ClickHouseAsyncConnectionPool,
        ClickHousePoolConfig,
        check_availability as clickhouse_check_availability,
    )

    __all__.extend(
        [
            "ClickHouseSyncConnectionPool",
            "ClickHouseAsyncConnectionPool",
            "ClickHousePoolConfig",
            "clickhouse_check_availability",
        ]
    )
except ImportError:
    pass

# MongoDB connectors
try:
    from .mongodb import (  # noqa: F401
        MongoSyncConnectionPool,
        MongoAsyncConnectionPool,
        MongoPoolConfig,
        check_availability as mongodb_check_availability,
    )

    __all__.extend(
        [
            "MongoSyncConnectionPool",
            "MongoAsyncConnectionPool",
            "MongoPoolConfig",
            "mongodb_check_availability",
        ]
    )
except ImportError:
    pass

# RabbitMQ connectors
try:
    from .rabbitmq import (  # noqa: F401
        RabbitMQSyncConnectionPool,
        RabbitMQAsyncConnectionPool,
        RabbitMQPoolConfig,
        check_availability as rabbitmq_check_availability,
    )

    __all__.extend(
        [
            "RabbitMQSyncConnectionPool",
            "RabbitMQAsyncConnectionPool",
            "RabbitMQPoolConfig",
            "rabbitmq_check_availability",
        ]
    )
except ImportError:
    pass

# Neo4j connectors
try:
    from .neo4j import (  # noqa: F401
        Neo4jSyncConnectionPool,
        Neo4jAsyncConnectionPool,
        Neo4jPoolConfig,
        check_availability as neo4j_check_availability,
    )

    __all__.extend(
        [
            "Neo4jSyncConnectionPool",
            "Neo4jAsyncConnectionPool",
            "Neo4jPoolConfig",
            "neo4j_check_availability",
        ]
    )
except ImportError:
    pass
