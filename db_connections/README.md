# Database Connections

A unified Python library for managing database connections across multiple Database Management Systems (DBMSs).

## Features

- 🔌 **Multiple DBMS Support**: PostgreSQL, Redis, ClickHouse, MongoDB, RabbitMQ, Neo4j
- ⚡ **Async & Sync**: Both async and synchronous connection pool implementations
- 🔄 **Connection Pooling**: Efficient connection reuse with configurable pool sizes
- 🏥 **Health Checks**: Built-in health monitoring for all connections
- 📊 **Metrics**: Track pool usage and performance
- 🔧 **Framework Integration**: Ready-to-use middleware for FastAPI, Flask, and Django
- 🎯 **Type Safety**: Full type hints for better IDE support
- 🧪 **Well Tested**: Comprehensive test coverage

## Installation

```bash
# Basic installation (no database drivers)
pip install db_connections

# With specific database support
pip install db_connections[postgres]      # PostgreSQL (sync + async)
pip install db_connections[postgres-sync]  # PostgreSQL (sync only)
pip install db_connections[postgres-async] # PostgreSQL (async only)

pip install db_connections[redis]          # Redis (sync + async)
pip install db_connections[redis-sync]   # Redis (sync only)
pip install db_connections[redis-async]    # Redis (async only)

pip install db_connections[mongodb]        # MongoDB (sync + async)
pip install db_connections[mongodb-sync]  # MongoDB (sync only)
pip install db_connections[mongodb-async] # MongoDB (async only)

pip install db_connections[clickhouse]     # ClickHouse (sync + async)
pip install db_connections[clickhouse-sync] # ClickHouse (sync only)
pip install db_connections[clickhouse-async] # ClickHouse (async only)

pip install db_connections[rabbitmq]       # RabbitMQ (sync + async)
pip install db_connections[rabbitmq-sync]  # RabbitMQ (sync only)
pip install db_connections[rabbitmq-async] # RabbitMQ (async only)

pip install db_connections[neo4j]          # Neo4j (sync + async)
pip install db_connections[neo4j-sync]     # Neo4j (sync only)
pip install db_connections[neo4j-async]    # Neo4j (async only)

# Install multiple databases (combine extras with commas)
pip install db_connections[postgres,redis,mongodb]  # PostgreSQL, Redis, and MongoDB

# With all databases
pip install db_connections[all]

# For development
pip install db_connections[dev]
```

## Quick Start

### PostgreSQL Example

```python
from db_connections.scr.all_db_connectors.connectors.postgres import (
    PostgresConnectionPool,
    PostgresPoolConfig,
)

# Configure pool
config = PostgresPoolConfig(
    host="localhost",
    port=5432,
    database="mydb",
    user="postgres",
    password="secret",
    min_size=2,
    max_size=10,
)

# Use pool (synchronous)
with PostgresConnectionPool(config) as pool:
    with pool.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users")
        results = cursor.fetchall()
```

### Redis Example

```python
from db_connections.scr.all_db_connectors.connectors.redis import (
    RedisSyncConnectionPool,
    RedisPoolConfig,
)

# Configure pool
config = RedisPoolConfig(
    host="localhost",
    port=6379,
    db=0,
    min_size=2,
    max_size=10,
)

# Use pool
with RedisSyncConnectionPool(config) as pool:
    with pool.get_connection() as conn:
        conn.set("key", "value")
        value = conn.get("key")
```

### MongoDB Example

```python
from db_connections.scr.all_db_connectors.connectors.mongodb import (
    MongoSyncConnectionPool,
    MongoPoolConfig,
)

# Configure pool
config = MongoPoolConfig(
    host="localhost",
    port=27017,
    database="mydb",
    username="admin",
    password="secret",
    min_size=2,
    max_size=10,
)

# Use pool
with MongoSyncConnectionPool(config) as pool:
    with pool.get_connection() as client:
        db = client[config.database]
        collection = db["users"]
        result = collection.insert_one({"name": "John", "email": "john@example.com"})
```

### Async Example (PostgreSQL)

```python
from db_connections.scr.all_db_connectors.connectors.postgres import (
    AsyncPostgresConnectionPool,
    PostgresPoolConfig,
)

config = PostgresPoolConfig(
    host="localhost",
    database="mydb",
    user="postgres",
    password="secret",
)

async with AsyncPostgresConnectionPool(config) as pool:
    async with pool.get_connection() as conn:
        results = await conn.fetch("SELECT * FROM users")
```

## Supported Databases

### PostgreSQL
- **Sync Driver**: `psycopg2-binary`
- **Async Driver**: `asyncpg`
- **Setup Guide**: See `scr/all_db_connectors/connectors/postgres/Setup_guide.md`

### Redis
- **Sync Driver**: `redis`
- **Async Driver**: `redis[hiredis]`
- **Setup Guide**: See `scr/all_db_connectors/connectors/redis/Setup_guide.md`

### MongoDB
- **Sync Driver**: `pymongo`
- **Async Driver**: `motor`
- **Setup Guide**: See `scr/all_db_connectors/connectors/mongodb/Setup_guide.md`

### ClickHouse
- **Sync Driver**: `clickhouse-connect`
- **Async Driver**: `clickhouse-connect`
- **Setup Guide**: See `scr/all_db_connectors/connectors/clickhouse/Setup_guide.md`

### RabbitMQ
- **Sync Driver**: `pika`
- **Async Driver**: `aio-pika`
- **Setup Guide**: See `scr/all_db_connectors/connectors/rabbitmq/Setup_guide.md`

### Neo4j
- **Sync Driver**: `neo4j`
- **Async Driver**: `neo4j`
- **Setup Guide**: See `scr/all_db_connectors/connectors/neo4j/Setup_guide.md`

## Common Features

All database connectors support:

### Connection Pooling
- Configurable min/max pool sizes
- Connection overflow handling
- Automatic connection recycling
- Thread-safe (sync) and coroutine-safe (async)

### Health Checks
- Pool health monitoring
- Database server health checks
- Response time tracking
- Detailed health status reporting

### Metrics
- Active/idle connection tracking
- Connection creation/closure counts
- Wait time statistics
- Pool utilization metrics

### Configuration Options
- Environment variable support
- Connection string/URL support
- SSL/TLS configuration
- Custom timeout settings

## Configuration Examples

### Using Environment Variables

All connectors support loading configuration from environment variables:

```python
# PostgreSQL
from db_connections.scr.all_db_connectors.connectors.postgres import (
    PostgresPoolConfig,
    PostgresConnectionPool,
)

config = PostgresPoolConfig.from_env()
pool = PostgresConnectionPool(config)

# Redis
from db_connections.scr.all_db_connectors.connectors.redis import (
    RedisPoolConfig,
    RedisSyncConnectionPool,
)

config = RedisPoolConfig.from_env()
pool = RedisSyncConnectionPool(config)
```

### Using Connection Strings

```python
# PostgreSQL
dsn = "postgresql://user:pass@localhost:5432/mydb?sslmode=require"
config = PostgresPoolConfig.from_dsn(dsn)

# Redis
url = "redis://localhost:6379/0"
config = RedisPoolConfig.from_url(url)

# MongoDB
uri = "mongodb://admin:secret@localhost:27017/mydb?authSource=admin"
config = MongoPoolConfig.from_uri(uri)
```

## Health Checks

All connectors provide health monitoring:

```python
# Check pool health
health = pool.health_check()
print(f"State: {health.state.value}")
print(f"Response time: {health.response_time_ms}ms")

# Check database health
db_health = pool.database_health_check()
print(f"Database version: {db_health.details.get('server_version')}")
```

## Metrics

Track pool usage and performance:

```python
metrics = pool.get_metrics()
print(f"Total connections: {metrics.total_connections}")
print(f"Active: {metrics.active_connections}")
print(f"Idle: {metrics.idle_connections}")
print(f"Average wait time: {metrics.average_wait_time_ms}ms")
```

## Framework Integration

### FastAPI Example

```python
from fastapi import FastAPI, Depends
from db_connections.scr.all_db_connectors.connectors.postgres import (
    AsyncPostgresConnectionPool,
    PostgresPoolConfig,
)

app = FastAPI()

@app.on_event("startup")
async def startup():
    config = PostgresPoolConfig.from_env()
    app.state.db_pool = AsyncPostgresConnectionPool(config)
    await app.state.db_pool.initialize_pool()

@app.on_event("shutdown")
async def shutdown():
    await app.state.db_pool.close_all_connections()

async def get_db():
    async with app.state.db_pool.get_connection() as conn:
        yield conn

@app.get("/users")
async def get_users(conn = Depends(get_db)):
    results = await conn.fetch("SELECT * FROM users")
    return results
```

## Documentation

For detailed setup and usage instructions for each database, see the Setup guides:

- [PostgreSQL Setup Guide](scr/all_db_connectors/connectors/postgres/Setup_guide.md)
- [Redis Setup Guide](scr/all_db_connectors/connectors/redis/Setup_guide.md)
- [MongoDB Setup Guide](scr/all_db_connectors/connectors/mongodb/Setup_guide.md)
- [ClickHouse Setup Guide](scr/all_db_connectors/connectors/clickhouse/Setup_guide.md)
- [RabbitMQ Setup Guide](scr/all_db_connectors/connectors/rabbitmq/Setup_guide.md)
- [Neo4j Setup Guide](scr/all_db_connectors/connectors/neo4j/Setup_guide.md)

## Requirements

- Python 3.8+
- See individual setup guides for database-specific driver requirements

## Contributing

Contributions are welcome! Please read our contributing guidelines first.

## License

MIT License - see LICENSE file for details


# ============================================================================
# Makefile
# ============================================================================
.PHONY: help install install-dev test lint format type-check clean build publish docs

help:
	@echo "Available commands:"
	@echo "  install       Install package"
	@echo "  install-dev   Install package with dev dependencies"
	@echo "  test          Run tests"
	@echo "  lint          Run linters"
	@echo "  format        Format code"
	@echo "  type-check    Run type checker"
	@echo "  clean         Clean build artifacts"
	@echo "  build         Build package"
	@echo "  publish       Publish to PyPI"
	@echo "  docs          Build documentation"

install:
	pip install -e .

install-dev:
	pip install -e ".[dev,all]"

test:
	python -m unittest discover -s tests -p "test_*.py"

lint:
	ruff check scr/ tests/

format:
	black scr/ tests/
	ruff check --fix scr/ tests/

type-check:
	mypy scr/

clean:
	rm -rf build/ dist/ *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov

build: clean
	python -m build

publish: build
	python -m twine upload dist/*

docs:
	cd docs && make html
