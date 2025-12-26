# PostgreSQL Connector - Installation & Setup

## Installation

### Option 1: Install with Sync Support (psycopg2)

```bash
pip install db_connections[postgres-sync]
```

### Option 2: Install with Async Support (asyncpg)

```bash
pip install db_connections[postgres-async]
```

### Option 3: Install with Both Sync and Async

```bash
pip install db_connections[postgres]
```

## Quick Start

### Synchronous Usage

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

# Use pool
with PostgresConnectionPool(config) as pool:
    with pool.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users")
        results = cursor.fetchall()
```

### Asynchronous Usage

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

## Configuration Options

### Basic Configuration

```python
config = PostgresPoolConfig(
    # Connection details
    host="localhost",
    port=5432,
    database="mydb",
    user="postgres",
    password="secret",
    
    # Pool sizing
    min_size=2,
    max_size=10,
    max_overflow=5,
    
    # Timeouts
    timeout=30,
    connection_timeout=10,
    
    # Connection lifecycle
    max_lifetime=1800,  # 30 minutes
    idle_timeout=300,   # 5 minutes
    
    # Validation
    validate_on_checkout=True,
    pre_ping=True,
)
```

### Environment Variables

```bash
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DATABASE=mydb
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=secret
export POSTGRES_SSLMODE=require
```

```python
config = PostgresPoolConfig.from_env()
```

### Connection String (DSN)

```python
dsn = "postgresql://user:pass@localhost:5432/mydb?sslmode=require"
config = PostgresPoolConfig.from_dsn(dsn)
```

## Features

### ✅ Connection Pooling
- Min/max pool size control
- Connection overflow handling
- Automatic connection recycling
- Thread-safe (sync) and coroutine-safe (async)

### ✅ Reliability
- Automatic reconnection on failure
- Connection validation (pre-ping)
- Retry logic with exponential backoff
- Transaction support

### ✅ Health Checks
- Pool health monitoring
- Database server health checks
- Response time tracking
- Detailed health status reporting

### ✅ Metrics
- Active/idle connection tracking
- Connection creation/closure counts
- Wait time statistics
- Pool utilization metrics

### ✅ Configuration
- Environment variable support
- Connection string (DSN) support
- SSL/TLS configuration
- Server settings customization

## Health Monitoring

```python
# Check pool health
health = pool.health_check()
print(f"State: {health.state.value}")
print(f"Message: {health.message}")
print(f"Response time: {health.response_time_ms}ms")

# Check database health
db_health = pool.database_health_check()
print(f"Database version: {db_health.details.get('server_version')}")
print(f"Active queries: {db_health.details.get('active_queries')}")
```

## Metrics Collection

```python
metrics = pool.get_metrics()
print(f"Total connections: {metrics.total_connections}")
print(f"Active: {metrics.active_connections}")
print(f"Idle: {metrics.idle_connections}")
print(f"Average wait time: {metrics.average_wait_time_ms}ms")
```

## Best Practices

### 1. Use Context Managers

Always use context managers to ensure proper cleanup:

```python
# Good
with pool.get_connection() as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT 1")

# Bad
conn = pool.get_connection()
cursor = conn.cursor()
cursor.execute("SELECT 1")
# Connection might not be returned!
```

### 2. Configure Pool Size Appropriately

```python
# For web applications
config = PostgresPoolConfig(
    min_size=5,   # Keep some connections warm
    max_size=20,  # Allow bursts
    max_overflow=10,  # Extra capacity
)

# For background workers
config = PostgresPoolConfig(
    min_size=2,
    max_size=5,
    max_overflow=0,
)
```

### 3. Enable Health Checks

```python
config = PostgresPoolConfig(
    validate_on_checkout=True,  # Validate before use
    pre_ping=True,              # Ping before queries
)
```

### 4. Configure Connection Lifecycle

```python
config = PostgresPoolConfig(
    max_lifetime=1800,  # Recycle after 30 minutes
    idle_timeout=300,   # Close idle after 5 minutes
    recycle_on_return=False,  # Only when needed
)
```

### 5. Use Appropriate Timeouts

```python
config = PostgresPoolConfig(
    timeout=30,            # Pool acquisition timeout
    connection_timeout=10, # TCP connection timeout
    command_timeout=60,    # Query timeout (async only)
)
```

## Error Handling

```python
from db_conections.scr.all_db_connectors.core.exceptions import (
    ConnectionError,
    PoolTimeoutError,
    PoolExhaustedError,
)

try:
    with pool.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users")
except PoolTimeoutError:
    print("No connections available")
except ConnectionError:
    print("Failed to connect to database")
except Exception as e:
    print(f"Query failed: {e}")
```

## FastAPI Integration Example

```python
from fastapi import FastAPI, Depends
from db_conections.scr.all_db_connectors.connectors.postgres import (
    AsyncPostgresConnectionPool,
    PostgresPoolConfig,
)

app = FastAPI()

# Initialize pool on startup
@app.on_event("startup")
async def startup():
    config = PostgresPoolConfig.from_env()
    app.state.db_pool = AsyncPostgresConnectionPool(config)
    await app.state.db_pool.initialize_pool()

@app.on_event("shutdown")
async def shutdown():
    await app.state.db_pool.close_all_connections()

# Dependency
async def get_db():
    async with app.state.db_pool.get_connection() as conn:
        yield conn

# Use in endpoints
@app.get("/users")
async def get_users(conn = Depends(get_db)):
    users = await conn.fetch("SELECT * FROM users")
    return users
```

## Troubleshooting

### Connection Pool Exhausted

```python
# Increase pool size
config = PostgresPoolConfig(
    max_size=20,      # More connections
    max_overflow=10,  # Allow temporary overflow
    timeout=60,       # Wait longer
)
```

### Slow Queries

```python
# Add query timeout
config = PostgresPoolConfig(
    command_timeout=30,  # Kill slow queries after 30s
)
```

### Connection Validation Failures

```python
# Enable more aggressive validation
config = PostgresPoolConfig(
    pre_ping=True,
    validate_on_checkout=True,
    ping_interval=30,  # Ping every 30s
)
```

## Testing

```python
# Test with pytest
import pytest
from db_conections.scr.all_db_connectors.connectors.postgres import PostgresConnectionPool, PostgresPoolConfig

@pytest.fixture
def db_pool():
    config = PostgresPoolConfig(
        host="localhost",
        database="test_db",
        user="test_user",
        password="test_pass",
    )
    pool = PostgresConnectionPool(config)
    yield pool
    pool.close_all_connections()

def test_connection(db_pool):
    with db_pool.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        assert cursor.fetchone()[0] == 1
```