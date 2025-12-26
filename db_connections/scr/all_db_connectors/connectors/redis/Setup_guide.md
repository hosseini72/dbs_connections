# Redis Connector - Installation & Setup

## Installation

### Option 1: Install with Sync Support

```bash
pip install db_connections[redis-sync]
```

### Option 2: Install with Async Support

```bash
pip install db_connections[redis-async]
```

### Option 3: Install with Both Sync and Async

```bash
pip install db_connections[redis]
```

## Quick Start

### Synchronous Usage

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
    password=None,
    min_size=2,
    max_size=10,
)

# Use pool
with RedisSyncConnectionPool(config) as pool:
    with pool.get_connection() as conn:
        conn.set("key", "value")
        value = conn.get("key")
```

### Asynchronous Usage

```python
from db_connections.scr.all_db_connectors.connectors.redis import (
    RedisAsyncConnectionPool,
    RedisPoolConfig,
)

config = RedisPoolConfig(
    host="localhost",
    port=6379,
    db=0,
)

async with RedisAsyncConnectionPool(config) as pool:
    async with pool.get_connection() as conn:
        await conn.set("key", "value")
        value = await conn.get("key")
```

## Configuration Options

### Basic Configuration

```python
config = RedisPoolConfig(
    # Connection details
    host="localhost",
    port=6379,
    db=0,
    password="secret",
    username="default",  # Redis 6+ ACL
    
    # Pool sizing
    min_size=2,
    max_size=10,
    max_overflow=5,
    
    # Timeouts
    timeout=10,  # Socket timeout in seconds
    socket_timeout=5,
    socket_connect_timeout=3,
    
    # Connection lifecycle
    max_lifetime=1800,  # 30 minutes
    idle_timeout=300,   # 5 minutes
    
    # Validation
    validate_on_checkout=True,
    pre_ping=True,
    
    # Redis-specific
    decode_responses=False,  # Set to True for automatic string conversion
    encoding="utf-8",
    retry_on_timeout=False,
)
```

### Environment Variables

```bash
export REDIS_HOST=localhost
export REDIS_PORT=6379
export REDIS_DB=0
export REDIS_PASSWORD=secret
export REDIS_USERNAME=default
export REDIS_URL=redis://localhost:6379/0
export REDIS_SSL=false
```

```python
config = RedisPoolConfig.from_env()
```

### Connection URL

```python
url = "redis://localhost:6379/0"
config = RedisPoolConfig.from_url(url)

# With authentication
url = "redis://username:password@localhost:6379/0"
config = RedisPoolConfig.from_url(url)

# With SSL (rediss://)
url = "rediss://localhost:6380/0"
config = RedisPoolConfig.from_url(url)
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
- Pipeline support for batch operations

### ✅ Health Checks
- Pool health monitoring
- Redis server health checks
- Response time tracking
- Memory usage monitoring
- Replication status checking

### ✅ Metrics
- Active/idle connection tracking
- Connection creation/closure counts
- Wait time statistics
- Pool utilization metrics

### ✅ Configuration
- Environment variable support
- Connection URL support
- SSL/TLS configuration
- Authentication support (password and ACL)

## Health Monitoring

```python
# Check pool health
health = pool.health_check()
print(f"State: {health.state.value}")
print(f"Message: {health.message}")
print(f"Response time: {health.response_time_ms}ms")

# Check Redis server health
db_health = pool.database_health_check()
print(f"Redis version: {db_health.details.get('redis_version')}")
print(f"Memory usage: {db_health.details.get('used_memory_mb')} MB")
print(f"Memory usage %: {db_health.details.get('memory_usage_percent')}%")
print(f"Connected clients: {db_health.details.get('connected_clients')}")
```

## Metrics Collection

```python
metrics = pool.get_metrics()
print(f"Total connections: {metrics.total_connections}")
print(f"Active: {metrics.active_connections}")
print(f"Idle: {metrics.idle_connections}")
print(f"Average wait time: {metrics.average_wait_time_ms}ms")
```

## Redis Operations Examples

### String Operations

```python
with pool.get_connection() as conn:
    # Set with expiration
    conn.set("key", "value", ex=3600)  # Expire in 1 hour
    
    # Get value
    value = conn.get("key")
    
    # Increment counter
    count = conn.incr("counter")
    
    # Set multiple
    conn.mset({"key1": "value1", "key2": "value2"})
```

### Hash Operations

```python
with pool.get_connection() as conn:
    # Set hash fields
    conn.hset("user:1", mapping={
        "name": "Alice",
        "email": "alice@example.com",
        "age": "30"
    })
    
    # Get all fields
    user = conn.hgetall("user:1")
    
    # Get specific field
    name = conn.hget("user:1", "name")
```

### List Operations

```python
with pool.get_connection() as conn:
    # Push to list
    conn.lpush("tasks", "task1", "task2")
    
    # Get range
    tasks = conn.lrange("tasks", 0, -1)
    
    # Pop from list
    task = conn.rpop("tasks")
```

### Set Operations

```python
with pool.get_connection() as conn:
    # Add to set
    conn.sadd("tags", "python", "redis", "database")
    
    # Get all members
    tags = conn.smembers("tags")
    
    # Check membership
    is_member = conn.sismember("tags", "python")
```

### Pipeline Operations (Batch)

```python
with pool.get_connection() as conn:
    pipeline = conn.pipeline()
    pipeline.set("key1", "value1")
    pipeline.set("key2", "value2")
    pipeline.get("key1")
    results = pipeline.execute()
```

## Async Pipeline Operations

```python
async with pool.get_connection() as conn:
    pipeline = conn.pipeline()
    pipeline.set("key1", "value1")
    pipeline.set("key2", "value2")
    pipeline.get("key1")
    results = await pipeline.execute()
```

## Best Practices

### 1. Use Context Managers

Always use context managers to ensure proper cleanup:

```python
# Good
with pool.get_connection() as conn:
    conn.set("key", "value")

# Bad
conn = pool.get_connection()
conn.set("key", "value")
# Connection might not be returned!
```

### 2. Configure Pool Size Appropriately

```python
# For web applications
config = RedisPoolConfig(
    min_size=5,   # Keep some connections warm
    max_size=20,  # Allow bursts
    max_overflow=10,  # Extra capacity
)

# For background workers
config = RedisPoolConfig(
    min_size=2,
    max_size=5,
    max_overflow=0,
)
```

### 3. Enable Health Checks

```python
config = RedisPoolConfig(
    validate_on_checkout=True,  # Validate before use
    pre_ping=True,              # Ping before queries
    health_check_interval=30,   # Check every 30 seconds
)
```

### 4. Use Decode Responses for Strings

```python
# If you work primarily with strings
config = RedisPoolConfig(
    decode_responses=True,  # Automatically decode bytes to strings
    encoding="utf-8",
)

# Then values are automatically strings
value = conn.get("key")  # Returns str, not bytes
```

### 5. Configure Appropriate Timeouts

```python
config = RedisPoolConfig(
    timeout=10,  # Socket timeout
    socket_connect_timeout=5,  # Connection timeout
    retry_on_timeout=True,  # Retry on timeout
    max_retries=3,  # Maximum retries
)
```

### 6. Use Pipelines for Batch Operations

```python
# Good: Use pipeline for multiple operations
pipeline = conn.pipeline()
for i in range(100):
    pipeline.set(f"key:{i}", f"value:{i}")
pipeline.execute()

# Bad: Multiple round trips
for i in range(100):
    conn.set(f"key:{i}", f"value:{i}")  # 100 round trips!
```

## Error Handling

```python
from db_connections.scr.all_db_connectors.core.exceptions import (
    ConnectionError,
    PoolTimeoutError,
    PoolExhaustedError,
)

try:
    with pool.get_connection() as conn:
        value = conn.get("key")
except PoolTimeoutError:
    print("No connections available")
except ConnectionError:
    print("Failed to connect to Redis")
except Exception as e:
    print(f"Operation failed: {e}")
```

## FastAPI Integration Example

```python
from fastapi import FastAPI, Depends
from db_connections.scr.all_db_connectors.connectors.redis import (
    RedisAsyncConnectionPool,
    RedisPoolConfig,
)

app = FastAPI()

# Initialize pool on startup
@app.on_event("startup")
async def startup():
    config = RedisPoolConfig.from_env()
    app.state.redis_pool = RedisAsyncConnectionPool(config)
    await app.state.redis_pool.initialize_pool()

@app.on_event("shutdown")
async def shutdown():
    await app.state.redis_pool.close_all_connections()

# Dependency
async def get_redis():
    async with app.state.redis_pool.get_connection() as conn:
        yield conn

# Use in endpoints
@app.get("/cache/{key}")
async def get_cache(key: str, conn = Depends(get_redis)):
    value = await conn.get(key)
    return {"key": key, "value": value}

@app.post("/cache/{key}")
async def set_cache(key: str, value: str, conn = Depends(get_redis)):
    await conn.set(key, value, ex=3600)  # Cache for 1 hour
    return {"key": key, "value": value, "cached": True}
```

## Flask Integration Example

```python
from flask import Flask, g
from db_connections.scr.all_db_connectors.connectors.redis import (
    RedisSyncConnectionPool,
    RedisPoolConfig,
)

app = Flask(__name__)

# Initialize pool
config = RedisPoolConfig.from_env()
redis_pool = RedisSyncConnectionPool(config)
redis_pool.initialize_pool()

@app.before_request
def get_redis():
    g.redis = redis_pool.get_connection()

@app.teardown_request
def close_redis(exception):
    redis = g.pop('redis', None)
    if redis:
        redis_pool.release_connection(redis)

@app.route('/cache/<key>')
def get_cache(key):
    with g.redis() as conn:
        value = conn.get(key)
        return {"key": key, "value": value.decode() if value else None}
```

## Troubleshooting

### Connection Pool Exhausted

```python
# Increase pool size
config = RedisPoolConfig(
    max_size=20,      # More connections
    max_overflow=10,  # Allow temporary overflow
    timeout=60,       # Wait longer
)
```

### Slow Responses

```python
# Check health for performance issues
health = pool.database_health_check()
if health.details.get('response_time_ms', 0) > 500:
    print("Redis is slow, check server resources")

# Enable retry on timeout
config = RedisPoolConfig(
    retry_on_timeout=True,
    timeout=10,
)
```

### Connection Validation Failures

```python
# Enable more aggressive validation
config = RedisPoolConfig(
    pre_ping=True,
    validate_on_checkout=True,
    ping_interval=30,  # Ping every 30s
)
```

### Memory Issues

```python
# Monitor memory usage
health = pool.database_health_check()
memory_percent = health.details.get('memory_usage_percent', 0)
if memory_percent > 90:
    print("Redis memory usage is high!")
```

## Testing

```python
# Test with pytest
import pytest
from db_connections.scr.all_db_connectors.connectors.redis import (
    RedisSyncConnectionPool,
    RedisPoolConfig,
)

@pytest.fixture
def redis_pool():
    config = RedisPoolConfig(
        host="localhost",
        port=6379,
        db=15,  # Use test database
    )
    pool = RedisSyncConnectionPool(config)
    yield pool
    pool.close_all_connections()

def test_connection(redis_pool):
    with redis_pool.get_connection() as conn:
        result = conn.ping()
        assert result is True

def test_set_get(redis_pool):
    with redis_pool.get_connection() as conn:
        conn.set("test_key", "test_value")
        value = conn.get("test_key")
        assert value == b"test_value" or value == "test_value"
```

## Advanced Configuration

### SSL/TLS Configuration

```python
config = RedisPoolConfig(
    host="redis.example.com",
    port=6380,
    ssl=True,
    ssl_cert_reqs="required",
    ssl_ca_certs="/path/to/ca.crt",
    ssl_certfile="/path/to/client.crt",
    ssl_keyfile="/path/to/client.key",
    ssl_check_hostname=True,
)
```

### Connection String with SSL

```python
url = "rediss://username:password@redis.example.com:6380/0"
config = RedisPoolConfig.from_url(url)
```

### Custom Socket Options

```python
config = RedisPoolConfig(
    host="localhost",
    port=6379,
    socket_keepalive=True,
    socket_keepalive_options={
        "TCP_KEEPIDLE": 1,
        "TCP_KEEPINTVL": 3,
        "TCP_KEEPCNT": 5,
    },
)
```

