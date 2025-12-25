# ClickHouse Connector - Installation & Setup

## Installation

### Option 1: Install with Sync Support

```bash
pip install db_connections[clickhouse-sync]
```

### Option 2: Install with Async Support

```bash
pip install db_connections[clickhouse-async]
```

### Option 3: Install with Both Sync and Async

```bash
pip install db_connections[clickhouse]
```

## Dependencies

Add these to your `pyproject.toml`:

```toml
[project.optional-dependencies]
clickhouse-sync = [
    "clickhouse-connect>=0.6.0",
]
clickhouse-async = [
    "clickhouse-connect>=0.6.0",
]
clickhouse = [
    "clickhouse-connect>=0.6.0",
]
```

## Quick Start

### Synchronous Usage

```python
from db_connections.scr.all_db_connectors.connectors.clickhouse import (
    ClickHouseSyncConnectionPool,
    ClickHousePoolConfig,
)

# Configure pool
config = ClickHousePoolConfig(
    host="localhost",
    port=9000,
    database="default",
    username="default",
    password="",
    min_connections=2,
    max_connections=10,
)

# Use pool
with ClickHouseSyncConnectionPool(config) as pool:
    with pool.get_connection() as client:
        # Execute a query
        result = client.query("SELECT 1 as value")
        print(result.result_rows)
        
        # Insert data
        client.insert(
            "users",
            [[1, "John", "john@example.com"]],
            column_names=["id", "name", "email"]
        )
```

### Asynchronous Usage

```python
from db_connections.scr.all_db_connectors.connectors.clickhouse import (
    ClickHouseAsyncConnectionPool,
    ClickHousePoolConfig,
)

config = ClickHousePoolConfig(
    host="localhost",
    port=9000,
    database="default",
    username="default",
    password="",
)

async with ClickHouseAsyncConnectionPool(config) as pool:
    async with pool.get_connection() as client:
        # Execute async query
        result = await client.query("SELECT 1 as value")
        print(result.result_rows)
        
        # Async insert
        await client.insert(
            "users",
            [[1, "John", "john@example.com"]],
            column_names=["id", "name", "email"]
        )
```

## Configuration Options

### Basic Configuration

```python
config = ClickHousePoolConfig(
    # Connection details
    host="localhost",
    port=9000,
    database="default",
    username="default",
    password="",
    
    # Pool sizing
    min_connections=2,
    max_connections=10,
    max_overflow=5,
    
    # Timeouts (in seconds)
    timeout=30,
    connect_timeout=10,
    send_receive_timeout=30,
    
    # Connection lifecycle
    max_lifetime=1800,  # 30 minutes
    idle_timeout=300,   # 5 minutes
    
    # Validation
    validate_on_checkout=True,
    pre_ping=True,
)
```

### ClickHouse-Specific Configuration

```python
config = ClickHousePoolConfig(
    host="localhost",
    port=9000,
    database="default",
    
    # Authentication
    username="default",
    password="secret",
    
    # SSL/TLS
    secure=False,
    verify=True,
    ca_certs="/path/to/ca.crt",
    cert="/path/to/client.crt",
    key="/path/to/client.key",
    
    # Protocol selection
    use_http=False,  # Use native protocol (default) or HTTP interface
    
    # Compression
    compression=False,
    
    # Query settings
    settings={
        "max_execution_time": 300,
        "max_memory_usage": 10000000000,
    },
    
    # Client identification
    client_name="MyApp",
    
    # Cluster settings
    cluster="my_cluster",
    alt_hosts=["host2:9000", "host3:9000"],
    
    # Performance settings
    insert_block_size=1048576,
    max_block_size=65536,
    
    # Retry settings
    max_retries=3,
    retry_delay=1.0,
    retry_backoff=2.0,
)
```

### Environment Variables

```bash
export CLICKHOUSE_HOST=localhost
export CLICKHOUSE_PORT=9000
export CLICKHOUSE_DATABASE=default
export CLICKHOUSE_USERNAME=default
export CLICKHOUSE_PASSWORD=secret
export CLICKHOUSE_SECURE=false
export CLICKHOUSE_USE_HTTP=false
export CLICKHOUSE_CONNECTION_URL=clickhouse://default:secret@localhost:9000/default
```

```python
config = ClickHousePoolConfig.from_env()
```

### Connection String (URL)

```python
# Standard connection
url = "clickhouse://default:secret@localhost:9000/default"

# SSL connection
url = "clickhouses://default:secret@localhost:9440/default"

# HTTP interface
url = "clickhouse://default:secret@localhost:8123/default?use_http=true"

config = ClickHousePoolConfig.from_url(url)
```

## Features

### ✅ Connection Pooling
- Min/max pool size control
- Connection overflow handling
- Automatic connection recycling
- Thread-safe (sync) and coroutine-safe (async)

### ✅ Reliability
- Automatic reconnection on failure
- Connection validation
- Retry logic with exponential backoff
- Connection health checks

### ✅ Health Checks
- Pool health monitoring
- Database server health checks
- Server info and status
- Response time tracking

### ✅ Metrics
- Active/idle connection tracking
- Connection creation/closure counts
- Wait time statistics
- Pool utilization metrics

### ✅ ClickHouse-Specific Features
- Native protocol and HTTP interface support
- SSL/TLS encryption
- Query settings and parameters
- Batch insert operations
- Cluster and replication support
- Compression support

## Common Use Cases

### Creating Tables

```python
with ClickHouseSyncConnectionPool(config) as pool:
    with pool.get_connection() as client:
        client.command("""
            CREATE TABLE IF NOT EXISTS events (
                event_id UInt64,
                event_type String,
                timestamp DateTime,
                data String
            ) ENGINE = MergeTree()
            ORDER BY (event_type, timestamp)
        """)
```

### Inserting Data

```python
with pool.get_connection() as client:
    # Single row insert
    client.insert(
        "events",
        [[1, "click", "2024-01-01 00:00:00", "data"]],
        column_names=["event_id", "event_type", "timestamp", "data"]
    )
    
    # Batch insert
    data = [
        [2, "view", "2024-01-01 00:01:00", "data2"],
        [3, "click", "2024-01-01 00:02:00", "data3"],
    ]
    client.insert("events", data, column_names=["event_id", "event_type", "timestamp", "data"])
```

### Querying Data

```python
with pool.get_connection() as client:
    # Simple query
    result = client.query("SELECT * FROM events LIMIT 10")
    for row in result.result_rows:
        print(row)
    
    # Query with parameters
    result = client.query(
        "SELECT * FROM events WHERE event_type = {type:String}",
        parameters={"type": "click"}
    )
    
    # Query with settings
    result = client.query(
        "SELECT * FROM events",
        settings={"max_execution_time": 10}
    )
```

### Health Monitoring

```python
pool = ClickHouseSyncConnectionPool(config)
pool.initialize_pool()

# Pool health
health = pool.health_check()
print(f"Pool health: {health.state}")

# Database health
db_health = pool.database_health_check()
print(f"Database health: {db_health.state}")
print(f"Response time: {db_health.response_time_ms}ms")

# Pool metrics
metrics = pool.get_metrics()
print(f"Active: {metrics.active_connections}")
print(f"Idle: {metrics.idle_connections}")
```

### Async Operations

```python
async with ClickHouseAsyncConnectionPool(config) as pool:
    async with pool.get_connection() as client:
        # Async query
        result = await client.query("SELECT * FROM events")
        
        # Async insert
        await client.insert("events", data, column_names=["event_id", "event_type"])
        
        # Concurrent queries
        import asyncio
        tasks = [
            client.query("SELECT count() FROM events WHERE event_type = 'click'"),
            client.query("SELECT count() FROM events WHERE event_type = 'view'"),
        ]
        results = await asyncio.gather(*tasks)
```

## Advanced Configuration

### SSL/TLS Configuration

```python
config = ClickHousePoolConfig(
    host="clickhouse.example.com",
    port=9440,  # SSL port
    secure=True,
    verify=True,
    ca_certs="/path/to/ca.crt",
    cert="/path/to/client.crt",
    key="/path/to/client.key",
)
```

### Cluster Configuration

```python
config = ClickHousePoolConfig(
    host="clickhouse-node1",
    port=9000,
    cluster="my_cluster",
    alt_hosts=["clickhouse-node2:9000", "clickhouse-node3:9000"],
)
```

### HTTP Interface

```python
config = ClickHousePoolConfig(
    host="localhost",
    port=8123,  # HTTP port
    use_http=True,
)
```

### Custom Query Settings

```python
config = ClickHousePoolConfig(
    host="localhost",
    port=9000,
    settings={
        "max_execution_time": 300,
        "max_memory_usage": 10000000000,
        "max_threads": 4,
    }
)
```

## Troubleshooting

### Connection Errors

```python
# Enable connection validation
config = ClickHousePoolConfig(
    host="localhost",
    port=9000,
    validate_on_checkout=True,
    pre_ping=True,
)

# Check pool status
pool = ClickHouseSyncConnectionPool(config)
pool.initialize_pool()
status = pool.pool_status()
print(status)
```

### Performance Tuning

```python
config = ClickHousePoolConfig(
    host="localhost",
    port=9000,
    # Increase pool size for high concurrency
    min_connections=5,
    max_connections=20,
    max_overflow=10,
    
    # Optimize timeouts
    timeout=60,
    connect_timeout=10,
    
    # Enable compression for large data
    compression=True,
    
    # Tune block sizes
    insert_block_size=1048576,
    max_block_size=65536,
)
```

### Monitoring

```python
# Get detailed metrics
metrics = pool.get_metrics()
print(f"Total connections: {metrics.total_connections}")
print(f"Active connections: {metrics.active_connections}")
print(f"Idle connections: {metrics.idle_connections}")
print(f"Average wait time: {metrics.average_wait_time_ms}ms")

# Health checks
health = pool.health_check()
db_health = pool.database_health_check()
```

## Best Practices

1. **Use Context Managers**: Always use context managers to ensure connections are properly released.

2. **Connection Pooling**: Configure appropriate min/max connections based on your workload.

3. **Error Handling**: Implement retry logic for transient failures.

4. **Health Monitoring**: Regularly check pool and database health.

5. **Query Optimization**: Use appropriate query settings and parameters.

6. **Batch Operations**: Use batch inserts for better performance.

7. **Connection Validation**: Enable `validate_on_checkout` and `pre_ping` for reliability.

## Examples

See `clickhouse_usage.py` for comprehensive examples including:
- Basic CRUD operations
- Batch inserts
- Query parameters and settings
- Health checks and monitoring
- Async operations
- Error handling
- And more...

## API Reference

### ClickHousePoolConfig

Main configuration class for ClickHouse connection pools.

**Key Parameters:**
- `host`: ClickHouse server host
- `port`: Server port (9000 for native, 8123 for HTTP)
- `database`: Database name
- `username`: Username for authentication
- `password`: Password for authentication
- `secure`: Enable SSL/TLS
- `use_http`: Use HTTP interface instead of native protocol
- `settings`: Query settings dictionary
- `min_connections`: Minimum pool size
- `max_connections`: Maximum pool size

### ClickHouseSyncConnectionPool

Synchronous connection pool for ClickHouse.

**Key Methods:**
- `get_connection()`: Get a connection from the pool
- `initialize_pool()`: Initialize the connection pool
- `health_check()`: Check pool health
- `database_health_check()`: Check database health
- `get_metrics()`: Get pool metrics
- `pool_status()`: Get pool status

### ClickHouseAsyncConnectionPool

Asynchronous connection pool for ClickHouse.

**Key Methods:**
- `get_connection()`: Get an async connection from the pool
- `initialize_pool()`: Initialize the connection pool
- `health_check()`: Check pool health (async)
- `database_health_check()`: Check database health (async)
- `get_metrics()`: Get pool metrics (async)
- `pool_status()`: Get pool status (async)

## Support

For issues and questions, please refer to the main project documentation or open an issue on the project repository.

