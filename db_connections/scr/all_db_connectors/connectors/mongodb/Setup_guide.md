# MongoDB Connector - Installation & Setup

## Installation

### Option 1: Install with Sync Support (pymongo)

```bash
pip install db_connections[mongodb-sync]
```

### Option 2: Install with Async Support (motor)

```bash
pip install db_connections[mongodb-async]
```

### Option 3: Install with Both Sync and Async

```bash
pip install db_connections[mongodb]
```

## Dependencies

Add these to your `pyproject.toml`:

```toml
[project.optional-dependencies]
mongodb-sync = [
    "pymongo>=4.6.0",
]
mongodb-async = [
    "motor>=3.3.0",
]
mongodb = [
    "pymongo>=4.6.0",
    "motor>=3.3.0",
]
```

## Quick Start

### Synchronous Usage

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
        
        # Insert a document
        result = collection.insert_one({"name": "John", "email": "john@example.com"})
        
        # Find documents
        user = collection.find_one({"name": "John"})
        print(user)
```

### Asynchronous Usage

```python
from db_connections.scr.all_db_connectors.connectors.mongodb import (
    MongoAsyncConnectionPool,
    MongoPoolConfig,
)

config = MongoPoolConfig(
    host="localhost",
    port=27017,
    database="mydb",
    username="admin",
    password="secret",
)

async with MongoAsyncConnectionPool(config) as pool:
    async with pool.get_connection() as client:
        db = client[config.database]
        collection = db["users"]
        
        # Insert a document
        result = await collection.insert_one({"name": "Jane", "email": "jane@example.com"})
        
        # Find documents
        user = await collection.find_one({"name": "Jane"})
        print(user)
```

## Configuration Options

### Basic Configuration

```python
config = MongoPoolConfig(
    # Connection details
    host="localhost",
    port=27017,
    database="mydb",
    username="admin",
    password="secret",
    
    # Pool sizing
    min_size=2,
    max_size=10,
    max_overflow=5,
    
    # Timeouts (in milliseconds for MongoDB)
    connect_timeout_ms=20000,
    socket_timeout_ms=30000,
    server_selection_timeout_ms=30000,
    
    # Connection lifecycle
    max_lifetime=1800,  # 30 minutes (in seconds)
    idle_timeout=300,   # 5 minutes (in seconds)
    max_idle_time_ms=300000,  # 5 minutes (in milliseconds)
    
    # Validation
    validate_on_checkout=True,
    pre_ping=True,
)
```

### MongoDB-Specific Configuration

```python
config = MongoPoolConfig(
    host="localhost",
    port=27017,
    database="mydb",
    
    # Authentication
    username="admin",
    password="secret",
    auth_source="admin",  # Authentication database
    auth_mechanism="SCRAM-SHA-256",  # or "SCRAM-SHA-1", "MONGODB-CR"
    
    # Replica Set
    replica_set="myReplicaSet",
    read_preference="secondaryPreferred",  # primary, secondary, etc.
    read_concern_level="majority",
    write_concern={"w": 1, "j": True},
    
    # SSL/TLS
    tls=True,
    tls_ca_file="/path/to/ca.pem",
    tls_certificate_key_file="/path/to/client.pem",
    tls_allow_invalid_certificates=False,
    
    # Compression
    compressors=["snappy", "zlib"],  # or ["zstd"]
    
    # Connection behavior
    retry_writes=True,
    retry_reads=True,
    direct_connection=False,  # Connect directly to single host
    
    # Application metadata
    app_name="MyMicroservice",
)
```

### Environment Variables

```bash
export MONGODB_HOST=localhost
export MONGODB_PORT=27017
export MONGODB_DATABASE=mydb
export MONGODB_USERNAME=admin
export MONGODB_PASSWORD=secret
export MONGODB_AUTH_SOURCE=admin
export MONGODB_REPLICA_SET=myReplicaSet
export MONGODB_TLS=true
export MONGODB_CONNECTION_STRING=mongodb://localhost:27017/mydb
```

```python
config = MongoPoolConfig.from_env()
```

### Connection String (URI)

```python
# Standard connection
uri = "mongodb://admin:secret@localhost:27017/mydb?authSource=admin"

# Replica set
uri = "mongodb://admin:secret@host1:27017,host2:27017,host3:27017/mydb?replicaSet=myReplicaSet"

# SSL/TLS (mongodb+srv for MongoDB Atlas)
uri = "mongodb+srv://admin:secret@cluster.mongodb.net/mydb?retryWrites=true&w=majority"

config = MongoPoolConfig.from_uri(uri)
```

## Features

### ✅ Connection Pooling
- Min/max pool size control
- Connection overflow handling
- Automatic connection recycling
- Thread-safe (sync) and coroutine-safe (async)

### ✅ Reliability
- Automatic reconnection on failure
- Connection validation (ping)
- Retry logic with exponential backoff
- Transaction support (MongoDB 4.0+)

### ✅ Health Checks
- Pool health monitoring
- Database server health checks
- Server status and version info
- Response time tracking

### ✅ Metrics
- Active/idle connection tracking
- Connection creation/closure counts
- Wait time statistics
- Pool utilization metrics

### ✅ MongoDB-Specific Features
- Replica set support
- Read preferences and concerns
- Write concerns
- SSL/TLS configuration
- Compression support
- Connection string (URI) support

## Health Monitoring

```python
from db_connections.scr.all_db_connectors.connectors.mongodb.health import MongoHealthChecker

# Check pool health
health_checker = MongoHealthChecker(pool)
pool_health = health_checker.check_pool()
print(f"State: {pool_health.state.value}")
print(f"Message: {pool_health.message}")
print(f"Response time: {pool_health.response_time_ms}ms")

# Check database health
db_health = health_checker.check_database()
print(f"Server version: {db_health.details.get('server_version')}")
print(f"Uptime: {db_health.details.get('uptime_seconds')} seconds")
print(f"Current connections: {db_health.details.get('current_connections')}")
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
with pool.get_connection() as client:
    db = client[database]
    collection = db["users"]
    collection.insert_one({"name": "John"})

# Bad
client = pool.get_connection()  # Not returned automatically!
db = client[database]
collection.insert_one({"name": "John"})
```

### 2. Configure Pool Size Appropriately

```python
# For web applications
config = MongoPoolConfig(
    min_size=5,      # Keep some connections warm
    max_pool_size=50,  # Allow more concurrent connections
)

# For background workers
config = MongoPoolConfig(
    min_size=2,
    max_pool_size=10,
)
```

### 3. Enable Health Checks

```python
config = MongoPoolConfig(
    validate_on_checkout=True,  # Validate before use
    pre_ping=True,              # Ping before queries
    heartbeat_frequency_ms=10000,  # Ping every 10 seconds
)
```

### 4. Configure Connection Lifecycle

```python
config = MongoPoolConfig(
    max_lifetime=1800,       # Recycle after 30 minutes
    max_idle_time_ms=300000, # Close idle after 5 minutes
)
```

### 5. Use Appropriate Timeouts

```python
config = MongoPoolConfig(
    connect_timeout_ms=20000,         # 20 seconds to connect
    socket_timeout_ms=30000,          # 30 seconds for socket operations
    server_selection_timeout_ms=30000, # 30 seconds to select server
)
```

### 6. Configure for Replica Sets

```python
config = MongoPoolConfig(
    replica_set="myReplicaSet",
    read_preference="secondaryPreferred",  # Read from secondary if available
    read_concern_level="majority",         # Read majority-committed data
    write_concern={"w": "majority", "j": True},  # Wait for majority write
)
```

### 7. Use Connection Strings for Complex Setups

```python
# MongoDB Atlas (Cloud)
uri = "mongodb+srv://user:pass@cluster.mongodb.net/db?retryWrites=true&w=majority"

# Replica Set
uri = "mongodb://user:pass@host1:27017,host2:27017,host3:27017/db?replicaSet=rs0"

config = MongoPoolConfig.from_uri(uri)
```

## Error Handling

```python
from db_connections.scr.all_db_connectors.core.exceptions import (
    ConnectionError,
    PoolTimeoutError,
    PoolExhaustedError,
)

try:
    with pool.get_connection() as client:
        db = client[database]
        collection = db["users"]
        collection.insert_one({"name": "John"})
except PoolTimeoutError:
    print("No connections available")
except ConnectionError:
    print("Failed to connect to MongoDB")
except Exception as e:
    print(f"Operation failed: {e}")
```

## FastAPI Integration Example

```python
from fastapi import FastAPI, Depends
from db_connections.scr.all_db_connectors.connectors.mongodb import (
    MongoAsyncConnectionPool,
    MongoPoolConfig,
)

app = FastAPI()

# Initialize pool on startup
@app.on_event("startup")
async def startup():
    config = MongoPoolConfig.from_env()
    app.state.db_pool = MongoAsyncConnectionPool(config)
    await app.state.db_pool.initialize_pool()

@app.on_event("shutdown")
async def shutdown():
    await app.state.db_pool.close_all_connections()

# Dependency
async def get_db():
    async with app.state.db_pool.get_connection() as client:
        yield client[app.state.db_pool.config.database]

# Use in endpoints
@app.get("/users")
async def get_users(db = Depends(get_db)):
    collection = db["users"]
    users = await collection.find({}).to_list(length=100)
    return users

@app.post("/users")
async def create_user(user: dict, db = Depends(get_db)):
    collection = db["users"]
    result = await collection.insert_one(user)
    return {"id": str(result.inserted_id)}
```

## Common Operations

### Insert Documents

```python
# Single document
result = collection.insert_one({"name": "John", "age": 30})
print(f"Inserted ID: {result.inserted_id}")

# Multiple documents
result = collection.insert_many([
    {"name": "Alice", "age": 25},
    {"name": "Bob", "age": 35},
])
print(f"Inserted {len(result.inserted_ids)} documents")
```

### Find Documents

```python
# Find one
user = collection.find_one({"name": "John"})

# Find many
users = collection.find({"age": {"$gt": 30}})
for user in users:
    print(user)

# With projection
users = collection.find(
    {"age": {"$gt": 30}},
    {"name": 1, "email": 1, "_id": 0}  # Only return name and email
)
```

### Update Documents

```python
# Update one
result = collection.update_one(
    {"name": "John"},
    {"$set": {"age": 31}}
)
print(f"Modified {result.modified_count} document")

# Update many
result = collection.update_many(
    {"age": {"$lt": 30}},
    {"$inc": {"age": 1}}  # Increment age by 1
)
print(f"Modified {result.modified_count} documents")
```

### Delete Documents

```python
# Delete one
result = collection.delete_one({"name": "John"})
print(f"Deleted {result.deleted_count} document")

# Delete many
result = collection.delete_many({"age": {"$lt": 18}})
print(f"Deleted {result.deleted_count} documents")
```

### Aggregation

```python
pipeline = [
    {"$match": {"status": "active"}},
    {"$group": {
        "_id": "$category",
        "total": {"$sum": "$amount"}
    }},
    {"$sort": {"total": -1}}
]

results = collection.aggregate(pipeline)
for result in results:
    print(result)
```

## Transactions

MongoDB transactions require a replica set or sharded cluster:

```python
# Sync transaction
with client.start_session() as session:
    with session.start_transaction():
        collection1.insert_one({"doc": 1}, session=session)
        collection2.insert_one({"doc": 2}, session=session)
        # Transaction commits on successful exit

# Async transaction
async with await client.start_session() as session:
    async with session.start_transaction():
        await collection1.insert_one({"doc": 1}, session=session)
        await collection2.insert_one({"doc": 2}, session=session)
        # Transaction commits on successful exit
```

## Troubleshooting

### Connection Pool Exhausted

```python
# Increase pool size
config = MongoPoolConfig(
    max_pool_size=100,  # More connections
    wait_queue_timeout_ms=60000,  # Wait longer
)
```

### Slow Operations

```python
# Add socket timeout
config = MongoPoolConfig(
    socket_timeout_ms=60000,  # 60 seconds
    server_selection_timeout_ms=30000,
)
```

### Connection Validation Failures

```python
# Enable more aggressive validation
config = MongoPoolConfig(
    pre_ping=True,
    validate_on_checkout=True,
    heartbeat_frequency_ms=5000,  # Ping every 5 seconds
)
```

### Replica Set Issues

```python
# Configure replica set properly
config = MongoPoolConfig(
    replica_set="myReplicaSet",  # Must match actual replica set name
    read_preference="primaryPreferred",  # Fallback to primary
    server_selection_timeout_ms=60000,  # Give more time for selection
)
```

## Testing

```python
# Test with pytest
import pytest
from db_connections.scr.all_db_connectors.connectors.mongodb import (
    MongoSyncConnectionPool,
    MongoPoolConfig,
)

@pytest.fixture
def db_pool():
    config = MongoPoolConfig(
        host="localhost",
        port=27017,
        database="test_db",
        username="test_user",
        password="test_pass",
    )
    pool = MongoSyncConnectionPool(config)
    yield pool
    pool.close_all_connections()

def test_connection(db_pool):
    with db_pool.get_connection() as client:
        db = client[db_pool.config.database]
        result = db.command("ping")
        assert result["ok"] == 1.0
```

