# Neo4j Connector - Installation & Setup

## Installation

### Option 1: Install with Sync Support

```bash
pip install db_connections[neo4j-sync]
```

### Option 2: Install with Async Support

```bash
pip install db_connections[neo4j-async]
```

### Option 3: Install with Both Sync and Async

```bash
pip install db_connections[neo4j]
```

## Quick Start

### Synchronous Usage

```python
from db_connections.scr.all_db_connectors.connectors.neo4j import (
    Neo4jSyncConnectionPool,
    Neo4jPoolConfig,
)

# Configure pool
config = Neo4jPoolConfig(
    host="localhost",
    port=7687,
    database="neo4j",
    username="neo4j",
    password="secret",
    min_connections=2,
    max_connections=10,
)

# Use pool
with Neo4jSyncConnectionPool(config) as pool:
    with pool.get_connection() as driver:
        with driver.session(database=config.database) as session:
            # Create a node
            session.run(
                "CREATE (p:Person {name: $name, email: $email})",
                name="John",
                email="john@example.com"
            )
            
            # Find nodes
            result = session.run(
                "MATCH (p:Person {name: $name}) RETURN p",
                name="John"
            )
            record = result.single()
            if record:
                person = record['p']
                print(person)
```

### Asynchronous Usage

```python
from db_connections.scr.all_db_connectors.connectors.neo4j import (
    Neo4jAsyncConnectionPool,
    Neo4jPoolConfig,
)

config = Neo4jPoolConfig(
    host="localhost",
    port=7687,
    database="neo4j",
    username="neo4j",
    password="secret",
)

async with Neo4jAsyncConnectionPool(config) as pool:
    async with pool.get_connection() as driver:
        async with driver.session(database=config.database) as session:
            # Create a node
            await session.run(
                "CREATE (p:Person {name: $name, email: $email})",
                name="Jane",
                email="jane@example.com"
            )
            
            # Find nodes
            result = await session.run(
                "MATCH (p:Person {name: $name}) RETURN p",
                name="Jane"
            )
            record = await result.single()
            if record:
                person = record['p']
                print(person)
```

## Configuration Options

### Basic Configuration

```python
config = Neo4jPoolConfig(
    # Connection details
    host="localhost",
    port=7687,
    database="neo4j",
    username="neo4j",
    password="secret",
    
    # Pool sizing
    min_connections=2,
    max_connections=10,
    max_overflow=5,
    
    # Timeouts (in seconds)
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

### Neo4j-Specific Configuration

```python
config = Neo4jPoolConfig(
    host="localhost",
    port=7687,
    database="neo4j",
    
    # Authentication
    username="neo4j",
    password="secret",
    
    # SSL/TLS
    encrypted=True,
    trust="TRUST_ALL_CERTIFICATES",  # or "TRUST_SYSTEM_CA_SIGNED_CERTIFICATES"
    trusted_certificate="/path/to/cert.pem",
    
    # Connection scheme
    use_neo4j_scheme=False,  # Use bolt:// instead of neo4j://
    use_bolt=True,           # Use Bolt protocol
    use_http=False,          # Use HTTP protocol
    
    # Routing (for Neo4j clusters)
    routing=True,            # Enable routing
    routing_context={"region": "us-east"},
    
    # Connection behavior
    connection_acquisition_timeout=30,
    max_connection_lifetime=3600,
    
    # Retry settings
    max_retries=3,
    retry_delay=1.0,
    retry_backoff=2.0,
)
```

### Environment Variables

```bash
export NEO4J_HOST=localhost
export NEO4J_PORT=7687
export NEO4J_DATABASE=neo4j
export NEO4J_USERNAME=neo4j
export NEO4J_PASSWORD=secret
export NEO4J_ENCRYPTED=true
export NEO4J_CONNECTION_URL=bolt://neo4j:secret@localhost:7687/neo4j
```

```python
config = Neo4jPoolConfig.from_env()
```

### Connection String (URI)

```python
# Standard Bolt connection
uri = "bolt://neo4j:secret@localhost:7687/neo4j"

# Encrypted Bolt connection
uri = "bolt+s://neo4j:secret@localhost:7687/neo4j"

# Neo4j routing (for clusters)
uri = "neo4j://neo4j:secret@localhost:7687/neo4j"

# HTTP connection
uri = "http://neo4j:secret@localhost:7474/neo4j"

config = Neo4jPoolConfig.from_url(uri)
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
- Transaction support

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

### ✅ Neo4j-Specific Features
- Bolt and HTTP protocol support
- Neo4j routing (cluster support)
- SSL/TLS configuration
- Connection string (URI) support
- Cypher query execution

## Health Monitoring

```python
from db_connections.scr.all_db_connectors.connectors.neo4j.health import (
    Neo4jHealthChecker
)

# Check pool health
health_checker = Neo4jHealthChecker(pool)
pool_health = health_checker.check_pool()
print(f"State: {pool_health.state.value}")
print(f"Message: {pool_health.message}")
print(f"Response time: {pool_health.response_time_ms}ms")

# Check database health
db_health = health_checker.check_database()
print(f"Database health: {db_health.state.value}")
print(f"Response time: {db_health.response_time_ms}ms")

# Check server info
with pool.get_connection() as driver:
    server_info = health_checker.check_server_info(driver)
    print(f"Server info: {server_info}")
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
with pool.get_connection() as driver:
    with driver.session(database=config.database) as session:
        result = session.run("MATCH (n) RETURN count(n)")

# Bad
driver = pool.get_connection()  # Not returned automatically!
session = driver.session()
result = session.run("MATCH (n) RETURN count(n)")
```

### 2. Configure Pool Size Appropriately

```python
# For web applications
config = Neo4jPoolConfig(
    min_connections=5,      # Keep some connections warm
    max_connections=50,     # Allow more concurrent connections
)

# For background workers
config = Neo4jPoolConfig(
    min_connections=2,
    max_connections=10,
)
```

### 3. Enable Health Checks

```python
config = Neo4jPoolConfig(
    validate_on_checkout=True,  # Validate before use
    pre_ping=True,              # Ping before queries
)
```

### 4. Configure Connection Lifecycle

```python
config = Neo4jPoolConfig(
    max_lifetime=1800,       # Recycle after 30 minutes
    idle_timeout=300,        # Close idle after 5 minutes
)
```

### 5. Use Appropriate Timeouts

```python
config = Neo4jPoolConfig(
    timeout=30,              # 30 seconds for queries
    connection_timeout=10,   # 10 seconds to connect
)
```

### 6. Configure for Clusters

```python
config = Neo4jPoolConfig(
    use_neo4j_scheme=True,   # Use neo4j:// for routing
    routing=True,            # Enable routing
    routing_context={"region": "us-east"},
)
```

### 7. Use Connection Strings for Complex Setups

```python
# Neo4j Aura (Cloud)
uri = "neo4j+s://neo4j:password@xxxxx.databases.neo4j.io/neo4j"

# Local cluster
uri = "neo4j://neo4j:password@localhost:7687/neo4j"

config = Neo4jPoolConfig.from_url(uri)
```

## Error Handling

```python
from db_connections.scr.all_db_connectors.core.exceptions import (
    ConnectionError,
    PoolTimeoutError,
    PoolExhaustedError,
)
from db_connections.scr.all_db_connectors.connectors.neo4j.exceptions import (
    Neo4jConnectionError,
)

try:
    with pool.get_connection() as driver:
        with driver.session(database=config.database) as session:
            result = session.run("MATCH (n) RETURN count(n)")
except PoolTimeoutError:
    print("No connections available")
except PoolExhaustedError:
    print("Pool exhausted")
except ConnectionError:
    print("Failed to connect to Neo4j")
except Neo4jConnectionError as e:
    print(f"Neo4j connection error: {e}")
except Exception as e:
    print(f"Operation failed: {e}")
```

## FastAPI Integration Example

```python
from fastapi import FastAPI, Depends
from db_connections.scr.all_db_connectors.connectors.neo4j import (
    Neo4jAsyncConnectionPool,
    Neo4jPoolConfig,
)

app = FastAPI()

# Initialize pool on startup
@app.on_event("startup")
async def startup():
    config = Neo4jPoolConfig.from_env()
    app.state.db_pool = Neo4jAsyncConnectionPool(config)
    await app.state.db_pool.initialize_pool()

@app.on_event("shutdown")
async def shutdown():
    await app.state.db_pool.close_all_connections()

# Dependency
async def get_db():
    async with app.state.db_pool.get_connection() as driver:
        async with driver.session(database=app.state.db_pool.config.database) as session:
            yield session

# Use in endpoints
@app.get("/people")
async def get_people(session = Depends(get_db)):
    result = await session.run("MATCH (p:Person) RETURN p LIMIT 10")
    records = await result.values()
    return [dict(record[0]) for record in records]

@app.post("/people")
async def create_person(person: dict, session = Depends(get_db)):
    result = await session.run(
        "CREATE (p:Person {name: $name, email: $email}) RETURN p",
        name=person["name"],
        email=person["email"]
    )
    record = await result.single()
    return dict(record['p'])
```

## Common Operations

### Create Nodes

```python
# Single node
session.run(
    "CREATE (p:Person {name: $name, age: $age})",
    name="John",
    age=30
)

# Multiple nodes
session.run("""
    CREATE (p1:Person {name: 'Alice', age: 25})
    CREATE (p2:Person {name: 'Bob', age: 35})
""")
```

### Find Nodes

```python
# Find one
result = session.run(
    "MATCH (p:Person {name: $name}) RETURN p",
    name="John"
)
record = result.single()

# Find many
result = session.run("MATCH (p:Person) WHERE p.age > $age RETURN p", age=30)
for record in result:
    person = record['p']
    print(person)
```

### Update Nodes

```python
# Update one
session.run(
    "MATCH (p:Person {name: $name}) SET p.age = $age",
    name="John",
    age=31
)

# Update many
session.run(
    "MATCH (p:Person) WHERE p.age < $age SET p.age = p.age + 1",
    age=30
)
```

### Delete Nodes

```python
# Delete one
session.run("MATCH (p:Person {name: $name}) DELETE p", name="John")

# Delete many
session.run("MATCH (p:Person) WHERE p.age < $age DELETE p", age=18)
```

### Create Relationships

```python
session.run("""
    MATCH (a:Person {name: $name1}), (b:Person {name: $name2})
    CREATE (a)-[:FRIENDS]->(b)
""", name1="Alice", name2="Bob")
```

### Query Relationships

```python
# Find friends
result = session.run("""
    MATCH (p:Person {name: $name})-[:FRIENDS]->(friend)
    RETURN friend.name as friend_name
""", name="Alice")

# Find path
result = session.run("""
    MATCH path = shortestPath(
        (start:Person {name: $start})-[:FRIENDS*]-(end:Person {name: $end})
    )
    RETURN length(path) as path_length
""", start="Alice", end="Charlie")
```

## Transactions

```python
# Sync transaction
with driver.session(database=config.database) as session:
    with session.begin_transaction() as tx:
        tx.run("CREATE (p:Person {name: $name})", name="Alice")
        tx.run("CREATE (p:Person {name: $name})", name="Bob")
        tx.commit()  # or tx.rollback() on error

# Async transaction
async with driver.session(database=config.database) as session:
    async with session.begin_transaction() as tx:
        await tx.run("CREATE (p:Person {name: $name})", name="Alice")
        await tx.run("CREATE (p:Person {name: $name})", name="Bob")
        await tx.commit()  # or await tx.rollback() on error
```

## Troubleshooting

### Connection Pool Exhausted

```python
# Increase pool size
config = Neo4jPoolConfig(
    max_connections=100,  # More connections
    max_overflow=20,     # More overflow
)
```

### Slow Operations

```python
# Increase timeout
config = Neo4jPoolConfig(
    timeout=60,              # 60 seconds for queries
    connection_timeout=30,   # 30 seconds to connect
)
```

### Connection Validation Failures

```python
# Enable more aggressive validation
config = Neo4jPoolConfig(
    pre_ping=True,
    validate_on_checkout=True,
)
```

### SSL/TLS Issues

```python
# Configure SSL properly
config = Neo4jPoolConfig(
    encrypted=True,
    trust="TRUST_ALL_CERTIFICATES",  # For development
    # trust="TRUST_SYSTEM_CA_SIGNED_CERTIFICATES",  # For production
    trusted_certificate="/path/to/cert.pem",
)
```

## Testing

```python
# Test with pytest
import pytest
from db_connections.scr.all_db_connectors.connectors.neo4j import (
    Neo4jSyncConnectionPool,
    Neo4jPoolConfig,
)

@pytest.fixture
def db_pool():
    config = Neo4jPoolConfig(
        host="localhost",
        port=7687,
        database="test_db",
        username="neo4j",
        password="test_pass",
    )
    pool = Neo4jSyncConnectionPool(config)
    yield pool
    pool.close_all_connections()

def test_connection(db_pool):
    with db_pool.get_connection() as driver:
        with driver.session(database=db_pool.config.database) as session:
            result = session.run("RETURN 1 as test")
            record = result.single()
            assert record['test'] == 1
```

