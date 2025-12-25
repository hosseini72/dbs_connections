"""
ClickHouse Connection Pool Usage Examples

This file demonstrates various ways to use the ClickHouse connection pools,
including basic queries, data insertion, batch operations, and advanced features.
"""

# Try to import clickhouse-connect
try:
    import clickhouse_connect
    CLICKHOUSE_AVAILABLE = True
except ImportError:
    CLICKHOUSE_AVAILABLE = False
    clickhouse_connect = None

from db_connections.scr.all_db_connectors.connectors.clickhouse import (
    ClickHouseSyncConnectionPool,
    ClickHouseAsyncConnectionPool,
    ClickHousePoolConfig,
    ClickHouseHealthChecker,
)


# =============================================================================
# Example 1: Basic Synchronous Usage
# =============================================================================

def example_sync_basic():
    """Basic synchronous usage with context manager."""
    print("\n=== Example 1: Basic Sync Usage ===")
    
    config = ClickHousePoolConfig(
        host="localhost",
        port=9000,
        database="default",
        username="default",
        password="",
        min_connections=2,
        max_connections=10,
    )
    
    # Using context manager (recommended)
    with ClickHouseSyncConnectionPool(config) as pool:
        with pool.get_connection() as client:
            # Execute a simple query
            result = client.query("SELECT 1 as value")
            print(f"Query result: {result.result_rows}")
            
            # Get server info
            server_info = client.get_server_info()
            print(f"Server version: {server_info.get('version_display', 'unknown')}")


# =============================================================================
# Example 2: Creating Tables and Inserting Data
# =============================================================================

def example_create_table_and_insert():
    """Create a table and insert data."""
    print("\n=== Example 2: Create Table and Insert Data ===")
    
    config = ClickHousePoolConfig(
        host="localhost",
        port=9000,
        database="default",
    )
    
    with ClickHouseSyncConnectionPool(config) as pool:
        with pool.get_connection() as client:
            # Create a table
            client.command("""
                CREATE TABLE IF NOT EXISTS users (
                    id UInt32,
                    name String,
                    email String,
                    created_at DateTime DEFAULT now()
                ) ENGINE = MergeTree()
                ORDER BY id
            """)
            print("Created table: users")
            
            # Insert a single row
            client.insert(
                "users",
                [[1, "John Doe", "john@example.com"]],
                column_names=["id", "name", "email"]
            )
            print("Inserted 1 row")
            
            # Insert multiple rows
            data = [
                [2, "Jane Smith", "jane@example.com"],
                [3, "Bob Johnson", "bob@example.com"],
            ]
            client.insert("users", data, column_names=["id", "name", "email"])
            print(f"Inserted {len(data)} rows")
            
            # Query the data
            result = client.query("SELECT * FROM users ORDER BY id")
            print(f"Total rows: {len(result.result_rows)}")
            for row in result.result_rows:
                print(f"  {row}")


# =============================================================================
# Example 3: Querying Data
# =============================================================================

def example_query_data():
    """Various query examples."""
    print("\n=== Example 3: Querying Data ===")
    
    config = ClickHousePoolConfig(host="localhost", port=9000)
    
    with ClickHouseSyncConnectionPool(config) as pool:
        with pool.get_connection() as client:
            # Simple SELECT query
            result = client.query("SELECT count() FROM system.tables")
            print(f"Number of tables: {result.result_rows[0][0]}")
            
            # Query with parameters
            result = client.query(
                "SELECT name, type FROM system.databases WHERE name = {db:String}",
                parameters={"db": "default"}
            )
            print(f"Database info: {result.result_rows}")
            
            # Query with settings
            result = client.query(
                "SELECT * FROM system.processes",
                settings={"max_execution_time": 10}
            )
            print(f"Active processes: {len(result.result_rows)}")
            
            # Iterate over large result sets
            result = client.query("SELECT number FROM system.numbers LIMIT 1000")
            count = 0
            for row in result.result_rows:
                count += 1
            print(f"Iterated over {count} rows")


# =============================================================================
# Example 4: Batch Insert Operations
# =============================================================================

def example_batch_insert():
    """Batch insert operations."""
    print("\n=== Example 4: Batch Insert Operations ===")
    
    config = ClickHousePoolConfig(host="localhost", port=9000)
    
    with ClickHouseSyncConnectionPool(config) as pool:
        with pool.get_connection() as client:
            # Create table for batch insert
            client.command("""
                CREATE TABLE IF NOT EXISTS events (
                    event_id UInt64,
                    event_type String,
                    timestamp DateTime,
                    data String
                ) ENGINE = MergeTree()
                ORDER BY (event_type, timestamp)
            """)
            
            # Prepare batch data
            batch_size = 1000
            events = []
            for i in range(batch_size):
                events.append([
                    i,
                    "click" if i % 2 == 0 else "view",
                    "2024-01-01 00:00:00",
                    f"event_data_{i}"
                ])
            
            # Insert batch
            client.insert(
                "events",
                events,
                column_names=["event_id", "event_type", "timestamp", "data"]
            )
            print(f"Inserted {batch_size} events in batch")
            
            # Verify
            result = client.query("SELECT count() FROM events")
            print(f"Total events in table: {result.result_rows[0][0]}")


# =============================================================================
# Example 5: Using Connection URL
# =============================================================================

def example_connection_url():
    """Using connection URL instead of individual parameters."""
    print("\n=== Example 5: Connection URL ===")
    
    # Using connection URL
    config = ClickHousePoolConfig(
        connection_url="clickhouse://default@localhost:9000/default"
    )
    
    with ClickHouseSyncConnectionPool(config) as pool:
        with pool.get_connection() as client:
            result = client.query("SELECT 1")
            print("Connected using URL")


# =============================================================================
# Example 6: SSL/TLS Connection
# =============================================================================

def example_ssl_connection():
    """Using SSL/TLS connection."""
    print("\n=== Example 6: SSL/TLS Connection ===")
    
    config = ClickHousePoolConfig(
        host="clickhouse.example.com",
        port=9440,  # SSL port
        database="default",
        secure=True,
        verify=True,
        ca_certs="/path/to/ca.crt",
        # Optional client certificate
        # cert="/path/to/client.crt",
        # key="/path/to/client.key",
    )
    
    try:
        with ClickHouseSyncConnectionPool(config) as pool:
            with pool.get_connection() as client:
                result = client.query("SELECT 1")
                print("Connected via SSL")
    except Exception as e:
        print(f"SSL connection failed: {e}")


# =============================================================================
# Example 7: Health Checks
# =============================================================================

def example_health_checks():
    """Health check examples."""
    print("\n=== Example 7: Health Checks ===")
    
    config = ClickHousePoolConfig(host="localhost", port=9000)
    
    with ClickHouseSyncConnectionPool(config) as pool:
        pool.initialize_pool()
        
        # Pool health check
        health = pool.health_check()
        print(f"Pool health: {health.state}")
        print(f"Message: {health.message}")
        
        # Database health check
        db_health = pool.database_health_check()
        print(f"Database health: {db_health.state}")
        print(f"Response time: {db_health.response_time_ms}ms")
        
        # Get pool metrics
        metrics = pool.get_metrics()
        print(f"Active connections: {metrics.active_connections}")
        print(f"Idle connections: {metrics.idle_connections}")
        print(f"Total connections: {metrics.total_connections}")


# =============================================================================
# Example 8: Pool Status and Monitoring
# =============================================================================

def example_pool_monitoring():
    """Pool status and monitoring."""
    print("\n=== Example 8: Pool Monitoring ===")
    
    config = ClickHousePoolConfig(
        host="localhost",
        port=9000,
        min_connections=2,
        max_connections=10,
    )
    
    pool = ClickHouseSyncConnectionPool(config)
    pool.initialize_pool()
    
    # Get pool status
    status = pool.pool_status()
    print(f"Pool initialized: {status['initialized']}")
    print(f"Total connections: {status['total_connections']}")
    print(f"Active connections: {status['active_connections']}")
    print(f"Idle connections: {status['idle_connections']}")
    print(f"Max connections: {status['max_connections']}")
    
    # Use connections
    with pool.get_connection() as client:
        client.query("SELECT 1")
        status = pool.pool_status()
        print(f"After use - Active: {status['active_connections']}")
    
    # Final status
    status = pool.pool_status()
    print(f"After release - Active: {status['active_connections']}")
    
    pool.close_all_connections()


# =============================================================================
# Example 9: Error Handling and Retries
# =============================================================================

def example_error_handling():
    """Error handling and retry logic."""
    print("\n=== Example 9: Error Handling ===")
    
    config = ClickHousePoolConfig(
        host="localhost",
        port=9000,
        max_retries=3,
        retry_delay=1.0,
        retry_backoff=2.0,
    )
    
    try:
        pool = ClickHouseSyncConnectionPool(config)
        with pool.get_connection() as client:
            # This will retry if connection fails
            result = client.query("SELECT 1")
            print("Query succeeded")
    except Exception as e:
        print(f"Query failed after retries: {e}")


# =============================================================================
# Example 10: Async Usage
# =============================================================================

async def example_async_basic():
    """Basic asynchronous usage."""
    print("\n=== Example 10: Async Usage ===")
    
    config = ClickHousePoolConfig(
        host="localhost",
        port=9000,
        database="default",
    )
    
    async with ClickHouseAsyncConnectionPool(config) as pool:
        async with pool.get_connection() as client:
            # Execute async query
            result = await client.query("SELECT 1 as value")
            print(f"Async query result: {result.result_rows}")
            
            # Async insert
            await client.insert(
                "users",
                [[4, "Async User", "async@example.com"]],
                column_names=["id", "name", "email"]
            )
            print("Async insert completed")


# =============================================================================
# Example 11: Concurrent Async Operations
# =============================================================================

async def example_async_concurrent():
    """Concurrent async operations."""
    print("\n=== Example 11: Concurrent Async Operations ===")
    
    import asyncio
    
    config = ClickHousePoolConfig(host="localhost", port=9000)
    
    async with ClickHouseAsyncConnectionPool(config) as pool:
        async def query_task(task_id):
            async with pool.get_connection() as client:
                result = await client.query(f"SELECT {task_id} as task_id")
                return result.result_rows[0][0]
        
        # Run multiple queries concurrently
        tasks = [query_task(i) for i in range(5)]
        results = await asyncio.gather(*tasks)
        print(f"Concurrent query results: {results}")


# =============================================================================
# Example 12: Using Settings and Query Parameters
# =============================================================================

def example_settings_and_params():
    """Using query settings and parameters."""
    print("\n=== Example 12: Settings and Parameters ===")
    
    config = ClickHousePoolConfig(
        host="localhost",
        port=9000,
        settings={
            "max_execution_time": 300,
            "max_memory_usage": 10000000000,
        }
    )
    
    with ClickHouseSyncConnectionPool(config) as pool:
        with pool.get_connection() as client:
            # Query with parameters
            result = client.query(
                """
                SELECT 
                    name,
                    type,
                    engine
                FROM system.tables
                WHERE database = {db:String}
                LIMIT {limit:UInt32}
                """,
                parameters={
                    "db": "default",
                    "limit": 10
                }
            )
            print(f"Found {len(result.result_rows)} tables")
            
            # Query with custom settings
            result = client.query(
                "SELECT * FROM system.processes",
                settings={"max_execution_time": 5}
            )
            print(f"Active processes: {len(result.result_rows)}")


# =============================================================================
# Example 13: Lazy Initialization
# =============================================================================

def example_lazy_initialization():
    """Lazy pool initialization."""
    print("\n=== Example 13: Lazy Initialization ===")
    
    config = ClickHousePoolConfig(host="localhost", port=9000)
    
    # Pool is not initialized yet
    pool = ClickHouseSyncConnectionPool(config)
    print(f"Pool initialized: {pool._initialized}")
    
    # First connection triggers initialization
    with pool.get_connection() as client:
        print(f"Pool initialized: {pool._initialized}")
        result = client.query("SELECT 1")
        print("Query executed")


# =============================================================================
# Example 14: Connection Validation
# =============================================================================

def example_connection_validation():
    """Connection validation on checkout."""
    print("\n=== Example 14: Connection Validation ===")
    
    config = ClickHousePoolConfig(
        host="localhost",
        port=9000,
        validate_on_checkout=True,
        pre_ping=True,
    )
    
    with ClickHouseSyncConnectionPool(config) as pool:
        # Connections are validated before use
        with pool.get_connection() as client:
            result = client.query("SELECT 1")
            print("Connection validated and used")


# =============================================================================
# Example 15: Environment Variables Configuration
# =============================================================================

def example_env_config():
    """Configuration from environment variables."""
    print("\n=== Example 15: Environment Variables ===")
    
    import os
    
    # Set environment variables
    os.environ["CLICKHOUSE_HOST"] = "localhost"
    os.environ["CLICKHOUSE_PORT"] = "9000"
    os.environ["CLICKHOUSE_DATABASE"] = "default"
    os.environ["CLICKHOUSE_USERNAME"] = "default"
    os.environ["CLICKHOUSE_PASSWORD"] = ""
    
    # Create config from environment
    config = ClickHousePoolConfig.from_env()
    
    with ClickHouseSyncConnectionPool(config) as pool:
        with pool.get_connection() as client:
            result = client.query("SELECT 1")
            print("Connected using environment variables")


# =============================================================================
# Main Execution
# =============================================================================

if __name__ == "__main__":
    if not CLICKHOUSE_AVAILABLE:
        print("clickhouse-connect is not installed.")
        print("Install it with: pip install clickhouse-connect")
        exit(1)
    
    # Run sync examples
    try:
        example_sync_basic()
        example_create_table_and_insert()
        example_query_data()
        example_batch_insert()
        example_connection_url()
        example_health_checks()
        example_pool_monitoring()
        example_error_handling()
        example_settings_and_params()
        example_lazy_initialization()
        example_connection_validation()
    except Exception as e:
        print(f"Error in sync examples: {e}")
    
    # Run async examples
    try:
        import asyncio
        asyncio.run(example_async_basic())
        asyncio.run(example_async_concurrent())
    except Exception as e:
        print(f"Error in async examples: {e}")
    
    print("\n=== All Examples Completed ===")

