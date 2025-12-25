"""
MongoDB Connection Pool Usage Examples

This file demonstrates various ways to use the MongoDB connection pools.
"""

import asyncio
from db_connections.scr.all_db_connectors.connectors.mongodb import (
    MongoSyncConnectionPool,
    MongoAsyncConnectionPool,
    MongoPoolConfig,
)


# =============================================================================
# Example 1: Basic Synchronous Usage
# =============================================================================

def example_sync_basic():
    """Basic synchronous usage with context manager."""
    print("\n=== Example 1: Basic Sync Usage ===")
    
    config = MongoPoolConfig(
        host="localhost",
        port=27017,
        database="mydb",
        username="admin",
        password="password",
        min_size=2,
        max_size=10,
    )
    
    # Using context manager (recommended)
    with MongoSyncConnectionPool(config) as pool:
        with pool.get_connection() as client:
            db = client[config.database]
            # Test connection
            result = db.command("ping")
            print(f"Ping result: {result}")
            
            # Access a collection
            collection = db["users"]
            # Insert a document
            doc = {"name": "John Doe", "email": "john@example.com"}
            result = collection.insert_one(doc)
            print(f"Inserted document ID: {result.inserted_id}")
            
            # Find documents
            user = collection.find_one({"name": "John Doe"})
            print(f"Found user: {user}")


# =============================================================================
# Example 2: Synchronous with Transactions
# =============================================================================

def example_sync_transaction():
    """Synchronous usage with transaction management."""
    print("\n=== Example 2: Sync Transaction ===")
    
    config = MongoPoolConfig.from_env(prefix="MONGODB_")
    
    pool = MongoSyncConnectionPool(config)
    
    try:
        with pool.get_connection() as client:
            db = client[config.database]
            collection = db["users"]
            
            # Start a session for transaction
            with client.start_session() as session:
                with session.start_transaction():
                    try:
                        # Insert multiple documents in transaction
                        collection.insert_one(
                            {"name": "Alice", "email": "alice@example.com"},
                            session=session
                        )
                        collection.insert_one(
                            {"name": "Bob", "email": "bob@example.com"},
                            session=session
                        )
                        
                        # Commit transaction (auto-commits when exiting context)
                        print("Transaction committed successfully")
                        
                    except Exception as e:
                        # Rollback on error (auto-rollback on exception)
                        session.abort_transaction()
                        print(f"Transaction rolled back: {e}")
                        raise
    
    finally:
        pool.close_all_connections()


# =============================================================================
# Example 3: Lazy vs Eager Initialization
# =============================================================================

def example_initialization():
    """Demonstrate lazy and eager initialization."""
    print("\n=== Example 3: Initialization Strategies ===")
    
    config = MongoPoolConfig(
        host="localhost",
        port=27017,
        database="mydb",
    )
    
    # Lazy initialization (pool created on first connection)
    pool_lazy = MongoSyncConnectionPool(config)
    print(f"Lazy pool initialized: {pool_lazy._initialized}")
    
    with pool_lazy.get_connection() as client:
        print(f"After first connection: {pool_lazy._initialized}")
    
    pool_lazy.close_all_connections()
    
    # Eager initialization (pool created immediately)
    pool_eager = MongoSyncConnectionPool(config)
    pool_eager.initialize_pool()
    print(f"Eager pool initialized: {pool_eager._initialized}")
    with pool_eager.get_connection() as client:
        pass  # Just to verify connection works
    pool_eager.close_all_connections()


# =============================================================================
# Example 4: Health Checks and Metrics
# =============================================================================

def example_health_and_metrics():
    """Monitor pool health and metrics."""
    print("\n=== Example 4: Health & Metrics ===")
    
    config = MongoPoolConfig(
        host="localhost",
        port=27017,
        database="mydb",
    )
    
    with MongoSyncConnectionPool(config) as pool:
        # Get pool status
        status = pool.pool_status()
        print(f"Pool status: {status}")
        
        # Get metrics
        metrics = pool.get_metrics()
        print(f"Pool metrics: {metrics}")
        
        # Health check
        health = pool.health_check()
        print(f"Pool health: {health.state.value} - {health.message}")
        
        # Database health check (using health checker)
        from db_connections.scr.all_db_connectors.connectors.mongodb.health import (  # noqa: E501
            MongoHealthChecker
        )
        health_checker = MongoHealthChecker(pool)
        db_health = health_checker.check_database()
        print(
            f"Database health: {db_health.state.value} - "
            f"{db_health.message}"
        )
        print(f"Response time: {db_health.response_time_ms:.2f}ms")
        if db_health.details:
            version = db_health.details.get('server_version')
            print(f"Server version: {version}")


# =============================================================================
# Example 5: Basic Asynchronous Usage
# =============================================================================

async def example_async_basic():
    """Basic asynchronous usage."""
    print("\n=== Example 5: Basic Async Usage ===")
    
    config = MongoPoolConfig(
        host="localhost",
        port=27017,
        database="mydb",
        username="admin",
        password="password",
        min_size=2,
        max_size=10,
    )
    
    async with MongoAsyncConnectionPool(config) as pool:
        async with pool.get_connection() as client:
            db = client[config.database]
            # Test connection
            result = await db.command("ping")
            print(f"Ping result: {result}")
            
            # Access a collection
            collection = db["users"]
            # Insert a document
            doc = {"name": "Jane Doe", "email": "jane@example.com"}
            result = await collection.insert_one(doc)
            print(f"Inserted document ID: {result.inserted_id}")


# =============================================================================
# Example 6: Async Batch Operations
# =============================================================================

async def example_async_batch():
    """Asynchronous batch operations."""
    print("\n=== Example 6: Async Batch Operations ===")
    
    config = MongoPoolConfig(
        host="localhost",
        port=27017,
        database="mydb",
    )
    
    async with MongoAsyncConnectionPool(config) as pool:
        async with pool.get_connection() as client:
            db = client[config.database]
            collection = db["users"]
            
            # Insert multiple documents
            documents = [
                {"name": "Alice", "email": "alice@example.com"},
                {"name": "Bob", "email": "bob@example.com"},
                {"name": "Charlie", "email": "charlie@example.com"},
            ]
            
            result = await collection.insert_many(documents)
            print(f"Inserted {len(result.inserted_ids)} documents")
            
            # Find multiple documents
            cursor = collection.find({"name": {"$in": ["Alice", "Bob"]}})
            users = await cursor.to_list(length=100)
            print(f"Found {len(users)} users")


# =============================================================================
# Example 7: Async Transaction
# =============================================================================

async def example_async_transaction():
    """Asynchronous transaction management."""
    print("\n=== Example 7: Async Transaction ===")
    
    config = MongoPoolConfig(
        host="localhost",
        port=27017,
        database="mydb",
    )
    
    async with MongoAsyncConnectionPool(config) as pool:
        async with pool.get_connection() as client:
            db = client[config.database]
            users_collection = db["users"]
            profiles_collection = db["profiles"]
            
            # Start a session for transaction
            async with await client.start_session() as session:
                async with session.start_transaction():
                    try:
                        # Insert user
                        user_result = await users_collection.insert_one(
                            {"name": "Jane Doe", "email": "jane@example.com"},
                            session=session
                        )
                        user_id = user_result.inserted_id
                        
                        # Insert profile
                        await profiles_collection.insert_one(
                            {"user_id": user_id, "bio": "Data Scientist"},
                            session=session
                        )
                        
                        print("Async transaction committed")
                    except Exception as e:
                        print(f"Transaction failed: {e}")
                        await session.abort_transaction()
                        raise


# =============================================================================
# Example 8: Concurrent Async Operations
# =============================================================================

async def example_async_concurrent():
    """Multiple concurrent async operations."""
    print("\n=== Example 8: Concurrent Async Operations ===")
    
    config = MongoPoolConfig(
        host="localhost",
        port=27017,
        database="mydb",
        max_size=20,
    )
    
    async def fetch_user(pool, user_id):
        """Fetch a single user."""
        async with pool.get_connection() as client:
            db = client[config.database]
            collection = db["users"]
            user = await collection.find_one({"_id": user_id})
            return user
    
    async with MongoAsyncConnectionPool(config) as pool:
        # Create some test users
        async with pool.get_connection() as client:
            db = client[config.database]
            collection = db["users"]
            result = await collection.insert_many([
                {"name": f"User {i}"} for i in range(10)
            ])
            user_ids = result.inserted_ids
        
        # Fetch users concurrently
        tasks = [fetch_user(pool, user_id) for user_id in user_ids[:10]]
        users = await asyncio.gather(*tasks)
        print(f"Fetched {len([u for u in users if u])} users concurrently")
        
        # Check pool status
        status = await pool.pool_status()
        print(f"Peak active connections: {status.get('active_connections')}")


# =============================================================================
# Example 9: Configuration from Environment
# =============================================================================

def example_env_config():
    """Load configuration from environment variables."""
    print("\n=== Example 9: Environment Config ===")
    
    # Set environment variables:
    # export MONGODB_HOST=localhost
    # export MONGODB_PORT=27017
    # export MONGODB_DATABASE=mydb
    # export MONGODB_USERNAME=admin
    # export MONGODB_PASSWORD=password
    
    config = MongoPoolConfig.from_env()
    print(f"Config from env: {config.host}:{config.port}/{config.database}")
    
    # Use custom prefix
    # export DB_HOST=localhost
    # export DB_DATABASE=mydb
    config_custom = MongoPoolConfig.from_env(prefix="DB_")
    print(f"Config with custom prefix: {config_custom.host}")


# =============================================================================
# Example 10: Connection String (URI)
# =============================================================================

def example_connection_string():
    """Use connection string (URI)."""
    print("\n=== Example 10: Connection String ===")
    
    # MongoDB connection URI
    uri = "mongodb://admin:password@localhost:27017/mydb?authSource=admin"
    config = MongoPoolConfig.from_uri(uri)
    
    with MongoSyncConnectionPool(config) as pool:
        with pool.get_connection() as client:
            db = client[config.database]
            result = db.command("ping")
            print(f"Connected and pinged database: {result}")
            
            # List collections
            collections = db.list_collection_names()
            print(f"Collections: {collections}")


# =============================================================================
# Example 11: Error Handling
# =============================================================================

def example_error_handling():
    """Demonstrate error handling."""
    print("\n=== Example 11: Error Handling ===")
    
    from db_connections.scr.all_db_connectors.core.exceptions import (
        ConnectionError,
        PoolTimeoutError,
    )
    
    config = MongoPoolConfig(
        host="localhost",
        port=27017,
        database="mydb",
        timeout=5,  # 5 second timeout
        max_retries=3,
    )
    
    try:
        with MongoSyncConnectionPool(config) as pool:
            with pool.get_connection() as client:
                db = client[config.database]
                collection = db["nonexistent_collection"]
                # This will work (MongoDB creates collections on first write)
                result = collection.find_one({"invalid": "query"})
                print(f"Query result: {result}")
    
    except ConnectionError as e:
        print(f"Connection error: {e}")
    except PoolTimeoutError as e:
        print(f"Pool timeout: {e}")
    except Exception as e:
        print(f"Operation error: {e}")


# =============================================================================
# Example 12: Replica Set and SSL Configuration
# =============================================================================

def example_replica_set_ssl():
    """Configure replica set and SSL."""
    print("\n=== Example 12: Replica Set & SSL ===")
    
    config = MongoPoolConfig(
        host="mongodb.example.com",
        port=27017,
        database="mydb",
        username="admin",
        password="password",
        replica_set="myReplicaSet",
        read_preference="secondaryPreferred",  # Read from secondary if available
        tls=True,
        tls_ca_file="/path/to/ca.pem",
        tls_certificate_key_file="/path/to/client.pem",
        tls_allow_invalid_certificates=False,
    )
    
    with MongoSyncConnectionPool(config) as pool:
        with pool.get_connection() as client:
            # Check replica set status
            admin_db = client.admin
            try:
                status = admin_db.command("replSetGetStatus")
                replica_set_name = status.get('set')
                print(f"Replica set status: {replica_set_name}")
            except Exception as e:
                print(f"Not a replica set or error: {e}")


# =============================================================================
# Example 13: Aggregation Pipeline
# =============================================================================

def example_aggregation():
    """Use aggregation pipelines."""
    print("\n=== Example 13: Aggregation Pipeline ===")
    
    config = MongoPoolConfig(
        host="localhost",
        port=27017,
        database="mydb",
    )
    
    with MongoSyncConnectionPool(config) as pool:
        with pool.get_connection() as client:
            db = client[config.database]
            collection = db["orders"]
            
            # Aggregation pipeline
            pipeline = [
                {"$match": {"status": "completed"}},
                {"$group": {
                    "_id": "$customer_id",
                    "total": {"$sum": "$amount"}
                }},
                {"$sort": {"total": -1}},
                {"$limit": 10}
            ]
            
            results = collection.aggregate(pipeline)
            for result in results:
                print(f"Customer {result['_id']}: ${result['total']}")


# =============================================================================
# Example 14: Indexes
# =============================================================================

def example_indexes():
    """Create and use indexes."""
    print("\n=== Example 14: Indexes ===")
    
    config = MongoPoolConfig(
        host="localhost",
        port=27017,
        database="mydb",
    )
    
    with MongoSyncConnectionPool(config) as pool:
        with pool.get_connection() as client:
            db = client[config.database]
            collection = db["users"]
            
            # Create indexes
            collection.create_index("email", unique=True)
            collection.create_index([("name", 1), ("age", -1)])
            print("Indexes created")
            
            # List indexes
            indexes = collection.list_indexes()
            for index in indexes:
                print(f"Index: {index}")


# =============================================================================
# Main Execution
# =============================================================================

if __name__ == "__main__":
    print("MongoDB Connection Pool Examples")
    print("=" * 60)
    
    # Sync examples
    try:
        example_sync_basic()
        # example_sync_transaction()  # Uncomment if MongoDB has transactions
        example_initialization()
        example_health_and_metrics()
        example_env_config()
        example_connection_string()
        example_error_handling()
        # example_replica_set_ssl()  # Uncomment if you have replica set
        # example_aggregation()  # Uncomment if you have orders collection
        # example_indexes()  # Uncomment if you have users collection
    except Exception as e:
        print(f"Sync example error: {e}")
    
    # Async examples
    print("\n" + "=" * 60)
    print("Async Examples")
    print("=" * 60)
    
    try:
        asyncio.run(example_async_basic())
        # asyncio.run(example_async_batch())  # Uncomment if you have users
        # asyncio.run(example_async_transaction())  # Uncomment if transactions
        # asyncio.run(example_async_concurrent())  # Uncomment if you have users
    except Exception as e:
        print(f"Async example error: {e}")

