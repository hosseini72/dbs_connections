"""
Redis Connection Pool Usage Examples

This file demonstrates various ways to use the Redis connection pools.
"""

import asyncio
from db_connections.scr.all_db_connectors.connectors.redis import (
    RedisSyncConnectionPool,
    RedisAsyncConnectionPool,
    RedisPoolConfig,
)


# =============================================================================
# Example 1: Basic Synchronous Usage
# =============================================================================

def example_sync_basic():
    """Basic synchronous usage with context manager."""
    print("\n=== Example 1: Basic Sync Usage ===")
    
    config = RedisPoolConfig(
        host="localhost",
        port=6379,
        db=0,
        password=None,
        min_size=2,
        max_size=10,
    )
    
    # Using context manager (recommended)
    with RedisSyncConnectionPool(config) as pool:
        with pool.get_connection() as conn:
            # Test connection
            result = conn.ping()
            print(f"Ping result: {result}")
            
            # Set a value
            conn.set("example:key", "Hello, Redis!")
            
            # Get a value
            value = conn.get("example:key")
            if value:
                print(f"Retrieved value: {value.decode() if isinstance(value, bytes) else value}")


# =============================================================================
# Example 2: Synchronous with Multiple Operations
# =============================================================================

def example_sync_operations():
    """Synchronous usage with various Redis operations."""
    print("\n=== Example 2: Sync Operations ===")
    
    config = RedisPoolConfig(
        host="localhost",
        port=6379,
        db=0,
    )
    
    pool = RedisSyncConnectionPool(config)
    
    try:
        with pool.get_connection() as conn:
            # String operations
            conn.set("user:1:name", "Alice")
            conn.set("user:1:email", "alice@example.com", ex=3600)  # Expire in 1 hour
            
            name = conn.get("user:1:name")
            print(f"User name: {name.decode() if isinstance(name, bytes) else name}")
            
            # Hash operations
            conn.hset("user:2", mapping={
                "name": "Bob",
                "email": "bob@example.com",
                "age": "30"
            })
            user_data = conn.hgetall("user:2")
            print(f"User data: {user_data}")
            
            # List operations
            conn.lpush("tasks", "task1", "task2", "task3")
            tasks = conn.lrange("tasks", 0, -1)
            print(f"Tasks: {tasks}")
            
            # Set operations
            conn.sadd("tags", "python", "redis", "database")
            tags = conn.smembers("tags")
            print(f"Tags: {tags}")
    
    finally:
        pool.close_all_connections()


# =============================================================================
# Example 3: Lazy vs Eager Initialization
# =============================================================================

def example_initialization():
    """Demonstrate lazy and eager initialization."""
    print("\n=== Example 3: Initialization Strategies ===")
    
    config = RedisPoolConfig(
        host="localhost",
        port=6379,
        db=0,
    )
    
    # Lazy initialization (pool created on first connection)
    pool_lazy = RedisSyncConnectionPool(config)
    print(f"Lazy pool initialized: {pool_lazy._initialized}")
    
    with pool_lazy.get_connection() as conn:
        conn.ping()
        print(f"After first connection: {pool_lazy._initialized}")
    
    pool_lazy.close_all_connections()
    
    # Eager initialization (pool created immediately)
    pool_eager = RedisSyncConnectionPool(config)
    pool_eager.initialize_pool()
    print(f"Eager pool initialized: {pool_eager._initialized}")
    pool_eager.close_all_connections()


# =============================================================================
# Example 4: Health Checks and Metrics
# =============================================================================

def example_health_and_metrics():
    """Monitor pool health and metrics."""
    print("\n=== Example 4: Health & Metrics ===")
    
    config = RedisPoolConfig(
        host="localhost",
        port=6379,
        db=0,
    )
    
    with RedisSyncConnectionPool(config) as pool:
        # Get pool status
        status = pool.pool_status()
        print(f"Pool status: {status}")
        
        # Get metrics
        metrics = pool.get_metrics()
        print(f"Pool metrics: {metrics}")
        
        # Health check
        health = pool.health_check()
        print(f"Pool health: {health.state.value} - {health.message}")
        
        # Database health check
        db_health = pool.database_health_check()
        print(f"Database health: {db_health.state.value} - {db_health.message}")
        print(f"Response time: {db_health.response_time_ms:.2f}ms")
        if db_health.details:
            print(f"Redis version: {db_health.details.get('redis_version')}")
            print(f"Memory usage: {db_health.details.get('used_memory_mb')} MB")


# =============================================================================
# Example 5: Basic Asynchronous Usage
# =============================================================================

async def example_async_basic():
    """Basic asynchronous usage."""
    print("\n=== Example 5: Basic Async Usage ===")
    
    config = RedisPoolConfig(
        host="localhost",
        port=6379,
        db=0,
        min_size=2,
        max_size=10,
    )
    
    async with RedisAsyncConnectionPool(config) as pool:
        async with pool.get_connection() as conn:
            # Test connection
            result = await conn.ping()
            print(f"Ping result: {result}")
            
            # Set a value
            await conn.set("async:key", "Hello, Async Redis!")
            
            # Get a value
            value = await conn.get("async:key")
            if value:
                print(f"Retrieved value: {value.decode() if isinstance(value, bytes) else value}")


# =============================================================================
# Example 6: Async Batch Operations
# =============================================================================

async def example_async_batch():
    """Asynchronous batch operations."""
    print("\n=== Example 6: Async Batch Operations ===")
    
    config = RedisPoolConfig(
        host="localhost",
        port=6379,
        db=0,
    )
    
    async with RedisAsyncConnectionPool(config) as pool:
        async with pool.get_connection() as conn:
            # Batch set operations
            pipeline = conn.pipeline()
            for i in range(10):
                pipeline.set(f"key:{i}", f"value:{i}")
            await pipeline.execute()
            print("Inserted 10 keys via pipeline")
            
            # Batch get operations
            keys = [f"key:{i}" for i in range(10)]
            values = await conn.mget(keys)
            print(f"Retrieved {len(values)} values")


# =============================================================================
# Example 7: Async with Pub/Sub
# =============================================================================

async def example_async_pubsub():
    """Asynchronous pub/sub example."""
    print("\n=== Example 7: Async Pub/Sub ===")
    
    config = RedisPoolConfig(
        host="localhost",
        port=6379,
        db=0,
    )
    
    async with RedisAsyncConnectionPool(config) as pool:
        async with pool.get_connection() as conn:
            pubsub = conn.pubsub()
            await pubsub.subscribe("mychannel")
            
            # Publish a message
            await conn.publish("mychannel", "Hello, subscribers!")
            
            # Receive messages (non-blocking example)
            message = await pubsub.get_message(timeout=1.0)
            if message:
                print(f"Received: {message}")
            
            await pubsub.unsubscribe("mychannel")
            await pubsub.close()


# =============================================================================
# Example 8: Concurrent Async Operations
# =============================================================================

async def example_async_concurrent():
    """Multiple concurrent async operations."""
    print("\n=== Example 8: Concurrent Async Operations ===")
    
    config = RedisPoolConfig(
        host="localhost",
        port=6379,
        db=0,
        max_size=20,
    )
    
    async def set_value(pool, key, value):
        """Set a single value."""
        async with pool.get_connection() as conn:
            await conn.set(key, value)
            return await conn.get(key)
    
    async with RedisAsyncConnectionPool(config) as pool:
        # Set 10 values concurrently
        tasks = [
            set_value(pool, f"concurrent:key:{i}", f"value:{i}")
            for i in range(10)
        ]
        results = await asyncio.gather(*tasks)
        print(f"Set {len(results)} values concurrently")
        
        # Check pool metrics
        metrics = await pool.get_metrics()
        print(f"Peak active connections: {metrics.active_connections}")


# =============================================================================
# Example 9: Configuration from Environment
# =============================================================================

def example_env_config():
    """Load configuration from environment variables."""
    print("\n=== Example 9: Environment Config ===")
    
    # Set environment variables:
    # export REDIS_HOST=localhost
    # export REDIS_PORT=6379
    # export REDIS_DB=0
    # export REDIS_PASSWORD=secret
    # export REDIS_URL=redis://localhost:6379/0
    
    config = RedisPoolConfig.from_env()
    print(f"Config from env: {config.host}:{config.port}/{config.db}")
    
    # Use custom prefix
    # export CACHE_HOST=localhost
    # export CACHE_PORT=6379
    config_custom = RedisPoolConfig.from_env(prefix="CACHE_")
    print(f"Config with custom prefix: {config_custom.host}")


# =============================================================================
# Example 10: Connection URL
# =============================================================================

def example_connection_url():
    """Use connection URL."""
    print("\n=== Example 10: Connection URL ===")
    
    url = "redis://localhost:6379/0"
    config = RedisPoolConfig.from_url(url)
    
    with RedisSyncConnectionPool(config) as pool:
        with pool.get_connection() as conn:
            info = conn.info("server")
            version = info.get("redis_version", "unknown")
            print(f"Redis version: {version}")
            
            # Test with password
            url_with_auth = "redis://:password@localhost:6379/0"
            config_auth = RedisPoolConfig.from_url(url_with_auth)


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
    
    config = RedisPoolConfig(
        host="localhost",
        port=6379,
        db=0,
        timeout=5,  # 5 second timeout
        max_retries=3,
    )
    
    try:
        with RedisSyncConnectionPool(config) as pool:
            with pool.get_connection() as conn:
                # This will fail if Redis is not running
                result = conn.ping()
                print(f"Connection successful: {result}")
    
    except ConnectionError as e:
        print(f"Connection error: {e}")
    except PoolTimeoutError as e:
        print(f"Pool timeout: {e}")
    except Exception as e:
        print(f"Operation error: {e}")


# =============================================================================
# Example 12: Decode Responses
# =============================================================================

def example_decode_responses():
    """Use decode_responses for automatic string conversion."""
    print("\n=== Example 12: Decode Responses ===")
    
    config = RedisPoolConfig(
        host="localhost",
        port=6379,
        db=0,
        decode_responses=True,  # Automatically decode bytes to strings
        encoding="utf-8",
    )
    
    with RedisSyncConnectionPool(config) as pool:
        with pool.get_connection() as conn:
            conn.set("string_key", "Hello, World!")
            value = conn.get("string_key")
            # value is already a string, no need to decode
            print(f"Value type: {type(value)}, Value: {value}")


# =============================================================================
# Example 13: SSL/TLS Configuration
# =============================================================================

def example_ssl_config():
    """Configure SSL/TLS connection."""
    print("\n=== Example 13: SSL/TLS Config ===")
    
    config = RedisPoolConfig(
        host="localhost",
        port=6380,  # Usually Redis with SSL uses different port
        db=0,
        ssl=True,
        ssl_cert_reqs="required",
        ssl_ca_certs="/path/to/ca.crt",
        ssl_certfile="/path/to/client.crt",
        ssl_keyfile="/path/to/client.key",
    )
    
    # Or use rediss:// URL
    url = "rediss://localhost:6380/0"
    config_ssl = RedisPoolConfig.from_url(url)


# =============================================================================
# Example 14: Retry and Timeout Configuration
# =============================================================================

def example_retry_config():
    """Configure retry logic and timeouts."""
    print("\n=== Example 14: Retry & Timeout Config ===")
    
    config = RedisPoolConfig(
        host="localhost",
        port=6379,
        db=0,
        timeout=10,  # Socket timeout in seconds
        socket_timeout=5,  # Specific socket timeout
        socket_connect_timeout=3,  # Connection timeout
        retry_on_timeout=True,  # Retry on timeout
        max_retries=3,  # Maximum retry attempts
        retry_delay=1.0,  # Delay between retries
        retry_backoff=2.0,  # Exponential backoff multiplier
    )
    
    print("Retry and timeout configured")


# =============================================================================
# Main Execution
# =============================================================================

if __name__ == "__main__":
    print("Redis Connection Pool Examples")
    print("=" * 60)
    
    # Sync examples
    try:
        example_sync_basic()
        example_sync_operations()
        example_initialization()
        example_health_and_metrics()
        example_env_config()
        example_connection_url()
        example_error_handling()
        example_decode_responses()
        example_ssl_config()
        example_retry_config()
    except Exception as e:
        print(f"Sync example error: {e}")
    
    # Async examples
    print("\n" + "=" * 60)
    print("Async Examples")
    print("=" * 60)
    
    try:
        asyncio.run(example_async_basic())
        asyncio.run(example_async_batch())
        asyncio.run(example_async_pubsub())
        asyncio.run(example_async_concurrent())
    except Exception as e:
        print(f"Async example error: {e}")

