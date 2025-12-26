"""
PostgreSQL Connection Pool Usage Examples

This file demonstrates various ways to use the PostgreSQL connection pools.
"""

import asyncio
from db_connections.scr.all_db_connectors.connectors.postgres import (
    PostgresConnectionPool,
    AsyncPostgresConnectionPool,
    PostgresPoolConfig,
)


# =============================================================================
# Example 1: Basic Synchronous Usage
# =============================================================================


def example_sync_basic():
    """Basic synchronous usage with context manager."""
    print("\n=== Example 1: Basic Sync Usage ===")

    config = PostgresPoolConfig(
        host="localhost",
        port=5432,
        database="mydb",
        user="postgres",
        password="password",
        min_size=2,
        max_size=10,
    )

    # Using context manager (recommended)
    with PostgresConnectionPool(config) as pool:
        with pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT version()")
            version = cursor.fetchone()
            print(f"PostgreSQL version: {version[0]}")
            cursor.close()


# =============================================================================
# Example 2: Synchronous with Transaction
# =============================================================================


def example_sync_transaction():
    """Synchronous usage with transaction management."""
    print("\n=== Example 2: Sync Transaction ===")

    config = PostgresPoolConfig.from_env(prefix="DB_")

    pool = PostgresConnectionPool(config)

    try:
        with pool.get_connection() as conn:
            # Start transaction
            conn.autocommit = False
            cursor = conn.cursor()

            try:
                # Execute multiple queries
                cursor.execute(
                    "INSERT INTO users (name, email) VALUES (%s, %s)",
                    ("John Doe", "john@example.com"),
                )
                cursor.execute(
                    "INSERT INTO profiles (user_id, bio) VALUES (%s, %s)",
                    (1, "Software Engineer"),
                )

                # Commit transaction
                conn.commit()
                print("Transaction committed successfully")

            except Exception as e:
                # Rollback on error
                conn.rollback()
                print(f"Transaction rolled back: {e}")
            finally:
                cursor.close()

    finally:
        pool.close_all_connections()


# =============================================================================
# Example 3: Lazy vs Eager Initialization
# =============================================================================


def example_initialization():
    """Demonstrate lazy and eager initialization."""
    print("\n=== Example 3: Initialization Strategies ===")

    config = PostgresPoolConfig(
        host="localhost",
        database="mydb",
        user="postgres",
        password="password",
    )

    # Lazy initialization (pool created on first connection)
    pool_lazy = PostgresConnectionPool(config)
    print(f"Lazy pool initialized: {pool_lazy._initialized}")

    with pool_lazy.get_connection() as conn:
        print(f"After first connection: {pool_lazy._initialized}")

    pool_lazy.close_all_connections()

    # Eager initialization (pool created immediately)
    pool_eager = PostgresConnectionPool(config)
    pool_eager.initialize_pool()
    print(f"Eager pool initialized: {pool_eager._initialized}")
    pool_eager.close_all_connections()


# =============================================================================
# Example 4: Health Checks and Metrics
# =============================================================================


def example_health_and_metrics():
    """Monitor pool health and metrics."""
    print("\n=== Example 4: Health & Metrics ===")

    config = PostgresPoolConfig(
        host="localhost",
        database="mydb",
        user="postgres",
        password="password",
    )

    with PostgresConnectionPool(config) as pool:
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
            print(f"Details: {db_health.details}")


# =============================================================================
# Example 5: Basic Asynchronous Usage
# =============================================================================


async def example_async_basic():
    """Basic asynchronous usage."""
    print("\n=== Example 5: Basic Async Usage ===")

    config = PostgresPoolConfig(
        host="localhost",
        database="mydb",
        user="postgres",
        password="password",
        min_size=2,
        max_size=10,
    )

    async with AsyncPostgresConnectionPool(config) as pool:
        async with pool.get_connection() as conn:
            version = await conn.fetchval("SELECT version()")
            print(f"PostgreSQL version: {version}")


# =============================================================================
# Example 6: Async Batch Operations
# =============================================================================


async def example_async_batch():
    """Asynchronous batch operations."""
    print("\n=== Example 6: Async Batch Operations ===")

    config = PostgresPoolConfig(
        host="localhost",
        database="mydb",
        user="postgres",
        password="password",
    )

    async with AsyncPostgresConnectionPool(config) as pool:
        async with pool.get_connection() as conn:
            # Fetch multiple rows
            users = await conn.fetch("SELECT id, name, email FROM users LIMIT 10")
            print(f"Fetched {len(users)} users")

            # Execute batch insert
            data = [
                ("Alice", "alice@example.com"),
                ("Bob", "bob@example.com"),
                ("Charlie", "charlie@example.com"),
            ]

            await conn.executemany(
                "INSERT INTO users (name, email) VALUES ($1, $2)", data
            )
            print(f"Inserted {len(data)} users")


# =============================================================================
# Example 7: Async Transaction
# =============================================================================


async def example_async_transaction():
    """Asynchronous transaction management."""
    print("\n=== Example 7: Async Transaction ===")

    config = PostgresPoolConfig(
        host="localhost",
        database="mydb",
        user="postgres",
        password="password",
    )

    async with AsyncPostgresConnectionPool(config) as pool:
        async with pool.get_connection() as conn:
            # Use asyncpg transaction
            async with conn.transaction():
                try:
                    await conn.execute(
                        "INSERT INTO users (name, email) VALUES ($1, $2)",
                        "Jane Doe",
                        "jane@example.com",
                    )
                    await conn.execute(
                        "INSERT INTO profiles (user_id, bio) VALUES ($1, $2)",
                        1,
                        "Data Scientist",
                    )
                    print("Async transaction committed")
                except Exception as e:
                    print(f"Transaction failed: {e}")
                    raise


# =============================================================================
# Example 8: Concurrent Async Operations
# =============================================================================


async def example_async_concurrent():
    """Multiple concurrent async operations."""
    print("\n=== Example 8: Concurrent Async Operations ===")

    config = PostgresPoolConfig(
        host="localhost",
        database="mydb",
        user="postgres",
        password="password",
        max_size=20,
    )

    async def fetch_user(pool, user_id):
        """Fetch a single user."""
        async with pool.get_connection() as conn:
            user = await conn.fetchrow("SELECT * FROM users WHERE id = $1", user_id)
            return user

    async with AsyncPostgresConnectionPool(config) as pool:
        # Fetch 10 users concurrently
        tasks = [fetch_user(pool, i) for i in range(1, 11)]
        users = await asyncio.gather(*tasks)
        print(f"Fetched {len(users)} users concurrently")

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
    # export POSTGRES_HOST=localhost
    # export POSTGRES_DATABASE=mydb
    # export POSTGRES_USER=postgres
    # export POSTGRES_PASSWORD=password

    config = PostgresPoolConfig.from_env()
    print(f"Config from env: {config.host}:{config.port}/{config.database}")

    # Use custom prefix
    # export DB_HOST=localhost
    # export DB_DATABASE=mydb
    config_custom = PostgresPoolConfig.from_env(prefix="DB_")
    print(f"Config with custom prefix: {config_custom.host}")


# =============================================================================
# Example 10: Connection String (DSN)
# =============================================================================


def example_dsn():
    """Use connection string (DSN)."""
    print("\n=== Example 10: Connection String ===")

    dsn = "postgresql://postgres:password@localhost:5432/mydb?sslmode=require"
    config = PostgresPoolConfig.from_dsn(dsn)

    with PostgresConnectionPool(config) as pool:
        with pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT current_database()")
            db_name = cursor.fetchone()[0]
            print(f"Connected to database: {db_name}")
            cursor.close()


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

    config = PostgresPoolConfig(
        host="localhost",
        database="mydb",
        user="postgres",
        password="password",
        timeout=5,  # 5 second timeout
        max_retries=3,
    )

    try:
        with PostgresConnectionPool(config) as pool:
            with pool.get_connection() as conn:
                cursor = conn.cursor()
                # This will fail if table doesn't exist
                cursor.execute("SELECT * FROM non_existent_table")

    except ConnectionError as e:
        print(f"Connection error: {e}")
    except PoolTimeoutError as e:
        print(f"Pool timeout: {e}")
    except Exception as e:
        print(f"Query error: {e}")


# =============================================================================
# Example 12: Custom Server Settings
# =============================================================================


def example_server_settings():
    """Configure server settings."""
    print("\n=== Example 12: Server Settings ===")

    config = PostgresPoolConfig(
        host="localhost",
        database="mydb",
        user="postgres",
        password="password",
        server_settings={
            "timezone": "UTC",
            "search_path": "public,custom_schema",
            "statement_timeout": "30000",  # 30 seconds
        },
        application_name="MyMicroservice",
    )

    with PostgresConnectionPool(config) as pool:
        with pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SHOW timezone")
            timezone = cursor.fetchone()[0]
            print(f"Timezone: {timezone}")

            cursor.execute("SHOW application_name")
            app_name = cursor.fetchone()[0]
            print(f"Application name: {app_name}")
            cursor.close()


# =============================================================================
# Main Execution
# =============================================================================

if __name__ == "__main__":
    print("PostgreSQL Connection Pool Examples")
    print("=" * 60)

    # Sync examples
    try:
        example_sync_basic()
        # example_sync_transaction()  # Uncomment if you have tables
        example_initialization()
        example_health_and_metrics()
        example_env_config()
        example_dsn()
        example_error_handling()
        example_server_settings()
    except Exception as e:
        print(f"Sync example error: {e}")

    # Async examples
    print("\n" + "=" * 60)
    print("Async Examples")
    print("=" * 60)

    try:
        asyncio.run(example_async_basic())
        # asyncio.run(example_async_batch())  # Uncomment if you have tables
        # asyncio.run(example_async_transaction())  # Uncomment if you have tables
        # asyncio.run(example_async_concurrent())  # Uncomment if you have tables
    except Exception as e:
        print(f"Async example error: {e}")
