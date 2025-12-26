"""
Neo4j Connection Pool Usage Examples

This file demonstrates various ways to use the Neo4j connection pools.
"""

import asyncio
from db_connections.scr.all_db_connectors.connectors.neo4j import (
    Neo4jSyncConnectionPool,
    Neo4jAsyncConnectionPool,
    Neo4jPoolConfig,
)


# =============================================================================
# Example 1: Basic Synchronous Usage
# =============================================================================


def example_sync_basic():
    """Basic synchronous usage with context manager."""
    print("\n=== Example 1: Basic Sync Usage ===")

    config = Neo4jPoolConfig(
        host="localhost",
        port=7687,
        database="neo4j",
        username="neo4j",
        password="password",
        min_connections=2,
        max_connections=10,
    )

    # Using context manager (recommended)
    with Neo4jSyncConnectionPool(config) as pool:
        with pool.get_connection() as driver:
            # Create a session
            with driver.session(database=config.database) as session:
                # Run a simple query
                result = session.run("RETURN 1 as test")
                record = result.single()
                print(f"Test query result: {record['test']}")

                # Create a node
                session.run(
                    "CREATE (p:Person {name: $name, email: $email})",
                    name="John Doe",
                    email="john@example.com",
                )
                print("Created person node")

                # Find nodes
                result = session.run(
                    "MATCH (p:Person {name: $name}) RETURN p", name="John Doe"
                )
                record = result.single()
                if record:
                    person = record["p"]
                    print(f"Found person: {person['name']}, {person['email']}")


# =============================================================================
# Example 2: Synchronous with Transactions
# =============================================================================


def example_sync_transaction():
    """Synchronous usage with transaction management."""
    print("\n=== Example 2: Sync Transaction ===")

    config = Neo4jPoolConfig(
        host="localhost",
        port=7687,
        database="neo4j",
        username="neo4j",
        password="password",
    )

    pool = Neo4jSyncConnectionPool(config)

    try:
        with pool.get_connection() as driver:
            # Start a transaction
            with driver.session(database=config.database) as session:
                with session.begin_transaction() as tx:
                    try:
                        # Create multiple nodes in transaction
                        tx.run(
                            "CREATE (p1:Person {name: $name1, email: $email1})",
                            name1="Alice",
                            email1="alice@example.com",
                        )
                        tx.run(
                            "CREATE (p2:Person {name: $name2, email: $email2})",
                            name2="Bob",
                            email2="bob@example.com",
                        )

                        # Create relationship
                        tx.run(
                            """
                            MATCH (p1:Person {name: $name1}), (p2:Person {name: $name2})
                            CREATE (p1)-[:KNOWS]->(p2)
                            """,
                            name1="Alice",
                            name2="Bob",
                        )

                        # Commit transaction
                        tx.commit()
                        print("Transaction committed successfully")

                    except Exception as e:
                        # Rollback on error
                        tx.rollback()
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

    config = Neo4jPoolConfig(
        host="localhost",
        port=7687,
        database="neo4j",
    )

    # Lazy initialization (pool created on first connection)
    pool_lazy = Neo4jSyncConnectionPool(config)
    print(f"Lazy pool initialized: {pool_lazy._initialized}")

    with pool_lazy.get_connection() as driver:
        print(f"After first connection: {pool_lazy._initialized}")

    pool_lazy.close_all_connections()

    # Eager initialization (pool created immediately)
    pool_eager = Neo4jSyncConnectionPool(config)
    pool_eager.initialize_pool()
    print(f"Eager pool initialized: {pool_eager._initialized}")
    with pool_eager.get_connection() as driver:
        pass  # Just to verify connection works
    pool_eager.close_all_connections()


# =============================================================================
# Example 4: Health Checks and Metrics
# =============================================================================


def example_health_and_metrics():
    """Monitor pool health and metrics."""
    print("\n=== Example 4: Health & Metrics ===")

    config = Neo4jPoolConfig(
        host="localhost",
        port=7687,
        database="neo4j",
    )

    with Neo4jSyncConnectionPool(config) as pool:
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
        from db_connections.scr.all_db_connectors.connectors.neo4j.health import (
            Neo4jHealthChecker,
        )

        health_checker = Neo4jHealthChecker(pool)
        db_health = health_checker.check_database()
        print(f"Database health: {db_health.state.value} - {db_health.message}")
        print(f"Response time: {db_health.response_time_ms:.2f}ms")


# =============================================================================
# Example 5: Basic Asynchronous Usage
# =============================================================================


async def example_async_basic():
    """Basic asynchronous usage."""
    print("\n=== Example 5: Basic Async Usage ===")

    config = Neo4jPoolConfig(
        host="localhost",
        port=7687,
        database="neo4j",
        username="neo4j",
        password="password",
        min_connections=2,
        max_connections=10,
    )

    async with Neo4jAsyncConnectionPool(config) as pool:
        async with pool.get_connection() as driver:
            # Create a session
            async with driver.session(database=config.database) as session:
                # Run a simple query
                result = await session.run("RETURN 1 as test")
                record = await result.single()
                print(f"Test query result: {record['test']}")

                # Create a node
                await session.run(
                    "CREATE (p:Person {name: $name, email: $email})",
                    name="Jane Doe",
                    email="jane@example.com",
                )
                print("Created person node")


# =============================================================================
# Example 6: Async Batch Operations
# =============================================================================


async def example_async_batch():
    """Asynchronous batch operations."""
    print("\n=== Example 6: Async Batch Operations ===")

    config = Neo4jPoolConfig(
        host="localhost",
        port=7687,
        database="neo4j",
    )

    async with Neo4jAsyncConnectionPool(config) as pool:
        async with pool.get_connection() as driver:
            async with driver.session(database=config.database) as session:
                # Create multiple nodes
                people = [
                    {"name": "Alice", "email": "alice@example.com"},
                    {"name": "Bob", "email": "bob@example.com"},
                    {"name": "Charlie", "email": "charlie@example.com"},
                ]

                for person in people:
                    await session.run(
                        "CREATE (p:Person {name: $name, email: $email})",
                        name=person["name"],
                        email=person["email"],
                    )

                print(f"Created {len(people)} person nodes")

                # Find multiple nodes
                result = await session.run(
                    "MATCH (p:Person) WHERE p.name IN $names RETURN p",
                    names=["Alice", "Bob"],
                )
                records = await result.values()
                print(f"Found {len(records)} people")


# =============================================================================
# Example 7: Async Transaction
# =============================================================================


async def example_async_transaction():
    """Asynchronous transaction management."""
    print("\n=== Example 7: Async Transaction ===")

    config = Neo4jPoolConfig(
        host="localhost",
        port=7687,
        database="neo4j",
    )

    async with Neo4jAsyncConnectionPool(config) as pool:
        async with pool.get_connection() as driver:
            async with driver.session(database=config.database) as session:
                async with session.begin_transaction() as tx:
                    try:
                        # Create user node
                        await tx.run(
                            "CREATE (u:User {name: $name, email: $email})",
                            name="Jane Doe",
                            email="jane@example.com",
                        )

                        # Create profile node
                        await tx.run(
                            "CREATE (pr:Profile {bio: $bio})", bio="Data Scientist"
                        )

                        # Create relationship
                        await tx.run(
                            """
                            MATCH (u:User {email: $email}), (pr:Profile {bio: $bio})
                            CREATE (u)-[:HAS_PROFILE]->(pr)
                            """,
                            email="jane@example.com",
                            bio="Data Scientist",
                        )

                        # Commit transaction
                        await tx.commit()
                        print("Async transaction committed")
                    except Exception as e:
                        print(f"Transaction failed: {e}")
                        await tx.rollback()
                        raise


# =============================================================================
# Example 8: Concurrent Async Operations
# =============================================================================


async def example_async_concurrent():
    """Multiple concurrent async operations."""
    print("\n=== Example 8: Concurrent Async Operations ===")

    config = Neo4jPoolConfig(
        host="localhost",
        port=7687,
        database="neo4j",
        max_connections=20,
    )

    async def create_person(pool, name, email):
        """Create a single person."""
        async with pool.get_connection() as driver:
            async with driver.session(database=config.database) as session:
                await session.run(
                    "CREATE (p:Person {name: $name, email: $email})",
                    name=name,
                    email=email,
                )
                return name

    async with Neo4jAsyncConnectionPool(config) as pool:
        # Create people concurrently
        tasks = [
            create_person(pool, f"User {i}", f"user{i}@example.com") for i in range(10)
        ]
        names = await asyncio.gather(*tasks)
        print(f"Created {len(names)} people concurrently")

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
    # export NEO4J_HOST=localhost
    # export NEO4J_PORT=7687
    # export NEO4J_DATABASE=neo4j
    # export NEO4J_USERNAME=neo4j
    # export NEO4J_PASSWORD=password

    try:
        config = Neo4jPoolConfig.from_env()
        print(f"Config from env: {config.host}:{config.port}/{config.database}")
    except Exception as e:
        print(f"Could not load from env: {e}")

    # Use custom prefix
    # export DB_HOST=localhost
    # export DB_DATABASE=neo4j
    try:
        config_custom = Neo4jPoolConfig.from_env(prefix="DB_")
        print(f"Config with custom prefix: {config_custom.host}")
    except Exception as e:
        print(f"Could not load with custom prefix: {e}")


# =============================================================================
# Example 10: Connection String (URI)
# =============================================================================


def example_connection_string():
    """Use connection string (URI)."""
    print("\n=== Example 10: Connection String ===")

    # Neo4j connection URI
    uri = "bolt://neo4j:password@localhost:7687/neo4j"
    try:
        config = Neo4jPoolConfig.from_url(uri)

        with Neo4jSyncConnectionPool(config) as pool:
            with pool.get_connection() as driver:
                with driver.session(database=config.database) as session:
                    result = session.run("RETURN 1 as test")
                    record = result.single()
                    print(f"Connected and queried database: {record['test']}")
    except Exception as e:
        print(f"Connection string example error: {e}")


# =============================================================================
# Example 11: Error Handling
# =============================================================================


def example_error_handling():
    """Demonstrate error handling."""
    print("\n=== Example 11: Error Handling ===")

    from db_connections.scr.all_db_connectors.core.exceptions import (
        ConnectionError,
        PoolTimeoutError,
        PoolExhaustedError,
    )

    config = Neo4jPoolConfig(
        host="localhost",
        port=7687,
        database="neo4j",
        timeout=5,  # 5 second timeout
        max_retries=3,
    )

    try:
        with Neo4jSyncConnectionPool(config) as pool:
            with pool.get_connection() as driver:
                with driver.session(database=config.database) as session:
                    result = session.run("MATCH (n) RETURN count(n) as count")
                    record = result.single()
                    print(f"Node count: {record['count']}")

    except ConnectionError as e:
        print(f"Connection error: {e}")
    except PoolTimeoutError as e:
        print(f"Pool timeout: {e}")
    except PoolExhaustedError as e:
        print(f"Pool exhausted: {e}")
    except Exception as e:
        print(f"Operation error: {e}")


# =============================================================================
# Example 12: Complex Cypher Queries
# =============================================================================


def example_complex_queries():
    """Use complex Cypher queries."""
    print("\n=== Example 12: Complex Cypher Queries ===")

    config = Neo4jPoolConfig(
        host="localhost",
        port=7687,
        database="neo4j",
    )

    with Neo4jSyncConnectionPool(config) as pool:
        with pool.get_connection() as driver:
            with driver.session(database=config.database) as session:
                # Create graph structure
                session.run("""
                    CREATE (alice:Person {name: 'Alice', age: 30})
                    CREATE (bob:Person {name: 'Bob', age: 25})
                    CREATE (charlie:Person {name: 'Charlie', age: 35})
                    CREATE (alice)-[:FRIENDS]->(bob)
                    CREATE (bob)-[:FRIENDS]->(charlie)
                """)

                # Find friends of friends
                result = session.run(
                    """
                    MATCH (p:Person {name: $name})-[:FRIENDS]->(friend)-[:FRIENDS]->(fof)
                    RETURN friend.name as friend, fof.name as friend_of_friend
                """,
                    name="Alice",
                )

                print("Friends of friends:")
                for record in result:
                    print(f"  {record['friend']} -> {record['friend_of_friend']}")


# =============================================================================
# Example 13: Graph Algorithms
# =============================================================================


def example_graph_algorithms():
    """Use graph algorithms."""
    print("\n=== Example 13: Graph Algorithms ===")

    config = Neo4jPoolConfig(
        host="localhost",
        port=7687,
        database="neo4j",
    )

    with Neo4jSyncConnectionPool(config) as pool:
        with pool.get_connection() as driver:
            with driver.session(database=config.database) as session:
                # Find shortest path
                result = session.run(
                    """
                    MATCH path = shortestPath(
                        (start:Person {name: $start})-[:FRIENDS*]-(end:Person {name: $end})
                    )
                    RETURN length(path) as path_length
                """,
                    start="Alice",
                    end="Charlie",
                )

                record = result.single()
                if record:
                    print(f"Shortest path length: {record['path_length']}")


# =============================================================================
# Example 14: Indexes and Constraints
# =============================================================================


def example_indexes():
    """Create and use indexes."""
    print("\n=== Example 14: Indexes ===")

    config = Neo4jPoolConfig(
        host="localhost",
        port=7687,
        database="neo4j",
    )

    with Neo4jSyncConnectionPool(config) as pool:
        with pool.get_connection() as driver:
            with driver.session(database=config.database) as session:
                # Create index
                session.run(
                    "CREATE INDEX person_name IF NOT EXISTS FOR (p:Person) ON (p.name)"
                )
                print("Index created on Person.name")

                # Create unique constraint
                session.run(
                    "CREATE CONSTRAINT person_email_unique IF NOT EXISTS "
                    "FOR (p:Person) REQUIRE p.email IS UNIQUE"
                )
                print("Unique constraint created on Person.email")


# =============================================================================
# Main Execution
# =============================================================================

if __name__ == "__main__":
    print("Neo4j Connection Pool Examples")
    print("=" * 60)

    # Sync examples
    try:
        example_sync_basic()
        example_sync_transaction()
        example_initialization()
        example_health_and_metrics()
        example_env_config()
        example_connection_string()
        example_error_handling()
        example_complex_queries()
        example_graph_algorithms()
        example_indexes()
    except Exception as e:
        print(f"Sync example error: {e}")

    # Async examples
    print("\n" + "=" * 60)
    print("Async Examples")
    print("=" * 60)

    try:
        asyncio.run(example_async_basic())
        asyncio.run(example_async_batch())
        asyncio.run(example_async_transaction())
        asyncio.run(example_async_concurrent())
    except Exception as e:
        print(f"Async example error: {e}")
