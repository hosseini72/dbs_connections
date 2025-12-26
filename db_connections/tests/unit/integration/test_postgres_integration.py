"""Integration tests for PostgreSQL connector.

These tests require a running PostgreSQL instance.
Use docker-compose to start test database:

    docker-compose -f tests/unit/integration/docker-compose.yml up -d

Run integration tests:
    python -m unittest discover -s tests/unit/integration -p "test_*.py" -v
"""

import sys
from pathlib import Path
import os
import asyncio
import unittest

# Add parent directory to path to allow imports
# test_postgres_integration.py is at: db_connections/tests/unit/integration/
# We need to go up 5 levels to get to the parent of db_connections
try:
    _file_path = Path(__file__).resolve()
except NameError:
    # __file__ might not be defined in some contexts
    _file_path = (
        Path(os.getcwd())
        / "tests"
        / "unit"
        / "integration"
        / "test_postgres_integration.py"
    )

parent_dir = _file_path.parent.parent.parent.parent.parent
parent_dir_str = str(parent_dir)
if parent_dir_str not in sys.path:
    sys.path.insert(0, parent_dir_str)

from db_connections.scr.all_db_connectors.connectors.postgres import (  # noqa: E402
    PostgresConnectionPool,
    AsyncPostgresConnectionPool,
    PostgresPoolConfig,
)
from db_connections.scr.all_db_connectors.core.health import HealthState  # noqa: E402


def get_test_config() -> PostgresPoolConfig:
    """Get test database configuration from environment."""
    return PostgresPoolConfig(
        host=os.getenv("TEST_POSTGRES_HOST", "localhost"),
        port=int(os.getenv("TEST_POSTGRES_PORT", "5432")),
        database=os.getenv("TEST_POSTGRES_DATABASE", "test_db"),
        user=os.getenv("TEST_POSTGRES_USER", "test_user"),
        password=os.getenv("TEST_POSTGRES_PASSWORD", "test_password"),
        min_size=2,
        max_size=10,
    )


def is_postgres_available() -> bool:
    """Check if PostgreSQL is available for testing."""
    config = get_test_config()

    try:
        import psycopg2

        conn = psycopg2.connect(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            password=config.password,
            connect_timeout=5,
        )
        conn.close()
        return True
    except Exception:
        return False


class TestPostgresConnectionPoolIntegration(unittest.TestCase):
    """Integration tests for sync PostgreSQL pool."""

    @classmethod
    def setUpClass(cls):
        """Set up test class."""
        cls.test_config = get_test_config()
        if not is_postgres_available():
            raise unittest.SkipTest("PostgreSQL not available for integration tests")

    def test_pool_lifecycle(self):
        """Test complete pool lifecycle."""
        # Create pool
        pool = PostgresConnectionPool(self.test_config)

        # Initialize
        pool.initialize_pool()
        self.assertTrue(pool._initialized)

        # Get connection
        with pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 as test")
            result = cursor.fetchone()
            self.assertEqual(result[0], 1)
            cursor.close()

        # Close pool
        pool.close_all_connections()
        self.assertTrue(pool._closed)

    def test_context_manager(self):
        """Test pool context manager."""
        with PostgresConnectionPool(self.test_config) as pool:
            with pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT version()")
                version = cursor.fetchone()
                self.assertIsNotNone(version)
                self.assertIn("PostgreSQL", version[0])
                cursor.close()

    def test_multiple_connections(self):
        """Test acquiring multiple connections."""
        with PostgresConnectionPool(self.test_config) as pool:
            # Get multiple connections
            with pool.get_connection() as conn1:
                with pool.get_connection() as conn2:
                    cursor1 = conn1.cursor()
                    cursor2 = conn2.cursor()

                    cursor1.execute("SELECT 1")
                    cursor2.execute("SELECT 2")

                    self.assertEqual(cursor1.fetchone()[0], 1)
                    self.assertEqual(cursor2.fetchone()[0], 2)

                    cursor1.close()
                    cursor2.close()

    def test_connection_validation(self):
        """Test connection validation."""
        config = PostgresPoolConfig(
            host=self.test_config.host,
            port=self.test_config.port,
            database=self.test_config.database,
            user=self.test_config.user,
            password=self.test_config.password,
            validate_on_checkout=True,
            pre_ping=True,
        )

        with PostgresConnectionPool(config) as pool:
            with pool.get_connection() as conn:
                # Connection should be validated
                self.assertTrue(pool.validate_connection(conn))

    def test_health_checks(self):
        """Test health check functionality."""
        with PostgresConnectionPool(self.test_config) as pool:
            # Pool health
            health = pool.health_check()
            self.assertEqual(health.state, HealthState.HEALTHY)
            self.assertIsNotNone(health.response_time_ms)

            # Database health
            db_health = pool.database_health_check()
            self.assertEqual(db_health.state, HealthState.HEALTHY)
            self.assertIn("server_version", db_health.details)

    def test_pool_metrics(self):
        """Test metrics collection."""
        with PostgresConnectionPool(self.test_config) as pool:
            with pool.get_connection() as conn:
                # Get metrics while connection is active
                metrics = pool.get_metrics()

                self.assertGreater(metrics.total_connections, 0)
                self.assertGreater(metrics.active_connections, 0)
                self.assertEqual(
                    metrics.max_connections,
                    self.test_config.max_size + self.test_config.max_overflow,
                )

    def test_transaction_commit(self):
        """Test transaction commit."""
        with PostgresConnectionPool(self.test_config) as pool:
            with pool.get_connection() as conn:
                conn.autocommit = False
                cursor = conn.cursor()

                try:
                    # Create temp table
                    cursor.execute("""
                        CREATE TEMP TABLE test_table (
                            id SERIAL PRIMARY KEY,
                            name VARCHAR(100)
                        )
                    """)

                    # Insert data
                    cursor.execute(
                        "INSERT INTO test_table (name) VALUES (%s)", ("test_name",)
                    )

                    # Commit
                    conn.commit()

                    # Verify
                    cursor.execute("SELECT name FROM test_table")
                    result = cursor.fetchone()
                    self.assertEqual(result[0], "test_name")

                finally:
                    cursor.close()

    def test_transaction_rollback(self):
        """Test transaction rollback."""
        with PostgresConnectionPool(self.test_config) as pool:
            with pool.get_connection() as conn:
                conn.autocommit = False
                cursor = conn.cursor()

                try:
                    # Create temp table
                    cursor.execute("""
                        CREATE TEMP TABLE test_table (
                            id SERIAL PRIMARY KEY,
                            name VARCHAR(100)
                        )
                    """)
                    conn.commit()

                    # Insert data
                    cursor.execute(
                        "INSERT INTO test_table (name) VALUES (%s)", ("test_name",)
                    )

                    # Rollback
                    conn.rollback()

                    # Verify nothing was inserted
                    cursor.execute("SELECT COUNT(*) FROM test_table")
                    count = cursor.fetchone()[0]
                    self.assertEqual(count, 0)

                finally:
                    cursor.close()


class TestAsyncPostgresConnectionPoolIntegration(unittest.IsolatedAsyncioTestCase):
    """Integration tests for async PostgreSQL pool."""

    @classmethod
    def setUpClass(cls):
        """Set up test class."""
        cls.test_config = get_test_config()
        if not is_postgres_available():
            raise unittest.SkipTest("PostgreSQL not available for integration tests")

    async def test_async_pool_lifecycle(self):
        """Test complete async pool lifecycle."""
        # Create pool
        pool = AsyncPostgresConnectionPool(self.test_config)

        # Initialize
        await pool.initialize_pool()
        self.assertTrue(pool._initialized)

        # Get connection
        async with pool.get_connection() as conn:
            result = await conn.fetchval("SELECT 1")
            self.assertEqual(result, 1)

        # Close pool
        await pool.close_all_connections()
        self.assertTrue(pool._closed)

    async def test_async_context_manager(self):
        """Test async pool context manager."""
        async with AsyncPostgresConnectionPool(self.test_config) as pool:
            async with pool.get_connection() as conn:
                version = await conn.fetchval("SELECT version()")
                self.assertIsNotNone(version)
                self.assertIn("PostgreSQL", version)

    async def test_async_multiple_connections(self):
        """Test acquiring multiple async connections."""
        async with AsyncPostgresConnectionPool(self.test_config) as pool:
            async with pool.get_connection() as conn1:
                async with pool.get_connection() as conn2:
                    result1 = await conn1.fetchval("SELECT 1")
                    result2 = await conn2.fetchval("SELECT 2")

                    self.assertEqual(result1, 1)
                    self.assertEqual(result2, 2)

    async def test_async_concurrent_queries(self):
        """Test concurrent query execution."""
        async with AsyncPostgresConnectionPool(self.test_config) as pool:

            async def execute_query(value):
                async with pool.get_connection() as conn:
                    return await conn.fetchval(f"SELECT {value}")

            # Execute 5 queries concurrently
            results = await asyncio.gather(*[execute_query(i) for i in range(1, 6)])

            self.assertEqual(results, [1, 2, 3, 4, 5])

    async def test_async_health_checks(self):
        """Test async health check functionality."""
        async with AsyncPostgresConnectionPool(self.test_config) as pool:
            # Pool health
            health = await pool.health_check()
            self.assertEqual(health.state, HealthState.HEALTHY)

            # Database health
            db_health = await pool.database_health_check()
            self.assertEqual(db_health.state, HealthState.HEALTHY)
            self.assertIn("server_version", db_health.details)

    async def test_async_transaction_commit(self):
        """Test async transaction commit."""
        async with AsyncPostgresConnectionPool(self.test_config) as pool:
            async with pool.get_connection() as conn:
                async with conn.transaction():
                    # Create temp table
                    await conn.execute("""
                        CREATE TEMP TABLE test_table (
                            id SERIAL PRIMARY KEY,
                            name VARCHAR(100)
                        )
                    """)

                    # Insert data
                    await conn.execute(
                        "INSERT INTO test_table (name) VALUES ($1)", "test_name"
                    )

                # Verify (outside transaction, so it was committed)
                result = await conn.fetchval("SELECT name FROM test_table LIMIT 1")
                self.assertEqual(result, "test_name")

    async def test_async_transaction_rollback(self):
        """Test async transaction rollback."""
        async with AsyncPostgresConnectionPool(self.test_config) as pool:
            async with pool.get_connection() as conn:
                # Create temp table first
                await conn.execute("""
                    CREATE TEMP TABLE test_table (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(100)
                    )
                """)

                try:
                    async with conn.transaction():
                        # Insert data
                        await conn.execute(
                            "INSERT INTO test_table (name) VALUES ($1)", "test_name"
                        )

                        # Force rollback
                        raise Exception("Rollback test")
                except Exception:
                    pass

                # Verify nothing was inserted
                count = await conn.fetchval("SELECT COUNT(*) FROM test_table")
                self.assertEqual(count, 0)

    async def test_async_batch_operations(self):
        """Test async batch operations."""
        async with AsyncPostgresConnectionPool(self.test_config) as pool:
            async with pool.get_connection() as conn:
                # Create temp table
                await conn.execute("""
                    CREATE TEMP TABLE test_table (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(100)
                    )
                """)

                # Batch insert
                data = [
                    ("Alice",),
                    ("Bob",),
                    ("Charlie",),
                ]

                await conn.executemany(
                    "INSERT INTO test_table (name) VALUES ($1)", data
                )

                # Verify
                count = await conn.fetchval("SELECT COUNT(*) FROM test_table")
                self.assertEqual(count, 3)

                names = await conn.fetch("SELECT name FROM test_table ORDER BY name")
                self.assertEqual(
                    [r["name"] for r in names], ["Alice", "Bob", "Charlie"]
                )


class TestPostgresConnectionPoolStress(unittest.TestCase):
    """Stress tests for connection pool."""

    @classmethod
    def setUpClass(cls):
        """Set up test class."""
        cls.test_config = get_test_config()
        if not is_postgres_available():
            raise unittest.SkipTest("PostgreSQL not available for integration tests")

    @unittest.skip("Skipping slow stress test by default")
    def test_many_sequential_connections(self):
        """Test acquiring many connections sequentially."""
        with PostgresConnectionPool(self.test_config) as pool:
            for i in range(50):
                with pool.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT %s", (i,))
                    result = cursor.fetchone()[0]
                    self.assertEqual(result, i)
                    cursor.close()


class TestAsyncPostgresConnectionPoolStress(unittest.IsolatedAsyncioTestCase):
    """Stress tests for async connection pool."""

    @classmethod
    def setUpClass(cls):
        """Set up test class."""
        cls.test_config = get_test_config()
        if not is_postgres_available():
            raise unittest.SkipTest("PostgreSQL not available for integration tests")

    @unittest.skip("Skipping slow stress test by default")
    async def test_many_concurrent_connections(self):
        """Test many concurrent connections."""
        async with AsyncPostgresConnectionPool(self.test_config) as pool:

            async def query(i):
                async with pool.get_connection() as conn:
                    return await conn.fetchval("SELECT $1", i)

            # 100 concurrent queries
            results = await asyncio.gather(*[query(i) for i in range(100)])

            self.assertEqual(len(results), 100)
            self.assertEqual(sum(results), sum(range(100)))


if __name__ == "__main__":
    unittest.main()
