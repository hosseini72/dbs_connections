"""Integration tests for PostgreSQL connector.

These tests require a running PostgreSQL instance.
Use docker-compose to start test database:

    docker-compose -f tests/integration/docker-compose.yml up -d

Run integration tests:
    pytest tests/integration -m integration
"""

import os
import pytest
import asyncio

from mycompany_db.connectors.postgres import (
    PostgresConnectionPool,
    AsyncPostgresConnectionPool,
    PostgresPoolConfig,
)
from mycompany_db.core.health import HealthState


# Skip all tests if PostgreSQL is not available
pytestmark = pytest.mark.integration


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


@pytest.fixture(scope="module")
def postgres_available():
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
            connect_timeout=5
        )
        conn.close()
        return True
    except Exception:
        pytest.skip("PostgreSQL not available for integration tests")


@pytest.fixture
def test_config():
    """Provide test configuration."""
    return get_test_config()


class TestPostgresConnectionPoolIntegration:
    """Integration tests for sync PostgreSQL pool."""
    
    def test_pool_lifecycle(self, test_config, postgres_available):
        """Test complete pool lifecycle."""
        # Create pool
        pool = PostgresConnectionPool(test_config)
        
        # Initialize
        pool.initialize_pool()
        assert pool._initialized
        
        # Get connection
        with pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 as test")
            result = cursor.fetchone()
            assert result[0] == 1
            cursor.close()
        
        # Close pool
        pool.close_all_connections()
        assert pool._closed
    
    def test_context_manager(self, test_config, postgres_available):
        """Test pool context manager."""
        with PostgresConnectionPool(test_config) as pool:
            with pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT version()")
                version = cursor.fetchone()
                assert version is not None
                assert "PostgreSQL" in version[0]
                cursor.close()
    
    def test_multiple_connections(self, test_config, postgres_available):
        """Test acquiring multiple connections."""
        with PostgresConnectionPool(test_config) as pool:
            # Get multiple connections
            with pool.get_connection() as conn1:
                with pool.get_connection() as conn2:
                    cursor1 = conn1.cursor()
                    cursor2 = conn2.cursor()
                    
                    cursor1.execute("SELECT 1")
                    cursor2.execute("SELECT 2")
                    
                    assert cursor1.fetchone()[0] == 1
                    assert cursor2.fetchone()[0] == 2
                    
                    cursor1.close()
                    cursor2.close()
    
    def test_connection_validation(self, test_config, postgres_available):
        """Test connection validation."""
        config = PostgresPoolConfig(
            host=test_config.host,
            port=test_config.port,
            database=test_config.database,
            user=test_config.user,
            password=test_config.password,
            validate_on_checkout=True,
            pre_ping=True,
        )
        
        with PostgresConnectionPool(config) as pool:
            with pool.get_connection() as conn:
                # Connection should be validated
                assert pool.validate_connection(conn)
    
    def test_health_checks(self, test_config, postgres_available):
        """Test health check functionality."""
        with PostgresConnectionPool(test_config) as pool:
            # Pool health
            health = pool.health_check()
            assert health.state == HealthState.HEALTHY
            assert health.response_time_ms is not None
            
            # Database health
            db_health = pool.database_health_check()
            assert db_health.state == HealthState.HEALTHY
            assert "server_version" in db_health.details
    
    def test_pool_metrics(self, test_config, postgres_available):
        """Test metrics collection."""
        with PostgresConnectionPool(test_config) as pool:
            with pool.get_connection() as conn:
                # Get metrics while connection is active
                metrics = pool.get_metrics()
                
                assert metrics.total_connections > 0
                assert metrics.active_connections > 0
                assert metrics.max_connections == test_config.max_size + test_config.max_overflow
    
    def test_transaction_commit(self, test_config, postgres_available):
        """Test transaction commit."""
        with PostgresConnectionPool(test_config) as pool:
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
                        "INSERT INTO test_table (name) VALUES (%s)",
                        ("test_name",)
                    )
                    
                    # Commit
                    conn.commit()
                    
                    # Verify
                    cursor.execute("SELECT name FROM test_table")
                    result = cursor.fetchone()
                    assert result[0] == "test_name"
                    
                finally:
                    cursor.close()
    
    def test_transaction_rollback(self, test_config, postgres_available):
        """Test transaction rollback."""
        with PostgresConnectionPool(test_config) as pool:
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
                        "INSERT INTO test_table (name) VALUES (%s)",
                        ("test_name",)
                    )
                    
                    # Rollback
                    conn.rollback()
                    
                    # Verify nothing was inserted
                    cursor.execute("SELECT COUNT(*) FROM test_table")
                    count = cursor.fetchone()[0]
                    assert count == 0
                    
                finally:
                    cursor.close()


class TestAsyncPostgresConnectionPoolIntegration:
    """Integration tests for async PostgreSQL pool."""
    
    @pytest.mark.asyncio
    async def test_async_pool_lifecycle(self, test_config, postgres_available):
        """Test complete async pool lifecycle."""
        # Create pool
        pool = AsyncPostgresConnectionPool(test_config)
        
        # Initialize
        await pool.initialize_pool()
        assert pool._initialized
        
        # Get connection
        async with pool.get_connection() as conn:
            result = await conn.fetchval("SELECT 1")
            assert result == 1
        
        # Close pool
        await pool.close_all_connections()
        assert pool._closed
    
    @pytest.mark.asyncio
    async def test_async_context_manager(self, test_config, postgres_available):
        """Test async pool context manager."""
        async with AsyncPostgresConnectionPool(test_config) as pool:
            async with pool.get_connection() as conn:
                version = await conn.fetchval("SELECT version()")
                assert version is not None
                assert "PostgreSQL" in version
    
    @pytest.mark.asyncio
    async def test_async_multiple_connections(self, test_config, postgres_available):
        """Test acquiring multiple async connections."""
        async with AsyncPostgresConnectionPool(test_config) as pool:
            async with pool.get_connection() as conn1:
                async with pool.get_connection() as conn2:
                    result1 = await conn1.fetchval("SELECT 1")
                    result2 = await conn2.fetchval("SELECT 2")
                    
                    assert result1 == 1
                    assert result2 == 2
    
    @pytest.mark.asyncio
    async def test_async_concurrent_queries(self, test_config, postgres_available):
        """Test concurrent query execution."""
        async with AsyncPostgresConnectionPool(test_config) as pool:
            async def execute_query(value):
                async with pool.get_connection() as conn:
                    return await conn.fetchval(f"SELECT {value}")
            
            # Execute 5 queries concurrently
            results = await asyncio.gather(*[
                execute_query(i) for i in range(1, 6)
            ])
            
            assert results == [1, 2, 3, 4, 5]
    
    @pytest.mark.asyncio
    async def test_async_health_checks(self, test_config, postgres_available):
        """Test async health check functionality."""
        async with AsyncPostgresConnectionPool(test_config) as pool:
            # Pool health
            health = await pool.health_check()
            assert health.state == HealthState.HEALTHY
            
            # Database health
            db_health = await pool.database_health_check()
            assert db_health.state == HealthState.HEALTHY
            assert "server_version" in db_health.details
    
    @pytest.mark.asyncio
    async def test_async_transaction_commit(self, test_config, postgres_available):
        """Test async transaction commit."""
        async with AsyncPostgresConnectionPool(test_config) as pool:
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
                        "INSERT INTO test_table (name) VALUES ($1)",
                        "test_name"
                    )
                
                # Verify (outside transaction, so it was committed)
                result = await conn.fetchval(
                    "SELECT name FROM test_table LIMIT 1"
                )
                assert result == "test_name"
    
    @pytest.mark.asyncio
    async def test_async_transaction_rollback(self, test_config, postgres_available):
        """Test async transaction rollback."""
        async with AsyncPostgresConnectionPool(test_config) as pool:
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
                            "INSERT INTO test_table (name) VALUES ($1)",
                            "test_name"
                        )
                        
                        # Force rollback
                        raise Exception("Rollback test")
                except Exception:
                    pass
                
                # Verify nothing was inserted
                count = await conn.fetchval("SELECT COUNT(*) FROM test_table")
                assert count == 0
    
    @pytest.mark.asyncio
    async def test_async_batch_operations(self, test_config, postgres_available):
        """Test async batch operations."""
        async with AsyncPostgresConnectionPool(test_config) as pool:
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
                    "INSERT INTO test_table (name) VALUES ($1)",
                    data
                )
                
                # Verify
                count = await conn.fetchval("SELECT COUNT(*) FROM test_table")
                assert count == 3
                
                names = await conn.fetch("SELECT name FROM test_table ORDER BY name")
                assert [r["name"] for r in names] == ["Alice", "Bob", "Charlie"]


class TestPostgresConnectionPoolStress:
    """Stress tests for connection pool."""
    
    @pytest.mark.slow
    def test_many_sequential_connections(self, test_config, postgres_available):
        """Test acquiring many connections sequentially."""
        with PostgresConnectionPool(test_config) as pool:
            for i in range(50):
                with pool.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT %s", (i,))
                    result = cursor.fetchone()[0]
                    assert result == i
                    cursor.close()
    
    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_many_concurrent_connections(self, test_config, postgres_available):
        """Test many concurrent connections."""
        async with AsyncPostgresConnectionPool(test_config) as pool:
            async def query(i):
                async with pool.get_connection() as conn:
                    return await conn.fetchval("SELECT $1", i)
            
            # 100 concurrent queries
            results = await asyncio.gather(*[
                query(i) for i in range(100)
            ])
            
            assert len(results) == 100
            assert sum(results) == sum(range(100))


# Run with: pytest tests/integration/test_postgres_integration.py -v -m integration