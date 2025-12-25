"""Pytest configuration and shared fixtures for all tests."""

import os
import pytest
import asyncio
from typing import Generator, AsyncGenerator


# =============================================================================
# Pytest Configuration
# =============================================================================

def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "unit: mark test as a unit test"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as an integration test"
    )
    config.addinivalue_line(
        "markers", "postgres: mark test as postgres-specific"
    )
    config.addinivalue_line(
        "markers", "redis: mark test as redis-specific"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )


# =============================================================================
# Event Loop Configuration for Async Tests
# =============================================================================

@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for the entire test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


# =============================================================================
# Environment Setup
# =============================================================================

@pytest.fixture(scope="session", autouse=True)
def setup_test_env():
    """Setup test environment variables."""
    # PostgreSQL test environment
    os.environ.setdefault("TEST_POSTGRES_HOST", "localhost")
    os.environ.setdefault("TEST_POSTGRES_PORT", "5432")
    os.environ.setdefault("TEST_POSTGRES_DATABASE", "test_db")
    os.environ.setdefault("TEST_POSTGRES_USER", "test_user")
    os.environ.setdefault("TEST_POSTGRES_PASSWORD", "test_password")
    
    yield
    
    # Cleanup is automatic when test session ends


# =============================================================================
# Mock Connection Classes
# =============================================================================

class MockPsycopg2Connection:
    """Mock psycopg2 connection for testing."""
    
    def __init__(self):
        self.closed = False
        self.autocommit = False
        self._cursor = None
    
    def cursor(self):
        """Return mock cursor."""
        return MockPsycopg2Cursor()
    
    def commit(self):
        """Mock commit."""
        pass
    
    def rollback(self):
        """Mock rollback."""
        pass
    
    def close(self):
        """Mock close."""
        self.closed = True


class MockPsycopg2Cursor:
    """Mock psycopg2 cursor for testing."""
    
    def __init__(self):
        self.closed = False
        self._results = [(1,)]
    
    def execute(self, query, params=None):
        """Mock execute."""
        if "non_existent" in query:
            raise Exception("Table does not exist")
        pass
    
    def executemany(self, query, params):
        """Mock executemany."""
        pass
    
    def fetchone(self):
        """Mock fetchone."""
        return self._results[0] if self._results else None
    
    def fetchall(self):
        """Mock fetchall."""
        return self._results
    
    def fetchmany(self, size=1):
        """Mock fetchmany."""
        return self._results[:size]
    
    def close(self):
        """Mock close."""
        self.closed = True


class MockAsyncpgTransaction:
    """Mock asyncpg transaction."""
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False


class MockAsyncpgConnection:
    """Mock asyncpg connection for testing."""
    
    def __init__(self):
        self.closed = False
    
    async def fetch(self, query, *args):
        """Mock fetch."""
        return [{"id": 1, "name": "test"}]
    
    async def fetchval(self, query, *args):
        """Mock fetchval."""
        if "SELECT 1" in query:
            return 1
        elif "version" in query.lower():
            return "PostgreSQL 15.0"
        elif "count" in query.lower():
            return 5
        return "test_value"
    
    async def fetchrow(self, query, *args):
        """Mock fetchrow."""
        return {"id": 1, "name": "test"}
    
    async def execute(self, query, *args):
        """Mock execute."""
        return "INSERT 0 1"
    
    async def executemany(self, query, args):
        """Mock executemany."""
        pass
    
    async def close(self):
        """Mock close."""
        self.closed = True
    
    def transaction(self):
        """Mock transaction context manager."""
        return MockAsyncpgTransaction()


# =============================================================================
# Mock Pool Classes
# =============================================================================

class MockPsycopg2Pool:
    """Mock psycopg2 ThreadedConnectionPool."""
    
    def __init__(self, minconn, maxconn, **kwargs):
        self.minconn = minconn
        self.maxconn = maxconn
        self.connections = []
        self.in_use = set()
        
        # Create initial connections
        for _ in range(minconn):
            self.connections.append(MockPsycopg2Connection())
    
    def getconn(self):
        """Get connection from pool."""
        if self.connections:
            conn = self.connections.pop(0)
            self.in_use.add(id(conn))
            return conn
        elif len(self.in_use) < self.maxconn:
            conn = MockPsycopg2Connection()
            self.in_use.add(id(conn))
            return conn
        return None
    
    def putconn(self, conn, close=False):
        """Return connection to pool."""
        conn_id = id(conn)
        if conn_id in self.in_use:
            self.in_use.remove(conn_id)
        
        if close:
            conn.close()
        else:
            self.connections.append(conn)
    
    def closeall(self):
        """Close all connections."""
        for conn in self.connections:
            conn.close()
        self.connections.clear()
        self.in_use.clear()


class MockAsyncpgPool:
    """Mock asyncpg Pool."""
    
    def __init__(self, min_size, max_size):
        self.min_size = min_size
        self.max_size = max_size
        self._size = min_size
        self._idle_size = min_size
        self.closed = False
    
    async def acquire(self, timeout=None):
        """Acquire connection from pool."""
        return MockAsyncpgConnection()
    
    async def release(self, connection, timeout=None):
        """Release connection to pool."""
        pass
    
    async def close(self):
        """Close pool."""
        self.closed = True
    
    def get_size(self):
        """Get pool size."""
        return self._size
    
    def get_idle_size(self):
        """Get idle connections."""
        return self._idle_size


# =============================================================================
# Fixture Exports
# =============================================================================

@pytest.fixture
def mock_psycopg2_connection():
    """Provide mock psycopg2 connection."""
    return MockPsycopg2Connection()


@pytest.fixture
def mock_psycopg2_pool():
    """Provide mock psycopg2 pool."""
    return MockPsycopg2Pool(minconn=2, maxconn=10)


@pytest.fixture
def mock_asyncpg_connection():
    """Provide mock asyncpg connection."""
    return MockAsyncpgConnection()


@pytest.fixture
def mock_asyncpg_pool():
    """Provide mock asyncpg pool."""
    return MockAsyncpgPool(min_size=2, max_size=10)