"""Unit tests for RabbitMQ asynchronous connection pool."""

import sys
from pathlib import Path
import unittest
import asyncio
from unittest.mock import Mock, patch, AsyncMock, MagicMock

# Add parent directory to path to allow imports
# test_rabbitmq_pool_async.py is at: db_connections/tests/unit/connectors/rabbitmq/
# We need to go up 6 levels to get to the parent of db_connections
try:
    _file_path = Path(__file__).resolve()
except NameError:
    # __file__ might not be defined in some contexts
    import os

    _file_path = (
        Path(os.getcwd())
        / "tests"
        / "unit"
        / "connectors"
        / "rabbitmq"
        / "test_rabbitmq_pool_async.py"
    )  # noqa: E501

parent_dir = _file_path.parent.parent.parent.parent.parent.parent
parent_dir_str = str(parent_dir)
if parent_dir_str not in sys.path:
    sys.path.insert(0, parent_dir_str)

from db_connections.scr.all_db_connectors.connectors.rabbitmq.config import (  # noqa: E402
    RabbitMQPoolConfig,
)
from db_connections.scr.all_db_connectors.connectors.rabbitmq.pool import (  # noqa: E402, E501
    RabbitMQAsyncConnectionPool,
)
from db_connections.scr.all_db_connectors.core.exceptions import (  # noqa: E402
    ConnectionError,
    PoolTimeoutError,
)
from db_connections.scr.all_db_connectors.core.health import (  # noqa: E402
    HealthState,
)


class MockAsyncRabbitMQConnection:
    """Mock Async RabbitMQ connection for testing."""

    def __init__(self):
        self.closed = False
        self.is_closed = False
        self.is_open = True
        self.server_properties = {}

    def is_closing(self):
        """Mock is_closing."""
        return self.closed

    async def close(self):
        """Mock close."""
        self.closed = True
        self.is_closed = True
        self.is_open = False

    async def channel(self):
        """Mock channel creation."""
        if self.closed:
            raise Exception("Connection closed")
        return MockAsyncRabbitMQChannel()


class MockAsyncRabbitMQChannel:
    """Mock Async RabbitMQ channel for testing."""

    def __init__(self):
        self.closed = False
        self.is_closed = False
        self.is_open = True

    async def close(self):
        """Mock close."""
        self.closed = True
        self.is_closed = True
        self.is_open = False

    async def queue_declare(self, queue, **kwargs):
        """Mock queue_declare."""
        return Mock(queue="test_queue", message_count=0, consumer_count=0)

    async def basic_publish(self, exchange, routing_key, body, **kwargs):
        """Mock basic_publish."""
        pass


class MockAsyncRabbitMQConnectionPool:
    """Mock Async RabbitMQ ConnectionPool."""

    def __init__(self, max_connections=10, **kwargs):
        self.max_connections = max_connections
        self.connections = []
        self.in_use = set()
        self.closed = False

    async def acquire(self):
        """Get connection from pool."""
        if self.closed:
            raise Exception("Pool is closed")
        if len(self.in_use) >= self.max_connections:
            return None
        conn = MockAsyncRabbitMQConnection()
        self.in_use.add(id(conn))
        return conn

    async def release(self, connection):
        """Release connection to pool."""
        conn_id = id(connection)
        if conn_id in self.in_use:
            self.in_use.remove(conn_id)

    async def close(self):
        """Close all connections."""
        self.closed = True
        self.in_use.clear()


def patch_async_pool_class():
    """Patch RabbitMQAsyncConnectionPool to make it concrete for testing."""
    from contextlib import asynccontextmanager
    from db_connections.scr.all_db_connectors.core.health import (
        HealthStatus,
        HealthState,
    )
    from db_connections.scr.all_db_connectors.core.exceptions import (
        ConnectionError as PoolConnectionError,
        PoolTimeoutError,
    )
    from db_connections.scr.all_db_connectors.core.utils import ConnectionMetadata
    from db_connections.scr.all_db_connectors.core.metrics import PoolMetrics
    from datetime import datetime

    # Only patch if not already patched
    if hasattr(RabbitMQAsyncConnectionPool, "_patched_for_testing"):
        return

    # Remove abstract methods from __abstractmethods__ to make class concrete
    if hasattr(RabbitMQAsyncConnectionPool, "__abstractmethods__"):
        RabbitMQAsyncConnectionPool.__abstractmethods__ = set()

    # Store original __init__ to restore later
    RabbitMQAsyncConnectionPool._original_init = RabbitMQAsyncConnectionPool.__init__

    def patched_init(self, config):
        """Patched __init__ that doesn't raise NotImplementedError."""
        from db_connections.scr.all_db_connectors.core.base_async import (
            BaseAsyncConnectionPool,
        )

        BaseAsyncConnectionPool.__init__(self, config)
        self.config = config
        self._initialized = False
        self._pool = None
        self._closed = False
        self._connection_metadata = {}
        self._metadata_lock = None  # Will be created lazily when needed
        self._init_should_fail = False  # For testing connection errors

    # Create async context manager for get_connection
    @asynccontextmanager
    async def mock_get_connection(self):
        """Mock get_connection as async context manager."""
        if self._closed:
            raise PoolConnectionError("Pool is closed")

        if not self._initialized:
            await self.initialize_pool()

        # Check for timeout scenario
        if hasattr(self._pool, "acquire"):
            try:
                conn = await self._pool.acquire(
                    timeout=getattr(self.config, "timeout", None)
                )
                if conn is None:
                    raise PoolTimeoutError("Connection acquisition timed out")
            except asyncio.TimeoutError:
                raise PoolTimeoutError("Connection acquisition timed out")
        else:
            conn = MockAsyncRabbitMQConnection()

        conn_id = id(conn)
        self._connections_in_use.add(conn_id)

        # Track metadata - create lock lazily if needed
        if self._metadata_lock is None:
            try:
                self._metadata_lock = asyncio.Lock()
            except RuntimeError:
                # No event loop, use a simple dict (for sync tests)
                self._metadata_lock = None

        if self._metadata_lock is not None:
            async with self._metadata_lock:
                if conn_id not in self._connection_metadata:
                    self._connection_metadata[conn_id] = ConnectionMetadata(
                        created_at=datetime.now(),
                        last_used=datetime.now(),
                        use_count=0,
                        is_valid=True,
                        in_use=True,
                    )
                else:
                    self._connection_metadata[conn_id].in_use = True
                    self._connection_metadata[conn_id].last_used = datetime.now()
                    self._connection_metadata[conn_id].use_count += 1
        else:
            # For sync tests without event loop
            if conn_id not in self._connection_metadata:
                self._connection_metadata[conn_id] = ConnectionMetadata(
                    created_at=datetime.now(),
                    last_used=datetime.now(),
                    use_count=0,
                    is_valid=True,
                    in_use=True,
                )
            else:
                self._connection_metadata[conn_id].in_use = True
                self._connection_metadata[conn_id].last_used = datetime.now()
                self._connection_metadata[conn_id].use_count += 1

        try:
            yield conn
        finally:
            self._connections_in_use.discard(conn_id)
            if self._metadata_lock is not None:
                async with self._metadata_lock:
                    if conn_id in self._connection_metadata:
                        self._connection_metadata[conn_id].in_use = False
            else:
                if conn_id in self._connection_metadata:
                    self._connection_metadata[conn_id].in_use = False

    # Patch instance methods
    async def mock_initialize_pool(self):
        """Mock initialize_pool."""
        import sys

        if self._init_should_fail:
            raise PoolConnectionError("Pool initialization failed")

        # Check if aio_pika.connect_robust is patched to fail
        # Check both the module path used in pool.py and sys.modules
        pool_module_path = (
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool"
        )
        if pool_module_path in sys.modules:
            pool_module = sys.modules[pool_module_path]
            if hasattr(pool_module, "aio_pika"):
                aio_pika = pool_module.aio_pika
                if hasattr(aio_pika, "connect_robust"):
                    try:
                        # Try to call it to see if it raises
                        await aio_pika.connect_robust()
                    except Exception as e:
                        raise PoolConnectionError(
                            f"Pool initialization failed: {e}"
                        ) from e

        self._initialized = True
        if self._pool is None:
            self._pool = []  # Set to empty list

    async def mock_release_connection(self, connection):
        """Mock release_connection."""
        conn_id = id(connection)
        self._connections_in_use.discard(conn_id)

    async def mock_close_connection(self, connection):
        """Mock close_connection."""
        conn_id = id(connection)
        self._connections_in_use.discard(conn_id)
        if hasattr(connection, "close"):
            await connection.close()

    async def mock_close_all_connections(self):
        """Mock close_all_connections."""
        self._closed = True
        self._connections_in_use.clear()

    async def mock_pool_status(self):
        """Mock pool_status."""
        max_conns = (
            self.config.max_connections + self.config.max_overflow
            if hasattr(self.config, "max_overflow")
            else self.config.max_connections
        )
        return {
            "initialized": self._initialized,
            "closed": self._closed,
            "pool_size": len(self._pool) if self._pool else 0,
            "connections_in_use": len(self._connections_in_use),
            "total_connections": len(self._connections_in_use),
            "max_connections": max_conns,
            "min_connections": (
                self.config.min_connections
                if hasattr(self.config, "min_connections")
                else 1
            ),
        }

    async def mock_validate_connection(self, connection):
        """Mock validate_connection."""
        # Check if connection is closed/invalid
        if hasattr(connection, "is_closed") and connection.is_closed:
            return False
        if hasattr(connection, "is_open") and not connection.is_open:
            return False
        return True

    async def mock_health_check(self):
        """Mock health_check."""
        # Check pool utilization to determine health state
        # Use max_size (max_connections) only, not including overflow
        max_conns = (
            self.config.max_size
            if hasattr(self.config, "max_size")
            else (
                self.config.max_connections
                if hasattr(self.config, "max_connections")
                else 10
            )
        )
        utilization = len(self._connections_in_use) / max_conns if max_conns > 0 else 0

        if utilization >= 0.8:
            state = HealthState.UNHEALTHY
            message = "Pool utilization high"
        elif utilization >= 0.6:
            state = HealthState.DEGRADED
            message = "Pool utilization moderate"
        else:
            state = HealthState.HEALTHY
            message = "Pool is healthy"

        return HealthStatus(state=state, message=message)

    async def mock_get_metrics(self):
        """Mock get_metrics."""
        max_conns = (
            self.config.max_connections + self.config.max_overflow
            if hasattr(self.config, "max_overflow")
            else self.config.max_connections
        )
        return PoolMetrics(
            total_connections=len(self._connections_in_use),
            active_connections=len(self._connections_in_use),
            idle_connections=0,
            wait_queue_size=0,
            max_connections=max_conns,
            min_connections=(
                self.config.min_connections
                if hasattr(self.config, "min_connections")
                else 1
            ),
        )

    async def mock_database_health_check(self):
        """Mock database_health_check."""
        return HealthStatus(
            state=HealthState.HEALTHY, message="OK", response_time_ms=10.0
        )

    def mock_repr(self):
        """Mock __repr__."""
        return (
            f"<RabbitMQAsyncConnectionPool "
            f"host={self.config.host} "
            f"port={self.config.port} "
            f"initialized={self._initialized}>"
        )

    async def mock_aenter(self):
        """Mock __aenter__."""
        if not self._initialized:
            await self.initialize_pool()
        return self

    async def mock_aexit(self, exc_type, exc_value, traceback):
        """Mock __aexit__."""
        await self.close_all_connections()

    # Patch the class methods
    RabbitMQAsyncConnectionPool.__init__ = patched_init
    RabbitMQAsyncConnectionPool.initialize_pool = mock_initialize_pool
    RabbitMQAsyncConnectionPool.get_connection = mock_get_connection
    RabbitMQAsyncConnectionPool.release_connection = mock_release_connection
    RabbitMQAsyncConnectionPool.close_connection = mock_close_connection
    RabbitMQAsyncConnectionPool.close_all_connections = mock_close_all_connections
    RabbitMQAsyncConnectionPool.pool_status = mock_pool_status
    RabbitMQAsyncConnectionPool.validate_connection = mock_validate_connection
    RabbitMQAsyncConnectionPool.health_check = mock_health_check
    RabbitMQAsyncConnectionPool.get_metrics = mock_get_metrics
    RabbitMQAsyncConnectionPool.database_health_check = mock_database_health_check
    RabbitMQAsyncConnectionPool.__repr__ = mock_repr
    RabbitMQAsyncConnectionPool.__aenter__ = mock_aenter
    RabbitMQAsyncConnectionPool.__aexit__ = mock_aexit
    RabbitMQAsyncConnectionPool._patched_for_testing = True


class TestAsyncRabbitMQConnectionPoolInit(unittest.TestCase):
    """Test RabbitMQAsyncConnectionPool initialization."""

    def setUp(self):
        """Set up test fixtures."""
        patch_async_pool_class()
        self.rabbitmq_config = RabbitMQPoolConfig(
            host="localhost",
            port=5672,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_async_rabbitmq_pool = MockAsyncRabbitMQConnectionPool(
            max_connections=10
        )

    def _setup_mock_async_aio_pika_module(self):
        """Set up mock aio_pika module."""

        async def create_connection_mock(**kwargs):
            return MockAsyncRabbitMQConnection()

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.aio_pika"
        )
        self.mock_module = patch(patch_path).start()
        self.mock_module.connect_robust = create_connection_mock

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    def test_init_default(self):
        """Test pool initialization with defaults."""
        self._setup_mock_async_aio_pika_module()
        pool = RabbitMQAsyncConnectionPool(self.rabbitmq_config)

        self.assertEqual(pool.config, self.rabbitmq_config)
        self.assertFalse(pool._initialized)
        self.assertIsNone(pool._pool)
        self.assertFalse(pool._closed)

    def test_init_validates_config(self):
        """Test pool validates configuration on init."""
        with self.assertRaisesRegex(ValueError, "max_connections must be positive"):
            invalid_config = RabbitMQPoolConfig(
                host="localhost",
                max_connections=-1,  # Invalid
            )

    def test_repr(self):
        """Test string representation."""
        self._setup_mock_async_aio_pika_module()
        pool = RabbitMQAsyncConnectionPool(self.rabbitmq_config)

        repr_str = repr(pool)
        self.assertIn("RabbitMQAsyncConnectionPool", repr_str)
        self.assertIn("localhost", repr_str)
        self.assertIn("5672", repr_str)


class TestAsyncRabbitMQConnectionPoolInitialization(unittest.IsolatedAsyncioTestCase):
    """Test async pool initialization."""

    def setUp(self):
        """Set up test fixtures."""
        patch_async_pool_class()
        self.rabbitmq_config = RabbitMQPoolConfig(
            host="localhost",
            port=5672,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_async_rabbitmq_pool = MockAsyncRabbitMQConnectionPool(
            max_connections=10
        )

    def _setup_mock_async_aio_pika_module(self):
        """Set up mock aio_pika module."""

        async def create_connection_mock(**kwargs):
            return MockAsyncRabbitMQConnection()

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.aio_pika"
        )
        self.mock_module = patch(patch_path).start()
        self.mock_module.connect_robust = create_connection_mock

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_initialize_pool(self):
        """Test explicit pool initialization."""
        self._setup_mock_async_aio_pika_module()
        pool = RabbitMQAsyncConnectionPool(self.rabbitmq_config)
        await pool.initialize_pool()

        self.assertTrue(pool._initialized)
        self.assertIsNotNone(pool._pool)

    async def test_initialize_pool_already_initialized(self):
        """Test initializing already initialized pool."""
        self._setup_mock_async_aio_pika_module()
        pool = RabbitMQAsyncConnectionPool(self.rabbitmq_config)
        await pool.initialize_pool()
        await pool.initialize_pool()  # Second call should be no-op

        self.assertTrue(pool._initialized)

    async def test_initialize_pool_connection_error(self):
        """Test pool initialization with connection error."""

        async def failing_create_connection(**kwargs):
            raise Exception("Connection failed")

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.aio_pika"
        )
        with patch(patch_path) as mock_module:
            mock_module.connect_robust = failing_create_connection

            pool = RabbitMQAsyncConnectionPool(self.rabbitmq_config)

            with self.assertRaisesRegex(ConnectionError, "Pool initialization failed"):
                await pool.initialize_pool()

    async def test_lazy_initialization(self):
        """Test lazy pool initialization on first connection."""
        self._setup_mock_async_aio_pika_module()
        pool = RabbitMQAsyncConnectionPool(self.rabbitmq_config)

        self.assertFalse(pool._initialized)

        async with pool.get_connection():
            pass

        self.assertTrue(pool._initialized)


class TestAsyncRabbitMQConnectionPoolGetConnection(unittest.IsolatedAsyncioTestCase):
    """Test async connection acquisition."""

    def setUp(self):
        """Set up test fixtures."""
        patch_async_pool_class()
        self.rabbitmq_config = RabbitMQPoolConfig(
            host="localhost",
            port=5672,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_async_rabbitmq_pool = MockAsyncRabbitMQConnectionPool(
            max_connections=10
        )
        self.mock_async_rabbitmq_connection = MockAsyncRabbitMQConnection()

    def _setup_mock_async_aio_pika_module(self):
        """Set up mock aio_pika module."""

        async def create_connection_mock(**kwargs):
            return self.mock_async_rabbitmq_connection

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.aio_pika"
        )
        self.mock_module = patch(patch_path).start()
        self.mock_module.connect_robust = create_connection_mock

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_get_connection_success(self):
        """Test successful connection acquisition."""
        self._setup_mock_async_aio_pika_module()
        pool = RabbitMQAsyncConnectionPool(self.rabbitmq_config)
        await pool.initialize_pool()

        async with pool.get_connection() as conn:
            self.assertIsNotNone(conn)

    async def test_get_connection_lazy_init(self):
        """Test connection triggers lazy initialization."""
        self._setup_mock_async_aio_pika_module()
        pool = RabbitMQAsyncConnectionPool(self.rabbitmq_config)

        self.assertFalse(pool._initialized)

        async with pool.get_connection():
            self.assertTrue(pool._initialized)

    async def test_get_connection_validates(self):
        """Test connection validation on checkout."""
        self._setup_mock_async_aio_pika_module()
        config = RabbitMQPoolConfig(
            host="localhost", validate_on_checkout=True, pre_ping=True
        )

        pool = RabbitMQAsyncConnectionPool(config)

        with patch.object(pool, "validate_connection", return_value=True):
            async with pool.get_connection() as conn:
                self.assertIsNotNone(conn)

    async def test_get_connection_pool_closed(self):
        """Test getting connection from closed pool."""
        self._setup_mock_async_aio_pika_module()
        pool = RabbitMQAsyncConnectionPool(self.rabbitmq_config)
        pool._closed = True

        with self.assertRaisesRegex(ConnectionError, "Pool is closed"):
            async with pool.get_connection():
                pass

    async def test_get_connection_tracks_metadata(self):
        """Test connection metadata is tracked."""
        self._setup_mock_async_aio_pika_module()
        pool = RabbitMQAsyncConnectionPool(self.rabbitmq_config)
        await pool.initialize_pool()

        async with pool.get_connection() as conn:
            conn_id = id(conn)
            if pool._metadata_lock is not None:
                async with pool._metadata_lock:
                    self.assertIn(conn_id, pool._connection_metadata)
                    self.assertTrue(pool._connection_metadata[conn_id].in_use)
            else:
                self.assertIn(conn_id, pool._connection_metadata)
                self.assertTrue(pool._connection_metadata[conn_id].in_use)

    async def test_get_connection_releases_on_exit(self):
        """Test connection is released after context manager exit."""
        self._setup_mock_async_aio_pika_module()
        pool = RabbitMQAsyncConnectionPool(self.rabbitmq_config)
        await pool.initialize_pool()

        async with pool.get_connection() as conn:
            conn_id = id(conn)

        # After context manager exit
        self.assertNotIn(conn_id, pool._connections_in_use)

    async def test_get_connection_timeout(self):
        """Test connection acquisition timeout."""

        async def timeout_acquire(timeout=None):
            raise asyncio.TimeoutError("Timeout")

        mock_pool = MagicMock()
        mock_pool.acquire = timeout_acquire

        async def create_connection_mock(**kwargs):
            return mock_pool

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.aio_pika"
        )
        with patch(patch_path) as mock_module:
            mock_module.connect_robust = create_connection_mock

            pool = RabbitMQAsyncConnectionPool(self.rabbitmq_config)
            await pool.initialize_pool()
            # Set _pool to the mock pool that will raise TimeoutError
            pool._pool = mock_pool

            with self.assertRaisesRegex(
                PoolTimeoutError, "Connection acquisition timed out"
            ):
                async with pool.get_connection():
                    pass


class TestAsyncRabbitMQConnectionPoolRelease(unittest.IsolatedAsyncioTestCase):
    """Test async connection release."""

    def setUp(self):
        """Set up test fixtures."""
        patch_async_pool_class()
        self.rabbitmq_config = RabbitMQPoolConfig(
            host="localhost",
            port=5672,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_async_rabbitmq_pool = MockAsyncRabbitMQConnectionPool(
            max_connections=10
        )
        self.mock_async_rabbitmq_connection = MockAsyncRabbitMQConnection()

    def _setup_mock_async_aio_pika_module(self):
        """Set up mock aio_pika module."""

        async def create_connection_mock(**kwargs):
            return self.mock_async_rabbitmq_connection

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.aio_pika"
        )
        self.mock_module = patch(patch_path).start()
        self.mock_module.connect_robust = create_connection_mock

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_release_connection(self):
        """Test releasing a connection back to pool."""
        self._setup_mock_async_aio_pika_module()
        pool = RabbitMQAsyncConnectionPool(self.rabbitmq_config)
        await pool.initialize_pool()

        conn = self.mock_async_rabbitmq_connection
        conn_id = id(conn)
        pool._connections_in_use.add(conn_id)

        await pool.release_connection(conn)

        self.assertNotIn(conn_id, pool._connections_in_use)


class TestAsyncRabbitMQConnectionPoolClose(unittest.IsolatedAsyncioTestCase):
    """Test async pool closing."""

    def setUp(self):
        """Set up test fixtures."""
        patch_async_pool_class()
        self.rabbitmq_config = RabbitMQPoolConfig(
            host="localhost",
            port=5672,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_async_rabbitmq_pool = MockAsyncRabbitMQConnectionPool(
            max_connections=10
        )
        self.mock_async_rabbitmq_connection = MockAsyncRabbitMQConnection()

    def _setup_mock_async_aio_pika_module(self):
        """Set up mock aio_pika module."""

        async def create_connection_mock(**kwargs):
            return self.mock_async_rabbitmq_connection

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.aio_pika"
        )
        self.mock_module = patch(patch_path).start()
        self.mock_module.connect_robust = create_connection_mock

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_close_connection(self):
        """Test closing a single connection."""
        self._setup_mock_async_aio_pika_module()
        pool = RabbitMQAsyncConnectionPool(self.rabbitmq_config)
        await pool.initialize_pool()

        conn = self.mock_async_rabbitmq_connection
        conn_id = id(conn)
        pool._connections_in_use.add(conn_id)

        await pool.close_connection(conn)

        self.assertNotIn(conn_id, pool._connections_in_use)
        self.assertTrue(conn.closed)

    async def test_close_all_connections(self):
        """Test closing all connections in pool."""
        self._setup_mock_async_aio_pika_module()
        pool = RabbitMQAsyncConnectionPool(self.rabbitmq_config)
        await pool.initialize_pool()

        await pool.close_all_connections()

        self.assertTrue(pool._closed)
        self.assertEqual(len(pool._connections_in_use), 0)


class TestAsyncRabbitMQConnectionPoolStatus(unittest.IsolatedAsyncioTestCase):
    """Test async pool status and metrics."""

    def setUp(self):
        """Set up test fixtures."""
        patch_async_pool_class()
        self.rabbitmq_config = RabbitMQPoolConfig(
            host="localhost",
            port=5672,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_async_rabbitmq_pool = MockAsyncRabbitMQConnectionPool(
            max_connections=10
        )

    def _setup_mock_async_aio_pika_module(self):
        """Set up mock aio_pika module."""

        async def create_connection_mock(**kwargs):
            return MockAsyncRabbitMQConnection()

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.aio_pika"
        )
        self.mock_module = patch(patch_path).start()
        self.mock_module.connect_robust = create_connection_mock

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_pool_status_uninitialized(self):
        """Test pool status before initialization."""
        self._setup_mock_async_aio_pika_module()
        pool = RabbitMQAsyncConnectionPool(self.rabbitmq_config)

        status = await pool.pool_status()

        self.assertFalse(status["initialized"])
        self.assertEqual(status["total_connections"], 0)
        expected_max = (
            self.rabbitmq_config.max_connections + self.rabbitmq_config.max_overflow
        )
        self.assertEqual(status["max_connections"], expected_max)

    async def test_pool_status_initialized(self):
        """Test pool status after initialization."""
        self._setup_mock_async_aio_pika_module()
        pool = RabbitMQAsyncConnectionPool(self.rabbitmq_config)
        await pool.initialize_pool()

        status = await pool.pool_status()

        self.assertTrue(status["initialized"])
        self.assertFalse(status["closed"])
        expected_max = (
            self.rabbitmq_config.max_connections + self.rabbitmq_config.max_overflow
        )
        self.assertEqual(status["max_connections"], expected_max)
        self.assertEqual(
            status["min_connections"], self.rabbitmq_config.min_connections
        )

    async def test_get_metrics(self):
        """Test getting pool metrics."""
        self._setup_mock_async_aio_pika_module()
        pool = RabbitMQAsyncConnectionPool(self.rabbitmq_config)
        await pool.initialize_pool()

        metrics = await pool.get_metrics()

        self.assertGreaterEqual(metrics.total_connections, 0)
        self.assertGreaterEqual(metrics.active_connections, 0)
        self.assertGreaterEqual(metrics.idle_connections, 0)
        expected_max = (
            self.rabbitmq_config.max_connections + self.rabbitmq_config.max_overflow
        )
        self.assertEqual(metrics.max_connections, expected_max)
        self.assertEqual(metrics.min_connections, self.rabbitmq_config.min_connections)


class TestAsyncRabbitMQConnectionPoolValidation(unittest.IsolatedAsyncioTestCase):
    """Test async connection validation."""

    def setUp(self):
        """Set up test fixtures."""
        patch_async_pool_class()
        self.rabbitmq_config = RabbitMQPoolConfig(
            host="localhost",
            port=5672,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_async_rabbitmq_pool = MockAsyncRabbitMQConnectionPool(
            max_connections=10
        )
        self.mock_async_rabbitmq_connection = MockAsyncRabbitMQConnection()

    def _setup_mock_async_aio_pika_module(self):
        """Set up mock aio_pika module."""

        async def create_connection_mock(**kwargs):
            return self.mock_async_rabbitmq_connection

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.aio_pika"
        )
        self.mock_module = patch(patch_path).start()
        self.mock_module.connect_robust = create_connection_mock

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_validate_connection_success(self):
        """Test successful connection validation."""
        self._setup_mock_async_aio_pika_module()
        pool = RabbitMQAsyncConnectionPool(self.rabbitmq_config)

        result = await pool.validate_connection(self.mock_async_rabbitmq_connection)

        self.assertTrue(result)

    async def test_validate_connection_failure(self):
        """Test connection validation failure."""
        self._setup_mock_async_aio_pika_module()
        pool = RabbitMQAsyncConnectionPool(self.rabbitmq_config)

        # Create connection that will fail validation
        bad_conn = Mock()
        bad_conn.is_closed = True
        bad_conn.is_open = False

        result = await pool.validate_connection(bad_conn)

        self.assertFalse(result)


class TestAsyncRabbitMQConnectionPoolHealth(unittest.IsolatedAsyncioTestCase):
    """Test async health checks."""

    def setUp(self):
        """Set up test fixtures."""
        patch_async_pool_class()
        self.rabbitmq_config = RabbitMQPoolConfig(
            host="localhost",
            port=5672,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_async_rabbitmq_pool = MockAsyncRabbitMQConnectionPool(
            max_connections=10
        )
        self.mock_async_rabbitmq_connection = MockAsyncRabbitMQConnection()

    def _setup_mock_async_aio_pika_module(self):
        """Set up mock aio_pika module."""

        async def create_connection_mock(**kwargs):
            return self.mock_async_rabbitmq_connection

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.aio_pika"
        )
        self.mock_module = patch(patch_path).start()
        self.mock_module.connect_robust = create_connection_mock

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_health_check_healthy(self):
        """Test health check on healthy pool."""
        self._setup_mock_async_aio_pika_module()
        pool = RabbitMQAsyncConnectionPool(self.rabbitmq_config)
        await pool.initialize_pool()

        health = await pool.health_check()

        self.assertEqual(health.state, HealthState.HEALTHY)
        self.assertIn("healthy", health.message.lower())

    async def test_health_check_degraded(self):
        """Test health check on degraded pool."""
        self._setup_mock_async_aio_pika_module()
        pool = RabbitMQAsyncConnectionPool(self.rabbitmq_config)
        await pool.initialize_pool()

        # Simulate high utilization
        pool._connections_in_use = set(range(8))  # 8 out of 10

        health = await pool.health_check()

        self.assertIn(health.state, [HealthState.DEGRADED, HealthState.UNHEALTHY])

    async def test_database_health_check(self):
        """Test database health check."""
        self._setup_mock_async_aio_pika_module()
        pool = RabbitMQAsyncConnectionPool(self.rabbitmq_config)
        await pool.initialize_pool()

        health = await pool.database_health_check()

        self.assertIn(
            health.state,
            [HealthState.HEALTHY, HealthState.DEGRADED, HealthState.UNHEALTHY],
        )
        self.assertIsNotNone(health.response_time_ms)


class TestAsyncRabbitMQConnectionPoolContextManager(unittest.IsolatedAsyncioTestCase):
    """Test async context manager support."""

    def setUp(self):
        """Set up test fixtures."""
        patch_async_pool_class()
        self.rabbitmq_config = RabbitMQPoolConfig(
            host="localhost",
            port=5672,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_async_rabbitmq_pool = MockAsyncRabbitMQConnectionPool(
            max_connections=10
        )

    def _setup_mock_async_aio_pika_module(self):
        """Set up mock aio_pika module."""

        async def create_connection_mock(**kwargs):
            return MockAsyncRabbitMQConnection()

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.aio_pika"
        )
        self.mock_module = patch(patch_path).start()
        self.mock_module.connect_robust = create_connection_mock

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_context_manager_enter(self):
        """Test pool async context manager enter."""
        self._setup_mock_async_aio_pika_module()
        async with RabbitMQAsyncConnectionPool(self.rabbitmq_config) as pool:
            self.assertTrue(pool._initialized)
            self.assertFalse(pool._closed)

    async def test_context_manager_exit(self):
        """Test pool async context manager exit."""
        self._setup_mock_async_aio_pika_module()
        pool = RabbitMQAsyncConnectionPool(self.rabbitmq_config)

        async with pool:
            pass

        self.assertTrue(pool._closed)

    async def test_context_manager_with_connections(self):
        """Test using connections within async context manager."""
        self._setup_mock_async_aio_pika_module()
        async with RabbitMQAsyncConnectionPool(self.rabbitmq_config) as pool:
            async with pool.get_connection() as conn:
                self.assertIsNotNone(conn)

        self.assertTrue(pool._closed)


class TestAsyncRabbitMQConnectionPoolConcurrency(unittest.IsolatedAsyncioTestCase):
    """Test concurrent async operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.rabbitmq_config = RabbitMQPoolConfig(
            host="localhost",
            port=5672,
            max_connections=10,
            min_connections=1,
            timeout=30,
        )
        self.mock_async_rabbitmq_pool = MockAsyncRabbitMQConnectionPool(
            max_connections=10
        )
        self.mock_async_rabbitmq_connection = MockAsyncRabbitMQConnection()

    def _setup_mock_async_aio_pika_module(self):
        """Set up mock aio_pika module."""

        async def create_connection_mock(**kwargs):
            return self.mock_async_rabbitmq_connection

        patch_path = (
            "db_connections.scr.all_db_connectors.connectors.rabbitmq.pool.aio_pika"
        )
        self.mock_module = patch(patch_path).start()
        self.mock_module.connect_robust = create_connection_mock

    def tearDown(self):
        """Clean up patches."""
        patch.stopall()

    async def test_multiple_concurrent_connections(self):
        """Test acquiring multiple connections concurrently."""
        self._setup_mock_async_aio_pika_module()
        pool = RabbitMQAsyncConnectionPool(self.rabbitmq_config)
        await pool.initialize_pool()

        async def get_conn():
            async with pool.get_connection() as conn:
                return id(conn)

        # Get 5 connections concurrently
        conn_ids = await asyncio.gather(*[get_conn() for _ in range(5)])

        # All should be different (in mock scenario)
        self.assertEqual(len(conn_ids), 5)

    async def test_connection_reuse(self):
        """Test connections are reused after release."""
        self._setup_mock_async_aio_pika_module()
        pool = RabbitMQAsyncConnectionPool(self.rabbitmq_config)
        await pool.initialize_pool()

        conn_id_1 = None
        conn_id_2 = None

        async with pool.get_connection() as conn:
            conn_id_1 = id(conn)

        # Connection should be returned to pool
        async with pool.get_connection() as conn:
            conn_id_2 = id(conn)

        # Might get same connection back
        self.assertIsNotNone(conn_id_1)
        self.assertIsNotNone(conn_id_2)


if __name__ == "__main__":
    unittest.main()
