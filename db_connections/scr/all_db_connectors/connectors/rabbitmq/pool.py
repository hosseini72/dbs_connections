"""RabbitMQ connection pool implementation (Synchronous and Asynchronous)."""

import logging
import queue
import threading
import time
from contextlib import contextmanager, asynccontextmanager
from datetime import datetime
from typing import Optional, Dict, Any

# Sync imports
try:
    import pika
    from pika import BlockingConnection, URLParameters, PlainCredentials
    from pika.connection import ConnectionParameters
    SYNC_AVAILABLE = True
except ImportError:
    SYNC_AVAILABLE = False
    BlockingConnection = None
    URLParameters = None
    ConnectionParameters = None
    PlainCredentials = None

# Async imports
try:
    import aio_pika
    ASYNC_AVAILABLE = True
except ImportError:
    ASYNC_AVAILABLE = False
    aio_pika = None

from db_connections.scr.all_db_connectors.core.base_sync import BaseSyncConnectionPool
from db_connections.scr.all_db_connectors.core.base_async import BaseAsyncConnectionPool
from db_connections.scr.all_db_connectors.core.exceptions import (
    ConnectionError,
    PoolTimeoutError,
    PoolExhaustedError,
    ValidationError,
)
from db_connections.scr.all_db_connectors.core.health import HealthStatus, HealthState
from db_connections.scr.all_db_connectors.core.metrics import PoolMetrics
from db_connections.scr.all_db_connectors.core.utils import (
    ConnectionMetadata,
    validate_pool_config,
    should_recycle_connection,
)
from db_connections.scr.all_db_connectors.connectors.rabbitmq.config import RabbitMQPoolConfig
from db_connections.scr.all_db_connectors.connectors.rabbitmq.exceptions import (
    RabbitMQConnectionError,
    RabbitMQValidationError,
)

logger = logging.getLogger(__name__)


class RabbitMQSyncConnectionPool(BaseSyncConnectionPool):
    """Synchronous RabbitMQ connection pool using pika.

    Features:
    - Connection pooling with min/max size control
    - Automatic connection validation and recycling
    - Health checks and metrics
    - Thread-safe operations
    - Retry logic with exponential backoff
    - Context manager support

    Example:
        >>> config = RabbitMQPoolConfig(
        ...     host="localhost",
        ...     port=5672,
        ...     virtual_host="/"
        ... )
        >>>
        >>> # Lazy initialization
        >>> pool = RabbitMQSyncConnectionPool(config)
        >>> with pool.get_connection() as conn:
        ...     channel = conn.channel()
        ...     channel.queue_declare(queue='test_queue')
        >>>
        >>> # Eager initialization with context manager
        >>> with RabbitMQSyncConnectionPool(config) as pool:
        ...     with pool.get_connection() as conn:
        ...         channel = conn.channel()
        ...         channel.basic_publish(exchange='', routing_key='test_queue', body='Hello')
    """

    def __init__(self, config: RabbitMQPoolConfig):
        """Initialize RabbitMQ connection pool.

        Args:
            config: RabbitMQ pool configuration.
        """
        if not SYNC_AVAILABLE:
            raise ImportError(
                "pika is required for synchronous RabbitMQ connections. "
                "Install it with: pip install pika"
            )

        super().__init__(config)
        self.config: RabbitMQPoolConfig = config

        # Validate configuration
        validate_pool_config(config)

        # Connection pool (queue-based)
        self._pool: Optional[queue.Queue] = None

        # Connection metadata tracking
        self._connection_metadata: Dict[int, ConnectionMetadata] = {}
        self._metadata_lock = threading.Lock()

        # Metrics tracking
        self._total_connections_created = 0
        self._total_connections_closed = 0
        self._wait_times = []
        self._wait_times_lock = threading.Lock()

        # Health checker (will be initialized after pool is created)
        self._health_checker: Optional[Any] = None

        logger.info(
            f"RabbitMQ connection pool initialized: "
            f"{config.host}:{config.port}/{config.virtual_host}"
        )

    def initialize_pool(self):
        """Initialize the connection pool.

        Creates a queue-based connection pool and populates it with
        initial connections.

        Raises:
            ConnectionError: If pool initialization fails.
        """
        if self._initialized:
            logger.warning("Pool already initialized")
            return

        try:
            logger.info(
                f"Initializing RabbitMQ pool: "
                f"min={self.config.min_connections}, max={self.config.max_connections}"
            )

            conn_params = self.config.get_connection_params()

            # Create queue-based pool
            max_pool_size = self.config.max_size + self.config.max_overflow
            self._pool = queue.Queue(maxsize=max_pool_size)

            # Create initial connections
            for _ in range(self.config.min_size):
                try:
                    connection = self._create_connection(conn_params)
                    conn_id = id(connection)
                    with self._metadata_lock:
                        self._connection_metadata[conn_id] = ConnectionMetadata()
                    self._pool.put_nowait(connection)
                    self._total_connections_created += 1
                except Exception as e:
                    logger.warning(f"Failed to create initial connection: {e}")
                    # Continue with other connections

            if self._total_connections_created == 0:
                raise RabbitMQConnectionError("Failed to create any initial connections")

            self._initialized = True

            # Initialize health checker after pool is created
            from db_connections.scr.all_db_connectors.connectors.rabbitmq.health import RabbitMQHealthChecker
            self._health_checker = RabbitMQHealthChecker(self)

            logger.info("RabbitMQ pool initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize RabbitMQ pool: {e}")
            raise RabbitMQConnectionError(f"Pool initialization failed: {e}") from e

    def _create_connection(self, params: Dict[str, Any]):
        """Create a new RabbitMQ connection.

        Args:
            params: Connection parameters.

        Returns:
            RabbitMQ connection object.
        """
        if "url" in params:
            parameters = URLParameters(params["url"])
        else:
            # Build credentials if username or password provided
            credentials = None
            if params.get("username") or params.get("password"):
                credentials = PlainCredentials(
                    params.get("username", "guest"),
                    params.get("password", "guest")
                )

            parameters = ConnectionParameters(
                host=params.get("host", "localhost"),
                port=params.get("port", 5672),
                virtual_host=params.get("virtual_host", "/"),
                credentials=credentials,
                heartbeat=params.get("heartbeat", 600),
                connection_attempts=params.get("connection_attempts", 3),
                retry_delay=params.get("retry_delay", 2.0),
                blocked_connection_timeout=params.get("blocked_connection_timeout"),
                ssl_options=params.get("ssl_options") if params.get("ssl") else None,
            )

        return BlockingConnection(parameters)

    @contextmanager
    def get_connection(self):
        """Get a connection from the pool.

        Yields:
            RabbitMQ connection object.

        Raises:
            PoolTimeoutError: If no connection available within timeout.
            ConnectionError: If connection acquisition fails.

        Example:
            >>> with pool.get_connection() as conn:
            ...     channel = conn.channel()
            ...     channel.queue_declare(queue='test_queue')
        """
        if self._closed:
            raise RabbitMQConnectionError("Pool is closed")

        if not self._initialized:
            self.initialize_pool()

        if self._pool is None:
            raise RabbitMQConnectionError("Pool not initialized")

        connection = None
        start_time = time.time()
        attempt = 0

        try:
            while attempt <= self.config.max_retries:
                try:
                    # Try to get connection from queue with timeout
                    try:
                        connection = self._pool.get(timeout=self.config.timeout)
                    except queue.Empty:
                        # Queue is empty, create new connection if under max
                        total_conns = len(self._connection_metadata)
                        max_conns = self.config.max_size + self.config.max_overflow
                        if total_conns < max_conns:
                            conn_params = self.config.get_connection_params()
                            connection = self._create_connection(conn_params)
                            conn_id = id(connection)
                            with self._metadata_lock:
                                self._connection_metadata[conn_id] = ConnectionMetadata()
                            self._total_connections_created += 1
                        else:
                            raise PoolExhaustedError("No connections available")

                    if connection is None:
                        raise PoolExhaustedError("No connections available")

                    # Record wait time
                    wait_time = (time.time() - start_time) * 1000
                    with self._wait_times_lock:
                        self._wait_times.append(wait_time)
                        # Keep only last 100 wait times
                        if len(self._wait_times) > 100:
                            self._wait_times.pop(0)

                    # Validate connection if configured
                    if self.config.pre_ping or self.config.validate_on_checkout:
                        if not self.validate_connection(connection):
                            logger.warning("Connection validation failed, recreating")
                            try:
                                connection.close()
                            except Exception:
                                pass
                            # Create new connection
                            conn_params = self.config.get_connection_params()
                            connection = self._create_connection(conn_params)
                            conn_id = id(connection)
                            with self._metadata_lock:
                                if conn_id not in self._connection_metadata:
                                    self._connection_metadata[conn_id] = ConnectionMetadata()
                            self._total_connections_created += 1

                    # Track connection metadata
                    conn_id = id(connection)
                    with self._metadata_lock:
                        if conn_id in self._connection_metadata:
                            self._connection_metadata[conn_id].mark_used()

                    self._connections_in_use.add(conn_id)

                    logger.debug(f"Connection acquired: {conn_id}")

                    # Yield connection to user
                    yield connection

                    # Connection returned successfully
                    break

                except Exception as e:
                    attempt += 1
                    if attempt > self.config.max_retries:
                        logger.error(f"Failed to acquire connection after {attempt} attempts")
                        raise RabbitMQConnectionError(f"Connection acquisition failed: {e}") from e

                    logger.warning(f"Connection attempt {attempt} failed, retrying: {e}")
                    time.sleep(self.config.retry_delay * (self.config.retry_backoff ** (attempt - 1)))

        finally:
            # Release connection back to pool
            if connection:
                try:
                    conn_id = id(connection)

                    # Update metadata
                    with self._metadata_lock:
                        if conn_id in self._connection_metadata:
                            metadata = self._connection_metadata[conn_id]
                            metadata.mark_released()

                            # Check if connection should be recycled
                            if should_recycle_connection(
                                metadata.to_dict(),
                                self.config
                            ):
                                logger.debug(f"Recycling connection: {conn_id}")
                                try:
                                    connection.close()
                                except Exception:
                                    pass
                                del self._connection_metadata[conn_id]
                                self._total_connections_closed += 1
                            else:
                                # Return to pool
                                try:
                                    self._pool.put_nowait(connection)
                                except queue.Full:
                                    # Pool is full, close connection
                                    try:
                                        connection.close()
                                    except Exception:
                                        pass
                                    del self._connection_metadata[conn_id]
                                    self._total_connections_closed += 1

                    self._connections_in_use.discard(conn_id)
                    logger.debug(f"Connection released: {conn_id}")

                except Exception as e:
                    logger.error(f"Error releasing connection: {e}")

    def release_connection(self, connection):
        """Release a connection back to the pool.

        Args:
            connection: Connection to release.
        """
        if connection and self._pool:
            try:
                conn_id = id(connection)

                with self._metadata_lock:
                    if conn_id in self._connection_metadata:
                        metadata = self._connection_metadata[conn_id]
                        metadata.mark_released()

                        if should_recycle_connection(metadata.to_dict(), self.config):
                            try:
                                connection.close()
                            except Exception:
                                pass
                            del self._connection_metadata[conn_id]
                            self._total_connections_closed += 1
                        else:
                            try:
                                self._pool.put_nowait(connection)
                            except queue.Full:
                                # Pool is full, close connection
                                try:
                                    connection.close()
                                except Exception:
                                    pass
                                del self._connection_metadata[conn_id]
                                self._total_connections_closed += 1

                self._connections_in_use.discard(conn_id)

            except Exception as e:
                logger.error(f"Error releasing connection: {e}")

    def close_connection(self, connection):
        """Close a specific connection.

        Args:
            connection: Connection to close.
        """
        if connection:
            try:
                conn_id = id(connection)
                connection.close()

                with self._metadata_lock:
                    if conn_id in self._connection_metadata:
                        del self._connection_metadata[conn_id]

                self._connections_in_use.discard(conn_id)
                self._total_connections_closed += 1

                logger.debug(f"Connection closed: {conn_id}")

            except Exception as e:
                logger.error(f"Error closing connection: {e}")

    def close_all_connections(self):
        """Close all connections in the pool."""
        if self._pool:
            try:
                logger.info("Closing all connections in pool")

                # Close all connections in queue
                while not self._pool.empty():
                    try:
                        conn = self._pool.get_nowait()
                        conn.close()
                    except Exception:
                        pass

                with self._metadata_lock:
                    self._connection_metadata.clear()

                self._connections_in_use.clear()
                self._closed = True
                self._pool = None

                logger.info("All connections closed")

            except Exception as e:
                logger.error(f"Error closing all connections: {e}")

    def pool_status(self) -> dict:
        """Get current pool status.

        Returns:
            Dictionary containing pool status information.
        """
        if not self._initialized or not self._pool:
            return {
                "initialized": False,
                "total_connections": 0,
                "active_connections": 0,
                "idle_connections": 0,
                "max_connections": self.config.max_size + self.config.max_overflow,
                "min_connections": self.config.min_size,
            }

        total_conns = len(self._connection_metadata)
        active_conns = len(self._connections_in_use)
        idle_conns = total_conns - active_conns

        return {
            "initialized": self._initialized,
            "closed": self._closed,
            "total_connections": total_conns,
            "active_connections": active_conns,
            "idle_connections": idle_conns,
            "max_connections": self.config.max_size + self.config.max_overflow,
            "min_connections": self.config.min_size,
            "total_created": self._total_connections_created,
            "total_closed": self._total_connections_closed,
        }

    def get_metrics(self) -> PoolMetrics:
        """Get pool metrics.

        Returns:
            PoolMetrics object with current pool statistics.
        """
        status = self.pool_status()

        # Calculate average wait time
        avg_wait_time = None
        with self._wait_times_lock:
            if self._wait_times:
                avg_wait_time = sum(self._wait_times) / len(self._wait_times)

        return PoolMetrics(
            total_connections=status["total_connections"],
            active_connections=status["active_connections"],
            idle_connections=status["idle_connections"],
            max_connections=status["max_connections"],
            min_connections=status["min_connections"],
            wait_queue_size=self._pool.qsize() if self._pool else 0,
            average_wait_time_ms=avg_wait_time,
        )

    def validate_connection(self, connection) -> bool:
        """Validate a connection is still usable.

        Args:
            connection: RabbitMQ connection to validate.

        Returns:
            True if connection is valid, False otherwise.
        """
        try:
            # Check if connection is closed
            if hasattr(connection, "is_closing") and connection.is_closing():
                return False
            if hasattr(connection, "is_closed") and connection.is_closed:
                return False
            if hasattr(connection, "is_open") and not connection.is_open:
                return False

            # Try to create a channel to verify connection is working
            channel = connection.channel()
            channel.close()
            return True

        except Exception as e:
            logger.warning(f"Connection validation failed: {e}")
            return False

    def health_check(self) -> HealthStatus:
        """Perform health check on the pool.

        Returns:
            HealthStatus indicating pool health.
        """
        try:
            if self._health_checker:
                return self._health_checker.check_pool()
            else:
                # Fallback
                if not self._initialized or not self._pool:
                    return HealthStatus(
                        state=HealthState.UNHEALTHY,
                        message="Pool not initialized",
                        checked_at=datetime.now(),
                    )

                return HealthStatus(
                    state=HealthState.HEALTHY,
                    message="Pool is healthy",
                    checked_at=datetime.now(),
                )
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return HealthStatus(
                state=HealthState.UNHEALTHY,
                message=f"Health check failed: {e}",
                checked_at=datetime.now(),
            )

    def __enter__(self):
        """Enter context manager."""
        if not self._initialized:
            self.initialize_pool()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit context manager."""
        self.close_all_connections()
        self._closed = True

    def __repr__(self) -> str:
        """String representation of the pool."""
        return (
            f"RabbitMQSyncConnectionPool("
            f"host={self.config.host}, "
            f"port={self.config.port}, "
            f"virtual_host={self.config.virtual_host}, "
            f"initialized={self._initialized}, "
            f"closed={self._closed})"
        )


# Async pool implementation would go here but for now we'll just define the class
# The __init__.py will handle the import gracefully if pool_async doesn't exist
class RabbitMQAsyncConnectionPool(BaseAsyncConnectionPool):
    """Asynchronous RabbitMQ connection pool using aio_pika.

    This is a placeholder. The full async implementation should be similar
    to RabbitMQSyncConnectionPool but using aio_pika.
    """

    def __init__(self, config: RabbitMQPoolConfig):
        """Initialize async RabbitMQ connection pool."""
        if not ASYNC_AVAILABLE:
            raise ImportError(
                "aio_pika is required for asynchronous RabbitMQ connections. "
                "Install it with: pip install aio-pika"
            )
        super().__init__(config)
        # TODO: Implement async pool
        raise NotImplementedError("Async pool not yet implemented")

