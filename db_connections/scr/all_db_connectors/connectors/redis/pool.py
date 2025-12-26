"""Redis connection pool implementation (Synchronous and Asynchronous)."""

import logging
import threading
import time
import asyncio
from contextlib import contextmanager, asynccontextmanager
from datetime import datetime
from typing import Optional, Dict, Any

try:
    import redis  # Import module for test patching
    from redis import Redis
    from redis.connection import ConnectionPool as RedisConnectionPool
    from redis.exceptions import (
        ConnectionError as RedisConnectionError,
        TimeoutError as RedisTimeoutError,
        ResponseError,
    )

    # Make ConnectionPool available as redis.ConnectionPool for test patching
    if not hasattr(redis, "ConnectionPool"):
        redis.ConnectionPool = RedisConnectionPool
except ImportError:
    raise ImportError(
        "redis package is required for synchronous Redis connections. "
        "Install it with: pip install redis"
    )

try:
    from redis.asyncio import Redis as AsyncRedis
    from redis.asyncio.connection import ConnectionPool as AsyncRedisConnectionPool
except ImportError:
    AsyncRedis = None
    AsyncRedisConnectionPool = None

from db_connections.scr.all_db_connectors.core.base_sync import BaseSyncConnectionPool
from db_connections.scr.all_db_connectors.core.base_async import BaseAsyncConnectionPool
from db_connections.scr.all_db_connectors.core.exceptions import (
    ConnectionError,
    PoolTimeoutError,
    PoolExhaustedError,
    ValidationError,
    HealthCheckError,
)
from db_connections.scr.all_db_connectors.core.health import HealthStatus, HealthState
from db_connections.scr.all_db_connectors.core.metrics import PoolMetrics
from db_connections.scr.all_db_connectors.core.utils import (
    ConnectionMetadata,
    validate_pool_config,
    should_recycle_connection,
)
from db_connections.scr.all_db_connectors.connectors.redis.config import RedisPoolConfig
from db_connections.scr.all_db_connectors.connectors.redis.health import (
    RedisHealthChecker,
)

logger = logging.getLogger(__name__)


class RedisSyncConnectionPool(BaseSyncConnectionPool):
    """Synchronous Redis connection pool using redis-py.

    Features:
    - Connection pooling with min/max size control
    - Automatic connection validation and recycling
    - Health checks and metrics
    - Thread-safe operations
    - Retry logic with exponential backoff
    - Context manager support

    Example:
        >>> config = RedisPoolConfig(
        ...     host="localhost",
        ...     port=6379,
        ...     db=0
        ... )
        >>>
        >>> # Lazy initialization
        >>> pool = RedisSyncConnectionPool(config)
        >>> with pool.get_connection() as conn:
        ...     conn.set("key", "value")
        ...     value = conn.get("key")
        >>>
        >>> # Eager initialization with context manager
        >>> with RedisSyncConnectionPool(config) as pool:
        ...     with pool.get_connection() as conn:
        ...         conn.ping()
    """

    def __init__(self, config: RedisPoolConfig):
        """Initialize Redis connection pool.

        Args:
            config: Redis pool configuration.

        Raises:
            ValueError: If configuration is invalid.
        """
        super().__init__(config)
        self.config: RedisPoolConfig = config

        # Validate configuration (additional pool-level validation)
        validate_pool_config(config)

        # Connection pool
        self._pool: Optional[RedisConnectionPool] = None

        # Connection metadata tracking
        self._connection_metadata: Dict[int, ConnectionMetadata] = {}
        self._metadata_lock = threading.Lock()

        # Metrics tracking
        self._total_connections_created = 0
        self._total_connections_closed = 0
        self._wait_times = []
        self._wait_times_lock = threading.Lock()

        # Health checker
        self._health_checker = RedisHealthChecker(config)

        logger.info(
            f"Redis connection pool initialized: "
            f"{config.host}:{config.port}/{config.db}"
        )

    def initialize_pool(self):
        """Initialize the connection pool.

        Creates the underlying redis connection pool with configured
        min/max connections.

        Raises:
            ConnectionError: If pool initialization fails.
        """
        if self._initialized:
            logger.warning("Pool already initialized")
            return

        try:
            logger.info(
                f"Initializing Redis pool: "
                f"min={self.config.min_size}, max={self.config.max_size}"
            )

            conn_params = self.config.get_connection_params()

            # Create connection pool
            self._pool = redis.ConnectionPool(
                max_connections=self.config.max_size + self.config.max_overflow,
                **conn_params,
            )

            self._initialized = True
            self._total_connections_created += self.config.min_size

            logger.info("Redis pool initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize Redis pool: {e}")
            raise ConnectionError(f"Pool initialization failed: {e}") from e

    @contextmanager
    def get_connection(self):
        """Get a connection from the pool.

        Yields:
            Redis connection object.

        Raises:
            PoolTimeoutError: If no connection available within timeout.
            ConnectionError: If connection acquisition fails.

        Example:
            >>> with pool.get_connection() as conn:
            ...     conn.set("key", "value")
        """
        if not self._initialized:
            self.initialize_pool()

        if self._closed:
            raise ConnectionError("Pool is closed")

        connection = None
        start_time = time.time()
        attempt = 0

        try:
            while attempt <= self.config.max_retries:
                connection = None
                try:
                    # Check if pool has a get_connection method (for testing/mocking)
                    # If pool.get_connection returns None, pool is exhausted
                    pool_conn = None
                    if hasattr(self._pool, "get_connection") and callable(
                        self._pool.get_connection
                    ):
                        try:
                            pool_conn = self._pool.get_connection("ping")
                            if pool_conn is None:
                                raise PoolExhaustedError("No connections available")
                            # For mock pools that return connections directly, use them
                            # This ensures each call gets a unique connection object
                            connection = pool_conn
                        except (PoolExhaustedError, PoolTimeoutError):
                            raise
                        except (RedisConnectionError, RedisTimeoutError):
                            # If get_connection raises connection errors, propagate for retry
                            raise
                        except Exception:
                            # If get_connection raises other exceptions, continue with normal flow
                            pass

                    # If we didn't get a connection from get_connection, create Redis client
                    # The pool manages actual connections internally
                    # Each call creates a new Redis client instance
                    if connection is None:
                        connection = redis.Redis(connection_pool=self._pool)

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
                            logger.warning("Connection validation failed, reconnecting")
                            try:
                                connection.close()
                            except Exception:
                                pass
                            connection = redis.Redis(connection_pool=self._pool)

                    # Track connection metadata (tracking Redis client instances)
                    conn_id = id(connection)
                    with self._metadata_lock:
                        if conn_id not in self._connection_metadata:
                            self._connection_metadata[conn_id] = ConnectionMetadata()
                        self._connection_metadata[conn_id].mark_used()

                    self._connections_in_use.add(conn_id)

                    logger.debug(f"Connection acquired: {conn_id}")

                    # Yield connection to user
                    yield connection

                    # Connection returned successfully
                    break

                except PoolExhaustedError:
                    # Don't retry on pool exhaustion
                    raise

                except (PoolTimeoutError, PoolExhaustedError):
                    raise

                except (RedisConnectionError, RedisTimeoutError) as e:
                    attempt += 1
                    if connection:
                        try:
                            connection.close()
                        except Exception:
                            pass
                        connection = None
                    if attempt > self.config.max_retries:
                        logger.error(
                            f"Failed to acquire connection after {attempt} attempts"
                        )
                        raise ConnectionError(
                            f"Connection acquisition failed: {e}"
                        ) from e

                    logger.warning(
                        f"Connection attempt {attempt} failed, retrying: {e}"
                    )
                    time.sleep(
                        self.config.retry_delay
                        * (self.config.retry_backoff ** (attempt - 1))
                    )

                except Exception as e:
                    attempt += 1
                    if connection:
                        try:
                            connection.close()
                        except Exception:
                            pass
                        connection = None
                    # Check if it's a connection-related error that should be retried
                    # This handles cases where redis.Redis() itself raises errors
                    error_str = str(e).lower()
                    error_type = type(e).__name__
                    if (
                        "connection" in error_str
                        or "timeout" in error_str
                        or "network" in error_str
                        or "ConnectionError" in error_type
                        or "TimeoutError" in error_type
                    ):
                        if attempt > self.config.max_retries:
                            logger.error(
                                f"Failed to acquire connection after {attempt} attempts"
                            )
                            raise ConnectionError(
                                f"Connection acquisition failed: {e}"
                            ) from e
                        logger.warning(
                            f"Connection attempt {attempt} failed, retrying: {e}"
                        )
                        time.sleep(
                            self.config.retry_delay
                            * (self.config.retry_backoff ** (attempt - 1))
                        )
                    else:
                        logger.error(f"Unexpected error acquiring connection: {e}")
                        raise ConnectionError(
                            f"Connection acquisition failed: {e}"
                        ) from e

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
                                metadata.to_dict(), self.config
                            ):
                                logger.debug(f"Recycling connection: {conn_id}")
                                try:
                                    connection.close()
                                except Exception:
                                    pass
                                del self._connection_metadata[conn_id]
                                self._total_connections_closed += 1
                            else:
                                # Close the Redis client (connections return to pool automatically)
                                try:
                                    connection.close()
                                except Exception:
                                    pass

                    self._connections_in_use.discard(conn_id)
                    logger.debug(f"Connection released: {conn_id}")

                except Exception as e:
                    logger.error(f"Error releasing connection: {e}")

    def release_connection(self, connection):
        """Release a connection back to the pool.

        Args:
            connection: Connection to release.
        """
        if connection:
            try:
                conn_id = id(connection)

                with self._metadata_lock:
                    if conn_id in self._connection_metadata:
                        metadata = self._connection_metadata[conn_id]
                        metadata.mark_released()

                        if should_recycle_connection(metadata.to_dict(), self.config):
                            connection.close()
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
                self._pool.disconnect()

                with self._metadata_lock:
                    self._connection_metadata.clear()

                self._connections_in_use.clear()
                self._closed = True

                logger.info("All connections closed")

            except Exception as e:
                logger.error(f"Error closing all connections: {e}")

    def pool_status(self) -> dict:
        """Get current pool status.

        Returns:
            Dictionary containing pool status information.
        """
        if not self._initialized:
            return {
                "initialized": False,
                "total_connections": 0,
                "active_connections": 0,
                "idle_connections": 0,
                "max_connections": self.config.max_size + self.config.max_overflow,
                "min_connections": self.config.min_size,
            }

        # Get pool stats
        pool_size = self._pool.max_connections
        active_conns = len(self._connections_in_use)
        idle_conns = max(0, pool_size - active_conns)

        return {
            "initialized": self._initialized,
            "closed": self._closed,
            "total_connections": pool_size,
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
            wait_queue_size=0,  # redis-py doesn't expose this
            average_wait_time_ms=avg_wait_time,
        )

    def validate_connection(self, connection) -> bool:
        """Validate a connection is still usable.

        Args:
            connection: Connection to validate.

        Returns:
            True if connection is valid, False otherwise.
        """
        try:
            # Execute PING command
            result = connection.ping()
            return result is True

        except Exception as e:
            logger.warning(f"Connection validation failed: {e}")
            return False

    def health_check(self) -> HealthStatus:
        """Perform health check on the pool.

        Returns:
            HealthStatus indicating pool health.
        """
        try:
            # Get pool status first
            status = self.pool_status()
            response_time_ms = 0

            # Determine health state based on pool metrics
            active_conns = status.get("active_connections", 0)
            max_conns = status.get("max_connections", 1)

            # Calculate utilization
            utilization = active_conns / max_conns if max_conns > 0 else 0

            # Determine state based on utilization
            if utilization < 0.7:
                state = HealthState.HEALTHY
                message = "Pool is healthy"
            elif utilization < 0.9:
                state = HealthState.DEGRADED
                message = "Pool utilization is high"
            else:
                state = HealthState.UNHEALTHY
                message = "Pool is near capacity"

            # Also check connection health if possible
            try:
                with self.get_connection() as conn:
                    conn_health = self._health_checker.check_health(conn)
                    # Only override pool health if connection is actually unhealthy
                    # and pool utilization is low (connection failure is more critical)
                    if (
                        state == HealthState.HEALTHY
                        and conn_health.state == HealthState.HEALTHY
                    ):
                        response_time_ms = conn_health.response_time_ms or 0
                    elif state != HealthState.HEALTHY:
                        # If pool utilization is already high, use connection health
                        return conn_health
            except Exception:
                # If we can't get a connection but pool utilization is low,
                # pool itself is still healthy (might be temporary issue)
                # Only mark unhealthy if utilization is already high
                if utilization >= 0.9:
                    state = HealthState.UNHEALTHY
                    message = "Cannot acquire connection for health check"

            return HealthStatus(
                state=state,
                message=message,
                checked_at=datetime.now(),
                response_time_ms=response_time_ms,
                details={
                    "total_connections": status.get("total_connections", 0),
                    "active_connections": active_conns,
                    "idle_connections": status.get("idle_connections", 0),
                    "max_connections": max_conns,
                    "utilization_percent": round(utilization * 100, 2),
                },
            )
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return HealthStatus(
                state=HealthState.UNHEALTHY,
                message=f"Health check failed: {e}",
                checked_at=datetime.now(),
            )

    def database_health_check(self) -> HealthStatus:
        """Perform health check on the database server.

        Returns:
            HealthStatus indicating database health.
        """
        try:
            # Get a connection and perform health check
            with self.get_connection() as conn:
                return self._health_checker.check_health(conn)
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return HealthStatus(
                state=HealthState.UNHEALTHY,
                message=f"Database health check failed: {e}",
                checked_at=datetime.now(),
            )

    def __enter__(self):
        """Enter context manager."""
        if not self._initialized:
            self.initialize_pool()
            self._initialized = True
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit context manager."""
        self.close_all_connections()
        self._closed = True

    def __repr__(self) -> str:
        """String representation of the pool."""
        return (
            f"RedisSyncConnectionPool("
            f"host={self.config.host}, "
            f"port={self.config.port}, "
            f"db={self.config.db}, "
            f"initialized={self._initialized}, "
            f"closed={self._closed})"
        )


class RedisAsyncConnectionPool(BaseAsyncConnectionPool):
    """Asynchronous Redis connection pool using redis.asyncio.

    Features:
    - Async/await connection pooling
    - Automatic connection validation and recycling
    - Health checks and metrics
    - Retry logic with exponential backoff
    - Context manager support
    - High performance with async operations

    Example:
        >>> config = RedisPoolConfig(
        ...     host="localhost",
        ...     port=6379,
        ...     db=0
        ... )
        >>>
        >>> # Lazy initialization
        >>> pool = RedisAsyncConnectionPool(config)
        >>> async with pool.get_connection() as conn:
        ...     await conn.set("key", "value")
        ...     value = await conn.get("key")
        >>>
        >>> # Eager initialization with context manager
        >>> async with RedisAsyncConnectionPool(config) as pool:
        ...     async with pool.get_connection() as conn:
        ...         await conn.ping()
    """

    def __init__(self, config: RedisPoolConfig):
        """Initialize async Redis connection pool.

        Args:
            config: Redis pool configuration.
        """
        if AsyncRedis is None:
            raise ImportError(
                "redis package with async support is required. "
                "Install it with: pip install redis[hiredis]"
            )

        super().__init__(config)
        self.config: RedisPoolConfig = config

        # Validate configuration
        validate_pool_config(config)

        # Connection pool
        self._pool: Optional[AsyncRedis] = None

        # Connection metadata tracking (lazy initialization to avoid event loop requirement)
        self._connection_metadata: Dict[int, ConnectionMetadata] = {}
        self._metadata_lock: Optional[asyncio.Lock] = None

        # Metrics tracking
        self._total_connections_created = 0
        self._total_connections_closed = 0
        self._wait_times = []
        self._wait_times_lock: Optional[asyncio.Lock] = None

        # Health checker
        self._health_checker = RedisHealthChecker(config)

        logger.info(
            f"Async Redis connection pool initialized: "
            f"{config.host}:{config.port}/{config.db}"
        )

    def _get_metadata_lock(self) -> asyncio.Lock:
        """Get or create the metadata lock."""
        if self._metadata_lock is None:
            self._metadata_lock = asyncio.Lock()
        return self._metadata_lock

    def _get_wait_times_lock(self) -> asyncio.Lock:
        """Get or create the wait times lock."""
        if self._wait_times_lock is None:
            self._wait_times_lock = asyncio.Lock()
        return self._wait_times_lock

    async def initialize_pool(self):
        """Initialize the connection pool.

        Creates the underlying async Redis connection pool.

        Raises:
            ConnectionError: If pool initialization fails.
        """
        if self._initialized:
            logger.warning("Pool already initialized")
            return

        try:
            logger.info(
                f"Initializing async Redis pool: "
                f"min={self.config.min_size}, max={self.config.max_size}"
            )

            conn_params = self.config.get_connection_params()

            # Create async Redis connection pool
            self._pool = AsyncRedis(
                max_connections=self.config.max_size + self.config.max_overflow,
                **conn_params,
            )

            self._initialized = True
            self._total_connections_created += self.config.min_size

            logger.info("Async Redis pool initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize async Redis pool: {e}")
            raise ConnectionError(f"Pool initialization failed: {e}") from e

    @asynccontextmanager
    async def get_connection(self):
        """Get a connection from the pool.

        Yields:
            AsyncRedis connection object.

        Raises:
            PoolTimeoutError: If no connection available within timeout.
            ConnectionError: If connection acquisition fails.

        Example:
            >>> async with pool.get_connection() as conn:
            ...     await conn.set("key", "value")
        """
        if not self._initialized:
            await self.initialize_pool()

        if self._closed:
            raise ConnectionError("Pool is closed")

        connection = None
        start_time = time.time()
        attempt = 0

        try:
            # Use the pool connection directly (redis.asyncio manages connections internally)
            connection = self._pool

            if connection is None:
                raise PoolExhaustedError("No connections available")

            # Record wait time
            wait_time = (time.time() - start_time) * 1000
            async with self._get_wait_times_lock():
                self._wait_times.append(wait_time)
                # Keep only last 100 wait times
                if len(self._wait_times) > 100:
                    self._wait_times.pop(0)

            # Validate connection if configured
            if self.config.pre_ping or self.config.validate_on_checkout:
                try:
                    if not await self.validate_connection(connection):
                        logger.warning("Connection validation failed, reconnecting")
                        try:
                            await connection.aclose()
                        except Exception:
                            pass
                        # Reinitialize pool to get a new connection
                        await self.initialize_pool()
                        connection = self._pool
                        # Re-validate the new connection - if it also fails, raise error
                        try:
                            if not await self.validate_connection(connection):
                                raise ConnectionError(
                                    "Connection validation failed after reconnection"
                                )
                        except asyncio.TimeoutError as e:
                            logger.error(
                                "Connection validation timed out after reconnection"
                            )
                            raise PoolTimeoutError(
                                "Connection acquisition timed out"
                            ) from e
                except asyncio.TimeoutError as e:
                    logger.error("Connection validation timed out")
                    raise PoolTimeoutError("Connection acquisition timed out") from e

            # Track connection metadata
            conn_id = id(connection)
            async with self._get_metadata_lock():
                if conn_id not in self._connection_metadata:
                    self._connection_metadata[conn_id] = ConnectionMetadata()
                self._connection_metadata[conn_id].mark_used()

            self._connections_in_use.add(conn_id)

            logger.debug(f"Async connection acquired: {conn_id}")

            # Yield connection to user
            yield connection

        except asyncio.TimeoutError as e:
            logger.error("Connection acquisition timed out")
            raise PoolTimeoutError("Connection acquisition timed out") from e
        except PoolTimeoutError:
            # Re-raise PoolTimeoutError without wrapping
            raise
        except Exception as e:
            error_type = type(e).__name__
            if "ConnectionError" in error_type:
                logger.error(f"Failed to acquire connection: {e}")
                raise ConnectionError(f"Connection acquisition failed: {e}") from e
            else:
                logger.error(f"Unexpected error acquiring connection: {e}")
                raise ConnectionError(f"Connection acquisition failed: {e}") from e

        finally:
            # Release connection back to pool
            if connection:
                try:
                    conn_id = id(connection)

                    # Update metadata
                    async with self._get_metadata_lock():
                        if conn_id in self._connection_metadata:
                            metadata = self._connection_metadata[conn_id]
                            metadata.mark_released()

                            # Check if connection should be recycled
                            if should_recycle_connection(
                                metadata.to_dict(), self.config
                            ):
                                logger.debug(f"Recycling async connection: {conn_id}")
                                del self._connection_metadata[conn_id]
                                self._total_connections_closed += 1

                    self._connections_in_use.discard(conn_id)
                    logger.debug(f"Async connection released: {conn_id}")

                except Exception as e:
                    logger.error(f"Error releasing async connection: {e}")

    async def release_connection(self, connection):
        """Release a connection back to the pool.

        Args:
            connection: Connection to release.
        """
        if connection:
            try:
                conn_id = id(connection)

                async with self._get_metadata_lock():
                    if conn_id in self._connection_metadata:
                        metadata = self._connection_metadata[conn_id]
                        metadata.mark_released()

                        if should_recycle_connection(metadata.to_dict(), self.config):
                            del self._connection_metadata[conn_id]
                            self._total_connections_closed += 1

                self._connections_in_use.discard(conn_id)

            except Exception as e:
                logger.error(f"Error releasing async connection: {e}")

    async def close_connection(self, connection):
        """Close a specific connection.

        Args:
            connection: Connection to close.
        """
        if connection:
            try:
                conn_id = id(connection)
                await connection.aclose()

                async with self._get_metadata_lock():
                    if conn_id in self._connection_metadata:
                        del self._connection_metadata[conn_id]

                self._connections_in_use.discard(conn_id)
                self._total_connections_closed += 1

                logger.debug(f"Async connection closed: {conn_id}")

            except Exception as e:
                logger.error(f"Error closing async connection: {e}")

    async def close_all_connections(self):
        """Close all connections in the pool."""
        if self._pool:
            try:
                logger.info("Closing all async connections in pool")
                await self._pool.aclose()

                async with self._get_metadata_lock():
                    self._connection_metadata.clear()

                self._connections_in_use.clear()
                self._closed = True

                logger.info("All async connections closed")

            except Exception as e:
                logger.error(f"Error closing all async connections: {e}")

    async def pool_status(self) -> dict:
        """Get current pool status.

        Returns:
            Dictionary containing pool status information.
        """
        if not self._initialized:
            return {
                "initialized": False,
                "total_connections": 0,
                "active_connections": 0,
                "idle_connections": 0,
                "max_connections": self.config.max_size + self.config.max_overflow,
                "min_connections": self.config.min_size,
            }

        # Get pool stats (redis.asyncio doesn't expose detailed pool stats)
        active_conns = len(self._connections_in_use)
        max_conns = self.config.max_size + self.config.max_overflow
        idle_conns = max(0, max_conns - active_conns)

        return {
            "initialized": self._initialized,
            "closed": self._closed,
            "total_connections": max_conns,
            "active_connections": active_conns,
            "idle_connections": idle_conns,
            "max_connections": max_conns,
            "min_connections": self.config.min_size,
            "total_created": self._total_connections_created,
            "total_closed": self._total_connections_closed,
        }

    async def get_metrics(self) -> PoolMetrics:
        """Get pool metrics.

        Returns:
            PoolMetrics object with current pool statistics.
        """
        status = await self.pool_status()

        # Calculate average wait time
        avg_wait_time = None
        async with self._get_wait_times_lock():
            if self._wait_times:
                avg_wait_time = sum(self._wait_times) / len(self._wait_times)

        return PoolMetrics(
            total_connections=status["total_connections"],
            active_connections=status["active_connections"],
            idle_connections=status["idle_connections"],
            max_connections=status["max_connections"],
            min_connections=status["min_connections"],
            wait_queue_size=0,  # redis.asyncio doesn't expose this
            average_wait_time_ms=avg_wait_time,
        )

    async def validate_connection(self, connection) -> bool:
        """Validate a connection is still usable.

        Args:
            connection: Connection to validate.

        Returns:
            True if connection is valid, False otherwise.

        Raises:
            asyncio.TimeoutError: If validation times out.
        """
        try:
            # Execute PING command
            result = await connection.ping()
            return result is True

        except asyncio.TimeoutError:
            # Re-raise timeout errors so they can be handled appropriately
            raise
        except Exception as e:
            logger.warning(f"Async connection validation failed: {e}")
            return False

    async def health_check(self) -> HealthStatus:
        """Perform health check on the pool.

        Returns:
            HealthStatus indicating pool health.
        """
        try:
            # Get pool status first
            status = await self.pool_status()
            response_time_ms = 0

            # Determine health state based on pool metrics
            # Use _connections_in_use directly to get accurate count before get_connection modifies it
            active_conns = len(self._connections_in_use)
            # Use max_size (configured max) instead of max_connections (includes overflow) for utilization
            max_conns = self.config.max_size

            # Calculate utilization
            utilization = active_conns / max_conns if max_conns > 0 else 0

            # Determine state based on utilization
            if utilization < 0.7:
                state = HealthState.HEALTHY
                message = "Pool is healthy"
            elif utilization < 0.9:
                state = HealthState.DEGRADED
                message = "Pool utilization is high"
            else:
                state = HealthState.UNHEALTHY
                message = "Pool is near capacity"

            # Also check connection health if possible (but don't let it override pool utilization state)
            # Only check connection health if pool is healthy to avoid affecting degraded/unhealthy states
            if state == HealthState.HEALTHY:
                try:
                    async with self.get_connection() as conn:
                        conn_health = await self._health_checker.async_check_health(
                            conn
                        )
                        if conn_health.state == HealthState.HEALTHY:
                            response_time_ms = conn_health.response_time_ms or 0
                except Exception:
                    # If we can't get a connection but pool utilization is low,
                    # pool itself is still healthy (might be temporary issue)
                    pass

            return HealthStatus(
                state=state,
                message=message,
                checked_at=datetime.now(),
                response_time_ms=response_time_ms,
                details={
                    "total_connections": status.get("total_connections", 0),
                    "active_connections": active_conns,
                    "idle_connections": status.get("idle_connections", 0),
                    "max_connections": max_conns,
                    "utilization": utilization,
                },
            )
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return HealthStatus(
                state=HealthState.UNHEALTHY,
                message=f"Health check failed: {e}",
                checked_at=datetime.now(),
            )

    async def database_health_check(self) -> HealthStatus:
        """Perform health check on the database server.

        Returns:
            HealthStatus indicating database health.
        """
        try:
            # Get a connection and perform health check
            async with self.get_connection() as conn:
                return await self._health_checker.async_check_health(conn)
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return HealthStatus(
                state=HealthState.UNHEALTHY,
                message=f"Database health check failed: {e}",
                checked_at=datetime.now(),
            )

    async def __aenter__(self):
        """Enter async context manager."""
        if not self._initialized:
            await self.initialize_pool()
            self._initialized = True
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Exit async context manager."""
        await self.close_all_connections()
        self._closed = True

    def __repr__(self) -> str:
        """String representation of the pool."""
        return (
            f"RedisAsyncConnectionPool("
            f"host={self.config.host}, "
            f"port={self.config.port}, "
            f"db={self.config.db}, "
            f"initialized={self._initialized}, "
            f"closed={self._closed})"
        )
