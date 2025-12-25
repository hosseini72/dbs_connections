"""ClickHouse connection pool implementation (Synchronous and Asynchronous)."""

import asyncio
import logging
import threading
import time
from contextlib import contextmanager, asynccontextmanager
from datetime import datetime
from typing import Optional, Dict, Any
from queue import Queue

# Sync imports
try:
    import clickhouse_connect
    SYNC_AVAILABLE = True
except ImportError:
    SYNC_AVAILABLE = False
    clickhouse_connect = None

# Async imports (clickhouse-connect supports async via get_async_client)
ASYNC_AVAILABLE = SYNC_AVAILABLE  # Same library, different function

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
from db_connections.scr.all_db_connectors.connectors.clickhouse.config import ClickHousePoolConfig
from db_connections.scr.all_db_connectors.connectors.clickhouse.health import ClickHouseHealthChecker

logger = logging.getLogger(__name__)


class ClickHouseSyncConnectionPool(BaseSyncConnectionPool):
    """Synchronous ClickHouse connection pool using clickhouse-connect.
    
    Features:
    - Connection pooling with min/max size control
    - Automatic connection validation and recycling
    - Health checks and metrics
    - Thread-safe operations
    - Retry logic with exponential backoff
    - Context manager support
    
    Example:
        >>> config = ClickHousePoolConfig(
        ...     host="localhost",
        ...     port=9000,
        ...     database="default"
        ... )
        >>> 
        >>> # Lazy initialization
        >>> pool = ClickHouseSyncConnectionPool(config)
        >>> with pool.get_connection() as client:
        ...     result = client.query("SELECT 1")
        >>> 
        >>> # Eager initialization with context manager
        >>> with ClickHouseSyncConnectionPool(config) as pool:
        ...     with pool.get_connection() as client:
        ...         result = client.query("SELECT 1")
    """
    
    def __init__(self, config: ClickHousePoolConfig):
        """Initialize ClickHouse connection pool.
        
        Args:
            config: ClickHouse pool configuration.
        """
        if not SYNC_AVAILABLE:
            raise ImportError(
                "clickhouse-connect is required for synchronous ClickHouse connections. "
                "Install it with: pip install clickhouse-connect"
            )
        
        super().__init__(config)
        self.config: ClickHousePoolConfig = config
        
        # Validate configuration
        validate_pool_config(config)
        
        # Connection pool (queue-based pool)
        self._pool: Optional[Queue] = None
        
        # Connection metadata tracking
        self._connection_metadata: Dict[int, ConnectionMetadata] = {}
        self._metadata_lock = threading.Lock()
        
        # Metrics tracking
        self._total_connections_created = 0
        self._total_connections_closed = 0
        self._wait_times = []
        self._wait_times_lock = threading.Lock()
        
        # Health checker
        self._health_checker: Optional[ClickHouseHealthChecker] = None
        
        logger.info(
            f"ClickHouse connection pool initialized: "
            f"{config.host}:{config.port}/{config.database}"
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
                f"Initializing ClickHouse pool: "
                f"min={self.config.min_size}, max={self.config.max_size}"
            )
            
            conn_params = self.config.get_connection_params()
            
            # Create queue-based pool
            max_pool_size = self.config.max_size + self.config.max_overflow
            self._pool = Queue(maxsize=max_pool_size)
            
            # Create initial connections
            for _ in range(self.config.min_size):
                try:
                    client = clickhouse_connect.get_client(**conn_params)
                    # Test connection
                    client.ping()
                    self._pool.put(client)
                    conn_id = id(client)
                    with self._metadata_lock:
                        self._connection_metadata[conn_id] = ConnectionMetadata()
                    self._total_connections_created += 1
                except Exception as e:
                    logger.warning(f"Failed to create initial connection: {e}")
                    # Continue with other connections
            
            if self._total_connections_created == 0:
                raise ConnectionError("Failed to create any initial connections")
            
            self._initialized = True
            
            # Initialize health checker after pool is created
            self._health_checker = ClickHouseHealthChecker(self)
            
            logger.info("ClickHouse pool initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize ClickHouse pool: {e}")
            raise ConnectionError(f"Pool initialization failed: {e}") from e
    
    @contextmanager
    def get_connection(self):
        """Get a connection from the pool.
        
        Yields:
            ClickHouse client object.
        
        Raises:
            PoolTimeoutError: If no connection available within timeout.
            ConnectionError: If connection acquisition fails.
            
        Example:
            >>> with pool.get_connection() as client:
            ...     result = client.query("SELECT 1")
        """
        if not self._initialized:
            self.initialize_pool()
        
        if self._closed:
            raise ConnectionError("Pool is closed")
        
        if self._pool is None:
            raise ConnectionError("Pool not initialized")
        
        connection = None
        start_time = time.time()
        attempt = 0
        
        try:
            while attempt <= self.config.max_retries:
                try:
                    # Try to get connection from queue with timeout
                    try:
                        connection = self._pool.get(timeout=self.config.timeout)
                    except:
                        # Queue is empty, create new connection if under max
                        conn_params = self.config.get_connection_params()
                        connection = clickhouse_connect.get_client(**conn_params)
                        connection.ping()  # Test connection
                        conn_id = id(connection)
                        with self._metadata_lock:
                            self._connection_metadata[conn_id] = ConnectionMetadata()
                        self._total_connections_created += 1
                    
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
                            connection = clickhouse_connect.get_client(**conn_params)
                            connection.ping()
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
                        raise ConnectionError(f"Connection acquisition failed: {e}") from e
                    
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
                                except:
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
                            except:
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
            connection: ClickHouse client to validate.
        
        Returns:
            True if connection is valid, False otherwise.
        """
        try:
            # Execute ping
            connection.ping()
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
    
    def database_health_check(self) -> HealthStatus:
        """Perform health check on the database server.
        
        Returns:
            HealthStatus indicating database health.
        """
        try:
            if self._health_checker:
                return self._health_checker.check_database()
            else:
                # Fallback if health checker not initialized
                with self.get_connection() as client:
                    client.ping()
                return HealthStatus(
                    state=HealthState.HEALTHY,
                    message="Database is healthy",
                    checked_at=datetime.now(),
                )
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
            f"ClickHouseSyncConnectionPool("
            f"host={self.config.host}, "
            f"port={self.config.port}, "
            f"database={self.config.database}, "
            f"initialized={self._initialized}, "
            f"closed={self._closed})"
        )


class ClickHouseAsyncConnectionPool(BaseAsyncConnectionPool):
    """Asynchronous ClickHouse connection pool using clickhouse-connect.
    
    Features:
    - Async/await connection pooling
    - Automatic connection validation and recycling
    - Health checks and metrics
    - Retry logic with exponential backoff
    - Context manager support
    - High performance with async operations
    
    Example:
        >>> config = ClickHousePoolConfig(
        ...     host="localhost",
        ...     port=9000,
        ...     database="default"
        ... )
        >>> 
        >>> # Lazy initialization
        >>> pool = ClickHouseAsyncConnectionPool(config)
        >>> async with pool.get_connection() as client:
        ...     result = await client.query("SELECT 1")
        >>> 
        >>> # Eager initialization with context manager
        >>> async with ClickHouseAsyncConnectionPool(config) as pool:
        ...     async with pool.get_connection() as client:
        ...         result = await client.query("SELECT 1")
    """
    
    def __init__(self, config: ClickHousePoolConfig):
        """Initialize async ClickHouse connection pool.
        
        Args:
            config: ClickHouse pool configuration.
        """
        if not ASYNC_AVAILABLE:
            raise ImportError(
                "clickhouse-connect is required for asynchronous ClickHouse connections. "
                "Install it with: pip install clickhouse-connect"
            )
        
        super().__init__(config)
        self.config: ClickHousePoolConfig = config
        
        # Validate configuration
        validate_pool_config(config)
        
        # Connection pool (async queue-based pool)
        self._pool: Optional[asyncio.Queue] = None
        
        # Connection metadata tracking (lazy initialization to avoid event loop requirement)
        self._connection_metadata: Dict[int, ConnectionMetadata] = {}
        self._metadata_lock: Optional[asyncio.Lock] = None
        
        # Metrics tracking
        self._total_connections_created = 0
        self._total_connections_closed = 0
        self._wait_times = []
        self._wait_times_lock: Optional[asyncio.Lock] = None
        
        # Health checker
        self._health_checker: Optional[ClickHouseHealthChecker] = None
        
        logger.info(
            f"Async ClickHouse connection pool initialized: "
            f"{config.host}:{config.port}/{config.database}"
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
        
        Creates an async queue-based connection pool and populates it with
        initial connections.
        
        Raises:
            ConnectionError: If pool initialization fails.
        """
        if self._initialized:
            logger.warning("Pool already initialized")
            return
        
        try:
            logger.info(
                f"Initializing async ClickHouse pool: "
                f"min={self.config.min_size}, max={self.config.max_size}"
            )
            
            conn_params = self.config.get_connection_params()
            
            # Create async queue-based pool
            max_pool_size = self.config.max_size + self.config.max_overflow
            self._pool = asyncio.Queue(maxsize=max_pool_size)
            
            # Create initial connections
            for _ in range(self.config.min_size):
                try:
                    client = await clickhouse_connect.get_async_client(**conn_params)
                    # Test connection
                    await client.ping()
                    await self._pool.put(client)
                    conn_id = id(client)
                    async with self._get_metadata_lock():
                        self._connection_metadata[conn_id] = ConnectionMetadata()
                    self._total_connections_created += 1
                except Exception as e:
                    logger.warning(f"Failed to create initial connection: {e}")
                    # Continue with other connections
            
            if self._total_connections_created == 0:
                raise ConnectionError("Failed to create any initial connections")
            
            self._initialized = True
            
            # Initialize health checker after pool is created
            self._health_checker = ClickHouseHealthChecker(self)
            
            logger.info("Async ClickHouse pool initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize async ClickHouse pool: {e}")
            raise ConnectionError(f"Pool initialization failed: {e}") from e
    
    @asynccontextmanager
    async def get_connection(self):
        """Get a connection from the pool.
        
        Yields:
            Async ClickHouse client object.
        
        Raises:
            PoolTimeoutError: If no connection available within timeout.
            ConnectionError: If connection acquisition fails.
            
        Example:
            >>> async with pool.get_connection() as client:
            ...     result = await client.query("SELECT 1")
        """
        if not self._initialized:
            await self.initialize_pool()
        
        if self._closed:
            raise ConnectionError("Pool is closed")
        
        if self._pool is None:
            raise ConnectionError("Pool not initialized")
        
        connection = None
        start_time = time.time()
        
        try:
            # Try to get connection from queue with timeout
            try:
                connection = await asyncio.wait_for(
                    self._pool.get(),
                    timeout=self.config.timeout
                )
            except asyncio.TimeoutError:
                # Queue is empty, create new connection if under max
                conn_params = self.config.get_connection_params()
                connection = await clickhouse_connect.get_async_client(**conn_params)
                await connection.ping()  # Test connection
                conn_id = id(connection)
                async with self._get_metadata_lock():
                    self._connection_metadata[conn_id] = ConnectionMetadata()
                self._total_connections_created += 1
            
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
                if not await self.validate_connection(connection):
                    logger.warning("Connection validation failed, recreating")
                    try:
                        await connection.close()
                    except Exception:
                        pass
                    # Create new connection
                    conn_params = self.config.get_connection_params()
                    connection = await clickhouse_connect.get_async_client(**conn_params)
                    await connection.ping()
                    conn_id = id(connection)
                    async with self._get_metadata_lock():
                        if conn_id not in self._connection_metadata:
                            self._connection_metadata[conn_id] = ConnectionMetadata()
                    self._total_connections_created += 1
            
            # Track connection metadata
            conn_id = id(connection)
            async with self._get_metadata_lock():
                if conn_id in self._connection_metadata:
                    self._connection_metadata[conn_id].mark_used()
            
            self._connections_in_use.add(conn_id)
            
            logger.debug(f"Async connection acquired: {conn_id}")
            
            # Yield connection to user
            yield connection
            
        except Exception as e:
            logger.error(f"Connection error: {e}")
            raise ConnectionError(f"Connection failed: {e}") from e
        
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
                                metadata.to_dict(), 
                                self.config
                            ):
                                logger.debug(f"Recycling async connection: {conn_id}")
                                try:
                                    await connection.close()
                                except Exception:
                                    pass
                                del self._connection_metadata[conn_id]
                                self._total_connections_closed += 1
                            else:
                                # Return to pool
                                try:
                                    self._pool.put_nowait(connection)
                                except:
                                    # Pool is full, close connection
                                    try:
                                        await connection.close()
                                    except Exception:
                                        pass
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
        if connection and self._pool:
            try:
                conn_id = id(connection)
                
                async with self._get_metadata_lock():
                    if conn_id in self._connection_metadata:
                        metadata = self._connection_metadata[conn_id]
                        metadata.mark_released()
                        
                        if should_recycle_connection(metadata.to_dict(), self.config):
                            try:
                                await connection.close()
                            except Exception:
                                pass
                            del self._connection_metadata[conn_id]
                            self._total_connections_closed += 1
                        else:
                            try:
                                self._pool.put_nowait(connection)
                            except:
                                # Pool is full, close connection
                                try:
                                    await connection.close()
                                except Exception:
                                    pass
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
                await connection.close()
                
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
                
                # Close all connections in queue
                while not self._pool.empty():
                    try:
                        conn = self._pool.get_nowait()
                        await conn.close()
                    except Exception:
                        pass
                
                async with self._get_metadata_lock():
                    self._connection_metadata.clear()
                
                self._connections_in_use.clear()
                self._closed = True
                self._pool = None
                
                logger.info("All async connections closed")
                
            except Exception as e:
                logger.error(f"Error closing all async connections: {e}")
    
    async def pool_status(self) -> dict:
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
            wait_queue_size=self._pool.qsize() if self._pool else 0,
            average_wait_time_ms=avg_wait_time,
        )
    
    async def validate_connection(self, connection) -> bool:
        """Validate a connection is still usable.
        
        Args:
            connection: Async ClickHouse client to validate.
        
        Returns:
            True if connection is valid, False otherwise.
        """
        try:
            # Execute ping
            await connection.ping()
            return True
            
        except Exception as e:
            logger.warning(f"Connection validation failed: {e}")
            return False
    
    async def health_check(self) -> HealthStatus:
        """Perform health check on the pool.
        
        Returns:
            HealthStatus indicating pool health.
        """
        try:
            if self._health_checker:
                return await self._health_checker.async_check_pool()
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
    
    async def database_health_check(self) -> HealthStatus:
        """Perform health check on the database server.
        
        Returns:
            HealthStatus indicating database health.
        """
        try:
            if self._health_checker:
                return await self._health_checker.async_check_database()
            else:
                # Fallback if health checker not initialized
                async with self.get_connection() as client:
                    await client.ping()
                return HealthStatus(
                    state=HealthState.HEALTHY,
                    message="Database is healthy",
                    checked_at=datetime.now(),
                )
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
            f"ClickHouseAsyncConnectionPool("
            f"host={self.config.host}, "
            f"port={self.config.port}, "
            f"database={self.config.database}, "
            f"initialized={self._initialized}, "
            f"closed={self._closed})"
        )

