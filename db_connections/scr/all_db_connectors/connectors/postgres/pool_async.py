"""PostgreSQL connection pool implementation (Asynchronous)."""

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional, Dict, Any

try:
    import asyncpg
except ImportError:
    raise ImportError(
        "asyncpg is required for asynchronous PostgreSQL connections. "
        "Install it with: pip install asyncpg"
    )

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
    async_retry_on_failure,
)
from db_connections.scr.all_db_connectors.connectors.postgres.config import PostgresPoolConfig
from db_connections.scr.all_db_connectors.connectors.postgres.health import async_check_connection

logger = logging.getLogger(__name__)


class AsyncPostgresConnectionPool(BaseAsyncConnectionPool):
    """Asynchronous PostgreSQL connection pool using asyncpg.
    
    Features:
    - Async/await connection pooling
    - Automatic connection validation and recycling
    - Health checks and metrics
    - Retry logic with exponential backoff
    - Context manager support
    - High performance with asyncpg
    
    Example:
        >>> config = PostgresPoolConfig(
        ...     host="localhost",
        ...     database="mydb",
        ...     user="user",
        ...     password="pass"
        ... )
        >>> 
        >>> # Lazy initialization
        >>> pool = AsyncPostgresConnectionPool(config)
        >>> async with pool.get_connection() as conn:
        ...     result = await conn.fetch("SELECT * FROM users")
        >>> 
        >>> # Eager initialization with context manager
        >>> async with AsyncPostgresConnectionPool(config) as pool:
        ...     async with pool.get_connection() as conn:
        ...         result = await conn.fetchval("SELECT 1")
    """
    
    def __init__(self, config: PostgresPoolConfig):
        """Initialize async PostgreSQL connection pool.
        
        Args:
            config: PostgreSQL pool configuration.
        """
        super().__init__(config)
        self.config: PostgresPoolConfig = config
        
        # Validate configuration
        validate_pool_config(config)
        
        # Connection pool
        self._pool: Optional[asyncpg.Pool] = None
        
        # Connection metadata tracking (lazy initialization to avoid event loop requirement)
        self._connection_metadata: Dict[int, ConnectionMetadata] = {}
        self._metadata_lock: Optional[asyncio.Lock] = None
        
        # Metrics tracking
        self._total_connections_created = 0
        self._total_connections_closed = 0
        self._wait_times = []
        self._wait_times_lock: Optional[asyncio.Lock] = None
        
        logger.info(
            f"Async PostgreSQL connection pool initialized: "
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
        
        Creates the underlying asyncpg connection pool.
        
        Raises:
            ConnectionError: If pool initialization fails.
        """
        if self._initialized:
            logger.warning("Pool already initialized")
            return
        
        try:
            logger.info(
                f"Initializing async PostgreSQL pool: "
                f"min={self.config.min_size}, max={self.config.max_size}"
            )
            
            # Prepare connection parameters
            conn_params = {
                "host": self.config.host,
                "port": self.config.port,
                "database": self.config.database,
                "user": self.config.user,
                "password": self.config.password,
                "min_size": self.config.min_size,
                "max_size": self.config.max_size + self.config.max_overflow,
                "timeout": self.config.timeout,
                "command_timeout": self.config.command_timeout,
                "max_queries": 50000,  # asyncpg default
                "max_cached_statement_lifetime": self.config.max_cached_statement_lifetime,
            }
            
            # Add SSL mode if not default
            if self.config.sslmode != "prefer":
                conn_params["ssl"] = self.config.sslmode
            
            # Add server settings if provided
            if self.config.server_settings:
                conn_params["server_settings"] = self.config.server_settings
            
            # Create connection pool
            self._pool = await asyncpg.create_pool(**conn_params)
            
            self._initialized = True
            self._total_connections_created += self.config.min_size
            
            logger.info("Async PostgreSQL pool initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize async PostgreSQL pool: {e}")
            raise ConnectionError(f"Pool initialization failed: {e}") from e
    
    @asynccontextmanager
    async def get_connection(self):
        """Get a connection from the pool.
        
        Yields:
            asyncpg connection object.
        
        Raises:
            PoolTimeoutError: If no connection available within timeout.
            ConnectionError: If connection acquisition fails.
            
        Example:
            >>> async with pool.get_connection() as conn:
            ...     result = await conn.fetch("SELECT * FROM users")
        """
        if not self._initialized:
            await self.initialize_pool()
        
        if self._closed:
            raise ConnectionError("Pool is closed")
        
        connection = None
        start_time = time.time()
        
        try:
            # Acquire connection with retry logic
            connection = await self._acquire_with_retry()
            
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
                    logger.warning("Connection validation failed, reconnecting")
                    await self._pool.release(connection, timeout=5)
                    connection = await self._pool.acquire(timeout=self.config.timeout)
            
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
        except Exception as e:
            # Check if it's an asyncpg error (but handle mocks gracefully)
            error_type = type(e).__name__
            if "PostgresError" in error_type or "Postgres" in str(type(e)):
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
                                metadata.to_dict(), 
                                self.config
                            ):
                                logger.debug(f"Recycling async connection: {conn_id}")
                                await self._pool.release(connection, timeout=5)
                                await connection.close()
                                del self._connection_metadata[conn_id]
                                self._total_connections_closed += 1
                            else:
                                await self._pool.release(connection, timeout=5)
                    
                    self._connections_in_use.discard(conn_id)
                    logger.debug(f"Async connection released: {conn_id}")
                    
                except Exception as e:
                    logger.error(f"Error releasing async connection: {e}")
    
    @async_retry_on_failure(max_retries=3, retry_delay=1.0, retry_backoff=2.0)
    async def _acquire_with_retry(self):
        """Acquire connection with automatic retry.
        
        Returns:
            asyncpg connection.
        """
        return await self._pool.acquire(timeout=self.config.timeout)
    
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
                            await self._pool.release(connection, timeout=5)
                            await connection.close()
                            del self._connection_metadata[conn_id]
                            self._total_connections_closed += 1
                        else:
                            await self._pool.release(connection, timeout=5)
                
                self._connections_in_use.discard(conn_id)
                
            except Exception as e:
                logger.error(f"Error releasing async connection: {e}")
    
    async def close_connection(self, connection):
        """Close a specific connection.
        
        Args:
            connection: Connection to close.
        """
        if connection:
            conn_id = id(connection)
            try:
                if self._pool:
                    await self._pool.release(connection, timeout=5)
            except Exception as e:
                logger.warning(f"Error releasing connection from pool: {e}")
            
            # Always try to close the connection
            if hasattr(connection, 'close'):
                try:
                    await connection.close()
                except Exception as e:
                    logger.warning(f"Error calling connection.close(): {e}")
            
            try:
                async with self._get_metadata_lock():
                    if conn_id in self._connection_metadata:
                        del self._connection_metadata[conn_id]
            except Exception as e:
                logger.warning(f"Error removing connection metadata: {e}")
            
            self._connections_in_use.discard(conn_id)
            self._total_connections_closed += 1
            
            logger.debug(f"Async connection closed: {conn_id}")
    
    async def close_all_connections(self):
        """Close all connections in the pool."""
        if self._pool:
            try:
                logger.info("Closing all async connections in pool")
                await self._pool.close()
                
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
        
        pool_size = self._pool.get_size()
        free_size = self._pool.get_idle_size()
        active_conns = len(self._connections_in_use)
        
        return {
            "initialized": self._initialized,
            "closed": self._closed,
            "total_connections": pool_size,
            "active_connections": active_conns,
            "idle_connections": free_size,
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
            wait_queue_size=0,  # asyncpg doesn't expose this directly
            average_wait_time_ms=avg_wait_time,
        )
    
    async def validate_connection(self, connection) -> bool:
        """Validate a connection is still usable.
        
        Args:
            connection: Connection to validate.
        
        Returns:
            True if connection is valid, False otherwise.
        """
        try:
            # Execute simple query
            result = await connection.fetchval("SELECT 1")
            return result == 1
            
        except Exception as e:
            logger.warning(f"Async connection validation failed: {e}")
            return False
    
    async def health_check(self) -> HealthStatus:
        """Perform health check on the pool.
        
        Returns:
            HealthStatus indicating pool health.
        """
        start_time = time.time()
        
        try:
            status = await self.pool_status()
            response_time_ms = (time.time() - start_time) * 1000
            
            # Determine health state based on pool metrics
            active_conns = status["active_connections"]
            max_conns = status["max_connections"]
            
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
            
            return HealthStatus(
                state=state,
                message=message,
                checked_at=datetime.now(),
                response_time_ms=response_time_ms,
                details={
                    "total_connections": status["total_connections"],
                    "active_connections": active_conns,
                    "idle_connections": status["idle_connections"],
                    "max_connections": max_conns,
                    "utilization_percent": round(utilization * 100, 2),
                },
            )
        
        except Exception as e:
            response_time_ms = (time.time() - start_time) * 1000
            return HealthStatus(
                state=HealthState.UNHEALTHY,
                message=f"Pool health check failed: {e}",
                checked_at=datetime.now(),
                response_time_ms=response_time_ms,
                details={"error": str(e), "error_type": type(e).__name__},
            )
    
    async def database_health_check(self) -> HealthStatus:
        """Perform health check on the database server.
        
        Returns:
            HealthStatus indicating database health.
        """
        start_time = time.time()
        details = {}
        
        try:
            async with self.get_connection() as conn:
                # Check connection
                await conn.fetchval("SELECT 1")
                
                # Get PostgreSQL version
                version = await conn.fetchval("SHOW server_version")
                details["server_version"] = version
                
                # Get active connections count
                active_count = await conn.fetchval("""
                    SELECT count(*) 
                    FROM pg_stat_activity 
                    WHERE state = 'active'
                """)
                details["active_queries"] = active_count
                
                # Get database size (may fail with insufficient permissions)
                try:
                    db_size = await conn.fetchval(f"""
                        SELECT pg_size_pretty(pg_database_size('{self.config.database}'))
                    """)
                    details["database_size"] = db_size
                except Exception:
                    pass
                
                # Get connection count
                conn_count = await conn.fetchval("""
                    SELECT count(*) 
                    FROM pg_stat_activity 
                    WHERE datname = $1
                """, self.config.database)
                details["total_db_connections"] = conn_count
            
            response_time_ms = (time.time() - start_time) * 1000
            
            # Determine state based on response time
            if response_time_ms < 100:
                state = HealthState.HEALTHY
                message = "Database is healthy"
            elif response_time_ms < 500:
                state = HealthState.DEGRADED
                message = "Database response is slow"
            else:
                state = HealthState.UNHEALTHY
                message = "Database response is very slow"
            
            return HealthStatus(
                state=state,
                message=message,
                checked_at=datetime.now(),
                response_time_ms=response_time_ms,
                details=details,
            )
        
        except Exception as e:
            response_time_ms = (time.time() - start_time) * 1000
            return HealthStatus(
                state=HealthState.UNHEALTHY,
                message=f"Database health check failed: {e}",
                checked_at=datetime.now(),
                response_time_ms=response_time_ms,
                details={"error": str(e), "error_type": type(e).__name__},
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
            f"AsyncPostgresConnectionPool("
            f"host={self.config.host}, "
            f"database={self.config.database}, "
            f"initialized={self._initialized}, "
            f"closed={self._closed})"
        )