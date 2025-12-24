"""PostgreSQL connection pool implementation (Synchronous)."""

import logging
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Optional, Any, Dict

try:
    import psycopg2
    from psycopg2 import pool as psycopg2_pool
    from psycopg2.extras import RealDictCursor
except ImportError:
    raise ImportError(
        "psycopg2 is required for synchronous PostgreSQL connections. "
        "Install it with: pip install psycopg2-binary"
    )

from db_connections.scr.all_db_connectors.core.base_sync import BaseSyncConnectionPool
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
from db_connections.scr.all_db_connectors.connectors.postgres.config import PostgresPoolConfig
from db_connections.scr.all_db_connectors.connectors.postgres.health import PostgresHealthChecker

logger = logging.getLogger(__name__)


class PostgresConnectionPool(BaseSyncConnectionPool):
    """Synchronous PostgreSQL connection pool using psycopg2.
    
    Features:
    - Connection pooling with min/max size control
    - Automatic connection validation and recycling
    - Health checks and metrics
    - Thread-safe operations
    - Retry logic with exponential backoff
    - Context manager support
    
    Example:
        >>> config = PostgresPoolConfig(
        ...     host="localhost",
        ...     database="mydb",
        ...     user="user",
        ...     password="pass"
        ... )
        >>> 
        >>> # Lazy initialization
        >>> pool = PostgresConnectionPool(config)
        >>> with pool.get_connection() as conn:
        ...     cursor = conn.cursor()
        ...     cursor.execute("SELECT * FROM users")
        ...     results = cursor.fetchall()
        >>> 
        >>> # Eager initialization with context manager
        >>> with PostgresConnectionPool(config) as pool:
        ...     with pool.get_connection() as conn:
        ...         cursor = conn.cursor()
        ...         cursor.execute("SELECT 1")
    """
    
    def __init__(self, config: PostgresPoolConfig):
        """Initialize PostgreSQL connection pool.
        
        Args:
            config: PostgreSQL pool configuration.
        """
        super().__init__(config)
        self.config: PostgresPoolConfig = config
        
        # Validate configuration
        validate_pool_config(config)
        
        # Connection pool
        self._pool: Optional[psycopg2_pool.ThreadedConnectionPool] = None
        
        # Connection metadata tracking
        self._connection_metadata: Dict[int, ConnectionMetadata] = {}
        self._metadata_lock = threading.Lock()
        
        # Metrics tracking
        self._total_connections_created = 0
        self._total_connections_closed = 0
        self._wait_times = []
        self._wait_times_lock = threading.Lock()
        
        # Health checker
        self._health_checker = PostgresHealthChecker(self)
        
        logger.info(
            f"PostgreSQL connection pool initialized: "
            f"{config.host}:{config.port}/{config.database}"
        )
    
    def initialize_pool(self):
        """Initialize the connection pool.
        
        Creates the underlying psycopg2 connection pool with configured
        min/max connections.
        
        Raises:
            ConnectionError: If pool initialization fails.
        """
        if self._initialized:
            logger.warning("Pool already initialized")
            return
        
        try:
            logger.info(
                f"Initializing PostgreSQL pool: "
                f"min={self.config.min_size}, max={self.config.max_size}"
            )
            
            conn_params = self.config.get_connection_params()
            
            # Create threaded connection pool
            self._pool = psycopg2_pool.ThreadedConnectionPool(
                minconn=self.config.min_size,
                maxconn=self.config.max_size + self.config.max_overflow,
                **conn_params
            )
            
            self._initialized = True
            self._total_connections_created += self.config.min_size
            
            logger.info("PostgreSQL pool initialized successfully")
            
        except psycopg2.Error as e:
            logger.error(f"Failed to initialize PostgreSQL pool: {e}")
            raise ConnectionError(f"Pool initialization failed: {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error initializing pool: {e}")
            raise ConnectionError(f"Pool initialization failed: {e}") from e
    
    @contextmanager
    def get_connection(self):
        """Get a connection from the pool.
        
        Yields:
            psycopg2 connection object.
        
        Raises:
            PoolTimeoutError: If no connection available within timeout.
            ConnectionError: If connection acquisition fails.
            
        Example:
            >>> with pool.get_connection() as conn:
            ...     cursor = conn.cursor()
            ...     cursor.execute("SELECT 1")
        """
        if not self._initialized:
            self.initialize_pool()
        
        if self._closed:
            raise ConnectionError("Pool is closed")
        
        connection = None
        start_time = time.time()
        attempt = 0
        
        while attempt <= self.config.max_retries:
            try:
                # Try to get connection with timeout
                connection = self._pool.getconn()
                
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
                        self._pool.putconn(connection, close=True)
                        connection = self._pool.getconn()
                
                # Track connection metadata
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
                
            except psycopg2.OperationalError as e:
                attempt += 1
                if attempt > self.config.max_retries:
                    logger.error(f"Failed to acquire connection after {attempt} attempts")
                    raise ConnectionError(f"Connection acquisition failed: {e}") from e
                
                logger.warning(f"Connection attempt {attempt} failed, retrying: {e}")
                time.sleep(self.config.retry_delay * (self.config.retry_backoff ** (attempt - 1)))
            except psycopg2.InterfaceError as e:
                attempt += 1
                if attempt > self.config.max_retries:
                    logger.error(f"Failed to acquire connection after {attempt} attempts")
                    raise ConnectionError(f"Connection acquisition failed: {e}") from e
                
                logger.warning(f"Connection attempt {attempt} failed, retrying: {e}")
                time.sleep(self.config.retry_delay * (self.config.retry_backoff ** (attempt - 1)))
            except psycopg2.DatabaseError as e:
                attempt += 1
                if attempt > self.config.max_retries:
                    logger.error(f"Failed to acquire connection after {attempt} attempts")
                    raise ConnectionError(f"Connection acquisition failed: {e}") from e
                
                logger.warning(f"Connection attempt {attempt} failed, retrying: {e}")
                time.sleep(self.config.retry_delay * (self.config.retry_backoff ** (attempt - 1)))

            except (PoolTimeoutError, PoolExhaustedError):
                raise

            except Exception as e:
                logger.error(f"Unexpected error acquiring connection: {e}")
                raise ConnectionError(f"Connection acquisition failed: {e}") from e
            
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
                                    self._pool.putconn(connection, close=True)
                                    del self._connection_metadata[conn_id]
                                    self._total_connections_closed += 1
                                else:
                                    self._pool.putconn(connection)
                        
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
                            self._pool.putconn(connection, close=True)
                            del self._connection_metadata[conn_id]
                            self._total_connections_closed += 1
                        else:
                            self._pool.putconn(connection)
                
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
                self._pool.putconn(connection, close=True)
                
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
                self._pool.closeall()
                
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
            wait_queue_size=0,  # psycopg2 doesn't expose this
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
            # Execute simple query
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            cursor.close()
            
            return result and result[0] == 1
            
        except Exception as e:
            logger.warning(f"Connection validation failed: {e}")
            return False
    
    def health_check(self) -> HealthStatus:
        """Perform health check on the pool.
        
        Returns:
            HealthStatus indicating pool health.
        """
        try:
            return self._health_checker.check_pool()
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
            return self._health_checker.check_database()
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
            f"PostgresConnectionPool("
            f"host={self.config.host}, "
            f"database={self.config.database}, "
            f"initialized={self._initialized}, "
            f"closed={self._closed})"
        )