from abc import ABC, abstractmethod
from .config import BasePoolConfig
from .health import HealthStatus
from contextlib import contextmanager


class BaseSyncConnectionPool(ABC):
    """Base class for sync connection pools.
    Lifecycle of connection pool:
    1. Initialize the pool with configuration.
    2. Acquire connections from the pool.
    3. Release connections back to the pool.
    4. Close connections when no longer needed.
    5. Monitor pool status and health.


    Methods to be implemented by subclasses:
    - initialize_pool
    - get_connection
    - release_connection
    - close_connection
    - close_all_connections
    - pool_status
    - validate_connection
    - health_check
    Context manager support for automatic resource management.

    This class provides both Lazy and Eager initialization strategies.
    """

    @abstractmethod
    def __init__(self, config: BasePoolConfig):
        """Initialize the connection pool with the given configuration.
        Args:
            config (BasePoolConfig): Configuration for the connection pool.
        """
        self.config = config
        self._initialized = False
        self._pool = None
        self._connections_in_use = set()
        self._connections_available = set()
        self._closed = False

    @abstractmethod
    def initialize_pool(self):
        """Initialize the connection pool.
        In lazy initialization, this method is called when the first connection is requested. # noqa: E501
        In eager initialization, this method is called during the construction of the pool explicitly. # noqa: E501
        """
        pass

    @abstractmethod
    @contextmanager
    def get_connection(self):
        """
        Get a connection from the pool.

        Usage:
            with pool.connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")

        Yields:
            Database connection

        Raises:
            PoolTimeoutError: No connection available within timeout
            ConnectionError: Failed to establish connection
        """
        # Initialize the pool if not already done, in case of lazy initialization  # noqa: E501
        if not self._initialized:
            self.initialize_pool()
        pass

    @abstractmethod
    def release_connection(self, connection):
        """Release a connection back to the pool."""
        pass

    @abstractmethod
    def close_connection(self, connection):
        """Close a specific connection."""
        pass

    @abstractmethod
    def close_all_connections(self):
        """Close all connections in the pool."""
        pass

    @abstractmethod
    def pool_status(self) -> dict:
        """Get the current status of the connection pool."""
        pass

    @abstractmethod
    def validate_connection(self, connection) -> bool:
        """Internal: Check if connection is still valid.

        Args:
            conn: Connection to validate

        Returns:
            True if connection is usable, False otherwise."""
        pass

    @abstractmethod
    def health_check(self) -> HealthStatus:
        """
        Check pool health.

        Returns:
            Health status with details
        """
        pass

    # Context manager support for pool itself
    @abstractmethod
    def __enter__(self):
        """Enter the runtime context related to this object."""
        if not self._initialized:
            self.initialize_pool()
            self._initialized = True
        return self

    @abstractmethod
    def __exit__(self, exc_type, exc_value, traceback):
        """Exit the runtime context related to this object."""
        self.close_all_connections()
        self._closed = True
