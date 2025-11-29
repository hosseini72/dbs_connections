from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from .config import BasePoolConfig
from .health import HealthStatus


class BaseAsyncConnectionPool(ABC):
    """Base class for async connection pools.
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

    Context manager support for automatic resource
    management.

    This class provides both lazy and eager initialization
    strategies.
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
    async def initialize_pool(self):
        """Initialize the connection pool.
        In lazy initialization, this method is called when the first
        connection is requested.
        In eager initialization, this method is called during the
        construction of the pool explicitly.
        """
        pass

    @abstractmethod
    @asynccontextmanager
    async def get_connection(self):
        """
        Get a connection from the pool.
        """
        pass

    @abstractmethod
    async def release_connection(self, connection):
        """
        Release a connection back to the pool.
        """
        pass

    @abstractmethod
    async def close_connection(self, connection):
        """
        Close a specific connection.
        """
        pass

    @abstractmethod
    async def close_all_connections(self):
        """
        Close all connections in the pool.
        """
        pass

    @abstractmethod
    async def pool_status(self) -> dict:
        """
        Get the current status of the connection pool.

        Returns:
            dict: A dictionary containing pool status information.
        """
        pass

    @abstractmethod
    async def validate_connection(self, connection) -> bool:
        """
        Validate a given connection.

        Args:
            connection: The connection to validate.

        Returns:
            bool: True if the connection is valid, False otherwise.
        """
        pass

    @abstractmethod
    async def health_check(self) -> HealthStatus:
        """
        Perform a health check on the connection pool.

        Returns:
            HealthStatus: The result of the health check.
        """
        pass

    # Context manager support for pool itself
    @abstractmethod
    async def __aenter__(self):
        """Enter the runtime context related to this object."""
        if not self._initialized:
            await self.initialize_pool()
            self._initialized = True
        return self

    @abstractmethod
    async def __aexit__(self, exc_type, exc_value, traceback):
        """Exit the runtime context related to this object."""
        await self.close_all_connections()
        self._closed = True
