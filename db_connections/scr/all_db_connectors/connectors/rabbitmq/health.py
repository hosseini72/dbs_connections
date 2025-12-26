"""RabbitMQ health check implementation."""

import asyncio
import time
from datetime import datetime
from typing import Optional, Dict, Any, Union, TYPE_CHECKING

try:
    # RabbitMQ connection type (from pika/aio_pika)
    pass
except ImportError:
    pass

from db_connections.scr.all_db_connectors.core import HealthState, HealthStatus
from db_connections.scr.all_db_connectors.core import HealthCheckError

if TYPE_CHECKING:
    from .pool import RabbitMQSyncConnectionPool, RabbitMQAsyncConnectionPool


class RabbitMQHealthChecker:
    """Health checker for RabbitMQ connections and pools."""

    def __init__(
        self,
        pool_or_config: Union[
            "RabbitMQSyncConnectionPool",
            "RabbitMQAsyncConnectionPool",
            "RabbitMQPoolConfig",
        ],
    ):
        """Initialize health checker.

        Args:
            pool_or_config: RabbitMQ connection pool instance or config.
        """
        from .config import RabbitMQPoolConfig

        if isinstance(pool_or_config, RabbitMQPoolConfig):
            self.config = pool_or_config
            self.pool = None
        else:
            # It's a pool instance
            self.pool = pool_or_config
            self.config = pool_or_config.config

        self._last_check_time: Optional[datetime] = None
        self._last_status: Optional[HealthStatus] = None

    def check_connection(self, connection) -> HealthStatus:
        """Check health of a single connection.

        Args:
            connection: RabbitMQ connection to check.

        Returns:
            HealthStatus indicating connection health.
        """
        start_time = time.time()

        try:
            # Check if connection is open
            if hasattr(connection, "is_closing") and connection.is_closing():
                raise Exception("Connection is closing")
            if hasattr(connection, "is_closed") and connection.is_closed:
                raise Exception("Connection is closed")
            if hasattr(connection, "is_open") and not connection.is_open:
                raise Exception("Connection is not open")

            # Try to create a channel to verify connection
            channel = connection.channel()
            channel.close()

            end_time = time.time()
            response_time_ms = (end_time - start_time) * 1000

            # Determine state based on response time
            # Slow response (>1s) indicates degraded performance
            if response_time_ms > 2000:  # > 2 seconds
                state = HealthState.UNHEALTHY
                message = f"Connection check very slow: {response_time_ms:.2f}ms"
            elif response_time_ms > 1000:  # > 1 second
                state = HealthState.DEGRADED
                message = f"Connection check slow: {response_time_ms:.2f}ms"
            else:
                state = HealthState.HEALTHY
                message = "Connection is healthy"

            # Ensure response_time_ms is at least 0.001 to avoid 0.0
            if response_time_ms < 0.001:
                response_time_ms = 0.001

            return HealthStatus(
                state=state,
                message=message,
                checked_at=datetime.now(),
                response_time_ms=response_time_ms,
            )

        except Exception as e:
            end_time = time.time()
            response_time_ms = (end_time - start_time) * 1000
            # Ensure response_time_ms is at least 0.001
            if response_time_ms < 0.001:
                response_time_ms = 0.001
            return HealthStatus(
                state=HealthState.UNHEALTHY,
                message=f"Connection check failed: {str(e)}",
                checked_at=datetime.now(),
                response_time_ms=response_time_ms,
                details={"error": str(e), "error_type": type(e).__name__},
            )

    async def async_check_connection(self, connection) -> HealthStatus:
        """Async version: Check health of a single connection.

        Args:
            connection: Async RabbitMQ connection to check.

        Returns:
            HealthStatus indicating connection health.
        """
        start_time = time.time()

        try:
            # Check if connection is closed
            if connection.is_closed:
                raise Exception("Connection is closed")

            # Try to create a channel to verify connection
            channel = await connection.channel()
            await channel.close()

            end_time = time.time()
            response_time_ms = (end_time - start_time) * 1000

            # Determine state based on response time
            # Slow response (>1s) indicates degraded performance
            if response_time_ms > 2000:  # > 2 seconds
                state = HealthState.UNHEALTHY
                message = f"Connection check very slow: {response_time_ms:.2f}ms"
            elif response_time_ms > 1000:  # > 1 second
                state = HealthState.DEGRADED
                message = f"Connection check slow: {response_time_ms:.2f}ms"
            else:
                state = HealthState.HEALTHY
                message = "Connection is healthy"

            # Ensure response_time_ms is at least 0.001 to avoid 0.0
            if response_time_ms < 0.001:
                response_time_ms = 0.001

            return HealthStatus(
                state=state,
                message=message,
                checked_at=datetime.now(),
                response_time_ms=response_time_ms,
            )

        except Exception as e:
            end_time = time.time()
            response_time_ms = (end_time - start_time) * 1000
            # Ensure response_time_ms is at least 0.001
            if response_time_ms < 0.001:
                response_time_ms = 0.001
            return HealthStatus(
                state=HealthState.UNHEALTHY,
                message=f"Connection check failed: {str(e)}",
                checked_at=datetime.now(),
                response_time_ms=response_time_ms,
                details={"error": str(e), "error_type": type(e).__name__},
            )

    def check_pool(self) -> HealthStatus:
        """Check overall pool health.

        Returns:
            HealthStatus indicating pool health.
        """
        if self.pool is None:
            raise ValueError("Pool instance required for pool health check")

        start_time = time.time()

        try:
            status = self.pool.pool_status()
            response_time_ms = (time.time() - start_time) * 1000

            # Determine health state based on pool metrics
            total_conns = status.get("total_connections", 0)
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

            return HealthStatus(
                state=state,
                message=message,
                checked_at=datetime.now(),
                response_time_ms=response_time_ms,
                details={
                    "total_connections": total_conns,
                    "active_connections": active_conns,
                    "idle_connections": status.get("idle_connections", 0),
                    "max_connections": max_conns,
                    "min_connections": status.get("min_connections", 0),
                    "utilization": utilization,
                    "initialized": status.get("initialized", False),
                    "closed": status.get("closed", False),
                },
            )

        except Exception as e:
            response_time_ms = (time.time() - start_time) * 1000
            return HealthStatus(
                state=HealthState.UNHEALTHY,
                message=f"Pool health check failed: {str(e)}",
                checked_at=datetime.now(),
                response_time_ms=response_time_ms,
                details={"error": str(e), "error_type": type(e).__name__},
            )

    async def async_check_pool(self) -> HealthStatus:
        """Async version: Check overall pool health.

        Returns:
            HealthStatus indicating pool health.
        """
        if self.pool is None:
            raise ValueError("Pool instance required for pool health check")

        start_time = time.time()

        try:
            # For async pools, pool_status might be async too
            # If it's not, we'll call it synchronously
            if asyncio.iscoroutinefunction(self.pool.pool_status):
                status = await self.pool.pool_status()
            else:
                status = self.pool.pool_status()

            response_time_ms = (time.time() - start_time) * 1000

            # Determine health state based on pool metrics
            total_conns = status.get("total_connections", 0)
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

            return HealthStatus(
                state=state,
                message=message,
                checked_at=datetime.now(),
                response_time_ms=response_time_ms,
                details={
                    "total_connections": total_conns,
                    "active_connections": active_conns,
                    "idle_connections": status.get("idle_connections", 0),
                    "max_connections": max_conns,
                    "min_connections": status.get("min_connections", 0),
                    "utilization": utilization,
                    "initialized": status.get("initialized", False),
                    "closed": status.get("closed", False),
                },
            )

        except Exception as e:
            response_time_ms = (time.time() - start_time) * 1000
            return HealthStatus(
                state=HealthState.UNHEALTHY,
                message=f"Pool health check failed: {str(e)}",
                checked_at=datetime.now(),
                response_time_ms=response_time_ms,
                details={"error": str(e), "error_type": type(e).__name__},
            )

    def check_database(self) -> HealthStatus:
        """Check database server health.

        Returns:
            HealthStatus indicating database server health.
        """
        if self.pool is None:
            raise ValueError("Pool instance required for database health check")

        start_time = time.time()

        try:
            # Use a connection from the pool to check database
            with self.pool.get_connection() as connection:
                # Try to create a channel to check server health
                channel = connection.channel()
                # Declare a test queue to verify server is responding
                channel.queue_declare(
                    queue="health_check",
                    durable=False,
                    auto_delete=True,
                    exclusive=True,
                )
                channel.queue_delete(queue="health_check")
                channel.close()

            response_time_ms = (time.time() - start_time) * 1000

            return HealthStatus(
                state=HealthState.HEALTHY,
                message="Database is healthy",
                checked_at=datetime.now(),
                response_time_ms=response_time_ms,
            )

        except Exception as e:
            response_time_ms = (time.time() - start_time) * 1000
            return HealthStatus(
                state=HealthState.UNHEALTHY,
                message=f"Database health check failed: {str(e)}",
                checked_at=datetime.now(),
                response_time_ms=response_time_ms,
                details={"error": str(e), "error_type": type(e).__name__},
            )

    async def async_check_database(self) -> HealthStatus:
        """Async version: Check database server health.

        Returns:
            HealthStatus indicating database server health.
        """
        if self.pool is None:
            raise ValueError("Pool instance required for database health check")

        start_time = time.time()

        try:
            # Use a connection from the pool to check database
            async with self.pool.get_connection() as connection:
                # Try to create a channel to check server health
                channel = await connection.channel()
                # Declare a test queue to verify server is responding
                queue = await channel.declare_queue(
                    "health_check", durable=False, auto_delete=True
                )
                await queue.delete()
                await channel.close()

            response_time_ms = (time.time() - start_time) * 1000

            return HealthStatus(
                state=HealthState.HEALTHY,
                message="Database is healthy",
                checked_at=datetime.now(),
                response_time_ms=response_time_ms,
            )

        except Exception as e:
            response_time_ms = (time.time() - start_time) * 1000
            return HealthStatus(
                state=HealthState.UNHEALTHY,
                message=f"Database health check failed: {str(e)}",
                checked_at=datetime.now(),
                response_time_ms=response_time_ms,
                details={"error": str(e), "error_type": type(e).__name__},
            )

    def get_last_status(self) -> Optional[HealthStatus]:
        """Get the last health check status.

        Returns:
            Last HealthStatus if available, None otherwise.
        """
        return self._last_status

    def get_last_check_time(self) -> Optional[datetime]:
        """Get the timestamp of the last health check.

        Returns:
            Last check time if available, None otherwise.
        """
        return self._last_check_time

    def check_health(self, connection) -> HealthStatus:
        """Check health of a single connection (alias for check_connection).

        Args:
            connection: RabbitMQ connection to check.

        Returns:
            HealthStatus indicating connection health.
        """
        return self.check_connection(connection)

    async def async_check_health(self, connection) -> HealthStatus:
        """Async version: Check health of a single connection (alias for async_check_connection).

        Args:
            connection: Async RabbitMQ connection to check.

        Returns:
            HealthStatus indicating connection health.
        """
        return await self.async_check_connection(connection)

    def check_queue_status(self, channel, queue_name: str) -> Optional[Dict[str, Any]]:
        """Check status of a specific queue.

        Args:
            channel: RabbitMQ channel to use.
            queue_name: Name of the queue to check.

        Returns:
            Dictionary with queue status information, or None on error.
        """
        try:
            result = channel.queue_declare(queue=queue_name, passive=True)
            return {
                "queue": queue_name,
                "message_count": result.method.message_count,
                "consumer_count": result.method.consumer_count,
            }
        except Exception:
            return None

    def check_exchange_status(
        self, channel, exchange_name: str
    ) -> Optional[Dict[str, Any]]:
        """Check status of a specific exchange.

        Args:
            channel: RabbitMQ channel to use.
            exchange_name: Name of the exchange to check.

        Returns:
            Dictionary with exchange status information, or None on error.
        """
        try:
            channel.exchange_declare(exchange=exchange_name, passive=True)
            return {
                "exchange": exchange_name,
                "exists": True,
            }
        except Exception:
            return None

    def check_server_info(self, connection) -> Optional[Dict[str, Any]]:
        """Get server information from connection.

        Args:
            connection: RabbitMQ connection to check.

        Returns:
            Dictionary with server information, or None on error.
        """
        try:
            if (
                hasattr(connection, "server_properties")
                and connection.server_properties
            ):
                return {
                    "product": connection.server_properties.get("product", "Unknown"),
                    "version": connection.server_properties.get("version", "Unknown"),
                    "platform": connection.server_properties.get("platform", "Unknown"),
                }
            return None
        except Exception:
            return None

    def comprehensive_check(self, connection) -> Dict[str, Any]:
        """Perform a comprehensive health check including connection and server info.

        Args:
            connection: RabbitMQ connection to check.

        Returns:
            Dictionary with comprehensive health information.
        """
        connection_status = self.check_connection(connection)
        server_info = self.check_server_info(connection)

        return {
            "connection": connection_status,
            "server": server_info,
            "timestamp": datetime.now(),
        }
