"""Neo4j health check implementation."""

import asyncio
import time
from datetime import datetime
from typing import Optional, Dict, Any, Union, TYPE_CHECKING

try:
    # Neo4j driver type
    pass
except ImportError:
    pass

from db_connections.scr.all_db_connectors.core import HealthState, HealthStatus

if TYPE_CHECKING:
    from .pool import Neo4jSyncConnectionPool, Neo4jAsyncConnectionPool


class Neo4jHealthChecker:
    """Health checker for Neo4j connections and pools."""

    def __init__(
        self,
        pool_or_config: Union[
            "Neo4jSyncConnectionPool", "Neo4jAsyncConnectionPool", "Neo4jPoolConfig"
        ],
    ):
        """Initialize health checker.

        Args:
            pool_or_config: Neo4j connection pool instance or config.
        """
        from .config import Neo4jPoolConfig

        if isinstance(pool_or_config, Neo4jPoolConfig):
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
            connection: Neo4j driver to check.

        Returns:
            HealthStatus indicating connection health.
        """
        start_time = time.time()

        try:
            # Check if connection is closed - do this after getting start_time for accurate timing
            if getattr(connection, "closed", False):
                end_time = time.time()
                response_time_ms = (end_time - start_time) * 1000
                # Ensure response_time_ms is at least 0.001
                if response_time_ms < 0.001:
                    response_time_ms = 0.001
                return HealthStatus(
                    state=HealthState.UNHEALTHY,
                    message="Connection is closed",
                    checked_at=datetime.now(),
                    response_time_ms=response_time_ms,
                )

            # Execute verify_connectivity to verify connection
            connection.verify_connectivity()

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
            # Check if response time is effectively 0 (very small due to fast execution)
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
            connection: Async Neo4j driver to check.

        Returns:
            HealthStatus indicating connection health.
        """
        start_time = time.time()

        try:
            # Execute verify_connectivity
            await connection.verify_connectivity()
            response_time_ms = (time.time() - start_time) * 1000

            return HealthStatus(
                state=HealthState.HEALTHY,
                message="Connection is healthy",
                checked_at=datetime.now(),
                response_time_ms=response_time_ms,
            )

        except Exception as e:
            response_time_ms = (time.time() - start_time) * 1000
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
            with self.pool.get_connection() as driver:
                # Run a simple query to check database health
                with driver.session() as session:
                    result = session.run("RETURN 1 as health_check")
                    result.consume()  # Consume the result

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
            async with self.pool.get_connection() as driver:
                # Run a simple query to check database health
                async with driver.session() as session:
                    result = await session.run("RETURN 1 as health_check")
                    await result.consume()  # Consume the result

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

    def check_health(self, connection) -> HealthStatus:
        """Check health of a single connection.

        Args:
            connection: Neo4j driver to check.

        Returns:
            HealthStatus indicating connection health.
        """
        return self.check_connection(connection)

    async def async_check_health(self, connection) -> HealthStatus:
        """Async version: Check health of a single connection.

        Args:
            connection: Async Neo4j driver to check.

        Returns:
            HealthStatus indicating connection health.
        """
        return await self.async_check_connection(connection)

    def check_server_info(self, driver) -> Optional[Dict[str, Any]]:
        """Check Neo4j server information.

        Args:
            driver: Neo4j driver instance.

        Returns:
            Dictionary with server information, or None on error.
        """
        try:
            # Check if driver has session method
            if not hasattr(driver, "session") or not callable(
                getattr(driver, "session", None)
            ):
                return None

            # Get server info from driver
            # Neo4j driver doesn't expose server info directly, so we return basic info
            info = {
                "driver_type": type(driver).__name__,
                "connected": not getattr(driver, "closed", False),
            }
            return info
        except Exception:
            return None

    def check_database_status(self, driver) -> Optional[Dict[str, Any]]:
        """Check database status.

        Args:
            driver: Neo4j driver instance.

        Returns:
            Dictionary with database status, or None on error.
        """
        try:
            # Check if driver has session method
            if not hasattr(driver, "session") or not callable(
                getattr(driver, "session", None)
            ):
                return None

            # Run a simple query to check database status
            session = driver.session()
            # Handle both context manager and direct call
            if hasattr(session, "__enter__"):
                with session as s:
                    result = s.run("RETURN 1 as status")
                    if hasattr(result, "consume"):
                        result.consume()
            else:
                # Direct call if not a context manager
                result = session.run("RETURN 1 as status")
                if hasattr(result, "consume"):
                    result.consume()

            return {
                "status": "online",
                "accessible": True,
            }
        except Exception:
            return None

    def check_query_performance(self, driver, query: str) -> Optional[Dict[str, Any]]:
        """Check query performance.

        Args:
            driver: Neo4j driver instance.
            query: Cypher query to test.

        Returns:
            Dictionary with query performance metrics, or None on error.
        """
        try:
            # Check if driver has session method
            if not hasattr(driver, "session") or not callable(
                getattr(driver, "session", None)
            ):
                return None

            start_time = time.time()
            with driver.session() as session:
                result = session.run(query)
                if hasattr(result, "consume"):
                    result.consume()
            execution_time_ms = (time.time() - start_time) * 1000

            return {
                "query": query,
                "execution_time_ms": execution_time_ms,
                "success": True,
            }
        except Exception:
            # Return None on error (not a dict with error info)
            return None

    def comprehensive_check(self, driver) -> Dict[str, Any]:
        """Perform comprehensive health check.

        Args:
            driver: Neo4j driver instance.

        Returns:
            Dictionary with comprehensive health information.
        """
        connection_status = self.check_health(driver)
        server_info = self.check_server_info(driver)

        return {
            "connection": connection_status,  # Return HealthStatus object, not dict
            "server": server_info or {},
            "timestamp": datetime.now(),
        }

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
