"""ClickHouse health check implementation."""

import asyncio
import time
from datetime import datetime
from typing import Optional, Dict, Any, Union, TYPE_CHECKING

try:
    # ClickHouse client type (from clickhouse-connect)
    pass
except ImportError:
    pass

from db_connections.scr.all_db_connectors.core import HealthState, HealthStatus
from db_connections.scr.all_db_connectors.core import HealthCheckError

if TYPE_CHECKING:
    from .pool import ClickHouseSyncConnectionPool, ClickHouseAsyncConnectionPool


class ClickHouseHealthChecker:
    """Health checker for ClickHouse connections and pools."""

    def __init__(
        self,
        pool_or_config: Union[
            "ClickHouseSyncConnectionPool",
            "ClickHouseAsyncConnectionPool",
            "ClickHousePoolConfig",
        ],
    ):
        """Initialize health checker.

        Args:
            pool_or_config: ClickHouse connection pool instance or config.
        """
        from .config import ClickHousePoolConfig

        if isinstance(pool_or_config, ClickHousePoolConfig):
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
            connection: ClickHouse client to check.

        Returns:
            HealthStatus indicating connection health.
        """
        start_time = time.time()

        try:
            # Execute ping to verify connection
            connection.ping()

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

    async def async_check_connection(self, connection) -> HealthStatus:
        """Async version: Check health of a single connection.

        Args:
            connection: Async ClickHouse client to check.

        Returns:
            HealthStatus indicating connection health.
        """
        start_time = time.time()

        try:
            # Execute ping
            await connection.ping()
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
                    "utilization_percent": round(utilization * 100, 2),
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
        """Check overall pool health (async version).

        Returns:
            HealthStatus indicating pool health.
        """
        if self.pool is None:
            raise ValueError("Pool instance required for pool health check")

        start_time = time.time()

        try:
            if hasattr(self.pool, "pool_status") and not asyncio.iscoroutinefunction(
                self.pool.pool_status
            ):
                # Sync pool
                status = self.pool.pool_status()
            else:
                # Async pool
                status = await self.pool.pool_status()

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
                    "utilization_percent": round(utilization * 100, 2),
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

        Performs comprehensive checks including:
        - Connection test
        - Server version and status

        Returns:
            HealthStatus indicating database health.
        """
        if self.pool is None:
            raise ValueError("Pool instance required for database health check")

        start_time = time.time()
        details = {}

        try:
            with self.pool.get_connection() as client:
                # Check connection with ping
                client.ping()

                # Get server info
                try:
                    server_info = client.get_server_info()
                    details["server_version"] = (
                        f"{server_info.get('version_major', 'unknown')}."
                        f"{server_info.get('version_minor', 'unknown')}."
                        f"{server_info.get('revision', 'unknown')}"
                    )
                except Exception:
                    # May not be available
                    pass

                # Try to execute a simple query
                try:
                    result = client.query("SELECT 1")
                    details["query_test"] = "success"
                except Exception as e:
                    details["query_test"] = f"failed: {str(e)}"

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
                message=f"Database health check failed: {str(e)}",
                checked_at=datetime.now(),
                response_time_ms=response_time_ms,
                details={"error": str(e), "error_type": type(e).__name__},
            )

    async def async_check_database(self) -> HealthStatus:
        """Check database server health (async version).

        Returns:
            HealthStatus indicating database health.
        """
        if self.pool is None:
            raise ValueError("Pool instance required for database health check")

        start_time = time.time()
        details = {}

        try:
            async with self.pool.get_connection() as client:
                # Check connection with ping
                await client.ping()

                # Get server info
                try:
                    server_info = await client.get_server_info()
                    details["server_version"] = (
                        f"{server_info.get('version_major', 'unknown')}."
                        f"{server_info.get('version_minor', 'unknown')}."
                        f"{server_info.get('revision', 'unknown')}"
                    )
                except Exception:
                    # May not be available
                    pass

                # Try to execute a simple query
                try:
                    result = await client.query("SELECT 1")
                    details["query_test"] = "success"
                except Exception as e:
                    details["query_test"] = f"failed: {str(e)}"

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
                message=f"Database health check failed: {str(e)}",
                checked_at=datetime.now(),
                response_time_ms=response_time_ms,
                details={"error": str(e), "error_type": type(e).__name__},
            )

    def comprehensive_check(self) -> dict:
        """Perform comprehensive health check of all components.

        Returns:
            Dictionary with health status for each component.
        """
        return {
            "pool": self.check_pool(),
            "database": self.check_database(),
            "timestamp": datetime.now(),
        }

    async def async_comprehensive_check(self) -> dict:
        """Perform comprehensive health check of all components (async version).

        Returns:
            Dictionary with health status for each component.
        """
        pool_status, db_status = await asyncio.gather(
            self.async_check_pool(), self.async_check_database(), return_exceptions=True
        )

        # Handle exceptions
        if isinstance(pool_status, Exception):
            pool_status = HealthStatus(
                state=HealthState.UNHEALTHY,
                message=f"Pool check failed: {pool_status}",
                checked_at=datetime.now(),
            )
        if isinstance(db_status, Exception):
            db_status = HealthStatus(
                state=HealthState.UNHEALTHY,
                message=f"Database check failed: {db_status}",
                checked_at=datetime.now(),
            )

        return {
            "pool": pool_status,
            "database": db_status,
            "timestamp": datetime.now(),
        }
