"""PostgreSQL health check implementation."""

import time
from datetime import datetime
from typing import Optional, TYPE_CHECKING

from db_connections.scr.all_db_connectors.core import HealthState, HealthStatus
from db_connections.scr.all_db_connectors.core import HealthCheckError

if TYPE_CHECKING:
    from .pool import PostgresConnectionPool


class PostgresHealthChecker:
    """Health checker for PostgreSQL connections and pools."""

    def __init__(self, pool: "PostgresConnectionPool"):
        """Initialize health checker.

        Args:
            pool: PostgreSQL connection pool to monitor.
        """
        self.pool = pool

    def check_connection(self, connection) -> HealthStatus:
        """Check health of a single connection.

        Args:
            connection: PostgreSQL connection to check.

        Returns:
            HealthStatus indicating connection health.
        """
        start_time = time.time()

        try:
            # Execute simple query to verify connection
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            cursor.close()

            response_time_ms = (time.time() - start_time) * 1000

            if result and result[0] == 1:
                return HealthStatus(
                    state=HealthState.HEALTHY,
                    message="Connection is healthy",
                    checked_at=datetime.now(),
                    response_time_ms=response_time_ms,
                )
            else:
                return HealthStatus(
                    state=HealthState.UNHEALTHY,
                    message="Unexpected query result",
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
        start_time = time.time()

        try:
            status = self.pool.pool_status()
            response_time_ms = (time.time() - start_time) * 1000

            # Determine health state based on pool metrics
            total_conns = status["total_connections"]
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
                    "total_connections": total_conns,
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
                message=f"Pool health check failed: {str(e)}",
                checked_at=datetime.now(),
                response_time_ms=response_time_ms,
                details={"error": str(e), "error_type": type(e).__name__},
            )

    def check_database(self) -> HealthStatus:
        """Check database server health.

        Performs comprehensive checks including:
        - Connection test
        - Database version
        - Active connections
        - Database size (if accessible)

        Returns:
            HealthStatus indicating database health.
        """
        start_time = time.time()
        details = {}

        try:
            with self.pool.get_connection() as conn:
                cursor = conn.cursor()

                # Check connection
                cursor.execute("SELECT 1")

                # Get PostgreSQL version
                cursor.execute("SHOW server_version")
                version = cursor.fetchone()[0]
                details["server_version"] = version

                # Get active connections count
                cursor.execute("""
                    SELECT count(*) 
                    FROM pg_stat_activity 
                    WHERE state = 'active'
                """)
                active_count = cursor.fetchone()[0]
                details["active_queries"] = active_count

                # Get database size (may fail with insufficient permissions)
                try:
                    cursor.execute(f"""
                        SELECT pg_size_pretty(pg_database_size('{self.pool.config.database}'))
                    """)
                    db_size = cursor.fetchone()[0]
                    details["database_size"] = db_size
                except Exception:
                    # Ignore permission errors
                    pass

                # Get connection count
                cursor.execute(
                    """
                    SELECT count(*) 
                    FROM pg_stat_activity 
                    WHERE datname = %s
                """,
                    (self.pool.config.database,),
                )
                conn_count = cursor.fetchone()[0]
                details["total_db_connections"] = conn_count

                cursor.close()

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


async def async_check_connection(connection) -> HealthStatus:
    """Async version: Check health of a single connection.

    Args:
        connection: Asyncpg connection to check.

    Returns:
        HealthStatus indicating connection health.
    """
    start_time = time.time()

    try:
        # Execute simple query
        result = await connection.fetchval("SELECT 1")
        response_time_ms = (time.time() - start_time) * 1000

        if result == 1:
            return HealthStatus(
                state=HealthState.HEALTHY,
                message="Connection is healthy",
                checked_at=datetime.now(),
                response_time_ms=response_time_ms,
            )
        else:
            return HealthStatus(
                state=HealthState.UNHEALTHY,
                message="Unexpected query result",
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
