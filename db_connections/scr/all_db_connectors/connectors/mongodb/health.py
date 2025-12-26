"""MongoDB health check implementation."""

import asyncio
import time
from datetime import datetime
from typing import Optional, Dict, Any, Union, TYPE_CHECKING

try:
    from pymongo import MongoClient
    from motor.motor_asyncio import AsyncIOMotorClient
except ImportError:
    MongoClient = None
    AsyncIOMotorClient = None

from db_connections.scr.all_db_connectors.core import HealthState, HealthStatus

if TYPE_CHECKING:
    from .pool import MongoSyncConnectionPool, MongoAsyncConnectionPool


class MongoHealthChecker:
    """Health checker for MongoDB connections and pools."""

    def __init__(
        self,
        pool_or_config: Union[
            "MongoSyncConnectionPool", "MongoAsyncConnectionPool", "MongoPoolConfig"
        ],
    ):
        """Initialize health checker.

        Args:
            pool_or_config: MongoDB connection pool instance or config.
        """
        from .config import MongoPoolConfig

        if isinstance(pool_or_config, MongoPoolConfig):
            self.config = pool_or_config
            self.pool = None
        else:
            # It's a pool instance
            self.pool = pool_or_config
            self.config = pool_or_config.config

        self._last_check_time: Optional[datetime] = None
        self._last_status: Optional[HealthStatus] = None

    def check_health(self, connection: MongoClient) -> HealthStatus:
        """Check health of a single connection.

        Args:
            connection: MongoDB client to check.

        Returns:
            HealthStatus indicating connection health.
        """
        return self.check_connection(connection)

    def check_connection(self, connection: MongoClient) -> HealthStatus:
        """Check health of a single connection.

        Args:
            connection: MongoDB client to check.

        Returns:
            HealthStatus indicating connection health.
        """
        start_time = time.time()

        try:
            # Execute ping command to verify connection
            # Handle both property (connection.admin.command) and method (connection.admin().command)
            admin_db = connection.admin
            if callable(admin_db):
                admin_db = admin_db()
            result = admin_db.command("ping")

            response_time_ms = max((time.time() - start_time) * 1000, 0.001)

            if not (result and result.get("ok") == 1):
                return HealthStatus(
                    state=HealthState.UNHEALTHY,
                    message="Unexpected ping result",
                    checked_at=datetime.now(),
                    response_time_ms=response_time_ms,
                )

            # Check server status for additional health indicators
            try:
                server_status = admin_db.command("serverStatus")
                connections_info = server_status.get("connections", {})
                current_conns = connections_info.get("current", 0)
                available_conns = connections_info.get("available", 0)
                total_conns = current_conns + available_conns

                # Determine state based on connection utilization and response time
                if total_conns > 0:
                    utilization = current_conns / total_conns
                    if utilization > 0.9:
                        state = HealthState.UNHEALTHY
                        message = "Connection pool nearly exhausted"
                    elif utilization > 0.7:
                        state = HealthState.DEGRADED
                        message = "High connection utilization"
                    elif response_time_ms > 500:
                        state = HealthState.UNHEALTHY
                        message = "Slow response time"
                    elif response_time_ms > 100:
                        state = HealthState.DEGRADED
                        message = "Response time is slow"
                    else:
                        state = HealthState.HEALTHY
                        message = "Connection is healthy"
                else:
                    # No connection info available, use response time only
                    if response_time_ms > 500:
                        state = HealthState.UNHEALTHY
                        message = "Slow response time"
                    elif response_time_ms > 100:
                        state = HealthState.DEGRADED
                        message = "Response time is slow"
                    else:
                        state = HealthState.HEALTHY
                        message = "Connection is healthy"

                return HealthStatus(
                    state=state,
                    message=message,
                    checked_at=datetime.now(),
                    response_time_ms=response_time_ms,
                    details={
                        "current_connections": current_conns,
                        "available_connections": available_conns,
                        "utilization_percent": round(
                            (current_conns / total_conns * 100)
                            if total_conns > 0
                            else 0,
                            2,
                        ),
                    },
                )
            except Exception:
                # If serverStatus fails, just use ping result and response time
                if response_time_ms > 500:
                    state = HealthState.UNHEALTHY
                    message = "Slow response time"
                elif response_time_ms > 100:
                    state = HealthState.DEGRADED
                    message = "Response time is slow"
                else:
                    state = HealthState.HEALTHY
                    message = "Connection is healthy"

                return HealthStatus(
                    state=state,
                    message=message,
                    checked_at=datetime.now(),
                    response_time_ms=response_time_ms,
                )

        except Exception as e:
            response_time_ms = max((time.time() - start_time) * 1000, 0.001)
            return HealthStatus(
                state=HealthState.UNHEALTHY,
                message=f"Connection check failed: {str(e)}",
                checked_at=datetime.now(),
                response_time_ms=response_time_ms,
                details={"error": str(e), "error_type": type(e).__name__},
            )

    async def async_check_health(self, connection: AsyncIOMotorClient) -> HealthStatus:
        """Async version: Check health of a single connection.

        Args:
            connection: Async MongoDB client to check.

        Returns:
            HealthStatus indicating connection health.
        """
        return await self.async_check_connection(connection)

    async def async_check_connection(
        self, connection: AsyncIOMotorClient
    ) -> HealthStatus:
        """Async version: Check health of a single connection.

        Args:
            connection: Async MongoDB client to check.

        Returns:
            HealthStatus indicating connection health.
        """
        start_time = time.perf_counter()

        try:
            # Execute ping command
            # Handle both property (connection.admin.command) and method (connection.admin().command)
            admin_db = connection.admin
            if callable(admin_db):
                admin_db = admin_db()
            result = await admin_db.command("ping")
            response_time_ms = max((time.perf_counter() - start_time) * 1000, 0.001)

            if result and result.get("ok") == 1:
                return HealthStatus(
                    state=HealthState.HEALTHY,
                    message="Connection is healthy",
                    checked_at=datetime.now(),
                    response_time_ms=response_time_ms,
                )
            else:
                return HealthStatus(
                    state=HealthState.UNHEALTHY,
                    message="Unexpected ping result",
                    checked_at=datetime.now(),
                    response_time_ms=response_time_ms,
                )

        except Exception as e:
            response_time_ms = max((time.perf_counter() - start_time) * 1000, 0.001)
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
        - Replication status (if applicable)
        - Database stats

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
                admin_db = client.admin
                if callable(admin_db):
                    admin_db = admin_db()
                admin_db.command("ping")

                # Get server info
                server_info = client.server_info()
                details["server_version"] = server_info.get("version", "unknown")
                details["git_version"] = server_info.get("gitVersion", "unknown")

                # Get server status
                server_status = client.admin.command("serverStatus")
                details["uptime_seconds"] = server_status.get("uptime", 0)

                # Get connections info
                connections = server_status.get("connections", {})
                details["current_connections"] = connections.get("current", 0)
                details["available_connections"] = connections.get("available", 0)

                # Get replication info if applicable
                try:
                    repl_set_status = client.admin.command("replSetGetStatus")
                    details["replica_set"] = repl_set_status.get("set", "unknown")
                    details["replica_set_members"] = len(
                        repl_set_status.get("members", [])
                    )
                except Exception:
                    # Not a replica set
                    pass

                # Get database stats
                try:
                    db = client[self.config.database]
                    db_stats = db.command("dbStats")
                    details["database_size_mb"] = round(
                        db_stats.get("dataSize", 0) / (1024 * 1024), 2
                    )
                    details["collections"] = db_stats.get("collections", 0)
                except Exception:
                    # May fail with insufficient permissions
                    pass

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
                admin_db = client.admin
                if callable(admin_db):
                    admin_db = admin_db()
                await admin_db.command("ping")

                # Get server info
                server_info = await client.server_info()
                details["server_version"] = server_info.get("version", "unknown")
                details["git_version"] = server_info.get("gitVersion", "unknown")

                # Get server status
                server_status = await client.admin.command("serverStatus")
                details["uptime_seconds"] = server_status.get("uptime", 0)

                # Get connections info
                connections = server_status.get("connections", {})
                details["current_connections"] = connections.get("current", 0)
                details["available_connections"] = connections.get("available", 0)

                # Get replication info if applicable
                try:
                    repl_set_status = await client.admin.command("replSetGetStatus")
                    details["replica_set"] = repl_set_status.get("set", "unknown")
                    details["replica_set_members"] = len(
                        repl_set_status.get("members", [])
                    )
                except Exception:
                    # Not a replica set
                    pass

                # Get database stats
                try:
                    db = client[self.config.database]
                    db_stats = await db.command("dbStats")
                    details["database_size_mb"] = round(
                        db_stats.get("dataSize", 0) / (1024 * 1024), 2
                    )
                    details["collections"] = db_stats.get("collections", 0)
                except Exception:
                    # May fail with insufficient permissions
                    pass

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

    def comprehensive_check(self, connection: Optional[MongoClient] = None) -> dict:
        """Perform comprehensive health check of all components.

        Args:
            connection: Optional MongoDB client to check. If not provided,
                       uses pool connection.

        Returns:
            Dictionary with health status for each component.
        """
        result = {
            "timestamp": datetime.now(),
        }

        # Check pool if available
        if self.pool:
            try:
                result["pool"] = self.check_pool()
            except Exception:
                result["pool"] = HealthStatus(
                    state=HealthState.UNHEALTHY,
                    message="Pool check failed",
                    checked_at=datetime.now(),
                )

        # Check connection if provided
        if connection:
            try:
                result["connection"] = self.check_health(connection)
                # Also add server and replication status
                try:
                    server_status = self.check_server_status(connection)
                    result["server"] = (
                        server_status if server_status is not None else {}
                    )
                except Exception:
                    result["server"] = {}
                try:
                    result["replication"] = self.check_replication_status(connection)
                except Exception:
                    result["replication"] = None
            except Exception:
                result["connection"] = HealthStatus(
                    state=HealthState.UNHEALTHY,
                    message="Connection check failed",
                    checked_at=datetime.now(),
                )
                result["server"] = {}
                result["replication"] = None

        # Check database if pool is available
        if self.pool:
            try:
                result["database"] = self.check_database()
            except Exception:
                result["database"] = HealthStatus(
                    state=HealthState.UNHEALTHY,
                    message="Database check failed",
                    checked_at=datetime.now(),
                )

        return result

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

    def check_server_status(self, connection: MongoClient) -> Optional[Dict[str, Any]]:
        """Check MongoDB server status.

        Args:
            connection: MongoDB client to check.

        Returns:
            Dictionary with server status information, or None on error.
        """
        try:
            admin_db = connection.admin
            if callable(admin_db):
                admin_db = admin_db()
            status = admin_db.command("serverStatus")
            return {
                "uptime": status.get("uptime", 0),
                "version": status.get("version", "unknown"),
                "connections": status.get("connections", {}),
                "network": status.get("network", {}),
                "globalLock": status.get("globalLock", {}),
            }
        except Exception:
            return None

    async def async_check_server_status(
        self, connection: AsyncIOMotorClient
    ) -> Optional[Dict[str, Any]]:
        """Check MongoDB server status (async version).

        Args:
            connection: Async MongoDB client to check.

        Returns:
            Dictionary with server status information, or None on error.
        """
        try:
            admin_db = connection.admin
            if callable(admin_db):
                admin_db = admin_db()
            status = await admin_db.command("serverStatus")
            return {
                "uptime": status.get("uptime", 0),
                "version": status.get("version", "unknown"),
                "connections": status.get("connections", {}),
                "network": status.get("network", {}),
                "globalLock": status.get("globalLock", {}),
            }
        except Exception:
            return None

    def check_replication_status(
        self, connection: MongoClient
    ) -> Optional[Dict[str, Any]]:
        """Check MongoDB replication status.

        Args:
            connection: MongoDB client to check.

        Returns:
            Dictionary with replication status, or None if not a replica set or on error.
        """
        try:
            admin_db = connection.admin
            if callable(admin_db):
                admin_db = admin_db()

            # Try to get replication info from serverStatus first (works better with mocks)
            try:
                status = admin_db.command("serverStatus")
                repl = status.get("repl", {})
                if repl.get("setName"):
                    return {
                        "replica_set": repl.get("setName"),
                        "is_primary": repl.get("ismaster", False)
                        or repl.get("isWritablePrimary", False),
                        "is_secondary": repl.get("secondary", False),
                        "primary": repl.get("primary"),
                    }
            except Exception:
                pass

            # Fallback to hello/isMaster commands
            try:
                ismaster = admin_db.command("hello")
            except Exception:
                try:
                    ismaster = admin_db.command("isMaster")
                except Exception:
                    return None

            if not ismaster.get("setName"):
                return None

            return {
                "replica_set": ismaster.get("setName"),
                "is_primary": ismaster.get("ismaster", False)
                or ismaster.get("isWritablePrimary", False),
                "is_secondary": ismaster.get("secondary", False),
                "primary": ismaster.get("primary"),
            }
        except Exception:
            return None

    async def async_check_replication_status(
        self, connection: AsyncIOMotorClient
    ) -> Optional[Dict[str, Any]]:
        """Check MongoDB replication status (async version).

        Args:
            connection: Async MongoDB client to check.

        Returns:
            Dictionary with replication status, or None if not a replica set or on error.
        """
        try:
            admin_db = connection.admin
            if callable(admin_db):
                admin_db = admin_db()

            # Try to get replication info from serverStatus first (works better with mocks)
            try:
                status = await admin_db.command("serverStatus")
                repl = status.get("repl", {})
                if repl.get("setName"):
                    return {
                        "replica_set": repl.get("setName"),
                        "is_primary": repl.get("ismaster", False)
                        or repl.get("isWritablePrimary", False),
                        "is_secondary": repl.get("secondary", False),
                        "primary": repl.get("primary"),
                    }
            except Exception:
                pass

            # Fallback to hello/isMaster commands
            try:
                ismaster = await admin_db.command("hello")
            except Exception:
                try:
                    ismaster = await admin_db.command("isMaster")
                except Exception:
                    return None

            if not ismaster.get("setName"):
                return None

            return {
                "replica_set": ismaster.get("setName"),
                "is_primary": ismaster.get("ismaster", False)
                or ismaster.get("isWritablePrimary", False),
                "is_secondary": ismaster.get("secondary", False),
                "primary": ismaster.get("primary"),
            }
        except Exception:
            return None

    def check_database_stats(self, database) -> Optional[Dict[str, Any]]:
        """Check MongoDB database statistics.

        Args:
            database: MongoDB database object to check.

        Returns:
            Dictionary with database statistics, or None on error.
        """
        try:
            stats = database.command("dbStats")
            return {
                "collections": stats.get("collections", 0),
                "objects": stats.get("objects", 0),
                "dataSize": stats.get("dataSize", 0),
                "storageSize": stats.get("storageSize", 0),
                "indexes": stats.get("indexes", 0),
                "indexSize": stats.get("indexSize", 0),
            }
        except Exception:
            return None

    async def async_check_database_stats(self, database) -> Optional[Dict[str, Any]]:
        """Check MongoDB database statistics (async version).

        Args:
            database: Async MongoDB database object to check.

        Returns:
            Dictionary with database statistics, or None on error.
        """
        try:
            if asyncio.iscoroutinefunction(database.command):
                stats = await database.command("dbStats")
            else:
                stats = database.command("dbStats")
            return {
                "collections": stats.get("collections", 0),
                "objects": stats.get("objects", 0),
                "dataSize": stats.get("dataSize", 0),
                "storageSize": stats.get("storageSize", 0),
                "indexes": stats.get("indexes", 0),
                "indexSize": stats.get("indexSize", 0),
            }
        except Exception:
            return None
