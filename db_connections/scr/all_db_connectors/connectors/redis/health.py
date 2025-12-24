"""
Redis health checking utilities.

Provides comprehensive health checks for Redis connections including
server info, memory usage, replication status, and performance metrics.
"""

import asyncio
import time
from datetime import datetime
from typing import Dict, Any, Optional, Union, TYPE_CHECKING

try:
    from redis import Redis
    from redis.asyncio import Redis as AsyncRedis
except ImportError:
    raise ImportError(
        "redis package is required. Install with: pip install redis"
    )

from ...core.health import HealthStatus, HealthState
from ...core.exceptions import HealthCheckError
from .config import RedisPoolConfig

if TYPE_CHECKING:
    from .pool import RedisSyncConnectionPool, RedisAsyncConnectionPool


class RedisHealthChecker:
    """Health checker for Redis connections.
    
    Performs various health checks including:
    - Basic connectivity (PING)
    - Server information
    - Memory usage
    - Replication status
    - Persistence status
    - Key space statistics
    """
    
    def __init__(self, config_or_pool: Union[RedisPoolConfig, "RedisSyncConnectionPool", "RedisAsyncConnectionPool"]):
        """Initialize Redis health checker.
        
        Args:
            config_or_pool: Redis pool configuration or pool instance.
        """
        if isinstance(config_or_pool, RedisPoolConfig):
            self.config = config_or_pool
            self.pool = None
        else:
            # It's a pool instance
            self.pool = config_or_pool
            self.config = config_or_pool.config
        
        self._last_check_time: Optional[datetime] = None
        self._last_status: Optional[HealthStatus] = None
    
    def check_pool(self) -> HealthStatus:
        """Check overall pool health.
        
        Returns:
            HealthStatus indicating pool health.
        """
        if self.pool is None:
            raise ValueError("Pool instance required for pool health check")
        
        start_time = time.perf_counter()
        
        try:
            if hasattr(self.pool, 'pool_status'):
                # Sync pool
                status = self.pool.pool_status()
            else:
                # Async pool - would need to be awaited, but this is sync method
                raise ValueError("Use async_check_pool for async pools")
            
            response_time_ms = max((time.perf_counter() - start_time) * 1000, 0.001)
            
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
            response_time_ms = max((time.perf_counter() - start_time) * 1000, 0.001)
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
        
        start_time = time.perf_counter()
        
        try:
            if hasattr(self.pool, 'pool_status') and not asyncio.iscoroutinefunction(self.pool.pool_status):
                # Sync pool
                status = self.pool.pool_status()
            else:
                # Async pool
                status = await self.pool.pool_status()
            
            response_time_ms = max((time.perf_counter() - start_time) * 1000, 0.001)
            
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
            response_time_ms = max((time.perf_counter() - start_time) * 1000, 0.001)
            return HealthStatus(
                state=HealthState.UNHEALTHY,
                message=f"Pool health check failed: {str(e)}",
                checked_at=datetime.now(),
                response_time_ms=response_time_ms,
                details={"error": str(e), "error_type": type(e).__name__},
            )
    
    def check_health(self, connection: Redis) -> HealthStatus:
        """Perform a comprehensive health check on Redis connection.
        
        Args:
            connection: Redis connection to check.
            
        Returns:
            HealthStatus object with health information.
        """
        start_time = time.perf_counter()
        
        try:
            # Basic connectivity check
            connection.ping()
            
            # Get server info
            info = connection.info()
            
            # Calculate response time (ensure it's at least 0.001ms for very fast operations)
            response_time = max((time.perf_counter() - start_time) * 1000, 0.001)
            
            # Analyze health
            state, message, details = self._analyze_health(info, response_time)
            
            status = HealthStatus(
                state=state,
                message=message,
                checked_at=datetime.now(),
                response_time_ms=response_time,
                last_checked=datetime.now(),
                details=details,
            )
            
            self._last_check_time = datetime.now()
            self._last_status = status
            
            return status
            
        except Exception as e:
            response_time = max((time.perf_counter() - start_time) * 1000, 0.001)
            return HealthStatus(
                state=HealthState.UNHEALTHY,
                message=f"Health check failed: {str(e)}",
                checked_at=datetime.now(),
                response_time_ms=response_time,
                last_checked=datetime.now(),
                details={"error": str(e)},
            )
    
    async def async_check_health(self, connection: AsyncRedis) -> HealthStatus:
        """Perform an async comprehensive health check.
        
        Args:
            connection: Async Redis connection to check.
            
        Returns:
            HealthStatus object with health information.
        """
        start_time = time.perf_counter()
        
        try:
            # Basic connectivity check
            await connection.ping()
            
            # Get server info
            info = await connection.info()
            
            # Calculate response time (ensure it's at least 0.001ms for very fast operations)
            response_time = max((time.perf_counter() - start_time) * 1000, 0.001)
            
            # Analyze health
            state, message, details = self._analyze_health(info, response_time)
            
            status = HealthStatus(
                state=state,
                message=message,
                checked_at=datetime.now(),
                response_time_ms=response_time,
                last_checked=datetime.now(),
                details=details,
            )
            
            self._last_check_time = datetime.now()
            self._last_status = status
            
            return status
            
        except Exception as e:
            response_time = max((time.perf_counter() - start_time) * 1000, 0.001)
            return HealthStatus(
                state=HealthState.UNHEALTHY,
                message=f"Health check failed: {str(e)}",
                checked_at=datetime.now(),
                response_time_ms=response_time,
                last_checked=datetime.now(),
                details={"error": str(e)},
            )
    
    def _analyze_health(
        self,
        info: Dict[str, Any],
        response_time: float
    ) -> tuple[HealthState, str, Dict[str, Any]]:
        """Analyze Redis server info to determine health state.
        
        Args:
            info: Redis INFO command output.
            response_time: Response time in milliseconds.
            
        Returns:
            Tuple of (state, message, details).
        """
        details = {}
        warnings = []
        
        # Server information
        details["redis_version"] = info.get("redis_version", "unknown")
        details["uptime_seconds"] = info.get("uptime_in_seconds", 0)
        details["connected_clients"] = info.get("connected_clients", 0)
        
        # Memory usage
        used_memory = info.get("used_memory", 0)
        max_memory = info.get("maxmemory", 0)
        details["used_memory_mb"] = round(used_memory / (1024 * 1024), 2)
        
        if max_memory > 0:
            memory_percent = (used_memory / max_memory) * 100
            details["memory_usage_percent"] = round(memory_percent, 2)
            
            if memory_percent > 90:
                warnings.append(f"High memory usage: {memory_percent:.1f}%")
            elif memory_percent > 75:
                warnings.append(f"Memory usage elevated: {memory_percent:.1f}%")
        
        # Memory fragmentation
        mem_frag_ratio = info.get("mem_fragmentation_ratio", 0)
        details["memory_fragmentation_ratio"] = mem_frag_ratio
        
        if mem_frag_ratio > 1.5:
            warnings.append(f"High memory fragmentation: {mem_frag_ratio:.2f}")
        
        # Replication information
        role = info.get("role", "unknown")
        details["role"] = role
        
        if role == "master":
            connected_slaves = info.get("connected_slaves", 0)
            details["connected_slaves"] = connected_slaves
        elif role == "slave":
            master_link_status = info.get("master_link_status", "unknown")
            details["master_link_status"] = master_link_status
            
            if master_link_status != "up":
                warnings.append(f"Master link down: {master_link_status}")
        
        # Persistence information
        rdb_changes = info.get("rdb_changes_since_last_save", 0)
        rdb_last_save = info.get("rdb_last_save_time", 0)
        details["rdb_changes_since_last_save"] = rdb_changes
        details["rdb_last_save_time"] = rdb_last_save
        
        # AOF if enabled
        if info.get("aof_enabled", 0) == 1:
            aof_last_rewrite_status = info.get("aof_last_rewrite_status", "unknown")
            details["aof_enabled"] = True
            details["aof_last_rewrite_status"] = aof_last_rewrite_status
            
            if aof_last_rewrite_status != "ok":
                warnings.append(f"AOF rewrite issue: {aof_last_rewrite_status}")
        
        # Key space statistics
        keyspace = {}
        for db_key in info.keys():
            if db_key.startswith("db"):
                db_info = info[db_key]
                keyspace[db_key] = {
                    "keys": db_info.get("keys", 0),
                    "expires": db_info.get("expires", 0),
                }
        details["keyspace"] = keyspace
        
        # Performance metrics
        details["ops_per_sec"] = info.get("instantaneous_ops_per_sec", 0)
        details["response_time_ms"] = round(response_time, 2)
        
        if response_time > 1000:
            warnings.append(f"Slow response time: {response_time:.0f}ms")
        elif response_time > 500:
            warnings.append(f"Elevated response time: {response_time:.0f}ms")
        
        # Rejected connections
        rejected_connections = info.get("rejected_connections", 0)
        if rejected_connections > 0:
            details["rejected_connections"] = rejected_connections
            warnings.append(f"Rejected connections: {rejected_connections}")
        
        # Evicted keys
        evicted_keys = info.get("evicted_keys", 0)
        if evicted_keys > 0:
            details["evicted_keys"] = evicted_keys
            warnings.append(f"Keys evicted: {evicted_keys}")
        
        # Blocked clients
        blocked_clients = info.get("blocked_clients", 0)
        if blocked_clients > 0:
            details["blocked_clients"] = blocked_clients
        
        # Determine overall health state
        if warnings:
            details["warnings"] = warnings
            
            # Critical issues make it unhealthy
            critical_keywords = ["down", "failed", "rejected"]
            has_critical = any(
                any(keyword in warning.lower() for keyword in critical_keywords)
                for warning in warnings
            )
            
            if has_critical:
                state = HealthState.UNHEALTHY
                message = f"Redis unhealthy: {'; '.join(warnings[:2])}"
            else:
                state = HealthState.DEGRADED
                message = f"Redis degraded: {'; '.join(warnings[:2])}"
        else:
            state = HealthState.HEALTHY
            message = "Redis is healthy"
        
        return state, message, details
    
    def check_replication_lag(self, connection: Redis) -> Optional[int]:
        """Check replication lag for replica nodes.
        
        Args:
            connection: Redis connection to check.
            
        Returns:
            Replication lag in seconds, or None if not a replica.
        """
        try:
            info = connection.info("replication")
            
            if info.get("role") != "slave":
                return None
            
            master_repl_offset = info.get("master_repl_offset", 0)
            slave_repl_offset = info.get("slave_repl_offset", 0)
            
            # Calculate lag (simplified - actual lag depends on write rate)
            lag = master_repl_offset - slave_repl_offset
            
            return lag
            
        except Exception:
            return None
    
    def check_memory_pressure(self, connection: Redis) -> Dict[str, Any]:
        """Check memory pressure and related metrics.
        
        Args:
            connection: Redis connection to check.
            
        Returns:
            Dictionary with memory pressure information.
        """
        try:
            info = connection.info("memory")
            
            used_memory = info.get("used_memory", 0)
            max_memory = info.get("maxmemory", 0)
            used_memory_rss = info.get("used_memory_rss", 0)
            
            result = {
                "used_memory_mb": round(used_memory / (1024 * 1024), 2),
                "used_memory_rss_mb": round(used_memory_rss / (1024 * 1024), 2),
                "max_memory_mb": round(max_memory / (1024 * 1024), 2) if max_memory > 0 else None,
                "memory_fragmentation_ratio": info.get("mem_fragmentation_ratio", 0),
                "evicted_keys": info.get("evicted_keys", 0),
            }
            
            if max_memory > 0:
                result["memory_usage_percent"] = round((used_memory / max_memory) * 100, 2)
            
            return result
            
        except Exception as e:
            raise HealthCheckError(f"Failed to check memory pressure: {e}") from e
    
    def check_persistence_status(self, connection: Redis) -> Dict[str, Any]:
        """Check Redis persistence status (RDB and AOF).
        
        Args:
            connection: Redis connection to check.
            
        Returns:
            Dictionary with persistence status information.
        """
        try:
            info = connection.info("persistence")
            
            result = {
                "rdb_enabled": info.get("rdb_bgsave_in_progress", 0) == 0,
                "rdb_last_save_time": info.get("rdb_last_save_time", 0),
                "rdb_changes_since_last_save": info.get("rdb_changes_since_last_save", 0),
                "rdb_last_status": info.get("rdb_last_bgsave_status", "unknown"),
                "aof_enabled": info.get("aof_enabled", 0) == 1,
            }
            
            if result["aof_enabled"]:
                result["aof_current_size"] = info.get("aof_current_size", 0)
                result["aof_base_size"] = info.get("aof_base_size", 0)
                result["aof_last_rewrite_status"] = info.get("aof_last_rewrite_status", "unknown")
            
            return result
            
        except Exception as e:
            raise HealthCheckError(f"Failed to check persistence status: {e}") from e
    
    def get_slow_log(self, connection: Redis, count: int = 10) -> list:
        """Get Redis slow log entries.
        
        Args:
            connection: Redis connection.
            count: Number of slow log entries to retrieve.
            
        Returns:
            List of slow log entries.
        """
        try:
            return connection.slowlog_get(count)
        except Exception:
            return []