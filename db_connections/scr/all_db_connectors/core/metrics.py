"""
Shared types for database connectors.

This module defines common types and data structures used across different
database connectors in the system.
"""

from typing import Optional
from dataclasses import dataclass


@dataclass
class PoolMetrics:
    """Metrics related to the connection pool

    Attributes:
        total_connections (int): Total number of connections in the pool.
        active_connections (int): Number of active (in-use) connections.
        idle_connections (int): Number of idle (available) connections.
        max_connections (int): Maximum allowed connections in the pool.
        min_connections (int): Minimum maintained connections in the pool.
        wait_queue_size (int): Number of requests waiting for a connection.
        average_wait_time_ms (Optional[float]): Average wait time in milliseconds for acquiring a connection.
    """

    total_connections: int
    active_connections: int
    idle_connections: int
    max_connections: int
    min_connections: int
    wait_queue_size: int
    average_wait_time_ms: Optional[float] = None
