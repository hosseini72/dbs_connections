"""This module defines the health status of a database connection pool or database.
It includes the health state, message, and additional details about the health status."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional


class HealthState(Enum):
    """Health state of a connection pool or database"""

    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DEGRADED = "degraded"


@dataclass
class HealthStatus:
    """Represents the health status of a connection pool or database

    Attributes:
        state (HealthState): The current health state.
        message (str): Human-readable status message.
        checked_at (datetime): The timestamp when the health status was checked.
        response_time_ms (Optional[float]): Response time in milliseconds, if applicable.
        last_checked (datetime): The timestamp of the last health check.
        details (Optional[Dict[str, Any]]): Additional details about the health status.

    """

    state: HealthState
    message: str
    checked_at: datetime = field(default_factory=datetime.now)
    response_time_ms: Optional[float] = None
    last_checked: datetime = field(default_factory=datetime.now)
    details: Optional[Dict[str, Any]] = field(default_factory=dict)

    @property
    def is_healthy(self) -> bool:
        """Check if the health state is healthy"""
        return self.state == HealthState.HEALTHY

    @property
    def is_unhealthy(self) -> bool:
        """Check if the health state is unhealthy"""
        return self.state == HealthState.UNHEALTHY

    @property
    def is_degraded(self) -> bool:
        """Check if the health state is degraded"""
        return self.state == HealthState.DEGRADED
