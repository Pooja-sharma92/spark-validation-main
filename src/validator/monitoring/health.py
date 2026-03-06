"""
Health Check Module.

Simple health checks for framework components.
No external dependencies.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Callable, Awaitable, Any
import asyncio


class HealthStatus(Enum):
    """Health status levels."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class ComponentHealth:
    """Health status of a single component."""
    name: str
    status: HealthStatus
    message: str = ""
    last_check: Optional[datetime] = None
    response_time_ms: Optional[float] = None
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status.value,
            "message": self.message,
            "last_check": self.last_check.isoformat() if self.last_check else None,
            "response_time_ms": self.response_time_ms,
            "details": self.details,
        }


@dataclass
class SystemHealth:
    """Overall system health."""
    status: HealthStatus
    components: List[ComponentHealth]
    timestamp: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status.value,
            "timestamp": self.timestamp.isoformat(),
            "components": [c.to_dict() for c in self.components],
        }


# Type for health check functions
HealthCheckFunc = Callable[[], Awaitable[ComponentHealth]]


class HealthChecker:
    """
    Manages health checks for all components.

    Runs checks periodically and caches results.
    """

    def __init__(self, check_interval_seconds: float = 30.0):
        self.check_interval = check_interval_seconds
        self._checks: Dict[str, HealthCheckFunc] = {}
        self._results: Dict[str, ComponentHealth] = {}
        self._running = False
        self._task: Optional[asyncio.Task] = None

    def register(self, name: str, check_func: HealthCheckFunc) -> None:
        """Register a health check function."""
        self._checks[name] = check_func

    def unregister(self, name: str) -> None:
        """Remove a health check."""
        self._checks.pop(name, None)
        self._results.pop(name, None)

    async def check_component(self, name: str) -> ComponentHealth:
        """Run health check for a specific component."""
        if name not in self._checks:
            return ComponentHealth(
                name=name,
                status=HealthStatus.UNKNOWN,
                message="No health check registered",
            )

        check_func = self._checks[name]
        start_time = datetime.utcnow()

        try:
            result = await asyncio.wait_for(check_func(), timeout=10.0)
            result.response_time_ms = (
                datetime.utcnow() - start_time
            ).total_seconds() * 1000
            result.last_check = datetime.utcnow()
            self._results[name] = result
            return result

        except asyncio.TimeoutError:
            result = ComponentHealth(
                name=name,
                status=HealthStatus.UNHEALTHY,
                message="Health check timed out",
                last_check=datetime.utcnow(),
                response_time_ms=10000,
            )
            self._results[name] = result
            return result

        except Exception as e:
            result = ComponentHealth(
                name=name,
                status=HealthStatus.UNHEALTHY,
                message=f"Health check failed: {str(e)}",
                last_check=datetime.utcnow(),
            )
            self._results[name] = result
            return result

    async def check_all(self) -> SystemHealth:
        """Run all health checks and return system health."""
        components = []

        for name in self._checks:
            result = await self.check_component(name)
            components.append(result)

        # Determine overall status
        if not components:
            overall_status = HealthStatus.UNKNOWN
        elif all(c.status == HealthStatus.HEALTHY for c in components):
            overall_status = HealthStatus.HEALTHY
        elif any(c.status == HealthStatus.UNHEALTHY for c in components):
            overall_status = HealthStatus.UNHEALTHY
        else:
            overall_status = HealthStatus.DEGRADED

        return SystemHealth(
            status=overall_status,
            components=components,
        )

    def get_cached_health(self) -> SystemHealth:
        """Get cached health results without running checks."""
        components = list(self._results.values())

        if not components:
            return SystemHealth(
                status=HealthStatus.UNKNOWN,
                components=[],
            )

        # Check if any results are stale (older than 2x check interval)
        stale_threshold = timedelta(seconds=self.check_interval * 2)
        now = datetime.utcnow()

        for component in components:
            if component.last_check:
                age = now - component.last_check
                if age > stale_threshold:
                    component.status = HealthStatus.UNKNOWN
                    component.message = "Health check data is stale"

        # Determine overall status
        if all(c.status == HealthStatus.HEALTHY for c in components):
            overall_status = HealthStatus.HEALTHY
        elif any(c.status == HealthStatus.UNHEALTHY for c in components):
            overall_status = HealthStatus.UNHEALTHY
        else:
            overall_status = HealthStatus.DEGRADED

        return SystemHealth(
            status=overall_status,
            components=components,
        )

    async def start(self) -> None:
        """Start periodic health checking."""
        if self._running:
            return

        self._running = True
        self._task = asyncio.create_task(self._check_loop())

    async def stop(self) -> None:
        """Stop periodic health checking."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _check_loop(self) -> None:
        """Periodic health check loop."""
        while self._running:
            try:
                await self.check_all()
            except Exception:
                pass  # Log but don't crash

            await asyncio.sleep(self.check_interval)


# Pre-built health check functions
async def create_redis_check(redis_url: str) -> HealthCheckFunc:
    """Create a Redis health check function."""
    async def check() -> ComponentHealth:
        try:
            import redis.asyncio as redis
            client = redis.from_url(redis_url)
            await client.ping()
            await client.close()
            return ComponentHealth(
                name="redis",
                status=HealthStatus.HEALTHY,
                message="Redis is responding",
            )
        except Exception as e:
            return ComponentHealth(
                name="redis",
                status=HealthStatus.UNHEALTHY,
                message=f"Redis error: {str(e)}",
            )
    return check


async def create_postgres_check(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
) -> HealthCheckFunc:
    """Create a PostgreSQL health check function."""
    async def check() -> ComponentHealth:
        try:
            import asyncpg
            conn = await asyncpg.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
                timeout=5,
            )
            await conn.fetchval("SELECT 1")
            await conn.close()
            return ComponentHealth(
                name="postgres",
                status=HealthStatus.HEALTHY,
                message="PostgreSQL is responding",
            )
        except Exception as e:
            return ComponentHealth(
                name="postgres",
                status=HealthStatus.UNHEALTHY,
                message=f"PostgreSQL error: {str(e)}",
            )
    return check


def create_queue_check(queue_manager) -> HealthCheckFunc:
    """Create a queue manager health check."""
    async def check() -> ComponentHealth:
        try:
            stats = await queue_manager.get_stats()
            return ComponentHealth(
                name="queue",
                status=HealthStatus.HEALTHY,
                message=f"Queue active: {stats.total_pending} pending",
                details={
                    "pending": stats.total_pending,
                    "active": stats.active_validations,
                },
            )
        except Exception as e:
            return ComponentHealth(
                name="queue",
                status=HealthStatus.UNHEALTHY,
                message=f"Queue error: {str(e)}",
            )
    return check


def create_worker_pool_check(worker_pool) -> HealthCheckFunc:
    """Create a worker pool health check."""
    async def check() -> ComponentHealth:
        try:
            stats = worker_pool.get_stats()
            status = HealthStatus.HEALTHY

            if stats.active_workers == 0:
                status = HealthStatus.UNHEALTHY
                message = "No active workers"
            elif stats.idle_workers == 0 and stats.processing_workers > 0:
                status = HealthStatus.DEGRADED
                message = "All workers busy"
            else:
                message = f"{stats.active_workers} workers active"

            return ComponentHealth(
                name="workers",
                status=status,
                message=message,
                details={
                    "active": stats.active_workers,
                    "idle": stats.idle_workers,
                    "processing": stats.processing_workers,
                    "jobs_processed": stats.total_jobs_processed,
                },
            )
        except Exception as e:
            return ComponentHealth(
                name="workers",
                status=HealthStatus.UNHEALTHY,
                message=f"Worker pool error: {str(e)}",
            )
    return check


# Global health checker instance
_health_checker: Optional[HealthChecker] = None


def get_health_checker() -> HealthChecker:
    """Get the global health checker instance."""
    global _health_checker
    if _health_checker is None:
        _health_checker = HealthChecker()
    return _health_checker
