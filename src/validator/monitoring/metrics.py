"""
Simple In-Memory Metrics Collection.

Lightweight metrics without external dependencies.
Data is collected in memory and exposed via API for custom UI.
"""

import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from threading import Lock
from typing import Dict, List, Optional, Any
from enum import Enum


class MetricType(Enum):
    """Types of metrics."""
    COUNTER = "counter"      # Monotonically increasing
    GAUGE = "gauge"          # Point-in-time value
    HISTOGRAM = "histogram"  # Distribution of values


@dataclass
class TimeSeriesPoint:
    """A single point in a time series."""
    timestamp: datetime
    value: float
    labels: Dict[str, str] = field(default_factory=dict)


class Counter:
    """Simple counter metric."""

    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description
        self._value = 0
        self._lock = Lock()

    def inc(self, amount: float = 1) -> None:
        """Increment the counter."""
        with self._lock:
            self._value += amount

    @property
    def value(self) -> float:
        return self._value

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "type": "counter",
            "value": self._value,
        }


class Gauge:
    """Simple gauge metric (can go up or down)."""

    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description
        self._value = 0.0
        self._lock = Lock()

    def set(self, value: float) -> None:
        """Set the gauge value."""
        with self._lock:
            self._value = value

    def inc(self, amount: float = 1) -> None:
        """Increment the gauge."""
        with self._lock:
            self._value += amount

    def dec(self, amount: float = 1) -> None:
        """Decrement the gauge."""
        with self._lock:
            self._value -= amount

    @property
    def value(self) -> float:
        return self._value

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "type": "gauge",
            "value": self._value,
        }


class Histogram:
    """Simple histogram for tracking distributions."""

    def __init__(
        self,
        name: str,
        description: str = "",
        buckets: List[float] = None,
    ):
        self.name = name
        self.description = description
        self.buckets = buckets or [0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0]
        self._counts: Dict[float, int] = {b: 0 for b in self.buckets}
        self._counts[float('inf')] = 0
        self._sum = 0.0
        self._count = 0
        self._lock = Lock()

    def observe(self, value: float) -> None:
        """Record a value in the histogram."""
        with self._lock:
            self._sum += value
            self._count += 1
            for bucket in self.buckets:
                if value <= bucket:
                    self._counts[bucket] += 1
            self._counts[float('inf')] += 1

    @property
    def avg(self) -> float:
        if self._count == 0:
            return 0.0
        return self._sum / self._count

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "type": "histogram",
            "count": self._count,
            "sum": self._sum,
            "avg": self.avg,
            "buckets": {str(k): v for k, v in self._counts.items()},
        }


class TimeSeries:
    """
    Time series data with automatic pruning.

    Keeps recent data points for charting.
    """

    def __init__(
        self,
        name: str,
        max_points: int = 1000,
        max_age_hours: int = 24,
    ):
        self.name = name
        self.max_points = max_points
        self.max_age = timedelta(hours=max_age_hours)
        self._points: deque = deque(maxlen=max_points)
        self._lock = Lock()

    def add(self, value: float, labels: Dict[str, str] = None) -> None:
        """Add a data point."""
        point = TimeSeriesPoint(
            timestamp=datetime.utcnow(),
            value=value,
            labels=labels or {},
        )
        with self._lock:
            self._points.append(point)

    def get_points(
        self,
        since: Optional[datetime] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Get data points, optionally filtered by time."""
        with self._lock:
            points = list(self._points)

        if since:
            points = [p for p in points if p.timestamp >= since]

        # Return most recent points
        points = points[-limit:]

        return [
            {
                "timestamp": p.timestamp.isoformat(),
                "value": p.value,
                "labels": p.labels,
            }
            for p in points
        ]

    def prune_old(self) -> int:
        """Remove old data points."""
        cutoff = datetime.utcnow() - self.max_age
        removed = 0

        with self._lock:
            while self._points and self._points[0].timestamp < cutoff:
                self._points.popleft()
                removed += 1

        return removed


class MetricsRegistry:
    """
    Central registry for all metrics.

    Provides a simple API for creating and accessing metrics.
    """

    def __init__(self):
        self._counters: Dict[str, Counter] = {}
        self._gauges: Dict[str, Gauge] = {}
        self._histograms: Dict[str, Histogram] = {}
        self._time_series: Dict[str, TimeSeries] = {}
        self._lock = Lock()

    def counter(self, name: str, description: str = "") -> Counter:
        """Get or create a counter."""
        with self._lock:
            if name not in self._counters:
                self._counters[name] = Counter(name, description)
            return self._counters[name]

    def gauge(self, name: str, description: str = "") -> Gauge:
        """Get or create a gauge."""
        with self._lock:
            if name not in self._gauges:
                self._gauges[name] = Gauge(name, description)
            return self._gauges[name]

    def histogram(
        self,
        name: str,
        description: str = "",
        buckets: List[float] = None,
    ) -> Histogram:
        """Get or create a histogram."""
        with self._lock:
            if name not in self._histograms:
                self._histograms[name] = Histogram(name, description, buckets)
            return self._histograms[name]

    def time_series(
        self,
        name: str,
        max_points: int = 1000,
    ) -> TimeSeries:
        """Get or create a time series."""
        with self._lock:
            if name not in self._time_series:
                self._time_series[name] = TimeSeries(name, max_points)
            return self._time_series[name]

    def get_all(self) -> Dict[str, Any]:
        """Get all metrics as a dictionary."""
        return {
            "counters": {k: v.to_dict() for k, v in self._counters.items()},
            "gauges": {k: v.to_dict() for k, v in self._gauges.items()},
            "histograms": {k: v.to_dict() for k, v in self._histograms.items()},
            "time_series": list(self._time_series.keys()),
        }

    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of key metrics for dashboard."""
        return {
            "counters": {k: v.value for k, v in self._counters.items()},
            "gauges": {k: v.value for k, v in self._gauges.items()},
            "histograms": {
                k: {"count": v._count, "avg": v.avg}
                for k, v in self._histograms.items()
            },
        }


# Global registry instance
_registry: Optional[MetricsRegistry] = None


def get_registry() -> MetricsRegistry:
    """Get the global metrics registry."""
    global _registry
    if _registry is None:
        _registry = MetricsRegistry()
    return _registry


# Convenience functions
def counter(name: str, description: str = "") -> Counter:
    return get_registry().counter(name, description)


def gauge(name: str, description: str = "") -> Gauge:
    return get_registry().gauge(name, description)


def histogram(name: str, description: str = "", buckets: List[float] = None) -> Histogram:
    return get_registry().histogram(name, description, buckets)


def time_series(name: str, max_points: int = 1000) -> TimeSeries:
    return get_registry().time_series(name, max_points)


# Pre-defined validation metrics
class ValidationMetrics:
    """Pre-defined metrics for validation framework."""

    def __init__(self, registry: MetricsRegistry = None):
        self.registry = registry or get_registry()

        # Counters
        self.jobs_submitted = self.registry.counter(
            "validation_jobs_submitted",
            "Total jobs submitted for validation"
        )
        self.jobs_completed = self.registry.counter(
            "validation_jobs_completed",
            "Total jobs completed"
        )
        self.jobs_failed = self.registry.counter(
            "validation_jobs_failed",
            "Total jobs failed"
        )
        self.jobs_deduplicated = self.registry.counter(
            "validation_jobs_deduplicated",
            "Jobs skipped due to deduplication"
        )

        # Gauges
        self.queue_depth = self.registry.gauge(
            "validation_queue_depth",
            "Current queue depth"
        )
        self.active_workers = self.registry.gauge(
            "validation_active_workers",
            "Number of active workers"
        )
        self.active_validations = self.registry.gauge(
            "validation_active_count",
            "Currently running validations"
        )

        # Histograms
        self.validation_duration = self.registry.histogram(
            "validation_duration_seconds",
            "Validation duration in seconds",
            buckets=[1, 5, 10, 30, 60, 120, 300, 600],
        )

        # Time series for charts
        self.queue_depth_ts = self.registry.time_series(
            "queue_depth_history",
            max_points=1440,  # 24 hours at 1 min intervals
        )
        self.throughput_ts = self.registry.time_series(
            "throughput_history",
            max_points=1440,
        )

    def record_job_submitted(self) -> None:
        """Record a job submission."""
        self.jobs_submitted.inc()

    def record_job_completed(self, duration_seconds: float, passed: bool) -> None:
        """Record job completion."""
        self.jobs_completed.inc()
        self.validation_duration.observe(duration_seconds)
        if not passed:
            self.jobs_failed.inc()

    def update_queue_depth(self, depth: int) -> None:
        """Update current queue depth."""
        self.queue_depth.set(depth)
        self.queue_depth_ts.add(depth)

    def update_active_workers(self, count: int) -> None:
        """Update active worker count."""
        self.active_workers.set(count)

    def get_dashboard_data(self) -> Dict[str, Any]:
        """Get data for dashboard display."""
        return {
            "summary": {
                "jobs_submitted": self.jobs_submitted.value,
                "jobs_completed": self.jobs_completed.value,
                "jobs_failed": self.jobs_failed.value,
                "success_rate": (
                    (self.jobs_completed.value - self.jobs_failed.value)
                    / self.jobs_completed.value * 100
                    if self.jobs_completed.value > 0 else 0
                ),
                "queue_depth": self.queue_depth.value,
                "active_workers": self.active_workers.value,
                "avg_duration": self.validation_duration.avg,
            },
            "charts": {
                "queue_depth": self.queue_depth_ts.get_points(limit=100),
                "throughput": self.throughput_ts.get_points(limit=100),
            },
        }


# Global validation metrics instance
_validation_metrics: Optional[ValidationMetrics] = None


def get_validation_metrics() -> ValidationMetrics:
    """Get the global validation metrics instance."""
    global _validation_metrics
    if _validation_metrics is None:
        _validation_metrics = ValidationMetrics()
    return _validation_metrics
