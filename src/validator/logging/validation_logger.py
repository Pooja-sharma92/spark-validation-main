"""
Structured Validation Logger.

Collects logs with full context for UI display:
- Job ID and path
- Module/Component name
- Validation stage
- Success/Failure status
- Error details
- Timestamps

Supports in-memory storage with optional Redis persistence.
"""

import json
import uuid
from collections import deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from threading import Lock
from typing import Optional, List, Dict, Any, Deque
import asyncio


class LogLevel(Enum):
    """Log severity levels."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class LogCategory(Enum):
    """Log categories for filtering."""
    QUEUE = "queue"           # Queue operations
    EXECUTOR = "executor"     # Spark executor
    VALIDATION = "validation" # Validation stages
    STORAGE = "storage"       # Result storage
    SYSTEM = "system"         # System events


@dataclass
class ValidationLogEntry:
    """A single log entry with full context."""
    # Unique log ID
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])

    # Timestamp
    timestamp: datetime = field(default_factory=datetime.utcnow)

    # Log content
    level: LogLevel = LogLevel.INFO
    category: LogCategory = LogCategory.SYSTEM
    message: str = ""

    # Job context (optional)
    job_id: Optional[str] = None
    job_path: Optional[str] = None
    batch_id: Optional[str] = None

    # Validation context
    stage: Optional[str] = None  # syntax, logic, sql_parse, etc.
    stage_status: Optional[str] = None  # running, passed, failed

    # Module/Component
    module: str = "unknown"
    component: Optional[str] = None  # worker-1, executor-main, etc.

    # Error details
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    error_traceback: Optional[str] = None

    # Additional metadata
    metadata: Dict[str, Any] = field(default_factory=dict)

    # Duration (for stage completion logs)
    duration_seconds: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        data = {
            "id": self.id,
            "timestamp": self.timestamp.isoformat(),
            "level": self.level.value,
            "category": self.category.value,
            "message": self.message,
            "module": self.module,
        }

        # Add optional fields only if set
        if self.job_id:
            data["job_id"] = self.job_id
        if self.job_path:
            data["job_path"] = self.job_path
        if self.batch_id:
            data["batch_id"] = self.batch_id
        if self.stage:
            data["stage"] = self.stage
        if self.stage_status:
            data["stage_status"] = self.stage_status
        if self.component:
            data["component"] = self.component
        if self.error_type:
            data["error_type"] = self.error_type
        if self.error_message:
            data["error_message"] = self.error_message
        if self.error_traceback:
            data["error_traceback"] = self.error_traceback
        if self.duration_seconds is not None:
            data["duration_seconds"] = self.duration_seconds
        if self.metadata:
            data["metadata"] = self.metadata

        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ValidationLogEntry":
        """Create from dictionary."""
        return cls(
            id=data.get("id", uuid.uuid4().hex[:12]),
            timestamp=datetime.fromisoformat(data["timestamp"]) if "timestamp" in data else datetime.utcnow(),
            level=LogLevel(data.get("level", "info")),
            category=LogCategory(data.get("category", "system")),
            message=data.get("message", ""),
            job_id=data.get("job_id"),
            job_path=data.get("job_path"),
            batch_id=data.get("batch_id"),
            stage=data.get("stage"),
            stage_status=data.get("stage_status"),
            module=data.get("module", "unknown"),
            component=data.get("component"),
            error_type=data.get("error_type"),
            error_message=data.get("error_message"),
            error_traceback=data.get("error_traceback"),
            duration_seconds=data.get("duration_seconds"),
            metadata=data.get("metadata", {}),
        )


class ValidationLogStore:
    """
    In-memory log storage with size limits and backend support.

    Keeps recent logs for quick access and UI display.
    Also persists to configured backends (file, database, ES).
    """

    def __init__(
        self,
        max_entries: int = 10000,
        max_per_job: int = 500,
        backend_manager=None,
    ):
        self.max_entries = max_entries
        self.max_per_job = max_per_job

        # Main log buffer
        self._logs: Deque[ValidationLogEntry] = deque(maxlen=max_entries)

        # Index by job_id for fast lookup
        self._by_job: Dict[str, Deque[ValidationLogEntry]] = {}

        self._lock = Lock()

        # Backend manager for persistence
        self._backend_manager = backend_manager
        self._backend_queue: Deque[ValidationLogEntry] = deque(maxlen=1000)
        self._backend_task = None

    def set_backend_manager(self, manager) -> None:
        """Set the backend manager for log persistence."""
        self._backend_manager = manager

    def add(self, entry: ValidationLogEntry) -> None:
        """Add a log entry."""
        with self._lock:
            self._logs.append(entry)

            # Index by job if present
            if entry.job_id:
                if entry.job_id not in self._by_job:
                    self._by_job[entry.job_id] = deque(maxlen=self.max_per_job)
                self._by_job[entry.job_id].append(entry)

            # Queue for backend persistence
            if self._backend_manager:
                self._backend_queue.append(entry)

    async def flush_to_backends(self) -> int:
        """Flush queued entries to backends."""
        if not self._backend_manager:
            return 0

        with self._lock:
            if not self._backend_queue:
                return 0
            entries = list(self._backend_queue)
            self._backend_queue.clear()

        await self._backend_manager.write_batch(entries)
        return len(entries)

    async def start_backend_flusher(self, interval_seconds: float = 2.0) -> None:
        """Start background task to flush logs to backends."""
        import asyncio

        async def flusher():
            while True:
                try:
                    await self.flush_to_backends()
                except Exception:
                    pass
                await asyncio.sleep(interval_seconds)

        self._backend_task = asyncio.create_task(flusher())

    async def stop_backend_flusher(self) -> None:
        """Stop the backend flusher and flush remaining logs."""
        if self._backend_task:
            self._backend_task.cancel()
            try:
                await self._backend_task
            except asyncio.CancelledError:
                pass

        # Final flush
        await self.flush_to_backends()

    def get_recent(
        self,
        limit: int = 100,
        level: Optional[LogLevel] = None,
        category: Optional[LogCategory] = None,
        since: Optional[datetime] = None,
    ) -> List[ValidationLogEntry]:
        """Get recent log entries with optional filters."""
        with self._lock:
            logs = list(self._logs)

        # Apply filters
        if level:
            logs = [l for l in logs if l.level == level]
        if category:
            logs = [l for l in logs if l.category == category]
        if since:
            logs = [l for l in logs if l.timestamp >= since]

        # Return most recent
        return logs[-limit:]

    def get_by_job(
        self,
        job_id: str,
        limit: int = 100,
    ) -> List[ValidationLogEntry]:
        """Get logs for a specific job."""
        with self._lock:
            if job_id not in self._by_job:
                return []
            logs = list(self._by_job[job_id])

        return logs[-limit:]

    def get_errors(
        self,
        limit: int = 50,
        since: Optional[datetime] = None,
    ) -> List[ValidationLogEntry]:
        """Get recent error logs."""
        return self.get_recent(
            limit=limit,
            level=LogLevel.ERROR,
            since=since,
        )

    def get_job_summary(self, job_id: str) -> Dict[str, Any]:
        """Get a summary of logs for a job."""
        logs = self.get_by_job(job_id, limit=1000)

        if not logs:
            return {"job_id": job_id, "found": False}

        # Aggregate info
        stages = {}
        errors = []
        first_log = logs[0]
        last_log = logs[-1]

        for log in logs:
            if log.stage:
                if log.stage not in stages:
                    stages[log.stage] = {
                        "status": "unknown",
                        "duration": None,
                        "issues": 0,
                    }
                if log.stage_status:
                    stages[log.stage]["status"] = log.stage_status
                if log.duration_seconds:
                    stages[log.stage]["duration"] = log.duration_seconds
                if log.level in (LogLevel.ERROR, LogLevel.WARNING):
                    stages[log.stage]["issues"] += 1

            if log.level == LogLevel.ERROR:
                errors.append({
                    "stage": log.stage,
                    "message": log.error_message or log.message,
                    "timestamp": log.timestamp.isoformat(),
                })

        return {
            "job_id": job_id,
            "job_path": first_log.job_path,
            "found": True,
            "log_count": len(logs),
            "first_log": first_log.timestamp.isoformat(),
            "last_log": last_log.timestamp.isoformat(),
            "stages": stages,
            "errors": errors[:10],  # Limit errors shown
        }

    def cleanup_old(self, older_than_hours: int = 24) -> int:
        """Remove logs older than specified hours."""
        cutoff = datetime.utcnow() - timedelta(hours=older_than_hours)
        removed = 0

        with self._lock:
            # Clean main buffer
            while self._logs and self._logs[0].timestamp < cutoff:
                self._logs.popleft()
                removed += 1

            # Clean job index
            empty_jobs = []
            for job_id, logs in self._by_job.items():
                while logs and logs[0].timestamp < cutoff:
                    logs.popleft()
                if not logs:
                    empty_jobs.append(job_id)

            for job_id in empty_jobs:
                del self._by_job[job_id]

        return removed

    def get_stats(self) -> Dict[str, Any]:
        """Get log store statistics."""
        with self._lock:
            total = len(self._logs)
            jobs_tracked = len(self._by_job)

            level_counts = {}
            for log in self._logs:
                level = log.level.value
                level_counts[level] = level_counts.get(level, 0) + 1

        return {
            "total_logs": total,
            "jobs_tracked": jobs_tracked,
            "by_level": level_counts,
            "max_entries": self.max_entries,
        }


class ValidationLogger:
    """
    Structured logger for validation operations.

    Provides context-aware logging with automatic
    job/stage tracking.
    """

    def __init__(
        self,
        store: Optional[ValidationLogStore] = None,
        module: str = "validation",
        component: Optional[str] = None,
    ):
        self.store = store or get_log_store()
        self.module = module
        self.component = component

        # Current context (can be set for a job)
        self._job_id: Optional[str] = None
        self._job_path: Optional[str] = None
        self._batch_id: Optional[str] = None
        self._current_stage: Optional[str] = None

    def set_job_context(
        self,
        job_id: str,
        job_path: Optional[str] = None,
        batch_id: Optional[str] = None,
    ) -> None:
        """Set the current job context."""
        self._job_id = job_id
        self._job_path = job_path
        self._batch_id = batch_id

    def clear_job_context(self) -> None:
        """Clear the current job context."""
        self._job_id = None
        self._job_path = None
        self._batch_id = None
        self._current_stage = None

    def set_stage(self, stage: str) -> None:
        """Set the current validation stage."""
        self._current_stage = stage

    def _log(
        self,
        level: LogLevel,
        message: str,
        category: LogCategory = LogCategory.VALIDATION,
        stage: Optional[str] = None,
        stage_status: Optional[str] = None,
        error_type: Optional[str] = None,
        error_message: Optional[str] = None,
        error_traceback: Optional[str] = None,
        duration_seconds: Optional[float] = None,
        **metadata,
    ) -> ValidationLogEntry:
        """Create and store a log entry."""
        entry = ValidationLogEntry(
            level=level,
            category=category,
            message=message,
            job_id=self._job_id,
            job_path=self._job_path,
            batch_id=self._batch_id,
            stage=stage or self._current_stage,
            stage_status=stage_status,
            module=self.module,
            component=self.component,
            error_type=error_type,
            error_message=error_message,
            error_traceback=error_traceback,
            duration_seconds=duration_seconds,
            metadata=metadata if metadata else {},
        )

        self.store.add(entry)
        return entry

    # Convenience methods
    def debug(self, message: str, **kwargs) -> ValidationLogEntry:
        return self._log(LogLevel.DEBUG, message, **kwargs)

    def info(self, message: str, **kwargs) -> ValidationLogEntry:
        return self._log(LogLevel.INFO, message, **kwargs)

    def warning(self, message: str, **kwargs) -> ValidationLogEntry:
        return self._log(LogLevel.WARNING, message, **kwargs)

    def error(
        self,
        message: str,
        error: Optional[Exception] = None,
        **kwargs,
    ) -> ValidationLogEntry:
        error_type = None
        error_message = None
        error_traceback = None

        if error:
            error_type = type(error).__name__
            error_message = str(error)
            import traceback
            error_traceback = traceback.format_exc()

        return self._log(
            LogLevel.ERROR,
            message,
            error_type=error_type,
            error_message=error_message,
            error_traceback=error_traceback,
            **kwargs,
        )

    def critical(self, message: str, **kwargs) -> ValidationLogEntry:
        return self._log(LogLevel.CRITICAL, message, **kwargs)

    # Stage lifecycle methods
    def stage_start(self, stage: str) -> ValidationLogEntry:
        """Log the start of a validation stage."""
        self._current_stage = stage
        return self._log(
            LogLevel.INFO,
            f"Starting stage: {stage}",
            stage=stage,
            stage_status="running",
            category=LogCategory.VALIDATION,
        )

    def stage_complete(
        self,
        stage: str,
        passed: bool,
        duration_seconds: float,
        issues: int = 0,
    ) -> ValidationLogEntry:
        """Log the completion of a validation stage."""
        status = "passed" if passed else "failed"
        level = LogLevel.INFO if passed else LogLevel.WARNING

        return self._log(
            level,
            f"Stage {stage}: {status} ({duration_seconds:.2f}s, {issues} issues)",
            stage=stage,
            stage_status=status,
            duration_seconds=duration_seconds,
            category=LogCategory.VALIDATION,
            issues_count=issues,
        )

    def stage_error(
        self,
        stage: str,
        error: Exception,
        duration_seconds: Optional[float] = None,
    ) -> ValidationLogEntry:
        """Log a stage failure with error details."""
        self._current_stage = stage
        return self.error(
            f"Stage {stage} failed: {str(error)}",
            error=error,
            stage=stage,
            stage_status="error",
            duration_seconds=duration_seconds,
            category=LogCategory.VALIDATION,
        )

    # Job lifecycle methods
    def job_start(self, job_id: str, job_path: str) -> ValidationLogEntry:
        """Log the start of job validation."""
        self.set_job_context(job_id, job_path)
        return self._log(
            LogLevel.INFO,
            f"Starting validation: {job_path}",
            category=LogCategory.VALIDATION,
        )

    def job_complete(
        self,
        passed: bool,
        duration_seconds: float,
        stages_passed: int = 0,
        stages_total: int = 0,
    ) -> ValidationLogEntry:
        """Log job validation completion."""
        status = "passed" if passed else "failed"
        level = LogLevel.INFO if passed else LogLevel.ERROR

        entry = self._log(
            level,
            f"Validation {status}: {stages_passed}/{stages_total} stages passed ({duration_seconds:.2f}s)",
            category=LogCategory.VALIDATION,
            duration_seconds=duration_seconds,
            stages_passed=stages_passed,
            stages_total=stages_total,
            validation_result=status,
        )

        self.clear_job_context()
        return entry

    # Queue operations
    def queue_enqueue(self, job_id: str, priority: int) -> ValidationLogEntry:
        """Log job enqueue."""
        return self._log(
            LogLevel.INFO,
            f"Job enqueued with priority {priority}",
            category=LogCategory.QUEUE,
            priority=priority,
        )

    def queue_dequeue(self, job_id: str) -> ValidationLogEntry:
        """Log job dequeue."""
        return self._log(
            LogLevel.INFO,
            "Job dequeued for processing",
            category=LogCategory.QUEUE,
        )


# Global log store instance
_log_store: Optional[ValidationLogStore] = None
_log_store_lock = Lock()


def get_log_store() -> ValidationLogStore:
    """Get the global log store instance."""
    global _log_store
    with _log_store_lock:
        if _log_store is None:
            _log_store = ValidationLogStore()
        return _log_store


def get_logger(
    module: str = "validation",
    component: Optional[str] = None,
) -> ValidationLogger:
    """Get a validation logger."""
    return ValidationLogger(
        store=get_log_store(),
        module=module,
        component=component,
    )


# Context manager for job logging
class JobLogContext:
    """Context manager for job-scoped logging."""

    def __init__(
        self,
        logger: ValidationLogger,
        job_id: str,
        job_path: str,
        batch_id: Optional[str] = None,
    ):
        self.logger = logger
        self.job_id = job_id
        self.job_path = job_path
        self.batch_id = batch_id
        self._start_time: Optional[datetime] = None

    def __enter__(self) -> ValidationLogger:
        self.logger.set_job_context(self.job_id, self.job_path, self.batch_id)
        self.logger.job_start(self.job_id, self.job_path)
        self._start_time = datetime.utcnow()
        return self.logger

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = (datetime.utcnow() - self._start_time).total_seconds()
        if exc_type:
            self.logger.error(
                f"Job failed with exception: {exc_val}",
                error=exc_val,
            )
        self.logger.clear_job_context()
        return False
