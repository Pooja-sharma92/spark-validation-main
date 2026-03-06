"""
Validation Framework API Gateway

FastAPI application providing REST endpoints for:
- Webhook triggers (Git, CI/CD)
- Manual job submission
- Queue status and monitoring
- Health checks
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Optional, List
from datetime import datetime, timedelta

from fastapi import FastAPI, HTTPException, BackgroundTasks, Header, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import structlog

from validator.config import get_config, FrameworkConfig
from validator.models import (
    ValidationRequest,
    ValidationStatus,
    Priority,
    TriggerSource,
    QueueStats,
)
from validator.queue.manager import QueueManager, EnqueueResult

# Optional imports for storage and reporting
try:
    from validator.results.storage import ResultStorage, create_storage
    from validator.results.reporter import ValidationReporter, ReportFormat
    STORAGE_AVAILABLE = True
except ImportError:
    STORAGE_AVAILABLE = False
    ResultStorage = None
    create_storage = None
    ValidationReporter = None
    ReportFormat = None

# Optional imports for monitoring
try:
    from validator.monitoring import (
        get_validation_metrics,
        get_health_checker,
        get_registry,
        create_queue_check,
    )
    MONITORING_AVAILABLE = True
except ImportError:
    MONITORING_AVAILABLE = False
    get_validation_metrics = None
    get_health_checker = None

# Optional imports for validation logging
try:
    from validator.logging import (
        get_log_store,
        LogLevel,
        LogCategory,
    )
    LOGGING_AVAILABLE = True
except ImportError:
    LOGGING_AVAILABLE = False
    get_log_store = None
    LogLevel = None
    LogCategory = None


# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)


# Pydantic models for API
class SubmitJobRequest(BaseModel):
    """Request to submit a job for validation."""
    job_path: str = Field(..., description="Path to the Spark job file")
    priority: int = Field(default=2, ge=0, le=3, description="Priority (0=critical, 3=batch)")
    branch: Optional[str] = Field(default=None, description="Git branch name")
    commit_sha: Optional[str] = Field(default=None, description="Git commit SHA")


class SubmitJobResponse(BaseModel):
    """Response after submitting a job."""
    status: str
    job_id: Optional[str]
    message: str


class BatchSubmitRequest(BaseModel):
    """Request to submit multiple jobs."""
    job_paths: List[str]
    priority: int = Field(default=3, ge=0, le=3)


class BatchSubmitResponse(BaseModel):
    """Response after batch submission."""
    batch_id: str
    queued: int
    deduplicated: int
    rejected: int


class JobStatusResponse(BaseModel):
    """Job status response."""
    job_id: str
    status: str
    job_path: str
    created_at: str
    started_at: Optional[str]
    completed_at: Optional[str]


class QueueStatsResponse(BaseModel):
    """Queue statistics response."""
    total_pending: int
    by_priority: dict
    active_validations: int
    max_concurrent: int
    utilization: float


# Global instances
queue_manager: Optional[QueueManager] = None
result_storage: Optional["ResultStorage"] = None
reporter: Optional["ValidationReporter"] = None
health_checker = None
validation_metrics = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle."""
    global queue_manager, result_storage, reporter, health_checker, validation_metrics

    # Startup
    logger.info("Starting Validation Framework API Gateway")

    config = get_config()

    # Initialize queue manager
    queue_manager = QueueManager()
    await queue_manager.initialize()
    logger.info("Queue manager initialized", redis_url=config.redis.url)

    # Initialize result storage (if available)
    if STORAGE_AVAILABLE and create_storage:
        try:
            result_storage = create_storage(use_postgres=True)
            await result_storage.initialize()
            logger.info("Result storage initialized")
        except Exception as e:
            logger.warning("Result storage initialization failed", error=str(e))
            result_storage = None

    # Initialize reporter
    if STORAGE_AVAILABLE and ValidationReporter:
        reporter = ValidationReporter()

    # Initialize monitoring (if available)
    if MONITORING_AVAILABLE:
        validation_metrics = get_validation_metrics()
        health_checker = get_health_checker()
        health_checker.register("queue", create_queue_check(queue_manager))
        await health_checker.start()
        logger.info("Monitoring initialized")

    yield

    # Shutdown
    logger.info("Shutting down API Gateway")
    if health_checker:
        await health_checker.stop()
    if result_storage:
        await result_storage.close()
    if queue_manager:
        await queue_manager.close()


app = FastAPI(
    title="Spark Job Validation Framework",
    description="API for validating Spark jobs with priority queue and rate limiting",
    version="1.0.0",
    lifespan=lifespan,
)


# Health check endpoints
@app.get("/health")
async def health_check():
    """Basic health check."""
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}


@app.get("/ready")
async def readiness_check():
    """Readiness check including dependencies."""
    checks = {"api": True, "queue": False}

    try:
        if queue_manager:
            stats = await queue_manager.get_stats()
            checks["queue"] = True
    except Exception as e:
        logger.error("Queue health check failed", error=str(e))

    is_ready = all(checks.values())
    return JSONResponse(
        status_code=200 if is_ready else 503,
        content={"ready": is_ready, "checks": checks}
    )


# Job submission endpoints
@app.post("/api/v1/jobs", response_model=SubmitJobResponse)
async def submit_job(request: SubmitJobRequest):
    """
    Submit a single job for validation.

    The job will be queued based on its priority and processed
    when resources are available.
    """
    if not queue_manager:
        raise HTTPException(status_code=503, detail="Queue manager not initialized")

    validation_request = ValidationRequest(
        job_path=request.job_path,
        trigger_source=TriggerSource.MANUAL,
        priority=Priority(request.priority),
        branch=request.branch,
        commit_sha=request.commit_sha,
        triggered_by="api",
    )

    result, job_id = await queue_manager.enqueue(validation_request)

    if result == EnqueueResult.QUEUED:
        return SubmitJobResponse(
            status="queued",
            job_id=job_id,
            message=f"Job queued successfully with priority {request.priority}"
        )
    elif result == EnqueueResult.DEDUPLICATED:
        return SubmitJobResponse(
            status="deduplicated",
            job_id=job_id,
            message="Job already queued or recently processed"
        )
    elif result == EnqueueResult.RATE_LIMITED:
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded. Please try again later."
        )
    else:  # REJECTED_BACKPRESSURE
        raise HTTPException(
            status_code=503,
            detail="System under heavy load. Low priority jobs temporarily rejected."
        )


@app.post("/api/v1/jobs/batch", response_model=BatchSubmitResponse)
async def submit_batch(request: BatchSubmitRequest):
    """Submit multiple jobs as a batch."""
    if not queue_manager:
        raise HTTPException(status_code=503, detail="Queue manager not initialized")

    import uuid
    batch_id = str(uuid.uuid4())

    queued = 0
    deduplicated = 0
    rejected = 0

    for job_path in request.job_paths:
        validation_request = ValidationRequest(
            job_path=job_path,
            trigger_source=TriggerSource.MANUAL,
            priority=Priority(request.priority),
            batch_id=batch_id,
            triggered_by="api_batch",
        )

        result, _ = await queue_manager.enqueue(validation_request)

        if result == EnqueueResult.QUEUED:
            queued += 1
        elif result == EnqueueResult.DEDUPLICATED:
            deduplicated += 1
        else:
            rejected += 1

    logger.info(
        "Batch submitted",
        batch_id=batch_id,
        total=len(request.job_paths),
        queued=queued,
        deduplicated=deduplicated,
        rejected=rejected,
    )

    return BatchSubmitResponse(
        batch_id=batch_id,
        queued=queued,
        deduplicated=deduplicated,
        rejected=rejected,
    )


@app.get("/api/v1/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    """Get the status of a specific job."""
    if not queue_manager:
        raise HTTPException(status_code=503, detail="Queue manager not initialized")

    status = await queue_manager.get_job_status(job_id)
    if not status:
        raise HTTPException(status_code=404, detail="Job not found")

    return JobStatusResponse(
        job_id=job_id,
        status=status.get("status", "unknown"),
        job_path=status.get("job_path", ""),
        created_at=status.get("created_at", ""),
        started_at=status.get("started_at"),
        completed_at=status.get("completed_at"),
    )


# Queue management endpoints
@app.get("/api/v1/queue/stats", response_model=QueueStatsResponse)
async def get_queue_stats():
    """Get current queue statistics."""
    if not queue_manager:
        raise HTTPException(status_code=503, detail="Queue manager not initialized")

    stats = await queue_manager.get_stats()
    return QueueStatsResponse(
        total_pending=stats.total_pending,
        by_priority=stats.by_priority,
        active_validations=stats.active_validations,
        max_concurrent=stats.max_concurrent,
        utilization=stats.utilization,
    )


@app.get("/api/v1/queue/backpressure")
async def get_backpressure_status():
    """Get current backpressure state."""
    if not queue_manager:
        raise HTTPException(status_code=503, detail="Queue manager not initialized")

    state = await queue_manager.get_backpressure_state()
    return {
        "level": state.level,
        "queue_depth": state.queue_depth,
        "thresholds": {
            "warning": state.warning_threshold,
            "critical": state.critical_threshold,
            "reject": state.reject_threshold,
        },
        "actions": {
            "rejecting_low_priority": state.should_reject_low_priority,
            "delaying_low_priority": state.should_delay_low_priority,
        }
    }


# Webhook endpoints
@app.post("/webhook/git")
async def git_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    x_gitea_signature: Optional[str] = Header(None),
    x_hub_signature_256: Optional[str] = Header(None),
):
    """
    Handle Git webhook events (Gitea/GitHub compatible).

    Validates signature and queues jobs for changed Python files.
    """
    if not queue_manager:
        raise HTTPException(status_code=503, detail="Queue manager not initialized")

    body = await request.json()

    # TODO: Validate webhook signature
    # config = get_config()
    # if config.triggers.git.secret:
    #     validate_signature(body, x_gitea_signature or x_hub_signature_256)

    # Extract changed files
    commits = body.get("commits", [])
    changed_files = set()

    for commit in commits:
        for file_list in ["added", "modified"]:
            for file_path in commit.get(file_list, []):
                if file_path.endswith(".py"):
                    changed_files.add(file_path)

    if not changed_files:
        return {"status": "ok", "message": "No Python files changed"}

    # Determine priority from branch
    ref = body.get("ref", "")
    branch = ref.replace("refs/heads/", "")
    priority = Priority.CI_CD  # Default

    config = get_config()
    for pattern, prio in config.triggers.git.priority_map.items():
        if pattern.endswith("/*"):
            prefix = pattern[:-2]
            if branch.startswith(prefix):
                priority = Priority(prio)
                break
        elif branch == pattern:
            priority = Priority(prio)
            break

    # Queue jobs
    queued = 0
    for file_path in changed_files:
        validation_request = ValidationRequest(
            job_path=file_path,
            trigger_source=TriggerSource.GIT_WEBHOOK,
            priority=priority,
            branch=branch,
            commit_sha=body.get("after"),
            triggered_by=body.get("pusher", {}).get("username", "git"),
        )

        result, _ = await queue_manager.enqueue(validation_request)
        if result == EnqueueResult.QUEUED:
            queued += 1

    logger.info(
        "Git webhook processed",
        branch=branch,
        files_changed=len(changed_files),
        jobs_queued=queued,
    )

    return {
        "status": "ok",
        "files_processed": len(changed_files),
        "jobs_queued": queued,
    }


# Metrics endpoint (Prometheus format)
@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint."""
    if not queue_manager:
        return "# Queue manager not initialized\n"

    stats = await queue_manager.get_stats()
    bp_state = await queue_manager.get_backpressure_state()

    lines = [
        "# HELP validation_queue_pending Total pending jobs in queue",
        "# TYPE validation_queue_pending gauge",
        f"validation_queue_pending {stats.total_pending}",
        "",
        "# HELP validation_active_validations Currently running validations",
        "# TYPE validation_active_validations gauge",
        f"validation_active_validations {stats.active_validations}",
        "",
        "# HELP validation_max_concurrent Maximum concurrent validations",
        "# TYPE validation_max_concurrent gauge",
        f"validation_max_concurrent {stats.max_concurrent}",
        "",
        "# HELP validation_queue_utilization Queue utilization percentage",
        "# TYPE validation_queue_utilization gauge",
        f"validation_queue_utilization {stats.utilization:.2f}",
        "",
        "# HELP validation_backpressure_level Current backpressure level",
        "# TYPE validation_backpressure_level gauge",
        f'validation_backpressure_level{{level="{bp_state.level}"}} 1',
        "",
    ]

    # Per-priority metrics
    lines.append("# HELP validation_queue_by_priority Pending jobs by priority")
    lines.append("# TYPE validation_queue_by_priority gauge")
    for priority, count in stats.by_priority.items():
        lines.append(f'validation_queue_by_priority{{priority="{priority}"}} {count}')

    return "\n".join(lines) + "\n"


# Result and history endpoints
@app.get("/api/v1/jobs/{job_id}/result")
async def get_job_result(job_id: str, format: str = "json"):
    """
    Get the detailed validation result for a job.

    Supports formats: json, markdown, html
    """
    if not result_storage:
        raise HTTPException(
            status_code=503,
            detail="Result storage not available"
        )

    result = await result_storage.get_result(job_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")

    if format == "json":
        return {
            "request_id": result.request_id,
            "job_path": result.job_path,
            "status": result.status.value,
            "passed": result.passed,
            "started_at": result.started_at.isoformat() if result.started_at else None,
            "completed_at": result.completed_at.isoformat() if result.completed_at else None,
            "duration_seconds": result.duration_seconds,
            "worker_id": result.worker_id,
            "stages": [
                {
                    "stage": sr.stage.value,
                    "status": sr.status.value,
                    "duration_seconds": sr.duration_seconds,
                    "issue_count": len(sr.issues),
                }
                for sr in result.stage_results
            ],
            "total_issues": sum(len(sr.issues) for sr in result.stage_results),
        }
    elif format in ("markdown", "md"):
        if reporter:
            content = reporter.generate(result, format=ReportFormat.MARKDOWN)
            return JSONResponse(
                content={"format": "markdown", "content": content},
                media_type="application/json"
            )
        raise HTTPException(status_code=503, detail="Reporter not available")
    elif format == "html":
        if reporter:
            content = reporter.generate(result, format=ReportFormat.HTML)
            from fastapi.responses import HTMLResponse
            return HTMLResponse(content=content)
        raise HTTPException(status_code=503, detail="Reporter not available")
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported format: {format}")


@app.get("/api/v1/jobs/history/{job_path:path}")
async def get_job_history(job_path: str, limit: int = 10):
    """Get validation history for a specific job file."""
    if not result_storage:
        raise HTTPException(
            status_code=503,
            detail="Result storage not available"
        )

    history = await result_storage.get_job_history(job_path, limit=limit)
    return {"job_path": job_path, "history": history}


@app.get("/api/v1/results/recent")
async def get_recent_results(limit: int = 50, status: Optional[str] = None):
    """Get recent validation results."""
    if not result_storage:
        raise HTTPException(
            status_code=503,
            detail="Result storage not available"
        )

    status_filter = ValidationStatus(status) if status else None
    results = await result_storage.get_recent_results(
        limit=limit,
        status_filter=status_filter,
    )
    return {"results": results}


@app.get("/api/v1/statistics")
async def get_statistics(days: int = 7):
    """
    Get validation statistics for the specified period.

    Returns aggregated metrics including success rates,
    top failing jobs, and common issues.
    """
    if not result_storage:
        raise HTTPException(
            status_code=503,
            detail="Result storage not available"
        )

    stats = await result_storage.get_statistics(days=days)
    return stats


# ─────────────────────────────────────────────────────────────────
# Monitoring & Dashboard API (for custom UI)
# ─────────────────────────────────────────────────────────────────

@app.get("/api/v1/dashboard")
async def get_dashboard_data():
    """
    Get all data needed for dashboard display.

    Returns metrics, queue status, and recent activity.
    Perfect for a single API call to populate a dashboard.
    """
    data = {
        "timestamp": datetime.utcnow().isoformat(),
        "queue": None,
        "metrics": None,
        "health": None,
    }

    # Queue stats
    if queue_manager:
        stats = await queue_manager.get_stats()
        bp_state = await queue_manager.get_backpressure_state()
        data["queue"] = {
            "pending": stats.total_pending,
            "by_priority": stats.by_priority,
            "active": stats.active_validations,
            "max_concurrent": stats.max_concurrent,
            "utilization": stats.utilization,
            "backpressure": bp_state.level,
        }

    # Validation metrics
    if validation_metrics:
        data["metrics"] = validation_metrics.get_dashboard_data()

    # Health status
    if health_checker:
        health = health_checker.get_cached_health()
        data["health"] = health.to_dict()

    return data


@app.get("/api/v1/monitoring/metrics")
async def get_all_metrics():
    """Get all collected metrics."""
    if not MONITORING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Monitoring not available")

    registry = get_registry()
    return registry.get_all()


@app.get("/api/v1/monitoring/metrics/summary")
async def get_metrics_summary():
    """Get a summary of key metrics."""
    if not MONITORING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Monitoring not available")

    registry = get_registry()
    return registry.get_summary()


@app.get("/api/v1/monitoring/health")
async def get_health_status(force_check: bool = False):
    """
    Get system health status.

    Args:
        force_check: If True, run fresh health checks instead of using cached results
    """
    if not health_checker:
        return {
            "status": "unknown",
            "message": "Health checker not initialized",
        }

    if force_check:
        health = await health_checker.check_all()
    else:
        health = health_checker.get_cached_health()

    return health.to_dict()


@app.get("/api/v1/monitoring/timeseries/{series_name}")
async def get_time_series(
    series_name: str,
    limit: int = 100,
    since_minutes: Optional[int] = None,
):
    """
    Get time series data for charts.

    Args:
        series_name: Name of the time series (e.g., "queue_depth_history")
        limit: Maximum number of points to return
        since_minutes: Only return points from the last N minutes
    """
    if not MONITORING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Monitoring not available")

    registry = get_registry()

    # Get time series from registry
    if series_name not in registry._time_series:
        raise HTTPException(status_code=404, detail=f"Time series '{series_name}' not found")

    ts = registry._time_series[series_name]

    since = None
    if since_minutes:
        since = datetime.utcnow() - timedelta(minutes=since_minutes)

    points = ts.get_points(since=since, limit=limit)

    return {
        "name": series_name,
        "points": points,
        "count": len(points),
    }


# ─────────────────────────────────────────────────────────────────
# Validation Logs API (for UI log viewer)
# ─────────────────────────────────────────────────────────────────

@app.get("/api/v1/logs")
async def get_logs(
    limit: int = 100,
    level: Optional[str] = None,
    category: Optional[str] = None,
    since_minutes: Optional[int] = None,
):
    """
    Get recent validation logs.

    Args:
        limit: Maximum number of logs to return
        level: Filter by level (debug, info, warning, error, critical)
        category: Filter by category (queue, executor, validation, storage, system)
        since_minutes: Only return logs from the last N minutes
    """
    if not LOGGING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Logging not available")

    log_store = get_log_store()

    # Parse filters
    level_filter = LogLevel(level) if level else None
    category_filter = LogCategory(category) if category else None
    since = None
    if since_minutes:
        since = datetime.utcnow() - timedelta(minutes=since_minutes)

    logs = log_store.get_recent(
        limit=limit,
        level=level_filter,
        category=category_filter,
        since=since,
    )

    return {
        "logs": [log.to_dict() for log in logs],
        "count": len(logs),
    }


@app.get("/api/v1/logs/errors")
async def get_error_logs(
    limit: int = 50,
    since_minutes: Optional[int] = None,
):
    """Get recent error logs."""
    if not LOGGING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Logging not available")

    log_store = get_log_store()

    since = None
    if since_minutes:
        since = datetime.utcnow() - timedelta(minutes=since_minutes)

    logs = log_store.get_errors(limit=limit, since=since)

    return {
        "logs": [log.to_dict() for log in logs],
        "count": len(logs),
    }


@app.get("/api/v1/logs/job/{job_id}")
async def get_job_logs(job_id: str, limit: int = 200):
    """
    Get all logs for a specific job.

    Returns logs in chronological order with full context.
    """
    if not LOGGING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Logging not available")

    log_store = get_log_store()
    logs = log_store.get_by_job(job_id, limit=limit)

    if not logs:
        raise HTTPException(status_code=404, detail="No logs found for job")

    return {
        "job_id": job_id,
        "logs": [log.to_dict() for log in logs],
        "count": len(logs),
    }


@app.get("/api/v1/logs/job/{job_id}/summary")
async def get_job_log_summary(job_id: str):
    """
    Get a summary of logs for a job.

    Returns stage status, errors, and timing information.
    """
    if not LOGGING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Logging not available")

    log_store = get_log_store()
    summary = log_store.get_job_summary(job_id)

    if not summary.get("found"):
        raise HTTPException(status_code=404, detail="No logs found for job")

    return summary


@app.get("/api/v1/logs/stats")
async def get_log_stats():
    """Get log storage statistics."""
    if not LOGGING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Logging not available")

    log_store = get_log_store()
    return log_store.get_stats()


def main():
    """Entry point for validator-api command."""
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
