"""
Core data models for the Spark Job Validation Framework.

This module defines all the data structures used throughout the framework,
including validation requests, results, and status tracking.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import IntEnum, Enum
from typing import Optional, Dict, Any, List
import hashlib
import uuid


class Priority(IntEnum):
    """
    Validation priority levels.

    Lower number = higher priority. This allows natural sorting where
    priority 0 (CRITICAL) is processed before priority 3 (BATCH).
    """
    CRITICAL = 0  # Hotfix branches, production issues
    MANUAL = 1    # Manual triggers, PR reviews
    CI_CD = 2     # CI/CD pipelines, feature branches
    BATCH = 3     # Bulk scans, scheduled validations


class TriggerSource(str, Enum):
    """Source that triggered the validation."""
    GIT_WEBHOOK = "git_webhook"
    FILE_WATCHER = "file_watcher"
    CI_CD = "ci_cd"
    MANUAL = "manual"
    SCHEDULED = "scheduled"


class ValidationStatus(str, Enum):
    """Status of a validation job throughout its lifecycle."""
    PENDING = "pending"       # Queued, waiting to be processed
    RUNNING = "running"       # Currently being validated
    COMPLETED = "completed"   # Finished successfully
    FAILED = "failed"         # Validation found issues
    ERROR = "error"           # System error during validation
    TIMEOUT = "timeout"       # Exceeded time limit
    SKIPPED = "skipped"       # Skipped due to deduplication or backpressure


class ValidationStage(str, Enum):
    """Individual validation stages in the pipeline."""
    SYNTAX = "syntax"
    LOGIC = "logic"
    DATA = "data"
    EXPECTATIONS = "expectations"
    AI_ANALYSIS = "ai_analysis"


class Severity(str, Enum):
    """Severity level of validation issues."""
    CRITICAL = "critical"
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationRequest:
    """
    A request to validate a Spark job.

    This is the primary input to the validation system. Requests are created
    by trigger sources (webhooks, file watchers, etc.) and queued for processing.
    """
    job_path: str
    trigger_source: TriggerSource
    priority: Priority = Priority.CI_CD

    # Unique identifier for this request
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    # Batch grouping for burst handling
    batch_id: Optional[str] = None

    # Git-related metadata
    commit_sha: Optional[str] = None
    branch: Optional[str] = None

    # Who/what triggered this validation
    triggered_by: Optional[str] = None

    # Timestamps
    created_at: datetime = field(default_factory=datetime.utcnow)

    # Additional context
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Convert string enums if necessary."""
        if isinstance(self.trigger_source, str):
            self.trigger_source = TriggerSource(self.trigger_source)
        if isinstance(self.priority, int):
            self.priority = Priority(self.priority)

    @property
    def dedup_key(self) -> str:
        """
        Generate a deduplication key based on job path and content hash.

        Two requests for the same job within a short window should have
        the same dedup_key to prevent redundant validations.
        """
        key_data = f"{self.job_path}:{self.commit_sha or 'latest'}"
        return hashlib.sha256(key_data.encode()).hexdigest()[:16]

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary for Redis storage."""
        return {
            "id": self.id,
            "job_path": self.job_path,
            "trigger_source": self.trigger_source.value,
            "priority": self.priority.value,
            "batch_id": self.batch_id,
            "commit_sha": self.commit_sha,
            "branch": self.branch,
            "triggered_by": self.triggered_by,
            "created_at": self.created_at.isoformat(),
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ValidationRequest":
        """Deserialize from dictionary."""
        data = data.copy()
        data["trigger_source"] = TriggerSource(data["trigger_source"])
        data["priority"] = Priority(data["priority"])
        data["created_at"] = datetime.fromisoformat(data["created_at"])
        return cls(**data)


@dataclass
class ValidationIssue:
    """A single issue found during validation."""
    stage: ValidationStage
    severity: Severity
    message: str
    file_path: Optional[str] = None
    line_number: Optional[int] = None
    rule_id: Optional[str] = None
    suggestion: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "stage": self.stage.value,
            "severity": self.severity.value,
            "message": self.message,
            "file_path": self.file_path,
            "line_number": self.line_number,
            "rule_id": self.rule_id,
            "suggestion": self.suggestion,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ValidationIssue":
        data = data.copy()
        data["stage"] = ValidationStage(data["stage"])
        data["severity"] = Severity(data["severity"])
        return cls(**data)


@dataclass
class StageResult:
    """Result of a single validation stage."""
    stage: ValidationStage
    status: ValidationStatus
    duration_seconds: float
    issues: List[ValidationIssue] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        """Check if stage passed (no critical/error issues)."""
        return not any(
            issue.severity in (Severity.CRITICAL, Severity.ERROR)
            for issue in self.issues
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "stage": self.stage.value,
            "status": self.status.value,
            "duration_seconds": self.duration_seconds,
            "issues": [i.to_dict() for i in self.issues],
            "metrics": self.metrics,
        }


@dataclass
class ValidationResult:
    """
    Complete result of validating a Spark job.

    Contains results from all validation stages, overall status,
    and metadata about the validation run.
    """
    request_id: str
    job_path: str
    status: ValidationStatus

    # Results from each stage
    stage_results: List[StageResult] = field(default_factory=list)

    # Timing
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    # Worker info
    worker_id: Optional[str] = None

    # Error details if status is ERROR
    error_message: Optional[str] = None
    error_traceback: Optional[str] = None

    @property
    def duration_seconds(self) -> Optional[float]:
        """Total validation duration in seconds."""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

    @property
    def all_issues(self) -> List[ValidationIssue]:
        """Aggregate all issues from all stages."""
        issues = []
        for stage_result in self.stage_results:
            issues.extend(stage_result.issues)
        return issues

    @property
    def passed(self) -> bool:
        """Check if validation passed (all stages passed)."""
        return self.status == ValidationStatus.COMPLETED and all(
            sr.passed for sr in self.stage_results
        )

    @property
    def summary(self) -> Dict[str, int]:
        """Summary of issues by severity."""
        summary = {s.value: 0 for s in Severity}
        for issue in self.all_issues:
            summary[issue.severity.value] += 1
        return summary

    def to_dict(self) -> Dict[str, Any]:
        return {
            "request_id": self.request_id,
            "job_path": self.job_path,
            "status": self.status.value,
            "stage_results": [sr.to_dict() for sr in self.stage_results],
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "worker_id": self.worker_id,
            "error_message": self.error_message,
            "error_traceback": self.error_traceback,
            "duration_seconds": self.duration_seconds,
            "passed": self.passed,
            "summary": self.summary,
        }


@dataclass
class BatchSummary:
    """Summary of a batch validation run."""
    batch_id: str
    total_jobs: int
    completed: int
    failed: int
    errors: int
    skipped: int

    started_at: datetime
    completed_at: Optional[datetime] = None

    results: List[ValidationResult] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        """Percentage of jobs that passed validation."""
        if self.total_jobs == 0:
            return 0.0
        return (self.completed - self.failed) / self.total_jobs * 100

    @property
    def in_progress(self) -> int:
        """Number of jobs still running."""
        return self.total_jobs - self.completed - self.failed - self.errors - self.skipped


@dataclass
class QueueStats:
    """Statistics about the validation queue."""
    total_pending: int
    by_priority: Dict[str, int]
    active_validations: int
    max_concurrent: int
    oldest_pending_seconds: Optional[float] = None

    @property
    def utilization(self) -> float:
        """Current utilization as percentage."""
        if self.max_concurrent == 0:
            return 0.0
        return self.active_validations / self.max_concurrent * 100


@dataclass
class BackpressureState:
    """Current backpressure state of the system."""
    queue_depth: int
    warning_threshold: int
    critical_threshold: int
    reject_threshold: int

    @property
    def level(self) -> str:
        """Current backpressure level."""
        if self.queue_depth >= self.reject_threshold:
            return "reject"
        elif self.queue_depth >= self.critical_threshold:
            return "critical"
        elif self.queue_depth >= self.warning_threshold:
            return "warning"
        return "normal"

    @property
    def should_reject_low_priority(self) -> bool:
        """Whether to reject low priority requests."""
        return self.queue_depth >= self.reject_threshold

    @property
    def should_delay_low_priority(self) -> bool:
        """Whether to delay/demote low priority requests."""
        return self.queue_depth >= self.critical_threshold
