"""
Base classes for validation stages.

Each validation stage inherits from ValidationStage and implements
the validate() method. Stages are executed sequentially by the pipeline.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, TYPE_CHECKING
from enum import Enum
import time

if TYPE_CHECKING:
    from ..pipeline.context import ValidationContext


class Severity(Enum):
    """Issue severity levels."""
    ERROR = "ERROR"      # Blocking issue
    WARNING = "WARNING"  # Non-blocking issue
    INFO = "INFO"        # Informational


@dataclass
class ValidationIssue:
    """A single validation issue found during a stage."""
    stage: str
    severity: Severity
    message: str
    line: Optional[int] = None
    column: Optional[int] = None
    suggestion: Optional[str] = None
    rule: Optional[str] = None  # Rule identifier (e.g., "s3a_protocol")

    def to_dict(self) -> dict:
        return {
            "stage": self.stage,
            "severity": self.severity.value,
            "message": self.message,
            "line": self.line,
            "column": self.column,
            "suggestion": self.suggestion,
            "rule": self.rule,
        }


@dataclass
class StageResult:
    """Result of a single validation stage."""
    stage: str
    passed: bool
    duration_seconds: float
    issues: List[ValidationIssue] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)
    skipped: bool = False
    skip_reason: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "stage": self.stage,
            "passed": self.passed,
            "duration_seconds": self.duration_seconds,
            "issues": [i.to_dict() for i in self.issues],
            "details": self.details,
            "skipped": self.skipped,
            "skip_reason": self.skip_reason,
        }

    @property
    def error_count(self) -> int:
        return len([i for i in self.issues if i.severity == Severity.ERROR])

    @property
    def warning_count(self) -> int:
        return len([i for i in self.issues if i.severity == Severity.WARNING])


class ValidationStage(ABC):
    """
    Base class for all validation stages.

    Subclasses must implement:
        - name: str - unique stage identifier
        - validate(context) - the validation logic

    Optional overrides:
        - requires_spark: bool - whether this stage needs SparkSession
        - blocking: bool - whether failure stops subsequent stages
        - should_run(context) - conditional execution logic
    """

    name: str = ""
    requires_spark: bool = False
    blocking: bool = True

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the stage with optional configuration.

        Args:
            config: Stage-specific configuration from framework.yaml
        """
        self.config = config or {}
        self.timeout_seconds = self.config.get("timeout_seconds", 60)
        self.enabled = self.config.get("enabled", True)

    @abstractmethod
    def validate(self, context: "ValidationContext") -> StageResult:
        """
        Execute the validation logic.

        Args:
            context: ValidationContext with file content, spark session, etc.

        Returns:
            StageResult with pass/fail status and any issues found
        """
        pass

    def should_run(self, context: "ValidationContext") -> bool:
        """
        Determine if this stage should run.

        Override this method to add conditional execution logic.

        Args:
            context: ValidationContext

        Returns:
            True if stage should run, False to skip
        """
        if not self.enabled:
            return False

        if self.requires_spark and context.spark is None:
            return False

        return True

    def create_result(
        self,
        passed: bool,
        issues: List[ValidationIssue],
        duration: float,
        details: Optional[Dict[str, Any]] = None,
    ) -> StageResult:
        """Helper to create a StageResult."""
        return StageResult(
            stage=self.name,
            passed=passed,
            duration_seconds=duration,
            issues=issues,
            details=details or {},
        )

    def create_skipped_result(self, reason: str) -> StageResult:
        """Helper to create a skipped StageResult."""
        return StageResult(
            stage=self.name,
            passed=True,
            duration_seconds=0.0,
            skipped=True,
            skip_reason=reason,
        )

    def create_issue(
        self,
        severity: Severity,
        message: str,
        line: Optional[int] = None,
        column: Optional[int] = None,
        suggestion: Optional[str] = None,
        rule: Optional[str] = None,
    ) -> ValidationIssue:
        """Helper to create a ValidationIssue for this stage."""
        return ValidationIssue(
            stage=self.name,
            severity=severity,
            message=message,
            line=line,
            column=column,
            suggestion=suggestion,
            rule=rule,
        )

    def run_with_timing(self, context: "ValidationContext") -> StageResult:
        """
        Run validation with timing. Used by the pipeline.

        Args:
            context: ValidationContext

        Returns:
            StageResult with accurate duration
        """
        if not self.should_run(context):
            reason = "disabled" if not self.enabled else "missing requirements"
            return self.create_skipped_result(reason)

        start = time.time()
        try:
            result = self.validate(context)
            result.duration_seconds = time.time() - start
            return result
        except Exception as e:
            return StageResult(
                stage=self.name,
                passed=False,
                duration_seconds=time.time() - start,
                issues=[self.create_issue(
                    Severity.ERROR,
                    f"Stage execution failed: {e}",
                )],
                details={"exception": str(e)},
            )
