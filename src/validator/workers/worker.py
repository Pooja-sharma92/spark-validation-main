"""
Validation Worker - Consumes jobs from queue and executes validation.

Workers poll the queue for pending jobs, acquire concurrency slots,
and execute the validation pipeline. They report results back to
the queue manager and handle errors gracefully.
"""

import asyncio
import signal
import traceback
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, Callable, Awaitable, List
import structlog

from validator.models import (
    ValidationRequest,
    ValidationResult,
    ValidationStatus,
    ValidationStage,
    StageResult,
    ValidationIssue,
    Severity,
)
from validator.queue.manager import QueueManager
from validator.queue.concurrency import ConcurrencySlot
from validator.config import get_config, FrameworkConfig

# Optional imports for storage and notifications
try:
    from validator.results.storage import ResultStorage, InMemoryResultStorage
    STORAGE_AVAILABLE = True
except ImportError:
    STORAGE_AVAILABLE = False
    ResultStorage = None
    InMemoryResultStorage = None

try:
    from validator.results.notifier import NotificationManager
    from validator.results.reporter import ValidationReporter, ReportFormat
    NOTIFIER_AVAILABLE = True
except ImportError:
    NOTIFIER_AVAILABLE = False
    NotificationManager = None
    ValidationReporter = None
    ReportFormat = None


logger = structlog.get_logger(__name__)


class WorkerState(str, Enum):
    """Worker lifecycle states."""
    IDLE = "idle"
    POLLING = "polling"
    PROCESSING = "processing"
    STOPPING = "stopping"
    STOPPED = "stopped"


@dataclass
class WorkerStats:
    """Statistics for a worker."""
    jobs_processed: int = 0
    jobs_succeeded: int = 0
    jobs_failed: int = 0
    total_processing_seconds: float = 0.0
    last_job_at: Optional[datetime] = None

    @property
    def success_rate(self) -> float:
        if self.jobs_processed == 0:
            return 0.0
        return self.jobs_succeeded / self.jobs_processed * 100

    @property
    def avg_processing_seconds(self) -> float:
        if self.jobs_processed == 0:
            return 0.0
        return self.total_processing_seconds / self.jobs_processed


# Type for validation stage function
StageFunction = Callable[[str, dict], Awaitable[StageResult]]


class ValidationWorker:
    """
    Worker that processes validation jobs from the queue.

    Each worker:
    - Polls the queue for pending jobs
    - Acquires a concurrency slot before processing
    - Executes validation stages in sequence
    - Reports results and releases resources
    - Handles graceful shutdown
    """

    def __init__(
        self,
        queue_manager: QueueManager,
        worker_id: Optional[str] = None,
        config: Optional[FrameworkConfig] = None,
        storage: Optional["ResultStorage"] = None,
        notifier: Optional["NotificationManager"] = None,
        reporter: Optional["ValidationReporter"] = None,
    ):
        self.queue_manager = queue_manager
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.config = config or get_config()

        # Storage and notification integrations
        self.storage = storage
        self.notifier = notifier
        self.reporter = reporter

        self._state = WorkerState.IDLE
        self._stats = WorkerStats()
        self._current_job: Optional[ValidationRequest] = None
        self._current_slot: Optional[ConcurrencySlot] = None
        self._shutdown_event = asyncio.Event()

        # Register validation stages
        self._stages: List[tuple[ValidationStage, StageFunction]] = []
        self._register_default_stages()

    def _register_default_stages(self) -> None:
        """Register default validation stages based on config."""
        stage_configs = self.config.validation.stages

        for stage_config in stage_configs:
            if not stage_config.enabled:
                continue

            stage = ValidationStage(stage_config.name)
            func = self._get_stage_function(stage)
            if func:
                self._stages.append((stage, func))

    def _get_stage_function(self, stage: ValidationStage) -> Optional[StageFunction]:
        """Get the validation function for a stage."""
        stage_map = {
            ValidationStage.SYNTAX: self._validate_syntax,
            ValidationStage.LOGIC: self._validate_logic,
            ValidationStage.DATA: self._validate_data,
            ValidationStage.EXPECTATIONS: self._validate_expectations,
            ValidationStage.AI_ANALYSIS: self._validate_ai,
        }
        return stage_map.get(stage)

    @property
    def state(self) -> WorkerState:
        return self._state

    @property
    def stats(self) -> WorkerStats:
        return self._stats

    async def start(self) -> None:
        """Start the worker loop."""
        logger.info("Worker starting", worker_id=self.worker_id)
        self._state = WorkerState.POLLING
        self._shutdown_event.clear()

        try:
            while not self._shutdown_event.is_set():
                await self._process_next_job()
        except asyncio.CancelledError:
            logger.info("Worker cancelled", worker_id=self.worker_id)
        finally:
            self._state = WorkerState.STOPPED
            logger.info(
                "Worker stopped",
                worker_id=self.worker_id,
                stats=self._stats.__dict__,
            )

    async def stop(self, graceful: bool = True) -> None:
        """
        Stop the worker.

        Args:
            graceful: If True, wait for current job to complete
        """
        logger.info(
            "Worker stopping",
            worker_id=self.worker_id,
            graceful=graceful,
        )
        self._state = WorkerState.STOPPING
        self._shutdown_event.set()

    async def _process_next_job(self) -> None:
        """Attempt to dequeue and process the next job."""
        self._state = WorkerState.POLLING

        try:
            # Try to get a job (includes concurrency slot acquisition)
            result = await self.queue_manager.dequeue(
                worker_id=self.worker_id,
                timeout=5.0,  # Short timeout for responsive shutdown
            )

            if result is None:
                # No job available, sleep briefly
                await asyncio.sleep(1.0)
                return

            request, slot = result
            self._current_job = request
            self._current_slot = slot
            self._state = WorkerState.PROCESSING

            logger.info(
                "Processing job",
                worker_id=self.worker_id,
                job_id=request.id,
                job_path=request.job_path,
            )

            # Execute validation
            validation_result = await self._execute_validation(request)

            # Report result to queue
            await self.queue_manager.complete(
                job_id=request.id,
                result=validation_result,
                slot=slot,
            )

            # Store result in persistent storage
            await self._store_result(request, validation_result)

            # Send notifications
            await self._send_notifications(request, validation_result)

            # Update stats
            self._stats.jobs_processed += 1
            if validation_result.passed:
                self._stats.jobs_succeeded += 1
            else:
                self._stats.jobs_failed += 1

            if validation_result.duration_seconds:
                self._stats.total_processing_seconds += validation_result.duration_seconds

            self._stats.last_job_at = datetime.utcnow()

            logger.info(
                "Job completed",
                worker_id=self.worker_id,
                job_id=request.id,
                passed=validation_result.passed,
                duration=validation_result.duration_seconds,
            )

        except Exception as e:
            logger.error(
                "Job processing failed",
                worker_id=self.worker_id,
                error=str(e),
                traceback=traceback.format_exc(),
            )

            # Report failure if we have a job
            if self._current_job and self._current_slot:
                await self.queue_manager.fail(
                    job_id=self._current_job.id,
                    error_message=str(e),
                    slot=self._current_slot,
                )

        finally:
            self._current_job = None
            self._current_slot = None

    async def _execute_validation(
        self,
        request: ValidationRequest,
    ) -> ValidationResult:
        """
        Execute all validation stages for a job.

        Stages run sequentially. If a stage fails critically,
        subsequent stages may be skipped.
        """
        started_at = datetime.utcnow()
        stage_results: List[StageResult] = []
        skip_remaining = False

        for stage, func in self._stages:
            if skip_remaining:
                # Add skipped stage result
                stage_results.append(StageResult(
                    stage=stage,
                    status=ValidationStatus.SKIPPED,
                    duration_seconds=0.0,
                ))
                continue

            try:
                stage_start = datetime.utcnow()

                # Execute stage with timeout
                stage_config = next(
                    (s for s in self.config.validation.stages if s.name == stage.value),
                    None
                )
                timeout = stage_config.timeout_seconds if stage_config else 60

                result = await asyncio.wait_for(
                    func(request.job_path, request.metadata),
                    timeout=timeout,
                )

                stage_results.append(result)

                # Check if we should skip remaining stages
                if result.status == ValidationStatus.ERROR:
                    skip_remaining = True

            except asyncio.TimeoutError:
                stage_results.append(StageResult(
                    stage=stage,
                    status=ValidationStatus.TIMEOUT,
                    duration_seconds=timeout,
                    issues=[ValidationIssue(
                        stage=stage,
                        severity=Severity.ERROR,
                        message=f"Stage timed out after {timeout} seconds",
                    )],
                ))
                skip_remaining = True

            except Exception as e:
                stage_results.append(StageResult(
                    stage=stage,
                    status=ValidationStatus.ERROR,
                    duration_seconds=(datetime.utcnow() - stage_start).total_seconds(),
                    issues=[ValidationIssue(
                        stage=stage,
                        severity=Severity.CRITICAL,
                        message=f"Stage failed: {str(e)}",
                    )],
                ))
                skip_remaining = True

        completed_at = datetime.utcnow()

        # Determine overall status
        has_errors = any(
            sr.status in (ValidationStatus.ERROR, ValidationStatus.TIMEOUT)
            for sr in stage_results
        )
        has_failures = any(
            issue.severity in (Severity.CRITICAL, Severity.ERROR)
            for sr in stage_results
            for issue in sr.issues
        )

        if has_errors:
            status = ValidationStatus.ERROR
        elif has_failures:
            status = ValidationStatus.FAILED
        else:
            status = ValidationStatus.COMPLETED

        return ValidationResult(
            request_id=request.id,
            job_path=request.job_path,
            status=status,
            stage_results=stage_results,
            started_at=started_at,
            completed_at=completed_at,
            worker_id=self.worker_id,
        )

    # ==========================================
    # Storage and Notification Helpers
    # ==========================================

    async def _store_result(
        self,
        request: ValidationRequest,
        result: ValidationResult,
    ) -> None:
        """Store validation result in persistent storage."""
        if not self.storage:
            return

        try:
            await self.storage.store_result(result)
            logger.debug(
                "Result stored",
                job_id=request.id,
            )
        except Exception as e:
            logger.error(
                "Failed to store result",
                job_id=request.id,
                error=str(e),
            )
            # Don't re-raise - storage failure shouldn't fail the job

    async def _send_notifications(
        self,
        request: ValidationRequest,
        result: ValidationResult,
    ) -> None:
        """Send notifications for validation result."""
        if not self.notifier:
            return

        try:
            # Generate report content if reporter is available
            report_content = None
            if self.reporter and NOTIFIER_AVAILABLE:
                report_content = self.reporter.generate(
                    result,
                    format=ReportFormat.MARKDOWN,
                )

            # Send notifications
            notification_results = await self.notifier.notify(
                result,
                report_content=report_content,
            )

            if notification_results:
                logger.debug(
                    "Notifications sent",
                    job_id=request.id,
                    results=notification_results,
                )

        except Exception as e:
            logger.error(
                "Failed to send notifications",
                job_id=request.id,
                error=str(e),
            )
            # Don't re-raise - notification failure shouldn't fail the job

    # ==========================================
    # Validation Stage Implementations
    # ==========================================

    async def _validate_syntax(
        self,
        job_path: str,
        metadata: dict,
    ) -> StageResult:
        """
        Validate Python syntax.

        Uses AST parsing to detect syntax errors.
        """
        import ast
        import time

        start = time.time()
        issues = []

        try:
            with open(job_path, 'r') as f:
                source = f.read()

            ast.parse(source)

        except SyntaxError as e:
            issues.append(ValidationIssue(
                stage=ValidationStage.SYNTAX,
                severity=Severity.CRITICAL,
                message=f"Syntax error: {e.msg}",
                file_path=job_path,
                line_number=e.lineno,
            ))

        except FileNotFoundError:
            issues.append(ValidationIssue(
                stage=ValidationStage.SYNTAX,
                severity=Severity.CRITICAL,
                message=f"File not found: {job_path}",
                file_path=job_path,
            ))

        duration = time.time() - start

        return StageResult(
            stage=ValidationStage.SYNTAX,
            status=ValidationStatus.COMPLETED,
            duration_seconds=duration,
            issues=issues,
        )

    async def _validate_logic(
        self,
        job_path: str,
        metadata: dict,
    ) -> StageResult:
        """
        Validate code logic.

        Checks for:
        - Unused variables/imports
        - Dead code
        - Common anti-patterns
        """
        import ast
        import time

        start = time.time()
        issues = []

        try:
            with open(job_path, 'r') as f:
                source = f.read()

            tree = ast.parse(source)

            # Check for common issues
            for node in ast.walk(tree):
                # Warn about bare except
                if isinstance(node, ast.ExceptHandler) and node.type is None:
                    issues.append(ValidationIssue(
                        stage=ValidationStage.LOGIC,
                        severity=Severity.WARNING,
                        message="Bare 'except:' clause catches all exceptions",
                        file_path=job_path,
                        line_number=node.lineno,
                        rule_id="W001",
                    ))

                # Warn about star imports
                if isinstance(node, ast.ImportFrom) and any(
                    alias.name == '*' for alias in node.names
                ):
                    issues.append(ValidationIssue(
                        stage=ValidationStage.LOGIC,
                        severity=Severity.WARNING,
                        message=f"Star import from {node.module}",
                        file_path=job_path,
                        line_number=node.lineno,
                        rule_id="W002",
                    ))

        except Exception as e:
            issues.append(ValidationIssue(
                stage=ValidationStage.LOGIC,
                severity=Severity.ERROR,
                message=f"Logic validation failed: {str(e)}",
                file_path=job_path,
            ))

        duration = time.time() - start

        return StageResult(
            stage=ValidationStage.LOGIC,
            status=ValidationStatus.COMPLETED,
            duration_seconds=duration,
            issues=issues,
        )

    async def _validate_data(
        self,
        job_path: str,
        metadata: dict,
    ) -> StageResult:
        """
        Validate data sources and configurations.

        Checks for:
        - Valid SQL syntax in string literals
        - S3/MinIO configuration
        - Required configuration files
        """
        import time
        import re

        start = time.time()
        issues = []

        try:
            with open(job_path, 'r') as f:
                source = f.read()

            # Check for s3a:// usage (should use s3:// per CLAUDE.md)
            if 's3a://' in source:
                issues.append(ValidationIssue(
                    stage=ValidationStage.DATA,
                    severity=Severity.WARNING,
                    message="Using s3a:// protocol - consider using s3:// instead",
                    file_path=job_path,
                    rule_id="D001",
                ))

            # Check for hardcoded credentials
            credential_patterns = [
                (r'password\s*=\s*["\'][^"\']+["\']', "Hardcoded password detected"),
                (r'secret\s*=\s*["\'][^"\']+["\']', "Hardcoded secret detected"),
                (r'api_key\s*=\s*["\'][^"\']+["\']', "Hardcoded API key detected"),
            ]

            for pattern, message in credential_patterns:
                if re.search(pattern, source, re.IGNORECASE):
                    issues.append(ValidationIssue(
                        stage=ValidationStage.DATA,
                        severity=Severity.CRITICAL,
                        message=message,
                        file_path=job_path,
                        rule_id="D002",
                    ))

        except Exception as e:
            issues.append(ValidationIssue(
                stage=ValidationStage.DATA,
                severity=Severity.ERROR,
                message=f"Data validation failed: {str(e)}",
                file_path=job_path,
            ))

        duration = time.time() - start

        return StageResult(
            stage=ValidationStage.DATA,
            status=ValidationStatus.COMPLETED,
            duration_seconds=duration,
            issues=issues,
        )

    async def _validate_expectations(
        self,
        job_path: str,
        metadata: dict,
    ) -> StageResult:
        """
        Run Great Expectations validation if configured.

        Placeholder - actual implementation would integrate with GE.
        """
        import time

        start = time.time()
        issues = []

        # TODO: Integrate with Great Expectations
        # For now, just return success

        duration = time.time() - start

        return StageResult(
            stage=ValidationStage.EXPECTATIONS,
            status=ValidationStatus.COMPLETED,
            duration_seconds=duration,
            issues=issues,
            metrics={"expectation_suites_checked": 0},
        )

    async def _validate_ai(
        self,
        job_path: str,
        metadata: dict,
    ) -> StageResult:
        """
        Run AI-powered code analysis.

        Placeholder - would integrate with LLM analyzer.
        """
        import time

        start = time.time()
        issues = []

        # TODO: Integrate with AI analyzer from analyzer/ module

        duration = time.time() - start

        return StageResult(
            stage=ValidationStage.AI_ANALYSIS,
            status=ValidationStatus.COMPLETED,
            duration_seconds=duration,
            issues=issues,
            metrics={"ai_analysis_enabled": False},
        )
