"""
Trigger Gateway - Unified entry point for all validation triggers.

Normalizes events from different sources (Git webhooks, file watchers,
CI/CD pipelines, scheduled jobs) into ValidationRequests for queuing.
"""

import asyncio
import fnmatch
import hashlib
import hmac
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any, List, Callable, Awaitable
import structlog

from validator.models import (
    ValidationRequest,
    Priority,
    TriggerSource,
)
from validator.queue.manager import QueueManager, EnqueueResult
from validator.config import FrameworkConfig, get_config


logger = structlog.get_logger(__name__)


@dataclass
class TriggerEvent:
    """
    Raw event from a trigger source before normalization.

    This is the intermediate format between source-specific events
    and ValidationRequests.
    """
    source: TriggerSource
    files: List[str]
    metadata: Dict[str, Any] = field(default_factory=dict)

    # Git-specific
    branch: Optional[str] = None
    commit_sha: Optional[str] = None
    author: Optional[str] = None

    # Timing
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class TriggerResult:
    """Result of processing trigger events."""
    queued: int = 0
    deduplicated: int = 0
    rejected: int = 0
    filtered: int = 0
    errors: List[str] = field(default_factory=list)

    @property
    def total_processed(self) -> int:
        return self.queued + self.deduplicated + self.rejected + self.filtered


class TriggerSourceHandler(ABC):
    """Abstract base class for trigger source handlers."""

    @abstractmethod
    async def parse_event(self, raw_data: Any) -> Optional[TriggerEvent]:
        """Parse raw event data into a TriggerEvent."""
        pass

    @abstractmethod
    def validate_signature(self, payload: bytes, signature: str) -> bool:
        """Validate webhook signature if applicable."""
        pass


class TriggerGateway:
    """
    Central gateway for processing validation triggers.

    Responsibilities:
    - Register and manage trigger source handlers
    - Normalize events to ValidationRequests
    - Apply filtering rules (file patterns, branch patterns)
    - Detect and handle burst scenarios
    - Queue requests with appropriate priority
    """

    # Burst detection: more than this many files in one event
    BURST_THRESHOLD = 10

    def __init__(
        self,
        queue_manager: QueueManager,
        config: Optional[FrameworkConfig] = None,
    ):
        self.queue_manager = queue_manager
        self.config = config or get_config()
        self._handlers: Dict[TriggerSource, TriggerSourceHandler] = {}
        self._file_filters: List[str] = []
        self._ignore_patterns: List[str] = []

        # Load file filters from config
        self._load_filters()

    def _load_filters(self) -> None:
        """Load file filtering patterns from configuration."""
        file_watcher_config = self.config.triggers.file_watcher

        # Patterns to include
        self._file_filters = file_watcher_config.paths

        # Patterns to ignore
        self._ignore_patterns = file_watcher_config.ignore_patterns

    def register_handler(
        self,
        source: TriggerSource,
        handler: TriggerSourceHandler,
    ) -> None:
        """Register a handler for a trigger source."""
        self._handlers[source] = handler
        logger.info("Registered trigger handler", source=source.value)

    def _should_process_file(self, file_path: str) -> bool:
        """
        Check if a file should be processed based on filter patterns.

        Returns True if file matches include patterns and doesn't match
        any ignore patterns.
        """
        # Check ignore patterns first
        for pattern in self._ignore_patterns:
            if fnmatch.fnmatch(file_path, pattern):
                return False

        # If no include patterns, accept all .py files
        if not self._file_filters:
            return file_path.endswith('.py')

        # Check include patterns
        for pattern in self._file_filters:
            if fnmatch.fnmatch(file_path, pattern):
                return True

        return False

    def _determine_priority(
        self,
        event: TriggerEvent,
    ) -> Priority:
        """
        Determine validation priority based on trigger source and metadata.

        Priority mapping:
        - CRITICAL (0): hotfix branches, production issues
        - MANUAL (1): manual triggers, PR reviews
        - CI_CD (2): regular CI/CD, feature branches
        - BATCH (3): scheduled scans, bulk operations
        """
        # Manual triggers get MANUAL priority
        if event.source == TriggerSource.MANUAL:
            return Priority.MANUAL

        # Scheduled triggers get BATCH priority
        if event.source == TriggerSource.SCHEDULED:
            return Priority.BATCH

        # For Git triggers, check branch patterns
        if event.branch:
            priority_map = self.config.triggers.git.priority_map

            for pattern, priority in priority_map.items():
                if pattern.endswith("/*"):
                    # Prefix match (e.g., "hotfix/*")
                    prefix = pattern[:-2]
                    if event.branch.startswith(prefix):
                        return Priority(priority)
                elif event.branch == pattern:
                    # Exact match
                    return Priority(priority)

        # Default to CI_CD priority
        return Priority.CI_CD

    def _is_burst(self, event: TriggerEvent) -> bool:
        """Check if this event represents a burst scenario."""
        return len(event.files) > self.BURST_THRESHOLD

    async def process_event(
        self,
        event: TriggerEvent,
    ) -> TriggerResult:
        """
        Process a trigger event and queue validation requests.

        Steps:
        1. Filter files based on patterns
        2. Determine priority
        3. Create batch ID for burst scenarios
        4. Queue each file as a ValidationRequest
        """
        result = TriggerResult()

        # Filter files
        files_to_process = []
        for file_path in event.files:
            if self._should_process_file(file_path):
                files_to_process.append(file_path)
            else:
                result.filtered += 1

        if not files_to_process:
            logger.debug(
                "No files to process after filtering",
                original_count=len(event.files),
                filtered=result.filtered,
            )
            return result

        # Determine priority
        priority = self._determine_priority(event)

        # Create batch ID for burst scenarios
        batch_id = None
        if self._is_burst(event):
            import uuid
            batch_id = str(uuid.uuid4())
            # Demote to BATCH priority for large batches
            if priority > Priority.MANUAL:
                priority = Priority.BATCH
            logger.info(
                "Burst detected, creating batch",
                batch_id=batch_id,
                file_count=len(files_to_process),
            )

        # Queue each file
        for file_path in files_to_process:
            try:
                request = ValidationRequest(
                    job_path=file_path,
                    trigger_source=event.source,
                    priority=priority,
                    batch_id=batch_id,
                    branch=event.branch,
                    commit_sha=event.commit_sha,
                    triggered_by=event.author,
                    metadata=event.metadata,
                )

                enqueue_result, _ = await self.queue_manager.enqueue(request)

                if enqueue_result == EnqueueResult.QUEUED:
                    result.queued += 1
                elif enqueue_result == EnqueueResult.DEDUPLICATED:
                    result.deduplicated += 1
                else:
                    result.rejected += 1

            except Exception as e:
                result.errors.append(f"{file_path}: {str(e)}")
                logger.error(
                    "Failed to queue file",
                    file_path=file_path,
                    error=str(e),
                )

        logger.info(
            "Trigger event processed",
            source=event.source.value,
            queued=result.queued,
            deduplicated=result.deduplicated,
            rejected=result.rejected,
            filtered=result.filtered,
        )

        return result

    async def handle_raw_event(
        self,
        source: TriggerSource,
        raw_data: Any,
        signature: Optional[str] = None,
    ) -> TriggerResult:
        """
        Handle a raw event from a trigger source.

        This is the main entry point for webhook handlers.
        """
        handler = self._handlers.get(source)
        if not handler:
            logger.warning("No handler registered for source", source=source.value)
            return TriggerResult(errors=[f"No handler for {source.value}"])

        # Validate signature if provided
        if signature:
            # Note: signature validation needs raw bytes, not parsed data
            pass

        # Parse event
        event = await handler.parse_event(raw_data)
        if not event:
            return TriggerResult(errors=["Failed to parse event"])

        return await self.process_event(event)


class GitWebhookHandler(TriggerSourceHandler):
    """
    Handler for Git webhook events (Gitea/GitHub compatible).

    Parses push and pull_request events to extract changed files.
    """

    def __init__(self, secret: Optional[str] = None):
        self.secret = secret

    def validate_signature(self, payload: bytes, signature: str) -> bool:
        """
        Validate webhook signature.

        Supports both GitHub (sha256=...) and Gitea (sha256=...) formats.
        """
        if not self.secret:
            return True  # No secret configured, skip validation

        # Parse signature format
        if signature.startswith("sha256="):
            expected_sig = signature[7:]
            computed = hmac.new(
                self.secret.encode(),
                payload,
                hashlib.sha256,
            ).hexdigest()
            return hmac.compare_digest(computed, expected_sig)

        return False

    async def parse_event(self, raw_data: Dict[str, Any]) -> Optional[TriggerEvent]:
        """Parse Git webhook payload into TriggerEvent."""
        try:
            # Detect event type
            if "commits" in raw_data:
                return self._parse_push_event(raw_data)
            elif "pull_request" in raw_data:
                return self._parse_pr_event(raw_data)
            else:
                logger.warning("Unknown Git event type", keys=list(raw_data.keys()))
                return None

        except Exception as e:
            logger.error("Failed to parse Git event", error=str(e))
            return None

    def _parse_push_event(self, data: Dict[str, Any]) -> TriggerEvent:
        """Parse a push event."""
        # Extract changed files from commits
        changed_files = set()
        for commit in data.get("commits", []):
            for file_list in ["added", "modified"]:
                changed_files.update(commit.get(file_list, []))

        # Extract branch from ref
        ref = data.get("ref", "")
        branch = ref.replace("refs/heads/", "")

        return TriggerEvent(
            source=TriggerSource.GIT_WEBHOOK,
            files=list(changed_files),
            branch=branch,
            commit_sha=data.get("after"),
            author=data.get("pusher", {}).get("username"),
            metadata={
                "repository": data.get("repository", {}).get("full_name"),
                "before": data.get("before"),
                "compare_url": data.get("compare"),
            },
        )

    def _parse_pr_event(self, data: Dict[str, Any]) -> Optional[TriggerEvent]:
        """Parse a pull request event."""
        action = data.get("action")

        # Only process opened/synchronized PRs
        if action not in ["opened", "synchronize", "reopened"]:
            return None

        pr = data.get("pull_request", {})
        head = pr.get("head", {})

        # For PRs, we need to fetch the changed files via API
        # For now, return the PR branch for validation
        return TriggerEvent(
            source=TriggerSource.GIT_WEBHOOK,
            files=[],  # Would need API call to get changed files
            branch=head.get("ref"),
            commit_sha=head.get("sha"),
            author=pr.get("user", {}).get("login"),
            metadata={
                "pr_number": pr.get("number"),
                "pr_title": pr.get("title"),
                "base_branch": pr.get("base", {}).get("ref"),
            },
        )


class CICDWebhookHandler(TriggerSourceHandler):
    """Handler for CI/CD system webhooks (Jenkins, GitLab CI, etc.)."""

    async def parse_event(self, raw_data: Dict[str, Any]) -> Optional[TriggerEvent]:
        """Parse CI/CD webhook payload."""
        # Generic CI/CD event format
        return TriggerEvent(
            source=TriggerSource.CI_CD,
            files=raw_data.get("files", []),
            branch=raw_data.get("branch"),
            commit_sha=raw_data.get("commit"),
            author=raw_data.get("triggered_by"),
            metadata=raw_data.get("metadata", {}),
        )

    def validate_signature(self, payload: bytes, signature: str) -> bool:
        return True  # Implement based on CI/CD system


class ManualTriggerHandler(TriggerSourceHandler):
    """Handler for manual/API-triggered validations."""

    async def parse_event(self, raw_data: Dict[str, Any]) -> Optional[TriggerEvent]:
        """Parse manual trigger request."""
        files = raw_data.get("files", [])
        if isinstance(files, str):
            files = [files]

        return TriggerEvent(
            source=TriggerSource.MANUAL,
            files=files,
            branch=raw_data.get("branch"),
            commit_sha=raw_data.get("commit"),
            author=raw_data.get("user", "manual"),
            metadata=raw_data.get("metadata", {}),
        )

    def validate_signature(self, payload: bytes, signature: str) -> bool:
        return True  # API key validation handled elsewhere


def create_gateway(
    queue_manager: QueueManager,
    config: Optional[FrameworkConfig] = None,
) -> TriggerGateway:
    """
    Factory function to create and configure a TriggerGateway.

    Registers default handlers based on configuration.
    """
    config = config or get_config()
    gateway = TriggerGateway(queue_manager, config)

    # Register Git webhook handler
    if config.triggers.git.enabled:
        gateway.register_handler(
            TriggerSource.GIT_WEBHOOK,
            GitWebhookHandler(secret=config.triggers.git.secret),
        )

    # Register CI/CD handler
    if config.triggers.api.enabled:
        gateway.register_handler(TriggerSource.CI_CD, CICDWebhookHandler())

    # Register manual handler
    gateway.register_handler(TriggerSource.MANUAL, ManualTriggerHandler())

    return gateway
