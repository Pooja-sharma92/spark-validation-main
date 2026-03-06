"""
Notification System for Validation Results.

Supports multiple notification channels:
- Slack: Rich message formatting with attachments
- Webhook: Generic HTTP POST for custom integrations
- Email: SMTP-based email notifications (optional)
"""

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional, List, Dict, Any, Callable
import json
import structlog

from validator.models import (
    ValidationResult,
    ValidationStatus,
    Severity,
)


logger = structlog.get_logger(__name__)


class NotificationLevel(Enum):
    """When to send notifications."""
    ALL = "all"               # Every validation
    FAILURES_ONLY = "failures"  # Only failed validations
    CRITICAL = "critical"     # Only critical failures
    SUMMARY = "summary"       # Batch summaries only


@dataclass
class NotificationConfig:
    """Configuration for notification channels."""
    # Slack settings
    slack_enabled: bool = False
    slack_webhook_url: Optional[str] = None
    slack_channel_critical: str = "#validation-critical"
    slack_channel_summary: str = "#validation-summary"

    # Webhook settings
    webhook_enabled: bool = False
    webhook_url: Optional[str] = None
    webhook_headers: Dict[str, str] = field(default_factory=dict)

    # General settings
    notification_level: NotificationLevel = NotificationLevel.FAILURES_ONLY

    # Rate limiting
    min_interval_seconds: float = 5.0  # Min time between notifications

    # Retry settings
    max_retries: int = 3
    retry_delay_seconds: float = 1.0


class NotificationChannel(ABC):
    """Abstract base class for notification channels."""

    @abstractmethod
    async def send(
        self,
        result: ValidationResult,
        report_content: Optional[str] = None,
    ) -> bool:
        """Send notification for a validation result."""
        pass

    @abstractmethod
    async def send_batch_summary(
        self,
        results: List[ValidationResult],
        batch_id: str,
        report_content: Optional[str] = None,
    ) -> bool:
        """Send summary notification for batch validation."""
        pass


class SlackNotifier(NotificationChannel):
    """
    Slack notification channel using Incoming Webhooks.

    Sends rich messages with:
    - Color-coded status
    - Issue summary
    - Links to detailed reports
    """

    def __init__(self, config: NotificationConfig):
        self.config = config
        self._last_sent: Dict[str, datetime] = {}

    async def send(
        self,
        result: ValidationResult,
        report_content: Optional[str] = None,
    ) -> bool:
        """Send Slack notification for validation result."""
        if not self.config.slack_webhook_url:
            logger.warning("Slack webhook URL not configured")
            return False

        # Rate limiting
        if not self._should_send(result.job_id):
            logger.debug("Rate limited", job_id=result.job_id)
            return False

        # Build Slack message
        message = self._build_message(result)

        # Choose channel based on severity
        channel = (
            self.config.slack_channel_critical
            if result.status == ValidationStatus.FAILED
            else self.config.slack_channel_summary
        )

        return await self._post_to_slack(message, channel)

    async def send_batch_summary(
        self,
        results: List[ValidationResult],
        batch_id: str,
        report_content: Optional[str] = None,
    ) -> bool:
        """Send batch summary to Slack."""
        if not self.config.slack_webhook_url:
            return False

        message = self._build_batch_message(results, batch_id)
        return await self._post_to_slack(message, self.config.slack_channel_summary)

    def _should_send(self, job_id: str) -> bool:
        """Check if we should send notification (rate limiting)."""
        now = datetime.now()
        last = self._last_sent.get(job_id)

        if last:
            elapsed = (now - last).total_seconds()
            if elapsed < self.config.min_interval_seconds:
                return False

        self._last_sent[job_id] = now
        return True

    def _build_message(self, result: ValidationResult) -> Dict[str, Any]:
        """Build Slack message payload."""
        color = {
            ValidationStatus.PASSED: "#36a64f",      # Green
            ValidationStatus.FAILED: "#dc3545",      # Red
            ValidationStatus.PASSED_WITH_WARNINGS: "#ffc107",  # Yellow
            ValidationStatus.RUNNING: "#17a2b8",     # Blue
        }.get(result.status, "#6c757d")

        # Count issues by severity
        error_count = sum(1 for i in result.issues if i.severity == Severity.ERROR)
        warning_count = sum(1 for i in result.issues if i.severity == Severity.WARNING)

        # Build fields
        fields = [
            {
                "title": "Status",
                "value": result.status.value,
                "short": True,
            },
            {
                "title": "Duration",
                "value": f"{result.duration_seconds:.1f}s" if result.duration_seconds else "-",
                "short": True,
            },
        ]

        if error_count > 0 or warning_count > 0:
            fields.append({
                "title": "Issues",
                "value": f":red_circle: {error_count} errors, :large_yellow_circle: {warning_count} warnings",
                "short": True,
            })

        if result.branch:
            fields.append({
                "title": "Branch",
                "value": result.branch,
                "short": True,
            })

        # Top issues preview
        top_issues = []
        for issue in result.issues[:3]:
            emoji = ":red_circle:" if issue.severity == Severity.ERROR else ":large_yellow_circle:"
            top_issues.append(f"{emoji} `{issue.code}`: {issue.message[:60]}")

        text = "\n".join(top_issues) if top_issues else "No issues found"

        return {
            "attachments": [
                {
                    "color": color,
                    "title": f"Validation: {Path(result.job_path).name}",
                    "title_link": f"file://{result.job_path}",
                    "text": text,
                    "fields": fields,
                    "footer": f"Job ID: {result.job_id[:8]}",
                    "ts": int(result.timestamp.timestamp()),
                }
            ]
        }

    def _build_batch_message(
        self,
        results: List[ValidationResult],
        batch_id: str,
    ) -> Dict[str, Any]:
        """Build Slack message for batch summary."""
        total = len(results)
        passed = sum(1 for r in results if r.status == ValidationStatus.PASSED)
        failed = sum(1 for r in results if r.status == ValidationStatus.FAILED)
        warnings = sum(1 for r in results if r.status == ValidationStatus.PASSED_WITH_WARNINGS)

        # Overall color based on results
        if failed > 0:
            color = "#dc3545"  # Red
            emoji = ":x:"
        elif warnings > 0:
            color = "#ffc107"  # Yellow
            emoji = ":warning:"
        else:
            color = "#36a64f"  # Green
            emoji = ":white_check_mark:"

        success_rate = (passed + warnings) / total * 100 if total > 0 else 0

        # List failed jobs
        failed_jobs = [
            f"• `{Path(r.job_path).name}`"
            for r in results
            if r.status == ValidationStatus.FAILED
        ][:5]

        text = "\n".join(failed_jobs) if failed_jobs else "All validations passed!"

        return {
            "attachments": [
                {
                    "color": color,
                    "title": f"{emoji} Batch Validation Complete",
                    "text": text,
                    "fields": [
                        {"title": "Total", "value": str(total), "short": True},
                        {"title": "Passed", "value": f":white_check_mark: {passed}", "short": True},
                        {"title": "Failed", "value": f":x: {failed}", "short": True},
                        {"title": "Success Rate", "value": f"{success_rate:.1f}%", "short": True},
                    ],
                    "footer": f"Batch ID: {batch_id}",
                    "ts": int(datetime.now().timestamp()),
                }
            ]
        }

    async def _post_to_slack(
        self,
        message: Dict[str, Any],
        channel: str,
    ) -> bool:
        """Post message to Slack webhook."""
        import httpx

        message["channel"] = channel

        for attempt in range(self.config.max_retries):
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        self.config.slack_webhook_url,
                        json=message,
                        timeout=10.0,
                    )

                    if response.status_code == 200:
                        logger.info("Slack notification sent", channel=channel)
                        return True

                    logger.warning(
                        "Slack API error",
                        status=response.status_code,
                        body=response.text,
                    )

            except Exception as e:
                logger.warning(
                    "Slack notification failed",
                    attempt=attempt + 1,
                    error=str(e),
                )
                await asyncio.sleep(self.config.retry_delay_seconds)

        return False


class WebhookNotifier(NotificationChannel):
    """
    Generic webhook notification channel.

    Sends JSON payloads to any HTTP endpoint.
    Useful for custom integrations (JIRA, PagerDuty, etc.).
    """

    def __init__(self, config: NotificationConfig):
        self.config = config

    async def send(
        self,
        result: ValidationResult,
        report_content: Optional[str] = None,
    ) -> bool:
        """Send webhook notification."""
        if not self.config.webhook_url:
            logger.warning("Webhook URL not configured")
            return False

        payload = self._build_payload(result)
        return await self._post_webhook(payload)

    async def send_batch_summary(
        self,
        results: List[ValidationResult],
        batch_id: str,
        report_content: Optional[str] = None,
    ) -> bool:
        """Send batch summary via webhook."""
        if not self.config.webhook_url:
            return False

        payload = self._build_batch_payload(results, batch_id)
        return await self._post_webhook(payload)

    def _build_payload(self, result: ValidationResult) -> Dict[str, Any]:
        """Build webhook payload."""
        return {
            "event": "validation_complete",
            "timestamp": datetime.now().isoformat(),
            "data": {
                "job_id": result.job_id,
                "job_path": result.job_path,
                "status": result.status.value,
                "duration_seconds": result.duration_seconds,
                "issue_count": len(result.issues),
                "error_count": sum(
                    1 for i in result.issues if i.severity == Severity.ERROR
                ),
                "warning_count": sum(
                    1 for i in result.issues if i.severity == Severity.WARNING
                ),
                "branch": result.branch,
                "commit_sha": result.commit_sha,
            },
        }

    def _build_batch_payload(
        self,
        results: List[ValidationResult],
        batch_id: str,
    ) -> Dict[str, Any]:
        """Build batch summary webhook payload."""
        total = len(results)
        passed = sum(1 for r in results if r.status == ValidationStatus.PASSED)
        failed = sum(1 for r in results if r.status == ValidationStatus.FAILED)

        return {
            "event": "batch_validation_complete",
            "timestamp": datetime.now().isoformat(),
            "data": {
                "batch_id": batch_id,
                "total": total,
                "passed": passed,
                "failed": failed,
                "success_rate": (passed / total) if total > 0 else 0,
                "failed_jobs": [
                    {"job_id": r.job_id, "job_path": r.job_path}
                    for r in results
                    if r.status == ValidationStatus.FAILED
                ],
            },
        }

    async def _post_webhook(self, payload: Dict[str, Any]) -> bool:
        """Post payload to webhook endpoint."""
        import httpx

        headers = {
            "Content-Type": "application/json",
            **self.config.webhook_headers,
        }

        for attempt in range(self.config.max_retries):
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        self.config.webhook_url,
                        json=payload,
                        headers=headers,
                        timeout=10.0,
                    )

                    if response.status_code in (200, 201, 202, 204):
                        logger.info("Webhook notification sent")
                        return True

                    logger.warning(
                        "Webhook error",
                        status=response.status_code,
                    )

            except Exception as e:
                logger.warning(
                    "Webhook notification failed",
                    attempt=attempt + 1,
                    error=str(e),
                )
                await asyncio.sleep(self.config.retry_delay_seconds)

        return False


class NotificationManager:
    """
    Manages multiple notification channels.

    Central dispatcher for all notifications.
    Handles routing based on notification level.
    """

    def __init__(self, config: NotificationConfig):
        self.config = config
        self.channels: List[NotificationChannel] = []

        # Initialize enabled channels
        if config.slack_enabled and config.slack_webhook_url:
            self.channels.append(SlackNotifier(config))

        if config.webhook_enabled and config.webhook_url:
            self.channels.append(WebhookNotifier(config))

    def add_channel(self, channel: NotificationChannel) -> None:
        """Add a custom notification channel."""
        self.channels.append(channel)

    async def notify(
        self,
        result: ValidationResult,
        report_content: Optional[str] = None,
    ) -> Dict[str, bool]:
        """
        Send notifications for a validation result.

        Returns dict of channel -> success status.
        """
        # Check notification level
        if not self._should_notify(result):
            return {}

        results = {}
        for channel in self.channels:
            channel_name = channel.__class__.__name__
            try:
                success = await channel.send(result, report_content)
                results[channel_name] = success
            except Exception as e:
                logger.error(
                    "Notification channel failed",
                    channel=channel_name,
                    error=str(e),
                )
                results[channel_name] = False

        return results

    async def notify_batch(
        self,
        results: List[ValidationResult],
        batch_id: str,
        report_content: Optional[str] = None,
    ) -> Dict[str, bool]:
        """Send batch summary notifications."""
        notification_results = {}

        for channel in self.channels:
            channel_name = channel.__class__.__name__
            try:
                success = await channel.send_batch_summary(
                    results, batch_id, report_content
                )
                notification_results[channel_name] = success
            except Exception as e:
                logger.error(
                    "Batch notification failed",
                    channel=channel_name,
                    error=str(e),
                )
                notification_results[channel_name] = False

        return notification_results

    def _should_notify(self, result: ValidationResult) -> bool:
        """Check if notification should be sent based on level."""
        level = self.config.notification_level

        if level == NotificationLevel.ALL:
            return True

        if level == NotificationLevel.FAILURES_ONLY:
            return result.status == ValidationStatus.FAILED

        if level == NotificationLevel.CRITICAL:
            # Only notify if there are ERROR severity issues
            return any(
                i.severity == Severity.ERROR for i in result.issues
            )

        # SUMMARY level - don't send individual notifications
        return False


class InMemoryNotifier(NotificationChannel):
    """
    In-memory notifier for testing.

    Stores all notifications for inspection.
    """

    def __init__(self):
        self.notifications: List[Dict[str, Any]] = []
        self.batch_notifications: List[Dict[str, Any]] = []

    async def send(
        self,
        result: ValidationResult,
        report_content: Optional[str] = None,
    ) -> bool:
        """Store notification in memory."""
        self.notifications.append({
            "type": "single",
            "result": result,
            "report_content": report_content,
            "timestamp": datetime.now(),
        })
        return True

    async def send_batch_summary(
        self,
        results: List[ValidationResult],
        batch_id: str,
        report_content: Optional[str] = None,
    ) -> bool:
        """Store batch notification in memory."""
        self.batch_notifications.append({
            "type": "batch",
            "results": results,
            "batch_id": batch_id,
            "report_content": report_content,
            "timestamp": datetime.now(),
        })
        return True

    def clear(self) -> None:
        """Clear stored notifications."""
        self.notifications.clear()
        self.batch_notifications.clear()


def create_notification_manager(
    config: Optional[NotificationConfig] = None,
) -> NotificationManager:
    """Factory function to create notification manager."""
    return NotificationManager(config or NotificationConfig())
