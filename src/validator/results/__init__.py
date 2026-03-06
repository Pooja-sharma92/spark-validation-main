"""Results module for storage, reporting, and notifications."""

from validator.results.storage import (
    ResultStorage,
    InMemoryResultStorage,
    create_storage,
)
from validator.results.reporter import (
    ValidationReporter,
    ReportConfig,
    ReportFormat,
    create_reporter,
)
from validator.results.notifier import (
    NotificationManager,
    NotificationConfig,
    NotificationLevel,
    NotificationChannel,
    SlackNotifier,
    WebhookNotifier,
    InMemoryNotifier,
    create_notification_manager,
)

__all__ = [
    # Storage
    "ResultStorage",
    "InMemoryResultStorage",
    "create_storage",
    # Reporter
    "ValidationReporter",
    "ReportConfig",
    "ReportFormat",
    "create_reporter",
    # Notifier
    "NotificationManager",
    "NotificationConfig",
    "NotificationLevel",
    "NotificationChannel",
    "SlackNotifier",
    "WebhookNotifier",
    "InMemoryNotifier",
    "create_notification_manager",
]
