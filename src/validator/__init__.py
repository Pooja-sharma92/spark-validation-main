"""
Spark Job Validation Framework

A scalable framework for validating thousands of Spark jobs with:
- Multi-source triggers (Git webhook, file watcher, CI/CD, manual)
- Priority queue with rate limiting
- Concurrent validation with backpressure handling
- Result persistence and notification
"""

__version__ = "1.0.0"

from validator.models import (
    ValidationRequest,
    ValidationResult,
    ValidationStatus,
    Priority,
)

__all__ = [
    "ValidationRequest",
    "ValidationResult",
    "ValidationStatus",
    "Priority",
]
