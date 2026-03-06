"""
AI Error Classes.

Defines exception hierarchy for AI operations.
"""

from typing import Optional


class AIError(Exception):
    """Base exception for all AI-related errors."""

    def __init__(self, message: str, details: Optional[dict] = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def __str__(self) -> str:
        if self.details:
            return f"{self.message} - Details: {self.details}"
        return self.message


class ProviderError(AIError):
    """Error from a specific AI provider."""

    def __init__(
        self,
        message: str,
        provider: str,
        status_code: Optional[int] = None,
        details: Optional[dict] = None,
    ):
        super().__init__(message, details)
        self.provider = provider
        self.status_code = status_code

    def __str__(self) -> str:
        parts = [f"[{self.provider}] {self.message}"]
        if self.status_code:
            parts.append(f"(status: {self.status_code})")
        if self.details:
            parts.append(f"- Details: {self.details}")
        return " ".join(parts)


class ConfigurationError(AIError):
    """Error in AI configuration."""

    def __init__(self, message: str, missing_keys: Optional[list] = None):
        super().__init__(message)
        self.missing_keys = missing_keys or []

    def __str__(self) -> str:
        if self.missing_keys:
            return f"{self.message} - Missing: {', '.join(self.missing_keys)}"
        return self.message


class RateLimitError(ProviderError):
    """Provider rate limit exceeded."""

    def __init__(
        self,
        provider: str,
        retry_after: Optional[int] = None,
        details: Optional[dict] = None,
    ):
        super().__init__(
            f"Rate limit exceeded, retry after {retry_after}s" if retry_after else "Rate limit exceeded",
            provider=provider,
            status_code=429,
            details=details,
        )
        self.retry_after = retry_after


class TimeoutError(ProviderError):
    """Provider request timed out."""

    def __init__(self, provider: str, timeout_seconds: float):
        super().__init__(
            f"Request timed out after {timeout_seconds}s",
            provider=provider,
            details={"timeout_seconds": timeout_seconds},
        )
        self.timeout_seconds = timeout_seconds


class ResponseParseError(AIError):
    """Failed to parse LLM response."""

    def __init__(self, message: str, raw_response: Optional[str] = None):
        super().__init__(message)
        self.raw_response = raw_response

    def __str__(self) -> str:
        if self.raw_response:
            preview = self.raw_response[:200] + "..." if len(self.raw_response) > 200 else self.raw_response
            return f"{self.message} - Response preview: {preview}"
        return self.message
