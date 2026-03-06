"""
Base Provider Interface.

Defines the abstract interface that all AI providers must implement.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from datetime import datetime


@dataclass
class LLMResponse:
    """Standardized response from any LLM provider."""

    content: str
    model: str
    provider: str
    tokens_used: Optional[int] = None
    prompt_tokens: Optional[int] = None
    completion_tokens: Optional[int] = None
    latency_ms: Optional[float] = None
    raw_response: Optional[Dict[str, Any]] = None
    timestamp: datetime = field(default_factory=datetime.now)

    @property
    def total_tokens(self) -> Optional[int]:
        """Calculate total tokens if available."""
        if self.prompt_tokens is not None and self.completion_tokens is not None:
            return self.prompt_tokens + self.completion_tokens
        return self.tokens_used

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'content': self.content,
            'model': self.model,
            'provider': self.provider,
            'tokens_used': self.total_tokens,
            'prompt_tokens': self.prompt_tokens,
            'completion_tokens': self.completion_tokens,
            'latency_ms': self.latency_ms,
            'timestamp': self.timestamp.isoformat(),
        }


class BaseProvider(ABC):
    """
    Abstract base class for all AI providers.

    Each provider must implement:
    - call(): Make an LLM request
    - is_available(): Check if provider is configured
    - name: Provider identifier for logging
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize provider with configuration.

        Args:
            config: Provider-specific configuration dictionary
        """
        self.config = config
        self._validate_config()

    @abstractmethod
    def _validate_config(self) -> None:
        """
        Validate that required configuration is present.

        Raises:
            ConfigurationError: If required config is missing
        """
        pass

    @abstractmethod
    def call(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.3,
        max_tokens: int = 4096,
        **kwargs,
    ) -> LLMResponse:
        """
        Make an LLM call.

        Args:
            system_prompt: System message defining AI behavior
            user_prompt: User message/question
            temperature: Sampling temperature (0.0-1.0)
            max_tokens: Maximum tokens in response
            **kwargs: Provider-specific options

        Returns:
            LLMResponse with generated content

        Raises:
            ProviderError: If the API call fails
            TimeoutError: If the request times out
        """
        pass

    @abstractmethod
    def is_available(self) -> bool:
        """
        Check if provider is configured and reachable.

        Returns:
            True if provider can accept requests
        """
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """
        Provider name for logging and identification.

        Returns:
            Provider identifier string
        """
        pass

    @property
    def model(self) -> str:
        """Get the model name from config."""
        return self.config.get('model', 'unknown')

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(model={self.model})"
