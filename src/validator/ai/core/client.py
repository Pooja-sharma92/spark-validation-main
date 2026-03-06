"""
Unified LLM Client.

Provides a single interface for calling LLMs with automatic provider
fallback and retry logic.
"""

import os
import time
import logging
from typing import Dict, Any, List, Optional, Type

from .providers.base import BaseProvider, LLMResponse
from .providers.ollama import OllamaProvider
from .providers.azure_openai import AzureOpenAIProvider
from .errors import AIError, ProviderError, ConfigurationError, RateLimitError

logger = logging.getLogger(__name__)


# Registry of available providers
PROVIDER_REGISTRY: Dict[str, Type[BaseProvider]] = {
    "ollama": OllamaProvider,
    "azure-openai": AzureOpenAIProvider,
}


class UnifiedLLMClient:
    """
    Unified LLM client with provider fallback and retry support.

    Features:
    - Multiple provider support with fallback chain
    - Automatic retry with exponential backoff
    - Configuration from YAML or environment variables
    - Consistent response format across providers

    Usage:
        # From configuration
        client = UnifiedLLMClient.from_config(config)

        # Make a call
        response = client.call(
            system_prompt="You are a helpful assistant.",
            user_prompt="Explain PySpark joins."
        )
        print(response.content)
    """

    def __init__(
        self,
        providers: List[BaseProvider],
        max_retries: int = 3,
        retry_delay: float = 1.0,
        backoff_multiplier: float = 2.0,
    ):
        """
        Initialize the unified client.

        Args:
            providers: List of providers in fallback order
            max_retries: Maximum retry attempts per provider
            retry_delay: Initial delay between retries (seconds)
            backoff_multiplier: Multiplier for exponential backoff
        """
        if not providers:
            raise ConfigurationError("At least one provider is required")

        self.providers = providers
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.backoff_multiplier = backoff_multiplier

        logger.info(
            f"UnifiedLLMClient initialized with providers: "
            f"{[p.name for p in providers]}"
        )

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "UnifiedLLMClient":
        """
        Create client from configuration dictionary.

        Config structure:
            providers:
                primary: "ollama"
                fallback: "azure-openai"
                ollama:
                    base_url: "http://localhost:11434"
                    model: "llama3.1"
                azure_openai:
                    endpoint: "..."
                    api_key: "..."
                    deployment: "gpt-4"
            retry:
                max_attempts: 3
                delay_seconds: 1.0
                backoff_multiplier: 2.0

        Args:
            config: Configuration dictionary (usually from framework.yaml)

        Returns:
            Configured UnifiedLLMClient instance
        """
        providers_config = config.get("providers", config.get("ai_providers", {}))

        # Determine provider order
        primary = providers_config.get("primary", "ollama")
        fallback = providers_config.get("fallback", "azure-openai")

        provider_order = [primary]
        if fallback and fallback != primary:
            provider_order.append(fallback)

        # Additional providers from list if specified
        additional = providers_config.get("additional", [])
        for p in additional:
            if p not in provider_order:
                provider_order.append(p)

        # Initialize providers
        providers = []
        for provider_name in provider_order:
            provider_class = PROVIDER_REGISTRY.get(provider_name)
            if not provider_class:
                logger.warning(f"Unknown provider: {provider_name}, skipping")
                continue

            # Get provider-specific config
            # Handle both "azure_openai" and "azure-openai" naming
            config_key = provider_name.replace("-", "_")
            provider_config = providers_config.get(config_key, providers_config.get(provider_name, {}))

            try:
                provider = provider_class(provider_config)
                providers.append(provider)
                logger.debug(f"Initialized provider: {provider_name}")
            except ConfigurationError as e:
                logger.warning(f"Failed to initialize {provider_name}: {e}")
                continue

        if not providers:
            raise ConfigurationError(
                "No providers could be initialized. Check your AI configuration."
            )

        # Get retry config
        retry_config = config.get("retry", {})

        return cls(
            providers=providers,
            max_retries=retry_config.get("max_attempts", 3),
            retry_delay=retry_config.get("delay_seconds", 1.0),
            backoff_multiplier=retry_config.get("backoff_multiplier", 2.0),
        )

    def call(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.3,
        max_tokens: int = 4096,
        **kwargs,
    ) -> LLMResponse:
        """
        Make an LLM call with automatic fallback between providers.

        Args:
            system_prompt: System message defining AI behavior
            user_prompt: User message/question
            temperature: Sampling temperature (0.0-1.0)
            max_tokens: Maximum tokens in response
            **kwargs: Provider-specific options

        Returns:
            LLMResponse with generated content

        Raises:
            AIError: If all providers fail after retries
        """
        errors = []

        for provider in self.providers:
            try:
                response = self._call_with_retry(
                    provider,
                    system_prompt,
                    user_prompt,
                    temperature,
                    max_tokens,
                    **kwargs,
                )
                return response

            except RateLimitError as e:
                logger.warning(f"Rate limit on {provider.name}, trying next provider")
                errors.append(e)
                continue

            except ProviderError as e:
                logger.warning(f"Provider {provider.name} failed: {e}")
                errors.append(e)
                continue

            except Exception as e:
                logger.error(f"Unexpected error from {provider.name}: {e}")
                errors.append(e)
                continue

        # All providers failed
        error_summary = "; ".join([str(e) for e in errors])
        raise AIError(
            f"All AI providers failed. Errors: {error_summary}",
            details={"providers_tried": [p.name for p in self.providers]}
        )

    def _call_with_retry(
        self,
        provider: BaseProvider,
        system_prompt: str,
        user_prompt: str,
        temperature: float,
        max_tokens: int,
        **kwargs,
    ) -> LLMResponse:
        """
        Call a single provider with retry logic.

        Args:
            provider: The provider to call
            system_prompt: System message
            user_prompt: User message
            temperature: Sampling temperature
            max_tokens: Max tokens
            **kwargs: Additional options

        Returns:
            LLMResponse from successful call

        Raises:
            ProviderError: If all retries fail
        """
        last_error = None
        delay = self.retry_delay

        for attempt in range(self.max_retries):
            try:
                response = provider.call(
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    **kwargs,
                )

                if attempt > 0:
                    logger.info(
                        f"Provider {provider.name} succeeded on attempt {attempt + 1}"
                    )

                return response

            except RateLimitError:
                # Don't retry rate limits, bubble up for fallback
                raise

            except ProviderError as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    logger.warning(
                        f"Attempt {attempt + 1}/{self.max_retries} failed for "
                        f"{provider.name}: {e}. Retrying in {delay:.1f}s..."
                    )
                    time.sleep(delay)
                    delay *= self.backoff_multiplier

        # All retries exhausted
        raise last_error or ProviderError(
            "Max retries exceeded",
            provider=provider.name
        )

    def get_available_providers(self) -> List[str]:
        """
        Get list of currently available providers.

        Returns:
            List of provider names that are configured and reachable
        """
        available = []
        for provider in self.providers:
            if provider.is_available():
                available.append(provider.name)
        return available

    def __repr__(self) -> str:
        provider_names = [p.name for p in self.providers]
        return f"UnifiedLLMClient(providers={provider_names})"
