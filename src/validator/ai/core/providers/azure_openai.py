"""
Azure OpenAI Provider Implementation.

Supports Azure-hosted OpenAI models.
"""

import os
import time
import logging
from typing import Dict, Any, List, Optional

from .base import BaseProvider, LLMResponse
from ..errors import ProviderError, TimeoutError, ConfigurationError, RateLimitError

logger = logging.getLogger(__name__)


class AzureOpenAIProvider(BaseProvider):
    """
    Azure OpenAI provider for cloud LLM inference.

    Configuration:
        endpoint: Azure OpenAI endpoint URL
        api_key: Azure OpenAI API key
        deployment: Model deployment name
        api_version: API version (default: 2024-02-15-preview)
        timeout_seconds: Request timeout
    """

    DEFAULT_API_VERSION = "2024-02-15-preview"
    DEFAULT_TIMEOUT = 60

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Azure OpenAI provider.

        Args:
            config: Provider configuration with endpoint, api_key, deployment
        """
        # Set config first so _get_env_or_config works
        self.config = config

        # Set properties before calling super().__init__ which calls _validate_config
        self.endpoint = self._get_env_or_config("AZURE_OPENAI_ENDPOINT", "endpoint")
        self.api_key = self._get_env_or_config("AZURE_OPENAI_KEY", "api_key")
        self.deployment = self._get_env_or_config("AZURE_OPENAI_DEPLOYMENT", "deployment", "gpt-4")
        self.api_version = self._get_env_or_config("AZURE_OPENAI_API_VERSION", "api_version", self.DEFAULT_API_VERSION)
        self.timeout = config.get("timeout_seconds", self.DEFAULT_TIMEOUT)

        self._client = None

        # Now validate (skip super().__init__ since we set config manually)
        self._validate_config()

    def _get_env_or_config(self, env_key: str, config_key: str, default: str = None) -> Optional[str]:
        """Get value from environment variable or config, with optional default."""
        return os.getenv(env_key) or self.config.get(config_key, default)

    def _validate_config(self) -> None:
        """Validate Azure OpenAI configuration."""
        missing = []
        if not self.endpoint:
            missing.append("endpoint (or AZURE_OPENAI_ENDPOINT env)")
        if not self.api_key:
            missing.append("api_key (or AZURE_OPENAI_KEY env)")
        if not self.deployment:
            missing.append("deployment (or AZURE_OPENAI_DEPLOYMENT env)")

        if missing:
            raise ConfigurationError(
                "Azure OpenAI configuration incomplete",
                missing_keys=missing
            )

    @property
    def name(self) -> str:
        return "azure-openai"

    @property
    def model(self) -> str:
        return self.deployment or "gpt-4"

    def _get_client(self):
        """Lazy initialization of Azure OpenAI client."""
        if self._client is None:
            try:
                from openai import AzureOpenAI
                self._client = AzureOpenAI(
                    azure_endpoint=self.endpoint,
                    api_key=self.api_key,
                    api_version=self.api_version,
                )
            except ImportError:
                raise ConfigurationError(
                    "openai package not installed. Run: pip install openai"
                )
        return self._client

    def is_available(self) -> bool:
        """Check if Azure OpenAI is configured and reachable."""
        if not self.endpoint or not self.api_key:
            return False

        try:
            # Try to get client - will fail if credentials invalid
            client = self._get_client()
            # Could do a lightweight API call here, but for now just check config
            return True
        except Exception:
            return False

    def call(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.3,
        max_tokens: int = 4096,
        **kwargs,
    ) -> LLMResponse:
        """
        Make an Azure OpenAI API call.

        Args:
            system_prompt: System message
            user_prompt: User message
            temperature: Sampling temperature (0.0-1.0)
            max_tokens: Maximum tokens in response
            **kwargs: Additional options (e.g., top_p, frequency_penalty)

        Returns:
            LLMResponse with generated content

        Raises:
            ProviderError: If API call fails
            TimeoutError: If request times out
            RateLimitError: If rate limit exceeded
        """
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]

        return self.call_chat(messages, temperature, max_tokens, **kwargs)

    def call_chat(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.3,
        max_tokens: int = 4096,
        **kwargs,
    ) -> LLMResponse:
        """
        Make an Azure OpenAI chat API call.

        Args:
            messages: List of message dicts with 'role' and 'content'
            temperature: Sampling temperature
            max_tokens: Maximum tokens
            **kwargs: Additional options

        Returns:
            LLMResponse with generated content
        """
        client = self._get_client()
        start_time = time.time()

        try:
            # Build request parameters
            # Note: Newer models (O1, O3, GPT-5) use max_completion_tokens instead of max_tokens
            request_params = {
                "model": self.deployment,
                "messages": messages,
                "temperature": temperature,
            }

            # Use appropriate token limit parameter based on model
            if any(m in self.deployment.lower() for m in ['o1', 'o3', 'gpt-5', 'gpt5']):
                request_params["max_completion_tokens"] = max_tokens
            else:
                request_params["max_tokens"] = max_tokens

            # Add optional parameters
            if "top_p" in kwargs:
                request_params["top_p"] = kwargs["top_p"]
            if "frequency_penalty" in kwargs:
                request_params["frequency_penalty"] = kwargs["frequency_penalty"]
            if "presence_penalty" in kwargs:
                request_params["presence_penalty"] = kwargs["presence_penalty"]

            response = client.chat.completions.create(
                **request_params,
                timeout=self.timeout,
            )

            latency_ms = (time.time() - start_time) * 1000
            choice = response.choices[0]
            usage = response.usage

            return LLMResponse(
                content=choice.message.content or "",
                model=response.model,
                provider=self.name,
                prompt_tokens=usage.prompt_tokens if usage else None,
                completion_tokens=usage.completion_tokens if usage else None,
                latency_ms=latency_ms,
                raw_response={
                    "id": response.id,
                    "finish_reason": choice.finish_reason,
                    "usage": {
                        "prompt_tokens": usage.prompt_tokens,
                        "completion_tokens": usage.completion_tokens,
                        "total_tokens": usage.total_tokens,
                    } if usage else None,
                },
            )

        except Exception as e:
            error_str = str(e)

            # Check for rate limiting
            if "429" in error_str or "rate limit" in error_str.lower():
                raise RateLimitError(
                    provider=self.name,
                    retry_after=60,  # Default retry after
                )

            # Check for timeout
            if "timeout" in error_str.lower():
                raise TimeoutError(self.name, self.timeout)

            # Generic error
            raise ProviderError(
                error_str,
                provider=self.name,
            )
