"""
Ollama Provider Implementation.

Supports local LLM inference via Ollama API.
"""

import os
import time
import logging
from typing import Dict, Any, Optional

import requests

from .base import BaseProvider, LLMResponse
from ..errors import ProviderError, TimeoutError, ConfigurationError

logger = logging.getLogger(__name__)


class OllamaProvider(BaseProvider):
    """
    Ollama provider for local LLM inference.

    Configuration:
        base_url: Ollama server URL (default: http://localhost:11434)
        model: Model name (e.g., llama3.1, codellama, mistral)
        timeout_seconds: Request timeout
    """

    DEFAULT_BASE_URL = "http://localhost:11434"
    DEFAULT_MODEL = "llama3.1"
    DEFAULT_TIMEOUT = 120

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Ollama provider.

        Args:
            config: Provider configuration with base_url, model, timeout_seconds
        """
        super().__init__(config)
        self.base_url = self._get_config_value("base_url", self.DEFAULT_BASE_URL)
        self._model = self._get_config_value("model", self.DEFAULT_MODEL)
        self.timeout = config.get("timeout_seconds", self.DEFAULT_TIMEOUT)

    def _get_config_value(self, key: str, default: str) -> str:
        """Get config value with environment variable override."""
        env_key = f"OLLAMA_{key.upper()}"
        return os.getenv(env_key) or self.config.get(key, default)

    def _validate_config(self) -> None:
        """Validate Ollama configuration."""
        # Ollama has sensible defaults, no required config
        pass

    @property
    def name(self) -> str:
        return "ollama"

    @property
    def model(self) -> str:
        return self._model

    def is_available(self) -> bool:
        """Check if Ollama server is reachable."""
        try:
            response = requests.get(
                f"{self.base_url}/api/tags",
                timeout=5
            )
            return response.status_code == 200
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
        Make an Ollama API call.

        Args:
            system_prompt: System message
            user_prompt: User message
            temperature: Sampling temperature (0.0-1.0)
            max_tokens: Maximum tokens in response
            **kwargs: Additional options (e.g., top_p, top_k)

        Returns:
            LLMResponse with generated content

        Raises:
            ProviderError: If API call fails
            TimeoutError: If request times out
        """
        url = f"{self.base_url}/api/generate"

        # Combine prompts (Ollama doesn't have separate system message in generate)
        full_prompt = f"{system_prompt}\n\n{user_prompt}"

        payload = {
            "model": self._model,
            "prompt": full_prompt,
            "stream": False,
            "options": {
                "temperature": temperature,
                "num_predict": max_tokens,
            }
        }

        # Add any extra options from kwargs
        if "top_p" in kwargs:
            payload["options"]["top_p"] = kwargs["top_p"]
        if "top_k" in kwargs:
            payload["options"]["top_k"] = kwargs["top_k"]

        start_time = time.time()

        try:
            response = requests.post(
                url,
                json=payload,
                timeout=self.timeout
            )
            response.raise_for_status()

            data = response.json()
            latency_ms = (time.time() - start_time) * 1000

            return LLMResponse(
                content=data.get("response", ""),
                model=self._model,
                provider=self.name,
                tokens_used=data.get("eval_count"),
                prompt_tokens=data.get("prompt_eval_count"),
                completion_tokens=data.get("eval_count"),
                latency_ms=latency_ms,
                raw_response=data,
            )

        except requests.exceptions.Timeout:
            raise TimeoutError(self.name, self.timeout)
        except requests.exceptions.ConnectionError:
            raise ProviderError(
                f"Cannot connect to Ollama server at {self.base_url}",
                provider=self.name,
                details={"base_url": self.base_url}
            )
        except requests.exceptions.HTTPError as e:
            raise ProviderError(
                f"HTTP error: {e}",
                provider=self.name,
                status_code=e.response.status_code if e.response else None,
            )
        except Exception as e:
            raise ProviderError(
                str(e),
                provider=self.name,
            )

    def call_chat(
        self,
        messages: list,
        temperature: float = 0.3,
        max_tokens: int = 4096,
        **kwargs,
    ) -> LLMResponse:
        """
        Make an Ollama chat API call (for multi-turn conversations).

        Args:
            messages: List of message dicts with 'role' and 'content'
            temperature: Sampling temperature
            max_tokens: Maximum tokens

        Returns:
            LLMResponse with generated content
        """
        url = f"{self.base_url}/api/chat"

        payload = {
            "model": self._model,
            "messages": messages,
            "stream": False,
            "options": {
                "temperature": temperature,
                "num_predict": max_tokens,
            }
        }

        start_time = time.time()

        try:
            response = requests.post(
                url,
                json=payload,
                timeout=self.timeout
            )
            response.raise_for_status()

            data = response.json()
            latency_ms = (time.time() - start_time) * 1000

            return LLMResponse(
                content=data.get("message", {}).get("content", ""),
                model=self._model,
                provider=self.name,
                tokens_used=data.get("eval_count"),
                latency_ms=latency_ms,
                raw_response=data,
            )

        except Exception as e:
            raise ProviderError(str(e), provider=self.name)
