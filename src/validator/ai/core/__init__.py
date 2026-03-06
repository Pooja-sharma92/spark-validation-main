"""
AI Core - Shared infrastructure for AI providers.

Provides unified LLM client with provider abstraction and fallback support.
"""

from .client import UnifiedLLMClient
from .providers.base import BaseProvider, LLMResponse
from .errors import AIError, ProviderError, ConfigurationError

__all__ = [
    'UnifiedLLMClient',
    'BaseProvider',
    'LLMResponse',
    'AIError',
    'ProviderError',
    'ConfigurationError',
]
