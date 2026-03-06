"""
AI Package - Unified AI capabilities for Spark Validator.

This package consolidates all AI-powered features:
- classifier: Job classification (domain, module, job group, complexity)
- analyzer: Deep code analysis and recommendations
- core: LLM providers (OpenAI, Azure, Ollama, Anthropic)
- syntax: Syntax checking (future)

Usage:
    from validator.ai.classifier import LLMClassifier, ClassificationResult
    from validator.ai.analyzer import LLMAnalyzer
    from validator.ai.core import UnifiedLLMClient
"""

from .core import UnifiedLLMClient, LLMResponse
from .core.errors import AIError, ProviderError, ConfigurationError

__all__ = [
    'UnifiedLLMClient',
    'LLMResponse',
    'AIError',
    'ProviderError',
    'ConfigurationError',
]

__version__ = '1.0.0'
