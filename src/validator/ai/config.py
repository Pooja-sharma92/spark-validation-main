"""
AI Configuration Loader.

Loads and validates AI configuration from framework.yaml.
"""

import os
import re
import logging
from pathlib import Path
from typing import Dict, Any, Optional

import yaml

logger = logging.getLogger(__name__)


def _interpolate_env_vars(value: Any) -> Any:
    """
    Interpolate environment variables in config values.

    Supports format: ${VAR_NAME:default_value}
    """
    if isinstance(value, str):
        pattern = r'\$\{([^}:]+)(?::([^}]*))?\}'

        def replacer(match):
            var_name = match.group(1)
            default = match.group(2) if match.group(2) is not None else ""
            return os.getenv(var_name, default)

        return re.sub(pattern, replacer, value)

    elif isinstance(value, dict):
        return {k: _interpolate_env_vars(v) for k, v in value.items()}

    elif isinstance(value, list):
        return [_interpolate_env_vars(item) for item in value]

    return value


def load_ai_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load AI configuration from framework.yaml.

    Args:
        config_path: Path to config file. If None, searches common locations.

    Returns:
        AI configuration dictionary with environment variables interpolated.
    """
    if config_path is None:
        # Search common locations
        search_paths = [
            Path("src/config/framework.yaml"),
            Path("config/framework.yaml"),
            Path("framework.yaml"),
            Path(__file__).parent.parent / "config" / "framework.yaml",
        ]

        for path in search_paths:
            if path.exists():
                config_path = str(path)
                break

    if config_path is None:
        logger.warning("No config file found, using defaults")
        return get_default_config()

    try:
        with open(config_path, 'r') as f:
            full_config = yaml.safe_load(f)

        # Extract AI section
        ai_config = full_config.get("ai", full_config.get("classification", {}))

        # Interpolate environment variables
        ai_config = _interpolate_env_vars(ai_config)

        logger.info(f"Loaded AI config from {config_path}")
        return ai_config

    except Exception as e:
        logger.error(f"Failed to load config from {config_path}: {e}")
        return get_default_config()


def get_default_config() -> Dict[str, Any]:
    """
    Get default AI configuration.

    Returns:
        Default configuration dictionary
    """
    return {
        "enabled": True,
        "providers": {
            "primary": "ollama",
            "fallback": "azure-openai",
            "ollama": {
                "base_url": os.getenv("OLLAMA_BASE_URL", "http://localhost:11434"),
                "model": os.getenv("OLLAMA_MODEL", "llama3.1"),
                "timeout_seconds": 120,
            },
            "azure_openai": {
                "endpoint": os.getenv("AZURE_OPENAI_ENDPOINT", ""),
                "api_key": os.getenv("AZURE_OPENAI_KEY", ""),
                "deployment": os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4"),
                "api_version": os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview"),
                "timeout_seconds": 60,
            },
        },
        "retry": {
            "max_attempts": 3,
            "delay_seconds": 1.0,
            "backoff_multiplier": 2.0,
        },
        "classifier": {
            "enabled": True,
            "batch": {
                "max_concurrent_jobs": 5,
            },
            "category_discovery": {
                "enabled": True,
                "auto_approve_threshold": 0.9,
            },
        },
        "analyzer": {
            "enabled": True,
            "max_code_length": 8000,
        },
    }
