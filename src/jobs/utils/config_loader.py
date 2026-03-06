"""
Configuration Loader for STG_XTC_LD_TEST_JOB

This module provides configuration loading with:
- YAML file loading (base + environment-specific)
- Environment variable interpolation (${VAR_NAME})
- Deep merge of configuration dictionaries
- Backward compatibility with legacy JSON parameter files
- Pydantic validation

Usage:
    loader = ConfigLoader()
    config = loader.load(environment="prod")
    # Or with legacy JSON compatibility:
    config = loader.load(
        environment="prod",
        legacy_json_paths={
            'gdm': '/path/to/gdm_params.json',
            'pcs': '/path/to/pcs_params.json'
        }
    )
"""

import yaml
import json
import os
import re
from pathlib import Path
from typing import Dict, Any, Optional
from copy import deepcopy

import sys
# Add parent directory to path to import config module
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.job_config_schema import JobConfig


class ConfigLoader:
    """
    Configuration loader with YAML support, environment variable
    interpolation, and legacy JSON compatibility.
    """

    def __init__(self, config_dir: Optional[Path] = None):
        """
        Initialize configuration loader.

        Args:
            config_dir: Path to config directory. Defaults to ../config
        """
        if config_dir is None:
            # Default to ../config relative to this file
            self.config_dir = Path(__file__).parent.parent / "config"
        else:
            self.config_dir = Path(config_dir)

        if not self.config_dir.exists():
            raise FileNotFoundError(f"Config directory not found: {self.config_dir}")

    def load(
        self,
        environment: str = "dev",
        legacy_json_paths: Optional[Dict[str, str]] = None
    ) -> JobConfig:
        """
        Load and merge configuration from YAML files.

        Args:
            environment: Environment name (dev, test, prod)
            legacy_json_paths: Optional dict with 'gdm' and 'pcs' JSON file paths
                              for backward compatibility

        Returns:
            Validated JobConfig instance

        Example:
            # New way (YAML only)
            config = loader.load(environment="prod")

            # Old way (with legacy JSON)
            config = loader.load(
                environment="prod",
                legacy_json_paths={
                    'gdm': '/path/to/gdm_params.json',
                    'pcs': '/path/to/pcs_params.json'
                }
            )
        """
        # Step 1: Load base configuration
        base_config = self._load_yaml_file("base.yaml")

        # Step 2: Load environment-specific configuration
        env_config = self._load_yaml_file(f"{environment}.yaml")

        # Step 3: Deep merge configurations
        merged_config = self._deep_merge(base_config, env_config)

        # Step 4: Interpolate environment variables
        merged_config = self._interpolate_env_vars(merged_config)

        # Step 5: Merge legacy JSON parameters if provided
        if legacy_json_paths:
            runtime_params = self._load_legacy_json(legacy_json_paths)
            merged_config = self._merge_runtime_params(merged_config, runtime_params)

        # Step 6: Validate with Pydantic
        return JobConfig(**merged_config)

    def _load_yaml_file(self, filename: str) -> Dict[str, Any]:
        """
        Load YAML file from config directory.

        Args:
            filename: YAML filename

        Returns:
            Parsed YAML as dictionary
        """
        filepath = self.config_dir / filename
        if not filepath.exists():
            raise FileNotFoundError(f"Config file not found: {filepath}")

        with open(filepath, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)

        return data if data is not None else {}

    def _deep_merge(self, base: Dict, override: Dict) -> Dict:
        """
        Deep merge two dictionaries.

        Override values take precedence over base values.
        For nested dictionaries, merge recursively.

        Args:
            base: Base configuration dictionary
            override: Override configuration dictionary

        Returns:
            Merged dictionary
        """
        result = deepcopy(base)

        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                # Recursively merge nested dictionaries
                result[key] = self._deep_merge(result[key], value)
            else:
                # Override value
                result[key] = deepcopy(value)

        return result

    def _interpolate_env_vars(self, config: Any) -> Any:
        """
        Recursively replace ${VAR_NAME} with environment variable values.

        Supports:
        - ${VAR_NAME} - Required variable (raises error if not set)
        - ${VAR_NAME:-default} - Optional variable with default value

        Args:
            config: Configuration value (dict, list, str, or other)

        Returns:
            Configuration with environment variables interpolated
        """
        if isinstance(config, dict):
            return {k: self._interpolate_env_vars(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._interpolate_env_vars(item) for item in config]
        elif isinstance(config, str):
            return self._interpolate_string(config)
        else:
            return config

    def _interpolate_string(self, value: str) -> str:
        """
        Interpolate environment variables in a string.

        Patterns:
        - ${VAR_NAME} - Required variable
        - ${VAR_NAME:-default_value} - Optional variable with default

        Args:
            value: String value that may contain ${VAR} patterns

        Returns:
            String with variables interpolated
        """
        # Pattern to match ${VAR_NAME} or ${VAR_NAME:-default}
        pattern = r'\$\{([^}:]+)(?::-([^}]*))?\}'

        def replacer(match):
            var_name = match.group(1)
            default_value = match.group(2)

            env_value = os.environ.get(var_name)

            if env_value is not None:
                return env_value
            elif default_value is not None:
                return default_value
            else:
                raise ValueError(
                    f"Required environment variable not set: {var_name}\n"
                    f"Please set {var_name} or provide a default value with "
                    f"${{VAR_NAME:-default}}"
                )

        return re.sub(pattern, replacer, value)

    def _load_legacy_json(self, json_paths: Dict[str, str]) -> Dict[str, Any]:
        """
        Load legacy JSON parameter files.

        Args:
            json_paths: Dictionary with 'gdm' and 'pcs' keys pointing to JSON files

        Returns:
            Dictionary with 'gdm' and 'pcs' parameter dictionaries
        """
        result = {}

        for key, path in json_paths.items():
            if path and path != "(As pre-defined)":  # Skip default placeholder
                try:
                    with open(path, 'r', encoding='utf-8') as f:
                        result[key] = json.load(f)
                except FileNotFoundError:
                    raise FileNotFoundError(f"Legacy JSON file not found: {path}")
                except json.JSONDecodeError as e:
                    raise ValueError(f"Invalid JSON in {path}: {e}")

        return result

    def _merge_runtime_params(
        self,
        config: Dict[str, Any],
        runtime_params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Merge legacy runtime parameters into configuration.

        This provides backward compatibility with the old JSON parameter files.
        The runtime parameters are stored in a special 'runtime' section.

        Args:
            config: Base configuration dictionary
            runtime_params: Runtime parameters from JSON files

        Returns:
            Configuration with runtime parameters merged
        """
        result = deepcopy(config)

        # Add runtime parameters as a separate section
        # This allows the job code to access them via config.runtime.gdm['key']
        if not isinstance(result, dict):
            result = {}

        # Store runtime params for backward compatibility
        result['runtime'] = runtime_params

        return result


class RuntimeParams:
    """
    Helper class to access legacy runtime parameters.

    Usage:
        params = RuntimeParams(config)
        business_date = params.get_pcs_param("JpBusinessDate")
        reject_path = params.get_gdm_param("SSRejectFilePath")
    """

    def __init__(self, config: JobConfig):
        """
        Initialize with JobConfig.

        Args:
            config: JobConfig instance
        """
        self.config = config

    def get_gdm_param(self, key: str, default: Any = None) -> Any:
        """Get GDM parameter by key."""
        return self._get_runtime_param('gdm', key, default)

    def get_pcs_param(self, key: str, default: Any = None) -> Any:
        """Get PCS parameter by key."""
        return self._get_runtime_param('pcs', key, default)

    def _get_runtime_param(self, param_set: str, key: str, default: Any = None) -> Any:
        """
        Get runtime parameter value.

        Args:
            param_set: 'gdm' or 'pcs'
            key: Parameter key
            default: Default value if not found

        Returns:
            Parameter value or default
        """
        runtime = getattr(self.config, 'runtime', None)
        if runtime is None:
            return default

        param_dict = runtime.get(param_set, {})
        return param_dict.get(key, default)


# Convenience function for quick loading
def load_config(
    environment: str = "dev",
    legacy_json_paths: Optional[Dict[str, str]] = None,
    config_dir: Optional[Path] = None
) -> JobConfig:
    """
    Convenience function to load configuration.

    Args:
        environment: Environment name (dev, test, prod)
        legacy_json_paths: Optional legacy JSON file paths
        config_dir: Optional config directory path

    Returns:
        Validated JobConfig instance

    Example:
        from utils.config_loader import load_config
        config = load_config(environment="prod")
    """
    loader = ConfigLoader(config_dir=config_dir)
    return loader.load(environment=environment, legacy_json_paths=legacy_json_paths)
