"""
Configuration management for the Validation Framework.

Loads configuration from YAML files with environment variable interpolation.
Provides type-safe configuration objects using dataclasses.

FINAL FIX:
- If executor is run inside container with env vars like POSTGRES_HOST, POSTGRES_PORT,
  the framework must honor them even if YAML doesn't use ${...} placeholders.
- Otherwise defaults "localhost:5432" get used and container tries 127.0.0.1.
"""

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Dict, Any, List, Union
import yaml


# Environment variable pattern: ${VAR_NAME} or ${VAR_NAME:default}
ENV_VAR_PATTERN = re.compile(r"\$\{([^}:]+)(?::([^}]*))?\}")


def _interpolate_env_vars(value: Any) -> Any:
    """
    Recursively interpolate environment variables in configuration values.

    Supports:
    - ${VAR_NAME} - Required variable
    - ${VAR_NAME:default} - Variable with default value
    """
    if isinstance(value, str):

        def replace_var(match):
            var_name = match.group(1)
            default = match.group(2)
            env_value = os.environ.get(var_name)
            if env_value is not None:
                return env_value
            if default is not None:
                return default
            # Keep original if no value and no default
            return match.group(0)

        return ENV_VAR_PATTERN.sub(replace_var, value)

    elif isinstance(value, dict):
        return {k: _interpolate_env_vars(v) for k, v in value.items()}

    elif isinstance(value, list):
        return [_interpolate_env_vars(item) for item in value]

    return value


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(name)
    if v is None:
        return default
    v = str(v).strip()
    return default if v == "" else v


def _env_int(name: str, default: int) -> int:
    v = _env(name, None)
    if v is None:
        return default
    try:
        return int(v)
    except Exception:
        return default


def _apply_env_overrides(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    FINAL RULE:
    - If env vars are provided, they ALWAYS override YAML/defaults.
    - This prevents "localhost/127.0.0.1" inside container.
    """
    out = dict(data or {})

    # Ensure sections exist
    out.setdefault("redis", {})
    out.setdefault("postgres", {})

    # Redis: allow REDIS_URL override
    redis_url = _env("REDIS_URL")
    if redis_url:
        out["redis"]["url"] = redis_url

    # PostgreSQL: allow POSTGRES_* overrides
    pg_host = _env("POSTGRES_HOST")
    pg_port = _env("POSTGRES_PORT")
    pg_db = _env("POSTGRES_DB")
    pg_user = _env("POSTGRES_USER")
    pg_password = _env("POSTGRES_PASSWORD")

    if pg_host:
        out["postgres"]["host"] = pg_host
    if pg_port:
        try:
            out["postgres"]["port"] = int(pg_port)
        except Exception:
            pass
    if pg_db:
        # Note: in this project config uses "database"
        out["postgres"]["database"] = pg_db
    if pg_user:
        out["postgres"]["user"] = pg_user
    if pg_password is not None:
        out["postgres"]["password"] = pg_password

    return out


@dataclass
class RedisConfig:
    """Redis connection configuration."""
    url: str = "redis://localhost:6379/0"
    max_connections: int = 10


@dataclass
class PostgresConfig:
    """PostgreSQL connection configuration."""
    host: str = "localhost"
    port: int = 5432
    database: str = "validation_results"
    user: str = "postgres"
    password: str = ""

    @property
    def url(self) -> str:
        """Generate SQLAlchemy connection URL."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class PriorityRateLimitConfig:
    """Rate limit configuration for a priority level."""
    tokens_per_minute: float
    burst_size: int


@dataclass
class QueueConfig:
    """Queue management configuration."""
    max_concurrent: int = 5
    dedup_window_seconds: int = 60
    job_ttl_seconds: int = 86400

    # Rate limits by priority (key is priority level 0-3)
    rate_limits: Dict[int, Optional[PriorityRateLimitConfig]] = field(default_factory=lambda: {
        0: None,  # CRITICAL: No limit
        1: PriorityRateLimitConfig(10, 5),   # MANUAL
        2: PriorityRateLimitConfig(30, 10),  # CI_CD
        3: PriorityRateLimitConfig(100, 20), # BATCH
    })


@dataclass
class BackpressureConfig:
    """Backpressure threshold configuration."""
    warning_threshold: int = 50
    critical_threshold: int = 100
    reject_threshold: int = 200


@dataclass
class ValidationStageConfig:
    """Configuration for a validation stage."""
    name: str
    enabled: bool = True
    timeout_seconds: int = 60


@dataclass
class ValidationConfig:
    """Validation pipeline configuration."""
    stages: List[ValidationStageConfig] = field(default_factory=lambda: [
        ValidationStageConfig("syntax", True, 60),
        ValidationStageConfig("logic", True, 60),
        ValidationStageConfig("data", True, 120),
        ValidationStageConfig("expectations", True, 300),
        ValidationStageConfig("ai_analysis", False, 180),
    ])


@dataclass
class TriggerGitConfig:
    """Git webhook trigger configuration."""
    enabled: bool = True
    secret: str = ""
    events: List[str] = field(default_factory=lambda: ["push", "pull_request"])
    priority_map: Dict[str, int] = field(default_factory=lambda: {
        "hotfix/*": 0,
        "release/*": 1,
        "main": 2,
        "develop": 2,
        "feature/*": 3,
    })


@dataclass
class TriggerFileWatcherConfig:
    """File watcher trigger configuration."""
    enabled: bool = True
    paths: List[str] = field(default_factory=lambda: ["./jobs/**/*.py"])
    ignore_patterns: List[str] = field(default_factory=lambda: [
        "**/test_*.py",
        "**/__pycache__/**",
    ])
    debounce_seconds: float = 5.0


@dataclass
class TriggerApiConfig:
    """API trigger configuration."""
    enabled: bool = True
    auth_type: str = "api_key"
    auth_header: str = "X-API-Key"


@dataclass
class TriggerSchedulerConfig:
    """Scheduled trigger configuration."""
    enabled: bool = False
    jobs: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class TriggersConfig:
    """All trigger sources configuration."""
    git: TriggerGitConfig = field(default_factory=TriggerGitConfig)
    file_watcher: TriggerFileWatcherConfig = field(default_factory=TriggerFileWatcherConfig)
    api: TriggerApiConfig = field(default_factory=TriggerApiConfig)
    scheduler: TriggerSchedulerConfig = field(default_factory=TriggerSchedulerConfig)


@dataclass
class NotificationSlackConfig:
    """Slack notification configuration."""
    enabled: bool = False
    webhook_url: str = ""
    channels: Dict[str, str] = field(default_factory=dict)


@dataclass
class NotificationWebhookConfig:
    """Generic webhook notification configuration."""
    enabled: bool = False
    url: str = ""
    headers: Dict[str, str] = field(default_factory=dict)


@dataclass
class NotificationsConfig:
    """Notification configuration."""
    slack: NotificationSlackConfig = field(default_factory=NotificationSlackConfig)
    webhook: NotificationWebhookConfig = field(default_factory=NotificationWebhookConfig)


@dataclass
class MonitoringConfig:
    """Monitoring and metrics configuration."""
    prometheus_enabled: bool = True
    prometheus_port: int = 9090
    health_check_port: int = 8080


@dataclass
class FrameworkConfig:
    """Root configuration for the entire framework."""
    name: str = "spark-validator"
    version: str = "1.0.0"

    redis: RedisConfig = field(default_factory=RedisConfig)
    postgres: PostgresConfig = field(default_factory=PostgresConfig)
    queue: QueueConfig = field(default_factory=QueueConfig)
    backpressure: BackpressureConfig = field(default_factory=BackpressureConfig)
    validation: ValidationConfig = field(default_factory=ValidationConfig)
    triggers: TriggersConfig = field(default_factory=TriggersConfig)
    notifications: NotificationsConfig = field(default_factory=NotificationsConfig)
    monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)


def _dict_to_dataclass(data: Dict[str, Any], cls: type) -> Any:
    """
    Convert a dictionary to a dataclass instance, handling nested dataclasses.
    """
    if not hasattr(cls, "__dataclass_fields__"):
        return data

    field_values = {}
    for field_name, field_info in cls.__dataclass_fields__.items():
        if field_name in data:
            value = data[field_name]
            field_type = field_info.type

            # Handle Optional types
            origin = getattr(field_type, "__origin__", None)
            if origin is type(None) or (hasattr(field_type, "__args__") and type(None) in getattr(field_type, "__args__", ())):
                args = getattr(field_type, "__args__", ())
                field_type = next((t for t in args if t is not type(None)), field_type)

            # Handle nested dataclasses
            if hasattr(field_type, "__dataclass_fields__") and isinstance(value, dict):
                field_values[field_name] = _dict_to_dataclass(value, field_type)
            # Handle List of dataclasses
            elif origin is list and isinstance(value, list):
                args = getattr(field_type, "__args__", ())
                if args and hasattr(args[0], "__dataclass_fields__"):
                    field_values[field_name] = [
                        _dict_to_dataclass(item, args[0]) if isinstance(item, dict) else item
                        for item in value
                    ]
                else:
                    field_values[field_name] = value
            # Handle Dict with dataclass values
            elif origin is dict and isinstance(value, dict):
                field_values[field_name] = value
            else:
                field_values[field_name] = value

    return cls(**field_values)


def load_config(
    config_path: Optional[Union[str, Path]] = None,
    config_dict: Optional[Dict[str, Any]] = None,
) -> FrameworkConfig:
    """
    Load framework configuration from YAML file or dictionary.
    """
    data: Dict[str, Any] = {}

    # Load from file if provided
    if config_path:
        path = Path(config_path)
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                file_data = yaml.safe_load(f) or {}
                if isinstance(file_data, dict):
                    data.update(file_data)

    # Override with dictionary if provided
    if config_dict:
        _deep_merge(data, config_dict)

    # Interpolate environment variables in YAML values
    data = _interpolate_env_vars(data)

    # FINAL: apply env overrides even if YAML didn't use ${VAR}
    data = _apply_env_overrides(data)

    # Convert to dataclass
    return _dict_to_dataclass(data, FrameworkConfig)


def _deep_merge(base: Dict, override: Dict) -> None:
    """Deep merge override into base dictionary."""
    for key, value in override.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value


def save_config(config: FrameworkConfig, path: Union[str, Path]) -> None:
    """Save configuration to YAML file."""
    import dataclasses

    def to_dict(obj):
        if dataclasses.is_dataclass(obj):
            return {k: to_dict(v) for k, v in dataclasses.asdict(obj).items()}
        elif isinstance(obj, dict):
            return {k: to_dict(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [to_dict(item) for item in obj]
        return obj

    data = to_dict(config)

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)


# Default configuration instance
_default_config: Optional[FrameworkConfig] = None


def get_config() -> FrameworkConfig:
    """
    Get the global configuration instance.

    Creates a default configuration if not already loaded.
    """
    global _default_config
    if _default_config is None:
        config_locations = [
            Path("config/framework.yaml"),
            Path("framework.yaml"),
            Path.home() / ".spark-validator" / "config.yaml",
        ]

        for location in config_locations:
            if location.exists():
                _default_config = load_config(location)
                break
        else:
            # Even if file not found, env overrides still apply via load_config(config_dict={})
            _default_config = load_config(config_dict={})

    return _default_config


def set_config(config: FrameworkConfig) -> None:
    """Set the global configuration instance."""
    global _default_config
    _default_config = config
