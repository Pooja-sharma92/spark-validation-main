"""
Validation execution context.

The ValidationContext holds all information needed during validation,
including file content, Spark session, configuration, and shared data
between stages.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, TYPE_CHECKING
from pathlib import Path

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@dataclass
class ValidationContext:
    """
    Shared context for all validation stages.
    """

    # File information
    file_path: str
    code: str = ""

    # Runtime resources
    spark: Optional["SparkSession"] = None

    # Configuration
    config: Dict[str, Any] = field(default_factory=dict)
    job_params: Dict[str, Any] = field(default_factory=dict)
    python_paths: List[str] = field(default_factory=list)

    # Job metadata
    metadata: Dict[str, Any] = field(default_factory=dict)

    # Inter-stage shared data
    shared: Dict[str, Any] = field(default_factory=dict)

    # Execution flags
    dry_run: bool = False

    def __post_init__(self):
        """Load file content if not provided."""
        if not self.code and self.file_path:
            try:
                with open(self.file_path, "r", encoding="utf-8", errors="ignore") as f:
                    self.code = f.read()
            except Exception:
                pass  # handled by validation stages

    @property
    def file_name(self) -> str:
        return Path(self.file_path).name

    @property
    def file_dir(self) -> Path:
        return Path(self.file_path).parent

    @property
    def branch(self) -> Optional[str]:
        return self.metadata.get("branch")

    @property
    def commit_sha(self) -> Optional[str]:
        return self.metadata.get("commit_sha")

    @property
    def trigger_source(self) -> Optional[str]:
        return self.metadata.get("trigger_source")

    def get_stage_config(self, stage_name: str) -> Dict[str, Any]:
        """
        Stage config lookup supporting BOTH styles:

        1) Legacy:
           validation.stages:
             - name: execution
               ...

        2) New:
           pipeline.stage_settings:
             execution:
               ...
        """
        cfg = self.config or {}

        # --- New style ---
        try:
            p = (cfg.get("pipeline") or {}).get("stage_settings") or {}
            if isinstance(p, dict) and stage_name in p and isinstance(p.get(stage_name), dict):
                return p.get(stage_name) or {}
        except Exception:
            pass

        # --- Legacy style ---
        try:
            stages_config = (cfg.get("validation") or {}).get("stages") or []
            for stage_cfg in stages_config:
                if isinstance(stage_cfg, dict) and stage_cfg.get("name") == stage_name:
                    return stage_cfg
        except Exception:
            pass

        return {}

    def set_shared(self, key: str, value: Any) -> None:
        self.shared[key] = value

    def get_shared(self, key: str, default: Any = None) -> Any:
        return self.shared.get(key, default)

    @classmethod
    def from_job(
        cls,
        file_path: str,
        spark: Optional["SparkSession"] = None,
        config: Optional[Dict[str, Any]] = None,
        job_params: Optional[Dict[str, Any]] = None,
        python_paths: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        dry_run: bool = False,
    ) -> "ValidationContext":
        return cls(
            file_path=file_path,
            spark=spark,
            config=config or {},
            job_params=job_params or {},
            python_paths=python_paths or [],
            metadata=metadata or {},
            dry_run=dry_run,
        )
