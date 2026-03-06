# ====== src/validator/pipeline/runner.py ======

"""
Validation pipeline runner.

Orchestrates the execution of validation stages in sequence.
"""

import traceback
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, TYPE_CHECKING
from dataclasses import dataclass, field

from ..stages.base import ValidationStage, StageResult

if TYPE_CHECKING:
    from .context import ValidationContext


@dataclass
class ValidationResult:
    """Complete result of a validation pipeline run."""
    job_path: str
    passed: bool
    stages: List[StageResult]
    started_at: str
    completed_at: str
    duration_seconds: float
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "job_path": self.job_path,
            "passed": self.passed,
            "stages": [s.to_dict() for s in self.stages],
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration_seconds": self.duration_seconds,
            "error": self.error,
            "metadata": self.metadata,
        }

    @property
    def stage_summary(self) -> str:
        parts = []
        for stage in self.stages:
            status = "✓" if stage.passed else "✗"
            if stage.skipped:
                status = "○"
            parts.append(f"{status}{stage.stage}")
        return " → ".join(parts)


class ValidationPipeline:
    def __init__(self, stages: List[ValidationStage], config: Optional[Dict[str, Any]] = None):
        self.stages = stages
        self.config = config or {}

    @staticmethod
    def _stage_label(stage_obj: Any) -> str:
        # Prefer stage.name, then stage.stage, else class name
        for attr in ("name", "stage"):
            try:
                v = getattr(stage_obj, attr, None)
                if isinstance(v, str) and v.strip():
                    return v.strip()
            except Exception:
                pass
        return stage_obj.__class__.__name__

    def run(self, context: "ValidationContext") -> ValidationResult:
        start_time = datetime.now(timezone.utc)
        stage_results: List[StageResult] = []
        overall_error: Optional[str] = None

        for stage in self.stages:
            label = self._stage_label(stage)
            try:
                result = stage.run_with_timing(context)

                # ✅ Force correct stage label even if a stage returns wrong/blank
                if not getattr(result, "stage", None):
                    result.stage = label

                stage_results.append(result)

                if not result.passed and getattr(stage, "blocking", False) and not result.skipped:
                    break

            except Exception as e:
                overall_error = f"Pipeline error in {label}: {e}"
                stage_results.append(
                    StageResult(
                        stage=label,
                        passed=False,
                        duration_seconds=0.0,
                        issues=[],
                        details={
                            "error": str(e),
                            "traceback": traceback.format_exc(),
                        },
                    )
                )
                break

        completed_at = datetime.now(timezone.utc)
        passed = all(s.passed for s in stage_results)

        return ValidationResult(
            job_path=context.file_path,
            passed=passed,
            stages=stage_results,
            started_at=start_time.isoformat(),
            completed_at=completed_at.isoformat(),
            duration_seconds=(completed_at - start_time).total_seconds(),
            error=overall_error,
            metadata=context.metadata,
        )

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "ValidationPipeline":
        from ..stages import get_stages_from_config
        stages = get_stages_from_config(config)
        return cls(stages=stages, config=config)

    @classmethod
    def default(cls) -> "ValidationPipeline":
        from ..stages import DEFAULT_STAGES
        return cls(stages=DEFAULT_STAGES.copy())
