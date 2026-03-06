"""
Validation stages package.

Stage registry + factory and building stages from framework.yaml.

Supports BOTH formats:

1) New (current):
pipeline:
  enabled_stages: [pre_execution, syntax, ..., execution, golden_compare]
  stage_settings:
    execution: {...}
    golden_compare: {...}

2) Legacy:
validation:
  stages:
    - name: "syntax"
      enabled: true
"""

from __future__ import annotations

from typing import List, Dict, Any, Optional, Type

from .base import ValidationStage, StageResult, ValidationIssue, Severity

from .pre_execution_stage import PreExecutionStage
from .syntax_stage import SyntaxStage
from .imports_stage import ImportsStage
from .sql_stage import SqlStage
from .logic_stage import LogicStage
from .spark_enhanced_stage import SparkEnhancedStage
from .execution_stage import ExecutionStage
from .golden_compare_stage import GoldenCompareStage

from .duckdb_ddl_validation_stage import DuckDbDdlValidationStage
from .golden_compare_stage import GoldenCompareStage


from .iceberg_result_stage import IcebergResultStage


# ----------------------------------------------------------------------
# Stage registry
# ----------------------------------------------------------------------

STAGE_REGISTRY: Dict[str, Type[ValidationStage]] = {
    "pre_execution": PreExecutionStage,
    "syntax": SyntaxStage,
    "imports": ImportsStage,
    "sql": SqlStage,
    "logic": LogicStage,
    "spark_enhanced": SparkEnhancedStage,
    "duckdb_ddl_validation": DuckDbDdlValidationStage,
    "execution": ExecutionStage,
    "golden_compare": GoldenCompareStage,

    # ✅ NEW
    "iceberg_result": IcebergResultStage,
}


# ----------------------------------------------------------------------
# Default stage order (fallback only)
# ----------------------------------------------------------------------

DEFAULT_STAGE_ORDER = [
    "pre_execution",
    "syntax",
    "imports",
    "sql",
    "logic",
    "duckdb_ddl_validation",
    "spark_enhanced",
    "execution",
]

DEFAULT_STAGES: List[ValidationStage] = []


# ----------------------------------------------------------------------
# Safe stage wrapper (never lets pipeline printing break)
# ----------------------------------------------------------------------

class _StageInitErrorStage(ValidationStage):
    stage = "stage_init_error"
    name = "stage_init_error"

    def __init__(self, stage_name: str, error: str):
        super().__init__(config={})
        self.stage = stage_name
        self.name = stage_name
        self._err = error

    def validate(self, context) -> StageResult:
        issue = ValidationIssue(
            stage=self.stage,
            severity=Severity.ERROR,
            message=f"Failed to initialize stage '{self.stage}': {self._err}",
            suggestion="Fix stage import/constructor/config. Ensure validator/stages/__init__.py is the one actually imported at runtime.",
        )
        return StageResult(stage=self.stage, passed=False, issues=[issue], duration_seconds=0.0)


def _clean_stage_name(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    return s


def get_stage(name: str, config: Optional[Dict[str, Any]] = None) -> ValidationStage:
    """
    Create a stage instance by name.
    All stages should accept (config=...).
    """
    nm = _clean_stage_name(name)
    if not nm:
        return _StageInitErrorStage(stage_name="unknown", error="blank stage name in config")

    if nm not in STAGE_REGISTRY:
        return _StageInitErrorStage(
            stage_name=nm,
            error=f"Unknown stage '{nm}'. Available: {sorted(list(STAGE_REGISTRY.keys()))}",
        )

    cls = STAGE_REGISTRY[nm]
    cfg = config if isinstance(config, dict) else {}

    try:
        st = cls(config=cfg)
        if not isinstance(st, ValidationStage):
            return _StageInitErrorStage(stage_name=nm, error=f"Factory returned non-ValidationStage: {type(st)}")
        # Ensure stage attribute is present (helps printing)
        if not getattr(st, "stage", None):
            try:
                st.stage = nm
            except Exception:
                pass
        return st
    except Exception as e:
        return _StageInitErrorStage(stage_name=nm, error=str(e))


def _get_pipeline_enabled_stages(cfg: Dict[str, Any]) -> Optional[List[str]]:
    pipeline = (cfg or {}).get("pipeline") or {}
    enabled = pipeline.get("enabled_stages")

    if isinstance(enabled, list) and enabled:
        out: List[str] = []
        for x in enabled:
            nm = _clean_stage_name(x)
            if nm:
                out.append(nm)
        return out if out else None

    return None


def _get_pipeline_stage_settings(cfg: Dict[str, Any]) -> Dict[str, Any]:
    pipeline = (cfg or {}).get("pipeline") or {}
    ss = pipeline.get("stage_settings") or {}
    return ss if isinstance(ss, dict) else {}


def get_stages_from_config(config: Dict[str, Any]) -> List[ValidationStage]:
    """
    Build stages from framework.yaml.

    Priority:
    1) pipeline.enabled_stages
    2) legacy validation.stages
    3) DEFAULT_STAGE_ORDER
    """

    # ---------------- NEW STYLE ----------------
    enabled = _get_pipeline_enabled_stages(config)
    if enabled:
        stage_settings = _get_pipeline_stage_settings(config)
        stages: List[ValidationStage] = []

        for nm in enabled:
            cfg = stage_settings.get(nm) if isinstance(stage_settings, dict) else {}
            st = get_stage(nm, config=cfg if isinstance(cfg, dict) else {})
            stages.append(st)

        # ✅ HARD GUARANTEE: only ValidationStage objects
        stages = [s for s in stages if isinstance(s, ValidationStage)]
        return stages

    # ---------------- LEGACY STYLE ----------------
    legacy = (config.get("validation") or {}).get("stages") or []
    if isinstance(legacy, list) and legacy:
        stages: List[ValidationStage] = []
        for stage_cfg in legacy:
            if not isinstance(stage_cfg, dict):
                continue
            nm = _clean_stage_name(stage_cfg.get("name"))
            if not nm:
                continue
            if not stage_cfg.get("enabled", True):
                continue
            st = get_stage(nm, config=stage_cfg)
            stages.append(st)

        stages = [s for s in stages if isinstance(s, ValidationStage)]
        return stages

    # ---------------- FALLBACK ----------------
    stages: List[ValidationStage] = []
    for nm in DEFAULT_STAGE_ORDER:
        stages.append(get_stage(nm, config={}))

    stages = [s for s in stages if isinstance(s, ValidationStage)]
    return stages
