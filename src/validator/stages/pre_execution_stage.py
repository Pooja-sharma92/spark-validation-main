"""
Pre-Execution Validation stage (Part-3 integration).

This stage performs static, metadata-driven checks BEFORE any Spark execution:
- Resolve DS export XML path (or other metadata inputs)
- Generate metadata JSON + validation YAML
- Validate generated assets (YAML exists, schema record exists, etc.)

Designed to run without Databricks.

Option-1 DRY MODE behavior:
- In dry mode: DS export missing => WARNING + PASS (if fail_if_ds_export_missing=false)
- In full mode: DS export missing => ERROR + FAIL
"""

from __future__ import annotations

import shutil
import time
import csv
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from .base import ValidationStage, StageResult, ValidationIssue, Severity

# Part-3 core functions (copied under validator.integrations.part3)
from ..integrations.part3.uc_validation.uc_export_metadata import run_export_and_generate
from ..integrations.part3.uc_validation.validation_runner import validate_generated_assets


def _guess_repo_root(file_path: str) -> Path:
    """
    Best-effort guess of repo root by walking upwards until we find a folder that
    looks like the project root (contains 'src' or '.git').
    """
    p = Path(file_path).resolve()
    for parent in [p] + list(p.parents):
        if (parent / ".git").exists() or (parent / "src").exists():
            return parent
    return p.parent


def _find_ds_export_path(repo_root: Path, job_file_path: Path, explicit: Optional[str]) -> Optional[Path]:
    """
    Resolve DS export path from:
    1) explicit metadata['ds_export_path']
    2) repo conventions: ds_exports/<job_stem>.xml, inputs/xml/<job_stem>.xml
    """
    if explicit:
        p = Path(explicit)
        if p.exists():
            return p.resolve()

    stem = job_file_path.stem
    candidates = [
        repo_root / "ds_exports" / f"{stem}.xml",
        repo_root / "inputs" / "xml" / f"{stem}.xml",
        repo_root / "inputs" / "xml" / f"{stem}.XML",
        repo_root / "ds_exports" / f"{stem}.XML",
    ]
    for c in candidates:
        if c.exists():
            return c.resolve()
    return None


def _read_report_row(report_path: Path, job_filter: str) -> Tuple[Optional[Dict[str, Any]], int]:
    """
    Read validation_report.csv and return:
    - best-matching row for job_filter (or None)
    - total row count
    """
    if not report_path.exists():
        return None, 0

    rows = []
    with report_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)

    if not rows:
        return None, 0

    jf = job_filter.strip()

    # exact match
    for r in rows:
        if r.get("job_name") == jf:
            return r, len(rows)

    # case-insensitive exact match
    for r in rows:
        if (r.get("job_name") or "").lower() == jf.lower():
            return r, len(rows)

    # contains match
    for r in rows:
        if jf.lower() in (r.get("job_name") or "").lower():
            return r, len(rows)

    return None, len(rows)


def _get_bool(d: dict, key: str, default: bool = False) -> bool:
    try:
        v = d.get(key, default)
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            return v.strip().lower() in {"true", "1", "yes", "y"}
        return bool(v)
    except Exception:
        return default


class PreExecutionStage(ValidationStage):
    """
    Runs Part-3 validations as a single stage in Part-2 pipeline.

    Stage key: pre_execution
    UI label:  Pre-Execution Validation
    """
    stage = "pre_execution"
    name = "pre_execution"
    requires_spark = False
    blocking = True

    def validate(self, context) -> StageResult:
        start = time.time()

        # ------------------------------------------------------------
        # Stage config comes from pipeline.stage_settings.pre_execution
        # (because we updated validator/stages/__init__.py)
        # ------------------------------------------------------------
        stage_cfg = self.config or {}
        dry_mode = _get_bool(stage_cfg, "dry_mode", default=False)
        fail_if_ds_export_missing = _get_bool(stage_cfg, "fail_if_ds_export_missing", default=True)

        # Repo root resolution
        repo_root = Path(context.metadata.get("repo_root") or _guess_repo_root(context.file_path))
        job_file_path = Path(context.file_path).resolve()

        ds_export_path = _find_ds_export_path(
            repo_root=repo_root,
            job_file_path=job_file_path,
            explicit=context.metadata.get("ds_export_path"),
        )

        details: Dict[str, Any] = {
            "repo_root": str(repo_root),
            "job_file": str(job_file_path),
            "dry_mode": dry_mode,
            "fail_if_ds_export_missing": fail_if_ds_export_missing,
        }

        # ------------------------------------------------------------
        # ✅ Option-1 behavior:
        # DS export missing:
        # - dry_mode + fail_if_ds_export_missing=false -> WARN + PASS
        # - else -> ERROR + FAIL
        # ------------------------------------------------------------
        if not ds_export_path:
            msg = (
                "DS export XML not found for this job. Provide metadata['ds_export_path'] "
                "or place file under ds_exports/<job>.xml"
            )
            if dry_mode and (not fail_if_ds_export_missing):
                issue = ValidationIssue(
                    stage=self.name,
                    severity=Severity.WARNING,
                    message=msg,
                    suggestion="Dry mode: continuing without DS export. To enable full pre-execution, add ds_export_path or ds_exports/<job>.xml.",
                    rule="ds_export_missing",
                )
                return StageResult(
                    stage=self.name,
                    passed=True,
                    duration_seconds=time.time() - start,
                    issues=[issue],
                    details=details,
                )

            issue = ValidationIssue(
                stage=self.name,
                severity=Severity.ERROR,
                message=msg,
                suggestion="Add ds_export_path in job payload OR follow repo convention ds_exports/<job_stem>.xml",
                rule="ds_export_missing",
            )
            return StageResult(
                stage=self.name,
                passed=False,
                duration_seconds=time.time() - start,
                issues=[issue],
                details=details,
            )

        details["ds_export_path"] = str(ds_export_path)

        # ------------------------------------------------------------
        # Use container-safe temp dir (NOT under repo if repo might be readonly)
        # ------------------------------------------------------------
        base_tmp = Path("/tmp/spark-validator/.pre_execution/xml")
        try:
            base_tmp.mkdir(parents=True, exist_ok=True)
            # clear previous temp files
            for p in base_tmp.glob("*"):
                if p.is_file():
                    p.unlink()
            shutil.copy2(ds_export_path, base_tmp / ds_export_path.name)
        except Exception as e:
            issue = ValidationIssue(
                stage=self.name,
                severity=Severity.ERROR,
                message=f"Failed to prepare temp XML directory for Pre-Execution Validation: {e}",
                suggestion="Ensure executor workspace is writable and DS export path is accessible.",
                rule="pre_execution_prepare_failed",
            )
            details["tmp_xml_dir"] = str(base_tmp)
            return StageResult(
                stage=self.name,
                passed=False,
                duration_seconds=time.time() - start,
                issues=[issue],
                details=details,
            )

        # Part-3 expects xml_dir relative to project_root; if it is not relative, pass absolute safely.
        # Here we are using /tmp/... so it won't be under repo_root; pass absolute path to xml_dir.
        xml_dir = str(base_tmp)

        # Part-3 output dirs under repo_root (keep same convention)
        out_dir = stage_cfg.get("out_dir", "metadata/out")
        gen_dir = stage_cfg.get("gen_dir", "metadata/generated")

        details.update({
            "xml_dir": xml_dir,
            "out_dir": out_dir,
            "gen_dir": gen_dir,
        })

        issues = []

        # ------------------------------------------------------------
        # Generate metadata and YAML
        # ------------------------------------------------------------
        try:
            summary = run_export_and_generate(
                project_root=str(repo_root),
                xml_dir=xml_dir,
                out_dir=out_dir,
                gen_dir=gen_dir,
            )
            details["generated_count"] = len(summary.get("generated", [])) if isinstance(summary, dict) else None
        except Exception as e:
            issues.append(
                ValidationIssue(
                    stage=self.name,
                    severity=Severity.ERROR,
                    message=f"Metadata/YAML generation failed: {e}",
                    suggestion="Check DS export XML format and ensure required dependencies are available.",
                    rule="pre_execution_generate_failed",
                )
            )
            return StageResult(
                stage=self.name,
                passed=False,
                duration_seconds=time.time() - start,
                issues=issues,
                details=details,
            )

        # job_filter: config override OR ds export stem
        job_filter = stage_cfg.get("job_filter") or ds_export_path.stem
        details["job_filter"] = job_filter

        # ------------------------------------------------------------
        # Validate generated assets
        # ------------------------------------------------------------
        passed = True
        try:
            report = validate_generated_assets(str(repo_root), job_filter=job_filter)

            # ------------------------------------------------------------
            # validate_generated_assets returns dict (by design)
            # Store the report in details for UI/DB
            # ------------------------------------------------------------
            details["asset_validation_report"] = report

            # If an older version writes a CSV report path, we can still support it.
            # Otherwise, we treat dict status as informational.
            report_path_val = None
            if isinstance(report, dict):
                report_path_val = report.get("report_path")

            # ------------------------------------------------------------
            # CSV path branch (kept for backward compatibility)
            # ------------------------------------------------------------
            if report_path_val:
                details["report_path"] = str(report_path_val)

                row, total = _read_report_row(Path(report_path_val), job_filter)
                details["report_total_rows"] = total

                if not row:
                    issues.append(
                        ValidationIssue(
                            stage=self.name,
                            severity=Severity.WARNING,
                            message=f"Pre-Execution report generated but no matching row found for job '{job_filter}'.",
                            suggestion="Ensure job_filter matches DS job name OR check DS export filename conventions.",
                            rule="pre_execution_no_matching_row",
                        )
                    )
                    passed = True  # WARN only
                else:
                    details["job_name"] = row.get("job_name")
                    details["has_yaml"] = row.get("has_yaml")
                    details["has_output_file_template"] = row.get("has_output_file_template")
                    details["has_apt_schema_record"] = row.get("has_apt_schema_record")
                    details["status"] = row.get("status")

                    passed = (row.get("status") == "PASS")
                    if not passed:
                        issues.append(
                            ValidationIssue(
                                stage=self.name,
                                severity=Severity.ERROR,
                                message=f"Pre-Execution asset validation FAILED for job '{row.get('job_name')}'.",
                                suggestion="Fix missing YAML/asset issues shown in stage details and re-run.",
                                rule="pre_execution_assets_failed",
                            )
                        )

            # ------------------------------------------------------------
            # Dict-only branch (current validation_runner behavior)
            # ------------------------------------------------------------
            else:
                status = None
                missing = []
                notes = []

                if isinstance(report, dict):
                    status = report.get("status")
                    missing = report.get("missing") or []
                    notes = report.get("notes") or []

                details["asset_validation_status"] = status
                details["asset_validation_missing_count"] = len(missing)
                details["asset_validation_notes"] = notes

                # Your validation_runner returns status="WARN" if missing assets exist.
                # Treat WARN as warning-only (PASS), OK as PASS.
                if status == "WARN":
                    issues.append(
                        ValidationIssue(
                            stage=self.name,
                            severity=Severity.WARNING,
                            message="Pre-Execution asset validation reported missing items (WARN).",
                            suggestion="Review details.asset_validation_report['missing'] for missing paths.",
                            rule="pre_execution_assets_warn",
                        )
                    )
                    passed = True
                elif status in {"ERROR", "FAILED", "FAIL"}:
                    issues.append(
                        ValidationIssue(
                            stage=self.name,
                            severity=Severity.ERROR,
                            message="Pre-Execution asset validation reported failure.",
                            suggestion="Review details.asset_validation_report for error context.",
                            rule="pre_execution_assets_failed",
                        )
                    )
                    passed = False
                else:
                    passed = True

        except Exception as e:
            issues.append(
                ValidationIssue(
                    stage=self.name,
                    severity=Severity.ERROR,
                    message=f"Asset validation failed: {e}",
                    suggestion="Ensure metadata/out and metadata/generated exist and are readable.",
                    rule="pre_execution_validate_failed",
                )
            )
            passed = False

        # final pass/fail: pass only if no ERROR issues
        has_error = any(i.severity == Severity.ERROR for i in issues)
        return StageResult(
            stage=self.name,
            passed=bool(passed) and (not has_error),
            duration_seconds=time.time() - start,
            issues=issues,
            details=details,
        )
