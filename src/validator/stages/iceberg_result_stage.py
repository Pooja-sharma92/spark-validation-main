# ====== src/validator/stages/iceberg_result_stage.py ======
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    IntegerType,
    DoubleType,
)

from validator.stages.base import ValidationStage, StageResult, ValidationIssue, Severity


def _get_nested(cfg: dict, path: list, default=None):
    cur = cfg
    try:
        for k in path:
            cur = cur[k]
        return cur
    except Exception:
        return default


class IcebergResultStage(ValidationStage):
    """
    Writes one row per job-run into an Iceberg table.

    Fixes:
    - Uses explicit Spark schema (avoids CANNOT_DETERMINE_TYPE when values are None)
    - Uses stable Iceberg catalog/table creation and insert
    """

    stage = "iceberg_result"
    name = "iceberg_result"
    requires_spark = True
    blocking = False

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config=config or {})

    def _resolve_cfg(self, context) -> Dict[str, Any]:
        st_cfg = self.config if isinstance(self.config, dict) else {}
        pipe_cfg = _get_nested(context.config or {}, ["pipeline", "stage_settings", "iceberg_result"], {}) or {}
        out = {}
        out.update(st_cfg)
        out.update(pipe_cfg)
        return out

    def _safe_bool(self, v: Any, default: bool = False) -> bool:
        if v is None:
            return default
        if isinstance(v, bool):
            return v
        return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}

    def validate(self, context) -> StageResult:
        started = datetime.now(timezone.utc)

        spark = getattr(context, "spark", None)
        if spark is None:
            issue = ValidationIssue(
                stage=self.stage,
                severity=Severity.WARNING,
                message="Spark session not available; skipping Iceberg write.",
                suggestion="Run executor without --dry-run and ensure PySpark is available.",
            )
            return StageResult(stage=self.stage, passed=True, issues=[issue], duration_seconds=0.0)

        cfg = self._resolve_cfg(context)

        catalog = str(cfg.get("catalog") or "my_catalog").strip()
        database = str(cfg.get("database") or "validation").strip()
        table = str(cfg.get("table") or "job_runs").strip()

        enabled = self._safe_bool(cfg.get("enabled", True), True)
        if not enabled:
            issue = ValidationIssue(
                stage=self.stage,
                severity=Severity.INFO,
                message="Iceberg result stage disabled by config; skipping.",
                suggestion="Set pipeline.stage_settings.iceberg_result.enabled=true (or stage enabled).",
            )
            return StageResult(stage=self.stage, passed=True, issues=[issue], duration_seconds=0.0)

        full_db = f"{catalog}.{database}"
        full_table = f"{catalog}.{database}.{table}"

        issues = []
        passed = True

        # Explicit schema (IMPORTANT: avoids CANNOT_DETERMINE_TYPE)
        schema = StructType(
            [
                StructField("run_timestamp", StringType(), True),
                StructField("job_id", StringType(), True),
                StructField("job_path", StringType(), True),
                StructField("branch", StringType(), True),
                StructField("worker_id", StringType(), True),
                StructField("status", StringType(), True),
                StructField("passed", BooleanType(), True),
                StructField("execution_exit_code", IntegerType(), True),
                StructField("execution_duration_s", DoubleType(), True),
                StructField("output_path", StringType(), True),
                StructField("golden_path", StringType(), True),
                StructField("notes", StringType(), True),
            ]
        )

        try:
            # 1) Ensure namespace exists
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {full_db}")

            # 2) Ensure table exists
            spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {full_table} (
                    run_timestamp        STRING,
                    job_id               STRING,
                    job_path             STRING,
                    branch               STRING,
                    worker_id            STRING,
                    status               STRING,
                    passed               BOOLEAN,
                    execution_exit_code  INT,
                    execution_duration_s DOUBLE,
                    output_path          STRING,
                    golden_path          STRING,
                    notes                STRING
                )
                USING iceberg
                """
            )

            # 3) Collect values
            now_ts = datetime.now(timezone.utc).isoformat()

            meta = getattr(context, "metadata", {}) or {}
            job_id = str(meta.get("job_id") or meta.get("id") or getattr(context, "job_id", "") or "") or None
            job_path = str(getattr(context, "file_path", "") or "") or None
            branch = str(meta.get("branch") or "") or None
            worker_id = str(meta.get("worker_id") or "") or None

            pipeline_passed = True
            try:
                pipeline_passed = bool(context.get_shared("pipeline_passed"))
            except Exception:
                pass

            status = "PASSED" if pipeline_passed else "FAILED"

            exec_output = {}
            try:
                eo = context.get_shared("execution_output")
                if isinstance(eo, dict):
                    exec_output = eo
            except Exception:
                pass

            exit_code = exec_output.get("exit_code")
            dur_s = exec_output.get("duration_seconds")

            try:
                exit_code = int(exit_code) if exit_code is not None else None
            except Exception:
                exit_code = None

            try:
                dur_s = float(dur_s) if dur_s is not None else None
            except Exception:
                dur_s = None

            output_path = None
            try:
                output_path = str(context.get_shared("output_path") or "") or None
            except Exception:
                output_path = None

            golden_path = str(
                _get_nested(context.config or {}, ["pipeline", "stage_settings", "golden_compare", "golden_path"], "") or ""
            ) or None

            notes_obj = {
                "missing_paramset_key": str(context.get_shared("missing_paramset_key") or "")
                if hasattr(context, "get_shared")
                else ""
            }
            notes = json.dumps(notes_obj, ensure_ascii=False)[:2000] or None

            row = [
                (
                    now_ts,
                    job_id,
                    job_path,
                    branch,
                    worker_id,
                    status,
                    bool(pipeline_passed),
                    exit_code,
                    dur_s,
                    output_path,
                    golden_path,
                    notes,
                )
            ]

            df = spark.createDataFrame(row, schema=schema)

            # Insert (simple + compatible)
            df.createOrReplaceTempView("_tmp_job_run")
            spark.sql(
                f"""
                INSERT INTO {full_table}
                SELECT
                  run_timestamp, job_id, job_path, branch, worker_id,
                  status, passed, execution_exit_code, execution_duration_s,
                  output_path, golden_path, notes
                FROM _tmp_job_run
                """
            )

            issues.append(
                ValidationIssue(
                    stage=self.stage,
                    severity=Severity.INFO,
                    message=f"Wrote 1 job-run row into Iceberg table: {full_table}",
                    suggestion="Query the Iceberg table to verify persistence.",
                )
            )

        except Exception as e:
            passed = False
            issues.append(
                ValidationIssue(
                    stage=self.stage,
                    severity=Severity.ERROR,
                    message=f"Iceberg write failed: {e}",
                    suggestion=(
                        "Verify Iceberg packages are available in the Spark session used by executor. "
                        "Also ensure catalog/warehouse configs match (S3 endpoint + credentials)."
                    ),
                )
            )

        duration = (datetime.now(timezone.utc) - started).total_seconds()
        return StageResult(stage=self.stage, passed=passed, issues=issues, duration_seconds=duration)
