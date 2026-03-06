from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Optional


# =============================================================================
# SAFE PART-3 GENERATED ASSET VALIDATION
# =============================================================================
def validate_generated_assets(
    project_root: str = ".",
    job_name: Optional[str] = None,
    job_filter: Optional[str] = None,
) -> Dict[str, Any]:
    """
    This function is imported by:

        validator.stages.pre_execution_stage

    IMPORTANT:
    - Must NEVER raise exception
    - Must ALWAYS return dict
    - Must NOT assume part-3 assets exist
    - Must work inside executor container

    This validation is informational only.
    """

    # ------------------------------------------------------------------
    # Backward compatibility:
    # pre_execution_stage calls with job_filter
    # ------------------------------------------------------------------
    if job_filter and not job_name:
        job_name = job_filter

    root = Path(project_root).resolve()

    report: Dict[str, Any] = {
        "project_root": str(root),
        "job": job_name,
        "status": "OK",
        "found": [],
        "missing": [],
        "notes": [],
    }

    # ------------------------------------------------------------------
    # Common folders expected in repo
    # ------------------------------------------------------------------
    common_paths = [
        root / "config",
        root / "jobs",
        root / "reports",
        root / "reports" / "outputs",
        root / "metadata",
        root / "metadata" / "out",
    ]

    for p in common_paths:
        if p.exists():
            report["found"].append(str(p))
        else:
            report["missing"].append(str(p))

    # ------------------------------------------------------------------
    # Job-specific checks (if job name provided)
    # ------------------------------------------------------------------
    if job_name:
        metadata_file = root / "metadata" / "out" / f"{job_name}_metadata.json"
        if metadata_file.exists():
            report["found"].append(str(metadata_file))
        else:
            report["missing"].append(str(metadata_file))

        # output capture folder
        output_dir = root / "reports" / "outputs" / job_name
        if output_dir.exists():
            report["found"].append(str(output_dir))
        else:
            report["missing"].append(str(output_dir))

        # optional DS export
        ds_xml = root / "ds_exports" / f"{job_name}.xml"
        if ds_xml.exists():
            report["found"].append(str(ds_xml))
        else:
            report["notes"].append("DataStage XML not present (optional)")

        # runtime overlay
        overlay_dir = root / "runtime_overlays" / job_name
        if overlay_dir.exists():
            report["found"].append(str(overlay_dir))
        else:
            report["notes"].append("Runtime overlay not present (optional)")

    # ------------------------------------------------------------------
    # Final status
    # ------------------------------------------------------------------
    if report["missing"]:
        report["status"] = "WARN"
        report["notes"].append(
            "Some generated assets are missing. "
            "This does NOT fail validation. "
            "Pre-execution checks will continue."
        )

    return report


# =============================================================================
# OPTIONAL RUNTIME TABLE VALIDATION (CLI ONLY)
# =============================================================================
def _table_exists(spark, table: str) -> bool:
    try:
        spark.table(table).limit(1).collect()
        return True
    except Exception:
        return False


def _resolve_table_name(spark, table_arg: str) -> str:
    t = table_arg.strip()

    if t.count(".") >= 2:
        if _table_exists(spark, t):
            return t
        raise ValueError(f"TABLE_NOT_FOUND: {t}")

    candidates: List[str] = []

    if t.count(".") == 1:
        candidates += [
            t,
            f"hive_metastore.{t}",
            f"spark_catalog.{t}",
        ]
    else:
        candidates += [
            t,
            f"default.{t}",
            f"hive_metastore.default.{t}",
            f"spark_catalog.default.{t}",
        ]

    for c in candidates:
        if _table_exists(spark, c):
            return c

    raise ValueError("TABLE_NOT_FOUND. Tried: " + ", ".join(candidates))


def validate_runtime_output(
    job_name: str,
    project_root: str,
    table_arg: str,
    business_date: Optional[str],
) -> Dict[str, Any]:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    try:
        table = _resolve_table_name(spark, table_arg)
    except Exception as e:
        return {"job": job_name, "runtime_status": "FAILED", "error": str(e)}

    df = spark.table(table)

    if business_date:
        for c in ["business_date", "businessdate", "dt", "logdate"]:
            if c.lower() in {x.lower() for x in df.columns}:
                df = df.where(f"to_date({c}) = to_date('{business_date}')")
                break

    try:
        cnt = df.count()
        return {
            "job": job_name,
            "runtime_status": "OK" if cnt > 0 else "EMPTY",
            "table": table,
            "rows": cnt,
        }
    except Exception as e:
        return {"job": job_name, "runtime_status": "FAILED", "error": str(e)}


# =============================================================================
# CLI
# =============================================================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_root", default=".")
    parser.add_argument("--job", default=None)
    parser.add_argument("--runtime", action="store_true")
    parser.add_argument("--table", default=None)
    parser.add_argument("--business_date", default=None)

    args = parser.parse_args()

    report = validate_generated_assets(args.project_root, args.job)

    print(json.dumps(report, indent=2))

    if args.runtime:
        if not args.job or not args.table:
            raise SystemExit("--runtime requires --job and --table")

        result = validate_runtime_output(
            job_name=args.job,
            project_root=args.project_root,
            table_arg=args.table,
            business_date=args.business_date,
        )

        print("\nRUNTIME RESULT")
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
