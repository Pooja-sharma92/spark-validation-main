# ====== src/validator/stages/golden_compare_stage.py ======

"""
Golden Compare Stage (Approach-1 / A2)

Compares:
  - Golden CSV (test_assets/golden/data/<job>.csv)
  - Iceberg output table written by the job (detected from spark.sql writes)

Outputs:
  - A CSV report containing mismatch summary + sample mismatches

Row-level comparison supports:
  - KEY-based compare (recommended): full outer join on key columns
  - HASH-based compare (fallback): compare row-hash multiset if keys not available
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .base import ValidationStage, StageResult, Severity
from ..pipeline.context import ValidationContext


def _job_name_from_path(file_path: str) -> str:
    return Path(file_path).name.replace(".py", "")


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _read_golden_csv(spark, csv_path: Path, header: bool = True) -> DataFrame:
    return (
        spark.read
        .option("header", "true" if header else "false")
        .option("inferSchema", "true")
        .csv(str(csv_path))
    )


def _nullsafe_eq(a: F.Column, b: F.Column) -> F.Column:
    # True when both null or equal
    return (a.eqNullSafe(b))


def _hash_row(cols: List[F.Column]) -> F.Column:
    # stable row hash
    return F.sha2(F.concat_ws("||", *[F.coalesce(c.cast("string"), F.lit("∅")) for c in cols]), 256)


class GoldenCompareStage(ValidationStage):
    name = "golden_compare"
    requires_spark = True
    blocking = False  # you can set blocking true in framework.yaml if you want

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)

        self.golden_base_dir = self.config.get("golden_base_dir", "test_assets/golden/data")
        self.report_base_dir = self.config.get("report_base_dir", "reports/golden_compare")
        self.target_table = self.config.get("target_table")  # optional override
        self.keys: List[str] = self.config.get("keys", []) or []
        self.ignore_columns: List[str] = self.config.get("ignore_columns", []) or []
        self.sample_limit = int(self.config.get("sample_limit", 50))

    def validate(self, context: ValidationContext) -> StageResult:
        start = time.time()

        if context.dry_run:
            return self.create_result(
                passed=True,
                issues=[],
                duration=time.time() - start,
                details={"skipped": True, "reason": "dry_run mode"},
            )

        if not context.spark:
            return self.create_result(
                passed=False,
                issues=[self.create_issue(Severity.ERROR, "SparkSession not available for golden_compare")],
                duration=time.time() - start,
            )

        spark = context.spark
        job_name = _job_name_from_path(context.file_path)

        # ---- locate golden csv ----
        root = Path(".").resolve()
        golden_path = (root / self.golden_base_dir / f"{job_name}.csv")
        if not golden_path.exists():
            return self.create_result(
                passed=False,
                issues=[self.create_issue(Severity.ERROR, f"Golden CSV not found: {golden_path}")],
                duration=time.time() - start,
                details={"job_name": job_name, "golden_csv": str(golden_path)},
            )

        golden_df = _read_golden_csv(spark, golden_path)

        # ---- determine iceberg output table ----
        target = self.target_table
        exec_out = context.shared.get("execution_output")
        sql_targets = []
        if exec_out and getattr(exec_out, "result_info", None):
            sql_targets = (exec_out.result_info or {}).get("sql_write_targets", []) or []

        if not target:
            # If only one write target, use it; else pick the last one
            if sql_targets:
                target = sql_targets[-1]

        if not target:
            return self.create_result(
                passed=False,
                issues=[self.create_issue(
                    Severity.ERROR,
                    "No target_table configured and execution did not detect any spark.sql write targets."
                )],
                duration=time.time() - start,
                details={"detected_sql_write_targets": sql_targets},
            )

        # read iceberg table
        try:
            out_df = spark.table(target)
        except Exception as e:
            return self.create_result(
                passed=False,
                issues=[self.create_issue(Severity.ERROR, f"Cannot read output table '{target}': {e}")],
                duration=time.time() - start,
                details={"target_table": target},
            )

        # ---- align columns ----
        gcols = [c for c in golden_df.columns if c not in self.ignore_columns]
        ocols = [c for c in out_df.columns if c not in self.ignore_columns]

        common = [c for c in gcols if c in ocols]
        missing_in_output = [c for c in gcols if c not in ocols]
        extra_in_output = [c for c in ocols if c not in gcols]

        issues = []
        if missing_in_output:
            issues.append(self.create_issue(
                Severity.WARNING,
                f"Columns missing in output table vs golden: {missing_in_output}",
                rule="schema_mismatch",
            ))
        if extra_in_output:
            issues.append(self.create_issue(
                Severity.WARNING,
                f"Extra columns in output table vs golden (ignored for compare): {extra_in_output}",
                rule="schema_mismatch",
            ))

        if not common:
            issues.append(self.create_issue(
                Severity.ERROR,
                "No common columns between golden CSV and output table after ignore_columns filter.",
                rule="schema_mismatch",
            ))
            return self.create_result(
                passed=False,
                issues=issues,
                duration=time.time() - start,
                details={"golden_cols": gcols, "output_cols": ocols},
            )

        # project common columns only
        g = golden_df.select([F.col(c).alias(c) for c in common])
        o = out_df.select([F.col(c).alias(c) for c in common])

        # ---- compare ----
        if self.keys:
            passed, mismatch_count, report_path = self._compare_with_keys(
                job_name=job_name,
                g=g,
                o=o,
                common_cols=common,
                keys=self.keys,
                root=root,
            )
        else:
            passed, mismatch_count, report_path = self._compare_with_hash(
                job_name=job_name,
                g=g,
                o=o,
                common_cols=common,
                root=root,
            )

        if not passed:
            issues.append(self.create_issue(
                Severity.ERROR,
                f"Golden compare failed: {mismatch_count} mismatches. Report: {report_path}",
                rule="golden_compare",
            ))

        return self.create_result(
            passed=passed,
            issues=issues,
            duration=time.time() - start,
            details={
                "job_name": job_name,
                "golden_csv": str(golden_path),
                "target_table": target,
                "detected_sql_write_targets": sql_targets,
                "compare_mode": "keys" if self.keys else "hash",
                "mismatches": mismatch_count,
                "report_csv": str(report_path),
            },
        )

    def _compare_with_keys(
        self,
        job_name: str,
        g: DataFrame,
        o: DataFrame,
        common_cols: List[str],
        keys: List[str],
        root: Path,
    ) -> tuple[bool, int, Path]:
        # Validate keys exist
        for k in keys:
            if k not in common_cols:
                # key missing means we cannot do key-join compare
                report_dir = root / self.report_base_dir / job_name
                _ensure_dir(report_dir)
                report_path = report_dir / "golden_compare_report.csv"
                # write small report
                spark = g.sparkSession
                spark.createDataFrame(
                    [{"error": f"Key column '{k}' not present in both golden and output common columns"}]
                ).coalesce(1).write.mode("overwrite").option("header", "true").csv(str(report_path.parent / "report_tmp"))
                return False, 0, report_path

        # Deduplicate by keys (best-effort)
        gk = g.dropDuplicates(keys)
        ok = o.dropDuplicates(keys)

        # Full outer join on keys
        join_cond = [gk[k] == ok[k] for k in keys]
        joined = gk.alias("g").join(ok.alias("o"), on=join_cond, how="full")

        # detect missing rows
        missing_in_output = joined.filter(F.col("o." + keys[0]).isNull()).select(
            *[F.col("g." + k).alias(k) for k in keys]
        ).withColumn("mismatch_type", F.lit("MISSING_IN_OUTPUT"))

        missing_in_golden = joined.filter(F.col("g." + keys[0]).isNull()).select(
            *[F.col("o." + k).alias(k) for k in keys]
        ).withColumn("mismatch_type", F.lit("EXTRA_IN_OUTPUT"))

        # value mismatches
        non_key_cols = [c for c in common_cols if c not in keys]
        diff_exprs = []
        for c in non_key_cols:
            diff_exprs.append(~_nullsafe_eq(F.col(f"g.{c}"), F.col(f"o.{c}")))

        any_diff = diff_exprs[0]
        for e in diff_exprs[1:]:
            any_diff = any_diff | e

        value_diff = joined.filter(
            F.col("g." + keys[0]).isNotNull() & F.col("o." + keys[0]).isNotNull() & any_diff
        ).select(
            *[F.col(f"g.{k}").alias(k) for k in keys],
            F.lit("VALUE_MISMATCH").alias("mismatch_type"),
            *[
                F.col(f"g.{c}").cast("string").alias(f"golden__{c}") for c in non_key_cols
            ],
            *[
                F.col(f"o.{c}").cast("string").alias(f"output__{c}") for c in non_key_cols
            ],
        )

        mismatches = missing_in_output.unionByName(missing_in_golden, allowMissingColumns=True).unionByName(value_diff, allowMissingColumns=True)
        mismatch_count = mismatches.count()

        report_dir = root / self.report_base_dir / job_name
        _ensure_dir(report_dir)
        report_path = report_dir / "golden_compare_report.csv"

        # write sample mismatches only (keep it light)
        mismatches.limit(self.sample_limit).coalesce(1).write.mode("overwrite").option("header", "true").csv(str(report_dir / "golden_compare_report_tmp"))

        # also write summary file
        summary = [
            {"metric": "mismatch_count", "value": mismatch_count},
            {"metric": "sample_limit", "value": self.sample_limit},
        ]
        mismatches.sparkSession.createDataFrame(summary).coalesce(1).write.mode("overwrite").option("header", "true").csv(str(report_dir / "golden_compare_summary_tmp"))

        # Return logical path (folder-based CSV output)
        return (mismatch_count == 0), mismatch_count, report_path

    def _compare_with_hash(
        self,
        job_name: str,
        g: DataFrame,
        o: DataFrame,
        common_cols: List[str],
        root: Path,
    ) -> tuple[bool, int, Path]:
        # Hash each row; compare multiset counts
        g_h = g.withColumn("__row_hash", _hash_row([F.col(c) for c in common_cols])).groupBy("__row_hash").count().withColumnRenamed("count", "golden_count")
        o_h = o.withColumn("__row_hash", _hash_row([F.col(c) for c in common_cols])).groupBy("__row_hash").count().withColumnRenamed("count", "output_count")

        comp = g_h.join(o_h, on="__row_hash", how="full").na.fill(0)
        diff = comp.filter(F.col("golden_count") != F.col("output_count"))
        mismatch_count = diff.count()

        report_dir = root / self.report_base_dir / job_name
        _ensure_dir(report_dir)
        report_path = report_dir / "golden_compare_report.csv"

        diff.limit(self.sample_limit).coalesce(1).write.mode("overwrite").option("header", "true").csv(str(report_dir / "golden_compare_report_tmp"))

        summary = [
            {"metric": "mismatch_hashes", "value": mismatch_count},
            {"metric": "sample_limit", "value": self.sample_limit},
        ]
        diff.sparkSession.createDataFrame(summary).coalesce(1).write.mode("overwrite").option("header", "true").csv(str(report_dir / "golden_compare_summary_tmp"))

        return (mismatch_count == 0), mismatch_count, report_path
