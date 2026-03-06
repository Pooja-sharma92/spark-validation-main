"""Spark Enhanced Validation stage.

This stage adds "bank-grade" Spark validation checks on top of the existing
pipeline, without changing the overall architecture.

It targets the Spark/runtime-oriented part of the 6 pillars:
2) Schema drift detection
3) Data volume & distribution validation
4) Execution plan validation
5) Cross-engine consistency (via lightweight expectations)
6) Confidence score

Notes
-----
Some of these validations require domain-specific expectations (e.g., what the
expected schema or counts should be). To keep the framework reusable, this stage
will:
- Use expectations if present in the job payload metadata
- Otherwise run best-effort checks and produce warnings (not hard failures)

Expected metadata keys (optional)
--------------------------------
metadata:
  expected_schemas:
    table_name: [col1, col2, ...]
  input_tables: [table1, table2]
  table_keys:
    table_name: [primary_key_col]
  expected_counts:
    table_name: 12345

Stage configuration (framework.yaml)
-----------------------------------
validation:
  stages:
    - name: spark_enhanced
      enabled: true
      blocking: false
      sample_rows: 1000
      allow_full_count: false
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

from .base import ValidationStage, StageResult, ValidationIssue, Severity
from ..pipeline.context import ValidationContext


AGG_FUNCS = ("sum", "count", "avg", "min", "max")


def _extract_spark_sql_queries(code: str) -> List[str]:
    """Extract spark.sql(...) query strings (best-effort)."""
    # NOTE: Use single-quoted Python strings here, because the regex itself
    # contains triple-double-quotes.
    triple = re.findall(r'spark\.sql\(\s*"""([\s\S]*?)"""\s*\)', code, flags=re.IGNORECASE)
    single = re.findall(r"spark\.sql\(\s*'([^']*?)'\s*\)", code, flags=re.IGNORECASE)
    double = re.findall(r'spark\.sql\(\s*"([^"]*?)"\s*\)', code, flags=re.IGNORECASE)
    return [q.strip() for q in (triple + single + double) if q and q.strip()]


def _extract_from_tables(sql: str) -> Set[str]:
    """Extract table identifiers from FROM/JOIN clauses (best-effort)."""
    s = " ".join(sql.split())
    tables = set()
    for m in re.finditer(r"\bfrom\s+([a-zA-Z0-9_\.]+)", s, flags=re.IGNORECASE):
        tables.add(m.group(1))
    for m in re.finditer(r"\bjoin\s+([a-zA-Z0-9_\.]+)", s, flags=re.IGNORECASE):
        tables.add(m.group(1))
    return tables


@dataclass
class ScoreBreakdown:
    schema: int = 0
    data: int = 0
    plan: int = 0
    consistency: int = 0

    def total_penalty(self) -> int:
        return self.schema + self.data + self.plan + self.consistency


class SparkEnhancedStage(ValidationStage):
    """Run Spark-based validation checks before/around execution."""

    name = "spark_enhanced"
    requires_spark = True
    blocking = False

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.sample_rows = int(self.config.get("sample_rows", 1000))
        self.allow_full_count = bool(self.config.get("allow_full_count", False))

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
                issues=[self.create_issue(Severity.ERROR, "SparkSession not available")],
                duration=time.time() - start,
            )

        issues: List[ValidationIssue] = []
        details: Dict[str, Any] = {}
        score = 100
        breakdown = ScoreBreakdown()

        # Resolve tables to validate
        tables = self._resolve_tables(context)
        details["tables"] = sorted(tables)

        # 2) Schema drift detection
        schema_issues, schema_details, schema_penalty = self._schema_drift_checks(context, tables)
        issues.extend(schema_issues)
        details.update(schema_details)
        breakdown.schema += schema_penalty

        # 3) Data volume & distribution
        data_issues, data_details, data_penalty = self._data_profile_checks(context, tables)
        issues.extend(data_issues)
        details.update(data_details)
        breakdown.data += data_penalty

        # 4) Execution plan validation (best-effort via spark.sql queries)
        plan_issues, plan_details, plan_penalty = self._plan_checks(context)
        issues.extend(plan_issues)
        details.update(plan_details)
        breakdown.plan += plan_penalty

        # 5) Cross-engine consistency (via expectations)
        cons_issues, cons_details, cons_penalty = self._consistency_checks(context, tables)
        issues.extend(cons_issues)
        details.update(cons_details)
        breakdown.consistency += cons_penalty

        score = max(0, score - breakdown.total_penalty())
        details["confidence_score"] = score
        details["confidence_breakdown"] = {
            "schema_penalty": breakdown.schema,
            "data_penalty": breakdown.data,
            "plan_penalty": breakdown.plan,
            "consistency_penalty": breakdown.consistency,
        }

        # Blocking decision: fail only if we have ERRORs
        error_count = len([i for i in issues if i.severity == Severity.ERROR])
        passed = (error_count == 0)

        # Store score for UI/other stages
        context.set_shared("confidence_score", score)

        return self.create_result(
            passed=passed,
            issues=issues,
            duration=time.time() - start,
            details=details,
        )

    def _resolve_tables(self, context: ValidationContext) -> Set[str]:
        md = context.metadata or {}
        tables: Set[str] = set(md.get("input_tables") or [])

        # If not provided, infer from SQL strings in the code
        if not tables:
            for q in _extract_spark_sql_queries(context.code):
                tables |= _extract_from_tables(q)

        # As a final fallback, keep empty set (stage will become mostly no-op)
        return {t for t in tables if isinstance(t, str) and t.strip()}

    def _schema_drift_checks(self, context: ValidationContext, tables: Set[str]) -> Tuple[List[ValidationIssue], Dict[str, Any], int]:
        md = context.metadata or {}
        expected_schemas: Dict[str, List[str]] = md.get("expected_schemas") or {}

        issues: List[ValidationIssue] = []
        details: Dict[str, Any] = {"schema_drift": {}}
        penalty = 0

        if not tables:
            issues.append(self.create_issue(
                Severity.WARNING,
                "No input tables detected for schema drift validation",
                suggestion="Provide metadata.input_tables and metadata.expected_schemas for stronger checks.",
                rule="schema_drift_no_tables",
            ))
            return issues, details, 2

        for t in sorted(tables):
            try:
                cols = list(context.spark.table(t).columns)
            except Exception as e:
                issues.append(self.create_issue(
                    Severity.ERROR,
                    f"Input table not accessible in Spark: {t} ({e})",
                    suggestion="Ensure the table exists in the target catalog or provide correct table identifier.",
                    rule="schema_drift_table_missing",
                ))
                details["schema_drift"][t] = {"error": str(e)}
                penalty += 15
                continue

            expected = expected_schemas.get(t)
            if expected:
                actual_set = set(cols)
                expected_set = set(expected)
                missing = sorted(expected_set - actual_set)
                extra = sorted(actual_set - expected_set)
                if missing or extra:
                    issues.append(self.create_issue(
                        Severity.ERROR,
                        f"Schema drift detected for {t}: missing={len(missing)}, extra={len(extra)}",
                        suggestion="Update expectations or fix upstream schema changes before running job.",
                        rule="schema_drift_detected",
                    ))
                    penalty += 15
                details["schema_drift"][t] = {
                    "expected_columns": expected,
                    "actual_columns": cols,
                    "missing": missing,
                    "extra": extra,
                }
            else:
                # Expectations not provided -> informational schema snapshot
                details["schema_drift"][t] = {"actual_columns": cols, "expected_columns": None}
                penalty += 0

        return issues, details, penalty

    def _data_profile_checks(self, context: ValidationContext, tables: Set[str]) -> Tuple[List[ValidationIssue], Dict[str, Any], int]:
        md = context.metadata or {}
        table_keys: Dict[str, List[str]] = md.get("table_keys") or {}

        issues: List[ValidationIssue] = []
        details: Dict[str, Any] = {"data_profile": {}}
        penalty = 0

        if not tables:
            return issues, details, 0

        for t in sorted(tables):
            try:
                df = context.spark.table(t)
            except Exception as e:
                # Missing already handled in schema check; avoid duplicates
                details["data_profile"][t] = {"error": str(e)}
                continue

            # Lightweight sampling
            sampled = df.limit(self.sample_rows)
            sample_count = sampled.count()
            details["data_profile"][t] = {"sample_rows": sample_count}

            # Optional full count (can be expensive)
            if self.allow_full_count:
                try:
                    full_count = df.count()
                    details["data_profile"][t]["row_count"] = full_count
                except Exception as e:
                    details["data_profile"][t]["row_count_error"] = str(e)

            # Key distribution (sample-based)
            keys = table_keys.get(t) or []
            if keys:
                try:
                    from pyspark.sql.functions import col, count as f_count
                    key_col = keys[0]
                    top = (
                        sampled.groupBy(col(key_col))
                        .agg(f_count("*").alias("cnt"))
                        .orderBy(col("cnt").desc())
                        .limit(5)
                        .collect()
                    )
                    top_list = [{key_col: r[0], "cnt": int(r[1])} for r in top]
                    details["data_profile"][t]["top_key_counts"] = top_list

                    # Simple skew heuristic
                    if top_list and sample_count > 0:
                        max_cnt = max(x["cnt"] for x in top_list)
                        if max_cnt / max(1, sample_count) > 0.5:
                            issues.append(self.create_issue(
                                Severity.WARNING,
                                f"Possible skew detected in {t} on key '{key_col}' (top value dominates sample)",
                                suggestion="Consider salting, repartitioning, or broadcast strategy adjustments.",
                                rule="data_skew_warning",
                            ))
                            penalty += 5
                except Exception as e:
                    details["data_profile"][t]["key_profile_error"] = str(e)

            # Null ratio (sample-based, for first few columns)
            try:
                from pyspark.sql.functions import when, sum as f_sum
                cols = df.columns[:10]
                nulls = []
                for c in cols:
                    n = sampled.select(f_sum(when(col(c).isNull(), 1).otherwise(0)).alias("n")).collect()[0][0]
                    nulls.append({"column": c, "nulls": int(n) if n is not None else 0})
                details["data_profile"][t]["nulls_sample"] = nulls
            except Exception:
                # non-fatal
                pass

            # Basic volume anomaly heuristic
            if sample_count == 0:
                issues.append(self.create_issue(
                    Severity.WARNING,
                    f"No rows found in sample for table {t}",
                    suggestion="Confirm upstream landing/staging data availability for this job run.",
                    rule="data_empty_sample",
                ))
                penalty += 3

        return issues, details, penalty

    def _plan_checks(self, context: ValidationContext) -> Tuple[List[ValidationIssue], Dict[str, Any], int]:
        issues: List[ValidationIssue] = []
        details: Dict[str, Any] = {"plan_checks": {}}
        penalty = 0

        queries = _extract_spark_sql_queries(context.code)
        if not queries:
            return issues, details, 0

        for idx, q in enumerate(queries[:5], 1):  # cap for safety
            try:
                df = context.spark.sql(q).limit(0)
                plan = df._jdf.queryExecution().executedPlan().toString()
                details["plan_checks"][f"query_{idx}"] = {
                    "tables": sorted(_extract_from_tables(q)),
                    "plan_snippet": plan[:4000],
                }

                plan_low = plan.lower()
                if "sortmergejoin" in plan_low and "exchange" in plan_low:
                    issues.append(self.create_issue(
                        Severity.WARNING,
                        "Execution plan indicates potential heavy shuffle (SortMergeJoin + Exchange)",
                        suggestion="Consider broadcast joins for small dimensions, enable AQE, or review join keys/partitioning.",
                        rule="plan_shuffle_warning",
                    ))
                    penalty += 5

                if "broadcast" in plan_low:
                    # no penalty; informational
                    pass

            except Exception as e:
                details["plan_checks"][f"query_{idx}"] = {"error": str(e)}
                issues.append(self.create_issue(
                    Severity.INFO,
                    f"Plan check skipped for one query (could not compile in current environment): {e}",
                    suggestion="Provide test tables/testdata in dev to enable explain-plan checks.",
                    rule="plan_check_skipped",
                ))

        return issues, details, penalty

    def _consistency_checks(self, context: ValidationContext, tables: Set[str]) -> Tuple[List[ValidationIssue], Dict[str, Any], int]:
        md = context.metadata or {}
        expected_counts: Dict[str, int] = md.get("expected_counts") or {}

        issues: List[ValidationIssue] = []
        details: Dict[str, Any] = {"consistency": {}}
        penalty = 0

        if not expected_counts:
            # Nothing to compare against
            return issues, details, 0

        if not self.allow_full_count:
            issues.append(self.create_issue(
                Severity.INFO,
                "Expected counts provided but full count is disabled (allow_full_count=false)",
                suggestion="Enable spark_enhanced.allow_full_count to validate expected row counts.",
                rule="consistency_counts_skipped",
            ))
            return issues, details, 0

        for t, exp in expected_counts.items():
            try:
                actual = context.spark.table(t).count()
                details["consistency"][t] = {"expected": int(exp), "actual": int(actual)}
                if int(actual) != int(exp):
                    issues.append(self.create_issue(
                        Severity.ERROR,
                        f"Row-count mismatch for {t}: expected={exp}, actual={actual}",
                        suggestion="Investigate upstream data snapshot differences or incorrect expectations.",
                        rule="consistency_row_count_mismatch",
                    ))
                    penalty += 15
            except Exception as e:
                details["consistency"][t] = {"error": str(e)}

        return issues, details, penalty
