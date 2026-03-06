# ====== src/validator/stages/duckdb_ddl_validation_stage.py ======

"""
DuckDB DDL/SQL Validation Stage (Design-time).

Goal:
- Validate DDL/SQL scripts without Spark.
- Uses DuckDB when available to attempt parsing/execution safely.
- Also supports lightweight rule-based checks (regex) even if DuckDB isn't installed.

This stage is meant to be SAFE and NON-BLOCKING by default.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .base import ValidationStage, StageResult, Severity
from ..pipeline.context import ValidationContext


@dataclass
class DdlValidationFinding:
    severity: str  # "ERROR" | "WARNING"
    message: str
    file: str
    statement_index: Optional[int] = None
    snippet: Optional[str] = None
    rule: Optional[str] = None


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")


def _split_sql_statements(sql: str) -> List[str]:
    """
    Simple statement splitter. Good enough for most DDL scripts.
    It won't be perfect for edge cases (procedures with semicolons).
    """
    # remove line comments
    sql = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)
    # remove block comments
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)

    parts = [p.strip() for p in sql.split(";")]
    return [p for p in parts if p]


def _apply_regex_rules(text: str, rules: List[Dict[str, Any]], file_label: str) -> List[DdlValidationFinding]:
    findings: List[DdlValidationFinding] = []
    for r in rules:
        pat = r.get("pattern")
        if not pat:
            continue
        sev = (r.get("severity") or "WARNING").upper()
        msg = r.get("message") or f"Matched pattern: {pat}"
        rule_name = r.get("name") or "regex_rule"
        try:
            if re.search(pat, text, flags=re.IGNORECASE | re.MULTILINE):
                findings.append(
                    DdlValidationFinding(
                        severity=sev,
                        message=msg,
                        file=file_label,
                        rule=rule_name,
                        snippet=None,
                    )
                )
        except re.error:
            findings.append(
                DdlValidationFinding(
                    severity="WARNING",
                    message=f"Invalid regex in rule '{rule_name}': {pat}",
                    file=file_label,
                    rule=rule_name,
                )
            )
    return findings


def _duckdb_validate_statements(sql_text: str, file_label: str) -> List[DdlValidationFinding]:
    """
    Attempts to parse/execute statements in DuckDB safely.
    Not all target-platform DDL will be supported by DuckDB—treat errors as
    findings but keep stage non-blocking by default.
    """
    findings: List[DdlValidationFinding] = []

    try:
        import duckdb  # type: ignore
    except Exception:
        findings.append(
            DdlValidationFinding(
                severity="WARNING",
                message="duckdb not installed - skipping DuckDB execution checks (regex rules still applied).",
                file=file_label,
                rule="duckdb_missing",
            )
        )
        return findings

    stmts = _split_sql_statements(sql_text)
    if not stmts:
        return findings

    con = duckdb.connect(database=":memory:")
    try:
        for idx, stmt in enumerate(stmts, start=1):
            # Run in a transaction and rollback to avoid side effects
            try:
                con.execute("BEGIN TRANSACTION;")
                con.execute(stmt)
                con.execute("ROLLBACK;")
            except Exception as e:
                # Rollback best-effort
                try:
                    con.execute("ROLLBACK;")
                except Exception:
                    pass

                snippet = stmt[:250].replace("\n", " ").strip()
                findings.append(
                    DdlValidationFinding(
                        severity="WARNING",
                        message=f"DuckDB could not parse/execute statement #{idx}: {e}",
                        file=file_label,
                        statement_index=idx,
                        snippet=snippet,
                        rule="duckdb_execute",
                    )
                )
    finally:
        try:
            con.close()
        except Exception:
            pass

    return findings


class DuckDbDdlValidationStage(ValidationStage):
    """
    Stage name: duckdb_ddl_validation

    Config example:
      - name: "duckdb_ddl_validation"
        enabled: true
        blocking: false
        ddl_paths:
          - "ddls/**/*.sql"
        standards_path: "ddl_conversion_standard.md"   # optional
        regex_rules:
          - name: "no_select_star"
            severity: "WARNING"
            pattern: "\\bselect\\s+\\*\\b"
            message: "Avoid SELECT * in migration scripts."
    """
    name = "duckdb_ddl_validation"
    requires_spark = False
    blocking = False

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self.ddl_paths: List[str] = self.config.get("ddl_paths", []) or []
        self.standards_path: Optional[str] = self.config.get("standards_path")
        self.regex_rules: List[Dict[str, Any]] = self.config.get("regex_rules", []) or []
        # If you want to treat DuckDB execution errors as ERRORs, flip this:
        self.duckdb_error_severity = (self.config.get("duckdb_error_severity") or "WARNING").upper()

    def validate(self, context: ValidationContext) -> StageResult:
        start = time.time()

        if context.dry_run:
            return self.create_result(
                passed=True,
                issues=[],
                duration=time.time() - start,
                details={"skipped": True, "reason": "dry_run mode"},
            )

        root = Path(context.config.get("jobs", {}).get("base_path", "."))  # safe fallback
        # In executor, we usually run from /app; prefer current working directory
        cwd_root = Path(".").resolve()
        if (cwd_root / "src").exists():
            root = cwd_root

        # Build rule text optionally from standards file
        standards_text = ""
        if self.standards_path:
            sp = (root / self.standards_path)
            if sp.exists():
                standards_text = _read_text(sp)

        findings: List[DdlValidationFinding] = []

        # Expand ddl_paths patterns
        ddl_files: List[Path] = []
        for pat in self.ddl_paths:
            ddl_files.extend(sorted(root.glob(pat)))

        # If no ddl_paths specified, stage does nothing (PASS)
        if not ddl_files and not standards_text and not self.regex_rules:
            return self.create_result(
                passed=True,
                issues=[],
                duration=time.time() - start,
                details={"note": "No ddl_paths/standards/rules configured"},
            )

        # Apply rules to standards too (optional)
        if standards_text and self.regex_rules:
            findings.extend(_apply_regex_rules(standards_text, self.regex_rules, file_label=self.standards_path or "standards"))

        # Validate each ddl file
        for f in ddl_files:
            label = str(f.relative_to(root)) if f.is_relative_to(root) else str(f)
            sql_text = _read_text(f)

            if self.regex_rules:
                findings.extend(_apply_regex_rules(sql_text, self.regex_rules, file_label=label))

            # DuckDB parse/execute check
            duckdb_findings = _duckdb_validate_statements(sql_text, file_label=label)
            # allow severity override
            for d in duckdb_findings:
                if d.rule == "duckdb_execute":
                    d.severity = self.duckdb_error_severity
            findings.extend(duckdb_findings)

        issues = []
        has_error = False
        for f in findings:
            sev = Severity.ERROR if f.severity == "ERROR" else Severity.WARNING
            if sev == Severity.ERROR:
                has_error = True
            msg = f.message
            if f.statement_index:
                msg += f" (statement #{f.statement_index})"
            if f.snippet:
                msg += f" | snippet: {f.snippet}"

            issues.append(self.create_issue(sev, msg, rule=f.rule or "duckdb_ddl_validation", file=f.file))

        passed = not has_error
        return self.create_result(
            passed=passed,
            issues=issues,
            duration=time.time() - start,
            details={
                "ddl_files_checked": len(ddl_files),
                "findings": len(findings),
                "note": "DuckDB execution issues are warnings by default; tune via duckdb_error_severity/blocking.",
            },
        )
