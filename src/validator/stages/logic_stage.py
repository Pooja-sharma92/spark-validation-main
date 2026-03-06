"""
Logic validation stage.

Detects anti-patterns and code quality issues in Spark jobs.
"""

import re
import time
from typing import Dict, Any, List, Callable, Optional

from .base import ValidationStage, StageResult, ValidationIssue, Severity
from ..pipeline.context import ValidationContext


class LogicStage(ValidationStage):
    """
    Anti-pattern and logic validation.

    Checks for common issues:
    - Bare except clauses
    - .collect() without .limit()
    - Hardcoded credentials
    - s3a:// protocol (forbidden per CLAUDE.md)
    - print() statements in production code

    Rules can be enabled/disabled via configuration.
    """

    name = "logic"
    requires_spark = False
    blocking = True

    # Default rules (can be overridden by config)
    DEFAULT_RULES = [
        "bare_except",
        "collect_without_limit",
        "hardcoded_credentials",
        "s3a_protocol",
        "print_statements",
        "sql_group_by_consistency",
    ]

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.enabled_rules = self.config.get("rules", self.DEFAULT_RULES)

    def validate(self, context: ValidationContext) -> StageResult:
        """
        Scan code for anti-patterns.

        Args:
            context: ValidationContext with code to analyze

        Returns:
            StageResult with any issues found
        """
        start = time.time()
        issues = []

        lines = context.code.split("\n")

        for i, line in enumerate(lines, 1):
            line_stripped = line.strip()

            # Skip comments
            if line_stripped.startswith("#"):
                continue

            # Run each enabled rule
            if "bare_except" in self.enabled_rules:
                issue = self._check_bare_except(line_stripped, i)
                if issue:
                    issues.append(issue)

            if "collect_without_limit" in self.enabled_rules:
                issue = self._check_collect_without_limit(line_stripped, i)
                if issue:
                    issues.append(issue)

            if "hardcoded_credentials" in self.enabled_rules:
                issue = self._check_hardcoded_credentials(line, line_stripped, i)
                if issue:
                    issues.append(issue)

            if "s3a_protocol" in self.enabled_rules:
                issue = self._check_s3a_protocol(line, i)
                if issue:
                    issues.append(issue)

            if "print_statements" in self.enabled_rules:
                issue = self._check_print_statements(line_stripped, i, context)
                if issue:
                    issues.append(issue)

            if "sql_group_by_consistency" in self.enabled_rules:
                issue = self._check_sql_group_by_consistency(context.code)
                if issue:
                    issues.append(issue)

        # Pass if no ERROR-level issues
        error_count = len([i for i in issues if i.severity == Severity.ERROR])

        return self.create_result(
            passed=error_count == 0,
            issues=issues,
            duration=time.time() - start,
            details={
                "lines_scanned": len(lines),
                "rules_applied": self.enabled_rules,
            },
        )

    def _check_bare_except(self, line: str, line_num: int) -> Optional[ValidationIssue]:
        """Check for bare except clauses."""
        if line == "except:" or line.startswith("except: "):
            return self.create_issue(
                severity=Severity.WARNING,
                message="Bare except clause catches all exceptions including KeyboardInterrupt",
                line=line_num,
                suggestion="Use 'except Exception:' or catch specific exceptions",
                rule="bare_except",
            )
        return None

    def _check_collect_without_limit(self, line: str, line_num: int) -> Optional[ValidationIssue]:
        """Check for .collect() without .limit()."""
        if ".collect()" in line and ".limit(" not in line and ".take(" not in line:
            return self.create_issue(
                severity=Severity.WARNING,
                message=".collect() without limit may cause OOM on large datasets",
                line=line_num,
                suggestion="Add .limit(N) before .collect() or use .take(N)",
                rule="collect_without_limit",
            )
        return None

    def _check_hardcoded_credentials(
        self,
        line: str,
        line_stripped: str,
        line_num: int,
    ) -> Optional[ValidationIssue]:
        """Check for hardcoded credentials."""
        cred_patterns = ["password=", "secret=", "api_key=", "token=", "apikey="]

        for pattern in cred_patterns:
            if pattern in line.lower():
                # Skip if it's a comment
                if line_stripped.startswith("#"):
                    continue

                # Check if it has a string value (not just a variable reference)
                if '"' in line or "'" in line:
                    # Skip if it's reading from environment
                    if "os.environ" in line or "os.getenv" in line:
                        continue

                    return self.create_issue(
                        severity=Severity.ERROR,
                        message=f"Possible hardcoded credential: {pattern}",
                        line=line_num,
                        suggestion="Use environment variables or secrets manager",
                        rule="hardcoded_credentials",
                    )
        return None

    def _check_s3a_protocol(self, line: str, line_num: int) -> Optional[ValidationIssue]:
        """Check for s3a:// protocol (forbidden per CLAUDE.md)."""
        if "s3a://" in line:
            return self.create_issue(
                severity=Severity.ERROR,
                message="s3a:// protocol used - this is not allowed",
                line=line_num,
                suggestion="Use s3:// instead of s3a://",
                rule="s3a_protocol",
            )
        return None

    def _check_print_statements(
        self,
        line: str,
        line_num: int,
        context: ValidationContext,
    ) -> Optional[ValidationIssue]:
        """Check for print() statements in production code."""
        # Skip if file appears to be a test or debug file
        if "debug" in context.file_path.lower() or "test" in context.file_path.lower():
            return None

        if line.startswith("print("):
            return self.create_issue(
                severity=Severity.INFO,
                message="print() statement found - consider using logging",
                line=line_num,
                suggestion="Use structlog or logging module instead",
                rule="print_statements",
            )
        return None

    # ---------------------------------------------------------------------
    # SQL logical validation: SELECT vs GROUP BY consistency (static)
    # ---------------------------------------------------------------------
    def _check_sql_group_by_consistency(self, code: str) -> Optional[ValidationIssue]:
        """Heuristic check for SQL queries where SELECT contains non-aggregated columns not present in GROUP BY.

        This catches a common logic issue early (pre-execution).
        It scans for spark.sql(<triple-quoted string>) / spark.sql('...') / spark.sql("...") patterns.
        """
        queries = self._extract_spark_sql_queries(code)
        for q in queries:
            issue = self._validate_group_by_in_query(q)
            if issue:
                return issue
        return None

    def _extract_spark_sql_queries(self, code: str) -> List[str]:
        """Extract SQL strings passed to spark.sql(...)."""
        triple = re.findall(r"spark\.sql\(\s*\"\"\"([\s\S]*?)\"\"\"\s*\)", code, flags=re.IGNORECASE)
        single = re.findall(r"spark\.sql\(\s*'([^']*?)'\s*\)", code, flags=re.IGNORECASE)
        double = re.findall(r'spark\.sql\(\s*"([^\"]*?)"\s*\)', code, flags=re.IGNORECASE)
        return [q.strip() for q in (triple + single + double) if q and q.strip()]

    def _validate_group_by_in_query(self, query: str) -> Optional[ValidationIssue]:
        q = " ".join(query.strip().split())  # normalize whitespace
        qlow = q.lower()
        if "select " not in qlow or " from " not in qlow or " group by " not in qlow:
            return None

        select_part = q[qlow.find("select ") + len("select ") : qlow.find(" from ")]
        group_part = q[qlow.find(" group by ") + len(" group by ") :]

        for stop_kw in [" having ", " order by ", " limit ", " qualify "]:
            idx = group_part.lower().find(stop_kw)
            if idx != -1:
                group_part = group_part[:idx]
                break

        select_cols = self._split_sql_list(select_part)
        group_cols = {self._normalize_sql_identifier(c) for c in self._split_sql_list(group_part)}

        agg_fns = (
            "sum",
            "count",
            "avg",
            "min",
            "max",
            "collect_list",
            "collect_set",
            "stddev",
            "variance",
        )

        non_agg = []
        for expr in select_cols:
            expr_low = expr.lower()
            if any(fn + "(" in expr_low for fn in agg_fns):
                continue
            if re.fullmatch(r"\d+(?:\.\d+)?", expr.strip()) or expr.strip().startswith("'") or expr.strip().startswith('"'):
                continue

            base = re.split(r"\s+as\s+", expr, flags=re.IGNORECASE)[0].strip()
            base_norm = self._normalize_sql_identifier(base)
            if base_norm and base_norm not in group_cols:
                non_agg.append(base_norm)

        if non_agg:
            return self.create_issue(
                severity=Severity.ERROR,
                message=(
                    "SQL GROUP BY mismatch: non-aggregated columns not in GROUP BY: "
                    + ", ".join(sorted(set(non_agg)))
                ),
                suggestion="Add the missing columns to GROUP BY or wrap them with an aggregate.",
                rule="sql_group_by_consistency",
            )

        return None

    def _split_sql_list(self, part: str) -> List[str]:
        """Split a comma-separated SQL list while respecting parentheses."""
        items: List[str] = []
        buf = []
        depth = 0
        for ch in part:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth = max(0, depth - 1)
            if ch == ',' and depth == 0:
                item = "".join(buf).strip()
                if item:
                    items.append(item)
                buf = []
            else:
                buf.append(ch)
        tail = "".join(buf).strip()
        if tail:
            items.append(tail)
        return items

    def _normalize_sql_identifier(self, expr: str) -> str:
        """Best-effort normalize SQL identifier."""
        e = expr.strip()
        e = e.strip('`')
        e = re.sub(r"\s+", " ", e)
        return e
