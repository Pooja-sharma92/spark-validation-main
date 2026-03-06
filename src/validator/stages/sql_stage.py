"""
SQL validation stage.

Extracts and validates SQL statements from spark.sql() calls.
"""

import re
import time
from typing import Dict, Any, List, Tuple

from .base import ValidationStage, StageResult, ValidationIssue, Severity
from ..pipeline.context import ValidationContext


class SqlStage(ValidationStage):
    """
    SQL extraction and validation.

    Extracts SQL from spark.sql() calls and validates:
    - SQL syntax (using Spark if available)
    - Common anti-patterns (SELECT *)
    - SQL best practices

    This stage can optionally use Spark for more accurate validation.
    """

    name = "sql"
    requires_spark = False  # Can run without Spark, but better with it
    blocking = True

    # Patterns to extract SQL from spark.sql() calls
    SQL_PATTERNS = [
        r'spark\.sql\s*\(\s*"""(.*?)"""',  # Triple double quotes
        r"spark\.sql\s*\(\s*'''(.*?)'''",  # Triple single quotes
        r'spark\.sql\s*\(\s*"([^"]*)"',    # Double quotes
        r"spark\.sql\s*\(\s*'([^']*)'",    # Single quotes
        r'spark\.sql\s*\(\s*f"""(.*?)"""', # f-string triple double
        r"spark\.sql\s*\(\s*f'''(.*?)'''", # f-string triple single
    ]

    def validate(self, context: ValidationContext) -> StageResult:
        """
        Extract and validate SQL statements.

        Args:
            context: ValidationContext with code and optional Spark session

        Returns:
            StageResult with any SQL issues found
        """
        start = time.time()
        issues = []
        sql_statements: List[Tuple[str, int]] = []  # (sql, approx_line)

        # Extract SQL statements
        for pattern in self.SQL_PATTERNS:
            for match in re.finditer(pattern, context.code, re.DOTALL):
                sql = match.group(1).strip()
                if not sql:
                    continue

                # Approximate line number
                line_num = context.code[:match.start()].count('\n') + 1
                sql_statements.append((sql, line_num))

        # Validate each SQL statement
        for sql, line_num in sql_statements:
            # Skip if contains Python format variables (can't validate)
            if "{" in sql and "}" in sql:
                continue

            sql_issues = self._validate_sql(sql, line_num, context)
            issues.extend(sql_issues)

        # Store SQL info in shared context
        context.set_shared("sql_statements", sql_statements)
        context.set_shared("sql_count", len(sql_statements))

        # Pass if no ERROR-level issues
        error_count = len([i for i in issues if i.severity == Severity.ERROR])

        return self.create_result(
            passed=error_count == 0,
            issues=issues,
            duration=time.time() - start,
            details={
                "sql_statements_found": len(sql_statements),
            },
        )

    def _validate_sql(
        self,
        sql: str,
        line_num: int,
        context: ValidationContext,
    ) -> List[ValidationIssue]:
        """
        Validate a single SQL statement.

        Args:
            sql: SQL string to validate
            line_num: Approximate line number in source
            context: ValidationContext

        Returns:
            List of issues found
        """
        issues = []
        sql_upper = sql.upper()

        # Check for SELECT *
        if "SELECT *" in sql_upper or "SELECT  *" in sql_upper:
            issues.append(self.create_issue(
                severity=Severity.WARNING,
                message="SELECT * used - consider specifying columns explicitly",
                line=line_num,
                suggestion="List specific columns for better performance and clarity",
                rule="select_star",
            ))

        # Check for missing WHERE in DELETE/UPDATE
        if ("DELETE FROM" in sql_upper or "UPDATE " in sql_upper) and "WHERE" not in sql_upper:
            issues.append(self.create_issue(
                severity=Severity.WARNING,
                message="DELETE/UPDATE without WHERE clause",
                line=line_num,
                suggestion="Add WHERE clause to prevent unintended data modification",
                rule="missing_where",
            ))

        # Use Spark SQL parser if available for syntax validation
        if context.spark:
            try:
                # EXPLAIN will parse the SQL without executing
                context.spark.sql(f"EXPLAIN {sql}")
            except Exception as e:
                error_msg = str(e)
                # Check if it's a parse error vs missing table (table not found is OK)
                if "ParseException" in error_msg or "mismatched input" in error_msg.lower():
                    issues.append(self.create_issue(
                        severity=Severity.ERROR,
                        message=f"SQL parse error: {error_msg[:200]}",
                        line=line_num,
                        rule="sql_syntax",
                    ))
                # Table/column not found errors are warnings (tables might exist at runtime)
                elif "Table or view not found" in error_msg or "cannot resolve" in error_msg.lower():
                    issues.append(self.create_issue(
                        severity=Severity.WARNING,
                        message=f"SQL reference warning: {error_msg[:200]}",
                        line=line_num,
                        suggestion="Verify table/column exists at runtime",
                        rule="sql_reference",
                    ))

        return issues
