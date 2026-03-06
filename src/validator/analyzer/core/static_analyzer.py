"""
Static Code Analyzer
Uses AST parsing and regex to detect syntax errors, code smells, and anti-patterns
"""

import ast
import re
from typing import List, Dict, Any, Tuple
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Issue:
    """Represents a code issue"""
    severity: str  # critical, warning, info
    category: str  # syntax, performance, quality, security
    line: int
    column: int
    message: str
    suggestion: str = ""
    code_snippet: str = ""


@dataclass
class AnalysisResult:
    """Container for analysis results"""
    file_path: str
    issues: List[Issue] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)
    summary: Dict[str, int] = field(default_factory=dict)

    def add_issue(self, issue: Issue):
        self.issues.append(issue)
        # Update summary
        severity = issue.severity
        self.summary[severity] = self.summary.get(severity, 0) + 1


class StaticAnalyzer:
    """Static code analysis for Python/PySpark code"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.spark_patterns = self._load_spark_patterns()

    def analyze_file(self, file_path: str) -> AnalysisResult:
        """Analyze a single Python file"""
        result = AnalysisResult(file_path=file_path)

        with open(file_path, 'r', encoding='utf-8') as f:
            code = f.read()
            lines = code.split('\n')

        # Run all analyzers
        result.issues.extend(self._check_syntax(code, file_path))
        result.issues.extend(self._check_imports(code, lines))
        result.issues.extend(self._check_spark_antipatterns(code, lines))
        result.issues.extend(self._check_sql_injection(code, lines))
        result.issues.extend(self._check_code_quality(code, lines))

        # Calculate metrics
        result.metrics = self._calculate_metrics(code, lines)

        # Update summary
        for issue in result.issues:
            severity = issue.severity
            result.summary[severity] = result.summary.get(severity, 0) + 1

        return result

    def _check_syntax(self, code: str, file_path: str) -> List[Issue]:
        """Check Python syntax using AST"""
        issues = []
        try:
            ast.parse(code)
        except SyntaxError as e:
            issues.append(Issue(
                severity="critical",
                category="syntax",
                line=e.lineno or 0,
                column=e.offset or 0,
                message=f"Syntax error: {e.msg}",
                suggestion="Fix the syntax error before running the code"
            ))
        return issues

    def _check_imports(self, code: str, lines: List[str]) -> List[Issue]:
        """Check import statements for issues"""
        issues = []

        # Check for wildcard imports
        for i, line in enumerate(lines, 1):
            if re.match(r'^\s*from\s+\S+\s+import\s+\*', line):
                issues.append(Issue(
                    severity="warning",
                    category="quality",
                    line=i,
                    column=0,
                    message="Wildcard import detected",
                    suggestion="Use explicit imports instead of 'import *'",
                    code_snippet=line.strip()
                ))

        # Check for duplicate imports
        import_lines = {}
        for i, line in enumerate(lines, 1):
            match = re.match(r'^\s*(from|import)\s+(\S+)', line)
            if match:
                module = match.group(2)
                if module in import_lines:
                    issues.append(Issue(
                        severity="info",
                        category="quality",
                        line=i,
                        column=0,
                        message=f"Duplicate import of '{module}' (first at line {import_lines[module]})",
                        suggestion="Remove duplicate imports",
                        code_snippet=line.strip()
                    ))
                else:
                    import_lines[module] = i

        return issues

    def _check_spark_antipatterns(self, code: str, lines: List[str]) -> List[Issue]:
        """Detect Spark-specific anti-patterns"""
        issues = []

        # Pattern 1: collect() on large datasets
        for i, line in enumerate(lines, 1):
            if re.search(r'\.collect\(\)', line) and not re.search(r'#.*collect', line):
                issues.append(Issue(
                    severity="warning",
                    category="performance",
                    line=i,
                    column=0,
                    message="collect() detected - may cause OOM on large datasets",
                    suggestion="Consider using take(n), show(), or write to storage instead",
                    code_snippet=line.strip()
                ))

        # Pattern 2: Repeated selectExpr with same columns
        selectexpr_pattern = re.compile(r'\.selectExpr\(')
        selectexpr_lines = [i for i, line in enumerate(lines, 1) if selectexpr_pattern.search(line)]
        if len(selectexpr_lines) > 5:
            issues.append(Issue(
                severity="warning",
                category="performance",
                line=selectexpr_lines[0],
                column=0,
                message=f"Excessive use of selectExpr ({len(selectexpr_lines)} times)",
                suggestion="Consider consolidating type casting operations",
                code_snippet=f"Found at lines: {selectexpr_lines[:5]}..."
            ))

        # Pattern 3: Missing cache/persist for reused DataFrames
        df_vars = set()
        df_usage = {}
        for i, line in enumerate(lines, 1):
            # Find DataFrame assignments
            match = re.match(r'\s*(\w+)\s*=.*(?:spark\.sql|spark\.read|\.join|\.select)', line)
            if match:
                var = match.group(1)
                df_vars.add(var)
                df_usage[var] = df_usage.get(var, [])

            # Find DataFrame usage
            for var in df_vars:
                if re.search(rf'\b{var}\b', line):
                    df_usage[var] = df_usage.get(var, []) + [i]

        # Check for DataFrames used multiple times without cache
        for var, usage_lines in df_usage.items():
            if len(usage_lines) >= 3:
                # Check if cache/persist is used
                cache_found = any(
                    re.search(rf'{var}\.(?:cache|persist)\(', lines[i - 1])
                    for i in usage_lines if i <= len(lines)
                )
                if not cache_found:
                    issues.append(Issue(
                        severity="warning",
                        category="performance",
                        line=usage_lines[0],
                        column=0,
                        message=f"DataFrame '{var}' used {len(usage_lines)} times without cache/persist",
                        suggestion=f"Add {var}.cache() before reuse to improve performance",
                        code_snippet=f"Used at lines: {usage_lines[:5]}"
                    ))

        # Pattern 4: SQL injection vulnerability
        for i, line in enumerate(lines, 1):
            if 'spark.sql' in line and 'f"""' in line or "f'''" in line or 'f"' in line:
                issues.append(Issue(
                    severity="warning",
                    category="security",
                    line=i,
                    column=0,
                    message="Potential SQL injection using f-string in spark.sql()",
                    suggestion="Use parameterized queries or validate input",
                    code_snippet=line.strip()
                ))

        return issues

    def _check_sql_injection(self, code: str, lines: List[str]) -> List[Issue]:
        """Check for SQL injection vulnerabilities and SQL syntax errors"""
        issues = []

        # Pattern: f-string in SQL
        sql_fstring_pattern = re.compile(
            r'(spark\.sql|\.sql)\s*\(\s*f["\']|sql\s*=\s*f["\']',
            re.IGNORECASE
        )

        # Track SQL statements for syntax checking
        in_sql = False
        sql_start_line = 0
        sql_lines = []

        for i, line in enumerate(lines, 1):
            # Detect SQL statement start
            if re.search(r'sql\s*=\s*f?["\']', line, re.IGNORECASE):
                in_sql = True
                sql_start_line = i
                sql_lines = [line]
            elif in_sql:
                sql_lines.append(line)
                # Detect SQL statement end (closing triple quote or quote)
                if '"""' in line or "'''" in line or (line.strip().endswith('"') and not line.strip().endswith('\\"')):
                    in_sql = False
                    # Check complete SQL statement
                    full_sql = ' '.join(sql_lines)

                    # CRITICAL CHECK 1: Incomplete WHERE clause
                    if re.search(r'WHERE\s+=\s+', full_sql, re.IGNORECASE):
                        issues.append(Issue(
                            severity="critical",
                            category="syntax",
                            line=sql_start_line,
                            column=0,
                            message="SQL syntax error: WHERE clause missing column name",
                            suggestion="Add column name before '=' in WHERE clause (e.g., WHERE column_name = value)",
                            code_snippet=full_sql[:150]
                        ))

                    # Check for incomplete WHERE with operators
                    if re.search(r'WHERE\s+(<|>|!=|<=|>=)', full_sql, re.IGNORECASE):
                        issues.append(Issue(
                            severity="critical",
                            category="syntax",
                            line=sql_start_line,
                            column=0,
                            message="SQL syntax error: WHERE clause missing column name before operator",
                            suggestion="Add column name before operator in WHERE clause",
                            code_snippet=full_sql[:150]
                        ))

            # Original SQL injection check
            if sql_fstring_pattern.search(line):
                # Check if it's using parameters from external source
                if any(keyword in line.lower() for keyword in ['parameterset', 'args.', 'input']):
                    issues.append(Issue(
                        severity="critical",
                        category="security",
                        line=i,
                        column=0,
                        message="SQL injection risk: f-string with external parameters",
                        suggestion="Validate and sanitize input parameters, or use Spark's built-in parameterization",
                        code_snippet=line.strip()[:100]
                    ))

        return issues

    def _check_code_quality(self, code: str, lines: List[str]) -> List[Issue]:
        """Check general code quality issues"""
        issues = []

        # CRITICAL CHECK 2: Detect duplicate write operations
        # Look for INSERT INTO followed by createOrReplace on same table
        for i in range(len(lines) - 3):
            line_block = ' '.join(lines[i:i+4])

            # Pattern: INSERT INTO ... followed by writeTo(...).createOrReplace()
            if 'INSERT INTO' in line_block and 'createOrReplace' in line_block:
                # Extract table names
                insert_match = re.search(r'INSERT\s+INTO\s+(\S+)', line_block, re.IGNORECASE)
                replace_match = re.search(r'writeTo\(["\'](\S+)["\']\)', line_block)

                if insert_match and replace_match:
                    insert_table = insert_match.group(1)
                    replace_table = replace_match.group(1)

                    # Check if same table
                    if insert_table == replace_table or insert_table in replace_table or replace_table in insert_table:
                        issues.append(Issue(
                            severity="critical",
                            category="logic",
                            line=i + 1,
                            column=0,
                            message=f"Duplicate write detected: INSERT INTO followed by createOrReplace() on table '{insert_table}'",
                            suggestion="Remove either the INSERT INTO or createOrReplace() - the second operation will overwrite the first",
                            code_snippet=line_block[:200]
                        ))

        # Check for empty DataFrame creation immediately overwritten
        for i in range(len(lines) - 1):
            line1 = lines[i].strip()
            line2 = lines[i + 1].strip()

            if 'spark.createDataFrame([], ' in line1:
                # Extract variable name
                match = re.match(r'(\w+)\s*=\s*spark\.createDataFrame', line1)
                if match:
                    var_name = match.group(1)
                    # Check if next line reassigns the same variable
                    if line2.startswith(f'{var_name} ='):
                        issues.append(Issue(
                            severity="info",
                            category="quality",
                            line=i + 1,
                            column=0,
                            message=f"Useless empty DataFrame creation for '{var_name}'",
                            suggestion="Remove the empty DataFrame creation",
                            code_snippet=f"{line1}\n{line2}"
                        ))

        # Check for magic numbers
        for i, line in enumerate(lines, 1):
            # Find CASE WHEN with status codes
            if 'STAT IN' in line or 'STATUS IN' in line:
                magic_nums = re.findall(r"'(\d+)'", line)
                if magic_nums:
                    issues.append(Issue(
                        severity="info",
                        category="quality",
                        line=i,
                        column=0,
                        message=f"Magic numbers in status check: {magic_nums}",
                        suggestion="Define status codes as named constants",
                        code_snippet=line.strip()[:100]
                    ))

        # Check for hardcoded credentials (basic check)
        credential_patterns = [
            r'password\s*=\s*["\'](?!<|{|\$)',
            r'api[_-]?key\s*=\s*["\'](?!<|{|\$)',
            r'secret\s*=\s*["\'](?!<|{|\$)',
        ]
        for i, line in enumerate(lines, 1):
            for pattern in credential_patterns:
                if re.search(pattern, line, re.IGNORECASE):
                    issues.append(Issue(
                        severity="critical",
                        category="security",
                        line=i,
                        column=0,
                        message="Potential hardcoded credential detected",
                        suggestion="Use environment variables or secure credential management",
                        code_snippet=line.strip()[:50] + "..."
                    ))

        return issues

    def _calculate_metrics(self, code: str, lines: List[str]) -> Dict[str, Any]:
        """Calculate code metrics"""
        metrics = {
            'total_lines': len(lines),
            'code_lines': len([l for l in lines if l.strip() and not l.strip().startswith('#')]),
            'comment_lines': len([l for l in lines if l.strip().startswith('#')]),
            'blank_lines': len([l for l in lines if not l.strip()]),
        }

        # Count functions
        try:
            tree = ast.parse(code)
            metrics['functions'] = len([n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)])
            metrics['classes'] = len([n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)])
        except:
            metrics['functions'] = 0
            metrics['classes'] = 0

        # Count DataFrames
        df_pattern = re.compile(r'(\w+)_df\s*=')
        metrics['dataframes'] = len(set(df_pattern.findall(code)))

        # Count SQL queries
        metrics['sql_queries'] = len(re.findall(r'spark\.sql\(|sql\s*=\s*f?["\']', code))

        # Count joins
        metrics['joins'] = len(re.findall(r'\.join\(', code))

        return metrics

    def _load_spark_patterns(self) -> Dict[str, Any]:
        """Load Spark-specific patterns"""
        return {
            'expensive_operations': ['collect', 'count', 'show'],
            'transformation_ops': ['select', 'filter', 'join', 'groupBy', 'agg'],
            'action_ops': ['write', 'save', 'collect', 'count', 'show'],
        }


if __name__ == "__main__":
    # Test the analyzer
    import json

    analyzer = StaticAnalyzer({})
    result = analyzer.analyze_file("../jobs/STG_XTC_LD_TEST_JOB.py")

    print(f"\n=== Analysis Results for {result.file_path} ===")
    print(f"\nMetrics: {json.dumps(result.metrics, indent=2)}")
    print(f"\nSummary: {result.summary}")
    print(f"\nIssues found: {len(result.issues)}")

    for issue in result.issues[:10]:  # Show first 10 issues
        print(f"\n[{issue.severity.upper()}] {issue.category} (Line {issue.line})")
        print(f"  Message: {issue.message}")
        print(f"  Suggestion: {issue.suggestion}")
        if issue.code_snippet:
            print(f"  Code: {issue.code_snippet[:100]}")
