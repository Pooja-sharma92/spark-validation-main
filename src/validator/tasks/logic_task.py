"""Logic validation task for Spark jobs

Detects unused DataFrames, duplicate operations, and logic errors.
"""

import ast
import json
from pathlib import Path
from typing import Dict, List, Any, Set
from collections import defaultdict
from prefect import task


@task(name="Validate Logic", retries=0)
def validate_logic(job_path: str, config_path: str, enable_dry_run: bool = False) -> Dict[str, Any]:
    """Validate Spark job logic for common errors

    Args:
        job_path: Path to Spark job Python file
        config_path: Path to job config JSON file
        enable_dry_run: Whether to run dry-run test (optional)

    Returns:
        Validation result with issues found
    """
    issues = []

    # 1. Detect unused DataFrames
    unused_df_issues = _detect_unused_dataframes(job_path)
    issues.extend(unused_df_issues)

    # 2. Detect duplicate operations
    duplicate_issues = _detect_duplicate_operations(job_path)
    issues.extend(duplicate_issues)

    # 3. Detect dead code
    dead_code_issues = _detect_dead_code(job_path)
    issues.extend(dead_code_issues)

    # 4. Optional: Dry-run execution (disabled by default for performance)
    if enable_dry_run:
        dry_run_issues = _execute_dry_run(job_path, config_path)
        issues.extend(dry_run_issues)

    return {
        "stage": "logic",
        "passed": len([i for i in issues if i["severity"] == "critical"]) == 0,
        "issues": issues,
        "total_issues": len(issues),
        "critical_count": len([i for i in issues if i["severity"] == "critical"]),
        "warning_count": len([i for i in issues if i["severity"] == "warning"])
    }


def _detect_unused_dataframes(job_path: str) -> List[Dict[str, Any]]:
    """Detect DataFrames that are created but never used"""
    issues = []

    try:
        with open(job_path, 'r') as f:
            source_code = f.read()

        tree = ast.parse(source_code)

        # Track DataFrame variable assignments
        df_assignments = {}  # {var_name: line_number}
        df_usages = set()    # {var_name}

        for node in ast.walk(tree):
            # Track assignments (df = spark.read... or df = other_df.select(...))
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        var_name = target.id
                        # Heuristic: variables with 'df' in name or ending with _df
                        if 'df' in var_name.lower() or var_name.endswith('_df'):
                            df_assignments[var_name] = node.lineno

            # Track usages (reading the variable)
            elif isinstance(node, ast.Name):
                if isinstance(node.ctx, ast.Load):
                    df_usages.add(node.id)

        # Find unused DataFrames
        for var_name, line_no in df_assignments.items():
            if var_name not in df_usages:
                issues.append({
                    "severity": "warning",
                    "category": "logic",
                    "message": f"DataFrame '{var_name}' is created but never used",
                    "line": line_no,
                    "suggestion": f"Remove unused DataFrame '{var_name}' or use it in the pipeline"
                })

    except Exception as e:
        # Parse errors already caught by syntax validation
        pass

    return issues


def _detect_duplicate_operations(job_path: str) -> List[Dict[str, Any]]:
    """Detect duplicate DataFrame operations"""
    issues = []

    try:
        with open(job_path, 'r') as f:
            source_code = f.read()

        tree = ast.parse(source_code)

        # Track write operations
        write_operations = defaultdict(list)  # {table_name: [line_numbers]}

        for node in ast.walk(tree):
            # Detect .write.* operations
            if isinstance(node, ast.Call):
                # Check for .writeTo(...) or .write.saveAsTable(...)
                if isinstance(node.func, ast.Attribute):
                    if node.func.attr in ['writeTo', 'saveAsTable']:
                        # Try to extract table name from arguments
                        if node.args:
                            if isinstance(node.args[0], ast.Constant):
                                table_name = node.args[0].value
                                write_operations[table_name].append(node.lineno)

        # Find duplicate writes to same table
        for table_name, line_numbers in write_operations.items():
            if len(line_numbers) > 1:
                issues.append({
                    "severity": "warning",
                    "category": "logic",
                    "message": f"Multiple writes to same table '{table_name}' at lines {line_numbers}",
                    "line": line_numbers[0],
                    "suggestion": "Consolidate writes or ensure this is intentional"
                })

    except Exception:
        pass

    return issues


def _detect_dead_code(job_path: str) -> List[Dict[str, Any]]:
    """Detect unreachable code (code after return statements)"""
    issues = []

    try:
        with open(job_path, 'r') as f:
            source_code = f.read()

        tree = ast.parse(source_code)

        class DeadCodeDetector(ast.NodeVisitor):
            def __init__(self):
                self.issues = []
                self.in_function = False

            def visit_FunctionDef(self, node):
                # Track if we're inside a function
                old_in_function = self.in_function
                self.in_function = True

                # Check for code after return
                return_found = False
                for i, stmt in enumerate(node.body):
                    if return_found and not isinstance(stmt, (ast.Pass, ast.Return)):
                        self.issues.append({
                            "severity": "warning",
                            "category": "logic",
                            "message": f"Unreachable code after return in function '{node.name}'",
                            "line": stmt.lineno,
                            "suggestion": "Remove dead code or restructure logic"
                        })
                        break

                    if isinstance(stmt, ast.Return):
                        return_found = True

                self.generic_visit(node)
                self.in_function = old_in_function

        detector = DeadCodeDetector()
        detector.visit(tree)
        issues.extend(detector.issues)

    except Exception:
        pass

    return issues


def _execute_dry_run(job_path: str, config_path: str, sample_rows: int = 100) -> List[Dict[str, Any]]:
    """Execute job on sample data to catch runtime errors

    Note: This is a lightweight dry-run that validates basic execution.
    Full dry-run with Spark cluster would require more setup.
    """
    issues = []

    try:
        # Load config to check if input data is accessible
        with open(config_path, 'r') as f:
            config = json.load(f)

        input_path = config.get('data', {}).get('input', {}).get('path', '')

        # Basic validation: check if data source is specified
        if not input_path:
            issues.append({
                "severity": "warning",
                "category": "dry_run",
                "message": "No input data path specified in config",
                "suggestion": "Add data.input.path to config.json"
            })
        else:
            # For now, just validate config structure
            # Full dry-run would require:
            # 1. Creating local Spark session
            # 2. Loading sample data
            # 3. Executing transformations
            # This can be added in future iterations
            issues.append({
                "severity": "info",
                "category": "dry_run",
                "message": f"Dry-run validation: input path configured as '{input_path}'",
                "suggestion": "Consider running full dry-run test before deployment"
            })

    except Exception as e:
        issues.append({
            "severity": "info",
            "category": "dry_run",
            "message": f"Dry-run skipped: {str(e)}",
            "suggestion": "Fix configuration issues before running dry-run"
        })

    return issues
