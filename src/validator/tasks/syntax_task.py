"""Syntax validation task for Spark jobs

Validates Python syntax, imports, config files, and Spark configurations.
"""

import ast
import json
import importlib.util
import sys
from pathlib import Path
from typing import Dict, List, Any
from prefect import task

# Add parent directories to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "analyzer"))

try:
    from validator.analyzer.core.static_analyzer import StaticAnalyzer
    ANALYZER_AVAILABLE = True
except ImportError:
    ANALYZER_AVAILABLE = False


@task(name="Validate Syntax", retries=0)
def validate_syntax(job_path: str, config_path: str) -> Dict[str, Any]:
    """Validate Python syntax, imports, and configuration

    Args:
        job_path: Path to Spark job Python file
        config_path: Path to job config JSON file

    Returns:
        Validation result with issues found
    """
    issues = []

    # 1. Python syntax validation
    syntax_issues = _check_python_syntax(job_path)
    issues.extend(syntax_issues)

    # 2. Import validation
    import_issues = _check_imports(job_path)
    issues.extend(import_issues)

    # 3. Config file validation
    config_issues = _check_config_file(config_path)
    issues.extend(config_issues)

    # 4. Spark configuration validation
    spark_issues = _check_spark_configs(config_path)
    issues.extend(spark_issues)

    # 5. Static analysis (anti-patterns)
    if ANALYZER_AVAILABLE:
        static_issues = _check_static_analysis(job_path)
        issues.extend(static_issues)

    return {
        "stage": "syntax",
        "passed": len([i for i in issues if i["severity"] == "critical"]) == 0,
        "issues": issues,
        "total_issues": len(issues),
        "critical_count": len([i for i in issues if i["severity"] == "critical"]),
        "warning_count": len([i for i in issues if i["severity"] == "warning"])
    }


def _check_python_syntax(job_path: str) -> List[Dict[str, Any]]:
    """Check Python syntax using AST parsing"""
    issues = []

    try:
        with open(job_path, 'r') as f:
            source_code = f.read()

        # Parse AST
        ast.parse(source_code)

    except SyntaxError as e:
        issues.append({
            "severity": "critical",
            "category": "syntax",
            "message": f"Python syntax error at line {e.lineno}: {e.msg}",
            "line": e.lineno,
            "suggestion": "Fix the syntax error before proceeding"
        })
    except Exception as e:
        issues.append({
            "severity": "critical",
            "category": "syntax",
            "message": f"Failed to parse Python file: {str(e)}",
            "suggestion": "Ensure the file is valid Python code"
        })

    return issues


def _check_imports(job_path: str) -> List[Dict[str, Any]]:
    """Check if all imports can be resolved"""
    issues = []

    try:
        with open(job_path, 'r') as f:
            source_code = f.read()

        tree = ast.parse(source_code)

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if not _can_import(alias.name):
                        issues.append({
                            "severity": "warning",
                            "category": "imports",
                            "message": f"Cannot import '{alias.name}' - module may not be installed",
                            "line": node.lineno,
                            "suggestion": f"Install missing dependency: pip install {alias.name.split('.')[0]}"
                        })

            elif isinstance(node, ast.ImportFrom):
                module = node.module
                if module and not _can_import(module):
                    issues.append({
                        "severity": "warning",
                        "category": "imports",
                        "message": f"Cannot import from '{module}' - module may not be installed",
                        "line": node.lineno,
                        "suggestion": f"Install missing dependency: pip install {module.split('.')[0]}"
                    })

    except Exception as e:
        # Already caught by syntax check
        pass

    return issues


def _check_config_file(config_path: str) -> List[Dict[str, Any]]:
    """Validate JSON config file format"""
    issues = []

    try:
        with open(config_path, 'r') as f:
            config = json.load(f)

        # Check required sections
        required_sections = ['job', 'spark', 'data']
        for section in required_sections:
            if section not in config:
                issues.append({
                    "severity": "critical",
                    "category": "config",
                    "message": f"Missing required config section: '{section}'",
                    "suggestion": f"Add '{section}' section to config.json"
                })

    except json.JSONDecodeError as e:
        issues.append({
            "severity": "critical",
            "category": "config",
            "message": f"Invalid JSON format: {str(e)}",
            "suggestion": "Fix JSON syntax in config file"
        })
    except FileNotFoundError:
        issues.append({
            "severity": "critical",
            "category": "config",
            "message": f"Config file not found: {config_path}",
            "suggestion": "Ensure config.json exists in job directory"
        })

    return issues


def _check_spark_configs(config_path: str) -> List[Dict[str, Any]]:
    """Check Spark-specific configuration issues"""
    issues = []

    try:
        with open(config_path, 'r') as f:
            config = json.load(f)

        # Check for s3:// vs s3a:// issues
        if 'data' in config:
            input_path = config['data'].get('input', {}).get('path', '')

            # S3AFileSystem requires s3a:// not s3://
            if input_path.startswith('s3://'):
                issues.append({
                    "severity": "warning",
                    "category": "spark_config",
                    "message": f"Using 's3://' scheme - ensure S3AFileSystem is configured for 's3://' or use 's3a://'",
                    "suggestion": "Configure spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem or change to 's3a://'"
                })

        # Check for catalog configuration
        if 'spark' in config and 'catalog' in config['spark']:
            catalog_type = config['spark']['catalog'].get('type')
            if catalog_type == 'rest':
                if 'uri' not in config['spark']['catalog']:
                    issues.append({
                        "severity": "critical",
                        "category": "spark_config",
                        "message": "REST catalog requires 'uri' configuration",
                        "suggestion": "Add catalog URI to config (e.g., http://iceberg-rest:8181)"
                    })

    except Exception:
        # Already caught by config validation
        pass

    return issues


def _check_static_analysis(job_path: str) -> List[Dict[str, Any]]:
    """Run static analysis for anti-patterns"""
    issues = []

    try:
        analyzer = StaticAnalyzer({})
        results = analyzer.analyze_file(job_path)

        # Convert analyzer results to our format
        for result in results.get('issues', []):
            severity = "critical" if result.get('severity') == 'error' else "warning"
            issues.append({
                "severity": severity,
                "category": "static_analysis",
                "message": result.get('message', ''),
                "line": result.get('line'),
                "suggestion": result.get('suggestion', '')
            })

    except Exception as e:
        # Static analysis is optional
        issues.append({
            "severity": "info",
            "category": "static_analysis",
            "message": f"Static analysis skipped: {str(e)}",
            "suggestion": "Install analyzer dependencies for advanced checks"
        })

    return issues


def _can_import(module_name: str) -> bool:
    """Check if a module can be imported"""
    try:
        spec = importlib.util.find_spec(module_name)
        return spec is not None
    except (ImportError, ModuleNotFoundError, ValueError):
        return False
