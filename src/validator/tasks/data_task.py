"""Data validation task for Spark jobs

Validates data sources, accessibility, format, and basic data checks.
"""

import json
from typing import Dict, List, Any
from prefect import task


@task(name="Validate Data Sources", retries=2, retry_delay_seconds=10)
def validate_data_sources(
    job_path: str,
    config_path: str,
    required_columns: List[str] = None
) -> Dict[str, Any]:
    """Validate data sources and basic data quality

    Args:
        job_path: Path to Spark job Python file
        config_path: Path to job config JSON file
        required_columns: List of required column names (optional)

    Returns:
        Validation result with issues found
    """
    issues = []

    # Load config
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
    except Exception as e:
        return {
            "stage": "data",
            "passed": False,
            "issues": [{
                "severity": "critical",
                "category": "config",
                "message": f"Failed to load config: {str(e)}",
                "suggestion": "Ensure config.json is valid"
            }],
            "total_issues": 1,
            "critical_count": 1,
            "warning_count": 0
        }

    # 1. Check data source specification
    source_issues = _check_data_source_specification(config)
    issues.extend(source_issues)

    # 2. Check data source accessibility (lightweight - no Spark needed)
    access_issues = _check_data_source_accessibility(config)
    issues.extend(access_issues)

    # 3. Check required columns (requires Spark - optional)
    if required_columns:
        column_issues = _check_required_columns(config, required_columns)
        issues.extend(column_issues)

    return {
        "stage": "data",
        "passed": len([i for i in issues if i["severity"] == "critical"]) == 0,
        "issues": issues,
        "total_issues": len(issues),
        "critical_count": len([i for i in issues if i["severity"] == "critical"]),
        "warning_count": len([i for i in issues if i["severity"] == "warning"])
    }


def _check_data_source_specification(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Check if data sources are properly specified in config"""
    issues = []

    # Check input data
    if 'data' not in config:
        issues.append({
            "severity": "critical",
            "category": "data_source",
            "message": "Missing 'data' section in config",
            "suggestion": "Add 'data' section with input/output configuration"
        })
        return issues

    if 'input' not in config['data']:
        issues.append({
            "severity": "critical",
            "category": "data_source",
            "message": "Missing 'data.input' configuration",
            "suggestion": "Add input data source configuration"
        })
    else:
        input_config = config['data']['input']

        # Check path
        if 'path' not in input_config:
            issues.append({
                "severity": "critical",
                "category": "data_source",
                "message": "Missing input data path",
                "suggestion": "Add 'data.input.path' to config"
            })

        # Check format
        if 'format' not in input_config:
            issues.append({
                "severity": "warning",
                "category": "data_source",
                "message": "Missing input data format",
                "suggestion": "Add 'data.input.format' (csv, parquet, iceberg, etc.)"
            })

    # Check output configuration
    if 'output' not in config['data']:
        issues.append({
            "severity": "warning",
            "category": "data_source",
            "message": "Missing 'data.output' configuration",
            "suggestion": "Add output data configuration"
        })
    else:
        output_config = config['data']['output']

        # Check required output fields
        required_fields = ['catalog', 'database', 'table']
        for field in required_fields:
            if field not in output_config:
                issues.append({
                    "severity": "warning",
                    "category": "data_source",
                    "message": f"Missing output configuration: '{field}'",
                    "suggestion": f"Add 'data.output.{field}' to config"
                })

    return issues


def _check_data_source_accessibility(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Check if data sources are accessible (lightweight validation)"""
    issues = []

    try:
        input_config = config.get('data', {}).get('input', {})
        input_path = input_config.get('path', '')

        if not input_path:
            return issues

        # Check S3/MinIO configuration
        if input_path.startswith('s3://') or input_path.startswith('s3a://'):
            spark_config = config.get('spark', {})
            s3_config = spark_config.get('s3', {})

            # Check S3 credentials
            if 'access_key' not in s3_config:
                issues.append({
                    "severity": "critical",
                    "category": "data_access",
                    "message": "S3 access key not configured",
                    "suggestion": "Add 'spark.s3.access_key' to config"
                })

            if 'secret_key' not in s3_config:
                issues.append({
                    "severity": "critical",
                    "category": "data_access",
                    "message": "S3 secret key not configured",
                    "suggestion": "Add 'spark.s3.secret_key' to config"
                })

            if 'endpoint' not in s3_config:
                issues.append({
                    "severity": "warning",
                    "category": "data_access",
                    "message": "S3 endpoint not configured (using AWS default)",
                    "suggestion": "Add 'spark.s3.endpoint' for MinIO or custom S3"
                })

        # For local files, check if path looks valid
        elif input_path.startswith('file://') or input_path.startswith('/'):
            # Basic path validation
            if ' ' in input_path:
                issues.append({
                    "severity": "warning",
                    "category": "data_access",
                    "message": "Input path contains spaces - may cause issues",
                    "suggestion": "Use paths without spaces or properly escape them"
                })

        else:
            issues.append({
                "severity": "info",
                "category": "data_access",
                "message": f"Input path scheme not recognized: {input_path}",
                "suggestion": "Ensure path is valid (s3://, s3a://, file://, hdfs://, etc.)"
            })

    except Exception as e:
        issues.append({
            "severity": "info",
            "category": "data_access",
            "message": f"Data accessibility check skipped: {str(e)}",
            "suggestion": "Verify data source configuration"
        })

    return issues


def _check_required_columns(config: Dict[str, Any], required_columns: List[str]) -> List[Dict[str, Any]]:
    """Check if required columns exist in data source

    Note: This requires actual Spark execution and is optional.
    For now, we'll just note it as a check that should be done.
    """
    issues = []

    # This would require:
    # 1. Creating a Spark session
    # 2. Reading the data source (just schema, not full data)
    # 3. Checking column names
    #
    # For lightweight validation, we'll skip this and add it as an info message

    if required_columns:
        issues.append({
            "severity": "info",
            "category": "data_schema",
            "message": f"Required columns check: {', '.join(required_columns)}",
            "suggestion": "This check requires Spark session - run full validation to verify"
        })

    return issues
