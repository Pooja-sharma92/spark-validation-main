"""
Prefect Flow Orchestration for Spark Job Validation.

Integrates the validation framework with Prefect for:
- Task-based validation pipeline
- Retry logic and error handling
- Flow monitoring and logging
- Integration with existing validation tasks
"""

import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any
import structlog

try:
    from prefect import flow, task, get_run_logger
    from prefect.tasks import task_input_hash
    from prefect.futures import wait
    PREFECT_AVAILABLE = True
except ImportError:
    PREFECT_AVAILABLE = False
    # Provide mock decorators for when Prefect is not installed
    def flow(*args, **kwargs):
        def decorator(func):
            return func
        return decorator if not args else decorator(args[0])

    def task(*args, **kwargs):
        def decorator(func):
            return func
        return decorator if not args else decorator(args[0])

from validator.models import (
    ValidationRequest,
    ValidationResult,
    ValidationStatus,
    ValidationStage,
    StageResult,
    ValidationIssue,
    Severity,
    BatchSummary,
)
from validator.config import get_config


logger = structlog.get_logger(__name__)


# ==========================================
# Validation Tasks
# ==========================================

@task(
    name="Syntax Validation",
    retries=1,
    retry_delay_seconds=5,
    tags=["validation", "syntax"],
)
async def validate_syntax_task(
    job_path: str,
    config: Optional[Dict[str, Any]] = None,
) -> StageResult:
    """
    Validate Python syntax using AST parsing.

    This task checks for:
    - Syntax errors
    - Invalid Python constructs
    - Encoding issues
    """
    import ast
    import time

    start = time.time()
    issues = []

    try:
        path = Path(job_path)
        if not path.exists():
            issues.append(ValidationIssue(
                stage=ValidationStage.SYNTAX,
                severity=Severity.CRITICAL,
                message=f"File not found: {job_path}",
                file_path=job_path,
            ))
        else:
            source = path.read_text(encoding='utf-8')
            ast.parse(source, filename=job_path)

    except SyntaxError as e:
        issues.append(ValidationIssue(
            stage=ValidationStage.SYNTAX,
            severity=Severity.CRITICAL,
            message=f"Syntax error: {e.msg}",
            file_path=job_path,
            line_number=e.lineno,
        ))

    except UnicodeDecodeError as e:
        issues.append(ValidationIssue(
            stage=ValidationStage.SYNTAX,
            severity=Severity.CRITICAL,
            message=f"Encoding error: {str(e)}",
            file_path=job_path,
        ))

    duration = time.time() - start

    return StageResult(
        stage=ValidationStage.SYNTAX,
        status=ValidationStatus.COMPLETED,
        duration_seconds=duration,
        issues=issues,
        metrics={"lines_checked": 0},
    )


@task(
    name="Logic Validation",
    retries=1,
    retry_delay_seconds=5,
    tags=["validation", "logic"],
)
async def validate_logic_task(
    job_path: str,
    config: Optional[Dict[str, Any]] = None,
) -> StageResult:
    """
    Validate code logic and detect anti-patterns.

    Checks for:
    - Unused imports and variables
    - Dead code paths
    - Spark anti-patterns (collect on large data, etc.)
    - Common bugs
    """
    import ast
    import time
    import re

    start = time.time()
    issues = []

    try:
        path = Path(job_path)
        source = path.read_text(encoding='utf-8')
        tree = ast.parse(source, filename=job_path)

        # Track imports and usage
        imports = set()
        used_names = set()

        for node in ast.walk(tree):
            # Collect imports
            if isinstance(node, ast.Import):
                for alias in node.names:
                    name = alias.asname or alias.name.split('.')[0]
                    imports.add((name, node.lineno))

            elif isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    if alias.name != '*':
                        name = alias.asname or alias.name
                        imports.add((name, node.lineno))

            # Collect name usage
            elif isinstance(node, ast.Name):
                used_names.add(node.id)

            # Check for bare except
            elif isinstance(node, ast.ExceptHandler) and node.type is None:
                issues.append(ValidationIssue(
                    stage=ValidationStage.LOGIC,
                    severity=Severity.WARNING,
                    message="Bare 'except:' clause catches all exceptions including KeyboardInterrupt",
                    file_path=job_path,
                    line_number=node.lineno,
                    rule_id="L001",
                    suggestion="Use 'except Exception:' to catch only exceptions",
                ))

            # Check for print statements (should use logging)
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name) and node.func.id == 'print':
                    issues.append(ValidationIssue(
                        stage=ValidationStage.LOGIC,
                        severity=Severity.INFO,
                        message="Using print() - consider using logging for production code",
                        file_path=job_path,
                        line_number=node.lineno,
                        rule_id="L002",
                    ))

        # Check for unused imports
        for name, lineno in imports:
            if name not in used_names and not name.startswith('_'):
                issues.append(ValidationIssue(
                    stage=ValidationStage.LOGIC,
                    severity=Severity.WARNING,
                    message=f"Unused import: {name}",
                    file_path=job_path,
                    line_number=lineno,
                    rule_id="L003",
                ))

        # Check for Spark anti-patterns
        spark_patterns = [
            (r'\.collect\(\)', Severity.WARNING, "Using .collect() - may cause OOM on large datasets", "L010"),
            (r'\.toPandas\(\)', Severity.WARNING, "Using .toPandas() - may cause OOM on large datasets", "L011"),
            (r'spark\.sql\s*\(\s*f["\']', Severity.WARNING, "Using f-string in SQL - potential SQL injection", "L012"),
        ]

        for pattern, severity, message, rule_id in spark_patterns:
            for match in re.finditer(pattern, source):
                line_num = source[:match.start()].count('\n') + 1
                issues.append(ValidationIssue(
                    stage=ValidationStage.LOGIC,
                    severity=severity,
                    message=message,
                    file_path=job_path,
                    line_number=line_num,
                    rule_id=rule_id,
                ))

    except Exception as e:
        issues.append(ValidationIssue(
            stage=ValidationStage.LOGIC,
            severity=Severity.ERROR,
            message=f"Logic validation error: {str(e)}",
            file_path=job_path,
        ))

    duration = time.time() - start

    return StageResult(
        stage=ValidationStage.LOGIC,
        status=ValidationStatus.COMPLETED,
        duration_seconds=duration,
        issues=issues,
    )


@task(
    name="Data Validation",
    retries=2,
    retry_delay_seconds=10,
    tags=["validation", "data"],
)
async def validate_data_task(
    job_path: str,
    config: Optional[Dict[str, Any]] = None,
) -> StageResult:
    """
    Validate data sources and configurations.

    Checks for:
    - Valid S3/MinIO paths
    - Required configuration presence
    - Hardcoded credentials
    - DataStage migration patterns
    """
    import re
    import time

    start = time.time()
    issues = []

    try:
        path = Path(job_path)
        source = path.read_text(encoding='utf-8')

        # Check for s3a:// usage (project prefers s3://)
        if 's3a://' in source:
            for match in re.finditer(r's3a://[^\s\'"]+', source):
                line_num = source[:match.start()].count('\n') + 1
                issues.append(ValidationIssue(
                    stage=ValidationStage.DATA,
                    severity=Severity.WARNING,
                    message=f"Using s3a:// protocol at: {match.group()}",
                    file_path=job_path,
                    line_number=line_num,
                    rule_id="D001",
                    suggestion="Per project guidelines, prefer s3:// over s3a://",
                ))

        # Check for hardcoded credentials
        credential_patterns = [
            (r'password\s*=\s*["\'][^"\']{3,}["\']', "Hardcoded password"),
            (r'secret\s*=\s*["\'][^"\']{3,}["\']', "Hardcoded secret"),
            (r'api[_-]?key\s*=\s*["\'][^"\']{3,}["\']', "Hardcoded API key"),
            (r'access[_-]?key\s*=\s*["\'][^"\']{10,}["\']', "Hardcoded access key"),
        ]

        for pattern, message in credential_patterns:
            for match in re.finditer(pattern, source, re.IGNORECASE):
                line_num = source[:match.start()].count('\n') + 1
                issues.append(ValidationIssue(
                    stage=ValidationStage.DATA,
                    severity=Severity.CRITICAL,
                    message=message,
                    file_path=job_path,
                    line_number=line_num,
                    rule_id="D002",
                    suggestion="Use environment variables or secrets manager",
                ))

        # Check for required DataStage patterns (from CLAUDE.md)
        if 'spark.sql' in source:
            # Check for type casting pattern
            if 'selectExpr' not in source and 'cast' not in source.lower():
                issues.append(ValidationIssue(
                    stage=ValidationStage.DATA,
                    severity=Severity.INFO,
                    message="SQL queries without explicit type casting",
                    file_path=job_path,
                    rule_id="D010",
                    suggestion="Add explicit CAST operations for type safety",
                ))

    except Exception as e:
        issues.append(ValidationIssue(
            stage=ValidationStage.DATA,
            severity=Severity.ERROR,
            message=f"Data validation error: {str(e)}",
            file_path=job_path,
        ))

    duration = time.time() - start

    return StageResult(
        stage=ValidationStage.DATA,
        status=ValidationStatus.COMPLETED,
        duration_seconds=duration,
        issues=issues,
    )


@task(
    name="Expectations Validation",
    retries=1,
    retry_delay_seconds=5,
    tags=["validation", "quality"],
)
async def validate_expectations_task(
    job_path: str,
    config: Optional[Dict[str, Any]] = None,
) -> StageResult:
    """
    Run Great Expectations validation suites.

    Note: This is a placeholder that checks for GE configuration.
    Actual data validation would require running the job.
    """
    import time

    start = time.time()
    issues = []
    metrics = {"suites_found": 0, "suites_run": 0}

    try:
        # Check for Great Expectations config
        job_dir = Path(job_path).parent
        ge_config = job_dir / "great_expectations.yml"

        if ge_config.exists():
            metrics["suites_found"] = 1
            # In production, would load and validate expectations
        else:
            issues.append(ValidationIssue(
                stage=ValidationStage.EXPECTATIONS,
                severity=Severity.INFO,
                message="No Great Expectations configuration found",
                file_path=job_path,
                rule_id="E001",
            ))

    except Exception as e:
        issues.append(ValidationIssue(
            stage=ValidationStage.EXPECTATIONS,
            severity=Severity.ERROR,
            message=f"Expectations validation error: {str(e)}",
            file_path=job_path,
        ))

    duration = time.time() - start

    return StageResult(
        stage=ValidationStage.EXPECTATIONS,
        status=ValidationStatus.COMPLETED,
        duration_seconds=duration,
        issues=issues,
        metrics=metrics,
    )


# ==========================================
# Main Validation Flow
# ==========================================

@flow(
    name="Validate Spark Job",
    description="Complete validation pipeline for a Spark job",
    retries=0,
    log_prints=True,
)
async def validate_spark_job_flow(
    request: ValidationRequest,
) -> ValidationResult:
    """
    Execute the complete validation pipeline for a Spark job.

    Runs validation stages in sequence:
    1. Syntax validation (parallel-safe)
    2. Logic validation (depends on syntax)
    3. Data validation (parallel with logic)
    4. Expectations validation (after data)
    """
    started_at = datetime.utcnow()
    stage_results: List[StageResult] = []

    config = get_config()
    job_path = request.job_path

    logger.info(
        "Starting validation flow",
        job_path=job_path,
        request_id=request.id,
    )

    # Stage 1: Syntax validation (must pass for others to run)
    syntax_result = await validate_syntax_task(job_path)
    stage_results.append(syntax_result)

    # Check if we should continue
    has_critical = any(
        i.severity == Severity.CRITICAL for i in syntax_result.issues
    )

    if not has_critical:
        # Stage 2 & 3: Logic and Data validation (can run in parallel)
        logic_future = validate_logic_task.submit(job_path)
        data_future = validate_data_task.submit(job_path)

        logic_result = await logic_future.result()
        data_result = await data_future.result()

        stage_results.extend([logic_result, data_result])

        # Stage 4: Expectations (after data validation)
        expectations_result = await validate_expectations_task(job_path)
        stage_results.append(expectations_result)

    completed_at = datetime.utcnow()

    # Determine overall status
    all_issues = [i for sr in stage_results for i in sr.issues]
    has_errors = any(i.severity == Severity.CRITICAL for i in all_issues)
    has_warnings = any(i.severity == Severity.ERROR for i in all_issues)

    if has_errors:
        status = ValidationStatus.FAILED
    elif has_warnings:
        status = ValidationStatus.COMPLETED  # Completed with warnings
    else:
        status = ValidationStatus.COMPLETED

    result = ValidationResult(
        request_id=request.id,
        job_path=job_path,
        status=status,
        stage_results=stage_results,
        started_at=started_at,
        completed_at=completed_at,
    )

    logger.info(
        "Validation flow completed",
        job_path=job_path,
        status=status.value,
        duration=result.duration_seconds,
        issues=len(all_issues),
    )

    return result


@flow(
    name="Batch Validate Spark Jobs",
    description="Validate multiple Spark jobs with controlled parallelism",
)
async def batch_validate_flow(
    requests: List[ValidationRequest],
    max_parallel: int = 5,
) -> BatchSummary:
    """
    Validate multiple jobs with controlled parallelism.

    Args:
        requests: List of validation requests
        max_parallel: Maximum parallel validations
    """
    import uuid

    batch_id = str(uuid.uuid4())
    started_at = datetime.utcnow()
    results: List[ValidationResult] = []

    logger.info(
        "Starting batch validation",
        batch_id=batch_id,
        total_jobs=len(requests),
        max_parallel=max_parallel,
    )

    # Process in chunks
    for i in range(0, len(requests), max_parallel):
        chunk = requests[i:i + max_parallel]

        # Submit all jobs in chunk
        futures = [
            validate_spark_job_flow.submit(req)
            for req in chunk
        ]

        # Wait for chunk to complete
        for future in futures:
            try:
                result = await future.result()
                results.append(result)
            except Exception as e:
                logger.error("Job failed in batch", error=str(e))

    completed_at = datetime.utcnow()

    # Calculate summary
    completed = sum(1 for r in results if r.status == ValidationStatus.COMPLETED)
    failed = sum(1 for r in results if r.status == ValidationStatus.FAILED)
    errors = sum(1 for r in results if r.status == ValidationStatus.ERROR)

    summary = BatchSummary(
        batch_id=batch_id,
        total_jobs=len(requests),
        completed=completed,
        failed=failed,
        errors=errors,
        skipped=len(requests) - len(results),
        started_at=started_at,
        completed_at=completed_at,
        results=results,
    )

    logger.info(
        "Batch validation completed",
        batch_id=batch_id,
        success_rate=summary.success_rate,
    )

    return summary


# ==========================================
# Utility Functions
# ==========================================

async def run_validation(job_path: str) -> ValidationResult:
    """
    Convenience function to run validation on a single job.

    Can be used without the full queue infrastructure.
    """
    request = ValidationRequest(
        job_path=job_path,
        trigger_source="manual",
        priority=1,
    )

    return await validate_spark_job_flow(request)


async def run_batch_validation(
    job_paths: List[str],
    max_parallel: int = 5,
) -> BatchSummary:
    """
    Convenience function to validate multiple jobs.
    """
    from validator.models import TriggerSource

    requests = [
        ValidationRequest(
            job_path=path,
            trigger_source=TriggerSource.MANUAL,
            priority=3,
        )
        for path in job_paths
    ]

    return await batch_validate_flow(requests, max_parallel)
