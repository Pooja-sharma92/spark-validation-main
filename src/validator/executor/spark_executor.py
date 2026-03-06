"""
Local Spark Validation Executor.

A long-running Spark application that:
- Maintains a single SparkSession (local mode)
- Polls the validation queue for jobs
- Executes validations sequentially, reusing the session
- Reports results back to the queue/storage

This avoids the overhead of spark-submit for each validation.
"""

import asyncio
import signal
import sys
import time
import ast
import re
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict, Any
from enum import Enum

# PySpark imports
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.utils import AnalysisException, ParseException
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    SparkSession = None

import structlog

# Structlog for console output
structlog_logger = structlog.get_logger(__name__)

# Validation logger for UI (lazy import to avoid circular deps)
_validation_logger = None

def get_validation_logger():
    """Get or create the validation structlog_logger."""
    global _validation_logger
    if _validation_logger is None:
        try:
            from validator.logging import get_logger
            _validation_logger = get_logger(module="executor", component="spark-executor")
        except ImportError:
            _validation_logger = None
    return _validation_logger


@dataclass
class ExecutorConfig:
    """Configuration for the Spark executor."""
    # Spark settings
    app_name: str = "ValidationExecutor"
    master: str = "local[*]"  # Local mode, use all cores

    # Memory settings
    driver_memory: str = "2g"
    executor_memory: str = "2g"

    # Queue polling
    poll_interval_seconds: float = 2.0
    idle_sleep_seconds: float = 5.0

    # Redis connection
    redis_url: str = "redis://localhost:6379/0"

    # Validation settings
    sql_dry_run: bool = True  # Parse SQL without executing
    max_jobs_before_restart: int = 100  # Restart session after N jobs (memory cleanup)

    # Catalog settings (for SQL validation)
    catalog_name: str = "spark_catalog"
    warehouse_path: str = "/tmp/spark-warehouse"


class ValidationStage(Enum):
    """Validation stages."""
    SYNTAX = "syntax"
    IMPORTS = "imports"
    SQL_PARSE = "sql_parse"
    SPARK_PLAN = "spark_plan"
    LOGIC = "logic"


@dataclass
class StageResult:
    """Result of a single validation stage."""
    stage: str
    passed: bool
    duration_seconds: float
    issues: List[Dict[str, Any]] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationResult:
    """Complete validation result."""
    job_id: str
    job_path: str
    passed: bool
    stages: List[StageResult]
    started_at: datetime
    completed_at: datetime
    error: Optional[str] = None

    @property
    def duration_seconds(self) -> float:
        return (self.completed_at - self.started_at).total_seconds()


class SparkValidationExecutor:
    """
    Local Spark executor for job validation.

    Maintains a long-running SparkSession and processes
    validation jobs from the queue sequentially.
    """

    def __init__(self, config: Optional[ExecutorConfig] = None):
        if not SPARK_AVAILABLE:
            raise RuntimeError("PySpark is not installed")

        self.config = config or ExecutorConfig()
        self._spark: Optional[SparkSession] = None
        self._running = False
        self._jobs_processed = 0
        self._shutdown_event = asyncio.Event()

        # Redis client (lazy init)
        self._redis = None

    def _create_spark_session(self) -> SparkSession:
        """Create or recreate the SparkSession."""
        if self._spark is not None:
            try:
                self._spark.stop()
            except Exception:
                pass

        structlog_logger.info("Creating SparkSession", master=self.config.master)

        builder = (
            SparkSession.builder
            .appName(self.config.app_name)
            .master(self.config.master)
            .config("spark.driver.memory", self.config.driver_memory)
            .config("spark.executor.memory", self.config.executor_memory)
            .config("spark.sql.warehouse.dir", self.config.warehouse_path)
            # Performance settings for local mode
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.default.parallelism", "4")
            # Disable UI for headless operation
            .config("spark.ui.enabled", "false")
            # Reduce logging noise
            .config("spark.sql.adaptive.enabled", "true")
        )

        self._spark = builder.getOrCreate()
        self._spark.sparkContext.setLogLevel("WARN")

        structlog_logger.info("SparkSession created",
                   version=self._spark.version,
                   app_id=self._spark.sparkContext.applicationId)

        return self._spark

    @property
    def spark(self) -> SparkSession:
        """Get the current SparkSession, creating if needed."""
        if self._spark is None:
            self._create_spark_session()
        return self._spark

    async def _get_redis(self):
        """Get Redis client (lazy initialization)."""
        if self._redis is None:
            import redis.asyncio as redis
            self._redis = redis.from_url(self.config.redis_url)
        return self._redis

    async def _dequeue_job(self) -> Optional[Dict[str, Any]]:
        """Get next job from queue."""
        import json

        redis = await self._get_redis()

        # Try each priority queue in order (0 = highest)
        for priority in range(4):
            queue_key = f"validation:queue:priority:{priority}"

            # ZPOPMIN gets lowest score (earliest job)
            result = await redis.zpopmin(queue_key, count=1)

            if result:
                job_data, score = result[0]
                job = json.loads(job_data)

                # Mark as processing
                job_key = f"validation:job:{job['id']}"
                await redis.hset(job_key, mapping={
                    "status": "processing",
                    "started_at": datetime.utcnow().isoformat(),
                })

                structlog_logger.info("Dequeued job",
                           job_id=job["id"],
                           priority=priority,
                           job_path=job.get("job_path"))

                return job

        return None

    async def _report_result(self, result: ValidationResult) -> None:
        """Report validation result back to queue/storage."""
        import json

        redis = await self._get_redis()
        job_key = f"validation:job:{result.job_id}"

        # Update job status
        status = "completed" if result.passed else "failed"

        result_data = {
            "status": status,
            "completed_at": result.completed_at.isoformat(),
            "duration_seconds": result.duration_seconds,
            "passed": result.passed,
            "stages": json.dumps([
                {
                    "stage": s.stage,
                    "passed": s.passed,
                    "duration_seconds": s.duration_seconds,
                    "issues": s.issues,
                }
                for s in result.stages
            ]),
        }

        if result.error:
            result_data["error"] = result.error

        await redis.hset(job_key, mapping=result_data)

        # Set TTL for job metadata (24 hours)
        await redis.expire(job_key, 86400)

        structlog_logger.info("Result reported",
                   job_id=result.job_id,
                   passed=result.passed,
                   duration=f"{result.duration_seconds:.2f}s")

    def validate_job(self, job_path: str, job_id: str) -> ValidationResult:
        """
        Execute all validation stages for a job.

        This runs synchronously using the SparkSession.
        """
        started_at = datetime.utcnow()
        stages: List[StageResult] = []
        overall_passed = True
        error_msg = None

        # Get validation logger
        vlog = get_validation_logger()
        if vlog:
            vlog.set_job_context(job_id, job_path)
            vlog.job_start(job_id, job_path)

        try:
            # Read the job file
            path = Path(job_path)
            if not path.exists():
                raise FileNotFoundError(f"Job file not found: {job_path}")

            source_code = path.read_text()

            # Stage 1: Python Syntax Check
            stage_result = self._run_stage("syntax", self._validate_syntax, source_code, job_path, vlog)
            stages.append(stage_result)
            if not stage_result.passed:
                overall_passed = False

            # Stage 2: Import Analysis
            stage_result = self._run_stage("imports", self._validate_imports, source_code, job_path, vlog)
            stages.append(stage_result)
            if not stage_result.passed:
                overall_passed = False

            # Stage 3: SQL Parsing (extract and parse SQL strings)
            stage_result = self._run_stage("sql_parse", self._validate_sql_strings, source_code, job_path, vlog)
            stages.append(stage_result)
            if not stage_result.passed:
                overall_passed = False

            # Stage 4: Logic Analysis
            stage_result = self._run_stage("logic", self._validate_logic, source_code, job_path, vlog)
            stages.append(stage_result)
            # Logic issues are usually warnings, don't fail overall

        except Exception as e:
            error_msg = str(e)
            overall_passed = False
            structlog_logger.error("Validation failed", job_path=job_path, error=error_msg)
            if vlog:
                vlog.error(f"Validation failed: {error_msg}", error=e)

        completed_at = datetime.utcnow()
        duration = (completed_at - started_at).total_seconds()
        stages_passed = sum(1 for s in stages if s.passed)

        # Log completion
        if vlog:
            vlog.job_complete(
                passed=overall_passed,
                duration_seconds=duration,
                stages_passed=stages_passed,
                stages_total=len(stages),
            )
            vlog.clear_job_context()

        return ValidationResult(
            job_id=job_id,
            job_path=job_path,
            passed=overall_passed,
            stages=stages,
            started_at=started_at,
            completed_at=completed_at,
            error=error_msg,
        )

    def _run_stage(
        self,
        stage_name: str,
        stage_func,
        source: str,
        job_path: str,
        vlog,
    ) -> StageResult:
        """Run a validation stage with logging."""
        if vlog:
            vlog.stage_start(stage_name)

        start_time = time.time()

        try:
            result = stage_func(source, job_path)
            duration = time.time() - start_time

            if vlog:
                vlog.stage_complete(
                    stage=stage_name,
                    passed=result.passed,
                    duration_seconds=duration,
                    issues=len(result.issues),
                )

                # Log individual issues
                for issue in result.issues:
                    if issue.get("severity") == "error":
                        vlog.error(
                            issue.get("message", "Unknown error"),
                            stage=stage_name,
                            line=issue.get("line"),
                            rule=issue.get("rule"),
                        )
                    elif issue.get("severity") == "warning":
                        vlog.warning(
                            issue.get("message", "Unknown warning"),
                            stage=stage_name,
                            line=issue.get("line"),
                            rule=issue.get("rule"),
                        )

            return result

        except Exception as e:
            duration = time.time() - start_time
            if vlog:
                vlog.stage_error(stage_name, e, duration)

            return StageResult(
                stage=stage_name,
                passed=False,
                duration_seconds=duration,
                issues=[{
                    "severity": "error",
                    "message": f"Stage exception: {str(e)}",
                }],
            )

    def _validate_syntax(self, source: str, job_path: str) -> StageResult:
        """Validate Python syntax using AST."""
        start = time.time()
        issues = []
        passed = True

        try:
            ast.parse(source)
        except SyntaxError as e:
            passed = False
            issues.append({
                "severity": "error",
                "message": f"Syntax error: {e.msg}",
                "line": e.lineno,
                "column": e.offset,
            })

        return StageResult(
            stage=ValidationStage.SYNTAX.value,
            passed=passed,
            duration_seconds=time.time() - start,
            issues=issues,
        )

    def _validate_imports(self, source: str, job_path: str) -> StageResult:
        """Analyze imports for potential issues."""
        start = time.time()
        issues = []
        passed = True

        try:
            tree = ast.parse(source)

            for node in ast.walk(tree):
                # Check for star imports
                if isinstance(node, ast.ImportFrom):
                    if any(alias.name == '*' for alias in node.names):
                        issues.append({
                            "severity": "warning",
                            "message": f"Star import from {node.module}",
                            "line": node.lineno,
                            "rule": "W001",
                        })

                # Check for deprecated imports
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    module = node.module if isinstance(node, ast.ImportFrom) else None
                    if module and 'pyspark.mllib' in module:
                        issues.append({
                            "severity": "warning",
                            "message": "pyspark.mllib is deprecated, use pyspark.ml",
                            "line": node.lineno,
                            "rule": "W002",
                        })

        except Exception as e:
            passed = False
            issues.append({
                "severity": "error",
                "message": f"Import analysis failed: {str(e)}",
            })

        return StageResult(
            stage=ValidationStage.IMPORTS.value,
            passed=passed,
            duration_seconds=time.time() - start,
            issues=issues,
        )

    def _validate_sql_strings(self, source: str, job_path: str) -> StageResult:
        """Extract SQL strings and validate with Spark's parser."""
        start = time.time()
        issues = []
        passed = True
        sql_count = 0

        # Pattern to find SQL in spark.sql() calls
        sql_patterns = [
            r'spark\.sql\s*\(\s*["\'\']\"\"(.*?)["\'\']\"\"',  # Triple quotes
            r'spark\.sql\s*\(\s*["\']([^"\']+)["\']',  # Single/double quotes
            r'\.filter\s*\(\s*["\']([^"\']+)["\']',  # Filter expressions
        ]

        # Also look for SQL in string assignments that look like SQL
        sql_keywords = r'\b(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|WITH)\b'

        try:
            # Find potential SQL strings
            for pattern in sql_patterns:
                for match in re.finditer(pattern, source, re.IGNORECASE | re.DOTALL):
                    sql = match.group(1).strip()
                    if sql and len(sql) > 10:  # Skip very short strings
                        sql_count += 1

                        # Try to parse with Spark
                        try:
                            # Use Spark's SQL parser to validate
                            self.spark.sql(f"EXPLAIN {sql}")
                        except ParseException as e:
                            issues.append({
                                "severity": "error",
                                "message": f"SQL parse error: {str(e)[:100]}",
                                "sql_preview": sql[:50] + "..." if len(sql) > 50 else sql,
                                "rule": "SQL001",
                            })
                            passed = False
                        except AnalysisException:
                            # Analysis errors (missing tables, etc.) are OK for validation
                            # We're just checking syntax
                            pass
                        except Exception:
                            # Other errors - SQL might reference undefined tables
                            pass

        except Exception as e:
            issues.append({
                "severity": "warning",
                "message": f"SQL extraction failed: {str(e)}",
            })

        return StageResult(
            stage=ValidationStage.SQL_PARSE.value,
            passed=passed,
            duration_seconds=time.time() - start,
            issues=issues,
            details={"sql_strings_found": sql_count},
        )

    def _validate_logic(self, source: str, job_path: str) -> StageResult:
        """Check for common logic issues and anti-patterns."""
        start = time.time()
        issues = []

        try:
            tree = ast.parse(source)

            for node in ast.walk(tree):
                # Bare except
                if isinstance(node, ast.ExceptHandler) and node.type is None:
                    issues.append({
                        "severity": "warning",
                        "message": "Bare 'except:' catches all exceptions",
                        "line": node.lineno,
                        "rule": "L001",
                    })

                # collect() without limit
                if isinstance(node, ast.Call):
                    if isinstance(node.func, ast.Attribute):
                        if node.func.attr == 'collect' and not node.args:
                            issues.append({
                                "severity": "warning",
                                "message": "collect() without limit may cause OOM",
                                "line": node.lineno,
                                "rule": "L002",
                            })

            # Check for hardcoded credentials
            credential_patterns = [
                (r'password\s*=\s*["\'][^"\']+["\']', "Hardcoded password"),
                (r'secret\s*=\s*["\'][^"\']+["\']', "Hardcoded secret"),
                (r'api_key\s*=\s*["\'][^"\']+["\']', "Hardcoded API key"),
            ]

            for pattern, message in credential_patterns:
                if re.search(pattern, source, re.IGNORECASE):
                    issues.append({
                        "severity": "error",
                        "message": message,
                        "rule": "SEC001",
                    })

            # Check for s3a usage (per CLAUDE.md)
            if 's3a://' in source:
                issues.append({
                    "severity": "warning",
                    "message": "Using s3a:// - consider using s3:// instead",
                    "rule": "L003",
                })

        except Exception as e:
            issues.append({
                "severity": "warning",
                "message": f"Logic analysis error: {str(e)}",
            })

        # Logic issues are warnings, always pass
        return StageResult(
            stage=ValidationStage.LOGIC.value,
            passed=True,
            duration_seconds=time.time() - start,
            issues=issues,
        )

    async def run(self) -> None:
        """Main execution loop."""
        self._running = True

        # Create initial SparkSession
        self._create_spark_session()

        structlog_logger.info("Spark Executor started",
                   poll_interval=self.config.poll_interval_seconds)

        while self._running and not self._shutdown_event.is_set():
            try:
                # Get next job from queue
                job = await self._dequeue_job()

                if job is None:
                    # No jobs available, sleep
                    await asyncio.sleep(self.config.idle_sleep_seconds)
                    continue

                # Execute validation (synchronous Spark operation)
                job_path = job.get("job_path", "")
                job_id = job.get("id", "unknown")

                structlog_logger.info("Validating job", job_id=job_id, job_path=job_path)

                # Run validation in executor to not block event loop
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(
                    None,
                    self.validate_job,
                    job_path,
                    job_id,
                )

                # Report result
                await self._report_result(result)

                self._jobs_processed += 1

                # Check if we need to restart session (memory cleanup)
                if self._jobs_processed >= self.config.max_jobs_before_restart:
                    structlog_logger.info("Restarting SparkSession for memory cleanup",
                               jobs_processed=self._jobs_processed)
                    self._create_spark_session()
                    self._jobs_processed = 0

                # Small delay between jobs
                await asyncio.sleep(self.config.poll_interval_seconds)

            except asyncio.CancelledError:
                break
            except Exception as e:
                structlog_logger.error("Executor error", error=str(e))
                await asyncio.sleep(self.config.idle_sleep_seconds)

        # Cleanup
        await self._shutdown()

    async def _shutdown(self) -> None:
        """Clean shutdown."""
        structlog_logger.info("Shutting down Spark Executor")

        if self._spark:
            try:
                self._spark.stop()
            except Exception:
                pass
            self._spark = None

        if self._redis:
            await self._redis.close()
            self._redis = None

        structlog_logger.info("Spark Executor stopped",
                   total_jobs_processed=self._jobs_processed)

    def stop(self) -> None:
        """Signal the executor to stop."""
        self._running = False
        self._shutdown_event.set()


async def run_executor(
    config: Optional[ExecutorConfig] = None,
    log_dir: Optional[str] = None,
    enable_file_logging: bool = True,
) -> None:
    """
    Run the Spark executor as a standalone service.

    Args:
        config: Executor configuration
        log_dir: Directory for log files (default: ./logs/validation)
        enable_file_logging: Whether to enable file backend for logs
    """
    executor = SparkValidationExecutor(config)

    # Initialize file logging backend
    log_backend_manager = None
    log_store = None

    if enable_file_logging:
        try:
            from validator.logging import (
                get_log_store,
                LogBackendManager,
                create_file_backend,
            )

            log_store = get_log_store()
            log_backend_manager = LogBackendManager()

            # Add file backend
            file_backend = create_file_backend(
                log_dir=log_dir or "./logs/validation",
                rotate_size_mb=100.0,
                max_files=30,
            )
            log_backend_manager.add_backend(file_backend)

            # Initialize backends
            await log_backend_manager.initialize()

            # Connect to log store
            log_store.set_backend_manager(log_backend_manager)
            await log_store.start_backend_flusher(interval_seconds=2.0)

            structlog_logger.info("File logging initialized", log_dir=log_dir or "./logs/validation")

        except Exception as e:
            structlog_logger.warning("Failed to initialize file logging", error=str(e))

    # Handle signals
    loop = asyncio.get_event_loop()

    def signal_handler():
        structlog_logger.info("Received shutdown signal")
        executor.stop()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, signal_handler)

    try:
        await executor.run()
    except KeyboardInterrupt:
        executor.stop()
    finally:
        # Cleanup logging
        if log_store:
            await log_store.stop_backend_flusher()
        if log_backend_manager:
            await log_backend_manager.close()


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Spark Validation Executor - Local execution with session reuse"
    )
    parser.add_argument(
        "--redis-url",
        default="redis://localhost:6379/0",
        help="Redis connection URL",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=2.0,
        help="Queue polling interval in seconds",
    )
    parser.add_argument(
        "--driver-memory",
        default="2g",
        help="Spark driver memory",
    )
    parser.add_argument(
        "--max-jobs",
        type=int,
        default=100,
        help="Max jobs before session restart",
    )
    parser.add_argument(
        "--log-dir",
        default="./logs/validation",
        help="Directory for validation log files",
    )
    parser.add_argument(
        "--no-file-logging",
        action="store_true",
        help="Disable file logging (logs still go to memory store)",
    )

    args = parser.parse_args()

    config = ExecutorConfig(
        redis_url=args.redis_url,
        poll_interval_seconds=args.poll_interval,
        driver_memory=args.driver_memory,
        max_jobs_before_restart=args.max_jobs,
    )

    print("=" * 60)
    print("Spark Validation Executor")
    print("=" * 60)
    print(f"  Redis:         {config.redis_url}")
    print(f"  Poll Interval: {config.poll_interval_seconds}s")
    print(f"  Driver Memory: {config.driver_memory}")
    print(f"  Max Jobs:      {config.max_jobs_before_restart}")
    print(f"  Log Dir:       {args.log_dir}")
    print(f"  File Logging:  {'Disabled' if args.no_file_logging else 'Enabled'}")
    print("=" * 60)
    print("Press Ctrl+C to stop")
    print()

    asyncio.run(run_executor(
        config=config,
        log_dir=args.log_dir,
        enable_file_logging=not args.no_file_logging,
    ))


if __name__ == "__main__":
    main()
