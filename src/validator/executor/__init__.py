"""Spark Validation Executor - Local execution with session reuse."""

from validator.executor.spark_executor import (
    SparkValidationExecutor,
    ExecutorConfig,
    ValidationResult,
    StageResult,
    run_executor,
)

__all__ = [
    "SparkValidationExecutor",
    "ExecutorConfig",
    "ValidationResult",
    "StageResult",
    "run_executor",
]
