"""
Structured Logging Utilities for STG_XTC_LD_TEST_JOB

This module provides structured logging with JSON format support,
making it easy to aggregate and analyze logs in production.

Benefits:
- Structured JSON logs for log aggregation tools (Splunk, ELK)
- Automatic DataFrame row count tracking
- Execution time measurement
- Stage-based logging for easy debugging

Usage:
    from utils.config_loader import load_config
    from utils.logger import JobLogger, create_logger

    config = load_config(environment="prod")
    logger = create_logger(config, "STG_XTC_LD_TEST_JOB")

    # Log stage execution
    logger.log_stage("V138S0", "start")
    df = spark.sql(sql)
    logger.log_dataframe(df, "Acct_dtal_driving_df", "V138S0")
    logger.log_stage("V138S0", "complete", row_count=df.count())
"""

import logging
import json
import time
from datetime import datetime
from typing import Optional, Any, Dict
from pyspark.sql import DataFrame
import sys
from pathlib import Path

# Add parent directory to import config
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.job_config_schema import JobConfig


class JobLogger:
    """
    Structured logger for ETL jobs.

    Provides JSON-formatted logging with automatic DataFrame
    row count tracking and execution time measurement.
    """

    def __init__(
        self,
        name: str,
        level: str = "INFO",
        format_type: str = "json",
        log_schemas: bool = True,
        log_row_counts: bool = True
    ):
        """
        Initialize job logger.

        Args:
            name: Logger name (typically job name)
            level: Log level (DEBUG, INFO, WARN, ERROR)
            format_type: Log format ('json' or 'text')
            log_schemas: Whether to log DataFrame schemas
            log_row_counts: Whether to log DataFrame row counts
        """
        self.name = name
        self.format_type = format_type
        self.log_schemas = log_schemas
        self.log_row_counts = log_row_counts

        # Create logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, level.upper()))

        # Remove existing handlers to avoid duplicates
        self.logger.handlers.clear()

        # Create console handler
        handler = logging.StreamHandler()
        handler.setLevel(getattr(logging, level.upper()))

        # Set formatter based on format type
        if format_type == "json":
            formatter = JsonFormatter()
        else:
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )

        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        # Track stage execution times
        self._stage_start_times: Dict[str, float] = {}

    def log_stage(
        self,
        stage: str,
        status: str,
        **kwargs
    ):
        """
        Log stage execution status.

        Args:
            stage: Stage identifier (e.g., "V138S0", "JOB")
            status: Status message (e.g., "start", "complete", "failed")
            **kwargs: Additional fields to include in log

        Example:
            logger.log_stage("V138S0", "start")
            # ... process data ...
            logger.log_stage("V138S0", "complete", row_count=1234)
        """
        # Track start time
        if status == "start":
            self._stage_start_times[stage] = time.time()

        # Calculate duration if this is a completion
        duration_sec = None
        if status in ("complete", "success", "failed") and stage in self._stage_start_times:
            duration_sec = time.time() - self._stage_start_times[stage]
            del self._stage_start_times[stage]

        # Build log message
        log_data = {
            "stage": stage,
            "status": status,
            "timestamp": datetime.now().isoformat(),
            **kwargs
        }

        if duration_sec is not None:
            log_data["duration_sec"] = round(duration_sec, 2)

        # Log at appropriate level
        if status in ("failed", "error"):
            self._log(logging.ERROR, log_data)
        elif status in ("warning", "warn"):
            self._log(logging.WARNING, log_data)
        else:
            self._log(logging.INFO, log_data)

    def log_dataframe(
        self,
        df: DataFrame,
        name: str,
        stage: str,
        count: Optional[int] = None
    ):
        """
        Log DataFrame information.

        Args:
            df: DataFrame to log
            name: DataFrame name/identifier
            stage: Stage identifier
            count: Optional pre-computed row count (avoids counting again)

        Example:
            logger.log_dataframe(df, "Acct_dtal_driving_df", "V138S0")
        """
        log_data = {
            "stage": stage,
            "dataframe": name,
            "timestamp": datetime.now().isoformat()
        }

        # Add row count if enabled
        if self.log_row_counts:
            if count is not None:
                log_data["row_count"] = count
            else:
                log_data["row_count"] = df.count()

        # Add schema if enabled
        if self.log_schemas:
            log_data["columns"] = df.columns
            log_data["schema"] = str(df.schema.simpleString())

        self._log(logging.INFO, log_data)

    def log_sql(self, stage: str, sql: str, truncate_at: int = 500):
        """
        Log SQL query being executed.

        Args:
            stage: Stage identifier
            sql: SQL query string
            truncate_at: Truncate SQL at this length (default 500 chars)

        Example:
            logger.log_sql("V138S0", sql_query)
        """
        # Truncate very long SQL for readability
        sql_display = sql if len(sql) <= truncate_at else sql[:truncate_at] + "..."

        log_data = {
            "stage": stage,
            "action": "executing_sql",
            "sql": sql_display,
            "sql_length": len(sql),
            "timestamp": datetime.now().isoformat()
        }

        self._log(logging.DEBUG, log_data)

    def log_metrics(
        self,
        stage: str,
        metrics: Dict[str, Any]
    ):
        """
        Log custom metrics.

        Args:
            stage: Stage identifier
            metrics: Dictionary of metric name -> value

        Example:
            logger.log_metrics("V138S0", {
                "input_rows": 1000,
                "output_rows": 950,
                "filtered_rows": 50
            })
        """
        log_data = {
            "stage": stage,
            "metrics": metrics,
            "timestamp": datetime.now().isoformat()
        }

        self._log(logging.INFO, log_data)

    def log_error(
        self,
        stage: str,
        error: Exception,
        **kwargs
    ):
        """
        Log exception with context.

        Args:
            stage: Stage identifier
            error: Exception object
            **kwargs: Additional context

        Example:
            try:
                df = spark.sql(sql)
            except Exception as e:
                logger.log_error("V138S0", e, sql=sql)
                raise
        """
        log_data = {
            "stage": stage,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "timestamp": datetime.now().isoformat(),
            **kwargs
        }

        self._log(logging.ERROR, log_data)

    def _log(self, level: int, data: Any):
        """
        Internal logging method.

        Args:
            level: Logging level
            data: Data to log (will be formatted based on format_type)
        """
        if self.format_type == "json":
            # Formatter will handle JSON conversion
            self.logger.log(level, data)
        else:
            # Convert to string for text format
            if isinstance(data, dict):
                message = " | ".join(f"{k}={v}" for k, v in data.items())
            else:
                message = str(data)
            self.logger.log(level, message)

    def info(self, message: str, **kwargs):
        """Convenience method for INFO level logging."""
        self._log(logging.INFO, {"message": message, **kwargs})

    def warning(self, message: str, **kwargs):
        """Convenience method for WARNING level logging."""
        self._log(logging.WARNING, {"message": message, **kwargs})

    def error(self, message: str, **kwargs):
        """Convenience method for ERROR level logging."""
        self._log(logging.ERROR, {"message": message, **kwargs})

    def debug(self, message: str, **kwargs):
        """Convenience method for DEBUG level logging."""
        self._log(logging.DEBUG, {"message": message, **kwargs})


class JsonFormatter(logging.Formatter):
    """
    Custom formatter that outputs logs as JSON.

    This makes logs easy to parse and aggregate with tools
    like Splunk, ELK, or CloudWatch.
    """

    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as JSON.

        Args:
            record: Log record to format

        Returns:
            JSON-formatted log string
        """
        # If message is already a dict, use it directly
        if isinstance(record.msg, dict):
            log_data = record.msg.copy()
        else:
            log_data = {"message": str(record.msg)}

        # Add standard fields
        log_data.update({
            "level": record.levelname,
            "logger": record.name,
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
        })

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data)


# Convenience function
def create_logger(
    config: JobConfig,
    job_name: str
) -> JobLogger:
    """
    Create logger from configuration.

    Args:
        config: JobConfig instance
        job_name: Job name for logger

    Returns:
        Configured JobLogger instance

    Example:
        from utils.config_loader import load_config
        from utils.logger import create_logger

        config = load_config(environment="prod")
        logger = create_logger(config, "STG_XTC_LD_TEST_JOB")
    """
    return JobLogger(
        name=job_name,
        level=config.logging.level,
        format_type=config.logging.format,
        log_schemas=config.logging.log_schemas,
        log_row_counts=config.logging.log_row_counts
    )
