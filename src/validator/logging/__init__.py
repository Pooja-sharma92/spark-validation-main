"""Structured validation logging for UI display."""

from validator.logging.validation_logger import (
    ValidationLogger,
    ValidationLogStore,
    ValidationLogEntry,
    LogLevel,
    LogCategory,
    JobLogContext,
    get_log_store,
    get_logger,
)

from validator.logging.backends import (
    LogBackend,
    LogBackendManager,
    FileLogBackend,
    FileBackendConfig,
    PostgresLogBackend,
    PostgresBackendConfig,
    ElasticsearchLogBackend,
    ElasticsearchBackendConfig,
    create_file_backend,
    create_postgres_backend,
    create_elasticsearch_backend,
)

__all__ = [
    # Core logging
    "ValidationLogger",
    "ValidationLogStore",
    "ValidationLogEntry",
    "LogLevel",
    "LogCategory",
    "JobLogContext",
    "get_log_store",
    "get_logger",
    # Backends
    "LogBackend",
    "LogBackendManager",
    "FileLogBackend",
    "FileBackendConfig",
    "PostgresLogBackend",
    "PostgresBackendConfig",
    "ElasticsearchLogBackend",
    "ElasticsearchBackendConfig",
    # Factory functions
    "create_file_backend",
    "create_postgres_backend",
    "create_elasticsearch_backend",
]
