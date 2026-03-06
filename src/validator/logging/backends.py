"""
Log Storage Backends.

Supports multiple storage destinations:
- File: JSON lines format, with rotation
- PostgreSQL: Async database storage
- Elasticsearch: Full-text search support
- Memory: In-memory buffer (default)

Multiple backends can be active simultaneously.
"""

import asyncio
import json
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from queue import Queue, Empty
from threading import Thread, Lock
from typing import Optional, List, Dict, Any, Callable
import gzip
import shutil

from validator.logging.validation_logger import (
    ValidationLogEntry,
    LogLevel,
    LogCategory,
)


class LogBackend(ABC):
    """Abstract base class for log storage backends."""

    @abstractmethod
    async def write(self, entry: ValidationLogEntry) -> bool:
        """Write a single log entry. Returns True on success."""
        pass

    @abstractmethod
    async def write_batch(self, entries: List[ValidationLogEntry]) -> int:
        """Write multiple entries. Returns count of successful writes."""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close the backend and flush any pending writes."""
        pass

    async def initialize(self) -> None:
        """Initialize the backend (optional)."""
        pass


# ─────────────────────────────────────────────────────────────────
# File Backend
# ─────────────────────────────────────────────────────────────────

@dataclass
class FileBackendConfig:
    """Configuration for file-based log storage."""
    # Base directory for log files
    log_dir: str = "./logs/validation"

    # File naming
    file_prefix: str = "validation"
    file_extension: str = ".jsonl"

    # Rotation settings
    rotate_size_mb: float = 100.0  # Rotate when file exceeds this size
    rotate_interval_hours: int = 24  # Rotate every N hours
    max_files: int = 30  # Keep last N files

    # Compression
    compress_rotated: bool = True  # Gzip rotated files

    # Buffering
    buffer_size: int = 100  # Flush after N entries
    flush_interval_seconds: float = 5.0  # Flush every N seconds


class FileLogBackend(LogBackend):
    """
    File-based log storage using JSON Lines format.

    Features:
    - Automatic file rotation by size and time
    - Optional gzip compression for rotated files
    - Buffered writes for performance
    - Thread-safe write queue
    """

    def __init__(self, config: Optional[FileBackendConfig] = None):
        self.config = config or FileBackendConfig()
        self._buffer: List[ValidationLogEntry] = []
        self._buffer_lock = Lock()
        self._current_file: Optional[Path] = None
        self._current_file_handle = None
        self._last_rotation: datetime = datetime.utcnow()
        self._running = False
        self._flush_task: Optional[asyncio.Task] = None

        # Ensure log directory exists
        Path(self.config.log_dir).mkdir(parents=True, exist_ok=True)

    async def initialize(self) -> None:
        """Initialize the file backend."""
        self._open_new_file()
        self._running = True
        self._flush_task = asyncio.create_task(self._periodic_flush())

    def _open_new_file(self) -> Path:
        """Open a new log file."""
        if self._current_file_handle:
            self._current_file_handle.close()

        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.config.file_prefix}_{timestamp}{self.config.file_extension}"
        self._current_file = Path(self.config.log_dir) / filename
        self._current_file_handle = open(self._current_file, "a", encoding="utf-8")
        self._last_rotation = datetime.utcnow()

        return self._current_file

    def _should_rotate(self) -> bool:
        """Check if log file should be rotated."""
        if not self._current_file or not self._current_file.exists():
            return True

        # Check size
        size_mb = self._current_file.stat().st_size / (1024 * 1024)
        if size_mb >= self.config.rotate_size_mb:
            return True

        # Check time
        hours_since_rotation = (
            datetime.utcnow() - self._last_rotation
        ).total_seconds() / 3600
        if hours_since_rotation >= self.config.rotate_interval_hours:
            return True

        return False

    def _rotate_file(self) -> None:
        """Rotate the current log file."""
        if not self._current_file:
            return

        old_file = self._current_file

        # Open new file
        self._open_new_file()

        # Compress old file if configured
        if self.config.compress_rotated and old_file.exists():
            compressed_path = old_file.with_suffix(old_file.suffix + ".gz")
            with open(old_file, "rb") as f_in:
                with gzip.open(compressed_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
            old_file.unlink()

        # Cleanup old files
        self._cleanup_old_files()

    def _cleanup_old_files(self) -> None:
        """Remove old log files beyond max_files limit."""
        log_dir = Path(self.config.log_dir)
        pattern = f"{self.config.file_prefix}_*"

        files = sorted(log_dir.glob(pattern), key=lambda f: f.stat().st_mtime)

        while len(files) > self.config.max_files:
            oldest = files.pop(0)
            oldest.unlink()

    async def write(self, entry: ValidationLogEntry) -> bool:
        """Buffer a log entry for writing."""
        with self._buffer_lock:
            self._buffer.append(entry)

            if len(self._buffer) >= self.config.buffer_size:
                await self._flush()

        return True

    async def write_batch(self, entries: List[ValidationLogEntry]) -> int:
        """Write multiple entries."""
        with self._buffer_lock:
            self._buffer.extend(entries)

            if len(self._buffer) >= self.config.buffer_size:
                await self._flush()

        return len(entries)

    async def _flush(self) -> None:
        """Flush buffered entries to file."""
        with self._buffer_lock:
            if not self._buffer:
                return

            entries_to_write = self._buffer.copy()
            self._buffer.clear()

        if self._should_rotate():
            self._rotate_file()

        if not self._current_file_handle:
            self._open_new_file()

        for entry in entries_to_write:
            line = json.dumps(entry.to_dict(), ensure_ascii=False)
            self._current_file_handle.write(line + "\n")

        self._current_file_handle.flush()

    async def _periodic_flush(self) -> None:
        """Periodically flush buffer."""
        while self._running:
            await asyncio.sleep(self.config.flush_interval_seconds)
            try:
                await self._flush()
            except Exception:
                pass

    async def close(self) -> None:
        """Close the backend."""
        self._running = False

        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass

        await self._flush()

        if self._current_file_handle:
            self._current_file_handle.close()
            self._current_file_handle = None


# ─────────────────────────────────────────────────────────────────
# PostgreSQL Backend
# ─────────────────────────────────────────────────────────────────

@dataclass
class PostgresBackendConfig:
    """Configuration for PostgreSQL log storage."""
    host: str = "localhost"
    port: int = 5432
    database: str = "validation_logs"
    user: str = "validator"
    password: str = ""
    table_name: str = "validation_logs"

    # Connection pool
    min_connections: int = 2
    max_connections: int = 10

    # Batching
    batch_size: int = 50
    flush_interval_seconds: float = 2.0


class PostgresLogBackend(LogBackend):
    """
    PostgreSQL-based log storage.

    Features:
    - Async writes via asyncpg
    - Batched inserts for performance
    - Automatic table creation
    """

    def __init__(self, config: Optional[PostgresBackendConfig] = None):
        self.config = config or PostgresBackendConfig()
        self._pool = None
        self._buffer: List[ValidationLogEntry] = []
        self._buffer_lock = Lock()
        self._running = False
        self._flush_task: Optional[asyncio.Task] = None

    async def initialize(self) -> None:
        """Initialize database connection and table."""
        try:
            import asyncpg
        except ImportError:
            raise RuntimeError("asyncpg is required for PostgreSQL backend")

        self._pool = await asyncpg.create_pool(
            host=self.config.host,
            port=self.config.port,
            database=self.config.database,
            user=self.config.user,
            password=self.config.password,
            min_size=self.config.min_connections,
            max_size=self.config.max_connections,
        )

        # Create table if not exists
        await self._create_table()

        self._running = True
        self._flush_task = asyncio.create_task(self._periodic_flush())

    async def _create_table(self) -> None:
        """Create the logs table if it doesn't exist."""
        async with self._pool.acquire() as conn:
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.config.table_name} (
                    id VARCHAR(16) PRIMARY KEY,
                    timestamp TIMESTAMPTZ NOT NULL,
                    level VARCHAR(16) NOT NULL,
                    category VARCHAR(32) NOT NULL,
                    message TEXT NOT NULL,
                    job_id VARCHAR(64),
                    job_path TEXT,
                    batch_id VARCHAR(64),
                    stage VARCHAR(32),
                    stage_status VARCHAR(16),
                    module VARCHAR(64) NOT NULL,
                    component VARCHAR(64),
                    error_type VARCHAR(128),
                    error_message TEXT,
                    error_traceback TEXT,
                    duration_seconds FLOAT,
                    metadata JSONB,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_{self.config.table_name}_timestamp
                    ON {self.config.table_name} (timestamp DESC);

                CREATE INDEX IF NOT EXISTS idx_{self.config.table_name}_job_id
                    ON {self.config.table_name} (job_id) WHERE job_id IS NOT NULL;

                CREATE INDEX IF NOT EXISTS idx_{self.config.table_name}_level
                    ON {self.config.table_name} (level);
            """)

    async def write(self, entry: ValidationLogEntry) -> bool:
        """Buffer a log entry."""
        with self._buffer_lock:
            self._buffer.append(entry)

            if len(self._buffer) >= self.config.batch_size:
                await self._flush()

        return True

    async def write_batch(self, entries: List[ValidationLogEntry]) -> int:
        """Write multiple entries."""
        with self._buffer_lock:
            self._buffer.extend(entries)

            if len(self._buffer) >= self.config.batch_size:
                await self._flush()

        return len(entries)

    async def _flush(self) -> None:
        """Flush buffered entries to database."""
        with self._buffer_lock:
            if not self._buffer:
                return
            entries = self._buffer.copy()
            self._buffer.clear()

        if not self._pool:
            return

        async with self._pool.acquire() as conn:
            await conn.executemany(f"""
                INSERT INTO {self.config.table_name} (
                    id, timestamp, level, category, message,
                    job_id, job_path, batch_id, stage, stage_status,
                    module, component, error_type, error_message,
                    error_traceback, duration_seconds, metadata
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    $11, $12, $13, $14, $15, $16, $17
                )
                ON CONFLICT (id) DO NOTHING
            """, [
                (
                    e.id,
                    e.timestamp,
                    e.level.value,
                    e.category.value,
                    e.message,
                    e.job_id,
                    e.job_path,
                    e.batch_id,
                    e.stage,
                    e.stage_status,
                    e.module,
                    e.component,
                    e.error_type,
                    e.error_message,
                    e.error_traceback,
                    e.duration_seconds,
                    json.dumps(e.metadata) if e.metadata else None,
                )
                for e in entries
            ])

    async def _periodic_flush(self) -> None:
        """Periodically flush buffer."""
        while self._running:
            await asyncio.sleep(self.config.flush_interval_seconds)
            try:
                await self._flush()
            except Exception:
                pass

    async def close(self) -> None:
        """Close the backend."""
        self._running = False

        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass

        await self._flush()

        if self._pool:
            await self._pool.close()


# ─────────────────────────────────────────────────────────────────
# Elasticsearch Backend
# ─────────────────────────────────────────────────────────────────

@dataclass
class ElasticsearchBackendConfig:
    """Configuration for Elasticsearch log storage."""
    hosts: List[str] = field(default_factory=lambda: ["http://localhost:9200"])
    index_prefix: str = "validation-logs"
    index_date_format: str = "%Y.%m.%d"  # Daily indices

    # Authentication (optional)
    username: Optional[str] = None
    password: Optional[str] = None
    api_key: Optional[str] = None

    # Batching
    batch_size: int = 100
    flush_interval_seconds: float = 5.0


class ElasticsearchLogBackend(LogBackend):
    """
    Elasticsearch-based log storage.

    Features:
    - Full-text search on log messages
    - Time-based indices for easy retention management
    - Bulk indexing for performance
    """

    def __init__(self, config: Optional[ElasticsearchBackendConfig] = None):
        self.config = config or ElasticsearchBackendConfig()
        self._client = None
        self._buffer: List[ValidationLogEntry] = []
        self._buffer_lock = Lock()
        self._running = False
        self._flush_task: Optional[asyncio.Task] = None

    async def initialize(self) -> None:
        """Initialize Elasticsearch client."""
        try:
            from elasticsearch import AsyncElasticsearch
        except ImportError:
            raise RuntimeError("elasticsearch[async] is required for ES backend")

        client_kwargs = {"hosts": self.config.hosts}

        if self.config.api_key:
            client_kwargs["api_key"] = self.config.api_key
        elif self.config.username and self.config.password:
            client_kwargs["basic_auth"] = (
                self.config.username,
                self.config.password,
            )

        self._client = AsyncElasticsearch(**client_kwargs)

        # Create index template
        await self._create_index_template()

        self._running = True
        self._flush_task = asyncio.create_task(self._periodic_flush())

    async def _create_index_template(self) -> None:
        """Create index template for log indices."""
        template = {
            "index_patterns": [f"{self.config.index_prefix}-*"],
            "template": {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,
                },
                "mappings": {
                    "properties": {
                        "timestamp": {"type": "date"},
                        "level": {"type": "keyword"},
                        "category": {"type": "keyword"},
                        "message": {"type": "text"},
                        "job_id": {"type": "keyword"},
                        "job_path": {"type": "keyword"},
                        "batch_id": {"type": "keyword"},
                        "stage": {"type": "keyword"},
                        "stage_status": {"type": "keyword"},
                        "module": {"type": "keyword"},
                        "component": {"type": "keyword"},
                        "error_type": {"type": "keyword"},
                        "error_message": {"type": "text"},
                        "duration_seconds": {"type": "float"},
                        "metadata": {"type": "object", "enabled": False},
                    }
                }
            }
        }

        await self._client.indices.put_index_template(
            name=f"{self.config.index_prefix}-template",
            body=template,
        )

    def _get_index_name(self, entry: ValidationLogEntry) -> str:
        """Get index name for an entry based on timestamp."""
        date_str = entry.timestamp.strftime(self.config.index_date_format)
        return f"{self.config.index_prefix}-{date_str}"

    async def write(self, entry: ValidationLogEntry) -> bool:
        """Buffer a log entry."""
        with self._buffer_lock:
            self._buffer.append(entry)

            if len(self._buffer) >= self.config.batch_size:
                await self._flush()

        return True

    async def write_batch(self, entries: List[ValidationLogEntry]) -> int:
        """Write multiple entries."""
        with self._buffer_lock:
            self._buffer.extend(entries)

            if len(self._buffer) >= self.config.batch_size:
                await self._flush()

        return len(entries)

    async def _flush(self) -> None:
        """Flush buffered entries to Elasticsearch."""
        with self._buffer_lock:
            if not self._buffer:
                return
            entries = self._buffer.copy()
            self._buffer.clear()

        if not self._client:
            return

        # Build bulk request
        actions = []
        for entry in entries:
            actions.append({
                "index": {
                    "_index": self._get_index_name(entry),
                    "_id": entry.id,
                }
            })
            actions.append(entry.to_dict())

        await self._client.bulk(operations=actions)

    async def _periodic_flush(self) -> None:
        """Periodically flush buffer."""
        while self._running:
            await asyncio.sleep(self.config.flush_interval_seconds)
            try:
                await self._flush()
            except Exception:
                pass

    async def close(self) -> None:
        """Close the backend."""
        self._running = False

        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass

        await self._flush()

        if self._client:
            await self._client.close()


# ─────────────────────────────────────────────────────────────────
# Multi-Backend Manager
# ─────────────────────────────────────────────────────────────────

class LogBackendManager:
    """
    Manages multiple log backends.

    Writes to all configured backends simultaneously.
    Handles backend failures gracefully.
    """

    def __init__(self):
        self._backends: List[LogBackend] = []
        self._initialized = False

    def add_backend(self, backend: LogBackend) -> None:
        """Add a backend to the manager."""
        self._backends.append(backend)

    async def initialize(self) -> None:
        """Initialize all backends."""
        for backend in self._backends:
            try:
                await backend.initialize()
            except Exception as e:
                print(f"Failed to initialize backend {type(backend).__name__}: {e}")

        self._initialized = True

    async def write(self, entry: ValidationLogEntry) -> None:
        """Write to all backends."""
        for backend in self._backends:
            try:
                await backend.write(entry)
            except Exception:
                pass  # Log but don't fail

    async def write_batch(self, entries: List[ValidationLogEntry]) -> None:
        """Write batch to all backends."""
        for backend in self._backends:
            try:
                await backend.write_batch(entries)
            except Exception:
                pass

    async def close(self) -> None:
        """Close all backends."""
        for backend in self._backends:
            try:
                await backend.close()
            except Exception:
                pass


# ─────────────────────────────────────────────────────────────────
# Factory Functions
# ─────────────────────────────────────────────────────────────────

def create_file_backend(
    log_dir: str = "./logs/validation",
    rotate_size_mb: float = 100.0,
    max_files: int = 30,
) -> FileLogBackend:
    """Create a file log backend."""
    config = FileBackendConfig(
        log_dir=log_dir,
        rotate_size_mb=rotate_size_mb,
        max_files=max_files,
    )
    return FileLogBackend(config)


def create_postgres_backend(
    host: str = "localhost",
    port: int = 5432,
    database: str = "validation_logs",
    user: str = "validator",
    password: str = "",
) -> PostgresLogBackend:
    """Create a PostgreSQL log backend."""
    config = PostgresBackendConfig(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )
    return PostgresLogBackend(config)


def create_elasticsearch_backend(
    hosts: List[str] = None,
    index_prefix: str = "validation-logs",
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> ElasticsearchLogBackend:
    """Create an Elasticsearch log backend."""
    config = ElasticsearchBackendConfig(
        hosts=hosts or ["http://localhost:9200"],
        index_prefix=index_prefix,
        username=username,
        password=password,
    )
    return ElasticsearchLogBackend(config)
