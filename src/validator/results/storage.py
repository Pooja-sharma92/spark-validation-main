"""
Result Storage - Persistent storage for validation results.

Uses PostgreSQL for durable storage with:
- Async operations via asyncpg
- Repository pattern for clean data access
- Support for querying historical results
"""

import json
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
from dataclasses import dataclass
import asyncio
import structlog

try:
    import asyncpg
    ASYNCPG_AVAILABLE = True
except ImportError:
    asyncpg = None
    ASYNCPG_AVAILABLE = False

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


@dataclass
class StorageConfig:
    """Configuration for result storage."""
    host: str = "localhost"
    port: int = 5432
    database: str = "validation_results"
    user: str = "validator"
    password: str = ""
    min_connections: int = 2
    max_connections: int = 10


class ResultStorage:
    """
    PostgreSQL-based storage for validation results.

    Provides async methods for storing and querying validation
    results, with support for historical queries and statistics.
    """

    def __init__(self, config: Optional[StorageConfig] = None):
        self.config = config or self._load_config()
        self._pool: Optional["asyncpg.Pool"] = None

    def _load_config(self) -> StorageConfig:
        """
        Load config from framework configuration.

        NOTE:
        - This reads from validator.config.get_config()
        - create_storage() can override values at runtime (container networking)
        """
        framework_config = get_config()
        pg = framework_config.postgres
        return StorageConfig(
            host=pg.host,
            port=pg.port,
            database=pg.database,
            user=pg.user,
            password=pg.password,
        )

    async def initialize(self) -> None:
        """Initialize database connection pool."""
        if not ASYNCPG_AVAILABLE:
            logger.warning("asyncpg not available, storage disabled")
            return

        try:
            self._pool = await asyncpg.create_pool(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
                min_size=self.config.min_connections,
                max_size=self.config.max_connections,
            )
            logger.info(
                "Result storage initialized",
                database=self.config.database,
                host=self.config.host,
                port=self.config.port,
            )
        except Exception as e:
            logger.error("Failed to initialize storage", error=str(e))
            raise

    async def close(self) -> None:
        """Close database connection pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def store_request(self, request: ValidationRequest) -> None:
        """Store a validation request."""
        if not self._pool:
            return

        async with self._pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO validation_requests (
                    id, job_path, trigger_source, priority, batch_id,
                    commit_sha, branch, triggered_by, status, created_at, metadata
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (id) DO UPDATE SET
                    status = EXCLUDED.status,
                    metadata = EXCLUDED.metadata
            """,
                request.id,
                request.job_path,
                request.trigger_source.value,
                request.priority.value,
                request.batch_id,
                request.commit_sha,
                request.branch,
                request.triggered_by,
                ValidationStatus.PENDING.value,
                request.created_at,
                json.dumps(request.metadata),
            )

    async def update_request_status(
        self,
        request_id: str,
        status: ValidationStatus,
        started_at: Optional[datetime] = None,
        completed_at: Optional[datetime] = None,
        worker_id: Optional[str] = None,
    ) -> None:
        """Update the status of a validation request."""
        if not self._pool:
            return

        async with self._pool.acquire() as conn:
            await conn.execute("""
                UPDATE validation_requests
                SET status = $2,
                    started_at = COALESCE($3, started_at),
                    completed_at = COALESCE($4, completed_at),
                    worker_id = COALESCE($5, worker_id)
                WHERE id = $1
            """,
                request_id,
                status.value,
                started_at,
                completed_at,
                worker_id,
            )

    async def store_result(self, result: ValidationResult) -> None:
        """
        Store a complete validation result with all stages and issues.

        Uses a transaction to ensure atomicity.
        """
        if not self._pool:
            return

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("""
                    UPDATE validation_requests
                    SET status = $2,
                        started_at = $3,
                        completed_at = $4,
                        worker_id = $5
                    WHERE id = $1
                """,
                    result.request_id,
                    result.status.value,
                    result.started_at,
                    result.completed_at,
                    result.worker_id,
                )

                result_id = await conn.fetchval("""
                    INSERT INTO validation_results (
                        request_id, passed, summary, duration_seconds,
                        error_message, error_traceback
                    ) VALUES ($1, $2, $3, $4, $5, $6)
                    RETURNING id
                """,
                    result.request_id,
                    result.passed,
                    json.dumps(result.summary),
                    result.duration_seconds,
                    result.error_message,
                    result.error_traceback,
                )

                for stage_result in result.stage_results:
                    stage_id = await conn.fetchval("""
                        INSERT INTO stage_results (
                            result_id, stage, status, duration_seconds, metrics
                        ) VALUES ($1, $2, $3, $4, $5)
                        RETURNING id
                    """,
                        result_id,
                        stage_result.stage.value,
                        stage_result.status.value,
                        stage_result.duration_seconds,
                        json.dumps(stage_result.metrics),
                    )

                    if stage_result.issues:
                        await conn.executemany("""
                            INSERT INTO validation_issues (
                                stage_result_id, severity, message,
                                file_path, line_number, rule_id, suggestion
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                        """,
                            [
                                (
                                    stage_id,
                                    issue.severity.value,
                                    issue.message,
                                    issue.file_path,
                                    issue.line_number,
                                    issue.rule_id,
                                    issue.suggestion,
                                )
                                for issue in stage_result.issues
                            ],
                        )

        logger.info(
            "Stored validation result",
            request_id=result.request_id,
            passed=result.passed,
        )

    async def get_result(self, request_id: str) -> Optional[ValidationResult]:
        """Retrieve a validation result by request ID."""
        if not self._pool:
            return None

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT
                    r.id, r.job_path, r.status, r.started_at, r.completed_at, r.worker_id,
                    vr.passed, vr.error_message, vr.error_traceback
                FROM validation_requests r
                LEFT JOIN validation_results vr ON vr.request_id = r.id
                WHERE r.id = $1
            """, request_id)

            if not row:
                return None

            stage_rows = await conn.fetch("""
                SELECT sr.id, sr.stage, sr.status, sr.duration_seconds, sr.metrics
                FROM stage_results sr
                JOIN validation_results vr ON vr.id = sr.result_id
                WHERE vr.request_id = $1
                ORDER BY sr.created_at
            """, request_id)

            stage_results = []
            for sr in stage_rows:
                issue_rows = await conn.fetch("""
                    SELECT severity, message, file_path, line_number, rule_id, suggestion
                    FROM validation_issues
                    WHERE stage_result_id = $1
                """, sr['id'])

                issues = [
                    ValidationIssue(
                        stage=ValidationStage(sr['stage']),
                        severity=Severity(ir['severity']),
                        message=ir['message'],
                        file_path=ir['file_path'],
                        line_number=ir['line_number'],
                        rule_id=ir['rule_id'],
                        suggestion=ir['suggestion'],
                    )
                    for ir in issue_rows
                ]

                stage_results.append(StageResult(
                    stage=ValidationStage(sr['stage']),
                    status=ValidationStatus(sr['status']),
                    duration_seconds=sr['duration_seconds'],
                    issues=issues,
                    metrics=json.loads(sr['metrics']) if sr['metrics'] else {},
                ))

            return ValidationResult(
                request_id=request_id,
                job_path=row['job_path'],
                status=ValidationStatus(row['status']),
                stage_results=stage_results,
                started_at=row['started_at'],
                completed_at=row['completed_at'],
                worker_id=row['worker_id'],
                error_message=row['error_message'],
                error_traceback=row['error_traceback'],
            )

    async def get_job_history(self, job_path: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get validation history for a specific job."""
        if not self._pool:
            return []

        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    r.id, r.status, r.created_at, r.completed_at,
                    r.branch, r.commit_sha,
                    vr.passed, vr.summary
                FROM validation_requests r
                LEFT JOIN validation_results vr ON vr.request_id = r.id
                WHERE r.job_path = $1
                ORDER BY r.created_at DESC
                LIMIT $2
            """, job_path, limit)

            return [
                {
                    "request_id": row['id'],
                    "status": row['status'],
                    "created_at": row['created_at'].isoformat() if row['created_at'] else None,
                    "completed_at": row['completed_at'].isoformat() if row['completed_at'] else None,
                    "branch": row['branch'],
                    "commit_sha": row['commit_sha'],
                    "passed": row['passed'],
                    "summary": json.loads(row['summary']) if row['summary'] else None,
                }
                for row in rows
            ]

    async def get_recent_results(
        self,
        limit: int = 50,
        status_filter: Optional[ValidationStatus] = None,
    ) -> List[Dict[str, Any]]:
        """Get recent validation results."""
        if not self._pool:
            return []

        async with self._pool.acquire() as conn:
            if status_filter:
                rows = await conn.fetch("""
                    SELECT
                        r.id, r.job_path, r.status, r.created_at,
                        r.trigger_source, r.priority, vr.passed
                    FROM validation_requests r
                    LEFT JOIN validation_results vr ON vr.request_id = r.id
                    WHERE r.status = $1
                    ORDER BY r.created_at DESC
                    LIMIT $2
                """, status_filter.value, limit)
            else:
                rows = await conn.fetch("""
                    SELECT
                        r.id, r.job_path, r.status, r.created_at,
                        r.trigger_source, r.priority, vr.passed
                    FROM validation_requests r
                    LEFT JOIN validation_results vr ON vr.request_id = r.id
                    ORDER BY r.created_at DESC
                    LIMIT $1
                """, limit)

            return [
                {
                    "request_id": row['id'],
                    "job_path": row['job_path'],
                    "status": row['status'],
                    "created_at": row['created_at'].isoformat() if row['created_at'] else None,
                    "trigger_source": row['trigger_source'],
                    "priority": row['priority'],
                    "passed": row['passed'],
                }
                for row in rows
            ]

    async def get_statistics(self, days: int = 7) -> Dict[str, Any]:
        """Get validation statistics for the specified period."""
        if not self._pool:
            return {}

        since = datetime.utcnow() - timedelta(days=days)

        async with self._pool.acquire() as conn:
            stats = await conn.fetchrow("""
                SELECT
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE status = 'completed') as completed,
                    COUNT(*) FILTER (WHERE status = 'failed') as failed,
                    COUNT(*) FILTER (WHERE status = 'error') as errors,
                    AVG(EXTRACT(EPOCH FROM (completed_at - started_at)))
                        FILTER (WHERE completed_at IS NOT NULL) as avg_duration
                FROM validation_requests
                WHERE created_at >= $1
            """, since)

            by_source = await conn.fetch("""
                SELECT trigger_source, COUNT(*) as count
                FROM validation_requests
                WHERE created_at >= $1
                GROUP BY trigger_source
            """, since)

            top_failing = await conn.fetch("""
                SELECT job_path, COUNT(*) as fail_count
                FROM validation_requests r
                JOIN validation_results vr ON vr.request_id = r.id
                WHERE r.created_at >= $1 AND vr.passed = false
                GROUP BY job_path
                ORDER BY fail_count DESC
                LIMIT 10
            """, since)

            common_issues = await conn.fetch("""
                SELECT vi.rule_id, vi.message, COUNT(*) as count
                FROM validation_issues vi
                JOIN stage_results sr ON sr.id = vi.stage_result_id
                JOIN validation_results vr ON vr.id = sr.result_id
                JOIN validation_requests r ON r.id = vr.request_id
                WHERE r.created_at >= $1 AND vi.rule_id IS NOT NULL
                GROUP BY vi.rule_id, vi.message
                ORDER BY count DESC
                LIMIT 10
            """, since)

            return {
                "period_days": days,
                "total_validations": stats['total'],
                "completed": stats['completed'],
                "failed": stats['failed'],
                "errors": stats['errors'],
                "avg_duration_seconds": float(stats['avg_duration']) if stats['avg_duration'] else None,
                "success_rate": (
                    (stats['completed'] - stats['failed']) / stats['total'] * 100
                    if stats['total'] > 0 else 0
                ),
                "by_trigger_source": {row['trigger_source']: row['count'] for row in by_source},
                "top_failing_jobs": [{"job_path": row['job_path'], "count": row['fail_count']} for row in top_failing],
                "common_issues": [{"rule_id": row['rule_id'], "message": row['message'], "count": row['count']} for row in common_issues],
            }

    async def cleanup_old_results(self, days_to_keep: int = 90) -> int:
        """Clean up old validation results."""
        if not self._pool:
            return 0

        cutoff = datetime.utcnow() - timedelta(days=days_to_keep)

        async with self._pool.acquire() as conn:
            result = await conn.execute("""
                DELETE FROM validation_requests
                WHERE created_at < $1
            """, cutoff)

            count = int(result.split()[-1])
            logger.info("Cleaned up old results", deleted=count, cutoff=cutoff)
            return count


class InMemoryResultStorage:
    """In-memory storage for testing and development."""

    def __init__(self):
        self._requests: Dict[str, Dict] = {}
        self._results: Dict[str, ValidationResult] = {}

    async def initialize(self) -> None:
        pass

    async def close(self) -> None:
        pass

    async def store_request(self, request: ValidationRequest) -> None:
        self._requests[request.id] = request.to_dict()

    async def update_request_status(
        self,
        request_id: str,
        status: ValidationStatus,
        **kwargs,
    ) -> None:
        if request_id in self._requests:
            self._requests[request_id]['status'] = status.value

    async def store_result(self, result: ValidationResult) -> None:
        self._results[result.request_id] = result
        if result.request_id in self._requests:
            self._requests[result.request_id]['status'] = result.status.value

    async def get_result(self, request_id: str) -> Optional[ValidationResult]:
        return self._results.get(request_id)

    async def get_job_history(self, job_path: str, limit: int = 10) -> List[Dict]:
        results = [
            {"request_id": rid, **self._requests[rid]}
            for rid, r in self._results.items()
            if r.job_path == job_path
        ]
        return sorted(results, key=lambda x: x.get('created_at', ''), reverse=True)[:limit]

    async def get_recent_results(self, limit: int = 50, **kwargs) -> List[Dict]:
        items = list(self._requests.values())
        return sorted(items, key=lambda x: x.get('created_at', ''), reverse=True)[:limit]

    async def get_statistics(self, days: int = 7) -> Dict[str, Any]:
        total = len(self._results)
        passed = sum(1 for r in self._results.values() if r.passed)
        return {
            "total_validations": total,
            "completed": total,
            "failed": total - passed,
            "success_rate": passed / total * 100 if total > 0 else 0,
        }


def create_storage(
    use_postgres: bool = True,
    *,
    host: Optional[str] = None,
    port: Optional[int] = None,
    database: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    min_connections: Optional[int] = None,
    max_connections: Optional[int] = None,
    config: Optional[StorageConfig] = None,
) -> Union[ResultStorage, InMemoryResultStorage]:
    """
    Factory function to create appropriate storage backend.

    Backward compatible:
      - Existing calls work: create_storage(use_postgres=True)
      - New overrides work: create_storage(use_postgres=True, host="validation-postgres", port=5432, ...)

    Precedence:
      1) explicit config=StorageConfig(...)
      2) explicit keyword args host/port/database/user/password/...
      3) framework.yaml via get_config()
    """
    if use_postgres and ASYNCPG_AVAILABLE:
        if config is not None:
            return ResultStorage(config=config)

        # Base from framework config
        base = ResultStorage()._load_config()

        # Override if provided
        cfg = StorageConfig(
            host=str(host) if host is not None else base.host,
            port=int(port) if port is not None else int(base.port),
            database=str(database) if database is not None else base.database,
            user=str(user) if user is not None else base.user,
            password=str(password) if password is not None else base.password,
            min_connections=int(min_connections) if min_connections is not None else int(base.min_connections),
            max_connections=int(max_connections) if max_connections is not None else int(base.max_connections),
        )

        return ResultStorage(config=cfg)

    return InMemoryResultStorage()
