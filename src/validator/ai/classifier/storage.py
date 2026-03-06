"""
PostgreSQL Storage for Job Classification

Handles persistence of categories, classifications, batches, and suggestions.
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from uuid import UUID, uuid4

try:
    import asyncpg
    HAS_ASYNCPG = True
except ImportError:
    HAS_ASYNCPG = False

from .models import (
    Category,
    CategoryType,
    JobClassification,
    ClassificationBatch,
    BatchStatus,
    SuggestedCategory,
    SuggestionStatus,
    Complexity,
    ComplexityMetrics,
)

logger = logging.getLogger(__name__)


class ClassificationStorage:
    """
    PostgreSQL storage for classification data.

    Uses asyncpg for async database operations.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize storage.

        Args:
            config: PostgreSQL configuration from framework.yaml
        """
        self.config = config
        self.pool: Optional[asyncpg.Pool] = None

        self.host = config.get("host", "localhost")
        self.port = config.get("port", 5432)
        self.database = config.get("database", "validation_results")
        self.user = config.get("user", "postgres")
        self.password = config.get("password", "")

    async def connect(self) -> None:
        """Initialize connection pool"""
        if not HAS_ASYNCPG:
            raise ImportError("asyncpg package required. Install with: pip install asyncpg")

        if self.pool is None:
            self.pool = await asyncpg.create_pool(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                min_size=2,
                max_size=10,
            )
            logger.info(f"Connected to PostgreSQL at {self.host}:{self.port}/{self.database}")

    async def close(self) -> None:
        """Close connection pool"""
        if self.pool:
            await self.pool.close()
            self.pool = None

    # =========================================================================
    # Category Operations
    # =========================================================================

    async def get_categories(
        self,
        category_type: Optional[CategoryType] = None,
        approved_only: bool = True
    ) -> List[Category]:
        """Get all categories, optionally filtered by type"""
        query = "SELECT * FROM categories WHERE 1=1"
        params = []

        if category_type:
            params.append(category_type.value)
            query += f" AND type = ${len(params)}"

        if approved_only:
            query += " AND approved = true"

        query += " ORDER BY type, name"

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            return [self._row_to_category(row) for row in rows]

    async def get_category_by_id(self, category_id: UUID) -> Optional[Category]:
        """Get a category by ID"""
        query = "SELECT * FROM categories WHERE id = $1"
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, category_id)
            return self._row_to_category(row) if row else None

    async def get_category_by_name(
        self,
        category_type: CategoryType,
        name: str,
        parent_id: Optional[UUID] = None
    ) -> Optional[Category]:
        """Get a category by type and name"""
        if parent_id:
            query = "SELECT * FROM categories WHERE type = $1 AND LOWER(name) = LOWER($2) AND parent_id = $3"
            params = [category_type.value, name, parent_id]
        else:
            query = "SELECT * FROM categories WHERE type = $1 AND LOWER(name) = LOWER($2) AND parent_id IS NULL"
            params = [category_type.value, name]

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, *params)
            return self._row_to_category(row) if row else None

    async def create_category(self, category: Category) -> Category:
        """Create a new category"""
        query = """
            INSERT INTO categories (id, type, name, description, parent_id, ai_discovered, approved, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING *
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                query,
                category.id,
                category.type.value,
                category.name,
                category.description,
                category.parent_id,
                category.ai_discovered,
                category.approved,
                json.dumps(category.metadata),
            )
            return self._row_to_category(row)

    async def update_category(self, category: Category) -> Category:
        """Update an existing category"""
        query = """
            UPDATE categories
            SET name = $2, description = $3, parent_id = $4, approved = $5,
                approved_by = $6, approved_at = $7, metadata = $8, updated_at = NOW()
            WHERE id = $1
            RETURNING *
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                query,
                category.id,
                category.name,
                category.description,
                category.parent_id,
                category.approved,
                category.approved_by,
                category.approved_at,
                json.dumps(category.metadata),
            )
            return self._row_to_category(row)

    async def delete_category(self, category_id: UUID) -> bool:
        """Delete a category"""
        query = "DELETE FROM categories WHERE id = $1"
        async with self.pool.acquire() as conn:
            result = await conn.execute(query, category_id)
            return result == "DELETE 1"

    async def get_category_tree(self) -> List[Dict[str, Any]]:
        """Get hierarchical category tree"""
        query = """
            WITH RECURSIVE category_tree AS (
                SELECT id, type, name, description, parent_id, approved, 1 AS level,
                       ARRAY[name] AS path
                FROM categories WHERE parent_id IS NULL
                UNION ALL
                SELECT c.id, c.type, c.name, c.description, c.parent_id, c.approved,
                       ct.level + 1, ct.path || c.name
                FROM categories c
                INNER JOIN category_tree ct ON c.parent_id = ct.id
            )
            SELECT * FROM category_tree ORDER BY path
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query)
            return [dict(row) for row in rows]

    async def get_category_names(self) -> Dict[str, List[str]]:
        """Get all category names grouped by type (for classification prompts)"""
        query = "SELECT type, name FROM categories WHERE approved = true ORDER BY type, name"
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query)

        result = {"domains": [], "modules": [], "job_groups": []}
        for row in rows:
            if row["type"] == "domain":
                result["domains"].append(row["name"])
            elif row["type"] == "module":
                result["modules"].append(row["name"])
            elif row["type"] == "job_group":
                result["job_groups"].append(row["name"])

        return result

    # =========================================================================
    # Job Classification Operations
    # =========================================================================

    async def get_classification(self, job_path: str) -> Optional[JobClassification]:
        """Get classification for a job"""
        query = """
            SELECT jc.*, d.name as domain_name, m.name as module_name, jg.name as job_group_name
            FROM job_classifications jc
            LEFT JOIN categories d ON jc.domain_id = d.id
            LEFT JOIN categories m ON jc.module_id = m.id
            LEFT JOIN categories jg ON jc.job_group_id = jg.id
            WHERE jc.job_path = $1
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, job_path)
            return self._row_to_classification(row) if row else None

    async def store_classification(self, classification: JobClassification) -> JobClassification:
        """Store or update a job classification"""
        query = """
            INSERT INTO job_classifications (
                id, job_path, job_name, domain_id, module_id, job_group_id,
                complexity, complexity_score, complexity_reasoning, metrics,
                confidence_score, suggested_domain, suggested_module, suggested_job_group,
                ai_provider, ai_model, batch_id, raw_response
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
            ON CONFLICT (job_path) DO UPDATE SET
                domain_id = EXCLUDED.domain_id,
                module_id = EXCLUDED.module_id,
                job_group_id = EXCLUDED.job_group_id,
                complexity = EXCLUDED.complexity,
                complexity_score = EXCLUDED.complexity_score,
                complexity_reasoning = EXCLUDED.complexity_reasoning,
                metrics = EXCLUDED.metrics,
                confidence_score = EXCLUDED.confidence_score,
                suggested_domain = EXCLUDED.suggested_domain,
                suggested_module = EXCLUDED.suggested_module,
                suggested_job_group = EXCLUDED.suggested_job_group,
                ai_provider = EXCLUDED.ai_provider,
                ai_model = EXCLUDED.ai_model,
                batch_id = EXCLUDED.batch_id,
                raw_response = EXCLUDED.raw_response,
                updated_at = NOW()
            RETURNING *
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                query,
                classification.id,
                classification.job_path,
                classification.job_name,
                classification.domain_id,
                classification.module_id,
                classification.job_group_id,
                classification.complexity.value if classification.complexity else None,
                classification.complexity_score,
                classification.complexity_reasoning,
                json.dumps(classification.metrics.to_dict()) if classification.metrics else None,
                classification.confidence_score,
                classification.suggested_domain,
                classification.suggested_module,
                classification.suggested_job_group,
                classification.ai_provider,
                classification.ai_model,
                classification.batch_id,
                json.dumps(classification.raw_response) if classification.raw_response else None,
            )
            return self._row_to_classification(row)

    async def get_classifications(
        self,
        domain_id: Optional[UUID] = None,
        module_id: Optional[UUID] = None,
        job_group_id: Optional[UUID] = None,
        complexity: Optional[Complexity] = None,
        batch_id: Optional[UUID] = None,
        has_suggestions: Optional[bool] = None,
        limit: int = 100,
        offset: int = 0
    ) -> Tuple[List[JobClassification], int]:
        """Get classifications with filtering and pagination"""
        query = """
            SELECT jc.*, d.name as domain_name, m.name as module_name, jg.name as job_group_name
            FROM job_classifications jc
            LEFT JOIN categories d ON jc.domain_id = d.id
            LEFT JOIN categories m ON jc.module_id = m.id
            LEFT JOIN categories jg ON jc.job_group_id = jg.id
            WHERE 1=1
        """
        count_query = "SELECT COUNT(*) FROM job_classifications jc WHERE 1=1"
        params = []
        param_count = 0

        if domain_id:
            param_count += 1
            query += f" AND jc.domain_id = ${param_count}"
            count_query += f" AND jc.domain_id = ${param_count}"
            params.append(domain_id)

        if module_id:
            param_count += 1
            query += f" AND jc.module_id = ${param_count}"
            count_query += f" AND jc.module_id = ${param_count}"
            params.append(module_id)

        if job_group_id:
            param_count += 1
            query += f" AND jc.job_group_id = ${param_count}"
            count_query += f" AND jc.job_group_id = ${param_count}"
            params.append(job_group_id)

        if complexity:
            param_count += 1
            query += f" AND jc.complexity = ${param_count}"
            count_query += f" AND jc.complexity = ${param_count}"
            params.append(complexity.value)

        if batch_id:
            param_count += 1
            query += f" AND jc.batch_id = ${param_count}"
            count_query += f" AND jc.batch_id = ${param_count}"
            params.append(batch_id)

        if has_suggestions is not None:
            if has_suggestions:
                query += " AND (jc.suggested_domain IS NOT NULL OR jc.suggested_module IS NOT NULL OR jc.suggested_job_group IS NOT NULL)"
                count_query += " AND (jc.suggested_domain IS NOT NULL OR jc.suggested_module IS NOT NULL OR jc.suggested_job_group IS NOT NULL)"
            else:
                query += " AND jc.suggested_domain IS NULL AND jc.suggested_module IS NULL AND jc.suggested_job_group IS NULL"
                count_query += " AND jc.suggested_domain IS NULL AND jc.suggested_module IS NULL AND jc.suggested_job_group IS NULL"

        query += f" ORDER BY jc.classified_at DESC LIMIT ${param_count + 1} OFFSET ${param_count + 2}"
        params.extend([limit, offset])

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            count_row = await conn.fetchval(count_query, *params[:-2])

        classifications = [self._row_to_classification(row) for row in rows]
        return classifications, count_row

    # =========================================================================
    # Batch Operations
    # =========================================================================

    async def create_batch(self, batch: ClassificationBatch) -> ClassificationBatch:
        """Create a new classification batch"""
        query = """
            INSERT INTO classification_batches (
                id, name, status, directories, file_patterns, exclude_patterns,
                total_jobs, ai_provider, ai_model, triggered_by, options
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            RETURNING *
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                query,
                batch.id,
                batch.name,
                batch.status.value,
                json.dumps(batch.directories),
                json.dumps(batch.file_patterns),
                json.dumps(batch.exclude_patterns),
                batch.total_jobs,
                batch.ai_provider,
                batch.ai_model,
                batch.triggered_by,
                json.dumps(batch.options),
            )
            return self._row_to_batch(row)

    async def get_batch(self, batch_id: UUID) -> Optional[ClassificationBatch]:
        """Get a batch by ID"""
        query = "SELECT * FROM classification_batches WHERE id = $1"
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, batch_id)
            return self._row_to_batch(row) if row else None

    async def update_batch_progress(
        self,
        batch_id: UUID,
        processed_jobs: int,
        successful_jobs: int,
        failed_jobs: int,
        skipped_jobs: int = 0
    ) -> None:
        """Update batch progress"""
        query = """
            UPDATE classification_batches
            SET processed_jobs = $2, successful_jobs = $3, failed_jobs = $4, skipped_jobs = $5
            WHERE id = $1
        """
        async with self.pool.acquire() as conn:
            await conn.execute(query, batch_id, processed_jobs, successful_jobs, failed_jobs, skipped_jobs)

    async def update_batch_status(
        self,
        batch_id: UUID,
        status: BatchStatus,
        error_message: Optional[str] = None
    ) -> None:
        """Update batch status"""
        if status == BatchStatus.RUNNING:
            query = "UPDATE classification_batches SET status = $2, started_at = NOW() WHERE id = $1"
            params = [batch_id, status.value]
        elif status in (BatchStatus.COMPLETED, BatchStatus.FAILED, BatchStatus.CANCELLED):
            query = "UPDATE classification_batches SET status = $2, completed_at = NOW(), error_message = $3 WHERE id = $1"
            params = [batch_id, status.value, error_message]
        else:
            query = "UPDATE classification_batches SET status = $2 WHERE id = $1"
            params = [batch_id, status.value]

        async with self.pool.acquire() as conn:
            await conn.execute(query, *params)

    async def get_batches(
        self,
        status: Optional[BatchStatus] = None,
        limit: int = 20
    ) -> List[ClassificationBatch]:
        """Get recent batches"""
        query = "SELECT * FROM classification_batches"
        params = []

        if status:
            query += " WHERE status = $1"
            params.append(status.value)

        query += " ORDER BY created_at DESC LIMIT $" + str(len(params) + 1)
        params.append(limit)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            return [self._row_to_batch(row) for row in rows]

    # =========================================================================
    # Suggested Category Operations
    # =========================================================================

    async def add_suggestion(
        self,
        category_type: CategoryType,
        name: str,
        job_path: str,
        parent_name: Optional[str] = None,
        description: Optional[str] = None
    ) -> SuggestedCategory:
        """Add or update a category suggestion"""
        # Check if suggestion already exists
        query = """
            SELECT * FROM suggested_categories
            WHERE type = $1 AND LOWER(name) = LOWER($2) AND status = 'pending'
        """
        async with self.pool.acquire() as conn:
            existing = await conn.fetchrow(query, category_type.value, name)

            if existing:
                # Update existing suggestion
                suggested_by = json.loads(existing["suggested_by_jobs"])
                if job_path not in suggested_by:
                    suggested_by.append(job_path)

                update_query = """
                    UPDATE suggested_categories
                    SET suggested_by_jobs = $2, occurrence_count = $3, last_suggested_at = NOW()
                    WHERE id = $1
                    RETURNING *
                """
                row = await conn.fetchrow(
                    update_query,
                    existing["id"],
                    json.dumps(suggested_by),
                    len(suggested_by)
                )
            else:
                # Create new suggestion
                insert_query = """
                    INSERT INTO suggested_categories (id, type, name, description, parent_name, suggested_by_jobs, occurrence_count)
                    VALUES ($1, $2, $3, $4, $5, $6, 1)
                    RETURNING *
                """
                row = await conn.fetchrow(
                    insert_query,
                    uuid4(),
                    category_type.value,
                    name,
                    description,
                    parent_name,
                    json.dumps([job_path])
                )

            return self._row_to_suggestion(row)

    async def get_pending_suggestions(
        self,
        category_type: Optional[CategoryType] = None,
        limit: int = 50
    ) -> List[SuggestedCategory]:
        """Get pending suggestions"""
        query = "SELECT * FROM suggested_categories WHERE status = 'pending'"
        params = []

        if category_type:
            query += " AND type = $1"
            params.append(category_type.value)

        query += f" ORDER BY occurrence_count DESC, last_suggested_at DESC LIMIT ${len(params) + 1}"
        params.append(limit)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            return [self._row_to_suggestion(row) for row in rows]

    async def approve_suggestion(
        self,
        suggestion_id: UUID,
        reviewed_by: str,
        create_category: bool = True
    ) -> Optional[Category]:
        """Approve a suggested category"""
        async with self.pool.acquire() as conn:
            # Get suggestion
            suggestion = await conn.fetchrow(
                "SELECT * FROM suggested_categories WHERE id = $1",
                suggestion_id
            )
            if not suggestion:
                return None

            # Update suggestion status
            await conn.execute(
                """UPDATE suggested_categories
                   SET status = 'approved', reviewed_by = $2, reviewed_at = NOW()
                   WHERE id = $1""",
                suggestion_id, reviewed_by
            )

            if create_category:
                # Create the actual category
                category = Category(
                    id=uuid4(),
                    type=CategoryType(suggestion["type"]),
                    name=suggestion["name"],
                    description=suggestion.get("description"),
                    ai_discovered=True,
                    approved=True,
                    approved_by=reviewed_by,
                    approved_at=datetime.now(),
                )
                return await self.create_category(category)

            return None

    async def reject_suggestion(
        self,
        suggestion_id: UUID,
        reviewed_by: str,
        notes: Optional[str] = None
    ) -> bool:
        """Reject a suggested category"""
        query = """
            UPDATE suggested_categories
            SET status = 'rejected', reviewed_by = $2, reviewed_at = NOW(), review_notes = $3
            WHERE id = $1
        """
        async with self.pool.acquire() as conn:
            result = await conn.execute(query, suggestion_id, reviewed_by, notes)
            return result == "UPDATE 1"

    # =========================================================================
    # Statistics
    # =========================================================================

    async def get_classification_stats(self) -> Dict[str, Any]:
        """Get classification statistics"""
        async with self.pool.acquire() as conn:
            # Total classifications
            total = await conn.fetchval("SELECT COUNT(*) FROM job_classifications")

            # By complexity
            complexity_counts = await conn.fetch(
                "SELECT complexity, COUNT(*) as count FROM job_classifications GROUP BY complexity"
            )

            # By domain
            domain_counts = await conn.fetch("""
                SELECT c.name, COUNT(*) as count
                FROM job_classifications jc
                LEFT JOIN categories c ON jc.domain_id = c.id
                GROUP BY c.name
                ORDER BY count DESC
                LIMIT 10
            """)

            # Pending suggestions
            pending_suggestions = await conn.fetchval(
                "SELECT COUNT(*) FROM suggested_categories WHERE status = 'pending'"
            )

            # Recent batch info
            recent_batch = await conn.fetchrow(
                "SELECT * FROM classification_batches ORDER BY created_at DESC LIMIT 1"
            )

            return {
                "total_classifications": total,
                "by_complexity": {row["complexity"]: row["count"] for row in complexity_counts if row["complexity"]},
                "by_domain": {row["name"] or "Unclassified": row["count"] for row in domain_counts},
                "pending_suggestions": pending_suggestions,
                "recent_batch": self._row_to_batch(recent_batch).to_dict() if recent_batch else None,
            }

    # =========================================================================
    # Row Converters
    # =========================================================================

    def _row_to_category(self, row: Any) -> Category:
        """Convert database row to Category"""
        return Category(
            id=row["id"],
            type=CategoryType(row["type"]),
            name=row["name"],
            description=row.get("description"),
            parent_id=row.get("parent_id"),
            ai_discovered=row.get("ai_discovered", False),
            approved=row.get("approved", True),
            approved_by=row.get("approved_by"),
            approved_at=row.get("approved_at"),
            metadata=json.loads(row["metadata"]) if row.get("metadata") else {},
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _row_to_classification(self, row: Any) -> JobClassification:
        """Convert database row to JobClassification"""
        metrics = None
        if row.get("metrics"):
            metrics_data = json.loads(row["metrics"]) if isinstance(row["metrics"], str) else row["metrics"]
            metrics = ComplexityMetrics.from_dict(metrics_data)

        return JobClassification(
            id=row["id"],
            job_path=row["job_path"],
            job_name=row["job_name"],
            domain_id=row.get("domain_id"),
            module_id=row.get("module_id"),
            job_group_id=row.get("job_group_id"),
            domain_name=row.get("domain_name"),
            module_name=row.get("module_name"),
            job_group_name=row.get("job_group_name"),
            complexity=Complexity(row["complexity"]) if row.get("complexity") else None,
            complexity_score=row.get("complexity_score"),
            complexity_reasoning=row.get("complexity_reasoning"),
            metrics=metrics,
            confidence_score=row.get("confidence_score"),
            suggested_domain=row.get("suggested_domain"),
            suggested_module=row.get("suggested_module"),
            suggested_job_group=row.get("suggested_job_group"),
            ai_provider=row.get("ai_provider"),
            ai_model=row.get("ai_model"),
            batch_id=row.get("batch_id"),
            classified_at=row.get("classified_at", datetime.now()),
            updated_at=row.get("updated_at", datetime.now()),
            raw_response=json.loads(row["raw_response"]) if row.get("raw_response") else None,
        )

    def _row_to_batch(self, row: Any) -> ClassificationBatch:
        """Convert database row to ClassificationBatch"""
        return ClassificationBatch(
            id=row["id"],
            name=row.get("name"),
            status=BatchStatus(row["status"]),
            directories=json.loads(row["directories"]) if isinstance(row["directories"], str) else row["directories"],
            file_patterns=json.loads(row["file_patterns"]) if isinstance(row.get("file_patterns"), str) else row.get("file_patterns", ["*.py"]),
            exclude_patterns=json.loads(row["exclude_patterns"]) if isinstance(row.get("exclude_patterns"), str) else row.get("exclude_patterns", []),
            total_jobs=row.get("total_jobs", 0),
            processed_jobs=row.get("processed_jobs", 0),
            successful_jobs=row.get("successful_jobs", 0),
            failed_jobs=row.get("failed_jobs", 0),
            skipped_jobs=row.get("skipped_jobs", 0),
            ai_provider=row.get("ai_provider"),
            ai_model=row.get("ai_model"),
            created_at=row["created_at"],
            started_at=row.get("started_at"),
            completed_at=row.get("completed_at"),
            error_message=row.get("error_message"),
            triggered_by=row.get("triggered_by"),
            options=json.loads(row["options"]) if isinstance(row.get("options"), str) else row.get("options", {}),
        )

    def _row_to_suggestion(self, row: Any) -> SuggestedCategory:
        """Convert database row to SuggestedCategory"""
        return SuggestedCategory(
            id=row["id"],
            type=CategoryType(row["type"]),
            name=row["name"],
            description=row.get("description"),
            parent_name=row.get("parent_name"),
            suggested_by_jobs=json.loads(row["suggested_by_jobs"]) if isinstance(row["suggested_by_jobs"], str) else row["suggested_by_jobs"],
            occurrence_count=row.get("occurrence_count", 1),
            first_suggested_at=row.get("first_suggested_at", datetime.now()),
            last_suggested_at=row.get("last_suggested_at", datetime.now()),
            status=SuggestionStatus(row["status"]),
            merged_into_id=row.get("merged_into_id"),
            reviewed_by=row.get("reviewed_by"),
            reviewed_at=row.get("reviewed_at"),
            review_notes=row.get("review_notes"),
        )


async def create_storage(config: Dict[str, Any]) -> ClassificationStorage:
    """Factory function to create and connect storage"""
    storage = ClassificationStorage(config.get("postgres", {}))
    await storage.connect()
    return storage
