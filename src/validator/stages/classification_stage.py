"""
AI Classification Stage.

Classifies Spark jobs using AI to determine:
- Domain (Finance, Customer, Risk, etc.)
- Module (Loan Processing, Order Management, etc.)
- Job Group (ETL, Analytics, Reporting, etc.)
- Complexity (Low, Medium, High)

Classification results are saved to PostgreSQL for persistence.
"""

import os
import time
import logging
import hashlib
from typing import Dict, Any, Optional, TYPE_CHECKING

from .base import ValidationStage, StageResult, ValidationIssue, Severity

if TYPE_CHECKING:
    from ..pipeline.context import ValidationContext

logger = logging.getLogger(__name__)


class ClassificationStage(ValidationStage):
    """
    AI-powered job classification stage.

    This stage:
    1. Analyzes job code using AI (Azure OpenAI or Ollama)
    2. Classifies into Domain/Module/Job Group hierarchy
    3. Assesses complexity and provides reasoning
    4. Saves results to PostgreSQL database

    Configuration options:
        enabled: true/false
        blocking: false (classification errors shouldn't block validation)
        skip_if_classified: true (skip already classified jobs)
        force_reclassify: false (override skip_if_classified)
        save_to_db: true (persist results)
    """

    name = "classification"
    requires_spark = False
    blocking = False  # Classification failures shouldn't block validation

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the classification stage."""
        super().__init__(config)

        self.skip_if_classified = self.config.get("skip_if_classified", True)
        self.force_reclassify = self.config.get("force_reclassify", False)
        self.save_to_db = self.config.get("save_to_db", True)

        # Lazy initialization of classifier
        self._classifier = None
        self._storage = None
        self._category_manager = None

    def _get_classifier(self):
        """Lazy initialization of LLM classifier."""
        if self._classifier is None:
            try:
                from validator.ai.classifier import LLMClassifier
                from validator.ai.config import load_ai_config

                # Load AI config from framework.yaml
                config_path = os.path.join(
                    os.path.dirname(__file__),
                    "..", "..", "config", "framework.yaml"
                )
                ai_config = load_ai_config(config_path)
                self._classifier = LLMClassifier(ai_config)
            except Exception as e:
                logger.error(f"Failed to initialize classifier: {e}")
                raise

        return self._classifier

    async def _get_storage(self):
        """Lazy initialization of classification storage."""
        if self._storage is None:
            try:
                from validator.ai.classifier.storage import create_storage
                from validator.ai.config import load_ai_config

                config_path = os.path.join(
                    os.path.dirname(__file__),
                    "..", "..", "config", "framework.yaml"
                )
                config = load_ai_config(config_path)
                self._storage = await create_storage(config)
            except Exception as e:
                logger.error(f"Failed to initialize storage: {e}")
                raise

        return self._storage

    async def _get_category_manager(self):
        """Lazy initialization of category manager."""
        if self._category_manager is None:
            try:
                from validator.ai.classifier.category_manager import create_category_manager
                from validator.ai.config import load_ai_config

                config_path = os.path.join(
                    os.path.dirname(__file__),
                    "..", "..", "config", "framework.yaml"
                )
                config = load_ai_config(config_path)
                storage = await self._get_storage()
                self._category_manager = await create_category_manager(storage, config)
            except Exception as e:
                logger.error(f"Failed to initialize category manager: {e}")
                raise

        return self._category_manager

    def validate(self, context: "ValidationContext") -> StageResult:
        """
        Run AI classification on the job.

        Args:
            context: ValidationContext with job file path and content

        Returns:
            StageResult with classification outcome
        """
        start_time = time.time()
        issues = []

        try:
            # Get job path and content
            job_path = context.file_path
            code = context.file_content

            if not code:
                return self.create_skipped_result("No file content available")

            # Generate job ID from path (for deduplication)
            job_id = self._generate_job_id(job_path)

            # Check if already classified (unless force_reclassify)
            if self.skip_if_classified and not self.force_reclassify:
                existing = self._check_existing_classification(job_path)
                if existing:
                    duration = time.time() - start_time
                    return self.create_result(
                        passed=True,
                        issues=[],
                        duration=duration,
                        details={
                            "status": "already_classified",
                            "classification": existing,
                            "job_id": job_id,
                        }
                    )

            # Get existing categories for context
            existing_categories = self._get_existing_categories()

            # Classify the job
            classifier = self._get_classifier()
            result, provider = classifier.classify_job(
                code=code,
                file_path=job_path,
                existing_categories=existing_categories,
            )

            # Build classification details
            classification = {
                "job_path": job_path,
                "job_id": job_id,
                "domain": result.domain,
                "module": result.module,
                "job_group": result.job_group,
                "complexity": result.complexity.value,
                "complexity_score": result.complexity_score,
                "complexity_reasoning": result.complexity_reasoning,
                "confidence_score": result.confidence_score,
                "ai_provider": provider,
                "metrics": result.metrics.to_dict() if result.metrics else {},
                "domain_exists": result.domain_exists,
                "module_exists": result.module_exists,
                "job_group_exists": result.job_group_exists,
            }

            # Save to database if enabled
            if self.save_to_db:
                self._save_classification(job_path, classification, provider)

            # Add info about new categories discovered
            if not result.domain_exists:
                issues.append(self.create_issue(
                    Severity.INFO,
                    f"New domain suggested: {result.domain}",
                    rule="new_category"
                ))
            if not result.module_exists:
                issues.append(self.create_issue(
                    Severity.INFO,
                    f"New module suggested: {result.module}",
                    rule="new_category"
                ))
            if not result.job_group_exists:
                issues.append(self.create_issue(
                    Severity.INFO,
                    f"New job group suggested: {result.job_group}",
                    rule="new_category"
                ))

            duration = time.time() - start_time

            return self.create_result(
                passed=True,
                issues=issues,
                duration=duration,
                details={
                    "status": "classified",
                    "classification": classification,
                    "job_id": job_id,
                }
            )

        except Exception as e:
            logger.error(f"Classification failed: {e}")
            duration = time.time() - start_time

            issues.append(self.create_issue(
                Severity.WARNING,
                f"Classification failed: {str(e)}",
                rule="classification_error"
            ))

            return self.create_result(
                passed=True,  # Don't block on classification errors
                issues=issues,
                duration=duration,
                details={
                    "status": "error",
                    "error": str(e),
                }
            )

    def _generate_job_id(self, job_path: str) -> str:
        """Generate a unique ID for a job based on its path."""
        # Use MD5 hash of the normalized path
        normalized = os.path.normpath(os.path.abspath(job_path))
        return hashlib.md5(normalized.encode()).hexdigest()[:16]

    def _check_existing_classification(self, job_path: str) -> Optional[Dict[str, Any]]:
        """Check if job is already classified in database."""
        # For sync operation, we'll check via a simple query
        # In production, this would use async storage
        try:
            import psycopg2

            # Get connection from environment
            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                database=os.getenv("POSTGRES_DB", "validation_results"),
                user=os.getenv("POSTGRES_USER", "postgres"),
                password=os.getenv("POSTGRES_PASSWORD", ""),
            )

            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        j.job_path, j.job_name,
                        d.name as domain, m.name as module, g.name as job_group,
                        j.complexity, j.complexity_score, j.confidence_score
                    FROM job_classifications j
                    LEFT JOIN categories d ON j.domain_id = d.id
                    LEFT JOIN categories m ON j.module_id = m.id
                    LEFT JOIN categories g ON j.job_group_id = g.id
                    WHERE j.job_path = %s
                """, (job_path,))

                row = cur.fetchone()
                if row:
                    return {
                        "job_path": row[0],
                        "job_name": row[1],
                        "domain": row[2],
                        "module": row[3],
                        "job_group": row[4],
                        "complexity": row[5],
                        "complexity_score": row[6],
                        "confidence_score": row[7],
                    }

            conn.close()
            return None

        except Exception as e:
            logger.debug(f"Could not check existing classification: {e}")
            return None

    def _get_existing_categories(self) -> Dict[str, list]:
        """Get existing categories from database."""
        try:
            import psycopg2

            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                database=os.getenv("POSTGRES_DB", "validation_results"),
                user=os.getenv("POSTGRES_USER", "postgres"),
                password=os.getenv("POSTGRES_PASSWORD", ""),
            )

            categories = {"domains": [], "modules": [], "job_groups": []}

            with conn.cursor() as cur:
                cur.execute("SELECT name, type FROM categories WHERE is_active = true")
                for row in cur.fetchall():
                    name, cat_type = row
                    if cat_type == "domain":
                        categories["domains"].append(name)
                    elif cat_type == "module":
                        categories["modules"].append(name)
                    elif cat_type == "job_group":
                        categories["job_groups"].append(name)

            conn.close()
            return categories

        except Exception as e:
            logger.debug(f"Could not get existing categories: {e}")
            # Return some defaults
            return {
                "domains": ["Finance", "Customer", "Risk", "Operations"],
                "modules": ["Loan Processing", "Order Management"],
                "job_groups": ["ETL", "Analytics", "Reporting", "Fact Load"],
            }

    def _save_classification(
        self,
        job_path: str,
        classification: Dict[str, Any],
        provider: str
    ) -> None:
        """Save classification result to database."""
        try:
            import psycopg2

            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                database=os.getenv("POSTGRES_DB", "validation_results"),
                user=os.getenv("POSTGRES_USER", "postgres"),
                password=os.getenv("POSTGRES_PASSWORD", ""),
            )

            with conn.cursor() as cur:
                # Get or create category IDs
                domain_id = self._get_or_create_category(
                    cur, classification["domain"], "domain"
                )
                module_id = self._get_or_create_category(
                    cur, classification["module"], "module", domain_id
                )
                job_group_id = self._get_or_create_category(
                    cur, classification["job_group"], "job_group", module_id
                )

                # Insert or update classification
                job_name = os.path.basename(job_path)
                import json

                cur.execute("""
                    INSERT INTO job_classifications (
                        job_path, job_name, domain_id, module_id, job_group_id,
                        complexity, complexity_score, complexity_reasoning,
                        confidence_score, metrics, ai_provider
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (job_path) DO UPDATE SET
                        domain_id = EXCLUDED.domain_id,
                        module_id = EXCLUDED.module_id,
                        job_group_id = EXCLUDED.job_group_id,
                        complexity = EXCLUDED.complexity,
                        complexity_score = EXCLUDED.complexity_score,
                        complexity_reasoning = EXCLUDED.complexity_reasoning,
                        confidence_score = EXCLUDED.confidence_score,
                        metrics = EXCLUDED.metrics,
                        ai_provider = EXCLUDED.ai_provider,
                        updated_at = NOW()
                """, (
                    job_path,
                    job_name,
                    domain_id,
                    module_id,
                    job_group_id,
                    classification["complexity"],
                    classification["complexity_score"],
                    classification.get("complexity_reasoning"),
                    classification["confidence_score"],
                    json.dumps(classification.get("metrics", {})),
                    provider,
                ))

                conn.commit()

            conn.close()
            logger.info(f"Saved classification for {job_path}")

        except Exception as e:
            logger.error(f"Failed to save classification: {e}")

    def _get_or_create_category(
        self,
        cursor,
        name: str,
        cat_type: str,
        parent_id: Optional[str] = None
    ) -> str:
        """Get existing category or create new one."""
        import uuid

        # Check if exists
        if parent_id:
            cursor.execute(
                "SELECT id FROM categories WHERE name = %s AND type = %s AND parent_id = %s",
                (name, cat_type, parent_id)
            )
        else:
            cursor.execute(
                "SELECT id FROM categories WHERE name = %s AND type = %s AND parent_id IS NULL",
                (name, cat_type)
            )

        row = cursor.fetchone()
        if row:
            return row[0]

        # Create new category
        new_id = str(uuid.uuid4())
        cursor.execute("""
            INSERT INTO categories (id, name, type, parent_id, is_active)
            VALUES (%s, %s, %s, %s, true)
            RETURNING id
        """, (new_id, name, cat_type, parent_id))

        return cursor.fetchone()[0]
