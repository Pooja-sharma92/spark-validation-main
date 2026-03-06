"""
Category Manager

Handles category discovery, matching, and management.
Maps AI classifications to existing categories or suggests new ones.
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from uuid import UUID, uuid4

from .models import (
    Category,
    CategoryType,
    ClassificationResult,
    JobClassification,
    Complexity,
)
from .storage import ClassificationStorage

logger = logging.getLogger(__name__)


class CategoryManager:
    """
    Manages classification categories.

    Responsibilities:
    - Match AI classifications to existing categories
    - Suggest new categories when no match found
    - Auto-approve high-confidence suggestions
    - Maintain category hierarchy
    """

    def __init__(self, storage: ClassificationStorage, config: Dict[str, Any]):
        """
        Initialize category manager.

        Args:
            storage: ClassificationStorage instance
            config: Category discovery configuration
        """
        self.storage = storage
        self.config = config

        discovery_config = config.get("category_discovery", {})
        self.auto_approve_threshold = discovery_config.get("auto_approve_threshold", 0.9)
        self.min_occurrences = discovery_config.get("min_occurrences_for_suggestion", 2)
        self.discovery_enabled = discovery_config.get("enabled", True)

        # Cache for category lookups
        self._category_cache: Dict[str, Category] = {}
        self._cache_valid = False

    async def refresh_cache(self) -> None:
        """Refresh the category cache"""
        categories = await self.storage.get_categories(approved_only=True)
        self._category_cache = {}

        for cat in categories:
            key = f"{cat.type.value}:{cat.name.lower()}"
            self._category_cache[key] = cat

        self._cache_valid = True
        logger.debug(f"Category cache refreshed with {len(self._category_cache)} categories")

    async def get_existing_categories(self) -> Dict[str, List[str]]:
        """Get all existing category names for classification prompts"""
        return await self.storage.get_category_names()

    async def process_classification(
        self,
        job_path: str,
        job_name: str,
        result: ClassificationResult,
        provider: str,
        batch_id: Optional[UUID] = None
    ) -> JobClassification:
        """
        Process a classification result and create/update the job classification.

        Maps AI results to existing categories or creates suggestions.

        Args:
            job_path: Path to the job file
            job_name: Name of the job
            result: Classification result from AI
            provider: AI provider used
            batch_id: Optional batch ID

        Returns:
            JobClassification object (stored in database)
        """
        if not self._cache_valid:
            await self.refresh_cache()

        # Match or suggest categories
        domain_id, suggested_domain = await self._match_or_suggest(
            CategoryType.DOMAIN,
            result.domain,
            job_path,
            result.confidence_score
        )

        module_id, suggested_module = await self._match_or_suggest(
            CategoryType.MODULE,
            result.module,
            job_path,
            result.confidence_score,
            parent_name=result.domain
        )

        job_group_id, suggested_job_group = await self._match_or_suggest(
            CategoryType.JOB_GROUP,
            result.job_group,
            job_path,
            result.confidence_score
        )

        # Create classification record
        classification = JobClassification(
            id=uuid4(),
            job_path=job_path,
            job_name=job_name,
            domain_id=domain_id,
            module_id=module_id,
            job_group_id=job_group_id,
            complexity=result.complexity,
            complexity_score=result.complexity_score,
            complexity_reasoning=result.complexity_reasoning,
            metrics=result.metrics,
            confidence_score=result.confidence_score,
            suggested_domain=suggested_domain,
            suggested_module=suggested_module,
            suggested_job_group=suggested_job_group,
            ai_provider=provider,
            batch_id=batch_id,
            raw_response=result.raw_response,
        )

        # Store classification
        stored = await self.storage.store_classification(classification)

        # Update with category names for display
        if domain_id:
            cat = await self.storage.get_category_by_id(domain_id)
            if cat:
                stored.domain_name = cat.name
        if module_id:
            cat = await self.storage.get_category_by_id(module_id)
            if cat:
                stored.module_name = cat.name
        if job_group_id:
            cat = await self.storage.get_category_by_id(job_group_id)
            if cat:
                stored.job_group_name = cat.name

        return stored

    async def _match_or_suggest(
        self,
        category_type: CategoryType,
        name: str,
        job_path: str,
        confidence: float,
        parent_name: Optional[str] = None
    ) -> Tuple[Optional[UUID], Optional[str]]:
        """
        Match a category name to existing category or create suggestion.

        Returns:
            Tuple of (category_id or None, suggested_name or None)
        """
        if not name or name.lower() in ("unknown", "n/a", "none"):
            return None, None

        # Try to find existing category
        cache_key = f"{category_type.value}:{name.lower()}"
        if cache_key in self._category_cache:
            return self._category_cache[cache_key].id, None

        # Try fuzzy match
        matched = await self._fuzzy_match(category_type, name)
        if matched:
            return matched.id, None

        # No match found - handle as suggestion
        if not self.discovery_enabled:
            return None, name

        # Auto-approve if high confidence
        if confidence >= self.auto_approve_threshold:
            category = await self._auto_create_category(category_type, name, parent_name)
            if category:
                return category.id, None

        # Add to suggestions
        await self.storage.add_suggestion(
            category_type=category_type,
            name=name,
            job_path=job_path,
            parent_name=parent_name
        )

        return None, name

    async def _fuzzy_match(
        self,
        category_type: CategoryType,
        name: str
    ) -> Optional[Category]:
        """
        Attempt fuzzy matching against existing categories.

        Handles common variations:
        - Case differences
        - Minor spelling variations
        - Abbreviations
        """
        name_lower = name.lower().strip()

        # Check cache for exact match (case-insensitive)
        for key, cat in self._category_cache.items():
            if key.startswith(f"{category_type.value}:"):
                cat_name = cat.name.lower()

                # Exact match
                if cat_name == name_lower:
                    return cat

                # Contains match (for abbreviations)
                if len(name_lower) >= 3:
                    if name_lower in cat_name or cat_name in name_lower:
                        logger.debug(f"Fuzzy matched '{name}' to '{cat.name}'")
                        return cat

        return None

    async def _auto_create_category(
        self,
        category_type: CategoryType,
        name: str,
        parent_name: Optional[str] = None
    ) -> Optional[Category]:
        """
        Auto-create a category for high-confidence classifications.
        """
        # Find parent if specified
        parent_id = None
        if parent_name and category_type == CategoryType.MODULE:
            parent = await self._fuzzy_match(CategoryType.DOMAIN, parent_name)
            if parent:
                parent_id = parent.id

        # Create category
        category = Category(
            id=uuid4(),
            type=category_type,
            name=name.title(),  # Standardize to title case
            ai_discovered=True,
            approved=True,  # Auto-approved
            parent_id=parent_id,
        )

        try:
            created = await self.storage.create_category(category)
            logger.info(f"Auto-created {category_type.value} category: {name}")

            # Update cache
            cache_key = f"{category_type.value}:{name.lower()}"
            self._category_cache[cache_key] = created

            return created
        except Exception as e:
            logger.warning(f"Failed to auto-create category: {e}")
            return None

    async def get_category_tree(self) -> List[Dict[str, Any]]:
        """Get hierarchical category tree for UI"""
        return await self.storage.get_category_tree()

    async def create_category(
        self,
        category_type: CategoryType,
        name: str,
        description: Optional[str] = None,
        parent_id: Optional[UUID] = None
    ) -> Category:
        """Manually create a new category"""
        category = Category(
            id=uuid4(),
            type=category_type,
            name=name,
            description=description,
            parent_id=parent_id,
            ai_discovered=False,
            approved=True,
        )

        created = await self.storage.create_category(category)
        self._cache_valid = False  # Invalidate cache

        return created

    async def update_category(
        self,
        category_id: UUID,
        name: Optional[str] = None,
        description: Optional[str] = None,
        parent_id: Optional[UUID] = None
    ) -> Optional[Category]:
        """Update an existing category"""
        category = await self.storage.get_category_by_id(category_id)
        if not category:
            return None

        if name:
            category.name = name
        if description is not None:
            category.description = description
        if parent_id is not None:
            category.parent_id = parent_id

        updated = await self.storage.update_category(category)
        self._cache_valid = False

        return updated

    async def delete_category(self, category_id: UUID) -> bool:
        """Delete a category"""
        result = await self.storage.delete_category(category_id)
        if result:
            self._cache_valid = False
        return result

    async def get_pending_suggestions(
        self,
        category_type: Optional[CategoryType] = None
    ) -> List[Dict[str, Any]]:
        """Get pending category suggestions"""
        suggestions = await self.storage.get_pending_suggestions(category_type)
        return [s.to_dict() for s in suggestions]

    async def approve_suggestion(
        self,
        suggestion_id: UUID,
        reviewed_by: str
    ) -> Optional[Category]:
        """Approve a suggested category"""
        category = await self.storage.approve_suggestion(
            suggestion_id,
            reviewed_by,
            create_category=True
        )
        if category:
            self._cache_valid = False
        return category

    async def reject_suggestion(
        self,
        suggestion_id: UUID,
        reviewed_by: str,
        notes: Optional[str] = None
    ) -> bool:
        """Reject a suggested category"""
        return await self.storage.reject_suggestion(suggestion_id, reviewed_by, notes)

    async def merge_suggestion(
        self,
        suggestion_id: UUID,
        target_category_id: UUID,
        reviewed_by: str
    ) -> bool:
        """Merge a suggestion into an existing category"""
        # Get the suggestion
        suggestions = await self.storage.get_pending_suggestions()
        suggestion = next((s for s in suggestions if s.id == suggestion_id), None)

        if not suggestion:
            return False

        # Update suggestion status
        async with self.storage.pool.acquire() as conn:
            await conn.execute(
                """UPDATE suggested_categories
                   SET status = 'merged', merged_into_id = $2, reviewed_by = $3, reviewed_at = NOW()
                   WHERE id = $1""",
                suggestion_id, target_category_id, reviewed_by
            )

        return True


async def create_category_manager(
    storage: ClassificationStorage,
    config: Dict[str, Any]
) -> CategoryManager:
    """Factory function to create a category manager"""
    manager = CategoryManager(storage, config.get("classification", {}))
    await manager.refresh_cache()
    return manager
