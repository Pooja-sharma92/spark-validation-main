"""
Batch Classification Processor

Orchestrates batch classification of multiple jobs with:
- Directory scanning
- Parallel processing with concurrency control
- Progress tracking
- Error handling and retry logic
"""

import os
import asyncio
import fnmatch
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional, AsyncGenerator
from uuid import UUID, uuid4

from .models import (
    ClassificationBatch,
    BatchStatus,
    BatchRequest,
    JobClassification,
)
from .classifier import LLMClassifier, ClassificationError
from .category_manager import CategoryManager
from .storage import ClassificationStorage

logger = logging.getLogger(__name__)


class BatchProcessor:
    """
    Processes batch classification jobs.

    Features:
    - Scans directories for job files
    - Applies include/exclude patterns
    - Processes jobs with concurrency control
    - Tracks progress and updates database
    - Handles errors with retry logic
    """

    def __init__(
        self,
        storage: ClassificationStorage,
        classifier: LLMClassifier,
        category_manager: CategoryManager,
        config: Dict[str, Any]
    ):
        """
        Initialize batch processor.

        Args:
            storage: Classification storage
            classifier: LLM classifier instance
            category_manager: Category manager instance
            config: Batch processing configuration
        """
        self.storage = storage
        self.classifier = classifier
        self.category_manager = category_manager
        self.config = config

        batch_config = config.get("batch", {})
        self.max_concurrent = batch_config.get("max_concurrent_jobs", 5)
        self.retry_attempts = batch_config.get("retry_attempts", 2)
        self.progress_update_interval = batch_config.get("progress_update_interval", 5)

        # State
        self._running_batches: Dict[UUID, bool] = {}  # batch_id -> cancelled flag

    async def start_batch(self, request: BatchRequest) -> ClassificationBatch:
        """
        Start a new batch classification.

        Args:
            request: Batch request parameters

        Returns:
            Created batch object
        """
        # Scan for job files
        job_files = await self._scan_directories(
            request.directories,
            request.file_patterns,
            request.exclude_patterns
        )

        if not job_files:
            raise ValueError("No job files found in specified directories")

        # Create batch record
        batch = ClassificationBatch(
            id=uuid4(),
            name=request.name or f"Batch {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            status=BatchStatus.PENDING,
            directories=request.directories,
            file_patterns=request.file_patterns,
            exclude_patterns=request.exclude_patterns,
            total_jobs=len(job_files),
            ai_provider=request.ai_provider or self.classifier.primary_provider,
            triggered_by=request.triggered_by,
            options={"force_reclassify": request.force_reclassify},
        )

        # Store batch
        batch = await self.storage.create_batch(batch)

        # Start processing in background
        asyncio.create_task(self._process_batch(batch, job_files, request.force_reclassify))

        return batch

    async def _scan_directories(
        self,
        directories: List[str],
        include_patterns: List[str],
        exclude_patterns: List[str]
    ) -> List[str]:
        """Scan directories for matching job files"""
        job_files = []

        for directory in directories:
            dir_path = Path(directory).expanduser().resolve()
            if not dir_path.exists():
                logger.warning(f"Directory not found: {directory}")
                continue

            # Walk directory
            for root, _, files in os.walk(dir_path):
                for filename in files:
                    file_path = os.path.join(root, filename)
                    rel_path = os.path.relpath(file_path, dir_path)

                    # Check include patterns
                    included = any(
                        fnmatch.fnmatch(filename, pattern) or fnmatch.fnmatch(rel_path, pattern)
                        for pattern in include_patterns
                    )

                    if not included:
                        continue

                    # Check exclude patterns
                    excluded = any(
                        fnmatch.fnmatch(filename, pattern) or fnmatch.fnmatch(rel_path, pattern)
                        for pattern in exclude_patterns
                    )

                    if excluded:
                        continue

                    # Skip test files by default
                    if filename.startswith("test_") or "_test.py" in filename:
                        continue

                    # Skip __init__.py and __pycache__
                    if filename == "__init__.py" or "__pycache__" in file_path:
                        continue

                    job_files.append(file_path)

        logger.info(f"Found {len(job_files)} job files to classify")
        return sorted(job_files)

    async def _process_batch(
        self,
        batch: ClassificationBatch,
        job_files: List[str],
        force_reclassify: bool
    ) -> None:
        """Process all jobs in a batch"""
        self._running_batches[batch.id] = False  # Not cancelled

        try:
            # Update status to running
            await self.storage.update_batch_status(batch.id, BatchStatus.RUNNING)

            # Get existing categories for prompts
            existing_categories = await self.category_manager.get_existing_categories()

            # Process with concurrency control
            semaphore = asyncio.Semaphore(self.max_concurrent)
            processed = 0
            successful = 0
            failed = 0
            skipped = 0

            async def process_job(job_path: str) -> bool:
                nonlocal processed, successful, failed, skipped

                # Check for cancellation
                if self._running_batches.get(batch.id):
                    return False

                async with semaphore:
                    try:
                        # Check if already classified (unless force)
                        if not force_reclassify:
                            existing = await self.storage.get_classification(job_path)
                            if existing:
                                skipped += 1
                                processed += 1
                                return True

                        # Read job file
                        try:
                            with open(job_path, 'r', encoding='utf-8') as f:
                                code = f.read()
                        except Exception as e:
                            logger.error(f"Failed to read {job_path}: {e}")
                            failed += 1
                            processed += 1
                            return False

                        # Classify with retry
                        for attempt in range(self.retry_attempts + 1):
                            try:
                                result, provider = self.classifier.classify_job(
                                    code=code,
                                    file_path=job_path,
                                    existing_categories=existing_categories
                                )

                                # Process classification
                                job_name = Path(job_path).stem
                                await self.category_manager.process_classification(
                                    job_path=job_path,
                                    job_name=job_name,
                                    result=result,
                                    provider=provider,
                                    batch_id=batch.id
                                )

                                successful += 1
                                processed += 1
                                return True

                            except ClassificationError as e:
                                if attempt < self.retry_attempts:
                                    logger.warning(f"Retry {attempt + 1} for {job_path}: {e}")
                                    await asyncio.sleep(1)
                                else:
                                    logger.error(f"Failed to classify {job_path} after {self.retry_attempts + 1} attempts: {e}")
                                    failed += 1
                                    processed += 1
                                    return False

                    except Exception as e:
                        logger.error(f"Error processing {job_path}: {e}")
                        failed += 1
                        processed += 1
                        return False

            # Create tasks for all jobs
            tasks = [process_job(job_path) for job_path in job_files]

            # Process with progress updates
            update_task = asyncio.create_task(
                self._update_progress_loop(batch.id, lambda: (processed, successful, failed, skipped))
            )

            try:
                await asyncio.gather(*tasks)
            finally:
                update_task.cancel()
                try:
                    await update_task
                except asyncio.CancelledError:
                    pass

            # Final progress update
            await self.storage.update_batch_progress(
                batch.id, processed, successful, failed, skipped
            )

            # Determine final status
            if self._running_batches.get(batch.id):
                await self.storage.update_batch_status(batch.id, BatchStatus.CANCELLED)
            elif failed > 0 and successful == 0:
                await self.storage.update_batch_status(
                    batch.id, BatchStatus.FAILED,
                    f"All {failed} jobs failed"
                )
            else:
                await self.storage.update_batch_status(batch.id, BatchStatus.COMPLETED)

            logger.info(
                f"Batch {batch.id} completed: {successful} successful, "
                f"{failed} failed, {skipped} skipped"
            )

        except Exception as e:
            logger.exception(f"Batch {batch.id} failed with error: {e}")
            await self.storage.update_batch_status(
                batch.id, BatchStatus.FAILED, str(e)
            )

        finally:
            self._running_batches.pop(batch.id, None)

    async def _update_progress_loop(
        self,
        batch_id: UUID,
        get_progress: callable
    ) -> None:
        """Periodically update batch progress in database"""
        while True:
            try:
                await asyncio.sleep(self.progress_update_interval)
                processed, successful, failed, skipped = get_progress()
                await self.storage.update_batch_progress(
                    batch_id, processed, successful, failed, skipped
                )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Failed to update progress: {e}")

    async def cancel_batch(self, batch_id: UUID) -> bool:
        """Cancel a running batch"""
        if batch_id in self._running_batches:
            self._running_batches[batch_id] = True  # Set cancelled flag
            logger.info(f"Batch {batch_id} cancellation requested")
            return True
        return False

    async def get_batch_status(self, batch_id: UUID) -> Optional[Dict[str, Any]]:
        """Get current batch status"""
        batch = await self.storage.get_batch(batch_id)
        if not batch:
            return None

        return {
            **batch.to_dict(),
            "is_running": batch.id in self._running_batches,
            "is_cancelled": self._running_batches.get(batch.id, False),
        }

    async def get_recent_batches(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get recent batches"""
        batches = await self.storage.get_batches(limit=limit)
        return [b.to_dict() for b in batches]

    async def get_batch_results(
        self,
        batch_id: UUID,
        limit: int = 100,
        offset: int = 0
    ) -> Dict[str, Any]:
        """Get classification results for a batch"""
        classifications, total = await self.storage.get_classifications(
            batch_id=batch_id,
            limit=limit,
            offset=offset
        )

        return {
            "data": [c.to_dict() for c in classifications],
            "pagination": {
                "limit": limit,
                "offset": offset,
                "total": total,
                "total_pages": (total + limit - 1) // limit,
            }
        }


async def create_batch_processor(
    storage: ClassificationStorage,
    classifier: LLMClassifier,
    category_manager: CategoryManager,
    config: Dict[str, Any]
) -> BatchProcessor:
    """Factory function to create a batch processor"""
    return BatchProcessor(
        storage=storage,
        classifier=classifier,
        category_manager=category_manager,
        config=config.get("classification", {})
    )


# Standalone runner for batch classification
async def run_batch_classification(
    config: Dict[str, Any],
    directories: List[str],
    name: Optional[str] = None,
    force_reclassify: bool = False,
    triggered_by: str = "cli"
) -> ClassificationBatch:
    """
    Run a batch classification from command line or script.

    Args:
        config: Full framework configuration
        directories: Directories to scan
        name: Optional batch name
        force_reclassify: Whether to reclassify already classified jobs
        triggered_by: Who triggered this batch

    Returns:
        Completed batch object
    """
    from .storage import create_storage
    from .classifier import create_classifier
    from .category_manager import create_category_manager

    # Initialize components
    storage = await create_storage(config)
    classifier = create_classifier(config)
    category_manager = await create_category_manager(storage, config)
    processor = await create_batch_processor(storage, classifier, category_manager, config)

    try:
        # Start batch
        request = BatchRequest(
            directories=directories,
            name=name,
            force_reclassify=force_reclassify,
            triggered_by=triggered_by,
        )
        batch = await processor.start_batch(request)

        # Wait for completion
        while True:
            await asyncio.sleep(2)
            status = await processor.get_batch_status(batch.id)
            if status and status["status"] in ("completed", "failed", "cancelled"):
                break

        # Get final batch state
        return await storage.get_batch(batch.id)

    finally:
        await storage.close()


if __name__ == "__main__":
    import sys
    import yaml

    # Simple CLI for testing
    if len(sys.argv) < 2:
        print("Usage: python batch_processor.py <directory> [directory2 ...]")
        sys.exit(1)

    # Load config
    config_path = Path(__file__).parent.parent.parent / "config" / "framework.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Run batch
    directories = sys.argv[1:]
    batch = asyncio.run(run_batch_classification(
        config=config,
        directories=directories,
        name="CLI Batch",
        triggered_by="cli"
    ))

    print(f"\nBatch completed:")
    print(f"  Status: {batch.status.value}")
    print(f"  Total: {batch.total_jobs}")
    print(f"  Successful: {batch.successful_jobs}")
    print(f"  Failed: {batch.failed_jobs}")
    print(f"  Skipped: {batch.skipped_jobs}")
