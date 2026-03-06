"""
Basic tests for the validation framework queue system.

Run with: pytest tests/test_queue_basic.py -v
"""

import pytest
import asyncio
from datetime import datetime

# Test with in-memory implementations (no Redis required)
from src.validator.models import (
    ValidationRequest,
    ValidationResult,
    ValidationStatus,
    Priority,
    TriggerSource,
)
from src.validator.queue.rate_limiter import (
    InMemoryTokenBucket,
    RateLimitConfig,
    MultiPriorityRateLimiter,
)
from src.validator.queue.concurrency import InMemoryConcurrencyController
from src.validator.queue.manager import InMemoryQueueManager, EnqueueResult


class TestValidationRequest:
    """Test ValidationRequest model."""

    def test_create_request(self):
        request = ValidationRequest(
            job_path="/jobs/test_job.py",
            trigger_source=TriggerSource.MANUAL,
            priority=Priority.CI_CD,
        )

        assert request.job_path == "/jobs/test_job.py"
        assert request.trigger_source == TriggerSource.MANUAL
        assert request.priority == Priority.CI_CD
        assert request.id is not None

    def test_dedup_key(self):
        request1 = ValidationRequest(
            job_path="/jobs/test.py",
            trigger_source=TriggerSource.MANUAL,
            commit_sha="abc123",
        )
        request2 = ValidationRequest(
            job_path="/jobs/test.py",
            trigger_source=TriggerSource.GIT_WEBHOOK,
            commit_sha="abc123",
        )

        # Same job + commit = same dedup key
        assert request1.dedup_key == request2.dedup_key

    def test_to_dict_from_dict(self):
        request = ValidationRequest(
            job_path="/jobs/test.py",
            trigger_source=TriggerSource.MANUAL,
            priority=Priority.CRITICAL,
            branch="hotfix/fix-bug",
        )

        data = request.to_dict()
        restored = ValidationRequest.from_dict(data)

        assert restored.job_path == request.job_path
        assert restored.priority == request.priority
        assert restored.branch == request.branch


class TestRateLimiter:
    """Test rate limiter implementation."""

    @pytest.mark.asyncio
    async def test_token_bucket_acquire(self):
        config = RateLimitConfig(
            tokens_per_minute=60,  # 1 per second
            bucket_size=5,
            name="test",
        )
        limiter = InMemoryTokenBucket(config)

        # Should be able to acquire up to bucket_size immediately
        for _ in range(5):
            assert await limiter.acquire() is True

        # Next one should fail (bucket empty)
        assert await limiter.acquire() is False

    @pytest.mark.asyncio
    async def test_multi_priority_limiter(self):
        limiter = MultiPriorityRateLimiter()

        # Critical priority has no limit
        for _ in range(100):
            assert await limiter.acquire(Priority.CRITICAL) is True

        # Other priorities have limits
        acquired = 0
        for _ in range(20):
            if await limiter.acquire(Priority.MANUAL):
                acquired += 1

        # Should have hit rate limit
        assert acquired < 20


class TestConcurrencyController:
    """Test concurrency control."""

    @pytest.mark.asyncio
    async def test_acquire_release(self):
        controller = InMemoryConcurrencyController(max_concurrent=2)

        # Acquire two slots
        slot1 = await controller.acquire("worker-1", timeout=1.0)
        slot2 = await controller.acquire("worker-2", timeout=1.0)

        assert slot1 is not None
        assert slot2 is not None

        # Third should fail (no available slots)
        slot3 = await controller.acquire("worker-3", timeout=0.1)
        assert slot3 is None

        # Release one and try again
        await controller.release(slot1)

        slot3 = await controller.acquire("worker-3", timeout=1.0)
        assert slot3 is not None

    @pytest.mark.asyncio
    async def test_active_count(self):
        controller = InMemoryConcurrencyController(max_concurrent=5)

        assert await controller.get_active_count() == 0

        slot = await controller.acquire("worker-1")
        assert await controller.get_active_count() == 1

        await controller.release(slot)
        assert await controller.get_active_count() == 0


class TestQueueManager:
    """Test queue manager (in-memory version)."""

    @pytest.mark.asyncio
    async def test_enqueue_dequeue(self):
        manager = InMemoryQueueManager(max_concurrent=2)

        request = ValidationRequest(
            job_path="/jobs/test.py",
            trigger_source=TriggerSource.MANUAL,
            priority=Priority.CI_CD,
        )

        # Enqueue
        result, job_id = await manager.enqueue(request)
        assert result == EnqueueResult.QUEUED
        assert job_id is not None

        # Dequeue
        item = await manager.dequeue("worker-1", timeout=1.0)
        assert item is not None

        dequeued_request, slot = item
        assert dequeued_request.job_path == request.job_path

    @pytest.mark.asyncio
    async def test_deduplication(self):
        manager = InMemoryQueueManager()

        request = ValidationRequest(
            job_path="/jobs/test.py",
            trigger_source=TriggerSource.MANUAL,
            commit_sha="abc123",
        )

        # First enqueue succeeds
        result1, _ = await manager.enqueue(request)
        assert result1 == EnqueueResult.QUEUED

        # Second enqueue with same dedup key should be deduplicated
        request2 = ValidationRequest(
            job_path="/jobs/test.py",
            trigger_source=TriggerSource.GIT_WEBHOOK,
            commit_sha="abc123",
        )

        result2, _ = await manager.enqueue(request2)
        assert result2 == EnqueueResult.DEDUPLICATED

    @pytest.mark.asyncio
    async def test_priority_ordering(self):
        manager = InMemoryQueueManager(max_concurrent=5)

        # Enqueue jobs with different priorities
        low = ValidationRequest(
            job_path="/jobs/low.py",
            trigger_source=TriggerSource.MANUAL,
            priority=Priority.BATCH,
        )
        high = ValidationRequest(
            job_path="/jobs/high.py",
            trigger_source=TriggerSource.MANUAL,
            priority=Priority.CRITICAL,
        )

        await manager.enqueue(low)
        await manager.enqueue(high)

        # Higher priority should be dequeued first
        item = await manager.dequeue("worker-1", timeout=1.0)
        assert item is not None
        assert item[0].priority == Priority.CRITICAL

    @pytest.mark.asyncio
    async def test_queue_stats(self):
        manager = InMemoryQueueManager(max_concurrent=5)

        # Initially empty
        stats = await manager.get_stats()
        assert stats.total_pending == 0

        # Add some jobs
        for i in range(3):
            await manager.enqueue(ValidationRequest(
                job_path=f"/jobs/test_{i}.py",
                trigger_source=TriggerSource.MANUAL,
            ), skip_dedup=True)

        stats = await manager.get_stats()
        assert stats.total_pending == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
