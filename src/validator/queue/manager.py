"""
Queue Manager - Core orchestrator for validation task queuing.

Coordinates priority queues, rate limiting, concurrency control,
deduplication, and backpressure handling.
"""

import asyncio
import json
import time
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, List, Callable, Any, Union
from enum import Enum
import redis.asyncio as redis

from validator.models import (
    ValidationRequest,
    ValidationResult,
    ValidationStatus,
    Priority,
    QueueStats,
    BackpressureState,
)
from validator.queue.rate_limiter import (
    MultiPriorityRateLimiter,
    RateLimitConfig,
)
from validator.queue.concurrency import (
    ConcurrencyController,
    RedisConcurrencyController,
    InMemoryConcurrencyController,
    ConcurrencySlot,
)


logger = logging.getLogger(__name__)


class EnqueueResult(str, Enum):
    """Result of attempting to enqueue a validation request."""
    QUEUED = "queued"
    DEDUPLICATED = "deduplicated"
    RATE_LIMITED = "rate_limited"
    REJECTED_BACKPRESSURE = "rejected_backpressure"


@dataclass
class BackpressureConfig:
    """Configuration for backpressure thresholds."""
    warning_threshold: int = 50
    critical_threshold: int = 100
    reject_threshold: int = 200


@dataclass
class QueueManagerConfig:
    """Configuration for the queue manager."""
    redis_url: str = "redis://localhost:6379/0"
    max_concurrent: int = 5
    dedup_window_seconds: int = 60
    backpressure: BackpressureConfig = field(default_factory=BackpressureConfig)
    job_ttl_seconds: int = 86400  # 24 hours

    # Rate limit configs per priority (None = no limit)
    rate_limits: Dict[int, Optional[RateLimitConfig]] = field(default_factory=lambda: {
        0: None,  # CRITICAL: No limit
        1: RateLimitConfig(10, 5, "manual"),
        2: RateLimitConfig(30, 10, "ci_cd"),
        3: RateLimitConfig(100, 20, "batch"),
    })


class QueueManager:
    """
    Central manager for the validation queue system.

    Handles:
    - Multi-priority queue with Redis sorted sets
    - Rate limiting per priority level
    - Concurrency control for parallel validations
    - Request deduplication
    - Backpressure monitoring and handling
    - Dead letter queue for failed requests
    """

    # Redis key patterns
    KEY_QUEUE_PREFIX = "validation:queue:priority:"
    KEY_JOB_PREFIX = "validation:job:"
    KEY_DEDUP_PREFIX = "validation:dedup:"
    KEY_DEAD_LETTER = "validation:queue:dead_letter"
    KEY_STATS = "validation:stats"

    def __init__(self, config: Optional[QueueManagerConfig] = None):
        self.config = config or QueueManagerConfig()
        self._redis: Optional[redis.Redis] = None
        self._rate_limiter: Optional[MultiPriorityRateLimiter] = None
        self._concurrency: Optional[ConcurrencyController] = None
        self._initialized = False

    async def initialize(self) -> None:
        """Initialize Redis connection and components."""
        if self._initialized:
            return

        self._redis = await redis.from_url(
            self.config.redis_url,
            encoding="utf-8",
            decode_responses=True,
        )

        # Initialize rate limiter with Redis backend
        self._rate_limiter = MultiPriorityRateLimiter(
            redis_client=self._redis,
            configs=self.config.rate_limits,
        )

        # Initialize concurrency controller
        self._concurrency = RedisConcurrencyController(
            redis_client=self._redis,
            max_concurrent=self.config.max_concurrent,
        )

        self._initialized = True
        logger.info("QueueManager initialized with Redis backend")

    async def close(self) -> None:
        """Close Redis connection."""
        if self._redis:
            await self._redis.close()
            self._initialized = False

    def _queue_key(self, priority: int) -> str:
        """Get Redis key for a priority queue."""
        return f"{self.KEY_QUEUE_PREFIX}{priority}"

    def _job_key(self, job_id: str) -> str:
        """Get Redis key for job metadata."""
        return f"{self.KEY_JOB_PREFIX}{job_id}"

    def _dedup_key(self, dedup_hash: str) -> str:
        """Get Redis key for deduplication cache."""
        return f"{self.KEY_DEDUP_PREFIX}{dedup_hash}"

    async def _check_dedup(self, request: ValidationRequest) -> Optional[str]:
        """
        Check if this request is a duplicate.

        Returns:
            Existing job_id if duplicate, None otherwise
        """
        dedup_key = self._dedup_key(request.dedup_key)
        existing_job_id = await self._redis.get(dedup_key)
        return existing_job_id

    async def _set_dedup(self, request: ValidationRequest) -> None:
        """Set deduplication cache entry."""
        dedup_key = self._dedup_key(request.dedup_key)
        await self._redis.setex(
            dedup_key,
            self.config.dedup_window_seconds,
            request.id,
        )

    async def get_backpressure_state(self) -> BackpressureState:
        """Get current backpressure state."""
        total = await self.get_queue_depth()
        return BackpressureState(
            queue_depth=total,
            warning_threshold=self.config.backpressure.warning_threshold,
            critical_threshold=self.config.backpressure.critical_threshold,
            reject_threshold=self.config.backpressure.reject_threshold,
        )

    async def enqueue(
        self,
        request: ValidationRequest,
        skip_dedup: bool = False,
        skip_rate_limit: bool = False,
    ) -> tuple[EnqueueResult, Optional[str]]:
        """
        Enqueue a validation request.

        Args:
            request: The validation request to queue
            skip_dedup: Skip deduplication check
            skip_rate_limit: Skip rate limiting

        Returns:
            Tuple of (result, job_id or existing_job_id)
        """
        if not self._initialized:
            await self.initialize()

        # Check deduplication
        if not skip_dedup:
            existing_id = await self._check_dedup(request)
            if existing_id:
                logger.debug(f"Request deduplicated: {request.job_path} -> {existing_id}")
                return EnqueueResult.DEDUPLICATED, existing_id

        # Check backpressure
        bp_state = await self.get_backpressure_state()
        if bp_state.should_reject_low_priority and request.priority >= Priority.BATCH:
            logger.warning(f"Request rejected due to backpressure: {request.job_path}")
            return EnqueueResult.REJECTED_BACKPRESSURE, None

        # Demote priority if critical backpressure
        if bp_state.should_delay_low_priority and request.priority >= Priority.CI_CD:
            original_priority = request.priority
            request.priority = Priority.BATCH
            logger.info(f"Request demoted from P{original_priority} to P{request.priority}: {request.job_path}")

        # Check rate limit
        if not skip_rate_limit:
            if not await self._rate_limiter.acquire(request.priority):
                logger.debug(f"Request rate limited: {request.job_path}")
                return EnqueueResult.RATE_LIMITED, None

        # Store job metadata
        job_key = self._job_key(request.id)
        job_data = request.to_dict()
        job_data["status"] = ValidationStatus.PENDING.value
        job_data["queued_at"] = datetime.utcnow().isoformat()

        await self._redis.hset(job_key, mapping=job_data)
        await self._redis.expire(job_key, self.config.job_ttl_seconds)

        # Add to priority queue (sorted set with timestamp as score)
        queue_key = self._queue_key(request.priority)
        score = time.time()
        await self._redis.zadd(queue_key, {request.id: score})

        # Set dedup cache
        await self._set_dedup(request)

        logger.info(f"Enqueued job {request.id}: {request.job_path} (P{request.priority})")
        return EnqueueResult.QUEUED, request.id

    async def dequeue(
        self,
        worker_id: str,
        timeout: float = 30.0,
    ) -> Optional[tuple[ValidationRequest, ConcurrencySlot]]:
        """
        Dequeue the highest priority request and acquire a concurrency slot.

        Args:
            worker_id: ID of the worker requesting work
            timeout: Maximum time to wait for a slot

        Returns:
            Tuple of (request, slot) or None if no work available
        """
        if not self._initialized:
            await self.initialize()

        # First, try to acquire a concurrency slot
        slot = await self._concurrency.acquire(worker_id, timeout=timeout)
        if not slot:
            return None

        try:
            # Try each priority queue in order (0 is highest)
            for priority in sorted(Priority):
                queue_key = self._queue_key(priority)

                # Pop the oldest item from the queue
                result = await self._redis.zpopmin(queue_key, count=1)
                if not result:
                    continue

                job_id, _ = result[0]

                # Get job metadata
                job_key = self._job_key(job_id)
                job_data = await self._redis.hgetall(job_key)

                if not job_data:
                    # Job expired or was deleted
                    continue

                # Update status to RUNNING
                await self._redis.hset(job_key, "status", ValidationStatus.RUNNING.value)
                await self._redis.hset(job_key, "started_at", datetime.utcnow().isoformat())
                await self._redis.hset(job_key, "worker_id", worker_id)

                # Update slot with job_id
                slot.job_id = job_id

                request = ValidationRequest.from_dict(job_data)
                logger.info(f"Dequeued job {job_id} for worker {worker_id}")
                return request, slot

            # No work available, release the slot
            await self._concurrency.release(slot)
            return None

        except Exception as e:
            # Release slot on error
            await self._concurrency.release(slot)
            raise

    async def complete(
        self,
        job_id: str,
        result: ValidationResult,
        slot: ConcurrencySlot,
    ) -> None:
        """
        Mark a job as completed and release resources.

        Args:
            job_id: The job ID
            result: The validation result
            slot: The concurrency slot to release
        """
        job_key = self._job_key(job_id)

        # Update job status
        await self._redis.hset(job_key, "status", result.status.value)
        await self._redis.hset(job_key, "completed_at", datetime.utcnow().isoformat())
        await self._redis.hset(job_key, "result", json.dumps(result.to_dict()))

        # Release concurrency slot
        await self._concurrency.release(slot)

        logger.info(f"Completed job {job_id}: {result.status.value}")

    async def fail(
        self,
        job_id: str,
        error_message: str,
        slot: ConcurrencySlot,
        move_to_dead_letter: bool = True,
    ) -> None:
        """
        Mark a job as failed and optionally move to dead letter queue.

        Args:
            job_id: The job ID
            error_message: Error description
            slot: The concurrency slot to release
            move_to_dead_letter: Whether to move to dead letter queue
        """
        job_key = self._job_key(job_id)

        await self._redis.hset(job_key, "status", ValidationStatus.ERROR.value)
        await self._redis.hset(job_key, "error_message", error_message)
        await self._redis.hset(job_key, "completed_at", datetime.utcnow().isoformat())

        if move_to_dead_letter:
            await self._redis.zadd(self.KEY_DEAD_LETTER, {job_id: time.time()})

        await self._concurrency.release(slot)

        logger.error(f"Failed job {job_id}: {error_message}")

    async def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get the current status of a job."""
        job_key = self._job_key(job_id)
        return await self._redis.hgetall(job_key)

    async def get_queue_depth(self, priority: Optional[int] = None) -> int:
        """
        Get the total number of pending jobs.

        Args:
            priority: Specific priority to check, or None for all

        Returns:
            Number of pending jobs
        """
        if priority is not None:
            queue_key = self._queue_key(priority)
            return await self._redis.zcard(queue_key)

        total = 0
        for p in Priority:
            queue_key = self._queue_key(p)
            total += await self._redis.zcard(queue_key)
        return total

    async def get_stats(self) -> QueueStats:
        """Get comprehensive queue statistics."""
        by_priority = {}
        total = 0

        for p in Priority:
            count = await self.get_queue_depth(p)
            by_priority[p.name] = count
            total += count

        active = await self._concurrency.get_active_count()

        # Get oldest pending job age
        oldest_age = None
        for p in Priority:
            queue_key = self._queue_key(p)
            oldest = await self._redis.zrange(queue_key, 0, 0, withscores=True)
            if oldest:
                _, score = oldest[0]
                age = time.time() - score
                if oldest_age is None or age > oldest_age:
                    oldest_age = age

        return QueueStats(
            total_pending=total,
            by_priority=by_priority,
            active_validations=active,
            max_concurrent=self.config.max_concurrent,
            oldest_pending_seconds=oldest_age,
        )

    async def get_dead_letter_jobs(self, limit: int = 100) -> List[str]:
        """Get job IDs from the dead letter queue."""
        return await self._redis.zrange(self.KEY_DEAD_LETTER, 0, limit - 1)

    async def requeue_from_dead_letter(
        self,
        job_id: str,
        new_priority: Optional[Priority] = None,
    ) -> bool:
        """
        Move a job from dead letter queue back to the main queue.

        Args:
            job_id: Job to requeue
            new_priority: New priority level (uses original if None)

        Returns:
            True if requeued successfully
        """
        # Remove from dead letter
        removed = await self._redis.zrem(self.KEY_DEAD_LETTER, job_id)
        if not removed:
            return False

        # Get job data
        job_key = self._job_key(job_id)
        job_data = await self._redis.hgetall(job_key)
        if not job_data:
            return False

        # Update priority if specified
        priority = new_priority or Priority(int(job_data.get("priority", Priority.CI_CD)))
        if new_priority:
            await self._redis.hset(job_key, "priority", new_priority.value)

        # Reset status
        await self._redis.hset(job_key, "status", ValidationStatus.PENDING.value)
        await self._redis.hdel(job_key, "error_message", "started_at", "completed_at")

        # Add back to queue
        queue_key = self._queue_key(priority)
        await self._redis.zadd(queue_key, {job_id: time.time()})

        logger.info(f"Requeued job {job_id} from dead letter (P{priority})")
        return True


class InMemoryQueueManager:
    """
    In-memory queue manager for testing and single-process deployments.

    Provides the same interface as QueueManager but without Redis dependency.
    """

    def __init__(self, max_concurrent: int = 5):
        self.max_concurrent = max_concurrent
        self._queues: Dict[int, List[tuple[float, ValidationRequest]]] = {
            p: [] for p in Priority
        }
        self._jobs: Dict[str, Dict[str, Any]] = {}
        self._dedup_cache: Dict[str, str] = {}
        self._dead_letter: List[str] = []
        self._rate_limiter = MultiPriorityRateLimiter()
        self._concurrency = InMemoryConcurrencyController(max_concurrent)
        self._lock = asyncio.Lock()

    async def initialize(self) -> None:
        """No-op for in-memory implementation."""
        pass

    async def close(self) -> None:
        """No-op for in-memory implementation."""
        pass

    async def enqueue(
        self,
        request: ValidationRequest,
        skip_dedup: bool = False,
        skip_rate_limit: bool = False,
    ) -> tuple[EnqueueResult, Optional[str]]:
        async with self._lock:
            # Check dedup
            if not skip_dedup and request.dedup_key in self._dedup_cache:
                return EnqueueResult.DEDUPLICATED, self._dedup_cache[request.dedup_key]

            # Check rate limit
            if not skip_rate_limit:
                if not await self._rate_limiter.acquire(request.priority):
                    return EnqueueResult.RATE_LIMITED, None

            # Store job
            self._jobs[request.id] = {
                **request.to_dict(),
                "status": ValidationStatus.PENDING.value,
            }

            # Add to queue
            import heapq
            heapq.heappush(self._queues[request.priority], (time.time(), request))

            # Set dedup
            self._dedup_cache[request.dedup_key] = request.id

            return EnqueueResult.QUEUED, request.id

    async def dequeue(
        self,
        worker_id: str,
        timeout: float = 30.0,
    ) -> Optional[tuple[ValidationRequest, ConcurrencySlot]]:
        slot = await self._concurrency.acquire(worker_id, timeout=timeout)
        if not slot:
            return None

        async with self._lock:
            import heapq
            for priority in sorted(Priority):
                if self._queues[priority]:
                    _, request = heapq.heappop(self._queues[priority])
                    self._jobs[request.id]["status"] = ValidationStatus.RUNNING.value
                    slot.job_id = request.id
                    return request, slot

        await self._concurrency.release(slot)
        return None

    async def get_queue_depth(self, priority: Optional[int] = None) -> int:
        if priority is not None:
            return len(self._queues.get(priority, []))
        return sum(len(q) for q in self._queues.values())

    async def get_stats(self) -> QueueStats:
        by_priority = {p.name: len(self._queues[p]) for p in Priority}
        active = await self._concurrency.get_active_count()
        return QueueStats(
            total_pending=sum(by_priority.values()),
            by_priority=by_priority,
            active_validations=active,
            max_concurrent=self.max_concurrent,
        )


def create_queue_manager(
    use_redis: bool = True,
    config: Optional[QueueManagerConfig] = None,
) -> Union[QueueManager, InMemoryQueueManager]:
    """
    Factory function to create appropriate queue manager.

    Args:
        use_redis: Whether to use Redis backend
        config: Queue manager configuration

    Returns:
        QueueManager or InMemoryQueueManager instance
    """
    if use_redis:
        return QueueManager(config)
    return InMemoryQueueManager(
        max_concurrent=config.max_concurrent if config else 5
    )
