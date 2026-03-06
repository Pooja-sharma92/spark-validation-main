"""
Concurrency Controller for limiting parallel validations.

Implements a distributed semaphore pattern using Redis to control
the maximum number of concurrent validation tasks across all workers.
"""

import asyncio
import time
import uuid
from dataclasses import dataclass
from typing import Optional, Set, AsyncContextManager
from contextlib import asynccontextmanager
from abc import ABC, abstractmethod
import redis.asyncio as redis


@dataclass
class ConcurrencySlot:
    """Represents an acquired concurrency slot."""
    slot_id: str
    worker_id: str
    acquired_at: float
    job_id: Optional[str] = None


class ConcurrencyController(ABC):
    """Abstract base class for concurrency control."""

    @abstractmethod
    async def acquire(
        self,
        worker_id: str,
        job_id: Optional[str] = None,
        timeout: float = 30.0,
    ) -> Optional[ConcurrencySlot]:
        """
        Attempt to acquire a concurrency slot.

        Args:
            worker_id: Identifier for the worker requesting the slot
            job_id: Optional job ID associated with this slot
            timeout: Maximum seconds to wait for a slot

        Returns:
            ConcurrencySlot if acquired, None if timed out
        """
        pass

    @abstractmethod
    async def release(self, slot: ConcurrencySlot) -> bool:
        """
        Release a previously acquired slot.

        Args:
            slot: The slot to release

        Returns:
            True if released successfully
        """
        pass

    @abstractmethod
    async def get_active_count(self) -> int:
        """Get the number of currently active slots."""
        pass

    @abstractmethod
    async def get_active_slots(self) -> list[ConcurrencySlot]:
        """Get details of all active slots."""
        pass

    @asynccontextmanager
    async def slot(
        self,
        worker_id: str,
        job_id: Optional[str] = None,
        timeout: float = 30.0,
    ) -> AsyncContextManager[Optional[ConcurrencySlot]]:
        """
        Context manager for acquiring and releasing a slot.

        Usage:
            async with controller.slot(worker_id, job_id) as slot:
                if slot:
                    # Do work
                else:
                    # Handle timeout
        """
        slot = await self.acquire(worker_id, job_id, timeout)
        try:
            yield slot
        finally:
            if slot:
                await self.release(slot)


class InMemoryConcurrencyController(ConcurrencyController):
    """
    In-memory concurrency controller for single-process deployments.

    Uses asyncio.Semaphore for local concurrency control.
    """

    def __init__(self, max_concurrent: int = 5):
        self.max_concurrent = max_concurrent
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._active_slots: dict[str, ConcurrencySlot] = {}
        self._lock = asyncio.Lock()

    async def acquire(
        self,
        worker_id: str,
        job_id: Optional[str] = None,
        timeout: float = 30.0,
    ) -> Optional[ConcurrencySlot]:
        try:
            acquired = await asyncio.wait_for(
                self._semaphore.acquire(),
                timeout=timeout,
            )
            if not acquired:
                return None
        except asyncio.TimeoutError:
            return None

        slot_id = str(uuid.uuid4())
        slot = ConcurrencySlot(
            slot_id=slot_id,
            worker_id=worker_id,
            acquired_at=time.time(),
            job_id=job_id,
        )

        async with self._lock:
            self._active_slots[slot_id] = slot

        return slot

    async def release(self, slot: ConcurrencySlot) -> bool:
        async with self._lock:
            if slot.slot_id in self._active_slots:
                del self._active_slots[slot.slot_id]
                self._semaphore.release()
                return True
        return False

    async def get_active_count(self) -> int:
        return self.max_concurrent - self._semaphore._value

    async def get_active_slots(self) -> list[ConcurrencySlot]:
        async with self._lock:
            return list(self._active_slots.values())


class RedisConcurrencyController(ConcurrencyController):
    """
    Redis-backed concurrency controller for distributed deployments.

    Uses Redis sets with TTL for tracking active slots, providing
    automatic cleanup of stale slots from crashed workers.
    """

    # Key patterns
    KEY_ACTIVE_SET = "validation:concurrency:active"
    KEY_SLOT_PREFIX = "validation:concurrency:slot:"

    # Lua script for atomic acquire
    ACQUIRE_SCRIPT = """
    local active_key = KEYS[1]
    local slot_key = KEYS[2]
    local max_concurrent = tonumber(ARGV[1])
    local slot_id = ARGV[2]
    local worker_id = ARGV[3]
    local job_id = ARGV[4]
    local now = ARGV[5]
    local ttl = tonumber(ARGV[6])

    -- Check current count
    local current = redis.call('SCARD', active_key)
    if current >= max_concurrent then
        return 0
    end

    -- Add to active set
    redis.call('SADD', active_key, slot_id)

    -- Store slot details
    redis.call('HMSET', slot_key,
        'slot_id', slot_id,
        'worker_id', worker_id,
        'job_id', job_id,
        'acquired_at', now
    )
    redis.call('EXPIRE', slot_key, ttl)

    return 1
    """

    # Lua script for atomic release
    RELEASE_SCRIPT = """
    local active_key = KEYS[1]
    local slot_key = KEYS[2]
    local slot_id = ARGV[1]

    -- Remove from active set
    local removed = redis.call('SREM', active_key, slot_id)

    -- Delete slot details
    redis.call('DEL', slot_key)

    return removed
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        max_concurrent: int = 5,
        slot_ttl: int = 3600,  # 1 hour TTL for stale slot cleanup
    ):
        self.redis = redis_client
        self.max_concurrent = max_concurrent
        self.slot_ttl = slot_ttl
        self._acquire_sha: Optional[str] = None
        self._release_sha: Optional[str] = None

    async def _ensure_scripts(self) -> None:
        """Load Lua scripts into Redis."""
        if self._acquire_sha is None:
            self._acquire_sha = await self.redis.script_load(self.ACQUIRE_SCRIPT)
        if self._release_sha is None:
            self._release_sha = await self.redis.script_load(self.RELEASE_SCRIPT)

    def _slot_key(self, slot_id: str) -> str:
        return f"{self.KEY_SLOT_PREFIX}{slot_id}"

    async def acquire(
        self,
        worker_id: str,
        job_id: Optional[str] = None,
        timeout: float = 30.0,
    ) -> Optional[ConcurrencySlot]:
        await self._ensure_scripts()

        slot_id = str(uuid.uuid4())
        slot_key = self._slot_key(slot_id)
        now = time.time()

        start_time = time.time()

        while True:
            # Try to acquire
            result = await self.redis.evalsha(
                self._acquire_sha,
                2,
                self.KEY_ACTIVE_SET,
                slot_key,
                self.max_concurrent,
                slot_id,
                worker_id,
                job_id or "",
                str(now),
                self.slot_ttl,
            )

            if result == 1:
                return ConcurrencySlot(
                    slot_id=slot_id,
                    worker_id=worker_id,
                    acquired_at=now,
                    job_id=job_id,
                )

            # Check timeout
            elapsed = time.time() - start_time
            if elapsed >= timeout:
                return None

            # Wait before retry (exponential backoff with jitter)
            wait_time = min(0.1 * (2 ** min(int(elapsed), 5)), 1.0)
            await asyncio.sleep(wait_time)

    async def release(self, slot: ConcurrencySlot) -> bool:
        await self._ensure_scripts()

        slot_key = self._slot_key(slot.slot_id)

        result = await self.redis.evalsha(
            self._release_sha,
            2,
            self.KEY_ACTIVE_SET,
            slot_key,
            slot.slot_id,
        )

        return bool(result)

    async def get_active_count(self) -> int:
        return await self.redis.scard(self.KEY_ACTIVE_SET)

    async def get_active_slots(self) -> list[ConcurrencySlot]:
        # Get all active slot IDs
        slot_ids = await self.redis.smembers(self.KEY_ACTIVE_SET)

        slots = []
        for slot_id in slot_ids:
            slot_key = self._slot_key(slot_id.decode() if isinstance(slot_id, bytes) else slot_id)
            data = await self.redis.hgetall(slot_key)

            if data:
                # Decode bytes to str
                data = {
                    k.decode() if isinstance(k, bytes) else k:
                    v.decode() if isinstance(v, bytes) else v
                    for k, v in data.items()
                }

                slots.append(ConcurrencySlot(
                    slot_id=data.get("slot_id", ""),
                    worker_id=data.get("worker_id", ""),
                    acquired_at=float(data.get("acquired_at", 0)),
                    job_id=data.get("job_id") or None,
                ))

        return slots

    async def cleanup_stale_slots(self, max_age_seconds: float = 3600) -> int:
        """
        Remove slots that have been active for too long (likely from crashed workers).

        Returns:
            Number of stale slots cleaned up
        """
        now = time.time()
        cleaned = 0

        slots = await self.get_active_slots()
        for slot in slots:
            age = now - slot.acquired_at
            if age > max_age_seconds:
                if await self.release(slot):
                    cleaned += 1

        return cleaned

    async def extend_slot(self, slot: ConcurrencySlot) -> bool:
        """
        Extend the TTL of an active slot (heartbeat).

        Workers should call this periodically for long-running validations.
        """
        slot_key = self._slot_key(slot.slot_id)
        return await self.redis.expire(slot_key, self.slot_ttl)


def create_concurrency_controller(
    redis_client: Optional[redis.Redis] = None,
    max_concurrent: int = 5,
) -> ConcurrencyController:
    """
    Factory function to create appropriate concurrency controller.

    Args:
        redis_client: Redis client for distributed mode, None for in-memory
        max_concurrent: Maximum concurrent validations

    Returns:
        ConcurrencyController instance
    """
    if redis_client is not None:
        return RedisConcurrencyController(redis_client, max_concurrent)
    return InMemoryConcurrencyController(max_concurrent)
