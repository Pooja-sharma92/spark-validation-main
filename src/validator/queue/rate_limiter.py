"""
Token Bucket Rate Limiter for controlling validation throughput.

The token bucket algorithm allows for controlled bursting while maintaining
a steady average rate. Tokens are added to a bucket at a fixed rate, and
each request consumes one token. If no tokens are available, the request
must wait or be rejected.
"""

import time
import asyncio
from dataclasses import dataclass
from typing import Optional, Dict
from abc import ABC, abstractmethod
import redis.asyncio as redis


class RateLimiter(ABC):
    """Abstract base class for rate limiters."""

    @abstractmethod
    async def acquire(self, key: str = "default") -> bool:
        """
        Attempt to acquire a token.

        Args:
            key: Identifier for the rate limit bucket (e.g., priority level)

        Returns:
            True if token acquired, False if rate limit exceeded
        """
        pass

    @abstractmethod
    async def get_wait_time(self, key: str = "default") -> float:
        """
        Get the time to wait before a token becomes available.

        Args:
            key: Identifier for the rate limit bucket

        Returns:
            Seconds to wait, 0 if a token is available
        """
        pass

    @abstractmethod
    async def reset(self, key: str = "default") -> None:
        """Reset the rate limiter for a specific key."""
        pass


@dataclass
class RateLimitConfig:
    """Configuration for a rate limit bucket."""
    tokens_per_minute: float  # Rate at which tokens are added
    bucket_size: int          # Maximum tokens (burst capacity)
    name: str = "default"

    @property
    def tokens_per_second(self) -> float:
        return self.tokens_per_minute / 60.0


class InMemoryTokenBucket(RateLimiter):
    """
    In-memory token bucket implementation.

    Suitable for single-process deployments or testing.
    For distributed systems, use RedisTokenBucket.
    """

    def __init__(self, config: RateLimitConfig):
        self.config = config
        self._buckets: Dict[str, dict] = {}

    def _get_bucket(self, key: str) -> dict:
        """Get or create a bucket for the given key."""
        if key not in self._buckets:
            self._buckets[key] = {
                "tokens": float(self.config.bucket_size),
                "last_update": time.time(),
            }
        return self._buckets[key]

    def _refill(self, bucket: dict) -> None:
        """Add tokens based on elapsed time since last update."""
        now = time.time()
        elapsed = now - bucket["last_update"]
        tokens_to_add = elapsed * self.config.tokens_per_second

        bucket["tokens"] = min(
            self.config.bucket_size,
            bucket["tokens"] + tokens_to_add
        )
        bucket["last_update"] = now

    async def acquire(self, key: str = "default") -> bool:
        """Attempt to acquire a token."""
        bucket = self._get_bucket(key)
        self._refill(bucket)

        if bucket["tokens"] >= 1.0:
            bucket["tokens"] -= 1.0
            return True
        return False

    async def get_wait_time(self, key: str = "default") -> float:
        """Calculate time until a token is available."""
        bucket = self._get_bucket(key)
        self._refill(bucket)

        if bucket["tokens"] >= 1.0:
            return 0.0

        tokens_needed = 1.0 - bucket["tokens"]
        return tokens_needed / self.config.tokens_per_second

    async def reset(self, key: str = "default") -> None:
        """Reset the bucket to full capacity."""
        if key in self._buckets:
            self._buckets[key] = {
                "tokens": float(self.config.bucket_size),
                "last_update": time.time(),
            }


class RedisTokenBucket(RateLimiter):
    """
    Redis-backed token bucket for distributed rate limiting.

    Uses Redis for token storage, enabling rate limiting across
    multiple worker processes and machines.

    The implementation uses a Lua script for atomic operations.
    """

    # Lua script for atomic token bucket operations
    ACQUIRE_SCRIPT = """
    local key = KEYS[1]
    local rate = tonumber(ARGV[1])
    local bucket_size = tonumber(ARGV[2])
    local now = tonumber(ARGV[3])

    -- Get current state or initialize
    local data = redis.call('HMGET', key, 'tokens', 'last_update')
    local tokens = tonumber(data[1]) or bucket_size
    local last_update = tonumber(data[2]) or now

    -- Refill tokens based on elapsed time
    local elapsed = now - last_update
    tokens = math.min(bucket_size, tokens + elapsed * rate)

    -- Try to acquire a token
    local acquired = 0
    if tokens >= 1 then
        tokens = tokens - 1
        acquired = 1
    end

    -- Save state
    redis.call('HMSET', key, 'tokens', tokens, 'last_update', now)
    redis.call('EXPIRE', key, 3600)  -- Expire after 1 hour of inactivity

    return {acquired, tokens}
    """

    WAIT_TIME_SCRIPT = """
    local key = KEYS[1]
    local rate = tonumber(ARGV[1])
    local bucket_size = tonumber(ARGV[2])
    local now = tonumber(ARGV[3])

    local data = redis.call('HMGET', key, 'tokens', 'last_update')
    local tokens = tonumber(data[1]) or bucket_size
    local last_update = tonumber(data[2]) or now

    local elapsed = now - last_update
    tokens = math.min(bucket_size, tokens + elapsed * rate)

    if tokens >= 1 then
        return 0
    end

    local tokens_needed = 1 - tokens
    return tokens_needed / rate
    """

    def __init__(self, redis_client: redis.Redis, config: RateLimitConfig):
        self.redis = redis_client
        self.config = config
        self._acquire_sha: Optional[str] = None
        self._wait_sha: Optional[str] = None

    async def _ensure_scripts(self) -> None:
        """Load Lua scripts into Redis if not already loaded."""
        if self._acquire_sha is None:
            self._acquire_sha = await self.redis.script_load(self.ACQUIRE_SCRIPT)
        if self._wait_sha is None:
            self._wait_sha = await self.redis.script_load(self.WAIT_TIME_SCRIPT)

    def _make_key(self, key: str) -> str:
        """Generate Redis key for this rate limiter."""
        return f"ratelimit:{self.config.name}:{key}"

    async def acquire(self, key: str = "default") -> bool:
        """Attempt to acquire a token using Redis."""
        await self._ensure_scripts()

        redis_key = self._make_key(key)
        now = time.time()

        result = await self.redis.evalsha(
            self._acquire_sha,
            1,
            redis_key,
            self.config.tokens_per_second,
            self.config.bucket_size,
            now,
        )

        return bool(result[0])

    async def get_wait_time(self, key: str = "default") -> float:
        """Get wait time until a token is available."""
        await self._ensure_scripts()

        redis_key = self._make_key(key)
        now = time.time()

        result = await self.redis.evalsha(
            self._wait_sha,
            1,
            redis_key,
            self.config.tokens_per_second,
            self.config.bucket_size,
            now,
        )

        return float(result)

    async def reset(self, key: str = "default") -> None:
        """Reset the bucket to full capacity."""
        redis_key = self._make_key(key)
        await self.redis.delete(redis_key)


class MultiPriorityRateLimiter:
    """
    Rate limiter with different limits per priority level.

    Each priority level can have its own rate limit configuration,
    allowing higher priority requests to bypass or have higher limits.
    """

    # Default rate limits per priority (tokens per minute, burst size)
    DEFAULT_LIMITS = {
        0: None,                              # CRITICAL: No limit
        1: RateLimitConfig(10, 5, "manual"),  # MANUAL: 10/min, burst 5
        2: RateLimitConfig(30, 10, "ci_cd"),  # CI_CD: 30/min, burst 10
        3: RateLimitConfig(100, 20, "batch"), # BATCH: 100/min, burst 20
    }

    def __init__(
        self,
        redis_client: Optional[redis.Redis] = None,
        configs: Optional[Dict[int, Optional[RateLimitConfig]]] = None,
    ):
        self.redis = redis_client
        self.configs = configs or self.DEFAULT_LIMITS
        self._limiters: Dict[int, Optional[RateLimiter]] = {}

        # Initialize limiters for each priority
        for priority, config in self.configs.items():
            if config is None:
                self._limiters[priority] = None  # No limit
            elif redis_client is not None:
                self._limiters[priority] = RedisTokenBucket(redis_client, config)
            else:
                self._limiters[priority] = InMemoryTokenBucket(config)

    async def acquire(self, priority: int) -> bool:
        """
        Attempt to acquire a token for the given priority.

        Args:
            priority: Priority level (0=CRITICAL, 3=BATCH)

        Returns:
            True if acquired, False if rate limited
        """
        limiter = self._limiters.get(priority)
        if limiter is None:
            return True  # No limit for this priority
        return await limiter.acquire()

    async def get_wait_time(self, priority: int) -> float:
        """Get wait time for the given priority level."""
        limiter = self._limiters.get(priority)
        if limiter is None:
            return 0.0
        return await limiter.get_wait_time()

    async def wait_for_token(
        self,
        priority: int,
        max_wait: float = 60.0,
    ) -> bool:
        """
        Wait until a token is available or timeout.

        Args:
            priority: Priority level
            max_wait: Maximum seconds to wait

        Returns:
            True if token acquired, False if timed out
        """
        start_time = time.time()

        while True:
            if await self.acquire(priority):
                return True

            elapsed = time.time() - start_time
            if elapsed >= max_wait:
                return False

            wait_time = await self.get_wait_time(priority)
            remaining = max_wait - elapsed

            # Sleep for the minimum of wait_time and remaining time
            sleep_time = min(wait_time, remaining, 1.0)  # Cap at 1 second
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
