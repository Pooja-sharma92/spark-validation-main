"""
Worker Pool - Manages a pool of validation workers.

Handles:
- Worker lifecycle management
- Auto-scaling based on queue depth
- Graceful shutdown with work completion
- Worker health monitoring
"""

import asyncio
import signal
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, List
import structlog

from validator.workers.worker import ValidationWorker, WorkerState, WorkerStats
from validator.queue.manager import QueueManager
from validator.config import get_config, FrameworkConfig

# Optional imports for storage and notifications
try:
    from validator.results.storage import ResultStorage
    from validator.results.notifier import NotificationManager
    from validator.results.reporter import ValidationReporter
    RESULTS_AVAILABLE = True
except ImportError:
    RESULTS_AVAILABLE = False
    ResultStorage = None
    NotificationManager = None
    ValidationReporter = None


logger = structlog.get_logger(__name__)


class PoolState(str, Enum):
    """Worker pool lifecycle states."""
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    SCALING = "scaling"
    STOPPING = "stopping"


@dataclass
class PoolConfig:
    """Configuration for worker pool."""
    min_workers: int = 2
    max_workers: int = 10
    scale_up_threshold: int = 10  # Queue depth to trigger scale up
    scale_down_threshold: int = 2  # Queue depth to trigger scale down
    scale_interval_seconds: float = 30.0  # Time between scaling checks
    graceful_shutdown_timeout: float = 60.0


@dataclass
class PoolStats:
    """Aggregate statistics for the worker pool."""
    active_workers: int = 0
    idle_workers: int = 0
    processing_workers: int = 0
    total_jobs_processed: int = 0
    total_jobs_succeeded: int = 0
    total_jobs_failed: int = 0
    uptime_seconds: float = 0.0
    started_at: Optional[datetime] = None

    @property
    def success_rate(self) -> float:
        if self.total_jobs_processed == 0:
            return 0.0
        return self.total_jobs_succeeded / self.total_jobs_processed * 100


class WorkerPool:
    """
    Manages a pool of validation workers.

    Features:
    - Automatic scaling based on queue depth
    - Health monitoring and failed worker replacement
    - Graceful shutdown with configurable timeout
    - Aggregated statistics
    """

    def __init__(
        self,
        queue_manager: QueueManager,
        config: Optional[PoolConfig] = None,
        framework_config: Optional[FrameworkConfig] = None,
        storage: Optional["ResultStorage"] = None,
        notifier: Optional["NotificationManager"] = None,
        reporter: Optional["ValidationReporter"] = None,
    ):
        self.queue_manager = queue_manager
        self.config = config or PoolConfig()
        self.framework_config = framework_config or get_config()

        # Storage and notification components (passed to workers)
        self.storage = storage
        self.notifier = notifier
        self.reporter = reporter

        self._state = PoolState.STOPPED
        self._workers: Dict[str, ValidationWorker] = {}
        self._worker_tasks: Dict[str, asyncio.Task] = {}
        self._started_at: Optional[datetime] = None
        self._scaling_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

    @property
    def state(self) -> PoolState:
        return self._state

    @property
    def worker_count(self) -> int:
        return len(self._workers)

    async def start(self) -> None:
        """Start the worker pool."""
        if self._state != PoolState.STOPPED:
            logger.warning("Pool already running", state=self._state.value)
            return

        logger.info(
            "Starting worker pool",
            min_workers=self.config.min_workers,
            max_workers=self.config.max_workers,
        )

        self._state = PoolState.STARTING
        self._started_at = datetime.utcnow()
        self._shutdown_event.clear()

        # Initialize queue manager
        await self.queue_manager.initialize()

        # Start minimum number of workers
        for _ in range(self.config.min_workers):
            await self._spawn_worker()

        # Start auto-scaling task
        self._scaling_task = asyncio.create_task(self._auto_scale_loop())

        self._state = PoolState.RUNNING
        logger.info("Worker pool started", worker_count=self.worker_count)

    async def stop(self, graceful: bool = True) -> None:
        """
        Stop the worker pool.

        Args:
            graceful: If True, wait for workers to complete current jobs
        """
        if self._state == PoolState.STOPPED:
            return

        logger.info(
            "Stopping worker pool",
            graceful=graceful,
            worker_count=self.worker_count,
        )

        self._state = PoolState.STOPPING
        self._shutdown_event.set()

        # Stop scaling task
        if self._scaling_task:
            self._scaling_task.cancel()
            try:
                await self._scaling_task
            except asyncio.CancelledError:
                pass

        # Stop all workers
        if graceful:
            await self._graceful_shutdown()
        else:
            await self._force_shutdown()

        # Close queue manager
        await self.queue_manager.close()

        self._state = PoolState.STOPPED
        logger.info("Worker pool stopped")

    async def _graceful_shutdown(self) -> None:
        """Gracefully stop all workers, waiting for current jobs."""
        # Signal all workers to stop
        for worker in self._workers.values():
            await worker.stop(graceful=True)

        # Wait for all worker tasks with timeout
        if self._worker_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._worker_tasks.values(), return_exceptions=True),
                    timeout=self.config.graceful_shutdown_timeout,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "Graceful shutdown timeout, forcing stop",
                    timeout=self.config.graceful_shutdown_timeout,
                )
                await self._force_shutdown()

    async def _force_shutdown(self) -> None:
        """Forcefully stop all workers."""
        for task in self._worker_tasks.values():
            task.cancel()

        if self._worker_tasks:
            await asyncio.gather(*self._worker_tasks.values(), return_exceptions=True)

        self._workers.clear()
        self._worker_tasks.clear()

    async def _spawn_worker(self) -> str:
        """Spawn a new worker and return its ID."""
        worker = ValidationWorker(
            queue_manager=self.queue_manager,
            config=self.framework_config,
            storage=self.storage,
            notifier=self.notifier,
            reporter=self.reporter,
        )

        self._workers[worker.worker_id] = worker

        # Start worker task
        task = asyncio.create_task(
            self._run_worker(worker),
            name=f"worker-{worker.worker_id}",
        )
        self._worker_tasks[worker.worker_id] = task

        logger.info("Spawned worker", worker_id=worker.worker_id)
        return worker.worker_id

    async def _run_worker(self, worker: ValidationWorker) -> None:
        """Run a worker and handle its lifecycle."""
        try:
            await worker.start()
        except Exception as e:
            logger.error(
                "Worker crashed",
                worker_id=worker.worker_id,
                error=str(e),
            )
        finally:
            # Clean up worker references
            self._workers.pop(worker.worker_id, None)
            self._worker_tasks.pop(worker.worker_id, None)

    async def _stop_worker(self, worker_id: str) -> None:
        """Stop a specific worker."""
        worker = self._workers.get(worker_id)
        if not worker:
            return

        await worker.stop(graceful=True)

        task = self._worker_tasks.get(worker_id)
        if task:
            try:
                await asyncio.wait_for(task, timeout=30.0)
            except asyncio.TimeoutError:
                task.cancel()

        logger.info("Stopped worker", worker_id=worker_id)

    async def _auto_scale_loop(self) -> None:
        """Periodically check queue and scale workers."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.scale_interval_seconds)

                if self._state != PoolState.RUNNING:
                    continue

                await self._check_and_scale()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Auto-scale error", error=str(e))

    async def _check_and_scale(self) -> None:
        """Check queue depth and scale workers if needed."""
        stats = await self.queue_manager.get_stats()
        queue_depth = stats.total_pending
        current_workers = len(self._workers)

        # Check if we need to scale up
        if queue_depth >= self.config.scale_up_threshold:
            if current_workers < self.config.max_workers:
                self._state = PoolState.SCALING
                workers_to_add = min(
                    2,  # Add at most 2 workers at a time
                    self.config.max_workers - current_workers,
                )
                for _ in range(workers_to_add):
                    await self._spawn_worker()
                logger.info(
                    "Scaled up workers",
                    added=workers_to_add,
                    total=len(self._workers),
                    queue_depth=queue_depth,
                )
                self._state = PoolState.RUNNING

        # Check if we need to scale down
        elif queue_depth <= self.config.scale_down_threshold:
            if current_workers > self.config.min_workers:
                self._state = PoolState.SCALING
                # Find idle workers to stop
                idle_workers = [
                    w for w in self._workers.values()
                    if w.state == WorkerState.IDLE or w.state == WorkerState.POLLING
                ]

                workers_to_remove = min(
                    1,  # Remove at most 1 worker at a time
                    len(idle_workers),
                    current_workers - self.config.min_workers,
                )

                for worker in idle_workers[:workers_to_remove]:
                    await self._stop_worker(worker.worker_id)

                logger.info(
                    "Scaled down workers",
                    removed=workers_to_remove,
                    total=len(self._workers),
                    queue_depth=queue_depth,
                )
                self._state = PoolState.RUNNING

    async def scale_to(self, target_workers: int) -> None:
        """
        Manually scale to a specific number of workers.

        Args:
            target_workers: Desired number of workers
        """
        target = max(
            self.config.min_workers,
            min(self.config.max_workers, target_workers),
        )

        current = len(self._workers)

        if target == current:
            return

        self._state = PoolState.SCALING

        if target > current:
            # Scale up
            for _ in range(target - current):
                await self._spawn_worker()
        else:
            # Scale down
            idle_workers = [
                w for w in self._workers.values()
                if w.state != WorkerState.PROCESSING
            ]

            to_remove = current - target
            for worker in idle_workers[:to_remove]:
                await self._stop_worker(worker.worker_id)

        self._state = PoolState.RUNNING
        logger.info(
            "Manual scale completed",
            target=target,
            actual=len(self._workers),
        )

    def get_stats(self) -> PoolStats:
        """Get aggregated pool statistics."""
        stats = PoolStats(
            active_workers=len(self._workers),
            started_at=self._started_at,
        )

        if self._started_at:
            stats.uptime_seconds = (datetime.utcnow() - self._started_at).total_seconds()

        for worker in self._workers.values():
            if worker.state == WorkerState.PROCESSING:
                stats.processing_workers += 1
            else:
                stats.idle_workers += 1

            worker_stats = worker.stats
            stats.total_jobs_processed += worker_stats.jobs_processed
            stats.total_jobs_succeeded += worker_stats.jobs_succeeded
            stats.total_jobs_failed += worker_stats.jobs_failed

        return stats

    def get_worker_details(self) -> List[Dict]:
        """Get detailed status of each worker."""
        details = []
        for worker in self._workers.values():
            details.append({
                "worker_id": worker.worker_id,
                "state": worker.state.value,
                "stats": worker.stats.__dict__,
            })
        return details


async def run_worker_pool(
    queue_manager: QueueManager,
    config: Optional[PoolConfig] = None,
) -> None:
    """
    Run worker pool as a standalone service.

    Handles signals for graceful shutdown.
    """
    pool = WorkerPool(queue_manager, config)

    # Set up signal handlers
    loop = asyncio.get_event_loop()
    shutdown_event = asyncio.Event()

    def signal_handler():
        logger.info("Received shutdown signal")
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, signal_handler)

    try:
        await pool.start()

        # Wait for shutdown signal
        await shutdown_event.wait()

    finally:
        await pool.stop(graceful=True)
