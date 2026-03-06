"""
File Watcher - Monitor file system changes and trigger validations.

Uses watchdog library to detect file modifications, with debouncing
to avoid triggering multiple validations for rapid consecutive changes.
"""

import asyncio
import fnmatch
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Set, Dict, List, Callable, Awaitable
from collections import defaultdict

from watchdog.observers import Observer
from watchdog.events import (
    FileSystemEventHandler,
    FileCreatedEvent,
    FileModifiedEvent,
    FileMovedEvent,
    DirCreatedEvent,
)
import structlog

from validator.models import TriggerSource, ValidationRequest, Priority
from validator.queue.manager import QueueManager
from validator.trigger.gateway import TriggerGateway, TriggerEvent
from validator.config import FrameworkConfig, get_config


logger = structlog.get_logger(__name__)


@dataclass
class WatcherConfig:
    """Configuration for file watcher."""
    paths: List[str] = field(default_factory=lambda: ["./jobs"])
    patterns: List[str] = field(default_factory=lambda: ["*.py"])
    ignore_patterns: List[str] = field(default_factory=lambda: [
        "**/test_*.py",
        "**/__pycache__/**",
        "**/.git/**",
        "**/*.pyc",
    ])
    debounce_seconds: float = 5.0
    recursive: bool = True


class DebouncedEventCollector:
    """
    Collects file events and debounces them.

    When multiple changes happen to the same file in quick succession,
    only triggers one validation after the debounce period.
    """

    def __init__(
        self,
        debounce_seconds: float = 5.0,
        callback: Optional[Callable[[Set[str]], Awaitable[None]]] = None,
    ):
        self.debounce_seconds = debounce_seconds
        self.callback = callback

        # Track pending files and their last modification time
        self._pending_files: Dict[str, float] = {}
        self._lock = threading.Lock()
        self._timer: Optional[threading.Timer] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def set_event_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Set the asyncio event loop for callbacks."""
        self._loop = loop

    def add_file(self, file_path: str) -> None:
        """
        Add a file to the pending set.

        Resets the debounce timer.
        """
        with self._lock:
            self._pending_files[file_path] = time.time()
            self._reset_timer()

    def _reset_timer(self) -> None:
        """Reset the debounce timer."""
        if self._timer:
            self._timer.cancel()

        self._timer = threading.Timer(
            self.debounce_seconds,
            self._flush,
        )
        self._timer.daemon = True
        self._timer.start()

    def _flush(self) -> None:
        """Flush pending files and trigger callback."""
        with self._lock:
            if not self._pending_files:
                return

            files = set(self._pending_files.keys())
            self._pending_files.clear()

        if self.callback and self._loop:
            # Schedule callback in the asyncio event loop
            asyncio.run_coroutine_threadsafe(
                self.callback(files),
                self._loop,
            )

        logger.info(
            "Debounce flush",
            file_count=len(files),
        )

    def stop(self) -> None:
        """Stop the debounce timer."""
        if self._timer:
            self._timer.cancel()
            self._timer = None


class SparkJobEventHandler(FileSystemEventHandler):
    """
    Watchdog event handler for Spark job files.

    Filters events based on patterns and forwards to debouncer.
    """

    def __init__(
        self,
        config: WatcherConfig,
        debouncer: DebouncedEventCollector,
    ):
        super().__init__()
        self.config = config
        self.debouncer = debouncer

    def _should_process(self, path: str) -> bool:
        """Check if file should be processed."""
        # Must match at least one pattern
        matched = False
        for pattern in self.config.patterns:
            if fnmatch.fnmatch(path, pattern):
                matched = True
                break

        if not matched:
            return False

        # Must not match any ignore pattern
        for pattern in self.config.ignore_patterns:
            if fnmatch.fnmatch(path, pattern):
                return False

        return True

    def on_created(self, event: FileCreatedEvent) -> None:
        """Handle file creation."""
        if event.is_directory:
            return

        if self._should_process(event.src_path):
            logger.debug("File created", path=event.src_path)
            self.debouncer.add_file(event.src_path)

    def on_modified(self, event: FileModifiedEvent) -> None:
        """Handle file modification."""
        if event.is_directory:
            return

        if self._should_process(event.src_path):
            logger.debug("File modified", path=event.src_path)
            self.debouncer.add_file(event.src_path)

    def on_moved(self, event: FileMovedEvent) -> None:
        """Handle file move/rename."""
        if event.is_directory:
            return

        # Treat destination as a new/modified file
        if self._should_process(event.dest_path):
            logger.debug("File moved", src=event.src_path, dest=event.dest_path)
            self.debouncer.add_file(event.dest_path)


class FileWatcher:
    """
    Main file watcher service.

    Monitors configured directories for changes and triggers
    validations through the TriggerGateway.
    """

    def __init__(
        self,
        gateway: TriggerGateway,
        config: Optional[WatcherConfig] = None,
    ):
        self.gateway = gateway
        self.config = config or self._load_config_from_framework()

        self._observer: Optional[Observer] = None
        self._debouncer: Optional[DebouncedEventCollector] = None
        self._running = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def _load_config_from_framework(self) -> WatcherConfig:
        """Load watcher config from framework configuration."""
        framework_config = get_config()
        fw_config = framework_config.triggers.file_watcher

        return WatcherConfig(
            paths=fw_config.paths,
            patterns=["*.py"],  # Always watch Python files
            ignore_patterns=fw_config.ignore_patterns,
            debounce_seconds=fw_config.debounce_seconds,
            recursive=True,
        )

    async def _handle_changed_files(self, files: Set[str]) -> None:
        """
        Handle a batch of changed files.

        Creates a TriggerEvent and processes through the gateway.
        """
        if not files:
            return

        event = TriggerEvent(
            source=TriggerSource.FILE_WATCHER,
            files=list(files),
            metadata={
                "watcher_paths": self.config.paths,
            },
        )

        result = await self.gateway.process_event(event)

        logger.info(
            "File changes processed",
            files=len(files),
            queued=result.queued,
            deduplicated=result.deduplicated,
        )

    def start(self, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        """
        Start watching for file changes.

        Args:
            loop: The asyncio event loop for callbacks
        """
        if self._running:
            logger.warning("File watcher already running")
            return

        self._loop = loop or asyncio.get_event_loop()

        # Create debouncer
        self._debouncer = DebouncedEventCollector(
            debounce_seconds=self.config.debounce_seconds,
            callback=self._handle_changed_files,
        )
        self._debouncer.set_event_loop(self._loop)

        # Create event handler
        handler = SparkJobEventHandler(self.config, self._debouncer)

        # Create observer
        self._observer = Observer()

        # Schedule watches for each path
        for path in self.config.paths:
            # Expand glob patterns to directories
            expanded_path = self._expand_path(path)
            if expanded_path and os.path.isdir(expanded_path):
                self._observer.schedule(
                    handler,
                    expanded_path,
                    recursive=self.config.recursive,
                )
                logger.info("Watching directory", path=expanded_path)
            else:
                logger.warning("Path not found or not a directory", path=path)

        self._observer.start()
        self._running = True
        logger.info(
            "File watcher started",
            paths=self.config.paths,
            debounce=self.config.debounce_seconds,
        )

    def _expand_path(self, path: str) -> Optional[str]:
        """
        Expand a path pattern to an actual directory.

        For patterns like "./jobs/**/*.py", returns "./jobs".
        """
        # Remove glob patterns to get base directory
        parts = path.split("**")
        base = parts[0].rstrip("/")

        if not base:
            base = "."

        # Expand ~ and environment variables
        base = os.path.expanduser(base)
        base = os.path.expandvars(base)

        # Get absolute path
        base = os.path.abspath(base)

        return base if os.path.exists(base) else None

    def stop(self) -> None:
        """Stop watching for file changes."""
        if not self._running:
            return

        if self._debouncer:
            self._debouncer.stop()
            self._debouncer = None

        if self._observer:
            self._observer.stop()
            self._observer.join(timeout=5)
            self._observer = None

        self._running = False
        logger.info("File watcher stopped")

    @property
    def is_running(self) -> bool:
        """Check if watcher is running."""
        return self._running


async def run_file_watcher(
    gateway: TriggerGateway,
    config: Optional[WatcherConfig] = None,
) -> None:
    """
    Run file watcher as an async task.

    This is a convenience function for running the watcher in an
    asyncio context.
    """
    watcher = FileWatcher(gateway, config)
    loop = asyncio.get_event_loop()

    watcher.start(loop)

    try:
        # Keep running until cancelled
        while watcher.is_running:
            await asyncio.sleep(1)
    finally:
        watcher.stop()
