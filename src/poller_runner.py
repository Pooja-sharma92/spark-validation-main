#!/usr/bin/env python3
"""
Git Poller Runner - Monitors remote GitHub repositories and queues jobs.

Usage:
    python poller_runner.py [--config CONFIG_PATH] [--once]

This script:
1. Loads configuration from framework.yaml
2. Initializes Redis queue
3. Syncs configured GitHub repositories
4. Detects changed .py files and queues them for validation
5. Polls every 5 minutes (configurable)
"""

import asyncio
import argparse
import signal
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

import yaml


def load_config(config_path: str = None) -> dict:
    """Load configuration from YAML file."""
    if config_path is None:
        config_path = Path(__file__).parent / "config" / "framework.yaml"

    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Interpolate environment variables
    import os
    import re

    def interpolate(obj):
        if isinstance(obj, str):
            pattern = r'\$\{([^}:]+)(?::([^}]*))?\}'
            def replace(match):
                var_name = match.group(1)
                default = match.group(2) or ""
                return os.environ.get(var_name, default)
            return re.sub(pattern, replace, obj)
        elif isinstance(obj, dict):
            return {k: interpolate(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [interpolate(v) for v in obj]
        return obj

    return interpolate(config)


class SimpleQueueClient:
    """Simple Redis queue client for enqueueing validation jobs."""

    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        self.redis_url = redis_url
        self._redis = None

    async def connect(self):
        import redis.asyncio as redis
        self._redis = await redis.from_url(
            self.redis_url,
            encoding="utf-8",
            decode_responses=True,
        )
        print(f"✓ Connected to Redis: {self.redis_url}")

    async def close(self):
        if self._redis:
            await self._redis.close()

    async def enqueue_job(
        self,
        job_path: str,
        repo_name: str,
        branch: str,
        commit_sha: str,
        priority: int = 2,
    ) -> str:
        """
        Enqueue a validation job.

        Returns job_id.
        """
        import time
        import uuid
        import json
        from datetime import datetime

        job_id = str(uuid.uuid4())

        # Job metadata
        job_data = {
            "id": job_id,
            "job_path": job_path,
            "trigger_source": "GIT_WEBHOOK",
            "priority": str(priority),
            "branch": branch,
            "commit_sha": commit_sha,
            "status": "PENDING",
            "created_at": datetime.utcnow().isoformat(),
            "metadata": json.dumps({"repo_name": repo_name}),
        }

        # Store job metadata
        job_key = f"validation:job:{job_id}"
        await self._redis.hset(job_key, mapping=job_data)
        await self._redis.expire(job_key, 86400)  # 24 hour TTL

        # Add to priority queue
        queue_key = f"validation:queue:priority:{priority}"
        await self._redis.zadd(queue_key, {job_id: time.time()})

        return job_id

    async def get_queue_stats(self) -> dict:
        """Get queue statistics."""
        stats = {}
        total = 0
        for priority in range(4):
            queue_key = f"validation:queue:priority:{priority}"
            count = await self._redis.zcard(queue_key)
            stats[f"P{priority}"] = count
            total += count
        stats["total"] = total
        return stats


class PollerRunner:
    """Main poller runner that coordinates sync and queue."""

    def __init__(self, config: dict):
        self.config = config
        self.queue = None
        self.syncer = None
        self._running = False
        # Lazy-loaded classifier components
        self._classifier = None
        self._storage = None
        self._category_manager = None

    def _init_classifier(self) -> bool:
        """Lazy initialize classifier components."""
        if self._classifier is not None:
            return True

        ai_config = self.config.get("ai", {})
        if not ai_config.get("enabled", False):
            return False

        try:
            from validator.ai.classifier import LLMClassifier
            from validator.ai.classifier.storage import ClassificationStorage
            from validator.ai.classifier.category_manager import CategoryManager

            self._classifier = LLMClassifier({"ai": ai_config})
            self._storage = ClassificationStorage()
            self._category_manager = CategoryManager(self._storage)
            print("✓ AI classifier initialized")
            return True
        except Exception as e:
            print(f"  Note: AI classification unavailable ({e})")
            return False

    def _classify_job_if_needed(self, job_path: str) -> None:
        """Classify a job file if not already in database."""
        if not self._init_classifier():
            return

        try:
            # Check if already classified
            existing = self._storage.get_classification(job_path)
            if existing:
                return

            # Read job code
            from pathlib import Path
            job_file = Path(job_path)
            if not job_file.exists() or not job_file.suffix == '.py':
                return

            code = job_file.read_text(encoding='utf-8')
            existing_categories = self._category_manager.get_existing_categories()

            # Classify
            print(f"    ★ Classifying: {job_file.name}")
            result, provider = self._classifier.classify_job(
                code=code,
                file_path=job_path,
                existing_categories=existing_categories,
            )

            # Save to database
            self._storage.save_classification(
                job_path=job_path,
                classification=result,
                ai_provider=provider,
                category_manager=self._category_manager,
            )

            print(f"      → {result.domain}/{result.module}/{result.job_group} ({result.complexity.value})")

        except Exception as e:
            print(f"      Classification error: {e}")

    async def initialize(self):
        """Initialize components."""
        # Initialize queue client
        redis_url = self.config.get("redis", {}).get("url", "redis://localhost:6379/0")
        self.queue = SimpleQueueClient(redis_url)
        await self.queue.connect()

        # Initialize repo syncer (direct import to avoid circular deps)
        import subprocess
        from pathlib import Path
        from dataclasses import dataclass, field as dc_field
        from typing import List as TList, Optional as TOpt, Dict as TDict, Tuple

        @dataclass
        class RepoConfig:
            name: str
            url: str
            branches: TList[str] = dc_field(default_factory=lambda: ["main"])
            jobs_subdir: TOpt[str] = None

        @dataclass
        class SyncResult:
            repo_name: str
            success: bool
            branch: str
            old_commit: TOpt[str]
            new_commit: TOpt[str]
            changed_files: TList[str]
            error: TOpt[str] = None

            @property
            def has_changes(self) -> bool:
                return self.old_commit != self.new_commit and len(self.changed_files) > 0

        class RemoteRepoSyncer:
            def __init__(self, base_path: str, repos: TList[RepoConfig], file_patterns: TList[str] = None):
                self.base_path = Path(base_path)
                self.repos = repos
                self.file_patterns = file_patterns or ["*.py"]
                self._last_commits: TDict[str, TDict[str, str]] = {}
                self.base_path.mkdir(parents=True, exist_ok=True)

            def _run_git(self, repo_path: Path, *args: str, timeout: int = 60) -> Tuple[int, str, str]:
                try:
                    result = subprocess.run(["git", *args], cwd=str(repo_path), capture_output=True, text=True, timeout=timeout)
                    return result.returncode, result.stdout.strip(), result.stderr.strip()
                except Exception as e:
                    return -1, "", str(e)

            def _clone_repo(self, repo: RepoConfig) -> bool:
                repo_path = self.base_path / repo.name
                if repo_path.exists():
                    return True
                print(f"  Cloning {repo.name}...")
                try:
                    result = subprocess.run(["git", "clone", repo.url, str(repo_path)], capture_output=True, text=True, timeout=300)
                    return result.returncode == 0
                except:
                    return False

            def _fetch_repo(self, repo: RepoConfig) -> bool:
                code, _, _ = self._run_git(self.base_path / repo.name, "fetch", "origin", "--prune")
                return code == 0

            def _get_current_commit(self, repo: RepoConfig, branch: str) -> TOpt[str]:
                code, sha, _ = self._run_git(self.base_path / repo.name, "rev-parse", f"origin/{branch}")
                return sha if code == 0 else None

            def _get_changed_files(self, repo: RepoConfig, old: str, new: str) -> TList[str]:
                code, out, _ = self._run_git(self.base_path / repo.name, "diff", "--name-only", f"{old}..{new}")
                if code != 0:
                    return []
                import fnmatch
                files = out.split("\n") if out else []
                matched = []
                for f in files:
                    for p in self.file_patterns:
                        if fnmatch.fnmatch(f, p):
                            if repo.jobs_subdir:
                                if f.startswith(repo.jobs_subdir + "/"):
                                    matched.append(f)
                            else:
                                matched.append(f)
                            break
                return matched

            def sync_repo(self, repo: RepoConfig) -> TList[SyncResult]:
                results = []
                if not self._clone_repo(repo):
                    return [SyncResult(repo.name, False, "", None, None, [], "Clone failed")]
                if not self._fetch_repo(repo):
                    return [SyncResult(repo.name, False, "", None, None, [], "Fetch failed")]

                repo_commits = self._last_commits.setdefault(repo.name, {})
                for branch in repo.branches:
                    new_commit = self._get_current_commit(repo, branch)
                    if not new_commit:
                        results.append(SyncResult(repo.name, False, branch, None, None, [], f"Branch {branch} not found"))
                        continue
                    old_commit = repo_commits.get(branch)
                    if old_commit is None:
                        repo_commits[branch] = new_commit
                        results.append(SyncResult(repo.name, True, branch, None, new_commit, []))
                        continue
                    changed = self._get_changed_files(repo, old_commit, new_commit) if old_commit != new_commit else []
                    repo_commits[branch] = new_commit
                    results.append(SyncResult(repo.name, True, branch, old_commit, new_commit, changed))
                return results

            def sync_all(self) -> TList[SyncResult]:
                all_results = []
                for repo in self.repos:
                    all_results.extend(self.sync_repo(repo))
                return all_results

            def get_job_path(self, repo_name: str, relative_path: str) -> Path:
                return self.base_path / repo_name / relative_path

        jobs_base_path = self.config.get("jobs", {}).get("base_path", "./sbi")
        poller_config = self.config.get("triggers", {}).get("git_poller", {})

        repos = []
        for repo in poller_config.get("repositories", []):
            repos.append(RepoConfig(
                name=repo["name"],
                url=repo["url"],
                branches=repo.get("branches", ["main"]),
                jobs_subdir=repo.get("jobs_subdir"),
            ))

        file_patterns = poller_config.get("file_patterns", ["*.py"])

        self.syncer = RemoteRepoSyncer(
            base_path=jobs_base_path,
            repos=repos,
            file_patterns=file_patterns,
        )

        self.interval = poller_config.get("interval_seconds", 300)

        print(f"✓ Initialized poller:")
        print(f"  Base path: {jobs_base_path}")
        print(f"  Repos: {[r.name for r in repos]}")
        print(f"  Interval: {self.interval}s")

    async def poll_once(self) -> int:
        """
        Perform one poll cycle.

        Returns number of jobs queued.
        """
        print("\n" + "=" * 50)
        print(f"Polling repositories...")

        results = self.syncer.sync_all()
        queued_count = 0

        for result in results:
            if not result.success:
                print(f"  ✗ [{result.repo_name}] Error: {result.error}")
                continue

            if result.has_changes:
                print(f"  ✓ [{result.repo_name}/{result.branch}] {len(result.changed_files)} changed files")

                for file_path in result.changed_files:
                    # Get absolute path
                    abs_path = str(self.syncer.get_job_path(result.repo_name, file_path))

                    # Classify new job if not already in database
                    self._classify_job_if_needed(abs_path)

                    job_id = await self.queue.enqueue_job(
                        job_path=abs_path,
                        repo_name=result.repo_name,
                        branch=result.branch,
                        commit_sha=result.new_commit,
                    )
                    print(f"    → Queued: {file_path} ({job_id[:8]})")
                    queued_count += 1
            else:
                print(f"  - [{result.repo_name}/{result.branch}] No changes (commit: {result.new_commit[:8] if result.new_commit else 'N/A'})")

        # Show queue stats
        stats = await self.queue.get_queue_stats()
        print(f"\nQueue status: {stats}")

        return queued_count

    async def run(self, once: bool = False):
        """Run the polling loop."""
        await self.initialize()

        if once:
            await self.poll_once()
            return

        self._running = True
        print(f"\nStarting polling loop (interval: {self.interval}s)")
        print("Press Ctrl+C to stop\n")

        while self._running:
            try:
                await self.poll_once()
            except Exception as e:
                print(f"Poll error: {e}")

            if self._running:
                await asyncio.sleep(self.interval)

    def stop(self):
        """Stop the polling loop."""
        self._running = False
        print("\nStopping poller...")

    async def cleanup(self):
        """Cleanup resources."""
        if self.queue:
            await self.queue.close()


async def main():
    parser = argparse.ArgumentParser(description="Git Poller Runner")
    parser.add_argument("--config", help="Path to config file")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    args = parser.parse_args()

    config = load_config(args.config)
    runner = PollerRunner(config)

    # Handle signals
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, runner.stop)

    try:
        await runner.run(once=args.once)
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
