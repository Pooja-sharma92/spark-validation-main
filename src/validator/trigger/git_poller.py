"""
Git Repository Poller - Lightweight alternative to webhooks.

When webhooks are not available (firewall, local dev, etc.),
this poller checks for changes by periodically running git commands.

Three strategies available:
1. Local git fetch - Check remote for new commits
2. GitHub API polling - Query GitHub API for recent commits
3. RSS feed - Parse GitHub's commit RSS feed
"""

import asyncio
import subprocess
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Set, List, Dict, Any
import structlog

from validator.models import TriggerSource
from validator.trigger.gateway import TriggerGateway, TriggerEvent


logger = structlog.get_logger(__name__)


@dataclass
class PollerConfig:
    """Configuration for git poller."""
    # Polling interval in seconds
    interval_seconds: float = 60.0

    # Repository path (for local git strategy)
    repo_path: str = "."

    # Remote name to check
    remote: str = "origin"

    # Branches to monitor (empty = all)
    branches: List[str] = field(default_factory=lambda: ["main", "develop"])

    # GitHub API settings (for API strategy)
    github_token: Optional[str] = None
    github_repo: Optional[str] = None  # "owner/repo"

    # File patterns to trigger validation
    file_patterns: List[str] = field(default_factory=lambda: ["*.py"])


class LocalGitPoller:
    """
    Poll for changes using local git commands.

    This is the lightest option - no external dependencies,
    just uses git CLI commands.

    How it works:
    1. Run `git fetch` to get latest remote state
    2. Compare local HEAD with remote HEAD
    3. If different, get list of changed files
    4. Trigger validation for matching files
    """

    def __init__(
        self,
        gateway: TriggerGateway,
        config: Optional[PollerConfig] = None,
    ):
        self.gateway = gateway
        self.config = config or PollerConfig()
        self._running = False
        self._last_commits: Dict[str, str] = {}  # branch -> last seen commit

    def _run_git(self, *args: str) -> tuple[int, str, str]:
        """Run a git command and return (returncode, stdout, stderr)."""
        try:
            result = subprocess.run(
                ["git", *args],
                cwd=self.config.repo_path,
                capture_output=True,
                text=True,
                timeout=30,
            )
            return result.returncode, result.stdout.strip(), result.stderr.strip()
        except subprocess.TimeoutExpired:
            return -1, "", "Git command timed out"
        except Exception as e:
            return -1, "", str(e)

    async def _fetch_remote(self) -> bool:
        """Fetch latest changes from remote."""
        code, _, err = self._run_git("fetch", self.config.remote, "--prune")
        if code != 0:
            logger.warning("Git fetch failed", error=err)
            return False
        return True

    async def _get_remote_head(self, branch: str) -> Optional[str]:
        """Get the commit SHA of remote branch HEAD."""
        ref = f"{self.config.remote}/{branch}"
        code, sha, _ = self._run_git("rev-parse", ref)
        if code != 0:
            return None
        return sha

    async def _get_changed_files(
        self,
        old_commit: str,
        new_commit: str,
    ) -> List[str]:
        """Get list of files changed between two commits."""
        code, output, _ = self._run_git(
            "diff", "--name-only", f"{old_commit}..{new_commit}"
        )
        if code != 0:
            return []

        files = output.split("\n") if output else []

        # Filter by patterns
        import fnmatch
        matched = []
        for f in files:
            for pattern in self.config.file_patterns:
                if fnmatch.fnmatch(f, pattern):
                    matched.append(f)
                    break

        return matched

    async def _check_branch(self, branch: str) -> Optional[TriggerEvent]:
        """Check a single branch for changes."""
        remote_head = await self._get_remote_head(branch)
        if not remote_head:
            return None

        last_seen = self._last_commits.get(branch)

        if last_seen == remote_head:
            # No changes
            return None

        if last_seen is None:
            # First run, just record current state
            self._last_commits[branch] = remote_head
            logger.info("Initialized branch tracking", branch=branch, commit=remote_head[:8])
            return None

        # Changes detected!
        changed_files = await self._get_changed_files(last_seen, remote_head)

        if not changed_files:
            # Changes but no matching files
            self._last_commits[branch] = remote_head
            return None

        logger.info(
            "Changes detected",
            branch=branch,
            old_commit=last_seen[:8],
            new_commit=remote_head[:8],
            files=len(changed_files),
        )

        # Update last seen
        self._last_commits[branch] = remote_head

        return TriggerEvent(
            source=TriggerSource.GIT_WEBHOOK,  # Reuse same source type
            files=changed_files,
            branch=branch,
            commit_sha=remote_head,
            author="git-poller",
            metadata={
                "previous_commit": last_seen,
                "detection_method": "local_git_poll",
            },
        )

    async def _poll_once(self) -> None:
        """Perform one polling cycle."""
        # Fetch latest
        if not await self._fetch_remote():
            return

        # Check each branch
        for branch in self.config.branches:
            event = await self._check_branch(branch)
            if event:
                await self.gateway.process_event(event)

    async def start(self) -> None:
        """Start the polling loop."""
        if self._running:
            return

        self._running = True
        logger.info(
            "Git poller started",
            interval=self.config.interval_seconds,
            branches=self.config.branches,
        )

        while self._running:
            try:
                await self._poll_once()
            except Exception as e:
                logger.error("Poll cycle failed", error=str(e))

            await asyncio.sleep(self.config.interval_seconds)

    def stop(self) -> None:
        """Stop the polling loop."""
        self._running = False
        logger.info("Git poller stopped")


class GitHubAPIPoller:
    """
    Poll GitHub API for repository changes.

    Uses GitHub's REST API to check for new commits.
    Requires a GitHub token for higher rate limits.

    Rate limits:
    - Without token: 60 requests/hour
    - With token: 5000 requests/hour
    """

    GITHUB_API = "https://api.github.com"

    def __init__(
        self,
        gateway: TriggerGateway,
        config: Optional[PollerConfig] = None,
    ):
        self.gateway = gateway
        self.config = config or PollerConfig()
        self._running = False
        self._last_commits: Dict[str, str] = {}

        if not self.config.github_repo:
            raise ValueError("github_repo is required for GitHub API poller")

    async def _fetch_commits(self, branch: str) -> Optional[Dict[str, Any]]:
        """Fetch latest commit from GitHub API."""
        import httpx

        url = f"{self.GITHUB_API}/repos/{self.config.github_repo}/commits"
        params = {"sha": branch, "per_page": 1}

        headers = {"Accept": "application/vnd.github.v3+json"}
        if self.config.github_token:
            headers["Authorization"] = f"token {self.config.github_token}"

        async with httpx.AsyncClient() as client:
            response = await client.get(url, params=params, headers=headers)

            if response.status_code == 200:
                commits = response.json()
                return commits[0] if commits else None

            logger.warning(
                "GitHub API request failed",
                status=response.status_code,
                branch=branch,
            )
            return None

    async def _fetch_commit_files(self, sha: str) -> List[str]:
        """Fetch files changed in a specific commit."""
        import httpx

        url = f"{self.GITHUB_API}/repos/{self.config.github_repo}/commits/{sha}"

        headers = {"Accept": "application/vnd.github.v3+json"}
        if self.config.github_token:
            headers["Authorization"] = f"token {self.config.github_token}"

        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers)

            if response.status_code == 200:
                data = response.json()
                files = [f["filename"] for f in data.get("files", [])]

                # Filter by patterns
                import fnmatch
                matched = []
                for f in files:
                    for pattern in self.config.file_patterns:
                        if fnmatch.fnmatch(f, pattern):
                            matched.append(f)
                            break
                return matched

            return []

    async def _check_branch(self, branch: str) -> Optional[TriggerEvent]:
        """Check a branch for new commits."""
        commit_data = await self._fetch_commits(branch)
        if not commit_data:
            return None

        sha = commit_data["sha"]
        last_seen = self._last_commits.get(branch)

        if last_seen == sha:
            return None

        if last_seen is None:
            self._last_commits[branch] = sha
            logger.info("Initialized branch tracking", branch=branch, commit=sha[:8])
            return None

        # Get changed files
        files = await self._fetch_commit_files(sha)

        if not files:
            self._last_commits[branch] = sha
            return None

        logger.info(
            "Changes detected via GitHub API",
            branch=branch,
            commit=sha[:8],
            files=len(files),
        )

        self._last_commits[branch] = sha

        return TriggerEvent(
            source=TriggerSource.GIT_WEBHOOK,
            files=files,
            branch=branch,
            commit_sha=sha,
            author=commit_data.get("commit", {}).get("author", {}).get("name", "unknown"),
            metadata={
                "commit_message": commit_data.get("commit", {}).get("message", ""),
                "detection_method": "github_api_poll",
            },
        )

    async def start(self) -> None:
        """Start the polling loop."""
        if self._running:
            return

        self._running = True
        logger.info(
            "GitHub API poller started",
            repo=self.config.github_repo,
            interval=self.config.interval_seconds,
        )

        while self._running:
            try:
                for branch in self.config.branches:
                    event = await self._check_branch(branch)
                    if event:
                        await self.gateway.process_event(event)
            except Exception as e:
                logger.error("GitHub API poll failed", error=str(e))

            await asyncio.sleep(self.config.interval_seconds)

    def stop(self) -> None:
        """Stop polling."""
        self._running = False


class MultiRepoPoller:
    """
    Poll multiple remote Git repositories for changes.

    Uses RemoteRepoSyncer to:
    1. Clone repos that don't exist locally
    2. Fetch and detect changes on each poll cycle
    3. Classify new jobs using AI (if not already classified)
    4. Trigger validation for changed files

    This is the recommended approach for monitoring multiple
    remote repositories (e.g., GitHub Enterprise).
    """

    def __init__(
        self,
        gateway: TriggerGateway,
        base_path: str,
        repos: List[Dict[str, Any]],
        interval_seconds: float = 300.0,
        file_patterns: Optional[List[str]] = None,
        ai_config: Optional[Dict[str, Any]] = None,
    ):
        from validator.trigger.repo_syncer import RemoteRepoSyncer, RepoConfig

        self.gateway = gateway
        self.interval_seconds = interval_seconds
        self._running = False
        self._ai_config = ai_config

        # Lazy-loaded classifier components
        self._classifier = None
        self._storage = None
        self._category_manager = None

        # Convert dict configs to RepoConfig objects
        repo_configs = [
            RepoConfig(
                name=r["name"],
                url=r["url"],
                branches=r.get("branches", ["main"]),
                jobs_subdir=r.get("jobs_subdir"),
            )
            for r in repos
        ]

        self.syncer = RemoteRepoSyncer(
            base_path=base_path,
            repos=repo_configs,
            file_patterns=file_patterns or ["*.py"],
        )

    def _init_classifier(self) -> bool:
        """Lazy initialize classifier components."""
        if self._classifier is not None:
            return True

        if not self._ai_config or not self._ai_config.get("enabled", False):
            logger.debug("AI classification disabled")
            return False

        try:
            from validator.ai.classifier import LLMClassifier
            from validator.ai.classifier.storage import ClassificationStorage
            from validator.ai.classifier.category_manager import CategoryManager

            self._classifier = LLMClassifier({"ai": self._ai_config})
            self._storage = ClassificationStorage()
            self._category_manager = CategoryManager(self._storage)
            logger.info("AI classifier initialized for repo sync")
            return True
        except Exception as e:
            logger.warning("Failed to initialize classifier", error=str(e))
            return False

    async def _classify_job_if_needed(self, job_path: str) -> None:
        """
        Classify a job file if not already in database.

        This runs classification once per new job, saving results
        to PostgreSQL for the TreeView UI.
        """
        if not self._init_classifier():
            return

        try:
            # Check if already classified
            existing = self._storage.get_classification(job_path)
            if existing:
                logger.debug("Job already classified", path=job_path)
                return

            # Read job code
            from pathlib import Path
            job_file = Path(job_path)
            if not job_file.exists() or not job_file.suffix == '.py':
                return

            code = job_file.read_text(encoding='utf-8')

            # Get existing categories for matching
            existing_categories = self._category_manager.get_existing_categories()

            # Classify
            logger.info("Classifying new job", path=job_path)
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

            logger.info(
                "Job classified",
                path=job_path,
                domain=result.domain,
                module=result.module,
                job_group=result.job_group,
                complexity=result.complexity.value,
            )

        except Exception as e:
            logger.warning("Classification failed", path=job_path, error=str(e))

    async def _poll_once(self) -> None:
        """Perform one polling cycle across all repos."""
        results = self.syncer.sync_all()

        for result in results:
            if not result.success:
                logger.warning(
                    "Sync failed",
                    repo=result.repo_name,
                    error=result.error,
                )
                continue

            if result.has_changes:
                # Convert changed files to absolute paths
                abs_files = [
                    str(self.syncer.get_job_path(result.repo_name, f))
                    for f in result.changed_files
                ]

                # Classify each new/changed job before queuing
                # This is a one-time operation per job
                for job_path in abs_files:
                    await self._classify_job_if_needed(job_path)

                event = TriggerEvent(
                    source=TriggerSource.GIT_WEBHOOK,
                    files=abs_files,
                    branch=result.branch,
                    commit_sha=result.new_commit,
                    author="multi-repo-poller",
                    metadata={
                        "repo_name": result.repo_name,
                        "previous_commit": result.old_commit,
                        "detection_method": "multi_repo_poll",
                    },
                )

                await self.gateway.process_event(event)

    async def start(self) -> None:
        """Start the polling loop."""
        if self._running:
            return

        self._running = True
        logger.info(
            "Multi-repo poller started",
            interval=self.interval_seconds,
            repos=[r.name for r in self.syncer.repos],
        )

        while self._running:
            try:
                await self._poll_once()
            except Exception as e:
                logger.error("Poll cycle failed", error=str(e))

            await asyncio.sleep(self.interval_seconds)

    def stop(self) -> None:
        """Stop the polling loop."""
        self._running = False
        logger.info("Multi-repo poller stopped")

    def get_job_paths(self) -> List[Path]:
        """Get all monitored job directories."""
        return self.syncer.get_all_job_paths()


def create_git_poller(
    gateway: TriggerGateway,
    strategy: str = "local",  # "local", "github_api", or "multi_repo"
    config: Optional[PollerConfig] = None,
    # For multi_repo strategy:
    base_path: Optional[str] = None,
    repos: Optional[List[Dict[str, Any]]] = None,
    interval_seconds: float = 300.0,
    file_patterns: Optional[List[str]] = None,
    ai_config: Optional[Dict[str, Any]] = None,
):
    """
    Factory function to create appropriate git poller.

    Args:
        gateway: Trigger gateway to send events to
        strategy: "local" for git CLI, "github_api" for GitHub API,
                  "multi_repo" for multiple remote repositories
        config: Poller configuration (for local/github_api strategies)
        base_path: Base path for cloning repos (multi_repo only)
        repos: List of repo configs (multi_repo only)
        interval_seconds: Polling interval (multi_repo only)
        file_patterns: File patterns to match (multi_repo only)
        ai_config: AI configuration for job classification (multi_repo only)

    Returns:
        LocalGitPoller, GitHubAPIPoller, or MultiRepoPoller instance
    """
    if strategy == "multi_repo":
        if not base_path or not repos:
            raise ValueError("base_path and repos required for multi_repo strategy")
        return MultiRepoPoller(
            gateway=gateway,
            base_path=base_path,
            repos=repos,
            interval_seconds=interval_seconds,
            file_patterns=file_patterns,
            ai_config=ai_config,
        )
    if strategy == "github_api":
        return GitHubAPIPoller(gateway, config)
    return LocalGitPoller(gateway, config)
