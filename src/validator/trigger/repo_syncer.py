"""
Remote Repository Syncer - Clone and sync multiple remote Git repositories.

This module handles:
1. Cloning repositories that don't exist locally
2. Fetching/pulling updates from remote
3. Detecting changed files between syncs
4. Working with GitHub Enterprise (e.g., github.ibm.com)
"""

import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Dict, Tuple
import structlog

logger = structlog.get_logger(__name__)


@dataclass
class RepoConfig:
    """Configuration for a single repository to monitor."""
    name: str
    url: str
    branches: List[str] = field(default_factory=lambda: ["main"])
    jobs_subdir: Optional[str] = None  # Subdirectory containing job files


@dataclass
class SyncResult:
    """Result of a repository sync operation."""
    repo_name: str
    success: bool
    branch: str
    old_commit: Optional[str]
    new_commit: Optional[str]
    changed_files: List[str]
    error: Optional[str] = None

    @property
    def has_changes(self) -> bool:
        return self.old_commit != self.new_commit and len(self.changed_files) > 0


class RemoteRepoSyncer:
    """
    Syncs multiple remote Git repositories to a local base directory.

    For each configured repository:
    - Clones if not present locally
    - Fetches and detects changes on each sync cycle
    - Returns list of changed files matching patterns
    """

    def __init__(
        self,
        base_path: str,
        repos: List[RepoConfig],
        file_patterns: Optional[List[str]] = None,
    ):
        self.base_path = Path(base_path)
        self.repos = repos
        self.file_patterns = file_patterns or ["*.py"]
        self._last_commits: Dict[str, Dict[str, str]] = {}  # repo_name -> {branch: sha}

        # Ensure base path exists
        self.base_path.mkdir(parents=True, exist_ok=True)
        logger.info("RemoteRepoSyncer initialized", base_path=str(self.base_path), repos=len(repos))

    def _run_git(
        self,
        repo_path: Path,
        *args: str,
        timeout: int = 60,
    ) -> Tuple[int, str, str]:
        """Run a git command and return (returncode, stdout, stderr)."""
        try:
            result = subprocess.run(
                ["git", *args],
                cwd=str(repo_path),
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            return result.returncode, result.stdout.strip(), result.stderr.strip()
        except subprocess.TimeoutExpired:
            return -1, "", f"Git command timed out after {timeout}s"
        except Exception as e:
            return -1, "", str(e)

    def _clone_repo(self, repo: RepoConfig) -> bool:
        """Clone a repository if it doesn't exist locally."""
        repo_path = self.base_path / repo.name

        if repo_path.exists():
            logger.debug("Repository already exists", repo=repo.name, path=str(repo_path))
            return True

        logger.info("Cloning repository", repo=repo.name, url=repo.url)

        try:
            result = subprocess.run(
                ["git", "clone", repo.url, str(repo_path)],
                capture_output=True,
                text=True,
                timeout=300,  # 5 minutes for clone
            )

            if result.returncode != 0:
                logger.error("Clone failed", repo=repo.name, error=result.stderr)
                return False

            logger.info("Repository cloned successfully", repo=repo.name)
            return True

        except subprocess.TimeoutExpired:
            logger.error("Clone timed out", repo=repo.name)
            return False
        except Exception as e:
            logger.error("Clone error", repo=repo.name, error=str(e))
            return False

    def _fetch_repo(self, repo: RepoConfig) -> bool:
        """Fetch latest changes from remote."""
        repo_path = self.base_path / repo.name

        code, _, err = self._run_git(repo_path, "fetch", "origin", "--prune")
        if code != 0:
            logger.warning("Fetch failed", repo=repo.name, error=err)
            return False
        return True

    def _get_current_commit(self, repo: RepoConfig, branch: str) -> Optional[str]:
        """Get current commit SHA for a branch."""
        repo_path = self.base_path / repo.name

        # Get remote branch HEAD
        code, sha, _ = self._run_git(repo_path, "rev-parse", f"origin/{branch}")
        if code != 0:
            return None
        return sha

    def _get_changed_files(
        self,
        repo: RepoConfig,
        old_commit: str,
        new_commit: str,
    ) -> List[str]:
        """Get list of files changed between two commits, filtered by patterns."""
        repo_path = self.base_path / repo.name

        code, output, _ = self._run_git(
            repo_path, "diff", "--name-only", f"{old_commit}..{new_commit}"
        )
        if code != 0:
            return []

        files = output.split("\n") if output else []

        # Filter by patterns
        import fnmatch
        matched = []
        for f in files:
            for pattern in self.file_patterns:
                if fnmatch.fnmatch(f, pattern):
                    # Prepend jobs_subdir filter if configured
                    if repo.jobs_subdir:
                        if f.startswith(repo.jobs_subdir + "/") or f.startswith(repo.jobs_subdir + "\\"):
                            matched.append(f)
                    else:
                        matched.append(f)
                    break

        return matched

    def _fast_forward_branch(self, repo: RepoConfig, branch: str) -> bool:
        """Fast-forward local branch to match remote."""
        repo_path = self.base_path / repo.name

        # Checkout branch if not current
        code, current_branch, _ = self._run_git(repo_path, "rev-parse", "--abbrev-ref", "HEAD")
        if code == 0 and current_branch != branch:
            code, _, err = self._run_git(repo_path, "checkout", branch)
            if code != 0:
                # Branch might not exist locally, create tracking branch
                code, _, err = self._run_git(
                    repo_path, "checkout", "-b", branch, f"origin/{branch}"
                )
                if code != 0:
                    logger.warning("Checkout failed", repo=repo.name, branch=branch, error=err)
                    return False

        # Fast-forward merge
        code, _, err = self._run_git(repo_path, "merge", "--ff-only", f"origin/{branch}")
        if code != 0:
            logger.warning("Fast-forward failed", repo=repo.name, branch=branch, error=err)
            # Try reset if ff failed
            code, _, _ = self._run_git(repo_path, "reset", "--hard", f"origin/{branch}")

        return True

    def sync_repo(self, repo: RepoConfig) -> List[SyncResult]:
        """
        Sync a single repository and return results for each branch.

        Returns list of SyncResult, one per branch.
        """
        results = []

        # Ensure repo is cloned
        if not self._clone_repo(repo):
            return [SyncResult(
                repo_name=repo.name,
                success=False,
                branch="",
                old_commit=None,
                new_commit=None,
                changed_files=[],
                error="Clone failed",
            )]

        # Fetch latest
        if not self._fetch_repo(repo):
            return [SyncResult(
                repo_name=repo.name,
                success=False,
                branch="",
                old_commit=None,
                new_commit=None,
                changed_files=[],
                error="Fetch failed",
            )]

        # Check each branch
        repo_commits = self._last_commits.setdefault(repo.name, {})

        for branch in repo.branches:
            new_commit = self._get_current_commit(repo, branch)

            if not new_commit:
                results.append(SyncResult(
                    repo_name=repo.name,
                    success=False,
                    branch=branch,
                    old_commit=None,
                    new_commit=None,
                    changed_files=[],
                    error=f"Branch {branch} not found",
                ))
                continue

            old_commit = repo_commits.get(branch)

            if old_commit is None:
                # First run - initialize tracking
                repo_commits[branch] = new_commit
                logger.info(
                    "Branch tracking initialized",
                    repo=repo.name,
                    branch=branch,
                    commit=new_commit[:8],
                )
                results.append(SyncResult(
                    repo_name=repo.name,
                    success=True,
                    branch=branch,
                    old_commit=None,
                    new_commit=new_commit,
                    changed_files=[],
                ))
                continue

            if old_commit == new_commit:
                # No changes
                results.append(SyncResult(
                    repo_name=repo.name,
                    success=True,
                    branch=branch,
                    old_commit=old_commit,
                    new_commit=new_commit,
                    changed_files=[],
                ))
                continue

            # Changes detected!
            changed_files = self._get_changed_files(repo, old_commit, new_commit)

            logger.info(
                "Changes detected",
                repo=repo.name,
                branch=branch,
                old_commit=old_commit[:8],
                new_commit=new_commit[:8],
                changed_files=len(changed_files),
            )

            # Update local branch
            self._fast_forward_branch(repo, branch)

            # Update tracking
            repo_commits[branch] = new_commit

            results.append(SyncResult(
                repo_name=repo.name,
                success=True,
                branch=branch,
                old_commit=old_commit,
                new_commit=new_commit,
                changed_files=changed_files,
            ))

        return results

    def sync_all(self) -> List[SyncResult]:
        """Sync all configured repositories."""
        all_results = []

        for repo in self.repos:
            try:
                results = self.sync_repo(repo)
                all_results.extend(results)
            except Exception as e:
                logger.error("Sync failed", repo=repo.name, error=str(e))
                all_results.append(SyncResult(
                    repo_name=repo.name,
                    success=False,
                    branch="",
                    old_commit=None,
                    new_commit=None,
                    changed_files=[],
                    error=str(e),
                ))

        return all_results

    def get_job_path(self, repo_name: str, relative_path: str) -> Path:
        """Get absolute path to a job file."""
        return self.base_path / repo_name / relative_path

    def get_all_job_paths(self) -> List[Path]:
        """Get all job directories (base_path/repo_name/jobs_subdir)."""
        paths = []
        for repo in self.repos:
            repo_path = self.base_path / repo.name
            if repo.jobs_subdir:
                paths.append(repo_path / repo.jobs_subdir)
            else:
                paths.append(repo_path)
        return paths
