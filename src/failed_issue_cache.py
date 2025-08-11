"""Failed Issue Cache to avoid repeated 404 requests."""

import json
import time
from pathlib import Path
from typing import Dict, Set
from loguru import logger


class FailedIssueCache:
    """Cache system to remember failed issue requests and avoid repeated 404 attempts."""
    
    def __init__(self, cache_dir: str = "cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.cache_file = self.cache_dir / "failed_issues.json"
        
        # In-memory cache: repo_url -> set of failed issue numbers
        self.failed_issues: Dict[str, Set[int]] = {}

        # Repository-level failures: repo_url -> (failure_reason, timestamp)
        self.failed_repositories: Dict[str, tuple] = {}

        # Circuit breaker data: repo_url -> failure count and block time
        self.failure_counts: Dict[str, int] = {}
        self.blocked_until: Dict[str, float] = {}
        
        # Configuration
        self.failure_threshold = 10  # Block repo after 10 consecutive failures
        self.block_timeout = 300  # Block for 5 minutes
        self.max_cache_age = 86400 * 7  # Cache entries expire after 24 hours
        
        # Load existing cache
        self._load_cache()
    
    def _get_safe_repo_name(self, repo_url: str) -> str:
        """Convert repository URL to safe filename."""
        return repo_url.replace("https://", "").replace("/", "_")
    
    def _load_cache(self) -> None:
        """Load failed issues cache from disk."""
        try:
            if not self.cache_file.exists():
                return
            
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Load failed issues
            failed_data = data.get('failed_issues', {})
            for repo_url, issue_data in failed_data.items():
                if isinstance(issue_data, dict):
                    # New format with timestamps
                    current_time = time.time()
                    valid_issues = set()
                    for issue_str, timestamp in issue_data.items():
                        if current_time - timestamp < self.max_cache_age:
                            valid_issues.add(int(issue_str))
                    self.failed_issues[repo_url] = valid_issues
                else:
                    # Legacy format - assume recent
                    self.failed_issues[repo_url] = set(issue_data)
            
            # Load repository-level failures
            failed_repos_data = data.get('failed_repositories', {})
            for repo_url, failure_info in failed_repos_data.items():
                if isinstance(failure_info, dict):
                    reason = failure_info.get('reason', 'Unknown failure')
                    timestamp = failure_info.get('timestamp', time.time())
                    # Check if failure is still valid (not expired)
                    if time.time() - timestamp < self.max_cache_age:
                        self.failed_repositories[repo_url] = (reason, timestamp)
                elif isinstance(failure_info, list) and len(failure_info) == 2:
                    # Legacy format: [reason, timestamp]
                    reason, timestamp = failure_info
                    if time.time() - timestamp < self.max_cache_age:
                        self.failed_repositories[repo_url] = (reason, timestamp)

            # Load circuit breaker data
            self.failure_counts = data.get('failure_counts', {})
            self.blocked_until = data.get('blocked_until', {})

            repo_count = len(self.failed_issues)
            failed_repo_count = len(self.failed_repositories)
            logger.info(f"Loaded failed issue cache with {repo_count} repositories ({failed_repo_count} repository-level failures)")
            
        except Exception as e:
            logger.warning(f"Failed to load failed issue cache: {e}")
            self.failed_issues = {}
            self.failed_repositories = {}
            self.failure_counts = {}
            self.blocked_until = {}
    
    def _save_cache(self) -> None:
        """Save failed issues cache to disk."""
        try:
            current_time = time.time()
            
            # Convert sets to dict with timestamps for persistence
            failed_data = {}
            for repo_url, issues in self.failed_issues.items():
                failed_data[repo_url] = {str(issue): current_time for issue in issues}

            # Convert repository failures to dict format
            failed_repos_data = {}
            for repo_url, (reason, timestamp) in self.failed_repositories.items():
                failed_repos_data[repo_url] = {
                    'reason': reason,
                    'timestamp': timestamp
                }

            data = {
                'failed_issues': failed_data,
                'failed_repositories': failed_repos_data,
                'failure_counts': self.failure_counts,
                'blocked_until': self.blocked_until,
                'last_updated': current_time
            }
            
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
                
        except Exception as e:
            logger.error(f"Failed to save failed issue cache: {e}")
    
    def is_failed(self, repo_url: str, issue_number: int) -> bool:
        """Check if an issue has previously failed."""
        return issue_number in self.failed_issues.get(repo_url, set())
    
    def mark_failed(self, repo_url: str, issue_number: int) -> None:
        """Mark an issue as failed."""
        if repo_url not in self.failed_issues:
            self.failed_issues[repo_url] = set()
        
        self.failed_issues[repo_url].add(issue_number)
        
        # Update failure count for circuit breaker
        self.failure_counts[repo_url] = self.failure_counts.get(repo_url, 0) + 1
        
        # Check if we should block this repository
        if self.failure_counts[repo_url] >= self.failure_threshold:
            self.blocked_until[repo_url] = time.time() + self.block_timeout
            logger.warning(f"Blocking issue requests to {repo_url} for {self.block_timeout}s due to {self.failure_counts[repo_url]} failures")
        
        # Save cache periodically
        if len(self.failed_issues.get(repo_url, set())) % 5 == 0:
            self._save_cache()
    
    def mark_success(self, repo_url: str) -> None:
        """Mark a successful request to reset failure count."""
        if repo_url in self.failure_counts:
            self.failure_counts[repo_url] = 0
        
        # Remove block if it exists
        if repo_url in self.blocked_until:
            del self.blocked_until[repo_url]
    
    def should_attempt_repo(self, repo_url: str) -> bool:
        """Check if we should attempt requests to this repository (circuit breaker)."""
        if repo_url in self.blocked_until:
            if time.time() < self.blocked_until[repo_url]:
                return False
            else:
                # Reset after timeout
                del self.blocked_until[repo_url]
                self.failure_counts[repo_url] = 0
        
        return True
    
    def get_failed_count(self, repo_url: str) -> int:
        """Get the number of failed issues for a repository."""
        return len(self.failed_issues.get(repo_url, set()))

    def is_repository_failed(self, repo_url: str) -> bool:
        """Check if a repository has a cached failure."""
        if repo_url not in self.failed_repositories:
            return False

        # Check if failure is still valid (not expired)
        reason, timestamp = self.failed_repositories[repo_url]
        if time.time() - timestamp >= self.max_cache_age:
            # Remove expired failure
            del self.failed_repositories[repo_url]
            return False

        return True

    def get_repository_failure_reason(self, repo_url: str) -> str:
        """Get the failure reason for a repository."""
        if repo_url in self.failed_repositories:
            reason, timestamp = self.failed_repositories[repo_url]
            # Check if still valid
            if time.time() - timestamp < self.max_cache_age:
                return reason
            else:
                # Remove expired failure
                del self.failed_repositories[repo_url]
        return ""

    def mark_repository_failed(self, repo_url: str, reason: str) -> None:
        """Mark a repository as failed with a specific reason."""
        current_time = time.time()
        self.failed_repositories[repo_url] = (reason, current_time)

        logger.info(f"Cached repository failure: {repo_url} - {reason}")

        # Save cache immediately for repository-level failures
        self._save_cache()

    def remove_repository_failure(self, repo_url: str) -> None:
        """Remove a repository from the failed cache (e.g., if it succeeds later)."""
        if repo_url in self.failed_repositories:
            del self.failed_repositories[repo_url]
            logger.debug(f"Removed repository failure cache for: {repo_url}")

    def get_failed_repositories(self) -> Dict[str, str]:
        """Get all failed repositories with their reasons."""
        current_time = time.time()
        valid_failures = {}

        # Clean up expired failures while building the result
        for repo_url in list(self.failed_repositories.keys()):
            reason, timestamp = self.failed_repositories[repo_url]
            if current_time - timestamp < self.max_cache_age:
                valid_failures[repo_url] = reason
            else:
                del self.failed_repositories[repo_url]

        return valid_failures
    
    def cleanup_expired(self) -> None:
        """Remove expired cache entries."""
        current_time = time.time()
        
        for repo_url in list(self.failed_issues.keys()):
            # For now, keep all entries since we don't have timestamps in memory
            # This will be handled during load/save operations
            pass
        
        # Clean up expired blocks
        for repo_url in list(self.blocked_until.keys()):
            if current_time >= self.blocked_until[repo_url]:
                del self.blocked_until[repo_url]
                self.failure_counts[repo_url] = 0
    
    def get_stats(self) -> Dict:
        """Get cache statistics."""
        total_failed = sum(len(issues) for issues in self.failed_issues.values())
        blocked_repos = len([url for url, block_time in self.blocked_until.items()
                           if time.time() < block_time])

        # Count valid repository failures (not expired)
        current_time = time.time()
        valid_repo_failures = len([url for url, (reason, timestamp) in self.failed_repositories.items()
                                 if current_time - timestamp < self.max_cache_age])

        return {
            'total_failed_issues': total_failed,
            'repositories_with_failures': len(self.failed_issues),
            'failed_repositories': valid_repo_failures,
            'currently_blocked_repos': blocked_repos,
            'total_failure_counts': sum(self.failure_counts.values())
        }
    
    def __del__(self):
        """Save cache on destruction."""
        try:
            self._save_cache()
        except:
            pass
