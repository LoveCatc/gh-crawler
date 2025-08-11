"""Unified cache manager that replaces the three separate caching systems."""

import json
import threading
from pathlib import Path
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, asdict
from loguru import logger

from .models import PullRequestInfo
from .config import CACHE_DIR, CACHE_PR_SUBDIR, CACHE_CHECKPOINT_SUBDIR, CACHE_COMMITS_SUBDIR, CACHE_FLUSH_BATCH_SIZE, CACHE_FLUSH_INTERVAL


@dataclass
class RepositoryCrawlState:
    """Unified state for repository crawling progress."""
    repo_url: str
    total_prs_expected: int
    discovered_pr_urls: List[str]
    scraped_pr_numbers: List[int]
    failed_pr_urls: List[str]
    last_open_page: int
    last_closed_page: int
    open_pages_complete: bool
    closed_pages_complete: bool
    discovery_complete: bool
    scraping_complete: bool
    open_prs_found: int = 0
    closed_prs_found: int = 0
    commit_ids: List[str] = None  # Repository commit IDs
    pr_commit_mapping: Dict[int, List[str]] = None  # PR number -> commit IDs
    
    def __post_init__(self):
        if self.commit_ids is None:
            self.commit_ids = []
        if self.pr_commit_mapping is None:
            self.pr_commit_mapping = {}
    
    def to_dict(self) -> Dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'RepositoryCrawlState':
        # Add backward compatibility for new fields
        defaults = {
            'open_prs_found': 0,
            'closed_prs_found': 0,
            'commit_ids': [],
            'pr_commit_mapping': {}
        }
        for key, default_value in defaults.items():
            if key not in data:
                data[key] = default_value
        return cls(**data)


class UnifiedCacheManager:
    """Unified cache manager that handles all caching needs."""
    
    def __init__(self, cache_dir: str = CACHE_DIR):
        self.cache_dir = Path(cache_dir)
        self.pr_cache_dir = self.cache_dir / CACHE_PR_SUBDIR
        self.checkpoint_cache_dir = self.cache_dir / CACHE_CHECKPOINT_SUBDIR
        self.commits_cache_dir = self.cache_dir / CACHE_COMMITS_SUBDIR
        
        # Create cache directories
        self.cache_dir.mkdir(exist_ok=True)
        self.pr_cache_dir.mkdir(exist_ok=True)
        self.checkpoint_cache_dir.mkdir(exist_ok=True)
        self.commits_cache_dir.mkdir(exist_ok=True)
        
        # Thread safety for PR caching with optimized batch sizes
        self.write_lock = threading.Lock()
        self.write_queue = []
        self.batch_size = CACHE_FLUSH_BATCH_SIZE  # Increased from 10 to 20
        self.flush_interval = CACHE_FLUSH_INTERVAL  # Reduced from 30 to 15 seconds

        # Start background writer thread
        self.writer_thread = threading.Thread(target=self._background_writer, daemon=True)
        self.writer_thread.start()
    
    def _get_safe_repo_name(self, repo_url: str) -> str:
        """Convert repository URL to safe filename."""
        return repo_url.replace("https://", "").replace("/", "_")
    
    def is_repository_crawled_from_output(self, repo_url: str, output_dir: str) -> bool:
        """Check if repository exists in any output file (primary source of truth)."""
        try:
            output_path = Path(output_dir)
            if not output_path.exists():
                return False
            
            # Check all JSONL files in output directory
            for output_file in output_path.glob("*.jsonl"):
                try:
                    with open(output_file, 'r', encoding='utf-8') as f:
                        for line in f:
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                repo_data = json.loads(line)
                                if repo_data.get('url') == repo_url:
                                    return True
                            except json.JSONDecodeError:
                                continue
                except Exception as e:
                    logger.warning(f"Error reading output file {output_file}: {e}")
                    continue
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking output files: {e}")
            return False
    
    def get_crawled_repositories_from_output(self, output_dir: str) -> Set[str]:
        """Get all repository URLs from output files."""
        crawled_urls = set()
        try:
            output_path = Path(output_dir)
            if not output_path.exists():
                return crawled_urls
            
            for output_file in output_path.glob("*.jsonl"):
                try:
                    with open(output_file, 'r', encoding='utf-8') as f:
                        for line in f:
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                repo_data = json.loads(line)
                                if 'url' in repo_data:
                                    crawled_urls.add(repo_data['url'])
                            except json.JSONDecodeError:
                                continue
                except Exception as e:
                    logger.warning(f"Error reading output file {output_file}: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Error getting crawled repositories from output: {e}")
        
        return crawled_urls
    
    # PR Caching Methods
    def get_pr_cache_file(self, repo_url: str) -> Path:
        """Get PR cache file path for a repository."""
        safe_name = self._get_safe_repo_name(repo_url)
        return self.pr_cache_dir / f"{safe_name}_prs.jsonl"
    
    def cache_pr_immediately(self, repo_url: str, pr_info: PullRequestInfo) -> None:
        """Cache a single PR immediately (thread-safe)."""
        try:
            with self.write_lock:
                self.write_queue.append((repo_url, pr_info))
                
                # Flush if queue is getting large
                if len(self.write_queue) >= self.batch_size:
                    self._flush_queue()
                    
        except Exception as e:
            logger.error(f"Failed to queue PR for caching: {e}")
    
    def flush_cache(self) -> None:
        """Manually flush the write queue to disk (thread-safe public method)."""
        with self.write_lock:
            self._flush_queue()

    def _flush_queue(self) -> None:
        """Flush the write queue to disk."""
        if not self.write_queue:
            return

        try:
            # Group by repository
            repo_prs = {}
            for repo_url, pr_info in self.write_queue:
                if repo_url not in repo_prs:
                    repo_prs[repo_url] = []
                repo_prs[repo_url].append(pr_info)

            # Write to files
            for repo_url, prs in repo_prs.items():
                cache_file = self.get_pr_cache_file(repo_url)

                with open(cache_file, 'a', encoding='utf-8') as f:
                    for pr in prs:
                        pr_dict = pr.to_dict()
                        f.write(json.dumps(pr_dict, ensure_ascii=False) + '\n')

                logger.debug(f"Cached {len(prs)} PRs for {repo_url}")

            # Clear queue
            total_written = len(self.write_queue)
            self.write_queue.clear()

            if total_written > 0:
                logger.info(f"Flushed {total_written} PRs to cache")

        except Exception as e:
            logger.error(f"Error flushing PR cache queue: {e}")
    
    def _background_writer(self) -> None:
        """Background thread to periodically flush the write queue."""
        import time
        while True:
            try:
                time.sleep(self.flush_interval)  # Configurable flush interval
                with self.write_lock:
                    self._flush_queue()
            except Exception as e:
                logger.error(f"Error in background writer: {e}")
    
    def load_cached_prs(self, repo_url: str) -> List[PullRequestInfo]:
        """Load all cached PRs for a repository."""
        try:
            cache_file = self.get_pr_cache_file(repo_url)
            if not cache_file.exists():
                # Track cache miss
                try:
                    from .performance_monitor import get_performance_monitor
                    get_performance_monitor().increment_cache_misses()
                except ImportError:
                    pass
                return []
            
            prs = []
            with open(cache_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            pr_dict = json.loads(line)
                            pr = PullRequestInfo.from_dict(pr_dict)
                            prs.append(pr)
                        except Exception as e:
                            logger.warning(f"Failed to parse cached PR: {e}")
                            continue
            
            logger.info(f"Loaded {len(prs)} cached PRs for {repo_url}")

            # Track cache hit
            try:
                from .performance_monitor import get_performance_monitor
                get_performance_monitor().increment_cache_hits(len(prs))
            except ImportError:
                pass

            return prs
            
        except Exception as e:
            logger.error(f"Failed to load cached PRs: {e}")
            return []
    
    def get_cached_pr_numbers(self, repo_url: str) -> set:
        """Get set of PR numbers that are already cached."""
        try:
            cache_file = self.get_pr_cache_file(repo_url)
            if not cache_file.exists():
                return set()
            
            pr_numbers = set()
            with open(cache_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            pr_dict = json.loads(line)
                            pr_numbers.add(pr_dict.get('number'))
                        except Exception:
                            continue
            
            return pr_numbers
            
        except Exception as e:
            logger.error(f"Failed to get cached PR numbers: {e}")
            return set()
    
    def clear_pr_cache(self, repo_url: str) -> None:
        """Clear PR cache for a repository."""
        try:
            cache_file = self.get_pr_cache_file(repo_url)
            if cache_file.exists():
                cache_file.unlink()
                logger.info(f"Cleared PR cache for {repo_url}")
        except Exception as e:
            logger.error(f"Failed to clear PR cache: {e}")

    # Checkpoint Methods
    def get_checkpoint_file(self, repo_url: str) -> Path:
        """Get checkpoint file path for a repository."""
        safe_name = self._get_safe_repo_name(repo_url)
        return self.checkpoint_cache_dir / f"{safe_name}_state.json"

    def save_crawl_state(self, state: RepositoryCrawlState) -> None:
        """Save repository crawl state to checkpoint."""
        try:
            checkpoint_file = self.get_checkpoint_file(state.repo_url)
            with open(checkpoint_file, 'w') as f:
                json.dump(state.to_dict(), f, indent=2)
            logger.debug(f"Saved crawl state for {state.repo_url}")
        except Exception as e:
            logger.error(f"Failed to save crawl state: {e}")

    def load_crawl_state(self, repo_url: str) -> Optional[RepositoryCrawlState]:
        """Load repository crawl state from checkpoint."""
        try:
            checkpoint_file = self.get_checkpoint_file(repo_url)
            if not checkpoint_file.exists():
                return None

            with open(checkpoint_file, 'r') as f:
                data = json.load(f)

            state = RepositoryCrawlState.from_dict(data)
            logger.debug(f"Loaded crawl state for {repo_url}: {len(state.discovered_pr_urls)} URLs discovered, {len(state.scraped_pr_numbers)} scraped")
            return state

        except Exception as e:
            logger.error(f"Failed to load crawl state: {e}")
            return None

    def create_initial_crawl_state(self, repo_url: str, total_prs_expected: int) -> RepositoryCrawlState:
        """Create initial repository crawl state."""
        return RepositoryCrawlState(
            repo_url=repo_url,
            total_prs_expected=total_prs_expected,
            discovered_pr_urls=[],
            scraped_pr_numbers=[],
            failed_pr_urls=[],
            last_open_page=0,
            last_closed_page=0,
            open_pages_complete=False,
            closed_pages_complete=False,
            discovery_complete=False,
            scraping_complete=False
        )

    def clear_crawl_state(self, repo_url: str) -> None:
        """Clear crawl state for a repository."""
        try:
            checkpoint_file = self.get_checkpoint_file(repo_url)
            if checkpoint_file.exists():
                checkpoint_file.unlink()
                logger.info(f"Cleared crawl state for {repo_url}")
        except Exception as e:
            logger.error(f"Failed to clear crawl state: {e}")

    # Commit Caching Methods
    def get_commits_cache_file(self, repo_url: str) -> Path:
        """Get commits cache file path for a repository."""
        safe_name = self._get_safe_repo_name(repo_url)
        return self.commits_cache_dir / f"{safe_name}_commits.json"

    def save_repository_commits(self, repo_url: str, commit_ids: List[str]) -> None:
        """Save repository commit IDs to cache."""
        try:
            cache_file = self.get_commits_cache_file(repo_url)
            with open(cache_file, 'w') as f:
                json.dump({"repo_url": repo_url, "commit_ids": commit_ids}, f, indent=2)
            logger.info(f"Saved {len(commit_ids)} commit IDs for {repo_url}")
        except Exception as e:
            logger.error(f"Failed to save repository commits: {e}")

    def load_repository_commits(self, repo_url: str) -> List[str]:
        """Load repository commit IDs from cache."""
        try:
            cache_file = self.get_commits_cache_file(repo_url)
            if not cache_file.exists():
                return []

            with open(cache_file, 'r') as f:
                data = json.load(f)

            commit_ids = data.get('commit_ids', [])
            logger.info(f"Loaded {len(commit_ids)} commit IDs for {repo_url}")
            return commit_ids

        except Exception as e:
            logger.error(f"Failed to load repository commits: {e}")
            return []

    def save_pr_commits(self, repo_url: str, pr_number: int, commit_ids: List[str]) -> None:
        """Save commit IDs for a specific PR."""
        try:
            # Load existing PR commit mapping
            state = self.load_crawl_state(repo_url)
            if state is None:
                logger.warning(f"No crawl state found for {repo_url}, cannot save PR commits")
                return

            # Update PR commit mapping
            state.pr_commit_mapping[pr_number] = commit_ids

            # Save updated state
            self.save_crawl_state(state)
            logger.debug(f"Saved {len(commit_ids)} commit IDs for PR #{pr_number} in {repo_url}")

        except Exception as e:
            logger.error(f"Failed to save PR commits: {e}")

    def get_pr_commits(self, repo_url: str, pr_number: int) -> List[str]:
        """Get commit IDs for a specific PR."""
        try:
            state = self.load_crawl_state(repo_url)
            if state is None:
                return []

            return state.pr_commit_mapping.get(pr_number, [])

        except Exception as e:
            logger.error(f"Failed to get PR commits: {e}")
            return []

    # Utility Methods
    def cleanup_cache(self, repo_url: str) -> None:
        """Clean up all cache files for a repository."""
        try:
            self.clear_pr_cache(repo_url)
            self.clear_crawl_state(repo_url)

            # Clear commits cache
            commits_file = self.get_commits_cache_file(repo_url)
            if commits_file.exists():
                commits_file.unlink()

            logger.info(f"Cleaned up all cache for {repo_url}")
        except Exception as e:
            logger.error(f"Failed to cleanup cache: {e}")

    def get_cache_stats(self, repo_url: str) -> Dict:
        """Get cache statistics for a repository."""
        try:
            stats = {
                "pr_cache_exists": self.get_pr_cache_file(repo_url).exists(),
                "checkpoint_exists": self.get_checkpoint_file(repo_url).exists(),
                "commits_cache_exists": self.get_commits_cache_file(repo_url).exists(),
                "cached_pr_count": len(self.get_cached_pr_numbers(repo_url)),
                "cached_commit_count": len(self.load_repository_commits(repo_url))
            }

            # Get crawl state info
            state = self.load_crawl_state(repo_url)
            if state:
                stats.update({
                    "discovered_prs": len(state.discovered_pr_urls),
                    "scraped_prs": len(state.scraped_pr_numbers),
                    "failed_prs": len(state.failed_pr_urls),
                    "discovery_complete": state.discovery_complete,
                    "scraping_complete": state.scraping_complete
                })

            return stats

        except Exception as e:
            logger.error(f"Failed to get cache stats: {e}")
            return {}
