"""Aggressive PR caching system for immediate persistence."""

import json
import os
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import asdict
from loguru import logger
import threading
import time

from .models import PullRequestInfo


class PRCacheManager:
    """Aggressive caching system that immediately saves scraped PRs."""
    
    def __init__(self, cache_dir: str = "pr_cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.write_lock = threading.Lock()
        self.write_queue = []
        self.batch_size = 10
        self.flush_interval = 30  # seconds
        
        # Start background writer thread
        self.writer_thread = threading.Thread(target=self._background_writer, daemon=True)
        self.writer_thread.start()
        
    def get_cache_file(self, repo_url: str) -> Path:
        """Get cache file path for a repository."""
        safe_name = repo_url.replace("https://", "").replace("/", "_")
        return self.cache_dir / f"{safe_name}_prs.jsonl"
    
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
                cache_file = self.get_cache_file(repo_url)
                
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
            logger.error(f"Failed to flush PR cache: {e}")
    
    def _background_writer(self) -> None:
        """Background thread that periodically flushes the cache."""
        while True:
            try:
                time.sleep(self.flush_interval)
                with self.write_lock:
                    if self.write_queue:
                        self._flush_queue()
            except Exception as e:
                logger.error(f"Background writer error: {e}")
    
    def force_flush(self) -> None:
        """Force flush all pending writes."""
        with self.write_lock:
            self._flush_queue()
    
    def load_cached_prs(self, repo_url: str) -> List[PullRequestInfo]:
        """Load all cached PRs for a repository."""
        try:
            cache_file = self.get_cache_file(repo_url)
            if not cache_file.exists():
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
            return prs
            
        except Exception as e:
            logger.error(f"Failed to load cached PRs: {e}")
            return []
    
    def get_cached_pr_numbers(self, repo_url: str) -> set:
        """Get set of PR numbers that are already cached."""
        try:
            cache_file = self.get_cache_file(repo_url)
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
    
    def cleanup_cache(self, repo_url: str) -> None:
        """Remove cache file when no longer needed."""
        try:
            cache_file = self.get_cache_file(repo_url)
            if cache_file.exists():
                cache_file.unlink()
                logger.info(f"Cleaned up PR cache for {repo_url}")
        except Exception as e:
            logger.error(f"Failed to cleanup PR cache: {e}")
    
    def get_cache_stats(self, repo_url: str) -> Dict:
        """Get statistics about cached PRs."""
        try:
            cache_file = self.get_cache_file(repo_url)
            if not cache_file.exists():
                return {"cached_count": 0, "file_size": 0}
            
            # Count lines and get file size
            line_count = 0
            with open(cache_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        line_count += 1
            
            file_size = cache_file.stat().st_size
            
            return {
                "cached_count": line_count,
                "file_size": file_size,
                "file_size_mb": file_size / (1024 * 1024)
            }
            
        except Exception as e:
            logger.error(f"Failed to get cache stats: {e}")
            return {"cached_count": 0, "file_size": 0}
