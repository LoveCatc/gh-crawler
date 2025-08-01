"""Checkpoint manager for tracking crawled repositories and implementing resume functionality."""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set
from loguru import logger

from .config import CHECKPOINT_DIR, CHECKPOINT_DB_FILE, MAX_CHECKPOINT_AGE_DAYS
from .models import CrawledRepository


class CheckpointManager:
    """Manages checkpoint data for crawled repositories to enable resume functionality."""
    
    def __init__(self, checkpoint_dir: str = CHECKPOINT_DIR):
        """Initialize checkpoint manager.
        
        Args:
            checkpoint_dir: Directory to store checkpoint data
        """
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_file = self.checkpoint_dir / CHECKPOINT_DB_FILE
        self._crawled_repos: Dict[str, Dict] = {}
        self._load_checkpoint_data()
    
    def _load_checkpoint_data(self) -> None:
        """Load existing checkpoint data from disk."""
        try:
            if self.checkpoint_file.exists():
                logger.info(f"Loading checkpoint data from {self.checkpoint_file}")
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    self._crawled_repos = json.load(f)
                logger.info(f"Loaded {len(self._crawled_repos)} crawled repositories from checkpoint")
            else:
                logger.info("No existing checkpoint data found, starting fresh")
                self._crawled_repos = {}
        except Exception as e:
            logger.error(f"Error loading checkpoint data: {e}")
            self._crawled_repos = {}
    
    def _save_checkpoint_data(self) -> bool:
        """Save checkpoint data to disk.
        
        Returns:
            True if save was successful, False otherwise
        """
        try:
            # Ensure checkpoint directory exists
            self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
            
            # Write checkpoint data atomically
            temp_file = self.checkpoint_file.with_suffix('.tmp')
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(self._crawled_repos, f, indent=2, ensure_ascii=False)
            
            # Atomic rename
            temp_file.replace(self.checkpoint_file)
            logger.debug(f"Saved checkpoint data with {len(self._crawled_repos)} repositories")
            return True
            
        except Exception as e:
            logger.error(f"Error saving checkpoint data: {e}")
            return False
    
    def is_repository_crawled(self, repo_url: str, max_age_days: int = MAX_CHECKPOINT_AGE_DAYS) -> bool:
        """Check if a repository has been successfully crawled recently.
        
        Args:
            repo_url: Repository URL to check
            max_age_days: Maximum age in days for considering a crawl valid
            
        Returns:
            True if repository was crawled recently and successfully
        """
        if repo_url not in self._crawled_repos:
            return False
        
        repo_data = self._crawled_repos[repo_url]
        
        # Check if crawl was successful
        if not repo_data.get('crawl_success', False):
            return False
        
        # Check age of crawl
        try:
            crawl_time = datetime.fromisoformat(repo_data['crawl_timestamp'])
            age_limit = datetime.now() - timedelta(days=max_age_days)
            
            if crawl_time < age_limit:
                logger.debug(f"Repository {repo_url} crawl is too old ({crawl_time}), will re-crawl")
                return False
                
            return True
            
        except (KeyError, ValueError) as e:
            logger.warning(f"Invalid timestamp for repository {repo_url}: {e}")
            return False
    
    def mark_repository_crawled(self, repository: CrawledRepository) -> None:
        """Mark a repository as successfully crawled.
        
        Args:
            repository: The crawled repository data
        """
        self._crawled_repos[repository.url] = {
            'crawl_timestamp': repository.crawl_timestamp or datetime.now().isoformat(),
            'crawl_success': repository.crawl_success,
            'stars': repository.stars,
            'language': repository.language,
            'error_message': repository.error_message
        }
        
        # Save checkpoint data immediately for persistence
        self._save_checkpoint_data()
    
    def mark_repository_failed(self, repo_url: str, error_message: str) -> None:
        """Mark a repository crawl as failed.
        
        Args:
            repo_url: Repository URL that failed
            error_message: Error message describing the failure
        """
        self._crawled_repos[repo_url] = {
            'crawl_timestamp': datetime.now().isoformat(),
            'crawl_success': False,
            'error_message': error_message
        }
        
        # Save checkpoint data
        self._save_checkpoint_data()
    
    def get_crawled_repositories_from_output_files(self, output_dir: str) -> Set[str]:
        """Get set of repository URLs that exist in any output files.
        
        Args:
            output_dir: Directory containing output JSONL files
            
        Returns:
            Set of repository URLs found in output files
        """
        crawled_urls = set()
        output_path = Path(output_dir)
        
        if not output_path.exists():
            return crawled_urls
        
        # Find all JSONL files in output directory
        jsonl_files = list(output_path.glob("*.jsonl"))
        
        for jsonl_file in jsonl_files:
            try:
                logger.debug(f"Scanning output file: {jsonl_file}")
                with open(jsonl_file, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()
                        if not line:
                            continue
                        
                        try:
                            repo_data = json.loads(line)
                            if 'url' in repo_data:
                                crawled_urls.add(repo_data['url'])
                        except json.JSONDecodeError as e:
                            logger.warning(f"Invalid JSON on line {line_num} in {jsonl_file}: {e}")
                        except Exception as e:
                            logger.warning(f"Error parsing line {line_num} in {jsonl_file}: {e}")
                            
            except Exception as e:
                logger.error(f"Error reading output file {jsonl_file}: {e}")
        
        logger.info(f"Found {len(crawled_urls)} repositories in existing output files")
        return crawled_urls
    
    def filter_uncrawled_repositories(self, repositories: List, output_dir: str, current_output_file: str = None) -> List:
        """Filter out repositories that have already been crawled.

        Priority order for checking if a repository has been crawled:
        1. Current output file (for resume functionality)
        2. Other output files in the directory (for cross-threshold deduplication)
        3. Checkpoint data (only for recently successful crawls, as fallback)

        Args:
            repositories: List of InputRepository objects to filter
            output_dir: Directory containing output files to check
            current_output_file: Current output file path to also check for existing entries

        Returns:
            List of repositories that haven't been crawled yet
        """
        # Get repositories from ALL output files (cross-threshold deduplication)
        output_crawled = self.get_crawled_repositories_from_output_files(output_dir)

        # Also check the current output file if specified
        current_file_crawled = set()
        if current_output_file:
            from .io_handler import OutputHandler
            current_file_crawled = OutputHandler.get_existing_repository_urls(current_output_file)
            logger.info(f"Found {len(current_file_crawled)} repositories in current output file")

        # Combine all crawled repositories from output files
        all_output_crawled = output_crawled.union(current_file_crawled)

        uncrawled = []
        skipped_output = 0
        skipped_checkpoint = 0
        skipped_current = 0

        for repo in repositories:
            # Check if in current output file first (resume functionality)
            if repo.url in current_file_crawled:
                skipped_current += 1
                logger.debug(f"Skipping {repo.url} - found in current output file")
                continue

            # Check if in any output files (cross-threshold deduplication)
            if repo.url in output_crawled:
                skipped_output += 1
                logger.debug(f"Skipping {repo.url} - found in existing output files")
                continue

            # Only check checkpoint data if not found in output files
            # This ensures output files are the primary source of truth
            if repo.url not in all_output_crawled and self.is_repository_crawled(repo.url):
                skipped_checkpoint += 1
                logger.debug(f"Skipping {repo.url} - found in checkpoint data (no output file entry)")
                continue

            uncrawled.append(repo)

        logger.info(f"Repository filtering results:")
        logger.info(f"  - {len(uncrawled)} repositories to crawl")
        logger.info(f"  - {skipped_current} skipped (in current output file)")
        logger.info(f"  - {skipped_output} skipped (in other output files)")
        logger.info(f"  - {skipped_checkpoint} skipped (in checkpoint data only)")

        return uncrawled
    
    def cleanup_old_checkpoints(self, max_age_days: int = MAX_CHECKPOINT_AGE_DAYS * 2) -> None:
        """Remove old checkpoint entries to prevent the checkpoint file from growing too large.
        
        Args:
            max_age_days: Remove entries older than this many days
        """
        if not self._crawled_repos:
            return
        
        age_limit = datetime.now() - timedelta(days=max_age_days)
        old_count = len(self._crawled_repos)
        
        # Filter out old entries
        self._crawled_repos = {
            url: data for url, data in self._crawled_repos.items()
            if self._is_entry_recent(data, age_limit)
        }
        
        new_count = len(self._crawled_repos)
        removed_count = old_count - new_count
        
        if removed_count > 0:
            logger.info(f"Cleaned up {removed_count} old checkpoint entries")
            self._save_checkpoint_data()
    
    def _is_entry_recent(self, entry_data: Dict, age_limit: datetime) -> bool:
        """Check if a checkpoint entry is recent enough to keep.
        
        Args:
            entry_data: Checkpoint entry data
            age_limit: Cutoff datetime for keeping entries
            
        Returns:
            True if entry should be kept, False if it should be removed
        """
        try:
            crawl_time = datetime.fromisoformat(entry_data['crawl_timestamp'])
            return crawl_time >= age_limit
        except (KeyError, ValueError):
            # If we can't parse the timestamp, remove the entry
            return False
    
    def get_statistics(self) -> Dict[str, int]:
        """Get statistics about the checkpoint data.

        Returns:
            Dictionary with checkpoint statistics
        """
        total = len(self._crawled_repos)
        successful = sum(1 for data in self._crawled_repos.values() if data.get('crawl_success', False))
        failed = total - successful

        return {
            'total_repositories': total,
            'successful_crawls': successful,
            'failed_crawls': failed
        }

    def clear_checkpoint_data(self) -> bool:
        """Clear all checkpoint data (useful for testing or fresh starts).

        Returns:
            True if cleared successfully, False otherwise
        """
        try:
            self._crawled_repos = {}
            success = self._save_checkpoint_data()
            if success:
                logger.info("Cleared all checkpoint data")
            return success
        except Exception as e:
            logger.error(f"Error clearing checkpoint data: {e}")
            return False

    def remove_repository_from_checkpoint(self, repo_url: str) -> bool:
        """Remove a specific repository from checkpoint data.

        Args:
            repo_url: Repository URL to remove

        Returns:
            True if removed successfully, False otherwise
        """
        try:
            if repo_url in self._crawled_repos:
                del self._crawled_repos[repo_url]
                success = self._save_checkpoint_data()
                if success:
                    logger.info(f"Removed {repo_url} from checkpoint data")
                return success
            else:
                logger.info(f"Repository {repo_url} not found in checkpoint data")
                return True
        except Exception as e:
            logger.error(f"Error removing repository from checkpoint: {e}")
            return False
