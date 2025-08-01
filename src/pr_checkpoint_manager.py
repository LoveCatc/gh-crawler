"""PR checkpoint manager for resumable PR crawling."""

import json
import os
from pathlib import Path
from typing import Dict, List, Set, Optional
from dataclasses import dataclass, asdict
from loguru import logger


@dataclass
class PRCrawlState:
    """State of PR crawling for a repository."""
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
    # Track counts for open/closed PRs separately
    open_prs_found: int = 0
    closed_prs_found: int = 0
    
    def to_dict(self) -> Dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'PRCrawlState':
        # Add backward compatibility for new fields
        if 'open_prs_found' not in data:
            data['open_prs_found'] = 0
        if 'closed_prs_found' not in data:
            data['closed_prs_found'] = 0
        return cls(**data)


class PRCheckpointManager:
    """Manages checkpoints for resumable PR crawling."""
    
    def __init__(self, checkpoint_dir: str = "pr_checkpoints"):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(exist_ok=True)
        
    def get_checkpoint_file(self, repo_url: str) -> Path:
        """Get checkpoint file path for a repository."""
        # Convert URL to safe filename
        safe_name = repo_url.replace("https://", "").replace("/", "_")
        return self.checkpoint_dir / f"{safe_name}_pr_state.json"
    
    def save_state(self, state: PRCrawlState) -> None:
        """Save PR crawl state to checkpoint."""
        try:
            checkpoint_file = self.get_checkpoint_file(state.repo_url)
            with open(checkpoint_file, 'w') as f:
                json.dump(state.to_dict(), f, indent=2)
            logger.debug(f"Saved PR checkpoint for {state.repo_url}")
        except Exception as e:
            logger.error(f"Failed to save PR checkpoint: {e}")
    
    def load_state(self, repo_url: str) -> Optional[PRCrawlState]:
        """Load PR crawl state from checkpoint."""
        try:
            checkpoint_file = self.get_checkpoint_file(repo_url)
            if not checkpoint_file.exists():
                return None
                
            with open(checkpoint_file, 'r') as f:
                data = json.load(f)
            
            state = PRCrawlState.from_dict(data)
            logger.info(f"Loaded PR checkpoint for {repo_url}: {len(state.discovered_pr_urls)} URLs discovered, {len(state.scraped_pr_numbers)} scraped")
            return state
            
        except Exception as e:
            logger.error(f"Failed to load PR checkpoint: {e}")
            return None
    
    def create_initial_state(self, repo_url: str, total_prs_expected: int) -> PRCrawlState:
        """Create initial PR crawl state."""
        return PRCrawlState(
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
    
    def update_discovery_progress(self, state: PRCrawlState, 
                                pr_type: str, page: int, 
                                new_urls: List[str], 
                                pages_complete: bool) -> None:
        """Update discovery progress in state."""
        # Add new URLs (avoid duplicates) and count them
        existing_urls = set(state.discovered_pr_urls)
        new_count = 0
        for url in new_urls:
            if url not in existing_urls:
                state.discovered_pr_urls.append(url)
                existing_urls.add(url)
                new_count += 1

        # Update page progress and counts
        if pr_type == "open":
            state.last_open_page = page
            state.open_pages_complete = pages_complete
            state.open_prs_found += new_count
        else:
            state.last_closed_page = page
            state.closed_pages_complete = pages_complete
            state.closed_prs_found += new_count
        
        # Check if discovery is complete
        state.discovery_complete = state.open_pages_complete and state.closed_pages_complete
        
        # Save progress
        self.save_state(state)
        
        logger.info(f"Discovery progress - {pr_type} page {page}: {len(new_urls)} new URLs, "
                   f"total: {len(state.discovered_pr_urls)}, complete: {pages_complete}")
    
    def update_scraping_progress(self, state: PRCrawlState, 
                               pr_number: int, success: bool, 
                               pr_url: str = None) -> None:
        """Update scraping progress in state."""
        if success:
            if pr_number not in state.scraped_pr_numbers:
                state.scraped_pr_numbers.append(pr_number)
        else:
            if pr_url and pr_url not in state.failed_pr_urls:
                state.failed_pr_urls.append(pr_url)
        
        # Check if scraping is complete
        total_discovered = len(state.discovered_pr_urls)
        total_processed = len(state.scraped_pr_numbers) + len(state.failed_pr_urls)
        state.scraping_complete = (total_discovered > 0 and total_processed >= total_discovered)
        
        # Save progress every 10 PRs
        if len(state.scraped_pr_numbers) % 10 == 0:
            self.save_state(state)
        
        if len(state.scraped_pr_numbers) % 50 == 0:
            logger.info(f"Scraping progress: {len(state.scraped_pr_numbers)} scraped, "
                       f"{len(state.failed_pr_urls)} failed, "
                       f"{total_discovered - total_processed} remaining")
    
    def get_remaining_urls(self, state: PRCrawlState) -> List[str]:
        """Get list of PR URLs that still need to be scraped."""
        scraped_numbers = set(state.scraped_pr_numbers)
        failed_urls = set(state.failed_pr_urls)
        
        remaining = []
        for url in state.discovered_pr_urls:
            if url in failed_urls:
                continue
                
            # Extract PR number from URL
            try:
                pr_number = int(url.split('/pull/')[-1])
                if pr_number not in scraped_numbers:
                    remaining.append(url)
            except (ValueError, IndexError):
                # If we can't extract number, include it to be safe
                remaining.append(url)
        
        return remaining
    
    def get_progress_summary(self, state: PRCrawlState) -> Dict:
        """Get a summary of crawling progress."""
        remaining_urls = self.get_remaining_urls(state)
        
        return {
            "repo_url": state.repo_url,
            "total_expected": state.total_prs_expected,
            "discovered": len(state.discovered_pr_urls),
            "scraped": len(state.scraped_pr_numbers),
            "failed": len(state.failed_pr_urls),
            "remaining": len(remaining_urls),
            "discovery_complete": state.discovery_complete,
            "scraping_complete": state.scraping_complete,
            "open_pages_complete": state.open_pages_complete,
            "closed_pages_complete": state.closed_pages_complete,
            "last_open_page": state.last_open_page,
            "last_closed_page": state.last_closed_page,
            "coverage_percent": (len(state.scraped_pr_numbers) / max(state.total_prs_expected, 1)) * 100
        }
    
    def cleanup_checkpoint(self, repo_url: str) -> None:
        """Remove checkpoint file when crawling is complete."""
        try:
            checkpoint_file = self.get_checkpoint_file(repo_url)
            if checkpoint_file.exists():
                checkpoint_file.unlink()
                logger.info(f"Cleaned up PR checkpoint for {repo_url}")
        except Exception as e:
            logger.error(f"Failed to cleanup PR checkpoint: {e}")
    
    def list_active_checkpoints(self) -> List[Dict]:
        """List all active PR checkpoints."""
        checkpoints = []
        try:
            for checkpoint_file in self.checkpoint_dir.glob("*_pr_state.json"):
                with open(checkpoint_file, 'r') as f:
                    data = json.load(f)
                state = PRCrawlState.from_dict(data)
                summary = self.get_progress_summary(state)
                checkpoints.append(summary)
        except Exception as e:
            logger.error(f"Failed to list checkpoints: {e}")
        
        return checkpoints
