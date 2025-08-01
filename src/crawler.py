"""Main crawler with concurrent processing."""

import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional
from loguru import logger

from .models import InputRepository, CrawledRepository, CrawlResult
from .http_client import HTTPClient
from .repository_scraper import RepositoryScraper
from .aggressive_pr_scraper import AggressivePRScraper
from .config import MAX_CLOSED_PRS_TO_CRAWL
from .config import MAX_WORKERS
from .checkpoint_manager import CheckpointManager
from .io_handler import OutputHandler


class GitHubCrawler:
    """Main crawler with concurrent processing capabilities."""

    def __init__(self, max_workers: int = MAX_WORKERS, checkpoint_manager: Optional[CheckpointManager] = None):
        self.max_workers = max_workers
        self.checkpoint_manager = checkpoint_manager or CheckpointManager()
    
    def crawl_repositories(self, repositories: List[InputRepository], star_threshold: int, output_dir: str = "output", current_output_file: str = None) -> int:
        """Crawl multiple repositories concurrently, writing results immediately to disk.

        Returns:
            Number of repositories successfully crawled
        """
        # Filter repositories by star threshold
        filtered_repos = [repo for repo in repositories if repo.stars >= star_threshold]

        logger.info(f"Found {len(filtered_repos)} repositories above star threshold {star_threshold} (from {len(repositories)} total)")

        # Filter out already crawled repositories using checkpoint manager
        uncrawled_repos = self.checkpoint_manager.filter_uncrawled_repositories(filtered_repos, output_dir, current_output_file)

        if len(uncrawled_repos) < len(filtered_repos):
            skipped_count = len(filtered_repos) - len(uncrawled_repos)
            logger.info(f"Skipping {skipped_count} already crawled repositories")

        if not uncrawled_repos:
            logger.info("No new repositories to crawl")
            return 0

        logger.info(f"Crawling {len(uncrawled_repos)} new repositories")

        if not current_output_file:
            logger.error("No output file specified for immediate writing")
            return 0

        successful_count = 0
        
        # Use ThreadPoolExecutor for concurrent crawling
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all crawling tasks
            future_to_repo = {
                executor.submit(self._crawl_single_repository, repo): repo
                for repo in uncrawled_repos
            }

            # Process completed tasks
            for future in as_completed(future_to_repo):
                repo = future_to_repo[future]
                try:
                    result = future.result()
                    if result.success and result.repository:
                        # Immediately write the result to disk
                        write_success = OutputHandler.append_crawled_repository(result.repository, current_output_file)
                        if write_success:
                            successful_count += 1
                            # Mark repository as successfully crawled in checkpoint
                            self.checkpoint_manager.mark_repository_crawled(result.repository)
                            logger.info(f"Successfully crawled and saved: {repo.url}")
                        else:
                            # Mark as failed if we couldn't write to disk
                            self.checkpoint_manager.mark_repository_failed(repo.url, "Failed to write to disk")
                            logger.error(f"Crawled {repo.url} but failed to write to disk")
                    else:
                        # Mark repository as failed in checkpoint
                        self.checkpoint_manager.mark_repository_failed(repo.url, result.error or "Unknown error")
                        logger.error(f"Failed to crawl {repo.url}: {result.error}")
                except Exception as e:
                    # Mark repository as failed in checkpoint
                    self.checkpoint_manager.mark_repository_failed(repo.url, str(e))
                    logger.error(f"Exception crawling {repo.url}: {e}")

        logger.info(f"Completed crawling. Successfully processed and saved {successful_count} repositories")
        return successful_count
    
    def _crawl_single_repository(self, repo: InputRepository) -> CrawlResult:
        """Crawl a single repository."""
        try:
            logger.debug(f"Starting crawl for: {repo.url}")
            
            with HTTPClient() as client:
                # Initialize scrapers
                repo_scraper = RepositoryScraper(client)
                
                # Scrape repository statistics
                stats = repo_scraper.scrape_repository_stats(repo.url)
                if not stats:
                    return CrawlResult(
                        success=False,
                        error="Failed to scrape repository statistics"
                    )
                
                # Use AGGRESSIVE PR scraping with focus on latest closed PRs
                expected_prs = stats.total_pull_requests if stats else 1000
                open_prs = stats.open_pull_requests if stats else 50

                logger.info(f"🚀 Starting AGGRESSIVE PR crawling for {repo.url}")
                logger.info(f"Strategy: ALL open PRs ({open_prs:,}) + latest {MAX_CLOSED_PRS_TO_CRAWL:,} closed PRs")

                # Initialize aggressive scraper
                aggressive_scraper = AggressivePRScraper(max_workers=20, discovery_workers=10)
                pull_requests = aggressive_scraper.scrape_all_prs_aggressively(
                    repo.url,
                    expected_prs,
                    max_closed_prs=MAX_CLOSED_PRS_TO_CRAWL
                )
                
                # Create crawled repository object
                crawled_repo = CrawledRepository(
                    url=repo.url,
                    stars=repo.stars,
                    language=repo.language,
                    stats=stats,
                    pull_requests=pull_requests,
                    crawl_timestamp=datetime.now().isoformat(),
                    crawl_success=True
                )
                
                return CrawlResult(
                    success=True,
                    repository=crawled_repo
                )
                
        except Exception as e:
            logger.error(f"Error crawling repository {repo.url}: {e}")
            return CrawlResult(
                success=False,
                error=str(e)
            )


class CrawlerManager:
    """Manager for coordinating crawling operations."""

    def __init__(self, max_workers: int = MAX_WORKERS, checkpoint_manager: Optional[CheckpointManager] = None):
        self.checkpoint_manager = checkpoint_manager or CheckpointManager()
        self.crawler = GitHubCrawler(max_workers, self.checkpoint_manager)
    
    def process_repositories(self, repositories: List[InputRepository], star_threshold: int, output_dir: str = "output", current_output_file: str = None) -> int:
        """Process repositories with error handling and logging.

        Returns:
            Number of repositories successfully crawled and saved
        """
        start_time = time.time()

        logger.info(f"Starting repository processing with {len(repositories)} repositories")
        logger.info(f"Star threshold: {star_threshold}")
        logger.info(f"Max workers: {self.crawler.max_workers}")

        if current_output_file:
            logger.info(f"Results will be written immediately to: {current_output_file}")

        # Show checkpoint statistics
        stats = self.checkpoint_manager.get_statistics()
        logger.info(f"Checkpoint stats: {stats['total_repositories']} total, "
                   f"{stats['successful_crawls']} successful, {stats['failed_crawls']} failed")

        # Clean up old checkpoint entries
        self.checkpoint_manager.cleanup_old_checkpoints()

        try:
            successful_count = self.crawler.crawl_repositories(repositories, star_threshold, output_dir, current_output_file)

            end_time = time.time()
            duration = end_time - start_time

            logger.info(f"Processing completed in {duration:.2f} seconds")
            logger.info(f"Successfully processed and saved {successful_count} new repositories")

            return successful_count

        except Exception as e:
            logger.error(f"Error during repository processing: {e}")
            raise
