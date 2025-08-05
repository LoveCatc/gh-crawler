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
from .config import MAX_CLOSED_PRS_TO_CRAWL, CRAWL_CLOSED_PRS, CRAWL_OPEN_PRS, MIN_PRS_REQUIRED
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
                        # Check if repository meets minimum PR requirement
                        if self._meets_minimum_pr_requirement(result.repository):
                            # Write the result to disk
                            write_success = OutputHandler.append_crawled_repository(result.repository, current_output_file)
                            if write_success:
                                successful_count += 1
                                # Mark repository as successfully crawled in checkpoint
                                self.checkpoint_manager.mark_repository_crawled(result.repository)
                                logger.info(f"✅ Successfully crawled and saved: {repo.url}")
                            else:
                                # Mark as failed if we couldn't write to disk
                                self.checkpoint_manager.mark_repository_failed(repo.url, "Failed to write to disk")
                                logger.error(f"Crawled {repo.url} but failed to write to disk")
                        else:
                            # Continue scraping more PRs to meet minimum requirement
                            logger.info(f"🔄 Repository {repo.url} needs more PRs - continuing to scrape...")
                            additional_result = self._continue_scraping_until_requirement_met(repo, result.repository)

                            if additional_result and additional_result.success:
                                # Write the enhanced result to disk
                                write_success = OutputHandler.append_crawled_repository(additional_result.repository, current_output_file)
                                if write_success:
                                    successful_count += 1
                                    self.checkpoint_manager.mark_repository_crawled(additional_result.repository)
                                    logger.info(f"✅ Successfully crawled and saved after additional scraping: {repo.url}")
                                else:
                                    self.checkpoint_manager.mark_repository_failed(repo.url, "Failed to write to disk")
                                    logger.error(f"Crawled {repo.url} but failed to write to disk")
                            else:
                                logger.warning(f"⚠️  Could not meet minimum PR requirement for {repo.url} even after additional scraping")
                                # Don't mark as failed in checkpoint since we want to retry later
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
                
                # Check if PR crawling is enabled
                if not CRAWL_CLOSED_PRS and not CRAWL_OPEN_PRS:
                    logger.info(f"⚠️  PR crawling disabled for {repo.url} - skipping PR collection")
                    pull_requests = []
                else:
                    # Use AGGRESSIVE PR scraping with focus on latest closed PRs
                    total_prs = stats.total_pull_requests if stats else 1000
                    open_prs = stats.open_pull_requests if stats else 50
                    closed_prs = total_prs - open_prs if stats else 950
                    expected_to_crawl = min(closed_prs, MAX_CLOSED_PRS_TO_CRAWL) if CRAWL_CLOSED_PRS else 0

                    logger.info(f"🚀 Starting AGGRESSIVE PR crawling for {repo.url}")
                    logger.info(f"Repository has: {total_prs:,} total PRs ({open_prs:,} open, {closed_prs:,} closed)")

                    # Log strategy based on configuration
                    strategy_parts = []
                    if CRAWL_OPEN_PRS:
                        strategy_parts.append("ALL open PRs")
                    if CRAWL_CLOSED_PRS:
                        strategy_parts.append(f"latest {expected_to_crawl:,} closed PRs")

                    if strategy_parts:
                        strategy = " + ".join(strategy_parts)
                        logger.info(f"Strategy: {strategy}")
                    else:
                        logger.info("Strategy: No PRs will be crawled (both open and closed disabled)")

                    # Initialize aggressive scraper
                    aggressive_scraper = AggressivePRScraper(max_workers=20, discovery_workers=10)
                    pull_requests = aggressive_scraper.scrape_all_prs_aggressively(
                        repo.url,
                        expected_to_crawl,  # Expected closed PRs to crawl
                        max_closed_prs=MAX_CLOSED_PRS_TO_CRAWL if CRAWL_CLOSED_PRS else 0
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

    def _meets_minimum_pr_requirement(self, repository: CrawledRepository) -> bool:
        """Check if repository meets the minimum PR requirement.

        The requirement is to crawl min(1000, num_all_closed_PRs) closed PRs.
        This means we should crawl ALL closed PRs if there are fewer than 1000.

        The repository should be dropped if it has fewer PRs than this minimum requirement.
        """

        closed_prs_scraped = len([pr for pr in repository.pull_requests if pr.state in ['closed', 'merged']])

        # The target is min(1000, actual_closed_PRs_in_repo)
        target_closed_prs = min(MIN_PRS_REQUIRED, repository.stats.closed_pull_requests)

        # We should have scraped at least 90% of the target (allowing for some failed scrapes)
        # This is more strict than before to ensure quality
        required_threshold = target_closed_prs * 0.9
        meets_requirement = closed_prs_scraped >= required_threshold

        if not meets_requirement:
            logger.info(f"📊 Repository {repository.url} needs more PRs:")
            logger.info(f"   Scraped: {closed_prs_scraped} closed PRs")
            logger.info(f"   Required: {required_threshold:.1f} closed PRs (90% of {target_closed_prs})")
            logger.info(f"   Target: min({MIN_PRS_REQUIRED}, {repository.stats.closed_pull_requests} available)")
            logger.info(f"   Will continue scraping to meet requirement...")
        else:
            logger.info(f"✅ Repository {repository.url} meets requirement: "
                       f"{closed_prs_scraped} closed PRs scraped (target: {target_closed_prs})")

        return meets_requirement

    def _continue_scraping_until_requirement_met(self, repo: InputRepository,
                                               current_repository: CrawledRepository) -> Optional[CrawlResult]:
        """Continue scraping more PRs until minimum requirement is met with persistent retrying.

        Args:
            repo: Input repository information
            current_repository: Currently scraped repository data

        Returns:
            Enhanced CrawlResult with more PRs, or None if failed
        """
        try:
            target_closed_prs = min(MIN_PRS_REQUIRED, current_repository.stats.closed_pull_requests)
            required_threshold = target_closed_prs * 0.9

            # Initialize for retry loop
            working_repository = current_repository
            max_retry_attempts = 5  # Safety limit to prevent infinite loops
            retry_attempt = 0

            logger.info(f"🔄 Starting persistent scraping for {repo.url}:")
            logger.info(f"   Target: {target_closed_prs} closed PRs")
            logger.info(f"   Required: {required_threshold:.1f} closed PRs")
            logger.info(f"   Max retry attempts: {max_retry_attempts}")

            # Persistent retry loop
            while retry_attempt < max_retry_attempts:
                retry_attempt += 1

                # Check current status
                closed_prs_scraped = len([pr for pr in working_repository.pull_requests if pr.state in ['closed', 'merged']])
                prs_needed = max(0, int(required_threshold - closed_prs_scraped))

                logger.info(f"🔄 Retry attempt {retry_attempt}/{max_retry_attempts}:")
                logger.info(f"   Current: {closed_prs_scraped} closed PRs")
                logger.info(f"   Need: {prs_needed} more closed PRs")

                # Check if we already meet the requirement
                if self._meets_minimum_pr_requirement(working_repository):
                    logger.info(f"✅ Repository {repo.url} meets requirement after {retry_attempt-1} additional attempts!")
                    return CrawlResult(success=True, repository=working_repository)

                # If we need 0 more PRs but still don't meet requirement, something is wrong
                if prs_needed == 0:
                    logger.warning(f"⚠️  Repository {repo.url} calculation error - need 0 PRs but doesn't meet requirement")
                    break

                # Use aggressive scraper to get more PRs
                from .aggressive_pr_scraper import AggressivePRScraper
                from .http_client import HTTPClient
                aggressive_scraper = AggressivePRScraper(
                    max_workers=self.max_workers,
                    discovery_workers=10,
                    cache_manager=self.checkpoint_manager
                )

                # Calculate scraping limit for this attempt
                # Start with what we need, add buffer, and increase with each retry
                base_limit = max(prs_needed * 2, 100)  # At least 100 PRs per attempt
                attempt_multiplier = 1 + (retry_attempt * 0.5)  # Increase limit with each attempt
                additional_limit = int(base_limit * attempt_multiplier)

                logger.info(f"🚀 Attempt {retry_attempt}: Scraping up to {additional_limit} additional PRs")

                # Get additional PRs
                additional_prs = aggressive_scraper.scrape_all_prs_aggressively(
                    repo.url,
                    working_repository.stats.total_pull_requests,
                    max_closed_prs=additional_limit
                )

                # Merge with existing PRs (avoid duplicates)
                existing_pr_numbers = {pr.number for pr in working_repository.pull_requests}
                new_prs = [pr for pr in additional_prs if pr.number not in existing_pr_numbers]

                logger.info(f"📈 Attempt {retry_attempt}: Found {len(new_prs)} new PRs (total scraped: {len(additional_prs)}, duplicates: {len(additional_prs) - len(new_prs)})")

                # If no new PRs found, we've likely exhausted the repository
                if len(new_prs) == 0:
                    logger.warning(f"⚠️  No new PRs found in attempt {retry_attempt} - repository may be exhausted")
                    if retry_attempt >= 2:  # Give it at least 2 attempts before giving up
                        logger.warning(f"🛑 Stopping after {retry_attempt} attempts - no more PRs available")
                        break

                # Create enhanced repository for next iteration
                enhanced_prs = working_repository.pull_requests + new_prs
                working_repository = CrawledRepository(
                    url=working_repository.url,
                    stars=working_repository.stars,
                    language=working_repository.language,
                    stats=working_repository.stats,
                    pull_requests=enhanced_prs,
                    commit_ids=working_repository.commit_ids,
                    crawl_timestamp=working_repository.crawl_timestamp,
                    crawl_success=True
                )

                # Log progress
                new_closed_count = len([pr for pr in working_repository.pull_requests if pr.state in ['closed', 'merged']])
                logger.info(f"📊 After attempt {retry_attempt}: {new_closed_count} total closed PRs")

            # Final check after all attempts
            if self._meets_minimum_pr_requirement(working_repository):
                logger.info(f"✅ Repository {repo.url} meets requirement after {retry_attempt} attempts!")
                return CrawlResult(success=True, repository=working_repository)
            else:
                final_closed = len([pr for pr in working_repository.pull_requests if pr.state in ['closed', 'merged']])
                logger.warning(f"⚠️  Repository {repo.url} still doesn't meet requirement after {retry_attempt} attempts")
                logger.warning(f"   Final count: {final_closed} closed PRs (needed: {required_threshold:.1f})")
                logger.warning(f"   Returning best effort result with {len(working_repository.pull_requests)} total PRs")
                # Return the best effort - it's still better than the original
                return CrawlResult(success=True, repository=working_repository)

        except Exception as e:
            logger.error(f"Error in persistent scraping for {repo.url}: {e}")
            return None


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
