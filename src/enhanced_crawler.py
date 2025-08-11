"""Enhanced crawler with unified caching and minimum PR requirements."""

import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional
from loguru import logger

from .models import InputRepository, CrawledRepository, CrawlResult
from .http_client import HTTPClient
from .repository_scraper import RepositoryScraper
from .aggressive_pr_scraper import AggressivePRScraper
from .commit_scraper import CommitScraper
from .unified_cache_manager import UnifiedCacheManager
from .config import MAX_CLOSED_PRS_TO_CRAWL, CRAWL_CLOSED_PRS, CRAWL_OPEN_PRS, MIN_PRS_REQUIRED, MAX_PRS_FALLBACK
from .config import MAX_WORKERS, DISCOVERY_WORKERS, ENABLE_PROXY_REFRESH, QUIET_MODE, ENABLE_COMMIT_SCRAPING
from .io_handler import OutputHandler
from .failed_issue_cache import FailedIssueCache
from .issue_validator import IssueValidator
from .performance_monitor import get_performance_monitor


class EnhancedGitHubCrawler:
    """Enhanced GitHub crawler with unified caching and minimum PR enforcement."""

    def __init__(self, max_workers: int = MAX_WORKERS, cache_manager: Optional[UnifiedCacheManager] = None):
        self.max_workers = max_workers
        self.cache_manager = cache_manager or UnifiedCacheManager()
        # Initialize shared optimization components (thread-safe, shared)
        self.failed_cache = FailedIssueCache()
        self.validator = IssueValidator()
        # Core scrapers (shared HTTP session for repo-level operations)
        self.http_client = HTTPClient(enable_proxy_refresh=ENABLE_PROXY_REFRESH)
        self.repository_scraper = RepositoryScraper(self.http_client)
        self.commit_scraper = CommitScraper(self.http_client)


    def log_optimization_stats(self):
        """Log statistics about optimization effectiveness."""
        if QUIET_MODE:
            return  # Skip optimization stats in quiet mode

        stats = self.failed_cache.get_stats()
        logger.info(f"🚀 Optimization Statistics:")
        logger.info(f"   • Total failed issues cached: {stats['total_failed_issues']}")
        logger.info(f"   • Repositories with failures: {stats['repositories_with_failures']}")
        logger.info(f"   • Failed repositories cached: {stats['failed_repositories']}")
        logger.info(f"   • Currently blocked repos: {stats['currently_blocked_repos']}")
        logger.info(f"   • Total failure events: {stats['total_failure_counts']}")

        # Show some failed repositories as examples
        failed_repos = self.failed_cache.get_failed_repositories()
        if failed_repos:
            logger.info(f"   • Example failed repos:")
            for repo_url, reason in list(failed_repos.items())[:3]:  # Show first 3
                logger.info(f"     - {repo_url}: {reason}")
            if len(failed_repos) > 3:
                logger.info(f"     - ... and {len(failed_repos) - 3} more")

        if stats['total_failed_issues'] > 0:
            estimated_saved = stats['total_failed_issues'] * 3  # Assuming 3 retries per failed issue
            logger.info(f"   • Estimated requests saved: {estimated_saved}")
            logger.info(f"   • Network efficiency improvement: ~{(estimated_saved / (estimated_saved + stats['total_failed_issues'])) * 100:.1f}%")

    def crawl_repositories(self, repositories: List[InputRepository], star_threshold: int,
                          output_dir: str = "output", current_output_file: str = None) -> int:
        """Crawl multiple repositories with enhanced caching and PR requirements."""

        # Filter repositories by star threshold
        filtered_repos = [repo for repo in repositories if repo.stars >= star_threshold]
        logger.info(f"Found {len(filtered_repos)} repositories above star threshold {star_threshold}")

        # Filter out already crawled repositories using output files as source of truth
        uncrawled_repos = self._filter_uncrawled_repositories(filtered_repos, output_dir, current_output_file)

        if not uncrawled_repos:
            logger.info("No new repositories to crawl")
            return 0

        logger.info(f"Starting crawl for {len(uncrawled_repos)} new repositories")

        successful_count = 0
        performance_monitor = get_performance_monitor()

        # Process repositories with thread pool
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all crawl tasks
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
                            # Write to output file
                            write_success = OutputHandler.append_crawled_repository(result.repository, current_output_file)
                            if write_success:
                                successful_count += 1
                                logger.info(f"✅ Successfully crawled and saved: {repo.url}")
                            else:
                                logger.error(f"❌ Crawled {repo.url} but failed to write to disk")
                        else:
                            # Continue scraping more PRs to meet minimum requirement
                            logger.info(f"🔄 Repository {repo.url} needs more PRs - continuing to scrape...")
                            additional_result = self._continue_scraping_until_requirement_met(repo, result.repository)

                            if additional_result and additional_result.success:
                                # Write the enhanced result to disk
                                write_success = OutputHandler.append_crawled_repository(additional_result.repository, current_output_file)
                                if write_success:
                                    successful_count += 1
                                    performance_monitor.increment_repositories()
                                    performance_monitor.increment_prs(len(additional_result.repository.pull_requests))
                                    logger.info(f"✅ Successfully crawled and saved after additional scraping: {repo.url}")
                                else:
                                    logger.error(f"❌ Crawled {repo.url} but failed to write to disk")
                            else:
                                logger.warning(f"⚠️  Could not meet minimum PR requirement for {repo.url} even after additional scraping")
                    else:
                        logger.error(f"❌ Failed to crawl repository: {repo.url}")
                        if result.error:
                            logger.error(f"Error: {result.error}")

                except Exception as e:
                    logger.error(f"❌ Unexpected error crawling {repo.url}: {e}")

        # Log optimization statistics at the end
        self.log_optimization_stats()

        # Flush cache to ensure all data is written
        self.cache_manager.flush_cache()

        return successful_count

    def _filter_uncrawled_repositories(self, repositories: List[InputRepository],
                                     output_dir: str, current_output_file: str = None) -> List[InputRepository]:
        """Filter out repositories that have already been crawled."""

        # Get all crawled repositories from output files
        crawled_urls = self.cache_manager.get_crawled_repositories_from_output(output_dir)

        # Also check current output file if specified
        if current_output_file:
            current_crawled = OutputHandler.get_existing_repository_urls(current_output_file)
            crawled_urls.update(current_crawled)

        # Filter out already crawled repositories
        uncrawled = []
        for repo in repositories:
            if repo.url not in crawled_urls:
                uncrawled.append(repo)
            else:
                logger.debug(f"Skipping {repo.url} - already crawled")

        logger.info(f"Filtered out {len(repositories) - len(uncrawled)} already crawled repositories")
        return uncrawled

    def _crawl_single_repository(self, repo: InputRepository) -> CrawlResult:
        """Crawl a single repository with enhanced features."""
        try:
            # Check if this repository has a cached failure first
            if self.failed_cache.is_repository_failed(repo.url):
                failure_reason = self.failed_cache.get_repository_failure_reason(repo.url)
                logger.info(f"⏭️  Skipping {repo.url} - cached failure: {failure_reason}")
                return CrawlResult(success=False, error=f"Cached failure: {failure_reason}")

            logger.info(f"🚀 Starting enhanced crawl for: {repo.url}")

            # Step 1: Scrape basic repository statistics
            stats = self.repository_scraper.scrape_repository_stats(repo.url)
            if not stats:
                # Use detailed error message if available
                error_msg = getattr(self.repository_scraper, 'last_error', None) or "Failed to scrape repository stats"
                # Cache this repository-level failure
                self.failed_cache.mark_repository_failed(repo.url, f"Stats scraping failed: {error_msg}")
                return CrawlResult(success=False, error=error_msg)

            logger.info(f"📊 Repository stats: {stats.total_pull_requests:,} total PRs "
                       f"({stats.open_pull_requests:,} open, {stats.closed_pull_requests:,} closed)")

            # Step 2: Determine how many PRs we'll actually crawl
            # The requirement is min(MIN_PRS_REQUIRED, num_all_closed_PRs) - i.e. crawl all closed PRs if fewer than MIN_PRS_REQUIRED
            target_closed_prs = min(stats.closed_pull_requests, MAX_CLOSED_PRS_TO_CRAWL) if CRAWL_CLOSED_PRS else 0
            target_open_prs = stats.open_pull_requests if CRAWL_OPEN_PRS else 0
            total_target_prs = target_closed_prs + target_open_prs

            # If PR counts are unavailable, treat as fetch failure, not as 'no closed PRs'
            if CRAWL_CLOSED_PRS and not getattr(stats, 'pr_counts_available', False):
                error_msg = "Failed to fetch PR counts"
                logger.warning(f"⚠️  Could not fetch PR counts for {repo.url} - skipping (fetch failure)")
                # Cache this repository-level failure
                self.failed_cache.mark_repository_failed(repo.url, error_msg)
                return CrawlResult(success=False, error=error_msg)

            # Only skip if repository has confirmed zero closed PRs (PR counts available)
            if CRAWL_CLOSED_PRS and stats.pr_counts_available and stats.closed_pull_requests == 0:
                error_msg = "No closed PRs available"
                logger.warning(f"⚠️  Repository {repo.url} has no closed PRs - skipping")
                # Cache this repository-level failure
                self.failed_cache.mark_repository_failed(repo.url, error_msg)
                return CrawlResult(success=False, error=error_msg)

            logger.info(f"📋 Target: {target_closed_prs} closed PRs + {target_open_prs} open PRs = {total_target_prs} total PRs")

            # Step 3: Scrape repository commits (optional)
            commit_ids = []
            if ENABLE_COMMIT_SCRAPING:
                logger.info(f"📝 Scraping repository commits for {repo.url}")
                from .config import MAX_COMMITS_TO_SCRAPE
                commit_ids = self.commit_scraper.scrape_repository_commits(repo.url, max_commits=MAX_COMMITS_TO_SCRAPE)
                logger.info(f"Found {len(commit_ids)} repository commits")
            else:
                logger.info(f"⏭️  Skipping repository commit scraping (disabled for faster processing)")

            # Step 4: Scrape PRs with aggressive strategy
            pull_requests = []
            if CRAWL_CLOSED_PRS or CRAWL_OPEN_PRS:
                logger.info(f"🔍 Starting aggressive PR crawling for {repo.url}")

                # Initialize aggressive scraper with unified cache and optimized workers
                aggressive_scraper = AggressivePRScraper(
                    max_workers=self.max_workers,
                    discovery_workers=DISCOVERY_WORKERS,  # Use optimized discovery workers
                    cache_manager=self.cache_manager
                )

                total_prs = stats.total_pull_requests if stats else MAX_PRS_FALLBACK
                pull_requests = aggressive_scraper.scrape_all_prs_aggressively(
                    repo.url,
                    total_prs,
                    max_closed_prs=MAX_CLOSED_PRS_TO_CRAWL if CRAWL_CLOSED_PRS else 0
                )

                # Step 5: Scrape commit IDs for each PR (optional)
                if ENABLE_COMMIT_SCRAPING:
                    logger.info(f"📝 Scraping commit IDs for {len(pull_requests)} PRs")
                    self._scrape_pr_commits(pull_requests, repo.url)

                    # Step 5.5: Populate commit_id and previous_commit_id for each PR
                    logger.info(f"🔗 Populating commit references for {len(pull_requests)} PRs")
                    self._populate_pr_commit_references(pull_requests, commit_ids)
                else:
                    logger.info(f"⏭️  Skipping PR commit scraping (disabled for faster processing)")
                    # Set empty commit IDs for all PRs
                    for pr in pull_requests:
                        pr.commit_ids = []
                        pr.commit_id = ""
                        pr.previous_commit_id = ""

            # Step 6: Verify we got the expected number of PRs
            expected_prs = target_closed_prs + target_open_prs
            if len(pull_requests) < expected_prs * 0.8:  # Allow some tolerance for failed scrapes
                logger.warning(f"⚠️  Only scraped {len(pull_requests)} PRs for {repo.url}, "
                             f"expected around {expected_prs}")
                # Don't fail completely, just log the warning

            # Create crawled repository object
            crawled_repo = CrawledRepository(
                url=repo.url,
                stars=repo.stars,
                language=repo.language,
                stats=stats,
                pull_requests=pull_requests,
                commit_ids=commit_ids,
                crawl_timestamp=datetime.now().isoformat(),
                crawl_success=True
            )

            # Remove from failed repository cache if it was previously failed
            self.failed_cache.remove_repository_failure(repo.url)

            logger.info(f"✅ Successfully crawled {repo.url}: {len(pull_requests)} PRs, {len(commit_ids)} commits")
            return CrawlResult(success=True, repository=crawled_repo)

        except Exception as e:
            logger.error(f"❌ Error crawling repository {repo.url}: {e}")
            return CrawlResult(success=False, error=str(e))

    def _scrape_pr_commits(self, pull_requests: List, repo_url: str) -> None:
        """Scrape commit IDs for all PRs with parallel processing."""
        try:
            total_prs = len(pull_requests)
            logger.info(f"🔄 Processing commit IDs for {total_prs} PRs...")

            # Check cache first and separate cached vs uncached PRs
            cached_count = 0
            uncached_prs = []

            for pr in pull_requests:
                cached_commits = self.cache_manager.get_pr_commits(repo_url, pr.number)
                if cached_commits:
                    pr.commit_ids = cached_commits
                    cached_count += 1
                else:
                    uncached_prs.append(pr)

            if cached_count > 0:
                logger.info(f"📦 Using cached commits for {cached_count} PRs")

            if not uncached_prs:
                logger.info(f"✅ All PR commits loaded from cache")
                return

            logger.info(f"🌐 Scraping commits for {len(uncached_prs)} PRs (parallel processing)...")

            # Process uncached PRs in parallel with progress reporting
            self._scrape_pr_commits_parallel(uncached_prs, repo_url)

        except Exception as e:
            logger.error(f"Error scraping PR commits: {e}")

    def _scrape_pr_commits_parallel(self, uncached_prs: List, repo_url: str) -> None:
        """Scrape PR commits in parallel with progress reporting."""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        completed_count = 0
        total_count = len(uncached_prs)

        # Use a subset of workers for commit scraping to avoid overwhelming
        commit_workers = min(self.max_workers // 2, 16)  # Use half workers, max 16

        with ThreadPoolExecutor(max_workers=commit_workers) as executor:
            # Submit all PR commit scraping tasks
            future_to_pr = {
                executor.submit(self._scrape_single_pr_commits, pr, repo_url): pr
                for pr in uncached_prs
            }

            # Process completed tasks with progress reporting
            for future in as_completed(future_to_pr):
                pr = future_to_pr[future]
                completed_count += 1

                try:
                    commit_ids = future.result()
                    pr.commit_ids = commit_ids

                    # Cache the commits
                    if commit_ids:
                        self.cache_manager.save_pr_commits(repo_url, pr.number, commit_ids)

                    # Progress reporting every 10 PRs or at milestones
                    if completed_count % 10 == 0 or completed_count in [1, 5, total_count]:
                        progress_pct = (completed_count / total_count) * 100
                        logger.info(f"📝 Commit scraping progress: {completed_count}/{total_count} PRs ({progress_pct:.1f}%)")

                except Exception as e:
                    logger.warning(f"Failed to scrape commits for PR #{pr.number}: {e}")
                    pr.commit_ids = []

        logger.info(f"✅ Completed commit scraping for {total_count} PRs")

    def _scrape_single_pr_commits(self, pr, repo_url: str) -> List[str]:
        """Scrape commits for a single PR."""
        try:
            commit_ids = self.commit_scraper.scrape_pr_commits(pr.url)
            return commit_ids
        except Exception as e:
            logger.debug(f"Failed to scrape commits for PR #{pr.number}: {e}")
            return []

    def _populate_pr_commit_references(self, pull_requests: List, repo_commit_ids: List[str]) -> None:
        """Populate commit_id and previous_commit_id for each PR."""
        try:
            for pr in pull_requests:
                try:
                    # Find the primary commit ID for this PR
                    if pr.commit_ids:
                        # Use the first commit ID as the primary one
                        pr.commit_id = pr.commit_ids[0]

                        # Find the previous commit ID in repository history
                        if pr.commit_id in repo_commit_ids:
                            commit_index = repo_commit_ids.index(pr.commit_id)
                            # Get the previous commit (next in the list since commits are in reverse chronological order)
                            if commit_index + 1 < len(repo_commit_ids):
                                pr.previous_commit_id = repo_commit_ids[commit_index + 1]
                            else:
                                pr.previous_commit_id = ""  # This is the oldest commit
                        else:
                            pr.previous_commit_id = ""
                    else:
                        pr.commit_id = ""
                        pr.previous_commit_id = ""

                except Exception as e:
                    logger.warning(f"Failed to populate commit references for PR #{pr.number}: {e}")
                    pr.commit_id = ""
                    pr.previous_commit_id = ""

        except Exception as e:
            logger.error(f"Error populating PR commit references: {e}")

    def _meets_minimum_pr_requirement(self, repository: CrawledRepository) -> bool:
        """Check if repository meets the minimum PR requirement.

        The requirement is to crawl min(MIN_PRS_REQUIRED, num_all_closed_PRs) closed PRs.
        This means we should crawl ALL closed PRs if there are fewer than MIN_PRS_REQUIRED.

        The repository should be dropped if it has fewer PRs than this minimum requirement.
        """

        closed_prs_scraped = len([pr for pr in repository.pull_requests if pr.state in ['closed', 'merged']])

        # The target is min(MIN_PRS_REQUIRED, actual_closed_PRs_in_repo)
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
                aggressive_scraper = AggressivePRScraper(
                    max_workers=self.max_workers,
                    discovery_workers=DISCOVERY_WORKERS,  # Use optimized discovery workers
                    cache_manager=self.cache_manager
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
                    max_closed_prs=min(additional_limit, MAX_CLOSED_PRS_TO_CRAWL if CRAWL_CLOSED_PRS else 0)
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


class EnhancedCrawlerManager:
    """Manager for coordinating enhanced crawling operations."""

    def __init__(self, max_workers: int = MAX_WORKERS, cache_manager: Optional[UnifiedCacheManager] = None):
        self.cache_manager = cache_manager or UnifiedCacheManager()
        self.crawler = EnhancedGitHubCrawler(max_workers, self.cache_manager)

    def process_repositories(self, repositories: List[InputRepository], star_threshold: int,
                           output_dir: str, current_output_file: str) -> int:
        """Process repositories with enhanced crawling."""

        start_time = time.time()

        logger.info(f"🚀 Starting enhanced repository processing")
        logger.info(f"Target: {len(repositories)} repositories with star threshold {star_threshold}")
        logger.info(f"Minimum PR requirement: {MIN_PRS_REQUIRED}")
        logger.info(f"Output file: {current_output_file}")

        try:
            successful_count = self.crawler.crawl_repositories(
                repositories, star_threshold, output_dir, current_output_file
            )

            end_time = time.time()
            duration = end_time - start_time

            logger.info(f"✅ Enhanced processing completed in {duration:.2f} seconds")
            logger.info(f"Successfully processed and saved {successful_count} repositories")

            return successful_count

        except Exception as e:
            logger.error(f"❌ Error during enhanced repository processing: {e}")
            raise
