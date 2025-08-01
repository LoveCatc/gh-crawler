"""Aggressive parallel PR scraper with dynamic proxy support."""

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Set
from loguru import logger
import threading
import time

from .http_client import HTTPClient
from .models import PullRequestInfo
from .pr_scraper import PullRequestScraper
from .pr_checkpoint_manager import PRCheckpointManager, PRCrawlState
from .pr_cache_manager import PRCacheManager
from .config import MAX_CLOSED_PRS_TO_CRAWL, CRAWL_OPEN_PRS, CRAWL_CLOSED_PRS, REPOSITORY_PR_LIMITS


class AggressivePRScraper:
    """Aggressive parallel PR scraper optimized for dynamic proxy systems."""
    
    def __init__(self, max_workers: int = 20, discovery_workers: int = 10):
        self.max_workers = max_workers
        self.discovery_workers = discovery_workers
        self.checkpoint_manager = PRCheckpointManager()
        self.cache_manager = PRCacheManager()

        # Create multiple HTTP clients for parallel processing
        self.http_clients = [HTTPClient() for _ in range(max_workers)]
        self.pr_scrapers = [PullRequestScraper(client) for client in self.http_clients]

        # Thread-safe counters
        self.stats_lock = threading.Lock()
        self.discovered_count = 0
        self.scraped_count = 0
        self.failed_count = 0

    def get_pr_limit_for_repo(self, repo_url: str, default_limit: int = None) -> int:
        """Get the PR limit for a specific repository."""
        # Check per-repository configuration first
        if repo_url in REPOSITORY_PR_LIMITS:
            return REPOSITORY_PR_LIMITS[repo_url]

        # Use provided default or global default
        return default_limit if default_limit is not None else MAX_CLOSED_PRS_TO_CRAWL
        
    def scrape_all_prs_aggressively(self, repo_url: str, total_prs_expected: int,
                                   max_closed_prs: int = None) -> List[PullRequestInfo]:
        """Aggressively scrape PRs with focus on latest closed PRs and all open PRs."""
        try:
            # Get the actual limit for this repository
            actual_limit = self.get_pr_limit_for_repo(repo_url, max_closed_prs)

            logger.info(f"🚀 Starting AGGRESSIVE PR crawl for: {repo_url}")
            logger.info(f"Expected total PRs: {total_prs_expected:,}")
            logger.info(f"Max workers: {self.max_workers}, Discovery workers: {self.discovery_workers}")
            logger.info(f"Strategy: Latest {actual_limit:,} closed PRs + all open PRs")

            # Load or create checkpoint state
            state = self.checkpoint_manager.load_state(repo_url)
            if state is None:
                # Adjust expected total based on our crawling strategy
                expected_to_crawl = actual_limit + 100  # Estimate for open PRs
                state = self.checkpoint_manager.create_initial_state(repo_url, expected_to_crawl)
                logger.info("Starting fresh aggressive PR crawl")
            else:
                progress = self.checkpoint_manager.get_progress_summary(state)
                logger.info(f"Resuming aggressive PR crawl:")
                logger.info(f"  - Discovered: {progress['discovered']:,} URLs")
                logger.info(f"  - Scraped: {progress['scraped']:,} PRs")
                logger.info(f"  - Coverage: {progress['coverage_percent']:.1f}%")

            # Get already cached PRs to avoid re-scraping
            cached_pr_numbers = self.cache_manager.get_cached_pr_numbers(repo_url)
            logger.info(f"Found {len(cached_pr_numbers)} already cached PRs")

            # Phase 1: Aggressive URL Discovery (focused strategy)
            if not state.discovery_complete:
                logger.info("🔍 Phase 1: AGGRESSIVE URL Discovery (focused on latest closed PRs)...")
                self._discover_urls_aggressively(state, actual_limit)
            else:
                logger.info("✅ Phase 1: URL discovery already complete")

            # Phase 2: Aggressive PR Scraping
            if not state.scraping_complete:
                logger.info("⚡ Phase 2: AGGRESSIVE PR Scraping...")
                self._scrape_prs_aggressively(state, cached_pr_numbers)
            else:
                logger.info("✅ Phase 2: PR scraping already complete")

            # Force flush cache
            self.cache_manager.force_flush()

            # Load all results
            all_prs = self.cache_manager.load_cached_prs(repo_url)
            
            # Final stats
            final_progress = self.checkpoint_manager.get_progress_summary(state)
            cache_stats = self.cache_manager.get_cache_stats(repo_url)
            
            logger.info(f"🎉 AGGRESSIVE PR crawl completed for {repo_url}:")
            logger.info(f"  - Total discovered: {final_progress['discovered']:,}")
            logger.info(f"  - Successfully scraped: {len(all_prs):,}")
            logger.info(f"  - Cache size: {cache_stats['file_size_mb']:.1f} MB")
            logger.info(f"  - Coverage: {(len(all_prs) / max(total_prs_expected, 1)) * 100:.1f}%")

            return all_prs

        except Exception as e:
            logger.error(f"Error in aggressive PR scraping: {e}")
            return []

    def _discover_urls_aggressively(self, state: PRCrawlState, max_closed_prs: int = MAX_CLOSED_PRS_TO_CRAWL) -> None:
        """Discover PR URLs with focus on latest closed PRs and all open PRs."""
        try:
            # Discover open and closed PRs in parallel
            with ThreadPoolExecutor(max_workers=self.discovery_workers) as executor:
                futures = []

                # Submit discovery tasks based on configuration
                if CRAWL_OPEN_PRS and not state.open_pages_complete:
                    logger.info("🔍 Discovering ALL open PRs...")
                    future = executor.submit(self._discover_pr_urls_parallel, state, "open", None)
                    futures.append(future)

                if CRAWL_CLOSED_PRS and not state.closed_pages_complete:
                    logger.info(f"🔍 Discovering latest {max_closed_prs:,} closed PRs...")
                    future = executor.submit(self._discover_pr_urls_parallel, state, "closed", max_closed_prs)
                    futures.append(future)

                # Wait for completion
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Discovery task failed: {e}")

            # Mark discovery as complete
            state.discovery_complete = (
                (not CRAWL_OPEN_PRS or state.open_pages_complete) and
                (not CRAWL_CLOSED_PRS or state.closed_pages_complete)
            )
            self.checkpoint_manager.save_state(state)

            logger.info(f"🎯 URL discovery complete: {len(state.discovered_pr_urls):,} URLs found")

        except Exception as e:
            logger.error(f"Error in aggressive URL discovery: {e}")
            raise

    def _discover_pr_urls_parallel(self, state: PRCrawlState, pr_state: str, limit: int = None) -> None:
        """Discover PR URLs for a specific state with optional limit (for closed PRs)."""
        try:
            start_page = state.last_open_page + 1 if pr_state == "open" else state.last_closed_page + 1
            max_pages = 2000  # Increased for infinite persistence

            if limit:
                logger.info(f"🔍 Starting {pr_state} PR discovery from page {start_page} (limit: {limit:,}) with exponential backoff")
            else:
                logger.info(f"🔍 Starting {pr_state} PR discovery from page {start_page} (no limit) with exponential backoff")
            
            # Use multiple workers for page discovery
            with ThreadPoolExecutor(max_workers=5) as executor:
                page = start_page
                consecutive_empty = 0
                consecutive_failures = 0
                backoff_delay = 1  # Start with 1 second
                max_backoff = 60   # Max 60 seconds
                # Count PRs discovered for this state (open/closed) by tracking in checkpoint
                total_found = getattr(state, f'{pr_state}_prs_found', 0)

                # Keep going until we hit the limit - infinite persistence with smart backoff!
                while page <= max_pages:
                    # If we've had too many consecutive failures, increase the backoff significantly
                    if consecutive_failures > 10:
                        logger.info(f"🔄 Many consecutive failures ({consecutive_failures}), using extended backoff...")
                        time.sleep(min(consecutive_failures * 5, 300))  # Up to 5 minutes
                    # Check if we've hit the limit for closed PRs
                    if limit and total_found >= limit:
                        logger.info(f"✅ Reached limit of {limit:,} {pr_state} PRs")
                        break
                    # Submit batch of pages (smaller batch for better rate limit handling)
                    batch_size = 3
                    futures = []
                    
                    for i in range(batch_size):
                        if page + i > max_pages:
                            break
                        
                        future = executor.submit(
                            self._fetch_page_urls, 
                            state.repo_url, 
                            pr_state, 
                            page + i
                        )
                        futures.append((page + i, future))
                    
                    # Process results
                    batch_found = 0
                    batch_failures = 0
                    for page_num, future in futures:
                        try:
                            page_urls = future.result(timeout=30)

                            if page_urls:
                                # Apply limit if specified
                                if limit:
                                    remaining_slots = limit - total_found
                                    if remaining_slots <= 0:
                                        break
                                    page_urls = page_urls[:remaining_slots]

                                consecutive_empty = 0
                                consecutive_failures = 0  # Reset on success
                                backoff_delay = 1  # Reset backoff on success
                                batch_found += len(page_urls)

                                self.checkpoint_manager.update_discovery_progress(
                                    state, pr_state, page_num, page_urls, False
                                )

                                # Update total_found from the state after checkpoint update
                                total_found = getattr(state, f'{pr_state}_prs_found', 0)

                                # Check if we've hit the limit
                                if limit and total_found >= limit:
                                    logger.info(f"🎯 Reached target limit of {limit:,} {pr_state} PRs!")
                                    break
                            else:
                                consecutive_empty += 1

                        except Exception as e:
                            logger.warning(f"Failed to fetch page {page_num}: {e}")
                            batch_failures += 1

                            # Count failures for exponential backoff
                            if "429" in str(e) or "rate limit" in str(e).lower():
                                consecutive_failures += 1
                                logger.info(f"Rate limit hit, consecutive failures: {consecutive_failures}")
                            else:
                                consecutive_empty += 1

                    page += batch_size

                    # Only break if we hit the limit - ignore consecutive empty pages!
                    if limit and total_found >= limit:
                        logger.info(f"🎯 Reached target limit of {limit:,} {pr_state} PRs!")
                        break

                    # Exponential backoff for rate limiting
                    if batch_failures > 0:
                        # Exponential backoff: double the delay each time, up to max
                        backoff_delay = min(backoff_delay * 2, max_backoff)
                        logger.info(f"⏳ Rate limiting detected, backing off for {backoff_delay}s (failures: {consecutive_failures})")
                        time.sleep(backoff_delay)
                    elif consecutive_empty > 0:
                        # Small delay for empty pages
                        time.sleep(2)
            
            # Mark this state as complete
            if pr_state == "open":
                state.open_pages_complete = True
            else:
                state.closed_pages_complete = True

            self.checkpoint_manager.save_state(state)
            logger.info(f"✅ Completed {pr_state} PR discovery: {total_found:,} URLs found")

        except Exception as e:
            logger.error(f"Error in {pr_state} PR discovery: {e}")
            raise

    def _fetch_page_urls(self, repo_url: str, pr_state: str, page: int) -> List[str]:
        """Fetch PR URLs from a single page."""
        try:
            # Use a random HTTP client (each gets new proxy IP)
            client_idx = page % len(self.http_clients)
            client = self.http_clients[client_idx]
            
            pulls_url = f"{repo_url.rstrip('/')}/pulls?q=is%3Apr+is%3A{pr_state}&page={page}"
            soup = client.get_soup(pulls_url)
            
            if not soup:
                return []
            
            # Extract URLs using the existing method
            scraper = self.pr_scrapers[client_idx]
            urls = scraper._extract_pr_urls_from_page(soup, repo_url)
            
            if urls:
                logger.debug(f"Page {page} ({pr_state}): {len(urls)} URLs")
            
            return urls
            
        except Exception as e:
            logger.warning(f"Failed to fetch page {page} ({pr_state}): {e}")
            return []

    def _scrape_prs_aggressively(self, state: PRCrawlState, cached_pr_numbers: Set[int]) -> None:
        """Scrape PRs with maximum parallelism."""
        try:
            remaining_urls = self.checkpoint_manager.get_remaining_urls(state)
            
            # Filter out already cached PRs
            filtered_urls = []
            for url in remaining_urls:
                try:
                    pr_number = int(url.split('/pull/')[-1])
                    if pr_number not in cached_pr_numbers:
                        filtered_urls.append(url)
                except (ValueError, IndexError):
                    filtered_urls.append(url)  # Include if we can't parse number
            
            total_to_scrape = len(filtered_urls)
            logger.info(f"⚡ Starting aggressive scraping: {total_to_scrape:,} PRs to scrape")
            
            if total_to_scrape == 0:
                logger.info("✅ All PRs already scraped!")
                return
            
            # Process in parallel batches
            batch_size = self.max_workers * 2
            scraped_count = 0
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                for i in range(0, total_to_scrape, batch_size):
                    batch_urls = filtered_urls[i:i + batch_size]
                    
                    # Submit batch
                    futures = []
                    for j, url in enumerate(batch_urls):
                        scraper_idx = j % len(self.pr_scrapers)
                        future = executor.submit(
                            self._scrape_single_pr_aggressive,
                            self.pr_scrapers[scraper_idx],
                            url
                        )
                        futures.append((url, future))
                    
                    # Process results
                    for url, future in futures:
                        try:
                            pr_info = future.result(timeout=60)
                            if pr_info:
                                # Cache immediately
                                self.cache_manager.cache_pr_immediately(state.repo_url, pr_info)
                                self.checkpoint_manager.update_scraping_progress(state, pr_info.number, True)
                                scraped_count += 1
                            else:
                                pr_number = int(url.split('/pull/')[-1]) if '/pull/' in url else 0
                                self.checkpoint_manager.update_scraping_progress(state, pr_number, False, url)
                                
                        except Exception as e:
                            logger.warning(f"Failed to scrape {url}: {e}")
                    
                    # Progress update
                    if scraped_count % 100 == 0:
                        progress = self.checkpoint_manager.get_progress_summary(state)
                        logger.info(f"⚡ Progress: {scraped_count}/{total_to_scrape} in batch, "
                                  f"{progress['scraped']:,} total, {progress['coverage_percent']:.1f}% coverage")
            
            logger.info(f"🎉 Aggressive scraping completed: {scraped_count} PRs scraped in this session")
            
        except Exception as e:
            logger.error(f"Error in aggressive PR scraping: {e}")

    def _scrape_single_pr_aggressive(self, scraper: PullRequestScraper, pr_url: str) -> Optional[PullRequestInfo]:
        """Scrape a single PR aggressively (no delays)."""
        try:
            # Direct scraping without delays (proxy gives us new IP each time)
            return scraper._scrape_single_pr(pr_url)
        except Exception as e:
            logger.debug(f"Failed to scrape {pr_url}: {e}")
            return None
