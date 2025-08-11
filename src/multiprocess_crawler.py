"""Multiprocessing-enhanced crawler for maximum CPU utilization."""

import time
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from typing import List, Optional, Tuple
from loguru import logger
import os

from .models import InputRepository, CrawledRepository, CrawlResult
from .enhanced_crawler import EnhancedGitHubCrawler
from .unified_cache_manager import UnifiedCacheManager
from .io_handler import OutputHandler
from .config import REPO_WORKERS, MAX_WORKERS, DISCOVERY_WORKERS, MULTIPROCESS_THRESHOLD
from .performance_monitor import get_performance_monitor


def crawl_repository_batch_worker(batch_data: Tuple[List[InputRepository], str, int, dict]) -> List[Tuple[bool, Optional[CrawledRepository], str]]:
    """Worker function for crawling a batch of repositories in a separate process.

    Args:
        batch_data: Tuple of (repository_batch, output_file, max_workers, proxy_config)

    Returns:
        List of tuples: (success, crawled_repository, error_message)
    """
    repo_batch, output_file, max_workers, proxy_config = batch_data

    try:
        # Import here to avoid circular imports in multiprocessing
        from .io_handler import OutputHandler

        # Configure proxy settings for this process
        for key, value in proxy_config.items():
            os.environ[key] = value

        # Create a new cache manager for this process
        cache_manager = UnifiedCacheManager()

        # Use full worker count for better I/O parallelism within each process
        crawler = EnhancedGitHubCrawler(max_workers=max_workers, cache_manager=cache_manager)

        logger.info(f"🔄 Process {os.getpid()}: Starting batch crawl for {len(repo_batch)} repositories")

        results = []
        for repo in repo_batch:
            try:
                # Crawl the repository
                result = crawler._crawl_single_repository(repo)

                if result.success:
                    # Write to output file immediately for faster feedback
                    write_success = OutputHandler.append_crawled_repository(result.repository, output_file)

                    if write_success:
                        logger.info(f"✅ Process {os.getpid()}: Successfully crawled and saved: {repo.url}")
                        results.append((True, result.repository, ""))
                    else:
                        logger.error(f"❌ Process {os.getpid()}: Crawled {repo.url} but failed to write to disk")
                        results.append((False, None, "Failed to write to output file"))
                else:
                    logger.debug(f"❌ Process {os.getpid()}: Failed to crawl {repo.url}: {result.error}")
                    results.append((False, None, result.error or "Unknown error"))

            except Exception as e:
                error_msg = f"Exception crawling {repo.url}: {e}"
                logger.debug(error_msg)
                results.append((False, None, str(e)))

        # Flush cache before process exits to ensure all data is written
        cache_manager.flush_cache()

        logger.info(f"✅ Process {os.getpid()}: Completed batch of {len(repo_batch)} repositories")
        return results

    except Exception as e:
        error_msg = f"Process {os.getpid()}: Exception in batch worker: {e}"
        logger.error(error_msg)
        # Return failure for all repositories in the batch
        return [(False, None, str(e)) for _ in repo_batch]


class MultiprocessCrawler:
    """Enhanced crawler using both multiprocessing and multithreading for maximum performance."""
    
    def __init__(self, repo_workers: int = REPO_WORKERS, max_workers: int = MAX_WORKERS):
        self.repo_workers = repo_workers
        self.max_workers = max_workers
        self.cache_manager = UnifiedCacheManager()
        
        logger.info(f"🚀 Initialized MultiprocessCrawler:")
        logger.info(f"   Repository workers (processes): {self.repo_workers}")
        logger.info(f"   Thread workers per process: {self.max_workers}")
        logger.info(f"   Total potential workers: {self.repo_workers * self.max_workers}")
    
    def crawl_repositories(self, repositories: List[InputRepository], star_threshold: int,
                          output_dir: str = "output", current_output_file: str = None) -> int:
        """Crawl multiple repositories using optimized multiprocessing with batching."""

        # Filter repositories by star threshold
        filtered_repos = [repo for repo in repositories if repo.stars >= star_threshold]
        logger.info(f"Found {len(filtered_repos)} repositories above star threshold {star_threshold}")

        # Filter out already crawled repositories
        uncrawled_repos = self._filter_uncrawled_repositories(filtered_repos, output_dir, current_output_file)

        if not uncrawled_repos:
            logger.info("No new repositories to crawl")
            return 0

        # Calculate optimal batch size for faster feedback (smaller batches = faster output)
        # Use smaller batches to provide immediate feedback as repositories complete
        optimal_batch_size = max(1, min(3, len(uncrawled_repos) // self.repo_workers))
        if optimal_batch_size == 0:
            optimal_batch_size = 1  # Ensure at least 1 repository per batch

        logger.info(f"🚀 Starting optimized multiprocess crawl for {len(uncrawled_repos)} repositories")
        logger.info(f"Using {self.repo_workers} processes with batch size {optimal_batch_size}")
        logger.info(f"Each process will use {self.max_workers} threads for maximum I/O parallelism")

        successful_count = 0
        performance_monitor = get_performance_monitor()

        # Create repository batches for worker processes
        repo_batches = []
        for i in range(0, len(uncrawled_repos), optimal_batch_size):
            batch = uncrawled_repos[i:i + optimal_batch_size]
            repo_batches.append(batch)

        # Prepare proxy configuration to pass to worker processes
        proxy_config = {
            "PROXY_TYPE": os.environ.get("PROXY_TYPE", "none"),
            "PROXY_URL": os.environ.get("PROXY_URL", ""),
            "PROXY_HOST": os.environ.get("PROXY_HOST", ""),
            "PROXY_PORT": os.environ.get("PROXY_PORT", "1080"),
            "PROXY_USERNAME": os.environ.get("PROXY_USERNAME", ""),
            "PROXY_PASSWORD": os.environ.get("PROXY_PASSWORD", ""),
            "ENABLE_PROXY_REFRESH": os.environ.get("ENABLE_PROXY_REFRESH", "False"),
            "PROXY_REFRESH_INTERVAL": os.environ.get("PROXY_REFRESH_INTERVAL", "10"),
        }

        # Prepare data for worker processes
        batch_data_list = [(batch, current_output_file, self.max_workers, proxy_config) for batch in repo_batches]

        # Use ProcessPoolExecutor for batch-level parallelism
        with ProcessPoolExecutor(max_workers=self.repo_workers) as executor:
            # Submit all batch crawling tasks
            future_to_batch = {
                executor.submit(crawl_repository_batch_worker, batch_data): batch_data[0]
                for batch_data in batch_data_list
            }

            # Process completed batches
            for future in as_completed(future_to_batch):
                batch = future_to_batch[future]

                try:
                    batch_results = future.result()

                    # Process each repository result in the batch
                    for i, (success, crawled_repo, error_msg) in enumerate(batch_results):
                        repo = batch[i]

                        if success and crawled_repo:
                            # Repository was already written to output file by worker process
                            successful_count += 1
                            performance_monitor.increment_repositories()
                            performance_monitor.increment_prs(len(crawled_repo.pull_requests))
                            logger.debug(f"✅ Batch completed for: {repo.url}")
                        else:
                            performance_monitor.increment_errors()
                            logger.error(f"❌ Failed to crawl repository: {repo.url}")
                            if error_msg:
                                logger.debug(f"Error: {error_msg}")

                except Exception as e:
                    performance_monitor.increment_errors()
                    logger.error(f"❌ Exception processing batch: {e}")

        logger.info(f"🎯 Optimized multiprocess crawling completed: {successful_count}/{len(uncrawled_repos)} repositories successful")
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


class HybridCrawler:
    """Hybrid crawler that can switch between threading and multiprocessing based on workload."""

    def __init__(self, repo_workers: int = REPO_WORKERS, max_workers: int = MAX_WORKERS):
        self.repo_workers = repo_workers
        self.max_workers = max_workers
        self.multiprocess_crawler = MultiprocessCrawler(repo_workers, max_workers)
        self.enhanced_crawler = EnhancedGitHubCrawler(max_workers, UnifiedCacheManager())

        # Threshold for switching to multiprocessing (configurable to avoid overhead)
        # Only use multiprocessing when we have enough work to justify the overhead
        self.multiprocess_threshold = MULTIPROCESS_THRESHOLD
    
    def crawl_repositories(self, repositories: List[InputRepository], star_threshold: int,
                          output_dir: str = "output", current_output_file: str = None) -> int:
        """Crawl repositories using the optimal strategy based on workload size."""
        
        # Filter repositories by star threshold
        filtered_repos = [repo for repo in repositories if repo.stars >= star_threshold]
        
        # Decide on strategy based on number of repositories
        if len(filtered_repos) >= self.multiprocess_threshold:
            logger.info(f"🔀 Using MULTIPROCESSING strategy for {len(filtered_repos)} repositories")
            return self.multiprocess_crawler.crawl_repositories(
                repositories, star_threshold, output_dir, current_output_file
            )
        else:
            logger.info(f"🔀 Using THREADING strategy for {len(filtered_repos)} repositories")
            return self.enhanced_crawler.crawl_repositories(
                repositories, star_threshold, output_dir, current_output_file
            )


class MultiprocessCrawlerManager:
    """Manager for coordinating multiprocess crawling operations."""
    
    def __init__(self, repo_workers: int = REPO_WORKERS, max_workers: int = MAX_WORKERS):
        self.crawler = HybridCrawler(repo_workers, max_workers)
    
    def process_repositories(self, repositories: List[InputRepository], star_threshold: int,
                           output_dir: str, current_output_file: str) -> int:
        """Process repositories with optimized multiprocess crawling."""
        
        start_time = time.time()
        performance_monitor = get_performance_monitor()
        
        logger.info(f"🚀 Starting optimized repository processing")
        logger.info(f"Target: {len(repositories)} repositories with star threshold {star_threshold}")
        logger.info(f"Available CPU cores: {mp.cpu_count()}")
        logger.info(f"Repository workers: {self.crawler.repo_workers}")
        logger.info(f"Thread workers per process: {self.crawler.max_workers}")
        logger.info(f"Output file: {current_output_file}")
        
        try:
            successful_count = self.crawler.crawl_repositories(
                repositories, star_threshold, output_dir, current_output_file
            )
            
            end_time = time.time()
            duration = end_time - start_time
            
            logger.info(f"✅ Optimized processing completed in {duration:.2f} seconds")
            logger.info(f"Successfully processed and saved {successful_count} repositories")
            
            # Log performance summary
            summary = performance_monitor.get_metrics_summary()
            logger.info(f"📊 Performance Summary:")
            logger.info(f"   Throughput: {summary['repositories_per_minute']:.1f} repos/min")
            logger.info(f"   PRs scraped: {summary['prs_scraped']:,}")
            logger.info(f"   Requests made: {summary['requests_made']:,}")
            logger.info(f"   Average CPU: {summary['avg_cpu_usage']:.1f}%")
            
            return successful_count
            
        except Exception as e:
            logger.error(f"❌ Error during optimized repository processing: {e}")
            raise
