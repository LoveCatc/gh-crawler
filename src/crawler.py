"""Main crawler with concurrent processing."""

import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional
from loguru import logger

from .models import InputRepository, CrawledRepository, CrawlResult, RepositoryStats
from .http_client import HTTPClient
from .repository_scraper import RepositoryScraper
from .pr_scraper import PullRequestScraper
from .config import MAX_WORKERS


class GitHubCrawler:
    """Main crawler with concurrent processing capabilities."""
    
    def __init__(self, max_workers: int = MAX_WORKERS):
        self.max_workers = max_workers
    
    def crawl_repositories(self, repositories: List[InputRepository], star_threshold: int) -> List[CrawledRepository]:
        """Crawl multiple repositories concurrently."""
        # Filter repositories by star threshold
        filtered_repos = [repo for repo in repositories if repo.stars >= star_threshold]
        
        logger.info(f"Crawling {len(filtered_repos)} repositories (filtered from {len(repositories)} by star threshold {star_threshold})")
        
        crawled_repos = []
        
        # Use ThreadPoolExecutor for concurrent crawling
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all crawling tasks
            future_to_repo = {
                executor.submit(self._crawl_single_repository, repo): repo 
                for repo in filtered_repos
            }
            
            # Process completed tasks
            for future in as_completed(future_to_repo):
                repo = future_to_repo[future]
                try:
                    result = future.result()
                    if result.success and result.repository:
                        crawled_repos.append(result.repository)
                        logger.info(f"Successfully crawled: {repo.url}")
                    else:
                        logger.error(f"Failed to crawl {repo.url}: {result.error}")
                except Exception as e:
                    logger.error(f"Exception crawling {repo.url}: {e}")
        
        logger.info(f"Completed crawling. Successfully processed {len(crawled_repos)} repositories")
        return crawled_repos
    
    def _crawl_single_repository(self, repo: InputRepository) -> CrawlResult:
        """Crawl a single repository."""
        try:
            logger.debug(f"Starting crawl for: {repo.url}")
            
            with HTTPClient() as client:
                # Initialize scrapers
                repo_scraper = RepositoryScraper(client)
                pr_scraper = PullRequestScraper(client)
                
                # Scrape repository statistics
                stats = repo_scraper.scrape_repository_stats(repo.url)
                if not stats:
                    return CrawlResult(
                        success=False,
                        error="Failed to scrape repository statistics"
                    )
                
                # Scrape pull request details
                pull_requests = pr_scraper.scrape_pull_requests(repo.url, limit=20)
                
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
    
    def __init__(self, max_workers: int = MAX_WORKERS):
        self.crawler = GitHubCrawler(max_workers)
    
    def process_repositories(self, repositories: List[InputRepository], star_threshold: int) -> List[CrawledRepository]:
        """Process repositories with error handling and logging."""
        start_time = time.time()
        
        logger.info(f"Starting repository processing with {len(repositories)} repositories")
        logger.info(f"Star threshold: {star_threshold}")
        logger.info(f"Max workers: {self.crawler.max_workers}")
        
        try:
            results = self.crawler.crawl_repositories(repositories, star_threshold)
            
            end_time = time.time()
            duration = end_time - start_time
            
            logger.info(f"Processing completed in {duration:.2f} seconds")
            logger.info(f"Successfully processed {len(results)} repositories")
            
            return results
            
        except Exception as e:
            logger.error(f"Error during repository processing: {e}")
            raise
