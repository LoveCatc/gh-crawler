"""Repository information scraper."""

import re
from typing import Optional, Tuple
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from loguru import logger

from .http_client import HTTPClient
from .models import RepositoryStats


class RepositoryScraper:
    """Scraper for basic repository information."""
    
    def __init__(self, http_client: HTTPClient):
        self.client = http_client
    
    def scrape_repository_stats(self, repo_url: str) -> Optional[RepositoryStats]:
        """Scrape basic repository statistics."""
        try:
            logger.info(f"Scraping repository stats for: {repo_url}")
            
            # Get main repository page
            soup = self.client.get_soup(repo_url)
            if not soup:
                logger.error(f"Failed to get repository page: {repo_url}")
                return None
            
            stats = RepositoryStats()
            
            # Extract contributors count
            stats.contributors_count = self._extract_contributors_count(soup, repo_url)
            
            # Extract forks count
            stats.forks_count = self._extract_forks_count(soup)
            
            # Extract issues counts
            issues_counts = self._extract_issues_counts(soup, repo_url)
            if issues_counts:
                stats.total_issues, stats.open_issues, stats.closed_issues = issues_counts
            
            # Extract pull requests counts
            pr_counts = self._extract_pr_counts(soup, repo_url)
            if pr_counts:
                stats.total_pull_requests, stats.open_pull_requests, stats.closed_pull_requests = pr_counts
            
            logger.info(f"Successfully scraped stats for {repo_url}")
            return stats
            
        except Exception as e:
            logger.error(f"Error scraping repository stats for {repo_url}: {e}")
            return None
    
    def _extract_contributors_count(self, soup: BeautifulSoup, repo_url: str) -> int:
        """Extract contributors count from repository page."""
        try:
            # Try to find contributors link and extract count
            contributors_link = soup.find('a', href=re.compile(r'/graphs/contributors'))
            if contributors_link:
                # Look for count in the link text or nearby elements
                text = contributors_link.get_text(strip=True)
                numbers = re.findall(r'\d+', text)
                if numbers:
                    return int(numbers[0])
            
            # Alternative: try to get from insights page
            insights_url = urljoin(repo_url, '/graphs/contributors')
            insights_soup = self.client.get_soup(insights_url)
            if insights_soup:
                # Look for contributor count in insights page
                contributor_elements = insights_soup.find_all('a', class_='Link--primary')
                return len(contributor_elements) if contributor_elements else 0
            
            return 0
        except Exception as e:
            logger.warning(f"Failed to extract contributors count: {e}")
            return 0
    
    def _extract_forks_count(self, soup: BeautifulSoup) -> int:
        """Extract forks count from repository page."""
        try:
            # Look for fork count in the repository stats
            fork_link = soup.find('a', href=re.compile(r'/network/members'))
            if fork_link:
                text = fork_link.get_text(strip=True)
                numbers = re.findall(r'[\d,]+', text)
                if numbers:
                    return int(numbers[0].replace(',', ''))
            
            return 0
        except Exception as e:
            logger.warning(f"Failed to extract forks count: {e}")
            return 0
    
    def _extract_issues_counts(self, soup: BeautifulSoup, repo_url: str) -> Optional[Tuple[int, int, int]]:
        """Extract issues counts (total, open, closed)."""
        try:
            issues_url = urljoin(repo_url, '/issues')
            issues_soup = self.client.get_soup(issues_url)
            if not issues_soup:
                return None
            
            open_count = 0
            closed_count = 0
            
            # Look for issue count indicators
            open_link = issues_soup.find('a', href=re.compile(r'/issues\?q=is%3Aopen'))
            if open_link:
                text = open_link.get_text(strip=True)
                numbers = re.findall(r'[\d,]+', text)
                if numbers:
                    open_count = int(numbers[0].replace(',', ''))
            
            closed_link = issues_soup.find('a', href=re.compile(r'/issues\?q=is%3Aclosed'))
            if closed_link:
                text = closed_link.get_text(strip=True)
                numbers = re.findall(r'[\d,]+', text)
                if numbers:
                    closed_count = int(numbers[0].replace(',', ''))
            
            total_count = open_count + closed_count
            return total_count, open_count, closed_count
            
        except Exception as e:
            logger.warning(f"Failed to extract issues counts: {e}")
            return None
    
    def _extract_pr_counts(self, soup: BeautifulSoup, repo_url: str) -> Optional[Tuple[int, int, int]]:
        """Extract pull request counts (total, open, closed)."""
        try:
            pulls_url = urljoin(repo_url, '/pulls')
            pulls_soup = self.client.get_soup(pulls_url)
            if not pulls_soup:
                return None
            
            open_count = 0
            closed_count = 0
            
            # Look for PR count indicators
            open_link = pulls_soup.find('a', href=re.compile(r'/pulls\?q=is%3Aopen'))
            if open_link:
                text = open_link.get_text(strip=True)
                numbers = re.findall(r'[\d,]+', text)
                if numbers:
                    open_count = int(numbers[0].replace(',', ''))
            
            closed_link = pulls_soup.find('a', href=re.compile(r'/pulls\?q=is%3Aclosed'))
            if closed_link:
                text = closed_link.get_text(strip=True)
                numbers = re.findall(r'[\d,]+', text)
                if numbers:
                    closed_count = int(numbers[0].replace(',', ''))
            
            total_count = open_count + closed_count
            return total_count, open_count, closed_count
            
        except Exception as e:
            logger.warning(f"Failed to extract PR counts: {e}")
            return None
