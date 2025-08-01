"""Issue detail scraper for extracting complete issue content."""

import re
from datetime import datetime
from typing import List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from loguru import logger

from .http_client import HTTPClient
from .models import IssueInfo, Comment


class IssueScraper:
    """Scraper for detailed issue information including complete chat history."""

    def __init__(self, http_client: HTTPClient):
        self.client = http_client

    def scrape_issue(self, repo_url: str, issue_number: int) -> Optional[IssueInfo]:
        """Scrape complete information for a single issue.
        
        Args:
            repo_url: Repository URL (e.g., https://github.com/owner/repo)
            issue_number: Issue number to scrape
            
        Returns:
            IssueInfo object with complete issue content or None if failed
        """
        try:
            # Construct issue URL
            issue_url = f"{repo_url.rstrip('/')}/issues/{issue_number}"
            logger.debug(f"Scraping issue: {issue_url}")

            soup = self.client.get_soup(issue_url)
            if not soup:
                logger.warning(f"Failed to get soup for issue {issue_url}")
                return None

            # Extract basic issue information
            title = self._extract_title(soup)
            if not title:
                logger.warning(f"Could not extract title for issue {issue_number}")
                return None

            state = self._extract_state(soup)
            author = self._extract_author(soup)
            created_at = self._extract_created_at(soup)
            updated_at = self._extract_updated_at(soup)
            tags = self._extract_tags(soup)
            
            # Extract complete comment history
            comments = self._extract_comments(soup)

            issue_info = IssueInfo(
                number=issue_number,
                title=title,
                state=state,
                author=author,
                created_at=created_at,
                updated_at=updated_at,
                tags=tags,
                comments=comments,
                url=issue_url,
            )

            logger.debug(f"Successfully scraped issue #{issue_number}")
            return issue_info

        except Exception as e:
            logger.error(f"Error scraping issue {issue_number}: {e}")
            return None

    def _extract_title(self, soup: BeautifulSoup) -> str:
        """Extract issue title."""
        try:
            # Try multiple selectors for issue title
            title_selectors = [
                "h1.gh-header-title .js-issue-title",
                "h1 .js-issue-title",
                ".gh-header-title",
                "h1.gh-header-title",
                ".js-issue-title"
            ]
            
            for selector in title_selectors:
                title_elem = soup.select_one(selector)
                if title_elem:
                    return title_elem.get_text(strip=True)
            
            # Fallback: look for any h1 that might contain the title
            h1_elements = soup.find_all("h1")
            for h1 in h1_elements:
                text = h1.get_text(strip=True)
                if text and len(text) > 5:  # Reasonable title length
                    return text
                    
            return ""
        except Exception as e:
            logger.warning(f"Error extracting title: {e}")
            return ""

    def _extract_state(self, soup: BeautifulSoup) -> str:
        """Extract issue state (open/closed)."""
        try:
            # Look for state indicators
            state_selectors = [
                ".State--open",
                ".State--closed", 
                ".State--merged",
                "[data-hovercard-type='issue'] .State",
                ".gh-header-meta .State"
            ]
            
            for selector in state_selectors:
                state_elem = soup.select_one(selector)
                if state_elem:
                    state_text = state_elem.get_text(strip=True).lower()
                    if "open" in state_text:
                        return "open"
                    elif "closed" in state_text:
                        return "closed"
            
            # Default to open if we can't determine
            return "open"
        except Exception as e:
            logger.warning(f"Error extracting state: {e}")
            return "open"

    def _extract_author(self, soup: BeautifulSoup) -> str:
        """Extract the author who opened the issue."""
        try:
            # Look for author information
            author_selectors = [
                ".timeline-comment-header .author",
                ".timeline-comment .author",
                "[data-hovercard-type='user']",
                ".author"
            ]
            
            for selector in author_selectors:
                author_elem = soup.select_one(selector)
                if author_elem:
                    return author_elem.get_text(strip=True)
            
            return "unknown"
        except Exception as e:
            logger.warning(f"Error extracting author: {e}")
            return "unknown"

    def _extract_created_at(self, soup: BeautifulSoup) -> str:
        """Extract issue creation timestamp."""
        try:
            # Look for creation timestamp
            time_selectors = [
                ".timeline-comment-header relative-time",
                "relative-time[datetime]",
                "time[datetime]"
            ]
            
            for selector in time_selectors:
                time_elem = soup.select_one(selector)
                if time_elem and time_elem.get("datetime"):
                    return time_elem.get("datetime")
            
            return datetime.now().isoformat()
        except Exception as e:
            logger.warning(f"Error extracting created_at: {e}")
            return datetime.now().isoformat()

    def _extract_updated_at(self, soup: BeautifulSoup) -> str:
        """Extract issue last updated timestamp."""
        try:
            # For now, use created_at as updated_at
            # In a more sophisticated implementation, we could look for the last comment timestamp
            return self._extract_created_at(soup)
        except Exception as e:
            logger.warning(f"Error extracting updated_at: {e}")
            return datetime.now().isoformat()

    def _extract_tags(self, soup: BeautifulSoup) -> List[str]:
        """Extract issue labels/tags."""
        try:
            tags = []
            
            # Look for labels
            label_selectors = [
                ".js-issue-labels .IssueLabel",
                ".sidebar-labels .IssueLabel",
                ".labels .label"
            ]
            
            for selector in label_selectors:
                label_elements = soup.select(selector)
                for label in label_elements:
                    tag_text = label.get_text(strip=True)
                    if tag_text and tag_text not in tags:
                        tags.append(tag_text)
            
            return tags
        except Exception as e:
            logger.warning(f"Error extracting tags: {e}")
            return []

    def _extract_comments(self, soup: BeautifulSoup) -> List[Comment]:
        """Extract complete comment history in chronological order."""
        try:
            comments = []
            
            # Look for timeline comments (includes the original issue description)
            comment_selectors = [
                ".timeline-comment",
                ".js-timeline-item"
            ]
            
            for selector in comment_selectors:
                comment_elements = soup.select(selector)
                
                for comment_elem in comment_elements:
                    comment = self._extract_single_comment(comment_elem)
                    if comment:
                        comments.append(comment)
            
            # Sort comments by timestamp to ensure chronological order
            comments.sort(key=lambda c: c.timestamp)
            
            return comments
        except Exception as e:
            logger.warning(f"Error extracting comments: {e}")
            return []

    def _extract_single_comment(self, comment_elem) -> Optional[Comment]:
        """Extract a single comment from a comment element."""
        try:
            # Extract author
            author_elem = comment_elem.select_one(".author")
            author = author_elem.get_text(strip=True) if author_elem else "unknown"
            
            # Extract timestamp
            time_elem = comment_elem.select_one("relative-time[datetime]")
            timestamp = time_elem.get("datetime") if time_elem else datetime.now().isoformat()
            
            # Extract content
            content_selectors = [
                ".comment-body",
                ".timeline-comment-body",
                ".edit-comment-hide"
            ]
            
            content = ""
            for selector in content_selectors:
                content_elem = comment_elem.select_one(selector)
                if content_elem:
                    content = content_elem.get_text(strip=True)
                    break
            
            if not content or len(content) < 5:  # Skip very short or empty comments
                return None
            
            # Limit content length to avoid huge data
            if len(content) > 2000:
                content = content[:2000] + "..."
            
            return Comment(
                author=author,
                timestamp=timestamp,
                content=content
            )
            
        except Exception as e:
            logger.warning(f"Error extracting single comment: {e}")
            return None
