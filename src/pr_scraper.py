"""Pull request detail scraper."""

import re
from typing import List, Optional

from bs4 import BeautifulSoup
from loguru import logger

from .http_client import HTTPClient
from .models import PullRequestInfo


class PullRequestScraper:
    """Scraper for detailed pull request information."""

    def __init__(self, http_client: HTTPClient):
        self.client = http_client

    def scrape_pull_requests(
        self, repo_url: str, limit: int = 50
    ) -> List[PullRequestInfo]:
        """Scrape detailed information for pull requests."""
        try:
            logger.info(f"Scraping pull requests for: {repo_url}")

            # Get list of pull requests
            pr_urls = self._get_pr_urls(repo_url, limit)

            pull_requests = []
            for pr_url in pr_urls:
                pr_info = self._scrape_single_pr(pr_url)
                if pr_info:
                    pull_requests.append(pr_info)

            logger.info(
                f"Successfully scraped {len(pull_requests)} pull requests for {repo_url}"
            )
            return pull_requests

        except Exception as e:
            logger.error(f"Error scraping pull requests for {repo_url}: {e}")
            return []

    def _get_pr_urls(self, repo_url: str, limit: int) -> List[str]:
        """Get URLs of pull requests to scrape."""
        try:
            pulls_url = repo_url.rstrip("/") + "/pulls?q=is%3Apr"
            soup = self.client.get_soup(pulls_url)
            if not soup:
                return []

            pr_urls = []
            pr_links = soup.find_all("a", class_="Link--primary")

            for link in pr_links[:limit]:
                href = link.get("href")
                if href and "/pull/" in href:
                    # Construct full URL properly
                    if href.startswith("/"):
                        full_url = "https://github.com" + href
                    else:
                        full_url = repo_url.rstrip("/") + "/" + href.lstrip("/")
                    pr_urls.append(full_url)

            return pr_urls

        except Exception as e:
            logger.warning(f"Failed to get PR URLs: {e}")
            return []

    def _scrape_single_pr(self, pr_url: str) -> Optional[PullRequestInfo]:
        """Scrape detailed information for a single pull request."""
        try:
            logger.debug(f"Scraping PR: {pr_url}")

            soup = self.client.get_soup(pr_url)
            if not soup:
                return None

            # Extract PR number from URL
            pr_number = self._extract_pr_number(pr_url)
            if not pr_number:
                return None

            # Extract title
            title = self._extract_title(soup)

            # Extract state
            state = self._extract_state(soup)

            # Extract tags/labels
            tags = self._extract_tags(soup)

            # Extract comments
            comments = self._extract_comments(soup)

            # Extract related issues
            related_issues = self._extract_related_issues(soup)

            pr_info = PullRequestInfo(
                number=pr_number,
                title=title,
                state=state,
                tags=tags,
                comments=comments,
                related_issues=related_issues,
                url=pr_url,
            )

            logger.debug(f"Successfully scraped PR #{pr_number}")
            return pr_info

        except Exception as e:
            logger.warning(f"Failed to scrape PR {pr_url}: {e}")
            return None

    def _extract_pr_number(self, pr_url: str) -> Optional[int]:
        """Extract PR number from URL."""
        try:
            match = re.search(r"/pull/(\d+)", pr_url)
            return int(match.group(1)) if match else None
        except:
            return None

    def _extract_title(self, soup: BeautifulSoup) -> str:
        """Extract PR title."""
        try:
            title_element = soup.find("h1", class_="gh-header-title")
            if title_element:
                return title_element.get_text(strip=True)

            # Alternative selector
            title_element = soup.find("span", class_="js-issue-title")
            if title_element:
                return title_element.get_text(strip=True)

            return ""
        except:
            return ""

    def _extract_state(self, soup: BeautifulSoup) -> str:
        """Extract PR state (open, closed, merged)."""
        try:
            # Look for state indicators
            if soup.find("span", class_="State--merged"):
                return "merged"
            elif soup.find("span", class_="State--closed"):
                return "closed"
            elif soup.find("span", class_="State--open"):
                return "open"

            return "unknown"
        except:
            return "unknown"

    def _extract_tags(self, soup: BeautifulSoup) -> List[str]:
        """Extract tags/labels from PR."""
        try:
            tags = []
            label_elements = soup.find_all("a", class_="IssueLabel")

            for label in label_elements:
                tag_text = label.get_text(strip=True)
                if tag_text:
                    tags.append(tag_text)

            return tags
        except:
            return []

    def _extract_comments(self, soup: BeautifulSoup) -> List[str]:
        """Extract comments from PR."""
        try:
            comments = []
            comment_elements = soup.find_all("div", class_="comment-body")

            for comment in comment_elements:
                comment_text = comment.get_text(strip=True)
                if (
                    comment_text and len(comment_text) > 10
                ):  # Filter out very short comments
                    # Limit comment length to avoid huge data
                    comments.append(comment_text[:500])

            return comments[:10]  # Limit to first 10 comments
        except:
            return []

    def _extract_related_issues(self, soup: BeautifulSoup) -> List[int]:
        """Extract related issue numbers mentioned in PR."""
        try:
            related_issues = []

            # Look for issue references in the PR body and comments
            text_content = soup.get_text()

            # Find issue references like #123, fixes #123, closes #123
            issue_patterns = [
                r"#(\d+)",
                r"fixes?\s+#(\d+)",
                r"closes?\s+#(\d+)",
                r"resolves?\s+#(\d+)",
            ]

            for pattern in issue_patterns:
                matches = re.findall(pattern, text_content, re.IGNORECASE)
                for match in matches:
                    issue_num = int(match)
                    if issue_num not in related_issues:
                        related_issues.append(issue_num)

            return related_issues[:5]  # Limit to first 5 related issues
        except:
            return []
            return []
