"""Pull request detail scraper with resume functionality."""

import re
import time
from datetime import datetime
from typing import List, Optional

from bs4 import BeautifulSoup
from loguru import logger

from .http_client import HTTPClient
from .models import PullRequestInfo, Comment, IssueInfo
from .issue_scraper import IssueScraper
from .pr_checkpoint_manager import PRCheckpointManager, PRCrawlState


class PullRequestScraper:
    """Scraper for detailed pull request information with resume functionality."""

    def __init__(self, http_client: HTTPClient):
        self.client = http_client
        self.issue_scraper = IssueScraper(http_client)
        self.checkpoint_manager = PRCheckpointManager()

    def scrape_pull_requests(
        self, repo_url: str, total_prs_expected: int, limit: int = None
    ) -> List[PullRequestInfo]:
        """Scrape detailed information for ALL pull requests with resume functionality.

        Args:
            repo_url: Repository URL
            total_prs_expected: Expected total number of PRs (for progress tracking)
            limit: Maximum number of PRs to scrape (None = no limit, get all PRs)
        """
        try:
            logger.info(f"Starting resumable PR crawl for: {repo_url}")
            logger.info(f"Expected total PRs: {total_prs_expected:,}")

            # Load or create checkpoint state
            state = self.checkpoint_manager.load_state(repo_url)
            if state is None:
                state = self.checkpoint_manager.create_initial_state(repo_url, total_prs_expected)
                logger.info("Starting fresh PR crawl (no checkpoint found)")
            else:
                progress = self.checkpoint_manager.get_progress_summary(state)
                logger.info(f"Resuming PR crawl from checkpoint:")
                logger.info(f"  - Discovered: {progress['discovered']:,} URLs")
                logger.info(f"  - Scraped: {progress['scraped']:,} PRs")
                logger.info(f"  - Failed: {progress['failed']:,} PRs")
                logger.info(f"  - Coverage: {progress['coverage_percent']:.1f}%")

            # Phase 1: Discover all PR URLs (if not complete)
            if not state.discovery_complete:
                logger.info("Phase 1: Discovering all PR URLs...")
                self._discover_all_pr_urls_resumable(state, limit)
            else:
                logger.info("Phase 1: PR URL discovery already complete")

            # Phase 2: Scrape all discovered PRs (if not complete)
            if not state.scraping_complete:
                logger.info("Phase 2: Scraping PR content...")
                pull_requests = self._scrape_all_prs_resumable(state)
            else:
                logger.info("Phase 2: PR scraping already complete")
                # Load already scraped PRs (this is a simplified approach)
                pull_requests = []

            # Final progress report
            final_progress = self.checkpoint_manager.get_progress_summary(state)
            logger.info(f"PR crawl completed for {repo_url}:")
            logger.info(f"  - Total discovered: {final_progress['discovered']:,}")
            logger.info(f"  - Successfully scraped: {final_progress['scraped']:,}")
            logger.info(f"  - Failed: {final_progress['failed']:,}")
            logger.info(f"  - Coverage: {final_progress['coverage_percent']:.1f}%")

            # Cleanup checkpoint if fully complete
            if state.scraping_complete and final_progress['coverage_percent'] > 95:
                self.checkpoint_manager.cleanup_checkpoint(repo_url)

            return pull_requests

        except Exception as e:
            logger.error(f"Error in resumable PR scraping for {repo_url}: {e}")
            return []

    def _discover_all_pr_urls_resumable(self, state: PRCrawlState, limit: int = None) -> None:
        """Discover all PR URLs with resume functionality."""
        try:
            # Discover open PRs (if not complete)
            if not state.open_pages_complete:
                logger.info("Discovering open PRs...")
                self._discover_pr_urls_by_state_resumable(state, "open", limit)

            # Discover closed PRs (if not complete)
            if not state.closed_pages_complete:
                logger.info("Discovering closed PRs...")
                self._discover_pr_urls_by_state_resumable(state, "closed", limit)

            # Mark discovery as complete
            state.discovery_complete = state.open_pages_complete and state.closed_pages_complete
            self.checkpoint_manager.save_state(state)

            logger.info(f"PR URL discovery complete: {len(state.discovered_pr_urls):,} URLs found")

        except Exception as e:
            logger.error(f"Error in PR URL discovery: {e}")
            raise

    def _discover_pr_urls_by_state_resumable(self, state: PRCrawlState, pr_state: str, limit: int = None) -> None:
        """Discover PR URLs for a specific state (open/closed) with resume functionality."""
        try:
            start_page = state.last_open_page + 1 if pr_state == "open" else state.last_closed_page + 1
            max_pages = 1000  # Safety limit
            consecutive_empty_pages = 0
            max_consecutive_empty = 5  # Stop after 5 consecutive empty pages

            logger.info(f"Starting {pr_state} PR discovery from page {start_page}")

            for page in range(start_page, max_pages + 1):
                try:
                    # Construct URL for specific state and page
                    pulls_url = f"{state.repo_url.rstrip('/')}/pulls?q=is%3Apr+is%3A{pr_state}&page={page}"

                    logger.debug(f"Fetching page {page} of {pr_state} PRs...")

                    # Add exponential backoff for rate limiting
                    success, soup = self._fetch_with_backoff(pulls_url)
                    if not success:
                        logger.warning(f"Failed to fetch page {page} after retries, stopping discovery")
                        break

                    # Extract PR URLs from this page
                    page_pr_urls = self._extract_pr_urls_from_page(soup, state.repo_url)

                    if not page_pr_urls:
                        consecutive_empty_pages += 1
                        logger.debug(f"No {pr_state} PRs found on page {page} (consecutive empty: {consecutive_empty_pages})")

                        if consecutive_empty_pages >= max_consecutive_empty:
                            logger.info(f"Reached end of {pr_state} PRs (no results for {max_consecutive_empty} consecutive pages)")
                            break
                    else:
                        consecutive_empty_pages = 0
                        logger.debug(f"Found {len(page_pr_urls)} {pr_state} PRs on page {page}")

                    # Update progress
                    pages_complete = len(page_pr_urls) == 0 and consecutive_empty_pages >= max_consecutive_empty
                    self.checkpoint_manager.update_discovery_progress(
                        state, pr_state, page, page_pr_urls, pages_complete
                    )

                    # Check if we've hit the limit
                    if limit and len(state.discovered_pr_urls) >= limit:
                        logger.info(f"Reached discovery limit of {limit} PRs")
                        break

                    # If we found no PRs and hit consecutive limit, mark as complete
                    if pages_complete:
                        break

                except Exception as e:
                    logger.error(f"Error processing page {page} of {pr_state} PRs: {e}")
                    # Continue to next page rather than failing completely
                    continue

            # Mark this state as complete
            if pr_state == "open":
                state.open_pages_complete = True
            else:
                state.closed_pages_complete = True

            self.checkpoint_manager.save_state(state)
            logger.info(f"Completed {pr_state} PR discovery: {len([url for url in state.discovered_pr_urls if pr_state in url])} URLs")

        except Exception as e:
            logger.error(f"Error in {pr_state} PR discovery: {e}")
            raise

    def _fetch_with_backoff(self, url: str, max_retries: int = 5) -> tuple[bool, Optional[BeautifulSoup]]:
        """Fetch URL with exponential backoff for rate limiting."""
        for attempt in range(max_retries):
            try:
                soup = self.client.get_soup(url)
                if soup:
                    return True, soup

                # If we get None, it might be rate limiting
                wait_time = (2 ** attempt) * 2  # Exponential backoff: 2, 4, 8, 16, 32 seconds
                logger.warning(f"Failed to fetch {url}, attempt {attempt + 1}/{max_retries}. Waiting {wait_time}s...")
                time.sleep(wait_time)

            except Exception as e:
                wait_time = (2 ** attempt) * 2
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}/{max_retries}): {e}. Waiting {wait_time}s...")
                time.sleep(wait_time)

        logger.error(f"Failed to fetch {url} after {max_retries} attempts")
        return False, None

    def _scrape_all_prs_resumable(self, state: PRCrawlState) -> List[PullRequestInfo]:
        """Scrape all discovered PRs with resume functionality."""
        try:
            remaining_urls = self.checkpoint_manager.get_remaining_urls(state)
            total_remaining = len(remaining_urls)

            logger.info(f"Starting PR content scraping: {total_remaining:,} PRs remaining")

            pull_requests = []

            for i, pr_url in enumerate(remaining_urls, 1):
                try:
                    # Progress logging
                    if i % 25 == 0 or i == total_remaining:
                        progress = self.checkpoint_manager.get_progress_summary(state)
                        logger.info(f"Progress: {i}/{total_remaining} remaining, "
                                  f"{progress['scraped']:,} total scraped, "
                                  f"{progress['coverage_percent']:.1f}% coverage")

                    # Scrape single PR with backoff
                    pr_info = self._scrape_single_pr_with_backoff(pr_url)

                    if pr_info:
                        pull_requests.append(pr_info)
                        self.checkpoint_manager.update_scraping_progress(state, pr_info.number, True)
                    else:
                        # Extract PR number for failed tracking
                        try:
                            pr_number = int(pr_url.split('/pull/')[-1])
                            self.checkpoint_manager.update_scraping_progress(state, pr_number, False, pr_url)
                        except (ValueError, IndexError):
                            logger.warning(f"Could not extract PR number from {pr_url}")

                except Exception as e:
                    logger.error(f"Error scraping PR {pr_url}: {e}")
                    # Continue with next PR
                    continue

            # Final save
            self.checkpoint_manager.save_state(state)

            logger.info(f"PR content scraping completed: {len(pull_requests)} PRs scraped in this session")
            return pull_requests

        except Exception as e:
            logger.error(f"Error in resumable PR scraping: {e}")
            return []

    def _scrape_single_pr_with_backoff(self, pr_url: str, max_retries: int = 3) -> Optional[PullRequestInfo]:
        """Scrape a single PR with backoff for rate limiting."""
        for attempt in range(max_retries):
            try:
                pr_info = self._scrape_single_pr(pr_url)
                if pr_info:
                    return pr_info

                # If scraping failed, wait before retry
                wait_time = (2 ** attempt) * 1  # 1, 2, 4 seconds
                logger.debug(f"PR scraping failed for {pr_url}, attempt {attempt + 1}/{max_retries}. Waiting {wait_time}s...")
                time.sleep(wait_time)

            except Exception as e:
                wait_time = (2 ** attempt) * 1
                logger.debug(f"Error scraping PR {pr_url} (attempt {attempt + 1}/{max_retries}): {e}. Waiting {wait_time}s...")
                time.sleep(wait_time)

        logger.warning(f"Failed to scrape PR {pr_url} after {max_retries} attempts")
        return None

    def _get_all_pr_urls(self, repo_url: str, limit: int = None) -> List[str]:
        """Get URLs of ALL pull requests (open and closed) with pagination support."""
        try:
            all_pr_urls = []

            # Get both open and closed PRs
            pr_states = ["open", "closed"]

            for state in pr_states:
                logger.debug(f"Fetching {state} pull requests...")
                state_urls = self._get_pr_urls_for_state(repo_url, state, limit)
                all_pr_urls.extend(state_urls)
                logger.debug(f"Found {len(state_urls)} {state} pull requests")

            # Remove duplicates (shouldn't happen, but just in case)
            unique_urls = list(dict.fromkeys(all_pr_urls))

            logger.info(f"Total unique pull requests found: {len(unique_urls)}")
            return unique_urls

        except Exception as e:
            logger.warning(f"Failed to get all PR URLs: {e}")
            return []

    def _get_pr_urls_for_state(self, repo_url: str, state: str, limit: int = None) -> List[str]:
        """Get PR URLs for a specific state (open/closed) with pagination."""
        try:
            pr_urls = []
            page = 1
            max_pages = 100  # Safety limit to prevent infinite loops

            while page <= max_pages:
                # Construct URL for specific state and page
                pulls_url = f"{repo_url.rstrip('/')}/pulls?q=is%3Apr+is%3A{state}&page={page}"

                logger.debug(f"Fetching page {page} of {state} PRs: {pulls_url}")
                soup = self.client.get_soup(pulls_url)
                if not soup:
                    break

                # Find PR links on this page
                page_pr_urls = self._extract_pr_urls_from_page(soup, repo_url)

                if not page_pr_urls:
                    # No more PRs found, we've reached the end
                    logger.debug(f"No more {state} PRs found on page {page}")
                    break

                pr_urls.extend(page_pr_urls)
                logger.debug(f"Found {len(page_pr_urls)} {state} PRs on page {page}")

                # Check if we've hit the limit
                if limit and len(pr_urls) >= limit:
                    pr_urls = pr_urls[:limit]
                    break

                page += 1

            logger.info(f"Found {len(pr_urls)} {state} pull requests across {page-1} pages")
            return pr_urls

        except Exception as e:
            logger.warning(f"Failed to get {state} PR URLs: {e}")
            return []

    def _extract_pr_urls_from_page(self, soup, repo_url: str) -> List[str]:
        """Extract PR URLs from a single page."""
        try:
            pr_urls = []

            # Look for PR links with various selectors
            selectors = [
                "a.Link--primary",  # Primary link class
                "a[href*='/pull/']",  # Any link containing /pull/
                ".js-navigation-item a[href*='/pull/']",  # Navigation item links
            ]

            for selector in selectors:
                pr_links = soup.select(selector)

                for link in pr_links:
                    href = link.get("href")
                    if href and "/pull/" in href and href not in [url.split('/')[-1] for url in pr_urls]:
                        # Construct full URL properly
                        if href.startswith("/"):
                            full_url = "https://github.com" + href
                        else:
                            full_url = repo_url.rstrip("/") + "/" + href.lstrip("/")

                        # Avoid duplicates
                        if full_url not in pr_urls:
                            pr_urls.append(full_url)

            return pr_urls

        except Exception as e:
            logger.warning(f"Failed to extract PR URLs from page: {e}")
            return []

    def _get_pr_urls(self, repo_url: str, limit: int) -> List[str]:
        """Legacy method - kept for backward compatibility."""
        return self._get_all_pr_urls(repo_url, limit)

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

            # Extract basic information
            title = self._extract_title(soup)
            state = self._extract_state(soup)
            author = self._extract_author(soup)
            created_at = self._extract_created_at(soup)
            updated_at = self._extract_updated_at(soup)
            tags = self._extract_tags(soup)

            # Extract complete comment history
            comments = self._extract_complete_comments(soup)

            # Extract related issue IDs first
            related_issue_ids = self._extract_related_issue_ids(soup)

            # Extract complete content for related issues
            related_issues = self._extract_related_issues_content(pr_url, related_issue_ids)

            pr_info = PullRequestInfo(
                number=pr_number,
                title=title,
                state=state,
                author=author,
                created_at=created_at,
                updated_at=updated_at,
                tags=tags,
                comments=comments,
                related_issues=related_issues,
                url=pr_url,
            )

            logger.debug(f"Successfully scraped PR #{pr_number} with {len(comments)} comments and {len(related_issues)} related issues")
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
            # Look for state indicators - check both span and div elements
            state_selectors = [
                ".State--merged",
                ".State--closed",
                ".State--open",
                "[data-hovercard-type='pull_request'] .State",
                ".gh-header-meta .State"
            ]

            for selector in state_selectors:
                state_elem = soup.select_one(selector)
                if state_elem:
                    state_text = state_elem.get_text(strip=True).lower()
                    if "merged" in state_text:
                        return "merged"
                    elif "closed" in state_text:
                        return "closed"
                    elif "open" in state_text:
                        return "open"

            # Default to open if we can't determine
            return "open"
        except Exception as e:
            logger.warning(f"Error extracting state: {e}")
            return "open"

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

    def _extract_author(self, soup: BeautifulSoup) -> str:
        """Extract the author who opened the PR."""
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
        """Extract PR creation timestamp."""
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
        """Extract PR last updated timestamp."""
        try:
            # For now, use created_at as updated_at
            # In a more sophisticated implementation, we could look for the last comment timestamp
            return self._extract_created_at(soup)
        except Exception as e:
            logger.warning(f"Error extracting updated_at: {e}")
            return datetime.now().isoformat()

    def _extract_complete_comments(self, soup: BeautifulSoup) -> List[Comment]:
        """Extract complete comment history in chronological order."""
        try:
            comments = []

            # Look for timeline comments (includes the original PR description)
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
            logger.warning(f"Error extracting complete comments: {e}")
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

    def _extract_related_issue_ids(self, soup: BeautifulSoup) -> List[int]:
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

    def _extract_related_issues_content(self, pr_url: str, issue_ids: List[int]) -> List[IssueInfo]:
        """Extract complete content for related issues."""
        try:
            # Extract repository URL from PR URL
            # PR URL format: https://github.com/owner/repo/pull/123
            repo_url = "/".join(pr_url.split("/")[:5])  # https://github.com/owner/repo

            related_issues = []
            for issue_id in issue_ids:
                logger.debug(f"Scraping related issue #{issue_id} for PR {pr_url}")
                issue_info = self.issue_scraper.scrape_issue(repo_url, issue_id)
                if issue_info:
                    related_issues.append(issue_info)
                    logger.debug(f"Successfully scraped related issue #{issue_id}")
                else:
                    logger.warning(f"Failed to scrape related issue #{issue_id}")

            return related_issues
        except Exception as e:
            logger.error(f"Error extracting related issues content: {e}")
            return []
