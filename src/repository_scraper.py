"""Repository information scraper."""

import re
from typing import Optional, Tuple

from bs4 import BeautifulSoup
from loguru import logger

from .http_client import HTTPClient
from .models import RepositoryStats


class RepositoryScraper:
    """Scraper for basic repository information."""

    def __init__(self, http_client: HTTPClient):
        self.client = http_client
        self.last_error = None  # Store detailed error message for the caller

    def scrape_repository_stats(
        self, repo_url: str
    ) -> Optional[RepositoryStats]:
        """Scrape basic repository statistics."""
        try:
            logger.info(f"Scraping repository stats for: {repo_url}")

            # Get main repository page
            soup = self.client.get_soup(repo_url)
            if not soup:
                # Try to get more specific error information
                error_msg = self._get_detailed_error_message(repo_url)
                logger.error(f"Failed to get repository page: {repo_url} - {error_msg}")
                # Store the detailed error message for the caller
                self.last_error = error_msg
                return None

            stats = RepositoryStats()

            # Extract contributors count
            stats.contributors_count = self._extract_contributors_count(
                soup, repo_url
            )

            # Extract forks count
            stats.forks_count = self._extract_forks_count(soup)

            # Extract issues counts
            issues_counts = self._extract_issues_counts(soup, repo_url)
            if issues_counts:
                stats.total_issues, stats.open_issues, stats.closed_issues = (
                    issues_counts
                )

            # Extract pull requests counts
            pr_counts = self._extract_pr_counts(soup, repo_url)
            if pr_counts:
                (
                    stats.total_pull_requests,
                    stats.open_pull_requests,
                    stats.closed_pull_requests,
                ) = pr_counts
                # Mark PR counts available only if we actually parsed any (>0)
                stats.pr_counts_available = any([
                    stats.total_pull_requests,
                    stats.open_pull_requests,
                    stats.closed_pull_requests,
                ])
            else:
                stats.pr_counts_available = False

            logger.info(f"Successfully scraped stats for {repo_url}")
            return stats

        except Exception as e:
            error_msg = f"Error scraping repository stats for {repo_url}: {e}"
            logger.error(error_msg)
            self.last_error = str(e)
            return None

    def _get_detailed_error_message(self, repo_url: str) -> str:
        """Get detailed error message for failed repository access."""
        try:
            # Try to make a direct request to get the HTTP status code
            import requests
            response = self.client.session.get(repo_url, timeout=10)

            if response.status_code == 404:
                return "Repository not found (404) - it may have been deleted, made private, or the URL is incorrect"
            elif response.status_code == 403:
                return "Access forbidden (403) - repository may be private or rate limited"
            elif response.status_code == 429:
                return "Rate limited (429) - too many requests"
            elif response.status_code >= 500:
                return f"Server error ({response.status_code}) - GitHub may be experiencing issues"
            else:
                return f"HTTP error ({response.status_code})"

        except requests.exceptions.Timeout:
            return "Request timeout - network or server issues"
        except requests.exceptions.ConnectionError:
            return "Connection error - network issues or invalid URL"
        except Exception as e:
            return f"Unknown error: {str(e)}"

    def _extract_contributors_count(
        self, soup: BeautifulSoup, repo_url: str
    ) -> int:
        """Extract contributors count from repository page."""
        try:
            # Try to find contributors link and extract count
            contributors_link = soup.find(
                "a", href=re.compile(r"/graphs/contributors")
            )
            if contributors_link:
                # Look for count in the link text or nearby elements
                text = contributors_link.get_text(strip=True)
                numbers = re.findall(r"\d+", text)
                if numbers:
                    return int(numbers[0])

            # Alternative: try to get from insights page
            insights_url = repo_url.rstrip("/") + "/graphs/contributors"
            insights_soup = self.client.get_soup(insights_url)
            if insights_soup:
                # Look for contributor count in insights page
                contributor_elements = insights_soup.find_all(
                    "a", class_="Link--primary"
                )
                return len(contributor_elements) if contributor_elements else 0

            return 0
        except Exception as e:
            logger.warning(f"Failed to extract contributors count: {e}")
            return 0

    def _extract_forks_count(self, soup: BeautifulSoup) -> int:
        """Extract forks count from repository page."""
        try:
            # Look for fork count in the repository stats
            # Try multiple selectors for forks
            fork_selectors = [
                ("a", {"href": re.compile(r"/forks")}),
                ("a", {"href": re.compile(r"/network/members")}),
            ]

            for tag, attrs in fork_selectors:
                fork_link = soup.find(tag, attrs)
                if fork_link:
                    text = fork_link.get_text(strip=True)
                    logger.debug(f"Found fork link text: {text}")

                    # Extract numbers, handling 'k' suffix
                    if 'k' in text.lower():
                        # Handle format like "74.8k"
                        match = re.search(r"([\d.]+)k", text.lower())
                        if match:
                            number = float(match.group(1))
                            return int(number * 1000)

                    # Handle regular numbers with commas
                    numbers = re.findall(r"[\d,]+", text)
                    if numbers:
                        return int(numbers[0].replace(",", ""))

            return 0
        except Exception as e:
            logger.warning(f"Failed to extract forks count: {e}")
            return 0

    def _extract_issues_counts(
        self, soup: BeautifulSoup, repo_url: str
    ) -> Optional[Tuple[int, int, int]]:
        """Extract issues counts (total, open, closed)."""
        try:
            # First try to get open count from main page (more reliable)
            open_count, closed_count = self._extract_issues_from_main_page(soup)

            # If we got open count from main page, use it to calculate closed count
            if open_count > 0:
                logger.debug("Using main page open count for calculation...")
                # Get issues page to calculate closed count
                issues_url = repo_url.rstrip("/") + "/issues"
                issues_soup = self.client.get_soup(issues_url)
                if issues_soup:
                    # Calculate closed count using main page open count
                    # Formula: Closed Issues = Latest Issue Number - Open Issues - Total PRs
                    # (because PRs and issues share the same numbering space)
                    latest_issue_num = self._get_latest_issue_number(issues_soup)
                    if latest_issue_num > 0:
                        # Get total PR count to subtract from calculation
                        pr_counts = self._extract_pr_counts(soup, repo_url)
                        total_prs = pr_counts[0] if pr_counts else 0

                        # Calculate closed issues accounting for PRs
                        closed_count = max(0, latest_issue_num - open_count - total_prs)
                        logger.debug(f"Calculated closed accounting for PRs: {latest_issue_num} - {open_count} - {total_prs} = {closed_count}")

            # If main page method failed, try issues page extraction
            if open_count == 0:
                logger.debug("Main page failed, trying issues page...")
                issues_open, issues_closed = self._extract_issues_from_issues_page(repo_url)
                open_count = issues_open
                closed_count = issues_closed

            total_count = open_count + closed_count
            logger.debug(f"Final issue counts - Open: {open_count}, Closed: {closed_count}, Total: {total_count}")
            return total_count, open_count, closed_count

        except Exception as e:
            logger.warning(f"Failed to extract issues counts: {e}")
            return None

    def _extract_issues_from_main_page(self, soup: BeautifulSoup) -> Tuple[int, int]:
        """Try to extract issue counts from the main repository page."""
        try:
            open_count = 0
            closed_count = 0

            # Look for issue links on main page
            issue_links = soup.find_all("a", href=re.compile(r"/issues"))

            for link in issue_links:
                href = link.get('href', '')
                text = link.get_text(strip=True)

                # Look for patterns that indicate issue counts
                if 'issues' in href and any(char.isdigit() for char in text):
                    logger.debug(f"Found issue link on main page: {href} - {text}")

                    # Extract numbers from text, handling 'k' suffix
                    if 'k' in text.lower():
                        # Handle format like "2.6k"
                        match = re.search(r"([\d.]+)k", text.lower())
                        if match:
                            number = float(match.group(1))
                            count = int(number * 1000)
                        else:
                            continue
                    else:
                        # Handle regular numbers with commas
                        numbers = re.findall(r"[\d,]+", text)
                        if numbers:
                            count = int(numbers[0].replace(",", ""))
                        else:
                            continue

                    # Try to determine if it's open or closed based on context
                    if 'open' in text.lower() or 'open' in href.lower():
                        open_count = count
                    elif 'closed' in text.lower() or 'closed' in href.lower():
                        closed_count = count
                    elif open_count == 0:  # Assume first number found is open issues
                        open_count = count

            return open_count, closed_count

        except Exception as e:
            logger.debug(f"Failed to extract issues from main page: {e}")
            return 0, 0

    def _extract_issues_from_issues_page(self, repo_url: str) -> Tuple[int, int]:
        """Try to extract issue counts from the dedicated issues page."""
        try:
            issues_url = repo_url.rstrip("/") + "/issues"
            logger.debug(f"Fetching issues page: {issues_url}")
            issues_soup = self.client.get_soup(issues_url)
            if not issues_soup:
                logger.debug("Failed to get issues page")
                return 0, 0

            open_count = 0
            closed_count = 0

            # Method 1: Try new GitHub class-based selectors (as provided by user)
            open_count, closed_count = self._extract_issues_by_classes(issues_soup)

            # Method 2: If class-based extraction failed, try URL patterns
            if open_count == 0 and closed_count == 0:
                open_count, closed_count = self._extract_issues_by_url_patterns(issues_soup)

            logger.debug(f"Issues page extraction - Open: {open_count}, Closed: {closed_count}")
            return open_count, closed_count

        except Exception as e:
            logger.debug(f"Failed to extract issues from issues page: {e}")
            return 0, 0

    def _extract_issues_by_classes(self, soup: BeautifulSoup) -> Tuple[int, int]:
        """Extract issue counts using GitHub's class-based selectors and calculation method."""
        try:
            # Always use calculation method as it's more reliable
            # Get open issues count from the page
            open_count = self._get_open_issues_from_page(soup)

            # Calculate closed issues from latest issue number
            closed_count = self._calculate_closed_issues_from_latest(soup, open_count)

            return open_count, closed_count

        except Exception as e:
            logger.debug(f"Failed to extract issues by classes: {e}")
            return 0, 0

    def _find_closed_issues_marker(self, soup: BeautifulSoup) -> int:
        """Try to find closed issues count from page markers."""
        try:
            # Look for elements with "Closed" text and nearby count
            closed_elements = soup.find_all(string=re.compile(r"closed", re.I))

            for closed_text in closed_elements:
                parent = closed_text.parent if hasattr(closed_text, 'parent') else None
                if not parent:
                    continue

                # Look for count elements near this "Closed" text
                candidates = []

                # Check next siblings
                for sibling in parent.find_next_siblings():
                    text = sibling.get_text(strip=True)
                    numbers = re.findall(r'[\d,]+', text)
                    if numbers:
                        candidates.extend(numbers)
                    if len(candidates) > 5:
                        break

                # Check parent's siblings
                if parent.parent:
                    for sibling in parent.parent.find_next_siblings():
                        text = sibling.get_text(strip=True)
                        numbers = re.findall(r'[\d,]+', text)
                        if numbers:
                            candidates.extend(numbers)
                        if len(candidates) > 5:
                            break

                # Find a reasonable number for closed issues
                for num_str in candidates:
                    try:
                        num = int(num_str.replace(',', ''))
                        if 100 <= num <= 200000:  # Reasonable range
                            logger.debug(f"Found closed issues marker: {num}")
                            return num
                    except ValueError:
                        continue

            return 0

        except Exception as e:
            logger.debug(f"Failed to find closed issues marker: {e}")
            return 0

    def _calculate_closed_issues_from_latest(self, soup: BeautifulSoup, open_count: int) -> int:
        """Calculate closed issues from latest issue number minus open issues."""
        try:
            # Get latest issue number from the issues page
            latest_issue_num = self._get_latest_issue_number(soup)
            if latest_issue_num == 0:
                logger.debug("Could not get latest issue number")
                return 0

            if open_count == 0:
                logger.debug("Open count is 0, cannot calculate closed issues")
                return 0

            # Calculate closed issues
            # Formula: Closed Issues = Latest Issue Number - Open Issues
            # Note: This assumes issue numbers are sequential starting from 1
            # Some repos may have gaps due to deleted issues, but this is the best approximation
            closed_issues = latest_issue_num - open_count

            logger.debug(f"Calculated closed issues: {latest_issue_num} - {open_count} = {closed_issues}")
            return max(0, closed_issues)

        except Exception as e:
            logger.debug(f"Failed to calculate closed issues: {e}")
            return 0

    def _get_latest_issue_number(self, soup: BeautifulSoup) -> int:
        """Get the latest (highest) issue number from the issues page."""
        try:
            issue_numbers = []

            # Look for links to individual issues
            issue_links = soup.find_all("a", href=re.compile(r"/issues/\d+"))

            for link in issue_links:
                href = link.get('href', '')
                match = re.search(r'/issues/(\d+)', href)
                if match:
                    issue_num = int(match.group(1))
                    issue_numbers.append(issue_num)

            if issue_numbers:
                latest = max(issue_numbers)
                logger.debug(f"Latest issue number found: {latest}")
                return latest

            return 0

        except Exception as e:
            logger.debug(f"Error getting latest issue number: {e}")
            return 0

    def _get_open_issues_from_page(self, soup: BeautifulSoup) -> int:
        """Get open issues count from the issues page."""
        try:
            # Look for "Open" text with nearby numbers
            open_elements = soup.find_all(string=re.compile(r"open", re.I))

            for open_text in open_elements:
                parent = open_text.parent if hasattr(open_text, 'parent') else None
                if not parent:
                    continue

                # Look for numbers near "Open" text
                for sibling in parent.find_next_siblings():
                    text = sibling.get_text(strip=True)

                    # Handle 'k' suffix
                    if 'k' in text.lower():
                        match = re.search(r"([\d.]+)k", text.lower())
                        if match:
                            number = float(match.group(1))
                            count = int(number * 1000)
                            if 1 <= count <= 50000:  # Reasonable range for open issues
                                logger.debug(f"Found open issues from page (k format): {count}")
                                return count
                    else:
                        # Handle regular numbers
                        numbers = re.findall(r'[\d,]+', text)
                        if numbers:
                            try:
                                count = int(numbers[0].replace(',', ''))
                                if 1 <= count <= 50000:  # Reasonable range for open issues
                                    logger.debug(f"Found open issues from page: {count}")
                                    return count
                            except ValueError:
                                continue

            return 0

        except Exception as e:
            logger.debug(f"Error getting open issues from page: {e}")
            return 0

    def _extract_issues_by_url_patterns(self, soup: BeautifulSoup) -> Tuple[int, int]:
        """Extract issue counts using URL pattern matching (fallback method)."""
        try:
            open_count = 0
            closed_count = 0

            # Look for issue count indicators with multiple patterns
            patterns_to_try = [
                # Standard GitHub patterns
                (r"/issues\?q=is%3Aopen", "open"),
                (r"/issues\?q=is%3Aclosed", "closed"),
                # Alternative patterns
                (r"/issues\?q=is%3Aissue\+is%3Aopen", "open"),
                (r"/issues\?q=is%3Aissue\+is%3Aclosed", "closed"),
                (r"/issues\?q=is%3Aopen\+is%3Aissue", "open"),
                (r"/issues\?q=is%3Aclosed\+is%3Aissue", "closed"),
                # Broader patterns
                (r"issues.*open", "open"),
                (r"issues.*closed", "closed"),
            ]

            for pattern, issue_type in patterns_to_try:
                if (issue_type == "open" and open_count > 0) or (issue_type == "closed" and closed_count > 0):
                    continue  # Skip if we already found this type

                links = soup.find_all("a", href=re.compile(pattern, re.I))
                for link in links:
                    text = link.get_text(strip=True)
                    logger.debug(f"Found {issue_type} issue link text: {text}")
                    numbers = re.findall(r"[\d,]+", text)
                    if numbers:
                        count = int(numbers[0].replace(",", ""))
                        if issue_type == "open":
                            open_count = count
                        else:
                            closed_count = count
                        break

            return open_count, closed_count

        except Exception as e:
            logger.debug(f"Failed to extract issues by URL patterns: {e}")
            return 0, 0

    def _extract_pr_counts(
        self, soup: BeautifulSoup, repo_url: str
    ) -> Optional[Tuple[int, int, int]]:
        """Extract pull request counts (total, open, closed)."""
        try:
            pulls_url = repo_url.rstrip("/") + "/pulls"
            pulls_soup = self.client.get_soup(pulls_url)
            if not pulls_soup:
                return None

            open_count = 0
            closed_count = 0

            # Look for PR count indicators with multiple patterns
            open_patterns = [
                r"/pulls\?q=is%3Aopen",
                r"/pulls\?q=is%3Apr\+is%3Aopen",
                r"/pulls\?q=is%3Aopen\+is%3Apr"
            ]

            closed_patterns = [
                r"/pulls\?q=is%3Aclosed",
                r"/pulls\?q=is%3Apr\+is%3Aclosed",
                r"/pulls\?q=is%3Aclosed\+is%3Apr"
            ]

            # Extract open count
            for pattern in open_patterns:
                open_link = pulls_soup.find("a", href=re.compile(pattern))
                if open_link:
                    text = open_link.get_text(strip=True)
                    logger.debug(f"Found open PR link text: {text}")
                    numbers = re.findall(r"[\d,]+", text)
                    if numbers:
                        open_count = int(numbers[0].replace(",", ""))
                        break

            # Extract closed count
            for pattern in closed_patterns:
                closed_link = pulls_soup.find("a", href=re.compile(pattern))
                if closed_link:
                    text = closed_link.get_text(strip=True)
                    logger.debug(f"Found closed PR link text: {text}")
                    numbers = re.findall(r"[\d,]+", text)
                    if numbers:
                        closed_count = int(numbers[0].replace(",", ""))
                        break

            total_count = open_count + closed_count
            logger.debug(f"PR counts - Open: {open_count}, Closed: {closed_count}, Total: {total_count}")
            return total_count, open_count, closed_count

        except Exception as e:
            logger.warning(f"Failed to extract PR counts: {e}")
            return None
