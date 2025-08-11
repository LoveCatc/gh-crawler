"""Commit scraper for extracting repository and PR commit information."""

import re
from typing import List, Optional
from loguru import logger
from bs4 import BeautifulSoup

from .http_client import HTTPClient
from .config import ENABLE_PROXY_REFRESH


class CommitScraper:
    """Scraper for extracting commit information from GitHub repositories."""
    
    def __init__(self, http_client: HTTPClient = None):
        self.http_client = http_client or HTTPClient(enable_proxy_refresh=ENABLE_PROXY_REFRESH)
    
    def scrape_repository_commits(self, repo_url: str, max_commits: int = 1000) -> List[str]:
        """Scrape commit IDs from a repository's commit history.
        
        Args:
            repo_url: Repository URL (e.g., https://github.com/owner/repo)
            max_commits: Maximum number of commits to scrape
            
        Returns:
            List of commit SHA IDs
        """
        try:
            commit_ids = []
            page = 1
            max_pages = max_commits // 35 + 1  # GitHub shows ~35 commits per page
            
            logger.info(f"Scraping repository commits for {repo_url} (max: {max_commits})")
            
            while len(commit_ids) < max_commits and page <= max_pages:
                commits_url = f"{repo_url}/commits"
                if page > 1:
                    commits_url += f"?page={page}"
                
                logger.debug(f"Fetching commits page {page}: {commits_url}")
                
                response = self.http_client.get(commits_url)
                if not response or response.status_code != 200:
                    logger.warning(f"Failed to fetch commits page {page} for {repo_url}")
                    break
                
                soup = BeautifulSoup(response.text, 'lxml')
                
                # Find commit links - they have the pattern /owner/repo/commit/sha
                commit_links = soup.find_all('a', href=re.compile(r'/commit/[a-f0-9]{40}'))

                page_commits = []
                page_commits_set = set()  # Track commits seen on this page
                for link in commit_links:
                    href = link.get('href', '')
                    # Extract SHA from URL like /owner/repo/commit/sha
                    match = re.search(r'/commit/([a-f0-9]{40})', href)
                    if match:
                        commit_sha = match.group(1)
                        # Avoid duplicates both globally and within this page
                        if commit_sha not in commit_ids and commit_sha not in page_commits_set:
                            page_commits.append(commit_sha)
                            page_commits_set.add(commit_sha)
                
                if not page_commits:
                    logger.info(f"No more commits found on page {page}, stopping")
                    break
                
                commit_ids.extend(page_commits)
                logger.debug(f"Found {len(page_commits)} commits on page {page}")
                
                # Stop if we've reached the limit
                if len(commit_ids) >= max_commits:
                    commit_ids = commit_ids[:max_commits]
                    break
                
                page += 1
            
            logger.info(f"Scraped {len(commit_ids)} commit IDs for {repo_url}")
            return commit_ids
            
        except Exception as e:
            logger.error(f"Error scraping repository commits for {repo_url}: {e}")
            return []
    
    def scrape_pr_commits(self, pr_url: str) -> List[str]:
        """Scrape the merge commit ID for a specific pull request.

        This method extracts the merge commit ID that represents when the PR was
        merged into the main branch, not the feature branch commits.

        Note: Only merged PRs will have merge commit IDs. Closed (but not merged) PRs
        are still valid and counted toward requirements, but will return empty list
        since they don't have merge commits.

        Args:
            pr_url: Pull request URL (e.g., https://github.com/owner/repo/pull/123)

        Returns:
            List containing the merge commit SHA ID (or empty if PR is closed but not merged)
        """
        try:
            logger.debug(f"Scraping PR merge commit for {pr_url}")

            response = self.http_client.get(pr_url)
            if not response or response.status_code != 200:
                logger.warning(f"Failed to fetch PR page for {pr_url}")
                return []

            soup = BeautifulSoup(response.text, 'lxml')

            # Look for the merge commit ID in the PR page
            merge_commit_id = self._extract_merge_commit_id(soup)

            if merge_commit_id:
                logger.debug(f"Found merge commit {merge_commit_id} for PR {pr_url}")
                return [merge_commit_id]
            else:
                # Check if PR is actually merged before warning
                if self._is_pr_merged(soup):
                    logger.debug(f"No merge commit found for merged PR {pr_url} - may use different merge strategy")
                else:
                    logger.debug(f"PR {pr_url} is closed but not merged - no merge commit expected")
                return []

        except Exception as e:
            logger.error(f"Error scraping PR merge commit for {pr_url}: {e}")
            return []

    def _extract_merge_commit_id(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract the merge commit ID from a PR page.

        Args:
            soup: BeautifulSoup object of the PR page

        Returns:
            The merge commit SHA ID if found, None otherwise
        """
        try:
            # Look for merge commit information in various places

            # Method 1: Look for "merged commit <sha> into" text pattern
            page_text = soup.get_text()

            # Try multiple merge patterns
            merge_patterns = [
                r'merged commit ([a-f0-9]{7,40}) into',
                r'merged ([a-f0-9]{7,40}) into',
                r'merge commit ([a-f0-9]{7,40})',
                r'commit ([a-f0-9]{7,40}) was merged',
                r'merged pull request.*?([a-f0-9]{7,40})'
            ]

            for pattern in merge_patterns:
                match = re.search(pattern, page_text, re.IGNORECASE)
                if match:
                    short_sha = match.group(1)
                    # If it's a short SHA, try to find the full SHA
                    if len(short_sha) < 40:
                        full_sha = self._find_full_sha_from_short(soup, short_sha)
                        if full_sha:
                            return full_sha
                    elif len(short_sha) == 40:
                        return short_sha

            # Method 2: Look for merge commit links in the timeline
            # GitHub often shows merge commits with specific patterns
            all_links = soup.find_all('a', href=True)
            for link in all_links:
                href = link.get('href', '')

                # Look for commit links that are likely merge commits
                commit_match = re.search(r'/commit/([a-f0-9]{40})(?:/|$|\?|#)', href)
                if commit_match:
                    commit_sha = commit_match.group(1)

                    # Check if this looks like a merge commit based on context
                    if self._is_likely_merge_commit_link(link):
                        return commit_sha

            # Method 3: Look for merge event in the timeline
            # Find elements that indicate a merge event
            merge_elements = soup.find_all(string=re.compile(r'merged.*into.*main|merged.*into.*master', re.IGNORECASE))
            for element in merge_elements:
                parent = element.parent
                if parent:
                    # Look for commit SHA near the merge text
                    parent_text = parent.get_text()
                    sha_match = re.search(r'\b([a-f0-9]{7,40})\b', parent_text)
                    if sha_match:
                        sha = sha_match.group(1)
                        if len(sha) == 40:
                            return sha
                        elif len(sha) >= 7:
                            # Try to find full SHA
                            full_sha = self._find_full_sha_from_short(soup, sha)
                            if full_sha:
                                return full_sha

            return None

        except Exception as e:
            logger.debug(f"Error extracting merge commit ID: {e}")
            return None

    def _find_full_sha_from_short(self, soup: BeautifulSoup, short_sha: str) -> Optional[str]:
        """Try to find the full 40-character SHA from a short SHA.

        Args:
            soup: BeautifulSoup object of the PR page
            short_sha: Short SHA (7+ characters)

        Returns:
            Full 40-character SHA if found, None otherwise
        """
        try:
            # Look for any 40-character SHA that starts with the short SHA
            all_links = soup.find_all('a', href=True)
            for link in all_links:
                href = link.get('href', '')
                commit_match = re.search(r'/commit/([a-f0-9]{40})(?:/|$|\?|#)', href)
                if commit_match:
                    full_sha = commit_match.group(1)
                    if full_sha.startswith(short_sha):
                        return full_sha

            return None

        except Exception as e:
            logger.debug(f"Error finding full SHA from short SHA {short_sha}: {e}")
            return None

    def _is_likely_merge_commit_link(self, link) -> bool:
        """Check if a commit link is likely a merge commit.

        Args:
            link: BeautifulSoup link element

        Returns:
            True if the link is likely a merge commit, False otherwise
        """
        try:
            # Check the context around the link
            link_text = link.get_text(strip=True)

            # Look for merge-related keywords in the link text or nearby text
            merge_keywords = ['merged', 'merge', 'into main', 'into master']

            # Check the link text itself
            for keyword in merge_keywords:
                if keyword in link_text.lower():
                    return True

            # Check parent elements for merge context
            parent = link.parent
            if parent:
                parent_text = parent.get_text().lower()
                for keyword in merge_keywords:
                    if keyword in parent_text:
                        return True

            # Check if the link is in a timeline event that mentions merging
            timeline_element = link.find_parent(attrs={'class': re.compile(r'timeline|merge')})
            if timeline_element:
                timeline_text = timeline_element.get_text().lower()
                for keyword in merge_keywords:
                    if keyword in timeline_text:
                        return True

            return False

        except Exception as e:
            logger.debug(f"Error checking if link is merge commit: {e}")
            return False

    def _is_pr_merged(self, soup: BeautifulSoup) -> bool:
        """Check if a PR has been merged (not just closed) by looking for specific merge indicators.

        This distinguishes between:
        - Merged PRs: PRs that were merged into the main branch (should have commit_id)
        - Closed PRs: PRs that were closed without merging (no commit_id needed)
        """
        try:
            # Method 1: Look for specific merge state indicators in CSS classes
            merge_state_selectors = [
                ".State--merged",
                "[data-hovercard-type='pull_request'] .State--merged",
                ".gh-header-meta .State--merged"
            ]

            for selector in merge_state_selectors:
                merge_elem = soup.select_one(selector)
                if merge_elem:
                    state_text = merge_elem.get_text(strip=True).lower()
                    if "merged" in state_text:
                        return True

            # Method 2: Look for merge timeline events (more specific than just text search)
            # GitHub shows merge events in the timeline with specific patterns
            timeline_elements = soup.find_all(attrs={'class': re.compile(r'timeline|merge')})
            for element in timeline_elements:
                element_text = element.get_text().lower()
                # Look for specific merge patterns, not just the word "merged"
                if re.search(r'merged.*into.*main|merged.*into.*master|merged.*pull request', element_text):
                    return True

            # Method 3: Look for merge commit references
            # If there are merge commit patterns, it's likely merged
            page_text = soup.get_text()
            merge_commit_patterns = [
                r'merged commit [a-f0-9]{7,40} into',
                r'merge commit [a-f0-9]{7,40}',
                r'commit [a-f0-9]{7,40} was merged'
            ]

            for pattern in merge_commit_patterns:
                if re.search(pattern, page_text, re.IGNORECASE):
                    return True

            return False

        except Exception as e:
            logger.debug(f"Error checking if PR is merged: {e}")
            return False
    
    def _extract_commit_refs_from_text(self, text: str) -> List[str]:
        """Extract commit references from text content.
        
        Args:
            text: Text content to search for commit references
            
        Returns:
            List of commit SHA IDs found in the text
        """
        commit_refs = []
        
        # Look for full SHA (40 characters) only - we want consistency
        full_sha_pattern = r'\b[a-f0-9]{40}\b'
        full_matches = re.findall(full_sha_pattern, text, re.IGNORECASE)
        commit_refs.extend(full_matches)

        # Note: We intentionally don't extract short SHAs to maintain consistency
        # with the repository commit scraping which only uses full 40-char hashes
        
        return commit_refs
    
    def get_commit_details(self, repo_url: str, commit_sha: str) -> Optional[dict]:
        """Get detailed information about a specific commit.
        
        Args:
            repo_url: Repository URL
            commit_sha: Commit SHA ID
            
        Returns:
            Dictionary with commit details or None if not found
        """
        try:
            commit_url = f"{repo_url}/commit/{commit_sha}"
            
            response = self.http_client.get(commit_url)
            if not response or response.status_code != 200:
                logger.warning(f"Failed to fetch commit details for {commit_sha}")
                return None
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # Extract commit information
            commit_info = {
                'sha': commit_sha,
                'url': commit_url
            }
            
            # Try to extract commit message
            commit_message_elem = soup.find('div', class_='commit-message')
            if commit_message_elem:
                commit_info['message'] = commit_message_elem.get_text(strip=True)
            
            # Try to extract author information
            author_elem = soup.find('a', class_='commit-author')
            if author_elem:
                commit_info['author'] = author_elem.get_text(strip=True)
            
            # Try to extract commit date
            time_elem = soup.find('relative-time')
            if time_elem:
                commit_info['date'] = time_elem.get('datetime')
            
            return commit_info
            
        except Exception as e:
            logger.error(f"Error getting commit details for {commit_sha}: {e}")
            return None
    
    def validate_commit_sha(self, sha: str) -> bool:
        """Validate if a string is a valid commit SHA.
        
        Args:
            sha: String to validate
            
        Returns:
            True if valid SHA, False otherwise
        """
        if not sha:
            return False
        
        # Check if it's a valid hex string of appropriate length
        if not re.match(r'^[a-f0-9]+$', sha, re.IGNORECASE):
            return False
        
        # GitHub commit SHAs are 40 characters, but we accept shorter ones too
        return 7 <= len(sha) <= 40
