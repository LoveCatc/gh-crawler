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
            # Try updated selectors for GitHub's new interface
            title_selectors = [
                'h1[class*="PageHeader_Title"]',
                'h1[class*="prc-PageHeader-Title"]',
                'h1[class*="HeaderViewer"]',
                "h1.gh-header-title .js-issue-title",
                "h1 .js-issue-title",
                ".gh-header-title",
                "h1.gh-header-title",
                ".js-issue-title"
            ]

            for selector in title_selectors:
                title_elem = soup.select_one(selector)
                if title_elem:
                    title_text = title_elem.get_text(strip=True)
                    # Remove issue number from title if present (e.g., "Title#123" -> "Title")
                    if '#' in title_text:
                        title_text = title_text.split('#')[0].strip()
                    return title_text

            # Fallback: look for any h1 that might contain the title
            h1_elements = soup.find_all("h1")
            for h1 in h1_elements:
                text = h1.get_text(strip=True)
                if text and len(text) > 5 and 'sr-only' not in h1.get('class', []):  # Reasonable title length, not screen reader only
                    if '#' in text:
                        text = text.split('#')[0].strip()
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
            seen_comments = set()  # Track unique comments to avoid duplicates

            # First, try to extract comments from JSON data in script tags (new GitHub interface)
            json_comments = self._extract_comments_from_json(soup)
            if json_comments:
                logger.debug(f"Found {len(json_comments)} comments from JSON data")
                comments.extend(json_comments)
                for comment in json_comments:
                    seen_comments.add(comment.content.strip())

            # If no JSON comments found, fall back to HTML extraction
            if not comments:
                # First, extract the issue description as the first "comment"
                issue_body = self._extract_issue_body(soup)
                if issue_body:
                    comments.append(issue_body)
                    seen_comments.add(issue_body.content.strip())

                # Look for timeline comments with updated selectors for GitHub's new interface
                comment_selectors = [
                    ".timeline-comment",
                    ".js-timeline-item",
                    "[data-testid*='comment']",
                    "[class*='timeline']",
                    "[class*='comment']"
                ]

                for selector in comment_selectors:
                    comment_elements = soup.select(selector)

                    for comment_elem in comment_elements:
                        comment = self._extract_single_comment(comment_elem)
                        if comment:
                            # Use full content for deduplication to avoid false positives
                            comment_key = comment.content.strip()

                            if comment_key not in seen_comments:
                                seen_comments.add(comment_key)
                                comments.append(comment)

            # Sort comments by timestamp to ensure chronological order
            comments.sort(key=lambda c: c.timestamp)

            return comments
        except Exception as e:
            logger.warning(f"Error extracting comments: {e}")
            return []

    def _extract_issue_body(self, soup: BeautifulSoup) -> Optional[Comment]:
        """Extract the issue description/body as the first comment."""
        try:
            # Try different selectors for the issue body content
            body_selectors = [
                '[data-testid="issue-body"]',
                '.markdown-body',
                '[class*="IssueBody"]',
                '[class*="issue-body"]',
                '.comment-body'
            ]

            for selector in body_selectors:
                body_elements = soup.select(selector)
                for body_elem in body_elements:
                    content = body_elem.get_text(strip=True)
                    if content and len(content) > 50:  # Substantial content
                        # Clean up the content by removing common GitHub UI text
                        content = self._clean_issue_content(content)

                        if len(content) > 50:  # Still substantial after cleaning
                            # Extract author from issue metadata
                            author = self._extract_author(soup)
                            created_at = self._extract_created_at(soup)

                            # Limit content length
                            if len(content) > 2000:
                                content = content[:2000] + "..."

                            return Comment(
                                author=author,
                                timestamp=created_at,
                                content=content
                            )

            return None
        except Exception as e:
            logger.warning(f"Error extracting issue body: {e}")
            return None

    def _clean_issue_content(self, content: str) -> str:
        """Clean up issue content by removing GitHub UI elements."""
        try:
            # Remove common GitHub UI text patterns
            patterns_to_remove = [
                r'^Description[a-zA-Z0-9\s]*opened\s*on\s*[a-zA-Z0-9\s,]*Issue body actions',
                r'^[a-zA-Z0-9\s]*opened\s*on\s*[a-zA-Z0-9\s,]*Issue body actions',
                r'Issue body actions',
                r'Description\s*',
                r'opened\s*on\s*[a-zA-Z0-9\s,]*'
            ]

            cleaned_content = content
            for pattern in patterns_to_remove:
                cleaned_content = re.sub(pattern, '', cleaned_content, flags=re.IGNORECASE)

            # Remove extra whitespace
            cleaned_content = re.sub(r'\s+', ' ', cleaned_content).strip()

            return cleaned_content
        except Exception as e:
            logger.warning(f"Error cleaning issue content: {e}")
            return content

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

    def _extract_comments_from_json(self, soup: BeautifulSoup) -> List[Comment]:
        """Extract comments from JSON data embedded in script tags (new GitHub interface)."""
        try:
            import json
            comments = []

            # Look for script tags that contain comment data
            script_tags = soup.find_all('script')

            for script in script_tags:
                if not script.string:
                    continue

                script_content = script.string.strip()
                if not (script_content.startswith('{') and script_content.endswith('}')):
                    continue

                try:
                    data = json.loads(script_content)

                    # Look for timeline data in various possible locations
                    timeline_data = None

                    # Check for GraphQL preloaded queries (new GitHub format)
                    if 'payload' in data and 'preloadedQueries' in data['payload']:
                        queries = data['payload']['preloadedQueries']
                        for query in queries:
                            if isinstance(query, dict) and 'result' in query:
                                result = query['result']
                                if 'data' in result and 'repository' in result['data']:
                                    repo_data = result['data']['repository']
                                    if 'issue' in repo_data and 'frontTimelineItems' in repo_data['issue']:
                                        timeline_items = repo_data['issue']['frontTimelineItems']
                                        if 'edges' in timeline_items:
                                            timeline_data = timeline_items['edges']
                                            logger.debug(f"Found GraphQL timeline data with {len(timeline_data)} items")
                                            break

                    # Check for payload.preloaded_records with timeline data (fallback)
                    if not timeline_data and 'payload' in data and 'preloaded_records' in data['payload']:
                        records = data['payload']['preloaded_records']
                        for key, value in records.items():
                            if 'timeline' in key.lower() and isinstance(value, list):
                                timeline_data = value
                                break

                    # Check for direct timeline data
                    if not timeline_data and 'timeline' in data:
                        timeline_data = data['timeline']

                    # Check for comments array
                    if not timeline_data and 'comments' in data:
                        timeline_data = data['comments']

                    # Process timeline data if found
                    if timeline_data and isinstance(timeline_data, list):
                        logger.debug(f"Found timeline data with {len(timeline_data)} items")

                        for item in timeline_data:
                            if not isinstance(item, dict):
                                continue

                            # Extract comment information
                            comment = self._extract_comment_from_json_item(item)
                            if comment:
                                comments.append(comment)

                    # Also check for structured data (schema.org format)
                    if 'structured_data' in data or '@type' in data:
                        structured_data = data.get('structured_data', data)
                        if structured_data.get('@type') == 'DiscussionForumPosting':
                            # Extract the main issue content
                            article_body = structured_data.get('articleBody', '')
                            headline = structured_data.get('headline', '')

                            if article_body:
                                # This is the main issue description
                                author_info = structured_data.get('author', {})
                                author = author_info.get('name', 'unknown') if isinstance(author_info, dict) else str(author_info)

                                comment = Comment(
                                    author=author,
                                    timestamp=structured_data.get('datePublished', datetime.now().isoformat()),
                                    content=article_body
                                )
                                comments.append(comment)

                except json.JSONDecodeError:
                    continue
                except Exception as e:
                    logger.debug(f"Error processing script tag: {e}")
                    continue

            return comments

        except Exception as e:
            logger.warning(f"Error extracting comments from JSON: {e}")
            return []

    def _extract_comment_from_json_item(self, item: dict) -> Optional[Comment]:
        """Extract a comment from a JSON timeline item."""
        try:
            # Look for different possible structures
            comment_data = None

            # Handle GraphQL edge format (GitHub's new structure)
            if 'node' in item and isinstance(item['node'], dict):
                node = item['node']
                # Only process IssueComment nodes, skip other timeline events
                if node.get('__typename') == 'IssueComment':
                    comment_data = node
            # Check if this is a direct comment item
            elif item.get('__typename') == 'IssueComment' or 'body' in item:
                comment_data = item
            elif 'comment' in item:
                comment_data = item['comment']

            if not comment_data:
                return None

            # Extract comment fields
            content = comment_data.get('body', comment_data.get('bodyText', ''))
            if not content or len(content.strip()) < 5:
                return None

            # Extract author
            author = 'unknown'
            if 'author' in comment_data:
                author_data = comment_data['author']
                if isinstance(author_data, dict):
                    author = author_data.get('login', author_data.get('name', 'unknown'))
                elif isinstance(author_data, str):
                    author = author_data

            # Extract timestamp
            timestamp = comment_data.get('createdAt', comment_data.get('created_at', datetime.now().isoformat()))

            # Limit content length
            if len(content) > 2000:
                content = content[:2000] + "..."

            return Comment(
                author=author,
                timestamp=timestamp,
                content=content
            )

        except Exception as e:
            logger.debug(f"Error extracting comment from JSON item: {e}")
            return None
