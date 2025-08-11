"""Issue validation and deduplication utilities."""

import re
from typing import Dict, List, Set
from loguru import logger


class IssueValidator:
    """Validates and deduplicates issue numbers to avoid redundant requests."""
    
    def __init__(self):
        # Track scraped issues per repository to avoid duplicates
        self.scraped_issues: Dict[str, Set[int]] = {}
        
        # Track issues currently being processed to avoid concurrent requests
        self.processing_issues: Dict[str, Set[int]] = {}
    
    def validate_issue_number(self, repo_url: str, issue_number: int) -> bool:
        """Validate if an issue number is worth attempting to scrape."""
        
        # Basic range validation
        if issue_number <= 0:
            logger.debug(f"Skipping invalid issue number: {issue_number}")
            return False
        
        # Skip extremely high numbers that are likely PR numbers or invalid
        if issue_number > 100000:
            logger.debug(f"Skipping issue #{issue_number} - too high (likely PR number)")
            return False
        
        # Check if already scraped
        if self.is_already_scraped(repo_url, issue_number):
            logger.debug(f"Skipping issue #{issue_number} - already scraped")
            return False
        
        # Check if currently being processed
        if self.is_being_processed(repo_url, issue_number):
            logger.debug(f"Skipping issue #{issue_number} - currently being processed")
            return False
        
        return True
    
    def is_already_scraped(self, repo_url: str, issue_number: int) -> bool:
        """Check if an issue has already been scraped."""
        return issue_number in self.scraped_issues.get(repo_url, set())
    
    def is_being_processed(self, repo_url: str, issue_number: int) -> bool:
        """Check if an issue is currently being processed."""
        return issue_number in self.processing_issues.get(repo_url, set())
    
    def mark_processing(self, repo_url: str, issue_number: int) -> None:
        """Mark an issue as currently being processed."""
        if repo_url not in self.processing_issues:
            self.processing_issues[repo_url] = set()
        self.processing_issues[repo_url].add(issue_number)
    
    def mark_completed(self, repo_url: str, issue_number: int, success: bool = True) -> None:
        """Mark an issue as completed processing."""
        # Remove from processing set
        if repo_url in self.processing_issues:
            self.processing_issues[repo_url].discard(issue_number)
        
        # Add to scraped set if successful
        if success:
            if repo_url not in self.scraped_issues:
                self.scraped_issues[repo_url] = set()
            self.scraped_issues[repo_url].add(issue_number)
    
    def deduplicate_issue_list(self, repo_url: str, issue_numbers: List[int]) -> List[int]:
        """Remove duplicates and already-processed issues from a list."""
        unique_issues = []
        seen = set()
        
        for issue_num in issue_numbers:
            if issue_num in seen:
                continue
            
            if not self.validate_issue_number(repo_url, issue_num):
                continue
            
            seen.add(issue_num)
            unique_issues.append(issue_num)
        
        logger.debug(f"Deduplicated {len(issue_numbers)} issues to {len(unique_issues)} unique valid issues")
        return unique_issues
    
    def get_stats(self, repo_url: str) -> Dict:
        """Get statistics for a repository."""
        return {
            'scraped_issues': len(self.scraped_issues.get(repo_url, set())),
            'processing_issues': len(self.processing_issues.get(repo_url, set())),
        }
    
    def clear_repository(self, repo_url: str) -> None:
        """Clear all data for a repository."""
        self.scraped_issues.pop(repo_url, None)
        self.processing_issues.pop(repo_url, None)


class ImprovedIssueExtractor:
    """Improved issue reference extraction with better patterns."""
    
    def __init__(self):
        # More precise patterns to reduce false positives
        self.issue_patterns = [
            # Direct issue references with keywords
            r"(?:fixes?|closes?|resolves?|addresses?)\s+(?:issue\s+)?#(\d+)(?!\s*in\s+[\w-]+/[\w-]+)",
            r"(?:fix|close|resolve|address)\s+(?:issue\s+)?#(\d+)(?!\s*in\s+[\w-]+/[\w-]+)",
            
            # Issue-specific keywords
            r"(?:issue|bug|problem)\s+#(\d+)(?!\s*in\s+[\w-]+/[\w-]+)",
            
            # Related/duplicate references
            r"(?:see|related\s+to|duplicate\s+of|duplicates?)\s+(?:issue\s+)?#(\d+)(?!\s*in\s+[\w-]+/[\w-]+)",
            
            # GitHub auto-linking patterns (more conservative)
            r"(?:^|\s)#(\d+)(?:\s|$|[^\w/])",  # Standalone #number
        ]
        
        # Patterns to exclude (cross-repo references)
        self.exclude_patterns = [
            r"[\w-]+/[\w-]+#\d+",  # user/repo#123
            r"#\d+\s+in\s+[\w-]+/[\w-]+",  # #123 in user/repo
        ]
    
    def extract_issue_numbers(self, text: str) -> List[int]:
        """Extract issue numbers from text using improved patterns."""
        issue_numbers = []
        
        # First check if text contains cross-repo references to exclude
        for exclude_pattern in self.exclude_patterns:
            if re.search(exclude_pattern, text, re.IGNORECASE):
                logger.debug("Text contains cross-repo references, being more conservative")
                # Use only the most specific patterns
                patterns = self.issue_patterns[:3]  # Only keyword-based patterns
                break
        else:
            patterns = self.issue_patterns
        
        # Extract issue numbers using patterns
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE | re.MULTILINE)
            for match in matches:
                try:
                    issue_num = int(match)
                    if issue_num not in issue_numbers:
                        issue_numbers.append(issue_num)
                except ValueError:
                    continue
        
        return issue_numbers
    
    def extract_from_soup(self, soup) -> List[int]:
        """Extract issue numbers from BeautifulSoup object."""
        issue_numbers = []
        
        # 1. Find direct issue links in href attributes
        issue_links = soup.find_all('a', href=re.compile(r'/issues/(\d+)'))
        for link in issue_links:
            href = link.get('href', '')
            match = re.search(r'/issues/(\d+)', href)
            if match:
                issue_num = int(match.group(1))
                if issue_num not in issue_numbers:
                    issue_numbers.append(issue_num)
        
        # 2. Extract from text content
        text_content = soup.get_text()
        text_issues = self.extract_issue_numbers(text_content)
        
        # Combine and deduplicate
        for issue_num in text_issues:
            if issue_num not in issue_numbers:
                issue_numbers.append(issue_num)
        
        return issue_numbers[:10]  # Limit to first 10 to avoid excessive requests
