"""Data models for the GitHub crawler."""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class InputRepository:
    """Repository data from input JSON file."""
    url: str
    stars: int
    language: List[str]


@dataclass_json
@dataclass
class InputSummary:
    """Summary data from input JSON file."""
    total_repositories: int
    total_stars: int
    average_stars: float
    top_repository: Dict[str, Any]


@dataclass_json
@dataclass
class InputData:
    """Complete input data structure."""
    language: str
    summary: InputSummary
    repositories: List[InputRepository]


@dataclass_json
@dataclass
class Comment:
    """Represents a comment in a PR or issue."""
    author: str
    timestamp: str  # ISO format timestamp
    content: str


@dataclass_json
@dataclass
class IssueInfo:
    """Detailed information about an issue."""
    number: int
    title: str
    state: str  # open, closed
    author: str  # User who opened the issue
    created_at: str  # ISO format timestamp
    updated_at: str  # ISO format timestamp
    tags: List[str] = field(default_factory=list)
    comments: List[Comment] = field(default_factory=list)
    url: str = ""


@dataclass_json
@dataclass
class PullRequestInfo:
    """Detailed information about a pull request."""
    number: int
    title: str
    state: str  # open, closed, merged
    author: str  # User who opened the PR
    created_at: str  # ISO format timestamp
    updated_at: str  # ISO format timestamp
    tags: List[str] = field(default_factory=list)
    comments: List[Comment] = field(default_factory=list)
    related_issues: List[IssueInfo] = field(default_factory=list)  # Full issue content
    url: str = ""
    commit_ids: List[str] = field(default_factory=list)  # Commit IDs associated with this PR
    commit_id: str = ""  # Primary commit ID for this PR
    previous_commit_id: str = ""  # Previous commit ID in repository history


@dataclass_json
@dataclass
class RepositoryStats:
    """Statistics for a repository."""
    contributors_count: int = 0
    forks_count: int = 0
    total_issues: int = 0
    open_issues: int = 0
    closed_issues: int = 0
    total_pull_requests: int = 0
    open_pull_requests: int = 0
    closed_pull_requests: int = 0


@dataclass_json
@dataclass
class CrawledRepository:
    """Complete crawled repository information."""
    # Original data
    url: str
    stars: int
    language: List[str]

    # Crawled statistics
    stats: RepositoryStats

    # Detailed PR information
    pull_requests: List[PullRequestInfo] = field(default_factory=list)

    # Repository commit information
    commit_ids: List[str] = field(default_factory=list)  # Repository commit IDs

    # Metadata
    crawl_timestamp: Optional[str] = None
    crawl_success: bool = True
    error_message: Optional[str] = None


@dataclass_json
@dataclass
class CrawlResult:
    """Result of crawling operation."""
    success: bool
    repository: Optional[CrawledRepository] = None
    error: Optional[str] = None
    retry_count: int = 0
