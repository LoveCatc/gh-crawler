"""Configuration settings for the GitHub crawler."""

import os
from typing import Dict, Any

# Proxy configuration
PROXY_URL = "http://127.0.0.1:7892"
PROXIES = {
    "http": PROXY_URL,
    "https": PROXY_URL
}

# Request configuration
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds
BACKOFF_FACTOR = 2

# Concurrency configuration
MAX_WORKERS = 10
RATE_LIMIT_DELAY = 0.1  # seconds between requests (aggressive mode for dynamic proxy)

# PR Crawling Configuration
MAX_CLOSED_PRS_TO_CRAWL = 1000  # Default maximum number of closed PRs to crawl (latest first)
CRAWL_OPEN_PRS = False  # Whether to crawl open PRs (disabled per user request)
CRAWL_CLOSED_PRS = True  # Whether to crawl closed PRs

# Per-Repository PR Limits (optional - overrides default)
# Format: "repository_url": max_closed_prs
REPOSITORY_PR_LIMITS = {
    # Examples:
    # "https://github.com/apache/tvm": 3000,           # Large active project
    # "https://github.com/facebook/react": 1000,      # Very active, recent PRs most important
    # "https://github.com/microsoft/vscode": 5000,    # Huge project, need more history
    # "https://github.com/small/project": 500,        # Small project, fewer PRs needed
}

# Logging configuration
LOG_LEVEL = "INFO"
LOG_FILE = "crawler.log"
LOG_FORMAT = "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"

# GitHub URLs
GITHUB_BASE_URL = "https://github.com"

# User agent to simulate normal browser requests
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

# Default headers for requests
DEFAULT_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# Unified cache configuration
CACHE_DIR = "cache"
CACHE_PR_SUBDIR = "prs"
CACHE_CHECKPOINT_SUBDIR = "checkpoints"
CACHE_COMMITS_SUBDIR = "commits"

# Legacy checkpoint configuration (deprecated)
CHECKPOINT_DIR = "checkpoints"
CHECKPOINT_DB_FILE = "crawled_repositories.json"
MAX_CHECKPOINT_AGE_DAYS = 30  # Re-crawl repositories older than this

# Minimum PR requirements
MIN_PRS_REQUIRED = 1000  # Target number of closed PRs to crawl: min(1000, num_all_closed_PRs)
