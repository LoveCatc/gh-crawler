"""Configuration settings for the GitHub crawler."""

import os
from typing import Dict

# Proxy configuration
# Set PROXY_TYPE to 'http' for localhost non-auth proxy or 'socks5' for remote auth proxy
PROXY_TYPE = os.getenv("PROXY_TYPE", "http")  # 'http' or 'socks5'

# For localhost HTTP proxy (legacy mode)
PROXY_URL = os.getenv("PROXY_URL", "http://127.0.0.1:7892")

# For remote SOCKS5 proxy with authentication
PROXY_HOST = os.getenv("PROXY_HOST", "")  # e.g., "proxy.example.com"
PROXY_PORT = int(os.getenv("PROXY_PORT", "1080"))  # SOCKS5 default port
PROXY_USERNAME = os.getenv("PROXY_USERNAME", "")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD", "")

# Build proxy configuration based on type
def _build_proxy_config() -> Dict[str, str]:
    """Build proxy configuration based on proxy type."""
    if PROXY_TYPE.lower() == "socks5":
        if not PROXY_HOST:
            raise ValueError("PROXY_HOST must be set when using SOCKS5 proxy")

        if PROXY_USERNAME and PROXY_PASSWORD:
            # SOCKS5 with authentication
            proxy_url = f"socks5://{PROXY_USERNAME}:{PROXY_PASSWORD}@{PROXY_HOST}:{PROXY_PORT}"
        else:
            # SOCKS5 without authentication
            proxy_url = f"socks5://{PROXY_HOST}:{PROXY_PORT}"

        return {
            "http": proxy_url,
            "https": proxy_url
        }
    else:
        # HTTP proxy (localhost, legacy mode)
        return {
            "http": PROXY_URL,
            "https": PROXY_URL
        }

PROXIES = _build_proxy_config()

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
