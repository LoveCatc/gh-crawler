"""Configuration settings for the GitHub crawler."""

import os
from typing import Dict

# Proxy configuration
# Set PROXY_TYPE to 'http' for localhost non-auth proxy, 'socks5' for remote auth proxy, or 'none' to disable proxy
PROXY_TYPE = os.getenv("PROXY_TYPE", "http")  # 'http', 'socks5', or 'none'

# For localhost HTTP proxy (legacy mode)
PROXY_URL = os.getenv("PROXY_URL", "http://127.0.0.1:7892")

# For remote SOCKS5 proxy with authentication
PROXY_HOST = os.getenv("PROXY_HOST", "")  # e.g., "proxy.example.com"
PROXY_PORT = int(os.getenv("PROXY_PORT", "1080"))  # SOCKS5 default port
PROXY_USERNAME = os.getenv("PROXY_USERNAME", "")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD", "")

# Tunnel proxy configuration (for proxies that provide new IP:port per connection)
ENABLE_PROXY_REFRESH = bool(os.getenv("ENABLE_PROXY_REFRESH", "False"))  # Enable tunnel proxy mode
PROXY_REFRESH_INTERVAL = int(os.getenv("PROXY_REFRESH_INTERVAL", "10"))  # Refresh every N requests

# Build proxy configuration based on type
def _build_proxy_config() -> Dict[str, str]:
    """Build proxy configuration based on proxy type."""
    if PROXY_TYPE.lower() == "none":
        # No proxy
        return {}
    elif PROXY_TYPE.lower() == "socks5":
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

# Commit scraping configuration
# Limit how many repository commits to scrape for context/reference
MAX_COMMITS_TO_SCRAPE = 500
ENABLE_COMMIT_SCRAPING = bool(os.getenv("ENABLE_COMMIT_SCRAPING", "True"))  # Enable/disable commit scraping

# Concurrency configuration
CPU_COUNT = os.cpu_count() or 4

# Optimized worker configuration for maximum CPU utilization
# For I/O bound operations like web scraping, we can use many more threads than CPU cores
DEFAULT_MAX_WORKERS = max(32, CPU_COUNT * 8)  # Increased from 4x to 8x CPU cores
MAX_WORKERS = int(os.getenv("MAX_WORKERS", str(DEFAULT_MAX_WORKERS)))

# Repository-level parallelism (process-based for CPU-intensive operations)
DEFAULT_REPO_WORKERS = max(2, CPU_COUNT)  # Use half the cores for process-level parallelism
REPO_WORKERS = int(os.getenv("REPO_WORKERS", str(DEFAULT_REPO_WORKERS)))

# Multiprocessing threshold (minimum repositories to justify multiprocessing overhead)
DEFAULT_MULTIPROCESS_THRESHOLD = max(DEFAULT_REPO_WORKERS * 2, 6)
MULTIPROCESS_THRESHOLD = int(os.getenv("MULTIPROCESS_THRESHOLD", str(DEFAULT_MULTIPROCESS_THRESHOLD)))

# Aggressive rate limiting for better throughput
RATE_LIMIT_DELAY = float(os.getenv("RATE_LIMIT_DELAY", "0.1"))  # Reduced from 0.5 to 0.1 seconds

# Discovery workers for URL discovery phase
DEFAULT_DISCOVERY_WORKERS = max(8, CPU_COUNT * 2)  # Dedicated workers for URL discovery
DISCOVERY_WORKERS = int(os.getenv("DISCOVERY_WORKERS", str(DEFAULT_DISCOVERY_WORKERS)))

# PR Crawling Configuration
MAX_CLOSED_PRS_TO_CRAWL = 500  # Default maximum number of closed PRs to crawl (latest first)
CRAWL_OPEN_PRS = False  # Whether to crawl open PRs (disabled per user request)
CRAWL_CLOSED_PRS = True  # Whether to crawl closed PRs
# Fallbacks for estimates when stats partially missing
MAX_PRS_FALLBACK = 500  # used only when stats are missing; not a hard limit

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
MIN_PRS_REQUIRED = 500  # Target number of closed PRs to crawl: min(1000, num_all_closed_PRs)

# Performance monitoring configuration
ENABLE_PERFORMANCE_MONITORING = bool(os.getenv("ENABLE_PERFORMANCE_MONITORING", "True"))
PERFORMANCE_LOG_INTERVAL = int(os.getenv("PERFORMANCE_LOG_INTERVAL", "60"))  # seconds (increased from 30)
CPU_USAGE_THRESHOLD = float(os.getenv("CPU_USAGE_THRESHOLD", "80.0"))  # percentage
QUIET_MODE = bool(os.getenv("QUIET_MODE", "False"))  # Reduce verbose logging

# Batch processing configuration for better throughput
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))  # Number of items to process in each batch
CACHE_FLUSH_BATCH_SIZE = int(os.getenv("CACHE_FLUSH_BATCH_SIZE", "20"))  # Increased from 10
CACHE_FLUSH_INTERVAL = int(os.getenv("CACHE_FLUSH_INTERVAL", "15"))  # Reduced from 30 seconds
