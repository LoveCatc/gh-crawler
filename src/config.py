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
RATE_LIMIT_DELAY = 1  # seconds between requests

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
