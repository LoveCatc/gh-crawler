"""HTTP client with proxy support and retry logic."""

import time
from typing import Any, Dict, Optional

import requests
from bs4 import BeautifulSoup
from loguru import logger
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# Import for SOCKS5 support
try:
    # Try to import PySocks for SOCKS5 support
    import socks
    import socket
    SOCKS_AVAILABLE = True
except ImportError:
    SOCKS_AVAILABLE = False

from .config import (
    BACKOFF_FACTOR,
    DEFAULT_HEADERS,
    MAX_RETRIES,
    PROXIES,
    PROXY_TYPE,
    PROXY_HOST,
    PROXY_PORT,
    PROXY_USERNAME,
    PROXY_PASSWORD,
    RATE_LIMIT_DELAY,
    REQUEST_TIMEOUT,
    RETRY_DELAY,
)


class HTTPClient:
    """HTTP client with proxy support, retry logic, and rate limiting."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

        # Configure proxy based on type
        if PROXY_TYPE.lower() == "socks5":
            self._configure_socks5_proxy()
        else:
            # HTTP proxy (legacy mode)
            self.session.proxies.update(PROXIES)

        self.last_request_time = 0

    def _configure_socks5_proxy(self):
        """Configure SOCKS5 proxy with authentication support."""
        if not SOCKS_AVAILABLE:
            raise ImportError(
                "PySocks is required for SOCKS5 proxy support. "
                "Install it with: pip install requests[socks] or pip install PySocks"
            )

        if not PROXY_HOST:
            raise ValueError("PROXY_HOST must be set when using SOCKS5 proxy")

        # Set up SOCKS5 proxy
        if PROXY_USERNAME and PROXY_PASSWORD:
            # SOCKS5 with authentication
            proxy_url = f"socks5://{PROXY_USERNAME}:{PROXY_PASSWORD}@{PROXY_HOST}:{PROXY_PORT}"
        else:
            # SOCKS5 without authentication
            proxy_url = f"socks5://{PROXY_HOST}:{PROXY_PORT}"

        self.session.proxies.update({
            "http": proxy_url,
            "https": proxy_url
        })

        logger.info(f"Configured SOCKS5 proxy: {PROXY_HOST}:{PROXY_PORT} (auth: {'yes' if PROXY_USERNAME else 'no'})")

    def _rate_limit(self):
        """Implement rate limiting between requests."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < RATE_LIMIT_DELAY:
            sleep_time = RATE_LIMIT_DELAY - time_since_last
            logger.debug(
                f"Rate limiting: sleeping for {sleep_time:.2f} seconds"
            )
            time.sleep(sleep_time)
        self.last_request_time = time.time()

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=RETRY_DELAY, max=60),
        retry=retry_if_exception_type(
            (requests.RequestException, requests.Timeout)
        ),
    )
    def get(self, url: str, **kwargs) -> requests.Response:
        """Make a GET request with retry logic."""
        self._rate_limit()

        logger.debug(f"Making GET request to: {url}")

        try:
            response = self.session.get(url, timeout=REQUEST_TIMEOUT, **kwargs)
            response.raise_for_status()
            logger.debug(
                f"Successfully fetched: {url} (status: {response.status_code})"
            )
            return response

        except requests.RequestException as e:
            logger.warning(f"Request failed for {url}: {e}")
            raise

    def get_soup(self, url: str, **kwargs) -> Optional[BeautifulSoup]:
        """Get BeautifulSoup object from URL."""
        try:
            response = self.get(url, **kwargs)
            soup = BeautifulSoup(response.content, "html.parser")
            return soup
        except Exception as e:
            logger.error(f"Failed to get soup for {url}: {e}")
            return None

    def close(self):
        """Close the session."""
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """Close the session."""
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
