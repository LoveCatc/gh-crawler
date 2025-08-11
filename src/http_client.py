"""HTTP client with proxy support and retry logic."""

import os
import time
from typing import Any, Dict, Optional

import requests
from bs4 import BeautifulSoup
from loguru import logger
from tenacity import (
    retry,
    retry_if_exception_type,
    retry_if_not_exception_type,
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

    # Class variable to track if proxy configuration has been logged
    _proxy_logged = False

    def __init__(self, rate_limit_delay: Optional[float] = None, enable_proxy_refresh: bool = False):
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

        # Proxy refresh configuration for tunnel proxies
        self.enable_proxy_refresh = enable_proxy_refresh
        self.request_count = 0
        self.proxy_refresh_interval = int(os.getenv("PROXY_REFRESH_INTERVAL", "10"))  # Refresh every N requests

        # Configure proxy based on type (only log once)
        if PROXY_TYPE.lower() == "none":
            if not HTTPClient._proxy_logged:
                logger.info("No proxy configured - using direct connection")
                HTTPClient._proxy_logged = True
        elif PROXY_TYPE.lower() == "socks5":
            self._configure_socks5_proxy()
        else:
            # HTTP proxy (legacy mode)
            self._configure_http_proxy()

        self.last_request_time = 0
        # Allow per-client override of rate limit delay
        self._rate_limit_delay = RATE_LIMIT_DELAY if rate_limit_delay is None else rate_limit_delay

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

        if not HTTPClient._proxy_logged:
            logger.info(f"Configured SOCKS5 proxy: {PROXY_HOST}:{PROXY_PORT} (auth: {'yes' if PROXY_USERNAME else 'no'})")
            HTTPClient._proxy_logged = True

    def _configure_http_proxy(self):
        """Configure HTTP proxy with optional refresh support for tunnel proxies."""
        self.session.proxies.update(PROXIES)

        if not HTTPClient._proxy_logged:
            if self.enable_proxy_refresh:
                logger.info(f"Configured HTTP tunnel proxy with refresh every {self.proxy_refresh_interval} requests: {PROXIES.get('http', 'N/A')}")
            else:
                logger.info(f"Configured HTTP proxy: {PROXIES.get('http', 'N/A')}")
            HTTPClient._proxy_logged = True

    def _refresh_proxy_connection(self):
        """Refresh proxy connection for tunnel proxies that provide new IP:port per connection."""
        if not self.enable_proxy_refresh:
            return

        try:
            # Close existing session to force new connection
            self.session.close()

            # Create new session with fresh proxy connection
            self.session = requests.Session()
            self.session.headers.update(DEFAULT_HEADERS)

            # Reconfigure proxy based on type
            if PROXY_TYPE.lower() == "socks5":
                # Reconfigure SOCKS5 without logging
                if PROXY_USERNAME and PROXY_PASSWORD:
                    proxy_url = f"socks5://{PROXY_USERNAME}:{PROXY_PASSWORD}@{PROXY_HOST}:{PROXY_PORT}"
                else:
                    proxy_url = f"socks5://{PROXY_HOST}:{PROXY_PORT}"
                self.session.proxies.update({"http": proxy_url, "https": proxy_url})
                logger.debug("Refreshed SOCKS5 proxy connection for new tunnel IP")
            elif PROXY_TYPE.lower() == "http":
                self.session.proxies.update(PROXIES)
                logger.debug("Refreshed HTTP proxy connection for new tunnel IP")

        except Exception as e:
            logger.warning(f"Failed to refresh proxy connection: {e}")
            # Continue with existing session if refresh fails

    def _rate_limit(self):
        """Implement rate limiting between requests."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        delay = self._rate_limit_delay
        if time_since_last < delay:
            sleep_time = delay - time_since_last
            logger.debug(
                f"Rate limiting: sleeping for {sleep_time:.2f} seconds"
            )
            time.sleep(sleep_time)
        self.last_request_time = time.time()

    def _is_retryable_error(self, exception: Exception) -> bool:
        """Determine if an error should be retried."""
        if isinstance(exception, requests.HTTPError):
            # Don't retry client errors (4xx) - they're permanent
            if hasattr(exception, 'response') and exception.response is not None:
                status_code = exception.response.status_code
                if 400 <= status_code < 500:
                    return False
            return True

        # Retry network-related errors
        return isinstance(exception, (
            requests.Timeout,
            requests.ConnectionError,
            requests.TooManyRedirects
        ))

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=RETRY_DELAY, max=60),
        retry=retry_if_exception_type(
            (requests.Timeout, requests.ConnectionError, requests.TooManyRedirects)
        ),
    )
    def get(self, url: str, **kwargs) -> requests.Response:
        """Make a GET request with retry logic."""
        self._rate_limit()

        # Refresh proxy connection if using tunnel proxy
        if self.enable_proxy_refresh:
            self.request_count += 1
            if self.request_count % self.proxy_refresh_interval == 0:
                self._refresh_proxy_connection()

        logger.debug(f"Making GET request to: {url}")

        try:
            # Track request for performance monitoring
            try:
                from .performance_monitor import get_performance_monitor
                get_performance_monitor().increment_requests()
            except ImportError:
                pass  # Performance monitoring not available

            response = self.session.get(url, timeout=REQUEST_TIMEOUT, **kwargs)
            response.raise_for_status()
            logger.debug(
                f"Successfully fetched: {url} (status: {response.status_code})"
            )
            return response

        except requests.HTTPError as e:
            # Log different levels based on error type
            if hasattr(e, 'response') and e.response is not None:
                status_code = e.response.status_code
                if status_code == 404:
                    logger.debug(f"Resource not found (404): {url}")
                elif status_code == 403:
                    logger.warning(f"Access forbidden (403): {url}")
                elif 400 <= status_code < 500:
                    logger.warning(f"Client error ({status_code}): {url}")
                else:
                    logger.warning(f"HTTP error ({status_code}): {url}")
            else:
                logger.warning(f"HTTP error for {url}: {e}")
            raise
        except requests.RequestException as e:
            logger.warning(f"Request failed for {url}: {e}")
            raise

    def get_soup(self, url: str, **kwargs) -> Optional[BeautifulSoup]:
        """Get BeautifulSoup object from URL."""
        try:
            response = self.get(url, **kwargs)
            soup = BeautifulSoup(response.content, "lxml")
            return soup
        except requests.HTTPError as e:
            # Don't log 404s as errors - they're expected for non-existent resources
            if hasattr(e, 'response') and e.response is not None and e.response.status_code == 404:
                logger.debug(f"Resource not found: {url}")
            else:
                logger.error(f"HTTP error getting soup for {url}: {e}")
            return None
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
