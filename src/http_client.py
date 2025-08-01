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

from .config import (
    BACKOFF_FACTOR,
    DEFAULT_HEADERS,
    MAX_RETRIES,
    PROXIES,
    RATE_LIMIT_DELAY,
    REQUEST_TIMEOUT,
    RETRY_DELAY,
)


class HTTPClient:
    """HTTP client with proxy support, retry logic, and rate limiting."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.session.proxies.update(PROXIES)
        self.last_request_time = 0

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
