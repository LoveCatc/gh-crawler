"""Utility functions for the GitHub crawler."""

import time
import functools
from typing import Any, Callable, Optional
from loguru import logger

from .exceptions import CrawlerError


def retry_on_failure(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """Decorator for retrying functions on failure."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    
                    if attempt < max_retries:
                        wait_time = delay * (backoff ** attempt)
                        logger.warning(
                            f"Attempt {attempt + 1} failed for {func.__name__}: {e}. "
                            f"Retrying in {wait_time:.2f} seconds..."
                        )
                        time.sleep(wait_time)
                    else:
                        logger.error(
                            f"All {max_retries + 1} attempts failed for {func.__name__}: {e}"
                        )
            
            raise last_exception
        
        return wrapper
    return decorator


def safe_extract_text(element, default: str = "") -> str:
    """Safely extract text from a BeautifulSoup element."""
    try:
        if element:
            return element.get_text(strip=True)
        return default
    except Exception:
        return default


def safe_extract_int(text: str, default: int = 0) -> int:
    """Safely extract integer from text."""
    try:
        if not text:
            return default
        
        # Remove common formatting characters
        cleaned = text.replace(',', '').replace(' ', '')
        
        # Extract first number found
        import re
        numbers = re.findall(r'\d+', cleaned)
        if numbers:
            return int(numbers[0])
        
        return default
    except Exception:
        return default


def validate_url(url: str) -> bool:
    """Validate if URL is a valid GitHub repository URL."""
    try:
        import re
        pattern = r'^https://github\.com/[^/]+/[^/]+/?$'
        return bool(re.match(pattern, url))
    except Exception:
        return False


def format_duration(seconds: float) -> str:
    """Format duration in seconds to human-readable string."""
    if seconds < 60:
        return f"{seconds:.2f} seconds"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.2f} minutes"
    else:
        hours = seconds / 3600
        return f"{hours:.2f} hours"


def log_performance(func: Callable) -> Callable:
    """Decorator to log function performance."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        start_time = time.time()
        
        try:
            result = func(*args, **kwargs)
            end_time = time.time()
            duration = end_time - start_time
            
            logger.debug(f"{func.__name__} completed in {format_duration(duration)}")
            return result
            
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            
            logger.error(f"{func.__name__} failed after {format_duration(duration)}: {e}")
            raise
    
    return wrapper


class ErrorHandler:
    """Centralized error handling utilities."""
    
    @staticmethod
    def handle_network_error(error: Exception, url: str) -> None:
        """Handle network-related errors."""
        logger.error(f"Network error for {url}: {error}")
    
    @staticmethod
    def handle_scraping_error(error: Exception, url: str, element: str) -> None:
        """Handle scraping-related errors."""
        logger.warning(f"Scraping error for {element} at {url}: {error}")
    
    @staticmethod
    def handle_parsing_error(error: Exception, data_type: str) -> None:
        """Handle data parsing errors."""
        logger.error(f"Parsing error for {data_type}: {error}")
    
    @staticmethod
    def log_crawl_summary(total: int, successful: int, failed: int, duration: float) -> None:
        """Log crawling summary statistics."""
        success_rate = (successful / total * 100) if total > 0 else 0
        
        logger.info("=" * 50)
        logger.info("CRAWLING SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Total repositories: {total}")
        logger.info(f"Successfully crawled: {successful}")
        logger.info(f"Failed: {failed}")
        logger.info(f"Success rate: {success_rate:.1f}%")
        logger.info(f"Total duration: {format_duration(duration)}")
        logger.info("=" * 50)
