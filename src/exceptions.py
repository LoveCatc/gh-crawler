"""Custom exceptions for the GitHub crawler."""


class CrawlerError(Exception):
    """Base exception for crawler errors."""
    pass


class NetworkError(CrawlerError):
    """Exception for network-related errors."""
    pass


class ScrapingError(CrawlerError):
    """Exception for web scraping errors."""
    pass


class DataParsingError(CrawlerError):
    """Exception for data parsing errors."""
    pass


class ConfigurationError(CrawlerError):
    """Exception for configuration errors."""
    pass


class RateLimitError(NetworkError):
    """Exception for rate limiting errors."""
    pass


class ProxyError(NetworkError):
    """Exception for proxy-related errors."""
    pass
