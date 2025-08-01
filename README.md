# GitHub Repository Crawler

A Python application that crawls GitHub repositories to extract detailed information including contributors, forks, issues, and pull request details. The crawler uses proxy support and concurrent processing for efficient data collection.

## Features

- **Concurrent Processing**: Multi-threaded crawling for improved performance
- **Proxy Support**: Built-in proxy support for IP rotation
- **Robust Error Handling**: Comprehensive retry logic and error recovery
- **Detailed Logging**: Structured logging with both console and file output
- **Flexible Input/Output**: JSON input files and JSONL output format
- **Command Line Interface**: Easy-to-use CLI with multiple options

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd gh-crawler
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage

```bash
python main.py -i input_data.json -s 1000
```

### Advanced Usage

```bash
python main.py \
  --input-files data1.json data2.json \
  --star-threshold 5000 \
  --output-dir results/ \
  --max-workers 5 \
  --log-level INFO
```

### Command Line Options

- `-i, --input-files`: Input JSON files (can specify multiple)
- `-s, --star-threshold`: Minimum stars for repositories to crawl
- `-o, --output-dir`: Output directory for results (default: output)
- `-w, --max-workers`: Number of concurrent workers (default: 10)
- `-l, --log-level`: Logging level (DEBUG, INFO, WARNING, ERROR)
- `--log-file`: Custom log file path
- `--dry-run`: Show what would be processed without crawling

## Input Format

Input files should be JSON with the following structure:

```json
{
  "language": "Python",
  "summary": {
    "total_repositories": 388970,
    "total_stars": 56926598,
    "average_stars": 146.35,
    "top_repository": {
      "url": "https://github.com/public-apis/public-apis",
      "stars": 327362
    }
  },
  "repositories": [
    {
      "url": "https://github.com/public-apis/public-apis",
      "stars": 327362,
      "language": ["Python"]
    }
  ]
}
```

## Output Format

Results are saved in JSONL format with detailed repository information:

```json
{
  "url": "https://github.com/example/repo",
  "stars": 1000,
  "language": ["Python"],
  "stats": {
    "contributors_count": 50,
    "forks_count": 200,
    "total_issues": 100,
    "open_issues": 20,
    "closed_issues": 80,
    "total_pull_requests": 150,
    "open_pull_requests": 10,
    "closed_pull_requests": 140
  },
  "pull_requests": [
    {
      "number": 123,
      "title": "Fix bug in authentication",
      "state": "merged",
      "tags": ["bug-fix", "security"],
      "comments": ["Great fix!", "Thanks for the contribution"],
      "related_issues": [45, 67],
      "url": "https://github.com/example/repo/pull/123"
    }
  ],
  "crawl_timestamp": "2024-01-01T12:00:00",
  "crawl_success": true
}
```

## Configuration

The crawler uses a proxy at `http://127.0.0.1:7892` by default. Make sure your proxy is running before starting the crawler.

Key configuration options in `src/config.py`:
- `PROXY_URL`: Proxy server URL
- `MAX_WORKERS`: Default number of concurrent workers
- `REQUEST_TIMEOUT`: HTTP request timeout
- `MAX_RETRIES`: Maximum retry attempts for failed requests

## Project Structure

```
gh-crawler/
├── src/
│   ├── __init__.py
│   ├── __main__.py
│   ├── cli.py              # Command line interface
│   ├── config.py           # Configuration settings
│   ├── models.py           # Data models
│   ├── http_client.py      # HTTP client with proxy support
│   ├── repository_scraper.py # Repository statistics scraper
│   ├── pr_scraper.py       # Pull request detail scraper
│   ├── crawler.py          # Main crawler with concurrency
│   ├── io_handler.py       # Input/output handling
│   ├── exceptions.py       # Custom exceptions
│   └── utils.py            # Utility functions
├── main.py                 # Main entry point
├── requirements.txt        # Python dependencies
├── test_data.json         # Sample input data
└── README.md              # This file
```

## Logging

The crawler provides comprehensive logging:
- Console output with colored formatting
- Log file with rotation (10MB files, 7 days retention)
- Configurable log levels
- Performance metrics and error tracking

## Error Handling

- Automatic retry with exponential backoff
- Graceful handling of network errors
- Rate limiting to avoid overwhelming servers
- Detailed error logging and reporting

## Contributing

1. Follow the existing code structure
2. Add appropriate logging and error handling
3. Update documentation for new features
4. Test with sample data before submitting

## License

This project is for educational and research purposes. Please respect GitHub's terms of service and rate limits.
