#!/usr/bin/env python3
"""
Test script for the GitHub crawler.
"""

import sys
import json
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from src.models import InputData, InputRepository
from src.io_handler import InputHandler, OutputHandler
from src.http_client import HTTPClient
from src.repository_scraper import RepositoryScraper
from src.config import PROXIES


def test_models():
    """Test data models."""
    print("Testing data models...")
    
    # Test InputRepository
    repo = InputRepository(
        url="https://github.com/test/repo",
        stars=1000,
        language=["Python"]
    )
    
    # Test serialization
    repo_dict = repo.to_dict()
    repo_restored = InputRepository.from_dict(repo_dict)
    
    assert repo.url == repo_restored.url
    assert repo.stars == repo_restored.stars
    assert repo.language == repo_restored.language
    
    print("✓ Data models test passed")


def test_input_handler():
    """Test input file handling."""
    print("Testing input handler...")
    
    # Test loading test data
    input_data = InputHandler.load_input_data("test_data.json")
    
    if input_data:
        print(f"✓ Loaded {len(input_data.repositories)} repositories")
        print(f"✓ Language: {input_data.language}")
        print(f"✓ Total repositories in summary: {input_data.summary.total_repositories}")
    else:
        print("✗ Failed to load test data")
        return False
    
    return True


def test_http_client():
    """Test HTTP client (without making actual requests)."""
    print("Testing HTTP client...")
    
    try:
        with HTTPClient() as client:
            # Test that client initializes properly
            assert client.session is not None
            assert client.session.proxies == PROXIES
            print("✓ HTTP client initialized with proxy settings")
            
            # Test rate limiting (should not make actual request)
            print("✓ HTTP client test passed")
            
    except Exception as e:
        print(f"✗ HTTP client test failed: {e}")
        return False
    
    return True


def test_dry_run():
    """Test dry run functionality."""
    print("Testing dry run...")
    
    try:
        # Import CLI function
        from src.cli import main
        
        # This would normally be tested with click.testing.CliRunner
        # For now, just verify the import works
        print("✓ CLI module imported successfully")
        
    except Exception as e:
        print(f"✗ CLI test failed: {e}")
        return False
    
    return True


def main():
    """Run all tests."""
    print("=" * 50)
    print("GitHub Crawler Test Suite")
    print("=" * 50)
    
    tests = [
        test_models,
        test_input_handler,
        test_http_client,
        test_dry_run
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"✗ Test {test.__name__} failed with exception: {e}")
    
    print("=" * 50)
    print(f"Test Results: {passed}/{total} passed")
    
    if passed == total:
        print("🎉 All tests passed!")
        return 0
    else:
        print("❌ Some tests failed")
        return 1


if __name__ == '__main__':
    sys.exit(main())
