"""Command line interface for the GitHub crawler."""

import sys
from pathlib import Path
from typing import List
import click
from loguru import logger

from .config import MAX_WORKERS, LOG_LEVEL, LOG_FILE, LOG_FORMAT, MAX_CLOSED_PRS_TO_CRAWL, MIN_PRS_REQUIRED, REPO_WORKERS
from .io_handler import InputHandler, FileManager
from .enhanced_crawler import EnhancedCrawlerManager
from .multiprocess_crawler import MultiprocessCrawlerManager
from .performance_monitor import get_performance_monitor, log_final_performance_summary


def setup_logging(log_level: str = LOG_LEVEL, log_file: str = LOG_FILE):
    """Setup logging configuration."""
    # Remove default logger
    logger.remove()
    
    # Add console logger
    logger.add(
        sys.stdout,
        format=LOG_FORMAT,
        level=log_level,
        colorize=True
    )
    
    # Add file logger
    logger.add(
        log_file,
        format=LOG_FORMAT,
        level=log_level,
        rotation="10 MB",
        retention="7 days"
    )


@click.command()
@click.option(
    '--input-files', '-i',
    multiple=True,
    required=True,
    help='Input JSON files containing repository data'
)
@click.option(
    '--star-threshold', '-s',
    type=int,
    required=True,
    help='Minimum number of stars for repositories to crawl'
)
@click.option(
    '--output-dir', '-o',
    default='output',
    help='Output directory for JSONL files (default: output)'
)
@click.option(
    '--max-workers', '-w',
    type=int,
    default=MAX_WORKERS,
    help=f'Maximum number of concurrent workers (default: {MAX_WORKERS})'
)
@click.option(
    '--log-level', '-l',
    type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR']),
    default=LOG_LEVEL,
    help=f'Logging level (default: {LOG_LEVEL})'
)
@click.option(
    '--log-file',
    default=LOG_FILE,
    help=f'Log file path (default: {LOG_FILE})'
)
@click.option(
    '--max-closed-prs',
    type=int,
    help=f'Maximum number of closed PRs to crawl per repository (default: {MAX_CLOSED_PRS_TO_CRAWL})'
)
@click.option(
    '--dry-run',
    is_flag=True,
    help='Show what would be processed without actually crawling'
)
@click.option(
    '--repo-workers', '-rw',
    type=int,
    default=REPO_WORKERS,
    help=f'Number of repository worker processes (default: {REPO_WORKERS})'
)
@click.option(
    '--use-multiprocessing', '-mp',
    is_flag=True,
    help='Use multiprocessing for better CPU utilization'
)
@click.option(
    '--enable-tunnel-proxy', '-tp',
    is_flag=True,
    help='Enable tunnel proxy mode (refreshes connection for new IP per request)'
)
@click.option(
    '--quiet', '-q',
    is_flag=True,
    help='Reduce verbose logging output'
)
@click.option(
    '--skip-commits',
    is_flag=True,
    help='Skip commit scraping for faster processing (commits will be empty)'
)
def main(input_files: tuple, star_threshold: int, output_dir: str,
         max_workers: int, log_level: str, log_file: str, dry_run: bool, max_closed_prs: int,
         repo_workers: int, use_multiprocessing: bool, enable_tunnel_proxy: bool, quiet: bool, skip_commits: bool):
    """
    GitHub Repository Crawler
    
    Crawls GitHub repositories from input JSON files and saves detailed
    information to JSONL files. Uses proxy for requests and supports
    concurrent processing.
    
    Example usage:
    
        python -m src.cli -i data.json -s 500 -o results/
    
        python -m src.cli -i file1.json -i file2.json -s 500 -w 5
    """
    # Setup logging
    setup_logging(log_level, log_file)

    # Configure quiet mode if enabled
    if quiet:
        import os
        os.environ["QUIET_MODE"] = "True"

    # Configure commit scraping if disabled
    if skip_commits:
        import os
        os.environ["ENABLE_COMMIT_SCRAPING"] = "False"
        if not quiet:
            logger.info("⏭️  Commit scraping disabled - faster processing, empty commit fields")

    # Configure tunnel proxy if enabled
    if enable_tunnel_proxy:
        import os
        os.environ["ENABLE_PROXY_REFRESH"] = "True"
        if not quiet:
            logger.info("🔄 Tunnel proxy mode enabled - will refresh connections for new IPs")

    # Start performance monitoring
    performance_monitor = get_performance_monitor()

    logger.info("🚀 Starting GitHub Repository Crawler")
    logger.info(f"Input files: {list(input_files)}")
    logger.info(f"Star threshold: {star_threshold}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Max workers: {max_workers}")
    logger.info(f"Repository workers: {repo_workers}")
    logger.info(f"Use multiprocessing: {use_multiprocessing}")
    logger.info(f"Tunnel proxy mode: {enable_tunnel_proxy}")
    logger.info(f"Skip commit scraping: {skip_commits}")
    logger.info(f"Minimum PRs required: {MIN_PRS_REQUIRED}")
    logger.info(f"Dry run: {dry_run}")
    
    try:
        # Validate input files
        valid_input_files = FileManager.validate_input_files(list(input_files))
        if not valid_input_files:
            logger.error("No valid input files found")
            sys.exit(1)
        
        # Load input data
        input_data_list = InputHandler.load_multiple_input_files(valid_input_files)
        if not input_data_list:
            logger.error("Failed to load any input data")
            sys.exit(1)
        
        # Process each input file
        for i, input_data in enumerate(input_data_list):
            input_file = valid_input_files[i]
            logger.info(f"Processing input file: {input_file}")
            
            # Filter repositories by star threshold
            filtered_repos = [
                repo for repo in input_data.repositories 
                if repo.stars >= star_threshold
            ]
            
            logger.info(f"Found {len(filtered_repos)} repositories above star threshold "
                       f"(filtered from {len(input_data.repositories)} total)")
            
            if dry_run:
                logger.info("DRY RUN: Would process the following repositories:")
                for repo in filtered_repos[:10]:  # Show first 10
                    logger.info(f"  - {repo.url} ({repo.stars} stars)")
                if len(filtered_repos) > 10:
                    logger.info(f"  ... and {len(filtered_repos) - 10} more")
                continue
            
            if not filtered_repos:
                logger.warning(f"No repositories to process for {input_file}")
                continue
            
            # Generate output filename
            output_filename = FileManager.generate_output_filename(input_file, star_threshold)
            output_path = Path(output_dir) / output_filename
            
            # Ensure output directory exists
            if not FileManager.ensure_output_directory(str(output_path)):
                logger.error(f"Failed to create output directory for {output_path}")
                continue
            
            # Initialize crawler based on user preference
            if use_multiprocessing:
                logger.info("🔀 Using MULTIPROCESSING crawler for maximum CPU utilization")
                crawler_manager = MultiprocessCrawlerManager(repo_workers, max_workers)
            else:
                logger.info("🔀 Using ENHANCED THREADING crawler")
                crawler_manager = EnhancedCrawlerManager(max_workers)

            # Crawl repositories (results are written immediately to disk)
            logger.info(f"Starting crawl for {len(filtered_repos)} repositories")
            logger.info(f"Results will be written immediately to: {output_path}")

            successful_count = crawler_manager.process_repositories(
                input_data.repositories, star_threshold, output_dir, str(output_path)
            )

            # Results are already saved during crawling - just report the outcome
            if successful_count > 0:
                logger.info(f"Successfully crawled and saved {successful_count} new repositories to {output_path}")
            else:
                logger.info(f"No new repositories were crawled for {input_file} (all may have been previously crawled)")
        
        logger.info("GitHub Repository Crawler completed successfully")

        # Log final performance summary
        log_final_performance_summary()

    except KeyboardInterrupt:
        logger.warning("Crawling interrupted by user")
        # Still log performance summary even on interruption
        try:
            log_final_performance_summary()
        except:
            pass
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        # Still log performance summary even on error
        try:
            log_final_performance_summary()
        except:
            pass
        sys.exit(1)


if __name__ == '__main__':
    main()
