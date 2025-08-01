#!/usr/bin/env python3
"""
Configuration script for PR crawling parameters.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from src.config import MAX_CLOSED_PRS_TO_CRAWL, CRAWL_OPEN_PRS, CRAWL_CLOSED_PRS


def show_current_config():
    """Show current PR crawling configuration."""
    print("=" * 60)
    print("CURRENT PR CRAWLING CONFIGURATION")
    print("=" * 60)
    print(f"Max Closed PRs to Crawl: {MAX_CLOSED_PRS_TO_CRAWL:,}")
    print(f"Crawl Open PRs: {'✅ Yes' if CRAWL_OPEN_PRS else '❌ No'}")
    print(f"Crawl Closed PRs: {'✅ Yes' if CRAWL_CLOSED_PRS else '❌ No'}")
    print()
    
    if CRAWL_OPEN_PRS and CRAWL_CLOSED_PRS:
        print(f"📊 Strategy: ALL open PRs + latest {MAX_CLOSED_PRS_TO_CRAWL:,} closed PRs")
    elif CRAWL_OPEN_PRS:
        print("📊 Strategy: Only open PRs")
    elif CRAWL_CLOSED_PRS:
        print(f"📊 Strategy: Only latest {MAX_CLOSED_PRS_TO_CRAWL:,} closed PRs")
    else:
        print("⚠️  Strategy: No PRs will be crawled!")
    
    print()


def update_config():
    """Interactive configuration update."""
    print("=" * 60)
    print("UPDATE PR CRAWLING CONFIGURATION")
    print("=" * 60)
    
    config_file = Path(__file__).parent / 'src' / 'config.py'
    
    # Read current config
    with open(config_file, 'r') as f:
        content = f.read()
    
    print("Current settings:")
    print(f"  1. Max Closed PRs: {MAX_CLOSED_PRS_TO_CRAWL:,}")
    print(f"  2. Crawl Open PRs: {CRAWL_OPEN_PRS}")
    print(f"  3. Crawl Closed PRs: {CRAWL_CLOSED_PRS}")
    print()
    
    # Get new max closed PRs
    while True:
        try:
            new_max = input(f"Enter new max closed PRs to crawl (current: {MAX_CLOSED_PRS_TO_CRAWL:,}): ").strip()
            if not new_max:
                new_max = MAX_CLOSED_PRS_TO_CRAWL
                break
            new_max = int(new_max)
            if new_max < 0:
                print("❌ Please enter a positive number")
                continue
            break
        except ValueError:
            print("❌ Please enter a valid number")
    
    # Get crawl open PRs setting
    while True:
        crawl_open = input(f"Crawl open PRs? (y/n, current: {'y' if CRAWL_OPEN_PRS else 'n'}): ").strip().lower()
        if not crawl_open:
            crawl_open = CRAWL_OPEN_PRS
            break
        if crawl_open in ['y', 'yes', 'true', '1']:
            crawl_open = True
            break
        elif crawl_open in ['n', 'no', 'false', '0']:
            crawl_open = False
            break
        else:
            print("❌ Please enter y or n")
    
    # Get crawl closed PRs setting
    while True:
        crawl_closed = input(f"Crawl closed PRs? (y/n, current: {'y' if CRAWL_CLOSED_PRS else 'n'}): ").strip().lower()
        if not crawl_closed:
            crawl_closed = CRAWL_CLOSED_PRS
            break
        if crawl_closed in ['y', 'yes', 'true', '1']:
            crawl_closed = True
            break
        elif crawl_closed in ['n', 'no', 'false', '0']:
            crawl_closed = False
            break
        else:
            print("❌ Please enter y or n")
    
    # Update config file
    new_content = content
    new_content = new_content.replace(
        f"MAX_CLOSED_PRS_TO_CRAWL = {MAX_CLOSED_PRS_TO_CRAWL}",
        f"MAX_CLOSED_PRS_TO_CRAWL = {new_max}"
    )
    new_content = new_content.replace(
        f"CRAWL_OPEN_PRS = {CRAWL_OPEN_PRS}",
        f"CRAWL_OPEN_PRS = {crawl_open}"
    )
    new_content = new_content.replace(
        f"CRAWL_CLOSED_PRS = {CRAWL_CLOSED_PRS}",
        f"CRAWL_CLOSED_PRS = {crawl_closed}"
    )
    
    # Write updated config
    with open(config_file, 'w') as f:
        f.write(new_content)
    
    print()
    print("✅ Configuration updated successfully!")
    print()
    print("New settings:")
    print(f"  - Max Closed PRs: {new_max:,}")
    print(f"  - Crawl Open PRs: {crawl_open}")
    print(f"  - Crawl Closed PRs: {crawl_closed}")
    print()
    
    if crawl_open and crawl_closed:
        print(f"📊 New Strategy: ALL open PRs + latest {new_max:,} closed PRs")
    elif crawl_open:
        print("📊 New Strategy: Only open PRs")
    elif crawl_closed:
        print(f"📊 New Strategy: Only latest {new_max:,} closed PRs")
    else:
        print("⚠️  New Strategy: No PRs will be crawled!")


def show_presets():
    """Show common configuration presets."""
    print("=" * 60)
    print("COMMON CONFIGURATION PRESETS")
    print("=" * 60)
    
    presets = [
        {
            "name": "🚀 Ultra Fast (Recommended)",
            "description": "All open PRs + latest 1,000 closed PRs",
            "max_closed": 1000,
            "open": True,
            "closed": True
        },
        {
            "name": "⚡ Fast",
            "description": "All open PRs + latest 2,000 closed PRs",
            "max_closed": 2000,
            "open": True,
            "closed": True
        },
        {
            "name": "🔍 Comprehensive",
            "description": "All open PRs + latest 5,000 closed PRs",
            "max_closed": 5000,
            "open": True,
            "closed": True
        },
        {
            "name": "📊 Research Mode",
            "description": "All open PRs + latest 10,000 closed PRs",
            "max_closed": 10000,
            "open": True,
            "closed": True
        },
        {
            "name": "🎯 Open PRs Only",
            "description": "Only open PRs (fastest)",
            "max_closed": 0,
            "open": True,
            "closed": False
        },
        {
            "name": "📚 Recent Closed Only",
            "description": "Only latest 2,000 closed PRs",
            "max_closed": 2000,
            "open": False,
            "closed": True
        }
    ]
    
    for i, preset in enumerate(presets, 1):
        print(f"{i}. {preset['name']}")
        print(f"   {preset['description']}")
        print()


def main():
    """Main configuration interface."""
    while True:
        print("=" * 60)
        print("PR CRAWLING CONFIGURATION TOOL")
        print("=" * 60)
        print("1. Show current configuration")
        print("2. Update configuration")
        print("3. Show preset configurations")
        print("4. Exit")
        print()
        
        choice = input("Select an option (1-4): ").strip()
        
        if choice == '1':
            show_current_config()
        elif choice == '2':
            update_config()
        elif choice == '3':
            show_presets()
        elif choice == '4':
            print("👋 Goodbye!")
            break
        else:
            print("❌ Invalid choice. Please select 1-4.")
        
        input("\nPress Enter to continue...")
        print()


if __name__ == '__main__':
    main()
