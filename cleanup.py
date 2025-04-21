#!/usr/bin/env python
"""
Cleanup Script

This script helps clean up redundant files and directories in the trading system.
"""

import os
import sys
import shutil
import argparse
from pathlib import Path

# Define redundant files and directories
REDUNDANT_FILES = [
    # TradingView redundancies
    "market_data/tradingview_simple.py",
    "market_data/tradingview_realtime.py",
    "monitoring/tradingview_fetcher.py",
    
    # Nobitex redundancies
    "nobitex_client.py",  # Moved to exchange/nobitex_client.py
    
    # Log files
    "trading_system_test.log",
    
    # Temporary test files
    "component_test_results.md",
]

REDUNDANT_DIRS = [
    "old_exchanges",  # Renamed from exchanges
    "__pycache__",  # Python cache directories
]

def confirm(message):
    """Ask for confirmation."""
    response = input(f"{message} (y/n): ").lower()
    return response in ["y", "yes"]

def clean_directory(directory, dry_run=True):
    """Clean redundant files from a directory."""
    base_dir = Path(directory)
    
    # Process redundant files
    files_removed = 0
    for file_path in REDUNDANT_FILES:
        full_path = base_dir / file_path
        if full_path.exists():
            if dry_run:
                print(f"Would remove file: {full_path}")
            else:
                print(f"Removing file: {full_path}")
                os.remove(full_path)
            files_removed += 1
    
    # Process redundant directories
    dirs_removed = 0
    for dir_name in REDUNDANT_DIRS:
        # Find all instances of this directory
        for dir_path in base_dir.glob(f"**/{dir_name}"):
            if dir_path.is_dir():
                if dry_run:
                    print(f"Would remove directory: {dir_path}")
                else:
                    print(f"Removing directory: {dir_path}")
                    shutil.rmtree(dir_path)
                dirs_removed += 1
    
    return files_removed, dirs_removed

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Clean up redundant files and directories")
    parser.add_argument("--directory", "-d", default=".", help="Base directory to clean")
    parser.add_argument("--execute", "-e", action="store_true", help="Actually remove files (dry run by default)")
    args = parser.parse_args()
    
    print(f"Scanning directory: {args.directory}")
    print(f"Mode: {'Execute' if args.execute else 'Dry run'}")
    
    files_removed, dirs_removed = clean_directory(args.directory, not args.execute)
    
    print(f"\nSummary:")
    print(f"  {files_removed} redundant files {'removed' if args.execute else 'found'}")
    print(f"  {dirs_removed} redundant directories {'removed' if args.execute else 'found'}")
    
    if not args.execute and (files_removed > 0 or dirs_removed > 0):
        print("\nTo actually remove these files, run again with --execute")
    
    print("\nCleanup plan steps:")
    print("1. Consolidate TradingView implementation (move enhanced fetcher to market_data)")
    print("2. Consolidate Nobitex implementation (use exchange/nobitex.py as the main implementation)")
    print("3. Organize test files (already moved to tests/ directory)")
    print("4. Consolidate logging (already completed)")
    print("5. Check for any more redundancies")
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 