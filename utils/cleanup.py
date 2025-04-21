"""
Utility script for cleaning up temporary files and organizing the project structure.
This script helps maintain a clean workspace by removing cached files, logs, or
other temporary files that aren't needed for the project.
"""

import os
import shutil
import argparse
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Directories to clean
CACHE_DIRS = [
    '__pycache__',
    '.pytest_cache',
    '.mypy_cache'
]

# File extensions to clean
TEMP_EXTENSIONS = [
    '.pyc',
    '.pyo',
    '.pyd',
    '.~',
    '.bak',
    '.swp'
]

# Directories to skip
SKIP_DIRS = [
    '.git',
    '.venv',
    'venv',
    'env',
    'node_modules'
]


def clean_pycache(root_dir, recursive=True):
    """Clean Python cache directories and files."""
    count = 0
    
    for root, dirs, files in os.walk(root_dir):
        # Skip directories in SKIP_DIRS
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        
        # Remove cache directories
        for cache_dir in CACHE_DIRS:
            if cache_dir in dirs:
                cache_path = os.path.join(root, cache_dir)
                logger.info(f"Removing directory: {cache_path}")
                shutil.rmtree(cache_path)
                count += 1
                dirs.remove(cache_dir)
        
        # Remove temporary files
        for file in files:
            for ext in TEMP_EXTENSIONS:
                if file.endswith(ext):
                    file_path = os.path.join(root, file)
                    logger.info(f"Removing file: {file_path}")
                    os.remove(file_path)
                    count += 1
                    break
        
        # Stop recursion if not requested
        if not recursive:
            break
    
    return count


def clean_old_logs(log_dir, days=7):
    """Clean log files older than the specified number of days."""
    if not os.path.exists(log_dir):
        logger.warning(f"Log directory does not exist: {log_dir}")
        return 0
    
    count = 0
    cutoff_date = datetime.now() - timedelta(days=days)
    
    for filename in os.listdir(log_dir):
        file_path = os.path.join(log_dir, filename)
        
        # Skip directories
        if os.path.isdir(file_path):
            continue
        
        # Check if file is a log file
        if not (filename.endswith('.log') or filename.endswith('.txt')):
            continue
        
        # Check file modification time
        mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
        if mod_time < cutoff_date:
            logger.info(f"Removing old log file: {file_path}")
            os.remove(file_path)
            count += 1
    
    return count


def main():
    """Main function to run the cleanup script."""
    parser = argparse.ArgumentParser(description="Clean up temporary files and directories.")
    parser.add_argument("--root", default=os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')), 
                        help="Root directory to clean.")
    parser.add_argument("--no-recursive", action="store_false", dest="recursive",
                       help="Don't clean subdirectories recursively.")
    parser.add_argument("--logs", default=os.path.abspath(os.path.join(os.path.dirname(__file__), '../../logs')),
                       help="Directory containing log files to clean.")
    parser.add_argument("--log-age", type=int, default=7,
                       help="Remove log files older than this many days.")
    args = parser.parse_args()
    
    logger.info(f"Starting cleanup process in {args.root}")
    
    # Clean Python cache files
    cache_count = clean_pycache(args.root, args.recursive)
    logger.info(f"Removed {cache_count} cache directories and temporary files.")
    
    # Clean old log files
    log_count = clean_old_logs(args.logs, args.log_age)
    logger.info(f"Removed {log_count} old log files.")
    
    logger.info("Cleanup completed successfully.")


if __name__ == "__main__":
    main() 