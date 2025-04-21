#!/usr/bin/env python3
"""
Test Runner for Trading System

This script runs all the tests for the trading system package.
"""

import os
import sys
import unittest
import argparse
import logging
from pathlib import Path

# Add the parent directory to Python's path
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

def run_tests(test_path=None, verbose=False, failfast=False):
    """
    Run the tests from the given path or all tests if path is None.
    
    Args:
        test_path: Optional path to specific test or test package
        verbose: Whether to run tests in verbose mode
        failfast: Whether to stop at first failure
    """
    loader = unittest.TestLoader()
    
    if test_path:
        # Run specific test or test package
        path = Path(test_path)
        
        if path.is_file():
            # Run a specific test file
            test_module = path.stem
            test_dir = str(path.parent)
            if test_dir:
                sys.path.insert(0, test_dir)
            suite = loader.loadTestsFromName(test_module)
        else:
            # Run all tests in a directory
            suite = loader.discover(test_path)
    else:
        # Run all tests
        suite = loader.discover('tests', top_level_dir='.')
    
    # Run the tests
    verbosity = 2 if verbose else 1
    runner = unittest.TextTestRunner(verbosity=verbosity, failfast=failfast)
    result = runner.run(suite)
    
    return result.wasSuccessful()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run trading system tests.')
    parser.add_argument('test_path', nargs='?', help='Path to specific test or test package')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose output')
    parser.add_argument('-f', '--failfast', action='store_true', help='Stop on first fail or error')
    
    args = parser.parse_args()
    
    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Run the tests
    success = run_tests(args.test_path, args.verbose, args.failfast)
    
    # Exit with appropriate code
    sys.exit(0 if success else 1) 