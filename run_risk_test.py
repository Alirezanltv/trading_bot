#!/usr/bin/env python3
"""
Test runner for Risk Manager tests only.
"""

import os
import sys
import unittest
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# Add parent directory to path if needed
if os.path.basename(os.getcwd()) == "trading_system":
    sys.path.insert(0, os.path.abspath(".."))
else:
    sys.path.insert(0, os.path.abspath("."))

def run_tests():
    """Run risk manager tests."""
    from tests.test_phase1 import TestRiskManagerMultiLevel
    
    # Create test suite
    suite = unittest.TestSuite()
    
    # Add test cases
    print("Running Risk Manager tests")
    suite.addTest(unittest.makeSuite(TestRiskManagerMultiLevel))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Return success
    return len(result.failures) == 0 and len(result.errors) == 0

if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1) 