#!/usr/bin/env python3
"""
Test runner for Phase 1 components of the high-reliability trading system.

This script runs all tests for:
1. Enhanced Shadow Accounting
2. Multi-level Stop-Loss Mechanisms
3. Position Reconciliation

Usage:
    py -3.10 run_phase1_tests.py
"""

import os
import sys
import unittest
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"phase1_test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    ]
)

logger = logging.getLogger("phase1_tests")

# Add parent directory to path if needed
if os.path.basename(os.getcwd()) == "trading_system":
    sys.path.insert(0, os.path.abspath(".."))
else:
    sys.path.insert(0, os.path.abspath("."))

def run_tests():
    """Run all Phase 1 tests."""
    from tests.test_phase1 import (
        TestShadowAccountingEnhanced,
        TestRiskManagerMultiLevel,
        TestPositionReconciliation
    )
    
    # Create test suite
    suite = unittest.TestSuite()
    
    # Add test cases
    logger.info("Adding Shadow Accounting tests")
    suite.addTest(unittest.makeSuite(TestShadowAccountingEnhanced))
    
    logger.info("Adding Risk Manager tests")
    suite.addTest(unittest.makeSuite(TestRiskManagerMultiLevel))
    
    logger.info("Adding Position Reconciliation tests")
    suite.addTest(unittest.makeSuite(TestPositionReconciliation))
    
    # Run tests
    logger.info("=" * 70)
    logger.info("STARTING PHASE 1 COMPONENT TESTS")
    logger.info("=" * 70)
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Log results
    logger.info("=" * 70)
    logger.info("TEST RESULTS:")
    logger.info(f"Ran {result.testsRun} tests")
    logger.info(f"Failures: {len(result.failures)}")
    logger.info(f"Errors: {len(result.errors)}")
    logger.info(f"Skipped: {len(result.skipped)}")
    logger.info("=" * 70)
    
    # Print failures and errors
    if result.failures:
        logger.error("FAILURES:")
        for test, traceback in result.failures:
            logger.error(f"- {test}")
            logger.error(traceback)
    
    if result.errors:
        logger.error("ERRORS:")
        for test, traceback in result.errors:
            logger.error(f"- {test}")
            logger.error(traceback)
    
    return len(result.failures) == 0 and len(result.errors) == 0

if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1) 