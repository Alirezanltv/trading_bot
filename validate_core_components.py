#!/usr/bin/env python3
"""
Core Components Validation Runner

This script runs the validation tests for the core components of the trading system.
It ensures that the strategy, execution engine, and position management components
are working correctly together.
"""

import os
import sys
import subprocess
import argparse
from datetime import datetime

def run_validation(args):
    """
    Run the validation tests.
    
    Args:
        args: Command line arguments
    """
    # Create necessary directories
    os.makedirs("logs", exist_ok=True)
    os.makedirs("data", exist_ok=True)
    os.makedirs("data/test", exist_ok=True)
    os.makedirs("data/orders", exist_ok=True)
    os.makedirs("data/positions", exist_ok=True)
    os.makedirs("data/market_data", exist_ok=True)
    
    # Prepare command - use Python 3.10 explicitly
    cmd = [
        "py", "-3.10",
        "trading_system/tests/run_validation.py"
    ]
    
    # Add arguments
    if args.signal_flow:
        cmd.append("--signal-flow")
    if args.controller:
        cmd.append("--controller")
    if args.all:
        cmd.append("--all")
    
    # Run validation tests
    print(f"\n{'='*80}")
    print(f"Starting Core Components Validation at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}\n")
    
    try:
        # Run the tests
        process = subprocess.run(
            cmd,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True
        )
        
        # Print output
        print(process.stdout)
        
        print(f"\n{'='*80}")
        print("Validation completed successfully!")
        print(f"{'='*80}\n")
        
        return True
        
    except subprocess.CalledProcessError as e:
        # Print error output
        print(e.stdout)
        
        print(f"\n{'='*80}")
        print("Validation failed with errors!")
        print(f"{'='*80}\n")
        
        return False

def parse_args():
    """
    Parse command line arguments.
    
    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(description='Run core components validation')
    
    parser.add_argument('--all', action='store_true', help='Run all tests')
    parser.add_argument('--signal-flow', action='store_true', help='Run signal flow tests')
    parser.add_argument('--controller', action='store_true', help='Run controller tests')
    
    args = parser.parse_args()
    
    # If no specific tests are specified, run all tests
    if not (args.all or args.signal_flow or args.controller):
        args.all = True
    
    return args

if __name__ == "__main__":
    args = parse_args()
    success = run_validation(args)
    
    # Set exit code based on test success
    sys.exit(0 if success else 1) 