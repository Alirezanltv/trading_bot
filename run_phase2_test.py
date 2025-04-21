#!/usr/bin/env python3
"""
Phase 2 Test Runner

This script installs dependencies and runs the Phase 2 tests.
"""

import os
import sys
import subprocess
import platform
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("phase2_runner")

def is_windows():
    """Check if running on Windows."""
    return platform.system().lower() == "windows"

def get_python_command():
    """Get the appropriate Python command."""
    if is_windows():
        return "py -3.10"
    else:
        return "python3.10"

def run_command(command):
    """Run a shell command."""
    logger.info(f"Running: {command}")
    
    try:
        result = subprocess.run(
            command, 
            shell=True, 
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed with exit code {e.returncode}")
        logger.error(f"STDOUT: {e.stdout}")
        logger.error(f"STDERR: {e.stderr}")
        return False

def install_dependencies():
    """Install required dependencies."""
    logger.info("Installing Phase 2 dependencies...")
    
    python_cmd = get_python_command()
    
    # Get the absolute path to the requirements file
    current_dir = os.path.dirname(os.path.abspath(__file__))
    requirements_file = os.path.join(current_dir, "requirements_phase2.txt")
    
    if not os.path.exists(requirements_file):
        logger.error(f"Requirements file '{requirements_file}' not found")
        return False
    
    command = f"{python_cmd} -m pip install -r {requirements_file}"
    return run_command(command)

def run_tests():
    """Run the Phase 2 tests."""
    logger.info("Running Phase 2 tests...")
    
    python_cmd = get_python_command()
    
    # Get the absolute path to the test file
    current_dir = os.path.dirname(os.path.abspath(__file__))
    test_file = os.path.join(current_dir, "test_phase2.py")
    
    if not os.path.exists(test_file):
        logger.error(f"Test file '{test_file}' not found")
        return False
    
    command = f"{python_cmd} {test_file}"
    return run_command(command)

def main():
    """Main entry point."""
    try:
        logger.info("Starting Phase 2 Test Suite")
        
        # Install dependencies
        if not install_dependencies():
            logger.error("Failed to install dependencies")
            sys.exit(1)
        
        # Run tests
        if not run_tests():
            logger.error("Phase 2 tests failed")
            sys.exit(1)
        
        logger.info("Phase 2 tests completed successfully")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main() 