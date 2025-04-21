#!/usr/bin/env python
"""
High-Reliability Trading System Installer

This script installs and configures all necessary components for the
high-reliability crypto trading system.

Usage:
    py -3.10 -m trading_system.install_high_reliability [--dev] [--minimal]
    
Options:
    --dev       Install development dependencies
    --minimal   Install only core reliability components
"""

import argparse
import os
import platform
import subprocess
import sys
import shlex
from pathlib import Path


def print_step(message):
    """Print a step message with formatting."""
    print("\n" + "=" * 80)
    print(f"  {message}")
    print("=" * 80)


def run_command(command, description=None, check=True, shell=False):
    """Run a shell command and print its output."""
    if description:
        print(f"\n> {description}")
    
    # Convert command to string if it's a list and shell=True
    if isinstance(command, list) and shell:
        command = " ".join(command)
    
    print(f"$ {command if isinstance(command, str) else ' '.join(command)}")
    
    try:
        result = subprocess.run(
            command, 
            check=check, 
            text=True, 
            capture_output=True,
            shell=shell
        )
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        return result.returncode == 0
    except subprocess.CalledProcessError as e:
        print(f"Command failed with exit code {e.returncode}", file=sys.stderr)
        if e.stdout:
            print(e.stdout)
        if e.stderr:
            print(e.stderr, file=sys.stderr)
        return False
    except FileNotFoundError as e:
        print(f"Command not found: {e}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"Error executing command: {e}", file=sys.stderr)
        return False


def get_python_command():
    """Get the appropriate Python command for the current platform."""
    if platform.system() == "Windows":
        return "py -3.10"
    else:
        return "python3.10"


def install_dependencies(requirements_file, dev=False):
    """Install dependencies from requirements file."""
    python_cmd = get_python_command()
    
    if platform.system() == "Windows":
        # On Windows, use shell=True to handle the py launcher
        command = f"{python_cmd} -m pip install -r {requirements_file}"
        if dev:
            command += " --dev"
        return run_command(command, f"Installing dependencies from {requirements_file}", shell=True)
    else:
        # On Unix-like systems, use a list of arguments
        command = [python_cmd, "-m", "pip", "install", "-r", requirements_file]
        if dev:
            command.append("--dev")
        return run_command(command, f"Installing dependencies from {requirements_file}")


def create_directories():
    """Create necessary directories."""
    directories = [
        "logs",
        "cache",
        "data",
        "config"
    ]
    
    for directory in directories:
        path = Path("trading_system") / directory
        if not path.exists():
            print(f"Creating directory: {path}")
            path.mkdir(parents=True, exist_ok=True)


def create_default_config():
    """Create default configuration files."""
    config_dir = Path("trading_system/config")
    
    # Create high reliability config if it doesn't exist
    high_reliability_config = config_dir / "high_reliability_config.yaml"
    if not high_reliability_config.exists():
        print(f"Creating default high reliability configuration: {high_reliability_config}")
        with open(high_reliability_config, "w") as f:
            f.write("""# High Reliability Configuration

# Connection Pool Configuration
connection_pool:
  min_connections: 2
  max_connections: 10
  connection_timeout: 10
  idle_timeout: 60
  max_retries: 3
  retry_delay: 1.0

# Circuit Breaker Configuration
circuit_breaker:
  failure_threshold: 5
  reset_timeout: 60.0
  half_open_max_requests: 3
  success_threshold: 2

# Alert System Configuration
alert_system:
  channels:
    console: true
    email: false
    telegram: false
    webhook: false
  level_thresholds:
    console: info
    email: warning
    telegram: error
    webhook: warning
  max_history: 1000
  deduplication_window: 300

# Transaction Verification Configuration
transaction_verification:
  verification_timeout: 30
  max_retries: 3
  verification_interval: 5

# Shadow Accounting Configuration
shadow_accounting:
  reconciliation_interval: 300
  tolerance: 0.01
  max_history: 100
  auto_correct: true
""")


def run_tests():
    """Run the high reliability tests."""
    python_cmd = get_python_command()
    
    if platform.system() == "Windows":
        return run_command(
            f"{python_cmd} -m trading_system.test_high_reliability",
            "Running high reliability tests",
            shell=True
        )
    else:
        return run_command(
            [python_cmd, "-m", "trading_system.test_high_reliability"],
            "Running high reliability tests"
        )


def install_high_reliability(dev=False, minimal=False):
    """Install the high reliability trading system."""
    print_step("Starting High-Reliability Trading System Installation")
    
    # Determine project root
    project_root = Path(os.path.dirname(os.path.abspath(__file__))).parent
    os.chdir(project_root)
    
    print(f"Project root: {project_root}")
    
    # Install dependencies
    print_step("Installing Dependencies")
    requirements_file = "trading_system/high_reliability_requirements.txt"
    
    # Check if requirements file exists
    if not os.path.exists(requirements_file):
        print(f"Error: Requirements file not found: {requirements_file}")
        print("Current directory:", os.getcwd())
        print("Directory contents:", os.listdir())
        return False
    
    if minimal:
        # Create a temporary minimal requirements file
        minimal_file = os.path.join(os.getcwd(), "minimal_requirements.txt")
        print(f"Creating minimal requirements file: {minimal_file}")
        
        try:
            with open(minimal_file, "w") as f:
                with open(requirements_file, "r") as source:
                    for line in source:
                        if line.startswith("#"):
                            f.write(line)
                        elif any(keyword in line for keyword in ["aiohttp", "asyncio", "pika", "pybreaker", "tenacity"]):
                            f.write(line)
            requirements_file = minimal_file
        except Exception as e:
            print(f"Error creating minimal requirements file: {e}")
            return False
    
    # Install dependencies
    if not install_dependencies(requirements_file, dev):
        print("Failed to install dependencies")
        return False
    
    # Create necessary directories
    print_step("Creating Directories")
    create_directories()
    
    # Create default configuration
    print_step("Creating Default Configuration")
    create_default_config()
    
    # Run tests if not minimal install
    if not minimal:
        print_step("Running Tests")
        if not run_tests():
            print("Warning: Some tests failed, but continuing installation")
    
    print_step("Installation Complete")
    print("""
To verify the installation, run the high reliability demo:
    py -3.10 -m trading_system.examples.high_reliability_demo
    
Or run the tests:
    py -3.10 -m trading_system.test_high_reliability
    
Enjoy your high-reliability trading system!
""")
    
    return True


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Install high-reliability trading system components")
    parser.add_argument("--dev", action="store_true", help="Install development dependencies")
    parser.add_argument("--minimal", action="store_true", help="Install only core reliability components")
    
    args = parser.parse_args()
    
    success = install_high_reliability(args.dev, args.minimal)
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main()) 