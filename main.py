#!/usr/bin/env python3
"""
Trading System Entry Point

Main entry point for the trading system.
"""

import os
import sys
import asyncio
import argparse
from pathlib import Path
import signal
import json

# Add project root to path for easy imports
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from trading_system.core.logging import initialize_logging, get_logger
from trading_system.controller import TradingController


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Trading System")
    
    parser.add_argument(
        "--config", "-c", 
        type=str, 
        default="trading_system/config/trading_config.json",
        help="Path to configuration file"
    )
    
    parser.add_argument(
        "--log-level", "-l", 
        type=str, 
        choices=["debug", "info", "warning", "error", "critical"],
        default="info",
        help="Logging level"
    )
    
    parser.add_argument(
        "--log-dir", 
        type=str, 
        default="logs",
        help="Log directory"
    )
    
    parser.add_argument(
        "--console-only", 
        action="store_true",
        help="Log to console only (no file logging)"
    )
    
    return parser.parse_args()


async def main():
    """Main entry point."""
    # Parse arguments
    args = parse_args()
    
    # Initialize logging
    initialize_logging(
        log_level=args.log_level.upper(),
        file_logging=not args.console_only,
        log_dir=args.log_dir
    )
    
    # Get logger
    logger = get_logger("main")
    
    # Check if config file exists
    if not os.path.exists(args.config):
        logger.error(f"Configuration file not found: {args.config}")
        sys.exit(1)
    
    # Log startup information
    logger.info(f"Starting trading system with config: {args.config}")
    
    # Create controller
    controller = TradingController(args.config)
    
    # Initialize controller
    success = await controller.initialize()
    
    if not success:
        logger.error("Failed to initialize trading controller")
        sys.exit(1)
    
    # Set up signal handlers
    loop = asyncio.get_running_loop()
    
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig,
            lambda: asyncio.create_task(shutdown(controller))
        )
    
    # Run controller
    try:
        await controller.run()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
        await controller.shutdown()
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}", exc_info=True)
        await controller.shutdown()
        sys.exit(1)


async def shutdown(controller):
    """Graceful shutdown."""
    logger = get_logger("main")
    logger.info("Shutting down...")
    
    await controller.shutdown()


if __name__ == "__main__":
    asyncio.run(main()) 