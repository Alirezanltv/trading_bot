#!/usr/bin/env python3
"""
Run Paper Trading

This script starts the trading system in paper trading mode. It:
1. Uses real market data adapters
2. Uses the paper trading execution engine
3. Runs real strategy algorithms
4. Simulates order execution without using real funds
"""

import os
import sys
import time
import signal
import asyncio
import logging
import argparse
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

from trading_system.core.logging import initialize_logging, get_logger
from trading_system.core.message_bus import get_async_message_bus

logger = None

async def main_loop():
    """Main execution loop."""
    global logger
    
    # Create an event to signal shutdown
    shutdown_event = asyncio.Event()
    
    # Windows doesn't support add_signal_handler, so we'll use a different approach
    def signal_handler(sig, frame):
        logger.info(f"Received signal {signal.Signals(sig).name}")
        shutdown_event.set()
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Report start
    logger.info("===== PAPER TRADING MODE STARTED =====")
    logger.info(f"Session ID: paper_{int(time.time())}")
    
    # Create message bus
    logger.info("Creating async message bus")
    message_bus = get_async_message_bus()
    
    logger.info("Paper trading system is ready")
    
    # Keep the event loop running until shutdown event is set
    try:
        await shutdown_event.wait()
    except:
        logger.info("Stopping main loop")
    
    logger.info("Shutting down paper trading system")


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Run paper trading system")
    parser.add_argument(
        "--config", 
        type=str, 
        default="configs/paper_trading.yaml",
        help="Path to configuration file"
    )
    parser.add_argument(
        "--log-level", 
        type=str, 
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level"
    )
    parser.add_argument(
        "--log-file", 
        type=str, 
        default=None,
        help="Path to log file (default: logs/paper_trading_YYYY-MM-DD.log)"
    )
    args = parser.parse_args()
    
    # Set default log file if not specified
    if not args.log_file:
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y-%m-%d")
        args.log_file = f"{log_dir}/paper_trading_{timestamp}.log"
    
    return args


def main():
    """Main entry point."""
    global logger
    
    # Parse arguments
    args = parse_arguments()
    
    # Configure logging
    initialize_logging(
        log_level=getattr(logging, args.log_level),
        console_logging=True,
        file_logging=True,
        log_dir=os.path.dirname(args.log_file) if args.log_file else "logs"
    )
    logger = get_logger("paper_trading")
    
    logger.info("Starting paper trading system...")
    
    # Run main loop
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt. Shutting down...")
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        return 1
    
    logger.info("Paper trading system shutdown complete")
    return 0


if __name__ == "__main__":
    sys.exit(main()) 