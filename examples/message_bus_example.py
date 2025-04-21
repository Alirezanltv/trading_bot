#!/usr/bin/env python
"""
Message Bus Example

This script demonstrates the usage of the message bus with RabbitMQ.
"""

import asyncio
import os
import sys
import time
import signal
import threading
from typing import Dict, Any

# Add trading_system to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

# Set message bus mode to RabbitMQ
os.environ["TRADING_SYSTEM_MESSAGE_BUS_MODE"] = "rabbitmq"

from trading_system.core.message_bus import (
    message_bus, 
    get_async_message_bus, 
    MessageTypes, 
    MessageBusMode
)
from trading_system.core.logging import get_logger

logger = get_logger("examples.message_bus")

# Flag to control the running state
running = True


def handle_signal(signum, frame):
    """Handle termination signals."""
    global running
    logger.info(f"Received signal {signum}, shutting down...")
    running = False


async def async_message_handler(message_type: MessageTypes, data: Dict[str, Any]) -> None:
    """
    Async message handler for market data updates.
    
    Args:
        message_type: Message type
        data: Message data
    """
    logger.info(f"ASYNC - Received {message_type.value}: {data}")


def sync_message_handler(message_type: MessageTypes, data: Dict[str, Any]) -> None:
    """
    Synchronous message handler for system messages.
    
    Args:
        message_type: Message type
        data: Message data
    """
    logger.info(f"SYNC - Received {message_type.value}: {data}")


async def async_publisher():
    """Asynchronous message publisher."""
    logger.info("Starting async publisher...")
    
    # Get async message bus
    async_bus = get_async_message_bus()
    
    # Connect to message broker
    await async_bus.connect()
    
    # Subscribe to market data updates
    await async_bus.subscribe(MessageTypes.MARKET_DATA_UPDATE, async_message_handler)
    
    # Publish messages
    counter = 0
    while running:
        try:
            # Create message data
            data = {
                "timestamp": time.time(),
                "symbol": "BTC/USDT",
                "price": 50000.0 + counter,
                "volume": 1.0,
                "source": "async"
            }
            
            # Publish message
            await async_bus.publish(MessageTypes.MARKET_DATA_UPDATE, data)
            logger.info(f"ASYNC - Published message: {counter}")
            
            # Increment counter
            counter += 1
            
            # Wait
            await asyncio.sleep(1.0)
            
        except Exception as e:
            logger.error(f"Error in async publisher: {str(e)}")
            await asyncio.sleep(1.0)
    
    # Shutdown
    await async_bus.shutdown()
    logger.info("Async publisher stopped")


def sync_publisher():
    """Synchronous message publisher."""
    logger.info("Starting sync publisher...")
    
    # Subscribe to system messages
    message_bus.subscribe(MessageTypes.SYSTEM_STATUS, sync_message_handler)
    
    # Publish messages
    counter = 0
    while running:
        try:
            # Create message data
            data = {
                "timestamp": time.time(),
                "status": "running" if counter % 2 == 0 else "degraded",
                "uptime": counter,
                "source": "sync"
            }
            
            # Publish message
            message_bus.publish_sync(MessageTypes.SYSTEM_STATUS, data)
            logger.info(f"SYNC - Published message: {counter}")
            
            # Increment counter
            counter += 1
            
            # Wait
            time.sleep(2.0)
            
        except Exception as e:
            logger.error(f"Error in sync publisher: {str(e)}")
            time.sleep(1.0)
    
    # Shutdown
    message_bus.shutdown()
    logger.info("Sync publisher stopped")


async def main():
    """Main function."""
    # Set up signal handlers
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    
    logger.info(f"Starting message bus example with mode: {message_bus.get_mode().value}")
    
    # Start sync publisher in a separate thread
    sync_thread = threading.Thread(target=sync_publisher, daemon=True)
    sync_thread.start()
    
    # Start async publisher
    await async_publisher()
    
    # Wait for sync thread to finish
    if sync_thread.is_alive():
        sync_thread.join(timeout=2.0)
    
    logger.info("Example finished")


if __name__ == "__main__":
    try:
        # Run main function
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}", exc_info=True)
    finally:
        logger.info("Example exiting") 