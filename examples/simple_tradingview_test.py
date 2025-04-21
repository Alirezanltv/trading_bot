"""
Simple TradingView Integration Test

This example demonstrates a minimal working example of TradingView integration.
"""

import asyncio
import logging
from typing import Dict, Any

from trading_system.core.logging import initialize_logging, get_logger
from trading_system.core.message_bus import AsyncMessageBus
from trading_system.market_data.market_data_validator import validate_market_data
from trading_system.market_data.timeseries_db import get_timeseries_db

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("simple_test")

# Global variables
message_bus = None

async def handle_market_data(message_type, data: Dict[str, Any]) -> None:
    """
    Handle market data messages.
    
    Args:
        message_type: Message type
        data: Market data
    """
    logger.info(f"Received market data: {data.get('symbol', 'unknown')} - {data.get('timeframe', 'unknown')}")
    
    # Validate the data
    is_valid = validate_market_data(data, "candle")
    logger.info(f"Data validation result: {'Valid' if is_valid else 'Invalid'}")
    
    # Store in database if valid
    if is_valid:
        db = get_timeseries_db()
        success = db.store_market_data(
            data.get("symbol"), 
            data.get("timeframe"), 
            data
        )
        logger.info(f"Data storage result: {'Success' if success else 'Failed'}")

async def main():
    """Main entry point for the test."""
    logger.info("Starting simple TradingView integration test")
    
    # Create a message bus
    global message_bus
    message_bus = AsyncMessageBus()
    await message_bus.initialize()
    logger.info("Message bus initialized")
    
    # Test validate_market_data function
    sample_data = {
        "symbol": "BTC/USDT",
        "timeframe": "1h",
        "open": 50000.0,
        "high": 51000.0,
        "low": 49000.0,
        "close": 50500.0,
        "volume": 100.0,
        "time": 1617235200000  # Unix timestamp in ms
    }
    
    validation_result = validate_market_data(sample_data, "candle")
    logger.info(f"Sample data validation result: {validation_result}")
    
    # Test TimeSeriesDB singleton
    db = get_timeseries_db()
    logger.info(f"TimeSeriesDB instance: {db}")
    
    # Keep the test running for 5 seconds
    logger.info("Test running for 5 seconds...")
    await asyncio.sleep(5)
    
    logger.info("Test completed successfully")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
    except Exception as e:
        logger.error(f"Error in test: {str(e)}", exc_info=True) 