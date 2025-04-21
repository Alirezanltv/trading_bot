"""
TradingView Real-Time Data Integration Test

This example demonstrates the real-time market data collection from TradingView.
"""

import os
import sys
import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, Any, List

# Add the repository root to the Python path
script_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(os.path.dirname(script_dir))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from trading_system.core.logging import initialize_logging, get_logger
from trading_system.core.message_bus import MessageBus, MessageTypes, AsyncMessageBus
from trading_system.market_data.tradingview_realtime import TradingViewRealtimeStream
from trading_system.market_data.timeseries_db import TimeSeriesDB, get_timeseries_db

# Initialize logging
initialize_logging(log_level=logging.INFO)
logger = get_logger("examples.tradingview_test")


async def market_data_callback(candles: List[Dict[str, Any]]) -> None:
    """
    Callback function for market data updates.
    
    Args:
        candles: List of candle data
    """
    for candle in candles:
        dt = datetime.fromtimestamp(candle["timestamp"] / 1000)
        logger.info(
            f"Callback: {candle['symbol']} ({candle['timeframe']}) - "
            f"{dt.strftime('%Y-%m-%d %H:%M:%S')} "
            f"O: {candle['open']:.2f}, H: {candle['high']:.2f}, "
            f"L: {candle['low']:.2f}, C: {candle['close']:.2f}, V: {candle['volume']:.2f}"
        )


async def run_test() -> None:
    """Run the TradingView data integration test."""
    try:
        logger.info("Starting TradingView data integration test")
        
        # Create message bus
        message_bus = AsyncMessageBus()
        await message_bus.initialize()
        
        # Subscribe to market data messages
        await message_bus.subscribe(
            message_type=MessageTypes.MARKET_DATA_UPDATE,
            callback=lambda message_type, data: logger.info(f"Message received: {data.get('symbol', 'unknown')} - {data.get('timeframe', 'unknown')}")
        )
        
        # Create TradingView data stream
        config = {
            "symbols": ["BTC/USDT", "ETH/USDT", "XRP/USDT"],
            "timeframes": ["1m", "5m", "15m", "1h"],
            "backup_urls": [
                "wss://data.tradingview.com/socket.io/websocket",
                "wss://prodata.tradingview.com/socket.io/websocket"
            ]
        }
        
        tv_stream = TradingViewRealtimeStream(config, message_bus)
        
        # Initialize
        success = await tv_stream.initialize()
        if not success:
            logger.error("Failed to initialize TradingView stream")
            return
        
        # Subscribe to symbols
        for symbol in config["symbols"]:
            for timeframe in config["timeframes"]:
                await tv_stream.subscribe(symbol, timeframe, market_data_callback)
                logger.info(f"Subscribed to {symbol} ({timeframe})")
        
        # Run for a while to collect data
        logger.info("Collecting data for 5 minutes...")
        await asyncio.sleep(300)  # 5 minutes
        
        # Cleanup
        await tv_stream.shutdown()
        await message_bus.shutdown()
        
        logger.info("TradingView data integration test completed")
        
    except Exception as e:
        logger.error(f"Error in TradingView data integration test: {str(e)}", exc_info=True)


def main() -> None:
    """Main entry point."""
    try:
        # Run the test
        asyncio.run(run_test())
        
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)


if __name__ == "__main__":
    main() 