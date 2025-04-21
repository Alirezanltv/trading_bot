#!/usr/bin/env python3
"""
Test RSI Strategy

This script tests the RSI Strategy implementation with mock market data.
"""

import os
import sys
import json
import time
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'rsi_strategy_test_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)

logger = logging.getLogger("rsi_test")

# Import test mocks
from trading_system.tests.mocks import MockMarketDataProvider
from trading_system.strategy.rsi_strategy import RSIStrategy
from trading_system.strategy.base import SignalType

def run_strategy_test():
    """Run the RSI strategy test."""
    # Create necessary directories
    os.makedirs("data/test", exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    
    # Create test config
    config = {
        "id": "rsi_strategy_test",
        "type": "rsi",
        "symbols": ["btc-usdt"],
        "timeframe": "1h",
        "rsi_period": 14,
        "oversold_threshold": 30,
        "overbought_threshold": 70,
        "exit_threshold": 50,
        "signal_smoothing": 1
    }
    
    # Create strategy instance
    strategy = RSIStrategy(config, "rsi_strategy_test")
    
    # Create mock market data provider
    market_data = MockMarketDataProvider({
        "symbols": ["btc-usdt"],
        "data_directory": "data/test/market_data",
        "volatility": 0.05,  # Higher volatility for testing
        "trend": 0.01  # Slight upward trend
    })
    
    # Start components
    logger.info("Starting components...")
    market_data.start()
    strategy.start()
    
    try:
        # Feed data to the strategy
        symbol = "btc-usdt"
        timeframe = "1h"
        
        logger.info("Feeding initial market data to strategy...")
        for _ in range(30):  # Generate 30 candles
            data = market_data.get_data(symbol)
            ohlcv = market_data.get_historical_data(symbol, timeframe, 30)
            
            if ohlcv:
                # Convert to the format expected by the strategy
                strategy_data = {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "timestamp": datetime.now().timestamp(),
                    "ohlcv": ohlcv
                }
                
                # Update strategy
                strategy.update(strategy_data)
        
        # Now generate some signals
        logger.info("Generating signals...")
        for i in range(10):
            # Update market data
            market_data.update(symbol)
            data = market_data.get_data(symbol)
            ohlcv = market_data.get_historical_data(symbol, timeframe, 30)
            
            if ohlcv:
                # Convert to the format expected by the strategy
                strategy_data = {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "timestamp": datetime.now().timestamp(),
                    "ohlcv": ohlcv
                }
                
                # Update strategy
                strategy.update(strategy_data)
            
            # Generate signal
            signal_type, confidence, metadata = strategy.generate_signal(symbol, timeframe)
            
            # Log the signal
            current_price = data["last"]
            rsi_value = metadata.get("rsi_value", "N/A") if metadata else "N/A"
            logger.info(f"Signal {i+1}: {signal_type.name} with {confidence:.2f} confidence")
            logger.info(f"  Price: {current_price:.2f}, RSI: {rsi_value}")
            logger.info(f"  Metadata: {metadata}")
            
            # Wait a bit to simulate time passing
            time.sleep(0.5)
        
        logger.info("Test completed successfully")
        
    finally:
        # Stop components
        strategy.stop()
        market_data.stop()

if __name__ == "__main__":
    run_strategy_test() 