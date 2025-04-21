#!/usr/bin/env python
"""
TradingView integration test script.

This script tests the TradingView market data integration.
"""

import os
import sys
import json
import asyncio
import logging
import requests
from pprint import pprint
from datetime import datetime
from typing import Dict, Any, List

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from trading_system.core import setup_logging, get_logger
from trading_system.market_data.manager import market_data_manager
from trading_system.market_data.tradingview import TradingViewCredentials, TradingViewDataProvider
from trading_system.market_data.tradingview_webhook import webhook_handler

# Set up logging
setup_logging()
logger = get_logger("tradingview_test")

async def test_tradingview_setup():
    """Test TradingView setup."""
    logger.info("=== Testing TradingView Setup ===")
    
    # Set up configuration
    config = {
        "tradingview": {
            "username": os.getenv("TRADINGVIEW_USERNAME"),
            "password": os.getenv("TRADINGVIEW_PASSWORD"),
            "session_token": os.getenv("TRADINGVIEW_SESSION_TOKEN"),
            "webhook": {
                "host": "127.0.0.1",
                "port": 8080,
                "path": "/webhook/tradingview",
                "api_key": "test_api_key"
            }
        }
    }
    
    # Configure market data manager
    market_data_manager.config = config
    
    # Initialize market data manager
    success = await market_data_manager.initialize()
    logger.info(f"Market data manager initialization: {'Success' if success else 'Failed'}")
    
    if not success:
        logger.error("Failed to initialize market data manager")
        return False
    
    # Start market data manager
    success = await market_data_manager.start()
    logger.info(f"Market data manager start: {'Success' if success else 'Failed'}")
    
    if not success:
        logger.error("Failed to start market data manager")
        await market_data_manager.stop()
        return False
    
    logger.info("TradingView setup successful")
    return True

async def test_market_data_fetch():
    """Test market data fetch."""
    logger.info("=== Testing Market Data Fetch ===")
    
    # Test symbols
    symbols = ["BINANCE:BTCUSDT", "NASDAQ:AAPL", "FOREX:EURUSD"]
    
    for symbol in symbols:
        logger.info(f"Fetching market data for {symbol}")
        
        # Fetch market data
        market_data = await market_data_manager.fetch_market_data(
            symbol=symbol,
            timeframe="1D"
        )
        
        if market_data:
            logger.info(f"Successfully fetched market data for {symbol}")
            logger.info(f"Data points: {len(market_data.get('ohlcv', []))}")
            
            # Print last candle
            if market_data.get('ohlcv') and len(market_data['ohlcv']) > 0:
                last_candle = market_data['ohlcv'][-1]
                timestamp = datetime.fromtimestamp(last_candle.get('timestamp', 0)).strftime('%Y-%m-%d %H:%M:%S')
                logger.info(f"Last candle ({timestamp}): Open: {last_candle.get('open')}, "
                          f"High: {last_candle.get('high')}, Low: {last_candle.get('low')}, "
                          f"Close: {last_candle.get('close')}, Volume: {last_candle.get('volume')}")
            
            # Print market trend
            if 'trend' in market_data:
                logger.info(f"Market trend: {market_data['trend']}")
            
            # Print indicators
            if 'rsi' in market_data and market_data['rsi']:
                logger.info(f"RSI (14): {market_data['rsi'][-1]:.2f}")
            
            if 'macd' in market_data and market_data['macd'].get('histogram'):
                logger.info(f"MACD Histogram: {market_data['macd']['histogram'][-1]:.2f}")
            
            if 'bollinger_bands' in market_data and market_data['bollinger_bands'].get('upper'):
                bb = market_data['bollinger_bands']
                logger.info(f"Bollinger Bands: Upper: {bb['upper'][-1]:.2f}, "
                          f"Middle: {bb['middle'][-1]:.2f}, Lower: {bb['lower'][-1]:.2f}")
        else:
            logger.error(f"Failed to fetch market data for {symbol}")
        
        print("\n")
    
    return True

async def test_webhook_handler():
    """Test webhook handler."""
    logger.info("=== Testing Webhook Handler ===")
    
    # Webhook URL
    webhook_url = "http://127.0.0.1:8080/webhook/tradingview"
    
    # Create test signal
    signal = {
        "strategy": {
            "name": "Test Strategy",
            "action": "buy"
        },
        "ticker": "BINANCE:BTCUSDT",
        "price": 50000,
        "time": datetime.now().timestamp(),
        "interval": "1h",
        "exchange": "BINANCE"
    }
    
    # Send webhook request
    try:
        logger.info(f"Sending test webhook: {json.dumps(signal)}")
        
        headers = {
            "Content-Type": "application/json",
            "X-TradingView-Key": "test_api_key"
        }
        
        response = requests.post(webhook_url, json=signal, headers=headers)
        
        logger.info(f"Webhook response: {response.status_code} - {response.text}")
        
        if response.status_code == 200:
            logger.info("Webhook test successful")
            
            # Wait for signal to be processed
            await asyncio.sleep(2)
            
            # Check signal cache
            signals = await market_data_manager.get_latest_signals(
                symbol="BINANCE:BTCUSDT",
                signal_type="buy",
                limit=1
            )
            
            if signals and len(signals) > 0:
                logger.info("Signal successfully stored in cache")
                logger.info(f"Signal: {signals[0]}")
            else:
                logger.error("Signal not found in cache")
            
            return True
        else:
            logger.error(f"Webhook test failed: {response.status_code} - {response.text}")
            return False
        
    except Exception as e:
        logger.error(f"Error sending webhook: {str(e)}", exc_info=True)
        return False

async def cleanup():
    """Clean up resources."""
    logger.info("=== Cleaning Up ===")
    
    # Stop market data manager
    await market_data_manager.stop()
    logger.info("Market data manager stopped")

async def main():
    """Run all tests."""
    logger.info("Starting TradingView integration tests")
    
    try:
        # Test TradingView setup
        setup_success = await test_tradingview_setup()
        if not setup_success:
            logger.error("TradingView setup failed, aborting tests")
            return
        
        # Test market data fetch
        await test_market_data_fetch()
        print("\n")
        
        # Test webhook handler
        await test_webhook_handler()
        print("\n")
        
    except Exception as e:
        logger.error(f"Error during tests: {str(e)}", exc_info=True)
    
    finally:
        # Clean up
        await cleanup()
    
    logger.info("All tests completed")

if __name__ == "__main__":
    asyncio.run(main()) 