#!/usr/bin/env python
"""
Market Data Subsystem Demo

This example demonstrates the usage of the Market Data Subsystem.
It shows how to initialize the subsystem, register data sources,
and query market data with fallback and validation.
"""

import os
import sys
import json
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List

# Add parent directory to path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import trading system components
from trading_system.core.message_bus import MessageBus
from trading_system.core.logging import initialize_logging
from trading_system.market_data import (
    get_market_data_subsystem,
    MarketDataFacade,
    DataUnifier,
    TradingViewProvider
)
from trading_system.api.nobitex import NobitexAPIClient

# Initialize logging
logger = initialize_logging(level=logging.INFO)

async def process_tradingview_signal(signal: Dict[str, Any]) -> None:
    """Process a TradingView signal."""
    logger.info(f"Received TradingView signal for {signal.get('symbol')}")
    logger.info(f"Signal type: {signal.get('signal_type')}")
    logger.info(f"Action: {signal.get('action')}")
    logger.info(f"Price: {signal.get('price')}")
    logger.info("------------------------------")

async def fetch_and_display_market_data(facade: MarketDataFacade, symbol: str) -> None:
    """Fetch and display market data for a symbol."""
    # Get recent candles
    candles = await facade.get_market_data(
        symbol=symbol,
        timeframe="1h",
        limit=5
    )
    
    logger.info(f"Recent candles for {symbol}:")
    for candle in candles:
        time_str = datetime.fromtimestamp(candle["time"] / 1000).strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"{time_str} | O: {candle['open']} | H: {candle['high']} | L: {candle['low']} | C: {candle['close']} | V: {candle['volume']}")
    
    # Get ticker
    ticker = await facade.get_ticker(symbol)
    if ticker:
        logger.info(f"Current ticker for {symbol}: {ticker['price']}")
    
    # Get order book
    order_book = await facade.get_order_book(symbol, depth=5)
    if order_book:
        logger.info(f"Order book for {symbol}:")
        logger.info("Asks:")
        for ask in order_book["asks"][:5]:
            logger.info(f"  Price: {ask[0]}, Amount: {ask[1]}")
        logger.info("Bids:")
        for bid in order_book["bids"][:5]:
            logger.info(f"  Price: {bid[0]}, Amount: {bid[1]}")
    
    logger.info("------------------------------")

async def simulate_tradingview_signal(tv_provider: TradingViewProvider, symbol: str) -> None:
    """Simulate a TradingView signal."""
    signal = {
        "symbol": symbol,
        "strategy": "MACD Cross",
        "action": "buy",
        "price": 55000.0,
        "message": f"Buy signal for {symbol} at $55000",
        "timestamp": int(datetime.now().timestamp() * 1000)
    }
    
    logger.info(f"Simulating TradingView signal for {symbol}")
    result = await tv_provider.process_signal(signal)
    logger.info(f"Signal processing result: {result}")
    logger.info("------------------------------")

async def register_nobitex_client(facade: MarketDataFacade) -> None:
    """Register a Nobitex API client as a data provider."""
    from trading_system.market_data.market_data_facade import DataProviderType

    # Create Nobitex API client
    api_key = os.environ.get("NOBITEX_API_KEY", "")
    client = NobitexAPIClient(api_key=api_key)
    await client.initialize()
    
    # Create a wrapper provider for the API client
    class NobitexProvider:
        def __init__(self, api_client):
            self.api_client = api_client
            self.name = "NobitexProvider"
        
        async def get_market_data(self, symbol, timeframe, start_time=None, end_time=None, limit=100):
            # Convert to Nobitex symbol format
            nobitex_symbol = symbol.replace("/", "")
            
            # Get candles
            candles = await self.api_client.get_market_stats(nobitex_symbol)
            return self._format_candles(candles, symbol)
        
        async def get_ticker(self, symbol):
            # Convert to Nobitex symbol format
            nobitex_symbol = symbol.replace("/", "")
            
            # Get stats
            stats = await self.api_client.get_market_stats(nobitex_symbol)
            if stats:
                return {
                    "symbol": symbol,
                    "price": float(stats.get("latest", 0)),
                    "timestamp": int(datetime.now().timestamp() * 1000)
                }
            return None
        
        async def get_order_book(self, symbol, depth=10):
            # Convert to Nobitex symbol format
            nobitex_symbol = symbol.replace("/", "")
            
            # Get order book
            try:
                order_book = await self.api_client.get_order_book(nobitex_symbol)
                
                # Format order book
                return {
                    "symbol": symbol,
                    "bids": [[float(p), float(q)] for p, q in order_book.get("bids", [])],
                    "asks": [[float(p), float(q)] for p, q in order_book.get("asks", [])],
                    "timestamp": int(datetime.now().timestamp() * 1000)
                }
            except Exception as e:
                logger.error(f"Error getting order book: {str(e)}")
                return None
        
        def _format_candles(self, stats, symbol):
            # Create a single candle from stats
            if not stats:
                return []
            
            # Get the latest price
            latest = float(stats.get("latest", 0))
            
            # Create a candle with available data
            candle = {
                "time": int(datetime.now().timestamp() * 1000),
                "open": float(stats.get("dayOpen", latest)),
                "high": float(stats.get("dayHigh", latest)),
                "low": float(stats.get("dayLow", latest)),
                "close": latest,
                "volume": float(stats.get("volumeSrc", 0)),
                "symbol": symbol
            }
            
            return [candle]
    
    # Create provider
    provider = NobitexProvider(client)
    
    # Register with facade
    await facade.register_provider(
        provider=provider,
        provider_type=DataProviderType.PRIMARY,
        priority=80,
        name="nobitex"
    )
    
    logger.info("Registered Nobitex provider with MarketDataFacade")
    logger.info("------------------------------")

async def main():
    """Main function to demonstrate the Market Data Subsystem."""
    logger.info("Market Data Subsystem Demo")
    logger.info("==============================")
    
    # Create message bus
    message_bus = MessageBus()
    await message_bus.initialize()
    
    # Configure subsystem
    config = {
        "timeseries_db": {
            "enabled": False,  # Disable for demo
        },
        "tradingview": {
            "enabled": True,
            "validator": {
                "enabled": True,
                "rules": [
                    {
                        "name": "price_threshold",
                        "enabled": True,
                        "params": {"min_price": 100}
                    }
                ]
            }
        },
        "websocket": {
            "nobitex": {
                "enabled": False  # Disable for demo
            }
        },
        "facade": {
            "validation_level": "basic",
            "cache_expiry": 30,
            "selection_strategy": "priority"
        }
    }
    
    # Initialize subsystem
    subsystem = get_market_data_subsystem(config, message_bus)
    init_success = await subsystem.initialize()
    
    if not init_success:
        logger.error("Failed to initialize Market Data Subsystem")
        return
    
    logger.info("Market Data Subsystem initialized successfully")
    logger.info("Component status:")
    for component, status in subsystem.component_status.items():
        logger.info(f"  {component}: {'OK' if status else 'FAILED'}")
    logger.info("------------------------------")
    
    # Get components
    facade = subsystem.facade
    tradingview = subsystem.tradingview
    
    # Register TradingView signal callback
    await message_bus.subscribe("tradingview.signal.processed", process_tradingview_signal)
    
    # Register Nobitex client
    await register_nobitex_client(facade)
    
    # Test TradingView signal
    await simulate_tradingview_signal(tradingview, "BTC/USDT")
    
    # Test market data queries
    symbols = ["BTC/USDT", "ETH/USDT", "LTC/USDT"]
    for symbol in symbols:
        await fetch_and_display_market_data(facade, symbol)
    
    # Get and display stats
    facade_stats = facade.get_stats()
    logger.info("MarketDataFacade Stats:")
    logger.info(f"  Total requests: {facade_stats['stats']['total_requests']}")
    logger.info(f"  Cache hits: {facade_stats['stats']['cache_hits']}")
    logger.info(f"  Failed requests: {facade_stats['stats']['failed_requests']}")
    logger.info(f"  Provider status:")
    for name, status in facade_stats.get('provider_status', {}).items():
        logger.info(f"    {name}: {status}")
    logger.info("------------------------------")
    
    # Wait a bit for any async operations to complete
    await asyncio.sleep(1)
    
    # Shutdown
    await subsystem.shutdown()
    logger.info("Market Data Subsystem shutdown complete")

if __name__ == "__main__":
    # Run the main function
    asyncio.run(main()) 