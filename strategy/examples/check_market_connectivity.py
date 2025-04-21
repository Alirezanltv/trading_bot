#!/usr/bin/env python
"""
Market Data Connectivity Check

Simple script to verify connectivity to the Market Data Facade
and ensure that market data is flowing correctly.
"""

import asyncio
import os
import sys
import time
import logging
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

# Import and apply patches to fix compatibility issues
from trading_system.strategy.examples.enum_fix import *
from trading_system.strategy.examples.message_bus_patch import *
from trading_system.strategy.examples.provider_fix import *

# Import modules from the trading system
from trading_system.core.message_bus import MessageBus
from trading_system.core.logging import initialize_logging, get_logger
from trading_system.market_data.market_data_facade import MarketDataFacade

# Setup logging
initialize_logging()
logger = get_logger("connectivity_check")

class MarketDataConnectivityCheck:
    """Simple utility to check market data connectivity."""
    
    def __init__(self, symbol="BTC/USDT", timeframe="1m", source="tradingview"):
        """
        Initialize the connectivity checker.
        
        Args:
            symbol: Trading symbol to check
            timeframe: Chart timeframe to check
            source: Market data source
        """
        self.symbol = symbol
        self.timeframe = timeframe
        self.source = source
        
        # Components
        self.message_bus = None
        self.market_data = None
        
        # State
        self.data_points_received = 0
        self.last_data_time = None
        self.last_price = None
        self.start_time = None
        self.running = False

    async def initialize(self):
        """Initialize the message bus and market data facade."""
        logger.info(f"Initializing connectivity check for {self.symbol} {self.timeframe}...")
        
        # Create message bus
        self.message_bus = MessageBus()
        
        # Create a config dictionary for the market data facade
        config = {
            "validation_level": "basic",
            "message_bus": self.message_bus,
            "cache_size": 1000,
            "data_sources": {
                "tradingview": {
                    "enabled": True
                },
                "nobitex": {
                    "api_key": os.environ.get("NOBITEX_API_KEY", ""),
                    "api_secret": os.environ.get("NOBITEX_API_SECRET", ""),
                    "enabled": True if os.environ.get("NOBITEX_API_KEY") else False
                }
            }
        }
        
        try:
            # Create market data facade with config only (don't pass message_bus twice)
            self.market_data = MarketDataFacade(config)
            await self.market_data.initialize()
            
            # Register message handler - properly await this if it's async
            if asyncio.iscoroutinefunction(self.message_bus.subscribe):
                await self.message_bus.subscribe("market_data", self.handle_market_data)
            else:
                self.message_bus.subscribe("market_data", self.handle_market_data)
            
            logger.info("Initialization complete")
        except Exception as e:
            logger.error(f"Error initializing market data facade: {str(e)}")
            raise

    def handle_market_data(self, message):
        """
        Handle incoming market data.
        
        Args:
            message: Market data message
        """
        # Check if message is a proper message object with data attribute
        if hasattr(message, 'data'):
            data = message.data
        else:
            # If message itself is the data
            data = message
            
        if not isinstance(data, dict):
            logger.warning(f"Received non-dictionary message data: {type(data)}")
            return
            
        # Check if this is for our subscribed symbol/timeframe
        if data.get("symbol") == self.symbol and data.get("timeframe") == self.timeframe:
            self.data_points_received += 1
            self.last_data_time = datetime.now()
            
            # Extract and log the price if available
            candle_data = data.get("data")
            if isinstance(candle_data, dict) and "close" in candle_data:
                self.last_price = candle_data["close"]
                
                # Log every nth data point to avoid console spam
                if self.data_points_received % 5 == 0:
                    logger.info(f"Received {self.data_points_received} data points | Latest close price: {self.last_price}")
            else:
                # Log raw data if not in expected format
                logger.info(f"Received data: {data}")

    async def run(self, duration_seconds=60):
        """
        Run the connectivity check for the specified duration.
        
        Args:
            duration_seconds: Duration to run the check
        """
        self.running = True
        self.start_time = time.time()
        self.data_points_received = 0
        
        logger.info(f"Starting market data connectivity check for {duration_seconds} seconds")
        logger.info(f"Checking {self.symbol} on {self.timeframe} timeframe from {self.source}")
        
        try:
            # Initialize components
            await self.initialize()
            
            # Since there's no subscribe method, we'll directly request market data periodically
            # to simulate streaming data
            logger.info(f"Starting to poll for {self.symbol} {self.timeframe} data...")
            
            # Run for specified duration
            end_time = time.time() + duration_seconds
            
            while self.running and time.time() < end_time:
                # Calculate progress
                elapsed = time.time() - self.start_time
                remaining = end_time - time.time()
                
                # Request market data every few seconds
                try:
                    # Get current market data
                    data = await self.market_data.get_market_data(
                        symbol=self.symbol,
                        timeframe=self.timeframe,
                        limit=1  # Just get the latest candle
                    )
                    
                    if data and isinstance(data, list) and len(data) > 0:
                        # Process the data as if it came through the message bus
                        candle = data[0]
                        message_data = {
                            "symbol": self.symbol,
                            "timeframe": self.timeframe,
                            "data": candle
                        }
                        await asyncio.to_thread(self.handle_market_data, message_data)
                except Exception as e:
                    logger.error(f"Error fetching market data: {str(e)}")
                
                # Report progress every 10 seconds
                if int(elapsed) % 10 == 0 and int(elapsed) > 0:
                    data_rate = self.data_points_received / elapsed if elapsed > 0 else 0
                    logger.info(f"Progress: {elapsed:.1f}s/{duration_seconds}s | Data points: {self.data_points_received} | Rate: {data_rate:.2f}/s")
                
                # Small delay to prevent CPU hogging
                await asyncio.sleep(5)  # Poll every 5 seconds
                
            # Print summary
            self.print_summary()
            
        except Exception as e:
            logger.error(f"Error during connectivity check: {str(e)}")
        finally:
            # Clean up
            if self.running:
                await self.shutdown()

    async def shutdown(self):
        """Shut down the connectivity check."""
        if not self.running:
            return
            
        self.running = False
        logger.info("Shutting down connectivity check...")
        
        # Safety check to ensure market_data is not None
        if self.market_data is not None:
            try:
                # Call the proper shutdown method
                await self.market_data.shutdown()
            except Exception as e:
                logger.error(f"Error during shutdown: {str(e)}")
            
        logger.info("Shutdown complete")

    def print_summary(self):
        """Print a summary of the connectivity check results."""
        elapsed = time.time() - self.start_time
        data_rate = self.data_points_received / elapsed if elapsed > 0 else 0
        
        logger.info("=" * 50)
        logger.info("MARKET DATA CONNECTIVITY CHECK RESULTS")
        logger.info("=" * 50)
        logger.info(f"Symbol:           {self.symbol}")
        logger.info(f"Timeframe:        {self.timeframe}")
        logger.info(f"Data source:      {self.source}")
        logger.info(f"Duration:         {elapsed:.2f} seconds")
        logger.info(f"Data points:      {self.data_points_received}")
        logger.info(f"Data rate:        {data_rate:.2f} points per second")
        logger.info(f"Last price:       {self.last_price}")
        logger.info(f"Last update:      {self.last_data_time}")
        
        # Assess connectivity
        if self.data_points_received > 0:
            logger.info("STATUS:           SUCCESS - Data is flowing")
        else:
            logger.error("STATUS:           FAILED - No data received")
            
        logger.info("=" * 50)
        
        # Also write summary to a file
        try:
            with open("connectivity_check_results.txt", "w") as f:
                f.write("MARKET DATA CONNECTIVITY CHECK RESULTS\n")
                f.write("=" * 50 + "\n")
                f.write(f"Symbol:           {self.symbol}\n")
                f.write(f"Timeframe:        {self.timeframe}\n")
                f.write(f"Data source:      {self.source}\n")
                f.write(f"Duration:         {elapsed:.2f} seconds\n")
                f.write(f"Data points:      {self.data_points_received}\n")
                f.write(f"Data rate:        {data_rate:.2f} points per second\n")
                f.write(f"Last price:       {self.last_price}\n")
                f.write(f"Last update:      {self.last_data_time}\n")
                f.write("\n")
                if self.data_points_received > 0:
                    f.write("STATUS:           SUCCESS - Data is flowing\n")
                else:
                    f.write("STATUS:           FAILED - No data received\n")
                f.write("=" * 50 + "\n")
            logger.info("Results summary written to connectivity_check_results.txt")
        except Exception as e:
            logger.error(f"Error writing results to file: {str(e)}")


async def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Check Market Data Facade connectivity")
    parser.add_argument("--symbol", type=str, default="BTC/USDT", help="Symbol to check (default: BTC/USDT)")
    parser.add_argument("--timeframe", type=str, default="1m", help="Timeframe to check (default: 1m)")
    parser.add_argument("--source", type=str, default="tradingview", help="Market data source (default: tradingview)")
    parser.add_argument("--duration", type=int, default=60, help="Check duration in seconds (default: 60)")
    
    args = parser.parse_args()
    
    checker = MarketDataConnectivityCheck(
        symbol=args.symbol,
        timeframe=args.timeframe,
        source=args.source
    )
    
    await checker.run(args.duration)


if __name__ == "__main__":
    asyncio.run(main()) 