"""
Time Series Database Test Script

This script tests the TimeSeriesDB integration in the trading system.
It verifies that the connection to InfluxDB is working and can store and retrieve data.

Usage:
    py -3.10 trading_system/tests/market_data/test_timeseries_db.py
    
    Or use the wrapper script:
    py -3.10 test_db.py
"""

import asyncio
import sys
import time
import datetime
import logging
from typing import Dict, Any, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("timeseries_test")

# Import TimeSeriesDB
try:
    from trading_system.market_data.timeseries_db import TimeSeriesDB, get_timeseries_db
except ImportError:
    logger.error("Failed to import TimeSeriesDB. Make sure the trading_system package is in your Python path.")
    sys.exit(1)

async def test_store_retrieve_candle() -> bool:
    """Test storing and retrieving a candle."""
    try:
        # Get TimeSeriesDB instance
        logger.info("Getting TimeSeriesDB instance...")
        db = get_timeseries_db()
        
        # Test data
        symbol = "BTC/USDT"
        timeframe = "1h"
        timestamp = int(time.time() * 1000)
        
        # Create test candle
        candle = {
            "symbol": symbol,
            "timeframe": timeframe,
            "time": timestamp,
            "open": 50000.0,
            "high": 51000.0,
            "low": 49000.0,
            "close": 50500.0,
            "volume": 100.0
        }
        
        # Store the candle
        logger.info(f"Storing test candle for {symbol}...")
        success = db.store_market_data(symbol, timeframe, candle)
        
        if not success:
            logger.error("Failed to store candle")
            return False
        
        logger.info("Successfully stored test candle")
        
        # Give InfluxDB a moment to process the write
        await asyncio.sleep(1)
        
        # Retrieve the candle
        start_time = timestamp - 60000  # 1 minute before
        end_time = timestamp + 60000    # 1 minute after
        
        logger.info(f"Retrieving candle data for {symbol}...")
        candles = db.get_candlestick_data(
            symbol=symbol,
            interval=timeframe,
            start_time=start_time,
            end_time=end_time
        )
        
        if candles.empty:
            logger.error("Failed to retrieve candle data")
            return False
        
        logger.info(f"Successfully retrieved {len(candles)} candles")
        logger.info(f"Candle data: {candles.iloc[0].to_dict()}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error in test: {str(e)}")
        return False

async def test_query_performance() -> bool:
    """Test query performance with multiple requests."""
    try:
        # Get TimeSeriesDB instance
        db = get_timeseries_db()
        
        # Test symbols
        symbols = ["BTC/USDT", "ETH/USDT", "XRP/USDT"]
        timeframe = "1h"
        
        # Store some test data for each symbol
        logger.info("Storing test data for multiple symbols...")
        
        for symbol in symbols:
            # Create test candles for the last 24 hours
            current_time = int(time.time() * 1000)
            for i in range(24):
                candle_time = current_time - (i * 3600 * 1000)  # Go back i hours
                
                candle = {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "time": candle_time,
                    "open": 1000.0 + i,
                    "high": 1100.0 + i,
                    "low": 900.0 + i,
                    "close": 1050.0 + i,
                    "volume": 100.0 + i
                }
                
                db.store_market_data(symbol, timeframe, candle)
        
        logger.info("Successfully stored test data for multiple symbols")
        
        # Give InfluxDB a moment to process the writes
        await asyncio.sleep(2)
        
        # Test query performance for each symbol
        logger.info("Testing query performance...")
        start_time = int(time.time() * 1000) - (24 * 3600 * 1000)  # 24 hours ago
        end_time = int(time.time() * 1000)
        
        total_time = 0
        for symbol in symbols:
            start = time.time()
            
            candles = db.get_candlestick_data(
                symbol=symbol,
                interval=timeframe,
                start_time=start_time,
                end_time=end_time
            )
            
            query_time = time.time() - start
            total_time += query_time
            
            logger.info(f"Retrieved {len(candles)} candles for {symbol} in {query_time:.4f} seconds")
        
        avg_time = total_time / len(symbols)
        logger.info(f"Average query time: {avg_time:.4f} seconds")
        
        return not any(db.get_candlestick_data(symbol, timeframe, start_time, end_time).empty for symbol in symbols)
        
    except Exception as e:
        logger.error(f"Error in query performance test: {str(e)}")
        return False

async def test_concurrent_access() -> bool:
    """Test concurrent access to the database."""
    try:
        # Get TimeSeriesDB instance
        db = get_timeseries_db()
        
        # Test data
        symbol = "BTC/USDT"
        timeframe = "1m"
        
        # Create tasks for concurrent writes
        logger.info("Testing concurrent writes...")
        
        async def write_data(i: int) -> bool:
            try:
                timestamp = int(time.time() * 1000) + i
                
                candle = {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "time": timestamp,
                    "open": 50000.0 + i,
                    "high": 51000.0 + i,
                    "low": 49000.0 + i,
                    "close": 50500.0 + i,
                    "volume": 100.0 + i
                }
                
                return db.store_market_data(symbol, timeframe, candle)
            
            except Exception as e:
                logger.error(f"Error in concurrent write {i}: {str(e)}")
                return False
        
        # Create and run 10 concurrent write tasks
        write_tasks = [write_data(i) for i in range(10)]
        results = await asyncio.gather(*write_tasks)
        
        if not all(results):
            logger.error("Not all concurrent writes succeeded")
            return False
        
        logger.info("All concurrent writes succeeded")
        
        # Wait for InfluxDB to process writes
        await asyncio.sleep(2)
        
        # Test concurrent reads
        logger.info("Testing concurrent reads...")
        
        async def read_data(i: int) -> bool:
            try:
                start_time = int(time.time() * 1000) - 3600000  # 1 hour ago
                end_time = int(time.time() * 1000)
                
                candles = db.get_candlestick_data(
                    symbol=symbol,
                    interval=timeframe,
                    start_time=start_time,
                    end_time=end_time
                )
                
                return not candles.empty
            
            except Exception as e:
                logger.error(f"Error in concurrent read {i}: {str(e)}")
                return False
        
        # Create and run 10 concurrent read tasks
        read_tasks = [read_data(i) for i in range(10)]
        read_results = await asyncio.gather(*read_tasks)
        
        if not all(read_results):
            logger.error("Not all concurrent reads succeeded")
            return False
        
        logger.info("All concurrent reads succeeded")
        return True
        
    except Exception as e:
        logger.error(f"Error in concurrent access test: {str(e)}")
        return False

async def run_tests() -> None:
    """Run all tests."""
    logger.info("Starting TimeSeriesDB tests...")
    
    test_results = {
        "Store and Retrieve Candle": await test_store_retrieve_candle(),
        "Query Performance": await test_query_performance(),
        "Concurrent Access": await test_concurrent_access()
    }
    
    # Print test results
    logger.info("\n=== Test Results ===")
    all_passed = True
    
    for test_name, result in test_results.items():
        status = "PASSED" if result else "FAILED"
        logger.info(f"{test_name}: {status}")
        if not result:
            all_passed = False
    
    if all_passed:
        logger.info("\n✅ All tests passed! TimeSeriesDB is working correctly.")
    else:
        logger.error("\n❌ Some tests failed. Please check the logs for details.")
        logger.info("To fix issues, run 'py -3.10 setup_db.py'")

def main() -> None:
    """Main entry point."""
    try:
        # Run the tests
        asyncio.run(run_tests())
    except KeyboardInterrupt:
        logger.info("Tests interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")

if __name__ == "__main__":
    main() 