"""
Simulated Market Data Provider

This module provides simulated market data for testing strategies
without requiring real market connections.
"""

import os
import sys
import time
import random
import logging
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta

# Add the repository root to the Python path
script_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(os.path.dirname(script_dir))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from trading_system.core.logging import get_logger

# Configure logging - Using get_logger directly without initialize_logging
logger = get_logger("simulated_data")

class SimulatedDataProvider:
    """
    Simulated Market Data Provider
    
    Generates realistic market data for testing strategies, including:
    - Time series with trends, cycles, and random components
    - Price and volume data
    - OHLC candles at various timeframes
    - Common technical indicators
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the simulated data provider.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config or {}
        
        # Default settings
        self.seed = self.config.get("seed", int(time.time()))
        self.volatility = self.config.get("volatility", 0.015)  # 1.5% daily volatility
        self.trend = self.config.get("trend", 0.0001)  # 0.01% daily drift
        self.cycle_amplitude = self.config.get("cycle_amplitude", 0.05)  # 5% cycle amplitude
        self.cycle_period = self.config.get("cycle_period", 20)  # 20-day cycle
        
        # Set the random seed for reproducibility if needed
        random.seed(self.seed)
        np.random.seed(self.seed)
        
        # Cache for generated data
        self.price_cache = {}
        self.candle_cache = {}
        
        logger.info(f"Initialized simulated data provider with seed {self.seed}")
    
    def generate_prices(self, symbol: str, days: int = 365, start_price: Optional[float] = None) -> List[float]:
        """
        Generate simulated daily prices.
        
        Args:
            symbol: Trading symbol
            days: Number of days to generate
            start_price: Starting price (optional)
            
        Returns:
            List of daily prices
        """
        # Check if we have this in cache
        cache_key = f"{symbol}:{days}:{start_price}"
        if cache_key in self.price_cache:
            return self.price_cache[cache_key]
        
        # Determine starting price based on symbol if not provided
        if start_price is None:
            # Different starting prices for different asset types
            if "BTC" in symbol:
                start_price = 50000.0
            elif "ETH" in symbol:
                start_price = 3000.0
            elif "USD" in symbol and "T" not in symbol:
                start_price = 1.0  # For forex pairs
            else:
                # Use hash of symbol to get a consistent but different price for each symbol
                symbol_hash = hash(symbol) % 10000
                start_price = 10.0 + (symbol_hash / 100.0)
        
        # Generate daily returns
        # Combination of:
        # 1. Random walk (Brownian motion) with drift
        # 2. Cyclical component
        # 3. Random shocks/news events
        
        # Random walk
        daily_returns = np.random.normal(self.trend, self.volatility, days)
        
        # Add cyclical component
        t = np.arange(days)
        cycle = self.cycle_amplitude * np.sin(2 * np.pi * t / self.cycle_period)
        daily_returns += cycle
        
        # Add occasional shocks (positive and negative news)
        shock_days = np.random.choice(days, size=int(days * 0.05), replace=False)  # 5% of days have shocks
        shock_magnitude = np.random.normal(0, self.volatility * 5, len(shock_days))  # 5x normal volatility
        daily_returns[shock_days] += shock_magnitude
        
        # Convert returns to prices
        prices = [start_price]
        for ret in daily_returns:
            prices.append(prices[-1] * (1 + ret))
        
        # Cache the result
        self.price_cache[cache_key] = prices
        
        return prices
    
    def generate_candles(self, symbol: str, timeframe: str = "1d", count: int = 100,
                        end_time: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Generate OHLC candles for a symbol.
        
        Args:
            symbol: Trading symbol
            timeframe: Candle timeframe (e.g., "1m", "5m", "1h", "1d")
            count: Number of candles to generate
            end_time: End timestamp in seconds
            
        Returns:
            List of OHLC candles
        """
        # Check if we have this in cache
        cache_key = f"{symbol}:{timeframe}:{count}:{end_time}"
        if cache_key in self.candle_cache:
            return self.candle_cache[cache_key]
        
        # Set end time to current time if not provided
        if end_time is None:
            end_time = int(time.time())
        
        # Determine candle interval in seconds
        interval_seconds = self._parse_timeframe(timeframe)
        
        # Generate base prices (daily)
        daily_prices = self.generate_prices(symbol, days=max(365, count))
        
        # Interpolate to the requested timeframe
        # For timeframes smaller than 1d, we need to generate intraday price movements
        candles = []
        
        # Calculate total days needed based on timeframe and count
        days_needed = max(1, int((interval_seconds * count) / (24 * 60 * 60)) + 1)
        
        # Use a subset of daily prices
        base_prices = daily_prices[-days_needed:]
        
        # Generate candles
        for i in range(count):
            timestamp = end_time - (count - i - 1) * interval_seconds
            
            # Determine which day this candle belongs to
            day_index = min(len(base_prices) - 1, int((count - i - 1) * interval_seconds / (24 * 60 * 60)))
            
            # Get base price for this day
            base_price = base_prices[day_index]
            
            # Generate intraday volatility
            intraday_vol = self.volatility / np.sqrt(24 * 60 * 60 / interval_seconds)
            
            # Generate candle body
            open_price = base_price * (1 + np.random.normal(0, intraday_vol))
            close_price = open_price * (1 + np.random.normal(0, intraday_vol))
            
            # Generate high and low
            high_price = max(open_price, close_price) * (1 + abs(np.random.normal(0, intraday_vol)))
            low_price = min(open_price, close_price) * (1 - abs(np.random.normal(0, intraday_vol)))
            
            # Generate volume
            base_volume = base_price * 1000  # Base volume proportional to price
            volume = base_volume * (1 + np.random.normal(0, 0.3))  # 30% volume volatility
            
            # Add some correlation between price change and volume
            price_change = abs(close_price - open_price) / open_price
            volume *= (1 + 5 * price_change)  # Higher volume on larger price moves
            
            candles.append({
                "timestamp": timestamp,
                "datetime": datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S"),
                "open": round(open_price, 8),
                "high": round(high_price, 8),
                "low": round(low_price, 8),
                "close": round(close_price, 8),
                "volume": round(volume, 2)
            })
        
        # Cache the result
        self.candle_cache[cache_key] = candles
        
        return candles
    
    def get_ticker(self, symbol: str) -> Dict[str, Any]:
        """
        Get current ticker data for a symbol.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Ticker data
        """
        # Generate a single candle
        candle = self.generate_candles(symbol, timeframe="1m", count=1)[0]
        
        # Calculate 24h change
        yesterday_candle = self.generate_candles(symbol, timeframe="1d", count=2)[0]
        change_24h = (candle["close"] - yesterday_candle["close"]) / yesterday_candle["close"] * 100
        
        return {
            "symbol": symbol,
            "last_price": candle["close"],
            "bid": round(candle["close"] * 0.999, 8),  # 0.1% bid-ask spread
            "ask": round(candle["close"] * 1.001, 8),
            "high_24h": candle["high"],
            "low_24h": candle["low"],
            "volume_24h": candle["volume"],
            "change_24h": round(change_24h, 2),
            "timestamp": candle["timestamp"]
        }
    
    def get_order_book(self, symbol: str, depth: int = 20) -> Dict[str, Any]:
        """
        Generate a simulated order book.
        
        Args:
            symbol: Trading symbol
            depth: Order book depth
            
        Returns:
            Order book data
        """
        ticker = self.get_ticker(symbol)
        last_price = ticker["last_price"]
        
        # Generate bid and ask prices
        bids = []
        asks = []
        
        # Parameters for price and size distribution
        price_step = last_price * 0.0005  # 0.05% between price levels
        base_size = last_price * 10  # Base order size proportional to price
        size_decay = 0.9  # Each level has 90% of the size of the previous level
        
        # Generate bids (sorted by price descending)
        bid_price = last_price * 0.999  # Start 0.1% below last price
        for i in range(depth):
            size = base_size * (size_decay ** i) * (1 + np.random.normal(0, 0.2))  # Add some randomness to size
            bids.append([round(bid_price, 8), round(size, 8)])
            bid_price -= price_step * (1 + 0.1 * np.random.random())  # Slightly random price steps
        
        # Generate asks (sorted by price ascending)
        ask_price = last_price * 1.001  # Start 0.1% above last price
        for i in range(depth):
            size = base_size * (size_decay ** i) * (1 + np.random.normal(0, 0.2))
            asks.append([round(ask_price, 8), round(size, 8)])
            ask_price += price_step * (1 + 0.1 * np.random.random())
        
        return {
            "symbol": symbol,
            "timestamp": int(time.time()),
            "bids": bids,
            "asks": asks
        }
    
    def _parse_timeframe(self, timeframe: str) -> int:
        """
        Parse timeframe string to seconds.
        
        Args:
            timeframe: Timeframe string (e.g., "1m", "5m", "1h", "1d")
            
        Returns:
            Timeframe in seconds
        """
        unit = timeframe[-1].lower()
        value = int(timeframe[:-1])
        
        if unit == "m":
            return value * 60
        elif unit == "h":
            return value * 60 * 60
        elif unit == "d":
            return value * 24 * 60 * 60
        elif unit == "w":
            return value * 7 * 24 * 60 * 60
        else:
            raise ValueError(f"Unknown timeframe unit: {unit}")

def main():
    """Main function to test the simulated data provider."""
    try:
        logger.info("Starting simulated data provider")
        
        # Create simulated data provider
        provider = SimulatedDataProvider({
            "volatility": 0.02,  # 2% daily volatility
            "trend": 0.0002,     # 0.02% daily upward drift
            "seed": 42           # Fixed seed for reproducibility
        })
        
        # Test symbols
        symbols = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "XRP/USDT"]
        
        print("\n=== Simulated Market Data ===\n")
        
        # Display current tickers
        print("Current Market Prices:")
        for symbol in symbols:
            ticker = provider.get_ticker(symbol)
            change_sign = "+" if ticker["change_24h"] > 0 else ""
            print(f"{symbol}: ${ticker['last_price']:.2f} ({change_sign}{ticker['change_24h']}%)")
        
        print("\nOHLC Candles (last 5 daily):")
        # Get and display sample candles
        symbol = "BTC/USDT"
        candles = provider.generate_candles(symbol, timeframe="1d", count=5)
        
        for candle in candles:
            print(f"{candle['datetime']}: Open=${candle['open']:.2f}, High=${candle['high']:.2f}, "
                  f"Low=${candle['low']:.2f}, Close=${candle['close']:.2f}, Vol={candle['volume']:.2f}")
        
        print("\nOrder Book Sample:")
        # Display sample order book
        order_book = provider.get_order_book(symbol, depth=5)
        
        print(f"Top 5 Bids:")
        for price, size in order_book["bids"][:5]:
            print(f"  ${price:.2f}: {size:.8f}")
            
        print(f"Top 5 Asks:")
        for price, size in order_book["asks"][:5]:
            print(f"  ${price:.2f}: {size:.8f}")
        
        logger.info("Finished simulated data provider test")
        
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main() 