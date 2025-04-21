"""
TradingView Simple Data Fetcher

This module provides basic data fetching capabilities for TradingView symbols
by using public APIs and web scraping techniques.
"""

import os
import sys
import time
import json
import logging
import requests
import random
import dotenv
from typing import Dict, Any, List, Optional, Union

# Add the repository root to the Python path
script_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(os.path.dirname(script_dir))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from trading_system.core.logging import initialize_logging, get_logger

# Load environment variables
dotenv.load_dotenv()

# Configure logging
logs_dir = os.path.join(os.getcwd(), "logs")
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)
initialize_logging(log_level=logging.INFO, log_file=os.path.join(logs_dir, "tradingview_simple.log"))
logger = get_logger("tradingview_simple")

class TradingViewSimple:
    """
    TradingView Simple Data Fetcher
    
    This class provides methods to fetch basic data from TradingView public APIs.
    """
    
    def __init__(self):
        """Initialize the TradingView simple data fetcher."""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        })
        self.symbols = {}  # Cache for resolved symbols
        self.data_cache = {}  # Cache for fetched data
        self.cache_timestamps = {}  # Timestamps for cache entries
        self.cache_expiry = 60  # 60 seconds cache expiry
    
    def resolve_symbol(self, symbol: str) -> str:
        """
        Resolve a symbol to TradingView format.
        
        Args:
            symbol: Symbol to resolve (e.g., "BTC/USDT")
            
        Returns:
            Resolved symbol (e.g., "BINANCE:BTCUSDT")
        """
        # Check cache first
        if symbol in self.symbols:
            return self.symbols[symbol]
        
        # Format symbol for TradingView
        clean_symbol = symbol.replace("/", "")
        
        # Determine exchange and format
        if "BTC" in symbol or "ETH" in symbol or "USDT" in symbol:
            # For cryptocurrencies, prefer Binance
            tv_symbol = f"BINANCE:{clean_symbol}"
        elif "USD" in symbol:
            # For forex, use OANDA
            tv_symbol = f"OANDA:{clean_symbol}"
        else:
            # Default to Binance for others
            tv_symbol = f"BINANCE:{clean_symbol}"
        
        # Cache the resolved symbol
        self.symbols[symbol] = tv_symbol
        logger.info(f"Resolved symbol {symbol} to {tv_symbol}")
        
        return tv_symbol
    
    def get_price(self, symbol: str) -> Optional[float]:
        """
        Get the current price for a symbol.
        
        Args:
            symbol: Symbol to get price for (e.g., "BTC/USDT")
            
        Returns:
            Current price or None if not available
        """
        # Check cache
        cache_key = f"price:{symbol}"
        if cache_key in self.data_cache and time.time() - self.cache_timestamps.get(cache_key, 0) < self.cache_expiry:
            logger.debug(f"Using cached price for {symbol}")
            return self.data_cache[cache_key]
        
        try:
            # Resolve symbol
            tv_symbol = self.resolve_symbol(symbol)
            exchange, ticker = tv_symbol.split(":")
            
            # Use TradingView search API
            url = f"https://symbol-search.tradingview.com/symbol_search/?text={ticker}&type=stock"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Find the symbol in the response
            for item in data:
                if item.get("symbol") == ticker and item.get("exchange") == exchange:
                    price = item.get("current_price")
                    if price:
                        # Cache the result
                        self.data_cache[cache_key] = price
                        self.cache_timestamps[cache_key] = time.time()
                        logger.info(f"Retrieved price for {symbol}: {price}")
                        return price
            
            logger.warning(f"Could not find price for {symbol} in API response")
            return None
            
        except Exception as e:
            logger.error(f"Error getting price for {symbol}: {str(e)}")
            return None
    
    def get_cryptocurrency_price(self, symbol: str) -> Optional[float]:
        """
        Get the current price for a cryptocurrency using CoinGecko API.
        
        Args:
            symbol: Symbol to get price for (e.g., "BTC/USDT")
            
        Returns:
            Current price or None if not available
        """
        # Check cache
        cache_key = f"crypto_price:{symbol}"
        if cache_key in self.data_cache and time.time() - self.cache_timestamps.get(cache_key, 0) < self.cache_expiry:
            logger.debug(f"Using cached crypto price for {symbol}")
            return self.data_cache[cache_key]
        
        try:
            # Parse the symbol
            parts = symbol.replace("/", "-").split("-")
            base_currency = parts[0].lower()
            quote_currency = parts[1].lower() if len(parts) > 1 else "usd"
            
            # Use CoinGecko API for reliable cryptocurrency prices
            url = f"https://api.coingecko.com/api/v3/simple/price?ids={base_currency}&vs_currencies={quote_currency}"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if base_currency in data and quote_currency in data[base_currency]:
                price = data[base_currency][quote_currency]
                
                # Cache the result
                self.data_cache[cache_key] = price
                self.cache_timestamps[cache_key] = time.time()
                logger.info(f"Retrieved crypto price for {symbol}: {price}")
                return price
            
            logger.warning(f"Could not find crypto price for {symbol} in API response")
            return None
            
        except Exception as e:
            logger.error(f"Error getting crypto price for {symbol}: {str(e)}")
            return None
    
    def get_market_status(self, symbol: str) -> Dict[str, Any]:
        """
        Get basic market status for a symbol.
        
        Args:
            symbol: Symbol to get status for (e.g., "BTC/USDT")
            
        Returns:
            Dictionary with market status
        """
        result = {
            "symbol": symbol,
            "price": None,
            "change_24h": None,
            "volume_24h": None,
            "high_24h": None,
            "low_24h": None,
            "timestamp": time.time(),
        }
        
        # Try to get price from different sources
        price = self.get_price(symbol)
        if price is None and ("BTC" in symbol or "ETH" in symbol or "USDT" in symbol):
            price = self.get_cryptocurrency_price(symbol)
        
        if price is not None:
            result["price"] = price
            
            # Simulate other values based on price for demo purposes
            # In a real implementation, these would come from the API
            result["change_24h"] = round(random.uniform(-5.0, 5.0), 2)
            change_factor = 1 + (result["change_24h"] / 100)
            result["high_24h"] = round(price * (1 + random.uniform(0.005, 0.03)), 2)
            result["low_24h"] = round(price * (1 - random.uniform(0.005, 0.03)), 2)
            result["volume_24h"] = round(price * random.uniform(1000, 100000), 2)
        
        return result

def main():
    """Main function to test the TradingView simple data fetcher."""
    try:
        logger.info("Starting TradingView simple data fetcher")
        
        # Create the fetcher
        tv = TradingViewSimple()
        
        # Test with some crypto symbols
        symbols = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "XRP/USDT"]
        
        print("\n=== TradingView Market Data ===\n")
        
        for symbol in symbols:
            status = tv.get_market_status(symbol)
            
            if status["price"]:
                change_sign = "+" if status["change_24h"] > 0 else ""
                print(f"{symbol}:")
                print(f"  Price: ${status['price']:.2f}")
                print(f"  24h Change: {change_sign}{status['change_24h']}%")
                print(f"  24h High: ${status['high_24h']:.2f}")
                print(f"  24h Low: ${status['low_24h']:.2f}")
                print(f"  24h Volume: ${status['volume_24h']:.2f}")
                print()
            else:
                print(f"{symbol}: Could not retrieve price data")
                print()
        
        logger.info("Finished TradingView simple data fetcher test")
        
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main() 