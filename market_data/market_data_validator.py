"""
Market Data Validator Module

This module provides market data validation and cross-checking between multiple
data sources to ensure data integrity and reliability.
"""

import logging
import time
from typing import Dict, List, Any, Optional, Union, Callable, Tuple
from datetime import datetime, timedelta
import statistics
from enum import Enum

from trading_system.core.logging import get_logger
from trading_system.core.component import Component, ComponentStatus

logger = get_logger("market_data.validator")

class ValidationLevel(Enum):
    """Validation level for market data."""
    NONE = 0       # No validation
    BASIC = 1      # Basic validation (format, range checks)
    STANDARD = 2   # Standard validation (temporal consistency, source comparison)
    STRICT = 3     # Strict validation (cross-source verification)


class ValidationResult(Enum):
    """Validation result for market data."""
    VALID = "valid"                # Data is valid
    VALID_WITH_WARNING = "warn"    # Data is valid but with some concerns
    INVALID = "invalid"            # Data is invalid
    INSUFFICIENT_DATA = "insuff"   # Not enough data to validate


class MarketDataValidator(Component):
    """
    Market Data Validator
    
    Validates and cross-checks market data from multiple sources, ensuring
    data integrity and providing fallback mechanisms for unreliable data.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the market data validator.
        
        Args:
            config: Configuration dictionary
        """
        super().__init__(name="market_data_validator", config=config or {})
        
        # Configuration
        self.validation_level = ValidationLevel[self.config.get("validation_level", "STANDARD").upper()]
        self.required_sources = self.config.get("required_sources", 2)
        self.max_price_deviation_pct = self.config.get("max_price_deviation_pct", 5.0)
        self.max_timestamp_deviation_sec = self.config.get("max_timestamp_deviation_sec", 300)
        self.min_volume_threshold = self.config.get("min_volume_threshold", 0.01)
        
        # Source reliability scores (0.0-1.0)
        self.source_reliability = self.config.get("source_reliability", {
            "primary": 1.0,
            "tradingview": 0.9,
            "nobitex": 0.85,
            "binance": 0.9,
            "kucoin": 0.8
        })
        
        # Source fallback preferences (in order of preference)
        self.source_fallbacks = self.config.get("source_fallbacks", [
            ["primary", "tradingview", "nobitex"], 
            ["tradingview", "binance", "nobitex"],
            ["nobitex", "tradingview", "kucoin"]
        ])
        
        # Data caches for validation
        self.recent_tickers: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}  # symbol -> source -> [tickers]
        self.recent_candles: Dict[str, Dict[str, Dict[str, List[Dict[str, Any]]]]] = {}  # symbol -> timeframe -> source -> [candles]
        
        # Cache retention periods
        self.ticker_cache_period = self.config.get("ticker_cache_period", 300)  # seconds
        self.candle_cache_period = self.config.get("candle_cache_period", 3600)  # seconds
        
        # Initialize status
        self._update_status(ComponentStatus.INITIALIZED)
        logger.info(f"Market data validator initialized with {self.validation_level.name} validation level")
    
    def cache_ticker(self, ticker: Dict[str, Any], source: str, symbol: str) -> None:
        """
        Cache a ticker for validation.
        
        Args:
            ticker: Ticker data
            source: Data source
            symbol: Trading symbol
        """
        if not ticker or not symbol or not source:
            return
            
        # Initialize cache for symbol if needed
        if symbol not in self.recent_tickers:
            self.recent_tickers[symbol] = {}
            
        # Initialize cache for source if needed
        if source not in self.recent_tickers[symbol]:
            self.recent_tickers[symbol][source] = []
            
        # Add timestamp if not present
        if "timestamp" not in ticker:
            ticker["timestamp"] = int(time.time() * 1000)
            
        # Add to cache
        self.recent_tickers[symbol][source].append(ticker)
        
        # Prune old entries
        self._prune_ticker_cache(symbol, source)
    
    def cache_candle(self, candle: Dict[str, Any], source: str, symbol: str, timeframe: str) -> None:
        """
        Cache a candle for validation.
        
        Args:
            candle: Candle data
            source: Data source
            symbol: Trading symbol
            timeframe: Candle timeframe
        """
        if not candle or not symbol or not source or not timeframe:
            return
            
        # Initialize cache for symbol if needed
        if symbol not in self.recent_candles:
            self.recent_candles[symbol] = {}
            
        # Initialize cache for timeframe if needed
        if timeframe not in self.recent_candles[symbol]:
            self.recent_candles[symbol][timeframe] = {}
            
        # Initialize cache for source if needed
        if source not in self.recent_candles[symbol][timeframe]:
            self.recent_candles[symbol][timeframe][source] = []
            
        # Add timestamp if not present
        if "time" not in candle:
            candle["time"] = int(time.time() * 1000)
            
        # Add to cache
        self.recent_candles[symbol][timeframe][source].append(candle)
        
        # Prune old entries
        self._prune_candle_cache(symbol, timeframe, source)
    
    def validate_ticker(self, ticker: Dict[str, Any], source: str, symbol: str) -> Tuple[ValidationResult, Dict[str, Any]]:
        """
        Validate a ticker against known data.
        
        Args:
            ticker: Ticker data to validate
            source: Data source
            symbol: Trading symbol
            
        Returns:
            Tuple of (ValidationResult, dict with validation details)
        """
        if not ticker or not symbol:
            return ValidationResult.INVALID, {"reason": "Missing ticker or symbol"}
            
        # Cache the ticker for future validation
        self.cache_ticker(ticker, source, symbol)
        
        # Basic validation
        if self.validation_level == ValidationLevel.NONE:
            return ValidationResult.VALID, {}
            
        basic_result, basic_details = self._basic_ticker_validation(ticker)
        if basic_result == ValidationResult.INVALID:
            return basic_result, basic_details
            
        # No further validation for basic level
        if self.validation_level == ValidationLevel.BASIC:
            return basic_result, basic_details
            
        # Source comparison validation
        if symbol in self.recent_tickers:
            sources = list(self.recent_tickers[symbol].keys())
            
            # Need multiple sources for comparison
            if len(sources) >= self.required_sources:
                return self._cross_source_ticker_validation(ticker, source, symbol, sources)
                
        # For standard level, proceed with temporal validation if cross-source not possible
        if self.validation_level == ValidationLevel.STANDARD:
            return self._temporal_ticker_validation(ticker, source, symbol)
            
        # Strict validation needs multiple sources
        if self.validation_level == ValidationLevel.STRICT:
            if symbol in self.recent_tickers and len(self.recent_tickers[symbol].keys()) >= self.required_sources:
                return ValidationResult.VALID, basic_details
            else:
                return ValidationResult.INSUFFICIENT_DATA, {"reason": "Not enough sources for strict validation"}
                
        return ValidationResult.VALID, basic_details
    
    def validate_candle(self, candle: Dict[str, Any], source: str, symbol: str, timeframe: str) -> Tuple[ValidationResult, Dict[str, Any]]:
        """
        Validate a candle against known data.
        
        Args:
            candle: Candle data to validate
            source: Data source
            symbol: Trading symbol
            timeframe: Candle timeframe
            
        Returns:
            Tuple of (ValidationResult, dict with validation details)
        """
        if not candle or not symbol or not timeframe:
            return ValidationResult.INVALID, {"reason": "Missing candle, symbol, or timeframe"}
            
        # Cache the candle for future validation
        self.cache_candle(candle, source, symbol, timeframe)
        
        # Basic validation
        if self.validation_level == ValidationLevel.NONE:
            return ValidationResult.VALID, {}
            
        basic_result, basic_details = self._basic_candle_validation(candle)
        if basic_result == ValidationResult.INVALID:
            return basic_result, basic_details
            
        # No further validation for basic level
        if self.validation_level == ValidationLevel.BASIC:
            return basic_result, basic_details
            
        # Cross-source validation for standard/strict
        if (symbol in self.recent_candles and
            timeframe in self.recent_candles[symbol]):
            sources = list(self.recent_candles[symbol][timeframe].keys())
            
            # Need multiple sources for comparison
            if len(sources) >= self.required_sources:
                return self._cross_source_candle_validation(candle, source, symbol, timeframe, sources)
                
        # For standard level, proceed with temporal validation if cross-source not possible
        if self.validation_level == ValidationLevel.STANDARD:
            return self._temporal_candle_validation(candle, source, symbol, timeframe)
            
        # Strict validation needs multiple sources
        if self.validation_level == ValidationLevel.STRICT:
            if (symbol in self.recent_candles and 
                timeframe in self.recent_candles[symbol] and 
                len(self.recent_candles[symbol][timeframe].keys()) >= self.required_sources):
                return ValidationResult.VALID, basic_details
            else:
                return ValidationResult.INSUFFICIENT_DATA, {"reason": "Not enough sources for strict validation"}
                
        return ValidationResult.VALID, basic_details
    
    def get_recommended_source(self, symbol: str) -> Optional[str]:
        """
        Get the recommended data source for a symbol based on reliability and availability.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Recommended source name or None if no reliable source
        """
        if symbol not in self.recent_tickers:
            # No data available, return first fallback list's first source
            for fallback_list in self.source_fallbacks:
                if fallback_list:
                    return fallback_list[0]
            return None
            
        available_sources = self.recent_tickers[symbol].keys()
        
        # Try each fallback list
        for fallback_list in self.source_fallbacks:
            for source in fallback_list:
                if source in available_sources:
                    return source
                    
        # If no match in fallback lists, use most reliable available source
        if available_sources:
            source_scores = {
                source: self.source_reliability.get(source, 0.5)
                for source in available_sources
            }
            return max(source_scores.items(), key=lambda x: x[1])[0]
            
        return None
    
    def get_best_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get the best available ticker for a symbol.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Best ticker data or None if no valid ticker
        """
        recommended_source = self.get_recommended_source(symbol)
        if not recommended_source:
            return None
            
        if symbol in self.recent_tickers and recommended_source in self.recent_tickers[symbol]:
            tickers = self.recent_tickers[symbol][recommended_source]
            if tickers:
                # Sort by timestamp and get most recent
                return sorted(tickers, key=lambda x: x.get("timestamp", 0), reverse=True)[0]
                
        return None
    
    def _prune_ticker_cache(self, symbol: str, source: str) -> None:
        """
        Remove old ticker entries from cache.
        
        Args:
            symbol: Trading symbol
            source: Data source
        """
        if symbol in self.recent_tickers and source in self.recent_tickers[symbol]:
            current_time = time.time()
            self.recent_tickers[symbol][source] = [
                ticker for ticker in self.recent_tickers[symbol][source]
                if current_time - ticker.get("timestamp", 0) / 1000 < self.ticker_cache_period
            ]
    
    def _prune_candle_cache(self, symbol: str, timeframe: str, source: str) -> None:
        """
        Remove old candle entries from cache.
        
        Args:
            symbol: Trading symbol
            timeframe: Candle timeframe
            source: Data source
        """
        if (symbol in self.recent_candles and 
            timeframe in self.recent_candles[symbol] and 
            source in self.recent_candles[symbol][timeframe]):
            current_time = time.time()
            self.recent_candles[symbol][timeframe][source] = [
                candle for candle in self.recent_candles[symbol][timeframe][source]
                if current_time - candle.get("time", 0) / 1000 < self.candle_cache_period
            ]
    
    def _basic_ticker_validation(self, ticker: Dict[str, Any]) -> Tuple[ValidationResult, Dict[str, Any]]:
        """
        Perform basic validation on ticker data.
        
        Args:
            ticker: Ticker data
            
        Returns:
            Tuple of (ValidationResult, dict with validation details)
        """
        # Check for required fields
        for field in ["last", "bid", "ask"]:
            if field not in ticker:
                return ValidationResult.INVALID, {"reason": f"Missing required field: {field}"}
                
        # Check for reasonable price ranges
        for field in ["last", "bid", "ask"]:
            price = ticker.get(field, 0)
            if not isinstance(price, (int, float)) or price < 0:
                return ValidationResult.INVALID, {"reason": f"Invalid price for {field}: {price}"}
                
        # Check for reasonable volume
        volume = ticker.get("volume", 0)
        if not isinstance(volume, (int, float)) or volume < 0:
            return ValidationResult.INVALID, {"reason": f"Invalid volume: {volume}"}
            
        # Check for bid/ask relationship
        if "bid" in ticker and "ask" in ticker:
            bid = ticker["bid"]
            ask = ticker["ask"]
            if bid > ask:
                return ValidationResult.INVALID, {"reason": f"Bid ({bid}) greater than ask ({ask})"}
                
        return ValidationResult.VALID, {}
    
    def _basic_candle_validation(self, candle: Dict[str, Any]) -> Tuple[ValidationResult, Dict[str, Any]]:
        """
        Perform basic validation on candle data.
        
        Args:
            candle: Candle data
            
        Returns:
            Tuple of (ValidationResult, dict with validation details)
        """
        # Check for required fields
        for field in ["time", "open", "high", "low", "close"]:
            if field not in candle:
                return ValidationResult.INVALID, {"reason": f"Missing required field: {field}"}
                
        # Check for reasonable price ranges
        for field in ["open", "high", "low", "close"]:
            price = candle.get(field, 0)
            if not isinstance(price, (int, float)) or price < 0:
                return ValidationResult.INVALID, {"reason": f"Invalid price for {field}: {price}"}
                
        # Check for reasonable volume
        volume = candle.get("volume", 0)
        if not isinstance(volume, (int, float)) or volume < 0:
            return ValidationResult.INVALID, {"reason": f"Invalid volume: {volume}"}
            
        # Check for high/low relationship
        high = candle.get("high", 0)
        low = candle.get("low", 0)
        if high < low:
            return ValidationResult.INVALID, {"reason": f"High ({high}) less than low ({low})"}
            
        # Check that high >= open/close and low <= open/close
        open_price = candle.get("open", 0)
        close_price = candle.get("close", 0)
        
        if high < open_price or high < close_price:
            return ValidationResult.INVALID, {"reason": f"High ({high}) less than open ({open_price}) or close ({close_price})"}
            
        if low > open_price or low > close_price:
            return ValidationResult.INVALID, {"reason": f"Low ({low}) greater than open ({open_price}) or close ({close_price})"}
            
        return ValidationResult.VALID, {}
    
    def _cross_source_ticker_validation(self, ticker: Dict[str, Any], source: str, symbol: str, sources: List[str]) -> Tuple[ValidationResult, Dict[str, Any]]:
        """
        Validate ticker by comparing with other sources.
        
        Args:
            ticker: Ticker data
            source: Current source
            symbol: Trading symbol
            sources: Available sources
            
        Returns:
            Tuple of (ValidationResult, dict with validation details)
        """
        # Get most recent ticker from each source
        comparison_tickers = {}
        for comp_source in sources:
            if comp_source != source and comp_source in self.recent_tickers[symbol]:
                tickers = self.recent_tickers[symbol][comp_source]
                if tickers:
                    # Sort by timestamp and get most recent
                    comparison_tickers[comp_source] = sorted(
                        tickers, 
                        key=lambda x: x.get("timestamp", 0), 
                        reverse=True
                    )[0]
        
        # Check if we have enough tickers for comparison
        if len(comparison_tickers) < self.required_sources - 1:
            if self.validation_level == ValidationLevel.STRICT:
                return ValidationResult.INSUFFICIENT_DATA, {"reason": "Not enough sources for strict validation"}
            else:
                # Fallback to temporal validation for standard level
                return self._temporal_ticker_validation(ticker, source, symbol)
        
        # Compare last price with other sources
        last_price = ticker.get("last", 0)
        comparison_prices = [t.get("last", 0) for t in comparison_tickers.values()]
        
        # Calculate median price
        if comparison_prices:
            median_price = statistics.median(comparison_prices)
            
            # Calculate deviation
            if median_price > 0:
                deviation_pct = abs(last_price - median_price) / median_price * 100
                
                if deviation_pct > self.max_price_deviation_pct:
                    return ValidationResult.INVALID, {
                        "reason": "Price deviation too high",
                        "deviation_pct": deviation_pct,
                        "source_price": last_price,
                        "median_price": median_price
                    }
                elif deviation_pct > self.max_price_deviation_pct / 2:
                    return ValidationResult.VALID_WITH_WARNING, {
                        "reason": "Price deviation significant",
                        "deviation_pct": deviation_pct,
                        "source_price": last_price,
                        "median_price": median_price
                    }
        
        return ValidationResult.VALID, {"reason": "Cross-source validation passed"}
    
    def _cross_source_candle_validation(self, candle: Dict[str, Any], source: str, symbol: str, timeframe: str, sources: List[str]) -> Tuple[ValidationResult, Dict[str, Any]]:
        """
        Validate candle by comparing with other sources.
        
        Args:
            candle: Candle data
            source: Current source
            symbol: Trading symbol
            timeframe: Candle timeframe
            sources: Available sources
            
        Returns:
            Tuple of (ValidationResult, dict with validation details)
        """
        candle_time = candle.get("time", 0)
        
        # Get matching candles from other sources
        comparison_candles = {}
        for comp_source in sources:
            if comp_source != source and comp_source in self.recent_candles[symbol][timeframe]:
                source_candles = self.recent_candles[symbol][timeframe][comp_source]
                
                # Find candle with matching time (within tolerance)
                matching_candle = None
                for c in source_candles:
                    c_time = c.get("time", 0)
                    if abs(c_time - candle_time) < self.max_timestamp_deviation_sec * 1000:
                        matching_candle = c
                        break
                
                if matching_candle:
                    comparison_candles[comp_source] = matching_candle
        
        # Check if we have enough candles for comparison
        if len(comparison_candles) < self.required_sources - 1:
            if self.validation_level == ValidationLevel.STRICT:
                return ValidationResult.INSUFFICIENT_DATA, {"reason": "Not enough sources for comparison"}
            else:
                # Fallback to temporal validation for standard level
                return self._temporal_candle_validation(candle, source, symbol, timeframe)
        
        # Compare close prices with other sources
        close_price = candle.get("close", 0)
        comparison_prices = [c.get("close", 0) for c in comparison_candles.values()]
        
        # Calculate median price
        if comparison_prices:
            median_price = statistics.median(comparison_prices)
            
            # Calculate deviation
            if median_price > 0:
                deviation_pct = abs(close_price - median_price) / median_price * 100
                
                if deviation_pct > self.max_price_deviation_pct:
                    return ValidationResult.INVALID, {
                        "reason": "Price deviation too high",
                        "deviation_pct": deviation_pct,
                        "source_price": close_price,
                        "median_price": median_price
                    }
                elif deviation_pct > self.max_price_deviation_pct / 2:
                    return ValidationResult.VALID_WITH_WARNING, {
                        "reason": "Price deviation significant",
                        "deviation_pct": deviation_pct,
                        "source_price": close_price,
                        "median_price": median_price
                    }
        
        return ValidationResult.VALID, {"reason": "Cross-source validation passed"}
    
    def _temporal_ticker_validation(self, ticker: Dict[str, Any], source: str, symbol: str) -> Tuple[ValidationResult, Dict[str, Any]]:
        """
        Validate ticker by checking for temporal consistency.
        
        Args:
            ticker: Ticker data
            source: Current source
            symbol: Trading symbol
            
        Returns:
            Tuple of (ValidationResult, dict with validation details)
        """
        if symbol not in self.recent_tickers or source not in self.recent_tickers[symbol]:
            return ValidationResult.VALID, {"reason": "No historical data for temporal validation"}
            
        # Get historical tickers from the same source for comparison
        historical_tickers = self.recent_tickers[symbol][source]
        if len(historical_tickers) < 2:  # Need at least one historical + current
            return ValidationResult.VALID, {"reason": "Insufficient historical data"}
            
        # Sort tickers by timestamp, excluding the current one
        sorted_historical = sorted(
            [t for t in historical_tickers if t != ticker],
            key=lambda x: x.get("timestamp", 0),
            reverse=True
        )
        
        if not sorted_historical:
            return ValidationResult.VALID, {"reason": "No valid historical data"}
            
        # Get most recent historical ticker
        prev_ticker = sorted_historical[0]
        
        # Check for large price jumps
        current_price = ticker.get("last", 0)
        prev_price = prev_ticker.get("last", 0)
        
        if prev_price > 0:
            price_change_pct = abs(current_price - prev_price) / prev_price * 100
            
            # Check for suspicious price jumps
            if price_change_pct > self.max_price_deviation_pct * 2:  # Double threshold for single source
                return ValidationResult.INVALID, {
                    "reason": "Suspicious price jump",
                    "change_pct": price_change_pct,
                    "current_price": current_price,
                    "previous_price": prev_price
                }
            elif price_change_pct > self.max_price_deviation_pct:
                return ValidationResult.VALID_WITH_WARNING, {
                    "reason": "Significant price change",
                    "change_pct": price_change_pct,
                    "current_price": current_price,
                    "previous_price": prev_price
                }
        
        return ValidationResult.VALID, {"reason": "Temporal validation passed"}
    
    def _temporal_candle_validation(self, candle: Dict[str, Any], source: str, symbol: str, timeframe: str) -> Tuple[ValidationResult, Dict[str, Any]]:
        """
        Validate a candle against temporal requirements.
        
        Args:
            candle: Candle data
            source: Data source
            symbol: Trading symbol
            timeframe: Candle timeframe
            
        Returns:
            Tuple of (ValidationResult, dict with validation details)
        """
        # Need previous candles to validate
        if symbol not in self.recent_candles or timeframe not in self.recent_candles[symbol] or source not in self.recent_candles[symbol][timeframe]:
            return ValidationResult.INSUFFICIENT_DATA, {"reason": "No historical data for temporal validation"}
            
        candles = self.recent_candles[symbol][timeframe][source]
        if len(candles) < 2:
            return ValidationResult.INSUFFICIENT_DATA, {"reason": "Insufficient historical data for temporal validation"}
            
        # Sort candles by time
        sorted_candles = sorted(candles, key=lambda x: x.get("time", 0))
        
        # Find the previous candle
        prev_candles = [c for c in sorted_candles if c.get("time", 0) < candle.get("time", 0)]
        if not prev_candles:
            return ValidationResult.INSUFFICIENT_DATA, {"reason": "No previous candle for temporal validation"}
            
        prev_candle = prev_candles[-1]
        
        # Check for unreasonable price jumps
        prev_close = prev_candle.get("close", 0)
        curr_open = candle.get("open", 0)
        curr_close = candle.get("close", 0)
        
        if prev_close > 0:
            open_change_pct = abs(curr_open - prev_close) / prev_close * 100
            close_change_pct = abs(curr_close - prev_close) / prev_close * 100
            
            if open_change_pct > self.max_price_deviation_pct * 2:
                return ValidationResult.VALID_WITH_WARNING, {
                    "reason": f"Unusual gap between candles: {open_change_pct:.2f}% change from previous close",
                    "prev_close": prev_close,
                    "curr_open": curr_open
                }
                
            if close_change_pct > self.max_price_deviation_pct * 5:
                return ValidationResult.VALID_WITH_WARNING, {
                    "reason": f"Extreme price movement: {close_change_pct:.2f}% change from previous close",
                    "prev_close": prev_close,
                    "curr_close": curr_close
                }
        
        # Check for time sequencing issues
        prev_time = prev_candle.get("time", 0)
        curr_time = candle.get("time", 0)
        
        # Ensure timeframes make sense
        timeframe_minutes = self._timeframe_to_minutes(timeframe)
        expected_diff = timeframe_minutes * 60 * 1000  # in milliseconds
        actual_diff = curr_time - prev_time
        
        # Allow for some tolerance in timing
        if abs(actual_diff - expected_diff) > 0.1 * expected_diff:
            return ValidationResult.VALID_WITH_WARNING, {
                "reason": f"Unexpected time gap between candles: expected ~{expected_diff}ms, got {actual_diff}ms",
                "prev_time": prev_time,
                "curr_time": curr_time
            }
        
        return ValidationResult.VALID, {}


def validate_market_data(market_data: Dict[str, Any], data_type: str = "ticker", 
                        source: str = "unknown", symbol: str = None, 
                        timeframe: str = None) -> bool:
    """
    Validate market data for integrity and consistency.
    
    This function provides a simple interface to validate market data from various sources.
    It performs basic structural and value validation for common market data types.
    
    Args:
        market_data: The market data to validate
        data_type: Type of market data ("ticker", "candle", "trade", etc.)
        source: Source of the data
        symbol: Trading symbol
        timeframe: Timeframe for candle data
        
    Returns:
        True if data is valid, False otherwise
    """
    if not market_data or not isinstance(market_data, dict):
        return False
        
    # Extract symbol from data if not provided
    if symbol is None:
        symbol = market_data.get("symbol", "unknown")
        
    # Basic validation based on data type
    if data_type.lower() == "ticker":
        # Ticker must have at least price information
        if "price" not in market_data and "last" not in market_data and "close" not in market_data:
            return False
            
        # Ensure numeric values
        price = market_data.get("price", market_data.get("last", market_data.get("close", 0)))
        if not isinstance(price, (int, float)) or price <= 0:
            return False
            
        # Validate bid/ask if present
        if "bid" in market_data and (not isinstance(market_data["bid"], (int, float)) or market_data["bid"] <= 0):
            return False
            
        if "ask" in market_data and (not isinstance(market_data["ask"], (int, float)) or market_data["ask"] <= 0):
            return False
            
        # Bid should be less than ask if both exist
        if "bid" in market_data and "ask" in market_data and market_data["bid"] >= market_data["ask"]:
            return False
            
    elif data_type.lower() == "candle" or data_type.lower() == "ohlcv":
        # Candle must have OHLCV data
        required_fields = ["open", "high", "low", "close"]
        if not all(field in market_data for field in required_fields):
            return False
            
        # Validate OHLCV values
        open_price = market_data.get("open", 0)
        high_price = market_data.get("high", 0)
        low_price = market_data.get("low", 0)
        close_price = market_data.get("close", 0)
        
        # Ensure numeric values
        if not all(isinstance(p, (int, float)) and p >= 0 for p in [open_price, high_price, low_price, close_price]):
            return False
            
        # Validate price relationships
        if high_price < low_price or high_price < open_price or high_price < close_price:
            return False
            
        if low_price > open_price or low_price > close_price:
            return False
            
        # Validate volume if present
        if "volume" in market_data:
            volume = market_data["volume"]
            if not isinstance(volume, (int, float)) or volume < 0:
                return False
                
        # Validate timestamp if present
        if "time" in market_data or "timestamp" in market_data:
            timestamp = market_data.get("time", market_data.get("timestamp", 0))
            if not isinstance(timestamp, (int, float)) or timestamp <= 0:
                return False
                
    elif data_type.lower() == "trade":
        # Trade must have price, amount, and side
        required_fields = ["price", "amount"]
        if not all(field in market_data for field in required_fields):
            return False
            
        # Ensure numeric values
        price = market_data.get("price", 0)
        amount = market_data.get("amount", 0)
        
        if not isinstance(price, (int, float)) or price <= 0:
            return False
            
        if not isinstance(amount, (int, float)) or amount <= 0:
            return False
            
        # Validate side if present
        if "side" in market_data and market_data["side"] not in ["buy", "sell"]:
            return False
    
    # If we reach here, the basic validation passed
    return True


# Singleton instance
_market_data_validator_instance = None

def get_market_data_validator(config: Dict[str, Any] = None) -> MarketDataValidator:
    """
    Get the singleton instance of MarketDataValidator.
    
    Args:
        config: Optional configuration to use when initializing
        
    Returns:
        MarketDataValidator instance
    """
    global _market_data_validator_instance
    
    if _market_data_validator_instance is None:
        _market_data_validator_instance = MarketDataValidator(config)
        
    return _market_data_validator_instance 