"""
Market Data Facade

This module provides a unified interface to access market data from multiple redundant sources,
with automatic fallback and data validation. It integrates various data providers into a
resilient, fault-tolerant facade.
"""

import os
import time
import json
import logging
import asyncio
import random
from enum import Enum
from typing import Dict, List, Any, Optional, Union, Callable, Tuple
from datetime import datetime, timedelta

from trading_system.core.component import Component, ComponentStatus
from trading_system.core.logging import get_logger
from trading_system.core.message_bus import MessageBus, MessageTypes
from trading_system.market_data.timeseries_db import get_timeseries_db, TimeSeriesDB

logger = get_logger("market_data.facade")

class DataProviderType(Enum):
    """Types of market data providers."""
    PRIMARY = "primary"
    SECONDARY = "secondary"
    FALLBACK = "fallback"
    ARCHIVE = "archive"

class DataSourceStatus(Enum):
    """Status of a data source."""
    OPERATIONAL = "operational"     # Fully operational
    DEGRADED = "degraded"           # Operational but with issues
    UNAVAILABLE = "unavailable"     # Currently unavailable
    FAILED = "failed"               # Permanently failed

class DataValidationLevel(Enum):
    """Data validation levels."""
    NONE = "none"           # No validation
    BASIC = "basic"         # Basic validation (range checks, etc.)
    CROSS_SOURCE = "cross_source"  # Cross-validate with other sources
    STRICT = "strict"       # Strict validation with multiple checks

class MarketDataProvider:
    """
    Abstract base class for market data providers.
    
    This class defines the interface that all market data providers must implement
    to be compatible with the MarketDataFacade.
    """
    
    def __init__(self, name: str):
        """
        Initialize the market data provider.
        
        Args:
            name: Provider name
        """
        self.name = name
        self.status = DataSourceStatus.OPERATIONAL
    
    async def get_market_data(self, 
                          symbol: str, 
                          timeframe: str = "1m",
                          start_time: Union[datetime, str] = None,
                          end_time: Union[datetime, str] = None,
                          limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get market data (OHLCV candles).
        
        Args:
            symbol: Trading pair symbol
            timeframe: Candle timeframe
            start_time: Start time
            end_time: End time
            limit: Maximum number of candles to return
            
        Returns:
            List of OHLCV candles
        """
        raise NotImplementedError("Subclasses must implement get_market_data")
    
    async def get_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get ticker for a symbol.
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            Ticker data
        """
        raise NotImplementedError("Subclasses must implement get_ticker")
    
    async def get_order_book(self, symbol: str, depth: int = 10) -> Optional[Dict[str, Any]]:
        """
        Get order book for a symbol.
        
        Args:
            symbol: Trading pair symbol
            depth: Order book depth
            
        Returns:
            Order book data
        """
        raise NotImplementedError("Subclasses must implement get_order_book")
    
    async def get_recent_trades(self, symbol: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get recent trades for a symbol.
        
        Args:
            symbol: Trading pair symbol
            limit: Maximum number of trades to return
            
        Returns:
            List of recent trades
        """
        raise NotImplementedError("Subclasses must implement get_recent_trades")

class MarketDataFacade(Component):
    """
    Market Data Facade.
    
    Provides a unified interface to access market data from multiple sources
    with automatic fallback and data validation. Acts as a facade pattern
    implementation for the market data subsystem.
    """
    
    def __init__(self, config: Dict[str, Any], message_bus: Optional[MessageBus] = None):
        """
        Initialize the market data facade.
        
        Args:
            config: Configuration dictionary
            message_bus: Message bus instance
        """
        super().__init__(name="MarketDataFacade")
        
        # Configuration
        self.config = config or {}
        self.message_bus = message_bus
        
        # Data providers
        self.providers = {}
        self.provider_status = {}
        
        # Data validation
        self.validation_level = DataValidationLevel(self.config.get("validation_level", "basic"))
        self.max_age_seconds = self.config.get("max_age_seconds", 300)  # Default 5 minutes
        
        # Source selection strategy
        self.selection_strategy = self.config.get("selection_strategy", "priority")  # priority, round-robin, random
        self._current_provider_index = 0
        
        # Cache
        self.cache = {}
        self.cache_expiry = self.config.get("cache_expiry", 60)  # Default 60 seconds
        
        # Time-series database for persistent storage
        self.db: Optional[TimeSeriesDB] = None
        
        # Statistics
        self.stats = {
            "total_requests": 0,
            "cache_hits": 0,
            "failed_requests": 0,
            "validation_failures": 0,
            "source_switches": 0,
            "db_fallbacks": 0,
            "last_request_time": None
        }
    
    async def initialize(self) -> bool:
        """
        Initialize the market data facade.
        
        Returns:
            Initialization success
        """
        try:
            self._status = ComponentStatus.INITIALIZING
            logger.info("Initializing Market Data Facade")
            
            # Initialize time-series database
            db_config = self.config.get("timeseries_db", {})
            if db_config.get("enabled", True):
                self.db = get_timeseries_db(db_config)
                
                # Initialize database
                if not self.db._status == ComponentStatus.INITIALIZED:
                    if not await self.db.initialize():
                        logger.warning("Failed to initialize time-series database, continuing without persistence")
            
            # Register message handlers
            if self.message_bus:
                await self.message_bus.subscribe(
                    topic="market_data.request",
                    callback=self._handle_data_request
                )
                logger.info("Subscribed to market data requests")
            
            self._status = ComponentStatus.INITIALIZED
            logger.info("Market Data Facade initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing Market Data Facade: {str(e)}", exc_info=True)
            self._status = ComponentStatus.ERROR
            self._error = str(e)
            return False
    
    async def register_provider(self, 
                             provider: Any, 
                             provider_type: DataProviderType, 
                             priority: int = 0,
                             name: str = None) -> bool:
        """
        Register a market data provider.
        
        Args:
            provider: Market data provider instance
            provider_type: Provider type
            priority: Provider priority (higher is more priority)
            name: Provider name
            
        Returns:
            Registration success
        """
        try:
            if not provider:
                return False
            
            # Get provider name
            if not name:
                name = getattr(provider, "name", str(provider.__class__.__name__))
            
            # Create provider entry
            self.providers[name] = {
                "instance": provider,
                "type": provider_type,
                "priority": priority,
                "stats": {
                    "requests": 0,
                    "successful": 0,
                    "failed": 0,
                    "last_request_time": None
                }
            }
            
            # Set initial status
            self.provider_status[name] = DataSourceStatus.OPERATIONAL
            
            logger.info(f"Registered {provider_type.value} market data provider: {name}")
            return True
            
        except Exception as e:
            logger.error(f"Error registering market data provider: {str(e)}", exc_info=True)
            return False
    
    async def unregister_provider(self, name: str) -> bool:
        """
        Unregister a market data provider.
        
        Args:
            name: Provider name
            
        Returns:
            Unregistration success
        """
        if name in self.providers:
            del self.providers[name]
            
            if name in self.provider_status:
                del self.provider_status[name]
            
            logger.info(f"Unregistered market data provider: {name}")
            return True
        return False
    
    async def get_market_data(self, 
                           symbol: str, 
                           timeframe: str = "1m",
                           start_time: Union[datetime, str] = None,
                           end_time: Union[datetime, str] = None,
                           limit: int = 100,
                           use_cache: bool = True) -> List[Dict[str, Any]]:
        """
        Get market data from the best available source.
        
        Args:
            symbol: Trading pair symbol
            timeframe: Candle timeframe
            start_time: Start time as datetime or string (e.g., "2023-01-01")
            end_time: End time as datetime or string (e.g., "2023-01-02")
            limit: Maximum number of candles to return
            use_cache: Whether to use cached data
            
        Returns:
            List of OHLCV candles
        """
        try:
            self.stats["total_requests"] += 1
            self.stats["last_request_time"] = datetime.now().isoformat()
            
            # Parse times
            parsed_start, parsed_end = self._parse_time_range(start_time, end_time)
            
            # Check cache
            if use_cache:
                cache_key = f"candles:{symbol}:{timeframe}:{parsed_start}:{parsed_end}:{limit}"
                if cache_key in self.cache:
                    entry = self.cache[cache_key]
                    age = time.time() - entry["timestamp"]
                    if age < self.cache_expiry:
                        self.stats["cache_hits"] += 1
                        logger.debug(f"Using cached market data (age: {age:.1f}s)")
                        return entry["data"]
            
            # Get market data from providers
            data = await self._get_data_from_providers(
                data_type="market_data",
                params={
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "start_time": parsed_start,
                    "end_time": parsed_end,
                    "limit": limit
                }
            )
            
            if data:
                # Update cache
                if use_cache:
                    self.cache[cache_key] = {
                        "timestamp": time.time(),
                        "data": data
                    }
                
                # Store in database if available
                if self.db and self.db._status == ComponentStatus.INITIALIZED:
                    await self._store_data_in_db(symbol, timeframe, data)
                
                return data
            
            # Fall back to database if available
            if self.db and self.db._status == ComponentStatus.INITIALIZED:
                logger.info(f"Falling back to database for {symbol} {timeframe}")
                self.stats["db_fallbacks"] += 1
                
                # Get data from database
                data = await self.db.get_market_data(
                    symbol=symbol,
                    timeframe=timeframe,
                    start_time=parsed_start,
                    end_time=parsed_end
                )
                
                if data:
                    logger.info(f"Retrieved {len(data)} candles from database")
                    return data
            
            # No data available
            self.stats["failed_requests"] += 1
            logger.warning(f"No market data available for {symbol} {timeframe}")
            return []
            
        except Exception as e:
            self.stats["failed_requests"] += 1
            logger.error(f"Error getting market data: {str(e)}", exc_info=True)
            return []
    
    async def get_ticker(self, symbol: str, use_cache: bool = True) -> Optional[Dict[str, Any]]:
        """
        Get current ticker for a symbol.
        
        Args:
            symbol: Trading pair symbol
            use_cache: Whether to use cached data
            
        Returns:
            Ticker data
        """
        try:
            self.stats["total_requests"] += 1
            self.stats["last_request_time"] = datetime.now().isoformat()
            
            # Check cache
            if use_cache:
                cache_key = f"ticker:{symbol}"
                if cache_key in self.cache:
                    entry = self.cache[cache_key]
                    age = time.time() - entry["timestamp"]
                    if age < self.cache_expiry:
                        self.stats["cache_hits"] += 1
                        logger.debug(f"Using cached ticker (age: {age:.1f}s)")
                        return entry["data"]
            
            # Get ticker from providers
            data = await self._get_data_from_providers(
                data_type="ticker",
                params={
                    "symbol": symbol
                }
            )
            
            if data:
                # Update cache
                if use_cache:
                    self.cache[cache_key] = {
                        "timestamp": time.time(),
                        "data": data
                    }
                
                return data
            
            # No data available
            self.stats["failed_requests"] += 1
            logger.warning(f"No ticker available for {symbol}")
            return None
            
        except Exception as e:
            self.stats["failed_requests"] += 1
            logger.error(f"Error getting ticker: {str(e)}", exc_info=True)
            return None
    
    async def get_order_book(self, symbol: str, depth: int = 10, use_cache: bool = True) -> Optional[Dict[str, Any]]:
        """
        Get order book for a symbol.
        
        Args:
            symbol: Trading pair symbol
            depth: Order book depth
            use_cache: Whether to use cached data
            
        Returns:
            Order book data
        """
        try:
            self.stats["total_requests"] += 1
            self.stats["last_request_time"] = datetime.now().isoformat()
            
            # Check cache
            if use_cache:
                cache_key = f"orderbook:{symbol}:{depth}"
                if cache_key in self.cache:
                    entry = self.cache[cache_key]
                    age = time.time() - entry["timestamp"]
                    if age < self.cache_expiry:
                        self.stats["cache_hits"] += 1
                        logger.debug(f"Using cached order book (age: {age:.1f}s)")
                        return entry["data"]
            
            # Get order book from providers
            data = await self._get_data_from_providers(
                data_type="order_book",
                params={
                    "symbol": symbol,
                    "depth": depth
                }
            )
            
            if data:
                # Update cache
                if use_cache:
                    self.cache[cache_key] = {
                        "timestamp": time.time(),
                        "data": data
                    }
                
                return data
            
            # No data available
            self.stats["failed_requests"] += 1
            logger.warning(f"No order book available for {symbol}")
            return None
            
        except Exception as e:
            self.stats["failed_requests"] += 1
            logger.error(f"Error getting order book: {str(e)}", exc_info=True)
            return None
    
    async def get_recent_trades(self, symbol: str, limit: int = 100, use_cache: bool = True) -> List[Dict[str, Any]]:
        """
        Get recent trades for a symbol.
        
        Args:
            symbol: Trading pair symbol
            limit: Maximum number of trades to return
            use_cache: Whether to use cached data
            
        Returns:
            List of trades
        """
        try:
            self.stats["total_requests"] += 1
            self.stats["last_request_time"] = datetime.now().isoformat()
            
            # Check cache
            if use_cache:
                cache_key = f"trades:{symbol}:{limit}"
                if cache_key in self.cache:
                    entry = self.cache[cache_key]
                    age = time.time() - entry["timestamp"]
                    if age < self.cache_expiry:
                        self.stats["cache_hits"] += 1
                        logger.debug(f"Using cached trades (age: {age:.1f}s)")
                        return entry["data"]
            
            # Get trades from providers
            data = await self._get_data_from_providers(
                data_type="trades",
                params={
                    "symbol": symbol,
                    "limit": limit
                }
            )
            
            if data:
                # Update cache
                if use_cache:
                    self.cache[cache_key] = {
                        "timestamp": time.time(),
                        "data": data
                    }
                
                return data
            
            # No data available
            self.stats["failed_requests"] += 1
            logger.warning(f"No trades available for {symbol}")
            return []
            
        except Exception as e:
            self.stats["failed_requests"] += 1
            logger.error(f"Error getting trades: {str(e)}", exc_info=True)
            return []
    
    async def _get_data_from_providers(self, data_type: str, params: Dict[str, Any]) -> Any:
        """
        Get data from available providers.
        
        Args:
            data_type: Type of data to get (market_data, ticker, order_book, trades)
            params: Parameters for the data request
            
        Returns:
            Data from provider or None if not available
        """
        # Get all operational providers
        operational_providers = {
            name: provider for name, provider in self.providers.items()
            if self.provider_status.get(name) in [DataSourceStatus.OPERATIONAL, DataSourceStatus.DEGRADED]
        }
        
        if not operational_providers:
            logger.warning("No operational market data providers available")
            return None
        
        # Sort by priority
        providers_list = list(operational_providers.items())
        
        if self.selection_strategy == "priority":
            # Sort by priority (highest first)
            providers_list.sort(key=lambda x: x[1]["priority"], reverse=True)
        elif self.selection_strategy == "random":
            # Randomize order
            random.shuffle(providers_list)
        elif self.selection_strategy == "round-robin":
            # Rotate list to start with next provider
            if providers_list:
                self._current_provider_index = (self._current_provider_index + 1) % len(providers_list)
                providers_list = providers_list[self._current_provider_index:] + providers_list[:self._current_provider_index]
        
        # Try each provider in order
        data = None
        success = False
        
        for name, provider_info in providers_list:
            try:
                provider_instance = provider_info["instance"]
                
                # Update provider stats
                provider_info["stats"]["requests"] += 1
                provider_info["stats"]["last_request_time"] = datetime.now().isoformat()
                
                # Get method name
                method_name = f"get_{data_type}"
                if hasattr(provider_instance, method_name) and callable(getattr(provider_instance, method_name)):
                    method = getattr(provider_instance, method_name)
                    
                    # Call method with parameters
                    data = await method(**params)
                    
                    # Check if data is valid
                    if data is not None and (isinstance(data, dict) or (isinstance(data, list) and len(data) > 0)):
                        # Validate data
                        if await self._validate_data(data_type, data):
                            success = True
                            provider_info["stats"]["successful"] += 1
                            
                            # Update provider status if needed
                            if self.provider_status.get(name) == DataSourceStatus.DEGRADED:
                                self.provider_status[name] = DataSourceStatus.OPERATIONAL
                                logger.info(f"Provider {name} status changed to OPERATIONAL")
                            
                            logger.debug(f"Got data from provider {name}")
                            break
                        else:
                            logger.warning(f"Data validation failed for provider {name}")
                            self.stats["validation_failures"] += 1
                            provider_info["stats"]["failed"] += 1
                            data = None
                    else:
                        logger.warning(f"Provider {name} returned no valid data")
                        provider_info["stats"]["failed"] += 1
                else:
                    logger.warning(f"Provider {name} does not support method {method_name}")
                    provider_info["stats"]["failed"] += 1
                
            except Exception as e:
                logger.error(f"Error getting data from provider {name}: {str(e)}", exc_info=True)
                provider_info["stats"]["failed"] += 1
                
                # Update provider status
                if self.provider_status.get(name) == DataSourceStatus.OPERATIONAL:
                    self.provider_status[name] = DataSourceStatus.DEGRADED
                    logger.warning(f"Provider {name} status changed to DEGRADED")
        
        if not success:
            logger.warning(f"All providers failed to provide valid {data_type} data")
            return None
        
        return data
    
    async def _validate_data(self, data_type: str, data: Any) -> bool:
        """
        Validate market data.
        
        Args:
            data_type: Type of data to validate
            data: Data to validate
            
        Returns:
            Whether data is valid
        """
        # If no validation is required, return True
        if self.validation_level == DataValidationLevel.NONE:
            return True
        
        # Basic validation
        if data_type == "market_data":
            return self._validate_candles(data)
        elif data_type == "ticker":
            return self._validate_ticker(data)
        elif data_type == "order_book":
            return self._validate_order_book(data)
        elif data_type == "trades":
            return self._validate_trades(data)
        
        return True
    
    def _validate_candles(self, candles: List[Dict[str, Any]]) -> bool:
        """
        Validate OHLCV candles.
        
        Args:
            candles: List of OHLCV candles
            
        Returns:
            Whether candles are valid
        """
        if not isinstance(candles, list) or len(candles) == 0:
            return False
        
        for candle in candles:
            # Check required fields
            if not all(key in candle for key in ["time", "open", "high", "low", "close", "volume"]):
                return False
            
            # Check numeric values
            for key in ["open", "high", "low", "close", "volume"]:
                if not isinstance(candle[key], (int, float)) or candle[key] < 0:
                    return False
            
            # Check high/low consistency
            if candle["high"] < candle["low"] or candle["high"] < 0 or candle["low"] < 0:
                return False
            
            # Check open/close are within high/low
            if candle["open"] > candle["high"] or candle["open"] < candle["low"]:
                return False
            if candle["close"] > candle["high"] or candle["close"] < candle["low"]:
                return False
            
            # Check timestamp
            if self.validation_level != DataValidationLevel.NONE:
                # Convert to milliseconds if in seconds
                time_value = candle["time"]
                if isinstance(time_value, (int, float)) and time_value < 1e12:  # Likely seconds
                    time_value = time_value * 1000
                
                # Check if too old
                if isinstance(time_value, (int, float)):
                    now = datetime.now().timestamp() * 1000
                    age_seconds = (now - time_value) / 1000
                    
                    if age_seconds > self.max_age_seconds:
                        logger.warning(f"Candle too old: {age_seconds:.1f}s > {self.max_age_seconds}s")
                        # Only reject if using strict validation
                        if self.validation_level == DataValidationLevel.STRICT:
                            return False
        
        return True
    
    def _validate_ticker(self, ticker: Dict[str, Any]) -> bool:
        """
        Validate ticker data.
        
        Args:
            ticker: Ticker data
            
        Returns:
            Whether ticker is valid
        """
        if not isinstance(ticker, dict):
            return False
        
        # Check required fields
        if not all(key in ticker for key in ["symbol", "price"]):
            return False
        
        # Check numeric values
        if "price" in ticker and not isinstance(ticker["price"], (int, float)) or ticker["price"] < 0:
            return False
        
        # Check timestamp if available
        if "timestamp" in ticker and self.validation_level != DataValidationLevel.NONE:
            # Convert to milliseconds if in seconds
            time_value = ticker["timestamp"]
            if isinstance(time_value, (int, float)) and time_value < 1e12:  # Likely seconds
                time_value = time_value * 1000
            
            # Check if too old
            if isinstance(time_value, (int, float)):
                now = datetime.now().timestamp() * 1000
                age_seconds = (now - time_value) / 1000
                
                if age_seconds > self.max_age_seconds:
                    logger.warning(f"Ticker too old: {age_seconds:.1f}s > {self.max_age_seconds}s")
                    # Only reject if using strict validation
                    if self.validation_level == DataValidationLevel.STRICT:
                        return False
        
        return True
    
    def _validate_order_book(self, order_book: Dict[str, Any]) -> bool:
        """
        Validate order book data.
        
        Args:
            order_book: Order book data
            
        Returns:
            Whether order book is valid
        """
        if not isinstance(order_book, dict):
            return False
        
        # Check required fields
        if not all(key in order_book for key in ["symbol", "bids", "asks"]):
            return False
        
        # Check bids and asks
        if not isinstance(order_book["bids"], list) or not isinstance(order_book["asks"], list):
            return False
        
        # Check bids and asks format
        for bid in order_book["bids"]:
            if not isinstance(bid, (list, tuple)) or len(bid) < 2:
                return False
            if not isinstance(bid[0], (int, float)) or not isinstance(bid[1], (int, float)):
                return False
            if bid[0] < 0 or bid[1] < 0:
                return False
        
        for ask in order_book["asks"]:
            if not isinstance(ask, (list, tuple)) or len(ask) < 2:
                return False
            if not isinstance(ask[0], (int, float)) or not isinstance(ask[1], (int, float)):
                return False
            if ask[0] < 0 or ask[1] < 0:
                return False
        
        # Check timestamp if available
        if "timestamp" in order_book and self.validation_level != DataValidationLevel.NONE:
            # Convert to milliseconds if in seconds
            time_value = order_book["timestamp"]
            if isinstance(time_value, (int, float)) and time_value < 1e12:  # Likely seconds
                time_value = time_value * 1000
            
            # Check if too old
            if isinstance(time_value, (int, float)):
                now = datetime.now().timestamp() * 1000
                age_seconds = (now - time_value) / 1000
                
                if age_seconds > self.max_age_seconds:
                    logger.warning(f"Order book too old: {age_seconds:.1f}s > {self.max_age_seconds}s")
                    # Only reject if using strict validation
                    if self.validation_level == DataValidationLevel.STRICT:
                        return False
        
        return True
    
    def _validate_trades(self, trades: List[Dict[str, Any]]) -> bool:
        """
        Validate trades data.
        
        Args:
            trades: List of trades
            
        Returns:
            Whether trades are valid
        """
        if not isinstance(trades, list) or len(trades) == 0:
            return False
        
        for trade in trades:
            # Check required fields
            if not all(key in trade for key in ["symbol", "price", "amount"]):
                return False
            
            # Check numeric values
            if not isinstance(trade["price"], (int, float)) or trade["price"] < 0:
                return False
            if not isinstance(trade["amount"], (int, float)) or trade["amount"] < 0:
                return False
            
            # Check timestamp if available
            if "timestamp" in trade and self.validation_level != DataValidationLevel.NONE:
                # Convert to milliseconds if in seconds
                time_value = trade["timestamp"]
                if isinstance(time_value, (int, float)) and time_value < 1e12:  # Likely seconds
                    time_value = time_value * 1000
                
                # Check if too old
                if isinstance(time_value, (int, float)):
                    now = datetime.now().timestamp() * 1000
                    age_seconds = (now - time_value) / 1000
                    
                    if age_seconds > self.max_age_seconds:
                        logger.warning(f"Trade too old: {age_seconds:.1f}s > {self.max_age_seconds}s")
                        # Only reject if using strict validation
                        if self.validation_level == DataValidationLevel.STRICT:
                            return False
        
        return True
    
    def _parse_time_range(self, start_time: Union[datetime, str], end_time: Union[datetime, str]) -> Tuple[str, str]:
        """
        Parse time range for market data requests.
        
        Args:
            start_time: Start time as datetime or string
            end_time: End time as datetime or string
            
        Returns:
            Tuple of parsed start and end times
        """
        # Default start time if not provided
        if not start_time:
            parsed_start = "-1d"  # Last 24 hours
        elif isinstance(start_time, datetime):
            parsed_start = start_time.isoformat() + "Z"
        elif isinstance(start_time, str):
            # If not in relative format (e.g., "-1d"), parse as ISO
            if not start_time.startswith("-"):
                try:
                    parsed_start = datetime.fromisoformat(start_time.replace("Z", "+00:00")).isoformat() + "Z"
                except ValueError:
                    parsed_start = "-1d"  # Default if parsing fails
            else:
                parsed_start = start_time
        else:
            parsed_start = "-1d"  # Default
        
        # Default end time if not provided
        if not end_time:
            parsed_end = "now()"  # Current time
        elif isinstance(end_time, datetime):
            parsed_end = end_time.isoformat() + "Z"
        elif isinstance(end_time, str):
            # If not "now()", parse as ISO
            if end_time != "now()":
                try:
                    parsed_end = datetime.fromisoformat(end_time.replace("Z", "+00:00")).isoformat() + "Z"
                except ValueError:
                    parsed_end = "now()"  # Default if parsing fails
            else:
                parsed_end = end_time
        else:
            parsed_end = "now()"  # Default
        
        return parsed_start, parsed_end
    
    async def _store_data_in_db(self, symbol: str, timeframe: str, candles: List[Dict[str, Any]]) -> None:
        """
        Store market data in time-series database.
        
        Args:
            symbol: Trading pair symbol
            timeframe: Candle timeframe
            candles: List of OHLCV candles
        """
        if not self.db or self.db._status != ComponentStatus.INITIALIZED:
            return
        
        try:
            for candle in candles:
                await self.db.store_market_data(symbol, timeframe, candle)
            
            logger.debug(f"Stored {len(candles)} candles in database")
            
        except Exception as e:
            logger.error(f"Error storing market data in database: {str(e)}", exc_info=True)
    
    async def _handle_data_request(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle market data request from message bus.
        
        Args:
            message: Request message
            
        Returns:
            Response message
        """
        try:
            request_type = message.get("request_type")
            params = message.get("params", {})
            
            if request_type == "market_data":
                data = await self.get_market_data(
                    symbol=params.get("symbol", ""),
                    timeframe=params.get("timeframe", "1m"),
                    start_time=params.get("start_time"),
                    end_time=params.get("end_time"),
                    limit=params.get("limit", 100),
                    use_cache=params.get("use_cache", True)
                )
                return {"success": True, "data": data}
                
            elif request_type == "ticker":
                data = await self.get_ticker(
                    symbol=params.get("symbol", ""),
                    use_cache=params.get("use_cache", True)
                )
                return {"success": True, "data": data}
                
            elif request_type == "order_book":
                data = await self.get_order_book(
                    symbol=params.get("symbol", ""),
                    depth=params.get("depth", 10),
                    use_cache=params.get("use_cache", True)
                )
                return {"success": True, "data": data}
                
            elif request_type == "trades":
                data = await self.get_recent_trades(
                    symbol=params.get("symbol", ""),
                    limit=params.get("limit", 100),
                    use_cache=params.get("use_cache", True)
                )
                return {"success": True, "data": data}
            
            return {"success": False, "error": "Unknown request type"}
            
        except Exception as e:
            logger.error(f"Error handling data request: {str(e)}", exc_info=True)
            return {"success": False, "error": str(e)}
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get facade statistics.
        
        Returns:
            Dictionary of statistics
        """
        provider_stats = {
            name: provider["stats"] for name, provider in self.providers.items()
        }
        
        return {
            "stats": self.stats,
            "providers": provider_stats,
            "provider_status": {
                name: status.value for name, status in self.provider_status.items()
            }
        }

# Singleton instance
_instance = None

def get_market_data_facade(config: Dict[str, Any] = None, message_bus: Optional[MessageBus] = None) -> MarketDataFacade:
    """
    Get or create the MarketDataFacade instance.
    
    Args:
        config: Configuration dictionary
        message_bus: Message bus instance
        
    Returns:
        MarketDataFacade instance
    """
    global _instance
    if _instance is None:
        if config is None:
            config = {}
        _instance = MarketDataFacade(config, message_bus)
    return _instance 