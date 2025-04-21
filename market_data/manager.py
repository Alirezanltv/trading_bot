"""
Market Data Subsystem Manager.

This module provides the core functionality of the Market Data Subsystem,
integrating different data sources with a focus on TradingView.
"""

import os
import json
import time
import asyncio
import logging
import datetime
from typing import Dict, Any, List, Optional, Union, Callable

from trading_system.core.component import Component, ComponentStatus
from trading_system.core.message_bus import MessageBus, MessageTypes
from trading_system.core.logging import get_logger
from trading_system.core.config import ConfigManager

from trading_system.market_data.tradingview import TradingViewDataProvider, TradingViewCredentials, TradingViewWebhookHandler

logger = get_logger("market_data.manager")

class MarketDataManager(Component):
    """
    Market Data Manager component.
    
    Manages market data sources and provides a unified API for accessing
    market data throughout the trading system.
    """
    
    def __init__(self, config: Dict[str, Any] = None, message_bus: MessageBus = None):
        """
        Initialize the Market Data Manager.
        
        Args:
            config: Configuration dictionary
            message_bus: Message bus instance
        """
        super().__init__(name="MarketDataManager")
        
        # Configuration
        self.config = config or {}
        self.message_bus = message_bus
        
        # Provider configuration
        self.providers = {}
        self.available_providers = ["tradingview"]
        self.enabled_providers = self.config.get("providers", ["tradingview"])
        
        # TradingView components
        self.tradingview_provider = None
        self.tradingview_webhook = None
        
        # Data cache
        self.market_data_cache = {}
        self.signal_cache = {}
        
        # Cache expiry (in seconds)
        self.cache_expiry = {
            "market_data": 60,  # 1 minute
            "signal": 3600      # 1 hour
        }
    
    async def initialize(self) -> bool:
        """
        Initialize the Market Data Manager.
        
        Returns:
            Initialization success
        """
        try:
            self._status = ComponentStatus.INITIALIZING
            logger.info("Initializing Market Data Manager")
            
            # Initialize providers
            for provider_name in self.enabled_providers:
                if provider_name == "tradingview":
                    if not await self._initialize_tradingview():
                        logger.error("Failed to initialize TradingView provider")
                        continue
                    self.providers["tradingview"] = self.tradingview_provider
                
                # Add other providers here as needed
            
            if not self.providers:
                logger.error("No market data providers initialized")
                self._status = ComponentStatus.ERROR
                self._error = "No providers initialized"
                return False
            
            self._status = ComponentStatus.INITIALIZED
            logger.info("Market Data Manager initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing Market Data Manager: {str(e)}", exc_info=True)
            self._status = ComponentStatus.ERROR
            self._error = str(e)
            return False
    
    async def start(self) -> bool:
        """
        Start the Market Data Manager.
        
        Returns:
            Start success
        """
        try:
            logger.info("Starting Market Data Manager")
            
            # Start webhook handler if available and not in test mode
            if self.tradingview_webhook:
                if not await self.tradingview_webhook.start():
                    logger.error("Failed to start TradingView webhook handler")
                    return False
            
            self._status = ComponentStatus.OPERATIONAL
            logger.info("Market Data Manager started successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error starting Market Data Manager: {str(e)}", exc_info=True)
            self._status = ComponentStatus.ERROR
            self._error = str(e)
            return False
    
    async def stop(self) -> bool:
        """
        Stop the Market Data Manager.
        
        Returns:
            Stop success
        """
        try:
            logger.info("Stopping Market Data Manager")
            
            # Stop webhook handler if available
            if self.tradingview_webhook:
                await self.tradingview_webhook.stop()
            
            # Close TradingView provider if available
            if self.tradingview_provider:
                await self.tradingview_provider.close()
            
            self._status = ComponentStatus.STOPPED
            logger.info("Market Data Manager stopped successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping Market Data Manager: {str(e)}", exc_info=True)
            self._status = ComponentStatus.ERROR
            self._error = str(e)
            return False
    
    async def _initialize_tradingview(self) -> bool:
        """
        Initialize TradingView provider.
        
        Returns:
            Initialization success
        """
        try:
            # Get TradingView configuration
            tv_config = self.config.get("tradingview", {})
            
            # Check if we're in test mode
            test_mode = self.config.get("test_mode", False) or tv_config.get("test_mode", False)
            mock_data = self.config.get("mock_data", test_mode) or tv_config.get("mock_data", test_mode)
            webhook_enabled = tv_config.get("webhook_enabled", not test_mode)
            
            logger.info(f"Initializing TradingView provider (test_mode={test_mode}, mock_data={mock_data})")
            
            # Create credentials - use dummy values in test mode
            credentials = TradingViewCredentials(
                username=tv_config.get("username", "test_user"),
                password=tv_config.get("password", "test_password")
            )
            
            # Create data provider
            self.tradingview_provider = TradingViewDataProvider(
                credentials=credentials,
                config={
                    "test_mode": test_mode,
                    "mock_data": mock_data
                }
            )
            
            # Initialize data provider
            if not await self.tradingview_provider.initialize():
                logger.error("Failed to initialize TradingView data provider")
                return False
            
            # Initialize webhook handler if enabled
            if webhook_enabled and self.message_bus:
                logger.info("Initializing TradingView webhook handler")
                webhook_config = tv_config.get("webhook", {})
                
                self.tradingview_webhook = TradingViewWebhookHandler(
                    webhook_config,
                    self.message_bus
                )
                
                if not await self.tradingview_webhook.initialize():
                    logger.error("Failed to initialize TradingView webhook handler")
                    # Continue anyway as this is not critical
            else:
                logger.info("TradingView webhook handler disabled")
            
            logger.info("TradingView provider initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing TradingView provider: {str(e)}", exc_info=True)
            return False
    
    async def get_market_data(self, symbol: str, timeframe: str = "1h") -> Optional[Dict[str, Any]]:
        """
        Get market data for a symbol.
        
        Args:
            symbol: Trading symbol (e.g., "BTC/USDT")
            timeframe: Timeframe (e.g., "1h", "4h", "1d")
            
        Returns:
            Market data or None if not available
        """
        try:
            # Check cache
            cache_key = f"{symbol}_{timeframe}"
            if cache_key in self.market_data_cache:
                cached_data = self.market_data_cache[cache_key]
                # Check if cache is still valid
                if time.time() - cached_data["timestamp"] < self.cache_expiry["market_data"]:
                    return cached_data["data"]
            
            # Try TradingView provider first
            if "tradingview" in self.providers:
                data = await self.tradingview_provider.get_market_data(symbol, timeframe)
                if data:
                    # Update cache
                    self.market_data_cache[cache_key] = {
                        "data": data,
                        "timestamp": time.time()
                    }
                    return data
            
            # Try other providers if needed
            # ...
            
            logger.warning(f"No market data available for {symbol} ({timeframe})")
            return None
            
        except Exception as e:
            logger.error(f"Error getting market data for {symbol}: {str(e)}", exc_info=True)
            return None
    
    async def get_signals(self, symbol: str = None, signal_type: str = None, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get trading signals.
        
        Args:
            symbol: Filter by symbol (optional)
            signal_type: Filter by signal type (optional)
            limit: Maximum number of signals to return
            
        Returns:
            List of signals
        """
        try:
            # Get signals from cache
            signals = []
            
            if symbol:
                # Get signals for a specific symbol
                if symbol in self.signal_cache:
                    if signal_type:
                        # Get signals of a specific type
                        signals = self.signal_cache[symbol].get(signal_type, [])
                    else:
                        # Get all signals for the symbol
                        for signal_list in self.signal_cache[symbol].values():
                            signals.extend(signal_list)
            else:
                # Get all signals
                for symbol_signals in self.signal_cache.values():
                    if signal_type:
                        # Get signals of a specific type
                        signals.extend(symbol_signals.get(signal_type, []))
                    else:
                        # Get all signals
                        for signal_list in symbol_signals.values():
                            signals.extend(signal_list)
            
            # Sort by timestamp (newest first)
            signals.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
            
            # Limit number of signals
            return signals[:limit]
            
        except Exception as e:
            logger.error(f"Error getting signals: {str(e)}", exc_info=True)
            return []
    
    async def register_signal_handler(self, signal_type: str, handler: Callable) -> bool:
        """
        Register a signal handler.
        
        Args:
            signal_type: Signal type to handle
            handler: Handler function
            
        Returns:
            Registration success
        """
        try:
            if self.tradingview_webhook:
                self.tradingview_webhook.register_signal_handler(signal_type, handler)
                logger.info(f"Registered handler for {signal_type} signals")
                return True
            else:
                logger.warning("TradingView webhook handler not available")
                return False
                
        except Exception as e:
            logger.error(f"Error registering signal handler: {str(e)}", exc_info=True)
            return False

    def get_providers(self) -> List[Any]:
        """
        Get list of available data providers.
        
        Returns:
            List of data provider instances
        """
        provider_list = []
        
        if self.tradingview_provider:
            provider_list.append(self.tradingview_provider)
        
        # Add other providers here
        
        return provider_list

# Singleton instance
market_data_manager = MarketDataManager() 