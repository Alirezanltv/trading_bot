"""
Market Data Subsystem

This module integrates all market data components into a unified subsystem.
It provides a central access point to market data from various sources.
"""

import os
import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

from trading_system.core.component import Component, ComponentStatus
from trading_system.core.logging import get_logger
from trading_system.core.message_bus import MessageBus, MessageTypes
from trading_system.market_data.market_data_facade import MarketDataFacade, DataProviderType
from trading_system.market_data.data_unifier import DataUnifier
from trading_system.market_data.timeseries_db import TimeSeriesDB, get_timeseries_db
from trading_system.market_data.tradingview_provider import TradingViewProvider
from trading_system.market_data.tradingview_validator import TradingViewValidator

logger = get_logger("market_data.subsystem")

class MarketDataSubsystem(Component):
    """
    Market Data Subsystem
    
    This class integrates all market data components and provides a unified
    interface to access market data.
    """
    
    def __init__(self, config: Dict[str, Any], message_bus: Optional[MessageBus] = None):
        """
        Initialize the market data subsystem.
        
        Args:
            config: Configuration dictionary
            message_bus: Message bus instance
        """
        super().__init__(name="MarketDataSubsystem")
        
        # Configuration
        self.config = config or {}
        self.message_bus = message_bus
        
        # Components
        self.facade: Optional[MarketDataFacade] = None
        self.unifier: Optional[DataUnifier] = None
        self.db: Optional[TimeSeriesDB] = None
        self.tradingview_validator: Optional[TradingViewValidator] = None
        self.tradingview_provider: Optional[TradingViewProvider] = None
        self.websocket_clients: Dict[str, Any] = {}
        
        # Component configuration
        self.facade_config = self.config.get("facade", {})
        self.unifier_config = self.config.get("unifier", {})
        self.db_config = self.config.get("timeseries_db", {})
        self.tradingview_config = self.config.get("tradingview", {})
        self.websocket_config = self.config.get("websocket", {})
        
        # Initialize components
        self._init_components()
    
    def _init_components(self) -> None:
        """Initialize individual components."""
        # Initialize data unifier
        self.unifier = DataUnifier(self.unifier_config)
        
        # Initialize time-series database
        if self.db_config.get("enabled", True):
            self.db = get_timeseries_db(self.db_config)
        
        # Initialize market data facade
        self.facade = MarketDataFacade(self.facade_config, self.message_bus)
        
        # Initialize TradingView components
        if self.tradingview_config.get("enabled", True):
            self.tradingview_validator = TradingViewValidator(self.tradingview_config.get("validator", {}))
            self.tradingview_provider = TradingViewProvider(
                self.tradingview_config,
                message_bus=self.message_bus,
                validator=self.tradingview_validator
            )
    
    async def initialize(self) -> bool:
        """
        Initialize the market data subsystem.
        
        Returns:
            Initialization success
        """
        try:
            self._status = ComponentStatus.INITIALIZING
            logger.info("Initializing Market Data Subsystem")
            
            # Initialize time-series database
            if self.db and not self.db._status == ComponentStatus.INITIALIZED:
                if not await self.db.initialize():
                    logger.warning("Failed to initialize time-series database, continuing without persistence")
            
            # Initialize market data facade
            if not await self.facade.initialize():
                logger.error("Failed to initialize market data facade")
                self._status = ComponentStatus.ERROR
                self._error = "Failed to initialize market data facade"
                return False
            
            # Initialize TradingView components
            if self.tradingview_provider and not self.tradingview_provider._status == ComponentStatus.INITIALIZED:
                if not await self.tradingview_provider.initialize():
                    logger.warning("Failed to initialize TradingView provider, continuing without TradingView integration")
            
            # Initialize WebSocket clients
            for exchange, config in self.websocket_config.items():
                if not config.get("enabled", True):
                    continue
                
                try:
                    # Import the exchange-specific WebSocket client
                    module_name = f"trading_system.market_data.{exchange.lower()}_websocket"
                    class_name = f"{exchange.capitalize()}WebSocketClient"
                    
                    # Dynamic import
                    try:
                        module = __import__(module_name, fromlist=[class_name])
                        client_class = getattr(module, class_name)
                        
                        # Create client instance
                        client = client_class(config, message_bus=self.message_bus)
                        
                        # Initialize client
                        if not await client.initialize():
                            logger.warning(f"Failed to initialize {exchange} WebSocket client")
                            continue
                        
                        # Store client
                        self.websocket_clients[exchange] = client
                        
                        # Register with facade
                        await self.facade.register_provider(
                            provider=client,
                            provider_type=DataProviderType.PRIMARY if config.get("primary", True) else DataProviderType.SECONDARY,
                            priority=config.get("priority", 1),
                            name=f"{exchange}WebSocket"
                        )
                        
                        logger.info(f"Initialized {exchange} WebSocket client")
                        
                    except (ImportError, AttributeError) as e:
                        logger.warning(f"Failed to import {exchange} WebSocket client: {str(e)}")
                    
                except Exception as e:
                    logger.error(f"Error initializing {exchange} WebSocket client: {str(e)}", exc_info=True)
            
            # Register TradingView provider with facade
            if self.tradingview_provider and self.tradingview_provider._status == ComponentStatus.INITIALIZED:
                await self.facade.register_provider(
                    provider=self.tradingview_provider,
                    provider_type=DataProviderType.SECONDARY,
                    priority=self.tradingview_config.get("priority", 0),
                    name="TradingView"
                )
            
            # Register message handlers
            if self.message_bus:
                await self.message_bus.subscribe(
                    topic="market_data.request",
                    callback=self._handle_data_request
                )
            
            self._status = ComponentStatus.INITIALIZED
            logger.info("Market Data Subsystem initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing Market Data Subsystem: {str(e)}", exc_info=True)
            self._status = ComponentStatus.ERROR
            self._error = str(e)
            return False
    
    async def get_market_data(self, 
                           symbol: str, 
                           timeframe: str = "1m",
                           start_time: Any = None,
                           end_time: Any = None,
                           limit: int = 100,
                           use_cache: bool = True) -> List[Dict[str, Any]]:
        """
        Get market data from the facade.
        
        Args:
            symbol: Trading pair symbol
            timeframe: Candle timeframe
            start_time: Start time
            end_time: End time
            limit: Maximum number of candles to return
            use_cache: Whether to use cached data
            
        Returns:
            List of OHLCV candles
        """
        if not self.facade:
            return []
        
        return await self.facade.get_market_data(
            symbol=symbol,
            timeframe=timeframe,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            use_cache=use_cache
        )
    
    async def get_ticker(self, symbol: str, use_cache: bool = True) -> Optional[Dict[str, Any]]:
        """
        Get ticker from the facade.
        
        Args:
            symbol: Trading pair symbol
            use_cache: Whether to use cached data
            
        Returns:
            Ticker data
        """
        if not self.facade:
            return None
        
        return await self.facade.get_ticker(symbol, use_cache)
    
    async def get_order_book(self, symbol: str, depth: int = 10, use_cache: bool = True) -> Optional[Dict[str, Any]]:
        """
        Get order book from the facade.
        
        Args:
            symbol: Trading pair symbol
            depth: Order book depth
            use_cache: Whether to use cached data
            
        Returns:
            Order book data
        """
        if not self.facade:
            return None
        
        return await self.facade.get_order_book(symbol, depth, use_cache)
    
    async def get_recent_trades(self, symbol: str, limit: int = 100, use_cache: bool = True) -> List[Dict[str, Any]]:
        """
        Get recent trades from the facade.
        
        Args:
            symbol: Trading pair symbol
            limit: Maximum number of trades to return
            use_cache: Whether to use cached data
            
        Returns:
            List of recent trades
        """
        if not self.facade:
            return []
        
        return await self.facade.get_recent_trades(symbol, limit, use_cache)
    
    async def _handle_data_request(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle market data request.
        
        Args:
            message: Request message
            
        Returns:
            Response message
        """
        if not message or not isinstance(message, dict):
            return {"status": "error", "error": "Invalid request"}
        
        try:
            request_type = message.get("type")
            symbol = message.get("symbol")
            
            if not request_type or not symbol:
                return {"status": "error", "error": "Missing required parameters"}
            
            if request_type == "market_data":
                timeframe = message.get("timeframe", "1m")
                start_time = message.get("start_time")
                end_time = message.get("end_time")
                limit = message.get("limit", 100)
                
                data = await self.get_market_data(
                    symbol=symbol,
                    timeframe=timeframe,
                    start_time=start_time,
                    end_time=end_time,
                    limit=limit
                )
                
                return {
                    "status": "success",
                    "type": "market_data",
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "data": data
                }
                
            elif request_type == "ticker":
                data = await self.get_ticker(symbol)
                
                return {
                    "status": "success",
                    "type": "ticker",
                    "symbol": symbol,
                    "data": data
                }
                
            elif request_type == "order_book":
                depth = message.get("depth", 10)
                data = await self.get_order_book(symbol, depth)
                
                return {
                    "status": "success",
                    "type": "order_book",
                    "symbol": symbol,
                    "depth": depth,
                    "data": data
                }
                
            elif request_type == "trades":
                limit = message.get("limit", 100)
                data = await self.get_recent_trades(symbol, limit)
                
                return {
                    "status": "success",
                    "type": "trades",
                    "symbol": symbol,
                    "limit": limit,
                    "data": data
                }
                
            else:
                return {"status": "error", "error": f"Unsupported request type: {request_type}"}
            
        except Exception as e:
            logger.error(f"Error handling market data request: {str(e)}", exc_info=True)
            return {"status": "error", "error": str(e)}
    
    async def stop(self) -> bool:
        """
        Stop all components.
        
        Returns:
            Stop success
        """
        try:
            # Stop WebSocket clients
            for name, client in self.websocket_clients.items():
                try:
                    await client.stop()
                except Exception as e:
                    logger.error(f"Error stopping {name} WebSocket client: {str(e)}")
            
            # Stop TradingView provider
            if self.tradingview_provider:
                try:
                    await self.tradingview_provider.stop()
                except Exception as e:
                    logger.error(f"Error stopping TradingView provider: {str(e)}")
            
            # Stop market data facade
            if self.facade:
                try:
                    await self.facade.stop()
                except Exception as e:
                    logger.error(f"Error stopping market data facade: {str(e)}")
            
            # Stop time-series database
            if self.db:
                try:
                    await self.db.stop()
                except Exception as e:
                    logger.error(f"Error stopping time-series database: {str(e)}")
            
            self._status = ComponentStatus.STOPPED
            logger.info("Market Data Subsystem stopped successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping Market Data Subsystem: {str(e)}", exc_info=True)
            self._status = ComponentStatus.ERROR
            self._error = str(e)
            return False

def get_market_data_subsystem(config: Dict[str, Any] = None, message_bus: Optional[MessageBus] = None) -> MarketDataSubsystem:
    """
    Get or create a market data subsystem instance.
    
    Args:
        config: Configuration dictionary
        message_bus: Message bus instance
        
    Returns:
        Market data subsystem instance
    """
    return MarketDataSubsystem(config or {}, message_bus) 