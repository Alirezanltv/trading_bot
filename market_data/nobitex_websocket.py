"""
Nobitex WebSocket Adapter

This module provides a high-reliability adapter for the Nobitex WebSocket API,
implementing automatic reconnection, data validation, and fault tolerance.

Features:
- Real-time market data streaming with automatic reconnection
- Multiple data channel support (market data, orderbook, trades)
- Automatic failover to REST API when WebSocket is unavailable
- Data validation and sanitization
"""

import asyncio
import json
import logging
import time
from enum import Enum
from typing import Dict, List, Any, Optional, Callable, Set, Tuple

from trading_system.core.message_bus import MessageBus, get_message_bus
from trading_system.core.metrics import Metrics, get_metrics
from trading_system.core.component import Component, ComponentStatus
from trading_system.market_data.websocket_client import WebSocketClient, WebSocketStatus

logger = logging.getLogger(__name__)

class NobitexChannel(Enum):
    """Nobitex WebSocket channel types"""
    TRADES = "trades"
    ORDERBOOK = "orderbook"
    PRICE = "price"
    MARKET_DATA = "market_data"

class NobitexWebSocketAdapter(Component):
    """
    High-reliability adapter for Nobitex WebSocket API.
    
    This component provides a robust interface to the Nobitex WebSocket API
    with automatic reconnection, data validation, and fault tolerance.
    """
    
    # WebSocket endpoints
    WS_ENDPOINT = "wss://api.nobitex.ir/ws"
    
    def __init__(self, 
                name: str = "NobitexWebSocket",
                api_key: Optional[str] = None,
                api_secret: Optional[str] = None):
        """
        Initialize the Nobitex WebSocket adapter.
        
        Args:
            name: Component name
            api_key: Optional API key for authenticated connections
            api_secret: Optional API secret for authenticated connections
        """
        super().__init__(name=name)
        
        # API credentials
        self.api_key = api_key
        self.api_secret = api_secret
        
        # WebSocket client
        self.client = None
        
        # Subscriptions
        self.subscriptions: Dict[str, Set[str]] = {
            NobitexChannel.TRADES.value: set(),
            NobitexChannel.ORDERBOOK.value: set(),
            NobitexChannel.PRICE.value: set(),
            NobitexChannel.MARKET_DATA.value: set(),
        }
        
        # Callbacks
        self.callbacks: Dict[str, List[Callable]] = {}
        
        # Last received data cache
        self.last_data: Dict[str, Dict[str, Any]] = {}
        
        # Subscription lock
        self._subscription_lock = asyncio.Lock()
        
        # Message bus
        self.message_bus = get_message_bus()
        
        # Metrics
        self.metrics = get_metrics()
        
        # Fallback polling task
        self.fallback_task = None
        self.using_fallback = False
        
    async def initialize(self) -> bool:
        """
        Initialize the Nobitex WebSocket adapter.
        
        Returns:
            Initialization success
        """
        try:
            self._status = ComponentStatus.INITIALIZING
            logger.info(f"Initializing {self.name}")
            
            # Create WebSocket client
            self.client = WebSocketClient(
                name=self.name,
                url=self.WS_ENDPOINT,
                on_message=self._handle_websocket_message,
                auth_provider=self._get_auth_data if self.api_key else None,
                ping_interval=30.0,
                ping_timeout=10.0,
                initial_backoff=1.0,
                max_backoff=60.0,
                backoff_factor=2.0,
                circuit_breaker_threshold=3,
                circuit_breaker_timeout=60.0
            )
            
            # Initialize WebSocket client
            success = await self.client.initialize()
            
            if success:
                self._status = ComponentStatus.INITIALIZED
                logger.info(f"{self.name} initialized")
                return True
            else:
                logger.error(f"Failed to initialize {self.name}, starting fallback mode")
                self._status = ComponentStatus.DEGRADED
                
                # Start fallback polling
                await self._start_fallback_polling()
                return True  # Return true even with fallback, as the component is usable
                
        except Exception as e:
            self._status = ComponentStatus.ERROR
            self._error = str(e)
            logger.error(f"Error initializing {self.name}: {e}")
            return False
    
    async def stop(self) -> bool:
        """
        Stop the Nobitex WebSocket adapter.
        
        Returns:
            Stop success
        """
        try:
            logger.info(f"Stopping {self.name}")
            
            # Stop WebSocket client
            if self.client:
                await self.client.stop()
            
            # Stop fallback polling
            if self.fallback_task:
                self.fallback_task.cancel()
                try:
                    await self.fallback_task
                except asyncio.CancelledError:
                    pass
            
            self._status = ComponentStatus.STOPPED
            logger.info(f"{self.name} stopped")
            return True
            
        except Exception as e:
            self._status = ComponentStatus.ERROR
            self._error = str(e)
            logger.error(f"Error stopping {self.name}: {e}")
            return False
    
    def _get_auth_data(self) -> Dict[str, Any]:
        """
        Get authentication data for WebSocket connection.
        
        Returns:
            Authentication data
        """
        return {
            "type": "auth",
            "api_key": self.api_key
        }
    
    async def subscribe(self, channel: str, symbols: List[str], 
                      callback: Optional[Callable] = None) -> bool:
        """
        Subscribe to a channel for symbols.
        
        Args:
            channel: Channel name (trades, orderbook, price)
            symbols: List of symbols (e.g., ["btc-rls", "eth-rls"])
            callback: Optional callback function for received data
            
        Returns:
            Subscription success
        """
        try:
            async with self._subscription_lock:
                # Check if valid channel
                if channel not in [c.value for c in NobitexChannel]:
                    logger.error(f"Invalid channel: {channel}")
                    return False
                
                # Add to subscriptions
                self.subscriptions[channel].update(symbols)
                
                # Add callback if provided
                if callback:
                    subscription_id = self._get_subscription_id(channel, symbols)
                    if subscription_id not in self.callbacks:
                        self.callbacks[subscription_id] = []
                    if callback not in self.callbacks[subscription_id]:
                        self.callbacks[subscription_id].append(callback)
                
                # If using WebSocket, send subscription
                if self.client and self.client.status == WebSocketStatus.CONNECTED and not self.using_fallback:
                    subscription_message = self._create_subscription_message(channel, symbols)
                    await self.client.send(subscription_message)
                    logger.info(f"Subscribed to {channel} for {symbols}")
                    return True
                else:
                    logger.info(f"Added subscription to {channel} for {symbols} (will be activated when WebSocket connects)")
                    return True
                    
        except Exception as e:
            logger.error(f"Error subscribing to {channel} for {symbols}: {e}")
            return False
    
    async def unsubscribe(self, channel: str, symbols: List[str]) -> bool:
        """
        Unsubscribe from a channel for symbols.
        
        Args:
            channel: Channel name (trades, orderbook, price)
            symbols: List of symbols (e.g., ["btc-rls", "eth-rls"])
            
        Returns:
            Unsubscription success
        """
        try:
            async with self._subscription_lock:
                # Check if valid channel
                if channel not in [c.value for c in NobitexChannel]:
                    logger.error(f"Invalid channel: {channel}")
                    return False
                
                # Remove from subscriptions
                for symbol in symbols:
                    self.subscriptions[channel].discard(symbol)
                
                # Remove callbacks
                subscription_id = self._get_subscription_id(channel, symbols)
                if subscription_id in self.callbacks:
                    del self.callbacks[subscription_id]
                
                # If using WebSocket, send unsubscription
                if self.client and self.client.status == WebSocketStatus.CONNECTED and not self.using_fallback:
                    unsubscription_message = self._create_unsubscription_message(channel, symbols)
                    await self.client.send(unsubscription_message)
                    logger.info(f"Unsubscribed from {channel} for {symbols}")
                    return True
                else:
                    logger.info(f"Removed subscription to {channel} for {symbols}")
                    return True
                    
        except Exception as e:
            logger.error(f"Error unsubscribing from {channel} for {symbols}: {e}")
            return False
    
    def _create_subscription_message(self, channel: str, symbols: List[str]) -> Dict[str, Any]:
        """
        Create a subscription message.
        
        Args:
            channel: Channel name
            symbols: List of symbols
            
        Returns:
            Subscription message
        """
        return {
            "type": "subscribe",
            "channel": channel,
            "symbols": symbols
        }
    
    def _create_unsubscription_message(self, channel: str, symbols: List[str]) -> Dict[str, Any]:
        """
        Create an unsubscription message.
        
        Args:
            channel: Channel name
            symbols: List of symbols
            
        Returns:
            Unsubscription message
        """
        return {
            "type": "unsubscribe",
            "channel": channel,
            "symbols": symbols
        }
    
    def _get_subscription_id(self, channel: str, symbols: List[str]) -> str:
        """
        Get subscription ID.
        
        Args:
            channel: Channel name
            symbols: List of symbols
            
        Returns:
            Subscription ID
        """
        return f"{channel}:{','.join(sorted(symbols))}"
    
    async def _handle_websocket_message(self, data: Dict[str, Any]) -> None:
        """
        Handle a WebSocket message.
        
        Args:
            data: WebSocket message data
        """
        try:
            # Check if using fallback and reconnected
            if self.using_fallback and self.client and self.client.status == WebSocketStatus.CONNECTED:
                logger.info("WebSocket reconnected, stopping fallback polling")
                self.using_fallback = False
                
                # Stop fallback task
                if self.fallback_task:
                    self.fallback_task.cancel()
                    try:
                        await self.fallback_task
                    except asyncio.CancelledError:
                        pass
                    self.fallback_task = None
                
                # Resubscribe to all channels
                await self._resubscribe_all()
            
            # Handle message based on type
            message_type = data.get("type")
            
            if message_type == "error":
                logger.error(f"WebSocket error: {data.get('message')}")
                # Update metrics
                self.metrics.increment_counter(
                    "nobitex_websocket_errors",
                    {"error": data.get("code", "unknown")}
                )
                return
            
            if message_type == "subscribed":
                logger.info(f"Subscription confirmed: {data}")
                return
            
            if message_type == "unsubscribed":
                logger.info(f"Unsubscription confirmed: {data}")
                return
            
            if message_type == "update":
                # Process data update
                channel = data.get("channel")
                symbol = data.get("symbol")
                update_data = data.get("data", {})
                
                if not channel or not symbol:
                    logger.warning(f"Invalid update message: {data}")
                    return
                
                # Cache the data
                key = f"{channel}:{symbol}"
                self.last_data[key] = update_data
                
                # Validate data
                if not self._validate_data(channel, symbol, update_data):
                    logger.warning(f"Invalid data received for {channel}:{symbol}: {update_data}")
                    # Update metrics
                    self.metrics.increment_counter(
                        "nobitex_invalid_data",
                        {"channel": channel, "symbol": symbol}
                    )
                    return
                
                # Process callbacks
                await self._process_callbacks(channel, symbol, update_data)
                
                # Publish to message bus
                await self.message_bus.publish(
                    f"market_data.nobitex.{channel}.{symbol.replace('-', '_')}",
                    {
                        "channel": channel,
                        "symbol": symbol,
                        "data": update_data,
                        "timestamp": time.time()
                    }
                )
                
                # Update metrics
                self.metrics.increment_counter(
                    "nobitex_updates",
                    {"channel": channel, "symbol": symbol}
                )
                
                return
                
            # Unknown message type
            logger.warning(f"Unknown message type: {message_type}")
            
        except Exception as e:
            logger.error(f"Error handling WebSocket message: {e}, data: {data}")
    
    def _validate_data(self, channel: str, symbol: str, data: Dict[str, Any]) -> bool:
        """
        Validate received data.
        
        Args:
            channel: Channel name
            symbol: Symbol
            data: Data to validate
            
        Returns:
            Validation result
        """
        try:
            if channel == NobitexChannel.TRADES.value:
                # Validate trades data
                if not isinstance(data, list):
                    return False
                
                for trade in data:
                    if not all(k in trade for k in ["price", "amount", "timestamp", "type"]):
                        return False
                
                return True
                
            elif channel == NobitexChannel.ORDERBOOK.value:
                # Validate orderbook data
                if not all(k in data for k in ["bids", "asks"]):
                    return False
                
                for bid in data.get("bids", []):
                    if len(bid) != 2 or not isinstance(bid[0], (int, float)) or not isinstance(bid[1], (int, float)):
                        return False
                
                for ask in data.get("asks", []):
                    if len(ask) != 2 or not isinstance(ask[0], (int, float)) or not isinstance(ask[1], (int, float)):
                        return False
                
                return True
                
            elif channel == NobitexChannel.PRICE.value:
                # Validate price data
                return all(k in data for k in ["latest", "dayHigh", "dayLow", "dayChange"])
                
            elif channel == NobitexChannel.MARKET_DATA.value:
                # Validate market data
                return isinstance(data, dict) and len(data) > 0
                
            return False
            
        except Exception as e:
            logger.error(f"Error validating data: {e}")
            return False
    
    async def _process_callbacks(self, channel: str, symbol: str, data: Dict[str, Any]) -> None:
        """
        Process callbacks for received data.
        
        Args:
            channel: Channel name
            symbol: Symbol
            data: Data to process
        """
        # Get all matching subscription IDs
        # First, check for exact match
        exact_id = self._get_subscription_id(channel, [symbol])
        all_id = self._get_subscription_id(channel, ["all"])
        
        # Execute callbacks
        for sub_id in [exact_id, all_id]:
            if sub_id in self.callbacks:
                for callback in self.callbacks[sub_id]:
                    try:
                        if asyncio.iscoroutinefunction(callback):
                            await callback(channel, symbol, data)
                        else:
                            callback(channel, symbol, data)
                    except Exception as e:
                        logger.error(f"Error in callback for {sub_id}: {e}")
    
    async def _resubscribe_all(self) -> None:
        """Resubscribe to all active subscriptions."""
        async with self._subscription_lock:
            for channel, symbols in self.subscriptions.items():
                if symbols:
                    subscription_message = self._create_subscription_message(channel, list(symbols))
                    await self.client.send(subscription_message)
                    logger.info(f"Resubscribed to {channel} for {symbols}")
    
    async def _start_fallback_polling(self) -> None:
        """Start fallback polling using REST API."""
        if self.fallback_task:
            return
            
        logger.info("Starting fallback polling for market data")
        self.using_fallback = True
        self.fallback_task = asyncio.create_task(self._fallback_polling_loop())
    
    async def _fallback_polling_loop(self) -> None:
        """Fallback polling loop using REST API."""
        try:
            from trading_system.exchange.nobitex_adapter import EnhancedNobitexAdapter
            
            # Create Nobitex adapter
            adapter = EnhancedNobitexAdapter({
                "api_key": self.api_key,
                "api_secret": self.api_secret
            })
            
            while True:
                if self.client and self.client.status == WebSocketStatus.CONNECTED:
                    # WebSocket reconnected, stop fallback
                    logger.info("WebSocket reconnected, stopping fallback polling")
                    self.using_fallback = False
                    return
                
                # Poll for each subscribed channel and symbol
                for channel in self.subscriptions:
                    symbols = self.subscriptions[channel]
                    if not symbols:
                        continue
                    
                    for symbol in symbols:
                        try:
                            # Get data based on channel
                            data = None
                            
                            if channel == NobitexChannel.TRADES.value:
                                # Get recent trades
                                data = await adapter.get_transaction_history(symbol, limit=20)
                            elif channel == NobitexChannel.ORDERBOOK.value:
                                # Get orderbook
                                data = await adapter.get_order_book(symbol)
                            elif channel == NobitexChannel.PRICE.value or channel == NobitexChannel.MARKET_DATA.value:
                                # Get market info
                                data = await adapter.get_market_info(symbol)
                            
                            if data:
                                # Process the data
                                await self._process_callbacks(channel, symbol, data)
                                
                                # Publish to message bus
                                await self.message_bus.publish(
                                    f"market_data.nobitex.{channel}.{symbol.replace('-', '_')}",
                                    {
                                        "channel": channel,
                                        "symbol": symbol,
                                        "data": data,
                                        "timestamp": time.time(),
                                        "source": "fallback"
                                    }
                                )
                                
                                # Update metrics
                                self.metrics.increment_counter(
                                    "nobitex_fallback_updates",
                                    {"channel": channel, "symbol": symbol}
                                )
                            
                        except Exception as e:
                            logger.error(f"Error in fallback polling for {channel}:{symbol}: {e}")
                
                # Sleep between poll cycles
                await asyncio.sleep(5.0)  # Poll every 5 seconds
                
        except asyncio.CancelledError:
            logger.info("Fallback polling cancelled")
            raise
        except Exception as e:
            logger.error(f"Error in fallback polling loop: {e}")
            # Try to reconnect WebSocket if fallback fails
            if self.client:
                await self.client.initialize()
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get adapter statistics.
        
        Returns:
            Statistics dictionary
        """
        stats = {
            "status": self._status.value,
            "error": self._error,
            "using_fallback": self.using_fallback,
            "subscriptions": {k: list(v) for k, v in self.subscriptions.items()},
            "callback_count": sum(len(cb) for cb in self.callbacks.values()),
            "last_data_count": len(self.last_data)
        }
        
        # Add WebSocket client stats if available
        if self.client:
            stats["websocket"] = self.client.get_stats()
        
        return stats 