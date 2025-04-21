"""
TradingView Real-Time Data Module

This module provides real-time market data streaming from TradingView using WebSockets.
"""

import asyncio
import json
import logging
import ssl
import time
import websockets
import uuid
from typing import Dict, List, Any, Optional, Callable, Set
from datetime import datetime, timedelta

from trading_system.core.component import Component, ComponentStatus
from trading_system.core.message_bus import MessageBus, MessageTypes
from trading_system.core.logging import get_logger
from trading_system.market_data.timeseries_db import TimeSeriesDB, get_timeseries_db
from trading_system.market_data.market_data_validator import validate_market_data

# Constants
TRADINGVIEW_WS_URL = "wss://data.tradingview.com/socket.io/websocket"
PING_INTERVAL = 30  # seconds
RECONNECT_DELAY = 5  # seconds
MAX_RECONNECT_ATTEMPTS = 5
SESSION_TIMEOUT = 300  # seconds

logger = get_logger("market_data.tradingview_realtime")

class TradingViewRealtimeStream(Component):
    """
    TradingView real-time market data stream using WebSockets.
    
    Features:
    - Automatic reconnection
    - Data validation
    - Subscription management
    - Redundancy with fallback
    - Direct storage to time-series database
    """
    
    def __init__(self, config: Dict[str, Any], message_bus: Optional[MessageBus] = None):
        """
        Initialize the TradingView real-time stream.
        
        Args:
            config: Configuration dictionary
            message_bus: Optional message bus for publishing data
        """
        super().__init__(
            component_id="tradingview_realtime",
            name="TradingView Real-Time Stream",
            config=config
        )
        
        # Configuration
        self.symbols = config.get("symbols", [])
        self.timeframes = config.get("timeframes", ["1m", "5m", "15m", "1h", "4h", "1d"])
        self.backup_urls = config.get("backup_urls", [
            "wss://alternative1.tradingview.com/socket.io/websocket",
            "wss://alternative2.tradingview.com/socket.io/websocket"
        ])
        
        # Message bus for publishing market data
        self.message_bus = message_bus
        
        # WebSocket connection
        self.ws = None
        self.connected = False
        self.session_id = None
        self.auth_token = None
        self.chart_sessions = {}
        
        # Connection state
        self.active_url = TRADINGVIEW_WS_URL
        self.reconnect_attempts = 0
        self.last_ping_time = 0
        self.last_pong_time = 0
        
        # Subscription tracking
        self.active_subscriptions: Set[str] = set()
        self.subscription_callbacks: Dict[str, List[Callable]] = {}
        
        # Task management
        self.ping_task = None
        self.main_task = None
        self.reconnect_task = None
        
        # Data storage
        self.db = get_timeseries_db()
        
        # Redundancy
        self.fallback_mode = False
        self.fallback_index = 0
        
        logger.info(f"TradingView real-time stream initialized with {len(self.symbols)} symbols")
    
    async def initialize(self) -> bool:
        """
        Initialize the component.
        
        Returns:
            Success flag
        """
        logger.info("Initializing TradingView real-time stream")
        
        self._update_status(ComponentStatus.INITIALIZING)
        
        try:
            # Initialize database connection if needed
            if not self.db:
                self.db = get_timeseries_db()
                if not self.db:
                    logger.error("Failed to connect to time-series database")
                    self._update_status(ComponentStatus.ERROR)
                    return False
            
            # Start main task
            self.main_task = asyncio.create_task(self._main_loop())
            
            self._update_status(ComponentStatus.INITIALIZED)
            logger.info("TradingView real-time stream initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing TradingView real-time stream: {str(e)}")
            self._update_status(ComponentStatus.ERROR)
            return False
    
    async def shutdown(self) -> bool:
        """
        Shutdown the component.
        
        Returns:
            Success flag
        """
        logger.info("Shutting down TradingView real-time stream")
        
        self._update_status(ComponentStatus.STOPPING)
        
        try:
            # Cancel tasks
            for task in [self.main_task, self.ping_task, self.reconnect_task]:
                if task and not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
            
            # Close WebSocket connection
            if self.ws:
                await self.ws.close()
                self.ws = None
            
            self.connected = False
            self._update_status(ComponentStatus.STOPPED)
            logger.info("TradingView real-time stream shutdown complete")
            return True
            
        except Exception as e:
            logger.error(f"Error shutting down TradingView real-time stream: {str(e)}")
            return False
    
    async def subscribe(self, symbol: str, timeframe: str, callback: Optional[Callable] = None) -> bool:
        """
        Subscribe to market data for a symbol and timeframe.
        
        Args:
            symbol: Trading symbol
            timeframe: Chart timeframe
            callback: Optional callback function for data updates
            
        Returns:
            Success flag
        """
        subscription_key = f"{symbol}_{timeframe}"
        
        if subscription_key in self.active_subscriptions:
            # Already subscribed, just add callback
            if callback:
                if subscription_key not in self.subscription_callbacks:
                    self.subscription_callbacks[subscription_key] = []
                self.subscription_callbacks[subscription_key].append(callback)
            return True
        
        # Add to tracking
        self.active_subscriptions.add(subscription_key)
        
        # Add callback if provided
        if callback:
            if subscription_key not in self.subscription_callbacks:
                self.subscription_callbacks[subscription_key] = []
            self.subscription_callbacks[subscription_key].append(callback)
        
        # Send subscription message if connected
        if self.connected and self.ws:
            success = await self._send_subscription(symbol, timeframe)
            if not success:
                logger.error(f"Failed to subscribe to {symbol} ({timeframe})")
                self.active_subscriptions.remove(subscription_key)
                return False
        
        return True
    
    async def unsubscribe(self, symbol: str, timeframe: str) -> bool:
        """
        Unsubscribe from market data for a symbol and timeframe.
        
        Args:
            symbol: Trading symbol
            timeframe: Chart timeframe
            
        Returns:
            Success flag
        """
        subscription_key = f"{symbol}_{timeframe}"
        
        if subscription_key not in self.active_subscriptions:
            return True  # Already unsubscribed
        
        # Remove from tracking
        self.active_subscriptions.remove(subscription_key)
        
        # Remove callbacks
        if subscription_key in self.subscription_callbacks:
            del self.subscription_callbacks[subscription_key]
        
        # Send unsubscription message if connected
        if self.connected and self.ws:
            # Implementation-specific unsubscription logic
            chart_session = self.chart_sessions.get(subscription_key)
            if chart_session:
                try:
                    message = {
                        "m": "chart_detach_session",
                        "p": [chart_session]
                    }
                    await self.ws.send(json.dumps(message))
                    
                    # Remove from chart sessions
                    if subscription_key in self.chart_sessions:
                        del self.chart_sessions[subscription_key]
                        
                    return True
                except Exception as e:
                    logger.error(f"Error unsubscribing from {symbol} ({timeframe}): {str(e)}")
                    return False
        
        return True
    
    async def _main_loop(self) -> None:
        """Main operation loop."""
        while True:
            try:
                if not self.connected:
                    success = await self._connect()
                    if not success:
                        # Try fallback URLs
                        if not self.fallback_mode and self.backup_urls:
                            self.fallback_mode = True
                            self.fallback_index = 0
                            self.active_url = self.backup_urls[self.fallback_index]
                            logger.info(f"Trying fallback URL: {self.active_url}")
                        elif self.fallback_mode and self.backup_urls:
                            # Try next fallback URL
                            self.fallback_index = (self.fallback_index + 1) % len(self.backup_urls)
                            self.active_url = self.backup_urls[self.fallback_index]
                            logger.info(f"Trying next fallback URL: {self.active_url}")
                        
                        # Wait before reconnecting
                        await asyncio.sleep(RECONNECT_DELAY)
                        continue
                
                # Start ping task if needed
                if not self.ping_task or self.ping_task.done():
                    self.ping_task = asyncio.create_task(self._ping_loop())
                
                # Process messages
                async for message in self.ws:
                    await self._handle_message(message)
                
                # Connection closed
                logger.warning("WebSocket connection closed")
                self.connected = False
                
            except websockets.exceptions.ConnectionClosed:
                logger.warning("WebSocket connection closed unexpectedly")
                self.connected = False
                await asyncio.sleep(RECONNECT_DELAY)
                
            except Exception as e:
                logger.error(f"Error in main loop: {str(e)}")
                self.connected = False
                await asyncio.sleep(RECONNECT_DELAY)
    
    async def _connect(self) -> bool:
        """
        Connect to TradingView WebSocket.
        
        Returns:
            Success flag
        """
        try:
            logger.info(f"Connecting to TradingView WebSocket: {self.active_url}")
            
            # Add browser-like headers to avoid 403 rejection
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Origin": "https://www.tradingview.com",
                "Pragma": "no-cache",
                "Cache-Control": "no-cache",
                "Accept-Language": "en-US,en;q=0.9",
                "Sec-WebSocket-Extensions": "permessage-deflate; client_max_window_bits",
                "Sec-WebSocket-Version": "13"
            }
            
            self.ws = await websockets.connect(
                self.active_url,
                ssl=ssl.create_default_context(),
                ping_interval=None,  # We'll handle pings ourselves
                close_timeout=5,
                extra_headers=headers
            )
            
            # Send initial messages
            await self._send_auth_message()
            
            # Reset reconnect attempts if successful
            self.reconnect_attempts = 0
            self.connected = True
            self.last_ping_time = time.time()
            
            # Subscribe to all active subscriptions
            for subscription_key in self.active_subscriptions:
                symbol, timeframe = subscription_key.split("_")
                await self._send_subscription(symbol, timeframe)
            
            logger.info("Connected to TradingView WebSocket successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to TradingView WebSocket: {str(e)}")
            self.reconnect_attempts += 1
            
            # Try using the next fallback URL if available
            if self.backup_urls:
                fallback_index = (self.fallback_index + 1) % len(self.backup_urls)
                self.fallback_index = fallback_index
                self.active_url = self.backup_urls[fallback_index]
                logger.info(f"Trying next fallback URL: {self.active_url}")
                
            return False
    
    async def _send_auth_message(self) -> None:
        """Send authentication message."""
        try:
            # Generate session ID with realistic format
            client_id = f"cs_{uuid.uuid4().hex[:12]}_{int(time.time() * 1000)}"
            self.session_id = client_id
            
            # TradingView message format: ~m~LENGTH~m~PAYLOAD
            # where LENGTH is the length of PAYLOAD
            
            # Send connection initialization message
            payload = {"m":"quote_create_session","p":[client_id]}
            json_payload = json.dumps(payload)
            msg = f"~m~{len(json_payload)}~m~{json_payload}"
            await self.ws.send(msg)
            
            # Send chart create session
            chart_session = f"cs_{uuid.uuid4().hex[:8]}"
            payload = {"m":"chart_create_session","p":[chart_session, ""]}
            json_payload = json.dumps(payload)
            msg = f"~m~{len(json_payload)}~m~{json_payload}"
            await self.ws.send(msg)
            
            # Store chart session
            self.chart_session = chart_session
            
            # Request symbol search capabilities 
            payload = {"m":"symbol_search_capabilities","p":[]}
            json_payload = json.dumps(payload)
            msg = f"~m~{len(json_payload)}~m~{json_payload}"
            await self.ws.send(msg)
            
            logger.debug("Sent enhanced authentication messages")
            
        except Exception as e:
            logger.error(f"Error sending authentication message: {str(e)}")
            raise
    
    async def _send_subscription(self, symbol: str, timeframe: str) -> bool:
        """
        Send subscription message.
        
        Args:
            symbol: Trading symbol
            timeframe: Chart timeframe
            
        Returns:
            Success flag
        """
        try:
            subscription_key = f"{symbol}_{timeframe}"
            
            # Map timeframe to TradingView format
            tv_timeframe = self._map_timeframe(timeframe)
            
            # Create a unique series ID for this subscription
            series_id = f"sds_{uuid.uuid4().hex[:8]}"
            
            # Store mapping for later use
            self.chart_sessions[subscription_key] = {
                "chart_session": self.chart_session,
                "series_id": series_id
            }
            
            # Format symbol for TradingView
            formatted_symbol = self._format_symbol(symbol)
            
            # First, resolve the symbol
            payload = {"m":"resolve_symbol","p":[self.chart_session, "=" + formatted_symbol]}
            json_payload = json.dumps(payload)
            msg = f"~m~{len(json_payload)}~m~{json_payload}"
            await self.ws.send(msg)
            
            # Wait a short time for symbol resolution
            await asyncio.sleep(0.2)
            
            # Then create the series with the correct timeframe
            payload = {"m":"create_series","p":[
                self.chart_session,
                series_id,
                "s1",
                "=" + formatted_symbol,
                tv_timeframe,
                300,
                None,
                False
            ]}
            json_payload = json.dumps(payload)
            msg = f"~m~{len(json_payload)}~m~{json_payload}"
            await self.ws.send(msg)
            
            logger.info(f"Subscribed to {formatted_symbol} ({timeframe})")
            return True
            
        except Exception as e:
            logger.error(f"Error subscribing to {symbol} ({timeframe}): {str(e)}")
            return False
    
    def _map_timeframe(self, timeframe: str) -> str:
        """
        Map standard timeframe to TradingView format.
        
        Args:
            timeframe: Standard timeframe (e.g., "1h")
            
        Returns:
            TradingView timeframe
        """
        mapping = {
            "1m": "1",
            "3m": "3",
            "5m": "5",
            "15m": "15",
            "30m": "30",
            "1h": "60",
            "2h": "120",
            "4h": "240",
            "1d": "1D",
            "1w": "1W",
            "1M": "1M"
        }
        
        return mapping.get(timeframe, "60")  # Default to 1h
    
    def _format_symbol(self, symbol: str) -> str:
        """
        Format symbol for TradingView.
        
        Args:
            symbol: Standard symbol format (e.g., "BTC/USDT")
            
        Returns:
            TradingView symbol format (e.g., "BINANCE:BTCUSDT")
        """
        # Default exchange
        exchange = "BINANCE"
        
        # Remove the slash and replace with nothing
        if "/" in symbol:
            base, quote = symbol.split("/")
            return f"{exchange}:{base}{quote}"
        
        return symbol
    
    async def _ping_loop(self) -> None:
        """Send periodic pings to keep connection alive."""
        while self.connected:
            try:
                # Check if we need to send a ping
                current_time = time.time()
                if current_time - self.last_ping_time >= PING_INTERVAL:
                    # Send ping
                    if self.ws:
                        payload = {"m":"ping"}
                        json_payload = json.dumps(payload)
                        msg = f"~m~{len(json_payload)}~m~{json_payload}"
                        await self.ws.send(msg)
                        self.last_ping_time = current_time
                        logger.debug("Sent ping")
                
                # Check for ping timeout
                if self.last_pong_time > 0 and current_time - self.last_pong_time > SESSION_TIMEOUT:
                    logger.warning("Ping timeout - no pong received")
                    self.connected = False
                    break
                
                # Sleep for a bit
                await asyncio.sleep(5)
                
            except Exception as e:
                logger.error(f"Error in ping loop: {str(e)}")
                await asyncio.sleep(5)
    
    async def _handle_message(self, message: str) -> None:
        """
        Handle incoming WebSocket message.
        
        Args:
            message: WebSocket message
        """
        try:
            # TradingView sends messages in a special format: ~m~LENGTH~m~PAYLOAD
            # We need to extract the actual payload
            if message.startswith('~m~'):
                parts = message.split('~m~')
                # Find parts that could be JSON
                for part in parts:
                    if part and part[0] == '{':
                        try:
                            data = json.loads(part)
                            await self._process_message(data)
                        except json.JSONDecodeError:
                            continue
            else:
                # Try to parse as JSON directly
                try:
                    data = json.loads(message)
                    await self._process_message(data)
                except json.JSONDecodeError:
                    logger.warning(f"Received non-JSON message: {message[:100]}...")
                    
        except Exception as e:
            logger.error(f"Error handling message: {str(e)}")
            
    async def _process_message(self, data: Dict[str, Any]) -> None:
        """
        Process a parsed message based on its type.
        
        Args:
            data: Parsed message data
        """
        # Handle pong
        if "m" in data and data["m"] == "pong":
            self.last_pong_time = time.time()
            return
            
        # Handle data updates (timescale updates)
        if "m" in data and data["m"] == "timescale_update" and "p" in data:
            params = data["p"]
            if len(params) >= 2:
                session_id = params[0]
                update_data = params[1]
                
                # Find the subscription this update belongs to
                for sub_key, session_info in self.chart_sessions.items():
                    if session_info.get("chart_session") == session_id:
                        await self._process_market_data(sub_key, update_data)
                        break
                        
        # Handle symbol resolution result
        elif "m" in data and data["m"] == "symbol_resolved" and "p" in data:
            logger.debug(f"Symbol resolved: {data['p']}")
            
        # Handle series created confirmation
        elif "m" in data and data["m"] == "series_completed" and "p" in data:
            logger.debug(f"Series completed: {data['p']}")
            
        # Handle errors
        elif "m" in data and data["m"] == "critical_error" and "p" in data:
            logger.error(f"TradingView critical error: {data['p']}")
    
    async def _process_market_data(self, subscription_key: str, data: Dict[str, Any]) -> None:
        """
        Process market data from TradingView.
        
        Args:
            subscription_key: Subscription key (symbol_timeframe)
            data: Market data
        """
        try:
            symbol, timeframe = subscription_key.split("_")
            
            # Extract the candles data from the update
            candles_data = None
            
            # TradingView data structure can be complex, try to find the candle data
            if "s" in data and isinstance(data["s"], list):
                candles_data = data["s"]
            elif "bars" in data and isinstance(data["bars"], list):
                candles_data = data["bars"]
            elif "data" in data and isinstance(data["data"], list):
                candles_data = data["data"]
                
            if not candles_data:
                logger.warning(f"No candle data found in update for {subscription_key}")
                return
                
            # Process each candle
            for candle in candles_data:
                # TradingView candle format varies, try common formats
                try:
                    if len(candle) >= 6:  # Standard OHLCV format [time, open, high, low, close, volume]
                        candle_dict = {
                            "symbol": symbol,
                            "timeframe": timeframe,
                            "time": candle[0],
                            "open": candle[1],
                            "high": candle[2],
                            "low": candle[3],
                            "close": candle[4],
                            "volume": candle[5] if len(candle) > 5 else 0
                        }
                    elif isinstance(candle, dict):
                        # Already in dictionary format
                        candle_dict = {
                            "symbol": symbol,
                            "timeframe": timeframe,
                            **candle
                        }
                    else:
                        logger.warning(f"Unknown candle format: {candle}")
                        continue
                    
                    # Validate the candle data
                    if validate_market_data(candle_dict, "candle", "tradingview", symbol, timeframe):
                        # Store in time-series database if available
                        if self.db:
                            self.db.store_market_data(symbol, timeframe, candle_dict)
                        
                        # Publish to message bus if available
                        if self.message_bus:
                            await self.message_bus.publish(
                                message_type=MessageTypes.MARKET_DATA_UPDATE,
                                data=candle_dict
                            )
                        
                        # Call any subscription callbacks
                        if subscription_key in self.subscription_callbacks:
                            for callback in self.subscription_callbacks[subscription_key]:
                                try:
                                    if asyncio.iscoroutinefunction(callback):
                                        await callback([candle_dict])
                                    else:
                                        callback([candle_dict])
                                except Exception as e:
                                    logger.error(f"Error in subscription callback: {str(e)}")
                
                except Exception as e:
                    logger.error(f"Error processing candle: {str(e)}")
                
        except Exception as e:
            logger.error(f"Error processing market data: {str(e)}")
    
    def _update_status(self, status: ComponentStatus) -> None:
        """
        Update the component status synchronously.
        
        Args:
            status: New status to set
        """
        # Create a task to update the status asynchronously
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.create_task(self.set_status(status))
        else:
            # For testing environments or non-async contexts
            self._status = status
            self._status_changed_at = datetime.now()
            logger.info(f"Status changed to {status.value}")
            
            # Add to status history
            self._status_history.append(
                {
                    "status": self._status.value,
                    "timestamp": self._status_changed_at.isoformat()
                }
            ) 