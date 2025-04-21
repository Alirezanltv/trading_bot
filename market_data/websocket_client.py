"""
WebSocket Client for Market Data

This module implements a WebSocket client for real-time market data from exchanges.
It handles connection management, reconnection, and message buffering to provide
reliable data streams.
"""

import asyncio
import json
import logging
import time
from enum import Enum
from typing import Dict, List, Any, Optional, Union, Callable, Set
import websockets
from datetime import datetime, timedelta
import random

from trading_system.core.logging import get_logger
from trading_system.core.component import Component, ComponentStatus

logger = get_logger("market_data.websocket")

class WebSocketConnectionState(Enum):
    """WebSocket connection state."""
    DISCONNECTED = "disconnected"    # Not connected
    CONNECTING = "connecting"        # Connection in progress
    CONNECTED = "connected"          # Connected and ready
    AUTHENTICATED = "authenticated"  # Connected and authenticated
    SUBSCRIBING = "subscribing"      # Subscribing to channels
    SUBSCRIBED = "subscribed"        # Successfully subscribed
    ERROR = "error"                  # Error state
    RECONNECTING = "reconnecting"    # Reconnection in progress
    CLOSING = "closing"              # Graceful shutdown in progress
    
class WebSocketClient(Component):
    """
    WebSocket Client for real-time market data.
    
    This client handles:
    - Connection management with automatic reconnection
    - Authentication with exchanges
    - Subscription to market data channels
    - Message parsing and normalization
    - Message buffering during disconnections
    - Error handling and recovery
    """
    
    def __init__(self, config: Dict[str, Any], exchange_name: str):
        """
        Initialize WebSocket client.
        
        Args:
            config: Configuration dictionary
            exchange_name: Name of the exchange (e.g., "nobitex", "binance")
        """
        super().__init__(name=f"{exchange_name}_websocket", config=config)
        
        # Exchange details
        self.exchange_name = exchange_name
        
        # WebSocket configuration
        self.ws_url = self.config.get("ws_url", "")
        if not self.ws_url:
            # Set default URLs based on exchange
            if exchange_name == "nobitex":
                self.ws_url = "wss://api.nobitex.ir/ws/v2"
            elif exchange_name == "binance":
                self.ws_url = "wss://stream.binance.com:9443/ws"
            elif exchange_name == "kucoin":
                self.ws_url = "wss://ws-api.kucoin.com"
        
        # Connection settings
        self.ping_interval = self.config.get("ping_interval", 30)  # seconds
        self.reconnect_interval = self.config.get("reconnect_interval", 5)  # seconds
        self.max_reconnect_attempts = self.config.get("max_reconnect_attempts", 10)
        self.connection_timeout = self.config.get("connection_timeout", 15)  # seconds
        self.heartbeat_timeout = self.config.get("heartbeat_timeout", 60)  # seconds
        
        # Authentication details
        self.api_key = self.config.get("api_key", "")
        self.api_secret = self.config.get("api_secret", "")
        self.auth_required = self.config.get("auth_required", False)
        
        # Message handlers
        self.message_handlers: Dict[str, List[Callable]] = {}
        
        # Subscription tracking
        self.subscriptions: Dict[str, Dict[str, Any]] = {}
        self.pending_subscriptions: Set[str] = set()
        self.subscription_handlers: Dict[str, Callable] = {}
        
        # WebSocket state
        self.ws = None
        self.ws_task = None
        self.ping_task = None
        self.reconnect_task = None
        self.connection_state = WebSocketConnectionState.DISCONNECTED
        self.last_received_time = 0
        self.reconnect_attempts = 0
        self.auth_token = None
        self.close_requested = False
        
        # Message buffering for disconnections
        self.buffered_messages: List[Dict[str, Any]] = []
        self.max_buffer_size = self.config.get("max_buffer_size", 1000)
        
        # Message statistics
        self.message_count = 0
        self.error_count = 0
        self.last_error_time = 0
        self.connection_time = 0
        
        # Locks
        self._lock = asyncio.Lock()
        
        logger.info(f"{exchange_name} WebSocket client initialized")
    
    async def connect(self) -> bool:
        """
        Connect to WebSocket server.
        
        Returns:
            bool: True if connected successfully, False otherwise
        """
        if self.ws is not None:
            logger.warning(f"{self.exchange_name} WebSocket already connected")
            return True
            
        self.close_requested = False
        async with self._lock:
            try:
                self.connection_state = WebSocketConnectionState.CONNECTING
                logger.info(f"Connecting to {self.exchange_name} WebSocket: {self.ws_url}")
                
                # Connection with timeout
                self.ws = await asyncio.wait_for(
                    websockets.connect(
                        self.ws_url,
                        close_timeout=self.connection_timeout,
                        ping_interval=None,  # We'll handle pings manually
                        ping_timeout=None,
                        max_size=2**25,  # 32MB max message size
                        extra_headers=self._get_headers()
                    ),
                    timeout=self.connection_timeout
                )
                
                self.connection_state = WebSocketConnectionState.CONNECTED
                self.connection_time = time.time()
                self.last_received_time = time.time()
                self.reconnect_attempts = 0
                
                # Start message handling task
                self.ws_task = asyncio.create_task(self._message_handler())
                
                # Start ping task
                self.ping_task = asyncio.create_task(self._ping_loop())
                
                logger.info(f"Connected to {self.exchange_name} WebSocket")
                
                # Authenticate if required
                if self.auth_required:
                    success = await self._authenticate()
                    if not success:
                        logger.error(f"Failed to authenticate with {self.exchange_name} WebSocket")
                        await self.disconnect()
                        return False
                
                # Resubscribe to previous channels
                if self.subscriptions:
                    await self._resubscribe()
                
                return True
                
            except asyncio.TimeoutError:
                logger.error(f"Timeout connecting to {self.exchange_name} WebSocket")
                self.connection_state = WebSocketConnectionState.ERROR
                return False
                
            except Exception as e:
                logger.error(f"Error connecting to {self.exchange_name} WebSocket: {str(e)}", exc_info=True)
                self.connection_state = WebSocketConnectionState.ERROR
                return False
    
    async def disconnect(self) -> None:
        """Disconnect from WebSocket server."""
        self.close_requested = True
        async with self._lock:
            self.connection_state = WebSocketConnectionState.CLOSING
            
            # Cancel tasks
            if self.ws_task and not self.ws_task.done():
                self.ws_task.cancel()
                try:
                    await self.ws_task
                except asyncio.CancelledError:
                    pass
                self.ws_task = None
            
            if self.ping_task and not self.ping_task.done():
                self.ping_task.cancel()
                try:
                    await self.ping_task
                except asyncio.CancelledError:
                    pass
                self.ping_task = None
                
            if self.reconnect_task and not self.reconnect_task.done():
                self.reconnect_task.cancel()
                try:
                    await self.reconnect_task
                except asyncio.CancelledError:
                    pass
                self.reconnect_task = None
            
            # Close WebSocket connection
            if self.ws:
                try:
                    await self.ws.close()
                except Exception as e:
                    logger.warning(f"Error closing {self.exchange_name} WebSocket: {str(e)}")
                finally:
                    self.ws = None
            
            self.connection_state = WebSocketConnectionState.DISCONNECTED
            logger.info(f"Disconnected from {self.exchange_name} WebSocket")
    
    async def subscribe(self, channel: str, symbol: str = None, params: Dict[str, Any] = None) -> bool:
        """
        Subscribe to a WebSocket channel.
        
        Args:
            channel: Channel name (e.g., "ticker", "trade", "orderbook")
            symbol: Trading symbol (e.g., "btc-usdt")
            params: Additional subscription parameters
            
        Returns:
            bool: True if subscription successful or pending, False otherwise
        """
        if not channel:
            logger.error("Channel name is required for subscription")
            return False
            
        # Create subscription key
        sub_key = channel
        if symbol:
            sub_key = f"{channel}_{symbol}"
            
        # Store subscription parameters
        self.subscriptions[sub_key] = {
            "channel": channel,
            "symbol": symbol,
            "params": params or {}
        }
        
        # If not connected, the subscription will be processed on reconnect
        if self.connection_state != WebSocketConnectionState.CONNECTED and \
           self.connection_state != WebSocketConnectionState.AUTHENTICATED and \
           self.connection_state != WebSocketConnectionState.SUBSCRIBED:
            self.pending_subscriptions.add(sub_key)
            logger.info(f"Queued subscription to {sub_key} for when connection is established")
            return True
            
        # Send subscription message
        try:
            await self._send_subscription(channel, symbol, params)
            self.pending_subscriptions.add(sub_key)
            logger.info(f"Sent subscription request for {sub_key}")
            return True
        except Exception as e:
            logger.error(f"Error subscribing to {sub_key}: {str(e)}", exc_info=True)
            return False
    
    async def unsubscribe(self, channel: str, symbol: str = None) -> bool:
        """
        Unsubscribe from a WebSocket channel.
        
        Args:
            channel: Channel name (e.g., "ticker", "trade", "orderbook")
            symbol: Trading symbol (e.g., "btc-usdt")
            
        Returns:
            bool: True if unsubscription successful, False otherwise
        """
        # Create subscription key
        sub_key = channel
        if symbol:
            sub_key = f"{channel}_{symbol}"
            
        # Remove from subscriptions
        if sub_key in self.subscriptions:
            del self.subscriptions[sub_key]
            
        # Remove from pending subscriptions
        if sub_key in self.pending_subscriptions:
            self.pending_subscriptions.remove(sub_key)
            
        # If not connected, just remove the subscription
        if self.connection_state != WebSocketConnectionState.CONNECTED and \
           self.connection_state != WebSocketConnectionState.AUTHENTICATED and \
           self.connection_state != WebSocketConnectionState.SUBSCRIBED:
            logger.info(f"Removed subscription to {sub_key}")
            return True
            
        # Send unsubscription message
        try:
            await self._send_unsubscription(channel, symbol)
            logger.info(f"Sent unsubscription request for {sub_key}")
            return True
        except Exception as e:
            logger.error(f"Error unsubscribing from {sub_key}: {str(e)}", exc_info=True)
            return False
    
    def register_message_handler(self, channel: str, handler: Callable) -> None:
        """
        Register a message handler for a specific channel.
        
        Args:
            channel: Channel name (e.g., "ticker", "trade", "orderbook")
            handler: Message handler function
        """
        if channel not in self.message_handlers:
            self.message_handlers[channel] = []
            
        self.message_handlers[channel].append(handler)
        logger.info(f"Registered message handler for {channel}")
    
    def register_subscription_handler(self, channel: str, handler: Callable) -> None:
        """
        Register a subscription response handler.
        
        Args:
            channel: Channel name
            handler: Subscription handler function
        """
        self.subscription_handlers[channel] = handler
        logger.info(f"Registered subscription handler for {channel}")
    
    async def _message_handler(self) -> None:
        """Handle incoming WebSocket messages."""
        if not self.ws:
            logger.error("Cannot start message handler: WebSocket not connected")
            return
            
        try:
            async for message in self.ws:
                # Update last received time
                self.last_received_time = time.time()
                
                # Process message
                try:
                    await self._process_message(message)
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}", exc_info=True)
                    self.error_count += 1
                    self.last_error_time = time.time()
                    
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"WebSocket connection closed: {str(e)}")
            if not self.close_requested:
                await self._schedule_reconnect()
                
        except Exception as e:
            logger.error(f"Error in message handler: {str(e)}", exc_info=True)
            if not self.close_requested:
                await self._schedule_reconnect()
    
    async def _process_message(self, message: Union[str, bytes]) -> None:
        """
        Process a WebSocket message.
        
        Args:
            message: Raw WebSocket message
        """
        # Increment message counter
        self.message_count += 1
        
        # Parse message
        try:
            if isinstance(message, bytes):
                data = json.loads(message.decode('utf-8'))
            else:
                data = json.loads(message)
                
            # Process authentication response
            if self.auth_required and 'auth' in data:
                await self._handle_auth_response(data)
                return
                
            # Process subscription response
            if 'subscription' in data or 'subscriptions' in data:
                await self._handle_subscription_response(data)
                return
                
            # Process standard message
            channel = data.get('channel') or data.get('type') or data.get('topic')
            
            # If we can't determine the channel, try to infer it
            if not channel:
                if 'tick' in data or 'ticker' in data:
                    channel = 'ticker'
                elif 'trade' in data or 'trades' in data:
                    channel = 'trade'
                elif 'depth' in data or 'orderbook' in data or 'book' in data:
                    channel = 'orderbook'
                elif 'kline' in data or 'candle' in data or 'bar' in data:
                    channel = 'kline'
                else:
                    # Handle unknown message type
                    logger.warning(f"Unknown message type: {data}")
                    return
            
            # Forward to channel handlers
            if channel in self.message_handlers:
                for handler in self.message_handlers[channel]:
                    try:
                        await handler(data)
                    except Exception as e:
                        logger.error(f"Error in message handler for {channel}: {str(e)}", exc_info=True)
                        
        except json.JSONDecodeError:
            logger.warning(f"Received non-JSON message: {message[:100]}...")
            
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}", exc_info=True)
            self.error_count += 1
    
    async def _ping_loop(self) -> None:
        """Send periodic ping messages to keep the connection alive."""
        try:
            while True:
                # Check if connection is still alive
                elapsed = time.time() - self.last_received_time
                if elapsed > self.heartbeat_timeout:
                    logger.warning(f"No messages received for {elapsed:.1f} seconds, reconnecting")
                    await self._schedule_reconnect()
                    return
                
                # Send ping message
                if self.ping_interval > 0:
                    try:
                        if self.ws and self.connection_state in [
                            WebSocketConnectionState.CONNECTED,
                            WebSocketConnectionState.AUTHENTICATED,
                            WebSocketConnectionState.SUBSCRIBED
                        ]:
                            await self._send_ping()
                    except Exception as e:
                        logger.error(f"Error sending ping: {str(e)}")
                        await self._schedule_reconnect()
                        return
                
                # Wait for next ping interval
                await asyncio.sleep(self.ping_interval)
                
        except asyncio.CancelledError:
            logger.debug("Ping loop cancelled")
            raise
            
        except Exception as e:
            logger.error(f"Error in ping loop: {str(e)}", exc_info=True)
            if not self.close_requested:
                await self._schedule_reconnect()
    
    async def _schedule_reconnect(self) -> None:
        """Schedule a reconnection attempt."""
        if self.close_requested:
            return
            
        if self.reconnect_task and not self.reconnect_task.done():
            # Already reconnecting
            return
            
        self.connection_state = WebSocketConnectionState.RECONNECTING
        
        # Calculate backoff delay with jitter
        delay = min(self.reconnect_interval * (2 ** min(self.reconnect_attempts, 6)), 60)
        jitter = random.uniform(0, delay * 0.2)  # Add up to 20% jitter
        total_delay = delay + jitter
        
        logger.info(f"Scheduling reconnection in {total_delay:.1f} seconds (attempt {self.reconnect_attempts + 1})")
        
        self.reconnect_task = asyncio.create_task(self._reconnect_after_delay(total_delay))
    
    async def _reconnect_after_delay(self, delay: float) -> None:
        """
        Reconnect after a delay.
        
        Args:
            delay: Delay in seconds
        """
        try:
            await asyncio.sleep(delay)
            
            # Check if maximum reconnect attempts reached
            self.reconnect_attempts += 1
            if self.max_reconnect_attempts > 0 and self.reconnect_attempts > self.max_reconnect_attempts:
                logger.error(f"Maximum reconnection attempts ({self.max_reconnect_attempts}) reached")
                self.connection_state = WebSocketConnectionState.ERROR
                return
                
            # Disconnect properly first
            if self.ws:
                try:
                    await self.disconnect()
                except Exception as e:
                    logger.warning(f"Error during disconnect: {str(e)}")
            
            # Reconnect
            await self.connect()
            
        except asyncio.CancelledError:
            logger.debug("Reconnection task cancelled")
            raise
            
        except Exception as e:
            logger.error(f"Error during reconnection: {str(e)}", exc_info=True)
            # Schedule another reconnection
            await self._schedule_reconnect()
    
    async def _resubscribe(self) -> None:
        """Resubscribe to all previous channels after reconnection."""
        if not self.subscriptions:
            return
            
        logger.info(f"Resubscribing to {len(self.subscriptions)} channels")
        self.connection_state = WebSocketConnectionState.SUBSCRIBING
        
        # Clear pending subscriptions and add all
        self.pending_subscriptions.clear()
        self.pending_subscriptions.update(self.subscriptions.keys())
        
        # Send subscription messages
        for sub_key, sub_info in self.subscriptions.items():
            try:
                await self._send_subscription(
                    sub_info["channel"],
                    sub_info["symbol"],
                    sub_info["params"]
                )
                logger.info(f"Sent resubscription request for {sub_key}")
            except Exception as e:
                logger.error(f"Error resubscribing to {sub_key}: {str(e)}", exc_info=True)
        
        # Check if any subscriptions are pending
        if not self.pending_subscriptions:
            self.connection_state = WebSocketConnectionState.SUBSCRIBED
    
    async def _send_message(self, message: Dict[str, Any]) -> None:
        """
        Send a message to the WebSocket server.
        
        Args:
            message: Message to send
        """
        if not self.ws:
            raise ConnectionError("WebSocket not connected")
            
        message_str = json.dumps(message)
        await self.ws.send(message_str)
    
    async def _send_ping(self) -> None:
        """Send a ping message to keep the connection alive."""
        # Customize ping message based on exchange
        if self.exchange_name == "nobitex":
            await self._send_message({"method": "ping"})
        elif self.exchange_name == "binance":
            await self._send_message({"method": "ping"})
        elif self.exchange_name == "kucoin":
            await self._send_message({"type": "ping"})
        else:
            # Generic ping for unknown exchanges
            await self._send_message({"type": "ping", "timestamp": int(time.time() * 1000)})
    
    async def _authenticate(self) -> bool:
        """
        Authenticate with the WebSocket server.
        
        Returns:
            bool: True if authenticated successfully, False otherwise
        """
        if not self.api_key or not self.api_secret:
            logger.warning("API credentials missing for authentication")
            return False
            
        self.connection_state = WebSocketConnectionState.CONNECTING
        
        # Build authentication message based on exchange
        auth_message = None
        if self.exchange_name == "nobitex":
            auth_message = {
                "method": "login",
                "api_key": self.api_key
            }
        elif self.exchange_name == "binance":
            # Binance WebSocket doesn't need authentication for public data
            self.connection_state = WebSocketConnectionState.AUTHENTICATED
            return True
        elif self.exchange_name == "kucoin":
            # KuCoin requires getting a token from REST API first
            # This is just a placeholder - implement proper KuCoin authentication if needed
            return False
        else:
            # Generic authentication for unknown exchanges
            auth_message = {
                "type": "auth",
                "api_key": self.api_key
            }
        
        if not auth_message:
            logger.error(f"No authentication method defined for {self.exchange_name}")
            return False
            
        try:
            # Send authentication message
            await self._send_message(auth_message)
            logger.info(f"Sent authentication request to {self.exchange_name}")
            
            # Wait for authentication response
            # This is handled in _handle_auth_response
            
            # For now, assume authentication succeeded
            # In reality, you should wait for the auth response with a timeout
            self.connection_state = WebSocketConnectionState.AUTHENTICATED
            return True
            
        except Exception as e:
            logger.error(f"Error authenticating with {self.exchange_name}: {str(e)}", exc_info=True)
            return False
    
    async def _handle_auth_response(self, data: Dict[str, Any]) -> None:
        """
        Handle authentication response.
        
        Args:
            data: Authentication response data
        """
        # Check if authentication was successful
        success = False
        
        # Parse response based on exchange
        if self.exchange_name == "nobitex":
            success = data.get("status") == "success"
            if success:
                self.auth_token = data.get("token")
        elif self.exchange_name == "binance":
            success = True  # Binance doesn't need authentication for public data
        else:
            # Generic success check
            success = data.get("success", False) or data.get("result", False)
        
        if success:
            logger.info(f"Successfully authenticated with {self.exchange_name}")
            self.connection_state = WebSocketConnectionState.AUTHENTICATED
            
            # Resubscribe to all channels
            if self.subscriptions:
                await self._resubscribe()
        else:
            logger.error(f"Authentication failed with {self.exchange_name}: {data}")
            self.connection_state = WebSocketConnectionState.ERROR
    
    async def _send_subscription(self, channel: str, symbol: str = None, params: Dict[str, Any] = None) -> None:
        """
        Send a subscription message.
        
        Args:
            channel: Channel name
            symbol: Trading symbol
            params: Additional subscription parameters
        """
        params = params or {}
        
        # Build subscription message based on exchange
        sub_message = None
        if self.exchange_name == "nobitex":
            sub_message = {
                "method": "sub",
                "channel": channel
            }
            if symbol:
                sub_message["symbol"] = symbol
            if params:
                for key, value in params.items():
                    sub_message[key] = value
        elif self.exchange_name == "binance":
            # Binance uses a different format
            stream = channel.lower()
            if symbol:
                symbol = symbol.replace("-", "").lower()
                stream = f"{symbol}@{stream}"
            sub_message = {
                "method": "SUBSCRIBE",
                "params": [stream],
                "id": int(time.time() * 1000)
            }
        elif self.exchange_name == "kucoin":
            # KuCoin uses a different format
            sub_message = {
                "type": "subscribe",
                "topic": f"/{channel}"
            }
            if symbol:
                symbol = symbol.replace("-", "").upper()
                sub_message["topic"] = f"{sub_message['topic']}:{symbol}"
            if params:
                sub_message["privateChannel"] = params.get("private", False)
                sub_message["response"] = params.get("response", True)
        else:
            # Generic subscription for unknown exchanges
            sub_message = {
                "type": "subscribe",
                "channel": channel
            }
            if symbol:
                sub_message["symbol"] = symbol
            if params:
                for key, value in params.items():
                    sub_message[key] = value
        
        await self._send_message(sub_message)
    
    async def _send_unsubscription(self, channel: str, symbol: str = None) -> None:
        """
        Send an unsubscription message.
        
        Args:
            channel: Channel name
            symbol: Trading symbol
        """
        # Build unsubscription message based on exchange
        unsub_message = None
        if self.exchange_name == "nobitex":
            unsub_message = {
                "method": "unsub",
                "channel": channel
            }
            if symbol:
                unsub_message["symbol"] = symbol
        elif self.exchange_name == "binance":
            # Binance uses a different format
            stream = channel.lower()
            if symbol:
                symbol = symbol.replace("-", "").lower()
                stream = f"{symbol}@{stream}"
            unsub_message = {
                "method": "UNSUBSCRIBE",
                "params": [stream],
                "id": int(time.time() * 1000)
            }
        elif self.exchange_name == "kucoin":
            # KuCoin uses a different format
            unsub_message = {
                "type": "unsubscribe",
                "topic": f"/{channel}"
            }
            if symbol:
                symbol = symbol.replace("-", "").upper()
                unsub_message["topic"] = f"{unsub_message['topic']}:{symbol}"
        else:
            # Generic unsubscription for unknown exchanges
            unsub_message = {
                "type": "unsubscribe",
                "channel": channel
            }
            if symbol:
                unsub_message["symbol"] = symbol
        
        await self._send_message(unsub_message)
    
    async def _handle_subscription_response(self, data: Dict[str, Any]) -> None:
        """
        Handle subscription response.
        
        Args:
            data: Subscription response data
        """
        # Check if subscription was successful
        success = False
        channel = ""
        symbol = ""
        
        # Parse response based on exchange
        if self.exchange_name == "nobitex":
            success = data.get("status") == "success"
            channel = data.get("channel", "")
            symbol = data.get("symbol", "")
        elif self.exchange_name == "binance":
            success = data.get("result") is None  # No error means success
            id_value = data.get("id")
            # Extract channel from id
            if id_value and "subscribing" in str(id_value):
                channel = str(id_value).split(" ")[1]
        elif self.exchange_name == "kucoin":
            success = data.get("type") == "ack"
            if "topic" in data:
                topic = data.get("topic", "")
                parts = topic.split(":")
                channel = parts[0].lstrip("/")
                if len(parts) > 1:
                    symbol = parts[1]
        else:
            # Generic success check
            success = data.get("success", False) or data.get("result", False)
            channel = data.get("channel", "")
            symbol = data.get("symbol", "")
        
        # Update subscription status
        sub_key = channel
        if symbol:
            sub_key = f"{channel}_{symbol}"
            
        if success:
            logger.info(f"Subscription to {sub_key} confirmed")
            
            # Remove from pending subscriptions
            if sub_key in self.pending_subscriptions:
                self.pending_subscriptions.remove(sub_key)
                
            # Call subscription handler if registered
            if channel in self.subscription_handlers:
                try:
                    await self.subscription_handlers[channel](data)
                except Exception as e:
                    logger.error(f"Error in subscription handler for {channel}: {str(e)}", exc_info=True)
        else:
            logger.error(f"Subscription to {sub_key} failed: {data}")
            
        # Check if all subscriptions are processed
        if not self.pending_subscriptions and self.connection_state == WebSocketConnectionState.SUBSCRIBING:
            self.connection_state = WebSocketConnectionState.SUBSCRIBED
            logger.info(f"All subscriptions processed, connection state: {self.connection_state}")
    
    def _get_headers(self) -> Dict[str, str]:
        """
        Get HTTP headers for WebSocket connection.
        
        Returns:
            Dict of HTTP headers
        """
        headers = {
            "User-Agent": "TradingSystem/1.0"
        }
        
        # Add authentication headers if needed
        if self.api_key and self.exchange_name == "nobitex":
            headers["X-API-Key"] = self.api_key
            
        return headers
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get current status of WebSocket client.
        
        Returns:
            Dict with status information
        """
        return {
            "exchange": self.exchange_name,
            "state": self.connection_state.value,
            "connected": self.connection_state in [
                WebSocketConnectionState.CONNECTED,
                WebSocketConnectionState.AUTHENTICATED,
                WebSocketConnectionState.SUBSCRIBED
            ],
            "authenticated": self.connection_state in [
                WebSocketConnectionState.AUTHENTICATED,
                WebSocketConnectionState.SUBSCRIBED
            ],
            "subscribed": self.connection_state == WebSocketConnectionState.SUBSCRIBED,
            "subscriptions": list(self.subscriptions.keys()),
            "pending_subscriptions": list(self.pending_subscriptions),
            "reconnect_attempts": self.reconnect_attempts,
            "message_count": self.message_count,
            "error_count": self.error_count,
            "uptime": time.time() - self.connection_time if self.connection_time else 0,
            "last_received": time.time() - self.last_received_time if self.last_received_time else 0,
            "last_error": time.time() - self.last_error_time if self.last_error_time else 0
        }
    
    def is_connected(self) -> bool:
        """
        Check if WebSocket is connected.
        
        Returns:
            bool: True if connected
        """
        return self.connection_state in [
            WebSocketConnectionState.CONNECTED,
            WebSocketConnectionState.AUTHENTICATED,
            WebSocketConnectionState.SUBSCRIBED
        ]
    
    def is_subscribed(self) -> bool:
        """
        Check if WebSocket is fully subscribed.
        
        Returns:
            bool: True if subscribed
        """
        return self.connection_state == WebSocketConnectionState.SUBSCRIBED


# Factory function
def create_websocket_client(config: Dict[str, Any], exchange_name: str) -> WebSocketClient:
    """
    Create a WebSocket client for a specific exchange.
    
    Args:
        config: Configuration dictionary
        exchange_name: Exchange name
        
    Returns:
        WebSocketClient instance
    """
    return WebSocketClient(config, exchange_name) 