"""
TradingView WebSocket Client

This module implements a specialized WebSocket client for TradingView data,
handling authentication, subscription, and processing of TradingView-specific
data formats.
"""

import asyncio
import json
import logging
import time
import hmac
import hashlib
import base64
from typing import Dict, List, Any, Optional, Union, Callable, Set
from datetime import datetime, timedelta
import re
import uuid

from trading_system.core.logging import get_logger
from trading_system.core.component import Component, ComponentStatus
from trading_system.market_data.websocket_client import WebSocketClient, WebSocketConnectionState

logger = get_logger("market_data.tradingview_websocket")

class TradingViewMessageType:
    """TradingView message types."""
    PING = "ping"
    PONG = "pong"
    AUTH = "auth"
    QUOTE = "quote"
    TRADE = "trade"
    CHART = "chart"
    SERIES = "series"
    SYMBOL_INFO = "symbol_info"
    STUDY = "study"
    ALERT = "alert"
    SCREENER = "screener"


class TradingViewWebSocketClient(Component):
    """
    TradingView WebSocket Client
    
    Specialized WebSocket client for TradingView data with authentication,
    subscription, and parsing for TradingView-specific data formats.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the TradingView WebSocket client.
        
        Args:
            config: Configuration dictionary
        """
        super().__init__(name="tradingview_websocket", config=config)
        
        # WebSocket configuration
        self.ws_url = self.config.get("ws_url", "wss://data.tradingview.com/socket.io/websocket")
        
        # Authentication details
        self.username = self.config.get("username", "")
        self.password = self.config.get("password", "")
        self.session_token = self.config.get("session_token", "")
        self.auth_token = self.config.get("auth_token", "")
        
        # Subscription info
        self.symbols = set()  # Symbols to watch
        self.studies = set()  # Studies to track
        self.timeframes = set()  # Timeframes to monitor
        
        # Message handlers by type
        self.message_handlers: Dict[str, List[Callable]] = {}
        
        # Underlying WebSocket client
        self._ws_client = None
        
        # Session tracking
        self.chart_session = f"cs_{uuid.uuid4().hex[:12]}"
        self.quote_session = f"qs_{uuid.uuid4().hex[:12]}"
        self.private_channel = f"pc_{uuid.uuid4().hex[:12]}"
        self.chart_session_registered = False
        self.quote_session_registered = False
        
        # Subscription tracking
        self.subscribed_symbols: Dict[str, Dict[str, Any]] = {}  # symbol -> subscription info
        
        # Message parsing
        self.message_buffer = []
        
        # Rate limiting
        self.last_request_time = 0
        self.request_interval = 0.1  # seconds between requests
        
        # Data buffers
        self.last_quotes: Dict[str, Dict[str, Any]] = {}  # symbol -> quote data
        self.last_charts: Dict[str, Dict[str, Dict[str, Any]]] = {}  # symbol -> timeframe -> chart data
        
        logger.info("TradingView WebSocket client initialized")
    
    async def start(self) -> bool:
        """
        Start the WebSocket client and connect to TradingView.
        
        Returns:
            bool: True if started successfully, False otherwise
        """
        try:
            # Update component status
            self._update_status(ComponentStatus.INITIALIZING)
            
            # Create underlying WebSocket client
            ws_config = {
                "ws_url": self.ws_url,
                "ping_interval": 15,  # TradingView expects more frequent pings
                "reconnect_interval": 3,
                "heartbeat_timeout": 30
            }
            self._ws_client = WebSocketClient(ws_config, "tradingview")
            
            # Register message handler
            self._ws_client.register_message_handler("", self._handle_raw_message)
            
            # Connect to WebSocket server
            connected = await self._ws_client.connect()
            if not connected:
                logger.error("Failed to connect to TradingView WebSocket")
                self._update_status(ComponentStatus.ERROR, "Connection failed")
                return False
            
            # Authenticate
            if self.session_token or (self.username and self.password):
                authenticated = await self._authenticate()
                if not authenticated:
                    logger.error("Failed to authenticate with TradingView")
                    self._update_status(ComponentStatus.ERROR, "Authentication failed")
                    return False
            
            # Set up sessions
            await self._setup_sessions()
            
            # Subscribe to previously added symbols
            if self.symbols:
                await self._resubscribe_all()
            
            # Update component status
            self._update_status(ComponentStatus.OPERATIONAL)
            logger.info("TradingView WebSocket client started")
            return True
            
        except Exception as e:
            logger.error(f"Error starting TradingView WebSocket client: {str(e)}", exc_info=True)
            self._update_status(ComponentStatus.ERROR, f"Start error: {str(e)}")
            return False
    
    async def stop(self) -> bool:
        """
        Stop the WebSocket client.
        
        Returns:
            bool: True if stopped successfully, False otherwise
        """
        try:
            # Update component status
            self._update_status(ComponentStatus.STOPPING)
            
            # Disconnect WebSocket
            if self._ws_client:
                await self._ws_client.disconnect()
                self._ws_client = None
            
            # Update component status
            self._update_status(ComponentStatus.STOPPED)
            logger.info("TradingView WebSocket client stopped")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping TradingView WebSocket client: {str(e)}", exc_info=True)
            self._update_status(ComponentStatus.ERROR, f"Stop error: {str(e)}")
            return False
    
    async def add_symbol(self, symbol: str, timeframes: List[str] = None) -> bool:
        """
        Add a symbol to track.
        
        Args:
            symbol: Symbol to track (in TradingView format, e.g., "BINANCE:BTCUSDT")
            timeframes: List of timeframes to subscribe to (e.g., ["1", "5", "15", "60", "D"])
            
        Returns:
            bool: True if added successfully, False otherwise
        """
        try:
            # Convert symbol to TradingView format if needed
            if ":" not in symbol:
                symbol = f"BINANCE:{symbol}"
            
            # Add to symbols set
            self.symbols.add(symbol)
            
            # Default timeframes
            if not timeframes:
                timeframes = ["1", "5", "15", "60", "D"]
            
            # Add timeframes to set
            for tf in timeframes:
                self.timeframes.add(tf)
            
            # Subscribe if connected
            if self._ws_client and self._ws_client.is_connected():
                # Subscribe to quotes
                await self._subscribe_quotes(symbol)
                
                # Subscribe to charts for each timeframe
                for timeframe in timeframes:
                    await self._subscribe_chart(symbol, timeframe)
            
            logger.info(f"Added symbol {symbol} with timeframes {timeframes}")
            return True
            
        except Exception as e:
            logger.error(f"Error adding symbol {symbol}: {str(e)}", exc_info=True)
            return False
    
    async def remove_symbol(self, symbol: str) -> bool:
        """
        Remove a symbol from tracking.
        
        Args:
            symbol: Symbol to remove
            
        Returns:
            bool: True if removed successfully, False otherwise
        """
        try:
            # Convert symbol to TradingView format if needed
            if ":" not in symbol:
                symbol = f"BINANCE:{symbol}"
            
            # Remove from symbols set
            if symbol in self.symbols:
                self.symbols.remove(symbol)
            
            # Unsubscribe if connected
            if self._ws_client and self._ws_client.is_connected():
                # Unsubscribe from quotes
                await self._unsubscribe_quotes(symbol)
                
                # Unsubscribe from charts for all timeframes
                for timeframe in self.timeframes:
                    await self._unsubscribe_chart(symbol, timeframe)
            
            # Remove from subscription tracking
            if symbol in self.subscribed_symbols:
                del self.subscribed_symbols[symbol]
            
            # Remove from data buffers
            if symbol in self.last_quotes:
                del self.last_quotes[symbol]
            
            if symbol in self.last_charts:
                del self.last_charts[symbol]
            
            logger.info(f"Removed symbol {symbol}")
            return True
            
        except Exception as e:
            logger.error(f"Error removing symbol {symbol}: {str(e)}", exc_info=True)
            return False
    
    def register_message_handler(self, message_type: str, handler: Callable) -> None:
        """
        Register a message handler for a specific message type.
        
        Args:
            message_type: Message type to handle
            handler: Handler function
        """
        if message_type not in self.message_handlers:
            self.message_handlers[message_type] = []
        
        self.message_handlers[message_type].append(handler)
        logger.info(f"Registered handler for {message_type} messages")
    
    def get_last_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get the last quote for a symbol.
        
        Args:
            symbol: Symbol to get quote for
            
        Returns:
            Quote data or None if not available
        """
        # Convert symbol to TradingView format if needed
        if ":" not in symbol:
            symbol = f"BINANCE:{symbol}"
            
        return self.last_quotes.get(symbol)
    
    def get_last_chart(self, symbol: str, timeframe: str) -> Optional[Dict[str, Any]]:
        """
        Get the last chart for a symbol and timeframe.
        
        Args:
            symbol: Symbol to get chart for
            timeframe: Timeframe to get chart for
            
        Returns:
            Chart data or None if not available
        """
        # Convert symbol to TradingView format if needed
        if ":" not in symbol:
            symbol = f"BINANCE:{symbol}"
            
        if symbol in self.last_charts and timeframe in self.last_charts[symbol]:
            return self.last_charts[symbol][timeframe]
            
        return None
    
    async def _authenticate(self) -> bool:
        """
        Authenticate with TradingView.
        
        Returns:
            bool: True if authenticated successfully, False otherwise
        """
        try:
            # If session token is available, use it
            if self.session_token:
                # Create session authentication message
                auth_message = {
                    "m": "set_auth_token",
                    "p": [self.session_token]
                }
                
                # Send authentication message
                await self._send_message(auth_message)
                
                # For simplicity, we assume authentication succeeded
                # In a real implementation, we would check the response
                logger.info("Authenticated with TradingView using session token")
                return True
                
            # Otherwise use username/password
            elif self.username and self.password:
                # Create login message
                login_message = {
                    "m": "login",
                    "p": [self.username, self.password]
                }
                
                # Send login message
                await self._send_message(login_message)
                
                # For simplicity, we assume authentication succeeded
                # In a real implementation, we would check the response
                logger.info("Authenticated with TradingView using username/password")
                return True
                
            else:
                logger.warning("No authentication credentials provided")
                return False
                
        except Exception as e:
            logger.error(f"Error authenticating with TradingView: {str(e)}", exc_info=True)
            return False
    
    async def _setup_sessions(self) -> bool:
        """
        Set up quote and chart sessions.
        
        Returns:
            bool: True if sessions set up successfully, False otherwise
        """
        try:
            # Create quote session
            quote_session_message = {
                "m": "quote_create_session",
                "p": [self.quote_session]
            }
            await self._send_message(quote_session_message)
            
            # Create chart session
            chart_session_message = {
                "m": "chart_create_session",
                "p": [self.chart_session, ""]
            }
            await self._send_message(chart_session_message)
            
            # Set chart session flags
            flags_message = {
                "m": "set_chart_session_flags",
                "p": [self.chart_session, ["series_data_frequency", "study_data_frequency"]]
            }
            await self._send_message(flags_message)
            
            # For simplicity, we assume sessions are created successfully
            self.chart_session_registered = True
            self.quote_session_registered = True
            
            logger.info("TradingView sessions set up")
            return True
            
        except Exception as e:
            logger.error(f"Error setting up TradingView sessions: {str(e)}", exc_info=True)
            return False
    
    async def _resubscribe_all(self) -> None:
        """Resubscribe to all symbols and timeframes."""
        for symbol in self.symbols:
            # Subscribe to quotes
            await self._subscribe_quotes(symbol)
            
            # Subscribe to charts for each timeframe
            for timeframe in self.timeframes:
                await self._subscribe_chart(symbol, timeframe)
    
    async def _subscribe_quotes(self, symbol: str) -> bool:
        """
        Subscribe to quotes for a symbol.
        
        Args:
            symbol: Symbol to subscribe to
            
        Returns:
            bool: True if subscribed successfully, False otherwise
        """
        try:
            # Check if quote session is registered
            if not self.quote_session_registered:
                logger.error("Cannot subscribe to quotes: Quote session not registered")
                return False
                
            # Create subscription message
            subscription_message = {
                "m": "quote_add_symbols",
                "p": [self.quote_session, symbol]
            }
            
            # Send subscription message
            await self._send_message(subscription_message)
            
            # Add to subscription tracking
            if symbol not in self.subscribed_symbols:
                self.subscribed_symbols[symbol] = {}
            self.subscribed_symbols[symbol]["quotes"] = True
            
            logger.info(f"Subscribed to quotes for {symbol}")
            return True
            
        except Exception as e:
            logger.error(f"Error subscribing to quotes for {symbol}: {str(e)}", exc_info=True)
            return False
    
    async def _unsubscribe_quotes(self, symbol: str) -> bool:
        """
        Unsubscribe from quotes for a symbol.
        
        Args:
            symbol: Symbol to unsubscribe from
            
        Returns:
            bool: True if unsubscribed successfully, False otherwise
        """
        try:
            # Check if quote session is registered
            if not self.quote_session_registered:
                logger.error("Cannot unsubscribe from quotes: Quote session not registered")
                return False
                
            # Create unsubscription message
            unsubscription_message = {
                "m": "quote_remove_symbols",
                "p": [self.quote_session, symbol]
            }
            
            # Send unsubscription message
            await self._send_message(unsubscription_message)
            
            # Update subscription tracking
            if symbol in self.subscribed_symbols and "quotes" in self.subscribed_symbols[symbol]:
                self.subscribed_symbols[symbol]["quotes"] = False
            
            logger.info(f"Unsubscribed from quotes for {symbol}")
            return True
            
        except Exception as e:
            logger.error(f"Error unsubscribing from quotes for {symbol}: {str(e)}", exc_info=True)
            return False
    
    async def _subscribe_chart(self, symbol: str, timeframe: str) -> bool:
        """
        Subscribe to chart data for a symbol and timeframe.
        
        Args:
            symbol: Symbol to subscribe to
            timeframe: Timeframe to subscribe to
            
        Returns:
            bool: True if subscribed successfully, False otherwise
        """
        try:
            # Check if chart session is registered
            if not self.chart_session_registered:
                logger.error("Cannot subscribe to chart: Chart session not registered")
                return False
                
            # Convert timeframe if needed
            timeframe_str = self._convert_timeframe(timeframe)
            
            # Create subscription ID
            sub_id = f"cs_{symbol}_{timeframe_str}"
            
            # Create subscription message
            subscription_message = {
                "m": "chart_create_series",
                "p": [self.chart_session, sub_id, "s1", symbol, timeframe_str, 300, ""]
            }
            
            # Send subscription message
            await self._send_message(subscription_message)
            
            # Add to subscription tracking
            if symbol not in self.subscribed_symbols:
                self.subscribed_symbols[symbol] = {}
            if "charts" not in self.subscribed_symbols[symbol]:
                self.subscribed_symbols[symbol]["charts"] = {}
            self.subscribed_symbols[symbol]["charts"][timeframe] = sub_id
            
            # Initialize chart data buffer
            if symbol not in self.last_charts:
                self.last_charts[symbol] = {}
            self.last_charts[symbol][timeframe] = {}
            
            logger.info(f"Subscribed to chart for {symbol} at {timeframe}")
            return True
            
        except Exception as e:
            logger.error(f"Error subscribing to chart for {symbol} at {timeframe}: {str(e)}", exc_info=True)
            return False
    
    async def _unsubscribe_chart(self, symbol: str, timeframe: str) -> bool:
        """
        Unsubscribe from chart data for a symbol and timeframe.
        
        Args:
            symbol: Symbol to unsubscribe from
            timeframe: Timeframe to unsubscribe from
            
        Returns:
            bool: True if unsubscribed successfully, False otherwise
        """
        try:
            # Check if chart session is registered
            if not self.chart_session_registered:
                logger.error("Cannot unsubscribe from chart: Chart session not registered")
                return False
                
            # Get subscription ID
            if (symbol in self.subscribed_symbols and
                "charts" in self.subscribed_symbols[symbol] and
                timeframe in self.subscribed_symbols[symbol]["charts"]):
                sub_id = self.subscribed_symbols[symbol]["charts"][timeframe]
            else:
                # Not subscribed
                return True
                
            # Create unsubscription message
            unsubscription_message = {
                "m": "chart_delete_series",
                "p": [self.chart_session, sub_id]
            }
            
            # Send unsubscription message
            await self._send_message(unsubscription_message)
            
            # Update subscription tracking
            if timeframe in self.subscribed_symbols[symbol]["charts"]:
                del self.subscribed_symbols[symbol]["charts"][timeframe]
            
            # Remove from data buffer
            if symbol in self.last_charts and timeframe in self.last_charts[symbol]:
                del self.last_charts[symbol][timeframe]
            
            logger.info(f"Unsubscribed from chart for {symbol} at {timeframe}")
            return True
            
        except Exception as e:
            logger.error(f"Error unsubscribing from chart for {symbol} at {timeframe}: {str(e)}", exc_info=True)
            return False
    
    async def _send_message(self, message: Dict[str, Any]) -> None:
        """
        Send a message to TradingView.
        
        Args:
            message: Message to send
        """
        if not self._ws_client or not self._ws_client.is_connected():
            raise ConnectionError("WebSocket not connected")
            
        # Rate limiting
        now = time.time()
        if now - self.last_request_time < self.request_interval:
            await asyncio.sleep(self.request_interval - (now - self.last_request_time))
        
        # Format message for TradingView
        formatted_message = self._format_message(message)
        
        # Send message
        await self._ws_client._send_message({"data": formatted_message})
        
        # Update rate limit tracking
        self.last_request_time = time.time()
    
    def _format_message(self, message: Dict[str, Any]) -> str:
        """
        Format a message for TradingView.
        
        Args:
            message: Message to format
            
        Returns:
            Formatted message string
        """
        method = message.get("m", "")
        params = message.get("p", [])
        
        # Format as ~m~length~m~{json}
        json_message = json.dumps({"m": method, "p": params})
        message_length = len(json_message)
        
        return f"~m~{message_length}~m~{json_message}"
    
    async def _handle_raw_message(self, message: Dict[str, Any]) -> None:
        """
        Handle a raw WebSocket message.
        
        Args:
            message: Raw message
        """
        try:
            # Extract message data
            data = message.get("data", "")
            
            # Parse TradingView message format
            parsed_messages = self._parse_tradingview_message(data)
            
            # Process each parsed message
            for parsed in parsed_messages:
                await self._process_parsed_message(parsed)
                
        except Exception as e:
            logger.error(f"Error handling raw message: {str(e)}", exc_info=True)
    
    def _parse_tradingview_message(self, data: str) -> List[Dict[str, Any]]:
        """
        Parse a TradingView message.
        
        Args:
            data: Raw message data
            
        Returns:
            List of parsed messages
        """
        messages = []
        
        try:
            # TradingView uses a format like: ~m~length~m~{json}~m~length~m~{json}
            # Split by ~m~
            parts = data.split("~m~")
            
            # Process parts
            i = 0
            while i < len(parts):
                if parts[i] == "":
                    i += 1
                    continue
                    
                try:
                    # Length
                    length = int(parts[i])
                    i += 1
                    
                    # Skip the next part (which is empty)
                    i += 1
                    
                    # Message
                    if i < len(parts):
                        message = parts[i]
                        
                        # Check if it's a ping
                        if message.startswith("h"):
                            # Respond with pong
                            if self._ws_client:
                                asyncio.create_task(self._send_pong(message))
                        else:
                            # Parse JSON
                            try:
                                json_message = json.loads(message)
                                messages.append(json_message)
                            except json.JSONDecodeError:
                                logger.warning(f"Failed to parse JSON message: {message}")
                        
                        i += 1
                except (ValueError, IndexError):
                    # Skip invalid parts
                    i += 1
                    
        except Exception as e:
            logger.error(f"Error parsing TradingView message: {str(e)}", exc_info=True)
            
        return messages
    
    async def _send_pong(self, ping_message: str) -> None:
        """
        Send a pong response to a ping.
        
        Args:
            ping_message: Ping message
        """
        try:
            # Extract ping number
            ping_number = ping_message.replace("h", "")
            
            # Create pong message
            pong_message = f"~h~{ping_number}"
            
            # Send pong
            await self._ws_client._send_message({"data": pong_message})
            
        except Exception as e:
            logger.error(f"Error sending pong: {str(e)}", exc_info=True)
    
    async def _process_parsed_message(self, message: Dict[str, Any]) -> None:
        """
        Process a parsed TradingView message.
        
        Args:
            message: Parsed message
        """
        try:
            # Extract message type
            message_type = message.get("m", "")
            params = message.get("p", [])
            
            # Handle different message types
            if message_type == "quote_completed":
                # Quote subscription completed
                await self._handle_quote_completed(params)
                
            elif message_type == "quote_update":
                # Quote update
                await self._handle_quote_update(params)
                
            elif message_type == "series_completed":
                # Chart series subscription completed
                await self._handle_series_completed(params)
                
            elif message_type == "series_data":
                # Chart series data
                await self._handle_series_data(params)
                
            elif message_type == "timescale_update":
                # Timescale update (new bar)
                await self._handle_timescale_update(params)
                
            elif message_type == "symbol_error":
                # Symbol error
                await self._handle_symbol_error(params)
                
            # Call registered handlers for this message type
            if message_type in self.message_handlers:
                for handler in self.message_handlers[message_type]:
                    try:
                        await handler(params)
                    except Exception as e:
                        logger.error(f"Error in message handler for {message_type}: {str(e)}", exc_info=True)
                        
        except Exception as e:
            logger.error(f"Error processing parsed message: {str(e)}", exc_info=True)
    
    async def _handle_quote_completed(self, params: List[Any]) -> None:
        """
        Handle quote subscription completed message.
        
        Args:
            params: Message parameters
        """
        if len(params) < 2:
            return
            
        session_id = params[0]
        symbols = params[1]
        
        logger.info(f"Quote subscription completed for {symbols}")
    
    async def _handle_quote_update(self, params: List[Any]) -> None:
        """
        Handle quote update message.
        
        Args:
            params: Message parameters
        """
        if len(params) < 2:
            return
            
        session_id = params[0]
        quotes = params[1]
        
        for symbol, quote_data in quotes.items():
            # Process quote data
            parsed_quote = self._parse_quote_data(symbol, quote_data)
            
            # Store in last quotes
            self.last_quotes[symbol] = parsed_quote
            
            # Trigger handlers for quote updates
            if "quote_update" in self.message_handlers:
                for handler in self.message_handlers["quote_update"]:
                    try:
                        await handler(parsed_quote)
                    except Exception as e:
                        logger.error(f"Error in quote update handler: {str(e)}", exc_info=True)
    
    def _parse_quote_data(self, symbol: str, quote_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse quote data.
        
        Args:
            symbol: Symbol
            quote_data: Raw quote data
            
        Returns:
            Parsed quote data
        """
        # TradingView quote data structure varies, but typically includes:
        # v: [last_price, change, change_percent, ...]
        # f: [flags]
        # t: timestamp
        parsed = {
            "symbol": symbol,
            "timestamp": int(time.time() * 1000)  # Default to current time
        }
        
        if "v" in quote_data and isinstance(quote_data["v"], list):
            values = quote_data["v"]
            
            if len(values) >= 1:
                parsed["last"] = values[0]
            if len(values) >= 2:
                parsed["change"] = values[1]
            if len(values) >= 3:
                parsed["change_percent"] = values[2]
            if len(values) >= 4:
                parsed["volume"] = values[3]
            if len(values) >= 5:
                parsed["open"] = values[4]
            if len(values) >= 6:
                parsed["high"] = values[5]
            if len(values) >= 7:
                parsed["low"] = values[6]
            if len(values) >= 8:
                parsed["close"] = values[7]
                
        if "t" in quote_data:
            parsed["timestamp"] = quote_data["t"]
            
        return parsed
    
    async def _handle_series_completed(self, params: List[Any]) -> None:
        """
        Handle chart series subscription completed message.
        
        Args:
            params: Message parameters
        """
        if len(params) < 3:
            return
            
        session_id = params[0]
        series_id = params[1]
        symbol_info = params[2]
        
        logger.info(f"Chart series subscription completed for {series_id}")
    
    async def _handle_series_data(self, params: List[Any]) -> None:
        """
        Handle chart series data message.
        
        Args:
            params: Message parameters
        """
        if len(params) < 3:
            return
            
        session_id = params[0]
        series_id = params[1]
        series_data = params[2]
        
        # Extract symbol and timeframe from series_id
        symbol, timeframe = self._parse_series_id(series_id)
        if not symbol or not timeframe:
            return
            
        # Process series data
        parsed_data = self._parse_series_data(symbol, timeframe, series_data)
        
        # Store in last charts
        if symbol not in self.last_charts:
            self.last_charts[symbol] = {}
        self.last_charts[symbol][timeframe] = parsed_data
        
        # Trigger handlers for series data
        if "series_data" in self.message_handlers:
            for handler in self.message_handlers["series_data"]:
                try:
                    await handler(parsed_data)
                except Exception as e:
                    logger.error(f"Error in series data handler: {str(e)}", exc_info=True)
    
    def _parse_series_id(self, series_id: str) -> Tuple[str, str]:
        """
        Parse series ID to extract symbol and timeframe.
        
        Args:
            series_id: Series ID
            
        Returns:
            Tuple of (symbol, timeframe)
        """
        # Series ID format: cs_BINANCE:BTCUSDT_1
        parts = series_id.split("_")
        if len(parts) < 3:
            return None, None
            
        # Join all middle parts as symbol (in case symbol contains underscore)
        symbol = "_".join(parts[1:-1])
        timeframe = parts[-1]
        
        return symbol, timeframe
    
    def _parse_series_data(self, symbol: str, timeframe: str, series_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse series data.
        
        Args:
            symbol: Symbol
            timeframe: Timeframe
            series_data: Raw series data
            
        Returns:
            Parsed series data
        """
        parsed = {
            "symbol": symbol,
            "timeframe": timeframe,
            "timestamp": int(time.time() * 1000),  # Default to current time
            "candles": []
        }
        
        if "s" in series_data and "v" in series_data:
            status = series_data["s"]
            values = series_data["v"]
            
            # For now, we only process status "ok"
            if status == "ok" and isinstance(values, list):
                for candle_data in values:
                    if isinstance(candle_data, list) and len(candle_data) >= 6:
                        candle = {
                            "time": candle_data[0],  # Timestamp
                            "open": candle_data[1],
                            "high": candle_data[2],
                            "low": candle_data[3],
                            "close": candle_data[4],
                            "volume": candle_data[5]
                        }
                        parsed["candles"].append(candle)
                
        return parsed
    
    async def _handle_timescale_update(self, params: List[Any]) -> None:
        """
        Handle timescale update message (new bar).
        
        Args:
            params: Message parameters
        """
        if len(params) < 3:
            return
            
        session_id = params[0]
        update_data = params[1]
        
        # Process update data
        # This varies based on the update type
        
        # Trigger handlers for timescale update
        if "timescale_update" in self.message_handlers:
            for handler in self.message_handlers["timescale_update"]:
                try:
                    await handler(update_data)
                except Exception as e:
                    logger.error(f"Error in timescale update handler: {str(e)}", exc_info=True)
    
    async def _handle_symbol_error(self, params: List[Any]) -> None:
        """
        Handle symbol error message.
        
        Args:
            params: Message parameters
        """
        if len(params) < 2:
            return
            
        session_id = params[0]
        error_info = params[1]
        
        symbol = error_info.get("n", "unknown")
        error = error_info.get("v", {}).get("error", "unknown error")
        
        logger.error(f"Symbol error for {symbol}: {error}")
    
    def _convert_timeframe(self, timeframe: str) -> str:
        """
        Convert timeframe to TradingView format.
        
        Args:
            timeframe: Timeframe string
            
        Returns:
            TradingView timeframe format
        """
        # Map common timeframes to TradingView format
        timeframe_map = {
            "1m": "1",
            "3m": "3",
            "5m": "5",
            "15m": "15",
            "30m": "30",
            "1h": "60",
            "2h": "120",
            "4h": "240",
            "1d": "D",
            "1w": "W",
            "1M": "M"
        }
        
        # Check if already in TradingView format
        if timeframe in ["1", "3", "5", "15", "30", "60", "120", "240", "D", "W", "M"]:
            return timeframe
            
        # Check if in map
        if timeframe in timeframe_map:
            return timeframe_map[timeframe]
            
        # Try to parse as minutes/hours
        if timeframe.endswith("m"):
            try:
                minutes = int(timeframe[:-1])
                return str(minutes)
            except ValueError:
                pass
                
        if timeframe.endswith("h"):
            try:
                hours = int(timeframe[:-1])
                return str(hours * 60)
            except ValueError:
                pass
                
        # Default to 1 minute
        logger.warning(f"Unknown timeframe format: {timeframe}, defaulting to 1 minute")
        return "1"


async def create_tradingview_websocket(config: Dict[str, Any]) -> TradingViewWebSocketClient:
    """
    Create and start a TradingView WebSocket client.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        TradingViewWebSocketClient instance
    """
    client = TradingViewWebSocketClient(config)
    success = await client.start()
    if not success:
        logger.error("Failed to start TradingView WebSocket client")
    return client 