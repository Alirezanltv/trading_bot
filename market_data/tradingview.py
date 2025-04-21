"""
TradingView Integration Module

This module provides integration with TradingView for market data.
"""

import os
import time
import json
import random
import asyncio
import logging
import ssl
import websockets
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta

from trading_system.core.component import Component, ComponentStatus
from trading_system.core.message_bus import MessageBus, MessageTypes
from trading_system.core.logging import get_logger
from trading_system.core.config import ConfigManager

logger = get_logger("market_data.tradingview")

@dataclass
class TradingViewCredentials:
    """
    TradingView credentials for authentication.
    
    Attributes:
        username: TradingView username
        password: TradingView password
    """
    username: str
    password: str

class TradingViewDataProvider(Component):
    """
    TradingView market data provider.
    
    Retrieves market data from TradingView using their API.
    Implements caching, failover, and connection management.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize TradingView data provider.
        
        Args:
            config: Provider configuration
        """
        super().__init__(
            component_id="tradingview_provider",
            name="TradingView Data Provider",
            config=config
        )
        
        self.logger = get_logger("market_data.tradingview")
        
        # Credentials
        self.credentials = config.get("credentials", {})
        self.username = self.credentials.get("username", "")
        self.password = self.credentials.get("password", "")
        
        # Symbols and timeframes
        self.symbols = config.get("symbols", [])
        self.timeframes = config.get("timeframes", ["1h"])
        
        # Cache settings
        self.cache_ttl = timedelta(seconds=config.get("cache_ttl_seconds", 60))
        
        # API settings
        self.api_url = config.get("api_url", "https://scanner.tradingview.com")
        self.user_agent = config.get("user_agent", "Mozilla/5.0")
        self.request_timeout = config.get("request_timeout", 30)
        
        # Session and auth
        self.session = None
        self.auth_token = None
        self.last_auth_time = None
        
        # Data cache
        self.market_data_cache: Dict[str, Dict[str, Any]] = {}
        
        # Connection state
        self.connected = False
        self.connection_lock = asyncio.Lock()
        self.retry_count = 0
        self.max_retries = config.get("max_retries", 3)
        self.retry_delay = config.get("retry_delay_seconds", 5)
        
        # Background tasks
        self.update_task = None
        
        self.logger.info(f"TradingView data provider initialized with {len(self.symbols)} symbols")
    
    async def initialize(self) -> bool:
        """
        Initialize the data provider.
        
        Returns:
            Success flag
        """
        self.logger.info("Initializing TradingView data provider")
        
        try:
            # Update status
            self._update_status(ComponentStatus.INITIALIZING)
            
            # Import aiohttp here to avoid dependency issues
            import aiohttp
            
            # Create session
            self.session = aiohttp.ClientSession(
                headers={
                    "User-Agent": self.user_agent
                }
            )
            
            # Test connection
            success = await self._test_connection()
            
            if not success:
                self.logger.error("Failed to connect to TradingView API")
                self._update_status(ComponentStatus.ERROR)
                return False
            
            # Start update task
            self.update_task = asyncio.create_task(self._update_loop())
            
            # Update status
            self._update_status(ComponentStatus.OPERATIONAL)
            
            self.logger.info("TradingView data provider initialized successfully")
            return True
            
        except ImportError:
            self.logger.error("Failed to import aiohttp - please install it with 'pip install aiohttp'")
            self._update_status(ComponentStatus.ERROR)
            return False
            
        except Exception as e:
            self.logger.error(f"Error initializing TradingView data provider: {str(e)}")
            self._update_status(ComponentStatus.ERROR)
            return False
    
    async def shutdown(self) -> bool:
        """
        Shutdown the data provider.
        
        Returns:
            Success flag
        """
        self.logger.info("Shutting down TradingView data provider")
        
        try:
            # Update status
            self._update_status(ComponentStatus.SHUTTING_DOWN)
            
            # Cancel update task
            if self.update_task and not self.update_task.done():
                self.update_task.cancel()
                try:
                    await self.update_task
                except asyncio.CancelledError:
                    pass
            
            # Close session
            if self.session:
                await self.session.close()
                self.session = None
            
            # Update status
            self._update_status(ComponentStatus.SHUTDOWN)
            
            self.logger.info("TradingView data provider shutdown complete")
            return True
            
        except Exception as e:
            self.logger.error(f"Error shutting down TradingView data provider: {str(e)}")
            return False
    
    async def get_market_data(self, symbol: str, timeframe: str = "1h") -> Optional[Dict[str, Any]]:
        """
        Get market data for a symbol.
        
        Args:
            symbol: Trading symbol
            timeframe: Chart timeframe
            
        Returns:
            Market data or None if not available
        """
        # Check symbol support
        if symbol not in self.symbols:
            self.logger.warning(f"Symbol {symbol} not in configured symbols list")
            self.symbols.append(symbol)
        
        # Check timeframe support
        if timeframe not in self.timeframes:
            self.logger.warning(f"Timeframe {timeframe} not in configured timeframes list")
            self.timeframes.append(timeframe)
        
        # Get cache key
        cache_key = f"{symbol}_{timeframe}"
        
        # Check cache
        if cache_key in self.market_data_cache:
            cache_entry = self.market_data_cache[cache_key]
            cache_time = cache_entry.get("timestamp")
            
            # Check if cache is still valid
            if cache_time and datetime.now() - cache_time < self.cache_ttl:
                return cache_entry.get("data")
        
        # Not in cache or cache expired, fetch new data
        try:
            # Ensure connected
            if not self.connected:
                connected = await self._connect()
                if not connected:
                    self.logger.error("Failed to connect to TradingView API")
                    return None
            
            # Fetch data
            data = await self._fetch_market_data(symbol, timeframe)
            
            if data:
                # Update cache
                self.market_data_cache[cache_key] = {
                    "timestamp": datetime.now(),
                    "data": data
                }
                
                return data
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting market data for {symbol} ({timeframe}): {str(e)}")
            return None
    
    async def _connect(self) -> bool:
        """
        Connect to TradingView API.
        
        Returns:
            Success flag
        """
        async with self.connection_lock:
            if self.connected:
                return True
            
            self.logger.info("Connecting to TradingView API")
            
            try:
                # Check session
                if self.session is None:
                    import aiohttp
                    self.session = aiohttp.ClientSession(
                        headers={
                            "User-Agent": self.user_agent
                        }
                    )
                
                # Authenticate if credentials provided
                if self.username and self.password:
                    auth_success = await self._authenticate()
                    if not auth_success:
                        self.logger.error("TradingView authentication failed")
                        return False
                
                # Mark as connected
                self.connected = True
                self.retry_count = 0
                
                self.logger.info("Connected to TradingView API")
                return True
                
            except Exception as e:
                self.logger.error(f"Error connecting to TradingView API: {str(e)}")
                self.connected = False
                
                # Retry logic
                self.retry_count += 1
                if self.retry_count > self.max_retries:
                    self.logger.error(f"Exceeded maximum retry attempts ({self.max_retries})")
                    return False
                
                # Wait before retry
                retry_delay = self.retry_delay * (2 ** (self.retry_count - 1))  # Exponential backoff
                self.logger.info(f"Retrying connection in {retry_delay} seconds (attempt {self.retry_count})")
                await asyncio.sleep(retry_delay)
                
                return await self._connect()
    
    async def _authenticate(self) -> bool:
        """
        Authenticate with TradingView.
        
        Returns:
            Success flag
        """
        # Check if already authenticated recently
        if self.auth_token and self.last_auth_time:
            auth_age = datetime.now() - self.last_auth_time
            if auth_age < timedelta(hours=23):  # Token valid for 24h, refresh before expiry
                return True
        
        self.logger.info("Authenticating with TradingView")
        
        try:
            # Build auth payload
            auth_payload = {
                "username": self.username,
                "password": self.password,
                "remember": True
            }
            
            # Send auth request
            async with self.session.post(
                f"{self.api_url}/api/v1/signin/",
                json=auth_payload,
                timeout=self.request_timeout
            ) as response:
                if response.status == 200:
                    # Check for auth token in cookies
                    cookies = self.session.cookie_jar.filter_cookies(self.api_url)
                    if "auth_token" in cookies:
                        self.auth_token = cookies["auth_token"].value
                        self.last_auth_time = datetime.now()
                        self.logger.info("TradingView authentication successful")
                        return True
                
                # Authentication failed
                self.logger.error(f"TradingView authentication failed: {response.status}")
                self.auth_token = None
                self.last_auth_time = None
                return False
                
        except Exception as e:
            self.logger.error(f"Error during TradingView authentication: {str(e)}")
            return False
    
    async def _test_connection(self) -> bool:
        """
        Test connection to TradingView API.
        
        Returns:
            Success flag
        """
        self.logger.info("Testing connection to TradingView API")
        
        try:
            # Simple request to test connectivity
            async with self.session.get(
                f"{self.api_url}/api/v2/homepage/",
                timeout=self.request_timeout
            ) as response:
                if response.status == 200:
                    self.logger.info("TradingView API connection test successful")
                    return True
                
                self.logger.error(f"TradingView API connection test failed: {response.status}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error testing TradingView API connection: {str(e)}")
            return False
    
    async def _fetch_market_data(self, symbol: str, timeframe: str) -> Optional[Dict[str, Any]]:
        """
        Fetch market data from TradingView.
        
        Args:
            symbol: Trading symbol
            timeframe: Chart timeframe
            
        Returns:
            Market data or None if not available
        """
        self.logger.debug(f"Fetching market data for {symbol} ({timeframe})")
        
        # In a real implementation, this would use the TradingView API to fetch data
        # For this example, we'll just generate some random data
        
        # For testing purposes with a mock implementation
        # In reality, we would use the TradingView scanning API
        return self._generate_mock_data(symbol, timeframe)
    
    def _generate_mock_data(self, symbol: str, timeframe: str) -> Dict[str, Any]:
        """
        Generate mock market data for testing.
        
        Args:
            symbol: Trading symbol
            timeframe: Chart timeframe
            
        Returns:
            Mock market data
        """
        import random
        
        # Base price based on symbol
        base_price = 50000.0 if "btc" in symbol.lower() else 2000.0
        
        # Add some randomness
        price_range = base_price * 0.01  # 1% range
        current_price = base_price + (random.random() * price_range * 2) - price_range
        
        # Generate OHLCV data
        open_price = current_price * (1 + (random.random() * 0.01 - 0.005))
        high_price = max(open_price, current_price) * (1 + random.random() * 0.01)
        low_price = min(open_price, current_price) * (1 - random.random() * 0.01)
        volume = random.random() * 100 + 10
        
        # Calculate technical indicators
        rsi = random.random() * 100
        ma_50 = current_price * (1 + (random.random() * 0.02 - 0.01))
        ma_200 = current_price * (1 + (random.random() * 0.05 - 0.025))
        
        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "last": current_price,
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "volume": volume,
            "timestamp": datetime.now().timestamp(),
            "indicators": {
                "rsi": rsi,
                "ma_50": ma_50,
                "ma_200": ma_200
            }
        }
    
    async def _update_loop(self) -> None:
        """Background loop to update market data periodically."""
        self.logger.info("Starting market data update loop")
        
        update_interval = self.config.get("update_interval_seconds", 60)
        
        try:
            while True:
                for symbol in self.symbols:
                    for timeframe in self.timeframes:
                        # Update market data
                        try:
                            await self.get_market_data(symbol, timeframe)
                        except Exception as e:
                            self.logger.error(f"Error updating market data for {symbol} ({timeframe}): {str(e)}")
                
                # Wait for next update
                await asyncio.sleep(update_interval)
                
        except asyncio.CancelledError:
            self.logger.info("Market data update loop cancelled")
        except Exception as e:
            self.logger.error(f"Error in market data update loop: {str(e)}")
    
    def get_symbols(self) -> List[str]:
        """
        Get list of supported symbols.
        
        Returns:
            List of symbols
        """
        return self.symbols
    
    def get_timeframes(self) -> List[str]:
        """
        Get list of supported timeframes.
        
        Returns:
            List of timeframes
        """
        return self.timeframes

class TradingViewWebhookHandler:
    """
    TradingView Webhook Handler
    
    This class handles webhooks from TradingView for signals.
    """
    
    def __init__(self, config: Dict[str, Any], message_bus: MessageBus):
        """
        Initialize TradingView webhook handler.
        
        Args:
            config: Webhook configuration
            message_bus: Message bus for publishing signals
        """
        self.config = config or {}
        self.message_bus = message_bus
        
        # Server configuration
        self.host = config.get("webhook_host", "0.0.0.0")
        self.port = config.get("webhook_port", 5000)
        
        # Signal handlers
        self._signal_handlers = {}
        
        # Server state
        self.server = None
        
        # Component state
        self._status = ComponentStatus.UNINITIALIZED
        
    async def initialize(self) -> bool:
        """
        Initialize TradingView webhook handler.
        
        Returns:
            Initialization success
        """
        try:
            logger.info("Initializing TradingView webhook handler")
            
            # Register default signal handlers
            self.register_signal_handler("buy", self._handle_buy_signal)
            self.register_signal_handler("sell", self._handle_sell_signal)
            self.register_signal_handler("alert", self._handle_alert_signal)
            
            self._status = ComponentStatus.INITIALIZED
            logger.info("TradingView webhook handler initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing TradingView webhook handler: {str(e)}", exc_info=True)
            self._status = ComponentStatus.ERROR
            return False
            
    async def start(self) -> bool:
        """
        Start TradingView webhook handler.
        
        Returns:
            Start success
        """
        try:
            logger.info("Starting TradingView webhook handler")
            
            # Start webhook server
            from aiohttp import web
            
            app = web.Application()
            app.router.add_post("/webhook/{signal_type}", self._handle_webhook)
            
            self.server = web.Server(app)
            runner = web.ServerRunner(self.server)
            await runner.setup()
            
            site = web.TCPSite(runner, self.host, self.port)
            await site.start()
            
            self._status = ComponentStatus.OPERATIONAL
            logger.info(f"TradingView webhook handler started on {self.host}:{self.port}")
            return True
            
        except Exception as e:
            logger.error(f"Error starting TradingView webhook handler: {str(e)}", exc_info=True)
            self._status = ComponentStatus.ERROR
            return False
            
    async def stop(self) -> bool:
        """
        Stop TradingView webhook handler.
        
        Returns:
            Stop success
        """
        try:
            logger.info("Stopping TradingView webhook handler")
            
            # Stop webhook server
            if self.server:
                await self.server.shutdown()
                self.server = None
                
            self._status = ComponentStatus.STOPPED
            logger.info("TradingView webhook handler stopped")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping TradingView webhook handler: {str(e)}", exc_info=True)
            self._status = ComponentStatus.ERROR
            return False
            
    def register_signal_handler(self, signal_type: str, handler: callable) -> None:
        """
        Register a signal handler.
        
        Args:
            signal_type: Signal type
            handler: Handler function
        """
        self._signal_handlers[signal_type] = handler
        logger.debug(f"Registered handler for signal type: {signal_type}")
        
    async def _handle_webhook(self, request):
        """
        Handle webhook request.
        
        Args:
            request: Webhook request
            
        Returns:
            Response
        """
        from aiohttp import web
        
        try:
            # Get signal type from URL
            signal_type = request.match_info.get("signal_type", "unknown")
            
            # Get request body
            body = await request.json()
            
            logger.info(f"Received {signal_type} signal: {body}")
            
            # Handle signal
            if signal_type in self._signal_handlers:
                await self._signal_handlers[signal_type](body)
                return web.Response(text="OK")
            else:
                logger.warning(f"No handler for signal type: {signal_type}")
                return web.Response(text="Unknown signal type", status=400)
                
        except Exception as e:
            logger.error(f"Error handling webhook: {str(e)}", exc_info=True)
            return web.Response(text="Error", status=500)
            
    async def _handle_buy_signal(self, data: Dict[str, Any]) -> None:
        """
        Handle buy signal.
        
        Args:
            data: Signal data
        """
        # Extract signal data
        symbol = data.get("symbol", "")
        price = data.get("price", 0.0)
        amount = data.get("amount", 0.0)
        
        # Create strategy signal
        signal = {
            "strategy_id": data.get("strategy_id", "tradingview"),
            "symbol": symbol,
            "timeframe": data.get("timeframe", "1h"),
            "direction": "buy",
            "price": price,
            "amount": amount,
            "timestamp": int(time.time() * 1000),
            "metadata": data.get("metadata", {})
        }
        
        # Publish signal
        await self.message_bus.publish(MessageTypes.STRATEGY_SIGNAL, signal)
        logger.info(f"Published buy signal for {symbol} at {price}")
        
    async def _handle_sell_signal(self, data: Dict[str, Any]) -> None:
        """
        Handle sell signal.
        
        Args:
            data: Signal data
        """
        # Extract signal data
        symbol = data.get("symbol", "")
        price = data.get("price", 0.0)
        amount = data.get("amount", 0.0)
        
        # Create strategy signal
        signal = {
            "strategy_id": data.get("strategy_id", "tradingview"),
            "symbol": symbol,
            "timeframe": data.get("timeframe", "1h"),
            "direction": "sell",
            "price": price,
            "amount": amount,
            "timestamp": int(time.time() * 1000),
            "metadata": data.get("metadata", {})
        }
        
        # Publish signal
        await self.message_bus.publish(MessageTypes.STRATEGY_SIGNAL, signal)
        logger.info(f"Published sell signal for {symbol} at {price}")
        
    async def _handle_alert_signal(self, data: Dict[str, Any]) -> None:
        """
        Handle alert signal.
        
        Args:
            data: Signal data
        """
        # Extract signal data
        symbol = data.get("symbol", "")
        message = data.get("message", "")
        
        # Log alert
        logger.info(f"Alert for {symbol}: {message}")
        
        # You can also publish the alert to the message bus if needed 