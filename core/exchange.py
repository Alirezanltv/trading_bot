"""
Exchange integration layer for high-reliability trading system.
Provides a consistent interface for interacting with cryptocurrency exchanges.
"""

import os
import time
import json
import hmac
import hashlib
import base64
import threading
import uuid
from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, Any, Optional, List, Tuple, Union, Callable
from urllib.parse import urlencode

import ccxt
import requests
import websocket
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from .config import config
from .logging_setup import get_logger
from .component import Component, ComponentStatus
from .message_bus import message_bus, MessageTypes
from .database import database_manager

logger = get_logger("core.exchange")

class OrderType(Enum):
    """Order types supported by the system."""
    MARKET = "market"
    LIMIT = "limit"
    STOP_MARKET = "stop_market"
    STOP_LIMIT = "stop_limit"
    TAKE_PROFIT = "take_profit"
    TAKE_PROFIT_LIMIT = "take_profit_limit"

class OrderSide(Enum):
    """Order sides supported by the system."""
    BUY = "buy"
    SELL = "sell"

class OrderStatus(Enum):
    """Order statuses tracked by the system."""
    PENDING = "pending"          # Order created but not yet sent to exchange
    SUBMITTED = "submitted"      # Order submitted to exchange
    OPEN = "open"                # Order confirmed open on exchange
    PARTIALLY_FILLED = "partially_filled"  # Order partially filled
    FILLED = "filled"            # Order fully filled
    CANCELLED = "cancelled"      # Order cancelled
    REJECTED = "rejected"        # Order rejected by exchange
    EXPIRED = "expired"          # Order expired
    ERROR = "error"              # Error occurred processing the order

class ExchangeType(Enum):
    """Supported exchange types."""
    SPOT = "spot"                # Spot trading exchange
    FUTURES = "futures"          # Futures trading exchange
    MARGIN = "margin"            # Margin trading exchange

class WebsocketType(Enum):
    """Websocket subscription types."""
    TICKER = "ticker"
    TRADES = "trades"
    ORDERBOOK = "orderbook"
    OHLCV = "ohlcv"
    USER_DATA = "user_data"      # Account updates, orders, trades, etc.

class APIKeyManager:
    """Manages secure storage and retrieval of API keys."""
    
    def __init__(self):
        """Initialize the API key manager."""
        self.api_keys = {}
        self.key_file = os.path.join(config.get("system.data_dir"), "api_keys.json")
        self._load_keys()
        
    def _load_keys(self):
        """Load API keys from secure storage."""
        try:
            if os.path.exists(self.key_file):
                with open(self.key_file, 'r') as f:
                    self.api_keys = json.load(f)
                logger.info("API keys loaded from storage")
            else:
                # Load from environment or config
                keys_config = config.get("exchange.api_keys", {})
                for exchange, keys in keys_config.items():
                    if 'api_key' in keys and 'secret_key' in keys:
                        self.api_keys[exchange] = {
                            'api_key': keys['api_key'],
                            'secret_key': keys['secret_key'],
                            'password': keys.get('password'),
                            'additional_params': keys.get('additional_params', {})
                        }
                self._save_keys()
                logger.info("API keys loaded from config")
        except Exception as e:
            logger.error(f"Error loading API keys: {str(e)}", exc_info=True)
    
    def _save_keys(self):
        """Save API keys to secure storage."""
        try:
            with open(self.key_file, 'w') as f:
                json.dump(self.api_keys, f, indent=2)
            # Set restrictive permissions
            os.chmod(self.key_file, 0o600)
            logger.info("API keys saved to storage")
        except Exception as e:
            logger.error(f"Error saving API keys: {str(e)}", exc_info=True)
    
    def get_keys(self, exchange: str) -> Dict[str, str]:
        """Get API keys for a specific exchange."""
        if exchange in self.api_keys:
            return self.api_keys[exchange]
        else:
            logger.warning(f"No API keys found for exchange {exchange}")
            return {}
    
    def set_keys(self, exchange: str, api_key: str, secret_key: str, password: str = None, 
                additional_params: Dict[str, Any] = None) -> bool:
        """Set API keys for a specific exchange."""
        try:
            self.api_keys[exchange] = {
                'api_key': api_key,
                'secret_key': secret_key,
                'password': password,
                'additional_params': additional_params or {}
            }
            self._save_keys()
            logger.info(f"API keys updated for exchange {exchange}")
            return True
        except Exception as e:
            logger.error(f"Error setting API keys for {exchange}: {str(e)}", exc_info=True)
            return False

# Global API key manager instance
api_key_manager = APIKeyManager()

class ExchangeAdapter(Component, ABC):
    """Base class for all exchange adapters."""
    
    def __init__(self, exchange_id: str, exchange_type: ExchangeType = ExchangeType.SPOT):
        """
        Initialize an exchange adapter.
        
        Args:
            exchange_id: Exchange identifier (e.g., "binance", "coinbase")
            exchange_type: Type of exchange (spot, futures, margin)
        """
        super().__init__(name=f"{exchange_id.capitalize()}Exchange")
        
        self.exchange_id = exchange_id
        self.exchange_type = exchange_type
        
        # Exchange API configuration
        self.api_url = None
        self.ws_url = None
        self.api_keys = {}
        
        # Connection settings
        self.timeout = config.get("exchange.connection_timeout", 10)
        self.max_retries = config.get("exchange.max_retries", 3)
        self.rate_limit_margin = config.get("exchange.rate_limit_margin", 0.8)
        
        # Rate limiting
        self.rate_limits = []
        self.rate_limit_locks = {}
        self.rate_limit_usage = {}
        self.rate_limit_reset = {}
        
        # Websocket connections
        self.ws_connections = {}
        self.ws_subscriptions = {}
        self.ws_callbacks = {}
        self.ws_keep_alive = threading.Event()
        self.ws_reconnect_thread = None
        
        # CCXT integration
        self.ccxt_exchange = None
        
        # Exchange capabilities and settings
        self.supported_symbols = []
        self.trading_fees = {}
        self.precision = {}
        self.min_order_size = {}
        self.max_order_size = {}
        
        # Exchange status
        self.is_exchange_available = False
        self.last_availability_check = 0
        self.availability_check_interval = 60  # Check every minute
    
    def initialize(self) -> bool:
        """Initialize the exchange adapter."""
        if self._initialized:
            return True
        
        try:
            # Load API keys
            self.api_keys = api_key_manager.get_keys(self.exchange_id)
            if not self.api_keys:
                logger.warning(f"No API keys found for exchange {self.exchange_id}")
            
            # Initialize CCXT exchange
            if not self._init_ccxt():
                logger.error(f"Failed to initialize CCXT exchange for {self.exchange_id}")
                self._update_status(ComponentStatus.ERROR, f"Failed to initialize CCXT exchange")
                return False
            
            # Load exchange information
            self._load_exchange_info()
            
            # Check exchange availability
            if not self._check_exchange_availability():
                logger.warning(f"Exchange {self.exchange_id} is not available")
                self._update_status(ComponentStatus.DEGRADED, f"Exchange is not available")
                self._initialized = True
                return True  # Still initialize, just in degraded state
            
            # Initialize successfully
            self._initialized = True
            self._update_status(ComponentStatus.HEALTHY)
            logger.info(f"{self.exchange_id} exchange adapter initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing {self.exchange_id} exchange adapter: {str(e)}", exc_info=True)
            self._update_status(ComponentStatus.ERROR, str(e))
            return False
    
    def _init_ccxt(self) -> bool:
        """Initialize CCXT exchange integration."""
        try:
            # Get exchange class from CCXT
            exchange_class = getattr(ccxt, self.exchange_id)
            
            # Create exchange instance
            self.ccxt_exchange = exchange_class({
                'apiKey': self.api_keys.get('api_key'),
                'secret': self.api_keys.get('secret_key'),
                'password': self.api_keys.get('password'),
                'enableRateLimit': True,
                'rateLimit': int(ccxt.Exchange.rateLimit * (1 / self.rate_limit_margin)),
                'timeout': self.timeout * 1000,  # CCXT uses milliseconds
                'options': self.api_keys.get('additional_params', {})
            })
            
            # Set sandbox mode if in test mode
            if config.get("system.test_mode", False):
                if 'test' in self.ccxt_exchange.urls:
                    self.ccxt_exchange.urls['api'] = self.ccxt_exchange.urls['test']
                    logger.info(f"Using test environment for {self.exchange_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error initializing CCXT exchange for {self.exchange_id}: {str(e)}", exc_info=True)
            return False
    
    def _load_exchange_info(self) -> None:
        """Load exchange information and capabilities."""
        try:
            # Load markets
            markets = self.ccxt_exchange.load_markets()
            self.supported_symbols = list(markets.keys())
            
            # Load trading fees
            if hasattr(self.ccxt_exchange, 'fees') and 'trading' in self.ccxt_exchange.fees:
                self.trading_fees = self.ccxt_exchange.fees['trading']
            
            # Load precision and limits
            for symbol, market in markets.items():
                self.precision[symbol] = {
                    'price': market.get('precision', {}).get('price', 8),
                    'amount': market.get('precision', {}).get('amount', 8)
                }
                self.min_order_size[symbol] = market.get('limits', {}).get('amount', {}).get('min', 0)
                self.max_order_size[symbol] = market.get('limits', {}).get('amount', {}).get('max', float('inf'))
            
            # Load rate limits
            if hasattr(self.ccxt_exchange, 'rateLimit'):
                self.rate_limits.append({
                    'type': 'overall',
                    'limit': 1,
                    'interval': self.ccxt_exchange.rateLimit / 1000  # Convert to seconds
                })
            
            # Initialize rate limit locks
            for rate_limit in self.rate_limits:
                limit_type = rate_limit['type']
                if limit_type not in self.rate_limit_locks:
                    self.rate_limit_locks[limit_type] = threading.RLock()
                    self.rate_limit_usage[limit_type] = 0
                    self.rate_limit_reset[limit_type] = time.time()
            
            logger.info(f"Loaded {len(self.supported_symbols)} markets for {self.exchange_id}")
            
        except Exception as e:
            logger.error(f"Error loading exchange info for {self.exchange_id}: {str(e)}", exc_info=True)
    
    def _check_exchange_availability(self) -> bool:
        """Check if the exchange API is available."""
        current_time = time.time()
        
        # Only check if the interval has passed
        if current_time - self.last_availability_check < self.availability_check_interval:
            return self.is_exchange_available
        
        try:
            # Simple API call to check availability
            self.ccxt_exchange.fetch_time()
            self.is_exchange_available = True
            self.last_availability_check = current_time
            return True
            
        except Exception as e:
            logger.error(f"Exchange {self.exchange_id} is not available: {str(e)}")
            self.is_exchange_available = False
            self.last_availability_check = current_time
            return False
    
    def _check_health(self) -> Tuple[ComponentStatus, Dict[str, Any]]:
        """Check exchange adapter health."""
        metrics = {
            "is_exchange_available": self.is_exchange_available,
            "last_availability_check": self.last_availability_check,
            "supported_symbols": len(self.supported_symbols),
            "active_ws_connections": len(self.ws_connections)
        }
        
        # Check if exchange is available
        if not self._check_exchange_availability():
            return ComponentStatus.DEGRADED, metrics
        
        # Check websocket connections
        ws_errors = 0
        for ws_type, ws_conn in self.ws_connections.items():
            if not ws_conn or not getattr(ws_conn, 'sock', None):
                ws_errors += 1
        
        if ws_errors > 0:
            metrics["ws_errors"] = ws_errors
            return ComponentStatus.DEGRADED, metrics
        
        return ComponentStatus.HEALTHY, metrics
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((requests.exceptions.RequestException, ccxt.NetworkError))
    )
    def _api_request(self, method: str, endpoint: str, params: Dict[str, Any] = None, 
                   data: Dict[str, Any] = None, headers: Dict[str, str] = None, 
                   auth_required: bool = False) -> Dict[str, Any]:
        """
        Make an API request to the exchange.
        
        Args:
            method: HTTP method (GET, POST, DELETE, etc.)
            endpoint: API endpoint
            params: Query parameters
            data: Request body data
            headers: Request headers
            auth_required: Whether authentication is required
            
        Returns:
            API response as dictionary
        """
        # Just use CCXT for now
        try:
            if method == 'GET':
                if endpoint == 'ticker':
                    return self.ccxt_exchange.fetch_ticker(params.get('symbol'))
                elif endpoint == 'tickers':
                    return self.ccxt_exchange.fetch_tickers(params.get('symbols'))
                elif endpoint == 'orderbook':
                    return self.ccxt_exchange.fetch_order_book(params.get('symbol'), params.get('limit'))
                elif endpoint == 'trades':
                    return self.ccxt_exchange.fetch_trades(params.get('symbol'), params.get('limit'))
                elif endpoint == 'ohlcv':
                    return self.ccxt_exchange.fetch_ohlcv(
                        params.get('symbol'), 
                        params.get('timeframe', '1m'),
                        params.get('since'),
                        params.get('limit')
                    )
                elif endpoint == 'balance':
                    return self.ccxt_exchange.fetch_balance()
                elif endpoint == 'order':
                    return self.ccxt_exchange.fetch_order(params.get('id'), params.get('symbol'))
                elif endpoint == 'orders':
                    return self.ccxt_exchange.fetch_orders(params.get('symbol'), params.get('since'), params.get('limit'))
                elif endpoint == 'open_orders':
                    return self.ccxt_exchange.fetch_open_orders(params.get('symbol'), params.get('since'), params.get('limit'))
                elif endpoint == 'closed_orders':
                    return self.ccxt_exchange.fetch_closed_orders(params.get('symbol'), params.get('since'), params.get('limit'))
                elif endpoint == 'my_trades':
                    return self.ccxt_exchange.fetch_my_trades(params.get('symbol'), params.get('since'), params.get('limit'))
            elif method == 'POST':
                if endpoint == 'order':
                    return self.ccxt_exchange.create_order(
                        params.get('symbol'),
                        params.get('type'),
                        params.get('side'),
                        params.get('amount'),
                        params.get('price')
                    )
            elif method == 'DELETE':
                if endpoint == 'order':
                    return self.ccxt_exchange.cancel_order(params.get('id'), params.get('symbol'))
            
            raise NotImplementedError(f"API request {method} {endpoint} not implemented")
            
        except ccxt.BaseError as e:
            logger.error(f"CCXT error in {self.exchange_id} {method} {endpoint}: {str(e)}", exc_info=True)
            raise
    
    def start_websocket(self, ws_type: WebsocketType, symbols: List[str], callback: Callable) -> bool:
        """
        Start a websocket connection for specified symbols.
        
        Args:
            ws_type: Type of websocket data to subscribe to
            symbols: List of symbols to subscribe to
            callback: Function to call when data is received
            
        Returns:
            Success status
        """
        # Abstract method - implemented by specific exchange adapters
        return False
    
    def stop_websocket(self, ws_type: WebsocketType) -> bool:
        """
        Stop a websocket connection.
        
        Args:
            ws_type: Type of websocket to stop
            
        Returns:
            Success status
        """
        # Abstract method - implemented by specific exchange adapters
        return False
    
    def stop(self) -> bool:
        """Stop the exchange adapter."""
        if not self._running:
            return True
        
        try:
            # Stop websocket connections
            for ws_type in list(self.ws_connections.keys()):
                self.stop_websocket(ws_type)
            
            # Signal websocket keep-alive thread to stop
            self.ws_keep_alive.set()
            
            # Wait for websocket reconnect thread to finish
            if self.ws_reconnect_thread and self.ws_reconnect_thread.is_alive():
                self.ws_reconnect_thread.join(timeout=5.0)
            
            return super().stop()
            
        except Exception as e:
            logger.error(f"Error stopping {self.exchange_id} exchange adapter: {str(e)}", exc_info=True)
            return False
    
    def get_ticker(self, symbol: str) -> Dict[str, Any]:
        """
        Get ticker information for a symbol.
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            Ticker information
        """
        try:
            return self._api_request('GET', 'ticker', {'symbol': symbol})
        except Exception as e:
            logger.error(f"Error getting ticker for {symbol}: {str(e)}", exc_info=True)
            return {}
    
    def get_orderbook(self, symbol: str, limit: int = 20) -> Dict[str, Any]:
        """
        Get order book for a symbol.
        
        Args:
            symbol: Trading pair symbol
            limit: Number of entries to return
            
        Returns:
            Order book data
        """
        try:
            return self._api_request('GET', 'orderbook', {'symbol': symbol, 'limit': limit})
        except Exception as e:
            logger.error(f"Error getting orderbook for {symbol}: {str(e)}", exc_info=True)
            return {}
    
    def get_historical_ohlcv(self, symbol: str, timeframe: str = '1m', 
                           since: Optional[int] = None, limit: int = 100) -> List[List[float]]:
        """
        Get historical OHLCV data.
        
        Args:
            symbol: Trading pair symbol
            timeframe: Timeframe interval
            since: Start time in milliseconds
            limit: Number of candles to return
            
        Returns:
            List of OHLCV candles
        """
        try:
            return self._api_request('GET', 'ohlcv', {
                'symbol': symbol,
                'timeframe': timeframe,
                'since': since,
                'limit': limit
            })
        except Exception as e:
            logger.error(f"Error getting OHLCV for {symbol}: {str(e)}", exc_info=True)
            return []
    
    def get_account_balance(self) -> Dict[str, Any]:
        """
        Get account balance.
        
        Returns:
            Account balance information
        """
        try:
            return self._api_request('GET', 'balance', auth_required=True)
        except Exception as e:
            logger.error(f"Error getting account balance: {str(e)}", exc_info=True)
            return {}
    
    def create_order(self, symbol: str, order_type: OrderType, side: OrderSide, 
                    amount: float, price: float = None, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Create a new order.
        
        Args:
            symbol: Trading pair symbol
            order_type: Type of order
            side: Buy or sell
            amount: Order amount
            price: Order price (required for limit orders)
            params: Additional parameters specific to the exchange
            
        Returns:
            Order information
        """
        try:
            request_params = {
                'symbol': symbol,
                'type': order_type.value,
                'side': side.value,
                'amount': amount,
                'price': price,
            }
            
            # Add additional params if provided
            if params:
                request_params.update(params)
            
            return self._api_request('POST', 'order', request_params, auth_required=True)
        except Exception as e:
            logger.error(f"Error creating order for {symbol}: {str(e)}", exc_info=True)
            return {}
    
    def cancel_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """
        Cancel an existing order.
        
        Args:
            order_id: Order ID to cancel
            symbol: Trading pair symbol
            
        Returns:
            Cancellation result
        """
        try:
            return self._api_request('DELETE', 'order', {
                'id': order_id,
                'symbol': symbol
            }, auth_required=True)
        except Exception as e:
            logger.error(f"Error cancelling order {order_id}: {str(e)}", exc_info=True)
            return {}
    
    def get_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """
        Get information about an order.
        
        Args:
            order_id: Order ID
            symbol: Trading pair symbol
            
        Returns:
            Order information
        """
        try:
            return self._api_request('GET', 'order', {
                'id': order_id,
                'symbol': symbol
            }, auth_required=True)
        except Exception as e:
            logger.error(f"Error getting order {order_id}: {str(e)}", exc_info=True)
            return {}
    
    def get_open_orders(self, symbol: str = None) -> List[Dict[str, Any]]:
        """
        Get open orders.
        
        Args:
            symbol: Trading pair symbol (optional)
            
        Returns:
            List of open orders
        """
        try:
            return self._api_request('GET', 'open_orders', {
                'symbol': symbol
            }, auth_required=True)
        except Exception as e:
            logger.error(f"Error getting open orders: {str(e)}", exc_info=True)
            return []
    
    def get_order_history(self, symbol: str = None, since: int = None, 
                        limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get order history.
        
        Args:
            symbol: Trading pair symbol (optional)
            since: Start time in milliseconds
            limit: Number of orders to return
            
        Returns:
            List of historical orders
        """
        try:
            return self._api_request('GET', 'closed_orders', {
                'symbol': symbol,
                'since': since,
                'limit': limit
            }, auth_required=True)
        except Exception as e:
            logger.error(f"Error getting order history: {str(e)}", exc_info=True)
            return []
    
    def get_trade_history(self, symbol: str, since: int = None, 
                        limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get trade history.
        
        Args:
            symbol: Trading pair symbol
            since: Start time in milliseconds
            limit: Number of trades to return
            
        Returns:
            List of trades
        """
        try:
            return self._api_request('GET', 'my_trades', {
                'symbol': symbol,
                'since': since,
                'limit': limit
            }, auth_required=True)
        except Exception as e:
            logger.error(f"Error getting trade history for {symbol}: {str(e)}", exc_info=True)
            return []
    
    def convert_amount_to_precision(self, symbol: str, amount: float) -> float:
        """
        Convert amount to exchange precision.
        
        Args:
            symbol: Trading pair symbol
            amount: Amount to convert
            
        Returns:
            Converted amount
        """
        try:
            precision = self.precision.get(symbol, {}).get('amount', 8)
            return float(f"%.{precision}f" % amount)
        except Exception as e:
            logger.error(f"Error converting amount to precision: {str(e)}", exc_info=True)
            return amount
    
    def convert_price_to_precision(self, symbol: str, price: float) -> float:
        """
        Convert price to exchange precision.
        
        Args:
            symbol: Trading pair symbol
            price: Price to convert
            
        Returns:
            Converted price
        """
        try:
            precision = self.precision.get(symbol, {}).get('price', 8)
            return float(f"%.{precision}f" % price)
        except Exception as e:
            logger.error(f"Error converting price to precision: {str(e)}", exc_info=True)
            return price


class ExchangeManager(Component):
    """Manager for multiple exchange adapters."""
    
    def __init__(self):
        """Initialize the exchange manager."""
        super().__init__(name="ExchangeManager")
        
        # Map of exchange ID to adapter
        self.exchanges = {}
        
        # Default exchange
        self.default_exchange_id = config.get("exchange.default_exchange", "binance")
    
    def initialize(self) -> bool:
        """Initialize the exchange manager."""
        if self._initialized:
            return True
        
        try:
            # Initialize default exchange
            if not self.add_exchange(self.default_exchange_id):
                logger.error(f"Failed to initialize default exchange {self.default_exchange_id}")
                self._update_status(ComponentStatus.DEGRADED, f"Default exchange initialization failed")
            
            # Initialize successfully
            self._initialized = True
            self._update_status(ComponentStatus.HEALTHY)
            logger.info("Exchange manager initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing exchange manager: {str(e)}", exc_info=True)
            self._update_status(ComponentStatus.ERROR, str(e))
            return False
    
    def add_exchange(self, exchange_id: str, exchange_type: ExchangeType = ExchangeType.SPOT) -> bool:
        """
        Add an exchange adapter.
        
        Args:
            exchange_id: Exchange identifier
            exchange_type: Type of exchange
            
        Returns:
            Success status
        """
        try:
            # Check if exchange is already added
            if exchange_id in self.exchanges:
                logger.warning(f"Exchange {exchange_id} already added")
                return True
            
            # Create exchange adapter
            exchange = self._create_exchange_adapter(exchange_id, exchange_type)
            if not exchange:
                logger.error(f"Failed to create exchange adapter for {exchange_id}")
                return False
            
            # Initialize exchange
            if not exchange.initialize():
                logger.error(f"Failed to initialize exchange {exchange_id}")
                return False
            
            # Add to exchanges
            self.exchanges[exchange_id] = exchange
            logger.info(f"Added exchange {exchange_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error adding exchange {exchange_id}: {str(e)}", exc_info=True)
            return False
    
    def _create_exchange_adapter(self, exchange_id: str, exchange_type: ExchangeType) -> Optional[ExchangeAdapter]:
        """
        Create an exchange adapter for the specified exchange.
        
        Args:
            exchange_id: Exchange identifier
            exchange_type: Type of exchange
            
        Returns:
            Exchange adapter or None if not supported
        """
        # For now, just use a generic adapter
        return ExchangeAdapter(exchange_id, exchange_type)
    
    def get_exchange(self, exchange_id: str = None) -> Optional[ExchangeAdapter]:
        """
        Get an exchange adapter.
        
        Args:
            exchange_id: Exchange identifier (default if None)
            
        Returns:
            Exchange adapter or None if not found
        """
        if exchange_id is None:
            exchange_id = self.default_exchange_id
            
        return self.exchanges.get(exchange_id)
    
    def start(self) -> bool:
        """Start the exchange manager."""
        if not self._initialized:
            success = self.initialize()
            if not success:
                return False
        
        if self._running:
            return True
        
        try:
            # Start all exchanges
            for exchange_id, exchange in self.exchanges.items():
                if not exchange.start():
                    logger.error(f"Failed to start exchange {exchange_id}")
                    self._update_status(ComponentStatus.DEGRADED, f"Exchange {exchange_id} failed to start")
            
            return super().start()
            
        except Exception as e:
            logger.error(f"Error starting exchange manager: {str(e)}", exc_info=True)
            self._update_status(ComponentStatus.ERROR, str(e))
            return False
    
    def stop(self) -> bool:
        """Stop the exchange manager."""
        if not self._running:
            return True
        
        try:
            # Stop all exchanges
            for exchange_id, exchange in self.exchanges.items():
                if not exchange.stop():
                    logger.error(f"Failed to stop exchange {exchange_id}")
            
            return super().stop()
            
        except Exception as e:
            logger.error(f"Error stopping exchange manager: {str(e)}", exc_info=True)
            return False
    
    def _check_health(self) -> Tuple[ComponentStatus, Dict[str, Any]]:
        """Check exchange manager health."""
        metrics = {
            "exchanges": len(self.exchanges),
            "healthy_exchanges": 0,
            "degraded_exchanges": 0,
            "error_exchanges": 0
        }
        
        # Check all exchanges
        for exchange_id, exchange in self.exchanges.items():
            exchange_status = exchange.get_status()
            if exchange_status == ComponentStatus.HEALTHY:
                metrics["healthy_exchanges"] += 1
            elif exchange_status == ComponentStatus.DEGRADED:
                metrics["degraded_exchanges"] += 1
            else:
                metrics["error_exchanges"] += 1
        
        # Determine overall status
        if metrics["healthy_exchanges"] == 0:
            return ComponentStatus.ERROR, metrics
        elif metrics["error_exchanges"] > 0:
            return ComponentStatus.DEGRADED, metrics
        else:
            return ComponentStatus.HEALTHY, metrics


# Global exchange manager instance
exchange_manager = ExchangeManager() 