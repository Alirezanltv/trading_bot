"""
Nobitex exchange adapter for high-reliability trading system.
"""

import time
import hmac
import hashlib
import json
import base64
from typing import Dict, Any, List, Optional, Union, Tuple
import urllib.parse

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from trading_system.core import ExchangeAdapter, ExchangeType, OrderType, OrderSide, OrderStatus
from trading_system.core import WebsocketType, get_logger

logger = get_logger("exchanges.nobitex")


class NobitexExchangeAdapter(ExchangeAdapter):
    """
    Nobitex Exchange Adapter for the Iranian cryptocurrency exchange.
    """
    
    def __init__(self):
        """Initialize the Nobitex exchange adapter."""
        super().__init__(exchange_id="nobitex", exchange_type=ExchangeType.SPOT)
        
        # Nobitex API endpoints
        self.api_url = "https://api.nobitex.ir"
        self.ws_url = "wss://api.nobitex.ir/ws/v2"
        
        # Symbol mapping for Nobitex (e.g., btc-rls -> BTCRLS)
        self.symbol_mapping = {}
        self.reverse_symbol_mapping = {}
        
        # Market specific info
        self.markets_info = {}
        self.trading_fees = {
            'maker': 0.0025,  # 0.25%
            'taker': 0.0025,  # 0.25%
        }
        
        # Token for authenticated requests
        self.auth_token = None
        self.token_expires_at = 0
        self.token_refresh_margin = 300  # 5 minutes before expiration
    
    def initialize(self) -> bool:
        """Initialize the Nobitex exchange adapter."""
        if self._initialized:
            return True
        
        try:
            # Load the API key
            self.api_keys = self._get_api_keys()
            if not self.api_keys:
                logger.error("No API keys found for Nobitex")
                self._update_status("ERROR", "No API keys found")
                return False
            
            # Test the API connection and authenticate
            if not self._authenticate():
                logger.error("Failed to authenticate with Nobitex")
                self._update_status("ERROR", "Authentication failed")
                return False
            
            # Get market information
            self._load_markets()
            
            # Check if the exchange is available
            if not self._check_exchange_availability():
                logger.warning("Nobitex exchange is not available")
                self._update_status("DEGRADED", "Exchange is not available")
                return False
            
            self._initialized = True
            self._update_status("HEALTHY")
            logger.info("Nobitex exchange adapter initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing Nobitex exchange adapter: {str(e)}", exc_info=True)
            self._update_status("ERROR", str(e))
            return False
    
    def _get_api_keys(self) -> Dict[str, str]:
        """Get the API keys for Nobitex."""
        from trading_system.core import api_key_manager
        keys = api_key_manager.get_keys("nobitex")
        if not keys:
            logger.warning("No API keys found for Nobitex in API key manager")
        return keys
    
    def _authenticate(self) -> bool:
        """
        Authenticate with the Nobitex API and get a token.
        
        Returns:
            bool: Whether authentication was successful
        """
        try:
            # Check if we have a valid token
            current_time = time.time()
            if self.auth_token and current_time < (self.token_expires_at - self.token_refresh_margin):
                return True
            
            # Authenticate with username and password
            response = self._make_request(
                method="POST",
                endpoint="/auth/login/",
                data={
                    "username": self.api_keys.get("username"),
                    "password": self.api_keys.get("password")
                }
            )
            
            if not response or 'key' not in response:
                logger.error(f"Authentication failed: {response}")
                return False
            
            # Store the token
            self.auth_token = response['key']
            
            # Set expiry time (default to 24 hours if not provided)
            expires_in = response.get('expires_in', 86400)
            self.token_expires_at = time.time() + expires_in
            
            logger.info("Successfully authenticated with Nobitex")
            return True
            
        except Exception as e:
            logger.error(f"Error authenticating with Nobitex: {str(e)}", exc_info=True)
            return False
    
    def _load_markets(self) -> None:
        """Load market information from Nobitex."""
        try:
            # Get market information
            response = self._make_request(
                method="GET",
                endpoint="/v2/orderbook/all",
                params={}
            )
            
            if not response or 'status' not in response or response['status'] != 'ok':
                logger.error(f"Failed to get market information: {response}")
                return
            
            # Extract market symbols
            for symbol in response.get('data', {}):
                # Map from Nobitex format to standardized format
                # For example, BTCRLS -> btc-rls
                base_asset = symbol[:-3].lower()
                quote_asset = symbol[-3:].lower()
                
                standardized_symbol = f"{base_asset}-{quote_asset}"
                
                self.symbol_mapping[standardized_symbol] = symbol
                self.reverse_symbol_mapping[symbol] = standardized_symbol
                
                # Store market info
                self.markets_info[standardized_symbol] = {
                    'symbol': symbol,
                    'base_asset': base_asset,
                    'quote_asset': quote_asset,
                    'min_order_size': 0.0001,  # Default, will update if available
                    'precision': {
                        'price': 8,
                        'amount': 8
                    }
                }
                
                # Add to supported symbols
                if standardized_symbol not in self.supported_symbols:
                    self.supported_symbols.append(standardized_symbol)
            
            # Get additional market details (fees, limits, etc.)
            self._load_market_details()
            
            logger.info(f"Loaded {len(self.supported_symbols)} markets from Nobitex")
            
        except Exception as e:
            logger.error(f"Error loading markets from Nobitex: {str(e)}", exc_info=True)
    
    def _load_market_details(self) -> None:
        """Load additional market details."""
        try:
            # Get market status which includes last trade prices
            response = self._make_request(
                method="GET",
                endpoint="/market/stats",
                params={}
            )
            
            if not response or 'status' not in response or response['status'] != 'ok':
                logger.error(f"Failed to get market details: {response}")
                return
            
            # Update market details with last prices
            for symbol, market_data in response.get('stats', {}).items():
                standardized_symbol = self.reverse_symbol_mapping.get(symbol)
                if not standardized_symbol or standardized_symbol not in self.markets_info:
                    continue
                
                # Update with last price, 24h volume, etc.
                self.markets_info[standardized_symbol].update({
                    'last_price': float(market_data.get('latest', 0)),
                    '24h_high': float(market_data.get('dayHigh', 0)),
                    '24h_low': float(market_data.get('dayLow', 0)),
                    '24h_volume': float(market_data.get('volumeSrc', 0)),
                    '24h_change': float(market_data.get('dayChange', 0))
                })
            
        except Exception as e:
            logger.error(f"Error loading market details from Nobitex: {str(e)}", exc_info=True)
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((requests.exceptions.RequestException, requests.exceptions.Timeout))
    )
    def _make_request(self, method: str, endpoint: str, params: Dict[str, Any] = None, 
                     data: Dict[str, Any] = None, headers: Dict[str, str] = None, 
                     auth_required: bool = True) -> Dict[str, Any]:
        """
        Make a request to the Nobitex API.
        
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
        try:
            url = f"{self.api_url}{endpoint}"
            
            # Prepare headers
            if headers is None:
                headers = {}
            
            headers['Content-Type'] = 'application/json'
            
            # Add authentication token if required
            if auth_required:
                if not self._authenticate():
                    logger.error("Failed to authenticate for API request")
                    return {}
                
                headers['Authorization'] = f"Token {self.auth_token}"
            
            # Make the request
            logger.debug(f"Making Nobitex API request: {method} {endpoint}")
            
            if method == 'GET':
                response = requests.get(url, params=params, headers=headers, timeout=self.timeout)
            elif method == 'POST':
                response = requests.post(url, json=data, headers=headers, timeout=self.timeout)
            elif method == 'DELETE':
                response = requests.delete(url, json=data, headers=headers, timeout=self.timeout)
            else:
                logger.error(f"Unsupported HTTP method: {method}")
                return {}
            
            # Check for HTTP errors
            response.raise_for_status()
            
            # Parse JSON response
            result = response.json()
            
            # Check for API errors
            if result.get('status') == 'failed':
                error_msg = result.get('message', 'Unknown API error')
                logger.error(f"Nobitex API error: {error_msg}")
                # Raise exception to trigger retry if needed
                raise Exception(f"Nobitex API error: {error_msg}")
            
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error in Nobitex API: {str(e)}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Error in Nobitex API request: {str(e)}", exc_info=True)
            return {}
    
    def _map_symbol(self, symbol: str) -> str:
        """
        Map standardized symbol to Nobitex symbol.
        
        Args:
            symbol: Standardized symbol (e.g., 'btc-rls')
            
        Returns:
            Nobitex symbol (e.g., 'BTCRLS')
        """
        if symbol in self.symbol_mapping:
            return self.symbol_mapping[symbol]
        
        # Try to create if not found
        parts = symbol.split('-')
        if len(parts) == 2:
            nobitex_symbol = f"{parts[0].upper()}{parts[1].upper()}"
            self.symbol_mapping[symbol] = nobitex_symbol
            self.reverse_symbol_mapping[nobitex_symbol] = symbol
            return nobitex_symbol
        
        logger.warning(f"Could not map symbol {symbol} to Nobitex format")
        return symbol.upper().replace('-', '')
    
    def get_ticker(self, symbol: str) -> Dict[str, Any]:
        """
        Get ticker information for a symbol.
        
        Args:
            symbol: Trading pair symbol (e.g., 'btc-rls')
            
        Returns:
            Ticker information
        """
        try:
            nobitex_symbol = self._map_symbol(symbol)
            
            response = self._make_request(
                method="GET",
                endpoint="/market/stats",
                params={"srcCurrency": nobitex_symbol[:3], "dstCurrency": nobitex_symbol[3:]}
            )
            
            if not response or 'status' not in response or response['status'] != 'ok':
                logger.error(f"Failed to get ticker for {symbol}: {response}")
                return {}
            
            # Extract ticker data
            stats = response.get('stats', {}).get(nobitex_symbol, {})
            
            return {
                'symbol': symbol,
                'last': float(stats.get('latest', 0)),
                'bid': None,  # Not provided directly by Nobitex
                'ask': None,  # Not provided directly by Nobitex
                'high': float(stats.get('dayHigh', 0)),
                'low': float(stats.get('dayLow', 0)),
                'volume': float(stats.get('volumeSrc', 0)),
                'change': float(stats.get('dayChange', 0)),
                'timestamp': int(time.time() * 1000)
            }
            
        except Exception as e:
            logger.error(f"Error getting ticker for {symbol}: {str(e)}", exc_info=True)
            return {}
    
    def get_orderbook(self, symbol: str, limit: int = 20) -> Dict[str, Any]:
        """
        Get order book for a symbol.
        
        Args:
            symbol: Trading pair symbol (e.g., 'btc-rls')
            limit: Number of entries to return
            
        Returns:
            Order book data
        """
        try:
            nobitex_symbol = self._map_symbol(symbol)
            
            response = self._make_request(
                method="GET",
                endpoint=f"/v2/orderbook",
                params={"symbol": nobitex_symbol}
            )
            
            if not response or 'status' not in response or response['status'] != 'ok':
                logger.error(f"Failed to get orderbook for {symbol}: {response}")
                return {}
            
            # Extract order book data
            order_book = response.get('data', {})
            
            # Format order book data
            result = {
                'symbol': symbol,
                'timestamp': int(time.time() * 1000),
                'bids': [],
                'asks': []
            }
            
            # Process bids
            bids = order_book.get('bids', [])
            for i, bid in enumerate(bids):
                if i >= limit:
                    break
                result['bids'].append([float(bid[0]), float(bid[1])])
            
            # Process asks
            asks = order_book.get('asks', [])
            for i, ask in enumerate(asks):
                if i >= limit:
                    break
                result['asks'].append([float(ask[0]), float(ask[1])])
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting orderbook for {symbol}: {str(e)}", exc_info=True)
            return {}
    
    def get_historical_ohlcv(self, symbol: str, timeframe: str = '1d', 
                           since: Optional[int] = None, limit: int = 100) -> List[List[float]]:
        """
        Get historical OHLCV data.
        
        Args:
            symbol: Trading pair symbol (e.g., 'btc-rls')
            timeframe: Timeframe interval (1h, 4h, 1d, etc.)
            since: Start time in milliseconds
            limit: Number of candles to return
            
        Returns:
            List of OHLCV candles [timestamp, open, high, low, close, volume]
        """
        try:
            nobitex_symbol = self._map_symbol(symbol)
            
            # Map timeframe to resolution parameter
            resolution_map = {
                '1m': '1', 
                '5m': '5', 
                '15m': '15', 
                '30m': '30', 
                '1h': '60', 
                '4h': '240', 
                '1d': 'D', 
                '1w': 'W'
            }
            
            resolution = resolution_map.get(timeframe, 'D')
            
            # Calculate from and to timestamps
            to_ts = int(time.time())
            from_ts = since // 1000 if since else to_ts - (limit * self._get_timeframe_seconds(timeframe))
            
            response = self._make_request(
                method="GET",
                endpoint="/market/udf/history",
                params={
                    "symbol": nobitex_symbol,
                    "resolution": resolution,
                    "from": from_ts,
                    "to": to_ts
                }
            )
            
            if not response or 's' not in response or response['s'] != 'ok':
                logger.error(f"Failed to get OHLCV data for {symbol}: {response}")
                return []
            
            # Extract OHLCV data
            result = []
            timestamps = response.get('t', [])
            opens = response.get('o', [])
            highs = response.get('h', [])
            lows = response.get('l', [])
            closes = response.get('c', [])
            volumes = response.get('v', [])
            
            for i in range(min(len(timestamps), limit)):
                candle = [
                    timestamps[i] * 1000,  # Convert to milliseconds
                    float(opens[i]),
                    float(highs[i]),
                    float(lows[i]),
                    float(closes[i]),
                    float(volumes[i])
                ]
                result.append(candle)
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting OHLCV data for {symbol}: {str(e)}", exc_info=True)
            return []
    
    def _get_timeframe_seconds(self, timeframe: str) -> int:
        """
        Get the number of seconds in a timeframe.
        
        Args:
            timeframe: Timeframe string (1m, 5m, 15m, 30m, 1h, 4h, 1d, 1w)
            
        Returns:
            Number of seconds
        """
        timeframe_seconds = {
            '1m': 60,
            '5m': 300,
            '15m': 900,
            '30m': 1800,
            '1h': 3600,
            '4h': 14400,
            '1d': 86400,
            '1w': 604800
        }
        return timeframe_seconds.get(timeframe, 86400)
    
    def get_account_balance(self) -> Dict[str, Any]:
        """
        Get account balance.
        
        Returns:
            Account balance information
        """
        try:
            response = self._make_request(
                method="POST",
                endpoint="/users/wallets/list",
                data={},
                auth_required=True
            )
            
            if not response or 'status' not in response or response['status'] != 'ok':
                logger.error(f"Failed to get account balance: {response}")
                return {}
            
            # Extract balance data
            balances = {}
            
            for wallet in response.get('wallets', []):
                currency = wallet.get('currency', '').lower()
                balance = float(wallet.get('balance', 0))
                
                balances[currency] = {
                    'free': balance,
                    'used': 0,  # Not provided directly by Nobitex
                    'total': balance
                }
            
            return {
                'info': response,
                'balances': balances
            }
            
        except Exception as e:
            logger.error(f"Error getting account balance: {str(e)}", exc_info=True)
            return {}
    
    def create_order(self, symbol: str, order_type: OrderType, side: OrderSide, 
                    amount: float, price: float = None, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Create a new order.
        
        Args:
            symbol: Trading pair symbol (e.g., 'btc-rls')
            order_type: Type of order
            side: Buy or sell
            amount: Order amount
            price: Order price (required for limit orders)
            params: Additional parameters specific to the exchange
            
        Returns:
            Order information
        """
        try:
            nobitex_symbol = self._map_symbol(symbol)
            
            # Map order type
            type_map = {
                OrderType.MARKET: 'market',
                OrderType.LIMIT: 'limit'
            }
            
            # Map order side
            side_map = {
                OrderSide.BUY: 'buy',
                OrderSide.SELL: 'sell'
            }
            
            nobitex_type = type_map.get(order_type, 'limit')
            nobitex_side = side_map.get(side, 'buy')
            
            # Prepare order data
            order_data = {
                'type': nobitex_type,
                'execution': 'execution',
                'srcCurrency': self._get_base_currency(symbol),
                'dstCurrency': self._get_quote_currency(symbol),
                'amount': str(amount),
                'price': str(price) if price else None
            }
            
            # Add additional parameters
            if params:
                order_data.update(params)
            
            # Select appropriate endpoint based on side
            endpoint = "/market/orders/add"
            
            response = self._make_request(
                method="POST",
                endpoint=endpoint,
                data=order_data,
                auth_required=True
            )
            
            if not response or 'status' not in response or response['status'] != 'ok':
                logger.error(f"Failed to create order for {symbol}: {response}")
                return {}
            
            # Extract order information
            order_info = {
                'id': response.get('order', {}).get('id'),
                'symbol': symbol,
                'type': order_type.value,
                'side': side.value,
                'amount': amount,
                'price': price,
                'status': 'open',
                'timestamp': int(time.time() * 1000)
            }
            
            return order_info
            
        except Exception as e:
            logger.error(f"Error creating order for {symbol}: {str(e)}", exc_info=True)
            return {}
    
    def _get_base_currency(self, symbol: str) -> str:
        """Get base currency from symbol."""
        parts = symbol.split('-')
        return parts[0].upper() if len(parts) > 0 else ""
    
    def _get_quote_currency(self, symbol: str) -> str:
        """Get quote currency from symbol."""
        parts = symbol.split('-')
        return parts[1].upper() if len(parts) > 1 else ""
    
    def cancel_order(self, order_id: str, symbol: str = None) -> Dict[str, Any]:
        """
        Cancel an existing order.
        
        Args:
            order_id: Order ID to cancel
            symbol: Trading pair symbol (optional)
            
        Returns:
            Cancellation result
        """
        try:
            response = self._make_request(
                method="POST",
                endpoint="/market/orders/cancel",
                data={"order": order_id},
                auth_required=True
            )
            
            if not response or 'status' not in response or response['status'] != 'ok':
                logger.error(f"Failed to cancel order {order_id}: {response}")
                return {'success': False}
            
            return {
                'id': order_id,
                'symbol': symbol,
                'success': True,
                'timestamp': int(time.time() * 1000)
            }
            
        except Exception as e:
            logger.error(f"Error cancelling order {order_id}: {str(e)}", exc_info=True)
            return {'success': False}
    
    def get_order(self, order_id: str, symbol: str = None) -> Dict[str, Any]:
        """
        Get information about an order.
        
        Args:
            order_id: Order ID
            symbol: Trading pair symbol (optional)
            
        Returns:
            Order information
        """
        try:
            response = self._make_request(
                method="POST",
                endpoint="/market/orders/status",
                data={"id": order_id},
                auth_required=True
            )
            
            if not response or 'status' not in response or response['status'] != 'ok':
                logger.error(f"Failed to get order {order_id}: {response}")
                return {}
            
            # Extract order information
            order_data = response.get('order', {})
            
            # Map status
            status_map = {
                'new': OrderStatus.PENDING.value,
                'active': OrderStatus.OPEN.value,
                'done': OrderStatus.FILLED.value,
                'rejected': OrderStatus.REJECTED.value,
                'canceled': OrderStatus.CANCELLED.value
            }
            
            return {
                'id': order_id,
                'symbol': symbol,
                'type': order_data.get('type'),
                'side': 'buy' if order_data.get('type') == 'buy' else 'sell',
                'amount': float(order_data.get('amount', 0)),
                'price': float(order_data.get('price', 0)),
                'status': status_map.get(order_data.get('status'), 'unknown'),
                'filled': float(order_data.get('executed', 0)),
                'remaining': float(order_data.get('amount', 0)) - float(order_data.get('executed', 0)),
                'timestamp': order_data.get('created_at', int(time.time() * 1000))
            }
            
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
            # Prepare request parameters
            data = {}
            if symbol:
                data['srcCurrency'] = self._get_base_currency(symbol)
                data['dstCurrency'] = self._get_quote_currency(symbol)
            
            response = self._make_request(
                method="POST",
                endpoint="/market/orders/list",
                data=data,
                auth_required=True
            )
            
            if not response or 'status' not in response or response['status'] != 'ok':
                logger.error(f"Failed to get open orders: {response}")
                return []
            
            # Extract order information
            orders = []
            
            for order_data in response.get('orders', []):
                # Only include active orders
                if order_data.get('status') != 'active':
                    continue
                
                # Map status
                status_map = {
                    'new': OrderStatus.PENDING.value,
                    'active': OrderStatus.OPEN.value,
                    'done': OrderStatus.FILLED.value,
                    'rejected': OrderStatus.REJECTED.value,
                    'canceled': OrderStatus.CANCELLED.value
                }
                
                # Create standardized symbol
                std_symbol = f"{order_data.get('srcCurrency', '').lower()}-{order_data.get('dstCurrency', '').lower()}"
                
                orders.append({
                    'id': order_data.get('id'),
                    'symbol': std_symbol,
                    'type': order_data.get('type'),
                    'side': 'buy' if order_data.get('type') == 'buy' else 'sell',
                    'amount': float(order_data.get('amount', 0)),
                    'price': float(order_data.get('price', 0)),
                    'status': status_map.get(order_data.get('status'), 'unknown'),
                    'filled': float(order_data.get('executed', 0)),
                    'remaining': float(order_data.get('amount', 0)) - float(order_data.get('executed', 0)),
                    'timestamp': order_data.get('created_at', int(time.time() * 1000))
                })
            
            return orders
            
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
            # Prepare request parameters
            data = {}
            if symbol:
                data['srcCurrency'] = self._get_base_currency(symbol)
                data['dstCurrency'] = self._get_quote_currency(symbol)
            
            response = self._make_request(
                method="POST",
                endpoint="/market/orders/list",
                data=data,
                auth_required=True
            )
            
            if not response or 'status' not in response or response['status'] != 'ok':
                logger.error(f"Failed to get order history: {response}")
                return []
            
            # Extract order information
            orders = []
            
            for order_data in response.get('orders', []):
                # Skip active orders
                if order_data.get('status') == 'active':
                    continue
                
                # Skip orders before since timestamp
                if since and order_data.get('created_at', 0) < since:
                    continue
                
                # Map status
                status_map = {
                    'new': OrderStatus.PENDING.value,
                    'active': OrderStatus.OPEN.value,
                    'done': OrderStatus.FILLED.value,
                    'rejected': OrderStatus.REJECTED.value,
                    'canceled': OrderStatus.CANCELLED.value
                }
                
                # Create standardized symbol
                std_symbol = f"{order_data.get('srcCurrency', '').lower()}-{order_data.get('dstCurrency', '').lower()}"
                
                orders.append({
                    'id': order_data.get('id'),
                    'symbol': std_symbol,
                    'type': order_data.get('type'),
                    'side': 'buy' if order_data.get('type') == 'buy' else 'sell',
                    'amount': float(order_data.get('amount', 0)),
                    'price': float(order_data.get('price', 0)),
                    'status': status_map.get(order_data.get('status'), 'unknown'),
                    'filled': float(order_data.get('executed', 0)),
                    'remaining': float(order_data.get('amount', 0)) - float(order_data.get('executed', 0)),
                    'timestamp': order_data.get('created_at', int(time.time() * 1000))
                })
                
                # Limit the number of orders
                if len(orders) >= limit:
                    break
            
            return orders
            
        except Exception as e:
            logger.error(f"Error getting order history: {str(e)}", exc_info=True)
            return []
    
    def get_trade_history(self, symbol: str = None, since: int = None, 
                        limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get trade history.
        
        Args:
            symbol: Trading pair symbol (optional)
            since: Start time in milliseconds
            limit: Number of trades to return
            
        Returns:
            List of trades
        """
        try:
            # Prepare request parameters
            data = {}
            if symbol:
                data['srcCurrency'] = self._get_base_currency(symbol)
                data['dstCurrency'] = self._get_quote_currency(symbol)
            
            response = self._make_request(
                method="POST",
                endpoint="/market/trades/list",
                data=data,
                auth_required=True
            )
            
            if not response or 'status' not in response or response['status'] != 'ok':
                logger.error(f"Failed to get trade history: {response}")
                return []
            
            # Extract trade information
            trades = []
            
            for trade_data in response.get('trades', []):
                # Skip trades before since timestamp
                if since and trade_data.get('created_at', 0) < since:
                    continue
                
                # Create standardized symbol
                std_symbol = f"{trade_data.get('srcCurrency', '').lower()}-{trade_data.get('dstCurrency', '').lower()}"
                
                trades.append({
                    'id': trade_data.get('id'),
                    'order_id': trade_data.get('orderId'),
                    'symbol': std_symbol,
                    'side': trade_data.get('type', 'buy'),
                    'amount': float(trade_data.get('amount', 0)),
                    'price': float(trade_data.get('price', 0)),
                    'fee': float(trade_data.get('fee', 0)),
                    'timestamp': trade_data.get('created_at', int(time.time() * 1000))
                })
                
                # Limit the number of trades
                if len(trades) >= limit:
                    break
            
            return trades
            
        except Exception as e:
            logger.error(f"Error getting trade history: {str(e)}", exc_info=True)
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
            precision = self.markets_info.get(symbol, {}).get('precision', {}).get('amount', 8)
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
            precision = self.markets_info.get(symbol, {}).get('precision', {}).get('price', 8)
            return float(f"%.{precision}f" % price)
        except Exception as e:
            logger.error(f"Error converting price to precision: {str(e)}", exc_info=True)
            return price 