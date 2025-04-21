"""
Nobitex exchange adapter implementation for high-reliability trading system.

This adapter implements the ExchangeAdapter interface for Nobitex, an Iranian cryptocurrency exchange.
"""

import time
import json
import logging
import os
from typing import Dict, Any, List, Optional
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from trading_system.exchange.adapter import ExchangeAdapter, OrderResult, OrderInfo, MarketInfo


class NobitexAdapter(ExchangeAdapter):
    """
    Nobitex exchange adapter implementation.
    
    This adapter handles communication with the Nobitex exchange API,
    translating between the exchange-specific formats and the common formats
    defined by the ExchangeAdapter interface.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the Nobitex adapter.
        
        Args:
            config: Configuration dictionary containing API credentials and settings
        """
        super().__init__(name="nobitex", config=config)
        
        # API endpoints
        self.base_url = "https://api.nobitex.ir"
        
        # Credentials
        self.api_key = config.get("api_key")
        
        # Cache settings
        self.cache_dir = config.get("cache_dir", "./cache")
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Cache expiry times (in seconds)
        self.wallet_cache_expiry = 300  # 5 minutes
        self.market_stats_expiry = 30   # 30 seconds
        
        # Cache data
        self.markets_info = {}
        self.symbol_mapping = {}
        self.reverse_symbol_mapping = {}
        self.wallet_cache = {}
        self.wallet_cache_timestamp = 0
        
        # Request settings
        self.timeout = config.get("timeout", 15)
        self.max_retries = config.get("max_retries", 3)
        
        # Rate limiting
        self.api_calls = []
        self.max_calls_per_min = 60
        self.last_request_time = 0
        
    def _initialize(self) -> None:
        """Initialize exchange connection."""
        try:
            # Test connection
            if not self.ping():
                raise Exception("Failed to connect to Nobitex API")
                
            # Load market information
            self._load_markets()
            
            # Log successful initialization
            logging.info("Successfully initialized Nobitex exchange adapter")
            
        except Exception as e:
            logging.error(f"Failed to initialize Nobitex exchange adapter: {str(e)}")
            raise
    
    def _load_markets(self) -> None:
        """Load market information from Nobitex."""
        try:
            # Get market information
            response = self._make_request(
                method="GET",
                endpoint="/v2/orderbook/all",
                auth_required=False
            )
            
            if not response or 'status' not in response or response['status'] != 'ok':
                logging.error(f"Failed to get market information: {response}")
                return
            
            # Extract market symbols
            for symbol in response.get('data', {}):
                # Map from Nobitex format to standardized format
                # For example, BTCRLS -> btc-rls
                base_asset = symbol[:-3].lower()  # BTC -> btc
                quote_asset = symbol[-3:].lower() # RLS -> rls
                
                standardized_symbol = f"{base_asset}-{quote_asset}"
                
                self.symbol_mapping[standardized_symbol] = symbol
                self.reverse_symbol_mapping[symbol] = standardized_symbol
                
                # Store market info
                self.markets_info[standardized_symbol] = MarketInfo(
                    symbol=standardized_symbol,
                    base_asset=base_asset,
                    quote_asset=quote_asset,
                    min_price=0.00000001,  # Default values
                    max_price=1000000000000,
                    price_increment=0.00000001,
                    min_quantity=0.00000001,
                    max_quantity=1000000,
                    quantity_increment=0.00000001,
                    min_notional=10000,  # 10,000 RLS minimum order
                    status="active",
                    raw_data=response.get('data', {}).get(symbol, {})
                )
            
            logging.info(f"Loaded {len(self.markets_info)} markets from Nobitex")
            
        except Exception as e:
            logging.error(f"Error loading markets: {str(e)}")
            raise
    
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
            url = f"{self.base_url}{endpoint}"
            
            # Prepare headers
            if headers is None:
                headers = {}
            
            headers['Content-Type'] = 'application/json'
            
            # Add authentication token if required
            if auth_required and self.api_key:
                headers['Authorization'] = f"Token {self.api_key}"
            
            # Apply rate limiting
            self._enforce_rate_limit()
            
            # Make the request
            logging.debug(f"Making Nobitex API request: {method} {endpoint}")
            
            if method == 'GET':
                response = requests.get(url, params=params, headers=headers, timeout=self.timeout)
            elif method == 'POST':
                response = requests.post(url, json=data, headers=headers, timeout=self.timeout)
            elif method == 'DELETE':
                response = requests.delete(url, json=data, headers=headers, timeout=self.timeout)
            else:
                logging.error(f"Unsupported HTTP method: {method}")
                return {}
            
            # Check for HTTP errors
            response.raise_for_status()
            
            # Parse JSON response
            result = response.json()
            
            # Check for API errors
            if result.get('status') == 'failed':
                error_msg = result.get('message', 'Unknown API error')
                logging.error(f"Nobitex API error: {error_msg}")
                # Raise exception to trigger retry if needed
                raise Exception(f"Nobitex API error: {error_msg}")
            
            return result
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Request error in Nobitex API: {str(e)}")
            raise
        except Exception as e:
            logging.error(f"Error in Nobitex API request: {str(e)}")
            return {}
    
    def _enforce_rate_limit(self) -> None:
        """Enforce rate limiting to avoid API throttling."""
        current_time = time.time()
        
        # Remove API calls older than 60 seconds
        self.api_calls = [t for t in self.api_calls if current_time - t < 60]
        
        # Check if we're approaching the rate limit
        if len(self.api_calls) >= self.max_calls_per_min - 5:
            wait_time = 2.0  # Wait 2 seconds when approaching limit
            logging.warning(f"Approaching rate limit ({len(self.api_calls)}/{self.max_calls_per_min}), waiting {wait_time}s")
            time.sleep(wait_time)
            
        # Enforce minimum time between requests (0.5 second)
        time_since_last = current_time - self.last_request_time
        if time_since_last < 0.5:
            sleep_time = 0.5 - time_since_last
            time.sleep(sleep_time)
            
        # Record this API call
        self.api_calls.append(time.time())
        self.last_request_time = time.time()
    
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
        
        logging.warning(f"Could not map symbol {symbol} to Nobitex format")
        return symbol.upper().replace('-', '')
    
    def _get_base_currency(self, symbol: str) -> str:
        """Get base currency from symbol."""
        parts = symbol.split('-')
        if len(parts) == 2:
            return parts[0]
        return symbol[:-3] if len(symbol) > 3 else symbol
    
    def _get_quote_currency(self, symbol: str) -> str:
        """Get quote currency from symbol."""
        parts = symbol.split('-')
        if len(parts) == 2:
            return parts[1]
        return symbol[-3:] if len(symbol) > 3 else 'rls'
    
    def get_markets(self) -> Dict[str, MarketInfo]:
        """
        Get all available markets.
        
        Returns:
            Dict of symbol -> MarketInfo
        """
        # Reload markets if necessary
        if not self.markets_info:
            self._load_markets()
            
        return self.markets_info
    
    def get_ticker(self, symbol: str) -> Dict[str, Any]:
        """
        Get current ticker data for a symbol.
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            Dict containing ticker data
        """
        try:
            # Get stats for all markets
            response = self._make_request(
                method="GET",
                endpoint="/market/stats",
                auth_required=False
            )
            
            if not response or response.get('status') != 'ok':
                logging.error(f"Failed to get ticker data: {response}")
                return {}
            
            # Find the symbol in the response
            nobitex_symbol = self._map_symbol(symbol)
            
            stats = response.get('stats', {})
            if nobitex_symbol not in stats:
                logging.warning(f"Symbol {symbol} ({nobitex_symbol}) not found in ticker data")
                return {}
            
            symbol_stats = stats[nobitex_symbol]
            
            # Format the data
            return {
                'symbol': symbol,
                'price': float(symbol_stats.get('latest', 0)),
                'high': float(symbol_stats.get('dayHigh', 0)),
                'low': float(symbol_stats.get('dayLow', 0)),
                'volume': float(symbol_stats.get('volumeSrc', 0)),
                'timestamp': int(time.time() * 1000),
                'bid': float(symbol_stats.get('bestBuy', 0)),
                'ask': float(symbol_stats.get('bestSell', 0)),
                'raw_data': symbol_stats
            }
            
        except Exception as e:
            logging.error(f"Error getting ticker for {symbol}: {str(e)}")
            return {}
    
    def get_order_book(self, symbol: str, limit: int = 20) -> Dict[str, Any]:
        """
        Get order book for a symbol.
        
        Args:
            symbol: Trading pair symbol
            limit: Maximum number of bids/asks to return
            
        Returns:
            Dict containing order book data
        """
        try:
            # Map to Nobitex symbol format
            nobitex_symbol = self._map_symbol(symbol)
            
            # Make request
            response = self._make_request(
                method="POST",
                endpoint="/market/orders/book",
                data={'symbol': nobitex_symbol},
                auth_required=False
            )
            
            if not response or 'status' not in response or response['status'] != 'ok':
                logging.error(f"Failed to get order book for {symbol}: {response}")
                return {}
            
            # Format the response
            return {
                'symbol': symbol,
                'bids': response.get('bids', [])[:limit],
                'asks': response.get('asks', [])[:limit],
                'timestamp': int(time.time() * 1000),
                'raw_data': response
            }
            
        except Exception as e:
            logging.error(f"Error getting order book for {symbol}: {str(e)}")
            return {}
    
    def get_recent_trades(self, symbol: str, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get recent trades for a symbol.
        
        Args:
            symbol: Trading pair symbol
            limit: Maximum number of trades to return
            
        Returns:
            List of trades
        """
        try:
            # Map to Nobitex symbol format
            nobitex_symbol = self._map_symbol(symbol)
            
            # Make request
            response = self._make_request(
                method="POST",
                endpoint="/market/trades/list",
                data={'symbol': nobitex_symbol, 'count': limit},
                auth_required=False
            )
            
            if not response or 'status' not in response or response['status'] != 'ok':
                logging.error(f"Failed to get recent trades for {symbol}: {response}")
                return []
            
            # Format the trades
            trades = []
            for trade in response.get('trades', []):
                trades.append({
                    'id': trade.get('id', ''),
                    'price': float(trade.get('price', 0)),
                    'quantity': float(trade.get('quantity', 0)),
                    'time': int(trade.get('time', 0)),
                    'side': trade.get('type', ''),
                    'raw_data': trade
                })
            
            return trades
            
        except Exception as e:
            logging.error(f"Error getting recent trades for {symbol}: {str(e)}")
            return []
    
    def get_candles(self, symbol: str, interval: str, start_time: Optional[int] = None, 
                   end_time: Optional[int] = None, limit: int = 500) -> List[Dict[str, Any]]:
        """
        Get candlestick data for a symbol.
        
        Args:
            symbol: Trading pair symbol
            interval: Candlestick interval (e.g. '1m', '1h', '1d')
            start_time: Start time in milliseconds
            end_time: End time in milliseconds
            limit: Maximum number of candles to return
            
        Returns:
            List of candles
        """
        try:
            # Convert interval to resolution value for Nobitex
            resolution_map = {
                '1m': '1',
                '5m': '5',
                '15m': '15',
                '30m': '30',
                '1h': '60',
                '4h': '240',
                '1d': '1D',
                '1w': '1W',
            }
            
            resolution = resolution_map.get(interval, '60')  # Default to 1h
            
            # Map symbol to Nobitex format
            # For chart data API, convert the format
            symbol_parts = symbol.split('-')
            if len(symbol_parts) == 2:
                base = symbol_parts[0].upper()
                quote = "IRT" if symbol_parts[1].upper() == "RLS" else symbol_parts[1].upper()
                chart_symbol = f"{base}{quote}"
            else:
                chart_symbol = symbol.upper()
            
            # Prepare parameters
            params = {
                'symbol': chart_symbol,
                'resolution': resolution,
            }
            
            # Add time parameters if provided
            if start_time:
                params['from'] = int(start_time / 1000)  # Convert from ms to seconds
            if end_time:
                params['to'] = int(end_time / 1000)  # Convert from ms to seconds
            
            # Make request to TradingView endpoint
            url = "https://api.nobitex.ir/market/udf/history"
            response = requests.get(url, params=params, timeout=self.timeout)
            
            if response.status_code != 200:
                logging.error(f"Failed to get candles for {symbol}: HTTP {response.status_code}")
                return []
            
            data = response.json()
            
            # Check for errors
            if data.get('s') == 'error':
                logging.error(f"API error getting candles: {data.get('errmsg', 'Unknown error')}")
                return []
            
            # Format the candles
            candles = []
            times = data.get('t', [])
            opens = data.get('o', [])
            highs = data.get('h', [])
            lows = data.get('l', [])
            closes = data.get('c', [])
            volumes = data.get('v', [])
            
            for i in range(min(len(times), limit)):
                if i < len(times) and i < len(opens) and i < len(highs) and i < len(lows) and i < len(closes):
                    candle = {
                        'time': times[i] * 1000,  # Convert to milliseconds
                        'open': float(opens[i]),
                        'high': float(highs[i]),
                        'low': float(lows[i]),
                        'close': float(closes[i]),
                        'volume': float(volumes[i]) if i < len(volumes) else 0,
                    }
                    candles.append(candle)
            
            return candles
            
        except Exception as e:
            logging.error(f"Error getting candles for {symbol}: {str(e)}")
            return []
    
    def get_balances(self) -> Dict[str, Dict[str, float]]:
        """
        Get account balances.
        
        Returns:
            Dict of asset -> balance info (free, locked, total)
        """
        try:
            # Check if we can use cached data
            current_time = time.time()
            if self.wallet_cache and current_time - self.wallet_cache_timestamp < self.wallet_cache_expiry:
                logging.info("Using cached wallet data")
                return self.wallet_cache
            
            # Make request
            response = self._make_request(
                method="POST",
                endpoint="/users/wallets/list",
                auth_required=True
            )
            
            if not response or 'status' not in response or response['status'] != 'ok':
                logging.error(f"Failed to get wallet balances: {response}")
                return {}
            
            # Process and format the response
            balances = {}
            for wallet in response.get('wallets', []):
                currency = wallet.get('currency', '').lower()
                
                # Skip empty wallets
                if float(wallet.get('balance', 0)) <= 0:
                    continue
                
                balances[currency] = {
                    'free': float(wallet.get('balance', 0)) - float(wallet.get('frozen', 0)),
                    'locked': float(wallet.get('frozen', 0)),
                    'total': float(wallet.get('balance', 0))
                }
            
            # Update cache
            self.wallet_cache = balances
            self.wallet_cache_timestamp = current_time
            
            return balances
            
        except Exception as e:
            logging.error(f"Error getting balances: {str(e)}")
            return {}
    
    def place_order(self, symbol: str, side: str, order_type: str, quantity: float,
                   price: Optional[float] = None, stop_price: Optional[float] = None,
                   time_in_force: str = 'GTC', client_order_id: Optional[str] = None,
                   **kwargs) -> OrderResult:
        """
        Place an order.
        
        Args:
            symbol: Trading pair symbol
            side: Order side ('buy' or 'sell')
            order_type: Order type ('market', 'limit', etc.)
            quantity: Order quantity
            price: Order price (for limit orders)
            stop_price: Stop price (for stop orders)
            time_in_force: Time in force ('GTC', 'IOC', 'FOK')
            client_order_id: Client-generated order ID
            **kwargs: Additional exchange-specific parameters
            
        Returns:
            OrderResult containing result of order placement
        """
        try:
            # Split the symbol into base and quote currencies
            base_currency = self._get_base_currency(symbol)
            quote_currency = self._get_quote_currency(symbol)
            
            # Prepare the order payload
            payload = {
                'type': side,  # 'buy' or 'sell'
                'srcCurrency': base_currency,
                'dstCurrency': quote_currency,
                'amount': str(quantity),
                'price': str(price) if price else None,
                'execution': 'limit' if order_type.lower() == 'limit' else 'market'
            }
            
            # Add client order ID if provided
            if client_order_id:
                payload['clientOrderId'] = client_order_id
            
            # Make the API call
            response = self._make_request(
                method="POST",
                endpoint="/market/orders/add",
                data=payload,
                auth_required=True
            )
            
            if not response or 'status' not in response:
                return OrderResult(
                    success=False,
                    error="Invalid API response",
                    raw_response=response
                )
            
            if response['status'] != 'ok':
                return OrderResult(
                    success=False,
                    error=response.get('message', 'Unknown error'),
                    raw_response=response
                )
            
            # Extract order information
            order_data = response.get('order', {})
            
            return OrderResult(
                success=True,
                exchange_order_id=order_data.get('id', ''),
                client_order_id=client_order_id,
                status=order_data.get('status', 'unknown'),
                filled_quantity=float(order_data.get('executedAmount', 0)),
                average_price=float(order_data.get('avgPrice', 0)) if order_data.get('avgPrice') else None,
                timestamp=int(time.time() * 1000),
                raw_response=response
            )
            
        except Exception as e:
            logging.error(f"Error placing order for {symbol}: {str(e)}")
            return OrderResult(
                success=False,
                error=str(e)
            )
    
    def cancel_order(self, symbol: str, order_id: Optional[str] = None, 
                    client_order_id: Optional[str] = None) -> OrderResult:
        """
        Cancel an order.
        
        Args:
            symbol: Trading pair symbol
            order_id: Exchange order ID
            client_order_id: Client order ID
            
        Returns:
            OrderResult containing result of cancellation
        """
        try:
            if not order_id and not client_order_id:
                return OrderResult(
                    success=False,
                    error="Either order_id or client_order_id must be provided"
                )
            
            # Prepare the payload
            payload = {
                'order': order_id,
                'status': 'canceled'
            }
            
            # Add client order ID if provided
            if client_order_id and not order_id:
                # Try to find the order ID from client order ID
                open_orders = self.get_open_orders(symbol)
                for order in open_orders:
                    if order.client_order_id == client_order_id:
                        payload['order'] = order.exchange_order_id
                        break
                
                if 'order' not in payload or not payload['order']:
                    return OrderResult(
                        success=False,
                        error=f"Could not find order with client ID {client_order_id}"
                    )
            
            # Make the API call
            response = self._make_request(
                method="POST",
                endpoint="/market/orders/update-status",
                data=payload,
                auth_required=True
            )
            
            if not response or 'status' not in response:
                return OrderResult(
                    success=False,
                    error="Invalid API response",
                    raw_response=response
                )
            
            if response['status'] != 'ok':
                return OrderResult(
                    success=False,
                    error=response.get('message', 'Unknown error'),
                    raw_response=response
                )
            
            return OrderResult(
                success=True,
                exchange_order_id=order_id,
                client_order_id=client_order_id,
                status="canceled",
                timestamp=int(time.time() * 1000),
                raw_response=response
            )
            
        except Exception as e:
            logging.error(f"Error canceling order {order_id}: {str(e)}")
            return OrderResult(
                success=False,
                error=str(e)
            )
    
    def get_order(self, symbol: str, order_id: Optional[str] = None,
                 client_order_id: Optional[str] = None) -> OrderResult:
        """
        Get order information.
        
        Args:
            symbol: Trading pair symbol
            order_id: Exchange order ID
            client_order_id: Client order ID
            
        Returns:
            OrderResult containing order information
        """
        try:
            if not order_id and not client_order_id:
                return OrderResult(
                    success=False,
                    error="Either order_id or client_order_id must be provided"
                )
            
            # Use client_order_id to find order_id if necessary
            if client_order_id and not order_id:
                # Try to find the order ID from client order ID
                open_orders = self.get_open_orders(symbol)
                for order in open_orders:
                    if order.client_order_id == client_order_id:
                        order_id = order.exchange_order_id
                        break
                
                if not order_id:
                    return OrderResult(
                        success=False,
                        error=f"Could not find order with client ID {client_order_id}"
                    )
            
            # Prepare the payload
            payload = {
                'id': order_id
            }
            
            # Make the API call
            response = self._make_request(
                method="POST",
                endpoint="/market/orders/status",
                data=payload,
                auth_required=True
            )
            
            if not response or 'status' not in response:
                return OrderResult(
                    success=False,
                    error="Invalid API response",
                    raw_response=response
                )
            
            if response['status'] != 'ok':
                return OrderResult(
                    success=False,
                    error=response.get('message', 'Unknown error'),
                    raw_response=response
                )
            
            # Extract order information
            order_data = response.get('order', {})
            
            return OrderResult(
                success=True,
                exchange_order_id=order_id,
                client_order_id=client_order_id,
                status=order_data.get('status', 'unknown'),
                filled_quantity=float(order_data.get('executedAmount', 0)),
                average_price=float(order_data.get('avgPrice', 0)) if order_data.get('avgPrice') else None,
                timestamp=int(time.time() * 1000),
                raw_response=response
            )
            
        except Exception as e:
            logging.error(f"Error getting order {order_id}: {str(e)}")
            return OrderResult(
                success=False,
                error=str(e)
            )
    
    def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderInfo]:
        """
        Get all open orders.
        
        Args:
            symbol: Trading pair symbol (if None, get all open orders)
            
        Returns:
            List of OrderInfo
        """
        try:
            # Prepare the payload
            payload = {}
            
            if symbol:
                # Split the symbol
                base_currency = self._get_base_currency(symbol)
                quote_currency = self._get_quote_currency(symbol)
                
                payload['srcCurrency'] = base_currency
                payload['dstCurrency'] = quote_currency
            
            # Make the API call
            response = self._make_request(
                method="POST",
                endpoint="/market/orders/list",
                data=payload,
                auth_required=True
            )
            
            if not response or 'status' not in response or response['status'] != 'ok':
                logging.error(f"Failed to get open orders: {response}")
                return []
            
            # Format the orders
            orders = []
            for order_data in response.get('orders', []):
                # Create symbol from srcCurrency and dstCurrency
                src = order_data.get('srcCurrency', '').lower()
                dst = order_data.get('dstCurrency', '').lower()
                order_symbol = f"{src}-{dst}"
                
                # Skip orders for other symbols if symbol is specified
                if symbol and order_symbol != symbol.lower():
                    continue
                
                # Format the order
                order_info = OrderInfo(
                    exchange_order_id=order_data.get('id', ''),
                    client_order_id=order_data.get('clientOrderId'),
                    symbol=order_symbol,
                    side=order_data.get('type', ''),
                    order_type=order_data.get('execution', 'limit'),
                    quantity=float(order_data.get('amount', 0)),
                    price=float(order_data.get('price', 0)),
                    stop_price=None,
                    filled_quantity=float(order_data.get('executedAmount', 0)),
                    average_price=float(order_data.get('avgPrice', 0)) if order_data.get('avgPrice') else None,
                    status=order_data.get('status', 'unknown'),
                    time_in_force='GTC',
                    created_time=int(order_data.get('created_at', 0)) * 1000,
                    updated_time=int(order_data.get('updated_at', 0)) * 1000,
                    raw_data=order_data
                )
                
                orders.append(order_info)
            
            return orders
            
        except Exception as e:
            logging.error(f"Error getting open orders: {str(e)}")
            return []
    
    def get_order_history(self, symbol: Optional[str] = None, 
                         limit: int = 100) -> List[OrderInfo]:
        """
        Get order history.
        
        Args:
            symbol: Trading pair symbol (if None, get all order history)
            limit: Maximum number of orders to return
            
        Returns:
            List of OrderInfo
        """
        try:
            # Prepare the payload
            payload = {
                'status': 'all'
            }
            
            if symbol:
                # Split the symbol
                base_currency = self._get_base_currency(symbol)
                quote_currency = self._get_quote_currency(symbol)
                
                payload['srcCurrency'] = base_currency
                payload['dstCurrency'] = quote_currency
            
            # Make the API call
            response = self._make_request(
                method="POST",
                endpoint="/market/orders/list",
                data=payload,
                auth_required=True
            )
            
            if not response or 'status' not in response or response['status'] != 'ok':
                logging.error(f"Failed to get order history: {response}")
                return []
            
            # Format the orders
            orders = []
            for order_data in response.get('orders', [])[:limit]:
                # Create symbol from srcCurrency and dstCurrency
                src = order_data.get('srcCurrency', '').lower()
                dst = order_data.get('dstCurrency', '').lower()
                order_symbol = f"{src}-{dst}"
                
                # Skip orders for other symbols if symbol is specified
                if symbol and order_symbol != symbol.lower():
                    continue
                
                # Format the order
                order_info = OrderInfo(
                    exchange_order_id=order_data.get('id', ''),
                    client_order_id=order_data.get('clientOrderId'),
                    symbol=order_symbol,
                    side=order_data.get('type', ''),
                    order_type=order_data.get('execution', 'limit'),
                    quantity=float(order_data.get('amount', 0)),
                    price=float(order_data.get('price', 0)),
                    stop_price=None,
                    filled_quantity=float(order_data.get('executedAmount', 0)),
                    average_price=float(order_data.get('avgPrice', 0)) if order_data.get('avgPrice') else None,
                    status=order_data.get('status', 'unknown'),
                    time_in_force='GTC',
                    created_time=int(order_data.get('created_at', 0)) * 1000,
                    updated_time=int(order_data.get('updated_at', 0)) * 1000,
                    raw_data=order_data
                )
                
                orders.append(order_info)
            
            return orders
            
        except Exception as e:
            logging.error(f"Error getting order history: {str(e)}")
            return []
    
    def get_trade_history(self, symbol: Optional[str] = None, 
                         limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get trade history.
        
        Args:
            symbol: Trading pair symbol (if None, get all trade history)
            limit: Maximum number of trades to return
            
        Returns:
            List of trades
        """
        try:
            # Prepare the payload
            payload = {}
            
            if symbol:
                payload['symbol'] = self._map_symbol(symbol)
            
            # Make the API call
            response = self._make_request(
                method="POST",
                endpoint="/market/trades/list",
                data=payload,
                auth_required=False
            )
            
            if not response or 'status' not in response or response['status'] != 'ok':
                logging.error(f"Failed to get trade history: {response}")
                return []
            
            # Format the trades
            trades = []
            for trade_data in response.get('trades', [])[:limit]:
                trade = {
                    'id': trade_data.get('id', ''),
                    'symbol': self.reverse_symbol_mapping.get(trade_data.get('symbol', ''), trade_data.get('symbol', '')),
                    'price': float(trade_data.get('price', 0)),
                    'quantity': float(trade_data.get('quantity', 0)),
                    'side': trade_data.get('side', ''),
                    'time': int(trade_data.get('time', 0)),
                    'raw_data': trade_data
                }
                
                trades.append(trade)
            
            return trades
            
        except Exception as e:
            logging.error(f"Error getting trade history: {str(e)}")
            return []
    
    def get_server_time(self) -> int:
        """
        Get exchange server time.
        
        Returns:
            Server time in milliseconds
        """
        try:
            # Make the API call
            response = self._make_request(
                method="GET",
                endpoint="/time",
                auth_required=False
            )
            
            if not response or 'status' not in response or response['status'] != 'ok':
                logging.error(f"Failed to get server time: {response}")
                return int(time.time() * 1000)
            
            # Extract the server time
            return int(response.get('time', time.time() * 1000))
            
        except Exception as e:
            logging.error(f"Error getting server time: {str(e)}")
            return int(time.time() * 1000)
    
    def ping(self) -> bool:
        """
        Ping exchange to check connectivity.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Make a simple API call to check connectivity
            response = self._make_request(
                method="GET",
                endpoint="/v2/orderbook/all",
                auth_required=False
            )
            
            return response and 'status' in response and response['status'] == 'ok'
            
        except Exception as e:
            logging.error(f"Error pinging Nobitex exchange: {str(e)}")
            return False 