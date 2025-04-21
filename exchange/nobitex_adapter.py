"""
Nobitex Exchange Adapter

This module provides the adapter for integrating with the Nobitex cryptocurrency exchange.
"""

import os
import json
import time
import hmac
import hashlib
import asyncio
import aiohttp
import base64
from typing import Dict, Any, List, Optional, Tuple, Union
from datetime import datetime, timedelta
from dotenv import load_dotenv

from trading_system.core.component import Component, ComponentStatus
from trading_system.core.logging import get_logger
from trading_system.core.config import ConfigManager
from trading_system.models.order import Order, OrderType, OrderSide, OrderStatus, OrderTimeInForce

# Load environment variables for API keys
load_dotenv()

logger = get_logger("exchange.nobitex")

# Constants
NOBITEX_API_URL = "https://api.nobitex.ir"
RETRY_ATTEMPTS = 3
RETRY_DELAY = 1.0  # seconds
REQUEST_TIMEOUT = 30  # seconds
CONNECTION_POOL_SIZE = 20
RATE_LIMIT_PER_MIN = 180  # Adjust based on Nobitex's actual rate limits


class ApiCredentials:
    """Secure container for API credentials."""
    
    def __init__(self, api_key: str = None, api_secret: str = None):
        """
        Initialize API credentials.
        
        Args:
            api_key: API key
            api_secret: API secret
        """
        # Load from environment variables if not provided
        self.api_key = api_key or os.getenv("NOBITEX_API_KEY")
        self.api_secret = api_secret or os.getenv("NOBITEX_API_SECRET")
        
        if not self.api_key or not self.api_secret:
            logger.warning("Nobitex API credentials not found")


class RateLimiter:
    """Rate limiter for API requests."""
    
    def __init__(self, limit_per_minute: int = RATE_LIMIT_PER_MIN):
        """
        Initialize rate limiter.
        
        Args:
            limit_per_minute: Maximum requests per minute
        """
        self.limit = limit_per_minute
        self.interval = 60.0 / limit_per_minute  # seconds between requests
        self.last_request_time = 0.0
        self.lock = asyncio.Lock()
    
    async def acquire(self) -> None:
        """Acquire permission to make a request."""
        async with self.lock:
            current_time = time.time()
            elapsed = current_time - self.last_request_time
            
            if elapsed < self.interval:
                await asyncio.sleep(self.interval - elapsed)
            
            self.last_request_time = time.time()


class NobitexAdapter(Component):
    """
    Adapter for the Nobitex cryptocurrency exchange.
    
    Features:
    - Secure API key management
    - Connection pooling
    - Retry mechanisms
    - Rate limiting
    - Order status synchronization
    - Partial fills handling
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the Nobitex adapter.
        
        Args:
            config: Configuration dictionary
        """
        super().__init__(
            component_id="nobitex_adapter",
            name="Nobitex Exchange Adapter",
            config=config
        )
        
        # API credentials
        self.credentials = ApiCredentials(
            config.get("api_key"),
            config.get("api_secret")
        )
        
        # Connection management
        self.session = None
        self.rate_limiter = RateLimiter(config.get("rate_limit_per_min", RATE_LIMIT_PER_MIN))
        
        # Configuration
        self.retry_attempts = config.get("retry_attempts", RETRY_ATTEMPTS)
        self.retry_delay = config.get("retry_delay", RETRY_DELAY)
        self.request_timeout = config.get("request_timeout", REQUEST_TIMEOUT)
        self.connection_pool_size = config.get("connection_pool_size", CONNECTION_POOL_SIZE)
        
        # Market data cache
        self.markets_cache = {}
        self.markets_cache_timestamp = 0
        self.markets_cache_ttl = 300  # 5 minutes
        
        # Order tracking
        self.pending_orders: Dict[str, Dict[str, Any]] = {}
        self.order_check_interval = config.get("order_check_interval", 5)  # seconds
        self.order_update_task = None
        
        # Symbol mapping (convert internal symbols to Nobitex format)
        self.symbol_mapping: Dict[str, str] = {
            "BTC/USDT": "BTCUSDT",
            "ETH/USDT": "ETHUSDT",
            "LTC/USDT": "LTCUSDT",
            "XRP/USDT": "XRPUSDT",
            "BCH/USDT": "BCHUSDT",
            "BNB/USDT": "BNBUSDT",
            "DOGE/USDT": "DOGEUSDT",
            "BTC/USDT": "BTCIRT",
            "ETH/USDT": "ETHIRT",
            "LTC/USDT": "LTCIRT",
            "XRP/USDT": "XRPIRT",
            "BCH/USDT": "BCHIRT",
            "BNB/USDT": "BNBIRT",
            "DOGE/USDT": "DOGEIRT"
        }
        
        # Inverse symbol mapping
        self.inverse_symbol_mapping = {v: k for k, v in self.symbol_mapping.items()}
        
        logger.info("Nobitex exchange adapter initialized")
    
    async def initialize(self) -> bool:
        """
        Initialize the adapter.
        
        Returns:
            Success flag
        """
        try:
            self._update_status(ComponentStatus.INITIALIZING)
            
            # Check credentials
            if not self.credentials.api_key or not self.credentials.api_secret:
                logger.error("Missing API credentials")
                self._update_status(ComponentStatus.ERROR)
                return False
            
            # Create session with connection pooling
            connector = aiohttp.TCPConnector(
                limit=self.connection_pool_size,
                verify_ssl=True
            )
            
            self.session = aiohttp.ClientSession(
                connector=connector,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "TradingBot/1.0"
                }
            )
            
            # Test connection
            success = await self._test_connection()
            if not success:
                logger.error("Failed to connect to Nobitex API")
                self._update_status(ComponentStatus.ERROR)
                return False
            
            # Start order update task
            self.order_update_task = asyncio.create_task(self._order_update_loop())
            
            self._update_status(ComponentStatus.OPERATIONAL)
            logger.info("Nobitex exchange adapter initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing Nobitex adapter: {str(e)}")
            self._update_status(ComponentStatus.ERROR)
            return False
    
    async def shutdown(self) -> bool:
        """
        Shutdown the adapter.
        
        Returns:
            Success flag
        """
        try:
            self._update_status(ComponentStatus.SHUTTING_DOWN)
            
            # Cancel order update task
            if self.order_update_task and not self.order_update_task.done():
                self.order_update_task.cancel()
                try:
                    await self.order_update_task
                except asyncio.CancelledError:
                    pass
            
            # Close session
            if self.session:
                await self.session.close()
                self.session = None
            
            self._update_status(ComponentStatus.SHUTDOWN)
            logger.info("Nobitex exchange adapter shutdown complete")
            return True
            
        except Exception as e:
            logger.error(f"Error shutting down Nobitex adapter: {str(e)}")
            return False
    
    async def get_markets(self) -> Optional[Dict[str, Any]]:
        """
        Get available markets.
        
        Returns:
            Markets information
        """
        # Check cache
        current_time = time.time()
        if self.markets_cache and current_time - self.markets_cache_timestamp < self.markets_cache_ttl:
            return self.markets_cache
        
        # Fetch markets
        response = await self._make_request("GET", "/v2/markets")
        
        if not response or "status" not in response or response["status"] != "ok":
            logger.error(f"Failed to get markets: {response}")
            return None
        
        # Update cache
        self.markets_cache = response
        self.markets_cache_timestamp = current_time
        
        return response
    
    async def get_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get ticker for a symbol.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Ticker information
        """
        # Convert symbol format
        nobitex_symbol = self._convert_symbol_to_nobitex(symbol)
        if not nobitex_symbol:
            logger.error(f"Invalid symbol: {symbol}")
            return None
        
        # Get ticker
        response = await self._make_request(
            "POST",
            "/market/stats",
            {"srcCurrency": nobitex_symbol.split("USDT")[0].lower(), "dstCurrency": "usdt"}
        )
        
        if not response or "status" not in response or response["status"] != "ok":
            logger.error(f"Failed to get ticker for {symbol}: {response}")
            return None
        
        return response
    
    async def get_order_book(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get order book for a symbol.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Order book information
        """
        # Convert symbol format
        nobitex_symbol = self._convert_symbol_to_nobitex(symbol)
        if not nobitex_symbol:
            logger.error(f"Invalid symbol: {symbol}")
            return None
        
        # Get order book
        response = await self._make_request(
            "GET",
            f"/v2/orderbook/{nobitex_symbol}"
        )
        
        if not response or "status" not in response or response["status"] != "ok":
            logger.error(f"Failed to get order book for {symbol}: {response}")
            return None
        
        return response
    
    async def get_account_balance(self) -> Optional[Dict[str, Any]]:
        """
        Get account balance.
        
        Returns:
            Account balance information
        """
        response = await self._make_request(
            "POST",
            "/users/wallets/list",
            {},
            True
        )
        
        if not response or "status" not in response or response["status"] != "ok":
            logger.error(f"Failed to get account balance: {response}")
            return None
        
        return response
    
    async def create_order(self, order: Order) -> Tuple[bool, Optional[str], Optional[Dict[str, Any]]]:
        """
        Create an order on the exchange.
        
        Args:
            order: Order to create
            
        Returns:
            Success flag, error message (if any), and order response
        """
        try:
            # Convert symbol format
            nobitex_symbol = self._convert_symbol_to_nobitex(order.symbol)
            if not nobitex_symbol:
                return False, f"Invalid symbol: {order.symbol}", None
            
            # Extract currencies
            symbol_parts = nobitex_symbol.split("USDT")
            src_currency = symbol_parts[0].lower()
            dst_currency = "usdt"
            
            # Prepare order parameters
            params = {
                "type": self._convert_order_type(order.order_type),
                "srcCurrency": src_currency,
                "dstCurrency": dst_currency,
                "amount": str(order.quantity),
                "price": str(order.price) if order.price else None,
                "execution": self._convert_time_in_force(order.time_in_force),
                "clientOrderId": order.client_order_id
            }
            
            # Set order side
            if order.side == OrderSide.BUY:
                params["mode"] = "buy"
            else:
                params["mode"] = "sell"
            
            # Remove None values
            params = {k: v for k, v in params.items() if v is not None}
            
            # Create order
            response = await self._make_request(
                "POST",
                "/market/orders/add",
                params,
                True
            )
            
            if not response or "status" not in response or response["status"] != "ok":
                error_msg = f"Failed to create order: {response}"
                logger.error(error_msg)
                return False, error_msg, response
            
            # Update order with exchange order ID
            if "order" in response:
                order.exchange_order_id = str(response["order"]["id"])
                order.update_status(OrderStatus.ACCEPTED)
            else:
                error_msg = "Order created but no order ID returned"
                logger.warning(error_msg)
                order.update_status(OrderStatus.PENDING)
                
            # Add to pending orders for status tracking
            self.pending_orders[order.client_order_id] = {
                "order": order,
                "last_checked": time.time()
            }
            
            logger.info(f"Order {order.client_order_id} created: {order.exchange_order_id}")
            return True, None, response
            
        except Exception as e:
            error_msg = f"Error creating order: {str(e)}"
            logger.error(error_msg)
            return False, error_msg, None
    
    async def cancel_order(self, order_id: str) -> Tuple[bool, Optional[str]]:
        """
        Cancel an order on the exchange.
        
        Args:
            order_id: Order ID to cancel
            
        Returns:
            Success flag and error message (if any)
        """
        try:
            # Check if this is a client order ID or exchange order ID
            is_client_id = order_id.startswith("order_")
            
            # Find exchange order ID if this is a client order ID
            if is_client_id and order_id in self.pending_orders:
                order = self.pending_orders[order_id]["order"]
                exchange_order_id = order.exchange_order_id
                if not exchange_order_id:
                    return False, "Order has no exchange ID yet"
            else:
                exchange_order_id = order_id
            
            # Cancel order
            response = await self._make_request(
                "POST",
                "/market/orders/cancel",
                {"order": exchange_order_id},
                True
            )
            
            if not response or "status" not in response or response["status"] != "ok":
                error_msg = f"Failed to cancel order {order_id}: {response}"
                logger.error(error_msg)
                return False, error_msg
            
            logger.info(f"Order {order_id} canceled")
            
            # Remove from pending orders
            if is_client_id and order_id in self.pending_orders:
                del self.pending_orders[order_id]
            
            return True, None
            
        except Exception as e:
            error_msg = f"Error canceling order {order_id}: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
    
    async def get_order_status(self, order_id: str) -> Optional[Dict[str, Any]]:
        """
        Get order status from the exchange.
        
        Args:
            order_id: Order ID
            
        Returns:
            Order status information
        """
        try:
            # Check if this is a client order ID or exchange order ID
            is_client_id = order_id.startswith("order_")
            
            # Find exchange order ID if this is a client order ID
            if is_client_id and order_id in self.pending_orders:
                order = self.pending_orders[order_id]["order"]
                exchange_order_id = order.exchange_order_id
                if not exchange_order_id:
                    return None
            else:
                exchange_order_id = order_id
            
            # Get order status
            response = await self._make_request(
                "POST",
                "/market/orders/status",
                {"id": exchange_order_id},
                True
            )
            
            if not response or "status" not in response or response["status"] != "ok":
                logger.error(f"Failed to get order status for {order_id}: {response}")
                return None
            
            return response
            
        except Exception as e:
            logger.error(f"Error getting order status for {order_id}: {str(e)}")
            return None
    
    async def get_open_orders(self) -> Optional[List[Dict[str, Any]]]:
        """
        Get open orders.
        
        Returns:
            List of open orders
        """
        response = await self._make_request(
            "POST",
            "/market/orders/list",
            {"status": "open"},
            True
        )
        
        if not response or "status" not in response or response["status"] != "ok":
            logger.error(f"Failed to get open orders: {response}")
            return None
        
        return response.get("orders", [])
    
    async def _test_connection(self) -> bool:
        """
        Test connection to the exchange.
        
        Returns:
            Success flag
        """
        try:
            # Try to get account profile
            response = await self._make_request(
                "POST",
                "/users/profile",
                {},
                True
            )
            
            if not response or "status" not in response or response["status"] != "ok":
                logger.error(f"Connection test failed: {response}")
                return False
            
            logger.info("Connection test successful")
            return True
            
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False
    
    async def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Dict[str, Any] = None,
        auth: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Make a request to the Nobitex API.
        
        Args:
            method: HTTP method
            endpoint: API endpoint
            params: Request parameters
            auth: Whether to authenticate the request
            
        Returns:
            API response
        """
        url = f"{NOBITEX_API_URL}{endpoint}"
        headers = {}
        
        # Authenticate if required
        if auth:
            if not self.credentials.api_key or not self.credentials.api_secret:
                logger.error("Cannot make authenticated request: missing API credentials")
                return None
            
            # Add API key to headers
            headers["X-API-Key"] = self.credentials.api_key
            
            # Add signature for POST requests with params
            if method.upper() == "POST" and params:
                # Convert params to sorted string
                payload = json.dumps(params, sort_keys=True)
                
                # Create signature
                signature = hmac.new(
                    self.credentials.api_secret.encode(),
                    payload.encode(),
                    hashlib.sha256
                ).hexdigest()
                
                # Add signature to headers
                headers["X-API-Signature"] = signature
        
        # Convert params to JSON
        json_params = params if params else None
        
        # Apply rate limiting
        await self.rate_limiter.acquire()
        
        # Make request with retries
        for attempt in range(self.retry_attempts):
            try:
                async with self.session.request(
                    method=method,
                    url=url,
                    headers=headers,
                    json=json_params,
                    timeout=self.request_timeout
                ) as response:
                    # Check status code
                    if response.status != 200:
                        logger.warning(f"Request failed with status {response.status}: {await response.text()}")
                        
                        # Retry on 5xx errors and some 4xx errors
                        if response.status >= 500 or response.status in [429, 408]:
                            if attempt < self.retry_attempts - 1:
                                # Exponential backoff
                                delay = self.retry_delay * (2 ** attempt)
                                logger.info(f"Retrying in {delay:.2f} seconds (attempt {attempt + 1}/{self.retry_attempts})")
                                await asyncio.sleep(delay)
                                continue
                        
                        return None
                    
                    # Parse response
                    return await response.json()
                    
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.warning(f"Request error: {str(e)}")
                
                if attempt < self.retry_attempts - 1:
                    # Exponential backoff
                    delay = self.retry_delay * (2 ** attempt)
                    logger.info(f"Retrying in {delay:.2f} seconds (attempt {attempt + 1}/{self.retry_attempts})")
                    await asyncio.sleep(delay)
                    continue
                
                return None
        
        return None
    
    async def _order_update_loop(self) -> None:
        """Order update loop."""
        while True:
            try:
                # Get all pending orders
                for client_order_id, order_info in list(self.pending_orders.items()):
                    order = order_info["order"]
                    
                    # Skip if order has no exchange ID yet
                    if not order.exchange_order_id:
                        continue
                    
                    # Skip if already filled or canceled
                    if order.status in [OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.EXPIRED]:
                        # Remove from pending orders
                        del self.pending_orders[client_order_id]
                        continue
                    
                    # Check if it's time to update this order
                    current_time = time.time()
                    elapsed = current_time - order_info["last_checked"]
                    if elapsed < self.order_check_interval:
                        continue
                    
                    # Update last checked time
                    order_info["last_checked"] = current_time
                    
                    # Get order status
                    order_status = await self.get_order_status(order.exchange_order_id)
                    if not order_status:
                        continue
                    
                    # Update order based on status
                    await self._update_order_from_status(order, order_status)
                
                # Sleep for a bit
                await asyncio.sleep(1)
                
            except asyncio.CancelledError:
                logger.info("Order update loop cancelled")
                break
                
            except Exception as e:
                logger.error(f"Error in order update loop: {str(e)}")
                await asyncio.sleep(5)
    
    async def _update_order_from_status(self, order: Order, status_data: Dict[str, Any]) -> None:
        """
        Update order based on status from exchange.
        
        Args:
            order: Order to update
            status_data: Status data from exchange
        """
        try:
            order_data = status_data.get("order")
            if not order_data:
                return
            
            # Get status
            status = order_data.get("status")
            if not status:
                return
            
            # Update order status
            if status == "new":
                order.update_status(OrderStatus.ACCEPTED)
            elif status == "processing":
                # Check for partial fill
                if "matchedAmount" in order_data and float(order_data["matchedAmount"]) > 0:
                    # Update with partial fill
                    filled_quantity = float(order_data["matchedAmount"])
                    average_price = float(order_data.get("price", 0))
                    
                    # Update order fill info
                    if filled_quantity > order.filled_quantity:
                        order.update_fill(
                            filled_quantity - order.filled_quantity,
                            average_price
                        )
                else:
                    order.update_status(OrderStatus.ACCEPTED)
            elif status == "done":
                # Order is filled
                filled_quantity = float(order_data.get("matchedAmount", 0))
                average_price = float(order_data.get("price", 0))
                
                # Update order fill info
                if filled_quantity > order.filled_quantity:
                    order.update_fill(
                        filled_quantity - order.filled_quantity,
                        average_price
                    )
                
                order.update_status(OrderStatus.FILLED)
            elif status == "canceled":
                order.update_status(OrderStatus.CANCELED)
            elif status == "expired":
                order.update_status(OrderStatus.EXPIRED)
            
        except Exception as e:
            logger.error(f"Error updating order {order.client_order_id}: {str(e)}")
    
    def _convert_symbol_to_nobitex(self, symbol: str) -> Optional[str]:
        """
        Convert internal symbol format to Nobitex format.
        
        Args:
            symbol: Internal symbol format
            
        Returns:
            Nobitex symbol format
        """
        return self.symbol_mapping.get(symbol)
    
    def _convert_nobitex_to_symbol(self, nobitex_symbol: str) -> Optional[str]:
        """
        Convert Nobitex symbol format to internal format.
        
        Args:
            nobitex_symbol: Nobitex symbol format
            
        Returns:
            Internal symbol format
        """
        return self.inverse_symbol_mapping.get(nobitex_symbol)
    
    def _convert_order_type(self, order_type: OrderType) -> str:
        """
        Convert order type to Nobitex format.
        
        Args:
            order_type: Order type
            
        Returns:
            Nobitex order type
        """
        if order_type == OrderType.MARKET:
            return "market"
        elif order_type == OrderType.LIMIT:
            return "limit"
        else:
            return "limit"  # Default to limit for unsupported types
    
    def _convert_time_in_force(self, time_in_force: OrderTimeInForce) -> str:
        """
        Convert time in force to Nobitex format.
        
        Args:
            time_in_force: Time in force
            
        Returns:
            Nobitex time in force
        """
        if time_in_force == OrderTimeInForce.IOC:
            return "ioc"
        elif time_in_force == OrderTimeInForce.FOK:
            return "fok"
        else:
            return "gtc"  # Default to GTC for unsupported types 