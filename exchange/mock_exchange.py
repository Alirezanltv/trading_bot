"""
Mock Exchange Adapter

This module implements a mock exchange adapter for testing and development.
"""

import logging
import time
import uuid
from typing import Dict, Any, List, Optional

from trading_system.exchange.base_adapter import ExchangeAdapter
from trading_system.models.order import Order, OrderStatus, OrderType, OrderSide

logger = logging.getLogger(__name__)


class MockExchangeAdapter(ExchangeAdapter):
    """
    Mock exchange adapter for testing.
    
    This adapter simulates an exchange API for testing purposes.
    It doesn't make any actual API calls but responds with simulated data.
    """
    
    def __init__(
        self, 
        exchange_id: str = "mock", 
        api_key: str = None, 
        api_secret: str = None,
        additional_params: Dict[str, Any] = None
    ):
        """
        Initialize mock exchange adapter.
        
        Args:
            exchange_id: Unique identifier for the exchange
            api_key: API key for authentication
            api_secret: API secret for authentication
            additional_params: Additional parameters for configuration
        """
        super().__init__(exchange_id, api_key, api_secret, additional_params)
        self.orders: Dict[str, Dict[str, Any]] = {}
        self.market_data: Dict[str, Dict[str, Any]] = {}
        self.balances: Dict[str, float] = {
            "BTC": 1.0,
            "ETH": 10.0,
            "USDT": 10000.0,
            "USD": 10000.0
        }
        self.positions: Dict[str, Dict[str, Any]] = {}
        self.connected = False
        self.latency = additional_params.get("latency", 0.1) if additional_params else 0.1
        self.error_rate = additional_params.get("error_rate", 0.0) if additional_params else 0.0
        
        # Initialize market data for common trading pairs
        self._initialize_market_data()
    
    def _initialize_market_data(self) -> None:
        """Initialize market data for common trading pairs."""
        symbols = ["BTC/USDT", "ETH/USDT", "BTC/USD", "ETH/USD"]
        
        for symbol in symbols:
            base, quote = symbol.split("/")
            self.market_data[symbol] = {
                "symbol": symbol,
                "bid": 40000.0 if base == "BTC" else 2800.0,
                "ask": 40100.0 if base == "BTC" else 2820.0,
                "last": 40050.0 if base == "BTC" else 2810.0,
                "high": 41000.0 if base == "BTC" else 2900.0,
                "low": 39000.0 if base == "BTC" else 2700.0,
                "volume": 100.0,
                "timestamp": time.time()
            }
    
    def connect(self) -> bool:
        """
        Connect to the exchange.
        
        Returns:
            True if connection successful, False otherwise
        """
        # Simulate connection latency
        time.sleep(self.latency)
        self.connected = True
        logger.info(f"Connected to mock exchange {self.exchange_id}")
        return True
    
    def disconnect(self) -> bool:
        """
        Disconnect from the exchange.
        
        Returns:
            True if disconnection successful, False otherwise
        """
        self.connected = False
        logger.info(f"Disconnected from mock exchange {self.exchange_id}")
        return True
    
    def _check_connection(self) -> bool:
        """
        Check the connection status with the exchange.
        
        Returns:
            True if connected, False otherwise
        """
        return self.connected
    
    def validate_order(self, order: Order) -> Dict[str, Any]:
        """
        Validate an order before submission.
        
        Args:
            order: Order to validate
            
        Returns:
            Dictionary containing validation result
        """
        # Simulate validation latency
        time.sleep(self.latency)
        
        # Basic validation
        if not order.symbol:
            return {
                "valid": False,
                "message": "Symbol is required"
            }
        
        if order.quantity <= 0:
            return {
                "valid": False,
                "message": "Quantity must be positive"
            }
        
        # Check if we have sufficient balance
        base, quote = order.symbol.split("/")
        if order.side == OrderSide.BUY:
            # For buy orders, check quote currency balance
            price = order.price if order.price else self.market_data.get(order.symbol, {}).get("ask", 0)
            required_balance = order.quantity * price
            if required_balance > self.balances.get(quote, 0):
                return {
                    "valid": False,
                    "message": f"Insufficient {quote} balance"
                }
        else:
            # For sell orders, check base currency balance
            if order.quantity > self.balances.get(base, 0):
                return {
                    "valid": False,
                    "message": f"Insufficient {base} balance"
                }
        
        return {
            "valid": True,
            "message": "Order validated successfully",
            "details": {
                "fees": self._calculate_fees(order)
            }
        }
    
    def submit_order(self, order: Order) -> Dict[str, Any]:
        """
        Submit an order to the exchange.
        
        Args:
            order: Order to submit
            
        Returns:
            Dictionary containing submission result
        """
        # Simulate submission latency
        time.sleep(self.latency)
        
        # Validate the order
        validation = self.validate_order(order)
        if not validation["valid"]:
            return {
                "success": False,
                "message": validation["message"]
            }
        
        # Generate exchange order ID
        exchange_order_id = str(uuid.uuid4())
        
        # Create order record
        base, quote = order.symbol.split("/")
        price = order.price if order.price else self.market_data.get(order.symbol, {}).get("last", 0)
        
        # For market orders, fill immediately
        filled_quantity = 0.0
        fill_price = 0.0
        fees = 0.0
        
        if order.order_type == OrderType.MARKET:
            filled_quantity = order.quantity
            fill_price = price
            fees = self._calculate_fees(order, filled_quantity, fill_price)
            
            # Update balances
            if order.side == OrderSide.BUY:
                self.balances[base] = self.balances.get(base, 0) + filled_quantity
                self.balances[quote] = self.balances.get(quote, 0) - (filled_quantity * fill_price) - fees
            else:
                self.balances[base] = self.balances.get(base, 0) - filled_quantity
                self.balances[quote] = self.balances.get(quote, 0) + (filled_quantity * fill_price) - fees
        
        # Store order
        order_record = {
            "exchange_order_id": exchange_order_id,
            "client_order_id": order.client_order_id,
            "symbol": order.symbol,
            "side": order.side.value,
            "order_type": order.order_type.value,
            "quantity": order.quantity,
            "price": price,
            "status": OrderStatus.FILLED.value if filled_quantity == order.quantity else OrderStatus.NEW.value,
            "filled_quantity": filled_quantity,
            "fill_price": fill_price,
            "fees": fees,
            "timestamp": time.time()
        }
        
        self.orders[exchange_order_id] = order_record
        
        # Return result
        return {
            "success": True,
            "message": "Order submitted successfully",
            "exchange_order_id": exchange_order_id,
            "execution_id": str(uuid.uuid4()),
            "filled_quantity": filled_quantity,
            "fill_price": fill_price,
            "fees": fees,
            "status": order_record["status"]
        }
    
    def cancel_order(self, order: Order) -> Dict[str, Any]:
        """
        Cancel an order on the exchange.
        
        Args:
            order: Order to cancel
            
        Returns:
            Dictionary containing cancellation result
        """
        # Simulate cancellation latency
        time.sleep(self.latency)
        
        if not order.exchange_order_id:
            return {
                "success": False,
                "message": "No exchange order ID provided"
            }
        
        if order.exchange_order_id not in self.orders:
            return {
                "success": False,
                "message": f"Order with ID {order.exchange_order_id} not found"
            }
        
        order_record = self.orders[order.exchange_order_id]
        
        # Check if order can be cancelled
        if order_record["status"] == OrderStatus.FILLED.value:
            return {
                "success": False,
                "message": "Cannot cancel filled order"
            }
        
        if order_record["status"] == OrderStatus.CANCELED.value:
            return {
                "success": False,
                "message": "Order already cancelled"
            }
        
        # Update order status
        order_record["status"] = OrderStatus.CANCELED.value
        order_record["timestamp"] = time.time()
        
        return {
            "success": True,
            "message": "Order cancelled successfully"
        }
    
    def check_order_status(self, order: Order) -> Dict[str, Any]:
        """
        Check the status of an order on the exchange.
        
        Args:
            order: Order to check
            
        Returns:
            Dictionary containing order status
        """
        # Simulate status check latency
        time.sleep(self.latency)
        
        if not order.exchange_order_id:
            return {
                "success": False,
                "message": "No exchange order ID provided"
            }
        
        if order.exchange_order_id not in self.orders:
            return {
                "success": False,
                "message": f"Order with ID {order.exchange_order_id} not found",
                "exists": False
            }
        
        order_record = self.orders[order.exchange_order_id]
        
        # For demonstration purposes, randomly fill limit orders over time
        if (
            order_record["status"] == OrderStatus.NEW.value
            and order_record["order_type"] == OrderType.LIMIT.value
            and time.time() - order_record["timestamp"] > 5  # After 5 seconds
            and not order_record["filled_quantity"]
        ):
            # 50% chance to fill
            if time.random() < 0.5:
                order_record["filled_quantity"] = order_record["quantity"]
                order_record["fill_price"] = order_record["price"]
                order_record["fees"] = self._calculate_fees(
                    order, order_record["filled_quantity"], order_record["fill_price"]
                )
                order_record["status"] = OrderStatus.FILLED.value
        
        return {
            "success": True,
            "message": "Order status retrieved",
            "exists": True,
            "status": order_record["status"],
            "filled_quantity": order_record["filled_quantity"],
            "fill_price": order_record["fill_price"],
            "fees": order_record["fees"]
        }
    
    def get_market_data(self, symbol: str, data_type: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Get market data from the exchange.
        
        Args:
            symbol: Symbol to get data for
            data_type: Type of data to get (e.g., "ticker", "orderbook", "trades")
            params: Additional parameters for the data request
            
        Returns:
            Dictionary containing market data
        """
        # Simulate data retrieval latency
        time.sleep(self.latency)
        
        if symbol not in self.market_data:
            return {
                "success": False,
                "message": f"Symbol {symbol} not found",
                "data": None,
                "timestamp": time.time()
            }
        
        data = self.market_data[symbol]
        
        # Add a small random price movement
        price_change = data["last"] * 0.001 * (2 * time.random() - 1)
        data["last"] += price_change
        data["bid"] = data["last"] - data["last"] * 0.001
        data["ask"] = data["last"] + data["last"] * 0.001
        data["timestamp"] = time.time()
        
        # Increase volume
        data["volume"] += data["volume"] * 0.01 * time.random()
        
        if data_type == "ticker":
            return {
                "success": True,
                "message": "Ticker data retrieved",
                "data": data,
                "timestamp": time.time()
            }
        elif data_type == "orderbook":
            # Generate a simple order book
            book = {
                "bids": [
                    [data["bid"], 1.0],
                    [data["bid"] - data["bid"] * 0.001, 2.0],
                    [data["bid"] - data["bid"] * 0.002, 3.0]
                ],
                "asks": [
                    [data["ask"], 1.0],
                    [data["ask"] + data["ask"] * 0.001, 2.0],
                    [data["ask"] + data["ask"] * 0.002, 3.0]
                ]
            }
            return {
                "success": True,
                "message": "Order book data retrieved",
                "data": book,
                "timestamp": time.time()
            }
        elif data_type == "trades":
            # Generate some fake trades
            trades = []
            for i in range(5):
                trades.append({
                    "id": str(uuid.uuid4()),
                    "price": data["last"] + data["last"] * 0.001 * (2 * time.random() - 1),
                    "quantity": time.random() * 2,
                    "side": "buy" if time.random() > 0.5 else "sell",
                    "timestamp": time.time() - i * 10
                })
            return {
                "success": True,
                "message": "Trades data retrieved",
                "data": trades,
                "timestamp": time.time()
            }
        else:
            return {
                "success": False,
                "message": f"Unsupported data type: {data_type}",
                "data": None,
                "timestamp": time.time()
            }
    
    def get_account_balance(self) -> Dict[str, Any]:
        """
        Get account balance from the exchange.
        
        Returns:
            Dictionary containing account balance
        """
        # Simulate data retrieval latency
        time.sleep(self.latency)
        
        return {
            "success": True,
            "message": "Account balance retrieved",
            "balances": self.balances,
            "timestamp": time.time()
        }
    
    def get_positions(self) -> Dict[str, Any]:
        """
        Get current positions from the exchange.
        
        Returns:
            Dictionary containing positions
        """
        # Simulate data retrieval latency
        time.sleep(self.latency)
        
        positions = []
        for symbol, position in self.positions.items():
            positions.append({
                "symbol": symbol,
                "quantity": position["quantity"],
                "entry_price": position["entry_price"],
                "liquidation_price": position["liquidation_price"],
                "unrealized_pnl": position["unrealized_pnl"],
                "leverage": position["leverage"]
            })
        
        return {
            "success": True,
            "message": "Positions retrieved",
            "positions": positions,
            "timestamp": time.time()
        }
    
    def _calculate_fees(self, order: Order, quantity: float = None, price: float = None) -> float:
        """
        Calculate fees for an order.
        
        Args:
            order: Order to calculate fees for
            quantity: Quantity for fee calculation
            price: Price for fee calculation
            
        Returns:
            Fee amount
        """
        quantity = quantity if quantity is not None else order.quantity
        if price is None:
            if order.price:
                price = order.price
            else:
                price = self.market_data.get(order.symbol, {}).get("last", 0)
        
        # Use flat 0.1% fee for simplicity
        return quantity * price * 0.001 