"""
Order Model

This module defines the Order class which represents a trading order.
"""

import uuid
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, Any, Optional, List


class OrderType(Enum):
    """Enum representing different order types."""
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    TRAILING_STOP = "trailing_stop"


class OrderSide(Enum):
    """Enum representing order sides (buy/sell)."""
    BUY = "buy"
    SELL = "sell"


class OrderStatus(Enum):
    """Enum representing the status of an order."""
    CREATED = "created"  # Initial state when order is created
    VALIDATED = "validated"  # Order has been validated
    PENDING = "pending"  # Order has been submitted but not confirmed
    ACCEPTED = "accepted"  # Order has been accepted by the exchange
    PARTIAL_FILLED = "partial_filled"  # Order has been partially filled
    FILLED = "filled"  # Order has been completely filled
    CANCELED = "canceled"  # Order has been canceled
    REJECTED = "rejected"  # Order has been rejected
    EXPIRED = "expired"  # Order has expired
    ERROR = "error"  # An error occurred during order processing


class OrderTimeInForce(Enum):
    """Enum representing the time-in-force options for an order."""
    GTC = "gtc"  # Good Till Canceled
    IOC = "ioc"  # Immediate Or Cancel
    FOK = "fok"  # Fill Or Kill
    DAY = "day"  # Day Order


@dataclass
class Order:
    """
    Represents a trading order.
    
    Attributes:
        symbol (str): Trading symbol (e.g., "BTC/USD")
        order_type (OrderType): Type of order
        side (OrderSide): Buy or sell
        quantity (float): Quantity to buy/sell
        price (Optional[float]): Price for limit orders
        stop_price (Optional[float]): Stop price for stop orders
        time_in_force (OrderTimeInForce): Time in force option
        status (OrderStatus): Current status of the order
        exchange_id (str): ID of the exchange this order is for
        client_order_id (str): Client-generated order ID
        exchange_order_id (Optional[str]): Exchange-assigned order ID
        execution_id (Optional[str]): ID of the execution
        filled_quantity (float): Quantity that has been filled
        average_fill_price (Optional[float]): Average fill price
        timestamp_created (float): Timestamp when order was created
        timestamp_updated (float): Timestamp when order was last updated
        timestamp_submitted (Optional[float]): Timestamp when order was submitted
        timestamp_executed (Optional[float]): Timestamp when order was executed
        fees (float): Fees paid for this order
        metadata (Dict[str, Any]): Additional metadata for the order
    """
    
    # Required fields
    symbol: str
    order_type: OrderType
    side: OrderSide
    quantity: float
    exchange_id: str
    
    # Optional fields with defaults
    price: Optional[float] = None
    stop_price: Optional[float] = None
    time_in_force: OrderTimeInForce = OrderTimeInForce.GTC
    status: OrderStatus = OrderStatus.CREATED
    client_order_id: str = field(default_factory=lambda: f"order_{uuid.uuid4().hex}")
    exchange_order_id: Optional[str] = None
    execution_id: Optional[str] = None
    filled_quantity: float = 0.0
    average_fill_price: Optional[float] = None
    timestamp_created: float = field(default_factory=time.time)
    timestamp_updated: float = field(default_factory=time.time)
    timestamp_submitted: Optional[float] = None
    timestamp_executed: Optional[float] = None
    fees: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate the order after initialization."""
        # Ensure order_type is an OrderType enum
        if isinstance(self.order_type, str):
            self.order_type = OrderType(self.order_type)
        
        # Ensure side is an OrderSide enum
        if isinstance(self.side, str):
            self.side = OrderSide(self.side)
        
        # Ensure status is an OrderStatus enum
        if isinstance(self.status, str):
            self.status = OrderStatus(self.status)
        
        # Ensure time_in_force is an OrderTimeInForce enum
        if isinstance(self.time_in_force, str):
            self.time_in_force = OrderTimeInForce(self.time_in_force)
        
        # Price is required for limit orders
        if self.order_type in [OrderType.LIMIT, OrderType.STOP_LIMIT] and self.price is None:
            raise ValueError(f"Price is required for {self.order_type.value} orders")
        
        # Stop price is required for stop orders
        if self.order_type in [OrderType.STOP, OrderType.STOP_LIMIT] and self.stop_price is None:
            raise ValueError(f"Stop price is required for {self.order_type.value} orders")

    def update_status(self, status: OrderStatus, timestamp: Optional[float] = None):
        """
        Update the order status.
        
        Args:
            status: New status
            timestamp: Timestamp of the update (defaults to current time)
        """
        self.status = status
        self.timestamp_updated = timestamp or time.time()
        
        # Set specific timestamps based on status
        if status == OrderStatus.PENDING and not self.timestamp_submitted:
            self.timestamp_submitted = self.timestamp_updated
        elif status in [OrderStatus.FILLED, OrderStatus.PARTIAL_FILLED] and not self.timestamp_executed:
            self.timestamp_executed = self.timestamp_updated

    def update_fill(self, filled_quantity: float, fill_price: float, fees: float = 0.0):
        """
        Update order with fill information.
        
        Args:
            filled_quantity: Additional quantity filled
            fill_price: Price at which the order was filled
            fees: Fees for this fill
        """
        # Calculate new average fill price
        if self.filled_quantity > 0:
            total_value = self.filled_quantity * (self.average_fill_price or 0) + filled_quantity * fill_price
            self.average_fill_price = total_value / (self.filled_quantity + filled_quantity)
        else:
            self.average_fill_price = fill_price
        
        # Update filled quantity and fees
        self.filled_quantity += filled_quantity
        self.fees += fees
        
        # Update status based on fill
        if self.filled_quantity >= self.quantity:
            self.update_status(OrderStatus.FILLED)
        elif self.filled_quantity > 0:
            self.update_status(OrderStatus.PARTIAL_FILLED)

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert order to dictionary.
        
        Returns:
            Dictionary representation of the order
        """
        return {
            "symbol": self.symbol,
            "order_type": self.order_type.value,
            "side": self.side.value,
            "quantity": self.quantity,
            "price": self.price,
            "stop_price": self.stop_price,
            "time_in_force": self.time_in_force.value,
            "status": self.status.value,
            "exchange_id": self.exchange_id,
            "client_order_id": self.client_order_id,
            "exchange_order_id": self.exchange_order_id,
            "execution_id": self.execution_id,
            "filled_quantity": self.filled_quantity,
            "average_fill_price": self.average_fill_price,
            "timestamp_created": self.timestamp_created,
            "timestamp_updated": self.timestamp_updated,
            "timestamp_submitted": self.timestamp_submitted,
            "timestamp_executed": self.timestamp_executed,
            "fees": self.fees,
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Order':
        """
        Create an order from a dictionary.
        
        Args:
            data: Dictionary representation of the order
            
        Returns:
            Order instance
        """
        # Convert string enums to enum objects
        if "order_type" in data and isinstance(data["order_type"], str):
            data["order_type"] = OrderType(data["order_type"])
        
        if "side" in data and isinstance(data["side"], str):
            data["side"] = OrderSide(data["side"])
        
        if "status" in data and isinstance(data["status"], str):
            data["status"] = OrderStatus(data["status"])
        
        if "time_in_force" in data and isinstance(data["time_in_force"], str):
            data["time_in_force"] = OrderTimeInForce(data["time_in_force"])
        
        return cls(**data)


class OrderBook:
    """
    Order book for tracking orders.
    """
    
    def __init__(self):
        """Initialize order book."""
        self.orders: Dict[str, Order] = {}
    
    def add_order(self, order: Order) -> None:
        """
        Add order to order book.
        
        Args:
            order: Order to add
        """
        self.orders[order.client_order_id] = order
    
    def get_order(self, client_order_id: str) -> Optional[Order]:
        """
        Get order by client order ID.
        
        Args:
            client_order_id: Client-generated order ID
            
        Returns:
            Order or None if not found
        """
        return self.orders.get(client_order_id)
    
    def update_order(self, order: Order) -> None:
        """
        Update order in order book.
        
        Args:
            order: Order to update
        """
        if order.client_order_id in self.orders:
            self.orders[order.client_order_id] = order
    
    def remove_order(self, client_order_id: str) -> Optional[Order]:
        """
        Remove order from order book.
        
        Args:
            client_order_id: Client-generated order ID
            
        Returns:
            Removed order or None if not found
        """
        return self.orders.pop(client_order_id, None)
    
    def get_all_orders(self) -> List[Order]:
        """
        Get all orders.
        
        Returns:
            List of orders
        """
        return list(self.orders.values())
    
    def get_active_orders(self) -> List[Order]:
        """
        Get active orders.
        
        Returns:
            List of active orders
        """
        return [order for order in self.orders.values() 
                if order.status not in [OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED]]
    
    def get_orders_by_status(self, status: OrderStatus) -> List[Order]:
        """
        Get orders by status.
        
        Args:
            status: Order status
            
        Returns:
            List of orders with the given status
        """
        return [order for order in self.orders.values() if order.status == status]
    
    def get_orders_by_symbol(self, symbol: str) -> List[Order]:
        """
        Get orders by symbol.
        
        Args:
            symbol: Symbol
            
        Returns:
            List of orders for the given symbol
        """
        return [order for order in self.orders.values() if order.symbol == symbol]
    
    def get_orders_by_exchange(self, exchange_id: str) -> List[Order]:
        """
        Get orders by exchange.
        
        Args:
            exchange_id: Exchange ID
            
        Returns:
            List of orders for the given exchange
        """
        return [order for order in self.orders.values() if order.exchange_id == exchange_id]
    
    def clear(self) -> None:
        """Clear all orders."""
        self.orders.clear()
    
    def size(self) -> int:
        """
        Get number of orders.
        
        Returns:
            Number of orders
        """
        return len(self.orders) 