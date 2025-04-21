"""
Mock Order Module

This module provides mock order types and utilities for integration testing.
"""

import datetime
import uuid
from enum import Enum
from typing import Dict, List, Any, Optional


class OrderType(Enum):
    """Order type enumeration"""
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"


class OrderSide(Enum):
    """Order side enumeration"""
    BUY = "buy"
    SELL = "sell"


class OrderStatus(Enum):
    """Order status enumeration"""
    CREATED = "created"
    VALIDATING = "validating"
    VALIDATED = "validated"
    REJECTED = "rejected"
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    ERROR = "error"


class TimeInForce(Enum):
    """Time in force enumeration"""
    GTC = "gtc"  # Good Till Canceled
    IOC = "ioc"  # Immediate Or Cancel
    FOK = "fok"  # Fill Or Kill


class OrderFill:
    """Order fill data class"""
    
    def __init__(self, timestamp: datetime.datetime, quantity: float, price: float, fee: float = 0.0):
        """
        Initialize an order fill.
        
        Args:
            timestamp: Fill timestamp
            quantity: Fill quantity
            price: Fill price
            fee: Trading fee
        """
        self.timestamp = timestamp
        self.quantity = quantity
        self.price = price
        self.fee = fee
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "quantity": self.quantity,
            "price": self.price,
            "fee": self.fee
        }


class Order:
    """Mock order class for testing"""
    
    def __init__(
        self,
        symbol: str,
        side: OrderSide,
        type: OrderType,
        quantity: float,
        price: Optional[float] = None,
        time_in_force: TimeInForce = TimeInForce.GTC,
        strategy_id: Optional[str] = None,
        signal_id: Optional[str] = None,
        order_id: Optional[str] = None
    ):
        """
        Initialize an order.
        
        Args:
            symbol: Instrument symbol
            side: Order side (buy/sell)
            type: Order type (market/limit/etc)
            quantity: Order quantity
            price: Order price (required for limit orders)
            time_in_force: Time in force
            strategy_id: Strategy ID
            signal_id: Signal ID
            order_id: Order ID (generated if not provided)
        """
        self.id = order_id or f"order-{uuid.uuid4()}"
        self.symbol = symbol
        self.side = side
        self.type = type
        self.quantity = quantity
        self.price = price
        self.time_in_force = time_in_force
        self.strategy_id = strategy_id
        self.signal_id = signal_id
        
        # Order state
        self.status = OrderStatus.CREATED
        self.created_at = datetime.datetime.now()
        self.updated_at = self.created_at
        self.fills = []
        self.filled_quantity = 0
        self.filled_price = None
        self.error_message = None
        
        # Exchange-specific fields
        self.exchange_order_id = None
    
    def update_status(self, status: OrderStatus, message: Optional[str] = None):
        """
        Update the order status.
        
        Args:
            status: New status
            message: Optional status message
        """
        self.status = status
        self.updated_at = datetime.datetime.now()
        
        if message:
            self.error_message = message
    
    def add_fill(self, fill: OrderFill):
        """
        Add a fill to the order.
        
        Args:
            fill: Order fill
        """
        self.fills.append(fill)
        self.filled_quantity += fill.quantity
        
        # Calculate average filled price
        total_value = sum(f.price * f.quantity for f in self.fills)
        self.filled_price = total_value / self.filled_quantity if self.filled_quantity > 0 else None
        
        # Update status based on fill
        if self.filled_quantity >= self.quantity:
            self.status = OrderStatus.FILLED
        elif self.filled_quantity > 0:
            self.status = OrderStatus.PARTIALLY_FILLED
        
        self.updated_at = datetime.datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert order to dictionary."""
        return {
            "id": self.id,
            "symbol": self.symbol,
            "side": self.side.value,
            "type": self.type.value,
            "quantity": self.quantity,
            "price": self.price,
            "time_in_force": self.time_in_force.value,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "filled_quantity": self.filled_quantity,
            "filled_price": self.filled_price,
            "strategy_id": self.strategy_id,
            "signal_id": self.signal_id,
            "error_message": self.error_message,
            "exchange_order_id": self.exchange_order_id,
            "fills": [fill.to_dict() for fill in self.fills]
        } 