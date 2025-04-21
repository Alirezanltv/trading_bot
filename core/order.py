"""
Order management module for the trading system.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional, Dict, Any

class OrderType(Enum):
    """Types of orders that can be placed."""
    MARKET = "market"
    LIMIT = "limit"
    STOP_LOSS = "stop_loss"
    TAKE_PROFIT = "take_profit"

class OrderSide(Enum):
    """Sides of an order (buy/sell)."""
    BUY = "buy"
    SELL = "sell"

class OrderStatus(Enum):
    """Possible states of an order."""
    PENDING = "pending"
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"

@dataclass
class Order:
    """Represents a trading order in the system."""
    
    # Required fields
    order_id: str  # Unique identifier for the order
    symbol: str  # Trading pair symbol
    order_type: OrderType  # Type of order
    side: OrderSide  # Buy or sell
    quantity: Decimal  # Order quantity
    
    # Optional fields
    price: Optional[Decimal] = None  # Limit price for limit orders
    stop_price: Optional[Decimal] = None  # Stop price for stop orders
    status: OrderStatus = OrderStatus.PENDING  # Current order status
    filled_quantity: Decimal = Decimal('0')  # Amount filled so far
    average_fill_price: Optional[Decimal] = None  # Average price of fills
    created_at: datetime = datetime.now()  # Order creation timestamp
    updated_at: datetime = datetime.now()  # Last update timestamp
    expires_at: Optional[datetime] = None  # Order expiration time
    client_order_id: Optional[str] = None  # Client-provided order ID
    exchange_order_id: Optional[str] = None  # Exchange-provided order ID
    metadata: Dict[str, Any] = None  # Additional order metadata
    
    def __post_init__(self):
        """Initialize default values and validate order."""
        if self.metadata is None:
            self.metadata = {}
            
        # Validate order parameters
        if self.order_type == OrderType.LIMIT and self.price is None:
            raise ValueError("Limit orders must specify a price")
            
        if self.order_type in (OrderType.STOP_LOSS, OrderType.TAKE_PROFIT) and self.stop_price is None:
            raise ValueError("Stop orders must specify a stop price")
            
        if self.quantity <= 0:
            raise ValueError("Order quantity must be positive")
            
    def update_status(self, new_status: OrderStatus) -> None:
        """Update the order status and timestamp.
        
        Args:
            new_status: New status to set
        """
        self.status = new_status
        self.updated_at = datetime.now()
        
    def update_fill(self, fill_quantity: Decimal, fill_price: Decimal) -> None:
        """Update order with new fill information.
        
        Args:
            fill_quantity: Quantity filled in this update
            fill_price: Price of this fill
        """
        self.filled_quantity += fill_quantity
        
        # Update average fill price
        if self.average_fill_price is None:
            self.average_fill_price = fill_price
        else:
            # Weighted average of previous fills and new fill
            total_value = (self.filled_quantity - fill_quantity) * self.average_fill_price
            total_value += fill_quantity * fill_price
            self.average_fill_price = total_value / self.filled_quantity
            
        # Update status based on fill
        if self.filled_quantity >= self.quantity:
            self.status = OrderStatus.FILLED
        elif self.filled_quantity > 0:
            self.status = OrderStatus.PARTIALLY_FILLED
            
        self.updated_at = datetime.now()
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert order to dictionary representation.
        
        Returns:
            Dictionary containing order data
        """
        return {
            "order_id": self.order_id,
            "symbol": self.symbol,
            "order_type": self.order_type.value,
            "side": self.side.value,
            "quantity": str(self.quantity),
            "price": str(self.price) if self.price else None,
            "stop_price": str(self.stop_price) if self.stop_price else None,
            "status": self.status.value,
            "filled_quantity": str(self.filled_quantity),
            "average_fill_price": str(self.average_fill_price) if self.average_fill_price else None,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "client_order_id": self.client_order_id,
            "exchange_order_id": self.exchange_order_id,
            "metadata": self.metadata
        }
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Order':
        """Create an Order instance from a dictionary.
        
        Args:
            data: Dictionary containing order data
            
        Returns:
            Order instance
        """
        # Convert string values to appropriate types
        if "quantity" in data:
            data["quantity"] = Decimal(data["quantity"])
        if "price" in data and data["price"]:
            data["price"] = Decimal(data["price"])
        if "stop_price" in data and data["stop_price"]:
            data["stop_price"] = Decimal(data["stop_price"])
        if "filled_quantity" in data:
            data["filled_quantity"] = Decimal(data["filled_quantity"])
        if "average_fill_price" in data and data["average_fill_price"]:
            data["average_fill_price"] = Decimal(data["average_fill_price"])
            
        # Convert string enums to enum values
        if "order_type" in data:
            data["order_type"] = OrderType(data["order_type"])
        if "side" in data:
            data["side"] = OrderSide(data["side"])
        if "status" in data:
            data["status"] = OrderStatus(data["status"])
            
        # Convert ISO format strings to datetime
        for field in ["created_at", "updated_at", "expires_at"]:
            if field in data and data[field]:
                data[field] = datetime.fromisoformat(data[field])
                
        return cls(**data) 