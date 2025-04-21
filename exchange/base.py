"""
Exchange Base Module

This module defines the base interfaces and classes for exchange integration.
"""

from enum import Enum
from typing import Dict, List, Any, Optional, Union
from datetime import datetime

from trading_system.core.component import Component, ComponentStatus


class OrderSide(Enum):
    """Order side enumeration (buy/sell)."""
    BUY = "buy"
    SELL = "sell"


class OrderType(Enum):
    """Order type enumeration."""
    MARKET = "market"
    LIMIT = "limit"
    STOP_LOSS = "stop_loss"
    TAKE_PROFIT = "take_profit"
    STOP_LOSS_LIMIT = "stop_loss_limit"
    TAKE_PROFIT_LIMIT = "take_profit_limit"
    TRAILING_STOP = "trailing_stop"


class OrderStatus(Enum):
    """Order status enumeration."""
    NEW = "new"
    CREATED = "created"
    VALIDATING = "validating"
    READY = "ready"
    SUBMITTING = "submitting"
    SUBMITTED = "submitted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"
    FAILED = "failed"
    EXPIRED = "expired"


class OrderTimeInForce(Enum):
    """Order time in force enumeration."""
    GTC = "gtc"  # Good Till Canceled
    IOC = "ioc"  # Immediate Or Cancel
    FOK = "fok"  # Fill Or Kill


class Order:
    """
    Order model representing an exchange order.
    """
    
    def __init__(
        self, 
        order_id: str,
        symbol: str,
        order_type: OrderType,
        side: OrderSide,
        amount: float,
        price: Optional[float] = None,
        status: OrderStatus = OrderStatus.NEW,
        timestamp: Optional[int] = None,
        filled: float = 0.0,
        remaining: Optional[float] = None,
        average_price: Optional[float] = None,
        last_price: Optional[float] = None,
        params: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize an order.
        
        Args:
            order_id: Unique order ID from the exchange
            symbol: Trading pair symbol
            order_type: Order type
            side: Order side (buy/sell)
            amount: Order amount
            price: Order price (optional for market orders)
            status: Order status
            timestamp: Order creation timestamp (milliseconds)
            filled: Amount already filled
            remaining: Amount remaining to be filled
            average_price: Average fill price
            last_price: Last fill price
            params: Additional order parameters
        """
        self.order_id = order_id
        self.symbol = symbol
        self.order_type = order_type
        self.side = side
        self.amount = amount
        self.price = price
        self.status = status
        self.timestamp = timestamp or int(datetime.now().timestamp() * 1000)
        self.filled = filled
        self.remaining = remaining if remaining is not None else amount
        self.average_price = average_price
        self.last_price = last_price
        self.params = params or {}
        
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert order to dictionary representation.
        
        Returns:
            Dict: Order data
        """
        return {
            "order_id": self.order_id,
            "symbol": self.symbol,
            "order_type": self.order_type.value,
            "side": self.side.value,
            "amount": self.amount,
            "price": self.price,
            "status": self.status.value,
            "timestamp": self.timestamp,
            "filled": self.filled,
            "remaining": self.remaining,
            "average_price": self.average_price,
            "last_price": self.last_price,
            "params": self.params,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Order':
        """
        Create an order from dictionary data.
        
        Args:
            data: Order data dictionary
            
        Returns:
            Order: New order instance
        """
        return cls(
            order_id=data["order_id"],
            symbol=data["symbol"],
            order_type=OrderType(data["order_type"]),
            side=OrderSide(data["side"]),
            amount=data["amount"],
            price=data.get("price"),
            status=OrderStatus(data["status"]),
            timestamp=data.get("timestamp"),
            filled=data.get("filled", 0.0),
            remaining=data.get("remaining"),
            average_price=data.get("average_price"),
            last_price=data.get("last_price"),
            params=data.get("params", {})
        )


class ExchangeBase(Component):
    """Base class for exchange adapters."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the exchange adapter.
        
        Args:
            config: Exchange configuration
        """
        super().__init__(name="exchange", config=config or {})
        self.name = config.get("name", "unknown") if config else "unknown"
        self._status = ComponentStatus.CREATED
    
    def start(self) -> bool:
        """
        Start the exchange adapter.
        
        Returns:
            Success flag
        """
        self._update_status(ComponentStatus.OPERATIONAL)
        return True
    
    def stop(self) -> bool:
        """
        Stop the exchange adapter.
        
        Returns:
            Success flag
        """
        self._update_status(ComponentStatus.SHUTDOWN)
        return True
    
    def submit_order(self, order: Order) -> str:
        """
        Submit an order to the exchange.
        
        Args:
            order: Order to submit
            
        Returns:
            Exchange order ID
        """
        raise NotImplementedError("submit_order method must be implemented by subclass")
    
    def cancel_order(self, order_id: str) -> bool:
        """
        Cancel an order on the exchange.
        
        Args:
            order_id: Order ID to cancel
            
        Returns:
            Success flag
        """
        raise NotImplementedError("cancel_order method must be implemented by subclass")
    
    def get_order_status(self, order_id: str) -> Optional[Order]:
        """
        Get the status of an order from the exchange.
        
        Args:
            order_id: Order ID to check
            
        Returns:
            Order object or None if not found
        """
        raise NotImplementedError("get_order_status method must be implemented by subclass")
    
    def get_account_balances(self) -> Dict[str, float]:
        """
        Get account balances from the exchange.
        
        Returns:
            Account balances
        """
        raise NotImplementedError("get_account_balances method must be implemented by subclass")
    
    def get_open_positions(self) -> List[Dict[str, Any]]:
        """
        Get open positions from the exchange.
        
        Returns:
            List of open positions
        """
        raise NotImplementedError("get_open_positions method must be implemented by subclass")


class ExchangeManager:
    """Manager for exchange adapters."""
    
    def __init__(self):
        """Initialize the exchange manager."""
        self.exchanges = {}
        self._status = ComponentStatus.INITIALIZED
    
    def register_exchange(self, exchange: ExchangeBase) -> None:
        """
        Register an exchange adapter.
        
        Args:
            exchange: Exchange adapter to register
        """
        self.exchanges[exchange.name] = exchange
    
    def get_exchange(self, name: str) -> Optional[ExchangeBase]:
        """
        Get an exchange adapter by name.
        
        Args:
            name: Exchange name
            
        Returns:
            Exchange adapter or None if not found
        """
        return self.exchanges.get(name)


# Global instance of the exchange manager
exchange_manager = ExchangeManager() 