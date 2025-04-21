"""
Order Management Module

This module implements the OrderManager class which manages orders for the execution system.
"""

import logging
import time
from enum import Enum, auto
from typing import Dict, List, Optional, Any, Union
import uuid

from trading_system.models.order import Order, OrderStatus, OrderBook

logger = logging.getLogger(__name__)


class OrderType(Enum):
    """
    Order types supported by the execution engine.
    
    Extends the basic types with more advanced types.
    """
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    TRAILING_STOP = "trailing_stop"
    OCO = "oco"  # One-Cancels-Other
    ICEBERG = "iceberg"
    TWAP = "twap"
    VWAP = "vwap"
    PEG = "peg"
    TAKE_PROFIT = "take_profit"
    TAKE_PROFIT_LIMIT = "take_profit_limit"


class OrderTimeInForce(Enum):
    """
    Time in force options for orders.
    """
    GTC = "gtc"  # Good Till Cancelled
    IOC = "ioc"  # Immediate Or Cancel
    FOK = "fok"  # Fill Or Kill
    DAY = "day"  # Day Order
    GTD = "gtd"  # Good Till Date
    AT_THE_OPENING = "at_the_opening"
    AT_THE_CLOSE = "at_the_close"
    GTC_PO = "gtc_po"  # Good Till Canceled, Post Only


class OrderSide(Enum):
    """
    Order side.
    """
    BUY = "buy"
    SELL = "sell"


class Fill:
    """
    Represents a fill (execution) of an order.
    """
    
    def __init__(
        self,
        fill_id: str,
        order_id: str,
        symbol: str,
        price: float,
        quantity: float,
        timestamp: float = None,
        fee: float = 0.0,
        fee_currency: str = None,
        trade_id: str = None
    ):
        """
        Initialize fill.
        
        Args:
            fill_id: Unique fill identifier
            order_id: Order identifier
            symbol: Symbol for the fill
            price: Fill price
            quantity: Fill quantity
            timestamp: Fill timestamp
            fee: Fee amount
            fee_currency: Fee currency
            trade_id: Exchange trade ID
        """
        self.fill_id = fill_id
        self.order_id = order_id
        self.symbol = symbol
        self.price = price
        self.quantity = quantity
        self.timestamp = timestamp or time.time()
        self.fee = fee
        self.fee_currency = fee_currency
        self.trade_id = trade_id
        
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert fill to dictionary.
        
        Returns:
            Fill as dictionary
        """
        return {
            "fill_id": self.fill_id,
            "order_id": self.order_id,
            "symbol": self.symbol,
            "price": self.price,
            "quantity": self.quantity,
            "timestamp": self.timestamp,
            "fee": self.fee,
            "fee_currency": self.fee_currency,
            "trade_id": self.trade_id
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Fill':
        """
        Create fill from dictionary.
        
        Args:
            data: Fill data
            
        Returns:
            Fill instance
        """
        return cls(
            fill_id=data["fill_id"],
            order_id=data["order_id"],
            symbol=data["symbol"],
            price=data["price"],
            quantity=data["quantity"],
            timestamp=data.get("timestamp"),
            fee=data.get("fee", 0.0),
            fee_currency=data.get("fee_currency"),
            trade_id=data.get("trade_id")
        )


class ExtendedOrder:
    """
    Extended order class with additional functionality.
    
    Wraps the base Order class with additional execution-related functionality.
    """
    
    def __init__(self, order: Order):
        """
        Initialize extended order.
        
        Args:
            order: Base order to wrap
        """
        self.order = order
        self.fills: List[Fill] = []
        self.execution_reports: List[Dict[str, Any]] = []
        self.execution_start_time: Optional[float] = None
        self.execution_end_time: Optional[float] = None
        self.execution_latency: Optional[float] = None
        self.validation_result: Optional[Dict[str, Any]] = None
        self.execution_result: Optional[Dict[str, Any]] = None
        
    def add_fill(self, fill: Fill) -> None:
        """
        Add a fill to the order.
        
        Args:
            fill: Fill to add
        """
        self.fills.append(fill)
        
        # Update the base order
        self.order.update_fill(fill.quantity, fill.price, fill.fee)
        
    def add_execution_report(self, report: Dict[str, Any]) -> None:
        """
        Add execution report to the order.
        
        Args:
            report: Execution report
        """
        self.execution_reports.append(report)
        
    def calculate_execution_latency(self) -> Optional[float]:
        """
        Calculate execution latency.
        
        Returns:
            Execution latency in milliseconds, or None if not available
        """
        if self.execution_start_time and self.execution_end_time:
            self.execution_latency = (self.execution_end_time - self.execution_start_time) * 1000
            return self.execution_latency
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert extended order to dictionary.
        
        Returns:
            Extended order as dictionary
        """
        return {
            "order": self.order.to_dict(),
            "fills": [fill.to_dict() for fill in self.fills],
            "execution_reports": self.execution_reports,
            "execution_start_time": self.execution_start_time,
            "execution_end_time": self.execution_end_time,
            "execution_latency": self.execution_latency,
            "validation_result": self.validation_result,
            "execution_result": self.execution_result
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ExtendedOrder':
        """
        Create extended order from dictionary.
        
        Args:
            data: Extended order data
            
        Returns:
            ExtendedOrder instance
        """
        order = Order.from_dict(data["order"])
        extended_order = cls(order)
        extended_order.fills = [Fill.from_dict(fill_data) for fill_data in data.get("fills", [])]
        extended_order.execution_reports = data.get("execution_reports", [])
        extended_order.execution_start_time = data.get("execution_start_time")
        extended_order.execution_end_time = data.get("execution_end_time")
        extended_order.execution_latency = data.get("execution_latency")
        extended_order.validation_result = data.get("validation_result")
        extended_order.execution_result = data.get("execution_result")
        return extended_order
    
    @property
    def total_fees(self) -> float:
        """
        Calculate total fees for this order.
        
        Returns:
            Total fees
        """
        return sum(fill.fee for fill in self.fills)


class OrderManager:
    """
    Manages orders for the execution system.
    
    The OrderManager is responsible for storing orders, updating their status,
    and tracking orders throughout their lifecycle.
    """
    
    def __init__(self):
        """Initialize the order manager."""
        self.order_book = OrderBook()
        self.order_history = {}  # Store history of order updates
    
    def add_order(self, order: Order) -> bool:
        """
        Add an order to the order manager.
        
        Args:
            order: Order to add
            
        Returns:
            True if the order was added successfully, False otherwise
        """
        try:
            self.order_book.add_order(order)
            self.order_history[order.client_order_id] = [{
                "timestamp": time.time(),
                "status": order.status.value,
                "filled_quantity": order.filled_quantity,
                "average_fill_price": order.average_fill_price
            }]
            logger.info(f"Added order {order.client_order_id} to order manager")
            return True
        except Exception as e:
            logger.error(f"Error adding order {order.client_order_id}: {str(e)}", exc_info=True)
            return False
    
    def get_order(self, client_order_id: str) -> Optional[Order]:
        """
        Get an order by its client order ID.
        
        Args:
            client_order_id: Client order ID
            
        Returns:
            Order if found, None otherwise
        """
        return self.order_book.get_order(client_order_id)
    
    def update_order_status(self, client_order_id: str, status: OrderStatus, 
                           filled_quantity: Optional[float] = None,
                           fill_price: Optional[float] = None,
                           fees: Optional[float] = None) -> bool:
        """
        Update the status of an order.
        
        Args:
            client_order_id: Client order ID
            status: New order status
            filled_quantity: Optional filled quantity
            fill_price: Optional fill price
            fees: Optional fees
            
        Returns:
            True if the order was updated successfully, False otherwise
        """
        order = self.get_order(client_order_id)
        if not order:
            logger.warning(f"Order {client_order_id} not found")
            return False
        
        # Update order status
        order.update_status(status)
        
        # Update fill information if provided
        if filled_quantity is not None and fill_price is not None:
            order.update_fill(filled_quantity, fill_price, fees or 0.0)
        
        # Update order in order book
        self.order_book.update_order(order)
        
        # Add to order history
        if client_order_id in self.order_history:
            self.order_history[client_order_id].append({
                "timestamp": time.time(),
                "status": order.status.value,
                "filled_quantity": order.filled_quantity,
                "average_fill_price": order.average_fill_price,
                "fees": order.fees
            })
        
        logger.info(f"Updated order {client_order_id} status to {status.value}")
        return True
    
    def remove_order(self, client_order_id: str) -> bool:
        """
        Remove an order from the active orders.
        
        Args:
            client_order_id: Client order ID
            
        Returns:
            True if the order was removed, False otherwise
        """
        order = self.order_book.remove_order(client_order_id)
        if order:
            logger.info(f"Removed order {client_order_id} from active orders")
            return True
        return False
    
    def get_active_orders(self) -> List[Order]:
        """
        Get all active orders.
        
        Returns:
            List of active orders
        """
        return self.order_book.get_active_orders()
    
    def get_orders_by_status(self, status: OrderStatus) -> List[Order]:
        """
        Get orders by status.
        
        Args:
            status: Order status
            
        Returns:
            List of orders with the given status
        """
        return self.order_book.get_orders_by_status(status)
    
    def get_orders_by_symbol(self, symbol: str) -> List[Order]:
        """
        Get orders by symbol.
        
        Args:
            symbol: Symbol
            
        Returns:
            List of orders for the given symbol
        """
        return self.order_book.get_orders_by_symbol(symbol)
    
    def get_orders_by_exchange(self, exchange_id: str) -> List[Order]:
        """
        Get orders by exchange.
        
        Args:
            exchange_id: Exchange ID
            
        Returns:
            List of orders for the given exchange
        """
        return self.order_book.get_orders_by_exchange(exchange_id)
    
    def get_order_history(self, client_order_id: str) -> List[Dict[str, Any]]:
        """
        Get the history of an order.
        
        Args:
            client_order_id: Client order ID
            
        Returns:
            List of order history entries
        """
        return self.order_history.get(client_order_id, [])
    
    def clear_inactive_orders(self) -> int:
        """
        Clear inactive orders from the order book.
        
        Returns:
            Number of orders cleared
        """
        # Get orders that are in terminal states
        terminal_states = [
            OrderStatus.FILLED, OrderStatus.CANCELED, 
            OrderStatus.REJECTED, OrderStatus.ERROR,
            OrderStatus.EXPIRED
        ]
        
        inactive_orders = []
        for status in terminal_states:
            inactive_orders.extend(self.get_orders_by_status(status))
        
        # Remove inactive orders
        count = 0
        for order in inactive_orders:
            if self.remove_order(order.client_order_id):
                count += 1
        
        logger.info(f"Cleared {count} inactive orders")
        return count 