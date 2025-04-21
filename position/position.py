"""
Position tracking for high-reliability trading system.
"""

from enum import Enum
from typing import Dict, Any, List, Optional, Union
import time
import uuid
from decimal import Decimal

from trading_system.core import get_logger

logger = get_logger("position.position")


class PositionStatus(Enum):
    """Status of a position."""
    PENDING = "pending"       # Position is pending open
    OPEN = "open"             # Position is open
    CLOSING = "closing"       # Position is closing
    CLOSED = "closed"         # Position is closed 
    CANCELLED = "cancelled"   # Position was cancelled
    ERROR = "error"           # Error occurred with position


class Position:
    """
    Represents a trading position.
    
    A position tracks an open trade from entry to exit, including:
    - Entry and exit orders
    - Stop loss and take profit levels
    - P&L tracking
    - Risk metrics
    """
    
    def __init__(self, 
                symbol: str,
                side: str,
                entry_price: float,
                size: float,
                timestamp: Optional[int] = None,
                position_id: Optional[str] = None,
                stop_loss: Optional[float] = None,
                take_profit: Optional[float] = None,
                metadata: Optional[Dict[str, Any]] = None):
        """
        Initialize a position.
        
        Args:
            symbol: Trading pair symbol
            side: Trade side ('buy' or 'sell')
            entry_price: Entry price
            size: Position size in quote currency
            timestamp: Unix timestamp when position was created (defaults to current time)
            position_id: Unique position ID (defaults to generated UUID)
            stop_loss: Stop loss price
            take_profit: Take profit price
            metadata: Additional position metadata
        """
        # Basic position info
        self.symbol = symbol
        self.side = side.lower()
        self.entry_price = float(entry_price)
        self.size = float(size)
        self.timestamp = timestamp or int(time.time() * 1000)
        self.position_id = position_id or str(uuid.uuid4())
        self.status = PositionStatus.PENDING
        
        # Risk management levels
        self.stop_loss = float(stop_loss) if stop_loss is not None else None
        self.take_profit = float(take_profit) if take_profit is not None else None
        self.trailing_stop = None
        self.trailing_distance = None
        self.trailing_activated = False
        
        # Exit info
        self.exit_price = None
        self.exit_timestamp = None
        self.exit_reason = None
        
        # Performance tracking
        self.max_profit = 0
        self.max_loss = 0
        self.current_price = entry_price
        
        # Order tracking
        self.entry_order_id = None
        self.exit_order_id = None
        self.stop_loss_order_id = None
        self.take_profit_order_id = None
        
        # Risk tracking
        if self.side == 'buy':
            self.risk_amount = (self.entry_price - self.stop_loss) * self.size / self.entry_price if self.stop_loss else None
        else:
            self.risk_amount = (self.stop_loss - self.entry_price) * self.size / self.entry_price if self.stop_loss else None
        
        # Metadata
        self.metadata = metadata or {}
        
        # Transaction history
        self.transaction_history = []
        self._add_transaction("created", {"entry_price": self.entry_price, "size": self.size})
    
    def update_status(self, status: PositionStatus, reason: Optional[str] = None) -> None:
        """
        Update position status.
        
        Args:
            status: New position status
            reason: Reason for status update
        """
        old_status = self.status
        self.status = status
        
        # Log status change
        logger.info(f"Position {self.position_id} status changed: {old_status.value} -> {status.value} ({reason or 'N/A'})")
        
        # Record transaction
        self._add_transaction("status_update", {"old_status": old_status.value, "new_status": status.value, "reason": reason})
    
    def set_entry_order_id(self, order_id: str) -> None:
        """
        Set entry order ID.
        
        Args:
            order_id: Entry order ID
        """
        self.entry_order_id = order_id
        self._add_transaction("entry_order", {"order_id": order_id})
    
    def set_exit_order_id(self, order_id: str) -> None:
        """
        Set exit order ID.
        
        Args:
            order_id: Exit order ID
        """
        self.exit_order_id = order_id
        self._add_transaction("exit_order", {"order_id": order_id})
    
    def set_stop_loss_order_id(self, order_id: str) -> None:
        """
        Set stop loss order ID.
        
        Args:
            order_id: Stop loss order ID
        """
        self.stop_loss_order_id = order_id
        self._add_transaction("stop_loss_order", {"order_id": order_id})
    
    def set_take_profit_order_id(self, order_id: str) -> None:
        """
        Set take profit order ID.
        
        Args:
            order_id: Take profit order ID
        """
        self.take_profit_order_id = order_id
        self._add_transaction("take_profit_order", {"order_id": order_id})
    
    def open(self, executed_price: Optional[float] = None) -> None:
        """
        Mark position as open.
        
        Args:
            executed_price: Actual executed price (if different from entry_price)
        """
        if executed_price is not None and executed_price != self.entry_price:
            self.entry_price = float(executed_price)
            
            # Recalculate risk amount
            if self.side == 'buy' and self.stop_loss:
                self.risk_amount = (self.entry_price - self.stop_loss) * self.size / self.entry_price
            elif self.side == 'sell' and self.stop_loss:
                self.risk_amount = (self.stop_loss - self.entry_price) * self.size / self.entry_price
                
        self.update_status(PositionStatus.OPEN, "Position opened")
        self._add_transaction("opened", {"executed_price": executed_price or self.entry_price})
    
    def close(self, exit_price: float, reason: str, timestamp: Optional[int] = None) -> float:
        """
        Close the position.
        
        Args:
            exit_price: Exit price
            reason: Reason for closing
            timestamp: Exit timestamp (defaults to current time)
            
        Returns:
            Realized PnL
        """
        self.exit_price = float(exit_price)
        self.exit_timestamp = timestamp or int(time.time() * 1000)
        self.exit_reason = reason
        self.update_status(PositionStatus.CLOSED, reason)
        
        # Calculate PnL
        pnl = self.calculate_pnl(self.exit_price)
        
        # Record transaction
        self._add_transaction("closed", {
            "exit_price": self.exit_price,
            "pnl": pnl,
            "reason": reason
        })
        
        return pnl
    
    def cancel(self, reason: str) -> None:
        """
        Cancel the position.
        
        Args:
            reason: Reason for cancellation
        """
        self.update_status(PositionStatus.CANCELLED, reason)
        self._add_transaction("cancelled", {"reason": reason})
    
    def set_stop_loss(self, price: float) -> None:
        """
        Set stop loss price.
        
        Args:
            price: Stop loss price
        """
        # Skip if unchanged
        if self.stop_loss == price:
            return
            
        self.stop_loss = float(price)
        
        # Recalculate risk amount
        if self.side == 'buy':
            self.risk_amount = (self.entry_price - self.stop_loss) * self.size / self.entry_price
        else:
            self.risk_amount = (self.stop_loss - self.entry_price) * self.size / self.entry_price
        
        # Record transaction
        self._add_transaction("stop_loss_update", {"price": self.stop_loss})
        
        logger.info(f"Position {self.position_id} stop loss updated to {self.stop_loss}")
    
    def set_take_profit(self, price: float) -> None:
        """
        Set take profit price.
        
        Args:
            price: Take profit price
        """
        # Skip if unchanged
        if self.take_profit == price:
            return
            
        self.take_profit = float(price)
        
        # Record transaction
        self._add_transaction("take_profit_update", {"price": self.take_profit})
        
        logger.info(f"Position {self.position_id} take profit updated to {self.take_profit}")
    
    def set_trailing_stop(self, distance: float) -> None:
        """
        Set trailing stop distance.
        
        Args:
            distance: Trailing stop distance as percentage (0.01 = 1%)
        """
        self.trailing_distance = float(distance)
        
        # Initialize trailing stop level
        if self.side == 'buy':
            self.trailing_stop = self.current_price * (1 - distance)
        else:
            self.trailing_stop = self.current_price * (1 + distance)
        
        # Record transaction
        self._add_transaction("trailing_stop_set", {
            "distance": self.trailing_distance,
            "initial_level": self.trailing_stop
        })
        
        logger.info(f"Position {self.position_id} trailing stop set: distance={distance}, level={self.trailing_stop}")
    
    def update_price(self, price: float) -> Dict[str, Any]:
        """
        Update current price and check for stop conditions.
        
        Args:
            price: Current price
            
        Returns:
            Dict with trigger status:
            {
                "triggered": True if any stop was triggered,
                "type": "stop_loss", "take_profit", or "trailing_stop" if triggered
            }
        """
        old_price = self.current_price
        self.current_price = float(price)
        
        # Update max profit/loss
        unrealized_pnl = self.calculate_pnl(price)
        if unrealized_pnl > self.max_profit:
            self.max_profit = unrealized_pnl
        if unrealized_pnl < self.max_loss:
            self.max_loss = unrealized_pnl
        
        # Check if any stop condition is met
        result = {"triggered": False, "type": None}
        
        # Check take profit
        if self.take_profit is not None:
            if (self.side == 'buy' and price >= self.take_profit) or \
               (self.side == 'sell' and price <= self.take_profit):
                result["triggered"] = True
                result["type"] = "take_profit"
                return result
        
        # Check stop loss
        if self.stop_loss is not None:
            if (self.side == 'buy' and price <= self.stop_loss) or \
               (self.side == 'sell' and price >= self.stop_loss):
                result["triggered"] = True
                result["type"] = "stop_loss"
                return result
        
        # Update trailing stop if needed
        if self.trailing_stop is not None and self.trailing_distance is not None:
            # For long positions, move trailing stop up as price increases
            if self.side == 'buy' and price > old_price:
                new_stop = price * (1 - self.trailing_distance)
                if new_stop > self.trailing_stop:
                    self.trailing_stop = new_stop
                    self.trailing_activated = True
                    
                    # Record transaction only on significant changes (0.5% or more)
                    if (new_stop / self.trailing_stop - 1) >= 0.005:
                        self._add_transaction("trailing_stop_update", {"level": self.trailing_stop})
            
            # For short positions, move trailing stop down as price decreases
            elif self.side == 'sell' and price < old_price:
                new_stop = price * (1 + self.trailing_distance)
                if new_stop < self.trailing_stop:
                    self.trailing_stop = new_stop
                    self.trailing_activated = True
                    
                    # Record transaction only on significant changes (0.5% or more)
                    if (1 - new_stop / self.trailing_stop) >= 0.005:
                        self._add_transaction("trailing_stop_update", {"level": self.trailing_stop})
            
            # Check if trailing stop is triggered
            if self.trailing_activated:
                if (self.side == 'buy' and price <= self.trailing_stop) or \
                   (self.side == 'sell' and price >= self.trailing_stop):
                    result["triggered"] = True
                    result["type"] = "trailing_stop"
                    return result
        
        return result
    
    def calculate_pnl(self, price: float) -> float:
        """
        Calculate profit/loss at given price.
        
        Args:
            price: Price to calculate PnL at
            
        Returns:
            PnL amount
        """
        if self.side == 'buy':
            return (price - self.entry_price) / self.entry_price * self.size
        else:
            return (self.entry_price - price) / self.entry_price * self.size
    
    def calculate_pnl_percentage(self, price: float) -> float:
        """
        Calculate profit/loss percentage at given price.
        
        Args:
            price: Price to calculate PnL at
            
        Returns:
            PnL percentage
        """
        if self.side == 'buy':
            return (price - self.entry_price) / self.entry_price * 100
        else:
            return (self.entry_price - price) / self.entry_price * 100
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert position to dictionary."""
        return {
            "position_id": self.position_id,
            "symbol": self.symbol,
            "side": self.side,
            "entry_price": self.entry_price,
            "size": self.size,
            "stop_loss": self.stop_loss,
            "take_profit": self.take_profit,
            "trailing_stop": self.trailing_stop,
            "trailing_distance": self.trailing_distance,
            "status": self.status.value,
            "timestamp": self.timestamp,
            "exit_price": self.exit_price,
            "exit_timestamp": self.exit_timestamp,
            "exit_reason": self.exit_reason,
            "current_price": self.current_price,
            "unrealized_pnl": self.calculate_pnl(self.current_price),
            "unrealized_pnl_pct": self.calculate_pnl_percentage(self.current_price),
            "max_profit": self.max_profit,
            "max_loss": self.max_loss,
            "risk_amount": self.risk_amount,
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Position':
        """
        Create position from dictionary.
        
        Args:
            data: Position data dictionary
            
        Returns:
            Position instance
        """
        position = cls(
            symbol=data["symbol"],
            side=data["side"],
            entry_price=data["entry_price"],
            size=data["size"],
            timestamp=data.get("timestamp"),
            position_id=data.get("position_id"),
            stop_loss=data.get("stop_loss"),
            take_profit=data.get("take_profit"),
            metadata=data.get("metadata", {})
        )
        
        # Restore status
        if "status" in data:
            position.status = PositionStatus(data["status"])
            
        # Restore other fields
        if "trailing_stop" in data and data["trailing_stop"] is not None:
            position.trailing_stop = data["trailing_stop"]
        if "trailing_distance" in data and data["trailing_distance"] is not None:
            position.trailing_distance = data["trailing_distance"]
        if "exit_price" in data and data["exit_price"] is not None:
            position.exit_price = data["exit_price"]
        if "exit_timestamp" in data and data["exit_timestamp"] is not None:
            position.exit_timestamp = data["exit_timestamp"]
        if "exit_reason" in data and data["exit_reason"] is not None:
            position.exit_reason = data["exit_reason"]
        if "entry_order_id" in data:
            position.entry_order_id = data["entry_order_id"]
        if "exit_order_id" in data:
            position.exit_order_id = data["exit_order_id"]
        if "stop_loss_order_id" in data:
            position.stop_loss_order_id = data["stop_loss_order_id"]
        if "take_profit_order_id" in data:
            position.take_profit_order_id = data["take_profit_order_id"]
            
        # Update current price
        if "current_price" in data and data["current_price"] is not None:
            position.current_price = data["current_price"]
            
        return position
    
    def _add_transaction(self, transaction_type: str, details: Dict[str, Any]) -> None:
        """
        Add transaction to history.
        
        Args:
            transaction_type: Type of transaction
            details: Transaction details
        """
        transaction = {
            "timestamp": int(time.time() * 1000),
            "type": transaction_type,
            "details": details
        }
        self.transaction_history.append(transaction) 