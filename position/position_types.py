"""
Position Management Types

This module defines the core types and data structures used in the position management subsystem.
"""

from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Union, Any
import uuid


class PositionStatus(Enum):
    """Status of a position."""
    PENDING_OPEN = "pending_open"  # Position creation initiated but not confirmed
    OPEN = "open"                  # Position is active
    PENDING_CLOSE = "pending_close"  # Position close initiated but not confirmed
    CLOSED = "closed"              # Position is closed
    PARTIALLY_CLOSED = "partially_closed"  # Position partially closed
    CANCELED = "canceled"          # Position creation was canceled
    ERROR = "error"                # Position has an error state


class PositionType(Enum):
    """Type of position."""
    LONG = "long"      # Long position (buy)
    SHORT = "short"    # Short position (sell)


class PositionSource(Enum):
    """Source of a position."""
    STRATEGY = "strategy"       # Created by a strategy
    MANUAL = "manual"           # Created manually
    RECOVERY = "recovery"       # Created during system recovery
    SYSTEM = "system"           # Created by the system


class OrderStatus(Enum):
    """Status of an order."""
    PENDING = "pending"         # Order created but not sent
    SENT = "sent"               # Order sent to exchange
    PARTIALLY_FILLED = "partially_filled"  # Order partially filled
    FILLED = "filled"           # Order fully filled
    CANCELED = "canceled"       # Order canceled
    REJECTED = "rejected"       # Order rejected by exchange
    ERROR = "error"             # Order has an error state


class OrderType(Enum):
    """Type of order."""
    MARKET = "market"           # Market order
    LIMIT = "limit"             # Limit order
    STOP = "stop"               # Stop order
    STOP_LIMIT = "stop_limit"   # Stop-limit order


class OrderAction(Enum):
    """Action of an order."""
    BUY = "buy"                 # Buy order
    SELL = "sell"               # Sell order


class TriggerType(Enum):
    """Type of position trigger."""
    STOP_LOSS = "stop_loss"     # Stop-loss
    TAKE_PROFIT = "take_profit" # Take-profit
    TRAILING_STOP = "trailing_stop"  # Trailing stop
    TIME_LIMIT = "time_limit"   # Time-based exit


class RiskLevel(Enum):
    """Risk level for position sizing."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


@dataclass
class Order:
    """Represents an order in the system."""
    order_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    exchange_order_id: Optional[str] = None
    position_id: Optional[str] = None
    symbol: str = ""
    order_type: OrderType = OrderType.MARKET
    action: OrderAction = OrderAction.BUY
    quantity: float = 0.0
    price: Optional[float] = None
    stop_price: Optional[float] = None
    status: OrderStatus = OrderStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    filled_quantity: float = 0.0
    average_fill_price: Optional[float] = None
    fees: float = 0.0
    fee_currency: str = ""
    error_message: Optional[str] = None
    exchange: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "order_id": self.order_id,
            "exchange_order_id": self.exchange_order_id,
            "position_id": self.position_id,
            "symbol": self.symbol,
            "order_type": self.order_type.value,
            "action": self.action.value,
            "quantity": self.quantity,
            "price": self.price,
            "stop_price": self.stop_price,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "filled_quantity": self.filled_quantity,
            "average_fill_price": self.average_fill_price,
            "fees": self.fees,
            "fee_currency": self.fee_currency,
            "error_message": self.error_message,
            "exchange": self.exchange,
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Order':
        """Create from dictionary."""
        order_data = data.copy()
        
        # Convert string enums to Enum values
        if "order_type" in order_data and isinstance(order_data["order_type"], str):
            order_data["order_type"] = OrderType(order_data["order_type"])
        
        if "action" in order_data and isinstance(order_data["action"], str):
            order_data["action"] = OrderAction(order_data["action"])
        
        if "status" in order_data and isinstance(order_data["status"], str):
            order_data["status"] = OrderStatus(order_data["status"])
        
        # Convert datetime strings to datetime objects
        if "created_at" in order_data and isinstance(order_data["created_at"], str):
            order_data["created_at"] = datetime.fromisoformat(order_data["created_at"])
        
        if "updated_at" in order_data and isinstance(order_data["updated_at"], str):
            order_data["updated_at"] = datetime.fromisoformat(order_data["updated_at"])
        
        return cls(**order_data)


@dataclass
class PositionTrigger:
    """Represents a trigger for a position."""
    trigger_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    position_id: str = ""
    trigger_type: TriggerType = TriggerType.STOP_LOSS
    price: Optional[float] = None
    percentage: Optional[float] = None
    trail_value: Optional[float] = None
    expiration_time: Optional[datetime] = None
    is_active: bool = True
    created_at: datetime = field(default_factory=datetime.now)
    triggered_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "trigger_id": self.trigger_id,
            "position_id": self.position_id,
            "trigger_type": self.trigger_type.value,
            "price": self.price,
            "percentage": self.percentage,
            "trail_value": self.trail_value,
            "expiration_time": self.expiration_time.isoformat() if self.expiration_time else None,
            "is_active": self.is_active,
            "created_at": self.created_at.isoformat(),
            "triggered_at": self.triggered_at.isoformat() if self.triggered_at else None,
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PositionTrigger':
        """Create from dictionary."""
        trigger_data = data.copy()
        
        # Convert string enum to Enum value
        if "trigger_type" in trigger_data and isinstance(trigger_data["trigger_type"], str):
            trigger_data["trigger_type"] = TriggerType(trigger_data["trigger_type"])
        
        # Convert datetime strings to datetime objects
        if "expiration_time" in trigger_data and isinstance(trigger_data["expiration_time"], str):
            trigger_data["expiration_time"] = datetime.fromisoformat(trigger_data["expiration_time"])
        
        if "created_at" in trigger_data and isinstance(trigger_data["created_at"], str):
            trigger_data["created_at"] = datetime.fromisoformat(trigger_data["created_at"])
        
        if "triggered_at" in trigger_data and isinstance(trigger_data["triggered_at"], str):
            trigger_data["triggered_at"] = datetime.fromisoformat(trigger_data["triggered_at"])
        
        return cls(**trigger_data)


@dataclass
class Position:
    """Represents a trading position."""
    position_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    symbol: str = ""
    position_type: PositionType = PositionType.LONG
    status: PositionStatus = PositionStatus.PENDING_OPEN
    quantity: float = 0.0
    entry_price: Optional[float] = None
    current_price: Optional[float] = None
    exit_price: Optional[float] = None
    created_at: datetime = field(default_factory=datetime.now)
    opened_at: Optional[datetime] = None
    closed_at: Optional[datetime] = None
    pnl: float = 0.0
    pnl_percentage: float = 0.0
    fees: float = 0.0
    strategy_id: Optional[str] = None
    source: PositionSource = PositionSource.STRATEGY
    risk_level: RiskLevel = RiskLevel.MEDIUM
    exchange: str = ""
    open_orders: List[str] = field(default_factory=list)
    close_orders: List[str] = field(default_factory=list)
    triggers: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    notes: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "position_id": self.position_id,
            "symbol": self.symbol,
            "position_type": self.position_type.value,
            "status": self.status.value,
            "quantity": self.quantity,
            "entry_price": self.entry_price,
            "current_price": self.current_price,
            "exit_price": self.exit_price,
            "created_at": self.created_at.isoformat(),
            "opened_at": self.opened_at.isoformat() if self.opened_at else None,
            "closed_at": self.closed_at.isoformat() if self.closed_at else None,
            "pnl": self.pnl,
            "pnl_percentage": self.pnl_percentage,
            "fees": self.fees,
            "strategy_id": self.strategy_id,
            "source": self.source.value,
            "risk_level": self.risk_level.value,
            "exchange": self.exchange,
            "open_orders": self.open_orders,
            "close_orders": self.close_orders,
            "triggers": self.triggers,
            "metadata": self.metadata,
            "tags": self.tags,
            "notes": self.notes
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Position':
        """Create from dictionary."""
        position_data = data.copy()
        
        # Convert string enums to Enum values
        if "position_type" in position_data and isinstance(position_data["position_type"], str):
            position_data["position_type"] = PositionType(position_data["position_type"])
        
        if "status" in position_data and isinstance(position_data["status"], str):
            position_data["status"] = PositionStatus(position_data["status"])
        
        if "source" in position_data and isinstance(position_data["source"], str):
            position_data["source"] = PositionSource(position_data["source"])
        
        if "risk_level" in position_data and isinstance(position_data["risk_level"], str):
            position_data["risk_level"] = RiskLevel(position_data["risk_level"])
        
        # Convert datetime strings to datetime objects
        if "created_at" in position_data and isinstance(position_data["created_at"], str):
            position_data["created_at"] = datetime.fromisoformat(position_data["created_at"])
        
        if "opened_at" in position_data and isinstance(position_data["opened_at"], str):
            position_data["opened_at"] = datetime.fromisoformat(position_data["opened_at"])
        
        if "closed_at" in position_data and isinstance(position_data["closed_at"], str):
            position_data["closed_at"] = datetime.fromisoformat(position_data["closed_at"])
        
        return cls(**position_data)


@dataclass
class PortfolioSummary:
    """Summary of the portfolio."""
    total_equity: float = 0.0
    available_balance: float = 0.0
    open_position_count: int = 0
    total_position_value: float = 0.0
    total_pnl: float = 0.0
    total_pnl_percentage: float = 0.0
    total_fees: float = 0.0
    current_exposure: float = 0.0  # Percentage of total equity in open positions
    exposure_per_symbol: Dict[str, float] = field(default_factory=dict)
    exposure_per_strategy: Dict[str, float] = field(default_factory=dict)
    updated_at: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "total_equity": self.total_equity,
            "available_balance": self.available_balance,
            "open_position_count": self.open_position_count,
            "total_position_value": self.total_position_value,
            "total_pnl": self.total_pnl,
            "total_pnl_percentage": self.total_pnl_percentage,
            "total_fees": self.total_fees,
            "current_exposure": self.current_exposure,
            "exposure_per_symbol": self.exposure_per_symbol,
            "exposure_per_strategy": self.exposure_per_strategy,
            "updated_at": self.updated_at.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PortfolioSummary':
        """Create from dictionary."""
        portfolio_data = data.copy()
        
        # Convert datetime string to datetime object
        if "updated_at" in portfolio_data and isinstance(portfolio_data["updated_at"], str):
            portfolio_data["updated_at"] = datetime.fromisoformat(portfolio_data["updated_at"])
        
        return cls(**portfolio_data) 