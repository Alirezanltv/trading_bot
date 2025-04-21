"""
Position Model Module

This module defines the Position data model for tracking trading positions.
"""

from typing import Dict, Any, Optional, List
import json


class Position:
    """
    Position model representing a trading position for a specific symbol.
    """
    
    def __init__(self,
                 symbol: str,
                 quantity: float = 0.0,
                 average_price: float = 0.0,
                 realized_pnl: float = 0.0,
                 unrealized_pnl: float = 0.0,
                 strategy_id: Optional[str] = None):
        """
        Initialize position.
        
        Args:
            symbol: Trading symbol
            quantity: Position size (positive for long, negative for short)
            average_price: Average entry price
            realized_pnl: Realized profit/loss
            unrealized_pnl: Unrealized profit/loss
            strategy_id: ID of the strategy that owns this position
        """
        self.symbol = symbol
        self.quantity = float(quantity)
        self.average_price = float(average_price)
        self.realized_pnl = float(realized_pnl)
        self.unrealized_pnl = float(unrealized_pnl)
        self.strategy_id = strategy_id
    
    def update(self, fill_quantity: float, fill_price: float) -> None:
        """
        Update position with a new fill.
        
        Args:
            fill_quantity: Fill quantity (positive for buy, negative for sell)
            fill_price: Fill price
        """
        if (self.quantity > 0 and fill_quantity < 0) or (self.quantity < 0 and fill_quantity > 0):
            # Reducing or closing position
            close_quantity = min(abs(self.quantity), abs(fill_quantity))
            if self.quantity > 0:
                close_quantity = -close_quantity
            
            # Calculate realized P&L
            trade_pnl = close_quantity * (fill_price - self.average_price)
            self.realized_pnl += trade_pnl
            
            # Update remaining position
            if abs(close_quantity) < abs(self.quantity):
                self.quantity += fill_quantity
            else:
                # Position closed and potentially reversed
                remaining_quantity = fill_quantity + self.quantity
                if remaining_quantity != 0:
                    # Position reversed
                    self.quantity = remaining_quantity
                    self.average_price = fill_price
                else:
                    # Position fully closed
                    self.quantity = 0
                    self.average_price = 0
        else:
            # Increasing position
            if self.quantity == 0:
                # New position
                self.quantity = fill_quantity
                self.average_price = fill_price
            else:
                # Increase existing position
                total_cost = (self.average_price * self.quantity) + (fill_price * fill_quantity)
                self.quantity += fill_quantity
                self.average_price = total_cost / self.quantity if self.quantity != 0 else 0
    
    def update_unrealized_pnl(self, current_price: float) -> None:
        """
        Update unrealized P&L based on current market price.
        
        Args:
            current_price: Current market price of the symbol
        """
        if self.quantity != 0:
            self.unrealized_pnl = self.quantity * (current_price - self.average_price)
        else:
            self.unrealized_pnl = 0
    
    @property
    def is_flat(self) -> bool:
        """Check if position is flat (zero quantity)."""
        return self.quantity == 0
    
    @property
    def is_long(self) -> bool:
        """Check if position is long (positive quantity)."""
        return self.quantity > 0
    
    @property
    def is_short(self) -> bool:
        """Check if position is short (negative quantity)."""
        return self.quantity < 0
    
    @property
    def total_pnl(self) -> float:
        """Calculate total P&L (realized + unrealized)."""
        return self.realized_pnl + self.unrealized_pnl
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert position to dictionary.
        
        Returns:
            Position as dictionary
        """
        return {
            "symbol": self.symbol,
            "quantity": self.quantity,
            "average_price": self.average_price,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "strategy_id": self.strategy_id
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
        return cls(
            symbol=data["symbol"],
            quantity=data["quantity"],
            average_price=data["average_price"],
            realized_pnl=data.get("realized_pnl", 0.0),
            unrealized_pnl=data.get("unrealized_pnl", 0.0),
            strategy_id=data.get("strategy_id")
        )
    
    def to_json(self) -> str:
        """
        Convert position to JSON string.
        
        Returns:
            Position as JSON string
        """
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Position':
        """
        Create position from JSON string.
        
        Args:
            json_str: JSON string
            
        Returns:
            Position instance
        """
        data = json.loads(json_str)
        return cls.from_dict(data)
    
    def __str__(self) -> str:
        """String representation of the position."""
        return (f"Position(symbol={self.symbol}, quantity={self.quantity}, "
                f"average_price={self.average_price}, pnl={self.total_pnl})")


class PortfolioManager:
    """
    Manages a collection of positions across different symbols and strategies.
    """
    
    def __init__(self):
        """Initialize portfolio manager."""
        self.positions: Dict[str, Position] = {}
    
    def get_position(self, symbol: str, strategy_id: Optional[str] = None) -> Position:
        """
        Get position for a symbol and strategy.
        
        Args:
            symbol: Trading symbol
            strategy_id: Strategy ID (optional)
            
        Returns:
            Position for the symbol and strategy
        """
        key = self._get_position_key(symbol, strategy_id)
        if key not in self.positions:
            self.positions[key] = Position(symbol=symbol, strategy_id=strategy_id)
        return self.positions[key]
    
    def update_position(self, symbol: str, fill_quantity: float, fill_price: float, 
                       strategy_id: Optional[str] = None) -> Position:
        """
        Update position with a new fill.
        
        Args:
            symbol: Trading symbol
            fill_quantity: Fill quantity
            fill_price: Fill price
            strategy_id: Strategy ID (optional)
            
        Returns:
            Updated position
        """
        position = self.get_position(symbol, strategy_id)
        position.update(fill_quantity, fill_price)
        return position
    
    def get_all_positions(self) -> List[Position]:
        """
        Get all positions.
        
        Returns:
            List of all positions
        """
        return list(self.positions.values())
    
    def get_strategy_positions(self, strategy_id: str) -> List[Position]:
        """
        Get all positions for a specific strategy.
        
        Args:
            strategy_id: Strategy ID
            
        Returns:
            List of positions for the strategy
        """
        return [pos for pos in self.positions.values() if pos.strategy_id == strategy_id]
    
    def get_total_pnl(self) -> float:
        """
        Get total P&L across all positions.
        
        Returns:
            Total P&L
        """
        return sum(pos.total_pnl for pos in self.positions.values())
    
    def _get_position_key(self, symbol: str, strategy_id: Optional[str] = None) -> str:
        """
        Generate a unique key for a position.
        
        Args:
            symbol: Trading symbol
            strategy_id: Strategy ID (optional)
            
        Returns:
            Unique position key
        """
        return f"{symbol}_{strategy_id or 'default'}"
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert portfolio to dictionary.
        
        Returns:
            Portfolio as dictionary
        """
        return {key: pos.to_dict() for key, pos in self.positions.items()}
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PortfolioManager':
        """
        Create portfolio from dictionary.
        
        Args:
            data: Portfolio data dictionary
            
        Returns:
            PortfolioManager instance
        """
        portfolio = cls()
        for key, pos_data in data.items():
            position = Position.from_dict(pos_data)
            portfolio.positions[key] = position
        return portfolio 