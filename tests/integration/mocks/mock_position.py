"""
Mock Position Manager

This module provides a simulated position manager for integration testing.
"""

import asyncio
import logging
import random
import datetime
from enum import Enum
from typing import Dict, List, Any, Optional

from trading_system.core.component import Component
from trading_system.core.message_bus import (
    message_bus, get_async_message_bus
)
from trading_system.tests.integration.mocks.mock_message_types import MessageTypes
from trading_system.tests.integration.mocks.mock_component_status import ComponentStatus

# Import mock order types
from trading_system.tests.integration.mocks.mock_order import Order, OrderStatus, OrderSide

logger = logging.getLogger("integration.mock_position")


class PositionType(Enum):
    """Types of positions."""
    LONG = "long"
    SHORT = "short"
    FLAT = "flat"


class Position:
    """
    Class representing a trading position.
    
    This simulates a position with entry price, current price, 
    and calculation of unrealized P&L.
    """
    
    def __init__(self, symbol: str, quantity: float = 0.0, entry_price: float = 0.0,
                 current_price: float = 0.0, timestamp=None, position_type=None):
        """
        Initialize a position.
        
        Args:
            symbol: Instrument symbol
            quantity: Position quantity (positive for long, negative for short)
            entry_price: Average entry price
            current_price: Current market price
            timestamp: Position timestamp
            position_type: Type of position (LONG, SHORT, FLAT)
        """
        self.symbol = symbol
        self.quantity = quantity
        self.entry_price = entry_price
        self.current_price = current_price
        self.timestamp = timestamp or datetime.datetime.now()
        
        # Determine position type
        if position_type:
            self.type = position_type
        elif quantity > 0:
            self.type = PositionType.LONG
        elif quantity < 0:
            self.type = PositionType.SHORT
        else:
            self.type = PositionType.FLAT
        
        # Additional position data
        self.realized_pnl = 0.0
        self.unrealized_pnl = 0.0
        self.value = 0.0
        self.margin_used = 0.0
        self.cost_basis = abs(quantity * entry_price) if quantity and entry_price else 0.0
        
        # Update calculated fields
        self.update(current_price)
    
    def update(self, current_price: float):
        """
        Update position with current price.
        
        Args:
            current_price: Current market price
        """
        self.current_price = current_price
        
        # Calculate position value
        self.value = abs(self.quantity * current_price)
        
        # Calculate unrealized P&L
        if self.quantity != 0 and self.entry_price != 0:
            # For long positions
            if self.quantity > 0:
                self.unrealized_pnl = (current_price - self.entry_price) * self.quantity
            # For short positions
            elif self.quantity < 0:
                self.unrealized_pnl = (self.entry_price - current_price) * abs(self.quantity)
        else:
            self.unrealized_pnl = 0.0
    
    def add_fill(self, price: float, quantity: float, fee: float = 0.0):
        """
        Add a fill to the position.
        
        Args:
            price: Fill price
            quantity: Fill quantity (positive for buy, negative for sell)
            fee: Trading fee
        """
        if quantity == 0:
            return
        
        # Handle closing positions
        if (self.quantity > 0 and quantity < 0) or (self.quantity < 0 and quantity > 0):
            # Calculate how much of the position is being closed
            close_qty = min(abs(self.quantity), abs(quantity))
            
            # Calculate P&L for the closed portion
            if self.quantity > 0:  # Long position
                realized_pnl = (price - self.entry_price) * close_qty
            else:  # Short position
                realized_pnl = (self.entry_price - price) * close_qty
            
            # Subtract fee
            realized_pnl -= fee
            
            # Update realized P&L
            self.realized_pnl += realized_pnl
            
            # Handle case where position is completely closed
            if abs(close_qty) >= abs(self.quantity):
                # Position closed or reversed
                if abs(close_qty) == abs(quantity):
                    # Just closed the position
                    self.quantity = 0
                    self.type = PositionType.FLAT
                    self.entry_price = 0
                else:
                    # Position reversed
                    new_qty = self.quantity + quantity
                    self.entry_price = price
                    self.quantity = new_qty
                    self.type = PositionType.LONG if new_qty > 0 else PositionType.SHORT
            else:
                # Partially closed position
                remaining_qty = self.quantity + quantity
                self.quantity = remaining_qty
        else:
            # Adding to existing position or opening new position
            if self.quantity == 0:
                # New position
                self.entry_price = price
                self.quantity = quantity
                self.type = PositionType.LONG if quantity > 0 else PositionType.SHORT
            else:
                # Adding to existing position, calculate new average entry price
                old_cost = self.entry_price * self.quantity
                new_cost = price * quantity
                self.quantity += quantity
                self.entry_price = (old_cost + new_cost) / self.quantity
        
        # Update calculated fields
        self.cost_basis = abs(self.quantity * self.entry_price) if self.quantity and self.entry_price else 0.0
        self.update(price)
    
    def calculate_margin(self, margin_requirement: float = 0.1):
        """
        Calculate margin required for this position.
        
        Args:
            margin_requirement: Margin requirement as a fraction (e.g., 0.1 for 10%)
            
        Returns:
            Required margin
        """
        self.margin_used = self.value * margin_requirement
        return self.margin_used
    
    def to_dict(self):
        """Convert position to dictionary."""
        return {
            "symbol": self.symbol,
            "quantity": self.quantity,
            "entry_price": self.entry_price,
            "current_price": self.current_price,
            "timestamp": self.timestamp.isoformat(),
            "type": self.type.value,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "value": self.value,
            "margin_used": self.margin_used,
            "cost_basis": self.cost_basis
        }


class MockPositionManager(Component):
    """
    Mock position manager for integration testing.
    
    This component simulates a position manager that tracks trading positions
    and portfolio state.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the mock position manager.
        
        Args:
            config: Configuration settings
        """
        super().__init__("mock_position")
        self.config = config or {}
        
        # Position state
        self.active = False
        self.positions: Dict[str, Position] = {}  # Symbol -> Position
        
        # Account state
        self.balance = self.config.get("initial_balance", 100000.0)
        self.total_realized_pnl = 0.0
        self.total_unrealized_pnl = 0.0
        self.margin_used = 0.0
        self.available_balance = self.balance
        self.portfolio_value = self.balance
        self.margin_requirement = self.config.get("margin_requirement", 0.1)  # 10% margin by default
        
        # Position metrics
        self.metrics = {
            "fills_processed": 0,
            "positions_opened": 0,
            "positions_closed": 0,
            "total_trades": 0,
            "profitable_trades": 0,
            "losing_trades": 0
        }
        
        # Runtime variables
        self.position_task = None
        self.async_message_bus = None
        
        # Connection state
        self.connected = True
        
        logger.info("Mock position manager initialized")
    
    async def initialize(self):
        """Initialize the mock position manager."""
        logger.info("Initializing mock position manager")
        self.async_message_bus = get_async_message_bus()
        
        # Subscribe to fill events
        await self.async_message_bus.subscribe(
            MessageTypes.FILL,
            self._handle_fill
        )
        
        # Subscribe to market data for position updates
        await self.async_message_bus.subscribe(
            MessageTypes.MARKET_DATA_TICKER_UPDATE,
            self._handle_ticker_update
        )
        
        self.status = ComponentStatus.INITIALIZED
        return self.status
    
    async def start(self):
        """Start the mock position manager."""
        logger.info("Starting mock position manager")
        
        # Start the position monitoring task
        self.active = True
        self.position_task = asyncio.create_task(self._position_monitor_loop())
        
        self.status = ComponentStatus.OPERATIONAL
        return self.status
    
    async def stop(self):
        """Stop the mock position manager."""
        logger.info("Stopping mock position manager")
        
        # Stop the position monitoring task
        self.active = False
        if self.position_task:
            self.position_task.cancel()
            try:
                await self.position_task
            except asyncio.CancelledError:
                pass
            self.position_task = None
        
        self.status = ComponentStatus.SHUTDOWN
        return self.status
    
    async def get_status(self):
        """Get the current status of the mock position manager."""
        if not self.connected:
            return ComponentStatus.DEGRADED
        return self.status
    
    async def _position_monitor_loop(self):
        """Background task for continuous position monitoring."""
        try:
            while self.active:
                # Update portfolio metrics
                await self._update_portfolio_metrics()
                
                # Publish portfolio status periodically
                await self._publish_portfolio_update()
                
                # Sleep for a bit
                await asyncio.sleep(1.0)
                
        except asyncio.CancelledError:
            logger.info("Position monitor loop cancelled")
            raise
        except Exception as e:
            logger.error(f"Error in position monitor loop: {str(e)}", exc_info=True)
            self.status = ComponentStatus.ERROR
    
    async def _update_portfolio_metrics(self):
        """Update overall portfolio metrics."""
        # Reset metrics
        self.total_unrealized_pnl = 0.0
        self.margin_used = 0.0
        
        # Sum up position metrics
        for symbol, position in self.positions.items():
            self.total_unrealized_pnl += position.unrealized_pnl
            self.margin_used += position.calculate_margin(self.margin_requirement)
        
        # Calculate available balance
        self.available_balance = self.balance - self.margin_used
        
        # Calculate total portfolio value
        self.portfolio_value = self.balance + self.total_unrealized_pnl
    
    async def _handle_fill(self, message_type, fill_data):
        """
        Handle a fill event.
        
        Args:
            message_type: Message type
            fill_data: Fill data
        """
        try:
            # Extract data from fill message
            order_id = fill_data.get("order_id")
            symbol = fill_data.get("symbol")
            side = fill_data.get("side")
            fill = fill_data.get("fill", {})
            
            if not (symbol and fill):
                logger.warning(f"Ignoring fill with missing data: {fill_data}")
                return
            
            # Get fill details
            quantity = fill.get("quantity", 0.0)
            price = fill.get("price", 0.0)
            fee = fill.get("fee", 0.0)
            timestamp = fill.get("timestamp")
            
            if timestamp and isinstance(timestamp, str):
                timestamp = datetime.datetime.fromisoformat(timestamp)
            else:
                timestamp = datetime.datetime.now()
            
            # Adjust quantity based on side
            if side == "sell":
                quantity = -quantity
            
            logger.info(f"Processing fill: {symbol} {quantity} @ {price}, fee: {fee}")
            
            # Update position
            await self._update_position(symbol, price, quantity, fee, timestamp)
            
            # Update metrics
            self.metrics["fills_processed"] += 1
            self.metrics["total_trades"] += 1
            
        except Exception as e:
            logger.error(f"Error processing fill: {str(e)}", exc_info=True)
    
    async def _update_position(self, symbol: str, price: float, quantity: float, fee: float, timestamp):
        """
        Update a position based on a fill.
        
        Args:
            symbol: Instrument symbol
            price: Fill price
            quantity: Fill quantity (positive for buy, negative for sell)
            fee: Trading fee
            timestamp: Fill timestamp
        """
        # Get existing position or create new one
        if symbol in self.positions:
            position = self.positions[symbol]
            
            # Check if position is being closed
            prev_position_type = position.type
            
            # Record position size before update
            prev_quantity = position.quantity
            
            # Update the position with the fill
            position.add_fill(price, quantity, fee)
            
            # Check if position was closed or reversed
            if prev_position_type != PositionType.FLAT and position.type == PositionType.FLAT:
                # Position closed
                self.metrics["positions_closed"] += 1
                
                # Check if trade was profitable
                if position.realized_pnl > 0:
                    self.metrics["profitable_trades"] += 1
                else:
                    self.metrics["losing_trades"] += 1
                
                # Update realized P&L
                self.total_realized_pnl += position.realized_pnl
                self.balance += position.realized_pnl
                
                logger.info(f"Closed position: {symbol}, realized P&L: {position.realized_pnl:.2f}")
            elif prev_position_type != position.type and position.type != PositionType.FLAT:
                # Position reversed
                self.metrics["positions_closed"] += 1
                self.metrics["positions_opened"] += 1
                
                # Check if trade was profitable
                if position.realized_pnl > 0:
                    self.metrics["profitable_trades"] += 1
                else:
                    self.metrics["losing_trades"] += 1
                
                # Update realized P&L
                self.total_realized_pnl += position.realized_pnl
                self.balance += position.realized_pnl
                
                logger.info(f"Reversed position: {symbol}, realized P&L: {position.realized_pnl:.2f}")
        else:
            # Create new position
            position = Position(
                symbol=symbol,
                quantity=quantity,
                entry_price=price,
                current_price=price,
                timestamp=timestamp
            )
            
            self.positions[symbol] = position
            self.metrics["positions_opened"] += 1
            
            logger.info(f"Opened new position: {symbol} {quantity} @ {price}")
        
        # Update portfolio metrics
        await self._update_portfolio_metrics()
        
        # Publish position update
        await self._publish_position_update(position)
    
    async def _handle_ticker_update(self, message_type, ticker_data):
        """
        Handle ticker updates for position price updates.
        
        Args:
            message_type: Message type
            ticker_data: Ticker data
        """
        symbol = ticker_data.symbol
        price = ticker_data.price
        
        # Update position if we have one for this symbol
        if symbol in self.positions:
            position = self.positions[symbol]
            position.update(price)
            
            # Publish position update on significant price changes
            # (to avoid too frequent updates)
            if random.random() < 0.2:  # 20% chance to reduce update frequency
                await self._publish_position_update(position)
    
    async def _publish_position_update(self, position: Position):
        """
        Publish a position update.
        
        Args:
            position: Position to publish
        """
        try:
            # Create position message
            position_message = position.to_dict()
            
            # Add additional data for portfolio-wide metrics
            position_message.update({
                "portfolio_value": self.portfolio_value,
                "available_balance": self.available_balance,
                "margin_used": self.margin_used,
                "total_realized_pnl": self.total_realized_pnl,
                "total_unrealized_pnl": self.total_unrealized_pnl
            })
            
            # Publish update
            await self.async_message_bus.publish(
                MessageTypes.POSITION_UPDATE,
                position_message
            )
        except Exception as e:
            logger.error(f"Error publishing position update: {str(e)}", exc_info=True)
    
    async def _publish_portfolio_update(self):
        """Publish overall portfolio update."""
        try:
            # Create portfolio message
            portfolio_message = {
                "timestamp": datetime.datetime.now().isoformat(),
                "balance": self.balance,
                "margin_used": self.margin_used,
                "available_balance": self.available_balance,
                "total_realized_pnl": self.total_realized_pnl,
                "total_unrealized_pnl": self.total_unrealized_pnl,
                "portfolio_value": self.portfolio_value,
                "position_count": len(self.positions),
                "positions": [position.to_dict() for position in self.positions.values()]
            }
            
            # Publish update
            await self.async_message_bus.publish(
                MessageTypes.PORTFOLIO_UPDATE,
                portfolio_message
            )
        except Exception as e:
            logger.error(f"Error publishing portfolio update: {str(e)}", exc_info=True)
    
    # Simulation control methods
    
    def set_connected(self, connected: bool):
        """
        Set the connection state.
        
        Args:
            connected: Whether the component is connected
        """
        self.connected = connected
        logger.info(f"Set connected to {connected}")
        
        # Update status if it changes
        if not connected and self.status == ComponentStatus.OPERATIONAL:
            self.status = ComponentStatus.DEGRADED
        elif connected and self.status == ComponentStatus.DEGRADED:
            self.status = ComponentStatus.OPERATIONAL
    
    def set_initial_balance(self, balance: float):
        """
        Set the initial balance.
        
        Args:
            balance: Initial balance
        """
        self.balance = balance
        self.available_balance = balance - self.margin_used
        self.portfolio_value = balance + self.total_unrealized_pnl
        logger.info(f"Set initial balance to {balance}")
    
    def add_test_position(self, symbol: str, quantity: float, price: float):
        """
        Add a test position.
        
        Args:
            symbol: Instrument symbol
            quantity: Position quantity
            price: Position price
        """
        # Create position
        position = Position(
            symbol=symbol,
            quantity=quantity,
            entry_price=price,
            current_price=price
        )
        
        # Add to positions
        self.positions[symbol] = position
        self.metrics["positions_opened"] += 1
        
        # Update portfolio metrics
        asyncio.create_task(self._update_portfolio_metrics())
        
        logger.info(f"Added test position: {symbol} {quantity} @ {price}")
        
        return position
    
    def get_position(self, symbol: str) -> Optional[Position]:
        """
        Get a position by symbol.
        
        Args:
            symbol: Instrument symbol
            
        Returns:
            Position if found, None otherwise
        """
        return self.positions.get(symbol)
    
    def get_positions(self) -> Dict[str, Position]:
        """
        Get all positions.
        
        Returns:
            Dictionary of positions by symbol
        """
        return self.positions
    
    def get_portfolio_state(self) -> Dict[str, Any]:
        """
        Get the current portfolio state.
        
        Returns:
            Portfolio state dictionary
        """
        return {
            "balance": self.balance,
            "available_balance": self.available_balance,
            "margin_used": self.margin_used,
            "total_realized_pnl": self.total_realized_pnl,
            "total_unrealized_pnl": self.total_unrealized_pnl,
            "portfolio_value": self.portfolio_value,
            "position_count": len(self.positions),
            "metrics": self.metrics
        }
    
    def get_metrics(self):
        """Get position metrics."""
        return self.metrics 