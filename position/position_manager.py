"""
Position Management System.

This module implements position tracking with persistent storage
and reconciliation with exchange positions. It maintains a local
database of positions and ensures consistency with exchange data.
"""

import json
import os
import time
import uuid
from datetime import datetime
from enum import Enum, auto
from typing import Dict, List, Optional, Any, Set, Tuple
import asyncio
import logging
from decimal import Decimal

from trading_system.core.component import Component, ComponentStatus
from trading_system.core.logging import get_logger
from trading_system.execution.orders import Order, OrderStatus, OrderSide
from .shadow_accounting import ShadowPositionManager, ReconciliationStatus

logger = logging.getLogger(__name__)

class PositionStatus(Enum):
    """Position status."""
    OPEN = "open"
    PARTIALLY_CLOSED = "partially_closed"
    CLOSED = "closed"
    ERROR = "error"


class PositionType(Enum):
    """Position type."""
    LONG = "long"
    SHORT = "short"


class Position:
    """
    Trading position representation.
    
    Tracks all aspects of a trading position, including:
    - Entry and exit orders
    - Average entry and exit prices
    - Realized and unrealized PnL
    - Position status
    """
    
    def __init__(
        self,
        position_id: str,
        symbol: str,
        position_type: PositionType,
        entry_price: float,
        quantity: float,
        strategy_id: Optional[str] = None,
        timestamp: Optional[float] = None
    ):
        """
        Initialize position.
        
        Args:
            position_id: Unique position identifier
            symbol: Trading symbol
            position_type: Position type (long or short)
            entry_price: Initial entry price
            quantity: Initial quantity
            strategy_id: Strategy that created this position
            timestamp: Creation timestamp
        """
        # Core information
        self.position_id = position_id
        self.symbol = symbol
        self.position_type = position_type
        self.strategy_id = strategy_id
        self.created_at = timestamp or time.time()
        self.updated_at = self.created_at
        self.closed_at: Optional[float] = None
        
        # Position state
        self.status = PositionStatus.OPEN
        self.entry_price = entry_price
        self.exit_price: Optional[float] = None
        self.current_price: Optional[float] = None
        self.quantity = quantity
        self.initial_quantity = quantity
        self.remaining_quantity = quantity
        
        # Orders
        self.entry_orders: List[str] = []
        self.exit_orders: List[str] = []
        
        # Performance metrics
        self.realized_pnl = 0.0
        self.unrealized_pnl = 0.0
        self.fees = 0.0
        
        # Metadata
        self.metadata: Dict[str, Any] = {}
        self.tags: List[str] = []
    
    def update_prices(self, current_price: float) -> None:
        """
        Update position prices and profit calculations.
        
        Args:
            current_price: Current market price
        """
        # Store current price
        self.current_price = current_price
        
        # Calculate unrealized PnL
        if self.remaining_quantity > 0:
            if self.position_type == PositionType.LONG:
                self.unrealized_pnl = (current_price - self.entry_price) * self.remaining_quantity
            else:  # Short
                self.unrealized_pnl = (self.entry_price - current_price) * self.remaining_quantity
        else:
            self.unrealized_pnl = 0.0
        
        self.updated_at = time.time()
    
    def add_entry_order(self, order: Order) -> None:
        """
        Add an entry order to the position.
        
        Args:
            order: Entry order
        """
        if order.order_id in self.entry_orders:
            return
        
        self.entry_orders.append(order.order_id)
        
        # If order is filled, update position details
        if order.status == OrderStatus.FILLED:
            self._process_fill(order, is_entry=True)
        
        self.updated_at = time.time()
    
    def add_exit_order(self, order: Order) -> None:
        """
        Add an exit order to the position.
        
        Args:
            order: Exit order
        """
        if order.order_id in self.exit_orders:
            return
        
        self.exit_orders.append(order.order_id)
        
        # If order is filled, update position details
        if order.status == OrderStatus.FILLED:
            self._process_fill(order, is_entry=False)
        
        self.updated_at = time.time()
    
    def _process_fill(self, order: Order, is_entry: bool) -> None:
        """
        Process an order fill.
        
        Args:
            order: Order that was filled
            is_entry: Whether this is an entry order
        """
        if is_entry:
            # Entry order
            if self.position_type == PositionType.LONG and order.side == OrderSide.BUY:
                # Long position buy order
                self._update_entry(order)
            elif self.position_type == PositionType.SHORT and order.side == OrderSide.SELL:
                # Short position sell order
                self._update_entry(order)
        else:
            # Exit order
            if self.position_type == PositionType.LONG and order.side == OrderSide.SELL:
                # Long position sell order
                self._update_exit(order)
            elif self.position_type == PositionType.SHORT and order.side == OrderSide.BUY:
                # Short position buy order
                self._update_exit(order)
    
    def _update_entry(self, order: Order) -> None:
        """
        Update position based on entry order fill.
        
        Args:
            order: Filled entry order
        """
        # Sum up quantities
        executed_qty = order.executed_quantity
        
        # Calculate new average entry price
        if executed_qty > 0:
            self.entry_price = (
                (self.quantity * self.entry_price + executed_qty * order.average_fill_price())
                / (self.quantity + executed_qty)
            )
            
            # Update quantities
            self.quantity += executed_qty
            self.remaining_quantity += executed_qty
            
            # Add fees
            self.fees += order.total_fee()
    
    def _update_exit(self, order: Order) -> None:
        """
        Update position based on exit order fill.
        
        Args:
            order: Filled exit order
        """
        # Calculate exit details
        executed_qty = order.executed_quantity
        if executed_qty > 0:
            # Calculate exit price
            if self.exit_price is None:
                self.exit_price = order.average_fill_price()
            else:
                # Weighted average of previous exits and this exit
                closed_qty = self.initial_quantity - self.remaining_quantity
                self.exit_price = (
                    (closed_qty * self.exit_price + executed_qty * order.average_fill_price())
                    / (closed_qty + executed_qty)
                )
            
            # Calculate PnL for this exit
            if self.position_type == PositionType.LONG:
                trade_pnl = (order.average_fill_price() - self.entry_price) * executed_qty
            else:  # Short
                trade_pnl = (self.entry_price - order.average_fill_price()) * executed_qty
            
            # Add to realized PnL
            self.realized_pnl += trade_pnl
            
            # Update remaining quantity
            self.remaining_quantity -= executed_qty
            
            # Handle floating point precision issues
            epsilon = 0.000001
            if abs(self.remaining_quantity) < epsilon:
                self.remaining_quantity = 0.0
            
            # Add fees
            self.fees += order.total_fee()
            
            # Update status
            if self.remaining_quantity <= 0:
                self.status = PositionStatus.CLOSED
                self.closed_at = time.time()
            else:
                self.status = PositionStatus.PARTIALLY_CLOSED
    
    def process_order_update(self, order: Order) -> None:
        """
        Process an order update.
        
        Args:
            order: Updated order
        """
        # Check if this is an entry or exit order
        is_entry = order.order_id in self.entry_orders
        is_exit = order.order_id in self.exit_orders
        
        # If filled, process the fill
        if is_entry and order.status == OrderStatus.FILLED:
            self._process_fill(order, is_entry=True)
        elif is_exit and order.status == OrderStatus.FILLED:
            self._process_fill(order, is_entry=False)
        
        self.updated_at = time.time()
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert position to dictionary for persistence.
        
        Returns:
            Dictionary representation
        """
        return {
            "position_id": self.position_id,
            "symbol": self.symbol,
            "position_type": self.position_type.value,
            "strategy_id": self.strategy_id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "closed_at": self.closed_at,
            "status": self.status.value,
            "entry_price": self.entry_price,
            "exit_price": self.exit_price,
            "quantity": self.quantity,
            "initial_quantity": self.initial_quantity,
            "remaining_quantity": self.remaining_quantity,
            "entry_orders": self.entry_orders,
            "exit_orders": self.exit_orders,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "fees": self.fees,
            "metadata": self.metadata,
            "tags": self.tags
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Position':
        """
        Create position from dictionary.
        
        Args:
            data: Dictionary representation
            
        Returns:
            Position instance
        """
        position = cls(
            position_id=data.get("position_id", ""),
            symbol=data.get("symbol", ""),
            position_type=PositionType(data.get("position_type", "long")),
            entry_price=data.get("entry_price", 0.0),
            quantity=data.get("initial_quantity", 0.0),
            strategy_id=data.get("strategy_id"),
            timestamp=data.get("created_at")
        )
        
        # Update additional fields
        position.status = PositionStatus(data.get("status", "open"))
        position.updated_at = data.get("updated_at", position.created_at)
        position.closed_at = data.get("closed_at")
        position.exit_price = data.get("exit_price")
        position.quantity = data.get("quantity", 0.0)
        position.remaining_quantity = data.get("remaining_quantity", position.quantity)
        position.initial_quantity = data.get("initial_quantity", position.quantity)
        position.realized_pnl = data.get("realized_pnl", 0.0)
        position.unrealized_pnl = data.get("unrealized_pnl", 0.0)
        position.fees = data.get("fees", 0.0)
        position.metadata = data.get("metadata", {})
        position.tags = data.get("tags", [])
        
        # Entry and exit orders are loaded separately since they are just IDs here
        
        return position


class PositionManager(Component):
    """
    Position Manager for tracking and reconciling trading positions.
    
    Maintains a local database of positions and ensures consistency
    with exchange positions. Provides methods for creating, updating,
    and querying positions.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize position manager.
        
        Args:
            config: Position manager configuration
        """
        super().__init__("position_manager", config)
        
        # Initialize logger
        self.logger = get_logger("position.manager")
        
        # Get configuration
        position_config = config.get("position_manager", {})
        
        # Position state
        self.positions: Dict[str, Position] = {}
        self.positions_by_symbol: Dict[str, List[str]] = {}
        self.open_positions: Set[str] = set()
        
        # Order tracking
        self.order_to_position: Dict[str, str] = {}
        
        # Persistence
        self.persistence_enabled = position_config.get("persistence_enabled", True)
        self.persistence_path = position_config.get("persistence_path", "data/positions")
        self.persistence_interval = position_config.get("persistence_interval", 60)  # seconds
        self.last_persistence_time = 0
        
        # Initialize shadow accounting
        self.shadow_manager = ShadowPositionManager(
            db_path=config.get("db_path", "positions.db"),
            reconciliation_interval=config.get("reconciliation_interval", 300),
            tolerance=Decimal(str(config.get("tolerance", "0.0001"))),
            max_history=config.get("max_history", 1000),
            auto_correct=config.get("auto_correct", True)
        )
        
        # Position cache
        self._positions: Dict[str, Decimal] = {}
        self._lock = asyncio.Lock()
    
    async def initialize(self) -> None:
        """Initialize the position manager."""
        await super().initialize()
        await self.shadow_manager.initialize()
        
        # Load initial positions from shadow accounting
        positions = await self.shadow_manager.get_all_positions()
        self._positions = {pos["symbol"]: Decimal(str(pos["quantity"])) for pos in positions}
        
    async def start(self) -> None:
        """Start the position manager."""
        if self._status == ComponentStatus.OPERATIONAL:
            return
        
        self.logger.info("Starting position manager")
        
        # Ensure persistence directory exists
        if self.persistence_enabled:
            os.makedirs(self.persistence_path, exist_ok=True)
            await self._recover_positions()
        
        self._update_status(ComponentStatus.OPERATIONAL)
        self.logger.info("Position manager started")
        
        # Start reconciliation loop
        asyncio.create_task(self._reconciliation_loop())
    
    async def stop(self) -> None:
        """Stop the position manager."""
        if self._status != ComponentStatus.OPERATIONAL:
            return
        
        self.logger.info("Stopping position manager")
        
        # Persist positions one last time
        if self.persistence_enabled:
            await self._persist_positions()
        
        await self.shadow_manager.close()
        
        self._update_status(ComponentStatus.SHUTDOWN)
        self.logger.info("Position manager stopped")
    
    async def create_position(
        self,
        symbol: str,
        position_type: PositionType,
        entry_price: float,
        quantity: float,
        strategy_id: Optional[str] = None,
        entry_order: Optional[Order] = None,
        metadata: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None,
    ) -> str:
        """
        Create a new position.
        
        Args:
            symbol: Trading symbol
            position_type: Position type (long or short)
            entry_price: Initial entry price
            quantity: Initial quantity
            strategy_id: Strategy that created this position
            entry_order: Initial entry order
            metadata: Additional metadata
            tags: Position tags
            
        Returns:
            Position ID
        """
        # Generate unique position ID
        position_id = str(uuid.uuid4())
        
        # Create position
        position = Position(
            position_id=position_id,
            symbol=symbol,
            position_type=position_type,
            entry_price=entry_price,
            quantity=quantity,
            strategy_id=strategy_id
        )
        
        # Add metadata and tags if provided
        if metadata:
            position.metadata = metadata
        if tags:
            position.tags = tags
        
        # Add entry order if provided
        if entry_order:
            position.add_entry_order(entry_order)
            self.order_to_position[entry_order.order_id] = position_id
        
        # Store position
        self.positions[position_id] = position
        self.open_positions.add(position_id)
        
        # Update symbol index
        if symbol not in self.positions_by_symbol:
            self.positions_by_symbol[symbol] = []
        self.positions_by_symbol[symbol].append(position_id)
        
        # Persist position
        if self.persistence_enabled:
            await self._persist_position(position)
        
        self.logger.info(f"Created {position_type.value} position {position_id} for {symbol}: {quantity} @ {entry_price}")
        
        return position_id
    
    async def update_position(self, symbol: str, quantity_change: Decimal, 
                            source: str = "trade", metadata: Optional[Dict[str, Any]] = None) -> None:
        """Update a position with a quantity change.
        
        Args:
            symbol: Trading pair symbol
            quantity_change: Change in position quantity (positive for buys, negative for sells)
            source: Source of the position update
            metadata: Additional metadata about the update
        """
        async with self._lock:
            # Update internal cache
            current_position = self._positions.get(symbol, Decimal('0'))
            new_position = current_position + quantity_change
            self._positions[symbol] = new_position
            
            # Update shadow accounting
            await self.shadow_manager.update_position(
                symbol=symbol,
                quantity=new_position,
                source=source,
                metadata=metadata or {}
            )
            
            logger.info(f"Updated position for {symbol}: {current_position} -> {new_position} "
                       f"(change: {quantity_change})")
            
    async def get_position(self, symbol: str) -> Decimal:
        """Get current position quantity for a symbol.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Current position quantity
        """
        async with self._lock:
            return self._positions.get(symbol, Decimal("0"))
            
    async def get_all_positions(self) -> Dict[str, Decimal]:
        """Get all current positions.
        
        Returns:
            Dictionary mapping symbols to position quantities
        """
        async with self._lock:
            return self._positions.copy()
            
    async def process_order(self, order: Order) -> None:
        """Process an order and update positions accordingly.
        
        Args:
            order: The order to process
        """
        if order.status != "filled":
            return
            
        # Calculate quantity change based on order side
        quantity_change = order.filled_quantity
        if order.side == OrderSide.SELL:
            quantity_change = -quantity_change
            
        # Update position
        await self.update_position(
            symbol=order.symbol,
            quantity_change=quantity_change,
            source="order",
            metadata={
                "order_id": order.order_id,
                "exchange_order_id": order.exchange_order_id,
                "fill_price": str(order.average_fill_price),
                "order_type": order.order_type.value,
                "side": order.side.value
            }
        )
        
    async def _reconciliation_loop(self) -> None:
        """Background task for periodic position reconciliation."""
        while self.status == "running":
            try:
                # Perform reconciliation
                results = await self.shadow_manager.reconcile_positions()
                
                # Log reconciliation results
                for result in results:
                    if result["status"] == ReconciliationStatus.MISMATCHED:
                        logger.warning(f"Position mismatch detected for {result['symbol']}: "
                                     f"internal={result['internal_quantity']}, "
                                     f"exchange={result['exchange_quantity']}")
                    elif result["status"] == ReconciliationStatus.CORRECTED:
                        logger.info(f"Position corrected for {result['symbol']}: "
                                  f"{result['internal_quantity']} -> {result['exchange_quantity']}")
                        
                # Update internal cache with reconciled positions
                positions = await self.shadow_manager.get_all_positions()
                async with self._lock:
                    self._positions = {pos["symbol"]: Decimal(str(pos["quantity"])) 
                                     for pos in positions}
                    
            except Exception as e:
                logger.error(f"Error in reconciliation loop: {str(e)}")
                
            # Wait for next reconciliation interval
            await asyncio.sleep(self.config.get("reconciliation_interval", 300))
            
    async def get_health(self) -> Dict[str, Any]:
        """Get health status of the position manager.
        
        Returns:
            Dictionary containing health metrics
        """
        health = await super().get_health()
        
        # Add position-specific metrics
        health.update({
            "total_positions": len(self._positions),
            "reconciliation_status": "active" if self.status == "running" else "inactive",
            "last_reconciliation": datetime.now().isoformat()  # TODO: Track actual last reconciliation
        })
        
        return health
    
    async def reconcile_exchange_positions(self, exchange_positions: List[Dict[str, Any]]) -> None:
        """Reconcile positions with exchange data.
        
        Args:
            exchange_positions: List of position data from exchange
        """
        for exchange_pos in exchange_positions:
            symbol = exchange_pos["symbol"]
            exchange_qty = Decimal(str(exchange_pos["quantity"]))
            exchange_type = exchange_pos["type"]
            
            # Get internal position
            internal_qty = await self.get_position(symbol)
            
            # Check for mismatches
            if abs(exchange_qty - internal_qty) > self.config.get("tolerance", Decimal("0.0001")):
                logger.warning(f"Position mismatch for {symbol}: internal={internal_qty}, exchange={exchange_qty}")
                
                # Update position if auto-correct is enabled
                if self.config.get("auto_correct", True):
                    await self.update_position(
                        symbol=symbol,
                        quantity_change=exchange_qty - internal_qty,
                        source="reconciliation",
                        metadata={"exchange_data": exchange_pos}
                    )
                    
    async def get_stats(self) -> Dict[str, Any]:
        """Get position manager statistics.
        
        Returns:
            Dictionary of statistics
        """
        positions = await self.get_all_positions()
        return {
            "total_positions": len(positions),
            "symbols": list(positions.keys()),
            "reconciliation_status": "active" if self.status == "running" else "inactive",
            "last_reconciliation": datetime.now().isoformat()  # TODO: Track actual last reconciliation
        }
    
    def get_position_by_id(self, position_id: str) -> Optional[Position]:
        """
        Get position by ID.
        
        Args:
            position_id: Position ID
            
        Returns:
            Position if found, None otherwise
        """
        return self.positions.get(position_id)
    
    def get_open_positions(self, symbol: Optional[str] = None) -> List[Position]:
        """
        Get all open positions.
        
        Args:
            symbol: Optional symbol to filter by
            
        Returns:
            List of open positions
        """
        result = []
        for position_id in self.open_positions:
            position = self.positions.get(position_id)
            if position and (symbol is None or position.symbol == symbol):
                result.append(position)
        return result
    
    def get_positions_by_symbol(self, symbol: str) -> List[Position]:
        """
        Get all positions for a symbol.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            List of positions
        """
        result = []
        if symbol in self.positions_by_symbol:
            for position_id in self.positions_by_symbol[symbol]:
                position = self.positions.get(position_id)
                if position:
                    result.append(position)
        return result
    
    def get_position_by_order(self, order_id: str) -> Optional[Position]:
        """
        Get position by order ID.
        
        Args:
            order_id: Order ID
            
        Returns:
            Position if found, None otherwise
        """
        position_id = self.order_to_position.get(order_id)
        if position_id:
            return self.positions.get(position_id)
        return None
    
    async def _persist_positions(self) -> None:
        """Persist all positions to disk."""
        try:
            for position_id, position in list(self.positions.items()):
                await self._persist_position(position)
            
            self.last_persistence_time = time.time()
            
        except Exception as e:
            self.logger.exception(f"Error persisting positions: {str(e)}")
    
    async def _persist_position(self, position: Position) -> None:
        """
        Persist a single position to disk.
        
        Args:
            position: Position to persist
        """
        if not self.persistence_enabled:
            return
        
        try:
            # Create position data
            position_data = position.to_dict()
            
            # Determine file path based on position status
            if position.status == PositionStatus.CLOSED:
                path = os.path.join(self.persistence_path, "closed", f"{position.position_id}.json")
            else:
                path = os.path.join(self.persistence_path, "open", f"{position.position_id}.json")
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(path), exist_ok=True)
            
            # Write to file
            with open(path, "w") as f:
                json.dump(position_data, f, indent=2)
                
        except Exception as e:
            self.logger.exception(f"Error persisting position {position.position_id}: {str(e)}")
    
    async def _recover_positions(self) -> None:
        """Recover positions from disk on startup."""
        try:
            self.logger.info("Recovering positions from disk")
            
            # Recover open positions
            open_dir = os.path.join(self.persistence_path, "open")
            if os.path.exists(open_dir):
                for filename in os.listdir(open_dir):
                    if not filename.endswith(".json"):
                        continue
                    
                    try:
                        with open(os.path.join(open_dir, filename), "r") as f:
                            position_data = json.load(f)
                            position = Position.from_dict(position_data)
                            
                            # Add to positions and open positions
                            self.positions[position.position_id] = position
                            self.open_positions.add(position.position_id)
                            
                            # Update symbol index
                            if position.symbol not in self.positions_by_symbol:
                                self.positions_by_symbol[position.symbol] = []
                            self.positions_by_symbol[position.symbol].append(position.position_id)
                            
                            # Store order mappings
                            for order_id in position.entry_orders:
                                self.order_to_position[order_id] = position.position_id
                            for order_id in position.exit_orders:
                                self.order_to_position[order_id] = position.position_id
                            
                            self.logger.info(f"Recovered open position {position.position_id}")
                            
                    except Exception as e:
                        self.logger.error(f"Error recovering position from {filename}: {str(e)}")
            
            # Recover closed positions (up to a limit)
            closed_dir = os.path.join(self.persistence_path, "closed")
            max_closed = 1000  # Limit the number of closed positions to recover
            
            if os.path.exists(closed_dir):
                # Get most recent files based on modification time
                files = [(f, os.path.getmtime(os.path.join(closed_dir, f))) 
                         for f in os.listdir(closed_dir) if f.endswith(".json")]
                files.sort(key=lambda x: x[1], reverse=True)
                
                for filename, _ in files[:max_closed]:
                    try:
                        with open(os.path.join(closed_dir, filename), "r") as f:
                            position_data = json.load(f)
                            position = Position.from_dict(position_data)
                            
                            # Add to positions
                            self.positions[position.position_id] = position
                            
                            # Update symbol index
                            if position.symbol not in self.positions_by_symbol:
                                self.positions_by_symbol[position.symbol] = []
                            self.positions_by_symbol[position.symbol].append(position.position_id)
                            
                            # Store order mappings
                            for order_id in position.entry_orders:
                                self.order_to_position[order_id] = position.position_id
                            for order_id in position.exit_orders:
                                self.order_to_position[order_id] = position.position_id
                            
                    except Exception as e:
                        self.logger.error(f"Error recovering closed position from {filename}: {str(e)}")
            
            self.logger.info(f"Recovered {len(self.open_positions)} open positions and {len(self.positions) - len(self.open_positions)} closed positions")
            
        except Exception as e:
            self.logger.exception(f"Error recovering positions: {str(e)}") 