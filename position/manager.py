"""
Position manager for high-reliability trading system.

This module manages all positions and their lifecycle, including:
- Creating positions
- Monitoring positions
- Setting risk parameters
- Closing positions
- Tracking portfolio metrics
"""

from typing import Dict, Any, List, Optional, Union, Tuple
import time
import threading
import json
import os
from datetime import datetime, timedelta

from trading_system.core import Component, ComponentStatus, get_logger
from trading_system.position.position import Position, PositionStatus

logger = get_logger("position.manager")


class PositionManager(Component):
    """
    Manages all positions and their lifecycle.
    
    The position manager is responsible for:
    - Creating and tracking positions
    - Monitoring positions for stop conditions
    - Managing risk across all positions
    - Storing position history
    - Providing position status updates
    """
    
    def __init__(self, 
                name: str = "PositionManager",
                config: Dict[str, Any] = None,
                position_store_path: Optional[str] = None):
        """
        Initialize the position manager.
        
        Args:
            name: Component name
            config: Configuration parameters
            position_store_path: Path to store position data
        """
        super().__init__(name=name)
        
        # Default configuration
        default_config = {
            "max_positions": 10,  # Maximum number of open positions
            "max_positions_per_symbol": 3,  # Maximum positions per symbol
            "max_risk_per_position": 0.02,  # Maximum risk per position (2% of portfolio)
            "max_risk_total": 0.1,  # Maximum total risk (10% of portfolio)
            "enable_trailing_stops": True,  # Enable trailing stops
            "default_trailing_distance": 0.015,  # Default trailing stop distance (1.5%)
            "auto_adjust_risk": True,  # Automatically adjust position size based on risk
            "position_store_interval": 60  # Store positions every 60 seconds
        }
        
        # Merge configurations
        self.config = default_config.copy()
        if config:
            self.config.update(config)
        
        # Initialize position store
        self.position_store_path = position_store_path
        if not self.position_store_path:
            # Default to current directory
            self.position_store_path = "positions"
        
        # Create directory if it doesn't exist
        os.makedirs(self.position_store_path, exist_ok=True)
        
        # Initialize positions
        self.positions: Dict[str, Position] = {}
        self.closed_positions: Dict[str, Position] = {}
        
        # Portfolio metrics
        self.portfolio_value = 0
        self.portfolio_balance = 0
        self.total_pnl = 0
        self.total_unrealized_pnl = 0
        
        # Performance metrics
        self.metrics = {
            "total_positions_created": 0,
            "total_positions_closed": 0,
            "profitable_positions": 0,
            "losing_positions": 0,
            "win_rate": 0,
            "average_profit": 0,
            "average_loss": 0,
            "largest_profit": 0,
            "largest_loss": 0,
            "max_drawdown": 0,
            "current_drawdown": 0,
            "peak_portfolio_value": 0
        }
        
        # Risk metrics
        self.risk_metrics = {
            "total_risk": 0,
            "max_risk_reached": 0,
            "positions_at_risk": 0,
            "positions_in_profit": 0
        }
        
        # Thread for periodic position store
        self.store_thread = None
        self.stop_store_thread = threading.Event()
        
        # Load existing positions
        self._load_positions()
        
        # Start position store thread
        self._start_position_store_thread()
    
    def create_position(self, 
                        symbol: str, 
                        side: str, 
                        entry_price: float,
                        size: float,
                        stop_loss: Optional[float] = None,
                        take_profit: Optional[float] = None,
                        metadata: Optional[Dict[str, Any]] = None) -> Optional[Position]:
        """
        Create a new position.
        
        Args:
            symbol: Trading pair symbol
            side: Trade side ('buy' or 'sell')
            entry_price: Entry price
            size: Position size in base currency
            stop_loss: Stop loss price
            take_profit: Take profit price
            metadata: Additional position metadata
            
        Returns:
            Created position or None if error
        """
        try:
            # Check if max positions reached
            if len(self.positions) >= self.config["max_positions"]:
                logger.warning(f"Maximum positions ({self.config['max_positions']}) reached, cannot create new position")
                return None
            
            # Check if max positions per symbol reached
            symbol_positions = [p for p in self.positions.values() if p.symbol == symbol]
            if len(symbol_positions) >= self.config["max_positions_per_symbol"]:
                logger.warning(f"Maximum positions per symbol ({self.config['max_positions_per_symbol']}) reached for {symbol}")
                return None
            
            # Create position
            position = Position(
                symbol=symbol,
                side=side,
                entry_price=entry_price,
                size=size,
                stop_loss=stop_loss,
                take_profit=take_profit,
                metadata=metadata
            )
            
            # If auto risk adjustment is enabled, adjust stop loss
            if self.config["auto_adjust_risk"] and stop_loss is None:
                # Calculate appropriate stop loss based on risk settings
                if side == 'buy':
                    # For long positions, stop loss is below entry price
                    risk_distance = self.config["max_risk_per_position"]
                    stop_price = entry_price * (1 - risk_distance)
                    position.set_stop_loss(stop_price)
                else:
                    # For short positions, stop loss is above entry price
                    risk_distance = self.config["max_risk_per_position"]
                    stop_price = entry_price * (1 + risk_distance)
                    position.set_stop_loss(stop_price)
            
            # If trailing stops are enabled, set trailing stop
            if self.config["enable_trailing_stops"]:
                position.set_trailing_stop(self.config["default_trailing_distance"])
            
            # Add position to manager
            self.positions[position.position_id] = position
            
            # Update metrics
            self.metrics["total_positions_created"] += 1
            
            # Update risk metrics
            self._update_risk_metrics()
            
            logger.info(f"Created position {position.position_id}: {symbol} {side} at {entry_price}")
            
            # Store positions
            self._store_positions()
            
            return position
            
        except Exception as e:
            logger.error(f"Error creating position: {str(e)}", exc_info=True)
            return None
    
    def open_position(self, position_id: str, executed_price: Optional[float] = None) -> bool:
        """
        Mark a position as open.
        
        Args:
            position_id: Position ID
            executed_price: Actual executed price (if different from entry price)
            
        Returns:
            Success status
        """
        # Get position
        position = self.positions.get(position_id)
        if not position:
            logger.warning(f"Position {position_id} not found")
            return False
        
        # Open position
        try:
            position.open(executed_price)
            logger.info(f"Opened position {position_id}")
            
            # Store positions
            self._store_positions()
            
            return True
            
        except Exception as e:
            logger.error(f"Error opening position {position_id}: {str(e)}", exc_info=True)
            return False
    
    def close_position(self, position_id: str, exit_price: float, reason: str) -> Tuple[bool, float]:
        """
        Close a position.
        
        Args:
            position_id: Position ID
            exit_price: Exit price
            reason: Reason for closing
            
        Returns:
            Tuple of (success, pnl)
        """
        # Get position
        position = self.positions.get(position_id)
        if not position:
            logger.warning(f"Position {position_id} not found")
            return False, 0
        
        # Close position
        try:
            pnl = position.close(exit_price, reason)
            
            # Move to closed positions
            self.closed_positions[position_id] = position
            del self.positions[position_id]
            
            # Update metrics
            self.metrics["total_positions_closed"] += 1
            self.total_pnl += pnl
            
            if pnl > 0:
                self.metrics["profitable_positions"] += 1
                self.metrics["average_profit"] = (
                    (self.metrics["average_profit"] * (self.metrics["profitable_positions"] - 1) + pnl) / 
                    self.metrics["profitable_positions"]
                )
                if pnl > self.metrics["largest_profit"]:
                    self.metrics["largest_profit"] = pnl
            else:
                self.metrics["losing_positions"] += 1
                self.metrics["average_loss"] = (
                    (self.metrics["average_loss"] * (self.metrics["losing_positions"] - 1) + pnl) /
                    self.metrics["losing_positions"]
                )
                if pnl < self.metrics["largest_loss"]:
                    self.metrics["largest_loss"] = pnl
            
            # Update win rate
            if self.metrics["total_positions_closed"] > 0:
                self.metrics["win_rate"] = self.metrics["profitable_positions"] / self.metrics["total_positions_closed"]
            
            # Update risk metrics
            self._update_risk_metrics()
            
            logger.info(f"Closed position {position_id} at {exit_price} with PnL {pnl:.2f} ({reason})")
            
            # Store positions
            self._store_positions()
            
            return True, pnl
            
        except Exception as e:
            logger.error(f"Error closing position {position_id}: {str(e)}", exc_info=True)
            return False, 0
    
    def cancel_position(self, position_id: str, reason: str) -> bool:
        """
        Cancel a position.
        
        Args:
            position_id: Position ID
            reason: Reason for cancellation
            
        Returns:
            Success status
        """
        # Get position
        position = self.positions.get(position_id)
        if not position:
            logger.warning(f"Position {position_id} not found")
            return False
        
        # Cancel position
        try:
            position.cancel(reason)
            
            # Move to closed positions
            self.closed_positions[position_id] = position
            del self.positions[position_id]
            
            # Update metrics
            self.metrics["total_positions_closed"] += 1
            
            # Update risk metrics
            self._update_risk_metrics()
            
            logger.info(f"Cancelled position {position_id} ({reason})")
            
            # Store positions
            self._store_positions()
            
            return True
            
        except Exception as e:
            logger.error(f"Error cancelling position {position_id}: {str(e)}", exc_info=True)
            return False
    
    def update_position_price(self, position_id: str, price: float) -> Dict[str, Any]:
        """
        Update position with current price.
        
        Args:
            position_id: Position ID
            price: Current price
            
        Returns:
            Dict with update results including stop triggers
        """
        # Get position
        position = self.positions.get(position_id)
        if not position:
            return {"position_id": position_id, "found": False}
        
        # Update price
        try:
            result = position.update_price(price)
            
            # Update portfolio metrics
            self._update_portfolio_metrics()
            
            return {
                "position_id": position_id,
                "found": True,
                "price": price,
                "unrealized_pnl": position.calculate_pnl(price),
                "unrealized_pnl_pct": position.calculate_pnl_percentage(price),
                "triggered": result["triggered"],
                "trigger_type": result["type"]
            }
            
        except Exception as e:
            logger.error(f"Error updating position {position_id} price: {str(e)}", exc_info=True)
            return {"position_id": position_id, "found": True, "error": str(e)}
    
    def update_all_prices(self, prices: Dict[str, float]) -> Dict[str, Dict[str, Any]]:
        """
        Update all positions with current prices.
        
        Args:
            prices: Dict of symbol -> price
            
        Returns:
            Dict of position_id -> update results
        """
        results = {}
        positions_to_close = []
        
        # Update each position
        for position_id, position in self.positions.items():
            # Check if price available for symbol
            if position.symbol not in prices:
                continue
                
            # Update price
            price = prices[position.symbol]
            result = self.update_position_price(position_id, price)
            results[position_id] = result
            
            # Check if stop triggered
            if result.get("triggered", False):
                # Add to positions to close
                positions_to_close.append((position_id, price, f"Triggered {result.get('trigger_type', 'stop')}"))
        
        # Close triggered positions
        for position_id, price, reason in positions_to_close:
            self.close_position(position_id, price, reason)
        
        # Update portfolio metrics
        self._update_portfolio_metrics()
        
        return results
    
    def set_stop_loss(self, position_id: str, price: float) -> bool:
        """
        Set stop loss for a position.
        
        Args:
            position_id: Position ID
            price: Stop loss price
            
        Returns:
            Success status
        """
        # Get position
        position = self.positions.get(position_id)
        if not position:
            logger.warning(f"Position {position_id} not found")
            return False
        
        # Set stop loss
        try:
            position.set_stop_loss(price)
            
            # Update risk metrics
            self._update_risk_metrics()
            
            # Store positions
            self._store_positions()
            
            return True
            
        except Exception as e:
            logger.error(f"Error setting stop loss for position {position_id}: {str(e)}", exc_info=True)
            return False
    
    def set_take_profit(self, position_id: str, price: float) -> bool:
        """
        Set take profit for a position.
        
        Args:
            position_id: Position ID
            price: Take profit price
            
        Returns:
            Success status
        """
        # Get position
        position = self.positions.get(position_id)
        if not position:
            logger.warning(f"Position {position_id} not found")
            return False
        
        # Set take profit
        try:
            position.set_take_profit(price)
            
            # Store positions
            self._store_positions()
            
            return True
            
        except Exception as e:
            logger.error(f"Error setting take profit for position {position_id}: {str(e)}", exc_info=True)
            return False
    
    def set_trailing_stop(self, position_id: str, distance: float) -> bool:
        """
        Set trailing stop for a position.
        
        Args:
            position_id: Position ID
            distance: Trailing stop distance as percentage (0.01 = 1%)
            
        Returns:
            Success status
        """
        # Get position
        position = self.positions.get(position_id)
        if not position:
            logger.warning(f"Position {position_id} not found")
            return False
        
        # Set trailing stop
        try:
            position.set_trailing_stop(distance)
            
            # Store positions
            self._store_positions()
            
            return True
            
        except Exception as e:
            logger.error(f"Error setting trailing stop for position {position_id}: {str(e)}", exc_info=True)
            return False
    
    def get_position(self, position_id: str) -> Optional[Position]:
        """
        Get a position by ID.
        
        Args:
            position_id: Position ID
            
        Returns:
            Position or None if not found
        """
        # Try open positions
        if position_id in self.positions:
            return self.positions[position_id]
        
        # Try closed positions
        if position_id in self.closed_positions:
            return self.closed_positions[position_id]
        
        return None
    
    def get_open_positions(self) -> List[Position]:
        """
        Get all open positions.
        
        Returns:
            List of open positions
        """
        return list(self.positions.values())
    
    def get_closed_positions(self, limit: Optional[int] = None) -> List[Position]:
        """
        Get closed positions.
        
        Args:
            limit: Maximum number of positions to return (newest first)
            
        Returns:
            List of closed positions
        """
        positions = list(self.closed_positions.values())
        
        # Sort by timestamp (newest first)
        positions.sort(key=lambda p: p.exit_timestamp or 0, reverse=True)
        
        # Apply limit
        if limit is not None:
            positions = positions[:limit]
        
        return positions
    
    def get_positions_by_symbol(self, symbol: str) -> List[Position]:
        """
        Get all positions for a symbol.
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            List of positions
        """
        return [p for p in self.positions.values() if p.symbol == symbol]
    
    def get_portfolio_stats(self) -> Dict[str, Any]:
        """
        Get portfolio statistics.
        
        Returns:
            Dict of portfolio statistics
        """
        # Update portfolio metrics
        self._update_portfolio_metrics()
        
        # Calculate additional stats
        avg_hold_time = 0
        active_symbols = set()
        
        for position in self.positions.values():
            active_symbols.add(position.symbol)
        
        for position in self.closed_positions.values():
            if position.exit_timestamp and position.timestamp:
                avg_hold_time += (position.exit_timestamp - position.timestamp) / 1000  # Convert to seconds
        
        if self.metrics["total_positions_closed"] > 0:
            avg_hold_time /= self.metrics["total_positions_closed"]
        
        # Format hold time
        if avg_hold_time > 86400:  # More than a day
            avg_hold_time_str = f"{avg_hold_time / 86400:.1f} days"
        elif avg_hold_time > 3600:  # More than an hour
            avg_hold_time_str = f"{avg_hold_time / 3600:.1f} hours"
        elif avg_hold_time > 60:  # More than a minute
            avg_hold_time_str = f"{avg_hold_time / 60:.1f} minutes"
        else:
            avg_hold_time_str = f"{avg_hold_time:.1f} seconds"
        
        return {
            "portfolio_value": self.portfolio_value,
            "portfolio_balance": self.portfolio_balance,
            "total_pnl": self.total_pnl,
            "total_unrealized_pnl": self.total_unrealized_pnl,
            "open_positions": len(self.positions),
            "closed_positions": len(self.closed_positions),
            "win_rate": self.metrics["win_rate"],
            "active_symbols": len(active_symbols),
            "avg_hold_time": avg_hold_time,
            "avg_hold_time_str": avg_hold_time_str,
            "max_drawdown": self.metrics["max_drawdown"],
            "current_drawdown": self.metrics["current_drawdown"],
            "total_risk": self.risk_metrics["total_risk"],
            "risk_ratio": self.risk_metrics["total_risk"] / self.config["max_risk_total"] if self.config["max_risk_total"] > 0 else 0
        }
    
    def cleanup(self) -> None:
        """Clean up resources."""
        # Stop position store thread
        self.stop_store_thread.set()
        if self.store_thread and self.store_thread.is_alive():
            self.store_thread.join(timeout=2)
        
        # Store positions one last time
        self._store_positions()
    
    def _update_portfolio_metrics(self) -> None:
        """Update portfolio metrics."""
        # Calculate unrealized PnL
        total_unrealized_pnl = 0
        for position in self.positions.values():
            total_unrealized_pnl += position.calculate_pnl(position.current_price)
        
        self.total_unrealized_pnl = total_unrealized_pnl
        
        # Update portfolio value
        old_value = self.portfolio_value
        self.portfolio_value = self.portfolio_balance + self.total_pnl + self.total_unrealized_pnl
        
        # Update drawdown
        if self.portfolio_value > self.metrics["peak_portfolio_value"]:
            self.metrics["peak_portfolio_value"] = self.portfolio_value
            self.metrics["current_drawdown"] = 0
        else:
            # Calculate current drawdown
            if self.metrics["peak_portfolio_value"] > 0:
                current_drawdown = (self.metrics["peak_portfolio_value"] - self.portfolio_value) / self.metrics["peak_portfolio_value"]
                self.metrics["current_drawdown"] = current_drawdown
                
                # Update max drawdown if needed
                if current_drawdown > self.metrics["max_drawdown"]:
                    self.metrics["max_drawdown"] = current_drawdown
    
    def _update_risk_metrics(self) -> None:
        """Update risk metrics."""
        # Calculate total risk
        total_risk = 0
        positions_at_risk = 0
        positions_in_profit = 0
        
        for position in self.positions.values():
            # Skip positions without stop loss
            if position.risk_amount is None:
                continue
                
            total_risk += position.risk_amount
            
            # Check if position is at risk
            if position.calculate_pnl(position.current_price) < 0:
                positions_at_risk += 1
            else:
                positions_in_profit += 1
        
        self.risk_metrics["total_risk"] = total_risk
        self.risk_metrics["positions_at_risk"] = positions_at_risk
        self.risk_metrics["positions_in_profit"] = positions_in_profit
        
        # Update max risk reached
        if total_risk > self.risk_metrics["max_risk_reached"]:
            self.risk_metrics["max_risk_reached"] = total_risk
    
    def _store_positions(self) -> None:
        """Store positions to disk."""
        try:
            # Generate timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Create data directory
            positions_dir = os.path.join(self.position_store_path, "positions")
            os.makedirs(positions_dir, exist_ok=True)
            
            # Store open positions
            open_positions = {
                position_id: position.to_dict()
                for position_id, position in self.positions.items()
            }
            
            with open(os.path.join(positions_dir, "open_positions.json"), "w") as f:
                json.dump(open_positions, f, indent=2)
            
            # Store closed positions (only last 1000)
            closed_positions = {
                position_id: position.to_dict()
                for position_id, position in list(self.closed_positions.items())[-1000:]
            }
            
            with open(os.path.join(positions_dir, "closed_positions.json"), "w") as f:
                json.dump(closed_positions, f, indent=2)
            
            # Store metrics
            metrics = {
                "portfolio": {
                    "value": self.portfolio_value,
                    "balance": self.portfolio_balance,
                    "total_pnl": self.total_pnl,
                    "total_unrealized_pnl": self.total_unrealized_pnl
                },
                "performance": self.metrics,
                "risk": self.risk_metrics,
                "timestamp": int(time.time() * 1000)
            }
            
            with open(os.path.join(positions_dir, "metrics.json"), "w") as f:
                json.dump(metrics, f, indent=2)
                
        except Exception as e:
            logger.error(f"Error storing positions: {str(e)}", exc_info=True)
    
    def _load_positions(self) -> None:
        """Load positions from disk."""
        try:
            # Check if positions directory exists
            positions_dir = os.path.join(self.position_store_path, "positions")
            if not os.path.exists(positions_dir):
                logger.info("No positions directory found, starting with empty positions")
                return
            
            # Load open positions
            open_positions_file = os.path.join(positions_dir, "open_positions.json")
            if os.path.exists(open_positions_file):
                with open(open_positions_file, "r") as f:
                    open_positions_data = json.load(f)
                    
                for position_id, position_data in open_positions_data.items():
                    try:
                        position = Position.from_dict(position_data)
                        self.positions[position_id] = position
                    except Exception as e:
                        logger.error(f"Error loading position {position_id}: {str(e)}", exc_info=True)
            
            # Load closed positions
            closed_positions_file = os.path.join(positions_dir, "closed_positions.json")
            if os.path.exists(closed_positions_file):
                with open(closed_positions_file, "r") as f:
                    closed_positions_data = json.load(f)
                    
                for position_id, position_data in closed_positions_data.items():
                    try:
                        position = Position.from_dict(position_data)
                        self.closed_positions[position_id] = position
                    except Exception as e:
                        logger.error(f"Error loading closed position {position_id}: {str(e)}", exc_info=True)
            
            # Load metrics
            metrics_file = os.path.join(positions_dir, "metrics.json")
            if os.path.exists(metrics_file):
                with open(metrics_file, "r") as f:
                    metrics_data = json.load(f)
                    
                # Restore portfolio metrics
                portfolio = metrics_data.get("portfolio", {})
                self.portfolio_value = portfolio.get("value", 0)
                self.portfolio_balance = portfolio.get("balance", 0)
                self.total_pnl = portfolio.get("total_pnl", 0)
                self.total_unrealized_pnl = portfolio.get("total_unrealized_pnl", 0)
                
                # Restore performance metrics
                performance = metrics_data.get("performance", {})
                for key, value in performance.items():
                    if key in self.metrics:
                        self.metrics[key] = value
                
                # Restore risk metrics
                risk = metrics_data.get("risk", {})
                for key, value in risk.items():
                    if key in self.risk_metrics:
                        self.risk_metrics[key] = value
            
            # Update metrics
            self.metrics["total_positions_created"] = len(self.positions) + len(self.closed_positions)
            self.metrics["total_positions_closed"] = len(self.closed_positions)
            
            logger.info(f"Loaded {len(self.positions)} open positions and {len(self.closed_positions)} closed positions")
                
        except Exception as e:
            logger.error(f"Error loading positions: {str(e)}", exc_info=True)
    
    def _position_store_thread(self) -> None:
        """Thread function for periodic position store."""
        while not self.stop_store_thread.is_set():
            try:
                # Store positions
                self._store_positions()
                
                # Sleep for interval
                self.stop_store_thread.wait(self.config["position_store_interval"])
                
            except Exception as e:
                logger.error(f"Error in position store thread: {str(e)}", exc_info=True)
    
    def _start_position_store_thread(self) -> None:
        """Start the position store thread."""
        self.store_thread = threading.Thread(target=self._position_store_thread, daemon=True)
        self.store_thread.start()
    
    def _check_health(self) -> Tuple[ComponentStatus, Dict[str, Any]]:
        """Check component health."""
        metrics = {
            "open_positions": len(self.positions),
            "closed_positions": len(self.closed_positions),
            "total_pnl": self.total_pnl,
            "total_unrealized_pnl": self.total_unrealized_pnl,
            "portfolio_value": self.portfolio_value,
            "win_rate": self.metrics["win_rate"],
            "max_drawdown": self.metrics["max_drawdown"],
            "total_risk": self.risk_metrics["total_risk"]
        }
        
        # Component is healthy if position store thread is running
        if self.store_thread and self.store_thread.is_alive():
            return ComponentStatus.HEALTHY, metrics
        
        return ComponentStatus.DEGRADED, metrics 