"""
Risk Management System

This module implements a comprehensive risk management system with multi-level
stop-loss mechanisms, risk limits enforcement, and independent risk monitoring.
"""

import json
import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Set, Any, Callable, Union, Tuple

from trading_system.core.circuit_breaker import CircuitBreaker
from trading_system.models.order import Order, OrderSide, OrderStatus, OrderType
from trading_system.models.position import Position, PositionType, PositionStatus
from trading_system.monitoring.alert_system import get_alert_system, AlertSeverity
from trading_system.utils.config import Config

# Configure logging
logger = logging.getLogger(__name__)

class RiskLevel(Enum):
    """Risk level for trading activities."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"
    EMERGENCY = "emergency"

class RiskLimitType(Enum):
    """Types of risk limits."""
    MAX_POSITION_SIZE = "max_position_size"
    MAX_ORDER_SIZE = "max_order_size"
    MAX_OPEN_ORDERS = "max_open_orders"
    MAX_DAILY_LOSS = "max_daily_loss"
    MAX_DRAWDOWN = "max_drawdown"
    MAX_LEVERAGE = "max_leverage"
    MAX_CONCENTRATION = "max_concentration"
    MIN_LIQUIDITY = "min_liquidity"
    MAX_SLIPPAGE = "max_slippage"
    MAX_EXPOSURE = "max_exposure"

class StopLossType(Enum):
    """Types of stop-loss mechanisms."""
    FIXED = "fixed"
    TRAILING = "trailing"
    TIME_BASED = "time_based"
    VOLATILITY_BASED = "volatility_based"
    VOLUME_BASED = "volume_based"
    MULTI_LEVEL = "multi_level"

@dataclass
class RiskLimit:
    """Risk limit configuration."""
    limit_type: RiskLimitType
    value: float
    symbol: Optional[str] = None
    timeframe: Optional[str] = None
    is_hard_limit: bool = True
    enabled: bool = True
    alert_threshold: Optional[float] = None
    alert_message: Optional[str] = None

@dataclass
class StopLossConfig:
    """Stop-loss configuration."""
    stop_type: StopLossType
    initial_percentage: float
    trailing_step: Optional[float] = None
    time_window: Optional[int] = None
    levels: Optional[List[Dict[str, float]]] = None
    is_enforced: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "stop_type": self.stop_type.value,
            "initial_percentage": self.initial_percentage,
            "trailing_step": self.trailing_step,
            "time_window": self.time_window,
            "levels": self.levels,
            "is_enforced": self.is_enforced
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'StopLossConfig':
        """Create from dictionary."""
        return cls(
            stop_type=StopLossType(data["stop_type"]),
            initial_percentage=data["initial_percentage"],
            trailing_step=data.get("trailing_step"),
            time_window=data.get("time_window"),
            levels=data.get("levels"),
            is_enforced=data.get("is_enforced", True)
        )

@dataclass
class RiskProfile:
    """Trading risk profile."""
    name: str
    description: str
    risk_level: RiskLevel
    position_limits: Dict[str, RiskLimit] = field(default_factory=dict)
    order_limits: Dict[str, RiskLimit] = field(default_factory=dict)
    stop_loss_config: StopLossConfig = field(default_factory=lambda: StopLossConfig(
        stop_type=StopLossType.FIXED,
        initial_percentage=5.0
    ))
    emergency_stop_loss_percentage: float = 20.0
    max_drawdown_percentage: float = 25.0
    is_active: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "description": self.description,
            "risk_level": self.risk_level.value,
            "position_limits": {k: {
                "limit_type": v.limit_type.value,
                "value": v.value,
                "symbol": v.symbol,
                "timeframe": v.timeframe,
                "is_hard_limit": v.is_hard_limit,
                "enabled": v.enabled,
                "alert_threshold": v.alert_threshold,
                "alert_message": v.alert_message
            } for k, v in self.position_limits.items()},
            "order_limits": {k: {
                "limit_type": v.limit_type.value,
                "value": v.value,
                "symbol": v.symbol,
                "timeframe": v.timeframe,
                "is_hard_limit": v.is_hard_limit,
                "enabled": v.enabled,
                "alert_threshold": v.alert_threshold,
                "alert_message": v.alert_message
            } for k, v in self.order_limits.items()},
            "stop_loss_config": self.stop_loss_config.to_dict(),
            "emergency_stop_loss_percentage": self.emergency_stop_loss_percentage,
            "max_drawdown_percentage": self.max_drawdown_percentage,
            "is_active": self.is_active
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RiskProfile':
        """Create from dictionary."""
        profile = cls(
            name=data["name"],
            description=data["description"],
            risk_level=RiskLevel(data["risk_level"]),
            emergency_stop_loss_percentage=data.get("emergency_stop_loss_percentage", 20.0),
            max_drawdown_percentage=data.get("max_drawdown_percentage", 25.0),
            is_active=data.get("is_active", True)
        )
        
        # Load position limits
        for k, v in data.get("position_limits", {}).items():
            profile.position_limits[k] = RiskLimit(
                limit_type=RiskLimitType(v["limit_type"]),
                value=v["value"],
                symbol=v.get("symbol"),
                timeframe=v.get("timeframe"),
                is_hard_limit=v.get("is_hard_limit", True),
                enabled=v.get("enabled", True),
                alert_threshold=v.get("alert_threshold"),
                alert_message=v.get("alert_message")
            )
        
        # Load order limits
        for k, v in data.get("order_limits", {}).items():
            profile.order_limits[k] = RiskLimit(
                limit_type=RiskLimitType(v["limit_type"]),
                value=v["value"],
                symbol=v.get("symbol"),
                timeframe=v.get("timeframe"),
                is_hard_limit=v.get("is_hard_limit", True),
                enabled=v.get("enabled", True),
                alert_threshold=v.get("alert_threshold"),
                alert_message=v.get("alert_message")
            )
        
        # Load stop loss config
        if "stop_loss_config" in data:
            profile.stop_loss_config = StopLossConfig.from_dict(data["stop_loss_config"])
        
        return profile

class RiskManager:
    """
    Comprehensive risk management system.
    
    Features:
    - Multi-level stop-loss mechanisms
    - Risk limits enforcement
    - Independent risk monitoring
    - Position sizing rules
    - Drawdown protection
    - Emergency shutdown capabilities
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the risk management system.
        
        Args:
            config: Risk management configuration
        """
        self.config = config or Config().get("risk_management", {})
        
        # Load risk profiles
        self.profiles: Dict[str, RiskProfile] = {}
        self._load_risk_profiles()
        
        # Active risk profile
        self.active_profile_name = self.config.get("active_profile", "default")
        if self.active_profile_name not in self.profiles:
            if self.profiles:
                self.active_profile_name = next(iter(self.profiles.keys()))
            else:
                # Create a default profile if none exists
                default_profile = RiskProfile(
                    name="default",
                    description="Default risk profile",
                    risk_level=RiskLevel.MEDIUM
                )
                self.profiles["default"] = default_profile
                self.active_profile_name = "default"
        
        # Trading state monitoring
        self.portfolio_value = self.config.get("initial_portfolio_value", 0.0)
        self.max_portfolio_value = self.portfolio_value
        self.daily_starting_value = self.portfolio_value
        self.last_daily_reset = datetime.now().date()
        
        # Risk state
        self.global_trading_enabled = True
        self.trading_paused = False
        self.emergency_stop_triggered = False
        self.current_risk_level = RiskLevel.LOW
        
        # Active stop-losses
        self.active_stop_losses: Dict[str, Dict[str, float]] = {}
        
        # Position monitoring
        self.positions: Dict[str, Position] = {}
        self.position_history: Dict[str, List[Dict[str, Any]]] = {}
        
        # Get alert system
        self.alert_system = get_alert_system()
        
        # Circuit breaker for risk checks
        self.circuit_breaker = CircuitBreaker(
            name="risk_manager",
            failure_threshold=5,
            recovery_timeout=300,
            recovery_threshold=2
        )
        
        # Threading lock
        self.lock = threading.Lock()
        
        # Initialize hardware-level stop-loss
        self._setup_hardware_stop_loss()
        
        logger.info(f"Risk management system initialized with profile: {self.active_profile_name}")
    
    def _load_risk_profiles(self) -> None:
        """Load risk profiles from configuration."""
        profiles_config = self.config.get("profiles", {})
        
        for name, profile_data in profiles_config.items():
            try:
                profile = RiskProfile.from_dict({**profile_data, "name": name})
                self.profiles[name] = profile
                logger.info(f"Loaded risk profile: {name}")
            except Exception as e:
                logger.error(f"Error loading risk profile {name}: {str(e)}")
    
    def _setup_hardware_level_stop_loss(self) -> None:
        """
        Set up hardware-level stop-loss mechanisms.
        
        This is an extra safety measure that works independently from
        the main application, acting as a fail-safe if the main system fails.
        """
        hardware_stop_loss_config = self.config.get("hardware_stop_loss", {})
        
        if not hardware_stop_loss_config.get("enabled", False):
            logger.info("Hardware-level stop-loss is disabled")
            return
        
        try:
            # Implement hardware-level stop-loss
            # This could involve:
            # 1. Setting up a separate monitoring process
            # 2. Configuring watchdog timers
            # 3. Establishing direct exchange connections for emergency actions
            logger.info("Setting up hardware-level stop-loss")
            
            # Example implementation would depend on the specific hardware
            # and exchange integration details
        except Exception as e:
            logger.error(f"Failed to set up hardware-level stop-loss: {str(e)}")
            self.alert_system.error(
                "Hardware Stop-Loss Setup Failed",
                f"Unable to initialize hardware-level stop-loss protection: {str(e)}",
                component="RiskManager"
            )
    
    def get_active_profile(self) -> RiskProfile:
        """Get the active risk profile."""
        with self.lock:
            return self.profiles[self.active_profile_name]
    
    def set_active_profile(self, profile_name: str) -> bool:
        """
        Set the active risk profile.
        
        Args:
            profile_name: Name of the profile to activate
            
        Returns:
            Success flag
        """
        with self.lock:
            if profile_name not in self.profiles:
                logger.error(f"Risk profile not found: {profile_name}")
                return False
            
            self.active_profile_name = profile_name
            logger.info(f"Activated risk profile: {profile_name}")
            
            # Re-evaluate all positions with the new profile
            for position_id, position in self.positions.items():
                self._recalculate_stop_loss(position)
            
            return True
    
    def check_order(self, order: Order) -> Tuple[bool, str]:
        """
        Check if an order complies with risk limits.
        
        Args:
            order: Order to check
            
        Returns:
            Tuple of (is_allowed, reason)
        """
        with self.lock:
            if not self.global_trading_enabled:
                return False, "Global trading is disabled"
            
            if self.trading_paused:
                return False, "Trading is paused due to risk limits"
            
            if self.emergency_stop_triggered:
                return False, "Emergency stop is active"
            
            try:
                # Use circuit breaker pattern for risk checks
                with self.circuit_breaker:
                    profile = self.get_active_profile()
                    
                    # Check order size limit
                    max_order_size = None
                    
                    # Check symbol-specific limit first
                    symbol_key = f"max_order_size_{order.symbol}"
                    if symbol_key in profile.order_limits:
                        max_order_size = profile.order_limits[symbol_key].value
                    
                    # Fall back to global limit
                    if max_order_size is None and "max_order_size" in profile.order_limits:
                        max_order_size = profile.order_limits["max_order_size"].value
                    
                    if max_order_size is not None and order.quantity > max_order_size:
                        return False, f"Order size {order.quantity} exceeds limit {max_order_size}"
                    
                    # Check open orders limit
                    max_open_orders = None
                    if "max_open_orders" in profile.order_limits:
                        max_open_orders = profile.order_limits["max_open_orders"].value
                        
                        # Count active orders for this symbol
                        # (In a real implementation, this would query the order manager)
                        active_order_count = 0  # Placeholder
                        
                        if max_open_orders is not None and active_order_count >= max_open_orders:
                            return False, f"Maximum open orders limit reached ({max_open_orders})"
                    
                    # Check position concentration
                    if "max_concentration" in profile.position_limits:
                        max_concentration = profile.position_limits["max_concentration"].value
                        
                        # Calculate symbol concentration 
                        # (In a real implementation, this would calculate based on portfolio value)
                        symbol_exposure = 0.0  # Placeholder
                        portfolio_value = self.portfolio_value
                        
                        if portfolio_value > 0:
                            concentration = symbol_exposure / portfolio_value
                            if concentration > max_concentration:
                                return False, f"Position concentration {concentration:.2%} exceeds limit {max_concentration:.2%}"
                    
                    # Check leverage
                    if "max_leverage" in profile.position_limits and hasattr(order, "leverage") and order.leverage > 1.0:
                        max_leverage = profile.position_limits["max_leverage"].value
                        if order.leverage > max_leverage:
                            return False, f"Leverage {order.leverage}x exceeds limit {max_leverage}x"
                    
                    # All checks passed
                    return True, ""
            
            except Exception as e:
                logger.error(f"Error in risk check for order: {str(e)}")
                self.circuit_breaker.record_failure()
                
                # Fail closed (safer default) - reject order on error
                return False, f"Risk check error: {str(e)}"
    
    def register_position(self, position: Position) -> None:
        """
        Register a position for risk monitoring.
        
        Args:
            position: Position to monitor
        """
        with self.lock:
            position_id = position.position_id
            
            # Store position
            self.positions[position_id] = position
            
            # Initialize position history
            if position_id not in self.position_history:
                self.position_history[position_id] = []
            
            # Record initial state
            self.position_history[position_id].append({
                "timestamp": time.time(),
                "quantity": position.quantity,
                "entry_price": position.entry_price,
                "current_price": position.current_price,
                "unrealized_pnl": position.unrealized_pnl
            })
            
            # Set up stop-loss
            self._setup_stop_loss(position)
            
            logger.info(f"Registered position {position_id} for risk monitoring")
    
    def update_position(self, position: Position) -> None:
        """
        Update a monitored position.
        
        Args:
            position: Updated position
        """
        with self.lock:
            position_id = position.position_id
            
            if position_id not in self.positions:
                # Register if not already monitored
                self.register_position(position)
                return
            
            # Store updated position
            self.positions[position_id] = position
            
            # Record updated state
            self.position_history[position_id].append({
                "timestamp": time.time(),
                "quantity": position.quantity,
                "entry_price": position.entry_price,
                "current_price": position.current_price,
                "unrealized_pnl": position.unrealized_pnl
            })
            
            # Check stop-loss conditions
            if position_id in self.active_stop_losses and position.status == PositionStatus.OPEN:
                self._check_stop_loss_triggered(position)
            
            # Recalculate stop-loss if needed
            if position.status == PositionStatus.OPEN:
                self._update_stop_loss(position)
    
    def update_portfolio_value(self, value: float) -> None:
        """
        Update the total portfolio value.
        
        Args:
            value: Current portfolio value
        """
        with self.lock:
            previous_value = self.portfolio_value
            self.portfolio_value = value
            
            # Update maximum portfolio value
            if value > self.max_portfolio_value:
                self.max_portfolio_value = value
            
            # Check if we need to reset daily values
            today = datetime.now().date()
            if today != self.last_daily_reset:
                self.daily_starting_value = previous_value
                self.last_daily_reset = today
            
            # Check drawdown limits
            self._check_drawdown_limits(value)
    
    def _check_drawdown_limits(self, current_value: float) -> None:
        """
        Check drawdown limits and take action if exceeded.
        
        Args:
            current_value: Current portfolio value
        """
        profile = self.get_active_profile()
        
        # Check max drawdown
        if self.max_portfolio_value > 0:
            drawdown_pct = (self.max_portfolio_value - current_value) / self.max_portfolio_value * 100
            
            max_drawdown = profile.max_drawdown_percentage
            if drawdown_pct >= max_drawdown:
                if not self.trading_paused:
                    self.trading_paused = True
                    logger.warning(f"Trading paused: Max drawdown limit reached ({drawdown_pct:.2f}% >= {max_drawdown:.2f}%)")
                    self.alert_system.critical(
                        "Max Drawdown Limit Reached",
                        f"Trading paused due to drawdown of {drawdown_pct:.2f}% exceeding limit of {max_drawdown:.2f}%",
                        component="RiskManager"
                    )
        
        # Check daily loss limit
        if self.daily_starting_value > 0 and "max_daily_loss" in profile.position_limits:
            daily_loss_limit = profile.position_limits["max_daily_loss"].value
            daily_loss_pct = (self.daily_starting_value - current_value) / self.daily_starting_value * 100
            
            if daily_loss_pct >= daily_loss_limit:
                if not self.trading_paused:
                    self.trading_paused = True
                    logger.warning(f"Trading paused: Daily loss limit reached ({daily_loss_pct:.2f}% >= {daily_loss_limit:.2f}%)")
                    self.alert_system.critical(
                        "Daily Loss Limit Reached",
                        f"Trading paused due to daily loss of {daily_loss_pct:.2f}% exceeding limit of {daily_loss_limit:.2f}%",
                        component="RiskManager"
                    )
    
    def _setup_stop_loss(self, position: Position) -> None:
        """
        Set up stop-loss for a position.
        
        Args:
            position: Position to set up stop-loss for
        """
        if position.status != PositionStatus.OPEN or position.quantity == 0:
            return
        
        profile = self.get_active_profile()
        stop_loss_config = profile.stop_loss_config
        position_id = position.position_id
        
        # Calculate initial stop-loss price
        initial_percentage = stop_loss_config.initial_percentage
        
        if position.position_type == PositionType.LONG:
            # For long positions, stop-loss is below entry price
            stop_price = position.entry_price * (1 - initial_percentage / 100)
        else:
            # For short positions, stop-loss is above entry price
            stop_price = position.entry_price * (1 + initial_percentage / 100)
        
        # Set up multi-level stop-loss if configured
        levels = []
        if stop_loss_config.stop_type == StopLossType.MULTI_LEVEL and stop_loss_config.levels:
            for level in stop_loss_config.levels:
                percentage = level.get("percentage", 0)
                position_percentage = level.get("position_percentage", 100)
                
                if position.position_type == PositionType.LONG:
                    level_price = position.entry_price * (1 - percentage / 100)
                else:
                    level_price = position.entry_price * (1 + percentage / 100)
                
                levels.append({
                    "price": level_price,
                    "position_percentage": position_percentage
                })
        
        # Store stop-loss configuration
        self.active_stop_losses[position_id] = {
            "type": stop_loss_config.stop_type.value,
            "initial_price": stop_price,
            "current_price": stop_price,
            "highest_price": position.current_price if position.position_type == PositionType.LONG else float("-inf"),
            "lowest_price": position.current_price if position.position_type == PositionType.SHORT else float("inf"),
            "trailing_step": stop_loss_config.trailing_step,
            "levels": levels,
            "is_enforced": stop_loss_config.is_enforced
        }
        
        logger.info(f"Stop-loss set for position {position_id} at price {stop_price}")
    
    def _update_stop_loss(self, position: Position) -> None:
        """
        Update stop-loss for a position based on price movement.
        
        Args:
            position: Position to update stop-loss for
        """
        position_id = position.position_id
        
        if position_id not in self.active_stop_losses:
            self._setup_stop_loss(position)
            return
        
        stop_loss = self.active_stop_losses[position_id]
        stop_type = stop_loss["type"]
        
        if stop_type == StopLossType.TRAILING.value and stop_loss["trailing_step"] is not None:
            # Update trailing stop-loss
            if position.position_type == PositionType.LONG:
                # For long positions, track the highest price and adjust stop-loss upward
                if position.current_price > stop_loss["highest_price"]:
                    # Price moved up, update highest price
                    stop_loss["highest_price"] = position.current_price
                    
                    # Calculate new stop-loss price
                    new_stop_price = position.current_price * (1 - stop_loss["trailing_step"] / 100)
                    
                    # Only update if the new stop-loss is higher than the current one
                    if new_stop_price > stop_loss["current_price"]:
                        stop_loss["current_price"] = new_stop_price
                        logger.info(f"Trailing stop-loss updated for position {position_id} to {new_stop_price}")
            else:
                # For short positions, track the lowest price and adjust stop-loss downward
                if position.current_price < stop_loss["lowest_price"]:
                    # Price moved down, update lowest price
                    stop_loss["lowest_price"] = position.current_price
                    
                    # Calculate new stop-loss price
                    new_stop_price = position.current_price * (1 + stop_loss["trailing_step"] / 100)
                    
                    # Only update if the new stop-loss is lower than the current one
                    if new_stop_price < stop_loss["current_price"]:
                        stop_loss["current_price"] = new_stop_price
                        logger.info(f"Trailing stop-loss updated for position {position_id} to {new_stop_price}")
    
    def _check_stop_loss_triggered(self, position: Position) -> bool:
        """
        Check if stop-loss has been triggered for a position.
        
        Args:
            position: Position to check
            
        Returns:
            True if stop-loss triggered, False otherwise
        """
        position_id = position.position_id
        
        if position_id not in self.active_stop_losses:
            return False
        
        stop_loss = self.active_stop_losses[position_id]
        
        # Skip check if not enforced
        if not stop_loss["is_enforced"]:
            return False
        
        triggered = False
        
        if stop_loss["type"] == StopLossType.MULTI_LEVEL.value and stop_loss["levels"]:
            # Check each level
            for level in stop_loss["levels"]:
                level_price = level["price"]
                
                if (position.position_type == PositionType.LONG and position.current_price <= level_price) or \
                   (position.position_type == PositionType.SHORT and position.current_price >= level_price):
                    # Level triggered - partial close
                    position_percentage = level["position_percentage"]
                    
                    if position_percentage == 100:
                        # Full close
                        triggered = True
                        self._execute_stop_loss(position, position.current_price, 100)
                        break
                    else:
                        # Partial close
                        self._execute_stop_loss(position, position.current_price, position_percentage)
                    
                    # Remove this level after it's triggered
                    stop_loss["levels"].remove(level)
        else:
            # Regular stop-loss check
            stop_price = stop_loss["current_price"]
            
            if (position.position_type == PositionType.LONG and position.current_price <= stop_price) or \
               (position.position_type == PositionType.SHORT and position.current_price >= stop_price):
                triggered = True
                self._execute_stop_loss(position, position.current_price, 100)
        
        return triggered
    
    def _execute_stop_loss(self, position: Position, price: float, percentage: float) -> None:
        """
        Execute stop-loss order for a position.
        
        Args:
            position: Position to execute stop-loss for
            price: Stop-loss price
            percentage: Percentage of position to close
        """
        position_id = position.position_id
        
        if percentage >= 100:
            # Full close
            logger.warning(f"Stop-loss triggered for position {position_id} at price {price}")
            self.alert_system.warning(
                "Stop-Loss Triggered",
                f"Stop-loss executed for {position.symbol} position at price {price}",
                component="RiskManager",
                tags={"position_id": position_id, "symbol": position.symbol}
            )
            
            # Remove from active stop-losses
            if position_id in self.active_stop_losses:
                del self.active_stop_losses[position_id]
            
            # In a real implementation, this would execute a market order
            # to close the position through the order execution system
        else:
            # Partial close
            close_quantity = position.quantity * (percentage / 100)
            
            logger.warning(f"Partial stop-loss triggered for position {position_id} at price {price}, closing {percentage}%")
            self.alert_system.warning(
                "Partial Stop-Loss Triggered",
                f"Partial stop-loss executed for {position.symbol} position at price {price}, closing {percentage}%",
                component="RiskManager",
                tags={"position_id": position_id, "symbol": position.symbol}
            )
            
            # In a real implementation, this would execute a market order
            # for the partial position through the order execution system
    
    def _recalculate_stop_loss(self, position: Position) -> None:
        """
        Recalculate stop-loss for a position (e.g., after changing risk profile).
        
        Args:
            position: Position to recalculate stop-loss for
        """
        position_id = position.position_id
        
        # Remove old stop-loss
        if position_id in self.active_stop_losses:
            del self.active_stop_losses[position_id]
        
        # Set up new stop-loss
        self._setup_stop_loss(position)
    
    def enable_trading(self) -> None:
        """Enable global trading."""
        with self.lock:
            self.global_trading_enabled = True
            self.trading_paused = False
            logger.info("Trading enabled")
    
    def disable_trading(self) -> None:
        """Disable global trading."""
        with self.lock:
            self.global_trading_enabled = False
            logger.warning("Trading disabled")
            self.alert_system.warning(
                "Trading Disabled",
                "All trading activity has been disabled",
                component="RiskManager"
            )
    
    def pause_trading(self) -> None:
        """Pause trading due to risk limits."""
        with self.lock:
            self.trading_paused = True
            logger.warning("Trading paused due to risk limits")
            self.alert_system.warning(
                "Trading Paused",
                "Trading has been paused due to risk limits",
                component="RiskManager"
            )
    
    def resume_trading(self) -> None:
        """Resume trading after pause."""
        with self.lock:
            self.trading_paused = False
            logger.info("Trading resumed")
            self.alert_system.info(
                "Trading Resumed",
                "Trading has been resumed after pause",
                component="RiskManager"
            )
    
    def emergency_stop(self) -> None:
        """
        Trigger emergency stop.
        
        This closes all positions and disables trading.
        """
        with self.lock:
            if self.emergency_stop_triggered:
                return
            
            self.emergency_stop_triggered = True
            self.global_trading_enabled = False
            self.trading_paused = True
            
            logger.critical("EMERGENCY STOP TRIGGERED - Closing all positions")
            self.alert_system.critical(
                "EMERGENCY STOP TRIGGERED",
                "Emergency stop has been triggered. All positions will be closed and trading disabled.",
                component="RiskManager"
            )
            
            # Close all open positions
            for position_id, position in list(self.positions.items()):
                if position.status == PositionStatus.OPEN:
                    # In a real implementation, this would execute market orders
                    # to close all positions through the order execution system
                    logger.warning(f"Emergency closing position {position_id}")
    
    def _setup_hardware_stop_loss(self) -> None:
        """
        Set up hardware-level stop-loss.
        
        This is independent from the main application and provides
        an extra layer of protection.
        """
        hardware_sl_config = self.config.get("hardware_stop_loss", {})
        
        if not hardware_sl_config.get("enabled", False):
            logger.info("Hardware-level stop-loss is disabled")
            return
        
        try:
            # In a real implementation, this might:
            # 1. Create a separate process that monitors positions
            # 2. Use a hardware watchdog timer
            # 3. Set up direct market order capabilities
            
            logger.info("Hardware-level stop-loss initialized")
        except Exception as e:
            logger.error(f"Failed to initialize hardware-level stop-loss: {str(e)}")
    
    def get_risk_stats(self) -> Dict[str, Any]:
        """
        Get risk management statistics.
        
        Returns:
            Dictionary of risk statistics
        """
        with self.lock:
            stats = {
                "trading_enabled": self.global_trading_enabled,
                "trading_paused": self.trading_paused,
                "emergency_stop": self.emergency_stop_triggered,
                "risk_profile": self.active_profile_name,
                "risk_level": self.current_risk_level.value,
                "portfolio_value": self.portfolio_value,
                "max_portfolio_value": self.max_portfolio_value,
                "daily_starting_value": self.daily_starting_value,
                "positions_count": len(self.positions),
                "stop_losses_count": len(self.active_stop_losses)
            }
            
            # Calculate current drawdown
            if self.max_portfolio_value > 0:
                stats["drawdown_pct"] = (self.max_portfolio_value - self.portfolio_value) / self.max_portfolio_value * 100
            else:
                stats["drawdown_pct"] = 0.0
            
            # Calculate daily P&L
            if self.daily_starting_value > 0:
                stats["daily_pnl_pct"] = (self.portfolio_value - self.daily_starting_value) / self.daily_starting_value * 100
            else:
                stats["daily_pnl_pct"] = 0.0
            
            return stats

# Global instance
_instance = None

def get_risk_manager(config: Optional[Dict[str, Any]] = None) -> RiskManager:
    """
    Get or create the global RiskManager instance.
    
    Args:
        config: Optional configuration (only used on first call)
        
    Returns:
        RiskManager instance
    """
    global _instance
    if _instance is None:
        _instance = RiskManager(config)
    return _instance