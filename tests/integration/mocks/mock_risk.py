"""
Mock Risk Manager

This module provides a simulated risk manager for integration testing.
"""

import asyncio
import logging
import random
import datetime
from typing import Dict, List, Any, Optional

from trading_system.core.component import Component
from trading_system.core.message_bus import (
    message_bus, get_async_message_bus
)
from trading_system.tests.integration.mocks.mock_message_types import MessageTypes
from trading_system.tests.integration.mocks.mock_component_status import ComponentStatus

# Import mock order types
from trading_system.tests.integration.mocks.mock_order import Order, OrderStatus, OrderSide

logger = logging.getLogger("integration.mock_risk")


class RiskCheckResult:
    """Result of a risk check."""
    
    def __init__(self, passed: bool, reason: str = None):
        """
        Initialize the risk check result.
        
        Args:
            passed: Whether the risk check passed
            reason: Reason for failure if not passed
        """
        self.passed = passed
        self.reason = reason


class RiskLimitType:
    """Types of risk limits."""
    
    ORDER_SIZE = "order_size"
    POSITION_SIZE = "position_size"  
    ORDER_VALUE = "order_value"
    DAILY_LOSS = "daily_loss"
    DAILY_DRAWDOWN = "daily_drawdown"
    ORDER_FREQUENCY = "order_frequency"
    EXPOSURE = "exposure"
    CONCENTRATION = "concentration"


class MockRiskManager(Component):
    """
    Mock risk manager for integration testing.
    
    This component simulates a risk manager that validates orders against
    configurable risk limits and rules.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the mock risk manager.
        
        Args:
            config: Configuration settings
        """
        super().__init__("mock_risk")
        self.config = config or {}
        
        # Risk state
        self.active = False
        self.risk_limits = self._initialize_risk_limits()
        self.order_history = []  # Track recent orders for frequency checks
        
        # Risk check results history
        self.check_results = []
        
        # Risk metrics
        self.metrics = {
            "orders_checked": 0,
            "orders_approved": 0,
            "orders_rejected": 0,
            "risk_limit_breaches": 0,
            "limit_breaches_by_type": {limit_type: 0 for limit_type in dir(RiskLimitType) 
                                      if not limit_type.startswith('_')}
        }
        
        # Runtime variables
        self.risk_task = None
        self.async_message_bus = None
        
        # Position tracking for risk calculations
        self.positions = {}  # symbol -> position details
        self.portfolio_value = self.config.get("initial_portfolio_value", 100000)
        self.daily_starting_value = self.portfolio_value
        self.daily_low_value = self.portfolio_value
        
        # Simulation controls
        self.simulate_risk_breach = False
        self.breach_limit_type = None
        
        logger.info("Mock risk manager initialized")
    
    def _initialize_risk_limits(self) -> Dict[str, Any]:
        """
        Initialize risk limits from configuration.
        
        Returns:
            Risk limits dictionary
        """
        # Default risk limits
        default_limits = {
            RiskLimitType.ORDER_SIZE: {
                "BTC/USDT": 2.0,  # Maximum BTC order size
                "ETH/USDT": 50.0,  # Maximum ETH order size
                "default": 10.0    # Default for other assets
            },
            RiskLimitType.POSITION_SIZE: {
                "BTC/USDT": 5.0,   # Maximum BTC position
                "ETH/USDT": 100.0, # Maximum ETH position
                "default": 20.0    # Default for other assets
            },
            RiskLimitType.ORDER_VALUE: 25000,  # Maximum order value in quote currency
            RiskLimitType.DAILY_LOSS: 5000,    # Maximum daily loss
            RiskLimitType.DAILY_DRAWDOWN: 0.05,  # Maximum 5% daily drawdown
            RiskLimitType.ORDER_FREQUENCY: {
                "max_orders_per_minute": 10,
                "max_orders_per_hour": 100
            },
            RiskLimitType.EXPOSURE: 0.8,  # Maximum 80% of portfolio in positions
            RiskLimitType.CONCENTRATION: 0.3  # Maximum 30% in single asset
        }
        
        # Override defaults with config
        limits = default_limits.copy()
        config_limits = self.config.get("risk_limits", {})
        
        for limit_type, limit_value in config_limits.items():
            if limit_type in limits:
                if isinstance(limits[limit_type], dict) and isinstance(limit_value, dict):
                    # Merge dictionaries for nested configs
                    limits[limit_type].update(limit_value)
                else:
                    # Direct override
                    limits[limit_type] = limit_value
        
        return limits
    
    async def initialize(self):
        """Initialize the mock risk manager."""
        logger.info("Initializing mock risk manager")
        self.async_message_bus = get_async_message_bus()
        
        # Subscribe to order events
        await self.async_message_bus.subscribe(
            MessageTypes.ORDER_REQUEST,
            self._handle_order_request
        )
        
        # Subscribe to position updates for risk tracking
        await self.async_message_bus.subscribe(
            MessageTypes.POSITION_UPDATE,
            self._handle_position_update
        )
        
        # Subscribe to fills for tracking actual executions
        await self.async_message_bus.subscribe(
            MessageTypes.FILL, 
            self._handle_fill
        )
        
        self.status = ComponentStatus.INITIALIZED
        return self.status
    
    async def start(self):
        """Start the mock risk manager."""
        logger.info("Starting mock risk manager")
        
        # Start the background task
        self.active = True
        self.risk_task = asyncio.create_task(self._risk_monitor_loop())
        
        self.status = ComponentStatus.OPERATIONAL
        return self.status
    
    async def stop(self):
        """Stop the mock risk manager."""
        logger.info("Stopping mock risk manager")
        
        # Stop the monitoring task
        self.active = False
        if self.risk_task:
            self.risk_task.cancel()
            try:
                await self.risk_task
            except asyncio.CancelledError:
                pass
            self.risk_task = None
        
        self.status = ComponentStatus.SHUTDOWN
        return self.status
    
    async def get_status(self):
        """Get the current status of the mock risk manager."""
        return self.status
    
    async def _risk_monitor_loop(self):
        """Background task for continuous risk monitoring."""
        try:
            while self.active:
                # Check for overall risk levels
                await self._check_overall_risk()
                
                # Check for daily resets
                self._check_daily_reset()
                
                # Sleep for a bit
                await asyncio.sleep(1.0)
                
        except asyncio.CancelledError:
            logger.info("Risk monitor loop cancelled")
            raise
        except Exception as e:
            logger.error(f"Error in risk monitor loop: {str(e)}", exc_info=True)
            self.status = ComponentStatus.ERROR
    
    async def _check_overall_risk(self):
        """Check overall portfolio risk metrics."""
        # Calculate total position value
        total_position_value = sum(
            position.get("value", 0) for position in self.positions.values()
        )
        
        # Check exposure limit
        exposure_ratio = total_position_value / self.portfolio_value if self.portfolio_value else 0
        max_exposure = self.risk_limits[RiskLimitType.EXPOSURE]
        
        if exposure_ratio > max_exposure:
            logger.warning(f"Portfolio exposure ({exposure_ratio:.2f}) exceeds limit ({max_exposure:.2f})")
            await self._handle_risk_breach(RiskLimitType.EXPOSURE, 
                                          f"Portfolio exposure {exposure_ratio:.2f} exceeds {max_exposure:.2f}")
        
        # Check concentration limit
        max_concentration = self.risk_limits[RiskLimitType.CONCENTRATION]
        
        for symbol, position in self.positions.items():
            position_value = position.get("value", 0)
            concentration = position_value / self.portfolio_value if self.portfolio_value else 0
            
            if concentration > max_concentration:
                logger.warning(f"Concentration in {symbol} ({concentration:.2f}) exceeds limit ({max_concentration:.2f})")
                await self._handle_risk_breach(RiskLimitType.CONCENTRATION, 
                                             f"Concentration in {symbol} {concentration:.2f} exceeds {max_concentration:.2f}")
        
        # Check drawdown
        if self.portfolio_value < self.daily_low_value:
            self.daily_low_value = self.portfolio_value
            
        daily_drawdown = (self.daily_starting_value - self.daily_low_value) / self.daily_starting_value
        max_drawdown = self.risk_limits[RiskLimitType.DAILY_DRAWDOWN]
        
        if daily_drawdown > max_drawdown:
            logger.warning(f"Daily drawdown ({daily_drawdown:.2f}) exceeds limit ({max_drawdown:.2f})")
            await self._handle_risk_breach(RiskLimitType.DAILY_DRAWDOWN,
                                         f"Daily drawdown {daily_drawdown:.2f} exceeds {max_drawdown:.2f}")
    
    def _check_daily_reset(self):
        """Check if we need to reset daily metrics."""
        now = datetime.datetime.now()
        
        # If it's a new day (midnight), reset daily metrics
        if now.hour == 0 and now.minute == 0 and now.second < 10:  # Close to midnight
            logger.info("Resetting daily risk metrics")
            self.daily_starting_value = self.portfolio_value
            self.daily_low_value = self.portfolio_value
    
    async def _handle_order_request(self, message_type, order: Order):
        """
        Handle an order request for risk validation.
        
        Args:
            message_type: Message type
            order: Order object
        """
        logger.info(f"Validating order: {order.id}, {order.symbol}, {order.side}, {order.quantity}")
        
        # Update metrics
        self.metrics["orders_checked"] += 1
        
        # Perform risk checks
        result = await self._check_order_risk(order)
        
        # Store result
        self.check_results.append({
            "order_id": order.id,
            "timestamp": datetime.datetime.now(),
            "passed": result.passed,
            "reason": result.reason
        })
        
        if result.passed:
            # Order approved
            self.metrics["orders_approved"] += 1
            self.order_history.append({
                "order_id": order.id,
                "timestamp": datetime.datetime.now()
            })
            
            # Forward to next component in the pipeline (usually execution)
            await self.async_message_bus.publish(
                MessageTypes.ORDER_VALIDATED,
                order
            )
            
            logger.info(f"Order {order.id} validated")
        else:
            # Order rejected due to risk
            self.metrics["orders_rejected"] += 1
            
            # Update order status
            order.status = OrderStatus.REJECTED
            order.rejection_reason = result.reason
            
            # Publish rejection
            await self.async_message_bus.publish(
                MessageTypes.ORDER_REJECTED,
                order
            )
            
            logger.warning(f"Order {order.id} rejected: {result.reason}")
    
    async def _check_order_risk(self, order: Order) -> RiskCheckResult:
        """
        Check if an order passes all risk limits.
        
        Args:
            order: Order to check
            
        Returns:
            Risk check result
        """
        # Simulate a risk breach if configured
        if self.simulate_risk_breach:
            if self.breach_limit_type:
                return RiskCheckResult(
                    passed=False,
                    reason=f"Simulated {self.breach_limit_type} breach for testing"
                )
        
        # Check order size
        size_result = self._check_order_size(order)
        if not size_result.passed:
            return size_result
        
        # Check order value
        value_result = self._check_order_value(order)
        if not value_result.passed:
            return value_result
        
        # Check position limits
        position_result = self._check_position_limits(order)
        if not position_result.passed:
            return position_result
        
        # Check order frequency
        frequency_result = self._check_order_frequency(order)
        if not frequency_result.passed:
            return frequency_result
        
        # All checks passed
        return RiskCheckResult(passed=True)
    
    def _check_order_size(self, order: Order) -> RiskCheckResult:
        """
        Check if an order exceeds size limits.
        
        Args:
            order: Order to check
            
        Returns:
            Risk check result
        """
        # Get size limit for this symbol
        size_limits = self.risk_limits[RiskLimitType.ORDER_SIZE]
        max_size = size_limits.get(order.symbol, size_limits.get("default", float('inf')))
        
        if order.quantity > max_size:
            self.metrics["risk_limit_breaches"] += 1
            self.metrics["limit_breaches_by_type"][RiskLimitType.ORDER_SIZE] += 1
            
            return RiskCheckResult(
                passed=False,
                reason=f"Order size {order.quantity} exceeds limit {max_size} for {order.symbol}"
            )
        
        return RiskCheckResult(passed=True)
    
    def _check_order_value(self, order: Order) -> RiskCheckResult:
        """
        Check if an order exceeds value limits.
        
        Args:
            order: Order to check
            
        Returns:
            Risk check result
        """
        # Calculate order value
        order_value = order.quantity * order.price if order.price else 0
        
        # If price is not set (e.g., for market orders), estimate from position info
        if order_value == 0 and order.symbol in self.positions:
            position = self.positions[order.symbol]
            if "current_price" in position:
                order_value = order.quantity * position["current_price"]
        
        # Get value limit
        max_value = self.risk_limits[RiskLimitType.ORDER_VALUE]
        
        if order_value > max_value:
            self.metrics["risk_limit_breaches"] += 1
            self.metrics["limit_breaches_by_type"][RiskLimitType.ORDER_VALUE] += 1
            
            return RiskCheckResult(
                passed=False,
                reason=f"Order value {order_value} exceeds limit {max_value}"
            )
        
        return RiskCheckResult(passed=True)
    
    def _check_position_limits(self, order: Order) -> RiskCheckResult:
        """
        Check if an order would exceed position limits.
        
        Args:
            order: Order to check
            
        Returns:
            Risk check result
        """
        # Get current position for this symbol
        current_position = 0
        if order.symbol in self.positions:
            current_position = self.positions[order.symbol].get("quantity", 0)
        
        # Calculate new position size after this order
        new_position = current_position
        if order.side == OrderSide.BUY:
            new_position += order.quantity
        elif order.side == OrderSide.SELL:
            new_position -= order.quantity
        
        # Check against position limits (using absolute value for comparison)
        position_limits = self.risk_limits[RiskLimitType.POSITION_SIZE]
        max_position = position_limits.get(order.symbol, position_limits.get("default", float('inf')))
        
        if abs(new_position) > max_position:
            self.metrics["risk_limit_breaches"] += 1
            self.metrics["limit_breaches_by_type"][RiskLimitType.POSITION_SIZE] += 1
            
            return RiskCheckResult(
                passed=False,
                reason=f"New position {new_position} would exceed limit {max_position} for {order.symbol}"
            )
        
        return RiskCheckResult(passed=True)
    
    def _check_order_frequency(self, order: Order) -> RiskCheckResult:
        """
        Check if order frequency limits are exceeded.
        
        Args:
            order: Order to check
            
        Returns:
            Risk check result
        """
        frequency_limits = self.risk_limits[RiskLimitType.ORDER_FREQUENCY]
        now = datetime.datetime.now()
        
        # Count orders in the last minute
        one_minute_ago = now - datetime.timedelta(minutes=1)
        minute_orders = sum(1 for o in self.order_history 
                          if o["timestamp"] >= one_minute_ago)
        
        if minute_orders >= frequency_limits["max_orders_per_minute"]:
            self.metrics["risk_limit_breaches"] += 1
            self.metrics["limit_breaches_by_type"][RiskLimitType.ORDER_FREQUENCY] += 1
            
            return RiskCheckResult(
                passed=False,
                reason=f"Order frequency exceeds {frequency_limits['max_orders_per_minute']} per minute"
            )
        
        # Count orders in the last hour
        one_hour_ago = now - datetime.timedelta(hours=1)
        hour_orders = sum(1 for o in self.order_history 
                         if o["timestamp"] >= one_hour_ago)
        
        if hour_orders >= frequency_limits["max_orders_per_hour"]:
            self.metrics["risk_limit_breaches"] += 1
            self.metrics["limit_breaches_by_type"][RiskLimitType.ORDER_FREQUENCY] += 1
            
            return RiskCheckResult(
                passed=False,
                reason=f"Order frequency exceeds {frequency_limits['max_orders_per_hour']} per hour"
            )
        
        return RiskCheckResult(passed=True)
    
    async def _handle_position_update(self, message_type, position_data):
        """
        Handle position updates.
        
        Args:
            message_type: Message type
            position_data: Position data
        """
        # Update our position tracking
        symbol = position_data.get("symbol")
        if symbol:
            self.positions[symbol] = position_data
            
            # Update portfolio value if provided
            if "portfolio_value" in position_data:
                self.portfolio_value = position_data["portfolio_value"]
    
    async def _handle_fill(self, message_type, fill_data):
        """
        Handle fills to track actual executions.
        
        Args:
            message_type: Message type
            fill_data: Fill data
        """
        # Not used in basic implementation
        pass
    
    async def _handle_risk_breach(self, limit_type: str, message: str):
        """
        Handle a risk limit breach.
        
        Args:
            limit_type: Type of limit breached
            message: Breach message
        """
        self.metrics["risk_limit_breaches"] += 1
        if limit_type in self.metrics["limit_breaches_by_type"]:
            self.metrics["limit_breaches_by_type"][limit_type] += 1
        
        # Publish risk notification
        await self.async_message_bus.publish(
            MessageTypes.RISK_NOTIFICATION,
            {
                "type": "risk_breach",
                "limit_type": limit_type,
                "message": message,
                "timestamp": datetime.datetime.now().isoformat()
            }
        )
        
        logger.warning(f"Risk breach: {limit_type} - {message}")
    
    # Simulation control methods
    
    def set_risk_limit(self, limit_type: str, value, symbol: str = None):
        """
        Set a risk limit.
        
        Args:
            limit_type: Type of limit to set
            value: Limit value
            symbol: Symbol for symbol-specific limits
        """
        if limit_type in self.risk_limits:
            if symbol and isinstance(self.risk_limits[limit_type], dict):
                self.risk_limits[limit_type][symbol] = value
                logger.info(f"Set {limit_type} limit for {symbol} to {value}")
            else:
                self.risk_limits[limit_type] = value
                logger.info(f"Set {limit_type} limit to {value}")
    
    def set_simulate_risk_breach(self, enable: bool, limit_type: str = None):
        """
        Set whether to simulate a risk breach.
        
        Args:
            enable: Whether to simulate a breach
            limit_type: Type of limit to breach
        """
        self.simulate_risk_breach = enable
        self.breach_limit_type = limit_type
        logger.info(f"Set simulate risk breach to {enable} for {limit_type or 'any'}")
    
    def set_portfolio_value(self, value: float):
        """
        Set the portfolio value.
        
        Args:
            value: Portfolio value
        """
        self.portfolio_value = value
        logger.info(f"Set portfolio value to {value}")
    
    def get_metrics(self):
        """Get risk metrics."""
        return self.metrics
    
    def get_risk_limits(self):
        """Get current risk limits."""
        return self.risk_limits 