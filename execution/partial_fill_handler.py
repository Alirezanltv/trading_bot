"""
Partial Fill Handler

This module provides handling for partially filled orders, including:
- Partial fill detection and tracking
- Order completion strategies
- Position reconciliation with partial fills
"""

import asyncio
import logging
import time
import uuid
from enum import Enum
from typing import Dict, Any, List, Optional, Set, Union, Callable, Tuple
from datetime import datetime, timedelta

from trading_system.core.component import Component, ComponentStatus
from trading_system.core.logging import get_logger
from trading_system.exchange.base import OrderStatus, OrderType, OrderSide, Order
from trading_system.execution.orders import OrderFill

logger = get_logger("execution.partial_fill")

class FillStrategy(Enum):
    """Strategy for handling partially filled orders."""
    ACCEPT_PARTIAL = "accept_partial"  # Accept partial fills as complete
    RETRY_REMAINING = "retry_remaining"  # Retry remaining quantity
    WAIT_FOR_COMPLETE = "wait_for_complete"  # Wait for complete fill
    CANCEL_AND_RETRY = "cancel_and_retry"  # Cancel and retry with new order


class OrderExecutionStrategy(Enum):
    """Order execution strategy for large orders."""
    SINGLE_ORDER = "single_order"  # Single order execution
    ICEBERG = "iceberg"  # Iceberg order execution (show small portions of full order)
    TWAP = "twap"  # Time-weighted average price execution
    VWAP = "vwap"  # Volume-weighted average price execution


class ExecutionStats:
    """Statistics for order execution."""
    
    def __init__(self):
        """Initialize execution statistics."""
        self.total_orders = 0
        self.full_fills = 0
        self.partial_fills = 0
        self.unfilled_orders = 0
        self.slippage_total = 0.0
        self.slippage_count = 0
        self.retry_count = 0
        self.avg_fill_time_ms = 0
        self.total_fill_time_ms = 0
        self.fill_time_count = 0
    
    def record_order(self, order: Order, fill_status: OrderStatus, 
                    fill_amount: float, expected_amount: float,
                    fill_price: float, expected_price: float,
                    execution_time_ms: Optional[int] = None) -> None:
        """
        Record order execution statistics.
        
        Args:
            order: Order object
            fill_status: Fill status
            fill_amount: Filled amount
            expected_amount: Expected amount
            fill_price: Fill price
            expected_price: Expected price
            execution_time_ms: Execution time in milliseconds
        """
        self.total_orders += 1
        
        # Record fill status
        if fill_status == OrderStatus.FILLED:
            self.full_fills += 1
        elif fill_status == OrderStatus.PARTIALLY_FILLED:
            self.partial_fills += 1
        elif fill_status in [OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.EXPIRED]:
            self.unfilled_orders += 1
        
        # Record slippage if price is available
        if fill_price > 0 and expected_price > 0 and fill_amount > 0:
            if order.side == OrderSide.BUY:
                # For buys, slippage is positive if fill price is higher
                slippage_pct = (fill_price - expected_price) / expected_price * 100.0
            else:
                # For sells, slippage is positive if fill price is lower
                slippage_pct = (expected_price - fill_price) / expected_price * 100.0
                
            self.slippage_total += slippage_pct
            self.slippage_count += 1
        
        # Record execution time
        if execution_time_ms is not None and execution_time_ms > 0:
            self.total_fill_time_ms += execution_time_ms
            self.fill_time_count += 1
            self.avg_fill_time_ms = self.total_fill_time_ms / self.fill_time_count
    
    def record_retry(self) -> None:
        """Record an order retry."""
        self.retry_count += 1
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get execution statistics.
        
        Returns:
            Dict[str, Any]: Execution statistics
        """
        return {
            "total_orders": self.total_orders,
            "full_fills": self.full_fills,
            "partial_fills": self.partial_fills,
            "unfilled_orders": self.unfilled_orders,
            "fill_rate": (self.full_fills + self.partial_fills) / max(1, self.total_orders),
            "avg_slippage_pct": self.slippage_total / max(1, self.slippage_count),
            "retry_count": self.retry_count,
            "avg_fill_time_ms": self.avg_fill_time_ms
        }


class PartialFillHandler(Component):
    """
    Partial Fill Handler
    
    This component handles partially filled orders and implements
    strategies for completing order execution.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize partial fill handler.
        
        Args:
            config: Configuration dictionary
        """
        super().__init__(name="partial_fill_handler", config=config)
        
        # Default strategy
        self.default_strategy = FillStrategy(config.get("default_fill_strategy", "wait_for_complete"))
        
        # Order tracking
        self.partial_orders: Dict[str, Dict[str, Any]] = {}
        
        # Retry configuration
        self.max_retries = config.get("max_retries", 3)
        self.retry_interval_ms = config.get("retry_interval_ms", 1000)
        self.retry_backoff_factor = config.get("retry_backoff_factor", 2.0)
        
        # Wait configuration
        self.max_wait_time_ms = config.get("max_wait_time_ms", 60000)
        self.check_interval_ms = config.get("check_interval_ms", 1000)
        
        # Slippage configuration
        self.max_slippage_pct = config.get("max_slippage_pct", 1.0)
        
        # Strategies for large orders
        self.large_order_threshold = config.get("large_order_threshold", 1.0)
        self.default_large_order_strategy = OrderExecutionStrategy(
            config.get("default_large_order_strategy", "single_order")
        )
        
        # Statistics
        self.stats = ExecutionStats()
        
        # Exchange client
        self.exchange_client = None
        
        # Event handlers
        self.fill_handlers: List[Callable] = []
        
        # Locks
        self._lock = asyncio.Lock()
        
        logger.info("Partial fill handler initialized")
    
    async def start(self) -> bool:
        """
        Start the partial fill handler.
        
        Returns:
            bool: True if started successfully, False otherwise
        """
        try:
            self._status = ComponentStatus.INITIALIZING
            
            # Update status to initialized
            self._status = ComponentStatus.INITIALIZED
            logger.info("Partial fill handler started")
            return True
        except Exception as e:
            logger.error(f"Failed to start partial fill handler: {e}", exc_info=True)
            self._status = ComponentStatus.ERROR
            self._error = str(e)
            return False
    
    async def stop(self) -> bool:
        """
        Stop the partial fill handler.
        
        Returns:
            bool: True if stopped successfully, False otherwise
        """
        try:
            self._status = ComponentStatus.STOPPED
            logger.info("Partial fill handler stopped")
            return True
        except Exception as e:
            logger.error(f"Failed to stop partial fill handler: {e}", exc_info=True)
            self._status = ComponentStatus.ERROR
            self._error = str(e)
            return False
    
    def register_exchange_client(self, client: Any) -> None:
        """
        Register an exchange client.
        
        Args:
            client: Exchange client instance
        """
        self.exchange_client = client
        logger.info("Registered exchange client")
    
    def register_fill_handler(self, handler: Callable) -> None:
        """
        Register a fill handler.
        
        Args:
            handler: Fill handler function
        """
        self.fill_handlers.append(handler)
        logger.info("Registered fill handler")
    
    async def handle_partial_fill(self, order: Order, fill: OrderFill, 
                                 strategy: Optional[FillStrategy] = None) -> bool:
        """
        Handle a partially filled order.
        
        Args:
            order: Original order
            fill: Order fill information
            strategy: Fill strategy (defaults to configured default)
            
        Returns:
            bool: True if handled successfully, False otherwise
        """
        if not strategy:
            strategy = self.default_strategy
        
        order_id = order.order_id
        
        # Record stats
        execution_time_ms = None
        if hasattr(fill, "timestamp") and hasattr(order, "created_at"):
            execution_time_ms = fill.timestamp - order.created_at
            
        self.stats.record_order(
            order=order,
            fill_status=OrderStatus.PARTIALLY_FILLED,
            fill_amount=fill.quantity,
            expected_amount=order.amount,
            fill_price=fill.price,
            expected_price=order.price or 0.0,
            execution_time_ms=execution_time_ms
        )
        
        # Notify handlers
        for handler in self.fill_handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(order, fill, OrderStatus.PARTIALLY_FILLED)
                else:
                    handler(order, fill, OrderStatus.PARTIALLY_FILLED)
            except Exception as e:
                logger.error(f"Error in fill handler: {e}", exc_info=True)
        
        logger.info(f"Handling partial fill for order {order_id}: {fill.quantity}/{order.amount} @ {fill.price}")
        
        # Calculate remaining amount
        remaining_amount = order.amount - fill.quantity
        if remaining_amount <= 0:
            logger.warning(f"No remaining amount for order {order_id}, treating as fully filled")
            return True
        
        # Track partial order if not already tracked
        async with self._lock:
            if order_id not in self.partial_orders:
                self.partial_orders[order_id] = {
                    "order": order,
                    "filled_amount": fill.quantity,
                    "remaining_amount": remaining_amount,
                    "original_amount": order.amount,
                    "fill_price": fill.price,
                    "strategy": strategy,
                    "retries": 0,
                    "retry_orders": [],
                    "start_time": int(time.time() * 1000),
                    "last_update": int(time.time() * 1000),
                    "status": "active"
                }
        
        # Handle based on strategy
        if strategy == FillStrategy.ACCEPT_PARTIAL:
            return await self._handle_accept_partial(order_id)
        elif strategy == FillStrategy.RETRY_REMAINING:
            return await self._handle_retry_remaining(order_id)
        elif strategy == FillStrategy.WAIT_FOR_COMPLETE:
            return await self._handle_wait_for_complete(order_id)
        elif strategy == FillStrategy.CANCEL_AND_RETRY:
            return await self._handle_cancel_and_retry(order_id)
        else:
            logger.error(f"Unknown fill strategy: {strategy}")
            return False
    
    async def update_partial_fill(self, order_id: str, fill: OrderFill) -> bool:
        """
        Update a partial fill with additional fill information.
        
        Args:
            order_id: Order ID
            fill: New fill information
            
        Returns:
            bool: True if updated successfully, False otherwise
        """
        async with self._lock:
            if order_id not in self.partial_orders:
                logger.warning(f"Order {order_id} not found in partial orders")
                return False
            
            partial_order = self.partial_orders[order_id]
            order = partial_order["order"]
            
            # Update filled amount
            prev_filled = partial_order["filled_amount"]
            partial_order["filled_amount"] += fill.quantity
            partial_order["remaining_amount"] -= fill.quantity
            partial_order["last_update"] = int(time.time() * 1000)
            
            # Calculate weighted average fill price
            total_filled = partial_order["filled_amount"]
            if total_filled > 0:
                partial_order["fill_price"] = (
                    (prev_filled * partial_order["fill_price"]) + (fill.quantity * fill.price)
                ) / total_filled
            
            logger.info(f"Updated partial fill for order {order_id}: {total_filled}/{order.amount} @ {partial_order['fill_price']}")
            
            # Check if fully filled
            if partial_order["remaining_amount"] <= 0:
                partial_order["status"] = "completed"
                logger.info(f"Order {order_id} now fully filled")
                
                # Record stats
                execution_time_ms = partial_order["last_update"] - partial_order["start_time"]
                
                self.stats.record_order(
                    order=order,
                    fill_status=OrderStatus.FILLED,
                    fill_amount=total_filled,
                    expected_amount=order.amount,
                    fill_price=partial_order["fill_price"],
                    expected_price=order.price or 0.0,
                    execution_time_ms=execution_time_ms
                )
                
                # Notify handlers
                final_fill = OrderFill(
                    timestamp=int(time.time() * 1000),
                    price=partial_order["fill_price"],
                    quantity=total_filled,
                    fee=0.0
                )
                
                for handler in self.fill_handlers:
                    try:
                        if asyncio.iscoroutinefunction(handler):
                            await handler(order, final_fill, OrderStatus.FILLED)
                        else:
                            handler(order, final_fill, OrderStatus.FILLED)
                    except Exception as e:
                        logger.error(f"Error in fill handler: {e}", exc_info=True)
                
                # Remove from tracking
                del self.partial_orders[order_id]
            
            return True
    
    async def _handle_accept_partial(self, order_id: str) -> bool:
        """
        Handle accept partial strategy.
        
        Args:
            order_id: Order ID
            
        Returns:
            bool: True if handled successfully, False otherwise
        """
        async with self._lock:
            if order_id not in self.partial_orders:
                return False
            
            partial_order = self.partial_orders[order_id]
            
            # Mark as completed
            partial_order["status"] = "completed"
            logger.info(f"Accepted partial fill for order {order_id}")
            
            # Notify handlers of completion with partial amount
            order = partial_order["order"]
            filled_amount = partial_order["filled_amount"]
            fill_price = partial_order["fill_price"]
            
            # Create a completed fill notification
            final_fill = OrderFill(
                timestamp=int(time.time() * 1000),
                price=fill_price,
                quantity=filled_amount,
                fee=0.0
            )
            
            for handler in self.fill_handlers:
                try:
                    if asyncio.iscoroutinefunction(handler):
                        await handler(order, final_fill, OrderStatus.FILLED)
                    else:
                        handler(order, final_fill, OrderStatus.FILLED)
                except Exception as e:
                    logger.error(f"Error in fill handler: {e}", exc_info=True)
            
            # Remove from tracking
            del self.partial_orders[order_id]
            
            return True
    
    async def _handle_retry_remaining(self, order_id: str) -> bool:
        """
        Handle retry remaining strategy.
        
        Args:
            order_id: Order ID
            
        Returns:
            bool: True if handled successfully, False otherwise
        """
        partial_order = None
        
        async with self._lock:
            if order_id not in self.partial_orders:
                return False
            
            partial_order = self.partial_orders[order_id]
            
            # Check retry count
            if partial_order["retries"] >= self.max_retries:
                logger.warning(f"Max retries reached for order {order_id}, accepting partial fill")
                return await self._handle_accept_partial(order_id)
            
            # Increment retry count
            partial_order["retries"] += 1
            self.stats.record_retry()
        
        # Get original order details
        order = partial_order["order"]
        remaining_amount = partial_order["remaining_amount"]
        
        # Create new order for remaining amount
        try:
            if not self.exchange_client:
                logger.error("No exchange client registered")
                return False
            
            # Create retry order
            retry_order = await self.exchange_client.create_order(
                symbol=order.symbol,
                order_type=order.order_type,
                side=order.side,
                amount=remaining_amount,
                price=order.price,
                params={
                    "original_order_id": order_id,
                    "retry_count": partial_order["retries"],
                    "is_retry": True
                }
            )
            
            async with self._lock:
                if order_id in self.partial_orders:
                    # Store retry order
                    partial_order["retry_orders"].append(retry_order.order_id)
                    logger.info(f"Created retry order {retry_order.order_id} for remaining amount {remaining_amount}")
                else:
                    # Order was removed while we were creating retry
                    logger.warning(f"Original order {order_id} no longer in tracking")
            
            return True
            
        except Exception as e:
            logger.error(f"Error creating retry order: {e}", exc_info=True)
            
            # Mark as failed
            async with self._lock:
                if order_id in self.partial_orders:
                    partial_order["status"] = "failed"
            
            return False
    
    async def _handle_wait_for_complete(self, order_id: str) -> bool:
        """
        Handle wait for complete strategy.
        
        This starts a background task to monitor the order status.
        
        Args:
            order_id: Order ID
            
        Returns:
            bool: True if handled successfully, False otherwise
        """
        async with self._lock:
            if order_id not in self.partial_orders:
                return False
            
            partial_order = self.partial_orders[order_id]
            
            # Start monitoring task if exchange client is available
            if self.exchange_client:
                # Create monitoring task
                asyncio.create_task(self._monitor_order_completion(order_id))
                logger.info(f"Started monitoring task for order {order_id}")
                return True
            else:
                logger.error("No exchange client registered, cannot monitor order")
                return False
    
    async def _handle_cancel_and_retry(self, order_id: str) -> bool:
        """
        Handle cancel and retry strategy.
        
        Args:
            order_id: Order ID
            
        Returns:
            bool: True if handled successfully, False otherwise
        """
        partial_order = None
        
        async with self._lock:
            if order_id not in self.partial_orders:
                return False
            
            partial_order = self.partial_orders[order_id]
            
            # Check retry count
            if partial_order["retries"] >= self.max_retries:
                logger.warning(f"Max retries reached for order {order_id}, accepting partial fill")
                return await self._handle_accept_partial(order_id)
            
            # Increment retry count
            partial_order["retries"] += 1
            self.stats.record_retry()
        
        # Get original order details
        order = partial_order["order"]
        remaining_amount = partial_order["remaining_amount"]
        
        try:
            if not self.exchange_client:
                logger.error("No exchange client registered")
                return False
            
            # Cancel original order
            cancel_success = await self.exchange_client.cancel_order(order_id, order.symbol)
            
            if not cancel_success:
                logger.warning(f"Failed to cancel original order {order_id}")
                # Continue anyway
            
            # Create new order for remaining amount
            retry_order = await self.exchange_client.create_order(
                symbol=order.symbol,
                order_type=order.order_type,
                side=order.side,
                amount=remaining_amount,
                price=order.price,
                params={
                    "original_order_id": order_id,
                    "retry_count": partial_order["retries"],
                    "is_retry": True
                }
            )
            
            async with self._lock:
                if order_id in self.partial_orders:
                    # Store retry order
                    partial_order["retry_orders"].append(retry_order.order_id)
                    logger.info(f"Cancelled original order {order_id} and created retry order {retry_order.order_id}")
                else:
                    # Order was removed while we were creating retry
                    logger.warning(f"Original order {order_id} no longer in tracking")
            
            return True
            
        except Exception as e:
            logger.error(f"Error in cancel and retry: {e}", exc_info=True)
            
            # Mark as failed
            async with self._lock:
                if order_id in self.partial_orders:
                    partial_order["status"] = "failed"
            
            return False
    
    async def _monitor_order_completion(self, order_id: str) -> None:
        """
        Monitor order for completion.
        
        Args:
            order_id: Order ID
        """
        try:
            start_time = int(time.time() * 1000)
            check_interval = self.check_interval_ms / 1000  # Convert to seconds
            
            while True:
                # Check if order still exists in tracking
                async with self._lock:
                    if order_id not in self.partial_orders:
                        logger.info(f"Order {order_id} no longer in tracking, stopping monitoring")
                        return
                    
                    partial_order = self.partial_orders[order_id]
                    
                    # Check if timed out
                    current_time = int(time.time() * 1000)
                    if current_time - start_time > self.max_wait_time_ms:
                        logger.warning(f"Wait time exceeded for order {order_id}, switching to retry strategy")
                        await self._handle_retry_remaining(order_id)
                        return
                
                # Check order status
                try:
                    if not self.exchange_client:
                        logger.error("No exchange client registered")
                        return
                    
                    order_info = await self.exchange_client.get_order(order_id, partial_order["order"].symbol)
                    
                    if not order_info:
                        logger.warning(f"Order {order_id} not found on exchange")
                        await asyncio.sleep(check_interval)
                        continue
                    
                    # Update if order status has changed
                    if order_info.status != OrderStatus.PARTIALLY_FILLED:
                        if order_info.status == OrderStatus.FILLED:
                            # Order is now fully filled
                            logger.info(f"Order {order_id} now fully filled")
                            
                            # Create a final fill notification
                            final_fill = OrderFill(
                                timestamp=int(time.time() * 1000),
                                price=order_info.average_price or partial_order["fill_price"],
                                quantity=order_info.amount,
                                fee=0.0
                            )
                            
                            await self.update_partial_fill(order_id, final_fill)
                            return
                        elif order_info.status in [OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.EXPIRED]:
                            # Order was cancelled or rejected
                            logger.warning(f"Order {order_id} was {order_info.status.value}, switching to retry strategy")
                            await self._handle_retry_remaining(order_id)
                            return
                    
                    # Still partially filled, check filled amount
                    if order_info.filled != partial_order["filled_amount"]:
                        # Additional filling has occurred
                        additional_fill = OrderFill(
                            order_id=order_id,
                            amount=order_info.filled - partial_order["filled_amount"],
                            price=order_info.average_price or partial_order["fill_price"],
                            timestamp=int(time.time() * 1000)
                        )
                        
                        await self.update_partial_fill(order_id, additional_fill)
                        
                        # If fully filled now, we can stop monitoring
                        async with self._lock:
                            if order_id not in self.partial_orders:
                                return
                
                except Exception as e:
                    logger.error(f"Error checking order status: {e}", exc_info=True)
                
                # Wait before next check
                await asyncio.sleep(check_interval)
                
        except Exception as e:
            logger.error(f"Error in order monitoring task: {e}", exc_info=True)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get execution statistics.
        
        Returns:
            Dict[str, Any]: Execution statistics
        """
        return self.stats.get_stats()
        
    async def get_partial_orders(self) -> List[Dict[str, Any]]:
        """
        Get all currently tracked partial orders.
        
        Returns:
            List[Dict[str, Any]]: List of partial orders
        """
        async with self._lock:
            return list(self.partial_orders.values()) 