"""
Mock Execution Engine

This module provides a simulated execution engine for integration testing.
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
from trading_system.tests.integration.mocks.mock_order import Order, OrderFill, OrderStatus, OrderSide

logger = logging.getLogger("integration.mock_execution")


class MockExecutionEngine(Component):
    """
    Mock execution engine for integration testing.
    
    This component simulates an execution engine that processes order requests
    and generates fills based on configurable execution behavior for testing
    system behavior.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the mock execution engine.
        
        Args:
            config: Configuration settings
        """
        super().__init__("mock_execution")
        self.config = config or {}
        
        # Execution state
        self.active = False
        self.orders: Dict[str, Order] = {}  # Active orders by order_id
        self.completed_orders: Dict[str, Order] = {}  # Completed orders
        
        # Execution behavior configuration
        self.execution_config = self.config.get("execution", {
            "fill_delay_seconds": {
                "min": 0.5,
                "max": 2.0
            },
            "fill_probability": 0.95,  # Probability of successful fill
            "fill_slippage": {
                "mean": 0.0,  # Mean slippage (0 = exact price)
                "std_dev": 0.001  # Standard deviation of slippage (0.1%)
            },
            "partial_fill_probability": 0.3,  # Probability of partial fills
            "error_probability": 0.05,  # Probability of execution error
            "rejection_probability": 0.02  # Probability of order rejection
        })
        
        # Execution metrics
        self.metrics = {
            "orders_received": 0,
            "orders_filled": 0,
            "orders_rejected": 0,
            "orders_cancelled": 0,
            "orders_failed": 0,
            "total_fill_value": 0.0
        }
        
        # Runtime variables
        self.execution_task = None
        self.async_message_bus = None
    
    async def initialize(self):
        """Initialize the mock execution engine."""
        logger.info("Initializing mock execution engine")
        self.async_message_bus = get_async_message_bus()
        
        # Subscribe to order events
        await self.async_message_bus.subscribe(
            MessageTypes.ORDER_REQUEST,
            self._handle_order_request
        )
        
        await self.async_message_bus.subscribe(
            MessageTypes.ORDER_CANCEL_REQUEST,
            self._handle_cancel_request
        )
        
        self.status = ComponentStatus.INITIALIZED
        return self.status
    
    async def start(self):
        """Start the mock execution engine."""
        logger.info("Starting mock execution engine")
        
        # Start the execution processing task
        self.active = True
        self.execution_task = asyncio.create_task(self._execution_loop())
        
        self.status = ComponentStatus.OPERATIONAL
        return self.status
    
    async def stop(self):
        """Stop the mock execution engine."""
        logger.info("Stopping mock execution engine")
        
        # Stop the execution processing task
        self.active = False
        if self.execution_task:
            self.execution_task.cancel()
            try:
                await self.execution_task
            except asyncio.CancelledError:
                pass
            self.execution_task = None
        
        self.status = ComponentStatus.SHUTDOWN
        return self.status
    
    async def get_status(self):
        """Get the current status of the mock execution engine."""
        return self.status
    
    async def _execution_loop(self):
        """Main loop for processing order execution."""
        try:
            while self.active:
                # Process any pending order fills
                # (This is just for continuous background processing,
                # most fills are processed directly in response to order requests)
                
                # Simply sleep, since fills are done in response to requests
                await asyncio.sleep(0.1)
                
        except asyncio.CancelledError:
            logger.info("Execution loop cancelled")
            raise
        except Exception as e:
            logger.error(f"Error in execution loop: {str(e)}", exc_info=True)
            self.status = ComponentStatus.ERROR
    
    async def _handle_order_request(self, message_type, order):
        """
        Handle an order request.
        
        Args:
            message_type: Message type
            order: Order object
        """
        logger.info(f"Received order request: {order.id}, {order.symbol}, {order.side}, {order.quantity}")
        
        # Update metrics
        self.metrics["orders_received"] += 1
        
        # Process the order (in a separate task to avoid blocking)
        asyncio.create_task(self._process_order(order))
    
    async def _handle_cancel_request(self, message_type, cancel_request):
        """
        Handle a cancel request.
        
        Args:
            message_type: Message type
            cancel_request: Cancel request
        """
        order_id = cancel_request.order_id
        logger.info(f"Received cancel request for order: {order_id}")
        
        if order_id in self.orders:
            order = self.orders[order_id]
            
            # Check if order can be cancelled
            if order.status in [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED]:
                # Cancel the order
                order.status = OrderStatus.CANCELLED
                
                # Move to completed orders
                self.completed_orders[order_id] = order
                del self.orders[order_id]
                
                # Update metrics
                self.metrics["orders_cancelled"] += 1
                
                # Publish cancel confirmation
                await self._publish_order_update(order)
                
                logger.info(f"Order {order_id} cancelled successfully")
            else:
                logger.info(f"Cannot cancel order {order_id} with status {order.status}")
        else:
            logger.warning(f"Order {order_id} not found for cancellation")
    
    async def _process_order(self, order: Order):
        """
        Process an order.
        
        Args:
            order: Order object
        """
        # Check for immediate rejection
        if random.random() < self.execution_config["rejection_probability"]:
            order.status = OrderStatus.REJECTED
            
            # Update metrics
            self.metrics["orders_rejected"] += 1
            
            # Publish rejection
            await self._publish_order_update(order)
            
            logger.info(f"Order {order.id} rejected")
            return
        
        # Add the order to active orders
        self.orders[order.id] = order
        
        # Mark as new and publish update
        order.status = OrderStatus.NEW
        await self._publish_order_update(order)
        
        # Determine fill delay
        fill_delay = random.uniform(
            self.execution_config["fill_delay_seconds"]["min"],
            self.execution_config["fill_delay_seconds"]["max"]
        )
        
        logger.info(f"Processing order {order.id} with fill delay {fill_delay:.2f}s")
        
        # Wait for the fill delay
        await asyncio.sleep(fill_delay)
        
        # Check for execution error
        if random.random() < self.execution_config["error_probability"]:
            order.status = OrderStatus.FAILED
            
            # Update metrics
            self.metrics["orders_failed"] += 1
            
            # Publish failure
            await self._publish_order_update(order)
            
            logger.info(f"Order {order.id} failed during execution")
            return
        
        # Determine if we need partial fills
        if random.random() < self.execution_config["partial_fill_probability"]:
            # Generate partial fills
            fill_count = random.randint(2, 3)  # 2-3 partial fills
            remaining_qty = order.quantity
            
            for i in range(fill_count):
                # For the last fill, use all remaining quantity
                if i == fill_count - 1:
                    fill_qty = remaining_qty
                else:
                    # Random portion of remaining quantity
                    fill_qty = remaining_qty * random.uniform(0.3, 0.7)
                    fill_qty = round(fill_qty, 8)  # Round to 8 decimal places
                
                remaining_qty -= fill_qty
                
                # Calculate fill price with slippage
                fill_price = self._calculate_fill_price_with_slippage(order)
                
                # Create fill
                fill = OrderFill(
                    timestamp=datetime.datetime.now(),
                    quantity=fill_qty,
                    price=fill_price,
                    fee=fill_qty * fill_price * 0.001  # 0.1% fee
                )
                
                # Add fill to order
                order.add_fill(fill)
                
                # Update order status
                if remaining_qty > 0:
                    order.status = OrderStatus.PARTIALLY_FILLED
                else:
                    order.status = OrderStatus.FILLED
                
                # Publish order update with fill
                await self._publish_order_update(order)
                await self._publish_fill(order, fill)
                
                logger.info(f"Order {order.id} {order.status.value} with {fill_qty} @ {fill_price}")
                
                # Short delay between partial fills
                if i < fill_count - 1:
                    await asyncio.sleep(random.uniform(0.5, 1.5))
        else:
            # Single fill for the entire quantity
            fill_price = self._calculate_fill_price_with_slippage(order)
            
            # Create fill
            fill = OrderFill(
                timestamp=datetime.datetime.now(),
                quantity=order.quantity,
                price=fill_price,
                fee=order.quantity * fill_price * 0.001  # 0.1% fee
            )
            
            # Add fill to order
            order.add_fill(fill)
            
            # Update order status
            order.status = OrderStatus.FILLED
            
            # Publish order update with fill
            await self._publish_order_update(order)
            await self._publish_fill(order, fill)
            
            logger.info(f"Order {order.id} filled with {order.quantity} @ {fill_price}")
        
        # If order is complete, move to completed orders
        if order.status in [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.FAILED]:
            self.completed_orders[order.id] = order
            if order.id in self.orders:
                del self.orders[order.id]
            
            # Update metrics
            if order.status == OrderStatus.FILLED:
                self.metrics["orders_filled"] += 1
                for fill in order.fills:
                    self.metrics["total_fill_value"] += fill.quantity * fill.price
    
    def _calculate_fill_price_with_slippage(self, order: Order):
        """
        Calculate fill price with slippage.
        
        Args:
            order: Order object
            
        Returns:
            Fill price with slippage
        """
        # Base price from order
        base_price = order.price
        
        # If limit order with no price, use market price
        if base_price is None or base_price == 0:
            # In a real system, we would use the current market price
            # For simulation, just use a random price around 50000 (like BTC)
            base_price = random.uniform(45000, 55000)
        
        # Calculate slippage
        mean_slippage = self.execution_config["fill_slippage"]["mean"]
        std_dev = self.execution_config["fill_slippage"]["std_dev"]
        
        # Slippage is percentage of price
        slippage_pct = random.normalvariate(mean_slippage, std_dev)
        
        # Apply slippage (positive is worse for buyer, negative is worse for seller)
        if order.side == OrderSide.BUY:
            slippage_factor = 1 + slippage_pct
        else:  # SELL
            slippage_factor = 1 - slippage_pct
        
        fill_price = base_price * slippage_factor
        
        # Round to reasonable precision
        fill_price = round(fill_price, 2)
        
        return fill_price
    
    async def _publish_order_update(self, order: Order):
        """
        Publish an order update.
        
        Args:
            order: Order object
        """
        try:
            await self.async_message_bus.publish(
                MessageTypes.ORDER_UPDATE,
                order
            )
        except Exception as e:
            logger.error(f"Error publishing order update: {str(e)}", exc_info=True)
    
    async def _publish_fill(self, order: Order, fill: OrderFill):
        """
        Publish a fill event.
        
        Args:
            order: Order object
            fill: Fill object
        """
        try:
            # Create a message with both order and fill info
            fill_message = {
                "order_id": order.id,
                "symbol": order.symbol,
                "side": order.side.value,
                "fill": fill.to_dict()
            }
            
            await self.async_message_bus.publish(
                MessageTypes.FILL,
                fill_message
            )
        except Exception as e:
            logger.error(f"Error publishing fill: {str(e)}", exc_info=True)
    
    # Simulation control methods
    
    def set_execution_parameter(self, param_path: str, value: Any):
        """
        Set an execution parameter.
        
        Args:
            param_path: Parameter path (dot-separated)
            value: Parameter value
        """
        # Split the path into components
        components = param_path.split(".")
        
        # Navigate to the right location
        target = self.execution_config
        for comp in components[:-1]:
            if comp in target:
                target = target[comp]
            else:
                logger.error(f"Invalid parameter path: {param_path}")
                return
        
        # Set the value
        target[components[-1]] = value
        logger.info(f"Set execution parameter {param_path} to {value}")
    
    def get_metrics(self):
        """Get execution metrics."""
        return self.metrics
    
    def get_order(self, order_id: str) -> Optional[Order]:
        """
        Get an order by ID.
        
        Args:
            order_id: Order ID
            
        Returns:
            Order if found, None otherwise
        """
        if order_id in self.orders:
            return self.orders[order_id]
        elif order_id in self.completed_orders:
            return self.completed_orders[order_id]
        else:
            return None
    
    def get_active_orders(self) -> List[Order]:
        """
        Get all active orders.
        
        Returns:
            List of active orders
        """
        return list(self.orders.values())
    
    def get_completed_orders(self) -> List[Order]:
        """
        Get all completed orders.
        
        Returns:
            List of completed orders
        """
        return list(self.completed_orders.values()) 