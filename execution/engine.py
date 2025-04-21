"""
Execution Engine Implementation.

This module implements a core execution engine for processing orders with fault tolerance
and implementing a three-phase commit workflow for order processing.
"""

import asyncio
import json
import os
import time
import uuid
import threading
from datetime import datetime, timedelta
from enum import Enum, auto
from pathlib import Path
from typing import Dict, List, Optional, Any, Set, Tuple, Callable

from trading_system.core.component import Component, ComponentStatus
from trading_system.core.logging import get_logger
from trading_system.execution.orders import Order, OrderStatus, OrderSide, OrderType, OrderTimeInForce, OrderFill


class OrderProcessingStage(Enum):
    """
    Processing stages for orders in the execution engine.
    
    Implements the three-phase commit protocol:
    1. PREPARE: Validate and prepare the order
    2. COMMIT: Submit the order to the exchange
    3. VERIFY: Verify the order execution
    """
    PREPARE = "prepare"  # Validate and prepare order
    COMMIT = "commit"    # Submit order to exchange
    VERIFY = "verify"    # Verify order on exchange
    RECOVER = "recover"  # Handle recovery


class ExecutionEngine(Component):
    """
    Main execution engine for processing orders.
    
    Implements a three-phase commit workflow for order processing, including:
    - Order validation
    - Order submission
    - Order tracking
    - Error recovery
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize execution engine.
        
        Args:
            config: Execution engine configuration
        """
        super().__init__("execution_engine", config)
        
        # Initialize logger
        self.logger = get_logger("execution.engine")
        
        # Get configuration
        engine_config = config.get("execution_engine", {})
        
        # Exchange adapter
        exchange_adapter = engine_config.get("exchange_adapter", "nobitex")
        self.exchange = None  # Will be set when component is started
        
        # Order state storage
        self.orders: Dict[str, Order] = {}
        self.active_orders: Set[str] = set()
        self.pending_orders: Dict[str, Order] = {}
        self.completed_orders: Dict[str, Order] = {}
        self.retry_queue: List[str] = []
        self.execution_lock = threading.Lock()
        
        # Processing state
        self.order_queue = []
        self.queue_lock = threading.Lock()
        self.processing = False
        self.processing_thread = None
        
        # Circuit breaker state
        self.circuit_breaker_enabled = engine_config.get("circuit_breaker_enabled", True)
        self.failure_threshold = engine_config.get("failure_threshold", 3)
        self.circuit_open_timeout = engine_config.get("circuit_open_timeout", 300)  # seconds
        self.consecutive_failures = 0
        self.circuit_open_until = 0
        
        # Performance tracking
        self.execution_times: List[float] = []
        self.success_count = 0
        self.failure_count = 0
        
        # Order persistence
        self.persistence_enabled = engine_config.get("persistence_enabled", True)
        self.persistence_path = engine_config.get("persistence_path", "data/orders")
        self.persistence_interval = engine_config.get("persistence_interval", 60)  # seconds
        self.last_persistence_time = 0
        
        # Order callbacks
        self.order_callbacks: Dict[str, List[Callable[[Order], None]]] = {}
        
        # For recovery
        self.recovered = False
        
        self.logger.info("Execution engine initialized")
    
    def start(self) -> None:
        """Start the execution engine."""
        if self.status == ComponentStatus.RUNNING:
            return
        
        self.logger.info("Starting execution engine")
        
        # Initialize exchange adapter
        exchange_adapter = self.config.get("execution_engine", {}).get("exchange_adapter", "mock")
        
        if exchange_adapter == "mock":
            from trading_system.exchange.mock_exchange import MockExchangeAdapter
            self.exchange = MockExchangeAdapter(self.config.get("exchange", {}))
        elif exchange_adapter == "nobitex":
            try:
                from trading_system.exchange.nobitex_adapter import NobitexExchangeAdapter
                self.logger.info("Initializing Nobitex exchange adapter")
                self.exchange = NobitexExchangeAdapter(self.config.get("exchange", {}))
                self.logger.info("Nobitex exchange adapter initialized successfully")
            except Exception as e:
                self.logger.error(f"Failed to initialize Nobitex exchange adapter: {str(e)}")
                self.logger.warning("Falling back to mock exchange adapter")
                from trading_system.exchange.mock_exchange import MockExchangeAdapter
                self.exchange = MockExchangeAdapter(self.config.get("exchange", {}))
        else:
            self.logger.error(f"Unknown exchange adapter: {exchange_adapter}")
            self.status = ComponentStatus.ERROR
            return
        
        # Ensure persistence directory exists
        if self.persistence_enabled:
            os.makedirs(self.persistence_path, exist_ok=True)
            self._recover_orders()
        
        # Start processing
        self.processing = True
        self.processing_thread = threading.Thread(target=self._process_orders, daemon=True)
        self.processing_thread.start()
        
        self.status = ComponentStatus.RUNNING
        self.logger.info("Execution engine started")
    
    def stop(self) -> None:
        """Stop the execution engine."""
        if self.status != ComponentStatus.RUNNING:
            return
        
        self.logger.info("Stopping execution engine")
        
        # Stop processing
        self.processing = False
        if self.processing_thread:
            self.processing_thread.join(timeout=5.0)
            if self.processing_thread.is_alive():
                self.logger.warning("Processing thread did not terminate gracefully")
        
        # Persist orders one last time
        if self.persistence_enabled:
            self._persist_orders()
        
        self.status = ComponentStatus.STOPPED
        self.logger.info("Execution engine stopped")
    
    def submit_order(self, order: Order) -> str:
        """
        Submit an order for execution.
        
        Args:
            order: The order to submit
            
        Returns:
            The order ID
        """
        # Set initial order state
        order.update_status(OrderStatus.CREATED)
        
        # Store order
        self.orders[order.order_id] = order
        self.active_orders.add(order.order_id)
        
        # Add to processing queue
        with self.queue_lock:
            self.order_queue.append(order.order_id)
        
        self.logger.info(f"Order {order.order_id} queued for execution: {order.symbol} {order.side.value} {order.quantity}")
        
        return order.order_id
    
    def cancel_order(self, order_id: str) -> bool:
        """
        Cancel an order.
        
        Args:
            order_id: Order ID to cancel
            
        Returns:
            True if cancel request was successful
        """
        if order_id not in self.orders:
            self.logger.warning(f"Attempted to cancel unknown order: {order_id}")
            return False
        
        order = self.orders[order_id]
        
        # If order is not in a cancelable state
        if order.status not in (OrderStatus.SUBMITTED, OrderStatus.PARTIALLY_FILLED):
            self.logger.warning(f"Order {order_id} cannot be canceled in status {order.status.value}")
            return False
        
        try:
            # Attempt to cancel on the exchange
            if self.exchange:
                result = self.exchange.cancel_order(order)
                
                if result:
                    order.cancel("Canceled by request")
                    self.active_orders.discard(order_id)
                    self.completed_orders[order_id] = order
                    self._persist_order(order)
                    
                    # Trigger callbacks
                    self._trigger_callbacks(order)
                    
                    self.logger.info(f"Order {order_id} successfully canceled")
                    return True
                else:
                    self.logger.error(f"Failed to cancel order {order_id}")
                    return False
            else:
                self.logger.error("Exchange adapter not initialized")
                return False
                
        except Exception as e:
            self.logger.exception(f"Error canceling order {order_id}: {str(e)}")
            return False
    
    def get_order(self, order_id: str) -> Optional[Order]:
        """
        Get an order by ID.
        
        Args:
            order_id: Order ID
            
        Returns:
            Order if found, None otherwise
        """
        return self.orders.get(order_id)
    
    def get_active_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """
        Get all active orders.
        
        Args:
            symbol: Optional symbol to filter by
            
        Returns:
            List of active orders
        """
        result = []
        for order_id in self.active_orders:
            order = self.orders.get(order_id)
            if order and (symbol is None or order.symbol == symbol):
                result.append(order)
        return result
    
    def register_callback(self, order_id: str, callback: Callable[[Order], None]) -> None:
        """
        Register a callback for order updates.
        
        Args:
            order_id: Order ID
            callback: Callback function
        """
        if order_id not in self.order_callbacks:
            self.order_callbacks[order_id] = []
        
        self.order_callbacks[order_id].append(callback)
    
    def _trigger_callbacks(self, order: Order) -> None:
        """
        Trigger callbacks for an order.
        
        Args:
            order: Order that was updated
        """
        callbacks = self.order_callbacks.get(order.order_id, [])
        for callback in callbacks:
            try:
                callback(order)
            except Exception as e:
                self.logger.error(f"Error in order callback: {str(e)}")
    
    def _process_orders(self) -> None:
        """Process orders from the queue."""
        self.logger.info("Order processing started")
        
        while self.processing:
            try:
                # Get next order from queue
                order_id = None
                with self.queue_lock:
                    if self.order_queue:
                        order_id = self.order_queue.pop(0)
                
                if not order_id:
                    # Check if we need to persist orders
                    if (self.persistence_enabled and 
                        time.time() - self.last_persistence_time > self.persistence_interval):
                        self._persist_orders()
                    
                    # Nothing to do, sleep briefly
                    time.sleep(0.1)
                    continue
                
                # Check if circuit breaker is open
                if self._is_circuit_open():
                    self.logger.warning(f"Circuit breaker open, requeuing order {order_id}")
                    # Requeue the order for later
                    with self.queue_lock:
                        self.order_queue.append(order_id)
                    time.sleep(1.0)
                    continue
                
                # Get the order
                order = self.orders.get(order_id)
                if not order:
                    self.logger.error(f"Order {order_id} not found in order store")
                    continue
                
                # Process the order
                start_time = time.time()
                success = self._execute_order(order)
                execution_time = time.time() - start_time
                
                # Track performance
                self.execution_times.append(execution_time)
                if len(self.execution_times) > 100:
                    self.execution_times = self.execution_times[-100:]
                
                if success:
                    self.success_count += 1
                    self.consecutive_failures = 0
                else:
                    self.failure_count += 1
                    self.consecutive_failures += 1
                    
                    # Check if we need to open the circuit breaker
                    if (self.circuit_breaker_enabled and 
                        self.consecutive_failures >= self.failure_threshold):
                        self._open_circuit_breaker()
                
            except Exception as e:
                self.logger.exception(f"Error in order processing: {str(e)}")
                time.sleep(1.0)  # Prevent tight loop in case of persistent errors
        
        self.logger.info("Order processing stopped")
    
    def _execute_order(self, order: Order) -> bool:
        """
        Execute an order through the three-phase commit process.
        
        Args:
            order: Order to execute
            
        Returns:
            True if order was successfully processed
        """
        # Get a lock for this order execution
        with self.execution_lock:
            try:
                self.logger.info(f"Processing order {order.order_id}: {order.symbol} {order.side.value} {order.quantity}")
                
                # Phase 1: Prepare (Validate)
                if not self._prepare_order(order):
                    self._trigger_callbacks(order)
                    return False
                
                # Phase 2: Commit (Submit to exchange)
                if not self._commit_order(order):
                    self._trigger_callbacks(order)
                    return False
                
                # Phase 3: Verify (Check order status)
                if not self._verify_order(order):
                    # Verification failure is not a terminal state
                    # We'll retry later
                    with self.queue_lock:
                        self.order_queue.append(order.order_id)
                    return False
                
                # Order successfully processed
                self.logger.info(f"Order {order.order_id} successfully processed")
                
                # Update state
                if order.status in (OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED):
                    self.active_orders.discard(order.order_id)
                    self.completed_orders[order.order_id] = order
                
                # Persist order
                if self.persistence_enabled:
                    self._persist_order(order)
                
                # Trigger callbacks
                self._trigger_callbacks(order)
                
                return True
                
            except Exception as e:
                self.logger.exception(f"Error executing order {order.order_id}: {str(e)}")
                order.fail(f"Execution error: {str(e)}")
                
                # Add to retry queue if retryable
                if order.can_retry():
                    with self.queue_lock:
                        self.retry_queue.append(order.order_id)
                    self.logger.info(f"Order {order.order_id} added to retry queue")
                else:
                    self.active_orders.discard(order.order_id)
                    self.completed_orders[order.order_id] = order
                
                # Persist order
                if self.persistence_enabled:
                    self._persist_order(order)
                
                # Trigger callbacks
                self._trigger_callbacks(order)
                
                return False
    
    def _prepare_order(self, order: Order) -> bool:
        """
        Phase 1: Prepare order for execution.
        
        Validates the order and ensures all required fields are present.
        Sets order state to READY if validation passes.
        
        Args:
            order: Order to prepare
            
        Returns:
            True if order was successfully prepared
        """
        try:
            self.logger.debug(f"Preparing order {order.order_id}")
            
            # Update status
            order.update_status(OrderStatus.VALIDATING)
            
            # Basic validation
            if not order.symbol:
                order.reject("Missing symbol")
                return False
            
            if order.quantity <= 0:
                order.reject("Invalid quantity")
                return False
            
            # Validate with exchange
            if self.exchange:
                validation_result = self.exchange.validate_order(order)
                if not validation_result.get("valid", False):
                    order.reject(validation_result.get("reason", "Order validation failed"))
                    return False
                
                # Update order with any exchange-specific data
                if "metadata" in validation_result:
                    order.metadata.update(validation_result["metadata"])
            else:
                self.logger.error("Exchange adapter not initialized")
                order.fail("Exchange adapter not initialized")
                return False
            
            # Set as ready
            order.update_status(OrderStatus.READY)
            self.logger.debug(f"Order {order.order_id} prepared successfully")
            return True
            
        except Exception as e:
            self.logger.exception(f"Error preparing order {order.order_id}: {str(e)}")
            order.fail(f"Preparation error: {str(e)}")
            return False
    
    def _commit_order(self, order: Order) -> bool:
        """
        Phase 2: Commit order to exchange.
        
        Submits the order to the exchange and updates order state.
        
        Args:
            order: Order to commit
            
        Returns:
            True if order was successfully committed
        """
        try:
            self.logger.debug(f"Committing order {order.order_id}")
            
            # Update status
            order.update_status(OrderStatus.SUBMITTING)
            
            # Submit to exchange
            if self.exchange:
                result = self.exchange.submit_order(order)
                
                if not result.get("success", False):
                    order.reject(result.get("reason", "Order submission failed"))
                    return False
                
                # Update order with exchange data
                if "exchange_order_id" in result:
                    order.exchange_order_id = result["exchange_order_id"]
                
                if "metadata" in result:
                    order.metadata.update(result["metadata"])
            else:
                self.logger.error("Exchange adapter not initialized")
                order.fail("Exchange adapter not initialized")
                return False
            
            # Update status
            order.update_status(OrderStatus.SUBMITTED)
            self.logger.debug(f"Order {order.order_id} committed successfully")
            return True
            
        except Exception as e:
            self.logger.exception(f"Error committing order {order.order_id}: {str(e)}")
            order.fail(f"Submission error: {str(e)}")
            return False
    
    def _verify_order(self, order: Order) -> bool:
        """
        Phase 3: Verify order on exchange.
        
        Checks order status on exchange and updates local state.
        
        Args:
            order: Order to verify
            
        Returns:
            True if order was successfully verified
        """
        try:
            self.logger.debug(f"Verifying order {order.order_id}")
            
            # Skip verification if order wasn't submitted
            if order.status != OrderStatus.SUBMITTED:
                return False
            
            # Verify with exchange
            if self.exchange:
                result = self.exchange.check_order(order)
                
                if not result.get("found", False):
                    self.logger.warning(f"Order {order.order_id} not found on exchange")
                    return False
                
                # Update order status
                exchange_status = result.get("status")
                if exchange_status == "filled":
                    order.update_status(OrderStatus.FILLED)
                elif exchange_status == "partially_filled":
                    order.update_status(OrderStatus.PARTIALLY_FILLED)
                elif exchange_status == "canceled":
                    order.cancel("Canceled on exchange")
                elif exchange_status == "rejected":
                    order.reject("Rejected by exchange")
                
                # Update fills if provided
                if "fills" in result:
                    for fill_data in result["fills"]:
                        fill = OrderFill(
                            fill_id=fill_data.get("id", str(uuid.uuid4())),
                            timestamp=fill_data.get("timestamp", time.time()),
                            price=fill_data.get("price", 0.0),
                            quantity=fill_data.get("quantity", 0.0),
                            fee=fill_data.get("fee", 0.0),
                            fee_asset=fill_data.get("fee_asset")
                        )
                        order.add_fill(fill)
            else:
                self.logger.error("Exchange adapter not initialized")
                return False
            
            self.logger.debug(f"Order {order.order_id} verified successfully")
            return True
            
        except Exception as e:
            self.logger.exception(f"Error verifying order {order.order_id}: {str(e)}")
            # Don't fail the order here - verification can be retried
            return False
    
    def _is_circuit_open(self) -> bool:
        """
        Check if circuit breaker is open.
        
        Returns:
            True if circuit breaker is open
        """
        if not self.circuit_breaker_enabled:
            return False
        
        if time.time() < self.circuit_open_until:
            return True
        
        return False
    
    def _open_circuit_breaker(self) -> None:
        """Open the circuit breaker."""
        self.circuit_open_until = time.time() + self.circuit_open_timeout
        self.logger.warning(
            f"Circuit breaker opened after {self.consecutive_failures} consecutive failures. "
            f"Will remain open for {self.circuit_open_timeout} seconds."
        )
    
    def _persist_orders(self) -> None:
        """Persist all orders to disk."""
        try:
            for order_id, order in list(self.orders.items()):
                self._persist_order(order)
            
            self.last_persistence_time = time.time()
            
        except Exception as e:
            self.logger.exception(f"Error persisting orders: {str(e)}")
    
    def _persist_order(self, order: Order) -> None:
        """
        Persist a single order to disk.
        
        Args:
            order: Order to persist
        """
        if not self.persistence_enabled:
            return
        
        try:
            # Create order data
            order_data = order.to_dict()
            
            # Determine file path based on order status
            if order.status in (OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.FAILED):
                path = os.path.join(self.persistence_path, "completed", f"{order.order_id}.json")
            else:
                path = os.path.join(self.persistence_path, "active", f"{order.order_id}.json")
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(path), exist_ok=True)
            
            # Write to file
            with open(path, "w") as f:
                json.dump(order_data, f, indent=2)
                
        except Exception as e:
            self.logger.exception(f"Error persisting order {order.order_id}: {str(e)}")
    
    def _recover_orders(self) -> None:
        """Recover orders from disk on startup."""
        if not self.persistence_enabled or self.recovered:
            return
        
        try:
            self.logger.info("Recovering orders from disk")
            
            # Recover active orders
            active_dir = os.path.join(self.persistence_path, "active")
            if os.path.exists(active_dir):
                for filename in os.listdir(active_dir):
                    if not filename.endswith(".json"):
                        continue
                    
                    try:
                        with open(os.path.join(active_dir, filename), "r") as f:
                            order_data = json.load(f)
                            order = Order.from_dict(order_data)
                            
                            # Add to orders and active orders
                            self.orders[order.order_id] = order
                            self.active_orders.add(order.order_id)
                            
                            # Queue for processing if needed
                            if order.status in (OrderStatus.CREATED, OrderStatus.VALIDATING, OrderStatus.READY):
                                with self.queue_lock:
                                    self.order_queue.append(order.order_id)
                            
                            self.logger.info(f"Recovered active order {order.order_id}")
                            
                    except Exception as e:
                        self.logger.error(f"Error recovering order from {filename}: {str(e)}")
            
            # Recover completed orders (up to a limit)
            completed_dir = os.path.join(self.persistence_path, "completed")
            max_completed = 1000  # Limit the number of completed orders to recover
            
            if os.path.exists(completed_dir):
                # Get most recent files based on modification time
                files = [(f, os.path.getmtime(os.path.join(completed_dir, f))) 
                         for f in os.listdir(completed_dir) if f.endswith(".json")]
                files.sort(key=lambda x: x[1], reverse=True)
                
                for filename, _ in files[:max_completed]:
                    try:
                        with open(os.path.join(completed_dir, filename), "r") as f:
                            order_data = json.load(f)
                            order = Order.from_dict(order_data)
                            
                            # Add to orders and completed orders
                            self.orders[order.order_id] = order
                            self.completed_orders[order.order_id] = order
                            
                    except Exception as e:
                        self.logger.error(f"Error recovering completed order from {filename}: {str(e)}")
            
            self.recovered = True
            self.logger.info(f"Recovered {len(self.active_orders)} active orders and {len(self.completed_orders)} completed orders")
            
        except Exception as e:
            self.logger.exception(f"Error recovering orders: {str(e)}")
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get execution engine statistics.
        
        Returns:
            Dictionary of statistics
        """
        avg_execution_time = sum(self.execution_times) / len(self.execution_times) if self.execution_times else 0
        
        return {
            "active_orders": len(self.active_orders),
            "completed_orders": len(self.completed_orders),
            "queue_size": len(self.order_queue),
            "circuit_breaker_open": self._is_circuit_open(),
            "consecutive_failures": self.consecutive_failures,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "avg_execution_time": avg_execution_time,
        } 