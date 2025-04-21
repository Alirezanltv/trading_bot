"""
Nobitex Execution Engine

This module implements a production-grade execution engine for the Nobitex exchange
with robust error handling, retry logic, and a three-phase commit model.
"""

import os
import time
import json
import logging
import threading
import asyncio
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Any, Set, Callable
from dotenv import load_dotenv

from trading_system.core.component import Component, ComponentStatus
from trading_system.core.logging import get_logger
from trading_system.execution.orders import Order, OrderStatus, OrderSide, OrderType
from trading_system.exchange.nobitex_adapter import NobitexExchangeAdapter

# Load environment variables
load_dotenv()

# Transaction logging setup
TRANSACTION_LOG_DIR = Path("data/transactions")
TRANSACTION_LOG_DIR.mkdir(parents=True, exist_ok=True)


class ExecutionPhase(Enum):
    """Three-phase commit model phases for order execution."""
    PRE_COMMIT = "pre_commit"   # Validate signal + check funds
    COMMIT = "commit"           # Send order to exchange
    POST_COMMIT = "post_commit" # Confirm execution + update state


class ExecutionResult:
    """Execution result with detailed information about the execution process."""
    
    def __init__(self, success: bool, order_id: Optional[str] = None, exchange_order_id: Optional[str] = None):
        self.success = success
        self.order_id = order_id
        self.exchange_order_id = exchange_order_id
        self.error_message = None
        self.retry_count = 0
        self.execution_time = 0.0
        self.phase = None
        self.timestamp = datetime.now().isoformat()
    
    def set_error(self, message: str, phase: ExecutionPhase) -> None:
        """Set error information."""
        self.success = False
        self.error_message = message
        self.phase = phase
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging."""
        return {
            "success": self.success,
            "order_id": self.order_id,
            "exchange_order_id": self.exchange_order_id,
            "error_message": self.error_message,
            "retry_count": self.retry_count,
            "execution_time": self.execution_time,
            "phase": self.phase.value if self.phase else None,
            "timestamp": self.timestamp
        }


class NobitexExecutionEngine(Component):
    """
    Enhanced execution engine for Nobitex exchange.
    
    Features:
    - Three-phase commit model (pre-commit, commit, post-commit)
    - Robust error handling with retry mechanisms
    - Transaction logging for audit and debugging
    - Secure API key management
    - Partial fill detection and handling
    - Circuit breaker pattern to prevent cascading failures
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the Nobitex execution engine.
        
        Args:
            config: Configuration dictionary
        """
        super().__init__("nobitex_execution_engine", config)
        
        # Initialize logger
        self.logger = get_logger("execution.nobitex")
        
        # Engine operational status (separate from Component status)
        self.engine_status = "STOPPED"
        
        # Exchange adapter
        self.exchange = None
        
        # Order tracking
        self.orders: Dict[str, Order] = {}
        self.active_orders: Set[str] = set()
        self.execution_lock = threading.Lock()
        
        # Configuration
        engine_config = config.get("execution_engine", {})
        self.max_retries = engine_config.get("max_retries", 3)
        self.retry_delay = engine_config.get("retry_delay", 2.0)
        self.order_update_interval = engine_config.get("order_update_interval", 5.0)
        
        # Transaction logging
        self.transaction_log_path = engine_config.get("transaction_log_path", "data/transactions")
        os.makedirs(self.transaction_log_path, exist_ok=True)
        
        # Circuit breaker settings
        self.circuit_breaker_enabled = engine_config.get("circuit_breaker_enabled", True)
        self.failure_threshold = engine_config.get("failure_threshold", 3)
        self.circuit_open_timeout = engine_config.get("circuit_open_timeout", 300)  # seconds
        self.consecutive_failures = 0
        self.circuit_open_until = 0
        
        # Status tracking
        self.order_tracker_running = False
        self.order_tracker_thread = None
        
        self.logger.info("Nobitex execution engine initialized")
    
    def start(self) -> None:
        """Start the execution engine."""
        if self.engine_status == "RUNNING":
            return
        
        self.logger.info("Starting Nobitex execution engine")
        
        try:
            # Initialize exchange adapter
            exchange_config = self.config.get("exchange", {})
            self.exchange = NobitexExchangeAdapter(exchange_config)
            
            # Start order tracker
            self.order_tracker_running = True
            self.order_tracker_thread = threading.Thread(target=self._track_active_orders, daemon=True)
            self.order_tracker_thread.start()
            
            self.engine_status = "RUNNING"
            # Update component status
            self._update_status(ComponentStatus.OPERATIONAL)
            
            self.logger.info("Nobitex execution engine started successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to start Nobitex execution engine: {str(e)}")
            self.engine_status = "ERROR"
            # Update component status
            self._update_status(ComponentStatus.ERROR)
    
    def stop(self) -> None:
        """Stop the execution engine."""
        if self.engine_status != "RUNNING":
            return
        
        self.logger.info("Stopping Nobitex execution engine")
        
        # Stop order tracker
        self.order_tracker_running = False
        if self.order_tracker_thread:
            self.order_tracker_thread.join(timeout=5.0)
        
        self.engine_status = "STOPPED"
        # Update component status
        self._update_status(ComponentStatus.SHUTDOWN)
        
        self.logger.info("Nobitex execution engine stopped")
    
    def execute_order(self, order: Order) -> ExecutionResult:
        """
        Execute an order using the three-phase commit model.
        
        Args:
            order: The order to execute
            
        Returns:
            ExecutionResult with execution status and details
        """
        # Create result object
        result = ExecutionResult(success=False, order_id=order.order_id)
        
        # Check circuit breaker
        if self._is_circuit_open():
            result.set_error("Circuit breaker is open", ExecutionPhase.PRE_COMMIT)
            self._log_transaction(order, result)
            return result
        
        self.logger.info(f"Executing order: {order.order_id} - {order.symbol} {order.side.value} {order.quantity}")
        
        # Store order for tracking
        self.orders[order.order_id] = order
        
        # Start execution timer
        start_time = time.time()
        
        # Execute with retry logic
        retry_count = 0
        max_retries = self.max_retries
        
        while retry_count <= max_retries:
            try:
                # Phase 1: Pre-commit (validate and check funds)
                if not self._pre_commit(order, result):
                    break
                
                # Phase 2: Commit (send to exchange)
                if not self._commit(order, result):
                    # If we get a temporary error, retry
                    if retry_count < max_retries and self._is_retryable_error(result.error_message):
                        retry_count += 1
                        result.retry_count = retry_count
                        self.logger.warning(f"Retrying order {order.order_id} (attempt {retry_count}/{max_retries})")
                        time.sleep(self.retry_delay * retry_count)  # Exponential backoff
                        continue
                    break
                
                # Phase 3: Post-commit (verify execution)
                if not self._post_commit(order, result):
                    # If verification fails but order might be submitted, don't retry blindly
                    # Instead, we'll rely on the order tracker to resolve the status
                    if result.exchange_order_id:
                        self.logger.warning(f"Order {order.order_id} submitted but verification failed. Order tracker will resolve.")
                        self.active_orders.add(order.order_id)
                    break
                
                # Successful execution
                result.success = True
                break
                
            except Exception as e:
                self.logger.exception(f"Unexpected error executing order {order.order_id}: {str(e)}")
                result.set_error(f"Unexpected error: {str(e)}", ExecutionPhase.COMMIT)
                
                # Retry on unexpected errors
                if retry_count < max_retries:
                    retry_count += 1
                    result.retry_count = retry_count
                    self.logger.warning(f"Retrying order {order.order_id} after error (attempt {retry_count}/{max_retries})")
                    time.sleep(self.retry_delay * retry_count)
                else:
                    break
        
        # End execution timer
        end_time = time.time()
        result.execution_time = end_time - start_time
        
        # Update circuit breaker
        if result.success:
            self.consecutive_failures = 0
        else:
            self.consecutive_failures += 1
            if self.circuit_breaker_enabled and self.consecutive_failures >= self.failure_threshold:
                self._open_circuit_breaker()
        
        # Log transaction
        self._log_transaction(order, result)
        
        return result
    
    def _pre_commit(self, order: Order, result: ExecutionResult) -> bool:
        """
        Pre-commit phase: Validate order and check available funds.
        
        Args:
            order: Order to validate
            result: Result object to update
            
        Returns:
            True if pre-commit phase succeeds, False otherwise
        """
        result.phase = ExecutionPhase.PRE_COMMIT
        
        try:
            # Validate order
            self.logger.debug(f"Validating order {order.order_id}")
            validation = self.exchange.validate_order(order)
            
            if not validation.get("valid", False):
                error_msg = validation.get("message", "Order validation failed")
                order.update_status(OrderStatus.REJECTED)
                order.error_message = error_msg
                result.set_error(error_msg, ExecutionPhase.PRE_COMMIT)
                return False
            
            # Check available balance
            symbol = order.symbol
            base_asset, quote_asset = symbol.split("-")
            
            required_asset = quote_asset if order.side == OrderSide.BUY else base_asset
            required_amount = order.notional if order.side == OrderSide.BUY else order.quantity
            
            # Get account balance
            balances = self.exchange.get_balances()
            
            if required_asset not in balances:
                error_msg = f"Asset {required_asset} not found in account"
                order.update_status(OrderStatus.REJECTED)
                order.error_message = error_msg
                result.set_error(error_msg, ExecutionPhase.PRE_COMMIT)
                return False
            
            available = balances[required_asset].get("available", 0)
            
            if available < required_amount:
                error_msg = f"Insufficient {required_asset} balance. Required: {required_amount}, Available: {available}"
                order.update_status(OrderStatus.REJECTED)
                order.error_message = error_msg
                result.set_error(error_msg, ExecutionPhase.PRE_COMMIT)
                return False
            
            # Pre-commit successful
            order.update_status(OrderStatus.VALIDATING)
            return True
            
        except Exception as e:
            error_msg = f"Pre-commit error: {str(e)}"
            order.update_status(OrderStatus.REJECTED)
            order.error_message = error_msg
            result.set_error(error_msg, ExecutionPhase.PRE_COMMIT)
            return False
    
    def _commit(self, order: Order, result: ExecutionResult) -> bool:
        """
        Commit phase: Send order to exchange.
        
        Args:
            order: Order to submit
            result: Result object to update
            
        Returns:
            True if commit phase succeeds, False otherwise
        """
        result.phase = ExecutionPhase.COMMIT
        
        try:
            # Submit order to exchange
            self.logger.info(f"Submitting order to exchange: {order.order_id}")
            
            # Mark order as pending
            order.update_status(OrderStatus.SUBMITTING)
            
            # Submit order
            submit_result = self.exchange.submit_order(order)
            
            if not submit_result.get("success", False):
                error_msg = submit_result.get("message", "Order submission failed")
                order.update_status(OrderStatus.REJECTED)
                order.error_message = error_msg
                result.set_error(error_msg, ExecutionPhase.COMMIT)
                return False
            
            # Extract exchange order ID
            exchange_order_id = submit_result.get("exchange_order_id")
            if not exchange_order_id:
                error_msg = "Exchange did not return an order ID"
                order.update_status(OrderStatus.REJECTED)
                order.error_message = error_msg
                result.set_error(error_msg, ExecutionPhase.COMMIT)
                return False
            
            # Update order with exchange ID
            order.exchange_order_id = exchange_order_id
            order.update_status(OrderStatus.SUBMITTED)
            result.exchange_order_id = exchange_order_id
            
            return True
            
        except Exception as e:
            error_msg = f"Commit error: {str(e)}"
            order.update_status(OrderStatus.REJECTED)
            order.error_message = error_msg
            result.set_error(error_msg, ExecutionPhase.COMMIT)
            return False
    
    def _post_commit(self, order: Order, result: ExecutionResult) -> bool:
        """
        Post-commit phase: Verify order execution and update state.
        
        Args:
            order: Order to verify
            result: Result object to update
            
        Returns:
            True if post-commit phase succeeds, False otherwise
        """
        result.phase = ExecutionPhase.POST_COMMIT
        
        try:
            # Verify order on exchange
            self.logger.debug(f"Verifying order {order.order_id} on exchange")
            
            # Check order status
            check_result = self.exchange.check_order(order)
            
            if not check_result.get("success", False):
                error_msg = check_result.get("message", "Order verification failed")
                order.update_status(OrderStatus.UNKNOWN)
                order.error_message = error_msg
                result.set_error(error_msg, ExecutionPhase.POST_COMMIT)
                
                # Add to active orders for continued tracking
                self.active_orders.add(order.order_id)
                return False
            
            # Extract order status
            exchange_status = check_result.get("status")
            
            if exchange_status == "FILLED":
                fill_price = check_result.get("price", 0.0)
                fill_quantity = check_result.get("executed_quantity", 0.0)
                
                # Create order fill
                fill = OrderFill(
                    price=fill_price,
                    quantity=fill_quantity
                )
                
                order.add_fill(fill)
                return True
                
            elif exchange_status == "PARTIALLY_FILLED":
                fill_price = check_result.get("price", 0.0)
                fill_quantity = check_result.get("executed_quantity", 0.0)
                
                # Create order fill
                fill = OrderFill(
                    price=fill_price,
                    quantity=fill_quantity
                )
                
                order.add_fill(fill)
                
                # Add to active orders for continued tracking
                self.active_orders.add(order.order_id)
                return True
                
            elif exchange_status in ["NEW", "PENDING"]:
                # Add to active orders for continued tracking
                self.active_orders.add(order.order_id)
                return True
                
            elif exchange_status in ["REJECTED", "EXPIRED", "CANCELED"]:
                error_msg = check_result.get("message", f"Order {exchange_status.lower()}")
                order.update_status(OrderStatus.REJECTED)
                order.error_message = error_msg
                result.set_error(error_msg, ExecutionPhase.POST_COMMIT)
                return False
                
            else:
                # Unknown status, keep tracking
                order.update_status(OrderStatus.UNKNOWN)
                order.error_message = f"Unknown exchange status: {exchange_status}"
                self.active_orders.add(order.order_id)
                return True
            
        except Exception as e:
            error_msg = f"Post-commit error: {str(e)}"
            order.update_status(OrderStatus.UNKNOWN)
            order.error_message = error_msg
            result.set_error(error_msg, ExecutionPhase.POST_COMMIT)
            
            # Add to active orders for continued tracking
            self.active_orders.add(order.order_id)
            return False
    
    def _track_active_orders(self) -> None:
        """Track active orders and update their status periodically."""
        self.logger.info("Order tracker started")
        
        while self.order_tracker_running:
            try:
                # Get list of active orders to track
                active_order_ids = list(self.active_orders)
                
                for order_id in active_order_ids:
                    # Skip if order no longer exists
                    if order_id not in self.orders:
                        self.active_orders.discard(order_id)
                        continue
                    
                    order = self.orders[order_id]
                    
                    # Skip if order is already in a final state
                    if order.status in [OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED]:
                        self.active_orders.discard(order_id)
                        continue
                    
                    try:
                        # Check order status on exchange
                        check_result = self.exchange.check_order(order)
                        
                        if not check_result.get("success", False):
                            self.logger.warning(f"Failed to check status of order {order_id}: {check_result.get('message')}")
                            continue
                        
                        # Extract order status
                        exchange_status = check_result.get("status")
                        
                        if exchange_status == "FILLED":
                            fill_price = check_result.get("price", 0.0)
                            fill_quantity = check_result.get("executed_quantity", 0.0)
                            
                            # Create order fill
                            fill = OrderFill(
                                price=fill_price,
                                quantity=fill_quantity
                            )
                            
                            order.add_fill(fill)
                            self.active_orders.discard(order_id)
                            
                            # Log the fill
                            self._log_fill(order, fill_price, fill_quantity)
                            
                        elif exchange_status == "PARTIALLY_FILLED":
                            fill_price = check_result.get("price", 0.0)
                            fill_quantity = check_result.get("executed_quantity", 0.0)
                            
                            # Only update if new quantity is filled
                            current_filled = sum(fill.quantity for fill in order.fills)
                            
                            if fill_quantity > current_filled:
                                new_fill_quantity = fill_quantity - current_filled
                                
                                # Create order fill
                                fill = OrderFill(
                                    price=fill_price,
                                    quantity=new_fill_quantity
                                )
                                
                                order.add_fill(fill)
                                
                                # Log the partial fill
                                self._log_fill(order, fill_price, new_fill_quantity)
                            
                        elif exchange_status in ["REJECTED", "EXPIRED", "CANCELED"]:
                            order.update_status(OrderStatus.CANCELED)
                            order.error_message = f"Order {exchange_status.lower()} on exchange"
                            self.active_orders.discard(order_id)
                    
                    except Exception as e:
                        self.logger.error(f"Error updating order {order_id}: {str(e)}")
                
            except Exception as e:
                self.logger.error(f"Error in order tracker: {str(e)}")
            
            # Sleep before next update
            time.sleep(self.order_update_interval)
        
        self.logger.info("Order tracker stopped")
    
    def cancel_order(self, order_id: str) -> bool:
        """
        Cancel an order.
        
        Args:
            order_id: ID of the order to cancel
            
        Returns:
            True if cancellation was successful, False otherwise
        """
        # Check if order exists
        if order_id not in self.orders:
            self.logger.warning(f"Attempted to cancel unknown order: {order_id}")
            return False
        
        order = self.orders[order_id]
        
        # Check if order can be canceled
        if order.status not in [OrderStatus.SUBMITTED, OrderStatus.PARTIALLY_FILLED]:
            self.logger.warning(f"Order {order_id} cannot be canceled in status {order.status.value}")
            return False
        
        try:
            # Send cancellation request to exchange
            cancel_result = self.exchange.cancel_order(order)
            
            if cancel_result and cancel_result.get("success", False):
                order.update_status(OrderStatus.CANCELED)
                order.error_message = "Order canceled by request"
                self.active_orders.discard(order_id)
                return True
            else:
                error_msg = cancel_result.get("message", "Cancellation failed")
                self.logger.error(f"Failed to cancel order {order_id}: {error_msg}")
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
            Order object if found, None otherwise
        """
        return self.orders.get(order_id)
    
    def _is_circuit_open(self) -> bool:
        """Check if circuit breaker is open."""
        if not self.circuit_breaker_enabled:
            return False
        
        if self.circuit_open_until > time.time():
            self.logger.warning(f"Circuit breaker is open until {datetime.fromtimestamp(self.circuit_open_until)}")
            return True
        
        return False
    
    def _open_circuit_breaker(self) -> None:
        """Open the circuit breaker."""
        self.circuit_open_until = time.time() + self.circuit_open_timeout
        self.logger.warning(f"Circuit breaker opened until {datetime.fromtimestamp(self.circuit_open_until)}")
    
    def _is_retryable_error(self, error_message: str) -> bool:
        """Check if an error is retryable."""
        if not error_message:
            return False
        
        retryable_patterns = [
            "timeout", "connection", "network", "temporary", "retry",
            "rate limit", "busy", "overloaded", "unavailable"
        ]
        
        return any(pattern in error_message.lower() for pattern in retryable_patterns)
    
    def _log_transaction(self, order: Order, result: ExecutionResult) -> None:
        """
        Log transaction details for audit and debugging.
        
        Args:
            order: Order being executed
            result: Execution result
        """
        try:
            # Create transaction log entry
            transaction = {
                "timestamp": datetime.now().isoformat(),
                "order": order.to_dict(),
                "execution_result": result.to_dict()
            }
            
            # Generate log file name
            date_str = datetime.now().strftime("%Y%m%d")
            log_file = Path(self.transaction_log_path) / f"transactions_{date_str}.jsonl"
            
            # Append to log file
            with open(log_file, "a") as f:
                f.write(json.dumps(transaction) + "\n")
                
        except Exception as e:
            self.logger.error(f"Error logging transaction: {str(e)}")
    
    def _log_fill(self, order: Order, price: float, quantity: float) -> None:
        """
        Log order fill details.
        
        Args:
            order: Order that was filled
            price: Fill price
            quantity: Fill quantity
        """
        try:
            # Create fill log entry
            fill = {
                "timestamp": datetime.now().isoformat(),
                "order_id": order.order_id,
                "exchange_order_id": order.exchange_order_id,
                "symbol": order.symbol,
                "side": order.side.value,
                "price": price,
                "quantity": quantity,
                "notional": price * quantity,
                "status": order.status.value
            }
            
            # Generate log file name
            date_str = datetime.now().strftime("%Y%m%d")
            log_file = Path(self.transaction_log_path) / f"fills_{date_str}.jsonl"
            
            # Append to log file
            with open(log_file, "a") as f:
                f.write(json.dumps(fill) + "\n")
                
        except Exception as e:
            self.logger.error(f"Error logging fill: {str(e)}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get execution engine statistics."""
        return {
            "active_orders": len(self.active_orders),
            "total_orders": len(self.orders),
            "circuit_breaker_status": "open" if self._is_circuit_open() else "closed",
            "consecutive_failures": self.consecutive_failures,
            "engine_status": self.engine_status
        } 