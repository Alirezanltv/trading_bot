"""
Order Executor Module

This module implements the OrderExecutor component which is responsible for
reliable execution of orders across multiple venues while maintaining consistency.
"""

import asyncio
import time
import uuid
from enum import Enum, auto
from typing import Dict, List, Optional, Any, Union, Callable, Set, Tuple
import logging
import json
import traceback
from datetime import datetime, timedelta

from trading_system.core.component import Component, ComponentStatus
from trading_system.core.logging import get_logger
from trading_system.execution.transaction import (
    Transaction, TransactionParticipant, ParticipantRole, TransactionManager
)
from trading_system.models.order import Order, OrderStatus, OrderType
from trading_system.models.fill import Fill

logger = get_logger("execution.order_executor")


class ExecutionStrategy(Enum):
    """Available execution strategies."""
    DIRECT = auto()              # Direct execution to specified venue
    SMART = auto()               # Smart routing based on best execution
    TWAP = auto()                # Time-Weighted Average Price
    VWAP = auto()                # Volume-Weighted Average Price
    ICEBERG = auto()             # Iceberg/reserve order
    DARK_POOL = auto()           # Dark pool execution
    LIMIT_CHASE = auto()         # Limit chase algorithm


class ExecutionResult:
    """
    Result of an order execution attempt.
    """
    
    def __init__(self, 
                 order_id: str, 
                 success: bool,
                 status: OrderStatus,
                 fills: Optional[List[Fill]] = None,
                 error: Optional[str] = None,
                 transaction_id: Optional[str] = None):
        """
        Initialize execution result.
        
        Args:
            order_id: Order ID
            success: Whether execution was successful
            status: Current order status
            fills: List of fills from the execution
            error: Error message if execution failed
            transaction_id: Associated transaction ID
        """
        self.order_id = order_id
        self.success = success
        self.status = status
        self.fills = fills or []
        self.error = error
        self.transaction_id = transaction_id
        self.timestamp = time.time()
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary.
        
        Returns:
            Dictionary representation
        """
        return {
            "order_id": self.order_id,
            "success": self.success,
            "status": self.status.value if isinstance(self.status, OrderStatus) else self.status,
            "fills": [fill.to_dict() for fill in self.fills],
            "error": self.error,
            "transaction_id": self.transaction_id,
            "timestamp": self.timestamp
        }


class OrderParticipant(TransactionParticipant):
    """
    Order execution participant for the transaction system.
    """
    
    def __init__(self, 
                 name: str,
                 order: Order,
                 executor: 'OrderExecutor',
                 role: ParticipantRole = ParticipantRole.PRIMARY):
        """
        Initialize order participant.
        
        Args:
            name: Participant name
            order: Order to execute
            executor: Order executor
            role: Participant role
        """
        super().__init__(name=name, role=role)
        self.order = order
        self.executor = executor
        self.execution_id = str(uuid.uuid4())
        self.fills: List[Fill] = []
    
    async def prepare(self, 
                      transaction: Transaction, 
                      context: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """
        Prepare phase of the transaction.
        
        Args:
            transaction: Transaction object
            context: Transaction context
            
        Returns:
            Tuple of (success, result_dict)
        """
        try:
            logger.debug(f"Preparing order {self.order.id} for execution")
            
            # Validate order
            validation_result = self.executor.validate_order(self.order)
            if not validation_result["valid"]:
                return False, {
                    "error": f"Order validation failed: {validation_result['reason']}",
                    "validation_result": validation_result
                }
            
            # Check if order can be executed
            can_execute, reason = await self.executor.can_execute_order(self.order)
            if not can_execute:
                return False, {"error": f"Cannot execute order: {reason}"}
            
            # Allocate resources if needed
            allocation_result = await self.executor.allocate_resources(self.order)
            if not allocation_result["success"]:
                return False, {
                    "error": f"Resource allocation failed: {allocation_result['reason']}",
                    "allocation_result": allocation_result
                }
            
            # Update context with execution details
            execution_context = {
                "execution_id": self.execution_id,
                "order_id": self.order.id,
                "symbol": self.order.symbol,
                "side": self.order.side,
                "quantity": self.order.quantity,
                "price": self.order.price,
                "order_type": self.order.order_type
            }
            transaction.update_context(execution_context)
            
            return True, {
                "execution_id": self.execution_id,
                "order_id": self.order.id,
                "validation_result": validation_result,
                "allocation_result": allocation_result
            }
            
        except Exception as e:
            logger.error(f"Error preparing order {self.order.id}: {str(e)}", exc_info=True)
            return False, {"error": str(e), "traceback": traceback.format_exc()}
    
    async def pre_commit(self, 
                         transaction: Transaction, 
                         context: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """
        Pre-commit phase of the transaction.
        
        Args:
            transaction: Transaction object
            context: Transaction context
            
        Returns:
            Tuple of (success, result_dict)
        """
        try:
            logger.debug(f"Pre-committing order {self.order.id} for execution")
            
            # Send order to the venue but don't confirm yet
            submission_result = await self.executor.submit_order(self.order, confirm=False)
            if not submission_result["success"]:
                return False, {
                    "error": f"Order submission failed: {submission_result['reason']}",
                    "submission_result": submission_result
                }
            
            # Store submission result
            venue_order_id = submission_result.get("venue_order_id")
            if venue_order_id:
                self.order.venue_order_id = venue_order_id
                context["venue_order_id"] = venue_order_id
            
            return True, {
                "execution_id": self.execution_id,
                "order_id": self.order.id,
                "venue_order_id": venue_order_id,
                "submission_result": submission_result
            }
            
        except Exception as e:
            logger.error(f"Error pre-committing order {self.order.id}: {str(e)}", exc_info=True)
            return False, {"error": str(e), "traceback": traceback.format_exc()}
    
    async def commit(self, 
                     transaction: Transaction, 
                     context: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """
        Commit phase of the transaction.
        
        Args:
            transaction: Transaction object
            context: Transaction context
            
        Returns:
            Tuple of (success, result_dict)
        """
        try:
            logger.debug(f"Committing order {self.order.id} execution")
            
            # Confirm order submission
            confirmation_result = await self.executor.confirm_order(self.order)
            if not confirmation_result["success"]:
                return False, {
                    "error": f"Order confirmation failed: {confirmation_result['reason']}",
                    "confirmation_result": confirmation_result
                }
            
            # Get fills if any
            fills = confirmation_result.get("fills", [])
            self.fills.extend(fills)
            
            # Update order status
            self.order.status = confirmation_result.get("status", OrderStatus.ACKNOWLEDGED)
            
            # Update order in the database
            update_result = await self.executor.update_order_status(
                self.order.id, 
                self.order.status,
                fills=fills
            )
            
            return True, {
                "execution_id": self.execution_id,
                "order_id": self.order.id,
                "status": self.order.status.name,
                "fills": [fill.to_dict() for fill in fills],
                "confirmation_result": confirmation_result,
                "update_result": update_result
            }
            
        except Exception as e:
            logger.error(f"Error committing order {self.order.id}: {str(e)}", exc_info=True)
            return False, {"error": str(e), "traceback": traceback.format_exc()}
    
    async def abort(self, 
                    transaction: Transaction, 
                    context: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """
        Abort (rollback) the transaction.
        
        Args:
            transaction: Transaction object
            context: Transaction context
            
        Returns:
            Tuple of (success, result_dict)
        """
        try:
            logger.debug(f"Aborting order {self.order.id} execution")
            
            # Get abort reason
            reason = context.get("abort_reason", "Transaction aborted")
            
            # Check transaction state
            if transaction.state.name in ["PRE_COMMITTED", "COMMITTING", "COMMITTED"]:
                # Already submitted to venue, need to cancel
                venue_order_id = self.order.venue_order_id or context.get("venue_order_id")
                if venue_order_id:
                    cancellation_result = await self.executor.cancel_order(
                        self.order.id, 
                        venue_order_id,
                        reason=reason
                    )
                    
                    return True, {
                        "execution_id": self.execution_id,
                        "order_id": self.order.id,
                        "cancellation_result": cancellation_result
                    }
            
            # If just prepared, release resources
            if transaction.state.name in ["PREPARED", "PREPARING"]:
                release_result = await self.executor.release_resources(self.order)
                
                return True, {
                    "execution_id": self.execution_id,
                    "order_id": self.order.id,
                    "release_result": release_result
                }
            
            # Otherwise, nothing to do
            return True, {
                "execution_id": self.execution_id,
                "order_id": self.order.id,
                "message": "No action needed for abort"
            }
            
        except Exception as e:
            logger.error(f"Error aborting order {self.order.id}: {str(e)}", exc_info=True)
            return False, {"error": str(e), "traceback": traceback.format_exc()}


class OrderExecutor(Component):
    """
    Order Executor Component
    
    This component is responsible for reliable execution of orders
    across multiple venues while maintaining consistency.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize order executor.
        
        Args:
            config: Configuration dictionary
        """
        super().__init__(name="order_executor", config=config)
        
        # Configuration
        self.default_execution_timeout = self.config.get("default_execution_timeout", 30.0)
        self.max_concurrent_executions = self.config.get("max_concurrent_executions", 50)
        self.execution_history_size = self.config.get("execution_history_size", 1000)
        
        # Dependencies
        self.transaction_manager: Optional[TransactionManager] = None
        
        # Order execution history
        self.execution_history: Dict[str, ExecutionResult] = {}
        
        # Active orders
        self.active_orders: Dict[str, Order] = {}
        
        # Semaphore to limit concurrent executions
        self.concurrency_semaphore = asyncio.Semaphore(self.max_concurrent_executions)
        
        # Exchange connectors
        self.exchange_connectors: Dict[str, Any] = {}
        
        # Running state
        self.running = False
        
        # Initialize status
        self._update_status(ComponentStatus.INITIALIZED)
        logger.info("Order executor initialized")
    
    async def start(self) -> bool:
        """
        Start the order executor.
        
        Returns:
            bool: True if started successfully, False otherwise
        """
        try:
            if self.running:
                logger.warning("Order executor already running")
                return True
            
            # Update status
            self._update_status(ComponentStatus.INITIALIZING)
            
            # Check dependencies
            self.transaction_manager = self.get_dependency("transaction_manager")
            if not self.transaction_manager:
                self._update_status(ComponentStatus.ERROR, "Transaction manager dependency not found")
                return False
            
            # Initialize exchange connectors
            await self._initialize_exchange_connectors()
            
            # Set running state
            self.running = True
            
            # Update status
            self._update_status(ComponentStatus.OPERATIONAL)
            logger.info("Order executor started")
            return True
            
        except Exception as e:
            self._update_status(ComponentStatus.ERROR, f"Error starting: {str(e)}")
            logger.error(f"Error starting order executor: {str(e)}", exc_info=True)
            return False
    
    async def stop(self) -> bool:
        """
        Stop the order executor.
        
        Returns:
            bool: True if stopped successfully, False otherwise
        """
        try:
            if not self.running:
                return True
            
            # Update status
            self._update_status(ComponentStatus.STOPPING)
            
            # Cancel all active orders
            cancel_tasks = []
            for order_id, order in self.active_orders.items():
                if order.status not in [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED]:
                    logger.info(f"Cancelling active order {order_id} during shutdown")
                    task = asyncio.create_task(self.cancel_order(
                        order_id=order_id,
                        venue_order_id=order.venue_order_id,
                        reason="System shutdown"
                    ))
                    cancel_tasks.append(task)
            
            if cancel_tasks:
                await asyncio.gather(*cancel_tasks, return_exceptions=True)
            
            # Stop exchange connectors
            await self._stop_exchange_connectors()
            
            # Set running state
            self.running = False
            
            # Update status
            self._update_status(ComponentStatus.STOPPED)
            logger.info("Order executor stopped")
            return True
            
        except Exception as e:
            self._update_status(ComponentStatus.ERROR, f"Error stopping: {str(e)}")
            logger.error(f"Error stopping order executor: {str(e)}", exc_info=True)
            return False
    
    async def execute_order(self, order: Order) -> ExecutionResult:
        """
        Execute an order using the three-phase commit protocol.
        
        Args:
            order: Order to execute
            
        Returns:
            ExecutionResult object
        """
        if not self.running:
            return ExecutionResult(
                order_id=order.id,
                success=False,
                status=order.status,
                error="Order executor not running"
            )
        
        async with self.concurrency_semaphore:
            try:
                logger.info(f"Executing order {order.id} ({order.symbol} {order.side} {order.quantity})")
                
                # Register order as active
                self.active_orders[order.id] = order
                
                # Create transaction for order execution
                transaction = self.transaction_manager.create_transaction(
                    name=f"Order-{order.id}",
                    timeout=self.default_execution_timeout,
                    context={"order_id": order.id}
                )
                
                # Create order participant
                participant = OrderParticipant(
                    name=f"order-{order.id}",
                    order=order,
                    executor=self
                )
                transaction.add_participant(participant)
                
                # Execute transaction
                success = await self.transaction_manager.execute_transaction(transaction)
                
                # Get order status and fills
                fills = participant.fills
                
                # Create execution result
                result = ExecutionResult(
                    order_id=order.id,
                    success=success,
                    status=order.status,
                    fills=fills,
                    error=transaction.error_message if not success else None,
                    transaction_id=transaction.id
                )
                
                # Store in history
                self._store_execution_result(result)
                
                # Remove from active orders if execution completed
                if order.status in [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED]:
                    if order.id in self.active_orders:
                        del self.active_orders[order.id]
                
                return result
                
            except Exception as e:
                logger.error(f"Error executing order {order.id}: {str(e)}", exc_info=True)
                
                # Create error result
                result = ExecutionResult(
                    order_id=order.id,
                    success=False,
                    status=order.status,
                    error=f"Execution error: {str(e)}"
                )
                
                # Store in history
                self._store_execution_result(result)
                
                # Remove from active orders
                if order.id in self.active_orders:
                    del self.active_orders[order.id]
                
                return result
    
    def validate_order(self, order: Order) -> Dict[str, Any]:
        """
        Validate an order before execution.
        
        Args:
            order: Order to validate
            
        Returns:
            Validation result dictionary
        """
        # Check basic validity
        if not order.id:
            return {"valid": False, "reason": "Order ID is missing"}
        
        if not order.symbol:
            return {"valid": False, "reason": "Symbol is missing"}
        
        if not order.quantity or order.quantity <= 0:
            return {"valid": False, "reason": "Invalid quantity"}
        
        # Check order type-specific validation
        if order.order_type == OrderType.LIMIT and (order.price is None or order.price <= 0):
            return {"valid": False, "reason": "Limit order requires a valid price"}
        
        # Check if venue is supported
        if order.venue and order.venue not in self.exchange_connectors:
            return {"valid": False, "reason": f"Unsupported venue: {order.venue}"}
        
        return {"valid": True}
    
    async def can_execute_order(self, order: Order) -> Tuple[bool, str]:
        """
        Check if an order can be executed.
        
        Args:
            order: Order to check
            
        Returns:
            Tuple of (can_execute, reason)
        """
        # Check if system is running
        if not self.running:
            return False, "Order executor not running"
        
        # Determine exchange connector to use
        venue = order.venue
        if not venue:
            # Use default venue based on strategy
            venue = self._determine_best_venue(order)
        
        connector = self.exchange_connectors.get(venue)
        if not connector:
            return False, f"No connector available for venue {venue}"
        
        # Check if venue is available
        venue_status = await connector.get_status()
        if venue_status.get("status") != "available":
            return False, f"Venue {venue} is not available: {venue_status.get('reason', 'unknown reason')}"
        
        # Check symbol availability
        symbol_info = await connector.get_symbol_info(order.symbol)
        if not symbol_info.get("available", False):
            return False, f"Symbol {order.symbol} is not available on venue {venue}"
        
        # Check trading session
        session_info = await connector.get_trading_session(order.symbol)
        if not session_info.get("is_trading_session", False):
            return False, f"Not in trading session for {order.symbol} on venue {venue}"
        
        # All checks passed
        return True, ""
    
    async def allocate_resources(self, order: Order) -> Dict[str, Any]:
        """
        Allocate resources for order execution.
        
        Args:
            order: Order to allocate resources for
            
        Returns:
            Resource allocation result
        """
        # Determine venue
        venue = order.venue or self._determine_best_venue(order)
        
        # Get connector
        connector = self.exchange_connectors.get(venue)
        if not connector:
            return {"success": False, "reason": f"No connector available for venue {venue}"}
        
        # Check account balances for required margin/funds
        try:
            # This would typically validate against available balance
            # and potentially reserve the necessary funds or margin
            account_info = await connector.get_account_info()
            
            # Simple check for demonstration purposes
            if order.side == "BUY":
                # Assuming the price is the maximum we would pay (for market orders this is approximate)
                required_funds = order.price * order.quantity if order.price else None
                
                # If we can't determine required funds, just check if balance is positive
                if required_funds:
                    if account_info.get("available_balance", 0) < required_funds:
                        return {
                            "success": False, 
                            "reason": f"Insufficient funds: required {required_funds}, available {account_info.get('available_balance', 0)}"
                        }
            else:  # SELL
                # Check if we have enough of the asset to sell
                asset_balance = account_info.get("asset_balances", {}).get(order.symbol.split('/')[0], 0)
                if asset_balance < order.quantity:
                    return {
                        "success": False,
                        "reason": f"Insufficient asset balance: required {order.quantity}, available {asset_balance}"
                    }
            
            # Resource allocation successful
            return {
                "success": True,
                "venue": venue,
                "account_info": account_info
            }
            
        except Exception as e:
            logger.error(f"Error allocating resources for order {order.id}: {str(e)}", exc_info=True)
            return {"success": False, "reason": f"Resource allocation error: {str(e)}"}
    
    async def release_resources(self, order: Order) -> Dict[str, Any]:
        """
        Release resources allocated for order execution.
        
        Args:
            order: Order to release resources for
            
        Returns:
            Resource release result
        """
        # This would typically release any reserved funds or margin
        return {"success": True}
    
    async def submit_order(self, order: Order, confirm: bool = False) -> Dict[str, Any]:
        """
        Submit an order to the venue.
        
        Args:
            order: Order to submit
            confirm: Whether to confirm the order immediately
            
        Returns:
            Submission result
        """
        try:
            # Determine venue
            venue = order.venue or self._determine_best_venue(order)
            
            # Get connector
            connector = self.exchange_connectors.get(venue)
            if not connector:
                return {"success": False, "reason": f"No connector available for venue {venue}"}
            
            # Submit order to venue
            submission_result = await connector.submit_order(order, confirm=confirm)
            
            if submission_result.get("success", False):
                # Update order with venue order ID
                venue_order_id = submission_result.get("venue_order_id")
                if venue_order_id:
                    order.venue_order_id = venue_order_id
                
                # Update order status
                if confirm:
                    status = submission_result.get("status")
                    if status:
                        order.status = status
                else:
                    order.status = OrderStatus.PENDING
                
                return submission_result
            else:
                order.status = OrderStatus.REJECTED
                return submission_result
                
        except Exception as e:
            logger.error(f"Error submitting order {order.id}: {str(e)}", exc_info=True)
            order.status = OrderStatus.REJECTED
            return {"success": False, "reason": f"Submission error: {str(e)}"}
    
    async def confirm_order(self, order: Order) -> Dict[str, Any]:
        """
        Confirm an order that was previously submitted.
        
        Args:
            order: Order to confirm
            
        Returns:
            Confirmation result
        """
        try:
            # Determine venue
            venue = order.venue or self._determine_best_venue(order)
            
            # Get connector
            connector = self.exchange_connectors.get(venue)
            if not connector:
                return {"success": False, "reason": f"No connector available for venue {venue}"}
            
            # Check if venue order ID is available
            if not order.venue_order_id:
                return {"success": False, "reason": "No venue order ID available for confirmation"}
            
            # Confirm order
            confirmation_result = await connector.confirm_order(order.venue_order_id)
            
            if confirmation_result.get("success", False):
                # Update order status
                status = confirmation_result.get("status")
                if status:
                    order.status = status
                else:
                    order.status = OrderStatus.ACKNOWLEDGED
                
                # Handle immediate fills if any
                fills = confirmation_result.get("fills", [])
                if fills:
                    for fill in fills:
                        order.add_fill(fill)
                    
                    # Check if order is completely filled
                    if order.is_filled():
                        order.status = OrderStatus.FILLED
                
                return confirmation_result
            else:
                order.status = OrderStatus.REJECTED
                return confirmation_result
                
        except Exception as e:
            logger.error(f"Error confirming order {order.id}: {str(e)}", exc_info=True)
            return {"success": False, "reason": f"Confirmation error: {str(e)}"}
    
    async def cancel_order(self, 
                           order_id: str, 
                           venue_order_id: Optional[str] = None,
                           reason: str = "User requested cancellation") -> Dict[str, Any]:
        """
        Cancel an order.
        
        Args:
            order_id: Order ID to cancel
            venue_order_id: Venue order ID (if available)
            reason: Reason for cancellation
            
        Returns:
            Cancellation result
        """
        try:
            # Get order from active orders
            order = self.active_orders.get(order_id)
            if not order:
                return {"success": False, "reason": f"Order {order_id} not found in active orders"}
            
            # Use venue order ID from order if not provided
            if not venue_order_id:
                venue_order_id = order.venue_order_id
            
            if not venue_order_id:
                return {"success": False, "reason": "No venue order ID available for cancellation"}
            
            # Determine venue
            venue = order.venue or self._determine_best_venue(order)
            
            # Get connector
            connector = self.exchange_connectors.get(venue)
            if not connector:
                return {"success": False, "reason": f"No connector available for venue {venue}"}
            
            # Cancel order
            cancellation_result = await connector.cancel_order(venue_order_id, reason=reason)
            
            if cancellation_result.get("success", False):
                # Update order status
                order.status = OrderStatus.CANCELLED
                
                # Update order in database
                await self.update_order_status(order_id, OrderStatus.CANCELLED)
                
                # Remove from active orders
                if order_id in self.active_orders:
                    del self.active_orders[order_id]
                
                return cancellation_result
            else:
                return cancellation_result
                
        except Exception as e:
            logger.error(f"Error cancelling order {order_id}: {str(e)}", exc_info=True)
            return {"success": False, "reason": f"Cancellation error: {str(e)}"}
    
    async def update_order_status(self, 
                                  order_id: str, 
                                  status: OrderStatus,
                                  fills: Optional[List[Fill]] = None) -> Dict[str, Any]:
        """
        Update order status in database.
        
        Args:
            order_id: Order ID
            status: New order status
            fills: List of new fills
            
        Returns:
            Update result
        """
        try:
            # This would typically update the order in the database
            # For now, just log the status update
            logger.info(f"Updating order {order_id} status to {status.name}")
            
            if fills:
                for fill in fills:
                    logger.info(f"Order {order_id} fill: {fill.quantity} @ {fill.price}")
            
            # Update status in active orders if present
            order = self.active_orders.get(order_id)
            if order:
                order.status = status
                if fills:
                    for fill in fills:
                        order.add_fill(fill)
            
            return {"success": True}
            
        except Exception as e:
            logger.error(f"Error updating order {order_id} status: {str(e)}", exc_info=True)
            return {"success": False, "reason": f"Status update error: {str(e)}"}
    
    def _determine_best_venue(self, order: Order) -> str:
        """
        Determine the best venue for order execution.
        
        Args:
            order: Order to determine venue for
            
        Returns:
            Venue name
        """
        # If venue is specified, use it
        if order.venue:
            return order.venue
        
        # TODO: Implement smart venue selection based on:
        # - Best price
        # - Lowest fees
        # - Highest liquidity
        # - Fastest execution
        # - User preferences
        
        # For now, just return the first available venue
        if self.exchange_connectors:
            return next(iter(self.exchange_connectors.keys()))
        
        # Default venue
        return "default_venue"
    
    def _store_execution_result(self, result: ExecutionResult) -> None:
        """
        Store execution result in history.
        
        Args:
            result: Execution result
        """
        self.execution_history[result.order_id] = result
        
        # Trim history if needed
        if len(self.execution_history) > self.execution_history_size:
            oldest_id = min(self.execution_history.keys(), key=lambda k: self.execution_history[k].timestamp)
            del self.execution_history[oldest_id]
    
    def get_execution_history(self, 
                              limit: Optional[int] = None,
                              filter_success: Optional[bool] = None) -> List[Dict[str, Any]]:
        """
        Get execution history.
        
        Args:
            limit: Maximum number of results to return
            filter_success: Filter by success status
            
        Returns:
            List of execution results, sorted by timestamp (newest first)
        """
        # Filter results
        results = list(self.execution_history.values())
        
        if filter_success is not None:
            results = [r for r in results if r.success == filter_success]
        
        # Sort by timestamp (newest first)
        results.sort(key=lambda r: r.timestamp, reverse=True)
        
        # Apply limit
        if limit is not None:
            results = results[:limit]
        
        return [r.to_dict() for r in results]
    
    def get_execution_result(self, order_id: str) -> Optional[Dict[str, Any]]:
        """
        Get execution result for an order.
        
        Args:
            order_id: Order ID
            
        Returns:
            Execution result or None if not found
        """
        result = self.execution_history.get(order_id)
        return result.to_dict() if result else None
    
    def get_active_orders(self) -> List[Dict[str, Any]]:
        """
        Get active orders.
        
        Returns:
            List of active orders
        """
        return [order.to_dict() for order in self.active_orders.values()]
    
    async def _initialize_exchange_connectors(self) -> None:
        """Initialize exchange connectors from configuration."""
        exchange_configs = self.config.get("exchanges", {})
        
        for exchange_id, config in exchange_configs.items():
            try:
                # Import connector module
                connector_type = config.get("type", "generic")
                module_name = f"trading_system.connectors.{connector_type}"
                module = __import__(module_name, fromlist=["create_connector"])
                
                # Create connector
                connector = await module.create_connector(exchange_id, config)
                
                # Store connector
                self.exchange_connectors[exchange_id] = connector
                logger.info(f"Initialized exchange connector for {exchange_id}")
                
            except Exception as e:
                logger.error(f"Error initializing exchange connector for {exchange_id}: {str(e)}", exc_info=True)
    
    async def _stop_exchange_connectors(self) -> None:
        """Stop all exchange connectors."""
        stop_tasks = []
        
        for exchange_id, connector in self.exchange_connectors.items():
            try:
                task = asyncio.create_task(connector.stop())
                stop_tasks.append(task)
                
            except Exception as e:
                logger.error(f"Error stopping exchange connector for {exchange_id}: {str(e)}")
        
        if stop_tasks:
            await asyncio.gather(*stop_tasks, return_exceptions=True)


async def create_order_executor(config: Dict[str, Any], 
                                transaction_manager: TransactionManager) -> OrderExecutor:
    """
    Create and start an order executor.
    
    Args:
        config: Configuration dictionary
        transaction_manager: Transaction manager instance
        
    Returns:
        OrderExecutor instance
    """
    executor = OrderExecutor(config)
    executor.add_dependency("transaction_manager", transaction_manager)
    await executor.start()
    return executor 