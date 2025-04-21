"""
Executor Module

This module implements the Executor responsible for managing order execution
through a three-phase commit transaction system for reliable trading.
"""

import logging
import time
import uuid
from enum import Enum, auto
from typing import Dict, List, Optional, Any, Tuple, Callable

from trading_system.models.order import Order, OrderStatus, OrderSide
from trading_system.exchange.base_adapter import ExchangeAdapter
from trading_system.execution.orders import OrderManager
from trading_system.position.position_manager import PositionManager


class TransactionPhase(Enum):
    """Enum representing different phases in the three-phase commit transaction."""
    INIT = auto()
    VALIDATE = auto()
    EXECUTE = auto()
    COMMITTED = auto()
    ROLLED_BACK = auto()


class TransactionStatus(Enum):
    """Enum representing the status of a transaction."""
    PENDING = auto()
    COMPLETED = auto()
    FAILED = auto()


class ExecutionMode(Enum):
    """Enum representing different execution modes."""
    SYNCHRONOUS = "synchronous"
    ASYNCHRONOUS = "asynchronous"


class Transaction:
    """
    Represents a transaction in the three-phase commit system.
    
    A transaction encapsulates an order execution process, including validation,
    execution, and commit/rollback procedures.
    """
    
    def __init__(self, 
                 order: Order, 
                 exchange_adapter: ExchangeAdapter, 
                 execution_callback: Optional[Callable] = None):
        """
        Initialize a transaction.
        
        Args:
            order: The order to execute
            exchange_adapter: The exchange adapter to use for execution
            execution_callback: Optional callback to call when transaction completes
        """
        self.transaction_id = str(uuid.uuid4())
        self.order = order
        self.exchange_adapter = exchange_adapter
        self.execution_callback = execution_callback
        self.phase = TransactionPhase.INIT
        self.status = TransactionStatus.PENDING
        self.timestamp_created = time.time()
        self.timestamp_updated = self.timestamp_created
        self.error = None
        self.validation_result = None
        self.execution_result = None
        self.rollback_result = None
        
    def update_phase(self, phase: TransactionPhase, error: Optional[str] = None):
        """
        Update transaction phase.
        
        Args:
            phase: New transaction phase
            error: Optional error message
        """
        self.phase = phase
        self.timestamp_updated = time.time()
        
        if error:
            self.error = error
            self.status = TransactionStatus.FAILED
            
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert transaction to dictionary.
        
        Returns:
            Dictionary representation of transaction
        """
        return {
            "transaction_id": self.transaction_id,
            "order": self.order.to_dict() if self.order else None,
            "exchange_id": self.exchange_adapter.exchange_id if self.exchange_adapter else None,
            "phase": self.phase.name,
            "status": self.status.name,
            "timestamp_created": self.timestamp_created,
            "timestamp_updated": self.timestamp_updated,
            "error": self.error,
            "validation_result": self.validation_result,
            "execution_result": self.execution_result,
            "rollback_result": self.rollback_result
        }


class Executor:
    """
    Executor class that manages order execution through a three-phase commit system.
    
    The three-phase commit system consists of:
    1. Validation phase: Validate the order with the exchange
    2. Execution phase: Submit the order to the exchange
    3. Commit/Rollback phase: Confirm the transaction or roll it back if needed
    """
    
    def __init__(self, exchange_adapters: Dict[str, ExchangeAdapter], logger: Optional[logging.Logger] = None):
        """
        Initialize the Executor.
        
        Args:
            exchange_adapters: Dictionary mapping exchange IDs to exchange adapters
            logger: Optional logger instance
        """
        self.exchange_adapters = exchange_adapters
        self.logger = logger or logging.getLogger(__name__)
        self.transactions: Dict[str, Transaction] = {}
        self.active_orders: Dict[str, Order] = {}
        self.execution_mode = ExecutionMode.SYNCHRONOUS
        
        # Initialize managers
        self.order_manager = OrderManager()
        self.position_manager = None  # Will be set later
        
    def set_position_manager(self, position_manager: PositionManager) -> None:
        """
        Set the position manager.
        
        Args:
            position_manager: Position manager to use
        """
        self.position_manager = position_manager
        
    def set_execution_mode(self, mode: ExecutionMode):
        """
        Set the execution mode.
        
        Args:
            mode: Execution mode to set
        """
        self.execution_mode = mode
        self.logger.info(f"Execution mode set to {mode.value}")
        
    def create_transaction(self, order: Order, execution_callback: Optional[Callable] = None) -> Transaction:
        """
        Create a new transaction for an order.
        
        Args:
            order: Order to execute
            execution_callback: Optional callback for when execution completes
            
        Returns:
            Created transaction
            
        Raises:
            ValueError: If exchange adapter for the order's exchange_id is not found
        """
        if order.exchange_id not in self.exchange_adapters:
            raise ValueError(f"No exchange adapter found for exchange ID: {order.exchange_id}")
            
        exchange_adapter = self.exchange_adapters[order.exchange_id]
        
        # Create new transaction
        transaction = Transaction(order, exchange_adapter, execution_callback)
        self.transactions[transaction.transaction_id] = transaction
        
        self.logger.info(f"Created transaction {transaction.transaction_id} for order {order.client_order_id}")
        return transaction
    
    def execute_order(self, order: Order, execution_callback: Optional[Callable] = None) -> Tuple[bool, str, Optional[Transaction]]:
        """
        Execute an order through the three-phase commit system.
        
        Args:
            order: Order to execute
            execution_callback: Optional callback for when execution completes
            
        Returns:
            Tuple of (success, message, transaction)
            
        Raises:
            ValueError: If order validation fails
        """
        try:
            # Create transaction
            transaction = self.create_transaction(order, execution_callback)
            
            # Phase 1: Validate
            success, message = self._validate_phase(transaction)
            if not success:
                return False, message, transaction
                
            # Phase 2: Execute
            success, message = self._execute_phase(transaction)
            if not success:
                # Try to rollback if execution fails
                self._rollback_transaction(transaction)
                return False, message, transaction
                
            # Phase 3: Commit
            success, message = self._commit_transaction(transaction)
            
            return success, message, transaction
            
        except Exception as e:
            self.logger.error(f"Error executing order: {str(e)}", exc_info=True)
            return False, f"Execution error: {str(e)}", None
    
    def _validate_phase(self, transaction: Transaction) -> Tuple[bool, str]:
        """
        Execute the validation phase of the transaction.
        
        Args:
            transaction: Transaction to validate
            
        Returns:
            Tuple of (success, message)
        """
        transaction.update_phase(TransactionPhase.VALIDATE)
        self.logger.info(f"Validating transaction {transaction.transaction_id}")
        
        try:
            # Update order status
            transaction.order.update_status(OrderStatus.VALIDATED)
            
            # Validate order with exchange
            validation_result = transaction.exchange_adapter.validate_order(transaction.order)
            transaction.validation_result = validation_result
            
            if not validation_result.get("valid", False):
                error_msg = validation_result.get("message", "Order validation failed")
                transaction.update_phase(TransactionPhase.VALIDATE, error_msg)
                transaction.order.update_status(OrderStatus.REJECTED)
                return False, error_msg
                
            self.logger.info(f"Transaction {transaction.transaction_id} validated successfully")
            return True, "Order validated"
            
        except Exception as e:
            error_msg = f"Validation error: {str(e)}"
            transaction.update_phase(TransactionPhase.VALIDATE, error_msg)
            transaction.order.update_status(OrderStatus.ERROR)
            self.logger.error(error_msg, exc_info=True)
            return False, error_msg
    
    def _execute_phase(self, transaction: Transaction) -> Tuple[bool, str]:
        """
        Execute the execution phase of the transaction.
        
        Args:
            transaction: Transaction to execute
            
        Returns:
            Tuple of (success, message)
        """
        transaction.update_phase(TransactionPhase.EXECUTE)
        self.logger.info(f"Executing transaction {transaction.transaction_id}")
        
        try:
            # Update order status
            transaction.order.update_status(OrderStatus.PENDING)
            
            # Submit order to exchange
            execution_result = transaction.exchange_adapter.submit_order(transaction.order)
            transaction.execution_result = execution_result
            
            if not execution_result.get("success", False):
                error_msg = execution_result.get("message", "Order execution failed")
                transaction.update_phase(TransactionPhase.EXECUTE, error_msg)
                transaction.order.update_status(OrderStatus.REJECTED)
                return False, error_msg
            
            # Update order with exchange details
            exchange_order_id = execution_result.get("exchange_order_id")
            if exchange_order_id:
                transaction.order.exchange_order_id = exchange_order_id
            
            # Update order status
            transaction.order.update_status(OrderStatus.NEW)
            
            self.logger.info(f"Transaction {transaction.transaction_id} executed successfully")
            return True, "Order executed"
            
        except Exception as e:
            error_msg = f"Execution error: {str(e)}"
            transaction.update_phase(TransactionPhase.EXECUTE, error_msg)
            transaction.order.update_status(OrderStatus.ERROR)
            self.logger.error(error_msg, exc_info=True)
            return False, error_msg
    
    def _commit_transaction(self, transaction: Transaction) -> Tuple[bool, str]:
        """
        Commit the transaction.
        
        Args:
            transaction: Transaction to commit
            
        Returns:
            Tuple of (success, message)
        """
        transaction.update_phase(TransactionPhase.COMMITTED)
        self.logger.info(f"Committing transaction {transaction.transaction_id}")
        
        try:
            # Add order to active orders
            self.active_orders[transaction.order.client_order_id] = transaction.order
            
            # Add to order manager
            self.order_manager.add_order(transaction.order)
            
            # Call execution callback if provided
            if transaction.execution_callback:
                try:
                    transaction.execution_callback(transaction)
                except Exception as e:
                    self.logger.error(f"Error in execution callback: {str(e)}", exc_info=True)
            
            transaction.status = TransactionStatus.COMPLETED
            self.logger.info(f"Transaction {transaction.transaction_id} committed successfully")
            return True, "Transaction committed"
            
        except Exception as e:
            error_msg = f"Commit error: {str(e)}"
            transaction.update_phase(TransactionPhase.COMMITTED, error_msg)
            transaction.order.update_status(OrderStatus.ERROR)
            self.logger.error(error_msg, exc_info=True)
            return False, error_msg
    
    def _rollback_transaction(self, transaction: Transaction) -> Tuple[bool, str]:
        """
        Roll back a transaction.
        
        Args:
            transaction: Transaction to roll back
            
        Returns:
            Tuple of (success, message)
        """
        transaction.update_phase(TransactionPhase.ROLLED_BACK)
        self.logger.info(f"Rolling back transaction {transaction.transaction_id}")
        
        try:
            # If order was submitted to exchange, try to cancel it
            if transaction.order.status in [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED]:
                try:
                    cancel_result = transaction.exchange_adapter.cancel_order(transaction.order)
                    transaction.rollback_result = cancel_result
                    
                    if not cancel_result.get("success", False):
                        self.logger.warning(
                            f"Failed to cancel order {transaction.order.client_order_id} during rollback: "
                            f"{cancel_result.get('message', 'Unknown error')}"
                        )
                except Exception as e:
                    self.logger.error(f"Error cancelling order during rollback: {str(e)}", exc_info=True)
            
            # Update order status
            transaction.order.update_status(OrderStatus.CANCELED)
            
            # Remove from active orders if present
            if transaction.order.client_order_id in self.active_orders:
                del self.active_orders[transaction.order.client_order_id]
            
            transaction.status = TransactionStatus.FAILED
            self.logger.info(f"Transaction {transaction.transaction_id} rolled back")
            return True, "Transaction rolled back"
            
        except Exception as e:
            error_msg = f"Rollback error: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return False, error_msg
    
    def update_order_status(self, client_order_id: str, status: OrderStatus, 
                         filled_quantity: Optional[float] = None, 
                         fill_price: Optional[float] = None,
                         fees: Optional[float] = None) -> bool:
        """
        Update the status of an order.
        
        Args:
            client_order_id: Client order ID
            status: New order status
            filled_quantity: Optional filled quantity
            fill_price: Optional fill price
            fees: Optional fees
            
        Returns:
            True if the order was updated, False otherwise
        """
        if client_order_id not in self.active_orders:
            self.logger.warning(f"Order {client_order_id} not found in active orders")
            return False
        
        order = self.active_orders[client_order_id]
        
        # Update order status
        order.update_status(status)
        
        # Update fill information if provided
        if filled_quantity is not None and fill_price is not None:
            order.update_fill(filled_quantity, fill_price, fees or 0.0)
            
            # If order is fully filled, remove from active orders
            if status == OrderStatus.FILLED:
                del self.active_orders[client_order_id]
        
        # Update in order manager
        self.order_manager.update_order_status(
            client_order_id, status, filled_quantity, fill_price, fees
        )
        
        self.logger.info(f"Updated order {client_order_id} status to {status.value}")
        return True
    
    def check_order_status(self, client_order_id: str) -> Dict[str, Any]:
        """
        Check the status of an order.
        
        Args:
            client_order_id: Client order ID
            
        Returns:
            Dictionary with order status information
        """
        if client_order_id not in self.active_orders:
            return {
                "success": False,
                "message": f"Order {client_order_id} not found in active orders"
            }
        
        order = self.active_orders[client_order_id]
        
        # Check order status with exchange
        exchange_adapter = self.exchange_adapters.get(order.exchange_id)
        if not exchange_adapter:
            return {
                "success": False,
                "message": f"Exchange adapter not found for exchange ID: {order.exchange_id}"
            }
        
        try:
            # Get order status from exchange
            exchange_status = exchange_adapter.check_order_status(order)
            
            # Update order with exchange information
            if exchange_status.get("success", False):
                # Update order status
                new_status = exchange_status.get("status")
                if new_status:
                    order.update_status(OrderStatus(new_status))
                
                # Update fill information
                filled_quantity = exchange_status.get("filled_quantity")
                avg_fill_price = exchange_status.get("fill_price")
                fees = exchange_status.get("fees")
                
                if filled_quantity is not None and avg_fill_price is not None:
                    order.update_fill(filled_quantity, avg_fill_price, fees or 0.0)
                
                # Remove from active orders if filled or cancelled
                if order.status in [OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED]:
                    del self.active_orders[client_order_id]
            
            # Return order information
            return {
                "success": True,
                "order": order.to_dict(),
                "exchange_status": exchange_status
            }
            
        except Exception as e:
            error_msg = f"Error checking order status: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return {
                "success": False,
                "message": error_msg,
                "order": order.to_dict()
            }
    
    def cancel_order(self, client_order_id: str) -> Dict[str, Any]:
        """
        Cancel an order.
        
        Args:
            client_order_id: Client order ID
            
        Returns:
            Dictionary with cancellation result
        """
        if client_order_id not in self.active_orders:
            return {
                "success": False,
                "message": f"Order {client_order_id} not found in active orders"
            }
        
        order = self.active_orders[client_order_id]
        
        # Only allow cancelling certain order statuses
        if order.status not in [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED]:
            return {
                "success": False,
                "message": f"Cannot cancel order in state {order.status.value}"
            }
        
        # Get exchange adapter
        exchange_adapter = self.exchange_adapters.get(order.exchange_id)
        if not exchange_adapter:
            return {
                "success": False,
                "message": f"Exchange adapter not found for exchange ID: {order.exchange_id}"
            }
        
        try:
            # Cancel order with exchange
            cancel_result = exchange_adapter.cancel_order(order)
            
            if cancel_result.get("success", False):
                # Update order status
                order.update_status(OrderStatus.CANCELED)
                
                # Update in order manager
                self.order_manager.update_order_status(client_order_id, OrderStatus.CANCELED)
                
                # Remove from active orders
                del self.active_orders[client_order_id]
                
                self.logger.info(f"Order {client_order_id} cancelled successfully")
            else:
                self.logger.warning(
                    f"Failed to cancel order {client_order_id}: {cancel_result.get('message', 'Unknown error')}"
                )
            
            return {
                "success": cancel_result.get("success", False),
                "message": cancel_result.get("message", ""),
                "order": order.to_dict()
            }
            
        except Exception as e:
            error_msg = f"Error cancelling order: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return {
                "success": False,
                "message": error_msg,
                "order": order.to_dict()
            }
    
    def get_active_orders(self) -> List[Dict[str, Any]]:
        """
        Get all active orders.
        
        Returns:
            List of active orders as dictionaries
        """
        return [order.to_dict() for order in self.active_orders.values()]
    
    def get_transaction_history(self) -> List[Dict[str, Any]]:
        """
        Get transaction history.
        
        Returns:
            List of transactions as dictionaries
        """
        return [transaction.to_dict() for transaction in self.transactions.values()]
    
    def clear_transaction_history(self, older_than_seconds: Optional[float] = None):
        """
        Clear transaction history.
        
        Args:
            older_than_seconds: Optional age threshold for transactions to clear
        """
        if older_than_seconds is not None:
            current_time = time.time()
            to_remove = [
                transaction_id for transaction_id, transaction in self.transactions.items()
                if (current_time - transaction.timestamp_created) > older_than_seconds
                and transaction.status != TransactionStatus.PENDING
            ]
            
            for transaction_id in to_remove:
                del self.transactions[transaction_id]
                
            self.logger.info(f"Cleared {len(to_remove)} transactions older than {older_than_seconds} seconds")
        else:
            # Only clear completed transactions
            to_remove = [
                transaction_id for transaction_id, transaction in self.transactions.items()
                if transaction.status != TransactionStatus.PENDING
            ]
            
            for transaction_id in to_remove:
                del self.transactions[transaction_id]
                
            self.logger.info(f"Cleared {len(to_remove)} completed transactions") 