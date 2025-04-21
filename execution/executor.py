"""
Order Execution System

This module handles the reliable execution of orders using a three-phase commit protocol
to ensure atomicity and fault tolerance.
"""

import logging
import time
from typing import Dict, Any, List, Optional, Callable, Tuple
import uuid

from trading_system.execution.three_phase_commit import (
    TransactionManager, Transaction, TransactionState, TransactionParticipant
)
from trading_system.models.order import Order, OrderStatus
from trading_system.models.position import Position

logger = logging.getLogger(__name__)


class ExecutionResult:
    """
    Represents the result of an order execution.
    """
    
    def __init__(
        self, 
        success: bool, 
        order_id: str, 
        execution_id: str = None, 
        filled_quantity: float = 0.0,
        fill_price: float = 0.0,
        message: str = "",
        timestamp: float = None
    ):
        """
        Initialize execution result.
        
        Args:
            success: Whether the execution was successful
            order_id: ID of the order being executed
            execution_id: Unique ID for this execution
            filled_quantity: Quantity filled in this execution
            fill_price: Price at which the order was filled
            message: Additional information about the execution
            timestamp: Execution timestamp
        """
        self.success = success
        self.order_id = order_id
        self.execution_id = execution_id or str(uuid.uuid4())
        self.filled_quantity = filled_quantity
        self.fill_price = fill_price
        self.message = message
        self.timestamp = timestamp or time.time()
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert execution result to dictionary.
        
        Returns:
            Execution result as dictionary
        """
        return {
            "success": self.success,
            "order_id": self.order_id,
            "execution_id": self.execution_id,
            "filled_quantity": self.filled_quantity,
            "fill_price": self.fill_price,
            "message": self.message,
            "timestamp": self.timestamp
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ExecutionResult':
        """
        Create execution result from dictionary.
        
        Args:
            data: Execution result data
            
        Returns:
            ExecutionResult instance
        """
        return cls(
            success=data["success"],
            order_id=data["order_id"],
            execution_id=data["execution_id"],
            filled_quantity=data["filled_quantity"],
            fill_price=data["fill_price"],
            message=data["message"],
            timestamp=data["timestamp"]
        )


class OrderExecutor:
    """
    Executes orders reliably using the three-phase commit protocol.
    """
    
    def __init__(self, exchange_adapters: Dict[str, 'ExchangeAdapter'] = None):
        """
        Initialize order executor.
        
        Args:
            exchange_adapters: Dictionary mapping exchange IDs to exchange adapters
        """
        self.transaction_manager = TransactionManager()
        self.exchange_adapters = exchange_adapters or {}
        self.position_manager = None  # Will be set later
        self.order_manager = None     # Will be set later
        
    def set_position_manager(self, position_manager: 'PositionManager') -> None:
        """
        Set the position manager.
        
        Args:
            position_manager: Position manager to use
        """
        self.position_manager = position_manager
    
    def set_order_manager(self, order_manager: 'OrderManager') -> None:
        """
        Set the order manager.
        
        Args:
            order_manager: Order manager to use
        """
        self.order_manager = order_manager
    
    def add_exchange_adapter(self, exchange_id: str, adapter: 'ExchangeAdapter') -> None:
        """
        Add an exchange adapter.
        
        Args:
            exchange_id: Unique identifier for the exchange
            adapter: Exchange adapter instance
        """
        self.exchange_adapters[exchange_id] = adapter
    
    def execute_order(self, order: Order) -> ExecutionResult:
        """
        Execute an order using the three-phase commit protocol.
        
        Args:
            order: Order to execute
            
        Returns:
            Execution result
        """
        if order.status not in [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED]:
            return ExecutionResult(
                success=False,
                order_id=order.order_id,
                message=f"Cannot execute order in state {order.status.value}"
            )
        
        # Get the exchange adapter
        exchange_adapter = self.exchange_adapters.get(order.exchange_id)
        if not exchange_adapter:
            return ExecutionResult(
                success=False,
                order_id=order.order_id,
                message=f"Exchange adapter not found for {order.exchange_id}"
            )
        
        # Create a transaction
        transaction_data = {
            "order_id": order.order_id,
            "symbol": order.symbol,
            "side": order.side.value,
            "quantity": order.quantity,
            "price": order.price,
            "order_type": order.order_type.value,
            "exchange_id": order.exchange_id
        }
        
        transaction = self.transaction_manager.create_transaction(transaction_data)
        
        # Create transaction participants
        participants = {
            "exchange": ExchangeParticipant(exchange_adapter, order),
            "position": PositionParticipant(self.position_manager, order),
            "order": OrderParticipant(self.order_manager, order)
        }
        
        # Prepare phase
        prepare_functions = {
            participant_id: participant.prepare 
            for participant_id, participant in participants.items()
        }
        
        prepare_success = self.transaction_manager.prepare(
            transaction.transaction_id, prepare_functions
        )
        
        if not prepare_success:
            # Abort the transaction
            abort_functions = {
                participant_id: participant.abort 
                for participant_id, participant in participants.items()
            }
            self.transaction_manager.abort(transaction.transaction_id, abort_functions)
            
            return ExecutionResult(
                success=False,
                order_id=order.order_id,
                message=f"Failed to prepare transaction {transaction.transaction_id}"
            )
        
        # Commit phase
        commit_functions = {
            participant_id: participant.commit 
            for participant_id, participant in participants.items()
        }
        
        commit_success = self.transaction_manager.commit(
            transaction.transaction_id, commit_functions
        )
        
        if not commit_success:
            # This should theoretically never happen in a three-phase commit,
            # but we handle it just in case
            logger.critical(
                f"Failed to commit transaction {transaction.transaction_id} after successful prepare. "
                "System may be in an inconsistent state."
            )
            
            return ExecutionResult(
                success=False,
                order_id=order.order_id,
                message=f"Failed to commit transaction {transaction.transaction_id}"
            )
        
        # Get execution details from the exchange participant
        execution_details = participants["exchange"].execution_details
        
        return ExecutionResult(
            success=True,
            order_id=order.order_id,
            execution_id=execution_details.get("execution_id", str(uuid.uuid4())),
            filled_quantity=execution_details.get("filled_quantity", 0.0),
            fill_price=execution_details.get("fill_price", 0.0),
            message="Order executed successfully"
        )
    
    def cancel_order(self, order: Order) -> ExecutionResult:
        """
        Cancel an order using the three-phase commit protocol.
        
        Args:
            order: Order to cancel
            
        Returns:
            Execution result
        """
        if order.status not in [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED]:
            return ExecutionResult(
                success=False,
                order_id=order.order_id,
                message=f"Cannot cancel order in state {order.status.value}"
            )
        
        # Get the exchange adapter
        exchange_adapter = self.exchange_adapters.get(order.exchange_id)
        if not exchange_adapter:
            return ExecutionResult(
                success=False,
                order_id=order.order_id,
                message=f"Exchange adapter not found for {order.exchange_id}"
            )
        
        # Create a transaction
        transaction_data = {
            "order_id": order.order_id,
            "action": "cancel"
        }
        
        transaction = self.transaction_manager.create_transaction(transaction_data)
        
        # Create transaction participants
        # For cancel, we only need exchange and order participants
        participants = {
            "exchange": ExchangeCancelParticipant(exchange_adapter, order),
            "order": OrderCancelParticipant(self.order_manager, order)
        }
        
        # Prepare phase
        prepare_functions = {
            participant_id: participant.prepare 
            for participant_id, participant in participants.items()
        }
        
        prepare_success = self.transaction_manager.prepare(
            transaction.transaction_id, prepare_functions
        )
        
        if not prepare_success:
            # Abort the transaction
            abort_functions = {
                participant_id: participant.abort 
                for participant_id, participant in participants.items()
            }
            self.transaction_manager.abort(transaction.transaction_id, abort_functions)
            
            return ExecutionResult(
                success=False,
                order_id=order.order_id,
                message=f"Failed to prepare cancel transaction {transaction.transaction_id}"
            )
        
        # Commit phase
        commit_functions = {
            participant_id: participant.commit 
            for participant_id, participant in participants.items()
        }
        
        commit_success = self.transaction_manager.commit(
            transaction.transaction_id, commit_functions
        )
        
        if not commit_success:
            logger.critical(
                f"Failed to commit cancel transaction {transaction.transaction_id} after successful prepare. "
                "System may be in an inconsistent state."
            )
            
            return ExecutionResult(
                success=False,
                order_id=order.order_id,
                message=f"Failed to commit cancel transaction {transaction.transaction_id}"
            )
        
        return ExecutionResult(
            success=True,
            order_id=order.order_id,
            message="Order cancelled successfully"
        )
    
    def recover_pending_transactions(self) -> None:
        """
        Recover pending transactions after a system restart.
        This should be called during system initialization.
        """
        pending_transactions = self.transaction_manager.get_pending_transactions()
        
        if not pending_transactions:
            logger.info("No pending transactions to recover")
            return
        
        logger.info(f"Recovering {len(pending_transactions)} pending transactions")
        
        for transaction in pending_transactions:
            try:
                self._handle_recovery(transaction)
            except Exception as e:
                logger.exception(f"Error recovering transaction {transaction.transaction_id}: {e}")
    
    def _handle_recovery(self, transaction: Transaction) -> None:
        """
        Handle recovery of a specific transaction.
        
        Args:
            transaction: Transaction to recover
        """
        if transaction.state == TransactionState.INITIAL:
            # Transaction was just created, abort it
            self.transaction_manager.abort(transaction.transaction_id, {})
            
        elif transaction.state == TransactionState.PREPARING:
            # Transaction was in prepare phase, abort it
            self.transaction_manager.abort(transaction.transaction_id, {})
            
        elif transaction.state == TransactionState.PREPARED:
            # Transaction was prepared but not committed, need to commit it
            # This would require reconstructing the participants
            logger.warning(
                f"Transaction {transaction.transaction_id} was prepared but not committed. "
                "Manual recovery required."
            )
            
        elif transaction.state == TransactionState.COMMITTING:
            # Transaction was in commit phase, need to finish committing
            logger.warning(
                f"Transaction {transaction.transaction_id} was in commit phase. "
                "Manual recovery required."
            )
            
        elif transaction.state == TransactionState.ABORTING:
            # Transaction was in abort phase, need to finish aborting
            logger.warning(
                f"Transaction {transaction.transaction_id} was in abort phase. "
                "Manual recovery required."
            )


class ExchangeParticipant(TransactionParticipant):
    """
    Transaction participant for exchange operations.
    """
    
    def __init__(self, exchange_adapter: 'ExchangeAdapter', order: Order):
        """
        Initialize exchange participant.
        
        Args:
            exchange_adapter: Exchange adapter to use
            order: Order to execute
        """
        super().__init__("exchange")
        self.exchange_adapter = exchange_adapter
        self.order = order
        self.exchange_order_id = None
        self.execution_details = {}
    
    def prepare(self, transaction: Transaction) -> bool:
        """
        Prepare for the transaction by validating order and checking exchange connectivity.
        
        Args:
            transaction: Transaction to prepare for
            
        Returns:
            True if prepared successfully, False otherwise
        """
        try:
            # Check exchange connectivity
            if not self.exchange_adapter.is_connected():
                logger.error(f"Exchange {self.exchange_adapter.exchange_id} is not connected")
                return False
            
            # Validate order with exchange
            validation_result = self.exchange_adapter.validate_order(self.order)
            if not validation_result["valid"]:
                logger.error(f"Order validation failed: {validation_result['message']}")
                return False
            
            # Store validation data in transaction
            transaction.data["validation_result"] = validation_result
            
            return True
        except Exception as e:
            logger.exception(f"Error preparing exchange participant: {e}")
            return False
    
    def commit(self, transaction: Transaction) -> bool:
        """
        Commit the transaction by submitting the order to the exchange.
        
        Args:
            transaction: Transaction to commit
            
        Returns:
            True if committed successfully, False otherwise
        """
        try:
            # Submit order to exchange
            result = self.exchange_adapter.submit_order(self.order)
            
            if not result["success"]:
                logger.error(f"Failed to submit order to exchange: {result['message']}")
                return False
            
            # Store execution details
            self.exchange_order_id = result.get("exchange_order_id")
            self.execution_details = {
                "execution_id": result.get("execution_id", str(uuid.uuid4())),
                "exchange_order_id": self.exchange_order_id,
                "filled_quantity": result.get("filled_quantity", 0.0),
                "fill_price": result.get("fill_price", 0.0),
                "fees": result.get("fees", 0.0)
            }
            
            # Update transaction data
            transaction.data["execution_details"] = self.execution_details
            
            return True
        except Exception as e:
            logger.exception(f"Error committing exchange participant: {e}")
            return False
    
    def abort(self, transaction: Transaction) -> bool:
        """
        Abort the transaction.
        
        Args:
            transaction: Transaction to abort
            
        Returns:
            True if aborted successfully, False otherwise
        """
        # Nothing to do for exchange participant during abort of a new order
        return True


class ExchangeCancelParticipant(TransactionParticipant):
    """
    Transaction participant for exchange cancel operations.
    """
    
    def __init__(self, exchange_adapter: 'ExchangeAdapter', order: Order):
        """
        Initialize exchange cancel participant.
        
        Args:
            exchange_adapter: Exchange adapter to use
            order: Order to cancel
        """
        super().__init__("exchange")
        self.exchange_adapter = exchange_adapter
        self.order = order
    
    def prepare(self, transaction: Transaction) -> bool:
        """
        Prepare for the cancel transaction by checking if the order can be cancelled.
        
        Args:
            transaction: Transaction to prepare for
            
        Returns:
            True if prepared successfully, False otherwise
        """
        try:
            # Check exchange connectivity
            if not self.exchange_adapter.is_connected():
                logger.error(f"Exchange {self.exchange_adapter.exchange_id} is not connected")
                return False
            
            # Check if order exists and can be cancelled
            check_result = self.exchange_adapter.check_order_status(self.order)
            if not check_result["success"]:
                logger.error(f"Failed to check order status: {check_result['message']}")
                return False
            
            # Store check result in transaction
            transaction.data["check_result"] = check_result
            
            return True
        except Exception as e:
            logger.exception(f"Error preparing exchange cancel participant: {e}")
            return False
    
    def commit(self, transaction: Transaction) -> bool:
        """
        Commit the cancel transaction by cancelling the order on the exchange.
        
        Args:
            transaction: Transaction to commit
            
        Returns:
            True if committed successfully, False otherwise
        """
        try:
            # Cancel order on exchange
            result = self.exchange_adapter.cancel_order(self.order)
            
            if not result["success"]:
                logger.error(f"Failed to cancel order on exchange: {result['message']}")
                return False
            
            # Update transaction data
            transaction.data["cancel_result"] = result
            
            return True
        except Exception as e:
            logger.exception(f"Error committing exchange cancel participant: {e}")
            return False
    
    def abort(self, transaction: Transaction) -> bool:
        """
        Abort the cancel transaction.
        
        Args:
            transaction: Transaction to abort
            
        Returns:
            True if aborted successfully, False otherwise
        """
        # Nothing to do for exchange cancel participant during abort
        return True


class PositionParticipant(TransactionParticipant):
    """
    Transaction participant for position operations.
    """
    
    def __init__(self, position_manager: 'PositionManager', order: Order):
        """
        Initialize position participant.
        
        Args:
            position_manager: Position manager to use
            order: Order being executed
        """
        super().__init__("position")
        self.position_manager = position_manager
        self.order = order
        self.original_position = None
    
    def prepare(self, transaction: Transaction) -> bool:
        """
        Prepare for the transaction by checking and saving the current position state.
        
        Args:
            transaction: Transaction to prepare for
            
        Returns:
            True if prepared successfully, False otherwise
        """
        try:
            if self.position_manager is None:
                logger.warning("Position manager not set, skipping position validation")
                return True
            
            # Get current position
            self.original_position = self.position_manager.get_position(
                self.order.symbol, self.order.strategy_id
            )
            
            # Save original position data in transaction
            if self.original_position:
                transaction.data["original_position"] = self.original_position.to_dict()
            else:
                transaction.data["original_position"] = None
            
            return True
        except Exception as e:
            logger.exception(f"Error preparing position participant: {e}")
            return False
    
    def commit(self, transaction: Transaction) -> bool:
        """
        Commit the transaction by updating the position based on execution details.
        
        Args:
            transaction: Transaction to commit
            
        Returns:
            True if committed successfully, False otherwise
        """
        try:
            if self.position_manager is None:
                logger.warning("Position manager not set, skipping position update")
                return True
            
            # Get execution details from transaction
            execution_details = transaction.data.get("execution_details", {})
            filled_quantity = execution_details.get("filled_quantity", 0.0)
            fill_price = execution_details.get("fill_price", 0.0)
            
            if filled_quantity <= 0:
                logger.info("No quantity filled, skipping position update")
                return True
            
            # Calculate signed quantity based on order side
            sign = 1 if self.order.side.value.lower() == "buy" else -1
            signed_quantity = sign * filled_quantity
            
            # Update position
            self.position_manager.update_position(
                self.order.symbol,
                signed_quantity,
                fill_price,
                self.order.strategy_id
            )
            
            return True
        except Exception as e:
            logger.exception(f"Error committing position participant: {e}")
            return False
    
    def abort(self, transaction: Transaction) -> bool:
        """
        Abort the transaction.
        
        Args:
            transaction: Transaction to abort
            
        Returns:
            True if aborted successfully, False otherwise
        """
        # Nothing to do for position participant during abort of a new transaction
        return True


class OrderParticipant(TransactionParticipant):
    """
    Transaction participant for order operations.
    """
    
    def __init__(self, order_manager: 'OrderManager', order: Order):
        """
        Initialize order participant.
        
        Args:
            order_manager: Order manager to use
            order: Order being executed
        """
        super().__init__("order")
        self.order_manager = order_manager
        self.order = order
        self.original_order_state = None
    
    def prepare(self, transaction: Transaction) -> bool:
        """
        Prepare for the transaction by checking and saving the current order state.
        
        Args:
            transaction: Transaction to prepare for
            
        Returns:
            True if prepared successfully, False otherwise
        """
        try:
            if self.order_manager is None:
                logger.warning("Order manager not set, skipping order validation")
                return True
            
            # Check if order exists and has valid status
            existing_order = self.order_manager.get_order(self.order.order_id)
            if not existing_order:
                logger.error(f"Order {self.order.order_id} not found")
                return False
            
            if existing_order.status not in [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED]:
                logger.error(f"Order {self.order.order_id} has invalid status {existing_order.status.value}")
                return False
            
            # Save original order state
            self.original_order_state = existing_order.to_dict()
            transaction.data["original_order_state"] = self.original_order_state
            
            return True
        except Exception as e:
            logger.exception(f"Error preparing order participant: {e}")
            return False
    
    def commit(self, transaction: Transaction) -> bool:
        """
        Commit the transaction by updating the order based on execution details.
        
        Args:
            transaction: Transaction to commit
            
        Returns:
            True if committed successfully, False otherwise
        """
        try:
            if self.order_manager is None:
                logger.warning("Order manager not set, skipping order update")
                return True
            
            # Get execution details from transaction
            execution_details = transaction.data.get("execution_details", {})
            filled_quantity = execution_details.get("filled_quantity", 0.0)
            exchange_order_id = execution_details.get("exchange_order_id")
            
            # Update order with execution details
            if filled_quantity > 0:
                # Get the current order
                current_order = self.order_manager.get_order(self.order.order_id)
                
                # Update filled quantity
                new_filled_quantity = current_order.filled_quantity + filled_quantity
                
                # Determine new status
                new_status = OrderStatus.PARTIALLY_FILLED
                if abs(new_filled_quantity - current_order.quantity) < 1e-8:
                    new_status = OrderStatus.FILLED
                
                # Update order
                self.order_manager.update_order(
                    self.order.order_id,
                    {
                        "filled_quantity": new_filled_quantity,
                        "status": new_status,
                        "exchange_order_id": exchange_order_id,
                        "last_update_time": time.time()
                    }
                )
            else:
                # Just update exchange_order_id if no quantity was filled
                self.order_manager.update_order(
                    self.order.order_id,
                    {
                        "exchange_order_id": exchange_order_id,
                        "last_update_time": time.time()
                    }
                )
            
            return True
        except Exception as e:
            logger.exception(f"Error committing order participant: {e}")
            return False
    
    def abort(self, transaction: Transaction) -> bool:
        """
        Abort the transaction.
        
        Args:
            transaction: Transaction to abort
            
        Returns:
            True if aborted successfully, False otherwise
        """
        # Nothing specific to do for order participant during abort
        return True


class OrderCancelParticipant(TransactionParticipant):
    """
    Transaction participant for order cancel operations.
    """
    
    def __init__(self, order_manager: 'OrderManager', order: Order):
        """
        Initialize order cancel participant.
        
        Args:
            order_manager: Order manager to use
            order: Order to cancel
        """
        super().__init__("order")
        self.order_manager = order_manager
        self.order = order
        self.original_order_state = None
    
    def prepare(self, transaction: Transaction) -> bool:
        """
        Prepare for the cancel transaction by checking if the order can be cancelled.
        
        Args:
            transaction: Transaction to prepare for
            
        Returns:
            True if prepared successfully, False otherwise
        """
        try:
            if self.order_manager is None:
                logger.warning("Order manager not set, skipping order validation")
                return True
            
            # Check if order exists and has valid status for cancellation
            existing_order = self.order_manager.get_order(self.order.order_id)
            if not existing_order:
                logger.error(f"Order {self.order.order_id} not found")
                return False
            
            if existing_order.status not in [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED]:
                logger.error(f"Order {self.order.order_id} has invalid status for cancellation: {existing_order.status.value}")
                return False
            
            # Save original order state
            self.original_order_state = existing_order.to_dict()
            transaction.data["original_order_state"] = self.original_order_state
            
            return True
        except Exception as e:
            logger.exception(f"Error preparing order cancel participant: {e}")
            return False
    
    def commit(self, transaction: Transaction) -> bool:
        """
        Commit the cancel transaction by updating the order status.
        
        Args:
            transaction: Transaction to commit
            
        Returns:
            True if committed successfully, False otherwise
        """
        try:
            if self.order_manager is None:
                logger.warning("Order manager not set, skipping order update")
                return True
            
            # Update order status to CANCELLED
            self.order_manager.update_order(
                self.order.order_id,
                {
                    "status": OrderStatus.CANCELLED,
                    "last_update_time": time.time()
                }
            )
            
            return True
        except Exception as e:
            logger.exception(f"Error committing order cancel participant: {e}")
            return False
    
    def abort(self, transaction: Transaction) -> bool:
        """
        Abort the cancel transaction.
        
        Args:
            transaction: Transaction to abort
            
        Returns:
            True if aborted successfully, False otherwise
        """
        # Nothing specific to do for order cancel participant during abort
        return True 