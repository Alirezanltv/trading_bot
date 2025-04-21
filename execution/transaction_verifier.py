"""
Transaction Verification Pipeline

This module implements a three-phase commit transaction verification pipeline
for order execution to ensure reliability and fault tolerance.
"""

import asyncio
import time
import json
import uuid
from enum import Enum
from typing import Dict, Any, List, Optional, Callable, Tuple
from datetime import datetime, timedelta

from trading_system.core.component import Component, ComponentStatus
from trading_system.core.message_bus import MessageBus, MessageTypes
from trading_system.core.logging import get_logger
from trading_system.models.order import Order, OrderStatus

logger = get_logger("execution.transaction_verifier")

class TransactionPhase(Enum):
    """Transaction phases in the three-phase commit protocol."""
    PREPARE = "prepare"        # Phase 1: Prepare transaction
    PRE_COMMIT = "pre_commit"  # Phase 2: Pre-commit transaction
    COMMIT = "commit"          # Phase 3: Commit transaction
    ABORT = "abort"            # Abort transaction
    ROLLBACK = "rollback"      # Rollback transaction


class TransactionStatus(Enum):
    """Transaction status in the verification pipeline."""
    PENDING = "pending"        # Transaction is pending
    PREPARING = "preparing"    # Transaction is being prepared
    PREPARED = "prepared"      # Transaction has been prepared
    PRE_COMMITTED = "pre_committed"  # Transaction has been pre-committed
    COMMITTING = "committing"  # Transaction is being committed
    COMMITTED = "committed"    # Transaction has been committed
    ABORTING = "aborting"      # Transaction is being aborted
    ABORTED = "aborted"        # Transaction has been aborted
    ROLLING_BACK = "rolling_back"  # Transaction is being rolled back
    ROLLED_BACK = "rolled_back"    # Transaction has been rolled back
    FAILED = "failed"          # Transaction has failed
    TIMED_OUT = "timed_out"    # Transaction has timed out


class Transaction:
    """
    Represents a transaction in the verification pipeline.
    
    A transaction contains an order and metadata for tracking its state
    through the three-phase commit protocol.
    """
    
    def __init__(self, order: Order, transaction_id: Optional[str] = None):
        """
        Initialize a transaction.
        
        Args:
            order: Order to execute
            transaction_id: Optional transaction ID
        """
        self.transaction_id = transaction_id or f"txn_{uuid.uuid4().hex}"
        self.order = order
        self.status = TransactionStatus.PENDING
        self.phase = TransactionPhase.PREPARE
        self.timestamp_created = time.time()
        self.timestamp_updated = time.time()
        self.timestamp_committed = None
        self.attempts = 0
        self.max_attempts = 3
        self.timeout = 60  # 60 seconds timeout
        self.error = None
        self.metadata: Dict[str, Any] = {}
    
    def update_status(self, status: TransactionStatus, error: Optional[str] = None) -> None:
        """
        Update transaction status.
        
        Args:
            status: New status
            error: Optional error message
        """
        self.status = status
        self.timestamp_updated = time.time()
        
        if error:
            self.error = error
        
        if status == TransactionStatus.COMMITTED:
            self.timestamp_committed = self.timestamp_updated
    
    def update_phase(self, phase: TransactionPhase) -> None:
        """
        Update transaction phase.
        
        Args:
            phase: New phase
        """
        self.phase = phase
        self.timestamp_updated = time.time()
    
    def is_timed_out(self) -> bool:
        """
        Check if transaction has timed out.
        
        Returns:
            True if timed out, False otherwise
        """
        return time.time() - self.timestamp_updated > self.timeout
    
    def can_retry(self) -> bool:
        """
        Check if transaction can be retried.
        
        Returns:
            True if can retry, False otherwise
        """
        return self.attempts < self.max_attempts
    
    def increment_attempts(self) -> None:
        """Increment transaction attempts."""
        self.attempts += 1
        self.timestamp_updated = time.time()
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert transaction to dictionary.
        
        Returns:
            Dictionary representation of the transaction
        """
        return {
            "transaction_id": self.transaction_id,
            "order": self.order.to_dict(),
            "status": self.status.value,
            "phase": self.phase.value,
            "timestamp_created": self.timestamp_created,
            "timestamp_updated": self.timestamp_updated,
            "timestamp_committed": self.timestamp_committed,
            "attempts": self.attempts,
            "max_attempts": self.max_attempts,
            "timeout": self.timeout,
            "error": self.error,
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Transaction':
        """
        Create transaction from dictionary.
        
        Args:
            data: Dictionary representation of the transaction
            
        Returns:
            Transaction instance
        """
        order_data = data.get("order", {})
        order = Order.from_dict(order_data)
        
        transaction = cls(order, data.get("transaction_id"))
        transaction.status = TransactionStatus(data.get("status", TransactionStatus.PENDING.value))
        transaction.phase = TransactionPhase(data.get("phase", TransactionPhase.PREPARE.value))
        transaction.timestamp_created = data.get("timestamp_created", time.time())
        transaction.timestamp_updated = data.get("timestamp_updated", time.time())
        transaction.timestamp_committed = data.get("timestamp_committed")
        transaction.attempts = data.get("attempts", 0)
        transaction.max_attempts = data.get("max_attempts", 3)
        transaction.timeout = data.get("timeout", 60)
        transaction.error = data.get("error")
        transaction.metadata = data.get("metadata", {})
        
        return transaction


class TransactionVerifier(Component):
    """
    Transaction verification pipeline with three-phase commit protocol.
    
    This class implements a verification pipeline for order execution, ensuring
    that orders are executed reliably and with fault tolerance.
    """
    
    def __init__(self, config: Dict[str, Any], message_bus: MessageBus):
        """
        Initialize the transaction verifier.
        
        Args:
            config: Configuration dictionary
            message_bus: Message bus for communication
        """
        super().__init__(
            component_id="transaction_verifier",
            name="Transaction Verifier",
            config=config
        )
        
        self.message_bus = message_bus
        
        # Transaction tracking
        self.active_transactions: Dict[str, Transaction] = {}
        self.completed_transactions: Dict[str, Transaction] = {}
        self.failed_transactions: Dict[str, Transaction] = {}
        
        # Pipeline handlers
        self.prepare_handlers: List[Callable] = []
        self.pre_commit_handlers: List[Callable] = []
        self.commit_handlers: List[Callable] = []
        self.abort_handlers: List[Callable] = []
        self.rollback_handlers: List[Callable] = []
        
        # Configuration
        self.transaction_timeout = config.get("transaction_timeout", 60)
        self.max_retries = config.get("max_retries", 3)
        self.retry_delay = config.get("retry_delay", 1.0)
        
        # Task management
        self.cleanup_task = None
        
        logger.info("Transaction verifier initialized")
    
    async def initialize(self) -> bool:
        """
        Initialize the transaction verifier.
        
        Returns:
            Success flag
        """
        try:
            self._update_status(ComponentStatus.INITIALIZING)
            
            # Subscribe to order events
            await self.message_bus.subscribe(
                message_type=MessageTypes.ORDER,
                routing_key="order.new",
                callback=self._handle_new_order
            )
            
            await self.message_bus.subscribe(
                message_type=MessageTypes.ORDER,
                routing_key="order.cancel",
                callback=self._handle_cancel_order
            )
            
            # Start cleanup task
            self.cleanup_task = asyncio.create_task(self._cleanup_loop())
            
            self._update_status(ComponentStatus.OPERATIONAL)
            logger.info("Transaction verifier initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing transaction verifier: {str(e)}")
            self._update_status(ComponentStatus.ERROR)
            return False
    
    async def shutdown(self) -> bool:
        """
        Shutdown the transaction verifier.
        
        Returns:
            Success flag
        """
        try:
            self._update_status(ComponentStatus.SHUTTING_DOWN)
            
            # Cancel cleanup task
            if self.cleanup_task and not self.cleanup_task.done():
                self.cleanup_task.cancel()
                try:
                    await self.cleanup_task
                except asyncio.CancelledError:
                    pass
            
            # Complete any active transactions
            for transaction_id, transaction in list(self.active_transactions.items()):
                if transaction.status not in [
                    TransactionStatus.COMMITTED,
                    TransactionStatus.ABORTED,
                    TransactionStatus.ROLLED_BACK
                ]:
                    transaction.update_status(TransactionStatus.ABORTED, "Component shutdown")
                    self.completed_transactions[transaction_id] = transaction
                    del self.active_transactions[transaction_id]
            
            self._update_status(ComponentStatus.SHUTDOWN)
            logger.info("Transaction verifier shutdown complete")
            return True
            
        except Exception as e:
            logger.error(f"Error shutting down transaction verifier: {str(e)}")
            return False
    
    def register_prepare_handler(self, handler: Callable) -> None:
        """
        Register a prepare phase handler.
        
        Args:
            handler: Handler function
        """
        self.prepare_handlers.append(handler)
    
    def register_pre_commit_handler(self, handler: Callable) -> None:
        """
        Register a pre-commit phase handler.
        
        Args:
            handler: Handler function
        """
        self.pre_commit_handlers.append(handler)
    
    def register_commit_handler(self, handler: Callable) -> None:
        """
        Register a commit phase handler.
        
        Args:
            handler: Handler function
        """
        self.commit_handlers.append(handler)
    
    def register_abort_handler(self, handler: Callable) -> None:
        """
        Register an abort phase handler.
        
        Args:
            handler: Handler function
        """
        self.abort_handlers.append(handler)
    
    def register_rollback_handler(self, handler: Callable) -> None:
        """
        Register a rollback phase handler.
        
        Args:
            handler: Handler function
        """
        self.rollback_handlers.append(handler)
    
    async def execute_transaction(self, transaction: Transaction) -> Tuple[bool, Optional[str]]:
        """
        Execute a transaction through the verification pipeline.
        
        Args:
            transaction: Transaction to execute
            
        Returns:
            Success flag and optional error message
        """
        try:
            # Add to active transactions
            self.active_transactions[transaction.transaction_id] = transaction
            
            # Execute three-phase commit
            success, error = await self._execute_prepare_phase(transaction)
            if not success:
                await self._execute_abort_phase(transaction, error)
                return False, error
            
            success, error = await self._execute_pre_commit_phase(transaction)
            if not success:
                await self._execute_rollback_phase(transaction, error)
                return False, error
            
            success, error = await self._execute_commit_phase(transaction)
            if not success:
                await self._execute_rollback_phase(transaction, error)
                return False, error
            
            # Transaction successful
            transaction.update_status(TransactionStatus.COMMITTED)
            
            # Move to completed transactions
            self.completed_transactions[transaction.transaction_id] = transaction
            del self.active_transactions[transaction.transaction_id]
            
            # Publish event
            await self.message_bus.publish(
                message_type=MessageTypes.TRANSACTION,
                routing_key=f"transaction.committed.{transaction.transaction_id}",
                data=transaction.to_dict()
            )
            
            return True, None
            
        except Exception as e:
            error_msg = f"Error executing transaction: {str(e)}"
            logger.error(error_msg)
            
            # Abort transaction
            await self._execute_abort_phase(transaction, error_msg)
            
            return False, error_msg
    
    async def verify_transaction(self, transaction_id: str) -> Optional[Dict[str, Any]]:
        """
        Verify a transaction's status.
        
        Args:
            transaction_id: Transaction ID
            
        Returns:
            Transaction status or None if not found
        """
        # Check active transactions
        if transaction_id in self.active_transactions:
            return self.active_transactions[transaction_id].to_dict()
        
        # Check completed transactions
        if transaction_id in self.completed_transactions:
            return self.completed_transactions[transaction_id].to_dict()
        
        # Check failed transactions
        if transaction_id in self.failed_transactions:
            return self.failed_transactions[transaction_id].to_dict()
        
        return None
    
    async def _handle_new_order(self, message: Dict[str, Any]) -> None:
        """
        Handle new order message.
        
        Args:
            message: Order message
        """
        try:
            order_data = message.get("order")
            if not order_data:
                logger.error("Invalid order message: missing order data")
                return
            
            # Create order
            order = Order.from_dict(order_data)
            
            # Create transaction
            transaction = Transaction(order)
            
            # Execute transaction
            asyncio.create_task(self.execute_transaction(transaction))
            
        except Exception as e:
            logger.error(f"Error handling new order: {str(e)}")
    
    async def _handle_cancel_order(self, message: Dict[str, Any]) -> None:
        """
        Handle cancel order message.
        
        Args:
            message: Cancel order message
        """
        try:
            order_id = message.get("order_id")
            if not order_id:
                logger.error("Invalid cancel order message: missing order ID")
                return
            
            # Find transaction for order
            transaction_id = None
            for tid, txn in self.active_transactions.items():
                if txn.order.client_order_id == order_id:
                    transaction_id = tid
                    break
            
            if not transaction_id:
                logger.warning(f"Order {order_id} not found in active transactions")
                return
            
            # Abort transaction
            transaction = self.active_transactions[transaction_id]
            await self._execute_abort_phase(transaction, "Order cancelled")
            
        except Exception as e:
            logger.error(f"Error handling cancel order: {str(e)}")
    
    async def _execute_prepare_phase(self, transaction: Transaction) -> Tuple[bool, Optional[str]]:
        """
        Execute prepare phase for a transaction.
        
        Args:
            transaction: Transaction to prepare
            
        Returns:
            Success flag and optional error message
        """
        try:
            logger.info(f"Preparing transaction {transaction.transaction_id}")
            
            # Update transaction state
            transaction.update_status(TransactionStatus.PREPARING)
            transaction.update_phase(TransactionPhase.PREPARE)
            
            # Execute prepare handlers
            for handler in self.prepare_handlers:
                success, error = await handler(transaction)
                if not success:
                    logger.error(f"Prepare phase failed: {error}")
                    return False, error
            
            # Update transaction state
            transaction.update_status(TransactionStatus.PREPARED)
            
            # Publish event
            await self.message_bus.publish(
                message_type=MessageTypes.TRANSACTION,
                routing_key=f"transaction.prepared.{transaction.transaction_id}",
                data=transaction.to_dict()
            )
            
            return True, None
            
        except Exception as e:
            error_msg = f"Error in prepare phase: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
    
    async def _execute_pre_commit_phase(self, transaction: Transaction) -> Tuple[bool, Optional[str]]:
        """
        Execute pre-commit phase for a transaction.
        
        Args:
            transaction: Transaction to pre-commit
            
        Returns:
            Success flag and optional error message
        """
        try:
            logger.info(f"Pre-committing transaction {transaction.transaction_id}")
            
            # Update transaction state
            transaction.update_phase(TransactionPhase.PRE_COMMIT)
            
            # Execute pre-commit handlers
            for handler in self.pre_commit_handlers:
                success, error = await handler(transaction)
                if not success:
                    logger.error(f"Pre-commit phase failed: {error}")
                    return False, error
            
            # Update transaction state
            transaction.update_status(TransactionStatus.PRE_COMMITTED)
            
            # Publish event
            await self.message_bus.publish(
                message_type=MessageTypes.TRANSACTION,
                routing_key=f"transaction.pre_committed.{transaction.transaction_id}",
                data=transaction.to_dict()
            )
            
            return True, None
            
        except Exception as e:
            error_msg = f"Error in pre-commit phase: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
    
    async def _execute_commit_phase(self, transaction: Transaction) -> Tuple[bool, Optional[str]]:
        """
        Execute commit phase for a transaction.
        
        Args:
            transaction: Transaction to commit
            
        Returns:
            Success flag and optional error message
        """
        try:
            logger.info(f"Committing transaction {transaction.transaction_id}")
            
            # Update transaction state
            transaction.update_status(TransactionStatus.COMMITTING)
            transaction.update_phase(TransactionPhase.COMMIT)
            
            # Execute commit handlers
            for handler in self.commit_handlers:
                success, error = await handler(transaction)
                if not success:
                    logger.error(f"Commit phase failed: {error}")
                    return False, error
            
            # Update order status
            transaction.order.update_status(OrderStatus.ACCEPTED)
            
            # Publish event
            await self.message_bus.publish(
                message_type=MessageTypes.TRANSACTION,
                routing_key=f"transaction.committed.{transaction.transaction_id}",
                data=transaction.to_dict()
            )
            
            return True, None
            
        except Exception as e:
            error_msg = f"Error in commit phase: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
    
    async def _execute_abort_phase(self, transaction: Transaction, reason: str) -> None:
        """
        Execute abort phase for a transaction.
        
        Args:
            transaction: Transaction to abort
            reason: Reason for abort
        """
        try:
            logger.info(f"Aborting transaction {transaction.transaction_id}: {reason}")
            
            # Update transaction state
            transaction.update_status(TransactionStatus.ABORTING)
            transaction.update_phase(TransactionPhase.ABORT)
            
            # Execute abort handlers
            for handler in self.abort_handlers:
                try:
                    await handler(transaction, reason)
                except Exception as e:
                    logger.error(f"Error in abort handler: {str(e)}")
            
            # Update transaction state
            transaction.update_status(TransactionStatus.ABORTED, reason)
            
            # Update order status
            transaction.order.update_status(OrderStatus.REJECTED)
            
            # Move to failed transactions
            self.failed_transactions[transaction.transaction_id] = transaction
            if transaction.transaction_id in self.active_transactions:
                del self.active_transactions[transaction.transaction_id]
            
            # Publish event
            await self.message_bus.publish(
                message_type=MessageTypes.TRANSACTION,
                routing_key=f"transaction.aborted.{transaction.transaction_id}",
                data=transaction.to_dict()
            )
            
        except Exception as e:
            logger.error(f"Error in abort phase: {str(e)}")
    
    async def _execute_rollback_phase(self, transaction: Transaction, reason: str) -> None:
        """
        Execute rollback phase for a transaction.
        
        Args:
            transaction: Transaction to rollback
            reason: Reason for rollback
        """
        try:
            logger.info(f"Rolling back transaction {transaction.transaction_id}: {reason}")
            
            # Update transaction state
            transaction.update_status(TransactionStatus.ROLLING_BACK)
            transaction.update_phase(TransactionPhase.ROLLBACK)
            
            # Execute rollback handlers
            for handler in self.rollback_handlers:
                try:
                    await handler(transaction, reason)
                except Exception as e:
                    logger.error(f"Error in rollback handler: {str(e)}")
            
            # Update transaction state
            transaction.update_status(TransactionStatus.ROLLED_BACK, reason)
            
            # Update order status
            transaction.order.update_status(OrderStatus.REJECTED)
            
            # Move to failed transactions
            self.failed_transactions[transaction.transaction_id] = transaction
            if transaction.transaction_id in self.active_transactions:
                del self.active_transactions[transaction.transaction_id]
            
            # Publish event
            await self.message_bus.publish(
                message_type=MessageTypes.TRANSACTION,
                routing_key=f"transaction.rolled_back.{transaction.transaction_id}",
                data=transaction.to_dict()
            )
            
        except Exception as e:
            logger.error(f"Error in rollback phase: {str(e)}")
    
    async def _cleanup_loop(self) -> None:
        """Cleanup loop for transactions."""
        while True:
            try:
                # Check for timed out transactions
                current_time = time.time()
                
                for transaction_id, transaction in list(self.active_transactions.items()):
                    # Check for timeout
                    if current_time - transaction.timestamp_updated > self.transaction_timeout:
                        logger.warning(f"Transaction {transaction_id} timed out")
                        transaction.update_status(TransactionStatus.TIMED_OUT, "Transaction timed out")
                        
                        # Retry or abort
                        if transaction.can_retry():
                            logger.info(f"Retrying transaction {transaction_id}")
                            transaction.increment_attempts()
                            # Reset transaction
                            transaction.update_status(TransactionStatus.PENDING)
                            transaction.update_phase(TransactionPhase.PREPARE)
                            # Execute transaction again
                            asyncio.create_task(self.execute_transaction(transaction))
                        else:
                            logger.warning(f"Transaction {transaction_id} exceeded retry limit")
                            await self._execute_abort_phase(transaction, "Exceeded retry limit")
                
                # Prune old completed transactions (keep last 1000)
                if len(self.completed_transactions) > 1000:
                    # Sort by timestamp
                    sorted_transactions = sorted(
                        self.completed_transactions.items(),
                        key=lambda x: x[1].timestamp_committed or 0
                    )
                    # Remove oldest
                    for i in range(len(sorted_transactions) - 1000):
                        del self.completed_transactions[sorted_transactions[i][0]]
                
                # Prune old failed transactions (keep last 1000)
                if len(self.failed_transactions) > 1000:
                    # Sort by timestamp
                    sorted_transactions = sorted(
                        self.failed_transactions.items(),
                        key=lambda x: x[1].timestamp_updated
                    )
                    # Remove oldest
                    for i in range(len(sorted_transactions) - 1000):
                        del self.failed_transactions[sorted_transactions[i][0]]
                
                # Sleep for a bit
                await asyncio.sleep(1)
                
            except asyncio.CancelledError:
                logger.info("Cleanup loop cancelled")
                break
                
            except Exception as e:
                logger.error(f"Error in cleanup loop: {str(e)}")
                await asyncio.sleep(1) 