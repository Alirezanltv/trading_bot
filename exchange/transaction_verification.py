"""
Transaction Verification Pipeline

This module implements a robust three-phase commit protocol for transaction verification,
ensuring that orders are properly executed with validation, submission, and verification steps.

Features:
- Three-phase commit protocol (validation, submission, verification)
- Transaction logging and audit trail
- Retry mechanisms and fallback strategies
- Verification through order status and order book analysis
"""

import os
import time
import uuid
import json
import asyncio
import logging
from enum import Enum
from typing import Dict, List, Any, Optional, Union, Tuple, Callable
from datetime import datetime, timedelta

from trading_system.core.component import Component, ComponentStatus
from trading_system.core.logging import get_logger
from trading_system.execution.orders import Order, OrderStatus, OrderSide, OrderType

logger = get_logger("exchange.transaction_verification")

class TransactionPhase(Enum):
    """Transaction phases for the three-phase commit protocol."""
    PREPARE = "prepare"       # Initial validation
    COMMIT = "commit"         # Submission to exchange
    VERIFY = "verify"         # Verification of execution
    RECONCILE = "reconcile"   # Post-execution reconciliation
    COMPLETE = "complete"     # Transaction complete
    ROLLBACK = "rollback"     # Transaction rolled back
    FAILED = "failed"         # Transaction failed

class TransactionStatus(Enum):
    """Transaction status states."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"
    UNKNOWN = "unknown"

class TransactionError(Exception):
    """Base error for transaction verification issues."""
    pass

class ValidationError(TransactionError):
    """Error during transaction validation."""
    pass

class SubmissionError(TransactionError):
    """Error during transaction submission."""
    pass

class VerificationError(TransactionError):
    """Error during transaction verification."""
    pass

class ReconciliationError(TransactionError):
    """Error during transaction reconciliation."""
    pass

class Transaction:
    """Represents a transaction with its state and execution history."""
    
    def __init__(self, order: Order, transaction_id: str = None):
        """
        Initialize a transaction.
        
        Args:
            order: The order to execute
            transaction_id: Optional transaction ID (generated if not provided)
        """
        self.transaction_id = transaction_id or str(uuid.uuid4())
        self.order = order
        self.exchange_order_id = None
        self.status = TransactionStatus.PENDING
        self.current_phase = TransactionPhase.PREPARE
        self.creation_time = datetime.now()
        self.last_update_time = self.creation_time
        self.completion_time = None
        self.phases = []
        self.errors = []
        self.verification_attempts = 0
        self.max_verification_attempts = 5
        self.fallback_actions = []
        self.is_reconciled = False
        
        # Results from each phase
        self.validation_result = None
        self.submission_result = None
        self.verification_result = None
        self.reconciliation_result = None
        
        # Shadow state
        self.expected_fills = []
        self.confirmed_fills = []
        self.expected_status = None
    
    def update_phase(self, phase: TransactionPhase, result: Dict[str, Any] = None):
        """
        Update the transaction phase.
        
        Args:
            phase: New phase
            result: Optional result data
        """
        self.current_phase = phase
        self.last_update_time = datetime.now()
        
        phase_entry = {
            "phase": phase.value,
            "timestamp": self.last_update_time.isoformat(),
            "result": result or {}
        }
        
        self.phases.append(phase_entry)
        
        # Update specific phase results
        if phase == TransactionPhase.PREPARE:
            self.validation_result = result
        elif phase == TransactionPhase.COMMIT:
            self.submission_result = result
            if result and "order_id" in result:
                self.exchange_order_id = result["order_id"]
        elif phase == TransactionPhase.VERIFY:
            self.verification_result = result
        elif phase == TransactionPhase.RECONCILE:
            self.reconciliation_result = result
            self.is_reconciled = True
        
        # Update status based on phase
        if phase == TransactionPhase.PREPARE:
            self.status = TransactionStatus.IN_PROGRESS
        elif phase == TransactionPhase.COMPLETE:
            self.status = TransactionStatus.COMPLETED
            self.completion_time = datetime.now()
        elif phase == TransactionPhase.ROLLBACK:
            self.status = TransactionStatus.ROLLED_BACK
            self.completion_time = datetime.now()
        elif phase == TransactionPhase.FAILED:
            self.status = TransactionStatus.FAILED
            self.completion_time = datetime.now()
    
    def add_error(self, phase: TransactionPhase, error: Exception, data: Dict[str, Any] = None):
        """
        Add an error to the transaction log.
        
        Args:
            phase: Phase where the error occurred
            error: The exception
            data: Optional data related to the error
        """
        self.last_update_time = datetime.now()
        
        error_entry = {
            "phase": phase.value,
            "timestamp": self.last_update_time.isoformat(),
            "error": str(error),
            "data": data or {}
        }
        
        self.errors.append(error_entry)
    
    def add_fallback_action(self, action: str, result: Dict[str, Any] = None):
        """
        Add a fallback action to the transaction log.
        
        Args:
            action: Action description
            result: Optional result data
        """
        self.last_update_time = datetime.now()
        
        fallback_entry = {
            "action": action,
            "timestamp": self.last_update_time.isoformat(),
            "result": result or {}
        }
        
        self.fallback_actions.append(fallback_entry)
    
    def increment_verification_attempts(self) -> bool:
        """
        Increment verification attempts counter.
        
        Returns:
            True if max attempts not reached, False otherwise
        """
        self.verification_attempts += 1
        return self.verification_attempts <= self.max_verification_attempts
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert transaction to dictionary.
        
        Returns:
            Dictionary representation
        """
        # Handle side and type which could be strings or enums
        side = self.order.side.value if hasattr(self.order.side, 'value') else self.order.side
        
        # Handle case when order doesn't have a type attribute
        order_type = None
        if hasattr(self.order, 'type'):
            order_type = self.order.type.value if hasattr(self.order.type, 'value') else self.order.type
        
        return {
            "transaction_id": self.transaction_id,
            "order_id": self.order.order_id,
            "exchange_order_id": self.exchange_order_id,
            "symbol": self.order.symbol,
            "side": side,
            "type": order_type,
            "quantity": self.order.quantity,
            "price": self.order.price,
            "status": self.status.value,
            "current_phase": self.current_phase.value,
            "phases": self.phases,
            "errors": self.errors,
            "fallback_actions": self.fallback_actions,
            "creation_time": self.creation_time.isoformat(),
            "last_update_time": self.last_update_time.isoformat(),
            "completion_time": self.completion_time.isoformat() if self.completion_time else None,
            "verification_attempts": self.verification_attempts,
            "is_reconciled": self.is_reconciled
        }

class TransactionVerificationPipeline(Component):
    """
    Transaction Verification Pipeline implementing the three-phase commit protocol.
    
    This pipeline ensures that orders are executed reliably by breaking down the process into
    validation, submission, and verification phases, with fallback strategies for each phase.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the transaction verification pipeline.
        
        Args:
            config: Configuration dictionary containing:
                - max_transactions: Maximum number of transactions to keep in memory (default: 1000)
                - verification_interval: Interval for verification attempts in seconds (default: 2)
                - max_verification_attempts: Maximum verification attempts (default: 5)
                - verification_timeout: Timeout for verification in seconds (default: 30)
                - initial_verification_delay: Initial delay before verification in seconds (default: 0.5)
                - storage_path: Path for transaction log storage (default: "./logs/transactions")
                - validation_timeout: Timeout for validation in seconds (default: 5)
                - submission_timeout: Timeout for submission in seconds (default: 10)
                - reconciliation_interval: Interval for reconciliation in seconds (default: 3600)
                - retry_delay: Base delay for retries in seconds (default: 1)
        """
        super().__init__(name="TransactionVerificationPipeline")
        
        # Configuration
        self.config = config or {}
        self.max_transactions = self.config.get("max_transactions", 1000)
        self.verification_interval = self.config.get("verification_interval", 2)
        self.max_verification_attempts = self.config.get("max_verification_attempts", 5)
        self.verification_timeout = self.config.get("verification_timeout", 30)
        self.initial_verification_delay = self.config.get("initial_verification_delay", 0.5)
        self.storage_path = self.config.get("storage_path", "./logs/transactions")
        self.validation_timeout = self.config.get("validation_timeout", 5)
        self.submission_timeout = self.config.get("submission_timeout", 10)
        self.reconciliation_interval = self.config.get("reconciliation_interval", 3600)
        self.retry_delay = self.config.get("retry_delay", 1)
        
        # Transaction storage
        self.transactions = {}
        self.transaction_lock = asyncio.Lock()
        
        # Function handlers
        self.validate_func = None
        self.submit_func = None
        self.verify_func = None
        self.reconcile_func = None
        self.cancel_func = None
        
        # Background tasks
        self.verification_task = None
        self.reconciliation_task = None
    
    async def initialize(self) -> bool:
        """
        Initialize the transaction verification pipeline.
        
        Returns:
            Initialization success
        """
        try:
            self._status = ComponentStatus.INITIALIZING
            logger.info("Initializing Transaction Verification Pipeline")
            
            # Create storage directory if needed
            os.makedirs(self.storage_path, exist_ok=True)
            
            # Load saved transactions
            await self._load_saved_transactions()
            
            # Start background tasks
            self.verification_task = asyncio.create_task(self._verification_loop())
            self.reconciliation_task = asyncio.create_task(self._reconciliation_loop())
            
            self._status = ComponentStatus.INITIALIZED
            logger.info("Transaction Verification Pipeline initialized")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing Transaction Verification Pipeline: {str(e)}")
            self._status = ComponentStatus.ERROR
            self._error = str(e)
            return False
    
    async def stop(self) -> bool:
        """
        Stop the transaction verification pipeline.
        
        Returns:
            Stop success
        """
        try:
            logger.info("Stopping Transaction Verification Pipeline")
            
            # Cancel background tasks
            if self.verification_task:
                self.verification_task.cancel()
                try:
                    await self.verification_task
                except asyncio.CancelledError:
                    pass
                self.verification_task = None
            
            if self.reconciliation_task:
                self.reconciliation_task.cancel()
                try:
                    await self.reconciliation_task
                except asyncio.CancelledError:
                    pass
                self.reconciliation_task = None
            
            # Save transactions
            await self._save_transactions()
            
            self._status = ComponentStatus.STOPPED
            logger.info("Transaction Verification Pipeline stopped")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping Transaction Verification Pipeline: {str(e)}")
            self._status = ComponentStatus.ERROR
            self._error = str(e)
            return False
    
    def register_handlers(self,
                         validate_func: Callable,
                         submit_func: Callable,
                         verify_func: Callable,
                         reconcile_func: Optional[Callable] = None,
                         cancel_func: Optional[Callable] = None):
        """
        Register handler functions for the pipeline phases.
        
        Args:
            validate_func: Function to validate an order
            submit_func: Function to submit an order
            verify_func: Function to verify an order
            reconcile_func: Function to reconcile an order
            cancel_func: Function to cancel an order
        """
        self.validate_func = validate_func
        self.submit_func = submit_func
        self.verify_func = verify_func
        self.reconcile_func = reconcile_func
        self.cancel_func = cancel_func
        
        logger.info("Handler functions registered")
    
    async def execute_order(self, order: Order) -> Dict[str, Any]:
        """
        Execute an order through the verification pipeline.
        
        Args:
            order: Order to execute
            
        Returns:
            Execution result
            
        Raises:
            TransactionError: If execution fails
        """
        if not self.validate_func or not self.submit_func or not self.verify_func:
            raise TransactionError("Handler functions not registered")
        
        # Create transaction
        transaction = Transaction(order)
        
        # Add to storage
        async with self.transaction_lock:
            self.transactions[transaction.transaction_id] = transaction
            
            # Remove old transactions if needed
            if len(self.transactions) > self.max_transactions:
                oldest_transaction_id = min(
                    self.transactions.keys(),
                    key=lambda tid: self.transactions[tid].creation_time
                )
                del self.transactions[oldest_transaction_id]
        
        logger.info(f"Starting transaction {transaction.transaction_id} for order {order.order_id}")
        
        try:
            # Phase 1: Validation
            validation_result = await self._validate_transaction(transaction)
            
            # Phase 2: Submission
            submission_result = await self._submit_transaction(transaction)
            
            # Phase 3: Initial verification (async continuation in the verification loop)
            # Just do an initial check and return, the verification loop will continue checking
            await asyncio.sleep(self.initial_verification_delay)
            await self._verify_transaction(transaction)
            
            # Return current status
            return {
                "transaction_id": transaction.transaction_id,
                "order_id": order.order_id,
                "exchange_order_id": transaction.exchange_order_id,
                "status": transaction.status.value,
                "phase": transaction.current_phase.value,
                "is_complete": transaction.status in [
                    TransactionStatus.COMPLETED,
                    TransactionStatus.FAILED,
                    TransactionStatus.ROLLED_BACK
                ]
            }
            
        except Exception as e:
            logger.error(f"Error executing transaction {transaction.transaction_id}: {str(e)}")
            transaction.add_error(transaction.current_phase, e)
            transaction.update_phase(TransactionPhase.FAILED)
            
            # Try to cancel if we've submitted but verification failed
            if (
                transaction.current_phase == TransactionPhase.VERIFY and 
                transaction.exchange_order_id and 
                self.cancel_func
            ):
                try:
                    await self._cancel_transaction(transaction)
                except Exception as cancel_error:
                    logger.error(f"Error cancelling failed transaction: {str(cancel_error)}")
            
            raise TransactionError(f"Transaction execution failed: {str(e)}")
    
    async def get_transaction(self, transaction_id: str) -> Optional[Dict[str, Any]]:
        """
        Get transaction details.
        
        Args:
            transaction_id: Transaction ID
            
        Returns:
            Transaction details or None if not found
        """
        async with self.transaction_lock:
            if transaction_id in self.transactions:
                return self.transactions[transaction_id].to_dict()
            
            return None
    
    async def get_order_transaction(self, order_id: str) -> Optional[Dict[str, Any]]:
        """
        Get transaction by order ID.
        
        Args:
            order_id: Order ID
            
        Returns:
            Transaction details or None if not found
        """
        async with self.transaction_lock:
            for transaction in self.transactions.values():
                if transaction.order.order_id == order_id:
                    return transaction.to_dict()
            
            return None
    
    async def get_transactions(self, status: Optional[TransactionStatus] = None) -> List[Dict[str, Any]]:
        """
        Get all transactions, optionally filtered by status.
        
        Args:
            status: Optional status to filter by
            
        Returns:
            List of transaction dictionaries
        """
        async with self.transaction_lock:
            transactions = []
            
            for transaction in self.transactions.values():
                if status is None or transaction.status == status:
                    transactions.append(transaction.to_dict())
            
            return transactions
    
    async def cancel_transaction(self, transaction_id: str) -> Dict[str, Any]:
        """
        Cancel a transaction.
        
        Args:
            transaction_id: Transaction ID
            
        Returns:
            Cancellation result
            
        Raises:
            TransactionError: If cancellation fails or transaction not found
        """
        if not self.cancel_func:
            raise TransactionError("Cancel function not registered")
        
        async with self.transaction_lock:
            if transaction_id not in self.transactions:
                raise TransactionError(f"Transaction {transaction_id} not found")
            
            transaction = self.transactions[transaction_id]
        
        logger.info(f"Cancelling transaction {transaction_id}")
        
        try:
            return await self._cancel_transaction(transaction)
        except Exception as e:
            logger.error(f"Error cancelling transaction {transaction_id}: {str(e)}")
            raise TransactionError(f"Transaction cancellation failed: {str(e)}")
    
    async def _validate_transaction(self, transaction: Transaction) -> Dict[str, Any]:
        """
        Validate a transaction.
        
        Args:
            transaction: Transaction to validate
            
        Returns:
            Validation result
            
        Raises:
            ValidationError: If validation fails
        """
        logger.info(f"Validating transaction {transaction.transaction_id}")
        transaction.update_phase(TransactionPhase.PREPARE)
        
        try:
            # Execute validation with timeout
            validation_task = asyncio.create_task(
                self.validate_func(transaction.order)
            )
            
            try:
                validation_result = await asyncio.wait_for(
                    validation_task,
                    timeout=self.validation_timeout
                )
            except asyncio.TimeoutError:
                validation_task.cancel()
                raise ValidationError(f"Validation timed out after {self.validation_timeout} seconds")
            
            if not validation_result.get("is_valid", False):
                raise ValidationError(f"Order validation failed: {validation_result.get('reason', 'Unknown reason')}")
            
            # Update transaction
            transaction.update_phase(TransactionPhase.PREPARE, validation_result)
            
            return validation_result
            
        except Exception as e:
            transaction.add_error(TransactionPhase.PREPARE, e)
            transaction.update_phase(TransactionPhase.FAILED)
            raise ValidationError(f"Transaction validation failed: {str(e)}")
    
    async def _submit_transaction(self, transaction: Transaction) -> Dict[str, Any]:
        """
        Submit a transaction.
        
        Args:
            transaction: Transaction to submit
            
        Returns:
            Submission result
            
        Raises:
            SubmissionError: If submission fails
        """
        logger.info(f"Submitting transaction {transaction.transaction_id}")
        transaction.update_phase(TransactionPhase.COMMIT)
        
        try:
            # Execute submission with timeout
            submission_task = asyncio.create_task(
                self.submit_func(transaction.order)
            )
            
            try:
                submission_result = await asyncio.wait_for(
                    submission_task,
                    timeout=self.submission_timeout
                )
            except asyncio.TimeoutError:
                submission_task.cancel()
                raise SubmissionError(f"Submission timed out after {self.submission_timeout} seconds")
            
            if not submission_result.get("success", False):
                raise SubmissionError(f"Order submission failed: {submission_result.get('error', 'Unknown error')}")
            
            # Update transaction
            transaction.update_phase(TransactionPhase.COMMIT, submission_result)
            
            return submission_result
            
        except Exception as e:
            transaction.add_error(TransactionPhase.COMMIT, e)
            transaction.update_phase(TransactionPhase.FAILED)
            raise SubmissionError(f"Transaction submission failed: {str(e)}")
    
    async def _verify_transaction(self, transaction: Transaction) -> Dict[str, Any]:
        """
        Verify a transaction.
        
        Args:
            transaction: Transaction to verify
            
        Returns:
            Verification result
            
        Raises:
            VerificationError: If verification fails
        """
        logger.info(f"Verifying transaction {transaction.transaction_id}")
        
        # Skip if already complete
        if transaction.status in [TransactionStatus.COMPLETED, TransactionStatus.FAILED, TransactionStatus.ROLLED_BACK]:
            return {
                "success": True,
                "status": transaction.status.value,
                "message": "Transaction already complete"
            }
        
        # Check if we have an order ID
        if not transaction.exchange_order_id:
            transaction.add_error(
                TransactionPhase.VERIFY,
                VerificationError("No exchange order ID available for verification")
            )
            transaction.update_phase(TransactionPhase.FAILED)
            raise VerificationError("No exchange order ID available for verification")
        
        try:
            # Update phase
            transaction.update_phase(TransactionPhase.VERIFY)
            
            # Create transaction data with the order_id for verification
            transaction_data = {"order_id": transaction.exchange_order_id}
            
            # Execute verification
            verification_result = await self.verify_func(
                transaction.order,
                transaction_data
            )
            
            if not verification_result.get("is_verified", False):
                # If verification failed, increment attempt counter
                if transaction.increment_verification_attempts():
                    # We'll try again later
                    logger.warning(
                        f"Verification failed for transaction {transaction.transaction_id}, "
                        f"will retry ({transaction.verification_attempts}/{transaction.max_verification_attempts})"
                    )
                    
                    transaction.add_error(
                        TransactionPhase.VERIFY,
                        VerificationError(f"Verification failed: {verification_result.get('error', 'Unknown error')}")
                    )
                    
                    return verification_result
                else:
                    # Max attempts reached, mark as failed
                    logger.error(
                        f"Verification failed for transaction {transaction.transaction_id} "
                        f"after {transaction.verification_attempts} attempts"
                    )
                    
                    transaction.add_error(
                        TransactionPhase.VERIFY,
                        VerificationError("Max verification attempts reached")
                    )
                    
                    transaction.update_phase(TransactionPhase.FAILED)
                    
                    # Try to cancel if possible
                    if self.cancel_func:
                        try:
                            await self._cancel_transaction(transaction)
                        except Exception as cancel_error:
                            logger.error(f"Error cancelling failed transaction: {str(cancel_error)}")
                    
                    raise VerificationError("Max verification attempts reached")
            
            # Update transaction
            transaction.update_phase(TransactionPhase.VERIFY, verification_result)
            
            # Check if order is fully executed
            if verification_result.get("is_complete", False):
                # We need to perform reconciliation
                if self.reconcile_func:
                    try:
                        await self._reconcile_transaction(transaction)
                    except Exception as reconcile_error:
                        logger.warning(f"Reconciliation failed, but transaction is complete: {str(reconcile_error)}")
                
                # Mark as complete
                transaction.update_phase(TransactionPhase.COMPLETE)
            
            return verification_result
            
        except Exception as e:
            # Don't mark as failed here, let the verification loop handle retries
            if not isinstance(e, VerificationError) or transaction.verification_attempts >= transaction.max_verification_attempts:
                transaction.add_error(TransactionPhase.VERIFY, e)
                transaction.update_phase(TransactionPhase.FAILED)
            
            raise VerificationError(f"Transaction verification failed: {str(e)}")
    
    async def _reconcile_transaction(self, transaction: Transaction) -> Dict[str, Any]:
        """
        Reconcile a transaction.
        
        Args:
            transaction: Transaction to reconcile
            
        Returns:
            Reconciliation result
            
        Raises:
            ReconciliationError: If reconciliation fails
        """
        logger.info(f"Reconciling transaction {transaction.transaction_id}")
        
        # Skip if no reconciliation function
        if not self.reconcile_func:
            return {
                "success": True,
                "message": "No reconciliation function registered"
            }
        
        # Skip if already reconciled
        if transaction.is_reconciled:
            return {
                "success": True,
                "message": "Transaction already reconciled"
            }
        
        try:
            # Update phase
            transaction.update_phase(TransactionPhase.RECONCILE)
            
            # Create transaction data with the order_id for reconciliation
            transaction_data = {"order_id": transaction.exchange_order_id}
            
            # Execute reconciliation
            reconciliation_result = await self.reconcile_func(
                transaction.order,
                transaction_data
            )
            
            if not reconciliation_result.get("is_reconciled", False):
                transaction.add_error(
                    TransactionPhase.RECONCILE,
                    ReconciliationError(f"Reconciliation failed: {reconciliation_result.get('error', 'Unknown error')}")
                )
                
                # Don't fail the transaction, just log the error
                logger.warning(
                    f"Reconciliation failed for transaction {transaction.transaction_id}: "
                    f"{reconciliation_result.get('error', 'Unknown error')}"
                )
            else:
                # Update transaction
                transaction.update_phase(TransactionPhase.RECONCILE, reconciliation_result)
            
            return reconciliation_result
            
        except Exception as e:
            transaction.add_error(TransactionPhase.RECONCILE, e)
            
            # Don't fail the transaction, just log the error
            logger.warning(f"Reconciliation failed for transaction {transaction.transaction_id}: {str(e)}")
            
            raise ReconciliationError(f"Transaction reconciliation failed: {str(e)}")
    
    async def _cancel_transaction(self, transaction: Transaction) -> Dict[str, Any]:
        """
        Cancel a transaction.
        
        Args:
            transaction: Transaction to cancel
            
        Returns:
            Cancellation result
            
        Raises:
            TransactionError: If cancellation fails
        """
        logger.info(f"Cancelling transaction {transaction.transaction_id}")
        
        # Skip if no cancel function
        if not self.cancel_func:
            return {
                "success": False,
                "message": "No cancel function registered"
            }
        
        # Check if we have an order ID
        if not transaction.exchange_order_id:
            return {
                "success": False,
                "message": "No exchange order ID available for cancellation"
            }
        
        try:
            # Update phase
            transaction.update_phase(TransactionPhase.ROLLBACK)
            
            # Create transaction data with the order_id for cancellation
            transaction_data = {"order_id": transaction.exchange_order_id}
            
            # Execute cancellation
            cancellation_result = await self.cancel_func(
                transaction.order,
                transaction_data
            )
            
            if not cancellation_result.get("success", False):
                transaction.add_error(
                    TransactionPhase.ROLLBACK,
                    TransactionError(f"Cancellation failed: {cancellation_result.get('error', 'Unknown error')}")
                )
                
                return cancellation_result
            
            # Update transaction
            transaction.update_phase(TransactionPhase.ROLLBACK, cancellation_result)
            transaction.status = TransactionStatus.ROLLED_BACK
            
            return cancellation_result
            
        except Exception as e:
            transaction.add_error(
                TransactionPhase.ROLLBACK,
                TransactionError(f"Cancellation failed: {str(e)}")
            )
            
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _verification_loop(self):
        """Background task for verifying pending transactions."""
        while True:
            try:
                # Find transactions that need verification
                async with self.transaction_lock:
                    pending_verifications = [
                        transaction for transaction in self.transactions.values()
                        if transaction.current_phase == TransactionPhase.COMMIT or 
                           (transaction.current_phase == TransactionPhase.VERIFY and 
                            transaction.status != TransactionStatus.COMPLETED)
                    ]
                
                # Process each pending verification
                for transaction in pending_verifications:
                    try:
                        await self._verify_transaction(transaction)
                    except Exception as e:
                        logger.error(f"Error in verification loop for transaction {transaction.transaction_id}: {str(e)}")
                
                # Save transactions periodically
                await self._save_transactions()
                
                # Sleep before next iteration
                await asyncio.sleep(self.verification_interval)
                
            except asyncio.CancelledError:
                logger.info("Verification loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in verification loop: {str(e)}", exc_info=True)
                await asyncio.sleep(5)  # Wait a bit longer on error
    
    async def _reconciliation_loop(self):
        """Background task for reconciling completed transactions."""
        while True:
            try:
                # Sleep first to allow some time after startup
                await asyncio.sleep(self.reconciliation_interval)
                
                if not self.reconcile_func:
                    # No reconciliation function, skip
                    continue
                
                # Find transactions that need reconciliation
                async with self.transaction_lock:
                    pending_reconciliations = [
                        transaction for transaction in self.transactions.values()
                        if transaction.status == TransactionStatus.COMPLETED and not transaction.is_reconciled
                    ]
                
                # Process each pending reconciliation
                for transaction in pending_reconciliations:
                    try:
                        await self._reconcile_transaction(transaction)
                    except Exception as e:
                        logger.error(f"Error in reconciliation loop for transaction {transaction.transaction_id}: {str(e)}")
                
                # Save transactions
                await self._save_transactions()
                
            except asyncio.CancelledError:
                logger.info("Reconciliation loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in reconciliation loop: {str(e)}", exc_info=True)
                await asyncio.sleep(60)  # Wait a bit longer on error
    
    async def _save_transactions(self):
        """Save transactions to disk."""
        try:
            async with self.transaction_lock:
                # Filter to completed, failed, or rolled back transactions
                transactions_to_save = {
                    tid: transaction for tid, transaction in self.transactions.items()
                    if transaction.status in [
                        TransactionStatus.COMPLETED,
                        TransactionStatus.FAILED,
                        TransactionStatus.ROLLED_BACK
                    ]
                }
                
                if not transactions_to_save:
                    return
                
                # Convert to dictionaries
                transaction_dicts = {
                    tid: transaction.to_dict()
                    for tid, transaction in transactions_to_save.items()
                }
                
                # Save to file
                filename = os.path.join(
                    self.storage_path,
                    f"transactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                )
                
                with open(filename, "w") as f:
                    json.dump(transaction_dicts, f, indent=2)
                
                logger.debug(f"Saved {len(transaction_dicts)} transactions to {filename}")
                
        except Exception as e:
            logger.error(f"Error saving transactions: {str(e)}")
    
    async def _load_saved_transactions(self):
        """Load saved transactions from disk."""
        try:
            if not os.path.exists(self.storage_path):
                return
            
            # Find the most recent transaction file
            transaction_files = [
                f for f in os.listdir(self.storage_path)
                if f.startswith("transactions_") and f.endswith(".json")
            ]
            
            if not transaction_files:
                return
            
            # Sort by name (which includes timestamp)
            transaction_files.sort(reverse=True)
            
            # Load the most recent file
            latest_file = os.path.join(self.storage_path, transaction_files[0])
            
            with open(latest_file, "r") as f:
                transaction_dicts = json.load(f)
            
            logger.info(f"Loaded {len(transaction_dicts)} transactions from {latest_file}")
            
            # We don't actually restore them as active transactions,
            # just log for diagnostic purposes
            
        except Exception as e:
            logger.error(f"Error loading transactions: {str(e)}")

def get_transaction_verification_pipeline(config: Dict[str, Any] = None) -> TransactionVerificationPipeline:
    """
    Create or get a transaction verification pipeline.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        TransactionVerificationPipeline instance
    """
    pipeline = TransactionVerificationPipeline(config)
    return pipeline 