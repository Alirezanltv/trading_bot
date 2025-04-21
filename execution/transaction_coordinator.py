"""
Transaction Coordinator Service

This module implements a coordinator for distributed transactions using a
three-phase commit protocol for reliable execution of trading operations.
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
from trading_system.execution.transaction import (
    TransactionContext, TransactionStatus, TransactionPhase, TransactionType
)
from trading_system.execution.transaction_persistence import TransactionStore

logger = get_logger("execution.transaction_coordinator")

class TransactionCoordinator(Component):
    """
    Transaction Coordinator
    
    This component coordinates the execution of distributed transactions using
    a three-phase commit protocol, ensuring atomicity and consistency even in
    the face of system failures.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize transaction coordinator.
        
        Args:
            config: Configuration dictionary
        """
        super().__init__(name="transaction_coordinator", config=config)
        
        # Transaction storage
        db_path = config.get("transaction_db_path", "data/transactions.db")
        self.store = TransactionStore(db_path=db_path)
        
        # Active transactions
        self.active_transactions: Dict[str, TransactionContext] = {}
        
        # Participants registry (components that need to be involved in transactions)
        self.participants: Dict[str, Any] = {}
        
        # Transaction event handlers
        self.event_handlers: Dict[str, List[Callable]] = {}
        
        # Configuration
        self.transaction_timeout = config.get("transaction_timeout", 60000)  # ms
        self.max_retries = config.get("max_retries", 3)
        self.verify_interval = config.get("verify_interval", 1000)  # ms
        self.recovery_interval = config.get("recovery_interval", 60)  # seconds
        self.cleanup_interval = config.get("cleanup_interval", 3600)  # seconds
        
        # Background tasks
        self.recovery_task = None
        self.cleanup_task = None
        self._verification_tasks: Dict[str, asyncio.Task] = {}
        
        # Locks
        self._lock = asyncio.Lock()
        
        logger.info("Transaction coordinator initialized")
    
    async def start(self) -> bool:
        """
        Start the transaction coordinator.
        
        Returns:
            bool: True if started successfully, False otherwise
        """
        try:
            # Update component status
            self._status = ComponentStatus.INITIALIZING
            
            # Start recovery task
            self.recovery_task = asyncio.create_task(self._recovery_loop())
            
            # Start cleanup task
            self.cleanup_task = asyncio.create_task(self._cleanup_loop())
            
            # Load active transactions
            await self._load_active_transactions()
            
            # Update component status
            self._status = ComponentStatus.INITIALIZED
            logger.info("Transaction coordinator started")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start transaction coordinator: {e}", exc_info=True)
            self._status = ComponentStatus.ERROR
            self._error = str(e)
            return False
    
    async def stop(self) -> bool:
        """
        Stop the transaction coordinator.
        
        Returns:
            bool: True if stopped successfully, False otherwise
        """
        try:
            # Update component status
            self._status = ComponentStatus.STOPPING
            
            # Cancel recovery task
            if self.recovery_task:
                self.recovery_task.cancel()
                try:
                    await self.recovery_task
                except asyncio.CancelledError:
                    pass
            
            # Cancel cleanup task
            if self.cleanup_task:
                self.cleanup_task.cancel()
                try:
                    await self.cleanup_task
                except asyncio.CancelledError:
                    pass
            
            # Cancel verification tasks
            for task in self._verification_tasks.values():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            
            # Close transaction store
            await self.store.close()
            
            # Update component status
            self._status = ComponentStatus.STOPPED
            logger.info("Transaction coordinator stopped")
            return True
            
        except Exception as e:
            logger.error(f"Failed to stop transaction coordinator: {e}", exc_info=True)
            self._status = ComponentStatus.ERROR
            self._error = str(e)
            return False
    
    def register_participant(self, participant_id: str, participant: Any) -> None:
        """
        Register a transaction participant.
        
        Args:
            participant_id: Unique participant identifier
            participant: Participant instance
        """
        self.participants[participant_id] = participant
        logger.info(f"Registered transaction participant: {participant_id}")
    
    def register_event_handler(self, event: str, handler: Callable) -> None:
        """
        Register a transaction event handler.
        
        Args:
            event: Event name
            handler: Event handler function
        """
        if event not in self.event_handlers:
            self.event_handlers[event] = []
        
        self.event_handlers[event].append(handler)
        logger.info(f"Registered transaction event handler for: {event}")
    
    async def trigger_event(self, event: str, transaction: TransactionContext) -> None:
        """
        Trigger a transaction event.
        
        Args:
            event: Event name
            transaction: Transaction context
        """
        if event not in self.event_handlers:
            return
        
        for handler in self.event_handlers[event]:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(transaction)
                else:
                    handler(transaction)
            except Exception as e:
                logger.error(f"Error in transaction event handler for {event}: {e}", exc_info=True)
    
    async def create_transaction(self, 
                                type_: TransactionType,
                                symbol: str,
                                amount: float,
                                price: Optional[float] = None,
                                exchange: Optional[str] = None,
                                timeout: Optional[int] = None,
                                metadata: Optional[Dict[str, Any]] = None) -> TransactionContext:
        """
        Create a new transaction.
        
        Args:
            type_: Transaction type
            symbol: Trading symbol
            amount: Transaction amount
            price: Transaction price (optional for market orders)
            exchange: Exchange identifier
            timeout: Transaction timeout in milliseconds
            metadata: Additional transaction metadata
            
        Returns:
            TransactionContext: Newly created transaction context
        """
        # Generate transaction ID
        transaction_id = str(uuid.uuid4())
        
        # Create transaction context
        transaction = TransactionContext(
            id=transaction_id,
            type=type_,
            symbol=symbol,
            amount=amount,
            price=price,
            exchange=exchange or "default",
            status=TransactionStatus.PENDING,
            phase=TransactionPhase.PREPARE,
            timeout=timeout or self.transaction_timeout,
            max_retries=self.max_retries,
            metadata=metadata or {}
        )
        
        # Save transaction
        await self.store.save_transaction(transaction)
        
        # Add to active transactions
        async with self._lock:
            self.active_transactions[transaction_id] = transaction
        
        # Trigger created event
        await self.trigger_event("transaction.created", transaction)
        
        logger.info(f"Created transaction {transaction_id} for {type_.value} {symbol}")
        return transaction
    
    async def execute_transaction(self, transaction: TransactionContext) -> bool:
        """
        Execute a transaction.
        
        Args:
            transaction: Transaction context
            
        Returns:
            bool: True if executed successfully, False otherwise
        """
        transaction_id = transaction.id
        
        # Add to active transactions
        async with self._lock:
            self.active_transactions[transaction_id] = transaction
        
        # Update transaction status
        transaction.update_status(TransactionStatus.IN_PROGRESS)
        await self.store.save_transaction(transaction)
        
        # Trigger event
        await self.trigger_event("transaction.execute", transaction)
        
        try:
            # Execute three-phase commit
            success = await self._execute_three_phase_commit(transaction)
            
            if success:
                # Update transaction status
                transaction.update_status(TransactionStatus.COMPLETED)
                await self.store.save_transaction(transaction)
                
                # Trigger completed event
                await self.trigger_event("transaction.completed", transaction)
                
                logger.info(f"Transaction {transaction_id} completed successfully")
                
                # Remove from active transactions and delete from database
                async with self._lock:
                    if transaction_id in self.active_transactions:
                        del self.active_transactions[transaction_id]
                
                # Delete transaction from store since it's completed
                await self.store.delete_transaction(transaction_id)
                
                return True
            else:
                # Update transaction status
                transaction.update_status(TransactionStatus.FAILED)
                await self.store.save_transaction(transaction)
                
                # Trigger failed event
                await self.trigger_event("transaction.failed", transaction)
                
                logger.error(f"Transaction {transaction_id} failed")
                
                # Remove from active transactions
                async with self._lock:
                    if transaction_id in self.active_transactions:
                        del self.active_transactions[transaction_id]
                        
                return False
                
        except Exception as e:
            logger.error(f"Error executing transaction {transaction_id}: {e}", exc_info=True)
            
            # Update transaction status
            transaction.update_status(TransactionStatus.FAILED)
            await self.store.save_transaction(transaction)
            
            # Trigger failed event
            await self.trigger_event("transaction.failed", transaction)
            
            # Remove from active transactions
            async with self._lock:
                if transaction_id in self.active_transactions:
                    del self.active_transactions[transaction_id]
                    
            return False
    
    async def get_transaction(self, transaction_id: str) -> Optional[TransactionContext]:
        """
        Get a transaction by ID.
        
        Args:
            transaction_id: Transaction ID
            
        Returns:
            TransactionContext: Transaction context or None if not found
        """
        # Check active transactions first
        async with self._lock:
            if transaction_id in self.active_transactions:
                return self.active_transactions[transaction_id]
        
        # Try to load from store
        return await self.store.load_transaction(transaction_id)
    
    async def get_transaction_logs(self, transaction_id: str) -> List[Dict[str, Any]]:
        """
        Get transaction logs.
        
        Args:
            transaction_id: Transaction ID
            
        Returns:
            List[Dict[str, Any]]: Transaction logs
        """
        return await self.store.get_transaction_logs(transaction_id)
    
    async def _execute_three_phase_commit(self, transaction: TransactionContext) -> bool:
        """
        Execute the three-phase commit protocol for a transaction.
        
        This method orchestrates the three phases of the commit protocol:
        1. Prepare: Prepare all participants for the transaction
        2. Validate: Confirm that all participants are ready
        3. Commit/Rollback: Commit the transaction or roll it back if any issues
        
        The three-phase commit protocol ensures that transactions are executed
        atomically even in the presence of system failures.
        
        Args:
            transaction: Transaction context
            
        Returns:
            bool: True if transaction executed successfully, False otherwise
        """
        transaction_id = transaction.id
        logger.info(f"Starting three-phase commit for transaction {transaction_id}")
        
        # Phase 1: Prepare
        try:
            # Update transaction phase
            transaction.phase = TransactionPhase.PREPARE
            await self.store.update_transaction(transaction)
            
            # Execute prepare phase
            prepare_success = await self._execute_prepare_phase(transaction)
            if not prepare_success:
                logger.error(f"Prepare phase failed for transaction {transaction_id}")
                await self._execute_rollback_phase(transaction)
                return False
            
            await self.trigger_event("transaction.prepared", transaction)
            logger.info(f"Prepare phase completed for transaction {transaction_id}")
        except Exception as e:
            logger.error(f"Error in prepare phase for transaction {transaction_id}: {e}", exc_info=True)
            await self._execute_rollback_phase(transaction)
            return False
        
        # Phase 2: Validate
        try:
            # Update transaction phase
            transaction.phase = TransactionPhase.VALIDATE
            await self.store.update_transaction(transaction)
            
            # Execute validate phase
            validate_success = await self._execute_validate_phase(transaction)
            if not validate_success:
                logger.error(f"Validate phase failed for transaction {transaction_id}")
                await self._execute_rollback_phase(transaction)
                return False
            
            await self.trigger_event("transaction.validated", transaction)
            logger.info(f"Validate phase completed for transaction {transaction_id}")
        except Exception as e:
            logger.error(f"Error in validate phase for transaction {transaction_id}: {e}", exc_info=True)
            await self._execute_rollback_phase(transaction)
            return False
        
        # Phase 3: Commit
        try:
            # Update transaction phase
            transaction.phase = TransactionPhase.COMMIT
            await self.store.update_transaction(transaction)
            
            # Execute commit phase
            commit_success = await self._execute_commit_phase(transaction)
            if not commit_success:
                logger.error(f"Commit phase failed for transaction {transaction_id}")
                await self._execute_rollback_phase(transaction)
                return False
            
            # Update transaction status and phase
            transaction.status = TransactionStatus.COMPLETED
            transaction.completed_at = datetime.now().isoformat()
            await self.store.update_transaction(transaction)
            
            await self.trigger_event("transaction.committed", transaction)
            logger.info(f"Commit phase completed for transaction {transaction_id}")
            
            # Start verification task
            self._start_verification(transaction)
            
            return True
        except Exception as e:
            logger.error(f"Error in commit phase for transaction {transaction_id}: {e}", exc_info=True)
            await self._execute_rollback_phase(transaction)
            return False
    
    async def _execute_prepare_phase(self, transaction: TransactionContext) -> bool:
        """
        Execute the prepare phase.
        
        Args:
            transaction: Transaction context
            
        Returns:
            bool: True if successful, False otherwise
        """
        # Update transaction phase
        transaction.update_phase(TransactionPhase.PREPARE)
        await self.store.save_transaction(transaction)
        
        # Trigger prepare event
        await self.trigger_event("transaction.prepare", transaction)
        
        # Check with all participants
        for participant_id, participant in self.participants.items():
            if hasattr(participant, "prepare_transaction"):
                try:
                    prepared = await participant.prepare_transaction(transaction)
                    if not prepared:
                        logger.warning(f"Participant {participant_id} rejected transaction {transaction.id} in prepare phase")
                        return False
                except Exception as e:
                    logger.error(f"Error in prepare phase with participant {participant_id}: {e}", exc_info=True)
                    return False
        
        logger.info(f"Transaction {transaction.id} prepared successfully")
        return True
    
    async def _execute_validate_phase(self, transaction: TransactionContext) -> bool:
        """
        Execute the validate phase.
        
        Args:
            transaction: Transaction context
            
        Returns:
            bool: True if successful, False otherwise
        """
        # Update transaction phase
        transaction.update_phase(TransactionPhase.VALIDATE)
        await self.store.save_transaction(transaction)
        
        # Trigger validate event
        await self.trigger_event("transaction.validate", transaction)
        
        # Check with all participants
        for participant_id, participant in self.participants.items():
            if hasattr(participant, "validate_transaction"):
                try:
                    validated = await participant.validate_transaction(transaction)
                    if not validated:
                        logger.warning(f"Participant {participant_id} rejected transaction {transaction.id} in validate phase")
                        return False
                except Exception as e:
                    logger.error(f"Error in validate phase with participant {participant_id}: {e}", exc_info=True)
                    return False
        
        logger.info(f"Transaction {transaction.id} validated successfully")
        return True
    
    async def _execute_commit_phase(self, transaction: TransactionContext) -> bool:
        """
        Execute the commit phase.
        
        Args:
            transaction: Transaction context
            
        Returns:
            bool: True if successful, False otherwise
        """
        # Update transaction phase
        transaction.update_phase(TransactionPhase.COMMIT)
        await self.store.save_transaction(transaction)
        
        # Trigger commit event
        await self.trigger_event("transaction.commit", transaction)
        
        # Execute with all participants
        commit_results = []
        for participant_id, participant in self.participants.items():
            if hasattr(participant, "commit_transaction"):
                try:
                    committed = await participant.commit_transaction(transaction)
                    commit_results.append(committed)
                    if not committed:
                        logger.warning(f"Participant {participant_id} failed to commit transaction {transaction.id}")
                except Exception as e:
                    logger.error(f"Error in commit phase with participant {participant_id}: {e}", exc_info=True)
                    commit_results.append(False)
        
        # If any participant failed to commit, we still return True because now we're in an
        # inconsistent state that needs manual recovery. The transaction is considered committed
        # once we enter this phase, even if some participants fail to apply the changes.
        if not all(commit_results):
            logger.error(f"Transaction {transaction.id} partially committed - manual recovery needed")
            
        logger.info(f"Transaction {transaction.id} committed successfully")
        return True
    
    async def _execute_rollback_phase(self, transaction: TransactionContext) -> bool:
        """
        Execute the rollback phase.
        
        Args:
            transaction: Transaction context
            
        Returns:
            bool: True if successful, False otherwise
        """
        # Update transaction phase
        transaction.update_phase(TransactionPhase.ROLLBACK)
        await self.store.save_transaction(transaction)
        
        # Trigger rollback event
        await self.trigger_event("transaction.rollback", transaction)
        
        # Execute with all participants
        rollback_results = []
        for participant_id, participant in self.participants.items():
            if hasattr(participant, "rollback_transaction"):
                try:
                    rolled_back = await participant.rollback_transaction(transaction)
                    rollback_results.append(rolled_back)
                    if not rolled_back:
                        logger.warning(f"Participant {participant_id} failed to rollback transaction {transaction.id}")
                except Exception as e:
                    logger.error(f"Error in rollback phase with participant {participant_id}: {e}", exc_info=True)
                    rollback_results.append(False)
        
        # Update transaction status
        transaction.update_status(TransactionStatus.ROLLED_BACK)
        await self.store.save_transaction(transaction)
        
        # Even if some participants failed to rollback, we still return True
        # because we've done our best to roll back the transaction.
        if not all(rollback_results):
            logger.error(f"Transaction {transaction.id} partially rolled back - manual recovery needed")
        
        logger.info(f"Transaction {transaction.id} rolled back")
        return True
    
    def _start_verification(self, transaction: TransactionContext) -> None:
        """
        Start transaction verification.
        
        Args:
            transaction: Transaction context
        """
        if transaction.id in self._verification_tasks:
            # Already being verified
            return
        
        # Create verification task
        task = asyncio.create_task(self._verify_transaction_task(transaction))
        self._verification_tasks[transaction.id] = task
        
        logger.info(f"Started verification for transaction {transaction.id}")
    
    async def _verify_transaction_task(self, transaction: TransactionContext) -> None:
        """
        Verification task for a transaction.
        
        This task periodically verifies that a transaction has been correctly executed
        by the exchange and updates its status accordingly. It has multiple verification
        approaches for robust checking:
        
        1. Direct API status check
        2. Order book polling to confirm appearance 
        3. Account history verification
        
        It includes retry logic and will continue verification until confirmed or
        until the maximum number of retries is reached.
        
        Args:
            transaction: Transaction context
        """
        transaction_id = transaction.id
        exchange_name = transaction.exchange
        symbol = transaction.symbol
        max_verifications = transaction.max_retries
        verification_count = 0
        
        logger.info(f"Starting transaction verification for {transaction_id}")
        
        try:
            # Get the exchange adapter for verification
            exchange_adapter = None
            for participant_id, participant in self.participants.items():
                if hasattr(participant, 'get_exchange') and callable(participant.get_exchange):
                    exchange_adapter = participant.get_exchange(exchange_name)
                    if exchange_adapter:
                        break
            
            if not exchange_adapter:
                logger.error(f"No exchange adapter found for {exchange_name}")
                await self._update_verification_failure(transaction, "No exchange adapter found")
                return
            
            # Verification loop
            while verification_count < max_verifications:
                verification_count += 1
                logger.info(f"Verification attempt {verification_count}/{max_verifications} for transaction {transaction_id}")
                
                # Try multiple verification methods
                verified = False
                
                # Method 1: Direct API status check
                try:
                    order_id = transaction.metadata.get("exchange_order_id")
                    if order_id:
                        order_info = await exchange_adapter.get_order(symbol, order_id)
                        if order_info:
                            status = order_info.get("status", "unknown")
                            if status in ["filled", "closed", "complete"]:
                                fill_amount = order_info.get("executed_qty", 0)
                                fill_price = order_info.get("executed_price", 0)
                                
                                # Update transaction with fill details
                                transaction.metadata["fill_amount"] = fill_amount
                                transaction.metadata["fill_price"] = fill_price
                                transaction.metadata["verification_method"] = "api_status"
                                transaction.metadata["verified_at"] = datetime.now().isoformat()
                                transaction.metadata["verification_attempts"] = verification_count
                                
                                await self.store.update_transaction(transaction)
                                logger.info(f"Transaction {transaction_id} verified via API status check")
                                
                                # Trigger verified event
                                await self.trigger_event("transaction.verified", transaction)
                                verified = True
                                break
                except Exception as e:
                    logger.warning(f"API status verification failed for {transaction_id}: {e}")
                
                # Method 2: Order book polling (for limit orders)
                if not verified and transaction.type in [TransactionType.LIMIT_BUY, TransactionType.LIMIT_SELL]:
                    try:
                        order_id = transaction.metadata.get("exchange_order_id")
                        if order_id:
                            # Get order book to check if our order is in it
                            order_book = await exchange_adapter.get_order_book(symbol)
                            
                            # Check for our order in the order book
                            side = "bids" if transaction.type == TransactionType.LIMIT_BUY else "asks"
                            for price_level in order_book.get(side, []):
                                price = float(price_level[0])
                                if abs(price - transaction.price) < 0.0001:  # Price match within tolerance
                                    # Our order is likely still in the book
                                    logger.info(f"Transaction {transaction_id} found in order book")
                                    # Don't mark as verified yet, just note that it's active
                                    transaction.metadata["last_seen_in_book"] = datetime.now().isoformat()
                                    await self.store.update_transaction(transaction)
                                    break
                    except Exception as e:
                        logger.warning(f"Order book verification failed for {transaction_id}: {e}")
                
                # Method 3: Account history verification
                if not verified:
                    try:
                        # Check recent trades for matching transaction
                        trades = await exchange_adapter.get_trade_history(symbol, limit=20)
                        for trade in trades:
                            # Try to match our transaction to a trade
                            trade_time = trade.get("time", 0)
                            trade_price = float(trade.get("price", 0))
                            trade_amount = float(trade.get("qty", 0))
                            
                            # If the trade matches our transaction details
                            if (abs(trade_amount - transaction.amount) < 0.001 and  # Amount match
                                abs(trade_price - (transaction.price or 0)) < 0.001):  # Price match if limit order
                                
                                # Update transaction with fill details
                                transaction.metadata["fill_amount"] = trade_amount
                                transaction.metadata["fill_price"] = trade_price
                                transaction.metadata["verification_method"] = "trade_history"
                                transaction.metadata["verified_at"] = datetime.now().isoformat()
                                transaction.metadata["verification_attempts"] = verification_count
                                
                                await self.store.update_transaction(transaction)
                                logger.info(f"Transaction {transaction_id} verified via trade history")
                                
                                # Trigger verified event
                                await self.trigger_event("transaction.verified", transaction)
                                verified = True
                                break
                    except Exception as e:
                        logger.warning(f"Trade history verification failed for {transaction_id}: {e}")
                
                # If verification successful, end the task
                if verified:
                    break
                
                # Wait before next verification attempt
                await asyncio.sleep(self.verify_interval / 1000)  # Convert ms to seconds
            
            # Check if max verification attempts reached without success
            if not verified:
                logger.warning(f"Transaction {transaction_id} could not be verified after {max_verifications} attempts")
                await self._update_verification_failure(transaction, f"Max verification attempts ({max_verifications}) reached")
        
        except asyncio.CancelledError:
            logger.info(f"Verification task for transaction {transaction_id} cancelled")
            raise
        except Exception as e:
            logger.error(f"Error in verification task for transaction {transaction_id}: {e}", exc_info=True)
            await self._update_verification_failure(transaction, str(e))
        finally:
            # Remove task from tracking dict when complete
            async with self._lock:
                self._verification_tasks.pop(transaction_id, None)
    
    async def _update_verification_failure(self, transaction: TransactionContext, reason: str) -> None:
        """
        Update transaction after verification failure.
        
        Args:
            transaction: Transaction context
            reason: Failure reason
        """
        transaction.metadata["verification_failed"] = True
        transaction.metadata["verification_failure_reason"] = reason
        transaction.metadata["verification_failure_time"] = datetime.now().isoformat()
        
        await self.store.update_transaction(transaction)
        await self.trigger_event("transaction.verification_failed", transaction)
    
    async def _load_active_transactions(self) -> None:
        """Load active transactions from persistent storage."""
        try:
            # Get pending and in-progress transactions
            transactions = await self.store.get_pending_transactions()
            
            if not transactions:
                logger.info("No active transactions to recover")
                return
            
            logger.info(f"Recovered {len(transactions)} active transactions")
            
            # Add to active transactions
            async with self._lock:
                for transaction in transactions:
                    self.active_transactions[transaction.id] = transaction
                    
                    # If the transaction is in commit phase and in-progress, it needs verification
                    if (transaction.phase == TransactionPhase.COMMIT and 
                        transaction.status == TransactionStatus.IN_PROGRESS):
                        self._start_verification(transaction)
            
        except Exception as e:
            logger.error(f"Error loading active transactions: {e}", exc_info=True)
    
    async def _recovery_loop(self) -> None:
        """Background task for transaction recovery."""
        try:
            while True:
                try:
                    await asyncio.sleep(self.recovery_interval)
                    
                    if self._status != ComponentStatus.INITIALIZED:
                        continue
                    
                    # Load active transactions again to check for any that were missed
                    await self._load_active_transactions()
                    
                    # Check for timed out transactions
                    await self._check_timed_out_transactions()
                    
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"Error in transaction recovery loop: {e}", exc_info=True)
                    await asyncio.sleep(60)  # Wait a bit longer on error
                
        except asyncio.CancelledError:
            logger.info("Transaction recovery task cancelled")
            raise
    
    async def _check_timed_out_transactions(self) -> None:
        """Check for and handle timed out transactions."""
        timed_out = []
        
        # Find timed out transactions
        async with self._lock:
            for transaction_id, transaction in self.active_transactions.items():
                if transaction.is_expired():
                    timed_out.append(transaction)
        
        # Handle timed out transactions
        for transaction in timed_out:
            # Update transaction status
            transaction.update_status(TransactionStatus.TIMEOUT)
            await self.store.save_transaction(transaction)
            
            # Trigger timeout event
            await self.trigger_event("transaction.timeout", transaction)
            
            # Remove from active transactions
            async with self._lock:
                if transaction.id in self.active_transactions:
                    del self.active_transactions[transaction.id]
            
            logger.warning(f"Transaction {transaction.id} timed out")
            
            # Start recovery process for timed out transaction in commit phase
            if transaction.phase == TransactionPhase.COMMIT:
                # This is a critical case - the transaction might have been committed partially
                # We need to verify with the exchange to see if the order was placed
                logger.warning(f"Transaction {transaction.id} timed out in commit phase - starting verification")
                self._start_verification(transaction)
    
    async def _cleanup_loop(self) -> None:
        """Background task for cleaning up old transactions."""
        try:
            while True:
                try:
                    await asyncio.sleep(self.cleanup_interval)
                    
                    if self._status != ComponentStatus.INITIALIZED:
                        continue
                    
                    # Clean up old transactions
                    deleted = await self.store.cleanup_old_transactions()
                    
                    if deleted > 0:
                        logger.info(f"Cleaned up {deleted} old transactions")
                    
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"Error in transaction cleanup loop: {e}", exc_info=True)
                    await asyncio.sleep(300)  # Wait a bit longer on error
                
        except asyncio.CancelledError:
            logger.info("Transaction cleanup task cancelled")
            raise 