"""
Transaction verification pipeline with three-phase commit.

This module implements a robust transaction verification pipeline for order execution.
"""

import uuid
import time
import json
import asyncio
import logging
from enum import Enum
from typing import Dict, Any, List, Optional, Tuple, Callable, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from trading_system.core import Component, ComponentStatus, get_logger
from trading_system.core import message_bus, MessageTypes
from trading_system.exchange.base import OrderStatus, OrderType, OrderSide, Order

logger = get_logger("execution.transaction")

class TransactionPhase(Enum):
    """Transaction phases for three-phase commit."""
    PREPARE = "prepare"  # Phase 1: Prepare transaction
    VALIDATE = "validate"  # Phase 2: Validate transaction
    COMMIT = "commit"  # Phase 3: Commit transaction
    ABORT = "abort"  # Transaction aborted
    ROLLBACK = "rollback"  # Transaction rollback

class TransactionStatus(Enum):
    """Transaction status."""
    PENDING = "pending"  # Transaction is pending
    IN_PROGRESS = "in_progress"  # Transaction is in progress
    COMPLETED = "completed"  # Transaction completed successfully
    FAILED = "failed"  # Transaction failed
    ABORTED = "aborted"  # Transaction aborted
    ROLLING_BACK = "rolling_back"  # Transaction is being rolled back
    ROLLED_BACK = "rolled_back"  # Transaction rolled back successfully
    TIMEOUT = "timeout"  # Transaction timed out
    UNKNOWN = "unknown"  # Transaction status is unknown

class TransactionType(Enum):
    """Transaction types."""
    MARKET_BUY = "market_buy"  # Market buy order
    MARKET_SELL = "market_sell"  # Market sell order
    LIMIT_BUY = "limit_buy"  # Limit buy order
    LIMIT_SELL = "limit_sell"  # Limit sell order

@dataclass
class TransactionContext:
    """Transaction context."""
    id: str
    type: TransactionType
    symbol: str
    amount: float
    price: Optional[float]
    exchange: str
    status: TransactionStatus = TransactionStatus.PENDING
    phase: TransactionPhase = TransactionPhase.PREPARE
    created_at: int = field(default_factory=lambda: int(time.time() * 1000))
    updated_at: int = field(default_factory=lambda: int(time.time() * 1000))
    timeout: int = 60000  # 60 seconds in milliseconds
    retries: int = 0
    max_retries: int = 3
    order_id: Optional[str] = None
    client_order_id: Optional[str] = None
    order: Optional[Dict[str, Any]] = None
    verification_attempts: int = 0
    max_verification_attempts: int = 5
    verification_delay: int = 1000  # 1 second in milliseconds
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert transaction context to dictionary."""
        return {
            "id": self.id,
            "type": self.type.value,
            "symbol": self.symbol,
            "amount": self.amount,
            "price": self.price,
            "exchange": self.exchange,
            "status": self.status.value,
            "phase": self.phase.value,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "timeout": self.timeout,
            "retries": self.retries,
            "max_retries": self.max_retries,
            "order_id": self.order_id,
            "client_order_id": self.client_order_id,
            "order": self.order,
            "verification_attempts": self.verification_attempts,
            "max_verification_attempts": self.max_verification_attempts,
            "verification_delay": self.verification_delay,
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TransactionContext':
        """Create transaction context from dictionary."""
        return cls(
            id=data["id"],
            type=TransactionType(data["type"]),
            symbol=data["symbol"],
            amount=data["amount"],
            price=data["price"],
            exchange=data["exchange"],
            status=TransactionStatus(data["status"]),
            phase=TransactionPhase(data["phase"]),
            created_at=data["created_at"],
            updated_at=data["updated_at"],
            timeout=data["timeout"],
            retries=data["retries"],
            max_retries=data["max_retries"],
            order_id=data.get("order_id"),
            client_order_id=data.get("client_order_id"),
            order=data.get("order"),
            verification_attempts=data.get("verification_attempts", 0),
            max_verification_attempts=data.get("max_verification_attempts", 5),
            verification_delay=data.get("verification_delay", 1000),
            metadata=data.get("metadata", {})
        )
    
    def update_status(self, status: TransactionStatus) -> None:
        """
        Update transaction status.
        
        Args:
            status: New transaction status
        """
        self.status = status
        self.updated_at = int(time.time() * 1000)
    
    def update_phase(self, phase: TransactionPhase) -> None:
        """
        Update transaction phase.
        
        Args:
            phase: New transaction phase
        """
        self.phase = phase
        self.updated_at = int(time.time() * 1000)
    
    def is_expired(self) -> bool:
        """
        Check if transaction has expired.
        
        Returns:
            True if transaction has expired, False otherwise
        """
        return (int(time.time() * 1000) - self.created_at) > self.timeout
    
    def can_retry(self) -> bool:
        """
        Check if transaction can be retried.
        
        Returns:
            True if transaction can be retried, False otherwise
        """
        return self.retries < self.max_retries and not self.is_expired()
    
    def increment_retry(self) -> None:
        """Increment retry count."""
        self.retries += 1
        self.updated_at = int(time.time() * 1000)
    
    def increment_verification_attempt(self) -> None:
        """Increment verification attempt count."""
        self.verification_attempts += 1
        self.updated_at = int(time.time() * 1000)
    
    def can_verify(self) -> bool:
        """
        Check if transaction can be verified again.
        
        Returns:
            True if transaction can be verified again, False otherwise
        """
        return (self.verification_attempts < self.max_verification_attempts 
                and not self.is_expired())


class TransactionProcessor:
    """
    Transaction processor for three-phase commit.
    
    This class handles the processing of transactions through the three-phase commit protocol.
    """
    
    def __init__(self, exchange_manager: Any = None):
        """
        Initialize transaction processor.
        
        Args:
            exchange_manager: Exchange manager instance
        """
        self.exchange_manager = exchange_manager
        
        # Transaction registry
        self._transactions: Dict[str, TransactionContext] = {}
        
        # Locks for thread safety
        self._lock = asyncio.Lock()
        
        # Event listeners
        self._event_listeners: Dict[str, List[Callable]] = {}
        
        # Verification queue
        self._verification_queue: List[str] = []
        self._verification_running = False
    
    def register_event_listener(self, event: str, callback: Callable) -> None:
        """
        Register event listener.
        
        Args:
            event: Event name
            callback: Callback function
        """
        if event not in self._event_listeners:
            self._event_listeners[event] = []
        
        self._event_listeners[event].append(callback)
    
    async def trigger_event(self, event: str, context: TransactionContext) -> None:
        """
        Trigger event.
        
        Args:
            event: Event name
            context: Transaction context
        """
        if event in self._event_listeners:
            for callback in self._event_listeners[event]:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(context)
                    else:
                        callback(context)
                except Exception as e:
                    logger.error(f"Error in event listener for {event}: {str(e)}", exc_info=True)
    
    async def create_transaction(self, type: Union[TransactionType, str], symbol: str, 
                               amount: float, price: Optional[float] = None, 
                               exchange: Optional[str] = None,
                               metadata: Dict[str, Any] = None) -> TransactionContext:
        """
        Create transaction.
        
        Args:
            type: Transaction type
            symbol: Trading symbol
            amount: Order amount
            price: Order price (for limit orders)
            exchange: Exchange name
            metadata: Additional transaction metadata
            
        Returns:
            Transaction context
        """
        async with self._lock:
            # Generate transaction ID
            transaction_id = str(uuid.uuid4())
            
            # Convert string type to enum if needed
            if isinstance(type, str):
                type = TransactionType(type)
            
            # Create client order ID
            client_order_id = f"tx_{transaction_id.replace('-', '')[:16]}"
            
            # Create transaction context
            context = TransactionContext(
                id=transaction_id,
                type=type,
                symbol=symbol,
                amount=amount,
                price=price,
                exchange=exchange or "nobitex",  # Default to nobitex
                client_order_id=client_order_id,
                metadata=metadata or {}
            )
            
            # Register transaction
            self._transactions[transaction_id] = context
            
            # Trigger event
            await self.trigger_event("transaction_created", context)
            
            logger.info(f"Created transaction {transaction_id} for {symbol}")
            
            return context
    
    def get_transaction(self, transaction_id: str) -> Optional[TransactionContext]:
        """
        Get transaction context.
        
        Args:
            transaction_id: Transaction ID
            
        Returns:
            Transaction context or None if not found
        """
        return self._transactions.get(transaction_id)
    
    async def prepare_transaction(self, context: TransactionContext) -> bool:
        """
        Phase 1: Prepare transaction.
        
        Args:
            context: Transaction context
            
        Returns:
            True if transaction prepared successfully, False otherwise
        """
        try:
            logger.info(f"Preparing transaction {context.id}")
            
            # Update phase
            context.update_phase(TransactionPhase.PREPARE)
            context.update_status(TransactionStatus.IN_PROGRESS)
            
            # Validate parameters
            if context.type in [TransactionType.LIMIT_BUY, TransactionType.LIMIT_SELL] and context.price is None:
                logger.error(f"Price is required for limit orders: {context.id}")
                context.update_status(TransactionStatus.FAILED)
                await self.trigger_event("transaction_failed", context)
                return False
            
            # Get exchange
            exchange = self.exchange_manager.get_exchange(context.exchange)
            
            if not exchange:
                logger.error(f"Exchange {context.exchange} not found for transaction {context.id}")
                context.update_status(TransactionStatus.FAILED)
                await self.trigger_event("transaction_failed", context)
                return False
            
            # Trigger event
            await self.trigger_event("transaction_prepared", context)
            
            logger.info(f"Transaction {context.id} prepared successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error preparing transaction {context.id}: {str(e)}", exc_info=True)
            context.update_status(TransactionStatus.FAILED)
            await self.trigger_event("transaction_failed", context)
            return False
    
    async def validate_transaction(self, context: TransactionContext) -> bool:
        """
        Phase 2: Validate transaction.
        
        Args:
            context: Transaction context
            
        Returns:
            True if transaction validated successfully, False otherwise
        """
        try:
            logger.info(f"Validating transaction {context.id}")
            
            # Update phase
            context.update_phase(TransactionPhase.VALIDATE)
            
            # Get exchange
            exchange = self.exchange_manager.get_exchange(context.exchange)
            
            if not exchange:
                logger.error(f"Exchange {context.exchange} not found for transaction {context.id}")
                context.update_status(TransactionStatus.FAILED)
                await self.trigger_event("transaction_failed", context)
                return False
            
            # Determine order parameters
            order_type = OrderType.LIMIT if context.type in [TransactionType.LIMIT_BUY, TransactionType.LIMIT_SELL] else OrderType.MARKET
            order_side = OrderSide.BUY if context.type in [TransactionType.MARKET_BUY, TransactionType.LIMIT_BUY] else OrderSide.SELL
            
            try:
                # Fetch account information to validate balance
                balances = await exchange.fetch_balance()
                
                # Get required assets
                base_asset, quote_asset = context.symbol.split("/")
                
                # Check balance
                if order_side == OrderSide.BUY:
                    # For buy orders, check quote asset balance
                    required_balance = context.amount * (context.price or 0)
                    available_balance = balances.get(quote_asset, {}).get("available", 0)
                    
                    if available_balance < required_balance:
                        logger.error(f"Insufficient {quote_asset} balance for transaction {context.id}: {available_balance} < {required_balance}")
                        context.update_status(TransactionStatus.FAILED)
                        await self.trigger_event("transaction_failed", context)
                        return False
                else:
                    # For sell orders, check base asset balance
                    required_balance = context.amount
                    available_balance = balances.get(base_asset, {}).get("available", 0)
                    
                    if available_balance < required_balance:
                        logger.error(f"Insufficient {base_asset} balance for transaction {context.id}: {available_balance} < {required_balance}")
                        context.update_status(TransactionStatus.FAILED)
                        await self.trigger_event("transaction_failed", context)
                        return False
            
            except Exception as e:
                logger.error(f"Error validating balance for transaction {context.id}: {str(e)}", exc_info=True)
                # Continue with validation even if balance check fails
            
            # TODO: Add additional validation as needed
            
            # Trigger event
            await self.trigger_event("transaction_validated", context)
            
            logger.info(f"Transaction {context.id} validated successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error validating transaction {context.id}: {str(e)}", exc_info=True)
            context.update_status(TransactionStatus.FAILED)
            await self.trigger_event("transaction_failed", context)
            return False
    
    async def commit_transaction(self, context: TransactionContext) -> bool:
        """
        Phase 3: Commit transaction.
        
        Args:
            context: Transaction context
            
        Returns:
            True if transaction committed successfully, False otherwise
        """
        try:
            logger.info(f"Committing transaction {context.id}")
            
            # Update phase
            context.update_phase(TransactionPhase.COMMIT)
            
            # Get exchange
            exchange = self.exchange_manager.get_exchange(context.exchange)
            
            if not exchange:
                logger.error(f"Exchange {context.exchange} not found for transaction {context.id}")
                context.update_status(TransactionStatus.FAILED)
                await self.trigger_event("transaction_failed", context)
                return False
            
            # Determine order parameters
            order_type = OrderType.LIMIT if context.type in [TransactionType.LIMIT_BUY, TransactionType.LIMIT_SELL] else OrderType.MARKET
            order_side = OrderSide.BUY if context.type in [TransactionType.MARKET_BUY, TransactionType.LIMIT_BUY] else OrderSide.SELL
            
            # Prepare order parameters
            params = {
                "client_order_id": context.client_order_id
            }
            
            # Create order
            try:
                order = await exchange.create_order(
                    symbol=context.symbol,
                    order_type=order_type,
                    side=order_side,
                    amount=context.amount,
                    price=context.price if order_type == OrderType.LIMIT else None,
                    params=params
                )
                
                # Update context with order information
                context.order_id = order.id
                context.order = order.to_dict()
                
            except Exception as e:
                logger.error(f"Error creating order for transaction {context.id}: {str(e)}", exc_info=True)
                
                if context.can_retry():
                    logger.info(f"Retrying transaction {context.id} ({context.retries + 1}/{context.max_retries})")
                    context.increment_retry()
                    return await self.commit_transaction(context)
                else:
                    context.update_status(TransactionStatus.FAILED)
                    await self.trigger_event("transaction_failed", context)
                    return False
            
            # Add to verification queue
            await self.queue_for_verification(context)
            
            # Trigger event
            await self.trigger_event("transaction_committed", context)
            
            logger.info(f"Transaction {context.id} committed successfully, order ID: {context.order_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error committing transaction {context.id}: {str(e)}", exc_info=True)
            
            if context.can_retry():
                logger.info(f"Retrying transaction {context.id} ({context.retries + 1}/{context.max_retries})")
                context.increment_retry()
                return await self.commit_transaction(context)
            else:
                context.update_status(TransactionStatus.FAILED)
                await self.trigger_event("transaction_failed", context)
                return False
    
    async def abort_transaction(self, context: TransactionContext, reason: str = "") -> bool:
        """
        Abort transaction.
        
        Args:
            context: Transaction context
            reason: Abort reason
            
        Returns:
            True if transaction aborted successfully, False otherwise
        """
        try:
            logger.info(f"Aborting transaction {context.id}: {reason}")
            
            # Update phase and status
            context.update_phase(TransactionPhase.ABORT)
            context.update_status(TransactionStatus.ABORTED)
            
            # Add abort reason to metadata
            if reason:
                context.metadata["abort_reason"] = reason
            
            # Trigger event
            await self.trigger_event("transaction_aborted", context)
            
            logger.info(f"Transaction {context.id} aborted successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error aborting transaction {context.id}: {str(e)}", exc_info=True)
            return False
    
    async def rollback_transaction(self, context: TransactionContext) -> bool:
        """
        Rollback transaction.
        
        Args:
            context: Transaction context
            
        Returns:
            True if transaction rolled back successfully, False otherwise
        """
        try:
            logger.info(f"Rolling back transaction {context.id}")
            
            # Update phase and status
            context.update_phase(TransactionPhase.ROLLBACK)
            context.update_status(TransactionStatus.ROLLING_BACK)
            
            # Rollback depends on transaction phase
            if context.phase == TransactionPhase.COMMIT and context.order_id:
                # If order was created, cancel it
                exchange = self.exchange_manager.get_exchange(context.exchange)
                
                if exchange:
                    try:
                        logger.info(f"Canceling order {context.order_id} for transaction {context.id}")
                        await exchange.cancel_order(context.order_id, context.symbol)
                        
                        # Update metadata
                        context.metadata["rollback_cancelled_order"] = True
                        
                    except Exception as e:
                        logger.error(f"Error canceling order during rollback: {str(e)}", exc_info=True)
                        context.metadata["rollback_error"] = str(e)
            
            # Update status
            context.update_status(TransactionStatus.ROLLED_BACK)
            
            # Trigger event
            await self.trigger_event("transaction_rolled_back", context)
            
            logger.info(f"Transaction {context.id} rolled back successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error rolling back transaction {context.id}: {str(e)}", exc_info=True)
            context.metadata["rollback_error"] = str(e)
            return False
    
    async def execute_transaction(self, context: TransactionContext) -> bool:
        """
        Execute transaction through three-phase commit.
        
        Args:
            context: Transaction context
            
        Returns:
            True if transaction executed successfully, False otherwise
        """
        try:
            logger.info(f"Executing transaction {context.id}")
            
            # Phase 1: Prepare
            if not await self.prepare_transaction(context):
                logger.error(f"Failed to prepare transaction {context.id}")
                return False
            
            # Phase 2: Validate
            if not await self.validate_transaction(context):
                logger.error(f"Failed to validate transaction {context.id}")
                await self.abort_transaction(context, "Validation failed")
                return False
            
            # Phase 3: Commit
            if not await self.commit_transaction(context):
                logger.error(f"Failed to commit transaction {context.id}")
                await self.rollback_transaction(context)
                return False
            
            logger.info(f"Transaction {context.id} executed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error executing transaction {context.id}: {str(e)}", exc_info=True)
            await self.abort_transaction(context, str(e))
            return False
    
    async def queue_for_verification(self, context: TransactionContext) -> None:
        """
        Queue transaction for verification.
        
        Args:
            context: Transaction context
        """
        async with self._lock:
            if context.id not in self._verification_queue:
                self._verification_queue.append(context.id)
                
                # Start verification process if not already running
                if not self._verification_running:
                    self._verification_running = True
                    asyncio.create_task(self._process_verification_queue())
    
    async def _process_verification_queue(self) -> None:
        """Process verification queue."""
        try:
            while self._verification_queue:
                async with self._lock:
                    if not self._verification_queue:
                        break
                    
                    transaction_id = self._verification_queue[0]
                
                # Get transaction context
                context = self.get_transaction(transaction_id)
                
                if not context:
                    async with self._lock:
                        self._verification_queue.pop(0)
                    continue
                
                # Verify transaction
                verified = await self.verify_transaction(context)
                
                if verified:
                    # Transaction verified, remove from queue
                    async with self._lock:
                        self._verification_queue.pop(0)
                elif context.can_verify():
                    # Transaction not verified, but can retry
                    # Move to end of queue
                    async with self._lock:
                        self._verification_queue.pop(0)
                        self._verification_queue.append(transaction_id)
                    
                    # Wait before retrying
                    await asyncio.sleep(context.verification_delay / 1000)
                else:
                    # Transaction not verified and cannot retry
                    async with self._lock:
                        self._verification_queue.pop(0)
                    
                    # Update status
                    context.update_status(TransactionStatus.FAILED)
                    await self.trigger_event("transaction_failed", context)
                    
                    logger.error(f"Failed to verify transaction {context.id} after {context.verification_attempts} attempts")
            
            # Queue is empty
            self._verification_running = False
            
        except Exception as e:
            logger.error(f"Error processing verification queue: {str(e)}", exc_info=True)
            self._verification_running = False
    
    async def verify_transaction(self, context: TransactionContext) -> bool:
        """
        Verify transaction execution.
        
        Args:
            context: Transaction context
            
        Returns:
            True if transaction verified successfully, False otherwise
        """
        try:
            # Increment verification attempt
            context.increment_verification_attempt()
            
            logger.info(f"Verifying transaction {context.id} (attempt {context.verification_attempts}/{context.max_verification_attempts})")
            
            # Check if order ID is available
            if not context.order_id:
                logger.error(f"Order ID not available for transaction {context.id}")
                return False
            
            # Get exchange
            exchange = self.exchange_manager.get_exchange(context.exchange)
            
            if not exchange:
                logger.error(f"Exchange {context.exchange} not found for transaction {context.id}")
                return False
            
            # Fetch order
            try:
                order = await exchange.fetch_order(context.order_id, context.symbol)
                
                # Check order status
                if order.status in [OrderStatus.FILLED, OrderStatus.CLOSED]:
                    # Order completed successfully
                    context.order = order.to_dict()
                    context.update_status(TransactionStatus.COMPLETED)
                    await self.trigger_event("transaction_completed", context)
                    
                    logger.info(f"Transaction {context.id} verified successfully")
                    return True
                    
                elif order.status in [OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.EXPIRED]:
                    # Order failed
                    context.order = order.to_dict()
                    context.update_status(TransactionStatus.FAILED)
                    await self.trigger_event("transaction_failed", context)
                    
                    logger.error(f"Transaction {context.id} failed: Order status is {order.status.value}")
                    return True  # Return True to remove from queue
                    
                elif order.status in [OrderStatus.PARTIALLY_FILLED]:
                    # Order partially filled
                    context.order = order.to_dict()
                    
                    # Check if partial fill is acceptable
                    if context.metadata.get("accept_partial_fill", False):
                        context.update_status(TransactionStatus.COMPLETED)
                        await self.trigger_event("transaction_completed", context)
                        
                        logger.info(f"Transaction {context.id} verified successfully (partial fill accepted)")
                        return True
                    
                    # Continue verification for partial fills
                    return False
                
                # Order still in progress
                context.order = order.to_dict()
                return False
                
            except Exception as e:
                logger.error(f"Error fetching order for transaction {context.id}: {str(e)}", exc_info=True)
                return False
            
        except Exception as e:
            logger.error(f"Error verifying transaction {context.id}: {str(e)}", exc_info=True)
            return False
    
    def cleanup_transactions(self, max_age: int = 86400000) -> int:
        """
        Clean up old transactions.
        
        Args:
            max_age: Maximum transaction age in milliseconds (default: 24 hours)
            
        Returns:
            Number of transactions cleaned up
        """
        try:
            current_time = int(time.time() * 1000)
            transaction_ids = list(self._transactions.keys())
            count = 0
            
            for transaction_id in transaction_ids:
                context = self._transactions.get(transaction_id)
                
                if context and (current_time - context.created_at) > max_age:
                    self._transactions.pop(transaction_id, None)
                    count += 1
            
            if count > 0:
                logger.info(f"Cleaned up {count} old transactions")
            
            return count
            
        except Exception as e:
            logger.error(f"Error cleaning up transactions: {str(e)}", exc_info=True)
            return 0


# Create singleton instance
transaction_processor = None 