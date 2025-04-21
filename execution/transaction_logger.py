"""
Transaction Logger

This module provides detailed transaction logging capabilities for the execution engine,
ensuring accurate audit trails and debugging information for all trades.
"""

import os
import json
import time
import logging
from enum import Enum
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Union

from trading_system.execution.orders import Order, OrderStatus, OrderSide, OrderType


class TransactionType(Enum):
    """Types of transactions to log."""
    ORDER_CREATED = "order_created"
    ORDER_UPDATED = "order_updated"
    ORDER_SUBMITTED = "order_submitted"
    ORDER_FILLED = "order_filled"
    ORDER_PARTIAL_FILL = "order_partial_fill"
    ORDER_CANCELED = "order_canceled"
    ORDER_REJECTED = "order_rejected"
    ORDER_EXPIRED = "order_expired"
    ERROR = "error"
    EXECUTION_PHASE = "execution_phase"
    RETRY = "retry"


class TransactionLogger:
    """
    Transaction logger for detailed audit trails.
    
    Features:
    - Structured JSON logging of all transaction events
    - Daily log rotation
    - Separate files for different transaction types
    - Search and analysis capabilities
    """
    
    def __init__(self, log_dir: str = "data/transactions"):
        """
        Initialize the transaction logger.
        
        Args:
            log_dir: Directory for transaction logs
        """
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories
        (self.log_dir / "orders").mkdir(exist_ok=True)
        (self.log_dir / "fills").mkdir(exist_ok=True)
        (self.log_dir / "errors").mkdir(exist_ok=True)
        
        # Logger
        self.logger = logging.getLogger("execution.transactions")
    
    def _get_log_file(self, transaction_type: TransactionType) -> Path:
        """
        Get the appropriate log file path based on transaction type.
        
        Args:
            transaction_type: Type of transaction
            
        Returns:
            Path to log file
        """
        date_str = datetime.now().strftime("%Y%m%d")
        
        if transaction_type in [TransactionType.ORDER_FILLED, TransactionType.ORDER_PARTIAL_FILL]:
            return self.log_dir / "fills" / f"fills_{date_str}.jsonl"
        elif transaction_type == TransactionType.ERROR:
            return self.log_dir / "errors" / f"errors_{date_str}.jsonl"
        else:
            return self.log_dir / "orders" / f"orders_{date_str}.jsonl"
    
    def log_transaction(self, 
                      transaction_type: TransactionType, 
                      data: Dict[str, Any], 
                      order_id: Optional[str] = None,
                      exchange_order_id: Optional[str] = None,
                      symbol: Optional[str] = None) -> None:
        """
        Log a transaction event.
        
        Args:
            transaction_type: Type of transaction
            data: Transaction data
            order_id: Order ID (if applicable)
            exchange_order_id: Exchange order ID (if applicable)
            symbol: Trading symbol (if applicable)
        """
        try:
            # Create transaction record
            transaction = {
                "timestamp": datetime.now().isoformat(),
                "unix_timestamp": int(time.time() * 1000),
                "type": transaction_type.value,
                "order_id": order_id,
                "exchange_order_id": exchange_order_id,
                "symbol": symbol,
                "data": data
            }
            
            # Get appropriate log file
            log_file = self._get_log_file(transaction_type)
            
            # Write to log file
            with open(log_file, "a") as f:
                f.write(json.dumps(transaction) + "\n")
                
        except Exception as e:
            self.logger.error(f"Error logging transaction: {str(e)}")
    
    def log_order_creation(self, order: Order) -> None:
        """
        Log order creation event.
        
        Args:
            order: The order being created
        """
        self.log_transaction(
            TransactionType.ORDER_CREATED,
            order.to_dict(),
            order_id=order.order_id,
            symbol=order.symbol
        )
    
    def log_order_update(self, order: Order, previous_status: OrderStatus) -> None:
        """
        Log order status update.
        
        Args:
            order: Updated order
            previous_status: Previous order status
        """
        data = order.to_dict()
        data["previous_status"] = previous_status.value
        
        self.log_transaction(
            TransactionType.ORDER_UPDATED,
            data,
            order_id=order.order_id,
            exchange_order_id=order.exchange_order_id,
            symbol=order.symbol
        )
    
    def log_order_submission(self, order: Order, response: Dict[str, Any]) -> None:
        """
        Log order submission to exchange.
        
        Args:
            order: Order being submitted
            response: Exchange response
        """
        self.log_transaction(
            TransactionType.ORDER_SUBMITTED,
            {
                "order": order.to_dict(),
                "exchange_response": response
            },
            order_id=order.order_id,
            exchange_order_id=order.exchange_order_id,
            symbol=order.symbol
        )
    
    def log_order_fill(self, order: Order, price: float, quantity: float, full_fill: bool = True) -> None:
        """
        Log order fill event.
        
        Args:
            order: Filled order
            price: Fill price
            quantity: Fill quantity
            full_fill: Whether this is a full or partial fill
        """
        transaction_type = TransactionType.ORDER_FILLED if full_fill else TransactionType.ORDER_PARTIAL_FILL
        
        self.log_transaction(
            transaction_type,
            {
                "order": order.to_dict(),
                "fill_price": price,
                "fill_quantity": quantity,
                "fill_value": price * quantity,
                "fill_time": datetime.now().isoformat()
            },
            order_id=order.order_id,
            exchange_order_id=order.exchange_order_id,
            symbol=order.symbol
        )
    
    def log_order_cancel(self, order: Order, reason: str) -> None:
        """
        Log order cancellation.
        
        Args:
            order: Canceled order
            reason: Cancellation reason
        """
        self.log_transaction(
            TransactionType.ORDER_CANCELED,
            {
                "order": order.to_dict(),
                "reason": reason,
                "cancel_time": datetime.now().isoformat()
            },
            order_id=order.order_id,
            exchange_order_id=order.exchange_order_id,
            symbol=order.symbol
        )
    
    def log_order_rejection(self, order: Order, reason: str) -> None:
        """
        Log order rejection.
        
        Args:
            order: Rejected order
            reason: Rejection reason
        """
        self.log_transaction(
            TransactionType.ORDER_REJECTED,
            {
                "order": order.to_dict(),
                "reason": reason,
                "rejection_time": datetime.now().isoformat()
            },
            order_id=order.order_id,
            exchange_order_id=order.exchange_order_id,
            symbol=order.symbol
        )
    
    def log_error(self, message: str, order_id: Optional[str] = None, 
                exception: Optional[Exception] = None, context: Dict[str, Any] = None) -> None:
        """
        Log error event.
        
        Args:
            message: Error message
            order_id: Associated order ID (if applicable)
            exception: Exception object (if applicable)
            context: Additional context information
        """
        data = {
            "message": message,
            "time": datetime.now().isoformat(),
        }
        
        if exception:
            data["exception_type"] = type(exception).__name__
            data["exception_message"] = str(exception)
        
        if context:
            data["context"] = context
        
        self.log_transaction(
            TransactionType.ERROR,
            data,
            order_id=order_id
        )
    
    def log_execution_phase(self, order: Order, phase: str, success: bool, 
                          details: Dict[str, Any] = None) -> None:
        """
        Log execution phase.
        
        Args:
            order: Order being executed
            phase: Execution phase name
            success: Whether the phase was successful
            details: Additional phase details
        """
        data = {
            "order_id": order.order_id,
            "phase": phase,
            "success": success,
            "time": datetime.now().isoformat()
        }
        
        if details:
            data["details"] = details
        
        self.log_transaction(
            TransactionType.EXECUTION_PHASE,
            data,
            order_id=order.order_id,
            exchange_order_id=order.exchange_order_id,
            symbol=order.symbol
        )
    
    def log_retry(self, order: Order, phase: str, retry_count: int, 
                reason: str, next_attempt_delay: float) -> None:
        """
        Log retry attempt.
        
        Args:
            order: Order being retried
            phase: Execution phase being retried
            retry_count: Current retry count
            reason: Reason for retry
            next_attempt_delay: Delay before next attempt
        """
        self.log_transaction(
            TransactionType.RETRY,
            {
                "order_id": order.order_id,
                "phase": phase,
                "retry_count": retry_count,
                "reason": reason,
                "next_attempt_delay": next_attempt_delay,
                "time": datetime.now().isoformat()
            },
            order_id=order.order_id,
            exchange_order_id=order.exchange_order_id,
            symbol=order.symbol
        )
    
    def get_order_history(self, order_id: str) -> List[Dict[str, Any]]:
        """
        Get complete history for an order.
        
        Args:
            order_id: Order ID to search for
            
        Returns:
            List of transaction records for the order
        """
        history = []
        
        try:
            # Search all log files for this order ID
            for subdir in ["orders", "fills", "errors"]:
                log_dir = self.log_dir / subdir
                
                if not log_dir.exists():
                    continue
                
                for log_file in log_dir.glob("*.jsonl"):
                    with open(log_file, "r") as f:
                        for line in f:
                            try:
                                transaction = json.loads(line)
                                if transaction.get("order_id") == order_id:
                                    history.append(transaction)
                            except json.JSONDecodeError:
                                continue
            
            # Sort by timestamp
            history.sort(key=lambda x: x.get("unix_timestamp", 0))
            
        except Exception as e:
            self.logger.error(f"Error retrieving order history: {str(e)}")
        
        return history


# Global instance for easy access
transaction_logger = TransactionLogger()


def log_order_event(order: Order, event_type: str, **kwargs) -> None:
    """
    Helper function to log order events.
    
    Args:
        order: Order object
        event_type: Event type name
        **kwargs: Additional event data
    """
    if event_type == "created":
        transaction_logger.log_order_creation(order)
    elif event_type == "updated":
        transaction_logger.log_order_update(order, kwargs.get("previous_status", OrderStatus.CREATED))
    elif event_type == "submitted":
        transaction_logger.log_order_submission(order, kwargs.get("response", {}))
    elif event_type == "filled":
        transaction_logger.log_order_fill(
            order, 
            kwargs.get("price", 0.0), 
            kwargs.get("quantity", 0.0), 
            kwargs.get("full_fill", True)
        )
    elif event_type == "canceled":
        transaction_logger.log_order_cancel(order, kwargs.get("reason", ""))
    elif event_type == "rejected":
        transaction_logger.log_order_rejection(order, kwargs.get("reason", "")) 