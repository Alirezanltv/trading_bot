"""
Transaction Verification System

This module provides components for verifying and reconciling trade executions
between the trading system and the exchange. It ensures that:
1. Orders sent to the exchange are properly executed
2. Fills are recorded correctly and match exchange records
3. Positions are accurately tracked and reconciled
4. Any discrepancies are detected and reported
"""

import asyncio
import enum
import logging
import time
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Set, Tuple, Any, Union

from trading_system.core.logging import get_logger

logger = get_logger("core.transaction_verification")


class VerificationStatus(enum.Enum):
    """Verification status for transactions."""
    PENDING = "pending"               # Verification pending
    VERIFIED = "verified"             # Transaction verified
    DISCREPANCY = "discrepancy"       # Discrepancy detected
    RECONCILED = "reconciled"         # Manually reconciled
    TIMEOUT = "timeout"               # Verification timed out
    ERROR = "error"                   # Error during verification


class VerificationType(enum.Enum):
    """Types of verification checks."""
    ORDER_SUBMISSION = "order_submission"  # Order was submitted to exchange
    ORDER_ACCEPTANCE = "order_acceptance"  # Order was accepted by exchange
    EXECUTION_PRICE = "execution_price"    # Execution price verification
    EXECUTION_QUANTITY = "execution_quantity"  # Execution quantity verification
    POSITION_BALANCE = "position_balance"  # Position balance verification
    FEE_VERIFICATION = "fee_verification"  # Fee verification
    TRADE_TIMESTAMP = "trade_timestamp"    # Trade timestamp verification


class VerificationOutcome(enum.Enum):
    """Verification outcome."""
    SUCCESS = "success"          # Verification succeeded
    FAILURE = "failure"          # Verification failed
    PARTIAL = "partial"          # Partial verification (some checks passed, some failed)
    PENDING = "pending"          # Verification still pending
    NO_DATA = "no_data"          # No data available for verification
    ERROR = "error"              # Error during verification


class TransactionVerifier:
    """
    Transaction Verifier
    
    This class verifies transactions between the trading system and exchange
    to ensure they are properly executed and recorded.
    """
    
    def __init__(self, 
                 verification_timeout_sec: int = 30,
                 max_retries: int = 3,
                 retry_delay_sec: int = 2,
                 auto_reconcile: bool = False):
        """
        Initialize Transaction Verifier.
        
        Args:
            verification_timeout_sec: Timeout for verification in seconds
            max_retries: Maximum number of verification retries
            retry_delay_sec: Delay between retries in seconds
            auto_reconcile: Whether to automatically reconcile minor discrepancies
        """
        self._verification_timeout_sec = verification_timeout_sec
        self._max_retries = max_retries
        self._retry_delay_sec = retry_delay_sec
        self._auto_reconcile = auto_reconcile
        
        # Transaction tracking
        self._transactions: Dict[str, Dict[str, Any]] = {}
        self._pending_verifications: Dict[str, Set[VerificationType]] = {}
        self._verification_results: Dict[str, Dict[VerificationType, VerificationOutcome]] = {}
        
        # Verification locks to prevent race conditions
        self._locks: Dict[str, asyncio.Lock] = {}
        
        # Statistics
        self._stats = {
            "total_transactions": 0,
            "verified_transactions": 0,
            "discrepancy_transactions": 0,
            "reconciled_transactions": 0,
            "timeout_transactions": 0,
            "error_transactions": 0
        }
        
        logger.info(f"Transaction verifier initialized with timeout={verification_timeout_sec}s, " 
                   f"max_retries={max_retries}, retry_delay={retry_delay_sec}s, "
                   f"auto_reconcile={auto_reconcile}")
    
    async def register_transaction(self, 
                                   transaction_id: str,
                                   order_id: str,
                                   transaction_type: str,
                                   symbol: str,
                                   quantity: Decimal,
                                   price: Optional[Decimal] = None,
                                   timestamp: Optional[datetime] = None,
                                   fee: Optional[Decimal] = None,
                                   expected_verifications: Optional[List[VerificationType]] = None,
                                   metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        Register a transaction for verification.
        
        Args:
            transaction_id: Unique transaction ID (or None to generate one)
            order_id: Order ID associated with the transaction
            transaction_type: Type of transaction (buy, sell, etc.)
            symbol: Trading symbol
            quantity: Transaction quantity
            price: Transaction price
            timestamp: Transaction timestamp
            fee: Transaction fee
            expected_verifications: Types of verifications to perform
            metadata: Additional metadata
            
        Returns:
            Transaction ID
        """
        # Generate transaction ID if not provided
        if not transaction_id:
            transaction_id = str(uuid.uuid4())
        
        # Create timestamp if not provided
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        # Default verifications if not specified
        if expected_verifications is None:
            expected_verifications = [
                VerificationType.ORDER_SUBMISSION,
                VerificationType.ORDER_ACCEPTANCE,
                VerificationType.EXECUTION_PRICE,
                VerificationType.EXECUTION_QUANTITY,
                VerificationType.FEE_VERIFICATION
            ]
        
        # Create transaction record
        transaction = {
            "transaction_id": transaction_id,
            "order_id": order_id,
            "transaction_type": transaction_type,
            "symbol": symbol,
            "quantity": quantity,
            "price": price,
            "timestamp": timestamp,
            "fee": fee,
            "registration_time": datetime.utcnow(),
            "expected_verifications": expected_verifications,
            "verification_status": VerificationStatus.PENDING,
            "verification_timeout": timestamp + timedelta(seconds=self._verification_timeout_sec),
            "attempts": 0,
            "metadata": metadata or {}
        }
        
        # Store transaction and initialize verification tracking
        self._transactions[transaction_id] = transaction
        self._pending_verifications[transaction_id] = set(expected_verifications)
        self._verification_results[transaction_id] = {
            vtype: VerificationOutcome.PENDING for vtype in expected_verifications
        }
        
        # Create lock for this transaction
        self._locks[transaction_id] = asyncio.Lock()
        
        # Update statistics
        self._stats["total_transactions"] += 1
        
        logger.info(f"Registered transaction {transaction_id} for verification: "
                   f"{transaction_type} {quantity} {symbol} @ {price}")
        
        # Schedule verification timeout
        asyncio.create_task(self._handle_verification_timeout(transaction_id))
        
        return transaction_id
    
    async def verify(self, 
                     transaction_id: str,
                     verification_type: VerificationType,
                     actual_data: Dict[str, Any],
                     tolerance: Optional[Dict[str, Any]] = None) -> Tuple[VerificationOutcome, Optional[Dict[str, Any]]]:
        """
        Verify a specific aspect of a transaction.
        
        Args:
            transaction_id: Transaction ID
            verification_type: Type of verification
            actual_data: Actual data from exchange or execution
            tolerance: Tolerance parameters for verification
            
        Returns:
            Tuple of (verification outcome, discrepancy details if any)
        """
        # Check if transaction exists
        if transaction_id not in self._transactions:
            logger.warning(f"Cannot verify unknown transaction: {transaction_id}")
            return VerificationOutcome.ERROR, {"error": "Unknown transaction"}
        
        # Get transaction and lock
        transaction = self._transactions[transaction_id]
        lock = self._locks.get(transaction_id, asyncio.Lock())
        
        async with lock:
            # Check if verification is expected for this transaction
            if verification_type not in transaction.get("expected_verifications", []):
                logger.warning(f"Unexpected verification type {verification_type} for transaction {transaction_id}")
                return VerificationOutcome.ERROR, {"error": "Unexpected verification type"}
            
            # Check if verification is still pending
            pending_verifications = self._pending_verifications.get(transaction_id, set())
            if verification_type not in pending_verifications:
                current_outcome = self._verification_results.get(transaction_id, {}).get(verification_type)
                logger.info(f"Verification {verification_type} for {transaction_id} already completed with outcome {current_outcome}")
                return current_outcome, None
            
            # Perform verification based on type
            try:
                outcome, details = await self._perform_verification(
                    transaction, verification_type, actual_data, tolerance
                )
                
                # Update verification results
                self._verification_results[transaction_id][verification_type] = outcome
                
                # If verification is complete, remove from pending
                if outcome != VerificationOutcome.PENDING:
                    self._pending_verifications[transaction_id].discard(verification_type)
                    
                # Update overall status if needed
                if not self._pending_verifications[transaction_id]:
                    await self._update_overall_status(transaction_id)
                
                # Log outcome
                if outcome == VerificationOutcome.SUCCESS:
                    logger.info(f"Verification {verification_type} for {transaction_id} succeeded")
                elif outcome == VerificationOutcome.FAILURE:
                    logger.warning(f"Verification {verification_type} for {transaction_id} failed: {details}")
                elif outcome == VerificationOutcome.PARTIAL:
                    logger.warning(f"Verification {verification_type} for {transaction_id} partially succeeded: {details}")
                elif outcome == VerificationOutcome.NO_DATA:
                    logger.warning(f"No data for verification {verification_type} for {transaction_id}")
                
                return outcome, details
                
            except Exception as e:
                logger.error(f"Error during verification {verification_type} for {transaction_id}: {str(e)}", exc_info=True)
                self._verification_results[transaction_id][verification_type] = VerificationOutcome.ERROR
                return VerificationOutcome.ERROR, {"error": str(e)}
    
    async def _perform_verification(self,
                                  transaction: Dict[str, Any],
                                  verification_type: VerificationType,
                                  actual_data: Dict[str, Any],
                                  tolerance: Optional[Dict[str, Any]] = None) -> Tuple[VerificationOutcome, Optional[Dict[str, Any]]]:
        """
        Perform specific verification.
        
        Args:
            transaction: Transaction data
            verification_type: Type of verification
            actual_data: Actual data from exchange or execution
            tolerance: Tolerance parameters for verification
            
        Returns:
            Tuple of (verification outcome, discrepancy details if any)
        """
        transaction_id = transaction["transaction_id"]
        
        # Set default tolerance if not provided
        if tolerance is None:
            tolerance = {}
        
        # Default price tolerance (0.1%)
        price_tolerance = tolerance.get("price_tolerance_pct", Decimal("0.1")) / 100
        
        # Default quantity tolerance (0 - exact match)
        quantity_tolerance = tolerance.get("quantity_tolerance", Decimal("0"))
        
        # Default fee tolerance (1%)
        fee_tolerance = tolerance.get("fee_tolerance_pct", Decimal("1")) / 100
        
        # Default time tolerance (5 seconds)
        time_tolerance_sec = tolerance.get("time_tolerance_sec", 5)
        
        # Perform verification based on type
        if verification_type == VerificationType.ORDER_SUBMISSION:
            # Verify order was submitted to exchange
            if "exchange_submission_id" not in actual_data:
                return VerificationOutcome.FAILURE, {"error": "No exchange submission ID"}
            
            # Store exchange submission ID in transaction
            transaction["exchange_submission_id"] = actual_data["exchange_submission_id"]
            return VerificationOutcome.SUCCESS, None
            
        elif verification_type == VerificationType.ORDER_ACCEPTANCE:
            # Verify order was accepted by exchange
            if "exchange_order_id" not in actual_data:
                return VerificationOutcome.FAILURE, {"error": "No exchange order ID"}
            
            # Store exchange order ID in transaction
            transaction["exchange_order_id"] = actual_data["exchange_order_id"]
            return VerificationOutcome.SUCCESS, None
            
        elif verification_type == VerificationType.EXECUTION_PRICE:
            # Verify execution price
            if transaction.get("price") is None:
                # Market order, no specific price expected
                return VerificationOutcome.SUCCESS, None
                
            if "execution_price" not in actual_data:
                return VerificationOutcome.NO_DATA, {"error": "No execution price data"}
            
            expected_price = transaction["price"]
            actual_price = Decimal(str(actual_data["execution_price"]))
            
            # Calculate price difference percentage
            if expected_price == 0:
                price_diff_pct = Decimal("100") if actual_price != 0 else Decimal("0")
            else:
                price_diff_pct = abs((actual_price - expected_price) / expected_price * 100)
            
            # Check if within tolerance
            if price_diff_pct <= price_tolerance * 100:
                return VerificationOutcome.SUCCESS, None
            else:
                discrepancy = {
                    "expected_price": float(expected_price),
                    "actual_price": float(actual_price),
                    "difference_pct": float(price_diff_pct),
                    "tolerance_pct": float(price_tolerance * 100)
                }
                return VerificationOutcome.FAILURE, discrepancy
                
        elif verification_type == VerificationType.EXECUTION_QUANTITY:
            # Verify execution quantity
            if "execution_quantity" not in actual_data:
                return VerificationOutcome.NO_DATA, {"error": "No execution quantity data"}
            
            expected_quantity = transaction["quantity"]
            actual_quantity = Decimal(str(actual_data["execution_quantity"]))
            
            # Check if within tolerance
            quantity_diff = abs(actual_quantity - expected_quantity)
            if quantity_diff <= quantity_tolerance:
                return VerificationOutcome.SUCCESS, None
            else:
                discrepancy = {
                    "expected_quantity": float(expected_quantity),
                    "actual_quantity": float(actual_quantity),
                    "difference": float(quantity_diff),
                    "tolerance": float(quantity_tolerance)
                }
                return VerificationOutcome.FAILURE, discrepancy
                
        elif verification_type == VerificationType.FEE_VERIFICATION:
            # Verify transaction fee
            if transaction.get("fee") is None:
                # No fee specified, skip verification
                return VerificationOutcome.SUCCESS, None
                
            if "actual_fee" not in actual_data:
                return VerificationOutcome.NO_DATA, {"error": "No fee data"}
            
            expected_fee = transaction["fee"]
            actual_fee = Decimal(str(actual_data["actual_fee"]))
            
            # Calculate fee difference percentage
            if expected_fee == 0:
                fee_diff_pct = Decimal("100") if actual_fee != 0 else Decimal("0")
            else:
                fee_diff_pct = abs((actual_fee - expected_fee) / expected_fee * 100)
            
            # Check if within tolerance
            if fee_diff_pct <= fee_tolerance * 100:
                return VerificationOutcome.SUCCESS, None
            else:
                discrepancy = {
                    "expected_fee": float(expected_fee),
                    "actual_fee": float(actual_fee),
                    "difference_pct": float(fee_diff_pct),
                    "tolerance_pct": float(fee_tolerance * 100)
                }
                return VerificationOutcome.FAILURE, discrepancy
                
        elif verification_type == VerificationType.TRADE_TIMESTAMP:
            # Verify trade timestamp
            if "execution_time" not in actual_data:
                return VerificationOutcome.NO_DATA, {"error": "No execution time data"}
            
            expected_time = transaction["timestamp"]
            actual_time = actual_data["execution_time"]
            
            # Convert string to datetime if needed
            if isinstance(actual_time, str):
                try:
                    actual_time = datetime.fromisoformat(actual_time.replace("Z", "+00:00"))
                except ValueError:
                    try:
                        actual_time = datetime.strptime(actual_time, "%Y-%m-%dT%H:%M:%S.%fZ")
                    except ValueError:
                        return VerificationOutcome.ERROR, {"error": f"Invalid timestamp format: {actual_time}"}
            
            # Calculate time difference in seconds
            time_diff_sec = abs((actual_time - expected_time).total_seconds())
            
            # Check if within tolerance
            if time_diff_sec <= time_tolerance_sec:
                return VerificationOutcome.SUCCESS, None
            else:
                discrepancy = {
                    "expected_time": expected_time.isoformat(),
                    "actual_time": actual_time.isoformat(),
                    "difference_sec": time_diff_sec,
                    "tolerance_sec": time_tolerance_sec
                }
                return VerificationOutcome.FAILURE, discrepancy
                
        elif verification_type == VerificationType.POSITION_BALANCE:
            # Verify position balance
            if "system_position" not in actual_data or "exchange_position" not in actual_data:
                return VerificationOutcome.NO_DATA, {"error": "Missing position data"}
            
            system_position = Decimal(str(actual_data["system_position"]))
            exchange_position = Decimal(str(actual_data["exchange_position"]))
            
            # Position tolerance (default 0.01% of position)
            position_tolerance_pct = tolerance.get("position_tolerance_pct", Decimal("0.01"))
            
            # Calculate absolute tolerance
            if exchange_position == 0:
                abs_tolerance = Decimal("0.001")  # Small absolute tolerance for zero positions
            else:
                abs_tolerance = abs(exchange_position) * position_tolerance_pct / 100
            
            # Check if within tolerance
            position_diff = abs(system_position - exchange_position)
            if position_diff <= abs_tolerance:
                return VerificationOutcome.SUCCESS, None
            else:
                discrepancy = {
                    "system_position": float(system_position),
                    "exchange_position": float(exchange_position),
                    "difference": float(position_diff),
                    "tolerance": float(abs_tolerance)
                }
                return VerificationOutcome.FAILURE, discrepancy
                
        else:
            logger.warning(f"Unknown verification type: {verification_type}")
            return VerificationOutcome.ERROR, {"error": f"Unknown verification type: {verification_type}"}
    
    async def _update_overall_status(self, transaction_id: str) -> None:
        """
        Update the overall verification status for a transaction.
        
        Args:
            transaction_id: Transaction ID
        """
        # Get verification results
        results = self._verification_results.get(transaction_id, {})
        
        # Count outcomes
        success_count = sum(1 for outcome in results.values() if outcome == VerificationOutcome.SUCCESS)
        failure_count = sum(1 for outcome in results.values() if outcome == VerificationOutcome.FAILURE)
        pending_count = sum(1 for outcome in results.values() if outcome == VerificationOutcome.PENDING)
        
        # Get transaction
        transaction = self._transactions.get(transaction_id)
        if not transaction:
            return
            
        # Determine overall status
        if pending_count > 0:
            # Still pending
            transaction["verification_status"] = VerificationStatus.PENDING
        elif failure_count == 0:
            # All succeeded
            transaction["verification_status"] = VerificationStatus.VERIFIED
            self._stats["verified_transactions"] += 1
            logger.info(f"Transaction {transaction_id} fully verified")
        elif success_count == 0:
            # All failed
            transaction["verification_status"] = VerificationStatus.DISCREPANCY
            self._stats["discrepancy_transactions"] += 1
            logger.warning(f"Transaction {transaction_id} has discrepancies in all verifications")
        else:
            # Some succeeded, some failed
            transaction["verification_status"] = VerificationStatus.DISCREPANCY
            self._stats["discrepancy_transactions"] += 1
            logger.warning(f"Transaction {transaction_id} has discrepancies in some verifications")
            
            # Try auto-reconciliation if enabled
            if self._auto_reconcile:
                await self._try_auto_reconcile(transaction_id)
    
    async def _try_auto_reconcile(self, transaction_id: str) -> bool:
        """
        Try to automatically reconcile discrepancies.
        
        Args:
            transaction_id: Transaction ID
            
        Returns:
            bool: True if reconciled, False otherwise
        """
        # Get transaction
        transaction = self._transactions.get(transaction_id)
        if not transaction:
            return False
            
        # Get verification results
        results = self._verification_results.get(transaction_id, {})
        
        # Check each failed verification to see if it can be auto-reconciled
        reconcilable = True
        for vtype, outcome in results.items():
            if outcome != VerificationOutcome.FAILURE:
                continue
                
            # Check if this type of discrepancy can be auto-reconciled
            # This is a placeholder - implement specific reconciliation logic here
            if vtype == VerificationType.FEE_VERIFICATION:
                # Fees might be auto-reconcilable within a wider tolerance
                logger.info(f"Auto-reconciling fee discrepancy for transaction {transaction_id}")
                # Mark as reconciled
                self._verification_results[transaction_id][vtype] = VerificationOutcome.SUCCESS
            elif vtype == VerificationType.EXECUTION_PRICE and transaction.get("transaction_type") == "market":
                # Market orders might have different execution prices
                logger.info(f"Auto-reconciling market order price discrepancy for transaction {transaction_id}")
                # Mark as reconciled
                self._verification_results[transaction_id][vtype] = VerificationOutcome.SUCCESS
            else:
                # Can't reconcile this discrepancy
                reconcilable = False
                
        # If all discrepancies were reconciled, update status
        if reconcilable:
            transaction["verification_status"] = VerificationStatus.RECONCILED
            self._stats["reconciled_transactions"] += 1
            logger.info(f"Transaction {transaction_id} auto-reconciled")
            return True
            
        return False
    
    async def _handle_verification_timeout(self, transaction_id: str) -> None:
        """
        Handle verification timeout.
        
        Args:
            transaction_id: Transaction ID
        """
        # Get transaction
        transaction = self._transactions.get(transaction_id)
        if not transaction:
            return
            
        # Sleep until timeout
        timeout_time = transaction.get("verification_timeout")
        if timeout_time:
            sleep_time = (timeout_time - datetime.utcnow()).total_seconds()
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
                
        # Check if verification is still pending
        if transaction_id not in self._pending_verifications:
            return
            
        # Get lock
        lock = self._locks.get(transaction_id, asyncio.Lock())
        
        async with lock:
            # Check again after acquiring lock
            if transaction_id not in self._pending_verifications:
                return
                
            # Get pending verifications
            pending = self._pending_verifications[transaction_id]
            
            # If no pending verifications, we're done
            if not pending:
                return
                
            # Check if we should retry
            transaction["attempts"] += 1
            if transaction["attempts"] <= self._max_retries:
                # Schedule another timeout
                transaction["verification_timeout"] = datetime.utcnow() + timedelta(seconds=self._verification_timeout_sec)
                logger.info(f"Verification timeout for transaction {transaction_id}, retrying ({transaction['attempts']}/{self._max_retries})")
                asyncio.create_task(self._handle_verification_timeout(transaction_id))
                return
                
            # Mark pending verifications as timed out
            for vtype in pending:
                self._verification_results[transaction_id][vtype] = VerificationOutcome.NO_DATA
                
            # Update overall status
            transaction["verification_status"] = VerificationStatus.TIMEOUT
            self._stats["timeout_transactions"] += 1
            
            # Clear pending verifications
            self._pending_verifications.pop(transaction_id, None)
            
            logger.warning(f"Verification timed out for transaction {transaction_id}")
    
    async def get_transaction_status(self, transaction_id: str) -> Optional[Dict[str, Any]]:
        """
        Get verification status for a transaction.
        
        Args:
            transaction_id: Transaction ID
            
        Returns:
            Dict with transaction status or None if not found
        """
        # Get transaction
        transaction = self._transactions.get(transaction_id)
        if not transaction:
            return None
            
        # Get verification results
        results = self._verification_results.get(transaction_id, {})
        
        # Create status report
        status = {
            "transaction_id": transaction_id,
            "verification_status": transaction.get("verification_status", VerificationStatus.PENDING).value,
            "verification_results": {vtype.value: outcome.value for vtype, outcome in results.items()},
            "pending_verifications": [vtype.value for vtype in self._pending_verifications.get(transaction_id, set())],
            "registration_time": transaction.get("registration_time").isoformat(),
            "transaction_details": {
                "order_id": transaction.get("order_id"),
                "transaction_type": transaction.get("transaction_type"),
                "symbol": transaction.get("symbol"),
                "quantity": float(transaction.get("quantity")) if transaction.get("quantity") is not None else None,
                "price": float(transaction.get("price")) if transaction.get("price") is not None else None,
                "timestamp": transaction.get("timestamp").isoformat() if transaction.get("timestamp") else None
            }
        }
        
        return status
    
    async def get_all_transactions(self, 
                                 status_filter: Optional[VerificationStatus] = None,
                                 symbol_filter: Optional[str] = None,
                                 time_range: Optional[Tuple[datetime, datetime]] = None) -> List[Dict[str, Any]]:
        """
        Get all transactions with optional filtering.
        
        Args:
            status_filter: Filter by verification status
            symbol_filter: Filter by symbol
            time_range: Filter by time range (start, end)
            
        Returns:
            List of transaction status reports
        """
        result = []
        
        for transaction_id, transaction in self._transactions.items():
            # Apply filters
            if status_filter and transaction.get("verification_status") != status_filter:
                continue
                
            if symbol_filter and transaction.get("symbol") != symbol_filter:
                continue
                
            if time_range:
                start_time, end_time = time_range
                transaction_time = transaction.get("timestamp")
                if not transaction_time or transaction_time < start_time or transaction_time > end_time:
                    continue
                    
            # Get status report
            status = await self.get_transaction_status(transaction_id)
            if status:
                result.append(status)
                
        return result
    
    async def reconcile_transaction(self, transaction_id: str, notes: Optional[str] = None) -> bool:
        """
        Manually reconcile a transaction with discrepancies.
        
        Args:
            transaction_id: Transaction ID
            notes: Reconciliation notes
            
        Returns:
            bool: True if reconciled, False if error
        """
        # Get transaction
        transaction = self._transactions.get(transaction_id)
        if not transaction:
            logger.warning(f"Cannot reconcile unknown transaction: {transaction_id}")
            return False
            
        # Get lock
        lock = self._locks.get(transaction_id, asyncio.Lock())
        
        async with lock:
            # Check if already reconciled
            if transaction.get("verification_status") == VerificationStatus.RECONCILED:
                logger.info(f"Transaction {transaction_id} already reconciled")
                return True
                
            # Update status
            transaction["verification_status"] = VerificationStatus.RECONCILED
            transaction["reconciliation_time"] = datetime.utcnow()
            transaction["reconciliation_notes"] = notes
            
            # Update statistics
            self._stats["reconciled_transactions"] += 1
            if transaction.get("verification_status") == VerificationStatus.DISCREPANCY:
                self._stats["discrepancy_transactions"] -= 1
                
            # Clear pending verifications
            self._pending_verifications.pop(transaction_id, None)
            
            logger.info(f"Transaction {transaction_id} manually reconciled")
            return True
    
    def get_statistics(self) -> Dict[str, int]:
        """
        Get verification statistics.
        
        Returns:
            Dict of statistics
        """
        return self._stats.copy()
    
    async def cleanup_old_transactions(self, days: int = 7) -> int:
        """
        Remove old transactions from memory.
        
        Args:
            days: Age in days for transactions to remove
            
        Returns:
            int: Number of transactions removed
        """
        cutoff_time = datetime.utcnow() - timedelta(days=days)
        transaction_ids_to_remove = []
        
        # Find old transactions
        for transaction_id, transaction in self._transactions.items():
            registration_time = transaction.get("registration_time")
            if registration_time and registration_time < cutoff_time:
                transaction_ids_to_remove.append(transaction_id)
                
        # Remove transactions
        count = 0
        for transaction_id in transaction_ids_to_remove:
            # Get lock
            lock = self._locks.get(transaction_id, asyncio.Lock())
            
            async with lock:
                # Remove transaction data
                self._transactions.pop(transaction_id, None)
                self._pending_verifications.pop(transaction_id, None)
                self._verification_results.pop(transaction_id, None)
                self._locks.pop(transaction_id, None)
                count += 1
                
        logger.info(f"Removed {count} old transactions (older than {days} days)")
        return count 