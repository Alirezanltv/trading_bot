"""
Transaction Verification Pipeline Test

This example demonstrates the three-phase commit transaction verification pipeline
for order execution with retry and recovery mechanisms.
"""

import os
import sys
import asyncio
import json
import logging
import time
import uuid
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional

# Add the repository root to the Python path
script_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(os.path.dirname(script_dir))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from trading_system.core.logging import initialize_logging, get_logger
from trading_system.core.message_bus import MessageBus, MessageTypes
from trading_system.execution.transaction_verifier import (
    TransactionVerifier, Transaction, TransactionPhase, TransactionStatus
)
from trading_system.models.order import Order, OrderType, OrderSide, OrderStatus, OrderTimeInForce

# Initialize logging
initialize_logging(log_level=logging.INFO)
logger = get_logger("examples.transaction_verification_test")


# Mock handlers for transaction phases
async def prepare_handler(transaction: Transaction) -> Tuple[bool, Optional[str]]:
    """
    Mock prepare handler.
    
    Args:
        transaction: Transaction to prepare
        
    Returns:
        Success flag and error message (if any)
    """
    logger.info(f"Preparing transaction {transaction.transaction_id}")
    
    # Simulate validation
    order = transaction.order
    
    # Check if symbol is valid
    if order.symbol not in ["BTC/USDT", "ETH/USDT", "XRP/USDT"]:
        return False, f"Invalid symbol: {order.symbol}"
    
    # Check if price is within allowed range
    if order.order_type != OrderType.MARKET and order.price:
        if order.price <= 0:
            return False, f"Invalid price: {order.price}"
    
    # Check if quantity is within allowed range
    if order.quantity <= 0:
        return False, f"Invalid quantity: {order.quantity}"
    
    # Simulate some processing time
    await asyncio.sleep(0.5)
    
    # Success
    return True, None


async def pre_commit_handler(transaction: Transaction) -> Tuple[bool, Optional[str]]:
    """
    Mock pre-commit handler.
    
    Args:
        transaction: Transaction to pre-commit
        
    Returns:
        Success flag and error message (if any)
    """
    logger.info(f"Pre-committing transaction {transaction.transaction_id}")
    
    # Simulate validation with exchange
    order = transaction.order
    
    # Simulate API call to check if order can be placed
    await asyncio.sleep(0.5)
    
    # Add exchange order ID
    if not order.exchange_order_id:
        # Generate mock exchange order ID
        order.exchange_order_id = f"mock_exchange_{uuid.uuid4().hex[:8]}"
    
    # Success
    return True, None


async def commit_handler(transaction: Transaction) -> Tuple[bool, Optional[str]]:
    """
    Mock commit handler.
    
    Args:
        transaction: Transaction to commit
        
    Returns:
        Success flag and error message (if any)
    """
    logger.info(f"Committing transaction {transaction.transaction_id}")
    
    # Simulate placing order with exchange
    order = transaction.order
    
    # Simulate API call to place order
    await asyncio.sleep(1.0)
    
    # Simulate order execution
    if order.order_type == OrderType.MARKET:
        # For market orders, simulate fill
        fill_price = 50000.0 if order.symbol == "BTC/USDT" else 2000.0  # Mock prices
        order.update_fill(order.quantity, fill_price)
    
    # Success
    return True, None


async def abort_handler(transaction: Transaction, reason: str) -> None:
    """
    Mock abort handler.
    
    Args:
        transaction: Transaction to abort
        reason: Reason for abort
    """
    logger.info(f"Aborting transaction {transaction.transaction_id}: {reason}")
    
    # Simulate cleanup
    await asyncio.sleep(0.5)
    
    logger.info(f"Transaction {transaction.transaction_id} aborted")


async def rollback_handler(transaction: Transaction, reason: str) -> None:
    """
    Mock rollback handler.
    
    Args:
        transaction: Transaction to rollback
        reason: Reason for rollback
    """
    logger.info(f"Rolling back transaction {transaction.transaction_id}: {reason}")
    
    # Simulate canceling order with exchange if needed
    if transaction.order.exchange_order_id:
        # Simulate API call to cancel order
        await asyncio.sleep(0.5)
        
        logger.info(f"Canceled order {transaction.order.exchange_order_id}")
    
    logger.info(f"Transaction {transaction.transaction_id} rolled back")


async def create_test_order() -> Order:
    """
    Create a test order.
    
    Returns:
        Test order
    """
    return Order(
        symbol="BTC/USDT",
        order_type=OrderType.LIMIT,
        side=OrderSide.BUY,
        quantity=0.1,
        price=50000.0,
        exchange_id="nobitex"
    )


async def test_successful_transaction(verifier: TransactionVerifier) -> None:
    """
    Test a successful transaction.
    
    Args:
        verifier: Transaction verifier
    """
    logger.info("=== Testing Successful Transaction ===")
    
    # Create test order
    order = await create_test_order()
    
    # Create transaction
    transaction = Transaction(order)
    
    # Execute transaction
    success, error = await verifier.execute_transaction(transaction)
    
    if success:
        logger.info(f"Transaction {transaction.transaction_id} executed successfully")
    else:
        logger.error(f"Transaction {transaction.transaction_id} failed: {error}")
    
    # Verify transaction status
    status = await verifier.verify_transaction(transaction.transaction_id)
    
    if status:
        logger.info(f"Transaction status: {status['status']}")
    else:
        logger.error(f"Transaction {transaction.transaction_id} not found")


async def test_failed_prepare(verifier: TransactionVerifier) -> None:
    """
    Test a transaction that fails in the prepare phase.
    
    Args:
        verifier: Transaction verifier
    """
    logger.info("=== Testing Failed Prepare ===")
    
    # Create test order with invalid symbol
    order = await create_test_order()
    order.symbol = "INVALID/PAIR"
    
    # Create transaction
    transaction = Transaction(order)
    
    # Execute transaction
    success, error = await verifier.execute_transaction(transaction)
    
    if success:
        logger.error(f"Transaction {transaction.transaction_id} should have failed")
    else:
        logger.info(f"Transaction {transaction.transaction_id} failed as expected: {error}")
    
    # Verify transaction status
    status = await verifier.verify_transaction(transaction.transaction_id)
    
    if status:
        logger.info(f"Transaction status: {status['status']}")
    else:
        logger.error(f"Transaction {transaction.transaction_id} not found")


async def test_failed_pre_commit(verifier: TransactionVerifier) -> None:
    """
    Test a transaction that fails in the pre-commit phase.
    
    Args:
        verifier: Transaction verifier
    """
    logger.info("=== Testing Failed Pre-Commit ===")
    
    # Override pre-commit handler to fail
    original_handler = verifier.pre_commit_handlers[0]
    
    async def failing_pre_commit_handler(transaction: Transaction) -> Tuple[bool, Optional[str]]:
        logger.info(f"Failing pre-commit for transaction {transaction.transaction_id}")
        return False, "Simulated pre-commit failure"
    
    verifier.pre_commit_handlers[0] = failing_pre_commit_handler
    
    # Create test order
    order = await create_test_order()
    
    # Create transaction
    transaction = Transaction(order)
    
    # Execute transaction
    success, error = await verifier.execute_transaction(transaction)
    
    if success:
        logger.error(f"Transaction {transaction.transaction_id} should have failed")
    else:
        logger.info(f"Transaction {transaction.transaction_id} failed as expected: {error}")
    
    # Verify transaction status
    status = await verifier.verify_transaction(transaction.transaction_id)
    
    if status:
        logger.info(f"Transaction status: {status['status']}")
    else:
        logger.error(f"Transaction {transaction.transaction_id} not found")
    
    # Restore original handler
    verifier.pre_commit_handlers[0] = original_handler


async def test_failed_commit(verifier: TransactionVerifier) -> None:
    """
    Test a transaction that fails in the commit phase.
    
    Args:
        verifier: Transaction verifier
    """
    logger.info("=== Testing Failed Commit ===")
    
    # Override commit handler to fail
    original_handler = verifier.commit_handlers[0]
    
    async def failing_commit_handler(transaction: Transaction) -> Tuple[bool, Optional[str]]:
        logger.info(f"Failing commit for transaction {transaction.transaction_id}")
        return False, "Simulated commit failure"
    
    verifier.commit_handlers[0] = failing_commit_handler
    
    # Create test order
    order = await create_test_order()
    
    # Create transaction
    transaction = Transaction(order)
    
    # Execute transaction
    success, error = await verifier.execute_transaction(transaction)
    
    if success:
        logger.error(f"Transaction {transaction.transaction_id} should have failed")
    else:
        logger.info(f"Transaction {transaction.transaction_id} failed as expected: {error}")
    
    # Verify transaction status
    status = await verifier.verify_transaction(transaction.transaction_id)
    
    if status:
        logger.info(f"Transaction status: {status['status']}")
    else:
        logger.error(f"Transaction {transaction.transaction_id} not found")
    
    # Restore original handler
    verifier.commit_handlers[0] = original_handler


async def run_test() -> None:
    """Run the transaction verification test."""
    try:
        logger.info("Starting transaction verification test")
        
        # Create message bus
        message_bus = MessageBus()
        await message_bus.initialize()
        
        # Create transaction verifier
        config = {
            "transaction_timeout": 30,
            "max_retries": 3,
            "retry_delay": 1.0,
            "order_check_interval": 5
        }
        
        verifier = TransactionVerifier(config, message_bus)
        
        # Register handlers
        verifier.register_prepare_handler(prepare_handler)
        verifier.register_pre_commit_handler(pre_commit_handler)
        verifier.register_commit_handler(commit_handler)
        verifier.register_abort_handler(abort_handler)
        verifier.register_rollback_handler(rollback_handler)
        
        # Initialize verifier
        success = await verifier.initialize()
        if not success:
            logger.error("Failed to initialize transaction verifier")
            return
        
        # Run tests
        await test_successful_transaction(verifier)
        await asyncio.sleep(2)  # Pause between tests
        
        await test_failed_prepare(verifier)
        await asyncio.sleep(2)  # Pause between tests
        
        await test_failed_pre_commit(verifier)
        await asyncio.sleep(2)  # Pause between tests
        
        await test_failed_commit(verifier)
        
        # Cleanup
        await verifier.shutdown()
        await message_bus.shutdown()
        
        logger.info("Transaction verification test completed")
        
    except Exception as e:
        logger.error(f"Error in transaction verification test: {str(e)}", exc_info=True)


def main() -> None:
    """Main entry point."""
    try:
        # Run the test
        asyncio.run(run_test())
        
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)


if __name__ == "__main__":
    main() 