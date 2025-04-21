#!/usr/bin/env python
"""
Execution engine test script.

This script tests the execution engine and transaction verification pipeline.
"""

import os
import sys
import asyncio
import json
import time
import logging
from pprint import pprint
from typing import Dict, Any, List

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from trading_system.core import setup_logging, get_logger
from trading_system.exchange.base import exchange_manager
from trading_system.execution.engine import execution_engine
from trading_system.execution.transaction import TransactionType
from trading_system.exchange.nobitex import NobitexExchange

# Set up logging
setup_logging()
logger = get_logger("execution_test")

async def setup_test_environment():
    """Set up test environment."""
    logger.info("Setting up test environment")
    
    # Initialize exchange manager
    if not await exchange_manager.initialize():
        logger.error("Failed to initialize exchange manager")
        return False
    
    # Add Nobitex exchange
    if "nobitex" not in exchange_manager._exchanges:
        logger.info("Adding Nobitex exchange")
        added = await exchange_manager.add_exchange("nobitex", "nobitex", {
            "api_key": os.getenv("NOBITEX_API_KEY"),
            "api_secret": os.getenv("NOBITEX_API_SECRET"),
            "default": True
        })
        
        if not added:
            logger.error("Failed to add Nobitex exchange")
            return False
    
    # Initialize execution engine
    if not await execution_engine.initialize():
        logger.error("Failed to initialize execution engine")
        return False
    
    # Start execution engine
    if not await execution_engine.start():
        logger.error("Failed to start execution engine")
        return False
    
    logger.info("Test environment set up successfully")
    return True

async def cleanup_test_environment():
    """Clean up test environment."""
    logger.info("Cleaning up test environment")
    
    # Stop execution engine
    await execution_engine.stop()
    
    # Remove exchanges
    for name in list(exchange_manager._exchanges.keys()):
        await exchange_manager.remove_exchange(name)
    
    # Shutdown exchange manager
    await exchange_manager.shutdown()
    
    logger.info("Test environment cleaned up")

async def test_simple_transaction():
    """Test simple transaction."""
    logger.info("=== Testing Simple Transaction ===")
    
    # Get exchange
    exchange = exchange_manager.get_exchange("nobitex")
    
    if not exchange:
        logger.error("Nobitex exchange not found")
        return
    
    # Fetch ticker to get current price
    symbol = "BTC/USDT"
    ticker = await exchange.fetch_ticker(symbol)
    
    # Calculate price for limit order (10% below current price)
    price = ticker.last * 0.9
    amount = 0.001  # Small amount for testing
    
    logger.info(f"Current price: {ticker.last}, Order price: {price}, Amount: {amount}")
    
    # Create and execute transaction
    context = await execution_engine.execute_order(
        symbol=symbol,
        order_type="limit",
        side="buy",
        amount=amount,
        price=price,
        metadata={"test": True}
    )
    
    logger.info(f"Created transaction: {context.id}")
    
    # Monitor transaction status
    for _ in range(10):
        # Get transaction status
        status = await execution_engine.get_transaction_status(context.id)
        
        if not status:
            logger.error("Transaction not found")
            break
        
        logger.info(f"Transaction status: {status['status']}, Phase: {status['phase']}")
        
        # Check if transaction completed or failed
        if status['status'] in ['completed', 'failed', 'aborted', 'rolled_back']:
            logger.info(f"Transaction {status['status']}")
            break
        
        # Wait before checking again
        await asyncio.sleep(1)
    
    # Print final transaction status
    status = await execution_engine.get_transaction_status(context.id)
    
    if status:
        logger.info(f"Final transaction status: {status['status']}, Phase: {status['phase']}")
        
        # Cancel order if still active
        if status['status'] == 'in_progress' and status['order_id']:
            try:
                logger.info(f"Canceling order {status['order_id']}")
                await exchange.cancel_order(status['order_id'], symbol)
                logger.info("Order canceled")
            except Exception as e:
                logger.error(f"Error canceling order: {str(e)}")
    
    logger.info("Simple transaction test completed")

async def test_transaction_verification():
    """Test transaction verification."""
    logger.info("=== Testing Transaction Verification ===")
    
    # Get exchange
    exchange = exchange_manager.get_exchange("nobitex")
    
    if not exchange:
        logger.error("Nobitex exchange not found")
        return
    
    # Create buy transaction
    transaction_processor = execution_engine.transaction_processor
    
    context = await transaction_processor.create_transaction(
        type=TransactionType.LIMIT_BUY,
        symbol="BTC/USDT",
        amount=0.001,
        price=10000,  # Very low price, unlikely to be filled
        exchange="nobitex",
        metadata={"test": True, "accept_partial_fill": True}
    )
    
    logger.info(f"Created transaction: {context.id}")
    
    # Execute transaction manually
    success = await transaction_processor.execute_transaction(context)
    
    logger.info(f"Transaction execution: {success}")
    
    # Monitor transaction status
    for _ in range(5):
        # Get transaction context
        context = transaction_processor.get_transaction(context.id)
        
        if not context:
            logger.error("Transaction not found")
            break
        
        logger.info(f"Transaction status: {context.status.value}, Phase: {context.phase.value}")
        logger.info(f"Verification attempts: {context.verification_attempts}/{context.max_verification_attempts}")
        
        # Check if transaction completed or failed
        if context.status.value in ['completed', 'failed', 'aborted', 'rolled_back']:
            logger.info(f"Transaction {context.status.value}")
            break
        
        # Wait before checking again
        await asyncio.sleep(2)
    
    # Cancel order if still active
    if context.status.value == 'in_progress' and context.order_id:
        try:
            logger.info(f"Canceling order {context.order_id}")
            await exchange.cancel_order(context.order_id, context.symbol)
            logger.info("Order canceled")
        except Exception as e:
            logger.error(f"Error canceling order: {str(e)}")
    
    logger.info("Transaction verification test completed")

async def test_failed_transaction():
    """Test failed transaction."""
    logger.info("=== Testing Failed Transaction ===")
    
    # Create transaction with invalid parameters
    try:
        context = await execution_engine.execute_order(
            symbol="INVALID/PAIR",
            order_type="limit",
            side="buy",
            amount=0.001,
            price=10000,
            metadata={"test": True}
        )
        
        logger.info(f"Created transaction: {context.id}")
        
        # Monitor transaction status
        for _ in range(5):
            # Get transaction status
            status = await execution_engine.get_transaction_status(context.id)
            
            if not status:
                logger.error("Transaction not found")
                break
            
            logger.info(f"Transaction status: {status['status']}, Phase: {status['phase']}")
            
            # Check if transaction completed or failed
            if status['status'] in ['completed', 'failed', 'aborted', 'rolled_back']:
                logger.info(f"Transaction {status['status']}")
                break
            
            # Wait before checking again
            await asyncio.sleep(1)
    
    except Exception as e:
        logger.error(f"Error executing order: {str(e)}")
    
    logger.info("Failed transaction test completed")

async def main():
    """Run all tests."""
    logger.info("Starting execution engine tests")
    
    try:
        # Set up test environment
        if not await setup_test_environment():
            logger.error("Failed to set up test environment")
            return
        
        # Test simple transaction
        await test_simple_transaction()
        print("\n")
        
        # Test transaction verification
        await test_transaction_verification()
        print("\n")
        
        # Test failed transaction
        await test_failed_transaction()
        print("\n")
        
    except Exception as e:
        logger.error(f"Error during tests: {str(e)}", exc_info=True)
    
    finally:
        # Clean up test environment
        await cleanup_test_environment()
    
    logger.info("All tests completed")

if __name__ == "__main__":
    asyncio.run(main()) 