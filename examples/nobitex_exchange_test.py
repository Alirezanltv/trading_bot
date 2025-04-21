"""
Nobitex Exchange Integration Test

This example demonstrates the Nobitex exchange integration with API key management,
connection pooling, and retry mechanisms.
"""

import os
import sys
import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

# Add the repository root to the Python path
script_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(os.path.dirname(script_dir))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from trading_system.core.logging import initialize_logging, get_logger
from trading_system.exchange.nobitex_adapter import NobitexAdapter
from trading_system.models.order import Order, OrderType, OrderSide, OrderStatus, OrderTimeInForce

# Initialize logging
initialize_logging(log_level=logging.INFO)
logger = get_logger("examples.nobitex_exchange_test")


async def test_get_markets(adapter: NobitexAdapter) -> None:
    """
    Test getting markets.
    
    Args:
        adapter: Nobitex adapter
    """
    logger.info("=== Testing Get Markets ===")
    
    markets = await adapter.get_markets()
    
    if markets:
        logger.info(f"Got {len(markets.get('markets', []))} markets")
        
        # Print a few markets
        for i, market in enumerate(markets.get("markets", [])[:5]):
            logger.info(f"Market {i+1}: {market}")
    else:
        logger.error("Failed to get markets")


async def test_get_ticker(adapter: NobitexAdapter) -> None:
    """
    Test getting ticker.
    
    Args:
        adapter: Nobitex adapter
    """
    logger.info("=== Testing Get Ticker ===")
    
    symbols = ["BTC/USDT", "ETH/USDT", "XRP/USDT"]
    
    for symbol in symbols:
        ticker = await adapter.get_ticker(symbol)
        
        if ticker:
            logger.info(f"Ticker for {symbol}: {ticker}")
        else:
            logger.error(f"Failed to get ticker for {symbol}")


async def test_get_order_book(adapter: NobitexAdapter) -> None:
    """
    Test getting order book.
    
    Args:
        adapter: Nobitex adapter
    """
    logger.info("=== Testing Get Order Book ===")
    
    symbol = "BTC/USDT"
    
    order_book = await adapter.get_order_book(symbol)
    
    if order_book:
        logger.info(f"Order book for {symbol}: {order_book}")
    else:
        logger.error(f"Failed to get order book for {symbol}")


async def test_get_account_balance(adapter: NobitexAdapter) -> None:
    """
    Test getting account balance.
    
    Args:
        adapter: Nobitex adapter
    """
    logger.info("=== Testing Get Account Balance ===")
    
    balance = await adapter.get_account_balance()
    
    if balance:
        logger.info(f"Account balance: {balance}")
    else:
        logger.error("Failed to get account balance")


async def test_create_order(adapter: NobitexAdapter) -> None:
    """
    Test creating an order.
    
    Args:
        adapter: Nobitex adapter
    """
    logger.info("=== Testing Create Order ===")
    
    # Create a limit order
    order = Order(
        symbol="BTC/USDT",
        order_type=OrderType.LIMIT,
        side=OrderSide.BUY,
        quantity=0.001,  # Small quantity for testing
        price=20000.0,   # Below market price to avoid execution
        exchange_id="nobitex"
    )
    
    success, error, response = await adapter.create_order(order)
    
    if success:
        logger.info(f"Order created successfully: {order.exchange_order_id}")
    else:
        logger.error(f"Failed to create order: {error}")
    
    # Store order for cancellation
    return order


async def test_cancel_order(adapter: NobitexAdapter, order: Order) -> None:
    """
    Test canceling an order.
    
    Args:
        adapter: Nobitex adapter
        order: Order to cancel
    """
    logger.info("=== Testing Cancel Order ===")
    
    # Wait a bit to ensure order is in the system
    await asyncio.sleep(2)
    
    success, error = await adapter.cancel_order(order.client_order_id)
    
    if success:
        logger.info(f"Order {order.client_order_id} canceled successfully")
    else:
        logger.error(f"Failed to cancel order {order.client_order_id}: {error}")


async def test_get_order_status(adapter: NobitexAdapter, order: Order) -> None:
    """
    Test getting order status.
    
    Args:
        adapter: Nobitex adapter
        order: Order to check
    """
    logger.info("=== Testing Get Order Status ===")
    
    # Wait a bit to ensure order is in the system
    await asyncio.sleep(2)
    
    status = await adapter.get_order_status(order.client_order_id)
    
    if status:
        logger.info(f"Order status: {status}")
    else:
        logger.error(f"Failed to get status for order {order.client_order_id}")


async def test_get_open_orders(adapter: NobitexAdapter) -> None:
    """
    Test getting open orders.
    
    Args:
        adapter: Nobitex adapter
    """
    logger.info("=== Testing Get Open Orders ===")
    
    orders = await adapter.get_open_orders()
    
    if orders is not None:
        logger.info(f"Got {len(orders)} open orders")
        
        # Print a few orders
        for i, order in enumerate(orders[:5]):
            logger.info(f"Order {i+1}: {order}")
    else:
        logger.error("Failed to get open orders")


async def run_test() -> None:
    """Run the Nobitex exchange integration test."""
    try:
        logger.info("Starting Nobitex exchange integration test")
        
        # Create Nobitex adapter
        config = {
            # API credentials are loaded from environment variables
            "retry_attempts": 3,
            "retry_delay": 1.0,
            "connection_pool_size": 10,
            "rate_limit_per_min": 180
        }
        
        adapter = NobitexAdapter(config)
        
        # Initialize adapter
        success = await adapter.initialize()
        if not success:
            logger.error("Failed to initialize Nobitex adapter")
            return
        
        # Run tests
        await test_get_markets(adapter)
        await asyncio.sleep(1)  # Pause between tests
        
        await test_get_ticker(adapter)
        await asyncio.sleep(1)  # Pause between tests
        
        await test_get_order_book(adapter)
        await asyncio.sleep(1)  # Pause between tests
        
        await test_get_account_balance(adapter)
        await asyncio.sleep(1)  # Pause between tests
        
        # Only run these tests if we have valid API credentials
        if adapter.credentials.api_key and adapter.credentials.api_secret:
            # Test order creation and management
            order = await test_create_order(adapter)
            await asyncio.sleep(1)  # Pause between tests
            
            await test_get_order_status(adapter, order)
            await asyncio.sleep(1)  # Pause between tests
            
            await test_get_open_orders(adapter)
            await asyncio.sleep(1)  # Pause between tests
            
            await test_cancel_order(adapter, order)
        else:
            logger.warning("Skipping order tests due to missing API credentials")
        
        # Cleanup
        await adapter.shutdown()
        
        logger.info("Nobitex exchange integration test completed")
        
    except Exception as e:
        logger.error(f"Error in Nobitex exchange integration test: {str(e)}", exc_info=True)


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