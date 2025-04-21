#!/usr/bin/env python
"""
Exchange integration test script.

This script tests the exchange integration layer with Nobitex.
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
from trading_system.exchange.base import Exchange, ExchangeManager, OrderType, OrderSide
from trading_system.exchange.nobitex import NobitexExchange
from trading_system.exchange.config import exchange_config

# Set up logging
setup_logging()
logger = get_logger("exchange_test")

async def test_basic_connectivity():
    """Test basic exchange connectivity."""
    logger.info("=== Testing Basic Connectivity ===")
    
    # Create exchange instance
    exchange = NobitexExchange("nobitex_test", config={
        "api_key": os.getenv("NOBITEX_API_KEY"),
        "api_secret": os.getenv("NOBITEX_API_SECRET"),
    })
    
    # Connect to exchange
    connected = await exchange.connect()
    logger.info(f"Connected to exchange: {connected}")
    
    if connected:
        # Fetch markets
        markets = await exchange.fetch_markets()
        logger.info(f"Fetched {len(markets)} markets")
        
        # Print first 5 markets
        for i, (symbol, market) in enumerate(list(markets.items())[:5]):
            logger.info(f"Market {i+1}: {symbol} - {market.base_asset}/{market.quote_asset}")
        
        # Disconnect from exchange
        disconnected = await exchange.disconnect()
        logger.info(f"Disconnected from exchange: {disconnected}")
    
    logger.info("Basic connectivity test completed")

async def test_market_data():
    """Test market data endpoints."""
    logger.info("=== Testing Market Data ===")
    
    # Create exchange instance
    exchange = NobitexExchange("nobitex_test", config={
        "api_key": os.getenv("NOBITEX_API_KEY"),
        "api_secret": os.getenv("NOBITEX_API_SECRET"),
    })
    
    # Connect to exchange
    connected = await exchange.connect()
    
    if connected:
        try:
            # Fetch ticker
            symbol = "BTC/USDT"
            logger.info(f"Fetching ticker for {symbol}")
            ticker = await exchange.fetch_ticker(symbol)
            logger.info(f"Ticker: {ticker.symbol} - Last: {ticker.last}, Bid: {ticker.bid}, Ask: {ticker.ask}")
            
            # Fetch order book
            logger.info(f"Fetching order book for {symbol}")
            order_book = await exchange.fetch_order_book(symbol, limit=5)
            
            logger.info(f"Order Book: {order_book.symbol} - Timestamp: {order_book.timestamp}")
            logger.info(f"Top 3 Bids:")
            for i, (price, amount) in enumerate(order_book.bids[:3]):
                logger.info(f"  {i+1}: Price: {price}, Amount: {amount}")
            
            logger.info(f"Top 3 Asks:")
            for i, (price, amount) in enumerate(order_book.asks[:3]):
                logger.info(f"  {i+1}: Price: {price}, Amount: {amount}")
        
        finally:
            # Disconnect from exchange
            await exchange.disconnect()
    
    logger.info("Market data test completed")

async def test_account_data():
    """Test account data endpoints."""
    logger.info("=== Testing Account Data ===")
    
    # Create exchange instance
    exchange = NobitexExchange("nobitex_test", config={
        "api_key": os.getenv("NOBITEX_API_KEY"),
        "api_secret": os.getenv("NOBITEX_API_SECRET"),
    })
    
    # Connect to exchange
    connected = await exchange.connect()
    
    if connected:
        try:
            # Fetch balance
            logger.info("Fetching account balance")
            balances = await exchange.fetch_balance()
            
            logger.info(f"Fetched {len(balances)} balances")
            
            # Print non-zero balances
            non_zero = {asset: balance for asset, balance in balances.items() if balance.total > 0}
            
            for asset, balance in non_zero.items():
                logger.info(f"Balance: {asset} - Total: {balance.total}, Available: {balance.available}")
            
            # Fetch open orders
            logger.info("Fetching open orders")
            
            try:
                open_orders = await exchange.fetch_open_orders()
                logger.info(f"Fetched {len(open_orders)} open orders")
                
                for i, order in enumerate(open_orders):
                    logger.info(f"Order {i+1}: {order.symbol} - {order.side.value} {order.amount} @ {order.price}")
            
            except Exception as e:
                logger.error(f"Error fetching open orders: {str(e)}")
        
        finally:
            # Disconnect from exchange
            await exchange.disconnect()
    
    logger.info("Account data test completed")

async def test_order_lifecycle():
    """Test order creation and cancellation."""
    logger.info("=== Testing Order Lifecycle ===")
    logger.warning("This test will create and cancel real orders! Make sure you're on a test account or skip it.")
    
    # Ask for confirmation
    confirmation = input("Do you want to continue with order creation test? (y/n): ")
    
    if confirmation.lower() != "y":
        logger.info("Order lifecycle test skipped")
        return
    
    # Create exchange instance
    exchange = NobitexExchange("nobitex_test", config={
        "api_key": os.getenv("NOBITEX_API_KEY"),
        "api_secret": os.getenv("NOBITEX_API_SECRET"),
    })
    
    # Connect to exchange
    connected = await exchange.connect()
    
    if connected:
        try:
            # Fetch ticker to get current price
            symbol = "BTC/USDT"
            ticker = await exchange.fetch_ticker(symbol)
            
            # Calculate price for limit order (10% below current price)
            price = ticker.last * 0.9
            amount = 0.001  # Small amount for testing
            
            logger.info(f"Current price: {ticker.last}, Order price: {price}, Amount: {amount}")
            
            # Create limit order
            logger.info(f"Creating limit buy order for {symbol}")
            order = await exchange.create_order(
                symbol=symbol,
                order_type=OrderType.LIMIT,
                side=OrderSide.BUY,
                amount=amount,
                price=price
            )
            
            logger.info(f"Created order: ID: {order.id}, Status: {order.status.value}")
            
            # Wait a moment
            logger.info("Waiting 3 seconds...")
            await asyncio.sleep(3)
            
            # Fetch order
            logger.info(f"Fetching order {order.id}")
            updated_order = await exchange.fetch_order(order.id, symbol)
            
            logger.info(f"Order status: {updated_order.status.value}, Filled: {updated_order.filled}/{updated_order.amount}")
            
            # Cancel order
            logger.info(f"Canceling order {order.id}")
            canceled_order = await exchange.cancel_order(order.id, symbol)
            
            logger.info(f"Canceled order: ID: {canceled_order.id}, Status: {canceled_order.status.value}")
        
        except Exception as e:
            logger.error(f"Error in order lifecycle test: {str(e)}", exc_info=True)
        
        finally:
            # Disconnect from exchange
            await exchange.disconnect()
    
    logger.info("Order lifecycle test completed")

async def test_exchange_manager():
    """Test exchange manager functionality."""
    logger.info("=== Testing Exchange Manager ===")
    
    # Create exchange manager
    from trading_system.exchange.base import exchange_manager
    
    # Initialize exchange manager
    success = await exchange_manager.initialize()
    logger.info(f"Exchange manager initialized: {success}")
    
    if success:
        try:
            # Add Nobitex exchange
            logger.info("Adding Nobitex exchange")
            added = await exchange_manager.add_exchange("nobitex", "nobitex", {
                "api_key": os.getenv("NOBITEX_API_KEY"),
                "api_secret": os.getenv("NOBITEX_API_SECRET"),
                "default": True
            })
            
            logger.info(f"Added exchange: {added}")
            
            # Get exchange
            exchange = exchange_manager.get_exchange("nobitex")
            
            if exchange:
                # Fetch ticker
                symbol = "BTC/USDT"
                logger.info(f"Fetching ticker for {symbol}")
                ticker = await exchange.fetch_ticker(symbol)
                logger.info(f"Ticker: {ticker.symbol} - Last: {ticker.last}")
            
            # Check exchange manager health
            status, info = await exchange_manager._check_health()
            logger.info(f"Exchange manager health: {status.value}")
            logger.info(f"Health info: {info}")
            
            # Remove exchange
            logger.info("Removing Nobitex exchange")
            removed = await exchange_manager.remove_exchange("nobitex")
            logger.info(f"Removed exchange: {removed}")
        
        finally:
            # Shutdown exchange manager
            await exchange_manager.shutdown()
    
    logger.info("Exchange manager test completed")

async def main():
    """Run all tests."""
    logger.info("Starting exchange integration tests")
    
    try:
        # Test basic connectivity
        await test_basic_connectivity()
        print("\n")
        
        # Test market data
        await test_market_data()
        print("\n")
        
        # Test account data
        await test_account_data()
        print("\n")
        
        # Test exchange manager
        await test_exchange_manager()
        print("\n")
        
        # Test order lifecycle - Optional, creates real orders
        # await test_order_lifecycle()
        
    except Exception as e:
        logger.error(f"Error during tests: {str(e)}", exc_info=True)
    
    logger.info("All tests completed")

if __name__ == "__main__":
    asyncio.run(main()) 