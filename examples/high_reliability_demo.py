#!/usr/bin/env python3
"""
High Reliability Trading System Demo

This script demonstrates the high-reliability components of the trading system:
- Connection Pool for exchange API resilience
- Transaction Verification with three-phase commit
- Shadow Accounting with position reconciliation
- RabbitMQ messaging for reliable communication
- Circuit breakers for fault tolerance

Run with: py -3.10 -m trading_system.examples.high_reliability_demo
"""

import asyncio
import logging
import random
import time
import os
import json
import sys
from datetime import datetime
from typing import Dict, Any, List, Optional
import uuid

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger("high_reliability_demo")

# Import core components
from trading_system.exchange.connection_pool import ConnectionPool
from trading_system.exchange.transaction_verification import (
    TransactionVerificationPipeline, Transaction, 
    TransactionPhase, TransactionStatus
)
from trading_system.position.shadow_accounting import (
    ShadowPositionManager, PositionSource, ReconciliationStatus
)
from trading_system.messaging.rabbitmq_adapter import RabbitMQAdapter
from trading_system.core import message_bus


# Mock exchange client for demo
class MockExchangeClient:
    """Mock exchange client for testing"""
    
    def __init__(self, exchange_id: str = "mock_exchange"):
        self.exchange_id = exchange_id
        self.positions = {
            "BTC/USDT": {"quantity": 0.5, "price": 35000.0},
            "ETH/USDT": {"quantity": 5.0, "price": 2000.0},
            "SOL/USDT": {"quantity": 20.0, "price": 100.0},
        }
        self.orders = {}
        self.connection_errors = 0
        self.max_connection_errors = 3
        self.delay = 0.1
        
    async def get_positions(self):
        """Get positions from the exchange"""
        await asyncio.sleep(self.delay)  # Simulate network delay
        
        if self.connection_errors > 0:
            self.connection_errors -= 1
            raise ConnectionError("Simulated connection error")
        
        return [
            {
                "symbol": symbol,
                "quantity": data["quantity"],
                "price": data["price"],
                "value": data["quantity"] * data["price"]
            }
            for symbol, data in self.positions.items()
        ]
    
    async def create_order(self, symbol: str, side: str, quantity: float, price: float):
        """Create an order on the exchange"""
        await asyncio.sleep(self.delay)  # Simulate network delay
        
        if self.connection_errors > 0:
            self.connection_errors -= 1
            raise ConnectionError("Simulated connection error")
        
        order_id = f"order_{uuid.uuid4().hex[:8]}"
        self.orders[order_id] = {
            "id": order_id,
            "symbol": symbol,
            "side": side,
            "quantity": quantity,
            "price": price,
            "status": "open",
            "timestamp": datetime.now().isoformat()
        }
        
        # Update position
        if symbol not in self.positions:
            self.positions[symbol] = {"quantity": 0.0, "price": 0.0}
        
        # Simple position update logic
        if side == "buy":
            self.positions[symbol]["quantity"] += quantity
            self.positions[symbol]["price"] = price
        else:
            self.positions[symbol]["quantity"] -= quantity
            self.positions[symbol]["price"] = price
        
        return {"order_id": order_id, "status": "success"}
    
    async def get_order(self, order_id: str):
        """Get order details"""
        await asyncio.sleep(self.delay)  # Simulate network delay
        
        if self.connection_errors > 0:
            self.connection_errors -= 1
            raise ConnectionError("Simulated connection error")
        
        if order_id in self.orders:
            return self.orders[order_id]
        
        return None
    
    async def cancel_order(self, order_id: str):
        """Cancel an order"""
        await asyncio.sleep(self.delay)  # Simulate network delay
        
        if self.connection_errors > 0:
            self.connection_errors -= 1
            raise ConnectionError("Simulated connection error")
        
        if order_id in self.orders:
            self.orders[order_id]["status"] = "cancelled"
            return {"order_id": order_id, "status": "cancelled"}
        
        return {"order_id": order_id, "status": "not_found"}
    
    def simulate_connection_errors(self, count: int = 3):
        """Simulate connection errors for the next N calls"""
        self.connection_errors = count
        self.max_connection_errors = count
    
    def set_delay(self, delay: float):
        """Set simulated network delay"""
        self.delay = delay


class Order:
    """Simple order class for demo"""
    
    def __init__(self, symbol: str, side: str, quantity: float, price: float):
        self.order_id = f"order_{uuid.uuid4().hex[:8]}"
        self.symbol = symbol
        self.side = side
        self.quantity = quantity
        self.price = price
        self.exchange_id = "mock_exchange"
        self.status = "new"
        self.timestamp = datetime.now().isoformat()
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            "order_id": self.order_id,
            "symbol": self.symbol,
            "side": self.side,
            "quantity": self.quantity,
            "price": self.price,
            "exchange_id": self.exchange_id,
            "status": self.status,
            "timestamp": self.timestamp
        }


async def demo_connection_pool():
    """Demonstrate the connection pool with failures and recovery"""
    logger.info("\n=== DEMO: Connection Pool ===")
    
    def create_client():
        return MockExchangeClient()
    
    # Create connection pool
    config = {
        "min_connections": 2,
        "max_connections": 5,
        "max_requests_per_window": 10,
        "window_seconds": 1
    }
    
    logger.info("Initializing connection pool...")
    pool = ConnectionPool("MockExchange", create_client, config)
    await pool.initialize()
    
    # Show successful requests
    logger.info("Making successful API requests...")
    for i in range(3):
        result = await pool.execute("get_positions")
        logger.info(f"Request {i+1} succeeded: {len(result)} positions")
        if i == 0:
            positions = result
            logger.info(f"Positions: {positions}")
        await asyncio.sleep(0.5)
    
    # Demonstrate fault tolerance with connection errors
    logger.info("\nDemonstrating fault tolerance with connection errors...")
    
    # Inject connection errors in all clients
    for conn_id, conn in pool.connections.items():
        if hasattr(conn.client, "simulate_connection_errors"):
            conn.client.simulate_connection_errors(2)
            logger.info(f"Simulated connection errors for {conn_id}")
    
    # Execute requests and observe recovery
    for i in range(5):
        try:
            result = await pool.execute("get_positions")
            logger.info(f"Request {i+1} succeeded despite failures: {len(result)} positions")
        except Exception as e:
            logger.info(f"Request {i+1} failed as expected: {e}")
        
        await asyncio.sleep(1)
    
    # Display pool statistics
    stats = pool.get_stats()
    logger.info(f"\nConnection pool statistics:")
    logger.info(f"  Active connections: {stats['pool']['current_connections']}")
    logger.info(f"  Available connections: {stats['pool']['available_connections']}")
    logger.info(f"  Total requests: {stats['pool']['total_requests']}")
    logger.info(f"  Successful requests: {stats['pool']['successful_requests']}")
    logger.info(f"  Failed requests: {stats['pool']['failed_requests']}")
    
    # Cleanup
    await pool.stop()
    logger.info("Connection pool demo completed")
    return pool


async def demo_transaction_verification():
    """Demonstrate transaction verification with three-phase commit"""
    logger.info("\n=== DEMO: Transaction Verification ===")
    
    # Create a mock exchange client
    client = MockExchangeClient()
    
    # Handler for validation phase
    async def validate_handler(order):
        """Validate an order before submission"""
        logger.info(f"Validating order: {order.to_dict()}")
        
        # Check if the order is valid
        is_valid = True
        if order.quantity <= 0:
            is_valid = False
            
        # Simulate validation time
        await asyncio.sleep(0.2)
        
        # Return validation result
        return {
            "is_valid": is_valid,
            "data": {"pre_validation": "passed" if is_valid else "failed"}
        }
    
    # Handler for submission phase
    async def submit_handler(order):
        """Submit an order to the exchange"""
        logger.info(f"Submitting order: {order.to_dict()}")
        
        try:
            # Submit order to exchange
            result = await client.create_order(
                order.symbol, 
                order.side, 
                order.quantity, 
                order.price
            )
            
            logger.info(f"Order submitted successfully: {result}")
            
            return {
                "success": True,
                "order_id": result["order_id"],
                "data": result
            }
        except Exception as e:
            logger.error(f"Order submission error: {e}")
            raise
    
    # Handler for verification phase
    async def verify_handler(order, transaction_data):
        """Verify an order after submission"""
        logger.info(f"Verifying order: {order.order_id}")
        
        try:
            # Get the exchange order ID from the transaction data
            exchange_order_id = transaction_data.get("exchange_order_id")
            if not exchange_order_id and transaction_data.get("phases"):
                # Try to find it in the phases
                for phase in transaction_data.get("phases", []):
                    if phase.get("phase") == "commit" and phase.get("result", {}).get("order_id"):
                        exchange_order_id = phase.get("result", {}).get("order_id")
                        break
            
            if not exchange_order_id:
                logger.error(f"No exchange order ID found in transaction data")
                return None
                
            # Get the order status from the exchange
            exchange_order = await client.get_order(exchange_order_id)
            
            # If order not found or verification fails, simulation continues
            if random.random() < 0.3:  # 30% chance of verification failure for demo
                logger.warning("Verification failed intentionally for demo")
                return None  # This will cause a retry
            
            # Verify the order details
            if not exchange_order:
                logger.error(f"Order {exchange_order_id} not found on exchange")
                return None
            
            return {
                "success": True,
                "status": exchange_order["status"],
                "data": exchange_order
            }
            
        except Exception as e:
            logger.error(f"Order verification error: {e}")
            raise
    
    # Handler for rollback/cancellation
    async def cancel_handler(order, transaction_data):
        """Cancel an order if verification fails"""
        logger.info(f"Cancelling order: {order.order_id}")
        
        try:
            # Get the exchange order ID from the transaction data
            exchange_order_id = transaction_data.get("exchange_order_id")
            if not exchange_order_id and transaction_data.get("phases"):
                # Try to find it in the phases
                for phase in transaction_data.get("phases", []):
                    if phase.get("phase") == "commit" and phase.get("result", {}).get("order_id"):
                        exchange_order_id = phase.get("result", {}).get("order_id")
                        break
            
            if not exchange_order_id:
                logger.error(f"No exchange order ID found in transaction data")
                return {
                    "success": False,
                    "error": "No exchange order ID found"
                }
                
            # Cancel the order on the exchange
            result = await client.cancel_order(exchange_order_id)
            
            logger.info(f"Order cancelled: {result}")
            
            return {
                "success": True,
                "data": result
            }
        except Exception as e:
            logger.error(f"Order cancellation error: {e}")
            raise
    
    # Create transaction verification pipeline
    pipeline = TransactionVerificationPipeline(
        config={
            "storage_path": "data/transactions",
            "max_verification_attempts": 5,
            "retry_delay": 1.0,
            "verification_timeout": 10.0
        }
    )
    
    # Register handlers
    pipeline.register_handlers(
        validate_func=validate_handler,
        submit_func=submit_handler,
        verify_func=verify_handler,
        cancel_func=cancel_handler
    )
    
    # Initialize the pipeline
    await pipeline.initialize()
    
    # Create a test order
    order = Order("BTC/USDT", "buy", 0.1, 35000.0)
    
    # Execute the transaction
    logger.info("\nExecuting transaction with built-in reliability...")
    try:
        # Execute order and get result
        execution_result = await pipeline.execute_order(order)
        logger.info(f"Execution result: {execution_result}")
        
        # Get the transaction
        transaction_id = execution_result.get('transaction_id')
        transaction = await pipeline.get_transaction(transaction_id)
        
        if not transaction:
            logger.error(f"Transaction {transaction_id} not found")
            return
            
        logger.info("\nTransaction details:")
        logger.info(f"  ID: {transaction['transaction_id']}")
        logger.info(f"  Order ID: {transaction['order_id']}")
        logger.info(f"  Status: {transaction['status']}")
        logger.info(f"  Current Phase: {transaction['current_phase']}")
        
        # Wait for transaction to complete or fail
        max_wait_time = 30  # seconds
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            # Get updated transaction
            transaction = await pipeline.get_transaction(transaction_id)
            
            if transaction['status'] in ['completed', 'failed', 'rolled_back']:
                break
                
            logger.info(f"Waiting for transaction to complete... (current phase: {transaction['current_phase']})")
            await asyncio.sleep(2)
        
        # Get final transaction state
        transaction = await pipeline.get_transaction(transaction_id)
        
        logger.info("\nFinal transaction state:")
        logger.info(f"  Status: {transaction['status']}")
        logger.info(f"  Current Phase: {transaction['current_phase']}")
        logger.info(f"  Verification Attempts: {transaction['verification_attempts']}")
        
        # Show transaction phases
        logger.info("\nTransaction phases:")
        for phase in transaction.get('phases', []):
            phase_name = phase.get('phase', 'unknown')
            timestamp = phase.get('timestamp', 'unknown')
            logger.info(f"  {phase_name} @ {timestamp}")
        
        # Show any errors
        if transaction.get('errors', []):
            logger.info("\nTransaction errors:")
            for error in transaction.get('errors', []):
                phase = error.get('phase', 'unknown')
                timestamp = error.get('timestamp', 'unknown')
                error_msg = error.get('error', 'unknown')
                logger.info(f"  {phase} @ {timestamp}: {error_msg}")
        
    except Exception as e:
        logger.error(f"Transaction execution failed: {e}")
    
    # Cleanup
    await pipeline.stop()
    logger.info("Transaction verification demo completed")
    return pipeline


async def demo_shadow_accounting():
    """Demonstrate shadow accounting with position reconciliation"""
    logger.info("\n=== DEMO: Shadow Accounting ===")
    
    # Create mock exchange clients
    exchange1 = MockExchangeClient("exchange1")
    exchange2 = MockExchangeClient("exchange2")
    
    # Initialize shadow position manager
    shadow_manager = ShadowPositionManager(
        config={
            "db_path": "data/positions.db",
            "reconciliation_interval": 5,
            "tolerance": 0.01,
            "max_history": 100,
            "auto_correct": True
        }
    )
    
    # Initialize the manager
    await shadow_manager.initialize()
    
    # Register exchange clients
    await shadow_manager.register_exchange_client("exchange1", exchange1)
    await shadow_manager.register_exchange_client("exchange2", exchange2)
    
    # Show current positions
    positions = await shadow_manager.get_all_positions()
    logger.info(f"Initial positions: {len(positions)}")
    for pos in positions:
        logger.info(f"  {pos.get('exchange_id', 'unknown')}:{pos.get('symbol', 'unknown')} - {pos.get('quantity', 0.0)} @ {pos.get('price', 0.0)}")
    
    # Manually update a position to create a discrepancy
    logger.info("\nCreating position discrepancy...")
    await shadow_manager.update_position(
        exchange_id="exchange1", 
        symbol="BTC/USDT", 
        quantity=0.51, 
        price=35100.0, 
        source=PositionSource.INTERNAL
    )
    
    # Add some other positions for demonstration
    await shadow_manager.update_position(
        exchange_id="exchange1", 
        symbol="ETH/USDT", 
        quantity=5.0, 
        price=2001.0, 
        source=PositionSource.INTERNAL
    )
    
    await shadow_manager.update_position(
        exchange_id="exchange2", 
        symbol="BTC/USDT", 
        quantity=0.2, 
        price=36000.0, 
        source=PositionSource.INTERNAL
    )
    
    # Trigger position reconciliation
    logger.info("\nRunning position reconciliation...")
    reconciliation_result = await shadow_manager.reconcile_positions()
    
    # Show reconciliation results
    logger.info(f"Reconciliation results:")
    logger.info(f"  Matched: {reconciliation_result.get('matched', 0)}")
    logger.info(f"  Mismatched: {reconciliation_result.get('mismatched', 0)}")
    logger.info(f"  Corrected: {reconciliation_result.get('corrected', 0)}")
    logger.info(f"  Errors: {reconciliation_result.get('errors', 0)}")
    
    # Show updated positions
    positions = await shadow_manager.get_all_positions()
    logger.info(f"\nFinal positions after reconciliation:")
    for pos in positions:
        logger.info(f"  {pos.get('exchange_id', 'unknown')}:{pos.get('symbol', 'unknown')} - {pos.get('quantity', 0.0)} @ {pos.get('price', 0.0)}")
    
    # Show position history
    history = await shadow_manager.get_position_history("exchange1", "BTC/USDT", limit=5)
    logger.info(f"\nPosition history for exchange1:BTC/USDT:")
    for entry in history:
        logger.info(f"  {entry.get('timestamp', 'unknown')} - From {entry.get('old_quantity', 0.0)} @ {entry.get('old_price', 0.0)} to {entry.get('new_quantity', 0.0)} @ {entry.get('new_price', 0.0)} (Source: {entry.get('source', 'unknown')})")
    
    # Cleanup
    await shadow_manager.stop()
    logger.info("Shadow accounting demo completed")
    return shadow_manager


async def demo_rabbitmq(message_count=5):
    """Demonstrate RabbitMQ messaging with reliability features"""
    logger.info("\n=== DEMO: RabbitMQ Messaging ===")
    
    # Check if RabbitMQ is available
    try:
        import aiohttp
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get("http://localhost:15672", timeout=2) as resp:
                    rabbitmq_available = resp.status == 200
            except:
                rabbitmq_available = False
    except:
        rabbitmq_available = False
    
    if not rabbitmq_available:
        logger.warning("RabbitMQ is not available. This demo will be skipped.")
        logger.warning("To enable this demo, install RabbitMQ and make sure it's running.")
        return None
    
    # RabbitMQ configuration
    config = {
        "host": "localhost",
        "port": 5672,
        "username": "guest",
        "password": "guest",
        "vhost": "/",
        "connection_pool_size": 2,
        "channel_pool_size": 10,
        "heartbeat": 60
    }
    
    # Initialize RabbitMQ adapter
    logger.info("Initializing RabbitMQ adapter...")
    adapter = RabbitMQAdapter(config)
    await adapter.initialize()
    
    # Declare exchanges and queues
    await adapter.declare_exchange("market_data", ExchangeType.TOPIC)
    await adapter.declare_queue("price_data")
    await adapter.bind_queue("price_data", "market_data", "price.#")
    
    # Consume messages
    received_messages = []
    
    async def process_message(message):
        body = json.loads(message.body.decode())
        received_messages.append(body)
        logger.info(f"Received message: {body['symbol']} @ {body['price']}")
        
    await adapter.consume("price_data", process_message)
    
    # Publish messages with reliability
    logger.info(f"\nPublishing {message_count} messages with reliability...")
    symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "ADA/USDT", "DOT/USDT"]
    
    for i in range(message_count):
        symbol = random.choice(symbols)
        price = round(random.uniform(100, 50000), 2)
        
        message = {
            "symbol": symbol,
            "price": price,
            "timestamp": int(time.time() * 1000),
            "sequence": i + 1
        }
        
        # Publish with retry and circuit breaker
        try:
            await adapter.publish(
                "market_data", 
                f"price.{symbol.replace('/', '.')}",
                message,
                priority=MessagePriority.NORMAL
            )
            logger.info(f"Published: {symbol} @ {price}")
        except Exception as e:
            logger.error(f"Failed to publish message: {e}")
        
        await asyncio.sleep(0.5)
    
    # Wait for messages to be processed
    logger.info("\nWaiting for message processing...")
    await asyncio.sleep(3)
    
    # Display results
    logger.info(f"\nReceived {len(received_messages)} of {message_count} messages")
    
    # Cleanup
    await adapter.shutdown()
    logger.info("RabbitMQ messaging demo completed")
    return adapter


async def main():
    """Run the high reliability demo"""
    logger.info("\n==================================")
    logger.info("HIGH RELIABILITY TRADING SYSTEM DEMO")
    logger.info("==================================\n")
    
    logger.info("This demo showcases the high-reliability components of our trading system:")
    logger.info("1. Connection Pool - Resilient API connections with automatic retry and circuit breaker")
    logger.info("2. Transaction Verification - Three-phase commit protocol for guaranteed trade execution")
    logger.info("3. Shadow Accounting - Position reconciliation and discrepancy detection")
    logger.info("4. RabbitMQ Messaging - Reliable message delivery with connection pooling and publisher confirms")
    
    # Run demos
    try:
        from importlib.metadata import version
        versions = {
            "Python": sys.version.split()[0],
            "aiohttp": version("aiohttp"),
            "pika": version("pika"),
            "aio_pika": version("aio_pika"),
            "pybreaker": version("pybreaker"),
        }
        logger.info(f"\nVersions: {versions}")
    except:
        pass
    
    await demo_connection_pool()
    await demo_transaction_verification()
    await demo_shadow_accounting()
    
    try:
        await demo_rabbitmq()
    except Exception as e:
        logger.error(f"RabbitMQ demo failed: {e}")
    
    logger.info("\n==================================")
    logger.info("HIGH RELIABILITY DEMO COMPLETED")
    logger.info("==================================\n")


if __name__ == "__main__":
    # Import here to avoid circular imports
    from trading_system.messaging.rabbitmq_adapter import MessagePriority, ExchangeType
    
    # Run the demo
    asyncio.run(main()) 