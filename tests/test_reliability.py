#!/usr/bin/env python
"""
High Reliability Trading Components Test Suite

This script tests the high-reliability components of the trading system:
- Connection Pool for exchange API resilience
- Transaction Verification with three-phase commit
- Shadow Accounting with position reconciliation
- TradingView Webhook Handler for market data

Run with: py -3.10 trading_system/tests/test_reliability.py
"""

import os
import json
import asyncio
import logging
import sys
import random
import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger("reliability_test")

# Add parent directory to path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from trading_system.exchange.connection_pool import ConnectionPool
from trading_system.exchange.transaction_verification import (
    TransactionVerificationPipeline, Transaction, 
    TransactionPhase, TransactionStatus, TransactionError
)
from trading_system.position.shadow_accounting import (
    ShadowPositionManager, PositionSource, ReconciliationStatus
)
from trading_system.market_data.tradingview_webhook_handler import (
    TradingViewWebhookHandler, AlertType
)
from trading_system.core.message_bus import MessageBus

# Mock classes for testing
class MockExchangeClient:
    """Mock exchange client for testing"""
    
    def __init__(self, exchange_id: str = "mock_exchange"):
        self.exchange_id = exchange_id
        self.positions = {}
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
    """Simple order class for testing"""
    
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

async def test_connection_pool():
    """Test the connection pool with simulated failures"""
    logger.info("=== Testing Connection Pool ===")
    
    def create_client():
        return MockExchangeClient()
    
    # Create connection pool
    config = {
        "min_connections": 2,
        "max_connections": 5,
        "max_requests_per_window": 10,
        "window_seconds": 1
    }
    
    pool = ConnectionPool("MockExchange", create_client, config)
    
    await pool.initialize()
    logger.info("Connection pool initialized")
    
    # Test basic functionality
    logger.info("Testing basic requests...")
    for i in range(5):
        try:
            result = await pool.execute("get_positions")
            logger.info(f"Request {i+1} succeeded: {len(result)} positions")
        except Exception as e:
            logger.error(f"Request {i+1} failed: {e}")
    
    # Test connection failures and automatic reconnection
    logger.info("Testing connection failures and recovery...")
    
    # Simulate failures in all connections
    for conn_id, conn in pool.connections.items():
        if hasattr(conn.client, "simulate_connection_errors"):
            conn.client.simulate_connection_errors(2)
    
    # Execute requests and observe recovery
    for i in range(10):
        try:
            result = await pool.execute("get_positions")
            logger.info(f"Request {i+1} succeeded after failures: {len(result)} positions")
        except Exception as e:
            logger.info(f"Expected error during recovery: {e}")
        
        await asyncio.sleep(0.5)
    
    # Test rate limiting
    logger.info("Testing rate limiting...")
    
    # Set very low rate limits to test throttling
    pool.max_requests_per_window = 3
    pool.window_seconds = 2
    
    # Send many requests quickly
    tasks = []
    for i in range(10):
        tasks.append(pool.execute("get_positions"))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    success_count = sum(1 for r in results if not isinstance(r, Exception))
    error_count = sum(1 for r in results if isinstance(r, Exception))
    
    logger.info(f"Rate limiting test: {success_count} succeeded, {error_count} throttled/failed")
    
    # Get pool statistics
    stats = pool.get_stats()
    logger.info(f"Connection pool stats: {json.dumps(stats, indent=2)}")
    
    await pool.stop()
    logger.info("Connection pool stopped")
    return True

async def test_transaction_verification():
    """Test the transaction verification pipeline"""
    logger.info("=== Testing Transaction Verification ===")
    
    # Create mock exchange client
    exchange_client = MockExchangeClient()
    
    # Configure transaction verification pipeline
    config = {
        "verification_interval": 1,  # 1 second
        "reconciliation_interval": 5,  # 5 seconds
        "max_verification_attempts": 3,
        "storage_path": "data/transactions",
        "save_interval": 10  # 10 seconds
    }
    
    pipeline = TransactionVerificationPipeline(config)
    
    # Define handlers for the transaction pipeline
    async def validate_handler(order):
        """Validate the order before submission"""
        logger.info(f"Validating order: {order.to_dict()}")
        
        # Simulate validation checks
        if order.price <= 0 or order.quantity <= 0:
            return {
                "is_valid": False,
                "error": "Invalid price or quantity"
            }
        
        return {
            "is_valid": True,
            "data": {"pre_validation": "passed"}
        }
    
    async def submit_handler(order):
        """Submit the order to the exchange"""
        logger.info(f"Submitting order: {order.to_dict()}")
        
        try:
            # Submit to exchange
            result = await exchange_client.create_order(
                order.symbol,
                order.side,
                order.quantity,
                order.price
            )
            
            return {
                "success": True,
                "order_id": result["order_id"],
                "data": result
            }
        except Exception as e:
            logger.error(f"Order submission error: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def verify_handler(order, transaction_data):
        """Verify the order execution"""
        logger.info(f"Verifying order: {order.order_id}")
        
        try:
            # Get order from exchange
            order_id = transaction_data.get("order_id")
            if not order_id:
                return {
                    "is_verified": False,
                    "error": "No order ID in transaction data"
                }
            
            order_result = await exchange_client.get_order(order_id)
            
            if not order_result:
                return {
                    "is_verified": False,
                    "error": "Order not found on exchange"
                }
            
            if order_result["status"] in ["filled", "closed", "executed", "success"]:
                return {
                    "is_verified": True,
                    "data": order_result
                }
            elif order_result["status"] in ["cancelled", "rejected"]:
                return {
                    "is_verified": False,
                    "error": f"Order is {order_result['status']}",
                    "data": order_result
                }
            else:
                # Still pending
                return {
                    "is_verified": False,
                    "retry": True,
                    "data": order_result
                }
        except Exception as e:
            logger.error(f"Order verification error: {e}")
            return {
                "is_verified": False,
                "retry": True,
                "error": str(e)
            }
    
    async def reconcile_handler(order, transaction_data):
        """Reconcile the order with position data"""
        logger.info(f"Reconciling order: {order.order_id}")
        
        try:
            # Check positions match expected
            positions = await exchange_client.get_positions()
            
            # Find position for this symbol
            position = next((p for p in positions if p["symbol"] == order.symbol), None)
            
            # Simple reconciliation logic
            if position:
                return {
                    "is_reconciled": True,
                    "data": {
                        "position": position,
                        "expected_side": order.side,
                        "expected_quantity": order.quantity,
                        "expected_price": order.price
                    }
                }
            else:
                return {
                    "is_reconciled": False,
                    "error": "Position not found after order execution",
                    "data": {"positions": positions}
                }
        except Exception as e:
            logger.error(f"Order reconciliation error: {e}")
            return {
                "is_reconciled": False,
                "error": str(e)
            }
    
    async def cancel_handler(order, transaction_data):
        """Cancel the order if needed"""
        logger.info(f"Cancelling order: {order.order_id}")
        
        try:
            # Get order ID from transaction data
            order_id = transaction_data.get("order_id")
            if not order_id:
                return {
                    "success": False,
                    "error": "No order ID in transaction data"
                }
            
            # Cancel on exchange
            result = await exchange_client.cancel_order(order_id)
            
            return {
                "success": result["status"] == "cancelled",
                "data": result
            }
        except Exception as e:
            logger.error(f"Order cancellation error: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    # Register handlers
    pipeline.register_handlers(
        validate_func=validate_handler,
        submit_func=submit_handler,
        verify_func=verify_handler,
        reconcile_func=reconcile_handler,
        cancel_func=cancel_handler
    )
    
    # Initialize the pipeline
    await pipeline.initialize()
    logger.info("Transaction verification pipeline initialized")
    
    # Test successful transaction flow
    logger.info("Testing successful transaction flow...")
    test_order = Order("BTC/USDT", "buy", 0.1, 35000.0)
    
    # Set order as filled immediately for this test
    async def modify_order_status(order_id, status, delay=2):
        await asyncio.sleep(delay)
        if order_id in exchange_client.orders:
            exchange_client.orders[order_id]["status"] = status
    
    # Process the transaction
    transaction_result = await pipeline.execute_order(test_order)
    
    # Mark the order as filled after a delay
    order_id = transaction_result.get("order_id")
    if order_id:
        asyncio.create_task(modify_order_status(order_id, "filled", 2))
    
    logger.info(f"Transaction started: {transaction_result}")
    
    # Wait for the transaction to be verified
    for _ in range(10):
        await asyncio.sleep(1)
        transaction = await pipeline.get_transaction(transaction_result["transaction_id"])
        if transaction and transaction["status"] in [TransactionStatus.COMPLETED.value, TransactionStatus.FAILED.value]:
            break
    
    logger.info(f"Final transaction state: {transaction}")
    
    # Test failed verification with retry
    logger.info("Testing transaction with verification failure and retry...")
    test_order2 = Order("ETH/USDT", "sell", 1.0, 2000.0)
    
    # Simulate connection errors during verification
    exchange_client.simulate_connection_errors(2)
    
    # Process the transaction
    try:
        transaction_result2 = await pipeline.execute_order(test_order2)
        logger.info(f"Transaction with retry started: {transaction_result2}")

        # Mark the order as filled after delays and errors
        order_id2 = transaction_result2.get("order_id")
        if order_id2:
            asyncio.create_task(modify_order_status(order_id2, "filled", 4))
        
        # Wait for the transaction to be verified
        for _ in range(10):
            await asyncio.sleep(1)
            transaction2 = await pipeline.get_transaction(transaction_result2["transaction_id"])
            if transaction2 and transaction2["status"] in [TransactionStatus.COMPLETED.value, TransactionStatus.FAILED.value]:
                break
            
        logger.info(f"Final transaction state after retry: {transaction2}")
    except TransactionError as e:
        # This is expected due to simulated connection errors
        logger.info(f"Expected transaction error during submission with connection errors: {str(e)}")
    
    # Get all transactions
    all_transactions = await pipeline.get_transactions()
    logger.info(f"All transactions: {len(all_transactions)}")
    
    await pipeline.stop()
    logger.info("Transaction verification pipeline stopped")
    return True

async def test_shadow_accounting():
    """Test the shadow accounting system"""
    logger.info("=== Testing Shadow Accounting ===")
    
    # Create mock exchange clients
    exchange1 = MockExchangeClient("exchange1")
    exchange2 = MockExchangeClient("exchange2")
    
    # Prepare test data - add some positions directly to the mock exchanges
    exchange1.positions = {
        "BTC/USDT": {"quantity": 0.5, "price": 35000.0},
        "ETH/USDT": {"quantity": 5.0, "price": 2000.0}
    }
    
    exchange2.positions = {
        "BTC/USDT": {"quantity": 0.2, "price": 36000.0},
        "SOL/USDT": {"quantity": 20.0, "price": 100.0}
    }
    
    # Configure shadow accounting system
    config = {
        "db_path": "data/test_positions.db",
        "reconciliation_interval": 5,  # 5 seconds
        "tolerance": 0.001,
        "max_history": 100,
        "auto_correct": True
    }
    
    # Initialize shadow accounting
    shadow_manager = ShadowPositionManager(config)
    await shadow_manager.initialize()
    logger.info("Shadow position manager initialized")
    
    # Register exchange clients
    await shadow_manager.register_exchange_client("exchange1", exchange1)
    await shadow_manager.register_exchange_client("exchange2", exchange2)
    
    # Add some positions manually with slight discrepancies
    logger.info("Adding manual positions...")
    await shadow_manager.update_position(
        exchange_id="exchange1",
        symbol="BTC/USDT",
        quantity=0.501,  # Slight discrepancy
        price=35000.0,
        source=PositionSource.INTERNAL
    )
    
    await shadow_manager.update_position(
        exchange_id="exchange1",
        symbol="ETH/USDT",
        quantity=5.0,
        price=2001.0,  # Slight discrepancy
        source=PositionSource.INTERNAL
    )
    
    await shadow_manager.update_position(
        exchange_id="exchange2",
        symbol="BTC/USDT",
        quantity=0.2,
        price=36000.0,
        source=PositionSource.INTERNAL
    )
    
    # Missing SOL/USDT position for exchange2
    
    # Get all positions
    positions = await shadow_manager.get_all_positions()
    logger.info(f"Current positions: {len(positions)}")
    for pos in positions:
        logger.info(f"Position: {pos['exchange_id']}:{pos['symbol']} - {pos['quantity']} @ {pos['price']}")
    
    # Run reconciliation
    logger.info("Running position reconciliation...")
    reconciliation_result = await shadow_manager.reconcile_positions()
    
    logger.info(f"Reconciliation results: {reconciliation_result['matched']} matched, " +
                f"{reconciliation_result['mismatched']} mismatched, " +
                f"{reconciliation_result['corrected']} corrected")
    
    # Create a position history
    logger.info("Creating position history...")
    
    # Simulate some trades
    for i in range(5):
        # Update BTC position with small changes
        quantity = 0.5 + random.uniform(-0.05, 0.05)
        price = 35000.0 + random.uniform(-1000, 1000)
        
        await shadow_manager.update_position(
            exchange_id="exchange1",
            symbol="BTC/USDT",
            quantity=quantity,
            price=price,
            source=PositionSource.INTERNAL,
            reference_id=f"trade_{i+1}",
            metadata={"trade_id": f"trade_{i+1}"}
        )
        
        await asyncio.sleep(0.1)
    
    # Get position history
    history = await shadow_manager.get_position_history("exchange1", "BTC/USDT", 10)
    logger.info(f"Position history entries: {len(history)}")
    for entry in history[:3]:  # Show first 3 entries
        logger.info(f"History: {entry['timestamp']} - " +
                    f"From {entry['previous_quantity']} @ {entry['previous_price']} " +
                    f"to {entry['new_quantity']} @ {entry['new_price']}")
    
    # Get reconciliation history
    recon_history = await shadow_manager.get_reconciliation_history(5)
    logger.info(f"Reconciliation history entries: {len(recon_history)}")
    
    # Test reconnecting an exchange client
    logger.info("Testing exchange client update...")
    new_exchange1 = MockExchangeClient("exchange1")
    new_exchange1.positions = {
        "BTC/USDT": {"quantity": 0.6, "price": 34000.0},  # Changed position
        "ETH/USDT": {"quantity": 5.0, "price": 2000.0}
    }
    
    await shadow_manager.unregister_exchange_client("exchange1")
    await shadow_manager.register_exchange_client("exchange1", new_exchange1)
    
    # Run reconciliation again
    reconciliation_result2 = await shadow_manager.reconcile_positions()
    
    logger.info(f"Reconciliation after client update: {reconciliation_result2['matched']} matched, " +
                f"{reconciliation_result2['mismatched']} mismatched, " +
                f"{reconciliation_result2['corrected']} corrected")
    
    # Final positions
    final_positions = await shadow_manager.get_all_positions()
    logger.info(f"Final positions: {len(final_positions)}")
    for pos in final_positions:
        logger.info(f"Final position: {pos['exchange_id']}:{pos['symbol']} - {pos['quantity']} @ {pos['price']}")
    
    await shadow_manager.stop()
    logger.info("Shadow position manager stopped")
    return True

async def test_tradingview_webhook():
    """Test the TradingView webhook handler"""
    logger.info("=== Testing TradingView Webhook Handler ===")
    
    # Create message bus for distributing signals
    message_bus = MessageBus()
    
    # Setup message receiver
    received_signals = []
    
    async def handle_signal(message):
        topic = message.get('topic', 'unknown')
        signal = message.get('data', {})
        logger.info(f"Received signal on topic {topic}: {signal.get('ticker')}")
        received_signals.append(signal)
    
    await message_bus.subscribe("tradingview.signal", handle_signal)
    
    # Configure webhook handler
    config = {
        "port": 8888,
        "host": "localhost",
        "endpoint": "/webhook/tradingview",
        "secret_key": "test_secret_key",
        "require_signature": False,  # For testing
        "max_history_size": 100
    }
    
    webhook_handler = TradingViewWebhookHandler(config, message_bus)
    
    # Initialize webhook handler
    await webhook_handler.initialize()
    logger.info(f"TradingView webhook handler initialized at http://{config['host']}:{config['port']}{config['endpoint']}")
    
    # Register custom signal processor
    strategy_signals = []
    
    async def process_strategy_signal(signal):
        logger.info(f"Processing strategy signal: {signal.get('ticker')} - {signal.get('position')}")
        strategy_signals.append(signal)
        return True
    
    await webhook_handler.register_signal_processor(AlertType.STRATEGY, process_strategy_signal)
    
    # Simulate sending webhook data
    logger.info("Simulating webhook data...")
    
    # Helper function to send test alerts
    async def send_test_alert(alert_data):
        url = f"http://{config['host']}:{config['port']}{config['endpoint']}"
        
        headers = {
            "Content-Type": "application/json"
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=alert_data, headers=headers) as response:
                    status = response.status
                    response_text = await response.text()
                    logger.info(f"Webhook response: {status} - {response_text}")
                    return status == 200
        except Exception as e:
            logger.error(f"Error sending test alert: {e}")
            return False
    
    # We need to import aiohttp here for the test
    import aiohttp
    
    # Test alerts for different types
    test_alerts = [
        # Strategy alert
        {
            "ticker": "BTC/USDT",
            "exchange": "BINANCE",
            "message": "BTC/USDT: Strategy signals LONG entry",
            "price": 35000.0,
            "timeframe": "1h",
            "position": "long",
            "alert_type": "strategy"
        },
        # Price alert
        {
            "ticker": "ETH/USDT",
            "exchange": "BINANCE",
            "message": "ETH/USDT price above 2000",
            "price": 2000.0,
            "timeframe": "15m",
            "alert_type": "price"
        },
        # Indicator alert
        {
            "ticker": "SOL/USDT",
            "exchange": "BINANCE",
            "message": "SOL/USDT: RSI oversold (28.5)",
            "indicator": {
                "name": "RSI",
                "value": 28.5
            },
            "price": 100.0,
            "timeframe": "1d",
            "alert_type": "indicator"
        },
        # Invalid alert (missing ticker)
        {
            "message": "Invalid alert without ticker",
            "price": 100.0
        }
    ]
    
    # Send test alerts
    for alert in test_alerts:
        logger.info(f"Sending test alert for {alert.get('ticker', 'unknown')}...")
        success = await send_test_alert(alert)
        if success:
            logger.info("Alert sent successfully")
        else:
            logger.warning("Failed to send alert")
        
        # Give some time for processing
        await asyncio.sleep(0.5)
    
    # Check results
    logger.info(f"Strategy signals received: {len(strategy_signals)}")
    logger.info(f"All signals received via message bus: {len(received_signals)}")
    
    # Get signal history
    signal_history = webhook_handler.get_signal_history(10)
    logger.info(f"Signal history entries: {len(signal_history)}")
    
    # Get stats
    stats = webhook_handler.get_stats()
    logger.info(f"Webhook handler stats: valid={stats['valid_alerts']}, invalid={stats['invalid_alerts']}, errors={stats['error_alerts']}")
    
    # Stop components
    await webhook_handler.stop()
    
    logger.info("TradingView webhook handler stopped")
    return True

async def main():
    """Run all tests"""
    logger.info("Starting high reliability components test suite")
    
    # Create necessary directories
    os.makedirs("data", exist_ok=True)
    os.makedirs("data/transactions", exist_ok=True)
    
    # Run tests
    try:
        # Test connection pool
        await test_connection_pool()
        
        # Test transaction verification
        await test_transaction_verification()
        
        # Test shadow accounting
        await test_shadow_accounting()
        
        # Test TradingView webhook
        await test_tradingview_webhook()
        
        logger.info("All tests completed successfully!")
        
    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
        return False
    
    return True

if __name__ == "__main__":
    if sys.version_info < (3, 10):
        logger.error("This script requires Python 3.10 or higher")
        sys.exit(1)
        
    logger.info(f"Running with Python {sys.version}")
    asyncio.run(main()) 