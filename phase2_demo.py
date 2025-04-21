#!/usr/bin/env python3
"""
Phase 2 Demo

This script demonstrates the core Phase 2 components working together:
- Message reliability with RabbitMQ
- Transaction coordination with three-phase commit
- Partial fill handling
"""

import os
import sys
import time
import uuid
import asyncio
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# Add parent directory to path if needed
if os.path.basename(os.getcwd()) == "trading_system":
    sys.path.insert(0, os.path.abspath(".."))
else:
    sys.path.insert(0, os.path.abspath("."))

# Import components
from trading_system.messaging.message_reliability import (
    MessageStatus, RetryStrategy, MessageTracker
)
from trading_system.execution.transaction_persistence import TransactionStore
from trading_system.execution.transaction_coordinator import TransactionCoordinator
from trading_system.execution.transaction import (
    TransactionContext, TransactionStatus, TransactionPhase, TransactionType
)
from trading_system.execution.partial_fill_handler import (
    FillStrategy, PartialFillHandler
)
from trading_system.exchange.base import OrderStatus, OrderType, OrderSide, Order
from trading_system.execution.orders import OrderFill

# Mock components similar to test file
from trading_system.test_phase2 import MockExchangeClient, MockExchangeManager

# Create logger
logger = logging.getLogger("phase2_demo")

async def run_demo():
    """Run the Phase 2 demo."""
    logger.info("Starting Phase 2 Demo")
    
    # Initialize components
    message_tracker = MessageTracker()
    transaction_store = TransactionStore(db_path=":memory:")
    
    # Configure transaction coordinator
    config = {
        "transaction_db_path": ":memory:",
        "transaction_timeout": 30000,  # 30 seconds
        "verify_interval": 1000,  # 1 second
        "recovery_interval": 5,  # 5 seconds
        "cleanup_interval": 60  # 60 seconds
    }
    coordinator = TransactionCoordinator(config)
    
    # Create and configure mock exchange components
    exchange_client = MockExchangeClient()
    exchange_manager = MockExchangeManager()
    
    # Configure partial fill handler
    fill_config = {
        "default_fill_strategy": "wait_for_complete",
        "max_retries": 3,
        "retry_interval_ms": 1000,
        "max_wait_time_ms": 10000,
        "check_interval_ms": 1000
    }
    fill_handler = PartialFillHandler(fill_config)
    fill_handler.register_exchange_client(exchange_client)
    
    # Register exchange manager with transaction coordinator
    coordinator.register_participant("exchange_manager", exchange_manager)
    
    # Start components
    await coordinator.start()
    await fill_handler.start()
    
    try:
        # Demo 1: Message Tracking
        logger.info("\n=== MESSAGE TRACKING DEMO ===")
        
        # Track messages with different priorities
        message_ids = []
        for i in range(3):
            message_id = f"order-{uuid.uuid4()}"
            message_ids.append(message_id)
            
            # Create and track message
            message_data = {
                "order_type": "LIMIT_BUY" if i % 2 == 0 else "LIMIT_SELL",
                "symbol": "BTC/USD",
                "amount": 0.1 * (i + 1),
                "price": 50000 + (i * 1000),
                "timestamp": int(time.time() * 1000)
            }
            
            record = message_tracker.track_message(
                message_id=message_id,
                exchange="orders",
                routing_key=f"orders.{message_data['order_type'].lower()}",
                message_data=message_data
            )
            
            logger.info(f"Tracked message: {message_id} with data: {message_data}")
        
        # Acknowledge some messages
        for i, message_id in enumerate(message_ids):
            if i == 0:
                # First message succeeded
                message_tracker.update_status(message_id, MessageStatus.ACKNOWLEDGED)
                logger.info(f"Message {message_id} acknowledged")
            elif i == 1:
                # Second message failed
                message_tracker.update_status(message_id, MessageStatus.FAILED)
                logger.info(f"Message {message_id} failed")
            # Third message remains pending
        
        # Print message tracking stats
        stats = message_tracker.get_stats()
        logger.info(f"Message tracker stats: {stats}")
        
        # Show unacknowledged messages
        unack = message_tracker.get_unacknowledged_messages()
        logger.info(f"Unacknowledged messages: {len(unack)}")
        for msg in unack:
            logger.info(f"  - {msg['id']} ({msg['routing_key']})")
        
        # Demo 2: Transaction Processing
        logger.info("\n=== TRANSACTION COORDINATION DEMO ===")
        
        # Create and execute a successful transaction
        transaction = await coordinator.create_transaction(
            type_=TransactionType.LIMIT_BUY,
            symbol="ETH/USD",
            amount=2.0,
            price=1500.0,
            exchange="test-exchange",
            metadata={"source": "demo", "user_id": "demo-user"}
        )
        
        logger.info(f"Created transaction: {transaction.id}")
        logger.info(f"Transaction details: {transaction.to_dict()}")
        
        # Execute the transaction
        result = await coordinator.execute_transaction(transaction)
        logger.info(f"Transaction execution result: {result}")
        
        # Get transaction logs
        logs = await coordinator.get_transaction_logs(transaction.id)
        logger.info(f"Transaction logs: {len(logs)} entries")
        for log in logs:
            logger.info(f"  - {log['timestamp']}: {log['status']} ({log['phase']})")
        
        # Demo 3: Partial Fill Handling
        logger.info("\n=== PARTIAL FILL HANDLING DEMO ===")
        
        # Create an order and simulate a partial fill
        order = await exchange_client.create_order(
            symbol="BTC/USD",
            order_type=OrderType.LIMIT,
            side=OrderSide.BUY,
            amount=1.0,
            price=50000.0
        )
        
        logger.info(f"Created order: {order.order_id}")
        
        # Simulate a partial fill (60%)
        fill = exchange_client.simulate_partial_fill(
            order_id=order.order_id,
            fill_amount=0.6,
            fill_price=50001.0  # Slight slippage
        )
        
        # Handle the partial fill with different strategies
        logger.info("Testing different partial fill strategies:")
        
        # 1. Accept Partial
        await fill_handler.handle_partial_fill(
            order=order, 
            fill=fill, 
            strategy=FillStrategy.ACCEPT_PARTIAL
        )
        logger.info("ACCEPT_PARTIAL strategy demonstrated")
        
        # Create another order for retry strategy
        order2 = await exchange_client.create_order(
            symbol="ETH/USD",
            order_type=OrderType.LIMIT,
            side=OrderSide.BUY,
            amount=2.0,
            price=1500.0
        )
        
        # Simulate a partial fill (50%)
        fill2 = exchange_client.simulate_partial_fill(
            order_id=order2.order_id,
            fill_amount=1.0,
            fill_price=1502.0  # Slight slippage
        )
        
        # 2. Retry Remaining
        await fill_handler.handle_partial_fill(
            order=order2, 
            fill=fill2, 
            strategy=FillStrategy.RETRY_REMAINING
        )
        logger.info("RETRY_REMAINING strategy demonstrated")
        
        # Wait a moment for strategies to process
        await asyncio.sleep(3)
        
        # Get handler stats
        stats = fill_handler.get_stats()
        logger.info(f"Partial fill handler stats: {stats}")
        
        # Get active partial orders
        partial_orders = await fill_handler.get_partial_orders()
        logger.info(f"Active partial orders: {len(partial_orders)}")
        
    finally:
        # Clean up
        await coordinator.stop()
        await fill_handler.stop()
        await message_tracker.stop()
        await transaction_store.close()
    
    logger.info("Phase 2 Demo completed successfully")

def main():
    """Main entry point."""
    try:
        # Run the demo
        asyncio.run(run_demo())
        
    except Exception as e:
        logger.error(f"Demo failed: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main() 