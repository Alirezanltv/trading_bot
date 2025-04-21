"""
Nobitex Execution Engine Example

This example demonstrates how to use the NobitexExecutionEngine to place orders
and manage the three-phase commit process.
"""

import os
import sys
import time
import uuid
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

# Add the repository root to the Python path
script_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(os.path.dirname(script_dir)))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from trading_system.core.logging import initialize_logging, get_logger
from trading_system.execution.nobitex_engine import NobitexExecutionEngine, ExecutionResult
from trading_system.execution.orders import Order, OrderSide, OrderType, OrderTimeInForce

# Configure logging
logs_dir = Path("logs")
logs_dir.mkdir(exist_ok=True)
initialize_logging(log_level=logging.INFO, log_file=logs_dir / "execution_example.log")
logger = get_logger("execution_example")


def create_sample_config() -> Dict[str, Any]:
    """Create sample configuration for the execution engine."""
    return {
        "execution_engine": {
            "exchange_adapter": "nobitex",
            "max_retries": 3,
            "retry_delay": 2.0,
            "order_update_interval": 5.0,
            "transaction_log_path": "data/transactions",
            "circuit_breaker_enabled": True,
            "failure_threshold": 3,
            "circuit_open_timeout": 300,
        },
        "exchange": {
            "api_key": os.getenv("NOBITEX_API_KEY"),
            "api_secret": os.getenv("NOBITEX_API_SECRET"),
            "timeout": 15,
            "max_retries": 3
        }
    }


def create_market_buy_order(symbol: str, quantity: float) -> Order:
    """Create a sample market buy order."""
    order_id = str(uuid.uuid4())
    order = Order(
        order_id=order_id,
        symbol=symbol,
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=quantity,
        price=None,  # Market order
        time_in_force=OrderTimeInForce.IOC,
        timestamp=int(time.time() * 1000)
    )
    return order


def create_limit_sell_order(symbol: str, quantity: float, price: float) -> Order:
    """Create a sample limit sell order."""
    order_id = str(uuid.uuid4())
    order = Order(
        order_id=order_id,
        symbol=symbol,
        side=OrderSide.SELL,
        order_type=OrderType.LIMIT,
        quantity=quantity,
        price=price,
        time_in_force=OrderTimeInForce.GTC,
        timestamp=int(time.time() * 1000)
    )
    return order


def display_execution_result(result: ExecutionResult) -> None:
    """Display execution result details."""
    print("\n--- Execution Result ---")
    print(f"Success: {result.success}")
    print(f"Order ID: {result.order_id}")
    print(f"Exchange Order ID: {result.exchange_order_id}")
    
    if not result.success:
        print(f"Error Message: {result.error_message}")
        print(f"Failed Phase: {result.phase.value if result.phase else 'Unknown'}")
    
    print(f"Retry Count: {result.retry_count}")
    print(f"Execution Time: {result.execution_time:.3f} seconds")
    print(f"Timestamp: {result.timestamp}")
    print("------------------------\n")


def track_order_status(engine: NobitexExecutionEngine, order_id: str, timeout: int = 60) -> None:
    """Track order status until it reaches a final state or timeout."""
    print(f"\nTracking order {order_id}...")
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        order = engine.get_order(order_id)
        if not order:
            print(f"Order {order_id} not found")
            return
        
        print(f"Order Status: {order.status.value}")
        
        # Check if order is in a final state
        if order.status in [
            OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.EXPIRED
        ]:
            print(f"Order reached final state: {order.status.value}")
            
            # Display fills if any
            if order.fills:
                print("\nFills:")
                for i, fill in enumerate(order.fills):
                    print(f"  Fill {i+1}: {fill.quantity} @ {fill.price} ({fill.timestamp})")
            
            return
        
        # Wait before checking again
        time.sleep(5)
    
    print(f"Tracking timeout after {timeout} seconds. Last status: {order.status.value}")


def main() -> None:
    """Run the execution engine example."""
    logger.info("Starting Nobitex execution engine example")
    
    try:
        # Create configuration
        config = create_sample_config()
        
        # Initialize execution engine
        logger.info("Initializing execution engine")
        engine = NobitexExecutionEngine(config)
        
        # Start the engine
        logger.info("Starting execution engine")
        engine.start()
        
        # Display engine status
        stats = engine.get_stats()
        print(f"\nExecution Engine Status:")
        print(f"Active Orders: {stats['active_orders']}")
        print(f"Total Orders: {stats['total_orders']}")
        print(f"Circuit Breaker: {stats['circuit_breaker_status']}")
        
        # Ask for user input to place orders
        print("\n--- Order Placement Menu ---")
        print("1. Place market buy order")
        print("2. Place limit sell order")
        print("3. Exit")
        
        choice = input("\nSelect option (1-3): ")
        
        if choice == "1":
            symbol = input("Enter symbol (e.g., btc-usdt): ")
            quantity = float(input("Enter quantity: "))
            
            # Create and submit order
            order = create_market_buy_order(symbol, quantity)
            print(f"\nSubmitting market buy order for {quantity} {symbol.split('-')[0]}...")
            
            # Execute order
            result = engine.execute_order(order)
            display_execution_result(result)
            
            # Track order status if successfully submitted
            if result.success:
                track_order_status(engine, order.order_id)
                
        elif choice == "2":
            symbol = input("Enter symbol (e.g., btc-usdt): ")
            quantity = float(input("Enter quantity: "))
            price = float(input("Enter price: "))
            
            # Create and submit order
            order = create_limit_sell_order(symbol, quantity, price)
            print(f"\nSubmitting limit sell order for {quantity} {symbol.split('-')[0]} at {price}...")
            
            # Execute order
            result = engine.execute_order(order)
            display_execution_result(result)
            
            # Track order status if successfully submitted
            if result.success:
                track_order_status(engine, order.order_id)
        
        # Stop the engine
        logger.info("Stopping execution engine")
        engine.stop()
        
        logger.info("Execution engine example completed")
        
    except Exception as e:
        logger.exception(f"Error in execution engine example: {str(e)}")
    
    finally:
        print("\nExample completed. Check logs for details.")


if __name__ == "__main__":
    main() 