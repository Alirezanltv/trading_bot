"""
Execution Engine Test Script

This script tests the functionality of the execution engine components.
It performs simple validation and configuration checks without placing real orders.
"""

import os
import sys
import time
import uuid
import logging
from pathlib import Path
from typing import Dict, Any

# Add the repository root to the Python path
script_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(script_dir))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from dotenv import load_dotenv
from trading_system.core.logging import get_logger
from trading_system.execution.nobitex_engine import NobitexExecutionEngine
from trading_system.execution.orders import Order, OrderSide, OrderType, OrderTimeInForce, OrderStatus, OrderFill
from trading_system.utils.api_key_manager import ApiKeyManager, get_credentials, verify_credentials

# Load environment variables
load_dotenv()

# Configure logging
logs_dir = Path("logs")
logs_dir.mkdir(exist_ok=True)
# Configure basic logging directly
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(logs_dir / "execution_test.log")
    ]
)
logger = get_logger("execution_test")


def test_api_key_manager():
    """Test the API key manager component."""
    print("\n--- Testing API Key Manager ---")
    
    # Initialize key manager
    key_manager = ApiKeyManager()
    
    # Check if Nobitex credentials are available
    has_creds = verify_credentials("nobitex")
    print(f"Nobitex credentials available: {has_creds}")
    
    if has_creds:
        # Get credentials (obfuscated for display)
        creds = key_manager.get_exchange_credentials("nobitex")
        api_key = key_manager.get_obfuscated_key(creds.get("api_key", ""))
        api_secret = key_manager.get_obfuscated_key(creds.get("api_secret", "")) if "api_secret" in creds else None
        
        print(f"API Key: {api_key}")
        if api_secret:
            print(f"API Secret: {api_secret}")
    
    print("API Key Manager test completed")
    return has_creds


def test_execution_config():
    """Test execution engine configuration."""
    print("\n--- Testing Execution Engine Configuration ---")
    
    # Create configuration
    config = {
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
    
    try:
        # Initialize execution engine (but don't start it)
        engine = NobitexExecutionEngine(config)
        print("Execution engine configuration valid")
        
        # Start the engine
        print("Starting execution engine...")
        engine.start()
        print(f"Execution engine status: {engine.engine_status}")
        
        # Get engine stats
        stats = engine.get_stats()
        print("\nExecution Engine Statistics:")
        for key, value in stats.items():
            print(f"  {key}: {value}")
        
        # Stop the engine
        print("\nStopping execution engine...")
        engine.stop()
        print(f"Execution engine status: {engine.engine_status}")
        
        return True
        
    except Exception as e:
        logger.exception("Error testing execution engine configuration")
        print(f"Error: {str(e)}")
        return False


def test_transaction_logger():
    """Test transaction logger."""
    print("\n--- Testing Transaction Logger ---")
    
    from trading_system.execution.transaction_logger import transaction_logger, TransactionType
    
    # Create sample order
    order_id = str(uuid.uuid4())
    order = Order(
        order_id=order_id,
        symbol="btc-usdt",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=0.001,
        price=None,
        time_in_force=OrderTimeInForce.IOC
    )
    
    # Log sample transactions
    print("Logging sample transactions...")
    
    # Order creation
    transaction_logger.log_order_creation(order)
    print("- Logged order creation")
    
    # Order update
    order.update_status(OrderStatus.VALIDATING)
    transaction_logger.log_order_update(order, OrderStatus.CREATED)
    print("- Logged order update")
    
    # Order submission
    transaction_logger.log_order_submission(order, {"success": True, "exchange_order_id": "12345"})
    print("- Logged order submission")
    
    # Order fill
    transaction_logger.log_order_fill(order, 50000.0, 0.001, True)
    print("- Logged order fill")
    
    # Error
    transaction_logger.log_error("Test error message", order_id=order.order_id)
    print("- Logged error message")
    
    # Get order history
    print("\nRetrieving order history...")
    history = transaction_logger.get_order_history(order_id)
    print(f"Found {len(history)} transaction records for order {order_id}")
    
    return True


def main():
    """Run execution engine tests."""
    print("=== Execution Engine Component Tests ===\n")
    
    # Test API key manager
    api_key_result = test_api_key_manager()
    
    # Test transaction logger
    transaction_logger_result = test_transaction_logger()
    
    # Test execution engine
    execution_engine_result = test_execution_config()
    
    # Print summary
    print("\n=== Test Results ===")
    print(f"API Key Manager: {'PASS' if api_key_result else 'FAIL'}")
    print(f"Transaction Logger: {'PASS' if transaction_logger_result else 'FAIL'}")
    print(f"Execution Engine: {'PASS' if execution_engine_result else 'FAIL'}")
    
    overall_result = all([api_key_result, transaction_logger_result, execution_engine_result])
    print(f"\nOverall Result: {'PASS' if overall_result else 'FAIL'}")
    
    if not overall_result:
        print("\nSome tests failed. Check the logs for details.")
    else:
        print("\nAll tests passed successfully!")


if __name__ == "__main__":
    main() 