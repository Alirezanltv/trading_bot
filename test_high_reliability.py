"""
Test High-Reliability Components

This script tests the high-reliability components of our trading system,
verifying that they function correctly and interact appropriately.
"""

import asyncio
import logging
import random
import os
import sys
import time
from typing import Dict, Any, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('high_reliability_test.log')
    ]
)
logger = logging.getLogger("high_reliability_test")

# Import core components
from trading_system.core.circuit_breaker import CircuitBreaker, CircuitState
from trading_system.exchange.connection_pool import ConnectionPool, ConnectionStatus
from trading_system.core import message_bus


async def test_circuit_breaker() -> bool:
    """
    Test the circuit breaker implementation.
    
    Returns:
        Test result (True = pass, False = fail)
    """
    logger.info("Testing circuit breaker...")
    
    # Create a circuit breaker
    breaker = CircuitBreaker(
        name="test_breaker",
        failure_threshold=3,
        reset_timeout=1.0,  # Short timeout for testing
        half_open_max_requests=2,
        success_threshold=2
    )
    
    # Function that will sometimes fail
    async def test_operation(should_fail: bool = False):
        if should_fail:
            raise ValueError("Simulated failure")
        return "success"
    
    try:
        # Test successful operation
        result = await breaker.execute(test_operation)
        if result != "success":
            logger.error(f"Unexpected result: {result}")
            return False
        
        logger.info("Circuit breaker passed successful operation test")
        
        # Test failures to open circuit
        failures = 0
        for i in range(5):  # More than failure_threshold
            try:
                await breaker.execute(test_operation, should_fail=True)
            except ValueError:
                failures += 1  # Expected
        
        logger.info(f"Completed {failures} failed operations")
        
        # The circuit should now be open
        if breaker.state != CircuitState.OPEN:
            logger.error(f"Expected circuit to be OPEN, got {breaker.state}")
            return False
        
        logger.info("Circuit breaker successfully opened after failures")
        
        # Wait for reset timeout to transition to half-open
        logger.info("Waiting for circuit breaker reset timeout...")
        await asyncio.sleep(1.5)  # Longer than reset_timeout
        
        if breaker.state != CircuitState.HALF_OPEN:
            logger.error(f"Expected circuit to be HALF_OPEN, got {breaker.state}")
            return False
        
        logger.info("Circuit breaker successfully transitioned to HALF_OPEN")
        
        # Test half-open state
        # First success
        result = await breaker.execute(test_operation)
        if result != "success":
            logger.error(f"Unexpected result in half-open state: {result}")
            return False
        
        # Second success should close the circuit
        result = await breaker.execute(test_operation)
        
        logger.info("Successfully executed operations in HALF_OPEN state")
        
        # Check if circuit is closed
        if breaker.state != CircuitState.CLOSED:
            logger.error(f"Expected circuit to be CLOSED, got {breaker.state}")
            return False
        
        logger.info("Circuit breaker successfully closed after successful test operations")
        
        logger.info("Circuit breaker tests passed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error in circuit breaker test: {e}")
        return False


async def test_connection_pool() -> bool:
    """
    Test the connection pool implementation.
    
    Returns:
        Test result (True = pass, False = fail)
    """
    logger.info("Testing connection pool...")
    
    try:
        # Create a simple mock client
        class MockClient:
            async def get_status(self):
                return {"status": "ok"}
            
            async def get_data(self, param):
                if param == "error":
                    raise Exception("Simulated error")
                return {"data": param}
        
        # Function to create a new client
        def create_client():
            return MockClient()
        
        # Create a connection pool
        pool = ConnectionPool(
            name="test_pool",
            create_client_func=create_client,
            config={
                "min_connections": 2,
                "max_connections": 5,
                "connection_timeout": 2,
                "idle_timeout": 10,
                "max_retries": 2
            }
        )
        
        # Initialize the pool
        await pool.initialize()
        
        # Get a connection
        connection = await pool.get_connection()
        if not connection:
            logger.error("Failed to get connection")
            return False
        
        logger.info("Successfully got a connection from the pool")
        
        # Test executing an operation on the connection
        result = await pool.execute("get_status")
        if not result or result.get("status") != "ok":
            logger.error(f"Unexpected result from pool execute: {result}")
            return False
        
        logger.info("Successfully executed operation through the pool")
        
        # Test connection pool health check
        health = await pool.health_check()
        logger.info(f"Connection pool health: {health}")
        if "status" not in health:
            logger.error("Invalid connection pool health check result")
            return False
        
        # Shutdown the pool
        await pool.stop()
        
        logger.info("Connection pool tests passed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error in connection pool test: {e}")
        return False


async def test_message_bus() -> bool:
    """
    Test the message bus implementation.
    
    Returns:
        Test result (True = pass, False = fail)
    """
    logger.info("Testing message bus...")
    
    try:
        # Initialize message bus if not already
        if not hasattr(message_bus, "is_initialized") or not message_bus.is_initialized():
            logger.info("Initializing message bus")
            message_bus.initialize(in_memory=True)
        
        # Variable to track message receipt
        received_messages = []
        
        # Create a message type for testing
        TEST_MESSAGE = "test_message"
        
        # Create a subscription
        def message_handler(data):
            received_messages.append(data)
            logger.info(f"Received message: {data}")
        
        # Subscribe to test messages
        message_bus.subscribe(TEST_MESSAGE, message_handler)
        
        # Publish a test message
        test_data = {"test_id": 123, "value": "test_value"}
        message_bus.publish(TEST_MESSAGE, test_data)
        
        # Allow time for message processing
        await asyncio.sleep(0.5)
        
        # Check if message was received
        if not received_messages:
            logger.error("Message was not received")
            return False
        
        if received_messages[0].get("test_id") != 123:
            logger.error(f"Incorrect message received: {received_messages[0]}")
            return False
        
        logger.info("Message bus successfully delivered the message")
        
        # Unsubscribe
        message_bus.unsubscribe(TEST_MESSAGE, message_handler)
        
        # Clear received messages
        received_messages.clear()
        
        # Publish another message
        message_bus.publish(TEST_MESSAGE, {"test_id": 456})
        
        # Allow time for message processing
        await asyncio.sleep(0.5)
        
        # Check that no message was received after unsubscribing
        if received_messages:
            logger.error("Message was received after unsubscribing")
            return False
        
        logger.info("Message bus tests passed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error in message bus test: {e}")
        return False


async def test_reliability_components() -> bool:
    """
    Test integration of reliability components together.
    
    Returns:
        Test result (True = pass, False = fail)
    """
    logger.info("Testing reliability components integration...")
    
    # Test integration of circuit breaker and connection pool
    try:
        # Create a simple mock client with failures
        class UnreliableClient:
            def __init__(self):
                self.failure_rate = 0.0  # Start with no failures
            
            async def get_data(self):
                if random.random() < self.failure_rate:
                    raise Exception("Simulated random failure")
                return {"status": "ok"}
            
            def set_failure_rate(self, rate):
                self.failure_rate = rate
        
        # Create a shared client instance
        client = UnreliableClient()
        
        # Function to create a new client
        def create_client():
            return client
        
        # Create a connection pool
        pool = ConnectionPool(
            name="unreliable_pool",
            create_client_func=create_client,
            config={
                "min_connections": 1,
                "max_connections": 3,
                "connection_timeout": 1,
                "idle_timeout": 5,
                "max_retries": 2
            }
        )
        
        # Initialize the pool
        await pool.initialize()
        
        # Create a circuit breaker
        breaker = CircuitBreaker(
            name="reliability_breaker",
            failure_threshold=3,
            reset_timeout=2.0,
            half_open_max_requests=1,
            success_threshold=2
        )
        
        # Test with no failures
        logger.info("Testing with no failures...")
        for _ in range(5):
            result = await breaker.execute(pool.execute, "get_data")
            if not result or result.get("status") != "ok":
                logger.error(f"Unexpected result: {result}")
                return False
        
        logger.info("Successfully handled operations with no failures")
        
        # Test with increasing failure rate
        logger.info("Testing with increasing failure rate...")
        client.set_failure_rate(0.6)  # 60% failures
        
        # At this failure rate, we should eventually trip the circuit
        success_count = 0
        failure_count = 0
        for i in range(10):
            try:
                result = await breaker.execute(pool.execute, "get_data")
                success_count += 1
            except Exception:
                failure_count += 1
                # If we've had enough failures, the circuit should open
                if breaker.state == CircuitState.OPEN:
                    logger.info("Circuit breaker opened as expected after failures")
                    break
        
        logger.info(f"Completed test with {success_count} successes and {failure_count} failures")
        
        # Reset for further tests
        client.set_failure_rate(0.0)
        await asyncio.sleep(3.0)  # Wait for circuit reset
        
        # Circuit should be in half-open or closed state now
        if breaker.state not in [CircuitState.HALF_OPEN, CircuitState.CLOSED]:
            logger.error(f"Circuit didn't reset, state is {breaker.state}")
            return False
        
        # Test recovery
        logger.info("Testing recovery...")
        for _ in range(3):
            try:
                result = await breaker.execute(pool.execute, "get_data")
                if not result or result.get("status") != "ok":
                    logger.error(f"Unexpected result during recovery: {result}")
                    return False
            except Exception as e:
                logger.error(f"Unexpected error during recovery: {e}")
                return False
        
        # Circuit should be closed now
        if breaker.state != CircuitState.CLOSED:
            logger.error(f"Circuit didn't close after recovery, state is {breaker.state}")
            return False
        
        logger.info("Circuit breaker successfully recovered after failures")
        
        # Shutdown the pool
        await pool.stop()
        
        logger.info("Reliability components integration tests passed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error in reliability components integration test: {e}")
        return False


async def run_tests() -> bool:
    """
    Run all high-reliability component tests.
    
    Returns:
        Overall test result
    """
    tests = [
        ("Circuit Breaker", test_circuit_breaker),
        ("Connection Pool", test_connection_pool),
        ("Message Bus", test_message_bus),
        ("Reliability Components Integration", test_reliability_components),
    ]
    
    results = []
    
    for name, test_func in tests:
        logger.info(f"=== Running {name} tests ===")
        try:
            result = await test_func()
            results.append(result)
            logger.info(f"=== {name} tests {'PASSED' if result else 'FAILED'} ===\n")
        except Exception as e:
            logger.error(f"Error running {name} tests: {e}")
            results.append(False)
    
    # Overall result
    all_passed = all(results)
    if all_passed:
        logger.info("All high-reliability component tests PASSED")
    else:
        logger.error("Some high-reliability component tests FAILED")
    
    return all_passed


async def main():
    """Main function."""
    logger.info("Starting high-reliability component tests")
    
    success = await run_tests()
    
    return 0 if success else 1


if __name__ == "__main__":
    result = asyncio.run(main())
    sys.exit(result) 