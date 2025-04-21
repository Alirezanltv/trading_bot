"""
High-Reliability Trading System Installation Verification

This script verifies that the high-reliability components of the trading system
are installed and functioning correctly.
"""

import asyncio
import os
import sys
import logging
import time
from typing import Dict, Any, List, Tuple, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("verify_installation")

async def check_imports() -> Tuple[bool, List[str]]:
    """
    Check if required modules can be imported.
    
    Returns:
        Tuple of (success, list of error messages)
    """
    required_modules = [
        "trading_system.core.circuit_breaker",
        "trading_system.exchange.connection_pool",
        "trading_system.core.message_bus",
        "trading_system.exchange.transaction_verification",
        "trading_system.position.shadow_accounting",
        "trading_system.market_data.tradingview_webhook_handler"
    ]
    
    errors = []
    success = True
    
    for module_name in required_modules:
        try:
            # Attempt to import the module
            logger.info(f"Checking {module_name}...")
            module = __import__(module_name, fromlist=["*"])
            logger.info(f"✓ Successfully imported {module_name}")
        except ImportError as e:
            # If import fails, record the error
            errors.append(f"Failed to import {module_name}: {e}")
            success = False
        except Exception as e:
            # Catch other errors that might occur on import
            errors.append(f"Error importing {module_name}: {e}")
            success = False
    
    return success, errors

async def check_external_dependencies() -> Tuple[bool, List[str]]:
    """
    Check if external dependencies are available.
    
    Returns:
        Tuple of (success, list of error messages)
    """
    dependencies = [
        "aiohttp",
        "pybreaker",
        "pika"
    ]
    
    errors = []
    success = True
    
    for dep in dependencies:
        try:
            logger.info(f"Checking dependency: {dep}...")
            __import__(dep)
            logger.info(f"✓ Dependency {dep} is available")
        except ImportError:
            errors.append(f"Missing dependency: {dep}")
            success = False
        except Exception as e:
            errors.append(f"Error checking dependency {dep}: {e}")
            success = False
    
    return success, errors

async def check_circuit_breaker() -> Tuple[bool, List[str]]:
    """
    Check if the circuit breaker works correctly.
    
    Returns:
        Tuple of (success, list of error messages)
    """
    errors = []
    success = True
    
    try:
        logger.info("Testing circuit breaker...")
        from trading_system.core.circuit_breaker import CircuitBreaker, CircuitState, CircuitOpenError
        
        # Create a circuit breaker
        breaker = CircuitBreaker(
            name="test_breaker",
            failure_threshold=2,
            reset_timeout=0.5,  # Short timeout for testing
            half_open_max_requests=1,
            success_threshold=1
        )
        
        # Test function
        async def test_func(fail=False):
            if fail:
                raise ValueError("Test failure")
            return "success"
        
        # Test successful operation
        try:
            result = await asyncio.wait_for(breaker.execute(test_func), timeout=1.0)
            if result != "success":
                errors.append(f"Circuit breaker returned unexpected result: {result}")
                success = False
        except asyncio.TimeoutError:
            errors.append("Circuit breaker timed out during successful operation test")
            success = False
        except Exception as e:
            errors.append(f"Error in circuit breaker success test: {e}")
            success = False
        
        # Test that circuit opens after failures
        for i in range(3):  # More than failure_threshold
            try:
                await asyncio.wait_for(breaker.execute(test_func, True), timeout=1.0)
            except ValueError:
                pass  # Expected
            except CircuitOpenError:
                pass  # Expected after threshold
            except asyncio.TimeoutError:
                errors.append("Circuit breaker timed out during failure test")
                success = False
            except Exception as e:
                logger.warning(f"Got exception in failure test (may be expected): {e}")
        
        # Check if circuit is open
        if breaker.state != CircuitState.OPEN:
            errors.append(f"Circuit breaker failed to open after failures, state: {breaker.state}")
            success = False
        
        # Wait for reset timeout to transition to half-open
        logger.info("Waiting for circuit reset...")
        
        # Wait with timeout
        start_time = time.time()
        timeout = 2.0  # 2 seconds max wait
        while breaker.state == CircuitState.OPEN and (time.time() - start_time) < timeout:
            await asyncio.sleep(0.1)
        
        # Check if we timed out or transitioned
        if breaker.state == CircuitState.OPEN:
            errors.append("Circuit breaker failed to transition to HALF_OPEN within timeout")
            success = False
        
        logger.info("✓ Circuit breaker test passed")
        
    except Exception as e:
        errors.append(f"Error testing circuit breaker: {e}")
        success = False
    
    return success, errors

async def check_connection_pool() -> Tuple[bool, List[str]]:
    """
    Check if the connection pool works correctly.
    
    Returns:
        Tuple of (success, list of error messages)
    """
    errors = []
    success = True
    
    try:
        logger.info("Testing connection pool...")
        from trading_system.exchange.connection_pool import ConnectionPool
        
        # Create a mock client
        class MockClient:
            async def test_method(self):
                return {"status": "ok"}
        
        def create_client():
            return MockClient()
        
        # Create a connection pool
        pool = ConnectionPool(
            name="test_pool",
            create_client_func=create_client,
            config={
                "min_connections": 1,
                "max_connections": 2
            }
        )
        
        try:
            # Initialize the pool with timeout
            await asyncio.wait_for(pool.initialize(), timeout=3.0)
            
            # Test execution with timeout
            result = await asyncio.wait_for(pool.execute("test_method"), timeout=2.0)
            if result.get("status") != "ok":
                errors.append(f"Connection pool returned unexpected result: {result}")
                success = False
            
            # Test getting a connection with timeout
            connection = await asyncio.wait_for(pool.get_connection(), timeout=2.0)
            if connection is None:
                errors.append("Connection pool failed to provide a connection")
                success = False
            else:
                # Release the connection
                await pool.release_connection(connection)
            
        except asyncio.TimeoutError:
            errors.append("Connection pool operation timed out")
            success = False
        finally:
            # Always try to stop the pool
            try:
                await asyncio.wait_for(pool.stop(), timeout=2.0)
            except asyncio.TimeoutError:
                errors.append("Connection pool stop operation timed out")
            except Exception as e:
                logger.warning(f"Error stopping connection pool: {e}")
        
        logger.info("✓ Connection pool test passed")
        
    except Exception as e:
        errors.append(f"Error testing connection pool: {e}")
        success = False
    
    return success, errors

async def check_message_bus() -> Tuple[bool, List[str]]:
    """
    Check if the message bus works correctly.
    
    Returns:
        Tuple of (success, list of error messages)
    """
    errors = []
    success = True
    
    try:
        logger.info("Testing message bus...")
        from trading_system.core import message_bus
        
        # Initialize the message bus
        message_bus.initialize(in_memory=True)
        
        # Variable to track message receipt
        message_received = False
        
        # Handler function
        def message_handler(data):
            nonlocal message_received
            message_received = True
            logger.info(f"Message received: {data}")
        
        # Subscribe to a test message type
        message_bus.subscribe("test_message", message_handler)
        
        # Publish a message
        message_bus.publish("test_message", {"test": "data"})
        
        # Wait a bit for the message to be processed with timeout
        start_time = time.time()
        timeout = 2.0
        while not message_received and (time.time() - start_time) < timeout:
            await asyncio.sleep(0.1)
        
        if not message_received:
            errors.append("Message bus failed to deliver message within timeout")
            success = False
        
        # Unsubscribe
        message_bus.unsubscribe("test_message", message_handler)
        
        logger.info("✓ Message bus test passed")
        
    except Exception as e:
        errors.append(f"Error testing message bus: {e}")
        success = False
    
    return success, errors

async def run_verification() -> bool:
    """
    Run all verification checks.
    
    Returns:
        Overall verification result
    """
    checks = [
        ("Import Check", check_imports),
        ("External Dependencies Check", check_external_dependencies),
        ("Circuit Breaker Check", check_circuit_breaker),
        ("Connection Pool Check", check_connection_pool),
        ("Message Bus Check", check_message_bus)
    ]
    
    overall_success = True
    
    logger.info("\n" + "="*80)
    logger.info(" High-Reliability Trading System Installation Verification")
    logger.info("="*80 + "\n")
    
    for name, check_func in checks:
        logger.info(f"\n--- Running {name} ---")
        try:
            # Run check with timeout
            success, errors = await asyncio.wait_for(check_func(), timeout=15.0)
            
            if not success:
                overall_success = False
                logger.error(f"{name} failed with the following errors:")
                for error in errors:
                    logger.error(f"  • {error}")
            else:
                logger.info(f"{name} passed successfully")
                
        except asyncio.TimeoutError:
            overall_success = False
            logger.error(f"{name} check timed out after 15 seconds")
        except Exception as e:
            overall_success = False
            logger.error(f"Error running {name}: {e}")
    
    logger.info("\n" + "="*80)
    if overall_success:
        logger.info(" Verification completed successfully! All components are working correctly.")
    else:
        logger.error(" Verification found issues with the installation. See above for details.")
    logger.info("="*80 + "\n")
    
    return overall_success

async def main():
    """Main function."""
    try:
        # Overall timeout for the entire verification process
        success = await asyncio.wait_for(run_verification(), timeout=60.0)
        return 0 if success else 1
    except asyncio.TimeoutError:
        logger.error("Verification process timed out after 60 seconds")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error in verification: {e}")
        return 1

if __name__ == "__main__":
    try:
        result = asyncio.run(main())
        sys.exit(result)
    except KeyboardInterrupt:
        logger.info("Verification interrupted by user")
        sys.exit(130)  # 130 is the standard exit code for SIGINT 