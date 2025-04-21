"""
Circuit Breaker Test

This script verifies that the circuit breaker component works correctly.
"""

import asyncio
import logging
import time
import sys
from functools import partial

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("circuit_breaker_test")

# Import the circuit breaker
from trading_system.core.circuit_breaker import CircuitBreaker, CircuitState, CircuitOpenError

async def test_breaker():
    """Test the circuit breaker functionality."""
    logger.info("Creating circuit breaker...")
    
    # Create a circuit breaker with a short timeout
    breaker = CircuitBreaker(
        name="test_breaker",
        failure_threshold=2,  # Open after 2 failures
        reset_timeout=1.0,    # Reset after 1 second
        half_open_max_requests=1,
        success_threshold=1
    )
    
    # Simple test function
    async def test_function(fail=False):
        if fail:
            logger.info("Test function failing as requested")
            raise ValueError("Simulated failure")
        logger.info("Test function succeeding")
        return "success"
    
    # Test successful operation
    logger.info("Testing successful operation...")
    try:
        # Add timeout to prevent hanging
        result = await asyncio.wait_for(breaker.execute(test_function), timeout=2.0)
        logger.info(f"Result: {result}")
        logger.info(f"Breaker state: {breaker.state}")
    except asyncio.TimeoutError:
        logger.error("Timeout during successful operation test")
    except Exception as e:
        logger.error(f"Error: {e}")
    
    # Test failures
    logger.info("\nTesting failures to open the circuit...")
    for i in range(3):  # More than failure_threshold
        try:
            logger.info(f"Attempt {i+1}...")
            # Add timeout to prevent hanging
            await asyncio.wait_for(breaker.execute(test_function, True), timeout=2.0)
        except ValueError as e:
            # This is expected
            logger.info(f"Got expected error: {e}")
        except CircuitOpenError as e:
            # This is also expected after threshold
            logger.info(f"Got expected circuit open error: {e}")
        except asyncio.TimeoutError:
            logger.error(f"Timeout during failure test {i+1}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        
        # Check current state
        current_state = breaker.state
        logger.info(f"Breaker state after attempt {i+1}: {current_state}")
        
        # If circuit is open, we've achieved our goal
        if current_state == CircuitState.OPEN:
            logger.info("Circuit is now OPEN as expected")
            break
    
    # Wait for reset
    if breaker.state == CircuitState.OPEN:
        logger.info("\nBreaker is OPEN, waiting for reset timeout...")
        
        # Wait with timeout to prevent hanging
        wait_until = time.time() + 2.0  # 2 second maximum wait
        while breaker.state == CircuitState.OPEN and time.time() < wait_until:
            await asyncio.sleep(0.1)
            
        logger.info(f"Breaker state after waiting: {breaker.state}")
    
    # Test recovery
    if breaker.state == CircuitState.HALF_OPEN:
        logger.info("\nTesting recovery from HALF_OPEN state...")
        try:
            # Add timeout to prevent hanging
            result = await asyncio.wait_for(breaker.execute(test_function), timeout=2.0)
            logger.info(f"Result: {result}")
            logger.info(f"Breaker state after successful test: {breaker.state}")
        except asyncio.TimeoutError:
            logger.error("Timeout during recovery test")
        except Exception as e:
            logger.error(f"Error during recovery: {e}")
            logger.info(f"Breaker state: {breaker.state}")
    
    logger.info("\nTest completed.")
    # Success if circuit is now closed
    return breaker.state == CircuitState.CLOSED

async def main():
    """Main function."""
    try:
        # Set a global timeout for the entire test
        success = await asyncio.wait_for(test_breaker(), timeout=10.0)
        return 0 if success else 1
    except asyncio.TimeoutError:
        logger.error("Test timed out after 10 seconds")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error in test: {e}")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code) 