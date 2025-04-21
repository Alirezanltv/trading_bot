"""
Simplified Circuit Breaker Test

A minimal test script that focuses only on testing the circuit breaker 
without dependencies on other components.
"""

import asyncio
import logging
import time
import sys

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger()

class SimpleCircuitBreaker:
    """A very simple circuit breaker implementation for testing."""
    
    def __init__(self, name, failure_threshold=3, reset_timeout=5.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "closed"  # closed, open, half-open
        self.success_count = 0
        self.success_threshold = 2
    
    def check_state(self):
        """Check and potentially update the state."""
        if self.state == "open":
            # Check if reset timeout has passed
            if (self.last_failure_time and 
                time.time() - self.last_failure_time >= self.reset_timeout):
                logger.info(f"Circuit {self.name} transitioning to half-open (reset timeout passed)")
                self.state = "half-open"
                self.success_count = 0
        return self.state
    
    async def execute(self, func, *args, **kwargs):
        """Execute a function with circuit breaker protection."""
        # Check current state
        current_state = self.check_state()
        
        # If circuit is open, fail fast
        if current_state == "open":
            logger.info(f"Circuit {self.name} is open - failing fast")
            raise Exception(f"Circuit {self.name} is open")
        
        try:
            # Execute the function
            result = await func(*args, **kwargs)
            
            # On success in half-open state
            if current_state == "half-open":
                self.success_count += 1
                logger.info(f"Success in half-open state ({self.success_count}/{self.success_threshold})")
                
                # If we've had enough successes, close the circuit
                if self.success_count >= self.success_threshold:
                    logger.info(f"Circuit {self.name} transitioning to closed (success threshold reached)")
                    self.state = "closed"
                    self.failure_count = 0
            else:
                # Just reset failure count on success in closed state
                self.failure_count = 0
            
            return result
            
        except Exception as e:
            # On failure
            self.failure_count += 1
            self.last_failure_time = time.time()
            logger.info(f"Failure in circuit {self.name} ({self.failure_count}/{self.failure_threshold})")
            
            # If in half-open state, any failure opens the circuit
            if current_state == "half-open":
                logger.info(f"Circuit {self.name} transitioning to open (failure in half-open state)")
                self.state = "open"
            
            # If we've hit the threshold in closed state, open the circuit
            elif current_state == "closed" and self.failure_count >= self.failure_threshold:
                logger.info(f"Circuit {self.name} transitioning to open (failure threshold reached)")
                self.state = "open"
            
            # Re-raise the exception
            raise

async def test_simple_circuit_breaker():
    """Test the simple circuit breaker."""
    print("\nTesting simple circuit breaker...")
    
    # Create a circuit breaker
    breaker = SimpleCircuitBreaker(
        name="test_breaker",
        failure_threshold=2,
        reset_timeout=1.0
    )
    
    # Define test functions
    async def success_function():
        print("  Success function called")
        return "success"
    
    async def failure_function():
        print("  Failure function called")
        raise ValueError("Simulated failure")
    
    # Test 1: Success case
    print("\nTest 1: Success case")
    try:
        result = await breaker.execute(success_function)
        print(f"  Result: {result}")
        print(f"  Breaker state: {breaker.state}")
    except Exception as e:
        print(f"  Error: {e}")
    
    # Test 2: Failures to open the circuit
    print("\nTest 2: Failures to open circuit")
    for i in range(3):
        try:
            print(f"  Attempt {i+1}...")
            await breaker.execute(failure_function)
        except ValueError:
            print(f"  Got expected ValueError")
        except Exception as e:
            print(f"  Got other exception: {e}")
        
        print(f"  Breaker state after attempt {i+1}: {breaker.state}")
    
    # Test 3: Wait for reset timeout
    if breaker.state == "open":
        print("\nTest 3: Waiting for reset timeout")
        print(f"  Breaker state before waiting: {breaker.state}")
        print(f"  Waiting {breaker.reset_timeout+0.5} seconds...")
        
        await asyncio.sleep(breaker.reset_timeout + 0.5)
        
        # Check state after waiting
        breaker.check_state()  # Force state check
        print(f"  Breaker state after waiting: {breaker.state}")
    
    # Test 4: Recovery in half-open state
    if breaker.state == "half-open":
        print("\nTest 4: Recovery from half-open state")
        
        # First success
        try:
            result = await breaker.execute(success_function)
            print(f"  First success result: {result}")
            print(f"  Breaker state after first success: {breaker.state}")
        except Exception as e:
            print(f"  Error: {e}")
        
        # Second success should close the circuit
        try:
            result = await breaker.execute(success_function)
            print(f"  Second success result: {result}")
            print(f"  Breaker state after second success: {breaker.state}")
        except Exception as e:
            print(f"  Error: {e}")
    
    print("\nSimple circuit breaker test completed.")
    return breaker.state == "closed"

async def main():
    """Main function."""
    try:
        # Test with timeout
        success = await asyncio.wait_for(test_simple_circuit_breaker(), timeout=10.0)
        print(f"\nTest {'PASSED' if success else 'FAILED'}")
        return 0 if success else 1
    except asyncio.TimeoutError:
        print("\nTest FAILED - Timeout")
        return 1
    except Exception as e:
        print(f"\nTest FAILED - Unexpected error: {e}")
        return 1

if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
        sys.exit(130) 