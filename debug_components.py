"""
Debug High-Reliability Components

This script provides debug information about the high-reliability components 
and helps diagnose issues with them.
"""

import asyncio
import logging
import sys
import time
import os
import traceback
from typing import Dict, Any, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Debug level to see everything
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('component_debug.log')
    ]
)
logger = logging.getLogger("debug_components")

async def debug_circuit_breaker() -> None:
    """Debug the circuit breaker component."""
    logger.info("=== Circuit Breaker Debug ===")
    
    try:
        # Import the circuit breaker
        from trading_system.core.circuit_breaker import CircuitBreaker, CircuitState, CircuitOpenError
        logger.info("✓ Successfully imported CircuitBreaker module")
        
        # Check implementation details
        logger.info(f"CircuitBreaker class defined: {hasattr(CircuitBreaker, '__init__')}")
        logger.info(f"CircuitState enum values: {[s.value for s in CircuitState]}")
        
        # Create a test circuit breaker
        logger.info("Creating test circuit breaker...")
        breaker = CircuitBreaker(
            name="debug_breaker",
            failure_threshold=2,
            reset_timeout=1.0,
            half_open_max_requests=1,
            success_threshold=1
        )
        logger.info(f"Initial state: {breaker.state}")
        
        # Test successful operation
        logger.info("Testing successful operation...")
        async def test_success():
            return "success"
        
        try:
            result = await asyncio.wait_for(breaker.execute(test_success), timeout=1.0)
            logger.info(f"Success result: {result}")
            logger.info(f"State after success: {breaker.state}")
            logger.info(f"Metrics: {breaker.metrics.get_stats()}")
        except Exception as e:
            logger.error(f"Error in success test: {e}")
        
        # Test failure operation
        logger.info("Testing failure operation...")
        async def test_failure():
            raise ValueError("Test failure")
        
        try:
            await asyncio.wait_for(breaker.execute(test_failure), timeout=1.0)
        except ValueError as e:
            logger.info(f"Expected error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            
        logger.info(f"State after failure: {breaker.state}")
        logger.info(f"Metrics: {breaker.metrics.get_stats()}")
        
        logger.info("Circuit breaker debug completed")
        
    except ImportError as e:
        logger.error(f"Failed to import CircuitBreaker: {e}")
    except Exception as e:
        logger.error(f"Error in circuit breaker debug: {e}")
        logger.error(traceback.format_exc())

async def debug_connection_pool() -> None:
    """Debug the connection pool component."""
    logger.info("\n=== Connection Pool Debug ===")
    
    try:
        # Import the connection pool
        from trading_system.exchange.connection_pool import ConnectionPool, ConnectionStatus
        logger.info("✓ Successfully imported ConnectionPool module")
        
        # Check implementation details
        logger.info(f"ConnectionPool class defined: {hasattr(ConnectionPool, '__init__')}")
        logger.info(f"ConnectionStatus enum values: {[s.value for s in ConnectionStatus]}")
        
        # Simple mock client for testing
        class DebugClient:
            def __init__(self):
                self.call_count = 0
                logger.info("Mock client initialized")
                
            async def test_method(self, param=None):
                self.call_count += 1
                logger.info(f"Mock client test_method called ({self.call_count} times)")
                return {"status": "ok", "param": param, "count": self.call_count}
        
        # Create client factory
        def create_client():
            return DebugClient()
        
        # Create a test connection pool
        logger.info("Creating test connection pool...")
        pool = ConnectionPool(
            name="debug_pool",
            create_client_func=create_client,
            config={
                "min_connections": 1,
                "max_connections": 2,
                "connection_timeout": 5
            }
        )
        
        # Initialize the pool
        logger.info("Initializing connection pool...")
        try:
            result = await asyncio.wait_for(pool.initialize(), timeout=3.0)
            logger.info(f"Initialization result: {result}")
        except Exception as e:
            logger.error(f"Error initializing pool: {e}")
        
        # Get connection info
        logger.info(f"Connections: {len(pool.connections)}")
        logger.info(f"Available connections: {len(pool.available_connections)}")
        
        # Try to execute a method
        logger.info("Executing test method...")
        try:
            result = await asyncio.wait_for(pool.execute("test_method", "test_param"), timeout=2.0)
            logger.info(f"Execution result: {result}")
        except Exception as e:
            logger.error(f"Error executing method: {e}")
        
        # Get connection and execute directly
        logger.info("Getting connection and executing directly...")
        try:
            connection = await asyncio.wait_for(pool.get_connection(), timeout=2.0)
            if connection:
                result = await asyncio.wait_for(connection.execute("test_method"), timeout=2.0)
                logger.info(f"Direct execution result: {result}")
                await pool.release_connection(connection)
            else:
                logger.error("Failed to get connection")
        except Exception as e:
            logger.error(f"Error with direct execution: {e}")
        
        # Check health
        logger.info("Checking pool health...")
        try:
            health = await asyncio.wait_for(pool.health_check(), timeout=2.0)
            logger.info(f"Health check result: {health}")
        except Exception as e:
            logger.error(f"Error checking health: {e}")
        
        # Stop the pool
        logger.info("Stopping connection pool...")
        try:
            await asyncio.wait_for(pool.stop(), timeout=2.0)
            logger.info("Pool stopped successfully")
        except Exception as e:
            logger.error(f"Error stopping pool: {e}")
        
        logger.info("Connection pool debug completed")
        
    except ImportError as e:
        logger.error(f"Failed to import ConnectionPool: {e}")
    except Exception as e:
        logger.error(f"Error in connection pool debug: {e}")
        logger.error(traceback.format_exc())

async def debug_message_bus() -> None:
    """Debug the message bus component."""
    logger.info("\n=== Message Bus Debug ===")
    
    try:
        # Import the message bus
        from trading_system.core import message_bus
        logger.info("✓ Successfully imported message_bus module")
        
        # Check implementation details
        logger.info(f"Initialize function defined: {hasattr(message_bus, 'initialize')}")
        logger.info(f"Is initialized function defined: {hasattr(message_bus, 'is_initialized')}")
        
        # Initialize the message bus
        logger.info("Initializing message bus...")
        try:
            message_bus.initialize(in_memory=True)
            logger.info(f"Is initialized: {message_bus.is_initialized()}")
        except Exception as e:
            logger.error(f"Error initializing message bus: {e}")
        
        # Set up a message handler
        message_received = False
        received_messages = []
        
        def message_handler(data):
            nonlocal message_received
            message_received = True
            received_messages.append(data)
            logger.info(f"Message handler received: {data}")
        
        # Subscribe to a test topic
        logger.info("Subscribing to test topic...")
        try:
            message_bus.subscribe("debug_topic", message_handler)
            logger.info("Successfully subscribed")
        except Exception as e:
            logger.error(f"Error subscribing: {e}")
        
        # Publish a test message
        logger.info("Publishing test message...")
        try:
            test_data = {"test_id": 12345, "value": "debug_value", "timestamp": time.time()}
            message_bus.publish("debug_topic", test_data)
            logger.info("Message published")
        except Exception as e:
            logger.error(f"Error publishing: {e}")
        
        # Wait for message processing
        logger.info("Waiting for message processing...")
        start_time = time.time()
        max_wait = 2.0  # 2 seconds max
        
        while not message_received and (time.time() - start_time) < max_wait:
            await asyncio.sleep(0.1)
        
        if message_received:
            logger.info(f"Message was received: {received_messages}")
        else:
            logger.error("No message received within timeout")
        
        # Unsubscribe
        logger.info("Unsubscribing...")
        try:
            message_bus.unsubscribe("debug_topic", message_handler)
            logger.info("Successfully unsubscribed")
        except Exception as e:
            logger.error(f"Error unsubscribing: {e}")
        
        logger.info("Message bus debug completed")
        
    except ImportError as e:
        logger.error(f"Failed to import message_bus: {e}")
    except Exception as e:
        logger.error(f"Error in message bus debug: {e}")
        logger.error(traceback.format_exc())

async def debug_system_info() -> None:
    """Debug system information."""
    logger.info("\n=== System Information ===")
    
    try:
        import platform
        import multiprocessing
        
        logger.info(f"Python version: {platform.python_version()}")
        logger.info(f"Platform: {platform.platform()}")
        logger.info(f"CPU count: {multiprocessing.cpu_count()}")
        logger.info(f"Process ID: {os.getpid()}")
        
        # Event loop info
        loop = asyncio.get_running_loop()
        logger.info(f"Event loop: {loop}")
        logger.info(f"Event loop time: {loop.time()}")
        
        # Environment variables
        logger.info("Relevant environment variables:")
        for var in ["PYTHONPATH", "TRADING_SYSTEM_MESSAGE_BUS_MODE"]:
            logger.info(f"  {var}: {os.environ.get(var, 'Not set')}")
        
    except Exception as e:
        logger.error(f"Error getting system info: {e}")
        logger.error(traceback.format_exc())

async def run_debug() -> None:
    """Run all debug functions."""
    logger.info("="*80)
    logger.info(" High-Reliability Components Debug")
    logger.info("="*80)
    
    # System info
    await debug_system_info()
    
    # Debug each component
    await debug_circuit_breaker()
    await debug_connection_pool()
    await debug_message_bus()
    
    logger.info("\n"+"="*80)
    logger.info(" Debug completed - check component_debug.log for full details")
    logger.info("="*80)

async def main():
    """Main function."""
    try:
        # Set overall timeout
        await asyncio.wait_for(run_debug(), timeout=30.0)
        return 0
    except asyncio.TimeoutError:
        logger.error("Debug process timed out after 30 seconds")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error in debug process: {e}")
        logger.error(traceback.format_exc())
        return 1

if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Debug interrupted by user")
        sys.exit(130) 