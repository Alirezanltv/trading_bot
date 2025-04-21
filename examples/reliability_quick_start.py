"""
High Reliability Quick Start Example

This script demonstrates the key high reliability components working together
in a simple example. It demonstrates:

1. Circuit breaker for fault tolerance
2. Connection pool for managing connections
3. Message bus for inter-component communication
4. Transaction verification for reliable operations

This is a simplified example for getting started with the high reliability features.
"""

import asyncio
import logging
import random
import sys
import time
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("reliability_quick_start")

# Import core components
from trading_system.core.circuit_breaker import CircuitBreaker, CircuitState
from trading_system.exchange.connection_pool import ConnectionPool, ConnectionStatus
from trading_system.core import message_bus

# Initialize message bus
message_bus.initialize(in_memory=True)

class SimpleClient:
    """A simple client that sometimes fails randomly."""
    
    def __init__(self, name="client1", fail_rate=0.3):
        self.name = name
        self.fail_rate = fail_rate
        self.counter = 0
        logger.info(f"Created client: {name}")
    
    async def get_data(self, key=None):
        """Get some data, sometimes failing."""
        self.counter += 1
        
        # Simulate API call delay
        await asyncio.sleep(0.1)
        
        # Sometimes fail
        if random.random() < self.fail_rate:
            logger.warning(f"Client {self.name} is failing request {self.counter}")
            raise Exception(f"Simulated failure in get_data (request {self.counter})")
        
        # Return mock data
        data = {
            "client": self.name,
            "request_id": self.counter,
            "key": key or "default",
            "value": random.randint(1, 100),
            "timestamp": time.time()
        }
        logger.info(f"Client {self.name} successfully returned data for request {self.counter}")
        return data
    
    async def post_data(self, data):
        """Post some data, sometimes failing."""
        self.counter += 1
        
        # Simulate API call delay
        await asyncio.sleep(0.2)
        
        # Sometimes fail
        if random.random() < self.fail_rate:
            logger.warning(f"Client {self.name} is failing request {self.counter}")
            raise Exception(f"Simulated failure in post_data (request {self.counter})")
        
        # Return mock response
        response = {
            "client": self.name,
            "request_id": self.counter,
            "status": "success",
            "data_id": f"data_{random.randint(1000, 9999)}",
            "timestamp": time.time()
        }
        logger.info(f"Client {self.name} successfully posted data for request {self.counter}")
        return response

class DataService:
    """A service that fetches and posts data with high reliability."""
    
    def __init__(self):
        # Create a connection pool
        self.pool = ConnectionPool(
            name="data_service",
            create_client_func=lambda: SimpleClient(fail_rate=0.3),
            config={
                "min_connections": 2,
                "max_connections": 5,
                "connection_timeout": 5,
                "idle_timeout": 30,
                "max_retries": 3
            }
        )
        
        # Create circuit breakers
        self.get_breaker = CircuitBreaker(
            name="get_data_breaker",
            failure_threshold=3,
            reset_timeout=5.0,
            half_open_max_requests=2,
            success_threshold=2
        )
        
        self.post_breaker = CircuitBreaker(
            name="post_data_breaker",
            failure_threshold=3,
            reset_timeout=5.0,
            half_open_max_requests=1,
            success_threshold=2
        )
        
        # Setup message handlers
        message_bus.subscribe("data.request", self._handle_data_request)
    
    async def initialize(self):
        """Initialize the service."""
        logger.info("Initializing DataService...")
        await self.pool.initialize()
        logger.info("DataService initialized successfully")
    
    async def get_data(self, key: str) -> Optional[Dict[str, Any]]:
        """Get data with circuit breaker protection."""
        try:
            result = await self.get_breaker.execute(
                self.pool.execute,
                "get_data",
                key
            )
            
            # Publish success event
            message_bus.publish("data.retrieved", {
                "key": key,
                "success": True,
                "data": result
            })
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting data for key {key}: {e}")
            
            # Publish failure event
            message_bus.publish("data.error", {
                "key": key,
                "success": False,
                "error": str(e)
            })
            
            return None
    
    async def post_data(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Post data with circuit breaker protection."""
        try:
            result = await self.post_breaker.execute(
                self.pool.execute,
                "post_data",
                data
            )
            
            # Publish success event
            message_bus.publish("data.posted", {
                "data": data,
                "success": True,
                "result": result
            })
            
            return result
            
        except Exception as e:
            logger.error(f"Error posting data: {e}")
            
            # Publish failure event
            message_bus.publish("data.error", {
                "data": data,
                "success": False,
                "error": str(e)
            })
            
            return None
    
    async def _handle_data_request(self, data):
        """Handle a data request from the message bus."""
        logger.info(f"Received data request: {data}")
        
        if "key" in data:
            # Get data request
            result = await self.get_data(data["key"])
            logger.info(f"Handled get request for key {data['key']}")
        elif "data" in data:
            # Post data request
            result = await self.post_data(data["data"])
            logger.info(f"Handled post request for data")
    
    async def shutdown(self):
        """Shutdown the service."""
        logger.info("Shutting down DataService...")
        await self.pool.stop()
        logger.info("DataService shutdown complete")

class DataMonitor:
    """Monitor data events on the message bus."""
    
    def __init__(self):
        self.events = []
        self.errors = []
        
        # Subscribe to messages
        message_bus.subscribe("data.retrieved", self._on_data_retrieved)
        message_bus.subscribe("data.posted", self._on_data_posted)
        message_bus.subscribe("data.error", self._on_data_error)
    
    def _on_data_retrieved(self, data):
        """Handle data retrieved event."""
        logger.info(f"MONITOR: Data retrieved: {data.get('key')}")
        self.events.append({"type": "retrieved", "data": data})
    
    def _on_data_posted(self, data):
        """Handle data posted event."""
        logger.info(f"MONITOR: Data posted: {data.get('result', {}).get('data_id')}")
        self.events.append({"type": "posted", "data": data})
    
    def _on_data_error(self, data):
        """Handle data error event."""
        logger.warning(f"MONITOR: Error: {data.get('error')}")
        self.errors.append(data)
    
    def get_stats(self):
        """Get monitoring statistics."""
        retrieved = len([e for e in self.events if e["type"] == "retrieved"])
        posted = len([e for e in self.events if e["type"] == "posted"])
        errors = len(self.errors)
        
        return {
            "retrieved": retrieved,
            "posted": posted,
            "errors": errors,
            "total_events": len(self.events)
        }

async def run_demo():
    """Run the demonstration."""
    logger.info("\n" + "="*80)
    logger.info(" High Reliability Quick Start Demo")
    logger.info("="*80 + "\n")
    
    # Create components
    service = DataService()
    monitor = DataMonitor()
    
    # Initialize
    await service.initialize()
    
    try:
        # Run some data operations
        logger.info("\nRunning data operations...")
        
        # Get data operations
        for key in ["alpha", "beta", "gamma", "delta", "epsilon"]:
            try:
                # Add timeout to prevent hanging
                result = await asyncio.wait_for(service.get_data(key), timeout=3.0)
                if result:
                    logger.info(f"Got data for {key}: value={result.get('value')}")
            except asyncio.TimeoutError:
                logger.error(f"Timeout getting data for {key}")
            except Exception as e:
                logger.error(f"Error getting data for {key}: {e}")
            await asyncio.sleep(0.5)
        
        # Post data operations
        for i in range(5):
            data = {
                "id": f"item_{i}",
                "value": random.randint(1, 100),
                "timestamp": time.time()
            }
            
            try:
                # Add timeout to prevent hanging
                result = await asyncio.wait_for(service.post_data(data), timeout=3.0)
                if result:
                    logger.info(f"Posted data: {result.get('data_id')}")
            except asyncio.TimeoutError:
                logger.error(f"Timeout posting data for item_{i}")
            except Exception as e:
                logger.error(f"Error posting data for item_{i}: {e}")
            await asyncio.sleep(0.5)
        
        # Send some requests through the message bus
        logger.info("\nSending requests through the message bus...")
        pending_requests = 3
        for key in ["omega", "sigma", "theta"]:
            message_bus.publish("data.request", {"key": key})
            await asyncio.sleep(1.0)
        
        # Show circuit breaker states
        logger.info("\nCircuit breaker states:")
        logger.info(f"Get Breaker: {service.get_breaker.state}")
        logger.info(f"Post Breaker: {service.post_breaker.state}")
        
        # Show stats
        stats = monitor.get_stats()
        logger.info("\nMonitoring statistics:")
        for key, value in stats.items():
            logger.info(f"  {key}: {value}")
    
    finally:
        # Ensure we always clean up resources
        logger.info("\nShutting down service...")
        await service.shutdown()
    
    logger.info("\n" + "="*80)
    logger.info(" Demo Completed")
    logger.info("="*80)

async def main():
    """Main function."""
    try:
        # Set a global timeout to prevent hanging indefinitely
        await asyncio.wait_for(run_demo(), timeout=60.0)
        return 0
    except asyncio.TimeoutError:
        logger.error("Demo timed out after 60 seconds")
        return 1
    except Exception as e:
        logger.error(f"Error in demo: {e}")
        return 1

if __name__ == "__main__":
    result = asyncio.run(main())
    sys.exit(result) 