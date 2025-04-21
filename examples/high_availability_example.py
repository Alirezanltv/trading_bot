"""
High Availability Example

This example demonstrates how to implement high availability features
in the trading system using component failover and health monitoring.
"""

import asyncio
import logging
import random
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List

from trading_system.core.component import Component, ComponentStatus
from trading_system.core.message_bus import MessageBus, MessageTypes
from trading_system.core.high_availability import (
    get_failover_manager, 
    FailoverMode, 
    ComponentHealth,
    CircuitBreaker
)
from trading_system.monitoring.health_dashboard import create_health_dashboard

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("high_availability_example")


class ExampleMarketDataComponent(Component):
    """Example market data component to demonstrate failover."""
    
    def __init__(self, name: str, primary: bool = True):
        """
        Initialize the example component.
        
        Args:
            name: Component name
            primary: Whether this is a primary component
        """
        super().__init__(name=name)
        self.primary = primary
        self.simulated_health = ComponentHealth.HEALTHY
        self.failure_countdown = None
        
    async def initialize(self) -> bool:
        """
        Initialize the component.
        
        Returns:
            bool: Success flag
        """
        logger.info(f"Initializing {self.name}")
        await asyncio.sleep(1)  # Simulate initialization delay
        self.status = ComponentStatus.INITIALIZED
        return True
    
    async def start(self) -> bool:
        """
        Start the component.
        
        Returns:
            bool: Success flag
        """
        logger.info(f"Starting {self.name}")
        await asyncio.sleep(1)  # Simulate startup delay
        return True
    
    async def stop(self) -> bool:
        """
        Stop the component.
        
        Returns:
            bool: Success flag
        """
        logger.info(f"Stopping {self.name}")
        await asyncio.sleep(1)  # Simulate shutdown delay
        self.status = ComponentStatus.STOPPED
        return True
    
    async def get_market_data(self, symbol: str) -> Dict[str, Any]:
        """
        Get market data for a symbol.
        
        Args:
            symbol: The symbol to get data for
            
        Returns:
            Dict containing market data
        """
        # Simulate failure if health is not good
        if self.simulated_health == ComponentHealth.FAILED:
            raise Exception(f"Component {self.name} is in FAILED state")
            
        # Simulate degraded performance
        if self.simulated_health == ComponentHealth.DEGRADED:
            await asyncio.sleep(random.uniform(0.5, 1.5))
        else:
            await asyncio.sleep(random.uniform(0.1, 0.3))
        
        # Return simulated market data
        return {
            "symbol": symbol,
            "price": random.uniform(30000, 40000),
            "volume": random.uniform(10, 100),
            "timestamp": datetime.now().isoformat(),
            "source": self.name
        }
    
    async def simulate_health_change(self, new_health: ComponentHealth, duration_seconds: int = 30):
        """
        Simulate a health change for testing failover.
        
        Args:
            new_health: The new health state
            duration_seconds: How long to maintain this health state
        """
        logger.warning(f"Simulating {new_health.value} state for {self.name} "
                       f"for {duration_seconds} seconds")
        
        # Save previous health
        previous_health = self.simulated_health
        
        # Set new health
        self.simulated_health = new_health
        
        # Update component status
        if new_health == ComponentHealth.FAILED:
            self.status = ComponentStatus.ERROR
        elif new_health == ComponentHealth.DEGRADED:
            self.status = ComponentStatus.DEGRADED
        else:
            self.status = ComponentStatus.INITIALIZED
        
        # Set countdown for recovery
        self.failure_countdown = duration_seconds
        
        # Wait for duration then restore
        await asyncio.sleep(duration_seconds)
        
        # Restore previous health
        logger.info(f"Restoring {self.name} to {previous_health.value} state")
        self.simulated_health = previous_health
        
        # Update component status
        if previous_health == ComponentHealth.HEALTHY:
            self.status = ComponentStatus.INITIALIZED
        elif previous_health == ComponentHealth.DEGRADED:
            self.status = ComponentStatus.DEGRADED
        
        self.failure_countdown = None


class ExampleTradingSystem:
    """Example trading system demonstrating high availability."""
    
    def __init__(self):
        """Initialize the example trading system."""
        # Create message bus
        self.message_bus = MessageBus()
        
        # Create market data components
        self.primary_market_data = ExampleMarketDataComponent(
            name="PrimaryMarketData",
            primary=True
        )
        
        self.backup_market_data = ExampleMarketDataComponent(
            name="BackupMarketData",
            primary=False
        )
        
        # Get failover manager
        self.failover_manager = get_failover_manager(self.message_bus)
        
        # Create health dashboard
        self.dashboard = create_health_dashboard(
            message_bus=self.message_bus,
            config={
                "port": 8080,
                "theme": "dark"
            }
        )
        
        # Circuit breaker for API calls
        self.api_circuit_breaker = CircuitBreaker(
            name="external_api",
            failure_threshold=3,
            reset_timeout_seconds=30
        )
        
        # Active market data component (set by failover manager)
        self.active_market_data = None
    
    async def initialize(self):
        """Initialize the system."""
        logger.info("Initializing example trading system")
        
        # Initialize components
        await self.primary_market_data.initialize()
        await self.backup_market_data.initialize()
        
        # Register components with failover manager
        self.failover_manager.register_component(
            component_id="primary_market_data",
            component=self.primary_market_data,
            failover_group="market_data",
            is_primary=True,
            failover_mode=FailoverMode.ACTIVE_PASSIVE
        )
        
        self.failover_manager.register_component(
            component_id="backup_market_data",
            component=self.backup_market_data,
            failover_group="market_data",
            is_primary=False,
            failover_mode=FailoverMode.ACTIVE_PASSIVE
        )
        
        # Register health checks
        self.failover_manager.register_health_check(
            "primary_market_data",
            self._check_market_data_health
        )
        
        self.failover_manager.register_health_check(
            "backup_market_data",
            self._check_market_data_health
        )
        
        # Register failover event listener
        self.failover_manager.register_event_listener(
            "failover",
            self._handle_failover_event
        )
        
        # Initialize dashboard
        await self.dashboard.initialize()
        
        # Start failover manager
        self.failover_manager.start()
        
        # Start dashboard
        await self.dashboard.start()
        
        # Set initial active component
        active_component = self.failover_manager.failover_groups["market_data"]["active_component"]
        if active_component == "primary_market_data":
            self.active_market_data = self.primary_market_data
        else:
            self.active_market_data = self.backup_market_data
        
        logger.info(f"System initialized, active market data: {self.active_market_data.name}")
    
    def _check_market_data_health(self, component) -> ComponentHealth:
        """
        Custom health check for market data components.
        
        Args:
            component: The component to check
            
        Returns:
            ComponentHealth: The component health status
        """
        # This is a simplified example - in a real system you would
        # check things like response times, error rates, etc.
        return component.simulated_health
    
    async def _handle_failover_event(self, event_data):
        """
        Handle failover events.
        
        Args:
            event_data: Event data
        """
        logger.warning(f"Failover event detected: {event_data}")
        
        # Update active component
        if event_data["failover_group"] == "market_data":
            if event_data["new_component"] == "primary_market_data":
                self.active_market_data = self.primary_market_data
            else:
                self.active_market_data = self.backup_market_data
                
            logger.info(f"Switched active market data to: {self.active_market_data.name}")
    
    async def get_market_data(self, symbol: str) -> Dict[str, Any]:
        """
        Get market data with circuit breaker protection.
        
        Args:
            symbol: The symbol to get data for
            
        Returns:
            Dict containing market data
        """
        try:
            # Use circuit breaker to protect against repeated failures
            return await self.api_circuit_breaker.execute(
                lambda: self.active_market_data.get_market_data(symbol),
                fallback=lambda: self._get_fallback_market_data(symbol)
            )
        except Exception as e:
            logger.error(f"Error getting market data: {str(e)}")
            return self._get_fallback_market_data(symbol)
    
    async def _get_fallback_market_data(self, symbol: str) -> Dict[str, Any]:
        """
        Fallback method for market data when all else fails.
        
        Args:
            symbol: The symbol to get data for
            
        Returns:
            Dict containing fallback market data
        """
        logger.warning(f"Using fallback market data for {symbol}")
        
        # In a real system, this might load cached data from a database
        return {
            "symbol": symbol,
            "price": 0.0,
            "volume": 0.0,
            "timestamp": datetime.now().isoformat(),
            "source": "fallback_cache",
            "is_fallback": True
        }
    
    async def run_demo(self):
        """Run the high availability demo."""
        logger.info("Starting high availability demo")
        
        try:
            # Run for 5 minutes
            end_time = datetime.now() + timedelta(minutes=5)
            
            while datetime.now() < end_time:
                # Get market data every second to show active component
                symbol = "BTC/USDT"
                data = await self.get_market_data(symbol)
                
                logger.info(f"Market data from {data['source']}: {symbol} = ${data['price']:.2f}")
                
                # Periodically simulate failures in the primary market data
                if random.random() < 0.01:  # 1% chance each second
                    duration = random.randint(10, 20)
                    asyncio.create_task(
                        self.primary_market_data.simulate_health_change(
                            ComponentHealth.FAILED, 
                            duration
                        )
                    )
                
                # Periodically simulate degraded performance
                elif random.random() < 0.03:  # 3% chance each second
                    duration = random.randint(15, 30)
                    asyncio.create_task(
                        self.primary_market_data.simulate_health_change(
                            ComponentHealth.DEGRADED, 
                            duration
                        )
                    )
                
                # Sleep before next iteration
                await asyncio.sleep(1)
        
        except KeyboardInterrupt:
            logger.info("Demo interrupted by user")
        finally:
            # Stop everything
            logger.info("Stopping demo")
            self.failover_manager.stop()
            await self.dashboard.stop()
            await self.primary_market_data.stop()
            await self.backup_market_data.stop()


async def main():
    """Run the high availability example."""
    print("=" * 80)
    print("HIGH AVAILABILITY EXAMPLE")
    print("=" * 80)
    print("\nThis example demonstrates high availability features:")
    print("- Component health monitoring")
    print("- Automatic failover between primary and backup components")
    print("- Circuit breakers for protecting against cascading failures")
    print("- Real-time health dashboard")
    print("\nThe dashboard will be available at: http://localhost:8080")
    print("\nSimulated failures will occur randomly to demonstrate failover.")
    print("Press Ctrl+C to stop the demo.")
    print("=" * 80)
    
    # Create and run the trading system
    system = ExampleTradingSystem()
    await system.initialize()
    await system.run_demo()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExample stopped by user") 