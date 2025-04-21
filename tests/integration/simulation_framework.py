"""
Simulation Framework

This module provides a framework for simulating a complete trading system
with controlled time flow, event injection, and system monitoring.
"""

import asyncio
import datetime
import logging
import os
import sys
import time
import threading
from enum import Enum
from typing import Dict, List, Any, Callable, Optional, Set, Tuple, Union
import random

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("integration_test.log")
    ]
)

logger = logging.getLogger("integration.simulation")

# Import core components and types
from trading_system.core.message_bus import (
    message_bus, get_async_message_bus, MessageBusMode
)

# Import our mock types
from trading_system.tests.integration.mocks.mock_message_types import MessageTypes
from trading_system.tests.integration.mocks.mock_component_status import ComponentStatus

# Import mock components instead of real ones
from trading_system.tests.integration.mocks.mock_market_data import MockMarketDataSubsystem
from trading_system.tests.integration.mocks.mock_strategy import MockStrategyEngine
from trading_system.tests.integration.mocks.mock_risk import MockRiskManager
from trading_system.tests.integration.mocks.mock_position import MockPositionManager
from trading_system.tests.integration.mocks.mock_execution import MockExecutionEngine
from trading_system.tests.integration.mocks.mock_order import Order, OrderStatus, OrderSide


class ScenarioType(Enum):
    """Types of simulation scenarios."""
    NORMAL_TRADING = "normal_trading"
    MARKET_DATA_FAILURE = "market_data_failure"
    API_FAILURE = "api_failure" 
    RISK_LIMIT_BREACH = "risk_limit_breach"
    ORDER_EXECUTION_FAILURE = "order_execution_failure"


class SystemEvent(Enum):
    """Types of system events that can be injected."""
    MARKET_DATA_DISCONNECT = "market_data_disconnect"
    MARKET_DATA_RECONNECT = "market_data_reconnect"
    API_TIMEOUT = "api_timeout"
    API_ERROR = "api_error"
    API_RECOVERY = "api_recovery"
    RISK_LIMIT_REACHED = "risk_limit_reached"
    ORDER_REJECTION = "order_rejection"
    PARTIAL_FILL = "partial_fill"
    MARKET_PRICE_SHOCK = "market_price_shock"
    SYSTEM_SHUTDOWN = "system_shutdown"
    SYSTEM_RESTART = "system_restart"


class SimulationFramework:
    """
    Framework for simulating a complete trading system.
    
    This class sets up all trading system components, provides methods to control
    simulation time, inject events, and monitor component interactions.
    """
    
    def __init__(self, scenario_type: ScenarioType, config: Dict[str, Any] = None):
        """
        Initialize the simulation framework.
        
        Args:
            scenario_type: Type of scenario to simulate
            config: Configuration for the simulation
        """
        self.scenario_type = scenario_type
        self.config = config or {}
        
        # Initialize components
        self.components = {}
        self.component_statuses = {}
        self.event_queue = asyncio.Queue()
        self.stop_event = threading.Event()
        
        # Simulation state
        self.simulation_time = datetime.datetime.now()
        self.simulation_start_time = self.simulation_time
        self.simulation_end_time = self.simulation_time + datetime.timedelta(seconds=120)  # Default 2 minutes
        self.time_acceleration = self.config.get("time_acceleration", 10.0)  # Default 10x acceleration
        self.scheduled_events = []  # (time, event)
        
        # Event handlers
        self.event_handlers = {
            SystemEvent.MARKET_DATA_DISCONNECT: self._handle_market_data_disconnect,
            SystemEvent.MARKET_DATA_RECONNECT: self._handle_market_data_reconnect,
            SystemEvent.API_TIMEOUT: self._handle_api_timeout,
            SystemEvent.API_ERROR: self._handle_api_error,
            SystemEvent.API_RECOVERY: self._handle_api_recovery,
            SystemEvent.RISK_LIMIT_REACHED: self._handle_risk_limit_reached,
            SystemEvent.ORDER_REJECTION: self._handle_order_rejection,
            SystemEvent.PARTIAL_FILL: self._handle_partial_fill,
            SystemEvent.MARKET_PRICE_SHOCK: self._handle_market_price_shock,
            SystemEvent.SYSTEM_SHUTDOWN: self._handle_system_shutdown,
            SystemEvent.SYSTEM_RESTART: self._handle_system_restart,
            "MARKET_DATA_UPDATE_QUALITY": self._handle_market_data_update_quality
        }
        
        # Setup message capturing and logging
        self.captured_messages = []
        self.message_handlers = {}
        
        # Test results
        self.test_results = {
            "passed": False,
            "failures": [],
            "error_count": 0,
            "recovery_count": 0,
            "messages_processed": 0,
            "orders_executed": 0,
            "time_elapsed": 0,
            "simulation_steps": 0
        }
        
        logger.info(f"Simulation framework initialized for scenario: {scenario_type.value}")
    
    async def setup(self):
        """
        Set up the simulation environment.
        
        This initializes all trading system components and connects them.
        """
        logger.info("Setting up simulation environment...")
        
        # Configure for simulation
        os.environ["TRADING_SYSTEM_MODE"] = "simulation"
        os.environ["TRADING_SYSTEM_MESSAGE_BUS_MODE"] = "in_memory"
        
        # Initialize the message bus
        self.async_message_bus = get_async_message_bus()
        
        # Initialize components based on scenario
        await self._initialize_components()
        
        # Subscribe to relevant message types
        await self._setup_message_handlers()
        
        # Schedule scenario-specific events
        await self._schedule_scenario_events()
        
        logger.info("Simulation environment setup complete")
        return True
        
    async def _initialize_components(self):
        """Initialize all system components based on the scenario type."""
        # Create mock/simulation versions of all subsystems
        try:
            # Market Data Subsystem (with simulation support)
            self.components["market_data"] = self._create_market_data_subsystem()
            
            # Strategy Engine
            self.components["strategy"] = self._create_strategy_engine()
            
            # Risk Manager
            self.components["risk"] = self._create_risk_manager()
            
            # Position Manager
            self.components["position"] = self._create_position_manager()
            
            # Execution Engine
            self.components["execution"] = self._create_execution_engine()
            
            # Initialize all components
            for name, component in self.components.items():
                status = await component.initialize()
                self.component_statuses[name] = status
                logger.info(f"Component {name} initialized with status: {status}")
                
        except Exception as e:
            logger.error(f"Error initializing components: {str(e)}", exc_info=True)
            raise
            
    def _create_market_data_subsystem(self):
        """Create a mock market data subsystem with simulation capabilities."""
        # Use our mock implementation directly
        return MockMarketDataSubsystem(self.config.get("market_data", {}))
        
    def _create_strategy_engine(self):
        """Create a strategy engine for simulation."""
        # Use our mock implementation directly
        return MockStrategyEngine(self.config.get("strategy", {}))
        
    def _create_risk_manager(self):
        """Create a risk manager for simulation."""
        # Use our mock implementation directly
        return MockRiskManager(self.config.get("risk", {}))
        
    def _create_position_manager(self):
        """Create a position manager for simulation."""
        # Use our mock implementation directly
        return MockPositionManager(self.config.get("position", {}))
        
    def _create_execution_engine(self):
        """Create an execution engine for simulation."""
        # Use our mock implementation directly
        return MockExecutionEngine(self.config.get("execution", {}))
    
    async def _setup_message_handlers(self):
        """
        Set up message handlers to monitor system interactions.
        
        This allows monitoring of all messages flowing through the system.
        """
        # Set up handlers for all message types we want to monitor
        for message_type in MessageTypes:
            await self.async_message_bus.subscribe(
                message_type, self._message_handler
            )
    
    async def _message_handler(self, message_type, data):
        """
        Generic message handler that logs and captures all messages.
        
        Args:
            message_type: Type of message
            data: Message data
        """
        self.captured_messages.append((message_type, data))
        self.test_results["messages_processed"] += 1
        
        # If there's a specific handler for this message type, call it
        if message_type in self.message_handlers:
            await self.message_handlers[message_type](message_type, data)
    
    async def _schedule_scenario_events(self):
        """Schedule events based on the selected scenario type."""
        # Define scenario-specific events
        if self.scenario_type == ScenarioType.NORMAL_TRADING:
            # No special events for normal trading
            pass
            
        elif self.scenario_type == ScenarioType.MARKET_DATA_FAILURE:
            # Schedule market data disconnection and reconnection
            await self.schedule_event(SystemEvent.MARKET_DATA_DISCONNECT, 30)
            await self.schedule_event(SystemEvent.MARKET_DATA_RECONNECT, 45)
            
        elif self.scenario_type == ScenarioType.API_FAILURE:
            # Schedule API timeouts and errors
            await self.schedule_event(SystemEvent.API_TIMEOUT, 20)
            await self.schedule_event(SystemEvent.API_ERROR, 40)
            await self.schedule_event(SystemEvent.API_RECOVERY, 60)
            
        elif self.scenario_type == ScenarioType.RISK_LIMIT_BREACH:
            # Schedule risk limit breach
            await self.schedule_event(SystemEvent.RISK_LIMIT_REACHED, 25)
            
        elif self.scenario_type == ScenarioType.ORDER_EXECUTION_FAILURE:
            # Schedule order rejection and partial fill
            await self.schedule_event(SystemEvent.ORDER_REJECTION, 15)
            await self.schedule_event(SystemEvent.PARTIAL_FILL, 35)
            
        # Common events for all scenarios
        await self.schedule_event(SystemEvent.MARKET_PRICE_SHOCK, 5)
    
    async def schedule_event(self, event, seconds_from_now: int, data: Dict[str, Any] = None):
        """
        Schedule an event to occur at a specific time.
        
        Args:
            event: Event to trigger (SystemEvent or string event name)
            seconds_from_now: Seconds from now to trigger the event
            data: Additional event data
        """
        # Convert string event names to SystemEvent if needed
        if isinstance(event, str):
            try:
                event = SystemEvent[event]
            except KeyError:
                logger.warning(f"Unknown event name: {event}, using as custom event")
        
        # Ensure seconds_from_now is an integer
        seconds_from_now = int(seconds_from_now)
        
        # Calculate event time
        event_time = self.simulation_time + datetime.timedelta(seconds=seconds_from_now)
        self.scheduled_events.append((event_time, event, data or {}))
        
        # Log the scheduled event
        event_name = event.value if hasattr(event, 'value') else str(event)
        logger.info(f"Scheduled event {event_name} for {event_time}")
    
    async def run(self, duration=None):
        """
        Execute the simulation for the specified duration.
        
        Parameters
        ----------
        duration : int, optional
            Duration in simulation seconds. If not provided, runs until simulation_end_time.
        """
        logger.info(f"Starting simulation with scenario: {self.scenario_type.value}")
        
        # Initialize and start all components
        await self._initialize_components()
        await self._start_components()
        
        # Calculate end time based on duration parameter or predetermined end time
        if duration is not None:
            self.simulation_end_time = self.simulation_time + datetime.timedelta(seconds=duration)
            logger.info(f"Simulation will run for {duration} seconds until {self.simulation_end_time}")
        else:
            logger.info(f"Simulation will run until {self.simulation_end_time}")
        
        # Main simulation loop
        try:
            while self.simulation_time < self.simulation_end_time:
                # Perform a simulation step
                await self.simulation_step()
                
                # Monitor component status
                await self._monitor_components()
                
                # Small delay to avoid CPU overload in case of very fast simulation
                await asyncio.sleep(0)
                
                # Log progress periodically
                elapsed = (self.simulation_time - self.simulation_start_time).total_seconds()
                total = (self.simulation_end_time - self.simulation_start_time).total_seconds()
                if int(elapsed) % 10 == 0:
                    logger.info(f"Simulation progress: {elapsed:.1f}/{total:.1f} seconds ({elapsed/total*100:.1f}%)")
            
            logger.info("Simulation duration completed")
        except Exception as e:
            logger.error(f"Simulation error: {e}")
            raise
        finally:
            # Ensure components are properly shut down
            await self._shutdown_components()
        
        # Evaluate results
        success, details = await self._evaluate_results()
        
        logger.info(f"Simulation complete. Success: {success}")
        if details:
            logger.info(f"Details: {details}")
        
        return success, details
    
    async def _start_components(self):
        """Start all system components."""
        for name, component in self.components.items():
            try:
                status = await component.start()
                self.component_statuses[name] = status
                logger.info(f"Component {name} started with status: {status}")
            except Exception as e:
                logger.error(f"Error starting component {name}: {str(e)}", exc_info=True)
                self.test_results["failures"].append(f"Failed to start {name}: {str(e)}")
    
    async def _advance_time(self):
        """Advance the simulation time."""
        # In real mode, we use wall clock time
        # In accelerated mode, we move faster
        step_seconds = 0.1 * self.time_acceleration
        self.simulation_time += datetime.timedelta(seconds=step_seconds)
    
    async def _process_scheduled_events(self):
        """Process any events that are scheduled for the current time."""
        # Find events to trigger
        events_to_trigger = []
        remaining_events = []
        
        for event_time, event, data in self.scheduled_events:
            if event_time <= self.simulation_time:
                events_to_trigger.append((event, data))
            else:
                remaining_events.append((event_time, event, data))
        
        # Update the list of scheduled events
        self.scheduled_events = remaining_events
        
        # Trigger the events
        for event, data in events_to_trigger:
            await self._trigger_event(event, data)
    
    async def _process_event_queue(self):
        """Process events in the event queue."""
        # Process up to 10 events per step to avoid endless loop
        for _ in range(10):
            try:
                # Non-blocking get
                event, data = self.event_queue.get_nowait()
                await self._handle_event(event, data)
                self.event_queue.task_done()
            except asyncio.QueueEmpty:
                break
            except Exception as e:
                logger.error(f"Error processing event queue: {str(e)}", exc_info=True)
    
    async def _monitor_components(self):
        """Monitor the status of all components."""
        for name, component in self.components.items():
            try:
                status = await component.get_status()
                
                # Check for status changes
                if status != self.component_statuses.get(name):
                    logger.info(f"Component {name} status changed: {status}")
                    
                    # Record recovery events
                    if (self.component_statuses.get(name) in 
                        [ComponentStatus.ERROR, ComponentStatus.DEGRADED] and
                        status == ComponentStatus.OPERATIONAL):
                        self.test_results["recovery_count"] += 1
                    
                    # Record error events
                    if status in [ComponentStatus.ERROR, ComponentStatus.DEGRADED]:
                        self.test_results["error_count"] += 1
                    
                    self.component_statuses[name] = status
            except Exception as e:
                logger.error(f"Error monitoring component {name}: {str(e)}", exc_info=True)
    
    async def simulation_step(self):
        """Execute a simulation step."""
        try:
            # Update the simulation time
            self.simulation_time += datetime.timedelta(seconds=1 * self.time_acceleration)
            
            # Execute scheduled events
            await self._process_scheduled_events()
            
            # Process any events from the queue
            await self._process_event_queue()
            
            # Record the step
            self.test_results["simulation_steps"] += 1
            
        except Exception as e:
            logger.error(f"Error in simulation step: {str(e)}", exc_info=True)
            self.test_results["error_count"] += 1
    
    async def _shutdown_components(self):
        """Shut down all system components."""
        for name, component in reversed(list(self.components.items())):
            try:
                status = await component.stop()
                self.component_statuses[name] = status
                logger.info(f"Component {name} stopped with status: {status}")
            except Exception as e:
                logger.error(f"Error stopping component {name}: {str(e)}", exc_info=True)
    
    async def _trigger_event(self, event, data: Dict[str, Any] = None):
        """
        Trigger a system event.
        
        Args:
            event: Event to trigger (can be SystemEvent or string)
            data: Additional event data
        """
        data = data or {}
        
        # Get event name for logging
        if hasattr(event, 'value'):
            event_name = event.value
        else:
            event_name = str(event)
            
        logger.info(f"Triggering event: {event_name}")
        await self.event_queue.put((event, data))
    
    async def _handle_event(self, event, data: Dict[str, Any]):
        """
        Handle a system event.
        
        Args:
            event: Event to handle (can be SystemEvent or string)
            data: Event data
        """
        # Get event name for logging
        if hasattr(event, 'value'):
            event_name = event.value
        else:
            event_name = str(event)
            
        logger.info(f"Handling event: {event_name}")
        
        # Call the appropriate handler
        if event in self.event_handlers:
            try:
                await self.event_handlers[event](data)
            except Exception as e:
                logger.error(f"Error handling event {event_name}: {str(e)}", exc_info=True)
                self.test_results["failures"].append(f"Event handler failed for {event_name}: {str(e)}")
        else:
            logger.warning(f"No handler for event: {event_name}")
    
    async def _handle_market_data_disconnect(self, data):
        """Handle market data disconnection event."""
        if "market_data" in self.components:
            self.components["market_data"].set_connected(False)
            logger.info("Market data disconnected")
    
    async def _handle_market_data_reconnect(self, data):
        """Handle market data reconnection event."""
        if "market_data" in self.components:
            self.components["market_data"].set_connected(True)
            logger.info("Market data reconnected")
    
    async def _handle_api_timeout(self, data):
        """Handle API timeout event."""
        if "execution" in self.components:
            # Set latency temporarily high
            self.components["execution"].set_execution_parameter("fill_delay_seconds.max", 5.0)
            logger.info("API timeout simulated")
    
    async def _handle_api_error(self, data):
        """Handle API error event."""
        if "execution" in self.components:
            # Increase error probability temporarily
            self.components["execution"].set_execution_parameter("error_probability", 0.8)
            logger.info("API error probability increased")
    
    async def _handle_api_recovery(self, data):
        """Handle API recovery event."""
        if "execution" in self.components:
            # Reset to normal values
            self.components["execution"].set_execution_parameter("error_probability", 0.05)
            self.components["execution"].set_execution_parameter("fill_delay_seconds.max", 1.0)
            logger.info("API parameters reset to normal")
    
    async def _handle_risk_limit_reached(self, data):
        """Handle risk limit reached event."""
        if "risk" in self.components:
            await self.components["risk"].simulate_limit_breach()
    
    async def _handle_order_rejection(self, data):
        """Handle order rejection event."""
        if "execution" in self.components:
            # Increase rejection probability temporarily
            self.components["execution"].set_execution_parameter("rejection_probability", 0.8)
            logger.info("Order rejection probability increased")
    
    async def _handle_partial_fill(self, data):
        """Handle partial fill event."""
        if "execution" in self.components:
            # Set partial fill probability to 100%
            self.components["execution"].set_execution_parameter("partial_fill_probability", 1.0)
            logger.info("Partial fill probability set to 100%")
    
    async def _handle_market_price_shock(self, data):
        """Handle market price shock event."""
        if "market_data" in self.components:
            symbol = data.get("symbol", "BTC/USDT")
            percent = data.get("percent", random.uniform(-5.0, 5.0))
            self.components["market_data"].trigger_price_spike(symbol, percent)
            logger.info(f"Triggered price shock: {symbol} {percent}%")
    
    async def _handle_system_shutdown(self, data):
        """Handle system shutdown event."""
        for name, component in reversed(list(self.components.items())):
            await component.stop()
    
    async def _handle_system_restart(self, data):
        """Handle system restart event."""
        for name, component in self.components.items():
            await component.start()
    
    async def _handle_market_data_update_quality(self, data):
        """Handle market data quality update event."""
        if "market_data" in self.components:
            quality = data.get("quality", random.uniform(0.5, 1.0))
            self.components["market_data"].set_data_quality(quality)
            logger.info(f"Market data quality set to {quality:.2f}")
    
    async def _evaluate_results(self):
        """Evaluate test results to determine if the test passed."""
        # Define scenario-specific success criteria
        if self.scenario_type == ScenarioType.NORMAL_TRADING:
            # For normal trading, all components should end in OPERATIONAL state
            all_operational = all(
                status == ComponentStatus.OPERATIONAL or status == ComponentStatus.SHUTDOWN
                for status in self.component_statuses.values()
            )
            self.test_results["passed"] = all_operational and len(self.test_results["failures"]) == 0
            
        elif self.scenario_type == ScenarioType.MARKET_DATA_FAILURE:
            # Should recover from market data failure
            self.test_results["passed"] = (
                self.test_results["recovery_count"] > 0 and
                self.component_statuses.get("market_data") == ComponentStatus.OPERATIONAL
            )
            
        elif self.scenario_type == ScenarioType.API_FAILURE:
            # Should recover from API failures and execute orders
            self.test_results["passed"] = (
                self.test_results["recovery_count"] > 0 and
                self.test_results["orders_executed"] > 0
            )
            
        elif self.scenario_type == ScenarioType.RISK_LIMIT_BREACH:
            # Risk limits should prevent dangerous trades
            self.test_results["passed"] = (
                "risk" in self.components and
                self.components["risk"].limits_enforced
            )
            
        elif self.scenario_type == ScenarioType.ORDER_EXECUTION_FAILURE:
            # Should handle order failures gracefully
            self.test_results["passed"] = (
                "execution" in self.components and
                self.components["execution"].handled_failures
            )
            
        # Log test results
        if self.test_results["passed"]:
            logger.info(f"Test PASSED for scenario {self.scenario_type.value}")
        else:
            logger.error(f"Test FAILED for scenario {self.scenario_type.value}")
            for failure in self.test_results["failures"]:
                logger.error(f"Failure: {failure}")
        
        # Add detailed stats to the results
        self.test_results["component_statuses"] = {
            name: status.value for name, status in self.component_statuses.items()
        }
        
        return self.test_results["passed"], self.test_results
    
    def get_results(self):
        """Get test results."""
        return self.test_results


# Example usage
async def run_simulation_example():
    """Run an example simulation."""
    # Create and run a normal trading scenario
    sim = SimulationFramework(ScenarioType.NORMAL_TRADING)
    await sim.setup()
    results = await sim.run(duration=60)
    
    # Print results
    print(f"Simulation results: {'PASSED' if results[0] else 'FAILED'}")
    print(f"Messages processed: {results[1]['messages_processed']}")
    print(f"Orders executed: {results[1]['orders_executed']}")
    print(f"Errors: {results[1]['error_count']}")
    print(f"Recoveries: {results[1]['recovery_count']}")
    
    return results


if __name__ == "__main__":
    # Run the example
    asyncio.run(run_simulation_example()) 