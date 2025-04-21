"""
High Availability Module

This module provides high-availability capabilities for the trading system,
including health monitoring, automatic failover, and system recovery.
"""

import asyncio
import time
import logging
import threading
from enum import Enum
from typing import Dict, List, Any, Optional, Callable, Set, Tuple
from datetime import datetime, timedelta

from trading_system.core.component import Component, ComponentStatus
from trading_system.core.logging import get_logger
from trading_system.core.message_bus import MessageBus, MessageTypes
from trading_system.core.config import Config

logger = get_logger("core.high_availability")

class FailoverMode(Enum):
    """Failover modes for components."""
    ACTIVE_PASSIVE = "active_passive"  # One active, one standby
    ACTIVE_ACTIVE = "active_active"    # Multiple active instances
    N_PLUS_1 = "n_plus_1"              # N active instances plus 1 spare
    CLUSTER = "cluster"                # Clustered redundancy


class ComponentHealth(Enum):
    """Health status levels for components."""
    HEALTHY = "healthy"              # Fully operational
    DEGRADED = "degraded"            # Operating with reduced capability
    WARNING = "warning"              # Operating but showing warning signs
    CRITICAL = "critical"            # Nearly failing
    FAILED = "failed"                # Component has failed


class FailoverManager:
    """
    Manages high availability and failover for system components.
    
    Responsibilities:
    - Monitor component health
    - Detect failures
    - Initiate failover to backup components
    - Handle recovery of failed components
    - Report system health status
    """
    
    def __init__(self, message_bus: MessageBus, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the failover manager.
        
        Args:
            message_bus: The system message bus
            config: Configuration options
        """
        self.message_bus = message_bus
        self.config = config or {}
        
        # Component registry
        self.components: Dict[str, Dict[str, Any]] = {}
        
        # Health check registry
        self.health_checks: Dict[str, List[Callable]] = {}
        
        # Failover groups
        self.failover_groups: Dict[str, Dict[str, Any]] = {}
        
        # Health status history
        self.health_history: Dict[str, List[Tuple[datetime, ComponentHealth]]] = {}
        
        # Event listeners
        self.event_listeners: Dict[str, List[Callable]] = {}
        
        # Configure health check interval
        self.health_check_interval = self.config.get("health_check_interval_seconds", 5)
        
        # Health check thread
        self.health_check_thread = None
        self.running = False
        
        # Statistics
        self.stats = {
            "total_checks": 0,
            "failed_checks": 0,
            "failovers_triggered": 0,
            "components_recovered": 0,
            "last_check_time": None
        }
        
        # Register for component status messages
        self.message_bus.subscribe(
            MessageTypes.COMPONENT_STATUS_CHANGED,
            self._handle_component_status_change
        )
    
    def start(self):
        """Start the failover manager and health check thread."""
        if self.running:
            return
            
        self.running = True
        self.health_check_thread = threading.Thread(
            target=self._health_check_loop,
            daemon=True,
            name="FailoverManager-HealthCheck"
        )
        self.health_check_thread.start()
        logger.info("Failover Manager started")
    
    def stop(self):
        """Stop the failover manager."""
        self.running = False
        if self.health_check_thread:
            self.health_check_thread.join(timeout=5)
        logger.info("Failover Manager stopped")
    
    def register_component(self, component_id: str, component: Component, 
                          failover_group: Optional[str] = None,
                          is_primary: bool = True,
                          failover_mode: FailoverMode = FailoverMode.ACTIVE_PASSIVE):
        """
        Register a component for health monitoring and failover.
        
        Args:
            component_id: Unique ID for the component
            component: The component instance
            failover_group: Group name for failover (components in the same group are alternatives)
            is_primary: Whether this is the primary component in the group
            failover_mode: The failover mode for this component
        """
        if component_id in self.components:
            logger.warning(f"Component {component_id} already registered, updating configuration")
        
        self.components[component_id] = {
            "instance": component,
            "failover_group": failover_group,
            "is_primary": is_primary,
            "failover_mode": failover_mode,
            "health": ComponentHealth.HEALTHY,
            "last_health_check": datetime.now(),
            "error_count": 0,
            "recovery_attempts": 0
        }
        
        # Initialize health history
        self.health_history[component_id] = [
            (datetime.now(), ComponentHealth.HEALTHY)
        ]
        
        # Add to failover group
        if failover_group:
            if failover_group not in self.failover_groups:
                self.failover_groups[failover_group] = {
                    "components": [],
                    "mode": failover_mode,
                    "active_component": None
                }
            
            self.failover_groups[failover_group]["components"].append(component_id)
            
            # Set as active if primary or if no active component yet
            if is_primary or not self.failover_groups[failover_group]["active_component"]:
                self.failover_groups[failover_group]["active_component"] = component_id
        
        logger.info(f"Registered component {component_id} for health monitoring")
    
    def register_health_check(self, component_id: str, health_check: Callable):
        """
        Register a health check function for a component.
        
        Args:
            component_id: The component ID
            health_check: Function that returns a ComponentHealth value
        """
        if component_id not in self.health_checks:
            self.health_checks[component_id] = []
        
        self.health_checks[component_id].append(health_check)
        logger.debug(f"Registered health check for component {component_id}")
    
    def register_event_listener(self, event_type: str, listener: Callable):
        """
        Register a listener for failover events.
        
        Args:
            event_type: The event type (failover, recovery, degraded, etc.)
            listener: Callback function to be called when event occurs
        """
        if event_type not in self.event_listeners:
            self.event_listeners[event_type] = []
        
        self.event_listeners[event_type].append(listener)
    
    def get_component_health(self, component_id: str) -> ComponentHealth:
        """
        Get the current health status of a component.
        
        Args:
            component_id: The component ID
            
        Returns:
            ComponentHealth: The component's health status
        """
        if component_id not in self.components:
            logger.warning(f"Component {component_id} not registered")
            return ComponentHealth.FAILED
        
        return self.components[component_id]["health"]
    
    def get_system_health(self) -> Dict[str, Any]:
        """
        Get overall system health status.
        
        Returns:
            Dict containing system health information
        """
        critical_components = []
        failed_components = []
        degraded_components = []
        healthy_components = []
        
        for comp_id, comp_data in self.components.items():
            if comp_data["health"] == ComponentHealth.FAILED:
                failed_components.append(comp_id)
            elif comp_data["health"] == ComponentHealth.CRITICAL:
                critical_components.append(comp_id)
            elif comp_data["health"] == ComponentHealth.DEGRADED:
                degraded_components.append(comp_id)
            else:
                healthy_components.append(comp_id)
        
        return {
            "timestamp": datetime.now().isoformat(),
            "total_components": len(self.components),
            "healthy_components": len(healthy_components),
            "degraded_components": len(degraded_components),
            "critical_components": len(critical_components),
            "failed_components": len(failed_components),
            "critical_component_ids": critical_components,
            "failed_component_ids": failed_components,
            "degraded_component_ids": degraded_components,
            "stats": self.stats
        }
    
    def trigger_manual_failover(self, failover_group: str, target_component_id: str):
        """
        Manually trigger failover to a specific component.
        
        Args:
            failover_group: The failover group
            target_component_id: The component to failover to
        """
        if failover_group not in self.failover_groups:
            logger.error(f"Failover group {failover_group} not found")
            return False
        
        if target_component_id not in self.failover_groups[failover_group]["components"]:
            logger.error(f"Component {target_component_id} not in failover group {failover_group}")
            return False
        
        return self._execute_failover(failover_group, target_component_id, reason="MANUAL")
    
    def _health_check_loop(self):
        """Background thread that performs periodic health checks."""
        logger.info("Health check loop started")
        
        while self.running:
            try:
                # Perform health checks for all components
                for component_id in list(self.components.keys()):
                    self._check_component_health(component_id)
                
                # Update statistics
                self.stats["total_checks"] += 1
                self.stats["last_check_time"] = datetime.now().isoformat()
                
                # Sleep until next check interval
                time.sleep(self.health_check_interval)
                
            except Exception as e:
                logger.error(f"Error in health check loop: {str(e)}", exc_info=True)
                # Brief pause after an error
                time.sleep(1)
    
    def _check_component_health(self, component_id: str):
        """
        Check health for a specific component.
        
        Args:
            component_id: The component ID to check
        """
        if component_id not in self.components:
            return
        
        component_data = self.components[component_id]
        component = component_data["instance"]
        previous_health = component_data["health"]
        
        # Start with checking component status
        if component.status == ComponentStatus.ERROR:
            new_health = ComponentHealth.FAILED
        elif component.status == ComponentStatus.STOPPED:
            new_health = ComponentHealth.WARNING
        elif component.status == ComponentStatus.DEGRADED:
            new_health = ComponentHealth.DEGRADED
        elif component.status == ComponentStatus.INITIALIZED:
            new_health = ComponentHealth.HEALTHY
        else:
            new_health = ComponentHealth.WARNING
        
        # Run custom health checks if available
        if component_id in self.health_checks:
            try:
                for check_func in self.health_checks[component_id]:
                    check_result = check_func(component)
                    
                    # Update health to worst check result
                    if isinstance(check_result, ComponentHealth):
                        if check_result.value > new_health.value:
                            new_health = check_result
            except Exception as e:
                logger.error(f"Error in health check for {component_id}: {str(e)}")
                new_health = ComponentHealth.CRITICAL
                self.stats["failed_checks"] += 1
        
        # Update component health
        component_data["health"] = new_health
        component_data["last_health_check"] = datetime.now()
        
        # Update health history
        self.health_history[component_id].append((datetime.now(), new_health))
        # Keep history limited to last 100 entries
        if len(self.health_history[component_id]) > 100:
            self.health_history[component_id] = self.health_history[component_id][-100:]
        
        # Handle health changes
        if new_health != previous_health:
            logger.info(f"Component {component_id} health changed from {previous_health} to {new_health}")
            
            # Notify listeners
            self._notify_event_listeners("health_changed", {
                "component_id": component_id,
                "previous_health": previous_health,
                "new_health": new_health,
                "timestamp": datetime.now().isoformat()
            })
            
            # Handle failover logic if component failed
            if new_health == ComponentHealth.FAILED and previous_health != ComponentHealth.FAILED:
                self._handle_component_failure(component_id)
            
            # Handle recovery
            if previous_health == ComponentHealth.FAILED and new_health != ComponentHealth.FAILED:
                self._handle_component_recovery(component_id)
    
    def _handle_component_failure(self, component_id: str):
        """
        Handle component failure by initiating failover if needed.
        
        Args:
            component_id: The failed component ID
        """
        component_data = self.components.get(component_id)
        if not component_data:
            return
        
        logger.warning(f"Component failure detected: {component_id}")
        
        # Check if component is part of a failover group
        failover_group = component_data.get("failover_group")
        if not failover_group:
            logger.warning(f"Failed component {component_id} has no failover group")
            return
        
        # Check if this is the active component in the group
        group_data = self.failover_groups.get(failover_group)
        if not group_data or group_data.get("active_component") != component_id:
            logger.info(f"Failed component {component_id} is not the active component in group {failover_group}")
            return
        
        # Find best alternative component
        alternative = self._find_alternative_component(failover_group, component_id)
        if not alternative:
            logger.error(f"No alternative component available in failover group {failover_group}")
            return
        
        # Execute failover
        self._execute_failover(failover_group, alternative, reason="AUTOMATIC-FAILURE")
    
    def _find_alternative_component(self, failover_group: str, failed_component_id: str) -> Optional[str]:
        """
        Find the best alternative component in a failover group.
        
        Args:
            failover_group: The failover group name
            failed_component_id: The component that failed
            
        Returns:
            str: Component ID of the best alternative or None
        """
        group_data = self.failover_groups.get(failover_group)
        if not group_data:
            return None
        
        # Find healthy alternatives
        candidates = []
        for comp_id in group_data["components"]:
            if comp_id == failed_component_id:
                continue
                
            comp_data = self.components.get(comp_id)
            if not comp_data:
                continue
                
            # Check health
            if comp_data["health"] in [ComponentHealth.HEALTHY, ComponentHealth.DEGRADED]:
                candidates.append((comp_id, comp_data))
        
        if not candidates:
            return None
        
        # Prioritize based on is_primary flag and health
        candidates.sort(key=lambda x: (
            not x[1]["is_primary"],  # Primary components first
            x[1]["health"] != ComponentHealth.HEALTHY  # Healthy components first
        ))
        
        return candidates[0][0]
    
    def _execute_failover(self, failover_group: str, target_component_id: str, reason: str) -> bool:
        """
        Execute failover to a specific component.
        
        Args:
            failover_group: The failover group
            target_component_id: The component to failover to
            reason: The reason for failover
            
        Returns:
            bool: Success flag
        """
        group_data = self.failover_groups.get(failover_group)
        if not group_data:
            return False
        
        previous_active = group_data["active_component"]
        
        if previous_active == target_component_id:
            logger.info(f"Component {target_component_id} is already active in group {failover_group}")
            return True
        
        logger.warning(f"Executing failover in group {failover_group} from {previous_active} to {target_component_id} (Reason: {reason})")
        
        # Update active component
        group_data["active_component"] = target_component_id
        
        # Update stats
        self.stats["failovers_triggered"] += 1
        
        # Notify via message bus
        self.message_bus.publish(
            MessageTypes.FAILOVER_EXECUTED,
            {
                "failover_group": failover_group,
                "previous_component": previous_active,
                "new_component": target_component_id,
                "reason": reason,
                "timestamp": datetime.now().isoformat()
            }
        )
        
        # Notify event listeners
        self._notify_event_listeners("failover", {
            "failover_group": failover_group,
            "previous_component": previous_active,
            "new_component": target_component_id,
            "reason": reason,
            "timestamp": datetime.now().isoformat()
        })
        
        return True
    
    def _handle_component_recovery(self, component_id: str):
        """
        Handle recovery of a previously failed component.
        
        Args:
            component_id: The recovered component ID
        """
        component_data = self.components.get(component_id)
        if not component_data:
            return
        
        logger.info(f"Component recovery detected: {component_id}")
        
        # Update statistics
        self.stats["components_recovered"] += 1
        
        # Notify listeners
        self._notify_event_listeners("recovery", {
            "component_id": component_id,
            "timestamp": datetime.now().isoformat()
        })
        
        # Check if component should be promoted back to active
        failover_group = component_data.get("failover_group")
        if not failover_group:
            return
        
        group_data = self.failover_groups.get(failover_group)
        if not group_data:
            return
        
        # If this is a primary component and the current active component
        # is not a primary, consider switching back
        if (component_data["is_primary"] and 
            group_data["active_component"] != component_id and
            self.components.get(group_data["active_component"], {}).get("is_primary", False) == False):
            
            # Switch back to this component if configured to do so
            auto_failback = self.config.get("auto_failback", True)
            if auto_failback:
                self._execute_failover(failover_group, component_id, reason="AUTOMATIC-RECOVERY")
    
    def _handle_component_status_change(self, message):
        """
        Handle component status change message.
        
        Args:
            message: The status change message
        """
        component_id = message.get("component_id")
        if not component_id or component_id not in self.components:
            return
        
        # Trigger immediate health check for this component
        self._check_component_health(component_id)
    
    def _notify_event_listeners(self, event_type: str, event_data: Dict[str, Any]):
        """
        Notify registered event listeners.
        
        Args:
            event_type: The event type
            event_data: The event data
        """
        if event_type not in self.event_listeners:
            return
        
        for listener in self.event_listeners[event_type]:
            try:
                listener(event_data)
            except Exception as e:
                logger.error(f"Error in event listener for {event_type}: {str(e)}")


class CircuitBreaker:
    """
    Implements the Circuit Breaker pattern to prevent cascading failures.
    
    Circuit breakers prevent a failing operation from being repeatedly called, 
    which can drain system resources and cause cascading failures.
    """
    
    def __init__(self, name: str, failure_threshold: int = 3, reset_timeout_seconds: int = 60):
        """
        Initialize the circuit breaker.
        
        Args:
            name: Circuit breaker name
            failure_threshold: Number of failures before opening the circuit
            reset_timeout_seconds: Seconds before attempting to close circuit
        """
        self.name = name
        self.failure_threshold = failure_threshold
        self.reset_timeout_seconds = reset_timeout_seconds
        
        self.failure_count = 0
        self.last_failure_time = None
        self.last_success_time = None
        self.is_open = False
        self.half_open = False
        
        self.logger = get_logger(f"circuit_breaker.{name}")
    
    async def execute(self, operation: Callable, fallback: Optional[Callable] = None):
        """
        Execute an operation protected by the circuit breaker.
        
        Args:
            operation: The function to execute
            fallback: Fallback function to call if circuit is open
            
        Returns:
            The result of the operation or fallback
            
        Raises:
            Exception: If circuit is open and no fallback is provided
        """
        # Check if circuit is open
        if self.is_open:
            # Check if reset timeout has passed
            if self.last_failure_time and (datetime.now() - self.last_failure_time).total_seconds() > self.reset_timeout_seconds:
                self.logger.info(f"Circuit half-open, attempting reset: {self.name}")
                self.half_open = True
            else:
                self.logger.warning(f"Circuit open, bypassing call: {self.name}")
                if fallback:
                    return await fallback()
                raise Exception(f"Circuit open: {self.name}")
        
        try:
            # Execute the operation
            result = await operation()
            
            # Reset failure count on success
            self._record_success()
            return result
            
        except Exception as e:
            # Handle failure
            self._record_failure()
            
            # If circuit transitions to open, log it
            if self.is_open and not self.half_open:
                self.logger.error(f"Circuit opened after {self.failure_threshold} failures: {self.name}")
            
            # Use fallback if available
            if fallback:
                self.logger.info(f"Using fallback for {self.name}")
                return await fallback()
            
            # Re-raise the exception
            raise
    
    def _record_success(self):
        """Record a successful operation and reset circuit if half-open."""
        self.last_success_time = datetime.now()
        
        if self.half_open:
            self.is_open = False
            self.half_open = False
            self.failure_count = 0
            self.logger.info(f"Circuit closed after successful operation: {self.name}")
    
    def _record_failure(self):
        """Record a failed operation and open circuit if threshold reached."""
        self.last_failure_time = datetime.now()
        self.failure_count += 1
        
        if self.half_open:
            self.is_open = True
            self.half_open = False
            self.logger.warning(f"Circuit reopened after test failure: {self.name}")
        elif self.failure_count >= self.failure_threshold:
            self.is_open = True
            self.logger.warning(f"Circuit opened after {self.failure_count} failures: {self.name}")


def get_failover_manager(message_bus: Optional[MessageBus] = None) -> FailoverManager:
    """
    Get the singleton failover manager instance.
    
    Args:
        message_bus: The message bus to use (if not already instantiated)
        
    Returns:
        FailoverManager: The failover manager instance
    """
    global _failover_manager_instance
    
    if _failover_manager_instance is None:
        # Get message bus if not provided
        if message_bus is None:
            from trading_system.core.message_bus import get_message_bus
            message_bus = get_message_bus()
        
        # Load config
        from trading_system.core.config import get_config
        config = get_config()
        
        # Create failover manager
        _failover_manager_instance = FailoverManager(
            message_bus=message_bus,
            config=config.get("high_availability", {})
        )
    
    return _failover_manager_instance


# Singleton instance
_failover_manager_instance = None 