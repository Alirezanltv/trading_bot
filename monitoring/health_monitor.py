"""
Health Monitoring System

This module implements a comprehensive health monitoring system with component
health checks, self-healing capabilities, and real-time metrics collection
for system health monitoring.
"""

import asyncio
import logging
import time
import os
import socket
import json
from enum import Enum
from typing import Dict, List, Any, Optional, Union, Callable, Set, Tuple
from datetime import datetime, timedelta
import traceback

from trading_system.core.logging import get_logger
from trading_system.core.component import Component, ComponentStatus
from trading_system.core.message_bus import MessageTypes

logger = get_logger("monitoring.health")

class HealthStatus(Enum):
    """Health status levels."""
    HEALTHY = "healthy"           # Fully operational
    DEGRADED = "degraded"         # Operational with reduced functionality
    WARNING = "warning"           # Operational but requires attention
    CRITICAL = "critical"         # Failing but still partially operational
    FAILED = "failed"             # Completely failed


class HealthMetricType(Enum):
    """Types of health metrics."""
    COUNTER = "counter"           # Monotonically increasing counter
    GAUGE = "gauge"               # Value that can go up and down
    HISTOGRAM = "histogram"       # Distribution of values
    DURATION = "duration"         # Time duration measurement
    BOOLEAN = "boolean"           # True/false status
    STRING = "string"             # String status


class CheckResult:
    """Result of a health check."""
    
    def __init__(self, 
                 name: str,
                 status: HealthStatus,
                 message: str = "",
                 details: Dict[str, Any] = None,
                 timestamp: Optional[float] = None):
        """
        Initialize check result.
        
        Args:
            name: Name of the check
            status: Health status
            message: Status message
            details: Additional details
            timestamp: Check timestamp
        """
        self.name = name
        self.status = status
        self.message = message
        self.details = details or {}
        self.timestamp = timestamp or time.time()
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary.
        
        Returns:
            Dict of check result
        """
        return {
            "name": self.name,
            "status": self.status.value,
            "message": self.message,
            "details": self.details,
            "timestamp": self.timestamp,
            "formatted_time": datetime.fromtimestamp(self.timestamp).isoformat()
        }


class HealthMetric:
    """Health metric with value and metadata."""
    
    def __init__(self,
                 name: str,
                 type_: HealthMetricType,
                 value: Any,
                 description: str = "",
                 unit: str = "",
                 tags: Dict[str, str] = None,
                 timestamp: Optional[float] = None):
        """
        Initialize health metric.
        
        Args:
            name: Metric name
            type_: Metric type
            value: Metric value
            description: Metric description
            unit: Metric unit
            tags: Metric tags
            timestamp: Metric timestamp
        """
        self.name = name
        self.type = type_
        self.value = value
        self.description = description
        self.unit = unit
        self.tags = tags or {}
        self.timestamp = timestamp or time.time()
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary.
        
        Returns:
            Dict of metric
        """
        return {
            "name": self.name,
            "type": self.type.value,
            "value": self.value,
            "description": self.description,
            "unit": self.unit,
            "tags": self.tags,
            "timestamp": self.timestamp,
            "formatted_time": datetime.fromtimestamp(self.timestamp).isoformat()
        }


class HealthCheck:
    """Health check definition."""
    
    def __init__(self,
                 name: str,
                 check_function: Callable[[], CheckResult],
                 interval: float = 60.0,
                 description: str = "",
                 tags: Dict[str, str] = None,
                 timeout: float = 10.0,
                 enabled: bool = True,
                 retry_count: int = 3,
                 retry_delay: float = 5.0,
                 self_healing: Optional[Callable[[], None]] = None):
        """
        Initialize health check.
        
        Args:
            name: Check name
            check_function: Function that performs the check
            interval: Check interval in seconds
            description: Check description
            tags: Check tags
            timeout: Check timeout in seconds
            enabled: Whether check is enabled
            retry_count: Number of retries on failure
            retry_delay: Delay between retries in seconds
            self_healing: Function that attempts to heal the issue
        """
        self.name = name
        self.check_function = check_function
        self.interval = interval
        self.description = description
        self.tags = tags or {}
        self.timeout = timeout
        self.enabled = enabled
        self.retry_count = retry_count
        self.retry_delay = retry_delay
        self.self_healing = self_healing
        
        # Check history
        self.last_run_time = 0.0
        self.last_result: Optional[CheckResult] = None
        self.failure_count = 0
        self.success_count = 0
        self.healing_attempts = 0
        self.healing_successes = 0
        self.next_run_time = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary.
        
        Returns:
            Dict of health check
        """
        return {
            "name": self.name,
            "interval": self.interval,
            "description": self.description,
            "tags": self.tags,
            "timeout": self.timeout,
            "enabled": self.enabled,
            "retry_count": self.retry_count,
            "retry_delay": self.retry_delay,
            "has_self_healing": self.self_healing is not None,
            "last_run_time": self.last_run_time,
            "next_run_time": self.next_run_time,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "healing_attempts": self.healing_attempts,
            "healing_successes": self.healing_successes,
            "last_result": self.last_result.to_dict() if self.last_result else None
        }


class HealthMonitor(Component):
    """
    Health Monitoring System
    
    This component monitors the health of all system components, collects 
    metrics, provides real-time health status, and performs self-healing
    actions when possible.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize health monitor.
        
        Args:
            config: Configuration dictionary
        """
        super().__init__(name="health_monitor", config=config)
        
        # Health checks registry
        self.checks: Dict[str, HealthCheck] = {}
        
        # Health metrics registry
        self.metrics: Dict[str, HealthMetric] = {}
        
        # Components registry
        self.components: Dict[str, Component] = {}
        
        # Check history
        self.check_history: Dict[str, List[CheckResult]] = {}
        self.history_limit = self.config.get("history_limit", 100)
        
        # Alert thresholds
        self.alert_thresholds = self.config.get("alert_thresholds", {
            HealthStatus.WARNING.value: 2,    # Alert after 2 warnings
            HealthStatus.CRITICAL.value: 1,   # Alert immediately on critical
            HealthStatus.FAILED.value: 1      # Alert immediately on failure
        })
        
        # Alert status
        self.alert_counts: Dict[str, Dict[str, int]] = {}
        
        # Self-healing config
        self.self_healing_enabled = self.config.get("self_healing_enabled", True)
        self.self_healing_max_attempts = self.config.get("self_healing_max_attempts", 3)
        self.self_healing_cooldown = self.config.get("self_healing_cooldown", 3600)  # 1 hour
        
        # Check task
        self.check_task = None
        self.running = False
        
        # Message bus reference
        self.message_bus = None
        
        # Global system health assessment
        self.system_health = HealthStatus.HEALTHY
        self.system_health_updated = time.time()
        
        # Initialize directories
        self.data_dir = self.config.get("data_dir", "data/monitoring")
        os.makedirs(self.data_dir, exist_ok=True)
        
        # System info
        self.system_info = self._gather_system_info()
        
        # Initialize status
        self._update_status(ComponentStatus.INITIALIZED)
        logger.info("Health monitor initialized")
    
    async def start(self) -> bool:
        """
        Start the health monitor.
        
        Returns:
            bool: True if started successfully, False otherwise
        """
        try:
            if self.running:
                logger.warning("Health monitor already running")
                return True
                
            # Update status
            self._update_status(ComponentStatus.INITIALIZING)
            
            # Register built-in checks
            self._register_builtin_checks()
            
            # Start check task
            self.running = True
            self.check_task = asyncio.create_task(self._check_loop())
            
            # Update status
            self._update_status(ComponentStatus.OPERATIONAL)
            logger.info("Health monitor started")
            return True
            
        except Exception as e:
            self._update_status(ComponentStatus.ERROR, f"Error starting: {str(e)}")
            logger.error(f"Error starting health monitor: {str(e)}", exc_info=True)
            return False
    
    async def stop(self) -> bool:
        """
        Stop the health monitor.
        
        Returns:
            bool: True if stopped successfully, False otherwise
        """
        try:
            if not self.running:
                return True
                
            # Update status
            self._update_status(ComponentStatus.STOPPING)
            
            # Stop running flag
            self.running = False
            
            # Cancel check task
            if self.check_task:
                self.check_task.cancel()
                try:
                    await self.check_task
                except asyncio.CancelledError:
                    pass
                
            # Update status
            self._update_status(ComponentStatus.STOPPED)
            logger.info("Health monitor stopped")
            return True
            
        except Exception as e:
            self._update_status(ComponentStatus.ERROR, f"Error stopping: {str(e)}")
            logger.error(f"Error stopping health monitor: {str(e)}", exc_info=True)
            return False
    
    def set_message_bus(self, message_bus) -> None:
        """
        Set the message bus for alerts.
        
        Args:
            message_bus: Message bus instance
        """
        self.message_bus = message_bus
    
    def register_component(self, component: Component) -> None:
        """
        Register a component for health monitoring.
        
        Args:
            component: Component to monitor
        """
        self.components[component.name] = component
        logger.info(f"Registered component for monitoring: {component.name}")
    
    def register_check(self, check: HealthCheck) -> None:
        """
        Register a health check.
        
        Args:
            check: Health check to register
        """
        self.checks[check.name] = check
        self.check_history[check.name] = []
        check.next_run_time = time.time()
        logger.info(f"Registered health check: {check.name}")
    
    def register_metric(self, metric: HealthMetric) -> None:
        """
        Register a health metric.
        
        Args:
            metric: Health metric to register
        """
        self.metrics[metric.name] = metric
        logger.info(f"Registered health metric: {metric.name}")
    
    def update_metric(self, name: str, value: Any) -> None:
        """
        Update a health metric.
        
        Args:
            name: Metric name
            value: New value
        """
        if name in self.metrics:
            # Update existing metric
            self.metrics[name].value = value
            self.metrics[name].timestamp = time.time()
        else:
            # Create new metric with default type
            self.metrics[name] = HealthMetric(
                name=name,
                type_=HealthMetricType.GAUGE,
                value=value
            )
    
    def increment_counter(self, name: str, value: int = 1) -> None:
        """
        Increment a counter metric.
        
        Args:
            name: Metric name
            value: Increment amount
        """
        if name in self.metrics:
            # Update existing counter
            if self.metrics[name].type == HealthMetricType.COUNTER:
                self.metrics[name].value += value
                self.metrics[name].timestamp = time.time()
            else:
                logger.warning(f"Cannot increment non-counter metric: {name}")
        else:
            # Create new counter
            self.metrics[name] = HealthMetric(
                name=name,
                type_=HealthMetricType.COUNTER,
                value=value
            )
    
    def get_check_result(self, name: str) -> Optional[CheckResult]:
        """
        Get the most recent result for a check.
        
        Args:
            name: Check name
            
        Returns:
            Most recent check result or None
        """
        if name in self.checks:
            return self.checks[name].last_result
        return None
    
    def get_check_history(self, name: str) -> List[CheckResult]:
        """
        Get check history.
        
        Args:
            name: Check name
            
        Returns:
            List of check results
        """
        return self.check_history.get(name, [])
    
    def get_metric(self, name: str) -> Optional[HealthMetric]:
        """
        Get a health metric.
        
        Args:
            name: Metric name
            
        Returns:
            Health metric or None
        """
        return self.metrics.get(name)
    
    def get_component_health(self, name: str) -> HealthStatus:
        """
        Get health status for a component.
        
        Args:
            name: Component name
            
        Returns:
            Health status
        """
        if name in self.components:
            component = self.components[name]
            
            # Map component status to health status
            if component.status == ComponentStatus.OPERATIONAL:
                return HealthStatus.HEALTHY
            elif component.status == ComponentStatus.WARNING:
                return HealthStatus.WARNING
            elif component.status == ComponentStatus.ERROR:
                return HealthStatus.CRITICAL
            elif component.status == ComponentStatus.STOPPED:
                return HealthStatus.FAILED
            else:
                return HealthStatus.DEGRADED
        
        return HealthStatus.FAILED
    
    def get_system_health(self) -> HealthStatus:
        """
        Get overall system health status.
        
        Returns:
            System health status
        """
        return self.system_health
    
    def get_health_report(self) -> Dict[str, Any]:
        """
        Get comprehensive health report.
        
        Returns:
            Health report dictionary
        """
        # Gather component statuses
        component_statuses = {
            name: {
                "status": self.get_component_health(name).value,
                "component_status": component.status.value,
                "error": component.error
            } for name, component in self.components.items()
        }
        
        # Gather check results
        check_results = {
            name: check.last_result.to_dict() if check.last_result else None
            for name, check in self.checks.items()
        }
        
        # Gather metrics
        metric_values = {
            name: metric.to_dict()
            for name, metric in self.metrics.items()
        }
        
        # Compile report
        report = {
            "timestamp": time.time(),
            "formatted_time": datetime.now().isoformat(),
            "system_health": self.system_health.value,
            "system_health_updated": self.system_health_updated,
            "components": component_statuses,
            "checks": check_results,
            "metrics": metric_values,
            "system_info": self.system_info
        }
        
        return report
    
    def save_health_report(self, filename: Optional[str] = None) -> str:
        """
        Save health report to file.
        
        Args:
            filename: Output filename or None for auto-generated
            
        Returns:
            Path to saved file
        """
        # Generate filename if not provided
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"health_report_{timestamp}.json"
        
        # Ensure directory exists
        filepath = os.path.join(self.data_dir, filename)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        # Generate report
        report = self.get_health_report()
        
        # Save report
        with open(filepath, 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"Saved health report to {filepath}")
        return filepath
    
    async def run_check(self, name: str) -> Optional[CheckResult]:
        """
        Run a specific health check immediately.
        
        Args:
            name: Check name
            
        Returns:
            Check result or None
        """
        if name not in self.checks:
            logger.warning(f"Check not found: {name}")
            return None
        
        check = self.checks[name]
        if not check.enabled:
            logger.warning(f"Check is disabled: {name}")
            return None
        
        try:
            # Run the check with timeout
            result = await asyncio.wait_for(
                self._run_check_safe(check),
                timeout=check.timeout
            )
            
            # Update check history
            self._update_check_history(check, result)
            
            # Process check result
            await self._process_check_result(check, result)
            
            return result
            
        except asyncio.TimeoutError:
            # Create timeout result
            result = CheckResult(
                name=check.name,
                status=HealthStatus.CRITICAL,
                message=f"Check timed out after {check.timeout} seconds"
            )
            
            # Update check history
            self._update_check_history(check, result)
            
            # Process check result
            await self._process_check_result(check, result)
            
            return result
            
        except Exception as e:
            # Create error result
            result = CheckResult(
                name=check.name,
                status=HealthStatus.FAILED,
                message=f"Error running check: {str(e)}",
                details={"traceback": traceback.format_exc()}
            )
            
            # Update check history
            self._update_check_history(check, result)
            
            # Process check result
            await self._process_check_result(check, result)
            
            return result
    
    async def attempt_healing(self, check_name: str) -> bool:
        """
        Attempt to heal an issue.
        
        Args:
            check_name: Name of the check to heal
            
        Returns:
            bool: True if healing was attempted, False otherwise
        """
        if not self.self_healing_enabled:
            logger.warning("Self-healing is disabled")
            return False
        
        if check_name not in self.checks:
            logger.warning(f"Check not found: {check_name}")
            return False
        
        check = self.checks[check_name]
        
        if not check.self_healing:
            logger.warning(f"No self-healing function defined for check: {check_name}")
            return False
        
        if check.healing_attempts >= self.self_healing_max_attempts:
            logger.warning(f"Maximum healing attempts reached for check: {check_name}")
            return False
        
        # Increment healing attempts
        check.healing_attempts += 1
        
        try:
            # Run healing function
            logger.info(f"Attempting to heal issue: {check_name}")
            check.self_healing()
            
            # Increment healing successes
            check.healing_successes += 1
            
            # Run check again to verify healing
            await asyncio.sleep(check.retry_delay)
            result = await self.run_check(check_name)
            
            if result and result.status == HealthStatus.HEALTHY:
                logger.info(f"Self-healing successful for check: {check_name}")
                return True
            else:
                logger.warning(f"Self-healing attempted but check still failing: {check_name}")
                return True
                
        except Exception as e:
            logger.error(f"Error during self-healing for {check_name}: {str(e)}", exc_info=True)
            return True
    
    def _register_builtin_checks(self) -> None:
        """Register built-in health checks."""
        # System resource check
        self.register_check(HealthCheck(
            name="system_resources",
            check_function=self._check_system_resources,
            interval=60.0,
            description="Check system resources (CPU, memory, disk)",
            tags={"type": "system", "category": "resources"}
        ))
        
        # Component status check
        self.register_check(HealthCheck(
            name="component_status",
            check_function=self._check_component_status,
            interval=30.0,
            description="Check status of all registered components",
            tags={"type": "system", "category": "components"}
        ))
        
        # Database connection check
        self.register_check(HealthCheck(
            name="database_connection",
            check_function=self._check_database_connection,
            interval=60.0,
            description="Check database connection",
            tags={"type": "database", "category": "connectivity"}
        ))
        
        # Message bus connection check
        self.register_check(HealthCheck(
            name="message_bus_connection",
            check_function=self._check_message_bus_connection,
            interval=30.0,
            description="Check message bus connection",
            tags={"type": "messaging", "category": "connectivity"}
        ))
    
    async def _check_loop(self) -> None:
        """Background task to run health checks."""
        try:
            while self.running:
                # Find checks that need to be run
                current_time = time.time()
                checks_to_run = []
                
                for name, check in self.checks.items():
                    if check.enabled and current_time >= check.next_run_time:
                        checks_to_run.append(check)
                
                # Run checks concurrently
                if checks_to_run:
                    await asyncio.gather(*[
                        self.run_check(check.name)
                        for check in checks_to_run
                    ])
                
                # Update system health
                self._update_system_health()
                
                # Wait a short time before next iteration
                await asyncio.sleep(1.0)
                
        except asyncio.CancelledError:
            logger.info("Health check loop cancelled")
            raise
            
        except Exception as e:
            logger.error(f"Error in health check loop: {str(e)}", exc_info=True)
            self._update_status(ComponentStatus.ERROR, f"Check loop error: {str(e)}")
    
    async def _run_check_safe(self, check: HealthCheck) -> CheckResult:
        """
        Run a health check safely.
        
        Args:
            check: Health check to run
            
        Returns:
            Check result
        """
        try:
            # Update check timing
            check.last_run_time = time.time()
            check.next_run_time = check.last_run_time + check.interval
            
            # Run check function
            if asyncio.iscoroutinefunction(check.check_function):
                result = await check.check_function()
            else:
                result = check.check_function()
            
            # Update check counters
            if result.status == HealthStatus.HEALTHY:
                check.success_count += 1
                check.failure_count = 0
            else:
                check.failure_count += 1
            
            return result
            
        except Exception as e:
            # Create error result
            return CheckResult(
                name=check.name,
                status=HealthStatus.FAILED,
                message=f"Exception in check: {str(e)}",
                details={"traceback": traceback.format_exc()}
            )
    
    def _update_check_history(self, check: HealthCheck, result: CheckResult) -> None:
        """
        Update check history.
        
        Args:
            check: Health check
            result: Check result
        """
        # Update check's last result
        check.last_result = result
        
        # Add to history
        if check.name in self.check_history:
            history = self.check_history[check.name]
            history.append(result)
            
            # Limit history size
            if len(history) > self.history_limit:
                self.check_history[check.name] = history[-self.history_limit:]
    
    async def _process_check_result(self, check: HealthCheck, result: CheckResult) -> None:
        """
        Process a check result.
        
        Args:
            check: Health check
            result: Check result
        """
        # Initialize alert count for this check if not exists
        if check.name not in self.alert_counts:
            self.alert_counts[check.name] = {
                status.value: 0 for status in HealthStatus
            }
        
        # Reset alert count for statuses better than current
        for status in HealthStatus:
            if status.value not in self.alert_counts[check.name]:
                self.alert_counts[check.name][status.value] = 0
                
            if status != result.status and status.value < result.status.value:
                self.alert_counts[check.name][status.value] = 0
        
        # Increment alert count for current status
        self.alert_counts[check.name][result.status.value] += 1
        
        # Check if alert threshold reached
        threshold = self.alert_thresholds.get(result.status.value, 1)
        if self.alert_counts[check.name][result.status.value] >= threshold:
            # Generate alert
            await self._generate_alert(check, result)
            
            # Reset alert count
            self.alert_counts[check.name][result.status.value] = 0
        
        # Attempt self-healing if needed
        if (result.status in [HealthStatus.CRITICAL, HealthStatus.FAILED] and
            self.self_healing_enabled and
            check.self_healing is not None):
            # Only attempt healing after consecutive failures
            if check.failure_count >= threshold:
                await self.attempt_healing(check.name)
    
    async def _generate_alert(self, check: HealthCheck, result: CheckResult) -> None:
        """
        Generate an alert for a check result.
        
        Args:
            check: Health check
            result: Check result
        """
        # Skip if message bus not available
        if not self.message_bus:
            logger.warning("Cannot generate alert: Message bus not available")
            return
            
        try:
            # Create alert message
            alert = {
                "type": "health_alert",
                "check_name": check.name,
                "status": result.status.value,
                "message": result.message,
                "details": result.details,
                "timestamp": result.timestamp,
                "formatted_time": datetime.fromtimestamp(result.timestamp).isoformat()
            }
            
            # Send alert
            await self.message_bus.publish(
                MessageTypes.ALERT,
                alert
            )
            
            logger.warning(f"Generated alert for {check.name}: {result.status.value} - {result.message}")
            
        except Exception as e:
            logger.error(f"Error generating alert: {str(e)}", exc_info=True)
    
    def _update_system_health(self) -> None:
        """Update overall system health status."""
        # Default to healthy
        new_health = HealthStatus.HEALTHY
        
        # Check component health
        component_health = [
            self.get_component_health(name)
            for name in self.components
        ]
        
        # Check health check results
        check_health = [
            check.last_result.status
            for check in self.checks.values()
            if check.last_result is not None
        ]
        
        # Combine all health statuses
        all_health = component_health + check_health
        
        # Determine worst health status
        for status in [HealthStatus.FAILED, HealthStatus.CRITICAL, HealthStatus.WARNING, HealthStatus.DEGRADED]:
            if status in all_health:
                new_health = status
                break
        
        # Update system health if changed
        if new_health != self.system_health:
            # Log change
            if new_health.value > self.system_health.value:
                logger.warning(f"System health degraded from {self.system_health.value} to {new_health.value}")
            else:
                logger.info(f"System health improved from {self.system_health.value} to {new_health.value}")
                
            # Update status
            self.system_health = new_health
            self.system_health_updated = time.time()
    
    def _check_system_resources(self) -> CheckResult:
        """
        Check system resources (CPU, memory, disk).
        
        Returns:
            Check result
        """
        try:
            # Import psutil here to avoid dependency for whole module
            import psutil
            
            # Get CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # Get memory usage
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            
            # Get disk usage
            disk = psutil.disk_usage('/')
            disk_percent = disk.percent
            
            # Determine status based on thresholds
            status = HealthStatus.HEALTHY
            message = "System resources normal"
            
            if cpu_percent > 90 or memory_percent > 90 or disk_percent > 90:
                status = HealthStatus.CRITICAL
                message = "System resources critical"
            elif cpu_percent > 80 or memory_percent > 80 or disk_percent > 80:
                status = HealthStatus.WARNING
                message = "System resources high"
            
            # Update metrics
            self.update_metric("system.cpu.percent", cpu_percent)
            self.update_metric("system.memory.percent", memory_percent)
            self.update_metric("system.disk.percent", disk_percent)
            
            return CheckResult(
                name="system_resources",
                status=status,
                message=message,
                details={
                    "cpu_percent": cpu_percent,
                    "memory_percent": memory_percent,
                    "disk_percent": disk_percent
                }
            )
            
        except ImportError:
            return CheckResult(
                name="system_resources",
                status=HealthStatus.WARNING,
                message="psutil module not available for resource checks"
            )
        except Exception as e:
            return CheckResult(
                name="system_resources",
                status=HealthStatus.FAILED,
                message=f"Error checking system resources: {str(e)}"
            )
    
    def _check_component_status(self) -> CheckResult:
        """
        Check status of all registered components.
        
        Returns:
            Check result
        """
        try:
            # Count components by status
            status_counts = {}
            error_components = []
            
            for name, component in self.components.items():
                status = component.status.value
                if status not in status_counts:
                    status_counts[status] = 0
                status_counts[status] += 1
                
                if component.status == ComponentStatus.ERROR or component.status == ComponentStatus.WARNING:
                    error_components.append({
                        "name": name,
                        "status": status,
                        "error": component.error
                    })
            
            # Determine overall status
            if status_counts.get(ComponentStatus.ERROR.value, 0) > 0:
                status = HealthStatus.CRITICAL
                message = f"{status_counts.get(ComponentStatus.ERROR.value, 0)} components in ERROR state"
            elif status_counts.get(ComponentStatus.WARNING.value, 0) > 0:
                status = HealthStatus.WARNING
                message = f"{status_counts.get(ComponentStatus.WARNING.value, 0)} components in WARNING state"
            elif status_counts.get(ComponentStatus.STOPPED.value, 0) > 0:
                status = HealthStatus.DEGRADED
                message = f"{status_counts.get(ComponentStatus.STOPPED.value, 0)} components STOPPED"
            else:
                status = HealthStatus.HEALTHY
                message = "All components operational"
            
            # Update metrics
            for status_value, count in status_counts.items():
                self.update_metric(f"components.status.{status_value}", count)
            
            return CheckResult(
                name="component_status",
                status=status,
                message=message,
                details={
                    "status_counts": status_counts,
                    "error_components": error_components,
                    "total_components": len(self.components)
                }
            )
            
        except Exception as e:
            return CheckResult(
                name="component_status",
                status=HealthStatus.FAILED,
                message=f"Error checking component status: {str(e)}"
            )
    
    def _check_database_connection(self) -> CheckResult:
        """
        Check database connection.
        
        Returns:
            Check result
        """
        # In a real implementation, this would check the actual database
        # For now, just return a placeholder result
        return CheckResult(
            name="database_connection",
            status=HealthStatus.HEALTHY,
            message="Database connection check not implemented yet"
        )
    
    def _check_message_bus_connection(self) -> CheckResult:
        """
        Check message bus connection.
        
        Returns:
            Check result
        """
        try:
            if not self.message_bus:
                return CheckResult(
                    name="message_bus_connection",
                    status=HealthStatus.WARNING,
                    message="Message bus not configured"
                )
            
            # In a real implementation, this would ping the message bus
            # For now, just check if it's available
            status = HealthStatus.HEALTHY
            message = "Message bus connected"
            
            return CheckResult(
                name="message_bus_connection",
                status=status,
                message=message
            )
            
        except Exception as e:
            return CheckResult(
                name="message_bus_connection",
                status=HealthStatus.FAILED,
                message=f"Error checking message bus connection: {str(e)}"
            )
    
    def _gather_system_info(self) -> Dict[str, Any]:
        """
        Gather system information.
        
        Returns:
            Dict of system info
        """
        try:
            # Import psutil here to avoid dependency for whole module
            import psutil
            import platform
            
            # Get system info
            system_info = {
                "hostname": socket.gethostname(),
                "platform": platform.platform(),
                "python_version": platform.python_version(),
                "cpu_count": psutil.cpu_count(),
                "memory_total": psutil.virtual_memory().total,
                "disk_total": psutil.disk_usage('/').total,
                "boot_time": datetime.fromtimestamp(psutil.boot_time()).isoformat()
            }
            
            return system_info
            
        except ImportError:
            # Fallback if psutil not available
            import platform
            
            return {
                "hostname": socket.gethostname(),
                "platform": platform.platform(),
                "python_version": platform.python_version()
            }


async def create_health_monitor(config: Dict[str, Any]) -> HealthMonitor:
    """
    Create and start a health monitor.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        HealthMonitor instance
    """
    monitor = HealthMonitor(config)
    await monitor.start()
    return monitor 