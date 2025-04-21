"""
Health Monitoring Dashboard

This module provides a web-based dashboard for monitoring the health and status
of all trading system components, with realtime updates and alerts.
"""

import os
import asyncio
import json
import logging
import datetime
from typing import Dict, List, Any, Optional, Set
import threading
import time
from enum import Enum

import aiohttp
from aiohttp import web
import aiohttp_jinja2
import jinja2

from trading_system.core.component import Component, ComponentStatus
from trading_system.core.logging import get_logger
from trading_system.core.message_bus import MessageBus, MessageTypes
from trading_system.core.high_availability import (
    get_failover_manager, ComponentHealth, FailoverManager, FailoverMode
)
from trading_system.core.circuit_breaker import CircuitBreaker, CircuitBreakerStatus
from trading_system.monitoring.alert_system import get_alert_system, AlertSeverity, AlertStatus
from trading_system.utils.config import Config

logger = get_logger("monitoring.health_dashboard")

# Dashboard configuration
DEFAULT_CONFIG = {
    "host": "0.0.0.0",
    "port": 8080,
    "debug": False,
    "history_size": 100,  # Number of historical data points to keep
    "update_interval_ms": 1000,  # Refresh interval in milliseconds
    "component_timeout_seconds": 30,  # Component considered offline after this time
    "theme": "dark",  # or "light"
    "title": "Trading System Health Dashboard"
}


class AlertLevel(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class HealthDashboard(Component):
    """
    Web-based dashboard for monitoring system health.
    
    Provides real-time monitoring of:
    - Component status and health
    - System metrics and performance
    - Failover events and status
    - Critical alerts and notifications
    """
    
    def __init__(self, message_bus: MessageBus, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the health dashboard.
        
        Args:
            message_bus: Message bus instance
            config: Dashboard configuration
        """
        super().__init__(name="HealthDashboard")
        
        # Configuration
        self.config = DEFAULT_CONFIG.copy()
        if config:
            self.config.update(config)
        
        # Core dependencies
        self.message_bus = message_bus
        
        # Get failover manager
        self.failover_manager = get_failover_manager(message_bus)
        
        # Web app and server
        self.app = web.Application()
        self.runner = None
        self.site = None
        self.loop = None
        
        # Client connections for SSE (Server-Sent Events)
        self.sse_clients: Set[web.StreamResponse] = set()
        
        # System state
        self.components: Dict[str, Dict[str, Any]] = {}
        self.metrics: Dict[str, Dict[str, Any]] = {}
        self.alerts: List[Dict[str, Any]] = []
        self.failover_events: List[Dict[str, Any]] = []
        self.system_health: Dict[str, Any] = {}
        
        # Historical data for charts
        self.history = {
            "timestamps": [],
            "component_health": {},  # component_id -> list of health values
            "metrics": {}  # metric_name -> list of values
        }
        
        # Background tasks
        self.update_task = None
        self.cleanup_task = None
        self.running = False
        
        # Register routes and initialize template engine
        self._setup_routes()
        self._setup_templates()
        
        # Register message handlers
        self._register_message_handlers()
        
    async def initialize(self) -> bool:
        """
        Initialize the health dashboard.
        
        Returns:
            bool: Success flag
        """
        try:
            # Create event loop if not in asyncio context
            try:
                self.loop = asyncio.get_event_loop()
            except RuntimeError:
                self.loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self.loop)
            
            # Start web server
            self.runner = web.AppRunner(self.app)
            await self.runner.setup()
            
            # Get configuration
            host = self.config.get("host", "0.0.0.0")
            port = self.config.get("port", 8080)
            
            # Start web server
            self.site = web.TCPSite(self.runner, host, port)
            await self.site.start()
            
            logger.info(f"Health dashboard started at http://{host}:{port}")
            
            # Update status
            self.status = ComponentStatus.INITIALIZED
            return True
            
        except Exception as e:
            logger.error(f"Error initializing health dashboard: {str(e)}", exc_info=True)
            self.status = ComponentStatus.ERROR
            return False
    
    async def start(self) -> bool:
        """
        Start the health dashboard.
        
        Returns:
            bool: Success flag
        """
        try:
            # Start background tasks
            self.running = True
            self.update_task = asyncio.create_task(self._periodic_update())
            self.cleanup_task = asyncio.create_task(self._cleanup_old_connections())
            
            # Get initial component status
            self._update_component_status()
            
            # Add startup alert
            self._add_alert(
                "System Health Monitoring Started",
                "Health dashboard is now monitoring all system components.",
                AlertLevel.INFO
            )
            
            logger.info("Health dashboard started background tasks")
            return True
            
        except Exception as e:
            logger.error(f"Error starting health dashboard: {str(e)}", exc_info=True)
            return False
    
    async def stop(self) -> bool:
        """
        Stop the health dashboard.
        
        Returns:
            bool: Success flag
        """
        try:
            # Stop background tasks
            self.running = False
            
            if self.update_task:
                self.update_task.cancel()
                try:
                    await self.update_task
                except asyncio.CancelledError:
                    pass
            
            if self.cleanup_task:
                self.cleanup_task.cancel()
                try:
                    await self.cleanup_task
                except asyncio.CancelledError:
                    pass
            
            # Close all SSE connections
            for client in list(self.sse_clients):
                try:
                    await client.write_eof()
                except Exception:
                    pass
            self.sse_clients.clear()
            
            # Stop web server
            if self.site:
                await self.site.stop()
            
            if self.runner:
                await self.runner.cleanup()
            
            logger.info("Health dashboard stopped")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping health dashboard: {str(e)}", exc_info=True)
            return False
    
    def _setup_routes(self):
        """Set up web routes for the dashboard."""
        # Static files
        self.app.router.add_static('/static', os.path.join(os.path.dirname(__file__), 'static'))
        
        # Main routes
        self.app.router.add_get('/', self._handle_index)
        self.app.router.add_get('/components', self._handle_components)
        self.app.router.add_get('/metrics', self._handle_metrics)
        self.app.router.add_get('/alerts', self._handle_alerts)
        self.app.router.add_get('/failover', self._handle_failover)
        
        # API routes
        self.app.router.add_get('/api/system-health', self._api_system_health)
        self.app.router.add_get('/api/components', self._api_components)
        self.app.router.add_get('/api/metrics', self._api_metrics)
        self.app.router.add_get('/api/alerts', self._api_alerts)
        self.app.router.add_get('/api/failover-events', self._api_failover_events)
        self.app.router.add_get('/api/history', self._api_history)
        
        # SSE route for real-time updates
        self.app.router.add_get('/api/events', self._handle_sse)
        
        # Actions
        self.app.router.add_post('/api/actions/trigger-failover', self._api_trigger_failover)
        self.app.router.add_post('/api/actions/reset-component', self._api_reset_component)
    
    def _setup_templates(self):
        """Set up Jinja2 templates."""
        template_dir = os.path.join(os.path.dirname(__file__), 'templates')
        aiohttp_jinja2.setup(
            self.app,
            loader=jinja2.FileSystemLoader(template_dir)
        )
    
    def _register_message_handlers(self):
        """Register handlers for relevant message types."""
        # Component status
        self.message_bus.subscribe(
            MessageTypes.COMPONENT_STATUS,
            self._handle_component_status_message
        )
        
        # Component health
        self.message_bus.subscribe(
            MessageTypes.COMPONENT_HEALTH_CHANGED,
            self._handle_component_health_message
        )
        
        # Component metrics
        self.message_bus.subscribe(
            MessageTypes.COMPONENT_METRICS,
            self._handle_component_metrics_message
        )
        
        # Failover events
        self.message_bus.subscribe(
            MessageTypes.FAILOVER_EXECUTED,
            self._handle_failover_executed_message
        )
        
        self.message_bus.subscribe(
            MessageTypes.FAILOVER_COMPLETED,
            self._handle_failover_completed_message
        )
        
        self.message_bus.subscribe(
            MessageTypes.FAILOVER_FAILED,
            self._handle_failover_failed_message
        )
        
        # System errors
        self.message_bus.subscribe(
            MessageTypes.SYSTEM_ERROR,
            self._handle_system_error_message
        )
        
        # Circuit breaker events
        self.message_bus.subscribe(
            MessageTypes.CIRCUIT_BREAKER_OPENED,
            self._handle_circuit_breaker_message
        )
        
        self.message_bus.subscribe(
            MessageTypes.CIRCUIT_BREAKER_CLOSED,
            self._handle_circuit_breaker_message
        )
    
    async def _periodic_update(self):
        """Periodically update system state and notify clients."""
        try:
            while self.running:
                # Update component status
                self._update_component_status()
                
                # Update system health
                self._update_system_health()
                
                # Update history
                self._update_history()
                
                # Send updates to clients
                await self._broadcast_updates()
                
                # Wait for next update
                await asyncio.sleep(self.config.get("update_interval_ms", 1000) / 1000)
                
        except asyncio.CancelledError:
            logger.info("Health dashboard update task cancelled")
        except Exception as e:
            logger.error(f"Error in health dashboard update task: {str(e)}", exc_info=True)
    
    async def _cleanup_old_connections(self):
        """Clean up closed SSE connections."""
        try:
            while self.running:
                # Remove closed connections
                closed = set()
                for client in self.sse_clients:
                    if client.transport is None or client.transport.is_closing():
                        closed.add(client)
                
                self.sse_clients -= closed
                
                # Wait before next cleanup
                await asyncio.sleep(5)
                
        except asyncio.CancelledError:
            logger.info("Health dashboard cleanup task cancelled")
        except Exception as e:
            logger.error(f"Error in health dashboard cleanup task: {str(e)}", exc_info=True)
    
    def _update_component_status(self):
        """Update component status from failover manager."""
        if not self.failover_manager:
            return
        
        for comp_id, comp_data in self.failover_manager.components.items():
            component = comp_data.get("instance")
            if not component:
                continue
                
            health = comp_data.get("health", ComponentHealth.WARNING)
            
            # Update or add component
            if comp_id not in self.components:
                self.components[comp_id] = {
                    "id": comp_id,
                    "name": component.name,
                    "status": component.status.value,
                    "health": health.value,
                    "failover_group": comp_data.get("failover_group"),
                    "is_primary": comp_data.get("is_primary", False),
                    "last_update": datetime.datetime.now().isoformat(),
                    "metrics": {}
                }
            else:
                self.components[comp_id].update({
                    "status": component.status.value,
                    "health": health.value,
                    "last_update": datetime.datetime.now().isoformat()
                })
    
    def _update_system_health(self):
        """Update overall system health status."""
        if not self.failover_manager:
            return
            
        self.system_health = self.failover_manager.get_system_health()
    
    def _update_history(self):
        """Update historical data for charts."""
        # Add current timestamp
        now = datetime.datetime.now().isoformat()
        self.history["timestamps"].append(now)
        
        # Trim timestamps if over limit
        history_size = self.config.get("history_size", 100)
        if len(self.history["timestamps"]) > history_size:
            self.history["timestamps"] = self.history["timestamps"][-history_size:]
        
        # Update component health history
        for comp_id, comp_data in self.components.items():
            if comp_id not in self.history["component_health"]:
                self.history["component_health"][comp_id] = []
            
            # Add current health
            health_value = comp_data.get("health")
            self.history["component_health"][comp_id].append(health_value)
            
            # Trim if over limit
            if len(self.history["component_health"][comp_id]) > history_size:
                self.history["component_health"][comp_id] = self.history["component_health"][comp_id][-history_size:]
        
        # Update metrics history
        for metric_name, values in self.metrics.items():
            if metric_name not in self.history["metrics"]:
                self.history["metrics"][metric_name] = []
            
            # Add current value
            if "value" in values:
                self.history["metrics"][metric_name].append(values["value"])
                
                # Trim if over limit
                if len(self.history["metrics"][metric_name]) > history_size:
                    self.history["metrics"][metric_name] = self.history["metrics"][metric_name][-history_size:]
    
    def _add_alert(self, title: str, message: str, level: AlertLevel, 
                  component_id: Optional[str] = None, data: Optional[Dict[str, Any]] = None):
        """
        Add a new alert.
        
        Args:
            title: Alert title
            message: Alert message
            level: Alert level
            component_id: Optional component ID
            data: Optional additional data
        """
        alert = {
            "id": len(self.alerts) + 1,
            "title": title,
            "message": message,
            "level": level.value,
            "timestamp": datetime.datetime.now().isoformat(),
            "component_id": component_id,
            "data": data or {}
        }
        
        self.alerts.append(alert)
        
        # Trim alerts list if too long
        max_alerts = self.config.get("max_alerts", 1000)
        if len(self.alerts) > max_alerts:
            self.alerts = self.alerts[-max_alerts:]
        
        # Log alert
        log_level = logging.INFO
        if level == AlertLevel.WARNING:
            log_level = logging.WARNING
        elif level in (AlertLevel.ERROR, AlertLevel.CRITICAL):
            log_level = logging.ERROR
            
        logger.log(log_level, f"ALERT: {title} - {message}")
    
    async def _broadcast_updates(self):
        """Broadcast updates to all connected SSE clients."""
        if not self.sse_clients:
            return
            
        # Create update message
        update = {
            "type": "update",
            "timestamp": datetime.datetime.now().isoformat(),
            "system_health": self.system_health,
            "components": {
                comp_id: comp_data for comp_id, comp_data in self.components.items()
            }
        }
        
        message = f"data: {json.dumps(update)}\n\n"
        
        # Send to all clients
        closed_clients = set()
        for client in self.sse_clients:
            try:
                await client.write(message.encode('utf-8'))
                await client.drain()
            except (ConnectionResetError, RuntimeError, ConnectionError):
                closed_clients.add(client)
            except Exception as e:
                logger.error(f"Error sending update to client: {str(e)}")
                closed_clients.add(client)
        
        # Remove closed clients
        self.sse_clients -= closed_clients
    
    # Message handlers
    
    def _handle_component_status_message(self, message):
        """Handle component status message."""
        comp_id = message.get("component_id")
        if not comp_id:
            return
            
        status = message.get("status")
        if not status:
            return
            
        # Update component status
        if comp_id in self.components:
            prev_status = self.components[comp_id].get("status")
            self.components[comp_id]["status"] = status
            self.components[comp_id]["last_update"] = datetime.datetime.now().isoformat()
            
            # Add alert for status changes
            if prev_status != status:
                level = AlertLevel.INFO
                if status == ComponentStatus.ERROR.value:
                    level = AlertLevel.ERROR
                elif status == ComponentStatus.DEGRADED.value:
                    level = AlertLevel.WARNING
                    
                self._add_alert(
                    f"Component Status Change: {comp_id}",
                    f"Component {comp_id} status changed from {prev_status} to {status}",
                    level,
                    comp_id
                )
    
    def _handle_component_health_message(self, message):
        """Handle component health message."""
        comp_id = message.get("component_id")
        if not comp_id:
            return
            
        health = message.get("new_health")
        prev_health = message.get("previous_health")
        
        if not health or not prev_health:
            return
            
        # Update component health
        if comp_id in self.components:
            self.components[comp_id]["health"] = health
            self.components[comp_id]["last_update"] = datetime.datetime.now().isoformat()
            
            # Add alert for health changes to worse
            if health in [ComponentHealth.CRITICAL.value, ComponentHealth.FAILED.value]:
                level = AlertLevel.ERROR if health == ComponentHealth.FAILED.value else AlertLevel.WARNING
                
                self._add_alert(
                    f"Component Health Degraded: {comp_id}",
                    f"Component {comp_id} health changed from {prev_health} to {health}",
                    level,
                    comp_id
                )
    
    def _handle_component_metrics_message(self, message):
        """Handle component metrics message."""
        comp_id = message.get("component_id")
        if not comp_id:
            return
            
        metrics = message.get("metrics")
        if not metrics:
            return
            
        # Update component metrics
        if comp_id in self.components:
            self.components[comp_id]["metrics"] = metrics
            self.components[comp_id]["last_update"] = datetime.datetime.now().isoformat()
            
        # Also update global metrics collection
        for metric_name, value in metrics.items():
            # Create full metric name with component id
            full_name = f"{comp_id}.{metric_name}"
            
            self.metrics[full_name] = {
                "component_id": comp_id,
                "name": metric_name,
                "value": value,
                "timestamp": datetime.datetime.now().isoformat()
            }
    
    def _handle_failover_executed_message(self, message):
        """Handle failover executed message."""
        failover_group = message.get("failover_group")
        prev_component = message.get("previous_component")
        new_component = message.get("new_component")
        reason = message.get("reason")
        
        if not all([failover_group, prev_component, new_component, reason]):
            return
            
        # Add failover event
        failover_event = {
            "id": len(self.failover_events) + 1,
            "failover_group": failover_group,
            "previous_component": prev_component,
            "new_component": new_component,
            "reason": reason,
            "status": "executed",
            "timestamp": datetime.datetime.now().isoformat()
        }
        
        self.failover_events.append(failover_event)
        
        # Trim failover events list if too long
        max_events = self.config.get("max_failover_events", 1000)
        if len(self.failover_events) > max_events:
            self.failover_events = self.failover_events[-max_events:]
        
        # Add alert
        level = AlertLevel.WARNING if reason.startswith("AUTOMATIC") else AlertLevel.INFO
        
        self._add_alert(
            f"Failover Executed: {failover_group}",
            f"Failover from {prev_component} to {new_component} (Reason: {reason})",
            level,
            data={"failover_group": failover_group}
        )
    
    def _handle_failover_completed_message(self, message):
        """Handle failover completed message."""
        failover_id = message.get("failover_id")
        if not failover_id:
            return
            
        # Update failover event
        for event in self.failover_events:
            if event.get("id") == failover_id:
                event["status"] = "completed"
                event["completion_time"] = datetime.datetime.now().isoformat()
                
                # Add alert
                self._add_alert(
                    f"Failover Completed: {event.get('failover_group')}",
                    f"Failover to {event.get('new_component')} completed successfully",
                    AlertLevel.INFO,
                    data={"failover_group": event.get("failover_group")}
                )
                
                break
    
    def _handle_failover_failed_message(self, message):
        """Handle failover failed message."""
        failover_id = message.get("failover_id")
        error = message.get("error")
        
        if not failover_id:
            return
            
        # Update failover event
        for event in self.failover_events:
            if event.get("id") == failover_id:
                event["status"] = "failed"
                event["error"] = error
                event["failure_time"] = datetime.datetime.now().isoformat()
                
                # Add alert
                self._add_alert(
                    f"Failover Failed: {event.get('failover_group')}",
                    f"Failover to {event.get('new_component')} failed: {error}",
                    AlertLevel.ERROR,
                    data={"failover_group": event.get("failover_group")}
                )
                
                break
    
    def _handle_system_error_message(self, message):
        """Handle system error message."""
        error = message.get("error")
        source = message.get("source")
        
        if not error:
            return
            
        # Add alert
        self._add_alert(
            f"System Error: {source or 'Unknown Source'}",
            error,
            AlertLevel.ERROR,
            component_id=source
        )
    
    def _handle_circuit_breaker_message(self, message):
        """Handle circuit breaker message."""
        name = message.get("name")
        status = message.get("status")
        
        if not name or not status:
            return
            
        # Add alert
        action = "opened" if message.get("type") == MessageTypes.CIRCUIT_BREAKER_OPENED.value else "closed"
        level = AlertLevel.WARNING if action == "opened" else AlertLevel.INFO
        
        self._add_alert(
            f"Circuit Breaker {action.capitalize()}: {name}",
            f"Circuit breaker for {name} was {action} after {message.get('failure_count', 'N/A')} failures",
            level
        )
    
    # Web handlers
    
    async def _handle_index(self, request):
        """Handle index page request."""
        return aiohttp_jinja2.render_template(
            'index.html',
            request,
            {
                "title": self.config.get("title", "Trading System Health Dashboard"),
                "theme": self.config.get("theme", "dark"),
                "system_health": self.system_health
            }
        )
    
    async def _handle_components(self, request):
        """Handle components page request."""
        return aiohttp_jinja2.render_template(
            'components.html',
            request,
            {
                "title": "Components - " + self.config.get("title", "Trading System Health Dashboard"),
                "theme": self.config.get("theme", "dark"),
                "components": self.components
            }
        )
    
    async def _handle_metrics(self, request):
        """Handle metrics page request."""
        return aiohttp_jinja2.render_template(
            'metrics.html',
            request,
            {
                "title": "Metrics - " + self.config.get("title", "Trading System Health Dashboard"),
                "theme": self.config.get("theme", "dark"),
                "metrics": self.metrics,
                "history": self.history
            }
        )
    
    async def _handle_alerts(self, request):
        """Handle alerts page request."""
        return aiohttp_jinja2.render_template(
            'alerts.html',
            request,
            {
                "title": "Alerts - " + self.config.get("title", "Trading System Health Dashboard"),
                "theme": self.config.get("theme", "dark"),
                "alerts": self.alerts
            }
        )
    
    async def _handle_failover(self, request):
        """Handle failover page request."""
        return aiohttp_jinja2.render_template(
            'failover.html',
            request,
            {
                "title": "Failover - " + self.config.get("title", "Trading System Health Dashboard"),
                "theme": self.config.get("theme", "dark"),
                "failover_events": self.failover_events,
                "failover_groups": self.failover_manager.failover_groups if self.failover_manager else {}
            }
        )
    
    async def _handle_sse(self, request):
        """Handle SSE connections for real-time updates."""
        response = web.StreamResponse()
        response.headers['Content-Type'] = 'text/event-stream'
        response.headers['Cache-Control'] = 'no-cache'
        response.headers['Connection'] = 'keep-alive'
        response.headers['X-Accel-Buffering'] = 'no'  # Disable proxy buffering
        
        await response.prepare(request)
        
        # Add client to active connections
        self.sse_clients.add(response)
        
        # Send initial data
        initial_data = {
            "type": "initial",
            "timestamp": datetime.datetime.now().isoformat(),
            "system_health": self.system_health,
            "components": self.components
        }
        
        try:
            await response.write(f"data: {json.dumps(initial_data)}\n\n".encode('utf-8'))
            await response.drain()
            
            # Keep connection open until client disconnects
            while True:
                await asyncio.sleep(15)
                
                # Send heartbeat
                await response.write(b"event: heartbeat\ndata: heartbeat\n\n")
                await response.drain()
                
        except (ConnectionResetError, RuntimeError, ConnectionError):
            # Client disconnected
            if response in self.sse_clients:
                self.sse_clients.remove(response)
            return response
            
        except Exception as e:
            logger.error(f"Error in SSE connection: {str(e)}")
            if response in self.sse_clients:
                self.sse_clients.remove(response)
            return response
    
    # API handlers
    
    async def _api_system_health(self, request):
        """API endpoint for system health."""
        return web.json_response(self.system_health)
    
    async def _api_components(self, request):
        """API endpoint for components."""
        return web.json_response(list(self.components.values()))
    
    async def _api_metrics(self, request):
        """API endpoint for metrics."""
        return web.json_response(list(self.metrics.values()))
    
    async def _api_alerts(self, request):
        """API endpoint for alerts."""
        return web.json_response(self.alerts)
    
    async def _api_failover_events(self, request):
        """API endpoint for failover events."""
        return web.json_response(self.failover_events)
    
    async def _api_history(self, request):
        """API endpoint for historical data."""
        return web.json_response(self.history)
    
    async def _api_trigger_failover(self, request):
        """API endpoint for triggering failover."""
        try:
            data = await request.json()
            
            failover_group = data.get("failover_group")
            target_component_id = data.get("target_component_id")
            
            if not failover_group or not target_component_id:
                return web.json_response(
                    {"error": "Missing required parameters"},
                    status=400
                )
            
            # Trigger failover
            if not self.failover_manager:
                return web.json_response(
                    {"error": "Failover manager not available"},
                    status=500
                )
            
            success = self.failover_manager.trigger_manual_failover(
                failover_group, target_component_id
            )
            
            if success:
                return web.json_response({"status": "success"})
            else:
                return web.json_response(
                    {"error": "Failed to trigger failover"},
                    status=500
                )
                
        except Exception as e:
            logger.error(f"Error triggering failover: {str(e)}", exc_info=True)
            return web.json_response(
                {"error": str(e)},
                status=500
            )
    
    async def _api_reset_component(self, request):
        """API endpoint for resetting a component."""
        try:
            data = await request.json()
            
            component_id = data.get("component_id")
            
            if not component_id:
                return web.json_response(
                    {"error": "Missing component_id parameter"},
                    status=400
                )
            
            # Get component
            if not self.failover_manager or component_id not in self.failover_manager.components:
                return web.json_response(
                    {"error": f"Component {component_id} not found"},
                    status=404
                )
            
            component_data = self.failover_manager.components.get(component_id)
            component = component_data.get("instance")
            
            if not component:
                return web.json_response(
                    {"error": f"Component instance {component_id} not found"},
                    status=404
                )
            
            # Send reset request via message bus
            await self.message_bus.publish(
                MessageTypes.COMPONENT_STATUS,
                {
                    "component_id": component_id,
                    "action": "reset",
                    "source": "health_dashboard"
                }
            )
            
            return web.json_response({"status": "reset_requested"})
                
        except Exception as e:
            logger.error(f"Error resetting component: {str(e)}", exc_info=True)
            return web.json_response(
                {"error": str(e)},
                status=500
            )


def create_health_dashboard(message_bus: Optional[MessageBus] = None, 
                           config: Optional[Dict[str, Any]] = None) -> HealthDashboard:
    """
    Create a health dashboard instance.
    
    Args:
        message_bus: Optional message bus instance
        config: Optional configuration
        
    Returns:
        HealthDashboard: The dashboard instance
    """
    # Get message bus if not provided
    if message_bus is None:
        from trading_system.core.message_bus import get_message_bus
        message_bus = get_message_bus()
    
    # Create dashboard
    dashboard = HealthDashboard(message_bus, config)
    
    return dashboard 