"""
Component Base Class

This module implements the base Component class that all subsystems inherit from.
"""

import asyncio
import uuid
from enum import Enum
from typing import Dict, Any, Optional
from datetime import datetime
import logging


class ComponentStatus(Enum):
    """Status of a component in the system."""
    INITIALIZING = "initializing"
    INITIALIZED = "initialized"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"
    DEGRADED = "degraded"
    MAINTENANCE = "maintenance"


class Component:
    """
    Base component class that all subsystems inherit from.
    
    Provides common functionality such as:
    - Lifecycle management (initialization, shutdown)
    - Status tracking and health monitoring
    - Configuration management
    - Metrics collection
    """
    
    def __init__(self, component_id: Optional[str] = None, name: Optional[str] = None, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the component.
        
        Args:
            component_id: Unique identifier for the component (auto-generated if not provided)
            name: Human-readable name for the component
            config: Component configuration
        """
        # Generate unique ID if not provided
        self.component_id = component_id or f"{self.__class__.__name__.lower()}_{uuid.uuid4().hex[:8]}"
        
        # Set component name
        self.name = name or self.__class__.__name__
        
        # Store configuration
        self.config = config or {}
        
        # Status tracking
        self._status = ComponentStatus.INITIALIZING
        self._status_changed_at = datetime.now()
        self._status_history = [
            {
                "status": self._status.value,
                "timestamp": self._status_changed_at.isoformat()
            }
        ]
        
        # Health metrics
        self.startup_time = None
        self.last_activity = None
        self.error_count = 0
        self.warning_count = 0
        
        # Component is ready event
        self._ready = asyncio.Event()
        
        # Logging
        self._logger = logging.getLogger(f"component.{self.name}")
        self._lock = asyncio.Lock()
    
    @property
    def status(self) -> ComponentStatus:
        """Get the current status of the component."""
        return self._status
    
    async def set_status(self, new_status: ComponentStatus):
        """Set the status of the component.
        
        Args:
            new_status: New status to set
        """
        async with self._lock:
            if self._status != new_status:
                old_status = self._status
                self._status = new_status
                self._status_changed_at = datetime.now()
                self._logger.info(f"Status changed from {old_status.value} to {new_status.value}")
                
                # Add to status history
                self._status_history.append(
                    {
                        "status": self._status.value,
                        "timestamp": self._status_changed_at.isoformat()
                    }
                )
    
    async def initialize(self) -> bool:
        """
        Initialize the component.
        
        Returns:
            Success flag
        """
        try:
            await self.set_status(ComponentStatus.INITIALIZING)
            # Record startup time
            self.startup_time = datetime.now()
            
            # Set status to operational
            await self.set_status(ComponentStatus.INITIALIZED)
            
            # Signal that component is ready
            self._ready.set()
            
            return True
        except Exception as e:
            self._logger.error(f"Error initializing component: {str(e)}")
            await self.set_status(ComponentStatus.ERROR)
            return False
    
    async def shutdown(self) -> bool:
        """
        Shutdown the component.
        
        Returns:
            Success flag
        """
        try:
            if self._status not in (ComponentStatus.RUNNING, ComponentStatus.DEGRADED):
                self._logger.error("Cannot stop component: not running")
                return False
                
            await self.set_status(ComponentStatus.STOPPING)
            # Clear ready event
            self._ready.clear()
            
            # Set status to shutdown
            await self.set_status(ComponentStatus.STOPPED)
            
            return True
        except Exception as e:
            self._logger.error(f"Error stopping component: {str(e)}")
            await self.set_status(ComponentStatus.ERROR)
            return False
    
    async def wait_until_ready(self, timeout: Optional[float] = None) -> bool:
        """
        Wait until component is ready.
        
        Args:
            timeout: Maximum time to wait (in seconds)
            
        Returns:
            Success flag
        """
        try:
            # Wait for ready event
            await asyncio.wait_for(self._ready.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            # Timeout while waiting for component to be ready
            return False
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get component status.
        
        Returns:
            Status information
        """
        # Calculate uptime
        uptime = None
        if self.startup_time:
            uptime = str(datetime.now() - self.startup_time).split('.')[0]  # Format as HH:MM:SS
        
        # Return status information
        return {
            "component_id": self.component_id,
            "name": self.name,
            "status": self._status.value,
            "status_changed_at": self._status_changed_at.isoformat(),
            "uptime": uptime,
            "error_count": self.error_count,
            "warning_count": self.warning_count
        }
    
    def is_healthy(self) -> bool:
        """
        Check if component is healthy.
        
        Returns:
            Health status
        """
        return self._status in (ComponentStatus.INITIALIZED, ComponentStatus.INITIALIZING)
    
    def stop(self) -> None:
        """Stop the component (non-async version for signal handlers)."""
        # Update status
        self._update_status(ComponentStatus.STOPPING)
        
        # Clear ready event
        self._ready.clear()
        
        # Update status
        self._update_status(ComponentStatus.STOPPED) 

    def get_health(self) -> Dict[str, Any]:
        """Get health information about the component.
        
        Returns:
            Dict containing health information
        """
        return {
            "name": self.name,
            "status": self._status.value,
            "status_changed": self._status_changed_at.isoformat(),
            "uptime": (datetime.now() - self._status_changed_at).total_seconds()
        } 