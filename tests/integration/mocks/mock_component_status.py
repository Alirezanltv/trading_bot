"""
Mock Component Status

This module provides component status enumeration for integration testing.
"""

from enum import Enum, auto


class ComponentStatus(Enum):
    """Component status enumeration."""
    
    # Component lifecycle statuses
    UNINITIALIZED = auto()
    INITIALIZING = auto()
    INITIALIZED = auto()
    STARTING = auto()
    OPERATIONAL = auto()
    STOPPING = auto()
    SHUTDOWN = auto()
    
    # Error states
    ERROR = auto()
    DEGRADED = auto()
    CRITICAL = auto()
    RECOVERING = auto()
    
    # Special states
    MAINTENANCE = auto()
    SUSPENDED = auto()


# Export at module level
__all__ = ['ComponentStatus'] 