"""
Enum fix module

This module provides fixes for enumeration mismatches between components.
"""

from trading_system.core.component import ComponentStatus
import types

# 1. Add INITIALIZED as an alias for OPERATIONAL
setattr(ComponentStatus, 'INITIALIZED', ComponentStatus.OPERATIONAL)

# 2. Ensure ComponentStatus values don't get converted to strings
# Patch ComponentStatus._value_ attribute to ensure it returns the enum instance
# This is a more aggressive fix that prevents string conversion issues
original_getattribute = ComponentStatus.__getattribute__

def patched_getattribute(self, name):
    """Patch to ensure enum values maintain their enum type."""
    result = original_getattribute(self, name)
    
    # If accessing the 'value' attribute of an enum instance
    if name == 'value' and isinstance(self, ComponentStatus):
        # Return a wrapper that preserves enum type
        return EnumValueWrapper(self, result)
    
    return result

# Custom wrapper for enum values to prevent string attribute errors
class EnumValueWrapper:
    def __init__(self, enum_instance, string_value):
        self.enum_instance = enum_instance
        self.string_value = string_value
    
    def __str__(self):
        return self.string_value
    
    def __eq__(self, other):
        if isinstance(other, str):
            return self.string_value == other
        if isinstance(other, ComponentStatus):
            return self.enum_instance == other
        if isinstance(other, EnumValueWrapper):
            return self.string_value == other.string_value
        return False
    
    # Forward any attribute access to the enum instance
    def __getattr__(self, name):
        return getattr(self.enum_instance, name)

# Apply the patch
ComponentStatus.__getattribute__ = patched_getattribute

# 3. Monkey patch _update_status method in Component class 
# to handle string status values (defensive approach)
from trading_system.core.component import Component

original_update_status = Component._update_status

def patched_update_status(self, status):
    """
    Update component status, with string handling fix.
    
    Args:
        status: New status (ComponentStatus enum or string)
    """
    # Convert string status to enum if needed
    if isinstance(status, str):
        for enum_value in ComponentStatus:
            if enum_value.value == status:
                status = enum_value
                break
    
    # Call original method with proper enum value
    return original_update_status(self, status)

Component._update_status = patched_update_status 