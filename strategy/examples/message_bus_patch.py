"""
Message Bus Patch Module

This module patches the MessageBus class to handle both positional and keyword arguments
for the subscribe method to fix compatibility with market_data_facade.py.
"""

import asyncio
import inspect
from trading_system.core.message_bus import MessageBus
from enum import Enum
from typing import Callable

# Create a wrapper for string message types to make them behave like enums
class StringMessageType:
    def __init__(self, value):
        self.value = value
    
    def __str__(self):
        return self.value
    
    def __repr__(self):
        return f"StringMessageType({self.value})"
    
    def __eq__(self, other):
        if isinstance(other, str):
            return self.value == other
        if hasattr(other, 'value'):
            return self.value == other.value
        return False
        
    def __hash__(self):
        """Make this class hashable so it can be used as a dictionary key."""
        return hash(self.value)

# Create a wrapper for the subscribe method
original_subscribe = MessageBus.subscribe

# Check if the original method is async
is_async = inspect.iscoroutinefunction(original_subscribe)

if is_async:
    # Create async patched version
    async def patched_subscribe_async(self, *args, **kwargs):
        """
        Patched version of async subscribe that handles both positional and keyword arguments.
        
        This is needed because market_data_facade.py calls subscribe with keyword arguments
        (topic and callback), but the original implementation expects positional arguments.
        """
        # Handle keyword args
        if kwargs and not args:
            topic = kwargs.get('topic')
            callback = kwargs.get('callback')
            if topic and callback:
                # Convert string topic to wrapper with 'value' attribute
                if isinstance(topic, str):
                    topic = StringMessageType(topic)
                return await original_subscribe(self, topic, callback)
        
        # Handle positional args - wrap first arg if it's a string
        if args and len(args) >= 1 and isinstance(args[0], str):
            # Convert string to wrapper
            topic = StringMessageType(args[0])
            new_args = (topic,) + args[1:]
            return await original_subscribe(self, *new_args)
        
        # Pass through without changes
        return await original_subscribe(self, *args)
        
    # Replace method
    MessageBus.subscribe = patched_subscribe_async
else:
    # Create sync patched version
    def patched_subscribe_sync(self, *args, **kwargs):
        """
        Patched version of sync subscribe that handles both positional and keyword arguments.
        """
        # Handle keyword args
        if kwargs and not args:
            topic = kwargs.get('topic')
            callback = kwargs.get('callback')
            if topic and callback:
                # Convert string topic to wrapper with 'value' attribute
                if isinstance(topic, str):
                    topic = StringMessageType(topic)
                return original_subscribe(self, topic, callback)
        
        # Handle positional args - wrap first arg if it's a string
        if args and len(args) >= 1 and isinstance(args[0], str):
            # Convert string to wrapper
            topic = StringMessageType(args[0])
            new_args = (topic,) + args[1:]
            return original_subscribe(self, *new_args)
        
        # Pass through without changes
        return original_subscribe(self, *args)
        
    # Replace method
    MessageBus.subscribe = patched_subscribe_sync 