"""
Message Bus Module

This module provides a message bus for inter-component communication,
with support for both in-memory communication and distributed messaging via RabbitMQ.
"""

import asyncio
import enum
import os
import threading
from typing import Dict, Any, List, Callable, Optional, Set, Union, Type

from trading_system.core.logging import get_logger

# Check if RabbitMQ modules are available
try:
    from trading_system.core.rabbitmq_broker import rabbitmq_broker, DeliveryMode
    from trading_system.core.async_rabbitmq_broker import async_rabbitmq_broker
    RABBITMQ_AVAILABLE = True
except ImportError:
    RABBITMQ_AVAILABLE = False
    # Define DeliveryMode if not available from rabbitmq_broker
    class DeliveryMode(enum.Enum):
        """Message delivery mode enumeration."""
        TRANSIENT = 1  # Message is not persisted
        PERSISTENT = 2  # Message is persisted to disk

logger = get_logger("core.message_bus")

class MessageTypes(enum.Enum):
    """
    Message Types enumeration.
    
    This enum represents the possible message types that can be
    sent through the message bus.
    """
    # System messages
    SYSTEM_STARTUP = "system.startup"
    SYSTEM_SHUTDOWN = "system.shutdown"
    SYSTEM_ERROR = "system.error"
    SYSTEM_HEARTBEAT = "system.heartbeat"
    
    # Component messages
    COMPONENT_STATUS = "component.status"
    COMPONENT_STATUS_CHANGED = "component.status.changed"
    COMPONENT_ERROR = "component.error"
    COMPONENT_METRICS = "component.metrics"
    
    # High availability messages
    COMPONENT_HEALTH_CHANGED = "component.health.changed"
    FAILOVER_EXECUTED = "failover.executed"
    FAILOVER_COMPLETED = "failover.completed"
    FAILOVER_FAILED = "failover.failed"
    COMPONENT_RECOVERED = "component.recovered"
    CIRCUIT_BREAKER_OPENED = "circuit_breaker.opened"
    CIRCUIT_BREAKER_CLOSED = "circuit_breaker.closed"
    
    # Market data messages
    MARKET_DATA_UPDATE = "market_data.update"
    MARKET_DATA_ERROR = "market_data.error"
    MARKET_DATA_REQUEST = "market_data.request"
    MARKET_DATA_QUALITY = "market_data.quality"
    MARKET_DATA_RECONNECT = "market_data.reconnect"
    
    # Strategy messages
    STRATEGY_SIGNAL = "strategy.signal"
    STRATEGY_STATUS = "strategy.status"
    STRATEGY_ERROR = "strategy.error"
    STRATEGY_METRICS = "strategy.metrics"
    
    # Execution messages
    EXECUTION_ORDER_REQUEST = "execution.order_request"
    EXECUTION_ORDER_STATUS = "execution.order_status"
    EXECUTION_ORDER_ERROR = "execution.order_error"
    EXECUTION_ORDER_FILL = "execution.order_fill"
    EXECUTION_CANCEL_REQUEST = "execution.cancel_request"
    
    # Exchange messages
    EXCHANGE_ORDER = "exchange.order"
    EXCHANGE_ORDER_STATUS = "exchange.order_status"
    EXCHANGE_ERROR = "exchange.error"
    EXCHANGE_CANCEL = "exchange.cancel"
    EXCHANGE_RECONNECT = "exchange.reconnect"
    
    # Position messages
    POSITION_UPDATE = "position.update"
    POSITION_CLOSE = "position.close"
    POSITION_ERROR = "position.error"
    POSITION_RECONCILE = "position.reconcile"
    
    # Risk messages
    RISK_LIMIT_BREACH = "risk.limit_breach"
    RISK_EXPOSURE_UPDATE = "risk.exposure_update"
    RISK_ERROR = "risk.error"
    RISK_STOP_LOSS = "risk.stop_loss"
    RISK_EMERGENCY_CLOSE = "risk.emergency_close"


class MessageBusMode(enum.Enum):
    """Message bus operating mode."""
    IN_MEMORY = "in_memory"
    RABBITMQ = "rabbitmq"


class MessageBus:
    """
    Message Bus
    
    This class implements a pub/sub message bus for inter-component
    communication within the trading system. It supports both in-memory
    messaging and distributed messaging via RabbitMQ.
    """
    
    def __init__(self, mode: MessageBusMode = None):
        """
        Initialize message bus.
        
        Args:
            mode: Operating mode (in-memory or RabbitMQ)
        """
        # Determine mode from environment if not specified
        if mode is None:
            mode_str = os.environ.get("TRADING_SYSTEM_MESSAGE_BUS_MODE", "in_memory").lower()
            try:
                mode = MessageBusMode(mode_str)
            except ValueError:
                logger.warning(f"Invalid message bus mode '{mode_str}', falling back to in-memory")
                mode = MessageBusMode.IN_MEMORY
                
        # Check if RabbitMQ is available if requested
        if mode == MessageBusMode.RABBITMQ and not RABBITMQ_AVAILABLE:
            logger.warning("RabbitMQ mode requested but RabbitMQ modules not available, falling back to in-memory")
            mode = MessageBusMode.IN_MEMORY
            
        self._mode = mode
        
        # In-memory state
        self._subscribers: Dict[MessageTypes, List[Callable]] = {}
        self._message_queue = asyncio.Queue()
        self._processing = False
        self._processing_task = None
        
        # RabbitMQ state
        self._rabbitmq_connected = False
        if self._mode == MessageBusMode.RABBITMQ and RABBITMQ_AVAILABLE:
            try:
                self._rabbitmq_connected = rabbitmq_broker.connect()
                if not self._rabbitmq_connected:
                    logger.error("Failed to connect to RabbitMQ, falling back to in-memory")
                    self._mode = MessageBusMode.IN_MEMORY
            except Exception as e:
                logger.error(f"Error connecting to RabbitMQ: {str(e)}, falling back to in-memory")
                self._mode = MessageBusMode.IN_MEMORY
        
        # Thread-local storage for async event loop
        self._local = threading.local()
        
        logger.info(f"Message bus initialized in {self._mode.value} mode")
        
    def get_mode(self) -> MessageBusMode:
        """
        Get current operating mode.
        
        Returns:
            MessageBusMode: Current mode
        """
        return self._mode
        
    def _get_event_loop(self) -> asyncio.AbstractEventLoop:
        """
        Get event loop for current thread.
        
        Returns:
            asyncio.AbstractEventLoop: Event loop
        """
        try:
            return self._local.loop
        except AttributeError:
            try:
                self._local.loop = asyncio.get_event_loop()
            except RuntimeError:
                # No event loop in thread, create one
                self._local.loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self._local.loop)
            return self._local.loop
    
    async def publish(self, message_type: Union[MessageTypes, str], data: Dict[str, Any], 
                      delivery_mode: Optional[DeliveryMode] = None) -> None:
        """
        Publish a message to the bus.
        
        Args:
            message_type: Message type
            data: Message data
            delivery_mode: Delivery mode (for RabbitMQ, ignored for in-memory)
        """
        try:
            # Ensure we have a proper MessageType
            if isinstance(message_type, str):
                try:
                    message_type = MessageTypes(message_type)
                except ValueError:
                    logger.error(f"Invalid message type string: {message_type}")
                    return
                
            # Add message type to data if not present
            if "type" not in data:
                data["type"] = message_type.value
                
            if self._mode == MessageBusMode.RABBITMQ and self._rabbitmq_connected:
                # Use RabbitMQ for messaging
                dm = delivery_mode or DeliveryMode.PERSISTENT
                try:
                    success = rabbitmq_broker.publish(message_type.value, data, dm)
                    if not success:
                        logger.error(f"Error publishing to RabbitMQ: {message_type.value}, falling back to in-memory")
                        await self._publish_in_memory(message_type, data)
                except Exception as e:
                    logger.error(f"Error publishing to RabbitMQ: {str(e)}, falling back to in-memory")
                    await self._publish_in_memory(message_type, data)
            else:
                # Use in-memory messaging
                await self._publish_in_memory(message_type, data)
                    
        except Exception as e:
            logger.error(f"Error publishing message {message_type.value}: {str(e)}", exc_info=True)
    
    async def _publish_in_memory(self, message_type: MessageTypes, data: Dict[str, Any]) -> None:
        """
        Publish a message in-memory.
        
        Args:
            message_type: Message type
            data: Message data
        """
        try:
            # Get subscribers for this message type
            handlers = self._subscribers.get(message_type, [])
            
            if not handlers:
                # Skip if no subscribers
                return
                
            # Call all subscribers
            for handler in handlers:
                try:
                    if asyncio.iscoroutinefunction(handler):
                        # Schedule coroutine
                        asyncio.create_task(handler(message_type, data))
                    else:
                        # Call directly
                        handler(message_type, data)
                except Exception as e:
                    logger.error(f"Error in message handler for {message_type.value}: {str(e)}")
                    
        except Exception as e:
            logger.error(f"Error in _publish_in_memory: {str(e)}")
    
    def publish_sync(self, message_type: Union[MessageTypes, str], data: Dict[str, Any],
                     delivery_mode: Optional[DeliveryMode] = None) -> None:
        """
        Publish a message to the bus synchronously.
        
        Args:
            message_type: Message type
            data: Message data
            delivery_mode: Delivery mode (for RabbitMQ, ignored for in-memory)
        """
        # Get the event loop
        loop = self._get_event_loop()
        
        if loop.is_running():
            # Create a future that will be set when the publish is complete
            future = asyncio.run_coroutine_threadsafe(
                self.publish(message_type, data, delivery_mode), loop
            )
            # Wait for the future to complete
            future.result()
        else:
            # Run the publish coroutine in the event loop
            loop.run_until_complete(self.publish(message_type, data, delivery_mode))
    
    async def subscribe(self, message_type: Union[MessageTypes, str], callback: Callable) -> None:
        """
        Subscribe to messages of a specific type.
        
        Args:
            message_type: Message type
            callback: Callback function
        """
        try:
            # Ensure we have a proper MessageType
            if isinstance(message_type, str):
                try:
                    message_type = MessageTypes(message_type)
                except ValueError:
                    logger.error(f"Invalid message type string: {message_type}")
                    return
            
            # Set up RabbitMQ subscription if in RabbitMQ mode
            if self._mode == MessageBusMode.RABBITMQ and self._rabbitmq_connected:
                try:
                    # Define the handler for received messages
                    def rabbitmq_handler(data: Dict[str, Any], headers: Dict[str, Any]) -> bool:
                        try:
                            # Call the callback
                            if asyncio.iscoroutinefunction(callback):
                                # Schedule coroutine
                                asyncio.create_task(callback(message_type, data))
                            else:
                                # Call directly
                                callback(message_type, data)
                            return True
                        except Exception as e:
                            logger.error(f"Error in RabbitMQ message handler for {message_type.value}: {str(e)}")
                            return False
                            
                    # Create a queue for this message type
                    queue_name = f"queue.{message_type.value}"
                    routing_key = message_type.value
                    
                    # Declare queue and binding
                    rabbitmq_broker.declare_queue(queue_name)
                    rabbitmq_broker.bind_queue(queue_name, routing_key)
                    
                    # Set up consumer
                    rabbitmq_broker.consume(queue_name, rabbitmq_handler)
                    
                except Exception as e:
                    logger.error(f"Error setting up RabbitMQ subscription for {message_type.value}: {str(e)}")
            
            # Always register in-memory handler as fallback
            if message_type not in self._subscribers:
                self._subscribers[message_type] = []
                
            # Add callback if not already registered
            if callback not in self._subscribers[message_type]:
                self._subscribers[message_type].append(callback)
                
        except Exception as e:
            logger.error(f"Error in subscribe: {str(e)}")
    
    def subscribe_sync(self, message_type: Union[MessageTypes, str], callback: Callable) -> None:
        """
        Subscribe to messages of a specific type synchronously.
        
        Args:
            message_type: Message type
            callback: Callback function
        """
        # Get the event loop
        loop = self._get_event_loop()
        
        if loop.is_running():
            # Create a future that will be set when the subscribe is complete
            future = asyncio.run_coroutine_threadsafe(
                self.subscribe(message_type, callback), loop
            )
            # Wait for the future to complete
            future.result()
        else:
            # Run the subscribe coroutine in the event loop
            loop.run_until_complete(self.subscribe(message_type, callback))
    
    async def unsubscribe(self, message_type: Union[MessageTypes, str], callback: Callable) -> None:
        """
        Unsubscribe from messages of a specific type.
        
        Args:
            message_type: Message type
            callback: Callback function
        """
        try:
            # Ensure we have a proper MessageType
            if isinstance(message_type, str):
                try:
                    message_type = MessageTypes(message_type)
                except ValueError:
                    logger.error(f"Invalid message type string: {message_type}")
                    return
            
            # Remove from in-memory subscribers
            if message_type in self._subscribers and callback in self._subscribers[message_type]:
                self._subscribers[message_type].remove(callback)
                if not self._subscribers[message_type]:
                    del self._subscribers[message_type]
            
            # Note: For RabbitMQ, we don't remove the binding or queue, as other handlers might be using it.
            # We just stop calling this particular callback.
            
        except Exception as e:
            logger.error(f"Error in unsubscribe: {str(e)}")
    
    def unsubscribe_sync(self, message_type: Union[MessageTypes, str], callback: Callable) -> None:
        """
        Unsubscribe from messages of a specific type synchronously.
        
        Args:
            message_type: Message type
            callback: Callback function
        """
        # Get the event loop
        loop = self._get_event_loop()
        
        if loop.is_running():
            # Create a future that will be set when the unsubscribe is complete
            future = asyncio.run_coroutine_threadsafe(
                self.unsubscribe(message_type, callback), loop
            )
            # Wait for the future to complete
            future.result()
        else:
            # Run the unsubscribe coroutine in the event loop
            loop.run_until_complete(self.unsubscribe(message_type, callback))


class AsyncMessageBus:
    """
    Asynchronous Message Bus
    
    This class implements a pub/sub message bus for asynchronous inter-component
    communication within the trading system. It supports both in-memory
    messaging and distributed messaging via RabbitMQ.
    """
    
    def __init__(self, mode: MessageBusMode = None):
        """
        Initialize async message bus.
        
        Args:
            mode: Operating mode (in-memory or RabbitMQ)
        """
        # Determine mode from environment if not specified
        if mode is None:
            mode_str = os.environ.get("TRADING_SYSTEM_MESSAGE_BUS_MODE", "in_memory").lower()
            try:
                mode = MessageBusMode(mode_str)
            except ValueError:
                logger.warning(f"Invalid message bus mode '{mode_str}', falling back to in-memory")
                mode = MessageBusMode.IN_MEMORY
                
        # Check if RabbitMQ is available if requested
        if mode == MessageBusMode.RABBITMQ and not RABBITMQ_AVAILABLE:
            logger.warning("RabbitMQ mode requested but RabbitMQ modules not available, falling back to in-memory")
            mode = MessageBusMode.IN_MEMORY
            
        self._mode = mode
        
        # In-memory state
        self._subscribers: Dict[MessageTypes, List[Callable]] = {}
        self._message_queue = asyncio.Queue()
        self._lock = asyncio.Lock()
        
        # Connection status
        self._is_initialized = False
        
        logger.info(f"Async message bus initialized in {self._mode.value} mode")
    
    async def initialize(self) -> bool:
        """
        Initialize the async message bus.
        
        Returns:
            bool: Success flag
        """
        if self._is_initialized:
            return True
            
        try:
            # Initialize RabbitMQ if needed
            if self._mode == MessageBusMode.RABBITMQ and RABBITMQ_AVAILABLE:
                success = await async_rabbitmq_broker.connect()
                if not success:
                    logger.error("Failed to connect to RabbitMQ, falling back to in-memory")
                    self._mode = MessageBusMode.IN_MEMORY
            
            self._is_initialized = True
            return True
            
        except Exception as e:
            logger.error(f"Error initializing async message bus: {str(e)}", exc_info=True)
            return False
    
    async def publish(self, message_type: Union[MessageTypes, str], data: Dict[str, Any],
                      delivery_mode: Optional[DeliveryMode] = None) -> None:
        """
        Publish a message to the bus asynchronously.
        
        Args:
            message_type: Message type
            data: Message data
            delivery_mode: Delivery mode (for RabbitMQ, ignored for in-memory)
        """
        await self._ensure_initialized()
        
        try:
            # Ensure we have a proper MessageType
            if isinstance(message_type, str):
                try:
                    message_type = MessageTypes(message_type)
                except ValueError:
                    logger.error(f"Invalid message type string: {message_type}")
                    return
                
            # Add message type to data if not present
            if "type" not in data:
                data["type"] = message_type.value
                
            if self._mode == MessageBusMode.RABBITMQ and RABBITMQ_AVAILABLE:
                # Use RabbitMQ for messaging
                dm = delivery_mode or DeliveryMode.PERSISTENT
                try:
                    success = await async_rabbitmq_broker.publish(message_type.value, data, dm)
                    if not success:
                        logger.error(f"Error publishing to RabbitMQ: {message_type.value}, falling back to in-memory")
                        await self._publish_in_memory(message_type, data)
                except Exception as e:
                    logger.error(f"Error publishing to RabbitMQ: {str(e)}, falling back to in-memory")
                    await self._publish_in_memory(message_type, data)
            else:
                # Use in-memory messaging
                await self._publish_in_memory(message_type, data)
                    
        except Exception as e:
            logger.error(f"Error publishing message {message_type.value}: {str(e)}", exc_info=True)
    
    async def _publish_in_memory(self, message_type: MessageTypes, data: Dict[str, Any]) -> None:
        """
        Publish a message in-memory asynchronously.
        
        Args:
            message_type: Message type
            data: Message data
        """
        async with self._lock:
            # Get subscribers for this message type
            handlers = self._subscribers.get(message_type, [])
            
            if not handlers:
                # Skip if no subscribers
                return
                
            # Call all subscribers asynchronously
            tasks = []
            for handler in handlers:
                try:
                    if asyncio.iscoroutinefunction(handler):
                        # Create task
                        task = asyncio.create_task(handler(message_type, data))
                        tasks.append(task)
                    else:
                        # Call directly
                        handler(message_type, data)
                except Exception as e:
                    logger.error(f"Error in async message handler for {message_type.value}: {str(e)}")
                    
            # Wait for all handler tasks to complete
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
    
    async def subscribe(self, message_type: Union[MessageTypes, str], callback: Callable) -> None:
        """
        Subscribe to messages of a specific type asynchronously.
        
        Args:
            message_type: Message type
            callback: Callback function
        """
        await self._ensure_initialized()
        
        try:
            # Ensure we have a proper MessageType
            if isinstance(message_type, str):
                try:
                    message_type = MessageTypes(message_type)
                except ValueError:
                    logger.error(f"Invalid message type string: {message_type}")
                    return
            
            # Set up RabbitMQ subscription if in RabbitMQ mode
            if self._mode == MessageBusMode.RABBITMQ and RABBITMQ_AVAILABLE:
                try:
                    # Define the handler for received messages
                    async def rabbitmq_handler(data: Dict[str, Any], headers: Dict[str, Any]) -> None:
                        try:
                            # Call the callback
                            if asyncio.iscoroutinefunction(callback):
                                # Call coroutine
                                await callback(message_type, data)
                            else:
                                # Call directly
                                callback(message_type, data)
                        except Exception as e:
                            logger.error(f"Error in RabbitMQ async message handler for {message_type.value}: {str(e)}")
                            
                    # Create a queue for this message type
                    queue_name = f"queue.{message_type.value}"
                    routing_key = message_type.value
                    
                    # Declare queue and binding
                    await async_rabbitmq_broker.declare_queue(queue_name)
                    await async_rabbitmq_broker.bind_queue(queue_name, routing_key)
                    
                    # Set up consumer
                    await async_rabbitmq_broker.consume(queue_name, rabbitmq_handler)
                    
                except Exception as e:
                    logger.error(f"Error setting up RabbitMQ subscription async for {message_type.value}: {str(e)}")
            
            # Always register in-memory handler as fallback
            async with self._lock:
                if message_type not in self._subscribers:
                    self._subscribers[message_type] = []
                    
                # Add callback if not already registered
                if callback not in self._subscribers[message_type]:
                    self._subscribers[message_type].append(callback)
                    
        except Exception as e:
            logger.error(f"Error in async subscribe: {str(e)}")
    
    async def unsubscribe(self, message_type: Union[MessageTypes, str], callback: Callable) -> None:
        """
        Unsubscribe from messages of a specific type asynchronously.
        
        Args:
            message_type: Message type
            callback: Callback function
        """
        try:
            # Ensure we have a proper MessageType
            if isinstance(message_type, str):
                try:
                    message_type = MessageTypes(message_type)
                except ValueError:
                    logger.error(f"Invalid message type string: {message_type}")
                    return
            
            # Remove from in-memory subscribers
            async with self._lock:
                if message_type in self._subscribers and callback in self._subscribers[message_type]:
                    self._subscribers[message_type].remove(callback)
                    if not self._subscribers[message_type]:
                        del self._subscribers[message_type]
            
            # Note: For RabbitMQ, we don't remove the binding or queue, as other handlers might be using it.
            # We just stop calling this particular callback.
            
        except Exception as e:
            logger.error(f"Error in async unsubscribe: {str(e)}")
            
    async def _ensure_initialized(self) -> None:
        """Ensure message bus is initialized."""
        if not self._is_initialized:
            await self.initialize()
            
    def get_mode(self) -> MessageBusMode:
        """
        Get current operating mode.
        
        Returns:
            MessageBusMode: Current mode
        """
        return self._mode


# Global message bus instance
_message_bus = None
_is_initialized = False

def initialize(in_memory=True):
    """
    Initialize the message bus.
    
    Args:
        in_memory: Whether to use in-memory mode (True) or RabbitMQ (False)
        
    Returns:
        bool: Whether initialization was successful
    """
    global _message_bus, _is_initialized
    
    try:
        mode = MessageBusMode.IN_MEMORY if in_memory else MessageBusMode.RABBITMQ
        _message_bus = MessageBus(mode=mode)
        _is_initialized = True
        logger.info(f"Message bus initialized in {mode.value} mode")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize message bus: {e}")
        return False

def is_initialized():
    """
    Check if the message bus is initialized.
    
    Returns:
        bool: Whether the message bus is initialized
    """
    return _is_initialized

def subscribe(message_type, callback):
    """
    Subscribe to a message type.
    
    Args:
        message_type: The message type to subscribe to
        callback: The function to call when a message of this type is received
    """
    global _message_bus
    if not _is_initialized:
        initialize()
    
    _message_bus.subscribe_sync(message_type, callback)

def unsubscribe(message_type, callback):
    """
    Unsubscribe from a message type.
    
    Args:
        message_type: The message type to unsubscribe from
        callback: The function to unsubscribe
    """
    global _message_bus
    if _is_initialized:
        _message_bus.unsubscribe_sync(message_type, callback)

def publish(message_type, data):
    """
    Publish a message.
    
    Args:
        message_type: The message type to publish
        data: The data to publish
    """
    global _message_bus
    if not _is_initialized:
        initialize()
    
    _message_bus.publish_sync(message_type, data)

# Create async message bus - not initialized until needed
_async_message_bus = None

async def get_async_message_bus() -> AsyncMessageBus:
    """
    Get the async message bus singleton.
    
    Returns:
        AsyncMessageBus: Async message bus instance
    """
    global _async_message_bus
    if _async_message_bus is None:
        _async_message_bus = AsyncMessageBus(
            mode=MessageBusMode(os.environ.get("TRADING_SYSTEM_MESSAGE_BUS_MODE", "in_memory"))
        )
        await _async_message_bus.initialize()
    return _async_message_bus 