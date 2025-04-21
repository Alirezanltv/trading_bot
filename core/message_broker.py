"""
Message Broker Interface

This module defines the interfaces for a production-grade message broker system
that can be implemented with technologies like RabbitMQ or Kafka.
"""

import abc
import enum
from typing import Dict, Any, List, Callable, Optional, Union, TypeVar, Generic

# Reuse the existing message types
from trading_system.core.message_bus import MessageTypes

# Type for message handlers
T = TypeVar('T')
MessageHandler = Callable[[MessageTypes, Dict[str, Any]], None]
AsyncMessageHandler = Callable[[MessageTypes, Dict[str, Any]], Any]  # For async handlers


class DeliveryMode(enum.Enum):
    """Delivery mode for messages."""
    TRANSIENT = 1  # Non-persistent
    PERSISTENT = 2  # Persistent


class MessageBroker(abc.ABC):
    """
    Abstract base class for message brokers.
    
    This interface defines the methods that any message broker implementation
    must provide, supporting a publish/subscribe architecture with reliable
    message delivery.
    """
    
    @abc.abstractmethod
    def connect(self) -> bool:
        """
        Connect to the message broker.
        
        Returns:
            bool: True if connected successfully, False otherwise
        """
        pass
    
    @abc.abstractmethod
    def disconnect(self) -> bool:
        """
        Disconnect from the message broker.
        
        Returns:
            bool: True if disconnected successfully, False otherwise
        """
        pass
    
    @abc.abstractmethod
    def is_connected(self) -> bool:
        """
        Check if connected to the message broker.
        
        Returns:
            bool: True if connected, False otherwise
        """
        pass
    
    @abc.abstractmethod
    def publish(self, message_type: MessageTypes, data: Dict[str, Any], 
                delivery_mode: DeliveryMode = DeliveryMode.PERSISTENT) -> bool:
        """
        Publish a message to the broker.
        
        Args:
            message_type: Type of message
            data: Message data
            delivery_mode: Delivery mode (persistent or transient)
            
        Returns:
            bool: True if published successfully, False otherwise
        """
        pass
    
    @abc.abstractmethod
    def subscribe(self, message_type: MessageTypes, handler: MessageHandler) -> bool:
        """
        Subscribe to a message type.
        
        Args:
            message_type: Message type to subscribe to
            handler: Handler function to call when a message is received
            
        Returns:
            bool: True if subscribed successfully, False otherwise
        """
        pass
    
    @abc.abstractmethod
    def unsubscribe(self, message_type: MessageTypes, handler: MessageHandler) -> bool:
        """
        Unsubscribe from a message type.
        
        Args:
            message_type: Message type to unsubscribe from
            handler: Handler function to remove
            
        Returns:
            bool: True if unsubscribed successfully, False otherwise
        """
        pass
    
    @abc.abstractmethod
    def create_queue(self, queue_name: str, durable: bool = True, 
                     auto_delete: bool = False) -> bool:
        """
        Create a named queue.
        
        Args:
            queue_name: Name of the queue
            durable: Whether the queue survives broker restarts
            auto_delete: Whether the queue is deleted when no consumers are connected
            
        Returns:
            bool: True if created successfully, False otherwise
        """
        pass
    
    @abc.abstractmethod
    def create_exchange(self, exchange_name: str, exchange_type: str = 'topic',
                        durable: bool = True) -> bool:
        """
        Create a named exchange.
        
        Args:
            exchange_name: Name of the exchange
            exchange_type: Type of exchange (topic, direct, fanout, etc.)
            durable: Whether the exchange survives broker restarts
            
        Returns:
            bool: True if created successfully, False otherwise
        """
        pass
    
    @abc.abstractmethod
    def bind_queue(self, queue_name: str, exchange_name: str, 
                  routing_key: str) -> bool:
        """
        Bind a queue to an exchange with a routing key.
        
        Args:
            queue_name: Name of the queue
            exchange_name: Name of the exchange
            routing_key: Routing key for the binding
            
        Returns:
            bool: True if bound successfully, False otherwise
        """
        pass
    
    @abc.abstractmethod
    def get_dlq_name(self, queue_name: str) -> str:
        """
        Get the name of the dead-letter queue for a given queue.
        
        Args:
            queue_name: Name of the original queue
            
        Returns:
            str: Name of the dead-letter queue
        """
        pass


class AsyncMessageBroker(MessageBroker):
    """
    Abstract base class for asynchronous message brokers.
    
    This extends the base MessageBroker with asynchronous methods.
    """
    
    @abc.abstractmethod
    async def connect_async(self) -> bool:
        """
        Connect to the message broker asynchronously.
        
        Returns:
            bool: True if connected successfully, False otherwise
        """
        pass
    
    @abc.abstractmethod
    async def disconnect_async(self) -> bool:
        """
        Disconnect from the message broker asynchronously.
        
        Returns:
            bool: True if disconnected successfully, False otherwise
        """
        pass
    
    @abc.abstractmethod
    async def publish_async(self, message_type: MessageTypes, data: Dict[str, Any],
                           delivery_mode: DeliveryMode = DeliveryMode.PERSISTENT) -> bool:
        """
        Publish a message to the broker asynchronously.
        
        Args:
            message_type: Type of message
            data: Message data
            delivery_mode: Delivery mode (persistent or transient)
            
        Returns:
            bool: True if published successfully, False otherwise
        """
        pass
    
    @abc.abstractmethod
    async def subscribe_async(self, message_type: MessageTypes, 
                             handler: AsyncMessageHandler) -> bool:
        """
        Subscribe to a message type asynchronously.
        
        Args:
            message_type: Message type to subscribe to
            handler: Async handler function to call when a message is received
            
        Returns:
            bool: True if subscribed successfully, False otherwise
        """
        pass
    
    @abc.abstractmethod
    async def create_queue_async(self, queue_name: str, durable: bool = True,
                                auto_delete: bool = False) -> bool:
        """
        Create a named queue asynchronously.
        
        Args:
            queue_name: Name of the queue
            durable: Whether the queue survives broker restarts
            auto_delete: Whether the queue is deleted when no consumers are connected
            
        Returns:
            bool: True if created successfully, False otherwise
        """
        pass 