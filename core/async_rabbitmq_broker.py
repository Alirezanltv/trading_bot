"""
Async RabbitMQ Broker Module

This module provides an asynchronous RabbitMQ broker implementation for the trading system,
with connection pooling, automatic reconnection, and message reliability features.
"""

import os
import time
import json
import enum
import asyncio
import aio_pika
from aio_pika.abc import AbstractRobustConnection
from typing import Dict, Any, List, Callable, Optional, Union, Set
from functools import wraps

from trading_system.core.logging import get_logger
from trading_system.core.rabbitmq_broker import DeliveryMode, EXCHANGE_NAME, EXCHANGE_TYPE

# Constants
DEFAULT_RABBITMQ_HOST = "localhost"
DEFAULT_RABBITMQ_PORT = 5672
DEFAULT_RABBITMQ_USER = "guest"
DEFAULT_RABBITMQ_PASS = "guest"
DEFAULT_RABBITMQ_VHOST = "/"
RECONNECT_DELAY_SEC = 5
HEARTBEAT_INTERVAL_SEC = 30
CONNECTION_RETRY_COUNT = 5
RABBITMQ_TIMEOUT_SEC = 30

logger = get_logger("core.async_rabbitmq_broker")


def with_async_reconnect(func):
    """Decorator to handle connection failures and reconnects in async functions."""
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        retries = 0
        while retries < CONNECTION_RETRY_COUNT:
            try:
                if not await self.ensure_connection():
                    retries += 1
                    logger.warning(f"Failed to ensure connection, retry {retries}/{CONNECTION_RETRY_COUNT}")
                    await asyncio.sleep(RECONNECT_DELAY_SEC)
                    continue
                return await func(self, *args, **kwargs)
            except (aio_pika.exceptions.AMQPConnectionError, 
                    aio_pika.exceptions.ChannelInvalidStateError) as e:
                retries += 1
                logger.error(f"AMQP error in {func.__name__}: {str(e)}, retry {retries}/{CONNECTION_RETRY_COUNT}")
                # Reset connection and channel
                await self._close_connection()
                await asyncio.sleep(RECONNECT_DELAY_SEC)
            except Exception as e:
                retries += 1
                logger.error(f"Unexpected error in {func.__name__}: {str(e)}, retry {retries}/{CONNECTION_RETRY_COUNT}", 
                            exc_info=True)
                await asyncio.sleep(RECONNECT_DELAY_SEC)
        
        logger.error(f"Failed to execute {func.__name__} after {CONNECTION_RETRY_COUNT} retries")
        return None
    return wrapper


class AsyncRabbitMQBroker:
    """
    Async RabbitMQ Broker
    
    This class provides an asynchronous RabbitMQ client for publishing and consuming messages,
    with automatic reconnection, connection pooling, and message reliability features.
    """
    
    def __init__(self):
        """Initialize the async RabbitMQ broker."""
        # Configuration from environment
        self._host = os.environ.get("RABBITMQ_HOST", DEFAULT_RABBITMQ_HOST)
        self._port = int(os.environ.get("RABBITMQ_PORT", DEFAULT_RABBITMQ_PORT))
        self._user = os.environ.get("RABBITMQ_USER", DEFAULT_RABBITMQ_USER)
        self._pass = os.environ.get("RABBITMQ_PASS", DEFAULT_RABBITMQ_PASS)
        self._vhost = os.environ.get("RABBITMQ_VHOST", DEFAULT_RABBITMQ_VHOST)
        
        # Connection state
        self._connection: Optional[AbstractRobustConnection] = None
        self._channel: Optional[aio_pika.abc.AbstractChannel] = None
        self._is_connected = False
        self._connection_lock = asyncio.Lock()
        
        # Exchange and queue setup tracking
        self._exchange: Optional[aio_pika.abc.AbstractExchange] = None
        self._dlx_exchange: Optional[aio_pika.abc.AbstractExchange] = None
        self._queues_declared: Set[str] = set()
        self._queue_bindings: Dict[str, Set[str]] = {}
        
        # Connection string
        self._connection_string = (
            f"amqp://{self._user}:{self._pass}@{self._host}:{self._port}/{self._vhost}"
        )
        
        logger.info(f"Async RabbitMQ broker initialized: {self._host}:{self._port}/{self._vhost}")
    
    async def connect(self) -> bool:
        """
        Connect to RabbitMQ server asynchronously.
        
        Returns:
            bool: Connection success
        """
        async with self._connection_lock:
            if self._is_connected and self._connection and not self._connection.is_closed:
                return True
            
            try:
                logger.info(f"Connecting to RabbitMQ: {self._host}:{self._port}/{self._vhost}")
                
                # Configure connection parameters
                connection_params = {
                    "url": self._connection_string,
                    "heartbeat": HEARTBEAT_INTERVAL_SEC,
                    "timeout": RABBITMQ_TIMEOUT_SEC,
                }
                
                # Connect to RabbitMQ
                self._connection = await aio_pika.connect_robust(**connection_params)
                self._channel = await self._connection.channel()
                
                # Set channel QoS
                await self._channel.set_qos(prefetch_count=1)
                
                # Declare exchanges
                self._exchange = await self._channel.declare_exchange(
                    name=EXCHANGE_NAME,
                    type=EXCHANGE_TYPE,
                    durable=True,
                    auto_delete=False
                )
                
                self._dlx_exchange = await self._channel.declare_exchange(
                    name=f"{EXCHANGE_NAME}.dlx",
                    type="direct",
                    durable=True,
                    auto_delete=False
                )
                
                self._is_connected = True
                logger.info("Successfully connected to RabbitMQ")
                return True
                
            except aio_pika.exceptions.AMQPConnectionError as e:
                logger.error(f"AMQP connection error: {str(e)}")
                await self._close_connection()
                return False
                
            except Exception as e:
                logger.error(f"Unexpected error connecting to RabbitMQ: {str(e)}", exc_info=True)
                await self._close_connection()
                return False
    
    async def ensure_connection(self) -> bool:
        """
        Ensure that a connection to RabbitMQ is established.
        
        Returns:
            bool: Connection status
        """
        async with self._connection_lock:
            # Check if already connected
            if self._is_connected and self._connection and not self._connection.is_closed:
                return True
            
            # Try to connect
            return await self.connect()
    
    async def _close_connection(self) -> None:
        """Close connection to RabbitMQ."""
        try:
            if self._connection and not self._connection.is_closed:
                await self._connection.close()
                logger.info("Closed RabbitMQ connection")
        except Exception as e:
            logger.error(f"Error closing RabbitMQ connection: {str(e)}")
        finally:
            self._connection = None
            self._channel = None
            self._exchange = None
            self._dlx_exchange = None
            self._is_connected = False
    
    async def disconnect(self) -> None:
        """Close connection to RabbitMQ."""
        async with self._connection_lock:
            await self._close_connection()
            self._queues_declared.clear()
            self._queue_bindings.clear()
    
    @with_async_reconnect
    async def declare_queue(self, queue_name: str, durable: bool = True, auto_delete: bool = False,
                            arguments: Optional[Dict[str, Any]] = None) -> Optional[aio_pika.Queue]:
        """
        Declare a queue asynchronously.
        
        Args:
            queue_name: Queue name
            durable: Queue durability
            auto_delete: Auto-delete flag
            arguments: Additional arguments
            
        Returns:
            aio_pika.Queue: Declared queue object or None if failed
        """
        if queue_name in self._queues_declared:
            return await self._channel.get_queue(queue_name)
        
        # Setup dead letter queue arguments if not specified
        if arguments is None:
            arguments = {
                "x-dead-letter-exchange": f"{EXCHANGE_NAME}.dlx",
                "x-dead-letter-routing-key": f"{queue_name}.dlq"
            }
        
        # Declare queue
        queue = await self._channel.declare_queue(
            name=queue_name,
            durable=durable,
            auto_delete=auto_delete,
            arguments=arguments
        )
        
        # Declare dead letter queue for this queue
        dlq = await self._channel.declare_queue(
            name=f"{queue_name}.dlq",
            durable=True,
            auto_delete=False
        )
        
        # Bind dead letter queue
        await dlq.bind(
            exchange=self._dlx_exchange,
            routing_key=f"{queue_name}.dlq"
        )
        
        self._queues_declared.add(queue_name)
        logger.info(f"Declared queue: {queue_name}")
        return queue
    
    @with_async_reconnect
    async def bind_queue(self, queue_name: str, routing_key: str) -> bool:
        """
        Bind a queue to the exchange with a routing key asynchronously.
        
        Args:
            queue_name: Queue name
            routing_key: Routing key
            
        Returns:
            bool: Success flag
        """
        # Ensure queue is declared
        if queue_name not in self._queues_declared:
            queue = await self.declare_queue(queue_name)
            if not queue:
                return False
        else:
            queue = await self._channel.get_queue(queue_name)
        
        # Bind queue to exchange
        await queue.bind(
            exchange=self._exchange,
            routing_key=routing_key
        )
        
        # Track binding
        if queue_name not in self._queue_bindings:
            self._queue_bindings[queue_name] = set()
        self._queue_bindings[queue_name].add(routing_key)
        
        logger.info(f"Bound queue {queue_name} to routing key {routing_key}")
        return True
    
    @with_async_reconnect
    async def publish(self, routing_key: str, data: Dict[str, Any], 
                      delivery_mode: DeliveryMode = DeliveryMode.PERSISTENT,
                      expiration: Optional[int] = None,
                      headers: Optional[Dict[str, Any]] = None) -> bool:
        """
        Publish a message to RabbitMQ asynchronously.
        
        Args:
            routing_key: Routing key
            data: Message data
            delivery_mode: Delivery mode
            expiration: Message expiration in milliseconds
            headers: Message headers
            
        Returns:
            bool: Publish success
        """
        try:
            # Convert data to JSON
            message_body = json.dumps(data).encode("utf-8")
            
            # Prepare message properties
            properties = {
                "delivery_mode": delivery_mode.value,
                "content_type": "application/json",
                "headers": headers or {},
            }
            
            if expiration:
                properties["expiration"] = expiration
            
            # Create message
            message = aio_pika.Message(
                body=message_body,
                **properties
            )
            
            # Publish message
            await self._exchange.publish(
                message=message,
                routing_key=routing_key,
                mandatory=True
            )
            
            logger.debug(f"Published message to {routing_key}: {data.get('type', 'unknown')}")
            return True
            
        except Exception as e:
            logger.error(f"Error publishing message to {routing_key}: {str(e)}", exc_info=True)
            return False
    
    @with_async_reconnect
    async def consume(self, queue_name: str, callback: Callable, 
                      no_ack: bool = False) -> bool:
        """
        Set up a consumer for a queue asynchronously.
        
        Args:
            queue_name: Queue name
            callback: Async message callback function
            no_ack: No acknowledgment flag (True = auto-ack)
            
        Returns:
            bool: Success flag
        """
        try:
            # Ensure queue is declared
            if queue_name not in self._queues_declared:
                queue = await self.declare_queue(queue_name)
                if not queue:
                    return False
            else:
                queue = await self._channel.get_queue(queue_name)
            
            # Create wrapper for callback that handles errors and parsing
            async def callback_wrapper(message: aio_pika.abc.AbstractIncomingMessage):
                async with message.process(requeue=True):
                    try:
                        # Parse message body
                        message_data = json.loads(message.body.decode("utf-8"))
                        
                        # Call user callback
                        await callback(message_data, message.headers or {})
                        
                    except json.JSONDecodeError as e:
                        logger.error(f"Error decoding message: {str(e)}")
                        # Don't requeue malformed messages
                        message.reject(requeue=False)
                        
                    except Exception as e:
                        logger.error(f"Error processing message: {str(e)}", exc_info=True)
                        # Reject and requeue on processing error
                        message.reject(requeue=True)
                        raise
            
            # Start consuming
            await queue.consume(callback_wrapper, no_ack=no_ack)
            
            logger.info(f"Set up consumer for queue: {queue_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error setting up consumer for {queue_name}: {str(e)}", exc_info=True)
            return False
    
    @property
    def is_connected(self) -> bool:
        """
        Check if connected to RabbitMQ.
        
        Returns:
            bool: Connection status
        """
        if not self._is_connected or not self._connection:
            return False
        
        try:
            return not self._connection.is_closed
        except Exception:
            return False
    
    async def get_connection_info(self) -> Dict[str, Any]:
        """
        Get connection information.
        
        Returns:
            dict: Connection info
        """
        return {
            "host": self._host,
            "port": self._port,
            "vhost": self._vhost,
            "is_connected": self.is_connected,
            "queues_declared": list(self._queues_declared),
            "queue_bindings": {q: list(r) for q, r in self._queue_bindings.items()}
        }


# Create a singleton instance
async_rabbitmq_broker = AsyncRabbitMQBroker() 