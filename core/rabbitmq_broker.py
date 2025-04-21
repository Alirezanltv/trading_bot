"""
RabbitMQ Broker Module

This module provides a robust RabbitMQ broker implementation for the trading system,
with connection pooling, automatic reconnection, and message reliability features.
"""

import os
import time
import json
import enum
import pika
import threading
import logging
from typing import Dict, Any, List, Callable, Optional, Union
from functools import wraps

from trading_system.core.logging import get_logger

# Constants
DEFAULT_RABBITMQ_HOST = "localhost"
DEFAULT_RABBITMQ_PORT = 5672
DEFAULT_RABBITMQ_USER = "guest"
DEFAULT_RABBITMQ_PASS = "guest"
DEFAULT_RABBITMQ_VHOST = "/"
EXCHANGE_NAME = "trading_system"
EXCHANGE_TYPE = "topic"
RECONNECT_DELAY_SEC = 5
HEARTBEAT_INTERVAL_SEC = 30
CONNECTION_RETRY_COUNT = 5
RABBITMQ_TIMEOUT_SEC = 30

logger = get_logger("core.rabbitmq_broker")


class DeliveryMode(enum.Enum):
    """Message delivery mode for RabbitMQ."""
    TRANSIENT = 1  # Message is not persisted
    PERSISTENT = 2  # Message is persisted to disk


def with_reconnect(func):
    """Decorator to handle connection failures and reconnects."""
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        retries = 0
        while retries < CONNECTION_RETRY_COUNT:
            try:
                if not self.ensure_connection():
                    retries += 1
                    logger.warning(f"Failed to ensure connection, retry {retries}/{CONNECTION_RETRY_COUNT}")
                    time.sleep(RECONNECT_DELAY_SEC)
                    continue
                return func(self, *args, **kwargs)
            except pika.exceptions.AMQPConnectionError as e:
                retries += 1
                logger.error(f"AMQP connection error in {func.__name__}: {str(e)}, retry {retries}/{CONNECTION_RETRY_COUNT}")
                self._connection = None
                self._channel = None
                time.sleep(RECONNECT_DELAY_SEC)
            except pika.exceptions.AMQPChannelError as e:
                retries += 1
                logger.error(f"AMQP channel error in {func.__name__}: {str(e)}, retry {retries}/{CONNECTION_RETRY_COUNT}")
                self._channel = None
                time.sleep(RECONNECT_DELAY_SEC)
            except Exception as e:
                retries += 1
                logger.error(f"Unexpected error in {func.__name__}: {str(e)}, retry {retries}/{CONNECTION_RETRY_COUNT}", exc_info=True)
                time.sleep(RECONNECT_DELAY_SEC)
        
        logger.error(f"Failed to execute {func.__name__} after {CONNECTION_RETRY_COUNT} retries")
        return None
    return wrapper


class RabbitMQBroker:
    """
    RabbitMQ Broker
    
    This class provides a robust RabbitMQ client for publishing and consuming messages,
    with automatic reconnection, connection pooling, and message reliability features.
    """
    
    def __init__(self):
        """Initialize the RabbitMQ broker."""
        # Configuration from environment
        self._host = os.environ.get("RABBITMQ_HOST", DEFAULT_RABBITMQ_HOST)
        self._port = int(os.environ.get("RABBITMQ_PORT", DEFAULT_RABBITMQ_PORT))
        self._user = os.environ.get("RABBITMQ_USER", DEFAULT_RABBITMQ_USER)
        self._pass = os.environ.get("RABBITMQ_PASS", DEFAULT_RABBITMQ_PASS)
        self._vhost = os.environ.get("RABBITMQ_VHOST", DEFAULT_RABBITMQ_VHOST)
        
        # Connection state
        self._connection = None
        self._channel = None
        self._is_connected = False
        self._lock = threading.RLock()
        
        # Exchange and queue setup tracking
        self._exchange_declared = False
        self._queues_declared = set()
        self._queue_bindings = {}
        
        # Connection parameters
        self._connection_params = pika.ConnectionParameters(
            host=self._host,
            port=self._port,
            virtual_host=self._vhost,
            credentials=pika.PlainCredentials(self._user, self._pass),
            heartbeat=HEARTBEAT_INTERVAL_SEC,
            blocked_connection_timeout=RABBITMQ_TIMEOUT_SEC,
            connection_attempts=CONNECTION_RETRY_COUNT,
            retry_delay=RECONNECT_DELAY_SEC
        )
        
        logger.info(f"RabbitMQ broker initialized: {self._host}:{self._port}/{self._vhost}")
    
    def connect(self) -> bool:
        """
        Connect to RabbitMQ server.
        
        Returns:
            bool: Connection success
        """
        with self._lock:
            if self._is_connected:
                return True
            
            try:
                logger.info(f"Connecting to RabbitMQ: {self._host}:{self._port}/{self._vhost}")
                self._connection = pika.BlockingConnection(self._connection_params)
                self._channel = self._connection.channel()
                
                # Declare exchange
                self._channel.exchange_declare(
                    exchange=EXCHANGE_NAME,
                    exchange_type=EXCHANGE_TYPE,
                    durable=True,
                    auto_delete=False
                )
                self._exchange_declared = True
                
                # Declare dead letter exchange
                self._channel.exchange_declare(
                    exchange=f"{EXCHANGE_NAME}.dlx",
                    exchange_type="direct",
                    durable=True,
                    auto_delete=False
                )
                
                # Set up channel QoS
                self._channel.basic_qos(prefetch_count=1)
                
                self._is_connected = True
                logger.info("Successfully connected to RabbitMQ")
                return True
                
            except pika.exceptions.AMQPConnectionError as e:
                logger.error(f"AMQP connection error: {str(e)}")
                self._connection = None
                self._channel = None
                self._is_connected = False
                return False
                
            except Exception as e:
                logger.error(f"Unexpected error connecting to RabbitMQ: {str(e)}", exc_info=True)
                self._connection = None
                self._channel = None
                self._is_connected = False
                return False
    
    def ensure_connection(self) -> bool:
        """
        Ensure that a connection to RabbitMQ is established.
        
        Returns:
            bool: Connection status
        """
        with self._lock:
            # Check if already connected
            if self._is_connected and self._connection and self._connection.is_open:
                return True
            
            # Try to connect
            return self.connect()
    
    def disconnect(self) -> None:
        """Close connection to RabbitMQ."""
        with self._lock:
            try:
                if self._connection and self._connection.is_open:
                    self._connection.close()
                    logger.info("Disconnected from RabbitMQ")
            except Exception as e:
                logger.error(f"Error disconnecting from RabbitMQ: {str(e)}")
            finally:
                self._connection = None
                self._channel = None
                self._is_connected = False
                self._exchange_declared = False
                self._queues_declared = set()
    
    @with_reconnect
    def declare_queue(self, queue_name: str, durable: bool = True, auto_delete: bool = False,
                      arguments: Optional[Dict[str, Any]] = None) -> bool:
        """
        Declare a queue.
        
        Args:
            queue_name: Queue name
            durable: Queue durability
            auto_delete: Auto-delete flag
            arguments: Additional arguments
            
        Returns:
            bool: Success flag
        """
        with self._lock:
            if queue_name in self._queues_declared:
                return True
            
            # Setup dead letter queue arguments if not specified
            if arguments is None:
                arguments = {
                    "x-dead-letter-exchange": f"{EXCHANGE_NAME}.dlx",
                    "x-dead-letter-routing-key": f"{queue_name}.dlq"
                }
            
            # Declare queue
            self._channel.queue_declare(
                queue=queue_name,
                durable=durable,
                auto_delete=auto_delete,
                arguments=arguments
            )
            
            # Declare dead letter queue for this queue
            self._channel.queue_declare(
                queue=f"{queue_name}.dlq",
                durable=True,
                auto_delete=False
            )
            
            # Bind dead letter queue
            self._channel.queue_bind(
                queue=f"{queue_name}.dlq",
                exchange=f"{EXCHANGE_NAME}.dlx",
                routing_key=f"{queue_name}.dlq"
            )
            
            self._queues_declared.add(queue_name)
            logger.info(f"Declared queue: {queue_name}")
            return True
    
    @with_reconnect
    def bind_queue(self, queue_name: str, routing_key: str) -> bool:
        """
        Bind a queue to the exchange with a routing key.
        
        Args:
            queue_name: Queue name
            routing_key: Routing key
            
        Returns:
            bool: Success flag
        """
        with self._lock:
            # Ensure queue is declared
            if queue_name not in self._queues_declared:
                if not self.declare_queue(queue_name):
                    return False
            
            # Bind queue to exchange
            self._channel.queue_bind(
                queue=queue_name,
                exchange=EXCHANGE_NAME,
                routing_key=routing_key
            )
            
            # Track binding
            if queue_name not in self._queue_bindings:
                self._queue_bindings[queue_name] = set()
            self._queue_bindings[queue_name].add(routing_key)
            
            logger.info(f"Bound queue {queue_name} to routing key {routing_key}")
            return True
    
    @with_reconnect
    def publish(self, routing_key: str, data: Dict[str, Any], 
                delivery_mode: DeliveryMode = DeliveryMode.PERSISTENT,
                expiration: Optional[int] = None,
                headers: Optional[Dict[str, Any]] = None) -> bool:
        """
        Publish a message to RabbitMQ.
        
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
            
            # Prepare properties
            properties = pika.BasicProperties(
                delivery_mode=delivery_mode.value,
                content_type="application/json",
                headers=headers or {},
                expiration=str(expiration) if expiration else None
            )
            
            # Publish message
            self._channel.basic_publish(
                exchange=EXCHANGE_NAME,
                routing_key=routing_key,
                body=message_body,
                properties=properties,
                mandatory=True
            )
            
            logger.debug(f"Published message to {routing_key}: {data.get('type', 'unknown')}")
            return True
            
        except Exception as e:
            logger.error(f"Error publishing message to {routing_key}: {str(e)}", exc_info=True)
            return False
    
    @with_reconnect
    def consume(self, queue_name: str, callback: Callable, auto_ack: bool = False,
                exclusive: bool = False) -> bool:
        """
        Set up a consumer for a queue.
        
        Args:
            queue_name: Queue name
            callback: Message callback function
            auto_ack: Auto-acknowledge flag
            exclusive: Exclusive consumer flag
            
        Returns:
            bool: Success flag
        """
        try:
            # Ensure queue is declared
            if queue_name not in self._queues_declared:
                if not self.declare_queue(queue_name):
                    return False
            
            # Create wrapper for callback that handles errors and parsing
            def callback_wrapper(ch, method, properties, body):
                try:
                    # Parse message body
                    message_data = json.loads(body.decode("utf-8"))
                    
                    # Call user callback
                    result = callback(message_data, properties.headers or {})
                    
                    # Acknowledge message if auto_ack is False and callback succeeded
                    if not auto_ack and result:
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                    elif not auto_ack:
                        # Reject message and requeue if callback failed
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Error decoding message: {str(e)}")
                    # Reject malformed messages without requeuing
                    if not auto_ack:
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                        
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}", exc_info=True)
                    # Reject with requeue on processing error
                    if not auto_ack:
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            
            # Set up consumer
            self._channel.basic_consume(
                queue=queue_name,
                on_message_callback=callback_wrapper,
                auto_ack=auto_ack,
                exclusive=exclusive
            )
            
            logger.info(f"Set up consumer for queue: {queue_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error setting up consumer for {queue_name}: {str(e)}", exc_info=True)
            return False
    
    def start_consuming(self) -> None:
        """Start consuming from all registered queues."""
        try:
            if self.ensure_connection():
                logger.info("Starting consumer thread")
                self._channel.start_consuming()
            else:
                logger.error("Failed to start consuming: not connected")
        except Exception as e:
            logger.error(f"Error in consumer thread: {str(e)}", exc_info=True)
    
    def stop_consuming(self) -> None:
        """Stop consuming messages."""
        try:
            if self._channel:
                self._channel.stop_consuming()
                logger.info("Stopped consuming messages")
        except Exception as e:
            logger.error(f"Error stopping consumer: {str(e)}")
    
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
            return self._connection.is_open
        except Exception:
            return False
    
    def get_connection_info(self) -> Dict[str, Any]:
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
rabbitmq_broker = RabbitMQBroker() 