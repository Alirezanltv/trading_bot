"""
Message Bus

This module implements a high-reliability message bus using RabbitMQ for
inter-component communication with features like guaranteed delivery,
dead-letter queues, and circuit breakers.
"""

import json
import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Any, Callable, Union, Set

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPConnectionError, AMQPChannelError

from trading_system.core.circuit_breaker import CircuitBreaker
from trading_system.monitoring.alert_system import get_alert_system, AlertSeverity
from trading_system.utils.config import Config
from trading_system.utils.retry import retry_with_backoff

# Configure logging
logger = logging.getLogger(__name__)

class MessagePriority(Enum):
    """Priority levels for messages."""
    LOW = 1
    NORMAL = 5
    HIGH = 8
    CRITICAL = 10

class MessageStatus(Enum):
    """Status of a message."""
    PENDING = "pending"
    SENT = "sent"
    DELIVERED = "delivered"
    FAILED = "failed"
    RETRYING = "retrying"
    DEAD_LETTERED = "dead_lettered"

@dataclass
class MessageHeader:
    """Message header with metadata."""
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: float = field(default_factory=time.time)
    priority: MessagePriority = MessagePriority.NORMAL
    sender: str = ""
    reply_to: Optional[str] = None
    correlation_id: Optional[str] = None
    expiration: Optional[int] = None  # milliseconds
    content_type: str = "application/json"
    content_encoding: str = "utf-8"
    headers: Dict[str, str] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert header to dictionary."""
        return {
            "message_id": self.message_id,
            "timestamp": self.timestamp,
            "priority": self.priority.value,
            "sender": self.sender,
            "reply_to": self.reply_to,
            "correlation_id": self.correlation_id,
            "expiration": self.expiration,
            "content_type": self.content_type,
            "content_encoding": self.content_encoding,
            "headers": self.headers
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MessageHeader':
        """Create header from dictionary."""
        priority = data.get("priority", 5)
        if isinstance(priority, int):
            priority = MessagePriority(priority)
        else:
            priority = MessagePriority.NORMAL
        
        return cls(
            message_id=data.get("message_id", str(uuid.uuid4())),
            timestamp=data.get("timestamp", time.time()),
            priority=priority,
            sender=data.get("sender", ""),
            reply_to=data.get("reply_to"),
            correlation_id=data.get("correlation_id"),
            expiration=data.get("expiration"),
            content_type=data.get("content_type", "application/json"),
            content_encoding=data.get("content_encoding", "utf-8"),
            headers=data.get("headers", {})
        )

@dataclass
class Message:
    """Message for inter-component communication."""
    topic: str
    body: Dict[str, Any]
    header: MessageHeader = field(default_factory=MessageHeader)
    status: MessageStatus = MessageStatus.PENDING
    
    def to_json(self) -> str:
        """Serialize message to JSON."""
        return json.dumps({
            "topic": self.topic,
            "body": self.body,
            "header": self.header.to_dict()
        })
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Message':
        """Create message from JSON."""
        data = json.loads(json_str)
        
        return cls(
            topic=data["topic"],
            body=data["body"],
            header=MessageHeader.from_dict(data["header"]),
            status=MessageStatus.DELIVERED
        )

class MessageBusConfig:
    """Configuration for message bus."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize with configuration.
        
        Args:
            config: Message bus configuration
        """
        self.config = config or Config().get("message_bus", {})
        
        # RabbitMQ connection settings
        self.host = self.config.get("host", "localhost")
        self.port = self.config.get("port", 5672)
        self.virtual_host = self.config.get("virtual_host", "/")
        self.username = self.config.get("username", "guest")
        self.password = self.config.get("password", "guest")
        self.connection_attempts = self.config.get("connection_attempts", 3)
        self.heartbeat = self.config.get("heartbeat", 60)
        self.blocked_connection_timeout = self.config.get("blocked_connection_timeout", 300)
        self.connection_timeout = self.config.get("connection_timeout", 30)
        
        # Exchange settings
        self.exchange_name = self.config.get("exchange_name", "trading_system")
        self.exchange_type = self.config.get("exchange_type", "topic")
        self.exchange_durable = self.config.get("exchange_durable", True)
        
        # Queue settings
        self.queue_durable = self.config.get("queue_durable", True)
        self.queue_exclusive = self.config.get("queue_exclusive", False)
        self.queue_auto_delete = self.config.get("queue_auto_delete", False)
        self.queue_arguments = self.config.get("queue_arguments", {"x-message-ttl": 3600000})  # 1 hour
        
        # Consumer settings
        self.consumer_prefetch_count = self.config.get("consumer_prefetch_count", 10)
        self.consumer_auto_ack = self.config.get("consumer_auto_ack", False)
        
        # Publisher settings
        self.publisher_confirms = self.config.get("publisher_confirms", True)
        self.mandatory_publishing = self.config.get("mandatory_publishing", True)
        self.delivery_mode = self.config.get("delivery_mode", 2)  # 2 = persistent
        
        # Retry settings
        self.max_retries = self.config.get("max_retries", 3)
        self.initial_retry_delay = self.config.get("initial_retry_delay", 1.0)
        self.retry_backoff_factor = self.config.get("retry_backoff_factor", 2.0)
        
        # Circuit breaker settings
        self.circuit_breaker_threshold = self.config.get("circuit_breaker_threshold", 5)
        self.circuit_breaker_timeout = self.config.get("circuit_breaker_timeout", 60)
        self.circuit_breaker_recovery_threshold = self.config.get("circuit_breaker_recovery_threshold", 3)
        
        # Dead letter settings
        self.enable_dead_letter = self.config.get("enable_dead_letter", True)
        self.dead_letter_exchange = self.config.get("dead_letter_exchange", "trading_system.dlx")
        self.dead_letter_routing_key = self.config.get("dead_letter_routing_key", "dead.letter")

class MessageCallback:
    """Callback for message handling."""
    
    def __init__(self, callback: Callable[[Message], bool], queue_name: str, topics: List[str]):
        """
        Initialize callback.
        
        Args:
            callback: Callback function
            queue_name: Queue name
            topics: List of topics to subscribe to
        """
        self.callback = callback
        self.queue_name = queue_name
        self.topics = topics

class MessageBus:
    """
    High-reliability message bus for inter-component communication.
    
    Features:
    - Guaranteed message delivery
    - Dead-letter queue for failed messages
    - Message acknowledgment
    - Circuit breaker for failure handling
    - Auto-reconnection
    - Publisher confirms
    - Message prioritization
    """
    
    def __init__(self, 
                component_name: str,
                config: Optional[Dict[str, Any]] = None):
        """
        Initialize the message bus.
        
        Args:
            component_name: Name of the component using this bus
            config: Message bus configuration
        """
        self.component_name = component_name
        self.config = MessageBusConfig(config)
        
        # Connection state
        self.connection = None
        self.channel = None
        self.is_connected = False
        
        # Message handling
        self.callbacks: Dict[str, MessageCallback] = {}
        self.received_messages: Dict[str, Message] = {}
        self.sent_messages: Dict[str, Message] = {}
        
        # Circuit breaker
        self.publish_circuit_breaker = CircuitBreaker(
            name=f"message_bus_publish_{component_name}",
            failure_threshold=self.config.circuit_breaker_threshold,
            recovery_timeout=self.config.circuit_breaker_timeout,
            recovery_threshold=self.config.circuit_breaker_recovery_threshold
        )
        
        self.consume_circuit_breaker = CircuitBreaker(
            name=f"message_bus_consume_{component_name}",
            failure_threshold=self.config.circuit_breaker_threshold,
            recovery_timeout=self.config.circuit_breaker_timeout,
            recovery_threshold=self.config.circuit_breaker_recovery_threshold
        )
        
        # Consumer thread
        self._consumer_thread = None
        self._stop_event = threading.Event()
        
        # Alert system
        self.alert_system = get_alert_system()
        
        logger.info(f"Message bus initialized for component: {component_name}")
    
    def connect(self) -> bool:
        """
        Connect to the message broker.
        
        Returns:
            Success flag
        """
        if self.is_connected:
            return True
        
        try:
            # Create connection parameters
            credentials = pika.PlainCredentials(
                username=self.config.username,
                password=self.config.password
            )
            
            parameters = pika.ConnectionParameters(
                host=self.config.host,
                port=self.config.port,
                virtual_host=self.config.virtual_host,
                credentials=credentials,
                connection_attempts=self.config.connection_attempts,
                heartbeat=self.config.heartbeat,
                blocked_connection_timeout=self.config.blocked_connection_timeout,
                socket_timeout=self.config.connection_timeout
            )
            
            # Connect to RabbitMQ
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            
            # Set up publisher confirms if enabled
            if self.config.publisher_confirms:
                self.channel.confirm_delivery()
            
            # Declare the main exchange
            self.channel.exchange_declare(
                exchange=self.config.exchange_name,
                exchange_type=self.config.exchange_type,
                durable=self.config.exchange_durable
            )
            
            # Declare dead letter exchange if enabled
            if self.config.enable_dead_letter:
                self.channel.exchange_declare(
                    exchange=self.config.dead_letter_exchange,
                    exchange_type="topic",
                    durable=True
                )
                
                # Create dead letter queue
                self.channel.queue_declare(
                    queue=f"{self.component_name}.dead_letter",
                    durable=True,
                    exclusive=False,
                    auto_delete=False
                )
                
                # Bind dead letter queue
                self.channel.queue_bind(
                    exchange=self.config.dead_letter_exchange,
                    queue=f"{self.component_name}.dead_letter",
                    routing_key=self.config.dead_letter_routing_key
                )
            
            self.is_connected = True
            logger.info(f"Connected to RabbitMQ at {self.config.host}:{self.config.port}")
            
            # Reset circuit breakers after successful connection
            self.publish_circuit_breaker.reset()
            self.consume_circuit_breaker.reset()
            
            return True
            
        except Exception as e:
            logger.error(f"Error connecting to RabbitMQ: {str(e)}")
            self.is_connected = False
            return False
    
    def disconnect(self) -> bool:
        """
        Disconnect from the message broker.
        
        Returns:
            Success flag
        """
        if not self.is_connected:
            return True
        
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
            
            self.connection = None
            self.channel = None
            self.is_connected = False
            
            logger.info("Disconnected from RabbitMQ")
            return True
            
        except Exception as e:
            logger.error(f"Error disconnecting from RabbitMQ: {str(e)}")
            self.is_connected = False
            return False
    
    def reconnect(self) -> bool:
        """
        Reconnect to the message broker.
        
        Returns:
            Success flag
        """
        logger.info("Attempting to reconnect to RabbitMQ")
        
        # Disconnect if connected
        if self.is_connected:
            self.disconnect()
        
        # Connect again
        return self.connect()
    
    def subscribe(self, 
                queue_name: str, 
                topics: List[str], 
                callback: Callable[[Message], bool]) -> bool:
        """
        Subscribe to topics.
        
        Args:
            queue_name: Queue name
            topics: List of topics to subscribe to
            callback: Callback function
            
        Returns:
            Success flag
        """
        try:
            # Ensure connected
            if not self.is_connected and not self.connect():
                return False
            
            # Store callback
            self.callbacks[queue_name] = MessageCallback(callback, queue_name, topics)
            
            # Set up queue
            queue_arguments = dict(self.config.queue_arguments)
            
            # Add dead letter exchange if enabled
            if self.config.enable_dead_letter:
                queue_arguments["x-dead-letter-exchange"] = self.config.dead_letter_exchange
                queue_arguments["x-dead-letter-routing-key"] = self.config.dead_letter_routing_key
            
            # Declare queue
            self.channel.queue_declare(
                queue=queue_name,
                durable=self.config.queue_durable,
                exclusive=self.config.queue_exclusive,
                auto_delete=self.config.queue_auto_delete,
                arguments=queue_arguments
            )
            
            # Bind queue to topics
            for topic in topics:
                self.channel.queue_bind(
                    exchange=self.config.exchange_name,
                    queue=queue_name,
                    routing_key=topic
                )
            
            # Set QoS (prefetch count)
            self.channel.basic_qos(prefetch_count=self.config.consumer_prefetch_count)
            
            logger.info(f"Subscribed to topics {topics} on queue {queue_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error subscribing to topics: {str(e)}")
            return False
    
    def start_consuming(self) -> bool:
        """
        Start consuming messages.
        
        Returns:
            Success flag
        """
        if not self.callbacks:
            logger.warning("No callbacks registered, cannot start consuming")
            return False
        
        try:
            # Ensure connected
            if not self.is_connected and not self.connect():
                return False
            
            # Start consumer thread
            self._stop_event.clear()
            self._consumer_thread = threading.Thread(
                target=self._consume_loop,
                daemon=True,
                name=f"MessageBus_{self.component_name}_Consumer"
            )
            self._consumer_thread.start()
            
            logger.info("Started consuming messages")
            return True
            
        except Exception as e:
            logger.error(f"Error starting to consume messages: {str(e)}")
            return False
    
    def stop_consuming(self) -> bool:
        """
        Stop consuming messages.
        
        Returns:
            Success flag
        """
        if not self._consumer_thread:
            return True
        
        try:
            # Signal thread to stop
            self._stop_event.set()
            
            # Wait for thread to terminate
            self._consumer_thread.join(timeout=5.0)
            self._consumer_thread = None
            
            logger.info("Stopped consuming messages")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping message consumption: {str(e)}")
            return False
    
    @retry_with_backoff(max_retries=3, initial_wait=0.5, backoff_factor=2)
    def publish(self, 
              topic: str, 
              body: Dict[str, Any],
              priority: MessagePriority = MessagePriority.NORMAL,
              reply_to: Optional[str] = None,
              correlation_id: Optional[str] = None,
              expiration: Optional[int] = None,
              headers: Optional[Dict[str, str]] = None) -> Optional[str]:
        """
        Publish a message.
        
        Args:
            topic: Topic to publish to
            body: Message body
            priority: Message priority
            reply_to: Optional reply-to queue
            correlation_id: Optional correlation ID
            expiration: Optional expiration time (ms)
            headers: Optional message headers
            
        Returns:
            Message ID if published successfully, None otherwise
        """
        try:
            # Use circuit breaker
            if not self.publish_circuit_breaker.allow_request():
                logger.warning("Circuit breaker open, publish request rejected")
                return None
            
            with self.publish_circuit_breaker:
                # Ensure connected
                if not self.is_connected and not self.connect():
                    return None
                
                # Create message
                header = MessageHeader(
                    priority=priority,
                    sender=self.component_name,
                    reply_to=reply_to,
                    correlation_id=correlation_id,
                    expiration=expiration,
                    headers=headers or {}
                )
                
                message = Message(
                    topic=topic,
                    body=body,
                    header=header
                )
                
                # Store message
                message_id = message.header.message_id
                self.sent_messages[message_id] = message
                
                # Serialize message
                message_json = message.to_json()
                
                # Create properties
                properties = pika.BasicProperties(
                    content_type="application/json",
                    content_encoding="utf-8",
                    delivery_mode=self.config.delivery_mode,
                    priority=priority.value,
                    message_id=message_id,
                    timestamp=int(time.time()),
                    app_id=self.component_name,
                    headers=headers or {},
                    reply_to=reply_to,
                    correlation_id=correlation_id,
                    expiration=str(expiration) if expiration else None
                )
                
                # Publish message
                self.channel.basic_publish(
                    exchange=self.config.exchange_name,
                    routing_key=topic,
                    body=message_json.encode("utf-8"),
                    properties=properties,
                    mandatory=self.config.mandatory_publishing
                )
                
                # Update message status
                message.status = MessageStatus.SENT
                
                logger.debug(f"Published message to topic {topic}: {message_id}")
                return message_id
                
        except AMQPConnectionError as e:
            logger.error(f"Connection error publishing message: {str(e)}")
            self.is_connected = False
            self.publish_circuit_breaker.record_failure()
            
            # Raise for retry
            raise
            
        except Exception as e:
            logger.error(f"Error publishing message: {str(e)}")
            self.publish_circuit_breaker.record_failure()
            return None
    
    def reply(self, 
            original_message: Message, 
            body: Dict[str, Any],
            priority: MessagePriority = MessagePriority.NORMAL,
            headers: Optional[Dict[str, str]] = None) -> Optional[str]:
        """
        Reply to a message.
        
        Args:
            original_message: Original message to reply to
            body: Reply body
            priority: Message priority
            headers: Optional message headers
            
        Returns:
            Message ID if published successfully, None otherwise
        """
        if not original_message.header.reply_to:
            logger.warning("Cannot reply to message without reply_to queue")
            return None
        
        return self.publish(
            topic=original_message.header.reply_to,
            body=body,
            priority=priority,
            correlation_id=original_message.header.message_id,
            headers=headers
        )
    
    def purge_queue(self, queue_name: str) -> bool:
        """
        Purge a queue.
        
        Args:
            queue_name: Queue name
            
        Returns:
            Success flag
        """
        try:
            # Ensure connected
            if not self.is_connected and not self.connect():
                return False
            
            # Purge queue
            self.channel.queue_purge(queue=queue_name)
            
            logger.info(f"Purged queue: {queue_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error purging queue {queue_name}: {str(e)}")
            return False
    
    def delete_queue(self, queue_name: str) -> bool:
        """
        Delete a queue.
        
        Args:
            queue_name: Queue name
            
        Returns:
            Success flag
        """
        try:
            # Ensure connected
            if not self.is_connected and not self.connect():
                return False
            
            # Delete queue
            self.channel.queue_delete(queue=queue_name)
            
            # Remove callback if registered
            if queue_name in self.callbacks:
                del self.callbacks[queue_name]
            
            logger.info(f"Deleted queue: {queue_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting queue {queue_name}: {str(e)}")
            return False
    
    def _consume_loop(self) -> None:
        """Message consumption loop."""
        logger.info("Consumer thread started")
        
        while not self._stop_event.is_set():
            try:
                # Ensure connected
                if not self.is_connected and not self.connect():
                    time.sleep(1.0)
                    continue
                
                # Set up consumers for all registered callbacks
                for queue_name, callback_info in self.callbacks.items():
                    # Set up consumer
                    self.channel.basic_consume(
                        queue=queue_name,
                        on_message_callback=lambda ch, method, properties, body: 
                            self._handle_message(ch, method, properties, body, callback_info),
                        auto_ack=self.config.consumer_auto_ack
                    )
                
                # Start consuming (blocking call)
                logger.info("Starting to consume messages")
                try:
                    with self.consume_circuit_breaker:
                        self.channel.start_consuming()
                except KeyboardInterrupt:
                    self.channel.stop_consuming()
                    break
                
            except AMQPConnectionError as e:
                logger.error(f"Connection error in consumer: {str(e)}")
                self.is_connected = False
                self.consume_circuit_breaker.record_failure()
                
                # Sleep before reconnecting
                time.sleep(1.0)
                
            except AMQPChannelError as e:
                logger.error(f"Channel error in consumer: {str(e)}")
                
                # Try to reconnect
                self.reconnect()
                
            except Exception as e:
                logger.error(f"Error in consumer thread: {str(e)}")
                self.consume_circuit_breaker.record_failure()
                
                # Sleep before continuing
                time.sleep(1.0)
        
        # Stop consuming if loop is exited
        if self.channel and self.channel.is_open:
            try:
                self.channel.stop_consuming()
            except Exception:
                pass
        
        logger.info("Consumer thread stopped")
    
    def _handle_message(self, 
                      channel: BlockingChannel, 
                      method, 
                      properties, 
                      body: bytes,
                      callback_info: MessageCallback) -> None:
        """
        Handle received message.
        
        Args:
            channel: Pika channel
            method: Delivery method
            properties: Message properties
            body: Message body
            callback_info: Callback information
        """
        try:
            # Decode message
            message_json = body.decode("utf-8")
            message = Message.from_json(message_json)
            
            # Store received message
            message_id = message.header.message_id
            self.received_messages[message_id] = message
            
            logger.debug(f"Received message {message_id} on topic {message.topic}")
            
            # Call callback
            if callback_info.callback:
                success = callback_info.callback(message)
                
                # Acknowledge or reject message
                if success:
                    channel.basic_ack(delivery_tag=method.delivery_tag)
                    logger.debug(f"Acknowledged message {message_id}")
                else:
                    # Negative acknowledge, requeue if not too many retries
                    retry_count = int(properties.headers.get("x-retry-count", 0)) if properties.headers else 0
                    
                    if retry_count < self.config.max_retries:
                        # Requeue with increased retry count
                        retry_count += 1
                        
                        # Publish with retry header
                        new_headers = dict(properties.headers or {})
                        new_headers["x-retry-count"] = retry_count
                        
                        # Publish again with retry header
                        self.publish(
                            topic=message.topic,
                            body=message.body,
                            priority=MessagePriority(properties.priority) if properties.priority else MessagePriority.NORMAL,
                            reply_to=properties.reply_to,
                            correlation_id=properties.correlation_id,
                            expiration=int(properties.expiration) if properties.expiration else None,
                            headers=new_headers
                        )
                        
                        logger.warning(f"Requeued message {message_id} (retry {retry_count})")
                        
                        # Ack the original message
                        channel.basic_ack(delivery_tag=method.delivery_tag)
                    else:
                        # Reject and don't requeue (will go to dead-letter queue if configured)
                        channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
                        
                        logger.warning(f"Rejected message {message_id} after {retry_count} retries")
                        
                        # Alert on dead-lettered message
                        self.alert_system.warning(
                            "Message Dead-Lettered",
                            f"Message on topic {message.topic} was dead-lettered after {retry_count} retries",
                            component="MessageBus",
                            tags={"message_id": message_id, "topic": message.topic}
                        )
            else:
                # No callback registered, just acknowledge
                channel.basic_ack(delivery_tag=method.delivery_tag)
                
        except Exception as e:
            logger.error(f"Error handling message: {str(e)}")
            
            # Negative acknowledge with requeue
            try:
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            except Exception:
                pass
    
    def get_connection_status(self) -> Dict[str, Any]:
        """
        Get connection status information.
        
        Returns:
            Status dictionary
        """
        return {
            "is_connected": self.is_connected,
            "host": self.config.host,
            "port": self.config.port,
            "exchange": self.config.exchange_name,
            "component_name": self.component_name,
            "publish_circuit_breaker": {
                "status": self.publish_circuit_breaker.status.value,
                "failure_count": self.publish_circuit_breaker.failure_count
            },
            "consume_circuit_breaker": {
                "status": self.consume_circuit_breaker.status.value,
                "failure_count": self.consume_circuit_breaker.failure_count
            },
            "queue_count": len(self.callbacks),
            "sent_message_count": len(self.sent_messages),
            "received_message_count": len(self.received_messages)
        }

# Global instance
_instance = None

def get_message_bus(component_name: str, config: Optional[Dict[str, Any]] = None) -> MessageBus:
    """
    Get or create the global MessageBus instance.
    
    Args:
        component_name: Name of the component using this bus
        config: Optional configuration (only used on first call)
        
    Returns:
        MessageBus instance
    """
    global _instance
    if _instance is None:
        _instance = MessageBus(component_name, config)
    return _instance 