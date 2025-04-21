"""
RabbitMQ Adapter for High-Reliability Messaging

Features:
- Connection pooling with automatic reconnection
- Publisher confirms for guaranteed message delivery
- Consumer acknowledgments with redelivery
- Dead letter exchanges for failed messages
- Circuit breaker pattern for fault tolerance
- Persistent messages and durable queues
- Channel pooling for concurrent operations
- Topology recovery on reconnection
- Comprehensive error handling and recovery
"""

import asyncio
import json
import logging
import time
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

import aio_pika
import pybreaker
from aio_pika import connect_robust, Message, DeliveryMode, ExchangeType
from prometheus_client import Counter, Gauge, Histogram

logger = logging.getLogger(__name__)

# Metrics
MESSAGES_PUBLISHED = Counter('rabbitmq_messages_published_total', 'Total messages published', ['exchange', 'routing_key'])
MESSAGES_CONSUMED = Counter('rabbitmq_messages_consumed_total', 'Total messages consumed', ['queue'])
PUBLISH_LATENCY = Histogram('rabbitmq_publish_latency_seconds', 'Message publish latency in seconds')
CONSUME_LATENCY = Histogram('rabbitmq_consume_latency_seconds', 'Message consume latency in seconds')
CONNECTION_STATUS = Gauge('rabbitmq_connection_status', 'RabbitMQ connection status (1=connected, 0=disconnected)')
CHANNEL_POOL_SIZE = Gauge('rabbitmq_channel_pool_size', 'Size of the channel pool')
FAILED_MESSAGES = Counter('rabbitmq_failed_messages_total', 'Total failed messages', ['exchange', 'routing_key'])

class MessagePriority(Enum):
    LOW = 1
    NORMAL = 5
    HIGH = 8
    CRITICAL = 10

class RabbitMQAdapter:
    """High-reliability RabbitMQ adapter with connection pooling and guaranteed delivery."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the RabbitMQ adapter.
        
        Args:
            config: Configuration dictionary with the following keys:
                host: RabbitMQ host
                port: RabbitMQ port
                username: RabbitMQ username
                password: RabbitMQ password
                vhost: RabbitMQ virtual host
                connection_pool_size: Number of connections in the pool
                channel_pool_size: Number of channels per connection
                retry_interval: Seconds between connection retry attempts
                max_retries: Maximum number of retry attempts (-1 for infinite)
                heartbeat: Heartbeat interval in seconds
                connection_timeout: Connection timeout in seconds
                prefetch_count: Maximum number of unacknowledged messages
                dead_letter_exchange: Name of the dead letter exchange
                ssl: Whether to use SSL
                ssl_options: SSL options dictionary
        """
        self.config = config
        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 5672)
        self.username = config.get('username', 'guest')
        self.password = config.get('password', 'guest')
        self.vhost = config.get('vhost', '/')
        self.connection_pool_size = config.get('connection_pool_size', 2)
        self.channel_pool_size = config.get('channel_pool_size', 10)
        self.retry_interval = config.get('retry_interval', 5)
        self.max_retries = config.get('max_retries', -1)
        self.heartbeat = config.get('heartbeat', 60)
        self.connection_timeout = config.get('connection_timeout', 30)
        self.prefetch_count = config.get('prefetch_count', 100)
        self.dead_letter_exchange = config.get('dead_letter_exchange', 'dead_letters')
        self.ssl = config.get('ssl', False)
        self.ssl_options = config.get('ssl_options', {})
        
        # Connection and channel pools
        self.connections = []
        self.available_channels = []
        self.in_use_channels = set()
        
        # Circuit breaker for publish operations
        self.publish_breaker = pybreaker.CircuitBreaker(
            fail_max=3,
            reset_timeout=60,
            exclude=[aio_pika.exceptions.ConnectionClosed],
            name='rabbitmq_publish'
        )
        
        # Setup lock for connection operations
        self.connection_lock = asyncio.Lock()
        
        # Status flags
        self.initialized = False
        self.shutting_down = False
        
        # Declare exchanges and queues
        self.exchange_declarations = {}
        self.queue_declarations = {}
        
        # Setup task for connection monitoring
        self.connection_monitor_task = None
        
    async def initialize(self):
        """Initialize connections and channel pool."""
        if self.initialized:
            return
            
        async with self.connection_lock:
            if self.initialized:  # Double-check inside lock
                return
                
            logger.info(f"Initializing RabbitMQ adapter with {self.connection_pool_size} connections")
            CONNECTION_STATUS.set(0)
            
            # Create connection pool
            for _ in range(self.connection_pool_size):
                try:
                    connection = await self._create_connection()
                    self.connections.append(connection)
                    
                    # Create channels for this connection
                    for _ in range(self.channel_pool_size // self.connection_pool_size):
                        channel = await connection.channel()
                        await channel.set_qos(prefetch_count=self.prefetch_count)
                        self.available_channels.append((connection, channel))
                    
                except Exception as e:
                    logger.error(f"Failed to initialize RabbitMQ connection: {e}")
                    raise
            
            # Update metrics
            CHANNEL_POOL_SIZE.set(len(self.available_channels))
            CONNECTION_STATUS.set(1)
            
            # Create dead letter exchange
            await self.declare_exchange(
                self.dead_letter_exchange,
                ExchangeType.TOPIC,
                durable=True
            )
            
            # Start connection monitor
            self.connection_monitor_task = asyncio.create_task(self._monitor_connections())
            
            self.initialized = True
            logger.info("RabbitMQ adapter initialized successfully")

    async def _create_connection(self) -> aio_pika.RobustConnection:
        """Create a new robust connection to RabbitMQ with automatic reconnection."""
        connection_string = f"amqp://{self.username}:{self.password}@{self.host}:{self.port}/{self.vhost}"
        
        # Add SSL if enabled
        connection_kwargs = {
            "heartbeat": self.heartbeat,
            "timeout": self.connection_timeout,
        }
        
        if self.ssl:
            connection_kwargs["ssl"] = True
            connection_kwargs["ssl_options"] = self.ssl_options
            
        connection = await connect_robust(
            connection_string,
            **connection_kwargs
        )
        
        # Setup reconnection event handlers
        connection.reconnect_callbacks.append(self._on_connection_reconnect)
        connection.close_callbacks.append(self._on_connection_close)
        
        logger.info(f"Created connection to RabbitMQ: {self.host}:{self.port}")
        return connection
        
    async def _get_channel(self):
        """Get an available channel from the pool or create a new one."""
        if not self.initialized:
            await self.initialize()
            
        # Try to get a channel from the pool
        if self.available_channels:
            connection, channel = self.available_channels.pop()
            self.in_use_channels.add((connection, channel))
            CHANNEL_POOL_SIZE.set(len(self.available_channels))
            return connection, channel
            
        # No channels available, try to create a new one
        async with self.connection_lock:
            # Choose a connection round-robin style
            if not self.connections:
                raise RuntimeError("No RabbitMQ connections available")
                
            # Get the connection with the fewest channels
            connection = min(self.connections, key=lambda c: sum(1 for x in self.in_use_channels if x[0] == c))
            
            try:
                channel = await connection.channel()
                await channel.set_qos(prefetch_count=self.prefetch_count)
                self.in_use_channels.add((connection, channel))
                return connection, channel
            except Exception as e:
                logger.error(f"Failed to create channel: {e}")
                raise
                
    async def _release_channel(self, connection, channel):
        """Return a channel to the pool."""
        if (connection, channel) in self.in_use_channels:
            self.in_use_channels.remove((connection, channel))
            
            # Only return to pool if not closed
            if not channel.is_closed:
                self.available_channels.append((connection, channel))
                CHANNEL_POOL_SIZE.set(len(self.available_channels))
                
    async def declare_exchange(self, name: str, type_: ExchangeType = ExchangeType.TOPIC, 
                             durable: bool = True, auto_delete: bool = False,
                             arguments: Optional[Dict] = None) -> aio_pika.Exchange:
        """
        Declare an exchange.
        
        Args:
            name: Exchange name
            type_: Exchange type (DIRECT, TOPIC, FANOUT, HEADERS)
            durable: Whether the exchange survives broker restarts
            auto_delete: Whether to delete the exchange when no queues are bound
            arguments: Additional exchange arguments
            
        Returns:
            The declared exchange
        """
        if not self.initialized:
            await self.initialize()
            
        # Cache declaration parameters
        self.exchange_declarations[name] = {
            "type": type_,
            "durable": durable,
            "auto_delete": auto_delete,
            "arguments": arguments or {}
        }
        
        connection, channel = await self._get_channel()
        try:
            exchange = await channel.declare_exchange(
                name,
                type=type_,
                durable=durable,
                auto_delete=auto_delete,
                arguments=arguments
            )
            logger.info(f"Declared exchange '{name}' of type {type_.name}")
            return exchange
        finally:
            await self._release_channel(connection, channel)
            
    async def declare_queue(self, name: str, durable: bool = True, exclusive: bool = False,
                          auto_delete: bool = False, arguments: Optional[Dict] = None,
                          dead_letter: bool = True) -> aio_pika.Queue:
        """
        Declare a queue.
        
        Args:
            name: Queue name
            durable: Whether the queue survives broker restarts
            exclusive: Whether the queue is exclusive to this connection
            auto_delete: Whether to delete the queue when no consumers
            arguments: Additional queue arguments
            dead_letter: Whether to configure dead letter routing
            
        Returns:
            The declared queue
        """
        if not self.initialized:
            await self.initialize()
            
        # Prepare arguments
        args = arguments or {}
        
        # Add dead letter configuration if requested
        if dead_letter and self.dead_letter_exchange:
            args["x-dead-letter-exchange"] = self.dead_letter_exchange
            args["x-dead-letter-routing-key"] = f"failed.{name}"
            
        # Cache declaration parameters
        self.queue_declarations[name] = {
            "durable": durable,
            "exclusive": exclusive,
            "auto_delete": auto_delete,
            "arguments": args
        }
        
        connection, channel = await self._get_channel()
        try:
            queue = await channel.declare_queue(
                name,
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete,
                arguments=args
            )
            logger.info(f"Declared queue '{name}'")
            return queue
        finally:
            await self._release_channel(connection, channel)
            
    async def bind_queue(self, queue_name: str, exchange_name: str, routing_key: str = "#"):
        """
        Bind a queue to an exchange with a routing key.
        
        Args:
            queue_name: Queue name
            exchange_name: Exchange name
            routing_key: Routing key pattern
        """
        if not self.initialized:
            await self.initialize()
            
        connection, channel = await self._get_channel()
        try:
            # Get the exchange
            exchange = await channel.get_exchange(exchange_name)
            
            # Get the queue
            queue = await channel.get_queue(queue_name)
            
            # Bind queue to exchange
            await queue.bind(exchange, routing_key)
            logger.info(f"Bound queue '{queue_name}' to exchange '{exchange_name}' with routing key '{routing_key}'")
        finally:
            await self._release_channel(connection, channel)
            
    async def publish(self, exchange_name: str, routing_key: str, message: Union[dict, str, bytes],
                    priority: MessagePriority = MessagePriority.NORMAL,
                    persistent: bool = True, headers: Optional[Dict] = None,
                    expiration: Optional[int] = None, message_id: Optional[str] = None,
                    correlation_id: Optional[str] = None) -> bool:
        """
        Publish a message to an exchange.
        
        Args:
            exchange_name: Exchange name
            routing_key: Routing key
            message: Message content (dict, string, or bytes)
            priority: Message priority
            persistent: Whether to persist the message
            headers: Message headers
            expiration: Message expiration in milliseconds
            message_id: Message ID
            correlation_id: Correlation ID
            
        Returns:
            True if message was confirmed by the broker
        """
        if not self.initialized:
            await self.initialize()
            
        # Convert dict to JSON string
        if isinstance(message, dict):
            body = json.dumps(message).encode()
        elif isinstance(message, str):
            body = message.encode()
        else:
            body = message
            
        # Create message properties
        properties = {
            "delivery_mode": DeliveryMode.PERSISTENT if persistent else DeliveryMode.NOT_PERSISTENT,
            "priority": priority.value,
            "headers": headers or {},
            "message_id": message_id,
            "timestamp": int(time.time()),
            "correlation_id": correlation_id,
        }
        
        if expiration:
            properties["expiration"] = str(expiration)
            
        message_obj = Message(
            body=body,
            **properties
        )
        
        connection = None
        channel = None
        
        # Track publish time for metrics
        start_time = time.time()
        
        try:
            # Execute with circuit breaker protection
            return await self.publish_breaker.execute(self._publish_internal, 
                                                    exchange_name, routing_key, message_obj)
        except Exception as e:
            FAILED_MESSAGES.labels(exchange=exchange_name, routing_key=routing_key).inc()
            logger.error(f"Failed to publish message to {exchange_name}/{routing_key}: {e}")
            raise
    
    async def _publish_internal(self, exchange_name: str, routing_key: str, message_obj: Message) -> bool:
        """Internal method to publish a message with circuit breaker protection"""
        connection = None
        channel = None
        
        # Track publish time for metrics
        start_time = time.time()
        
        try:
            # Get a channel
            connection, channel = await self._get_channel()
            
            # Get the exchange
            exchange = await channel.get_exchange(exchange_name)
            
            # Enable publisher confirms
            await channel.confirm_delivery()
            
            # Publish the message
            await exchange.publish(message_obj, routing_key)
            
            # Update metrics
            publish_time = time.time() - start_time
            PUBLISH_LATENCY.observe(publish_time)
            MESSAGES_PUBLISHED.labels(exchange=exchange_name, routing_key=routing_key).inc()
            
            return True
        finally:
            if connection and channel:
                await self._release_channel(connection, channel)
                
    async def consume(self, queue_name: str, callback: Callable, consumer_tag: Optional[str] = None,
                    no_ack: bool = False, exclusive: bool = False, 
                    arguments: Optional[Dict] = None) -> str:
        """
        Register a consumer for a queue.
        
        Args:
            queue_name: Queue name
            callback: Async callback function(message)
            consumer_tag: Consumer tag
            no_ack: Whether to auto-acknowledge messages
            exclusive: Whether the consumer is exclusive
            arguments: Additional consumer arguments
            
        Returns:
            Consumer tag
        """
        if not self.initialized:
            await self.initialize()
            
        # Wrapper for the callback to handle metrics and errors
        async def _callback_wrapper(message):
            start_time = time.time()
            
            try:
                # Process message
                await callback(message)
                
                # Acknowledge message if not auto-ack
                if not no_ack:
                    await message.ack()
                    
                # Update metrics
                MESSAGES_CONSUMED.labels(queue=queue_name).inc()
                CONSUME_LATENCY.observe(time.time() - start_time)
                
            except Exception as e:
                logger.error(f"Error processing message from queue {queue_name}: {e}")
                
                # Reject message and requeue if not auto-ack
                if not no_ack:
                    await message.reject(requeue=True)
                    
                # You might want to implement a retry counter in message headers
                # to eventually move to dead letter after X retries
        
        connection, channel = await self._get_channel()
        
        # Get the queue
        queue = await channel.get_queue(queue_name)
        
        # Start consuming
        consumer_tag = await queue.consume(
            _callback_wrapper,
            consumer_tag=consumer_tag,
            no_ack=no_ack,
            exclusive=exclusive,
            arguments=arguments
        )
        
        logger.info(f"Started consuming from queue '{queue_name}' with tag '{consumer_tag}'")
        return consumer_tag
        
    async def _monitor_connections(self):
        """Periodically check connections and reconnect if needed."""
        while not self.shutting_down:
            try:
                reconnected = False
                
                async with self.connection_lock:
                    # Check each connection
                    for i, connection in enumerate(self.connections):
                        if connection.is_closed:
                            logger.warning(f"Connection {i} is closed, attempting to reconnect")
                            try:
                                # Create a new connection
                                new_connection = await self._create_connection()
                                
                                # Replace the closed connection
                                self.connections[i] = new_connection
                                
                                # Create new channels for this connection
                                for _ in range(self.channel_pool_size // self.connection_pool_size):
                                    channel = await new_connection.channel()
                                    await channel.set_qos(prefetch_count=self.prefetch_count)
                                    self.available_channels.append((new_connection, channel))
                                    
                                reconnected = True
                                logger.info(f"Successfully reconnected connection {i}")
                            except Exception as e:
                                logger.error(f"Failed to reconnect connection {i}: {e}")
                                
                    # Recreate topology if reconnected
                    if reconnected:
                        await self._restore_topology()
                        
                # Update connection status metric
                CONNECTION_STATUS.set(1 if any(not c.is_closed for c in self.connections) else 0)
                
            except Exception as e:
                logger.error(f"Error in connection monitor: {e}")
                
            # Wait before next check
            await asyncio.sleep(self.retry_interval)
            
    async def _restore_topology(self):
        """Restore exchanges and queues after reconnection."""
        try:
            # Restore exchanges
            for name, params in self.exchange_declarations.items():
                connection, channel = await self._get_channel()
                try:
                    await channel.declare_exchange(
                        name,
                        type=params["type"],
                        durable=params["durable"],
                        auto_delete=params["auto_delete"],
                        arguments=params["arguments"]
                    )
                finally:
                    await self._release_channel(connection, channel)
                    
            # Restore queues
            for name, params in self.queue_declarations.items():
                connection, channel = await self._get_channel()
                try:
                    await channel.declare_queue(
                        name,
                        durable=params["durable"],
                        exclusive=params["exclusive"],
                        auto_delete=params["auto_delete"],
                        arguments=params["arguments"]
                    )
                finally:
                    await self._release_channel(connection, channel)
                    
            logger.info("Successfully restored RabbitMQ topology")
        except Exception as e:
            logger.error(f"Failed to restore topology: {e}")
            
    async def _on_connection_reconnect(self, connection):
        """Called when a connection is reconnected."""
        logger.info(f"Connection reconnected to {self.host}:{self.port}")
        
    async def _on_connection_close(self, connection, exception):
        """Called when a connection is closed."""
        if exception:
            logger.warning(f"Connection to {self.host}:{self.port} closed with error: {exception}")
        else:
            logger.info(f"Connection to {self.host}:{self.port} closed")
            
    async def shutdown(self):
        """Gracefully shut down the adapter."""
        logger.info("Shutting down RabbitMQ adapter")
        self.shutting_down = True
        
        # Cancel connection monitor
        if self.connection_monitor_task:
            self.connection_monitor_task.cancel()
            try:
                await self.connection_monitor_task
            except asyncio.CancelledError:
                pass
                
        # Close all channels
        for connection, channel in list(self.in_use_channels) + self.available_channels:
            if not channel.is_closed:
                await channel.close()
                
        # Close all connections
        for connection in self.connections:
            if not connection.is_closed:
                await connection.close()
                
        self.connections = []
        self.available_channels = []
        self.in_use_channels = set()
        
        self.initialized = False
        CONNECTION_STATUS.set(0)
        logger.info("RabbitMQ adapter shut down successfully")

# Example usage
async def example():
    # Configuration
    config = {
        "host": "localhost",
        "port": 5672,
        "username": "guest",
        "password": "guest",
        "vhost": "/",
        "connection_pool_size": 2,
        "channel_pool_size": 10,
        "retry_interval": 5,
        "max_retries": -1,
        "heartbeat": 60,
        "connection_timeout": 30,
        "prefetch_count": 100,
        "dead_letter_exchange": "dead_letters",
        "ssl": False,
    }
    
    # Initialize adapter
    adapter = RabbitMQAdapter(config)
    await adapter.initialize()
    
    # Declare exchanges and queues
    await adapter.declare_exchange("market_data", ExchangeType.TOPIC)
    await adapter.declare_queue("price_candles")
    await adapter.bind_queue("price_candles", "market_data", "price.candles.*")
    
    # Publish a message
    await adapter.publish("market_data", "price.candles.BTCUSDT", {
        "symbol": "BTCUSDT",
        "interval": "1m",
        "open": 50000.0,
        "high": 50100.0,
        "low": 49900.0,
        "close": 50050.0,
        "volume": 10.5,
        "timestamp": int(time.time() * 1000)
    })
    
    # Define a consumer callback
    async def process_candle(message):
        body = json.loads(message.body.decode())
        print(f"Received candle: {body}")
    
    # Start consuming
    await adapter.consume("price_candles", process_candle)
    
    # Wait for messages
    await asyncio.sleep(60)
    
    # Shutdown
    await adapter.shutdown()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(example()) 