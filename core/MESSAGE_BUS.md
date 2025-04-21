# Message Bus Architecture

This document describes the message bus architecture used for inter-component communication in the trading system.

## Overview

The message bus provides a reliable, fault-tolerant communication mechanism between the various components of the trading system. It supports both in-memory messaging for development and testing, and distributed messaging using RabbitMQ for production deployments.

## Features

- **Pub/Sub Pattern**: Components can publish messages and subscribe to specific message types
- **Reliable Delivery**: Messages are guaranteed to be delivered, with acknowledgments and retries
- **Persistence**: Messages can be persisted to survive system restarts
- **Dead-Letter Queues**: Failed messages are captured for analysis and retry
- **Fault Tolerance**: Automatic reconnection and state recovery
- **Async/Sync Support**: Both synchronous and asynchronous APIs
- **Graceful Degradation**: Falls back to in-memory mode if broker is unavailable

## Message Types

The system uses a predefined set of message types, organized by subsystem:

- **System Messages**: Startup, shutdown, errors
- **Component Messages**: Status updates, errors
- **Market Data Messages**: Data updates, requests, errors
- **Strategy Messages**: Trading signals, status, errors
- **Execution Messages**: Order requests, statuses, errors
- **Exchange Messages**: Order operations, status, errors
- **Position Messages**: Position updates, close operations, errors
- **Risk Messages**: Limit breaches, exposure updates, errors

## Components

### MessageBroker Interface

The `MessageBroker` interface (`message_broker.py`) defines the core functionality that any message broker implementation must provide:

- Connection management
- Publishing messages
- Subscribing to message types
- Queue and exchange creation
- Dead-letter queue management

### RabbitMQ Implementation

The `RabbitMQBroker` (`rabbitmq_broker.py`) implements the `MessageBroker` interface using RabbitMQ:

- Provides reliable messaging with persistence
- Supports delivery acknowledgments
- Implements dead-letter queues for failed messages
- Automatically reconnects and recovers state
- Thread-safe operation

### Async RabbitMQ Implementation

The `AsyncRabbitMQBroker` (`async_rabbitmq_broker.py`) provides an asynchronous version of the RabbitMQ broker:

- Fully asynchronous API using `async`/`await`
- Non-blocking operations
- Compatible with async components

### Message Bus

The `MessageBus` class (`message_bus.py`) provides a unified API that automatically selects the appropriate implementation:

- Uses RabbitMQ in production mode
- Falls back to in-memory messaging for development/testing
- Maintains backward compatibility with existing code

## Configuration

The message bus mode can be configured in several ways:

1. **Environment Variable**: Set `TRADING_SYSTEM_MESSAGE_BUS_MODE` to either `in_memory` or `rabbitmq`
2. **Direct Configuration**: Pass the `mode` parameter when creating a `MessageBus` instance
3. **Default Behavior**: Falls back to in-memory mode if not specified or if RabbitMQ is unavailable

## Usage Examples

### Publishing Messages

```python
# Synchronous publishing
from trading_system.core.message_bus import message_bus, MessageTypes

# Publish a message
message_bus.publish_sync(
    message_type=MessageTypes.STRATEGY_SIGNAL,
    data={"symbol": "BTC/USDT", "action": "BUY", "price": 50000.0}
)

# Asynchronous publishing
import asyncio
from trading_system.core.message_bus import get_async_message_bus, MessageTypes

async def publish_example():
    async_bus = get_async_message_bus()
    await async_bus.publish(
        message_type=MessageTypes.STRATEGY_SIGNAL,
        data={"symbol": "BTC/USDT", "action": "BUY", "price": 50000.0}
    )

asyncio.run(publish_example())
```

### Subscribing to Messages

```python
# Synchronous handler
from trading_system.core.message_bus import message_bus, MessageTypes

async def handle_strategy_signal(message_type, data):
    print(f"Received signal: {data}")

# Subscribe to strategy signals
message_bus.subscribe(MessageTypes.STRATEGY_SIGNAL, handle_strategy_signal)

# Asynchronous handler
import asyncio
from trading_system.core.message_bus import get_async_message_bus, MessageTypes

async def async_handler(message_type, data):
    print(f"Async handler received: {data}")

async def subscribe_example():
    async_bus = get_async_message_bus()
    await async_bus.subscribe(MessageTypes.STRATEGY_SIGNAL, async_handler)

asyncio.run(subscribe_example())
```

## RabbitMQ Setup

To use RabbitMQ mode in production:

1. Install RabbitMQ server (or use a managed service)
2. Set the environment variable: `TRADING_SYSTEM_MESSAGE_BUS_MODE=rabbitmq`
3. Ensure the RabbitMQ connection parameters are configured:
   ```python
   # Default connection parameters
   DEFAULT_CONNECTION_PARAMS = {
       "host": "localhost",
       "port": 5672,
       "virtual_host": "/",
       "username": "guest",
       "password": "guest"
   }
   ```

## Message Flow

1. A component publishes a message with a specific type
2. The message bus routes the message to the appropriate exchange
3. The exchange routes the message to queues based on routing keys
4. Subscribers consume messages from their queues
5. Messages are acknowledged after successful processing
6. Failed messages are sent to dead-letter queues

## Error Handling

The message bus provides comprehensive error handling:

- **Automatic Reconnection**: Reconnects to RabbitMQ if the connection is lost
- **State Recovery**: Restores exchanges, queues, bindings, and subscriptions after reconnection
- **Dead-Letter Queues**: Failed messages are moved to dead-letter queues
- **Graceful Degradation**: Falls back to in-memory mode if RabbitMQ is unavailable
- **Comprehensive Logging**: Logs all errors and connection issues

## Best Practices

1. **Use Appropriate Message Types**: Choose the correct message type for each communication
2. **Keep Messages Small**: Avoid including large data structures in messages
3. **Handle Errors**: Always implement error handling in message handlers
4. **Use Async APIs**: Use asynchronous APIs for non-blocking operations
5. **Clean Up Resources**: Call `shutdown()` to properly clean up resources 