"""
Test Message Bus

This module contains tests for the message bus implementations.
"""

import os
import unittest
import asyncio
from unittest.mock import patch, MagicMock
from typing import Dict, Any, List

from trading_system.core.message_bus import (
    MessageBus,
    AsyncMessageBus,
    MessageTypes,
    MessageBusMode,
    get_async_message_bus
)
from trading_system.core.logging import get_logger

# Set up logger
logger = get_logger("tests.core.message_bus")


class TestMessageBus(unittest.TestCase):
    """Test case for the MessageBus class."""
    
    def setUp(self):
        """Set up test environment."""
        # Force in-memory mode for testing
        os.environ["TRADING_SYSTEM_MESSAGE_BUS_MODE"] = "in_memory"
        self.message_bus = MessageBus()
        self.received_messages = []
        
    def tearDown(self):
        """Clean up after tests."""
        self.message_bus.shutdown()
        self.received_messages = []
        
    async def message_handler(self, message_type, data):
        """Test message handler."""
        self.received_messages.append((message_type, data))
        
    def test_message_bus_init(self):
        """Test MessageBus initialization."""
        self.assertEqual(self.message_bus.get_mode(), MessageBusMode.IN_MEMORY)
        
    def test_subscribe_unsubscribe(self):
        """Test subscribe and unsubscribe methods."""
        # Subscribe to a message type
        self.message_bus.subscribe(MessageTypes.SYSTEM_STARTUP, self.message_handler)
        
        # Check that the handler is subscribed
        subscribers = self.message_bus.get_subscribers(MessageTypes.SYSTEM_STARTUP)
        self.assertEqual(len(subscribers), 1)
        self.assertEqual(subscribers[0], self.message_handler)
        
        # Unsubscribe from the message type
        self.message_bus.unsubscribe(MessageTypes.SYSTEM_STARTUP, self.message_handler)
        
        # Check that the handler is unsubscribed
        subscribers = self.message_bus.get_subscribers(MessageTypes.SYSTEM_STARTUP)
        self.assertEqual(len(subscribers), 0)
        
    def test_publish_sync(self):
        """Test synchronous publishing."""
        # Subscribe to a message type
        self.message_bus.subscribe(MessageTypes.SYSTEM_STARTUP, self.message_handler)
        
        # Publish a message
        test_data = {"status": "starting"}
        
        # Run in event loop
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.message_bus.publish(MessageTypes.SYSTEM_STARTUP, test_data))
        
        # Check that the message was received
        self.assertEqual(len(self.received_messages), 1)
        message_type, data = self.received_messages[0]
        self.assertEqual(message_type, MessageTypes.SYSTEM_STARTUP)
        self.assertEqual(data, test_data)
        
    def test_message_types(self):
        """Test message types."""
        # Get all message types
        message_types = list(MessageTypes)
        
        # Check that we have a reasonable number of message types
        self.assertGreater(len(message_types), 5)
        
        # Check specific message types
        self.assertIn(MessageTypes.SYSTEM_STARTUP, message_types)
        self.assertIn(MessageTypes.MARKET_DATA_UPDATE, message_types)
        self.assertIn(MessageTypes.STRATEGY_SIGNAL, message_types)
        
    @patch("trading_system.core.message_bus.rabbitmq_broker")
    def test_rabbitmq_mode(self, mock_rabbitmq):
        """Test RabbitMQ mode."""
        # Set up mock
        mock_rabbitmq.connect.return_value = True
        mock_rabbitmq.publish.return_value = True
        mock_rabbitmq.subscribe.return_value = True
        
        # Create message bus in RabbitMQ mode
        os.environ["TRADING_SYSTEM_MESSAGE_BUS_MODE"] = "rabbitmq"
        message_bus = MessageBus()
        
        # Check mode
        self.assertEqual(message_bus.get_mode(), MessageBusMode.RABBITMQ)
        
        # Test publish
        test_data = {"status": "test"}
        message_bus.publish_sync(MessageTypes.SYSTEM_STARTUP, test_data)
        
        # Check that RabbitMQ publish was called
        mock_rabbitmq.publish.assert_called_once()
        
        # Clean up
        message_bus.shutdown()


class TestAsyncMessageBus(unittest.IsolatedAsyncioTestCase):
    """Test case for the AsyncMessageBus class."""
    
    async def asyncSetUp(self):
        """Set up test environment."""
        # Force in-memory mode for testing
        os.environ["TRADING_SYSTEM_MESSAGE_BUS_MODE"] = "in_memory"
        self.message_bus = AsyncMessageBus()
        self.received_messages = []
        
    async def asyncTearDown(self):
        """Clean up after tests."""
        await self.message_bus.shutdown()
        self.received_messages = []
        
    async def message_handler(self, message_type, data):
        """Test message handler."""
        self.received_messages.append((message_type, data))
        
    async def test_message_bus_init(self):
        """Test AsyncMessageBus initialization."""
        self.assertEqual(self.message_bus.get_mode(), MessageBusMode.IN_MEMORY)
        
    async def test_subscribe_unsubscribe(self):
        """Test subscribe and unsubscribe methods."""
        # Subscribe to a message type
        await self.message_bus.subscribe(MessageTypes.SYSTEM_STARTUP, self.message_handler)
        
        # Check that the handler is subscribed
        # This is internal state, so we can't check directly without breaking encapsulation
        
        # Unsubscribe from the message type
        await self.message_bus.unsubscribe(MessageTypes.SYSTEM_STARTUP, self.message_handler)
        
    async def test_publish(self):
        """Test asynchronous publishing."""
        # Subscribe to a message type
        await self.message_bus.subscribe(MessageTypes.SYSTEM_STARTUP, self.message_handler)
        
        # Publish a message
        test_data = {"status": "starting"}
        await self.message_bus.publish(MessageTypes.SYSTEM_STARTUP, test_data)
        
        # Give the message time to be processed
        await asyncio.sleep(0.1)
        
        # Check that the message was received
        self.assertEqual(len(self.received_messages), 1)
        message_type, data = self.received_messages[0]
        self.assertEqual(message_type, MessageTypes.SYSTEM_STARTUP)
        self.assertEqual(data, test_data)
        
    async def test_get_async_message_bus(self):
        """Test get_async_message_bus function."""
        # Get the async message bus
        async_bus = get_async_message_bus()
        
        # Check that it's the right type
        self.assertIsInstance(async_bus, AsyncMessageBus)
        
        # Check that calling it again returns the same instance
        async_bus2 = get_async_message_bus()
        self.assertIs(async_bus, async_bus2)
        
    @patch("trading_system.core.message_bus.async_rabbitmq_broker")
    async def test_rabbitmq_mode(self, mock_rabbitmq):
        """Test RabbitMQ mode."""
        # Set up mock
        mock_rabbitmq.connect_async.return_value = True
        mock_rabbitmq.publish_async.return_value = True
        mock_rabbitmq.subscribe_async.return_value = True
        
        # Create message bus in RabbitMQ mode
        os.environ["TRADING_SYSTEM_MESSAGE_BUS_MODE"] = "rabbitmq"
        message_bus = AsyncMessageBus()
        
        # Connect
        await message_bus.connect()
        
        # Check mode
        self.assertEqual(message_bus.get_mode(), MessageBusMode.RABBITMQ)
        
        # Test publish
        test_data = {"status": "test"}
        await message_bus.publish(MessageTypes.SYSTEM_STARTUP, test_data)
        
        # Check that RabbitMQ publish was called
        mock_rabbitmq.publish_async.assert_called_once()
        
        # Clean up
        await message_bus.shutdown()


if __name__ == "__main__":
    unittest.main() 