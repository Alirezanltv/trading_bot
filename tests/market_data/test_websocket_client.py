import os
import sys
import json
import time
import asyncio
import unittest
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from trading_system.core.component import ComponentStatus
from trading_system.market_data.nobitex_websocket import NobitexWebSocketClient

class TestNobitexWebSocketClient(unittest.TestCase):
    """Test the Nobitex WebSocket client."""

    def setUp(self):
        """Set up test fixtures."""
        self.client = NobitexWebSocketClient()
        
        # Create test data
        self.ticker_data = {
            "channel": "ticker",
            "symbol": "BTCUSDT",
            "data": {
                "lastPrice": "50000",
                "bestBid": "49900",
                "bestAsk": "50100",
                "high24h": "51000",
                "low24h": "49000",
                "volume24h": "100",
                "timestamp": int(time.time() * 1000)
            }
        }
        
        self.trade_data = {
            "channel": "trades",
            "symbol": "BTCUSDT",
            "data": [
                {
                    "id": "123",
                    "price": "50000",
                    "amount": "1.5",
                    "side": "buy",
                    "timestamp": int(time.time() * 1000)
                }
            ]
        }
        
        self.orderbook_data = {
            "channel": "orderbook",
            "symbol": "BTCUSDT",
            "data": {
                "bids": [["49900", "1.0"], ["49800", "2.0"]],
                "asks": [["50100", "1.0"], ["50200", "2.0"]],
                "timestamp": int(time.time() * 1000)
            }
        }
        
        # Setup a default symbol mapping
        self.client.symbol_mapping = {"btc-usdt": "BTCUSDT"}
        self.client.reverse_symbol_mapping = {"BTCUSDT": "btc-usdt"}

    def tearDown(self):
        """Tear down test fixtures."""
        if self.client._client:
            asyncio.get_event_loop().run_until_complete(self.client.disconnect())
            
    async def async_setup(self):
        """Asynchronous setup for tests."""
        self.client._status = ComponentStatus.INITIALIZED
        
    def test_symbol_conversion(self):
        """Test symbol conversion between standard and exchange formats."""
        # Test conversion to exchange format
        self.assertEqual(self.client.get_exchange_symbol("btc-usdt"), "BTCUSDT")
        # Test conversion to standard format
        self.assertEqual(self.client.get_standard_symbol("BTCUSDT"), "btc-usdt")
        
    def test_default_mappings(self):
        """Test default symbol mappings."""
        client = NobitexWebSocketClient()
        client._create_default_mappings()
        self.assertIn("btc-usdt", client.symbol_mapping)
        self.assertIn("BTCUSDT", client.reverse_symbol_mapping)
        
    @patch('websockets.connect', new_callable=AsyncMock)
    async def test_connect_success(self, mock_connect):
        """Test successful WebSocket connection."""
        # Mock a successful connection
        mock_ws = AsyncMock()
        mock_connect.return_value = mock_ws
        
        # Test the connect method
        result = await self.client.connect()
        self.assertTrue(result)
        self.assertEqual(self.client._status, ComponentStatus.CONNECTED)
        mock_connect.assert_called_once()
        
    @patch('websockets.connect', new_callable=AsyncMock)
    async def test_connect_failure(self, mock_connect):
        """Test WebSocket connection failure."""
        # Mock a connection failure
        mock_connect.side_effect = Exception("Connection failed")
        
        # Test the connect method
        result = await self.client.connect()
        self.assertFalse(result)
        self.assertEqual(self.client._status, ComponentStatus.ERROR)
        
    @patch('websockets.connect', new_callable=AsyncMock)
    async def test_auto_reconnect(self, mock_connect):
        """Test automatic reconnection after disconnect."""
        # Mock a successful connection that later disconnects
        mock_ws = AsyncMock()
        mock_ws.send = AsyncMock()
        mock_ws.recv = AsyncMock(side_effect=[
            # First successful message
            json.dumps({"method": "auth", "result": {"status": "success", "token": "test-token"}}),
            # Then a connection close exception to trigger reconnect
            Exception("Connection closed")
        ])
        
        # Make connect return our mock on the first call, and a different mock on the second (reconnect)
        mock_connect.side_effect = [mock_ws, AsyncMock()]
        
        # Start the WebSocket listener in the background
        listener_task = asyncio.create_task(self.client._listen())
        
        # Wait a bit for the first connection and error
        await asyncio.sleep(0.1)
        
        # Manually call reconnect
        reconnect_task = asyncio.create_task(self.client._reconnect())
        
        # Wait for reconnect
        await asyncio.sleep(0.1)
        
        # Verify that connect was called twice (initial + reconnect)
        self.assertEqual(mock_connect.call_count, 2)
        
        # Clean up tasks
        listener_task.cancel()
        reconnect_task.cancel()
        
    async def test_process_ticker(self):
        """Test processing of ticker data."""
        # Setup a callback to capture processed data
        callback_called = asyncio.Event()
        callback_data = None
        
        async def ticker_callback(data):
            nonlocal callback_data
            callback_data = data
            callback_called.set()
        
        # Register the callback
        subscription_id = self.client._create_subscription_id("ticker", ["BTCUSDT"])
        self.client.subscription_callbacks[subscription_id] = [ticker_callback]
        
        # Process ticker data
        await self.client._process_ticker(self.ticker_data)
        
        # Wait for callback to be called with timeout
        await asyncio.wait_for(callback_called.wait(), timeout=1.0)
        
        # Verify processed data
        self.assertIsNotNone(callback_data)
        self.assertEqual(callback_data["symbol"], "btc-usdt")
        self.assertEqual(callback_data["price"], 50000.0)
        
    async def test_process_trades(self):
        """Test processing of trades data."""
        # Setup a callback to capture processed data
        callback_called = asyncio.Event()
        callback_data = None
        
        async def trades_callback(data):
            nonlocal callback_data
            callback_data = data
            callback_called.set()
        
        # Register the callback
        subscription_id = self.client._create_subscription_id("trades", ["BTCUSDT"])
        self.client.subscription_callbacks[subscription_id] = [trades_callback]
        
        # Process trades data
        await self.client._process_trades(self.trade_data)
        
        # Wait for callback to be called
        await asyncio.wait_for(callback_called.wait(), timeout=1.0)
        
        # Verify processed data
        self.assertIsNotNone(callback_data)
        self.assertIsInstance(callback_data, list)
        self.assertEqual(len(callback_data), 1)
        self.assertEqual(callback_data[0]["symbol"], "btc-usdt")
        self.assertEqual(callback_data[0]["price"], 50000.0)
        
    async def test_process_orderbook(self):
        """Test processing of orderbook data."""
        # Setup a callback to capture processed data
        callback_called = asyncio.Event()
        callback_data = None
        
        async def orderbook_callback(data):
            nonlocal callback_data
            callback_data = data
            callback_called.set()
        
        # Register the callback
        subscription_id = self.client._create_subscription_id("orderbook", ["BTCUSDT"])
        self.client.subscription_callbacks[subscription_id] = [orderbook_callback]
        
        # Process orderbook data
        await self.client._process_orderbook(self.orderbook_data)
        
        # Wait for callback to be called
        await asyncio.wait_for(callback_called.wait(), timeout=1.0)
        
        # Verify processed data
        self.assertIsNotNone(callback_data)
        self.assertEqual(callback_data["symbol"], "btc-usdt")
        self.assertEqual(len(callback_data["bids"]), 2)
        self.assertEqual(len(callback_data["asks"]), 2)
        self.assertEqual(callback_data["bids"][0][0], 49900.0)
        
    @patch('trading_system.market_data.nobitex_websocket.NobitexWebSocketClient._process_message')
    @patch('websockets.connect', new_callable=AsyncMock)
    async def test_message_handling(self, mock_connect, mock_process):
        """Test handling of different message types."""
        # Setup mock WebSocket
        mock_ws = AsyncMock()
        mock_connect.return_value = mock_ws
        
        # Define test messages
        auth_message = {"method": "auth", "result": {"status": "success", "token": "test-token"}}
        ticker_message = self.ticker_data
        error_message = {"error": "Some error"}
        
        # Convert to JSON
        auth_json = json.dumps(auth_message)
        ticker_json = json.dumps(ticker_message)
        error_json = json.dumps(error_message)
        
        # Set up the mock to return these messages in sequence
        mock_ws.recv.side_effect = [auth_json, ticker_json, error_json]
        
        # Connect to the WebSocket
        await self.client.connect()
        
        # Start listening
        listener_task = asyncio.create_task(self.client._listen())
        
        # Wait for messages to be processed
        await asyncio.sleep(0.2)
        
        # Verify process_message was called for each message
        self.assertEqual(mock_process.call_count, 3)
        mock_process.assert_any_call(auth_json)
        mock_process.assert_any_call(ticker_json)
        mock_process.assert_any_call(error_json)
        
        # Clean up
        listener_task.cancel()


# Integration tests (disabled by default, enable for actual testing)
class TestNobitexWebSocketIntegration(unittest.TestCase):
    """Integration tests for the Nobitex WebSocket client."""
    
    @unittest.skip("Skipping integration test that requires actual WebSocket connection")
    async def test_live_connection(self):
        """Test connection to the actual Nobitex WebSocket server."""
        client = NobitexWebSocketClient()
        
        # Connect to WebSocket
        connect_result = await client.connect()
        self.assertTrue(connect_result)
        
        # Subscribe to ticker for BTC/USDT
        subscription_success = asyncio.Event()
        
        async def on_ticker(data):
            print(f"Received ticker: {data}")
            subscription_success.set()
        
        await client.subscribe("ticker", ["btc-usdt"], on_ticker)
        
        # Wait for subscription data (timeout after 10 seconds)
        try:
            await asyncio.wait_for(subscription_success.wait(), timeout=10.0)
            self.assertTrue(subscription_success.is_set())
        except asyncio.TimeoutError:
            self.fail("Timed out waiting for ticker data")
        
        # Disconnect
        await client.disconnect()
    
    @unittest.skip("Skipping integration test for reconnection")
    async def test_live_reconnection(self):
        """Test reconnection to the Nobitex WebSocket server after disconnect."""
        client = NobitexWebSocketClient()
        
        # Connect to WebSocket
        connect_result = await client.connect()
        self.assertTrue(connect_result)
        
        # Get the initial WebSocket connection
        initial_ws = client._client
        
        # Force a disconnect
        await client.disconnect()
        
        # Wait a bit
        await asyncio.sleep(1)
        
        # Connect again
        reconnect_result = await client.connect()
        self.assertTrue(reconnect_result)
        
        # Verify we have a new WebSocket connection
        self.assertIsNotNone(client._client)
        self.assertNotEqual(initial_ws, client._client)
        
        # Disconnect
        await client.disconnect()


# Run the tests
if __name__ == '__main__':
    # Set up asyncio loop to run async tests
    loop = asyncio.get_event_loop()
    
    # Create test suite with only unit tests (skip integration tests by default)
    suite = unittest.TestLoader().loadTestsFromTestCase(TestNobitexWebSocketClient)
    
    # Run the tests
    unittest.TextTestRunner().run(suite) 