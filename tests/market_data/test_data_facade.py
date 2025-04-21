import os
import sys
import unittest
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from trading_system.market_data.market_data_facade import MarketDataFacade, MarketDataProvider
from trading_system.market_data.data_unifier import DataUnifier
from trading_system.core.message_bus import MessageBus, MessageType
from trading_system.core.component import ComponentStatus


class MockDataProvider(MarketDataProvider):
    """Mock implementation of MarketDataProvider for testing."""
    
    def __init__(self, name, priority=1, status=ComponentStatus.INITIALIZED):
        super().__init__(name=name, priority=priority)
        self._status = status
        self.get_candles_called = 0
        self.get_ticker_called = 0
        self.get_orderbook_called = 0
        self.get_trades_called = 0
        self.get_symbols_called = 0
        self.results = {}
    
    def set_result(self, method, result):
        """Set the result to return for a specific method."""
        self.results[method] = result
    
    def set_exception(self, method, exception):
        """Set an exception to raise for a specific method."""
        self.results[method] = exception
    
    async def initialize(self):
        """Initialize the provider."""
        return True
    
    async def get_candles(self, symbol, timeframe, limit=100, since=None):
        """Get candle data."""
        self.get_candles_called += 1
        if "get_candles" in self.results:
            result = self.results["get_candles"]
            if isinstance(result, Exception):
                raise result
            return result
        
        # Default mock data
        timestamp = int(datetime.now().timestamp() * 1000)
        return [
            {
                "symbol": symbol,
                "timeframe": timeframe,
                "time": timestamp - (i * 60000),
                "open": 50000.0 + i,
                "high": 51000.0 + i,
                "low": 49000.0 + i,
                "close": 50500.0 + i,
                "volume": 10.5 + i,
                "source": self.name
            }
            for i in range(limit)
        ]
    
    async def get_ticker(self, symbol):
        """Get ticker data."""
        self.get_ticker_called += 1
        if "get_ticker" in self.results:
            result = self.results["get_ticker"]
            if isinstance(result, Exception):
                raise result
            return result
        
        # Default mock data
        return {
            "symbol": symbol,
            "last": 50000.0,
            "bid": 49900.0,
            "ask": 50100.0,
            "high": 51000.0,
            "low": 49000.0,
            "volume": 10.5,
            "source": self.name
        }
    
    async def get_orderbook(self, symbol, limit=10):
        """Get orderbook data."""
        self.get_orderbook_called += 1
        if "get_orderbook" in self.results:
            result = self.results["get_orderbook"]
            if isinstance(result, Exception):
                raise result
            return result
        
        # Default mock data
        return {
            "symbol": symbol,
            "bids": [[49900.0 - (i * 100), 1.0 + i * 0.1] for i in range(limit)],
            "asks": [[50100.0 + (i * 100), 1.0 + i * 0.1] for i in range(limit)],
            "source": self.name
        }
    
    async def get_trades(self, symbol, limit=50, since=None):
        """Get recent trades."""
        self.get_trades_called += 1
        if "get_trades" in self.results:
            result = self.results["get_trades"]
            if isinstance(result, Exception):
                raise result
            return result
        
        # Default mock data
        timestamp = int(datetime.now().timestamp() * 1000)
        return {
            "symbol": symbol,
            "trades": [
                {
                    "id": i,
                    "timestamp": timestamp - (i * 1000),
                    "price": 50000.0 + (i % 10) * 10,
                    "amount": 0.1 + (i % 5) * 0.1,
                    "side": "buy" if i % 2 == 0 else "sell",
                    "cost": (50000.0 + (i % 10) * 10) * (0.1 + (i % 5) * 0.1)
                }
                for i in range(limit)
            ],
            "source": self.name
        }
    
    async def get_symbols(self):
        """Get available symbols."""
        self.get_symbols_called += 1
        if "get_symbols" in self.results:
            result = self.results["get_symbols"]
            if isinstance(result, Exception):
                raise result
            return result
        
        # Default mock data
        return ["BTC/USDT", "ETH/USDT", "XRP/USDT"]


class TestMarketDataFacade(unittest.TestCase):
    """Test the MarketDataFacade component."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create message bus
        self.message_bus = MessageBus()
        
        # Create data unifier
        self.data_unifier = DataUnifier()
        
        # Create facade
        self.facade = MarketDataFacade(
            message_bus=self.message_bus,
            data_unifier=self.data_unifier
        )
        
        # Create mock providers
        self.primary_provider = MockDataProvider(name="primary", priority=10)
        self.secondary_provider = MockDataProvider(name="secondary", priority=5)
        self.tertiary_provider = MockDataProvider(name="tertiary", priority=1)
        
        # Register providers
        self.facade.register_provider(self.primary_provider)
        self.facade.register_provider(self.secondary_provider)
        self.facade.register_provider(self.tertiary_provider)
    
    async def async_setup(self):
        """Asynchronous setup."""
        # Initialize facade
        await self.facade.initialize()
    
    async def teardown(self):
        """Clean up resources."""
        await self.facade.stop()
    
    async def test_provider_registration(self):
        """Test registration of providers."""
        await self.async_setup()
        
        # Check that all providers are registered
        self.assertEqual(len(self.facade._providers), 3)
        self.assertIn(self.primary_provider, self.facade._providers)
        self.assertIn(self.secondary_provider, self.facade._providers)
        self.assertIn(self.tertiary_provider, self.facade._providers)
        
        # Test provider lookup by name
        self.assertEqual(self.facade._get_provider_by_name("primary"), self.primary_provider)
        self.assertEqual(self.facade._get_provider_by_name("secondary"), self.secondary_provider)
        self.assertEqual(self.facade._get_provider_by_name("tertiary"), self.tertiary_provider)
        self.assertIsNone(self.facade._get_provider_by_name("nonexistent"))
    
    async def test_provider_priority_sorting(self):
        """Test that providers are sorted by priority."""
        await self.async_setup()
        
        # Get providers in priority order
        providers = self.facade._get_providers_by_priority()
        
        # Check order
        self.assertEqual(providers[0], self.primary_provider)
        self.assertEqual(providers[1], self.secondary_provider)
        self.assertEqual(providers[2], self.tertiary_provider)
    
    async def test_get_candles_primary_provider(self):
        """Test getting candles from primary provider."""
        await self.async_setup()
        
        # Set up mock data
        mock_candles = [
            {
                "symbol": "BTC/USDT",
                "timeframe": "1h",
                "time": 1609459200000,  # 2021-01-01T00:00:00Z
                "open": 29000.0,
                "high": 29100.0,
                "low": 28900.0,
                "close": 29050.0,
                "volume": 100.0,
                "source": "primary"
            }
        ]
        self.primary_provider.set_result("get_candles", mock_candles)
        
        # Get candles
        result = await self.facade.get_candles(symbol="BTC/USDT", timeframe="1h", limit=1)
        
        # Check that we got the expected result
        self.assertEqual(result, mock_candles)
        
        # Check that only primary provider was called
        self.assertEqual(self.primary_provider.get_candles_called, 1)
        self.assertEqual(self.secondary_provider.get_candles_called, 0)
        self.assertEqual(self.tertiary_provider.get_candles_called, 0)
    
    async def test_get_candles_fallback(self):
        """Test fallback to secondary provider if primary fails."""
        await self.async_setup()
        
        # Make primary provider fail
        self.primary_provider.set_exception("get_candles", Exception("Test error"))
        
        # Set up mock data for secondary provider
        mock_candles = [
            {
                "symbol": "BTC/USDT",
                "timeframe": "1h",
                "time": 1609459200000,
                "open": 29000.0,
                "high": 29100.0,
                "low": 28900.0,
                "close": 29050.0,
                "volume": 100.0,
                "source": "secondary"
            }
        ]
        self.secondary_provider.set_result("get_candles", mock_candles)
        
        # Get candles
        result = await self.facade.get_candles(symbol="BTC/USDT", timeframe="1h", limit=1)
        
        # Check that we got the expected result from secondary
        self.assertEqual(result, mock_candles)
        
        # Check that both providers were called
        self.assertEqual(self.primary_provider.get_candles_called, 1)
        self.assertEqual(self.secondary_provider.get_candles_called, 1)
        self.assertEqual(self.tertiary_provider.get_candles_called, 0)
    
    async def test_get_candles_all_providers_fail(self):
        """Test behavior when all providers fail."""
        await self.async_setup()
        
        # Make all providers fail
        self.primary_provider.set_exception("get_candles", Exception("Primary error"))
        self.secondary_provider.set_exception("get_candles", Exception("Secondary error"))
        self.tertiary_provider.set_exception("get_candles", Exception("Tertiary error"))
        
        # Get candles and expect an exception
        with self.assertRaises(Exception) as context:
            await self.facade.get_candles(symbol="BTC/USDT", timeframe="1h", limit=1)
        
        # Check that all providers were called
        self.assertEqual(self.primary_provider.get_candles_called, 1)
        self.assertEqual(self.secondary_provider.get_candles_called, 1)
        self.assertEqual(self.tertiary_provider.get_candles_called, 1)
        
        # Check error message indicates all providers failed
        self.assertIn("All providers failed", str(context.exception))
    
    async def test_get_ticker_primary_provider(self):
        """Test getting ticker from primary provider."""
        await self.async_setup()
        
        # Set up mock data
        mock_ticker = {
            "symbol": "BTC/USDT",
            "last": 29050.0,
            "bid": 29000.0,
            "ask": 29100.0,
            "source": "primary"
        }
        self.primary_provider.set_result("get_ticker", mock_ticker)
        
        # Get ticker
        result = await self.facade.get_ticker(symbol="BTC/USDT")
        
        # Check that we got the expected result
        self.assertEqual(result, mock_ticker)
        
        # Check that only primary provider was called
        self.assertEqual(self.primary_provider.get_ticker_called, 1)
        self.assertEqual(self.secondary_provider.get_ticker_called, 0)
        self.assertEqual(self.tertiary_provider.get_ticker_called, 0)
    
    async def test_get_orderbook_fallback(self):
        """Test fallback for orderbook data."""
        await self.async_setup()
        
        # Make primary provider fail
        self.primary_provider.set_exception("get_orderbook", Exception("Test error"))
        
        # Set up mock data for secondary provider
        mock_orderbook = {
            "symbol": "BTC/USDT",
            "bids": [[29000.0, 1.0], [28900.0, 2.0]],
            "asks": [[29100.0, 1.0], [29200.0, 2.0]],
            "source": "secondary"
        }
        self.secondary_provider.set_result("get_orderbook", mock_orderbook)
        
        # Get orderbook
        result = await self.facade.get_orderbook(symbol="BTC/USDT", limit=2)
        
        # Check that we got the expected result from secondary
        self.assertEqual(result, mock_orderbook)
        
        # Check that both providers were called
        self.assertEqual(self.primary_provider.get_orderbook_called, 1)
        self.assertEqual(self.secondary_provider.get_orderbook_called, 1)
        self.assertEqual(self.tertiary_provider.get_orderbook_called, 0)
    
    async def test_get_trades_fallback(self):
        """Test fallback for trades data."""
        await self.async_setup()
        
        # Make primary and secondary providers fail
        self.primary_provider.set_exception("get_trades", Exception("Primary error"))
        self.secondary_provider.set_exception("get_trades", Exception("Secondary error"))
        
        # Set up mock data for tertiary provider
        mock_trades = {
            "symbol": "BTC/USDT",
            "trades": [
                {
                    "id": 1,
                    "timestamp": 1609459200000,
                    "price": 29050.0,
                    "amount": 1.0,
                    "side": "buy"
                }
            ],
            "source": "tertiary"
        }
        self.tertiary_provider.set_result("get_trades", mock_trades)
        
        # Get trades
        result = await self.facade.get_trades(symbol="BTC/USDT", limit=1)
        
        # Check that we got the expected result from tertiary
        self.assertEqual(result, mock_trades)
        
        # Check that all providers were called
        self.assertEqual(self.primary_provider.get_trades_called, 1)
        self.assertEqual(self.secondary_provider.get_trades_called, 1)
        self.assertEqual(self.tertiary_provider.get_trades_called, 1)
    
    async def test_specific_provider_selection(self):
        """Test selecting a specific provider by name."""
        await self.async_setup()
        
        # Set up mock data for each provider
        self.primary_provider.set_result("get_ticker", {"source": "primary"})
        self.secondary_provider.set_result("get_ticker", {"source": "secondary"})
        self.tertiary_provider.set_result("get_ticker", {"source": "tertiary"})
        
        # Get ticker from specific provider
        result = await self.facade.get_ticker(symbol="BTC/USDT", provider="secondary")
        
        # Check that we got the expected result
        self.assertEqual(result["source"], "secondary")
        
        # Check that only the specified provider was called
        self.assertEqual(self.primary_provider.get_ticker_called, 0)
        self.assertEqual(self.secondary_provider.get_ticker_called, 1)
        self.assertEqual(self.tertiary_provider.get_ticker_called, 0)
    
    async def test_provider_status_check(self):
        """Test skipping providers that are not initialized."""
        await self.async_setup()
        
        # Set primary provider to error state
        self.primary_provider._status = ComponentStatus.ERROR
        
        # Set up mock data for secondary provider
        mock_ticker = {"source": "secondary"}
        self.secondary_provider.set_result("get_ticker", mock_ticker)
        
        # Get ticker
        result = await self.facade.get_ticker(symbol="BTC/USDT")
        
        # Check that we got the result from secondary
        self.assertEqual(result["source"], "secondary")
        
        # Check that primary provider was skipped
        self.assertEqual(self.primary_provider.get_ticker_called, 0)
        self.assertEqual(self.secondary_provider.get_ticker_called, 1)
    
    async def test_data_validation(self):
        """Test validation of data from providers."""
        await self.async_setup()
        
        # Enable validation
        self.facade._validate_data = True
        
        # Set up mock data with invalid values
        invalid_candles = [
            {
                "symbol": "BTC/USDT",
                "timeframe": "1h",
                "time": 1609459200000,
                "open": -29000.0,  # Negative price is invalid
                "high": 29100.0,
                "low": 28900.0,
                "close": 29050.0,
                "volume": 100.0,
                "source": "primary"
            }
        ]
        self.primary_provider.set_result("get_candles", invalid_candles)
        
        # Set up valid data for secondary provider
        valid_candles = [
            {
                "symbol": "BTC/USDT",
                "timeframe": "1h",
                "time": 1609459200000,
                "open": 29000.0,
                "high": 29100.0,
                "low": 28900.0,
                "close": 29050.0,
                "volume": 100.0,
                "source": "secondary"
            }
        ]
        self.secondary_provider.set_result("get_candles", valid_candles)
        
        # Get candles
        result = await self.facade.get_candles(symbol="BTC/USDT", timeframe="1h", limit=1)
        
        # Check that we got the valid data from secondary
        self.assertEqual(result, valid_candles)
        
        # Check that both providers were called
        self.assertEqual(self.primary_provider.get_candles_called, 1)
        self.assertEqual(self.secondary_provider.get_candles_called, 1)
    
    async def test_cross_validation(self):
        """Test cross-validation of data from multiple providers."""
        await self.async_setup()
        
        # Enable cross-validation
        self.facade._cross_validate = True
        self.facade._validation_threshold = 0.1  # 10% tolerance
        
        # Set up slightly different data for each provider
        primary_ticker = {
            "symbol": "BTC/USDT",
            "last": 29050.0,
            "bid": 29000.0,
            "ask": 29100.0,
            "source": "primary"
        }
        
        secondary_ticker = {
            "symbol": "BTC/USDT",
            "last": 29150.0,  # 0.34% higher
            "bid": 29100.0,
            "ask": 29200.0,
            "source": "secondary"
        }
        
        tertiary_ticker = {
            "symbol": "BTC/USDT",
            "last": 29550.0,  # 1.72% higher than primary, exceeds threshold
            "bid": 29500.0,
            "ask": 29600.0,
            "source": "tertiary"
        }
        
        self.primary_provider.set_result("get_ticker", primary_ticker)
        self.secondary_provider.set_result("get_ticker", secondary_ticker)
        self.tertiary_provider.set_result("get_ticker", tertiary_ticker)
        
        # Get ticker with cross-validation
        result = await self.facade.get_ticker(symbol="BTC/USDT", cross_validate=True)
        
        # Should average the primary and secondary results (tertiary exceeds threshold)
        expected_last = (29050.0 + 29150.0) / 2
        self.assertAlmostEqual(result["last"], expected_last)
        
        # Check that all providers were called
        self.assertEqual(self.primary_provider.get_ticker_called, 1)
        self.assertEqual(self.secondary_provider.get_ticker_called, 1)
        self.assertEqual(self.tertiary_provider.get_ticker_called, 1)
    
    async def test_caching(self):
        """Test that data is cached."""
        await self.async_setup()
        
        # Enable caching with a short TTL
        self.facade._use_cache = True
        self.facade._cache_ttl = 0.5  # 500ms
        
        # Get ticker first time
        await self.facade.get_ticker(symbol="BTC/USDT")
        
        # Get ticker again immediately
        await self.facade.get_ticker(symbol="BTC/USDT")
        
        # Primary provider should only be called once
        self.assertEqual(self.primary_provider.get_ticker_called, 1)
        
        # Wait for cache to expire
        await asyncio.sleep(0.6)
        
        # Get ticker again after cache expiry
        await self.facade.get_ticker(symbol="BTC/USDT")
        
        # Primary provider should be called again
        self.assertEqual(self.primary_provider.get_ticker_called, 2)
    
    async def test_round_robin_selection(self):
        """Test round-robin provider selection."""
        await self.async_setup()
        
        # Set selection strategy to round-robin
        self.facade._selection_strategy = "round_robin"
        
        # Set up mock data for each provider
        self.primary_provider.set_result("get_ticker", {"source": "primary"})
        self.secondary_provider.set_result("get_ticker", {"source": "secondary"})
        self.tertiary_provider.set_result("get_ticker", {"source": "tertiary"})
        
        # First call should use first provider
        result1 = await self.facade.get_ticker(symbol="BTC/USDT")
        self.assertEqual(result1["source"], "primary")
        
        # Second call should use second provider
        result2 = await self.facade.get_ticker(symbol="BTC/USDT")
        self.assertEqual(result2["source"], "secondary")
        
        # Third call should use third provider
        result3 = await self.facade.get_ticker(symbol="BTC/USDT")
        self.assertEqual(result3["source"], "tertiary")
        
        # Fourth call should cycle back to first provider
        result4 = await self.facade.get_ticker(symbol="BTC/USDT")
        self.assertEqual(result4["source"], "primary")
    
    async def test_multiple_providers_same_priority(self):
        """Test handling of multiple providers with the same priority."""
        # Create facade
        facade = MarketDataFacade(
            message_bus=self.message_bus,
            data_unifier=self.data_unifier
        )
        
        # Create providers with same priority
        provider1 = MockDataProvider(name="provider1", priority=5)
        provider2 = MockDataProvider(name="provider2", priority=5)
        
        # Register providers
        facade.register_provider(provider1)
        facade.register_provider(provider2)
        
        # Initialize facade
        await facade.initialize()
        
        # Set up mock data
        provider1.set_result("get_ticker", {"source": "provider1"})
        provider2.set_result("get_ticker", {"source": "provider2"})
        
        # Get ticker - first provider with same priority should be used
        result = await facade.get_ticker(symbol="BTC/USDT")
        
        # Since order is not deterministic, check that one of them was used
        self.assertIn(result["source"], ["provider1", "provider2"])
        
        # Only one provider should be called
        self.assertEqual(provider1.get_ticker_called + provider2.get_ticker_called, 1)
    
    async def test_provider_timeout(self):
        """Test handling of provider timeout."""
        await self.async_setup()
        
        # Set a short timeout
        self.facade._provider_timeout = 0.1  # 100ms
        
        # Make primary provider hang
        async def slow_response():
            await asyncio.sleep(0.5)  # Longer than timeout
            return {"source": "primary"}
        
        self.primary_provider.get_ticker = AsyncMock(side_effect=slow_response)
        
        # Set up secondary provider with quick response
        self.secondary_provider.set_result("get_ticker", {"source": "secondary"})
        
        # Get ticker
        result = await self.facade.get_ticker(symbol="BTC/USDT")
        
        # Should get result from secondary
        self.assertEqual(result["source"], "secondary")


# Integration test
@unittest.skip("Skipping integration test")
class TestMarketDataFacadeIntegration(unittest.TestCase):
    """Integration tests for MarketDataFacade with real providers."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create message bus
        self.message_bus = MessageBus()
        
        # Create data unifier
        self.data_unifier = DataUnifier()
        
        # Create facade
        self.facade = MarketDataFacade(
            message_bus=self.message_bus,
            data_unifier=self.data_unifier
        )
    
    async def setup_real_providers(self):
        """Set up real market data providers."""
        # This would be implemented with actual data providers
        pass
    
    async def test_real_provider_redundancy(self):
        """Test redundancy between real providers."""
        # This would test actual data providers
        pass


# Run the tests
if __name__ == '__main__':
    # Create test loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # Run the tests
    try:
        # Create test suite
        suite = unittest.TestSuite()
        
        # Add TestMarketDataFacade tests with async setup
        for method_name in dir(TestMarketDataFacade):
            if method_name.startswith('test_'):
                # Create test case
                test_case = TestMarketDataFacade(method_name)
                # Add async setup and teardown
                setattr(test_case, 'setUp', lambda: None)  # No-op synchronous setup
                setattr(test_case, '_setUp', test_case.setUp)  # Save original setUp
                setattr(test_case, '_async_setup_done', False)
                
                # Wrap test method to run async setup first
                original_method = getattr(test_case, method_name)
                
                async def run_test_with_async_setup(self, original_method=original_method):
                    if not self._async_setup_done:
                        await self.async_setup()
                        self._async_setup_done = True
                    try:
                        await original_method()
                    finally:
                        await self.teardown()
                
                setattr(test_case, method_name, run_test_with_async_setup.__get__(test_case))
                
                # Add to suite
                suite.addTest(test_case)
        
        # Run the suite
        result = unittest.TextTestRunner().run(suite)
        
        # Check results
        if not result.wasSuccessful():
            sys.exit(1)
    finally:
        # Close the loop
        loop.close() 