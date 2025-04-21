import os
import sys
import unittest
import asyncio
import logging
import time
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from trading_system.market_data.market_data_subsystem import MarketDataSubsystem
from trading_system.market_data.market_data_facade import MarketDataFacade
from trading_system.market_data.data_unifier import DataUnifier
from trading_system.market_data.timeseries_db import TimeSeriesDB, get_timeseries_db
from trading_system.market_data.tradingview_validator import TradingViewValidator
from trading_system.market_data.tradingview_provider import TradingViewProvider
from trading_system.core.message_bus import MessageBus, MessageType
from trading_system.core.component import ComponentStatus

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MockWebSocketClient:
    """Mock WebSocket client for testing."""
    
    def __init__(self, exchange="nobitex"):
        self.exchange = exchange
        self.connected = False
        self.subscriptions = []
        self.message_callback = None
        self.messages = []
        self.connection_attempts = 0
    
    async def connect(self):
        """Connect to the WebSocket server."""
        self.connected = True
        self.connection_attempts += 1
        return True
    
    async def disconnect(self):
        """Disconnect from the WebSocket server."""
        self.connected = False
        self.subscriptions = []
        return True
    
    async def subscribe(self, channel, symbol=None):
        """Subscribe to a channel."""
        if not self.connected:
            await self.connect()
        
        subscription = {"channel": channel}
        if symbol:
            subscription["symbol"] = symbol
        
        self.subscriptions.append(subscription)
        return True
    
    async def unsubscribe(self, channel, symbol=None):
        """Unsubscribe from a channel."""
        subscription = {"channel": channel}
        if symbol:
            subscription["symbol"] = symbol
        
        if subscription in self.subscriptions:
            self.subscriptions.remove(subscription)
        
        return True
    
    def set_message_callback(self, callback):
        """Set the callback for received messages."""
        self.message_callback = callback
    
    async def simulate_message(self, message):
        """Simulate receiving a message."""
        self.messages.append(message)
        if self.message_callback:
            await self.message_callback(message)


class MockTimeSeriesDB:
    """Mock TimeSeriesDB for testing."""
    
    def __init__(self):
        self.candles = {}
        self.signals = []
        self.status = ComponentStatus.INITIALIZED
    
    async def initialize(self):
        """Initialize the database."""
        self.status = ComponentStatus.INITIALIZED
        return True
    
    async def stop(self):
        """Stop the database."""
        return True
    
    async def store_market_data(self, symbol, timeframe, candle):
        """Store market data."""
        key = f"{symbol}_{timeframe}"
        if key not in self.candles:
            self.candles[key] = []
        
        self.candles[key].append(candle)
        return True
    
    async def get_market_data(self, symbol, timeframe, start_time=None, end_time=None):
        """Get market data."""
        key = f"{symbol}_{timeframe}"
        if key not in self.candles:
            return []
        
        # Filter by time if start_time and end_time are provided
        if start_time and end_time:
            if isinstance(start_time, str):
                # Convert relative time (-1h, -1d) to timestamp
                if start_time.startswith("-"):
                    hours = 1
                    if "h" in start_time:
                        hours = int(start_time[1:-1])
                    elif "d" in start_time:
                        hours = int(start_time[1:-1]) * 24
                    
                    start_timestamp = int((datetime.now() - timedelta(hours=hours)).timestamp() * 1000)
                else:
                    # Assume ISO format
                    start_timestamp = int(datetime.fromisoformat(start_time.replace("Z", "+00:00")).timestamp() * 1000)
            else:
                start_timestamp = start_time
            
            if end_time == "now()":
                end_timestamp = int(datetime.now().timestamp() * 1000)
            elif isinstance(end_time, str):
                # Assume ISO format
                end_timestamp = int(datetime.fromisoformat(end_time.replace("Z", "+00:00")).timestamp() * 1000)
            else:
                end_timestamp = end_time
            
            return [
                candle for candle in self.candles[key]
                if candle["time"] >= start_timestamp and candle["time"] <= end_timestamp
            ]
        
        return self.candles[key]
    
    async def store_signal(self, signal):
        """Store signal data."""
        self.signals.append(signal)
        return True
    
    async def get_signals(self, symbol=None, signal_type=None, start_time=None, end_time=None):
        """Get signals."""
        filtered_signals = self.signals
        
        if symbol:
            filtered_signals = [s for s in filtered_signals if s["symbol"] == symbol]
        
        if signal_type:
            filtered_signals = [s for s in filtered_signals if s.get("type") == signal_type]
        
        # Filter by time if start_time and end_time are provided
        if start_time and end_time:
            if isinstance(start_time, str):
                if start_time.startswith("-"):
                    hours = 1
                    if "h" in start_time:
                        hours = int(start_time[1:-1])
                    elif "d" in start_time:
                        hours = int(start_time[1:-1]) * 24
                    
                    start_timestamp = int((datetime.now() - timedelta(hours=hours)).timestamp() * 1000)
                else:
                    start_timestamp = int(datetime.fromisoformat(start_time.replace("Z", "+00:00")).timestamp() * 1000)
            else:
                start_timestamp = start_time
            
            if end_time == "now()":
                end_timestamp = int(datetime.now().timestamp() * 1000)
            elif isinstance(end_time, str):
                end_timestamp = int(datetime.fromisoformat(end_time.replace("Z", "+00:00")).timestamp() * 1000)
            else:
                end_timestamp = end_time
            
            filtered_signals = [
                signal for signal in filtered_signals
                if signal["timestamp"] >= start_timestamp and signal["timestamp"] <= end_timestamp
            ]
        
        return filtered_signals


class MockProvider:
    """Mock market data provider for testing."""
    
    def __init__(self, name, priority=1):
        self.name = name
        self.priority = priority
        self.status = ComponentStatus.INITIALIZED
        self.candles = {}
        self.tickers = {}
        self.orderbooks = {}
        self.trades = {}
        self.symbols = ["BTC/USDT", "ETH/USDT", "XRP/USDT"]
    
    async def initialize(self):
        """Initialize the provider."""
        self.status = ComponentStatus.INITIALIZED
        return True
    
    async def stop(self):
        """Stop the provider."""
        return True
    
    def set_candles(self, symbol, timeframe, candles):
        """Set candle data for testing."""
        key = f"{symbol}_{timeframe}"
        self.candles[key] = candles
    
    def set_ticker(self, symbol, ticker):
        """Set ticker data for testing."""
        self.tickers[symbol] = ticker
    
    def set_orderbook(self, symbol, orderbook):
        """Set orderbook data for testing."""
        self.orderbooks[symbol] = orderbook
    
    def set_trades(self, symbol, trades):
        """Set trades data for testing."""
        self.trades[symbol] = trades
    
    async def get_candles(self, symbol, timeframe, limit=100, since=None):
        """Get candle data."""
        key = f"{symbol}_{timeframe}"
        if key not in self.candles:
            # Generate mock candles
            timestamp = int(datetime.now().timestamp() * 1000)
            return [
                {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "time": timestamp - (i * 60000),
                    "open": 50000.0 - i,
                    "high": 51000.0 - i,
                    "low": 49000.0 - i,
                    "close": 50500.0 - i,
                    "volume": 10.5 + i,
                    "source": self.name
                }
                for i in range(limit)
            ]
        
        return self.candles[key][:limit]
    
    async def get_ticker(self, symbol):
        """Get ticker data."""
        if symbol not in self.tickers:
            # Generate mock ticker
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
        
        return self.tickers[symbol]
    
    async def get_orderbook(self, symbol, limit=10):
        """Get orderbook data."""
        if symbol not in self.orderbooks:
            # Generate mock orderbook
            return {
                "symbol": symbol,
                "bids": [[49900.0 - (i * 100), 1.0 + i * 0.1] for i in range(limit)],
                "asks": [[50100.0 + (i * 100), 1.0 + i * 0.1] for i in range(limit)],
                "source": self.name
            }
        
        return self.orderbooks[symbol]
    
    async def get_trades(self, symbol, limit=50, since=None):
        """Get recent trades."""
        if symbol not in self.trades:
            # Generate mock trades
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
        
        return self.trades[symbol]
    
    async def get_symbols(self):
        """Get available symbols."""
        return self.symbols


class TestMarketDataSubsystem(unittest.TestCase):
    """Integration tests for the MarketDataSubsystem."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create message bus
        self.message_bus = MessageBus()
        
        # Create data unifier
        self.data_unifier = DataUnifier()
        
        # Create mock DB
        self.mock_db = MockTimeSeriesDB()
        
        # Patch the get_timeseries_db function to return our mock
        self.patcher = patch('trading_system.market_data.timeseries_db.get_timeseries_db', 
                            return_value=self.mock_db)
        self.mock_get_db = self.patcher.start()
        
        # Create providers
        self.primary_provider = MockProvider(name="primary", priority=10)
        self.secondary_provider = MockProvider(name="secondary", priority=5)
        
        # Create WebSocket clients
        self.nobitex_ws_client = MockWebSocketClient(exchange="nobitex")
        
        # Create component references for the subsystem
        self.components = {
            "data_unifier": self.data_unifier,
            "message_bus": self.message_bus,
            "websocket_clients": {
                "nobitex": self.nobitex_ws_client
            }
        }
        
        # Create subsystem config
        self.config = {
            "timeseries_db": {
                "url": "http://localhost:8086",
                "token": "test-token",
                "org": "test-org",
                "bucket": "test-bucket"
            },
            "tradingview": {
                "validation_rules": {
                    "price_threshold": 0.5,
                    "volume_threshold": 0.3,
                    "time_window": 300,  # 5 minutes
                    "duplicate_window": 3600  # 1 hour
                }
            },
            "symbols": ["BTC/USDT", "ETH/USDT", "XRP/USDT"],
            "provider_timeout": 5.0,
            "cross_validate": True,
            "validation_threshold": 0.1,
            "use_cache": True,
            "cache_ttl": 60.0,
            "selection_strategy": "priority"
        }
        
        # Create subsystem
        self.subsystem = MarketDataSubsystem(config=self.config, components=self.components)
        
        # Add providers
        self.subsystem.register_provider(self.primary_provider)
        self.subsystem.register_provider(self.secondary_provider)
    
    def tearDown(self):
        """Clean up after tests."""
        self.patcher.stop()
    
    async def async_setup(self):
        """Set up asynchronous components."""
        # Initialize the subsystem
        result = await self.subsystem.initialize()
        self.assertTrue(result, "Subsystem initialization failed")
    
    async def async_teardown(self):
        """Clean up asynchronous components."""
        await self.subsystem.stop()
    
    async def test_subsystem_initialization(self):
        """Test subsystem initialization."""
        # Initialize
        result = await self.subsystem.initialize()
        
        # Check result
        self.assertTrue(result, "Subsystem initialization failed")
        
        # Check that components are initialized
        self.assertEqual(self.subsystem.status, ComponentStatus.INITIALIZED)
        self.assertIsNotNone(self.subsystem.data_facade)
        self.assertIsNotNone(self.subsystem.tradingview_provider)
        self.assertIsNotNone(self.subsystem.tradingview_validator)
    
    async def test_get_market_data(self):
        """Test getting market data through the subsystem."""
        await self.async_setup()
        
        # Set up mock data
        timestamp = int(datetime.now().timestamp() * 1000)
        mock_candles = [
            {
                "symbol": "BTC/USDT",
                "timeframe": "1h",
                "time": timestamp - 3600000,  # 1 hour ago
                "open": 50000.0,
                "high": 51000.0,
                "low": 49000.0,
                "close": 50500.0,
                "volume": 10.5,
                "source": "primary"
            },
            {
                "symbol": "BTC/USDT",
                "timeframe": "1h",
                "time": timestamp - 7200000,  # 2 hours ago
                "open": 49500.0,
                "high": 50500.0,
                "low": 49000.0,
                "close": 50000.0,
                "volume": 12.3,
                "source": "primary"
            }
        ]
        
        self.primary_provider.set_candles("BTC/USDT", "1h", mock_candles)
        
        # Get candles
        candles = await self.subsystem.get_candles(symbol="BTC/USDT", timeframe="1h", limit=10)
        
        # Check result
        self.assertEqual(len(candles), 2)
        self.assertEqual(candles[0]["symbol"], "BTC/USDT")
        self.assertEqual(candles[0]["timeframe"], "1h")
        self.assertEqual(candles[0]["open"], 50000.0)
        self.assertEqual(candles[0]["source"], "primary")
        
        # Check that data was stored in the database
        db_candles = await self.mock_db.get_market_data(symbol="BTC/USDT", timeframe="1h")
        self.assertEqual(len(db_candles), 2)
    
    async def test_get_ticker(self):
        """Test getting ticker data through the subsystem."""
        await self.async_setup()
        
        # Set up mock data
        mock_ticker = {
            "symbol": "BTC/USDT",
            "last": 50000.0,
            "bid": 49900.0,
            "ask": 50100.0,
            "high": 51000.0,
            "low": 49000.0,
            "volume": 100.5,
            "source": "primary"
        }
        
        self.primary_provider.set_ticker("BTC/USDT", mock_ticker)
        
        # Get ticker
        ticker = await self.subsystem.get_ticker(symbol="BTC/USDT")
        
        # Check result
        self.assertEqual(ticker["symbol"], "BTC/USDT")
        self.assertEqual(ticker["last"], 50000.0)
        self.assertEqual(ticker["bid"], 49900.0)
        self.assertEqual(ticker["ask"], 50100.0)
        self.assertEqual(ticker["source"], "primary")
    
    async def test_get_orderbook(self):
        """Test getting orderbook data through the subsystem."""
        await self.async_setup()
        
        # Set up mock data
        mock_orderbook = {
            "symbol": "BTC/USDT",
            "bids": [
                [49900.0, 1.0],
                [49800.0, 2.0]
            ],
            "asks": [
                [50100.0, 1.5],
                [50200.0, 2.5]
            ],
            "source": "primary"
        }
        
        self.primary_provider.set_orderbook("BTC/USDT", mock_orderbook)
        
        # Get orderbook
        orderbook = await self.subsystem.get_orderbook(symbol="BTC/USDT", limit=10)
        
        # Check result
        self.assertEqual(orderbook["symbol"], "BTC/USDT")
        self.assertEqual(len(orderbook["bids"]), 2)
        self.assertEqual(len(orderbook["asks"]), 2)
        self.assertEqual(orderbook["bids"][0][0], 49900.0)
        self.assertEqual(orderbook["asks"][0][0], 50100.0)
        self.assertEqual(orderbook["source"], "primary")
    
    async def test_tradingview_signal_processing(self):
        """Test processing TradingView signals."""
        await self.async_setup()
        
        # Set up mock ticker data for cross-referencing
        mock_ticker = {
            "symbol": "BTC/USDT",
            "last": 50000.0,
            "bid": 49900.0,
            "ask": 50100.0,
            "source": "primary"
        }
        
        self.primary_provider.set_ticker("BTC/USDT", mock_ticker)
        
        # Create a valid signal
        timestamp = int(datetime.now().timestamp() * 1000)
        signal = {
            "exchange": "BINANCE",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "price": "50000",
            "strategy": "MACD Crossover",
            "timestamp": timestamp,
            "interval": "1h"
        }
        
        # Process signal
        msg_id = "test-signal-1"
        await self.subsystem.process_tradingview_signal(signal, msg_id=msg_id)
        
        # Check that signal was validated and stored
        signals = await self.mock_db.get_signals(symbol="BTC/USDT")
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0]["action"], "buy")
        self.assertEqual(signals[0]["price"], 50000.0)
        self.assertEqual(signals[0]["strategy"], "MACD Crossover")
        
        # Process duplicate signal
        await self.subsystem.process_tradingview_signal(signal, msg_id="test-signal-2")
        
        # Check that duplicate was rejected
        signals = await self.mock_db.get_signals(symbol="BTC/USDT")
        self.assertEqual(len(signals), 1)  # Still only one signal
        
        # Process invalid signal (price too far from market)
        invalid_signal = signal.copy()
        invalid_signal["price"] = "60000"  # 20% higher than market
        invalid_signal["timestamp"] = timestamp + 1000
        
        await self.subsystem.process_tradingview_signal(invalid_signal, msg_id="test-signal-3")
        
        # Check that invalid signal was rejected
        signals = await self.mock_db.get_signals(symbol="BTC/USDT")
        self.assertEqual(len(signals), 1)  # Still only one signal
    
    async def test_websocket_subscription(self):
        """Test WebSocket subscription through the subsystem."""
        await self.async_setup()
        
        # Subscribe to market data
        result = await self.subsystem.subscribe_market_data(
            exchange="nobitex",
            symbol="BTC/USDT",
            channels=["ticker", "orderbook"]
        )
        
        # Check result
        self.assertTrue(result)
        
        # Check subscriptions
        self.assertEqual(len(self.nobitex_ws_client.subscriptions), 2)
        self.assertIn({"channel": "ticker", "symbol": "BTC/USDT"}, self.nobitex_ws_client.subscriptions)
        self.assertIn({"channel": "orderbook", "symbol": "BTC/USDT"}, self.nobitex_ws_client.subscriptions)
        
        # Simulate ticker message
        ticker_msg = {
            "channel": "ticker",
            "symbol": "BTC/USDT",
            "data": {
                "last": "50000",
                "bid": "49900",
                "ask": "50100",
                "high": "51000",
                "low": "49000",
                "volume": "100.5"
            }
        }
        
        await self.nobitex_ws_client.simulate_message(ticker_msg)
        
        # Wait for message processing
        await asyncio.sleep(0.1)
        
        # Unsubscribe
        result = await self.subsystem.unsubscribe_market_data(
            exchange="nobitex",
            symbol="BTC/USDT",
            channels=["ticker"]
        )
        
        # Check result
        self.assertTrue(result)
        
        # Check subscription was removed
        self.assertEqual(len(self.nobitex_ws_client.subscriptions), 1)
        self.assertNotIn({"channel": "ticker", "symbol": "BTC/USDT"}, self.nobitex_ws_client.subscriptions)
        self.assertIn({"channel": "orderbook", "symbol": "BTC/USDT"}, self.nobitex_ws_client.subscriptions)
    
    async def test_provider_fallback(self):
        """Test fallback between providers."""
        await self.async_setup()
        
        # Set up different data for each provider
        primary_ticker = {
            "symbol": "BTC/USDT",
            "last": 50000.0,
            "bid": 49900.0,
            "ask": 50100.0,
            "source": "primary"
        }
        
        secondary_ticker = {
            "symbol": "BTC/USDT",
            "last": 50100.0,
            "bid": 50000.0,
            "ask": 50200.0,
            "source": "secondary"
        }
        
        self.primary_provider.set_ticker("BTC/USDT", primary_ticker)
        self.secondary_provider.set_ticker("BTC/USDT", secondary_ticker)
        
        # Get ticker with primary provider
        ticker = await self.subsystem.get_ticker(symbol="BTC/USDT")
        
        # Check result from primary
        self.assertEqual(ticker["source"], "primary")
        
        # Set primary provider to error status
        self.primary_provider.status = ComponentStatus.ERROR
        
        # Get ticker again, should fallback to secondary
        ticker = await self.subsystem.get_ticker(symbol="BTC/USDT")
        
        # Check result from secondary
        self.assertEqual(ticker["source"], "secondary")
    
    async def test_historical_data_query(self):
        """Test querying historical data from the database."""
        await self.async_setup()
        
        # Set up mock data
        timestamp = int(datetime.now().timestamp() * 1000)
        mock_candles = [
            {
                "symbol": "BTC/USDT",
                "timeframe": "1h",
                "time": timestamp - 3600000,  # 1 hour ago
                "open": 50000.0,
                "high": 51000.0,
                "low": 49000.0,
                "close": 50500.0,
                "volume": 10.5,
                "source": "primary"
            },
            {
                "symbol": "BTC/USDT",
                "timeframe": "1h",
                "time": timestamp - 7200000,  # 2 hours ago
                "open": 49500.0,
                "high": 50500.0,
                "low": 49000.0,
                "close": 50000.0,
                "volume": 12.3,
                "source": "primary"
            }
        ]
        
        # Store candles in the database
        for candle in mock_candles:
            await self.mock_db.store_market_data(
                symbol="BTC/USDT",
                timeframe="1h",
                candle=candle
            )
        
        # Query historical data
        candles = await self.subsystem.get_historical_data(
            symbol="BTC/USDT",
            timeframe="1h",
            start_time="-3h",
            end_time="now()"
        )
        
        # Check result
        self.assertEqual(len(candles), 2)
        self.assertEqual(candles[0]["symbol"], "BTC/USDT")
        self.assertEqual(candles[0]["timeframe"], "1h")
        
        # Query with specific time range
        candles = await self.subsystem.get_historical_data(
            symbol="BTC/USDT",
            timeframe="1h",
            start_time="-1.5h",  # Should only include the 1-hour-ago candle
            end_time="now()"
        )
        
        # Check result
        self.assertEqual(len(candles), 1)
        self.assertEqual(candles[0]["time"], timestamp - 3600000)
    
    async def test_signal_history_query(self):
        """Test querying signal history from the database."""
        await self.async_setup()
        
        # Set up mock signals
        timestamp = int(datetime.now().timestamp() * 1000)
        signals = [
            {
                "symbol": "BTC/USDT",
                "action": "buy",
                "price": 50000.0,
                "strategy": "MACD Crossover",
                "timestamp": timestamp - 3600000,  # 1 hour ago
                "confidence": 0.95,
                "source": "tradingview"
            },
            {
                "symbol": "BTC/USDT",
                "action": "sell",
                "price": 51000.0,
                "strategy": "MACD Crossover",
                "timestamp": timestamp - 7200000,  # 2 hours ago
                "confidence": 0.92,
                "source": "tradingview"
            },
            {
                "symbol": "ETH/USDT",
                "action": "buy",
                "price": 3000.0,
                "strategy": "RSI Oversold",
                "timestamp": timestamp - 3600000,  # 1 hour ago
                "confidence": 0.88,
                "source": "tradingview"
            }
        ]
        
        # Store signals in the database
        for signal in signals:
            await self.mock_db.store_signal(signal)
        
        # Query signal history for BTC/USDT
        btc_signals = await self.subsystem.get_signal_history(
            symbol="BTC/USDT",
            start_time="-3h",
            end_time="now()"
        )
        
        # Check result
        self.assertEqual(len(btc_signals), 2)
        self.assertEqual(btc_signals[0]["symbol"], "BTC/USDT")
        self.assertEqual(btc_signals[0]["action"], "buy")
        self.assertEqual(btc_signals[1]["action"], "sell")
        
        # Query signal history for specific strategy
        macd_signals = await self.subsystem.get_signal_history(
            strategy="MACD Crossover",
            start_time="-3h",
            end_time="now()"
        )
        
        # Check result
        self.assertEqual(len(macd_signals), 2)
        
        # Query signal history for specific action
        buy_signals = await self.subsystem.get_signal_history(
            action="buy",
            start_time="-3h",
            end_time="now()"
        )
        
        # Check result
        self.assertEqual(len(buy_signals), 2)
        self.assertEqual(buy_signals[0]["action"], "buy")
        self.assertEqual(buy_signals[1]["action"], "buy")
    
    async def test_cross_validation_with_multiple_providers(self):
        """Test cross-validation of data from multiple providers."""
        await self.async_setup()
        
        # Enable cross-validation
        self.subsystem.data_facade._cross_validate = True
        self.subsystem.data_facade._validation_threshold = 0.1  # 10% tolerance
        
        # Set up slightly different data for each provider
        primary_ticker = {
            "symbol": "BTC/USDT",
            "last": 50000.0,
            "bid": 49900.0,
            "ask": 50100.0,
            "source": "primary"
        }
        
        secondary_ticker = {
            "symbol": "BTC/USDT",
            "last": 50200.0,  # 0.4% higher
            "bid": 50100.0,
            "ask": 50300.0,
            "source": "secondary"
        }
        
        self.primary_provider.set_ticker("BTC/USDT", primary_ticker)
        self.secondary_provider.set_ticker("BTC/USDT", secondary_ticker)
        
        # Get ticker with cross-validation
        ticker = await self.subsystem.get_ticker(symbol="BTC/USDT", cross_validate=True)
        
        # Should average the results
        expected_last = (50000.0 + 50200.0) / 2
        self.assertAlmostEqual(ticker["last"], expected_last)
        
        # Set secondary provider data to exceed threshold
        secondary_ticker["last"] = 55000.0  # 10% higher, exceeds threshold
        self.secondary_provider.set_ticker("BTC/USDT", secondary_ticker)
        
        # Get ticker with cross-validation, should ignore secondary
        ticker = await self.subsystem.get_ticker(symbol="BTC/USDT", cross_validate=True)
        
        # Should use primary only
        self.assertEqual(ticker["last"], 50000.0)
    
    async def test_websocket_reconnection(self):
        """Test WebSocket reconnection on disconnection."""
        await self.async_setup()
        
        # Subscribe to market data
        await self.subsystem.subscribe_market_data(
            exchange="nobitex",
            symbol="BTC/USDT",
            channels=["ticker"]
        )
        
        # Check initial connection
        self.assertTrue(self.nobitex_ws_client.connected)
        self.assertEqual(self.nobitex_ws_client.connection_attempts, 1)
        
        # Simulate disconnect
        self.nobitex_ws_client.connected = False
        
        # Simulate reconnection check (this would normally be triggered by a timer)
        await self.subsystem.websocket_clients["nobitex"]._check_connection()
        
        # Check reconnection
        self.assertTrue(self.nobitex_ws_client.connected)
        self.assertEqual(self.nobitex_ws_client.connection_attempts, 2)
        
        # Check subscriptions were restored
        self.assertEqual(len(self.nobitex_ws_client.subscriptions), 1)
        self.assertIn({"channel": "ticker", "symbol": "BTC/USDT"}, self.nobitex_ws_client.subscriptions)


# Run the tests
if __name__ == '__main__':
    # Create test loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # Create test suite
    suite = unittest.TestSuite()
    
    # Add tests with async setup/teardown
    for method_name in dir(TestMarketDataSubsystem):
        if method_name.startswith('test_'):
            test_case = TestMarketDataSubsystem(method_name)
            
            # Original method
            original_method = getattr(test_case, method_name)
            
            # Create wrapped method
            async def run_test_with_async_setup(self, original_method=original_method):
                try:
                    await self.async_setup()
                    await original_method()
                finally:
                    await self.async_teardown()
            
            # Replace method with wrapped version
            setattr(test_case, method_name, run_test_with_async_setup.__get__(test_case))
            
            # Add to suite
            suite.addTest(test_case)
    
    # Run tests
    result = unittest.TextTestRunner().run(suite)
    
    # Check results
    if not result.wasSuccessful():
        sys.exit(1)
    
    # Close the loop
    loop.close() 