import os
import sys
import json
import time
import asyncio
import unittest
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from trading_system.core.component import ComponentStatus
from trading_system.core.message_bus import MessageBus
from trading_system.market_data.tradingview_validator import (
    TradingViewValidator, ValidationResult, SignalConfidence, ValidationRule
)
from trading_system.market_data.tradingview_provider import (
    TradingViewProvider, TradingViewSignalType
)

class TestTradingViewValidator(unittest.TestCase):
    """Test the TradingView signal validator."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create test configuration
        self.config = {
            "enabled": True,
            "rules": [
                {
                    "name": "price_threshold",
                    "enabled": True,
                    "params": {"min_price": 100.0, "max_price": 100000.0}
                },
                {
                    "name": "time_window",
                    "enabled": True,
                    "params": {"window_seconds": 3600}
                },
                {
                    "name": "duplicate_check",
                    "enabled": True,
                    "params": {"window_seconds": 300}
                }
            ]
        }
        
        # Create validator
        self.validator = TradingViewValidator(self.config)
        
        # Sample valid signal
        self.valid_signal = {
            "symbol": "BTC/USDT",
            "strategy": "MACD Cross",
            "action": "buy",
            "price": 50000.0,
            "timestamp": int(datetime.now().timestamp() * 1000)
        }
        
        # Invalid signal (missing price)
        self.invalid_signal = {
            "symbol": "BTC/USDT",
            "strategy": "MACD Cross",
            "action": "buy"
        }
        
        # Invalid signal (price below threshold)
        self.low_price_signal = {
            "symbol": "BTC/USDT",
            "strategy": "MACD Cross",
            "action": "buy",
            "price": 50.0,
            "timestamp": int(datetime.now().timestamp() * 1000)
        }
        
        # Invalid signal (price above threshold)
        self.high_price_signal = {
            "symbol": "BTC/USDT",
            "strategy": "MACD Cross",
            "action": "buy",
            "price": 200000.0,
            "timestamp": int(datetime.now().timestamp() * 1000)
        }
        
        # Old signal (outside time window)
        old_time = datetime.now() - timedelta(hours=2)
        self.old_signal = {
            "symbol": "BTC/USDT",
            "strategy": "MACD Cross",
            "action": "buy",
            "price": 50000.0,
            "timestamp": int(old_time.timestamp() * 1000)
        }
    
    async def async_setup(self):
        """Asynchronous setup for tests."""
        await self.validator.initialize()
        self.validator._status = ComponentStatus.INITIALIZED
    
    async def test_initialize(self):
        """Test validator initialization."""
        result = await self.validator.initialize()
        self.assertTrue(result)
        self.assertEqual(self.validator._status, ComponentStatus.INITIALIZED)
        
    async def test_validate_valid_signal(self):
        """Test validation of a valid signal."""
        await self.async_setup()
        
        # Validate signal
        result = await self.validator.validate_signal(self.valid_signal)
        
        # Verify result
        self.assertTrue(result.is_valid)
        self.assertEqual(result.confidence, SignalConfidence.HIGH)
        
    async def test_validate_invalid_signal(self):
        """Test validation of an invalid signal."""
        await self.async_setup()
        
        # Validate signal
        result = await self.validator.validate_signal(self.invalid_signal)
        
        # Verify result
        self.assertFalse(result.is_valid)
        self.assertEqual(result.confidence, SignalConfidence.REJECTED)
        
    async def test_validate_low_price_signal(self):
        """Test validation of a signal with price below threshold."""
        await self.async_setup()
        
        # Validate signal
        result = await self.validator.validate_signal(self.low_price_signal)
        
        # Verify result
        self.assertFalse(result.is_valid)
        
    async def test_validate_high_price_signal(self):
        """Test validation of a signal with price above threshold."""
        await self.async_setup()
        
        # Validate signal
        result = await self.validator.validate_signal(self.high_price_signal)
        
        # Verify result
        self.assertFalse(result.is_valid)
        
    async def test_validate_old_signal(self):
        """Test validation of an old signal."""
        await self.async_setup()
        
        # Validate signal
        result = await self.validator.validate_signal(self.old_signal)
        
        # Verify result
        self.assertFalse(result.is_valid)
        
    async def test_duplicate_signal_detection(self):
        """Test detection of duplicate signals."""
        await self.async_setup()
        
        # Validate signal first time
        result1 = await self.validator.validate_signal(self.valid_signal)
        self.assertTrue(result1.is_valid)
        
        # Validate same signal again
        result2 = await self.validator.validate_signal(self.valid_signal)
        self.assertFalse(result2.is_valid)
        
        # Verify error message indicates duplicate
        self.assertIn("duplicate", result2.error.lower())
        
    async def test_validation_with_disabled_rules(self):
        """Test validation with disabled rules."""
        # Create config with disabled rules
        config = {
            "enabled": True,
            "rules": [
                {
                    "name": "price_threshold",
                    "enabled": False,  # Disabled
                    "params": {"min_price": 100.0, "max_price": 100000.0}
                },
                {
                    "name": "time_window",
                    "enabled": True,
                    "params": {"window_seconds": 3600}
                }
            ]
        }
        
        # Create validator with disabled rules
        validator = TradingViewValidator(config)
        await validator.initialize()
        validator._status = ComponentStatus.INITIALIZED
        
        # Validate signal with price below threshold (should pass because rule is disabled)
        result = await validator.validate_signal(self.low_price_signal)
        
        # Verify result
        self.assertTrue(result.is_valid)
        
    @patch('trading_system.market_data.tradingview_validator.TradingViewValidator._get_current_price')
    async def test_cross_reference_validation(self, mock_get_price):
        """Test validation with cross-reference to current price."""
        await self.async_setup()
        
        # Add cross_reference rule
        self.validator.rules.append({
            "name": ValidationRule.CROSS_REFERENCE.value,
            "enabled": True,
            "params": {"max_deviation": 0.05}  # 5% max deviation
        })
        
        # Setup mock to return a price close to the signal price
        mock_get_price.return_value = 51000.0  # About 2% above signal price
        
        # Validate signal
        result = await self.validator.validate_signal(self.valid_signal)
        
        # Verify result
        self.assertTrue(result.is_valid)
        
        # Setup mock to return a price far from the signal price
        mock_get_price.return_value = 60000.0  # 20% above signal price
        
        # Validate signal
        result = await self.validator.validate_signal(self.valid_signal)
        
        # Verify result (should fail due to price deviation)
        self.assertFalse(result.is_valid)


class TestTradingViewProvider(unittest.TestCase):
    """Test the TradingView signal provider."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create message bus
        self.message_bus = AsyncMock(spec=MessageBus)
        
        # Create test configuration
        self.config = {
            "enabled": True,
            "validator": {
                "enabled": True,
                "rules": [
                    {
                        "name": "price_threshold",
                        "enabled": True,
                        "params": {"min_price": 100.0}
                    }
                ]
            }
        }
        
        # Create provider
        self.provider = TradingViewProvider(self.config, self.message_bus)
        
        # Sample valid signal
        self.valid_signal = {
            "symbol": "BTC/USDT",
            "strategy": "MACD Cross",
            "action": "buy",
            "price": 50000.0,
            "timestamp": int(datetime.now().timestamp() * 1000)
        }
        
        # Signal without type indicator
        self.untyped_signal = {
            "symbol": "BTC/USDT",
            "action": "buy",
            "price": 50000.0
        }
        
        # Price alert
        self.price_alert = {
            "symbol": "BTC/USDT",
            "price": 50000.0,
            "condition": "cross_above"
        }
        
        # Invalid symbol format
        self.prefixed_symbol = {
            "symbol": "BINANCE:BTCUSDT",
            "strategy": "MACD Cross",
            "action": "buy",
            "price": 50000.0
        }
    
    async def async_setup(self):
        """Asynchronous setup for tests."""
        # Mock validator
        self.provider.validator = AsyncMock()
        self.provider.validator._status = ComponentStatus.INITIALIZED
        self.provider.validator.validate_signal.return_value = ValidationResult(
            is_valid=True,
            confidence=SignalConfidence.HIGH
        )
        
        # Initialize provider
        await self.provider.initialize()
        self.provider._status = ComponentStatus.INITIALIZED
    
    async def test_initialize(self):
        """Test provider initialization."""
        with patch.object(TradingViewValidator, 'initialize', return_value=True):
            result = await self.provider.initialize()
            self.assertTrue(result)
            self.assertEqual(self.provider._status, ComponentStatus.INITIALIZED)
        
    async def test_process_valid_signal(self):
        """Test processing of a valid signal."""
        await self.async_setup()
        
        # Process signal
        result = await self.provider.process_signal(self.valid_signal)
        
        # Verify result
        self.assertTrue(result)
        self.assertEqual(self.provider.stats["total_signals"], 1)
        self.assertEqual(self.provider.stats["valid_signals"], 1)
        
        # Verify message bus was called
        self.message_bus.publish.assert_called()
        
    async def test_process_invalid_signal(self):
        """Test processing of an invalid signal."""
        await self.async_setup()
        
        # Mock validator to reject signal
        self.provider.validator.validate_signal.return_value = ValidationResult(
            is_valid=False,
            confidence=SignalConfidence.REJECTED,
            reason="Test rejection"
        )
        
        # Process signal
        result = await self.provider.process_signal(self.valid_signal)
        
        # Verify result
        self.assertFalse(result)
        self.assertEqual(self.provider.stats["total_signals"], 1)
        self.assertEqual(self.provider.stats["invalid_signals"], 1)
        
        # Verify message bus was not called
        self.message_bus.publish.assert_not_called()
        
    async def test_signal_type_detection(self):
        """Test detection of signal types."""
        await self.async_setup()
        
        # Check strategy alert detection
        signal_type = self.provider._get_signal_type(self.valid_signal)
        self.assertEqual(signal_type, TradingViewSignalType.STRATEGY_ALERT.value)
        
        # Check price alert detection
        signal_type = self.provider._get_signal_type(self.price_alert)
        self.assertEqual(signal_type, TradingViewSignalType.PRICE_ALERT.value)
        
        # Check untyped signal (should default to custom)
        signal_type = self.provider._get_signal_type(self.untyped_signal)
        self.assertEqual(signal_type, TradingViewSignalType.CUSTOM_ALERT.value)
        
    async def test_signal_normalization(self):
        """Test normalization of TradingView signals."""
        await self.async_setup()
        
        # Normalize signal
        normalized = self.provider._normalize_signal(self.valid_signal)
        
        # Verify normalization
        self.assertIn("signal_id", normalized)
        self.assertIn("signal_type", normalized)
        self.assertEqual(normalized["symbol"], "BTC/USDT")
        
        # Test symbol prefix extraction
        normalized = self.provider._normalize_signal(self.prefixed_symbol)
        self.assertEqual(normalized["symbol"], "BTCUSDT")
        self.assertEqual(normalized["exchange"], "BINANCE")
        
    async def test_custom_signal_processor(self):
        """Test custom signal processor."""
        await self.async_setup()
        
        # Create a custom processor
        processor_called = False
        
        async def custom_processor(signal):
            nonlocal processor_called
            processor_called = True
            return True
        
        # Register processor
        await self.provider.register_signal_processor(
            TradingViewSignalType.STRATEGY_ALERT, 
            custom_processor
        )
        
        # Process signal
        await self.provider.process_signal(self.valid_signal)
        
        # Verify processor was called
        self.assertTrue(processor_called)
        
    async def test_signal_history(self):
        """Test signal history tracking."""
        await self.async_setup()
        
        # Process signals
        await self.provider.process_signal(self.valid_signal)
        await self.provider.process_signal(self.price_alert)
        
        # Get history
        history = self.provider.get_signal_history()
        
        # Verify history
        self.assertEqual(len(history), 2)
        
        # Get filtered history
        strategy_history = self.provider.get_signal_history(
            signal_type=TradingViewSignalType.STRATEGY_ALERT.value
        )
        
        # Verify filtered history
        self.assertEqual(len(strategy_history), 1)


# Integration tests for TradingView validation
class TestTradingViewIntegration(unittest.TestCase):
    """Integration tests for TradingView components."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create message bus
        self.message_bus = MessageBus()
        
        # Create configuration
        self.config = {
            "enabled": True,
            "validator": {
                "enabled": True,
                "rules": [
                    {
                        "name": "price_threshold",
                        "enabled": True,
                        "params": {"min_price": 100.0}
                    },
                    {
                        "name": "duplicate_check",
                        "enabled": True,
                        "params": {"window_seconds": 300}
                    }
                ]
            }
        }
        
        # Sample signals
        self.valid_signal = {
            "symbol": "BTC/USDT",
            "strategy": "MACD Cross",
            "action": "buy",
            "price": 50000.0,
            "timestamp": int(datetime.now().timestamp() * 1000)
        }
        
        self.invalid_signal = {
            "symbol": "BTC/USDT",
            "strategy": "MACD Cross",
            "action": "buy",
            "price": 50.0
        }
        
        # Create message handlers
        self.received_signals = []
        
    async def message_handler(self, message):
        """Handle messages from the message bus."""
        self.received_signals.append(message)
        
    @unittest.skip("Skipping integration test that requires message bus")
    async def test_provider_with_validator(self):
        """Test integration of provider with validator."""
        # Initialize message bus
        await self.message_bus.initialize()
        
        # Create provider
        provider = TradingViewProvider(self.config, self.message_bus)
        await provider.initialize()
        
        # Subscribe to signals
        await self.message_bus.subscribe(
            topic="tradingview.signal.processed",
            callback=self.message_handler
        )
        
        # Process valid signal
        valid_result = await provider.process_signal(self.valid_signal)
        
        # Process invalid signal
        invalid_result = await provider.process_signal(self.invalid_signal)
        
        # Wait for messages to be processed
        await asyncio.sleep(0.1)
        
        # Verify results
        self.assertTrue(valid_result)
        self.assertFalse(invalid_result)
        
        # Verify messages
        self.assertEqual(len(self.received_signals), 1)
        
        # Process duplicate signal
        duplicate_result = await provider.process_signal(self.valid_signal)
        
        # Verify duplicate was rejected
        self.assertFalse(duplicate_result)
        
        # Shutdown
        await self.message_bus.shutdown()


# Run the tests
if __name__ == '__main__':
    # Set up asyncio loop to run async tests
    loop = asyncio.get_event_loop()
    
    # Create test suite
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestTradingViewValidator))
    suite.addTest(unittest.makeSuite(TestTradingViewProvider))
    
    # Run the tests
    unittest.TextTestRunner().run(suite) 