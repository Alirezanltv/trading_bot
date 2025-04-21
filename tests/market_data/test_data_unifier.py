import os
import sys
import unittest
from datetime import datetime
from unittest.mock import patch, MagicMock

# Add parent directory to path to make imports work correctly
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from trading_system.market_data.data_unifier import DataUnifier

class TestDataUnifier(unittest.TestCase):
    """Test the DataUnifier component."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.unifier = DataUnifier()
        
        # Sample timestamp for consistent testing
        self.timestamp = int(datetime(2023, 1, 1, 12, 0, 0).timestamp() * 1000)
        
        # Sample market data from different sources
        self.nobitex_candle = {
            "date": self.timestamp,
            "open": "50000",
            "high": "51000",
            "low": "49000",
            "close": "50500",
            "volume": "10.5",
            "trades": 100
        }
        
        self.binance_candle = {
            "openTime": self.timestamp,
            "open": "50000",
            "high": "51000",
            "low": "49000",
            "close": "50500",
            "volume": "10.5",
            "closeTime": self.timestamp + 60000,
            "quoteVolume": "525000",
            "trades": 100,
            "takerBuyBaseAssetVolume": "5.25",
            "takerBuyQuoteAssetVolume": "262500"
        }
        
        self.kucoin_candle = {
            "timestamp": self.timestamp,
            "open": "50000",
            "high": "51000",
            "low": "49000",
            "close": "50500",
            "volume": "10.5",
            "turnover": "525000"
        }
        
        self.tradingview_signal = {
            "ticker": "BTCUSDT",
            "side": "BUY",
            "price": "50000",
            "strategy": "MACD Crossover",
            "time": self.timestamp,
            "confidence": 0.95
        }
        
        self.nobitex_ticker = {
            "symbol": "BTCIRT",
            "lastTradePrice": "2000000000",
            "bestSellPrice": "2010000000",
            "bestBuyPrice": "1990000000",
            "dayHigh": "2050000000",
            "dayLow": "1950000000",
            "dayVolume": "10.5",
            "dayChange": "2.5"
        }
        
        self.binance_ticker = {
            "symbol": "BTCUSDT",
            "priceChange": "100",
            "priceChangePercent": "0.2",
            "weightedAvgPrice": "50200",
            "prevClosePrice": "50000",
            "lastPrice": "50100",
            "lastQty": "0.1",
            "bidPrice": "50050",
            "bidQty": "1.5",
            "askPrice": "50150",
            "askQty": "0.8",
            "openPrice": "50000",
            "highPrice": "51000",
            "lowPrice": "49000",
            "volume": "10.5",
            "quoteVolume": "525000",
            "openTime": self.timestamp - 86400000,
            "closeTime": self.timestamp,
            "firstId": 100,
            "lastId": 200,
            "count": 100
        }
    
    def test_normalize_candle_nobitex(self):
        """Test normalization of Nobitex candle data."""
        symbol = "BTC/USDT"
        timeframe = "1h"
        
        # Normalize the candle
        result = self.unifier.normalize_candle(
            data=self.nobitex_candle,
            source="nobitex",
            symbol=symbol,
            timeframe=timeframe
        )
        
        # Verify standard format
        self.assertEqual(result["symbol"], symbol)
        self.assertEqual(result["timeframe"], timeframe)
        self.assertEqual(result["time"], self.timestamp)
        self.assertEqual(result["open"], 50000.0)
        self.assertEqual(result["high"], 51000.0)
        self.assertEqual(result["low"], 49000.0)
        self.assertEqual(result["close"], 50500.0)
        self.assertEqual(result["volume"], 10.5)
        self.assertEqual(result["source"], "nobitex")
    
    def test_normalize_candle_binance(self):
        """Test normalization of Binance candle data."""
        symbol = "BTC/USDT"
        timeframe = "1h"
        
        # Normalize the candle
        result = self.unifier.normalize_candle(
            data=self.binance_candle,
            source="binance",
            symbol=symbol,
            timeframe=timeframe
        )
        
        # Verify standard format
        self.assertEqual(result["symbol"], symbol)
        self.assertEqual(result["timeframe"], timeframe)
        self.assertEqual(result["time"], self.timestamp)
        self.assertEqual(result["open"], 50000.0)
        self.assertEqual(result["high"], 51000.0)
        self.assertEqual(result["low"], 49000.0)
        self.assertEqual(result["close"], 50500.0)
        self.assertEqual(result["volume"], 10.5)
        self.assertEqual(result["source"], "binance")
        self.assertEqual(result["trades"], 100)
        self.assertEqual(result["quote_volume"], 525000.0)
    
    def test_normalize_candle_kucoin(self):
        """Test normalization of KuCoin candle data."""
        symbol = "BTC/USDT"
        timeframe = "1h"
        
        # Normalize the candle
        result = self.unifier.normalize_candle(
            data=self.kucoin_candle,
            source="kucoin",
            symbol=symbol,
            timeframe=timeframe
        )
        
        # Verify standard format
        self.assertEqual(result["symbol"], symbol)
        self.assertEqual(result["timeframe"], timeframe)
        self.assertEqual(result["time"], self.timestamp)
        self.assertEqual(result["open"], 50000.0)
        self.assertEqual(result["high"], 51000.0)
        self.assertEqual(result["low"], 49000.0)
        self.assertEqual(result["close"], 50500.0)
        self.assertEqual(result["volume"], 10.5)
        self.assertEqual(result["source"], "kucoin")
        self.assertEqual(result["quote_volume"], 525000.0)
    
    def test_normalize_candle_with_missing_source(self):
        """Test normalization of candle data with missing source."""
        symbol = "BTC/USDT"
        timeframe = "1h"
        
        # Create a minimal candle
        min_candle = {
            "time": self.timestamp,
            "open": 50000.0,
            "high": 51000.0,
            "low": 49000.0,
            "close": 50500.0
        }
        
        # Normalize the candle without source
        result = self.unifier.normalize_candle(
            data=min_candle,
            symbol=symbol,
            timeframe=timeframe
        )
        
        # Verify standard format
        self.assertEqual(result["symbol"], symbol)
        self.assertEqual(result["timeframe"], timeframe)
        self.assertEqual(result["time"], self.timestamp)
        self.assertEqual(result["open"], 50000.0)
        self.assertEqual(result["high"], 51000.0)
        self.assertEqual(result["low"], 49000.0)
        self.assertEqual(result["close"], 50500.0)
        self.assertEqual(result["volume"], 0.0)  # Default volume
        self.assertNotIn("source", result)  # No source should be present
    
    def test_normalize_empty_candle(self):
        """Test normalization of empty candle data."""
        # Normalize empty data
        result = self.unifier.normalize_candle(data=None)
        self.assertIsNone(result)
        
        # Normalize empty dict
        result = self.unifier.normalize_candle(data={})
        self.assertIsNone(result)
    
    def test_normalize_ticker_nobitex(self):
        """Test normalization of Nobitex ticker data."""
        symbol = "BTC/IRT"
        
        # Normalize the ticker
        result = self.unifier.normalize_ticker(
            data=self.nobitex_ticker,
            source="nobitex",
            symbol=symbol
        )
        
        # Verify standard format
        self.assertEqual(result["symbol"], symbol)
        self.assertEqual(result["last"], 2000000000.0)
        self.assertEqual(result["bid"], 1990000000.0)
        self.assertEqual(result["ask"], 2010000000.0)
        self.assertEqual(result["high"], 2050000000.0)
        self.assertEqual(result["low"], 1950000000.0)
        self.assertEqual(result["volume"], 10.5)
        self.assertEqual(result["change_percent"], 2.5)
        self.assertEqual(result["source"], "nobitex")
    
    def test_normalize_ticker_binance(self):
        """Test normalization of Binance ticker data."""
        symbol = "BTC/USDT"
        
        # Normalize the ticker
        result = self.unifier.normalize_ticker(
            data=self.binance_ticker,
            source="binance",
            symbol=symbol
        )
        
        # Verify standard format
        self.assertEqual(result["symbol"], symbol)
        self.assertEqual(result["last"], 50100.0)
        self.assertEqual(result["bid"], 50050.0)
        self.assertEqual(result["ask"], 50150.0)
        self.assertEqual(result["high"], 51000.0)
        self.assertEqual(result["low"], 49000.0)
        self.assertEqual(result["open"], 50000.0)
        self.assertEqual(result["volume"], 10.5)
        self.assertEqual(result["quote_volume"], 525000.0)
        self.assertEqual(result["change"], 100.0)
        self.assertEqual(result["change_percent"], 0.2)
        self.assertEqual(result["source"], "binance")
    
    def test_normalize_ticker_empty_symbol(self):
        """Test normalization of ticker with empty symbol."""
        # Test with symbol in data
        ticker_with_symbol = {
            "symbol": "BTCUSDT",
            "lastPrice": "50100",
            "bidPrice": "50050",
            "askPrice": "50150",
        }
        
        result = self.unifier.normalize_ticker(
            data=ticker_with_symbol,
            source="binance"
        )
        
        self.assertEqual(result["symbol"], "BTCUSDT")
        
        # Test with missing symbol
        ticker_no_symbol = {
            "lastPrice": "50100",
            "bidPrice": "50050",
            "askPrice": "50150",
        }
        
        result = self.unifier.normalize_ticker(
            data=ticker_no_symbol,
            source="binance"
        )
        
        self.assertIsNone(result)  # Should return None when symbol is required
    
    def test_normalize_ticker_empty(self):
        """Test normalization of empty ticker data."""
        # Normalize empty data
        result = self.unifier.normalize_ticker(data=None)
        self.assertIsNone(result)
        
        # Normalize empty dict
        result = self.unifier.normalize_ticker(data={})
        self.assertIsNone(result)
    
    def test_normalize_signal(self):
        """Test normalization of trading signal data."""
        # Normalize the signal
        result = self.unifier.normalize_signal(
            data=self.tradingview_signal,
            source="tradingview"
        )
        
        # Verify standard format
        self.assertEqual(result["symbol"], "BTC/USDT")
        self.assertEqual(result["action"], "buy")
        self.assertEqual(result["price"], 50000.0)
        self.assertEqual(result["strategy"], "MACD Crossover")
        self.assertEqual(result["timestamp"], self.timestamp)
        self.assertEqual(result["confidence"], 0.95)
        self.assertEqual(result["source"], "tradingview")
    
    def test_normalize_signal_empty(self):
        """Test normalization of empty signal data."""
        # Normalize empty data
        result = self.unifier.normalize_signal(data=None)
        self.assertIsNone(result)
        
        # Normalize empty dict
        result = self.unifier.normalize_signal(data={})
        self.assertIsNone(result)
    
    def test_symbol_conversion(self):
        """Test symbol conversion between formats."""
        # Standard to exchange format (e.g., binance)
        self.assertEqual(self.unifier.convert_symbol("BTC/USDT", from_format="standard", to_format="binance"), "BTCUSDT")
        
        # Exchange to standard format
        self.assertEqual(self.unifier.convert_symbol("BTCUSDT", from_format="exchange", to_format="standard"), "BTC/USDT")
        
        # Empty symbol handling
        self.assertEqual(self.unifier.convert_symbol("", from_format="standard", to_format="binance"), "")
        self.assertEqual(self.unifier.convert_symbol(None, from_format="standard", to_format="binance"), None)
    
    def test_timeframe_conversion(self):
        """Test timeframe conversion between formats."""
        # Standard to Nobitex
        self.assertEqual(self.unifier.convert_timeframe("1h", "nobitex"), "60")
        
        # Standard to Binance
        self.assertEqual(self.unifier.convert_timeframe("1d", "binance"), "1d")
        
        # Standard to KuCoin
        self.assertEqual(self.unifier.convert_timeframe("15m", "kucoin"), "15min")
        
        # Empty timeframe handling
        self.assertEqual(self.unifier.convert_timeframe("", "binance"), "")
        self.assertEqual(self.unifier.convert_timeframe(None, "binance"), None)
        
        # Unknown format should raise ValueError
        with self.assertRaises(ValueError):
            self.unifier.convert_timeframe("1h", "unknown")

if __name__ == "__main__":
    unittest.main() 