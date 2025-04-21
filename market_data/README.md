# Market Data Subsystem

A comprehensive market data collection, validation, storage, and distribution system for the trading bot.

## Overview

The Market Data Subsystem provides a robust infrastructure for handling market data from multiple sources with validation, redundancy, and persistence. The system is designed to be fault-tolerant, ensuring continuous data availability even when individual data sources fail.

## Key Features

- **Multiple Data Sources**: Support for exchange REST APIs, WebSockets, TradingView signals, and more
- **Data Validation**: Comprehensive validation of market data to ensure accuracy and integrity
- **Time-Series Storage**: Persistent storage of market data in a time-series database (InfluxDB)
- **Redundancy**: Automatic failover between data sources when primary sources are unavailable
- **Unified Interface**: A single interface to access market data regardless of the underlying source
- **Data Normalization**: Standardization of data formats from different sources
- **WebSocket Integration**: Real-time market data via WebSocket connections
- **TradingView Integration**: Processing and validation of TradingView signals
- **Caching**: In-memory caching to reduce database load and improve performance

## Architecture

The Market Data Subsystem consists of several components:

### 1. Market Data Facade

The `MarketDataFacade` provides a unified interface to access market data from multiple sources with automatic failover and validation. It implements the Facade design pattern, hiding the complexity of multiple data sources behind a simple interface.

Key features:
- Source prioritization and selection strategies
- Data validation at multiple levels
- Automatic fallback to alternative sources
- In-memory caching
- Integration with time-series database for persistence
- Message bus integration for event-driven architecture

### 2. Data Unifier

The `DataUnifier` normalizes market data from different sources into standardized formats for consistent processing throughout the trading system.

Key features:
- Standardization of candle data
- Normalization of ticker information
- Order book format unification
- Timestamp standardization

### 3. TradingView Integration

The TradingView integration consists of two main components:

#### TradingViewValidator

Validates TradingView signals for authenticity and relevance, implementing various validation rules to ensure signal integrity.

Key features:
- Multiple validation rules
- Signal confidence scoring
- Cross-validation with other data sources
- Duplicate detection
- Time window validation

#### TradingViewProvider

Handles incoming TradingView signals, validates them, and distributes them to subscribers via the message bus.

Key features:
- Signal type classification
- Signal processing workflow
- History tracking for duplicate detection
- Integration with message bus for distribution

### 4. WebSocket Clients

WebSocket clients provide real-time market data directly from exchanges.

Key features:
- Automatic reconnection
- Message parsing and normalization
- Subscription management
- Error handling and recovery
- Message buffering during disconnections

### 5. Time-Series Database

The `TimeSeriesDB` component provides persistent storage for market data using InfluxDB.

Key features:
- Efficient storage of time-series data
- Querying with time range support
- Automatic data retention policies
- Multi-granularity storage (different timeframes)
- Data compression

## Usage

### Basic Usage

```python
from trading_system.market_data import get_market_data_subsystem
from trading_system.core.message_bus import MessageBus

# Create message bus
message_bus = MessageBus()
await message_bus.initialize()

# Initialize market data subsystem
config = {
    "timeseries_db": {
        "enabled": True,
        "url": "http://localhost:8086",
        "token": "your_influxdb_token",
        "org": "your_organization",
        "bucket": "market_data"
    }
}

subsystem = get_market_data_subsystem(config, message_bus)
await subsystem.initialize()

# Get market data facade
facade = subsystem.facade

# Get market data
candles = await facade.get_market_data(
    symbol="BTC/USDT",
    timeframe="1h",
    limit=100
)

# Get current ticker
ticker = await facade.get_ticker("BTC/USDT")

# Get order book
order_book = await facade.get_order_book("BTC/USDT", depth=10)

# Shutdown
await subsystem.shutdown()
```

### Handling TradingView Signals

```python
from trading_system.market_data import get_tradingview_provider

# Initialize TradingView provider
tv_provider = get_tradingview_provider(config, message_bus)
await tv_provider.initialize()

# Register signal processor
async def process_buy_signal(signal):
    symbol = signal.get("symbol")
    price = signal.get("price")
    print(f"Buy signal for {symbol} at {price}")
    return True

await tv_provider.register_signal_processor("strategy_alert", process_buy_signal)

# Process a signal
signal = {
    "symbol": "BTC/USDT",
    "strategy": "MACD Cross",
    "action": "buy",
    "price": 55000.0,
    "message": "Buy signal for BTC/USDT at $55000"
}

result = await tv_provider.process_signal(signal)
```

## Configuration

The Market Data Subsystem can be configured through a configuration dictionary:

```python
config = {
    "timeseries_db": {
        "enabled": True,
        "url": "http://localhost:8086",
        "token": "your_influxdb_token",
        "org": "your_organization",
        "bucket": "market_data",
        "retention_policy": "30d"
    },
    "tradingview": {
        "enabled": True,
        "validator": {
            "enabled": True,
            "rules": [
                {
                    "name": "price_threshold",
                    "enabled": True,
                    "params": {"min_price": 100}
                },
                {
                    "name": "time_window",
                    "enabled": True,
                    "params": {"window_seconds": 3600}
                }
            ]
        }
    },
    "websocket": {
        "nobitex": {
            "enabled": True,
            "symbols": ["BTC/USDT", "ETH/USDT"],
            "reconnect_interval": 5
        }
    },
    "facade": {
        "validation_level": "basic",
        "cache_expiry": 60,
        "selection_strategy": "priority",
        "max_age_seconds": 300
    }
}
```

## Examples

See the `trading_system/examples/market_data_demo.py` for a comprehensive example of using the Market Data Subsystem.

## Requirements

- Python 3.9+
- InfluxDB 2.0+ (if using time-series database)
- Required Python packages:
  - aiohttp
  - influxdb-client
  - websocket-client

## Future Enhancements

- Additional exchange WebSocket clients
- Enhanced cross-validation between data sources
- Machine learning-based anomaly detection for data validation
- Compression and efficient storage of large data sets
- Integration with additional data sources (news feeds, etc.)
- Real-time data analysis and event detection 