# Trading System Monitoring Tools

This directory contains real-time monitoring tools for the trading system, including:
- TradingView market data monitoring
- Nobitex account balance monitoring
- Signal generation and analysis

## TradingView Fetcher and Monitor

The TradingView integration provides real-time market data and technical analysis for cryptocurrency markets.

### Features

- Real-time price data from TradingView
- Technical indicators (RSI, MACD, EMA, etc.)
- Trading signal generation based on indicator analysis
- Historical chart data (candles)
- Fallback data sources when TradingView API is unavailable

### Usage

To use the TradingView monitor, run the following command from the project root:

```bash
python run_tradingview_monitor.py [options]
```

Available options:
- `--symbols` - Comma-separated list of symbols to monitor (e.g., "BTC/USDT,ETH/USDT,SOL/USDT")
- `--interval` - Update interval in seconds (default: 60)
- `--timeframe` - Timeframe for analysis (default: 1D, options: 1m, 5m, 15m, 1h, 4h, 1D, 1W)

Example:
```bash
python run_tradingview_monitor.py --symbols "BTC/USDT,ETH/USDT,DOT/USDT" --interval 30 --timeframe 1h
```

### Display

The monitor displays a table with the following information for each symbol:
- Current price and 24h change
- RSI value (color-coded for overbought/oversold conditions)
- MACD value
- Trading signal (BUY, SELL, NEUTRAL) with strength indicator
- Detailed signal reasons for the top symbol

## Account Monitor

The account monitor connects to your Nobitex account to display balance information and track performance.

### Features

- Real-time account balance updates
- Asset allocation visualization
- Value tracking in IRT (Iranian Rials)
- Price updates from Nobitex API

### Usage

To use the account monitor, run:

```bash
python trading_system/monitoring/account_monitor.py
```

## Dependencies

These monitoring tools require the following dependencies:
- `websockets` - For WebSocket connections to TradingView
- `aiohttp` - For HTTP requests to alternative APIs
- `pandas` - For data manipulation and analysis
- `tabulate` - For table formatting in console output
- `colorama` - For colored console output

Install dependencies with:
```bash
pip install websockets aiohttp pandas tabulate colorama
```

## Configuration

The monitors read configuration from `.env` file at the project root:

```
# Nobitex API credentials
NOBITEX_API_KEY=your_api_key
NOBITEX_SECRET_KEY=your_secret_key

# TradingView credentials (optional)
TV_USERNAME=your_tradingview_username
TV_PASSWORD=your_tradingview_password
```

## Development

To extend the monitoring tools:

1. To add new indicators, modify the `get_indicators` method in `TradingViewFetcher` class
2. To add new trading signals, modify the `_generate_signals` method in `TradingViewMonitor` class
3. To change display format, modify the `_display_market_data` method

### Architecture

The monitoring system follows this architecture:
- `TradingViewFetcher` - Core class for fetching data from TradingView API
- `TradingViewMonitor` - Processes data and generates signals
- `NobitexClient` - Connects to Nobitex API for account information
- `AccountMonitor` - Displays account balance and asset information 