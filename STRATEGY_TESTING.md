# Strategy Subsystem Testing Guide

This guide explains how to test the Strategy Subsystem with real-time market data to verify signal generation.

## Prerequisites

- Python 3.10 installed
- Market Data Subsystem properly configured
- All dependencies installed

## Testing Process

### 1. Verify Market Data Connectivity

Before testing signal generation, it's important to verify that the Market Data Facade is properly connected and delivering data.

Run the connectivity check:
```
check_connectivity.bat
```

This will:
- Connect to the Market Data Facade
- Subscribe to a single symbol and timeframe
- Monitor and display incoming data
- Verify that market data is flowing correctly

If the connectivity check fails, troubleshoot the Market Data Subsystem before proceeding.

### 2. Test Real-time Signal Generation

Once market data connectivity is confirmed, test the full Strategy Subsystem with real-time signal generation:

```
run_live_tests.bat
```

This will:
- Initialize the Strategy Engine
- Register the RSI strategy
- Connect to the Market Data Facade
- Process live market data
- Generate trading signals
- Monitor strategy performance
- Generate a test report

The test will run for 5 minutes by default, displaying progress updates and signal information as it runs.

### 3. Review Results

After the test completes:
- Check the console output for generated signals
- Review the test report (saved in `data/reports/`)
- Examine strategy performance metrics
- Verify signal distribution across symbols and timeframes

## Configuration

You can customize the test configuration by editing the batch files or by passing command-line arguments directly:

### Connectivity Check Options

```
python strategy/examples/check_market_connectivity.py --symbol BTC/USDT --timeframe 1m --duration 60 --source binance
```

### Live Test Options

```
python strategy/examples/test_live_signals.py --symbols BTC/USDT,ETH/USDT --timeframes 1m,5m,15m,1h --duration 300 --source binance
```

## Troubleshooting

If you encounter issues:

1. **No market data received:**
   - Check network connectivity
   - Verify API credentials are correctly configured
   - Ensure the data source is available and operational

2. **No signals generated:**
   - Check that market data is being received properly
   - Review strategy parameters to ensure they're appropriate for current market conditions
   - Verify that the strategy is registered correctly with the Strategy Engine

3. **Component initialization errors:**
   - Check that all dependencies are installed
   - Verify Python version (3.10 recommended)
   - Check for configuration errors in components

4. **Performance issues:**
   - Reduce the number of symbols and timeframes being processed
   - Monitor system resource usage during testing
   - Check for memory leaks or excessive CPU usage

## Next Steps

After verifying that the Strategy Subsystem is functioning correctly, the next phase is to implement the Position Management Subsystem, which will handle strategy signal execution. 