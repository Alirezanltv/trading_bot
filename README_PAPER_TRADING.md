# Paper Trading Mode

This document explains how to run the trading system in paper trading mode.

## Overview

Paper trading mode allows you to:
- Use real market data from exchanges
- Test trading strategies in real-time
- Simulate order execution without using real funds
- Track performance metrics and portfolio changes

## Requirements

- Python 3.10 or higher
- All dependencies installed (`pip install -r requirements.txt`)
- Configuration file (see `configs/paper_trading.yaml`)

## Quick Start

1. **Configure your paper trading settings:**

   Review and modify the configuration file at `configs/paper_trading.yaml`:
   - Adjust virtual balances
   - Configure market data sources
   - Set up risk parameters
   - Activate desired trading strategies

2. **Start paper trading:**

   ```bash
   python trading_system/run_paper_trading.py
   ```

   Optional parameters:
   ```bash
   python trading_system/run_paper_trading.py --config configs/custom_config.yaml --log-level DEBUG
   ```

3. **Monitor the running system:**

   - Check logs at `logs/paper_trading_YYYY-MM-DD.log`
   - Monitor positions and order execution
   - Review performance metrics

## Configuration Options

### Virtual Balances

Set your virtual balances in the configuration file:
```yaml
execution:
  paper_trading:
    virtual_balance:
      USDT: 10000.0
      IRT: 5000000.0
      BTC: 0.1
```

### Trading Strategies

Enable or disable strategies:
```yaml
strategy:
  active_strategies:
    - "MovingAverageCrossover"
    - "BollingerBreakout"
```

Customize strategy parameters:
```yaml
strategy_params:
  MovingAverageCrossover:
    fast_period: 9
    slow_period: 21
```

### Risk Management

Set risk limits:
```yaml
risk:
  max_position_value_percent: 5.0
  max_daily_drawdown_percent: 3.0
```

## Data Storage

Paper trading data is stored in:
- Order history: `data/paper_trading/orders/paper_trading_orders.csv`
- Position data: `data/paper_trading/positions.db`
- TradingView signals: `data/paper_trading/tradingview_signals.db`

## Realistic Execution Simulation

The paper trading engine can simulate realistic execution conditions:

```yaml
execution:
  paper_trading:
    order_execution_mode: "realistic"  # Options: instant, realistic, manual
    simulated_latency_ms: 500
    simulated_slippage_percent: 0.1
```

## Performance Analysis

After a paper trading session, you can analyze performance:
- Check logs for trade results and metrics
- Review position history
- Analyze drawdown and volatility

## Common Issues

- **Market data connection issues**: Check your network and API keys
- **Strategy errors**: Check strategy logs for specific error messages
- **Configuration problems**: Validate YAML formatting in config files

## Advanced Usage

### Custom Market Data Sources

You can modify the market data sources in your configuration:
```yaml
market_data:
  nobitex:
    subscription_symbols:
      - "BTC/USDT"
      - "ETH/USDT"
```

### TradingView Integration

To receive TradingView signals, set up a webhook:
```yaml
tradingview:
  webhook_port: 8080
  webhook_path: "/tradingview/webhook"
  auth_token: "YOUR_SECRET_TOKEN"
```

Then configure your TradingView alerts to send webhooks to your endpoint.

## License

This software is proprietary and confidential. Unauthorized copying, transferring or reproduction of the contents is strictly prohibited.

---

For more information, contact the development team.