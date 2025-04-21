# Position Management Subsystem

The Position Management Subsystem is responsible for tracking, managing, and monitoring trading positions in the trading system. It serves as the bridge between the Strategy and Execution subsystems, storing state, ensuring risk boundaries are respected, and preparing for live trade execution.

## Features

- **Position Tracking**: Track open and closed positions in real-time
- **Portfolio Management**: Monitor overall portfolio exposure and risk
- **Position Sizing**: Apply risk-based position sizing rules
- **Risk Management**: Implement stop loss, take profit, and trailing stop mechanisms
- **Data Persistence**: Store position data for recovery and audit
- **Exchange Reconciliation**: Reconcile positions with exchange data

## Components

### Position Types (`position_types.py`)

Core data structures and enums for the position management subsystem:

- `Position`: Represents a trading position with details like symbol, type, quantity, price, etc.
- `Order`: Represents an order associated with a position
- `PositionTrigger`: Represents a trigger condition (stop loss, take profit, etc.)
- `PortfolioSummary`: Represents the portfolio state with exposure metrics

### Position Manager (`position_manager.py`)

The core component responsible for managing positions:

- Create, update, and close positions
- Track orders related to positions
- Apply position sizing
- Monitor position triggers
- Calculate and track P&L

### Position Sizing (`position_sizing.py`)

Strategies for calculating appropriate position sizes:

- Fixed Risk: Risk a fixed percentage of account per trade
- Percentage of Equity: Allocate a fixed percentage of equity per trade
- Kelly Criterion: Calculate optimal position size based on win rate and R/R
- Volatility-based: Adjust position size based on market volatility

### Position Storage (`position_storage.py`)

Persistent storage for positions and related data using SQLite:

- Store position data, orders, and triggers
- Query positions by various criteria
- Store portfolio snapshots over time

### Position Service (`position_service.py`)

Service layer that bridges strategies and position management:

- Process strategy signals to create/modify positions
- Monitor positions for trigger conditions
- Handle position lifecycle events

## Usage

### Basic Usage

```python
from trading_system.position.position_manager import PositionManager
from trading_system.position.position_types import Position, PositionType

# Create position manager
config = {"position_management": {"max_exposure": 0.8}}
position_manager = PositionManager(config=config)

# Create a position
position = position_manager.create_position(
    symbol="BTC/USDT",
    position_type=PositionType.LONG,
    quantity=0.1,
    entry_price=50000.0,
    strategy_id="trend_following"
)

# Update a position with current market price
position_manager.process_market_update("BTC/USDT", 52000.0)

# Close a position
position_manager.close_position(
    position_id=position.position_id,
    exit_price=52000.0
)

# Get portfolio summary
portfolio = position_manager.get_portfolio_summary()
print(f"Total P&L: ${portfolio.total_pnl}")
```

### Using Position Service with Strategies

```python
from trading_system.position.position_service import PositionService

# Create position service
position_service = PositionService(position_manager, config)

# Process a strategy signal
signal = {
    "action": "buy",
    "symbol": "ETH/USDT",
    "price": 3000.0,
    "stop_loss": 2850.0,
    "take_profit": 3300.0,
    "timeframe": "1h",
    "indicators": {"rsi": 32, "macd": "bullish"}
}

position = position_service.process_strategy_signal("mean_reversion", signal)
```

## Examples

See the `examples` directory for sample scripts that demonstrate the usage of the Position Management subsystem.

### Running the Example

```
python -m trading_system.position.examples.position_manager_example
```

This script demonstrates:
- Creating sample positions with different strategies
- Applying stop loss and take profit levels
- Simulating market price updates
- Analyzing position performance

## Configuration

The Position Management subsystem accepts configuration options for customizing behavior:

```python
config = {
    "position_management": {
        "max_exposure": 0.8,             # Maximum portfolio exposure
        "max_symbol_exposure": 0.25,     # Maximum exposure per symbol
        "max_strategy_exposure": 0.5,    # Maximum exposure per strategy
        "default_stop_loss": 0.05,       # Default stop loss (5%)
        "default_take_profit": 0.1,      # Default take profit (10%)
        "monitoring_interval": 10,       # Check triggers every 10 seconds
        "reconciliation_interval": 3600  # Reconcile with exchange hourly
    },
    "position_sizing": {
        "default_type": "fixed_risk",
        "default_config": {
            "risk_percentage": 0.01      # Risk 1% per trade
        },
        "strategies": {
            "trend_following": {
                "type": "percentage_of_equity",
                "config": {
                    "percentage": 0.05   # Allocate 5% per trade
                }
            }
        }
    }
}
```

## Data Persistence

Position data is persisted in an SQLite database for durability and recovery:

- Positions, orders, and triggers are stored in separate tables
- Portfolio snapshots are captured over time
- Data can be queried for reporting and analysis

## Integration

The Position Management subsystem is designed to integrate with other components:

- Receives signals from the Strategy subsystem
- Forwards execution requests to the Execution subsystem
- Updates positions based on market data from the Market Data subsystem
- Reconciles with exchange data via the Exchange Integration subsystem 