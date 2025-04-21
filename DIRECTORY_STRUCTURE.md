# Trading System Directory Structure

This document outlines the directory structure of the trading system project and explains the purpose of each component.

## Root Directory

The root directory for the project is:

```
E:\Programming\Trading_bot_4.4.2025\trading_system
```

This is the permanent project root, containing all modules, scripts, and assets.

## Directory Structure

```
trading_system/
├── __init__.py                 # Main package initialization
├── README.md                   # Project documentation
├── requirements.txt            # Project dependencies
├── setup.py                    # Package installation script
├── .env                        # Environment variables (not in version control)
├── .gitignore                  # Git ignore rules
│
├── core/                       # Core system components
│   ├── __init__.py
│   ├── component.py            # Base component class
│   ├── message_bus.py          # Inter-component communication
│   ├── logging.py              # Centralized logging
│   ├── config.py               # Configuration management
│   └── database.py             # Database access
│
├── exchange/                   # Exchange adapters
│   ├── __init__.py
│   ├── adapter.py              # Base adapter interface
│   ├── nobitex_adapter.py      # Nobitex exchange adapter
│   ├── nobitex_client.py       # Nobitex API client
│   └── simulated_exchange.py   # Simulated exchange for testing
│
├── market_data/                # Market data subsystem
│   ├── __init__.py
│   ├── market_data_subsystem.py  # Subsystem main module
│   ├── market_data_facade.py   # Data provider facade
│   ├── data_unifier.py         # Data normalization
│   ├── timeseries_db.py        # Time-series database integration
│   ├── tradingview_validator.py # Signal validation
│   ├── tradingview_provider.py  # TradingView signal provider
│   └── nobitex_websocket.py    # Nobitex WebSocket client
│
├── strategy/                   # Trading strategies
│   ├── __init__.py
│   ├── base.py                 # Strategy base class
│   ├── rsi_strategy.py         # RSI-based strategy
│   └── indicators.py           # Technical indicators
│
├── execution/                  # Order execution
│   ├── __init__.py
│   ├── order_manager.py        # Order management
│   └── position_manager.py     # Position tracking
│
├── risk/                       # Risk management
│   ├── __init__.py
│   ├── risk_manager.py         # Risk management rules
│   └── position_sizer.py       # Position sizing
│
├── utils/                      # Utility functions
│   ├── __init__.py
│   ├── cleanup.py              # Project cleanup utility
│   └── time_utils.py           # Time conversion utilities
│
├── config/                     # Configuration files
│   ├── __init__.py
│   ├── trading_config.json     # Trading configuration
│   └── test_config.json        # Test configuration
│
├── data/                       # Data storage
│   ├── __init__.py
│   ├── positions/              # Position data
│   ├── orders/                 # Order data
│   ├── market_data/            # Market data storage
│   ├── logs/                   # Log files
│   └── api_keys.json           # API keys (not in version control)
│
├── examples/                   # Example scripts
│   ├── __init__.py
│   ├── nobitex_test.py         # Nobitex API test
│   └── market_data_demo.py     # Market data subsystem demo
│
└── tests/                      # Test modules
    ├── __init__.py
    ├── market_data/            # Market data tests
    │   ├── __init__.py
    │   ├── test_timeseries_db.py
    │   ├── test_data_unifier.py
    │   ├── test_data_facade.py
    │   ├── test_tradingview_integration.py
    │   ├── test_websocket_client.py
    │   └── test_market_data_subsystem.py
    ├── exchange/               # Exchange tests
    │   ├── __init__.py
    │   └── test_nobitex_adapter.py
    ├── core/                   # Core module tests
    │   ├── __init__.py
    │   └── test_message_bus.py
    └── integration/            # Integration tests
        ├── __init__.py
        └── test_end_to_end.py
```

## Key Components

1. **Core**: Fundamental system components used by all other modules.
2. **Exchange**: Adapters for interacting with cryptocurrency exchanges.
3. **Market Data**: Components for collecting, validating, and storing market data.
4. **Strategy**: Trading strategies and technical indicators.
5. **Execution**: Order placement and management.
6. **Risk**: Risk management and position sizing.
7. **Utils**: Utility functions and helper classes.
8. **Config**: Configuration files and settings.
9. **Data**: Data storage and persistence.
10. **Tests**: Test modules for all components.

## Modular Design

The system is organized with a modular design, where each component has a specific responsibility and communicates with other components through well-defined interfaces. This design allows for:

- **Extensibility**: New components (e.g., exchanges, strategies) can be added without modifying existing code.
- **Testability**: Components can be tested in isolation.
- **Maintainability**: Clear separation of concerns makes the codebase easier to understand and maintain.
- **Reusability**: Components can be reused in different contexts.

## Package Initialization

All directories include `__init__.py` files to make them proper Python packages, enabling clean imports like:

```python
from trading_system.market_data.data_unifier import DataUnifier
from trading_system.strategy.rsi_strategy import RSIStrategy
```

## Configuration Management

Configuration is managed through:

1. **Environment variables**: Sensitive information like API keys
2. **Configuration files**: Trading parameters and settings
3. **Command-line arguments**: Runtime configuration

## Testing

The test suite includes:

1. **Unit tests**: Testing individual components in isolation
2. **Integration tests**: Testing interactions between components
3. **End-to-end tests**: Testing complete workflows

Tests can be run using:

```
python -m unittest discover -s trading_system/tests
``` 