"""
Exchange Adapters Package

Provides adapters for various exchanges, implementing a common interface.
"""

from trading_system.exchange.base_adapter import ExchangeAdapter
from trading_system.exchange.mock_exchange import MockExchangeAdapter

__all__ = ['ExchangeAdapter', 'MockExchangeAdapter']

# Try to import main components
try:
    from trading_system.exchange.adapter import ExchangeAdapter, OrderResult, OrderInfo, MarketInfo
except ImportError:
    pass  # These might not be available yet

# Try to import specific exchange implementations
try:
    from trading_system.exchange.nobitex_adapter import NobitexExchangeAdapter
except ImportError:
    pass  # NobitexExchangeAdapter might not be available yet

# Try to import base components
try:
    from trading_system.exchange.base import ExchangeBase, OrderSide, OrderType
except ImportError:
    pass  # Base components might not be available yet

# Optional imports for other exchange implementations
try:
    from trading_system.exchange.nobitex import NobitexExchange
except ImportError:
    pass

try:
    from trading_system.exchange.nobitex_client import NobitexClient
except ImportError:
    pass

try:
    from trading_system.exchange.simulated import SimulatedExchange
except ImportError:
    pass

# Add components that exist in the global namespace
for name in ['NobitexExchangeAdapter', 'NobitexClient', 'SimulatedExchange', 
             'ExchangeAdapter', 'OrderResult', 'OrderInfo', 'MarketInfo',
             'ExchangeBase', 'OrderSide', 'OrderType']:
    if name in globals():
        __all__.append(name) 