"""
Engine Fix Module

This module patches the StrategyEngine class to fix the incorrect import for MarketDataFacade.
"""

import sys
from types import ModuleType
from importlib.util import spec_from_loader, module_from_spec
from importlib.machinery import SourceFileLoader

# Create a fake module for the missing import
module_name = "trading_system.market_data.data_facade"
spec = spec_from_loader(module_name, loader=None)
facade_module = module_from_spec(spec)
sys.modules[module_name] = facade_module

# Add the MarketDataFacade class to the module
from trading_system.market_data.market_data_facade import MarketDataFacade, get_timeseries_db

# Add the MarketDataFacade class to our fake module
setattr(facade_module, 'MarketDataFacade', MarketDataFacade)

# Create a get_market_data_facade function
def get_market_data_facade():
    """
    Get the global market data facade instance.
    
    Returns:
        MarketDataFacade instance
    """
    from trading_system.market_data.market_data_facade import MarketDataFacade
    config = {
        "validation_level": "none",
        "cache_size": 1000,
        "data_sources": {
            "tradingview": {
                "enabled": True
            }
        }
    }
    
    return MarketDataFacade(config)

# Add the function to our fake module
setattr(facade_module, 'get_market_data_facade', get_market_data_facade)

# Fix the StrategyEngine __init__ method
from trading_system.strategy.engine import StrategyEngine

# Store the original constructor
original_init = StrategyEngine.__init__

# Create a new constructor that handles both dict configs and MessageBus
def patched_init(self, config=None):
    """
    Patched version of __init__ that handles both config dict and MessageBus objects.
    """
    from trading_system.core.message_bus import MessageBus
    
    # If config is a MessageBus, store it separately and use default config
    if isinstance(config, MessageBus):
        self.message_bus = config
        config = None
    
    # Call original init with dict config
    original_init(self, config)

# Replace the constructor
StrategyEngine.__init__ = patched_init

print("Engine fix applied: Patched trading_system.market_data.data_facade module") 