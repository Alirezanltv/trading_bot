"""
Provider Fix Module

This module patches the MarketDataFacade to automatically register the TradingView provider
during initialization to ensure market data is available.
"""

import asyncio
import logging
from typing import Dict, Any, Optional

from trading_system.core.message_bus import MessageBus
from trading_system.market_data.market_data_facade import MarketDataFacade, DataProviderType, ComponentStatus, DataValidationLevel
from trading_system.market_data.tradingview_provider import get_tradingview_provider
from trading_system.strategy.examples.provider_adapter import get_adapted_tradingview_provider
from trading_system.core.logging import get_logger

logger = get_logger("provider_fix")

# Store the original initialize method
original_initialize = MarketDataFacade.initialize

async def patched_initialize(self) -> bool:
    """
    Patched version of initialize that automatically registers the TradingView provider.
    """
    # Call the original initialize method
    result = await original_initialize(self)
    
    # Check if initialization was successful
    if result and self._status == ComponentStatus.INITIALIZED:
        # Disable validation so that mock data is accepted
        self.validation_level = DataValidationLevel.NONE
        logger.info("Setting validation level to NONE to accept mock data")
        
        # Get or create TradingView provider
        try:
            logger.info("Creating and registering adapted TradingView provider")
            
            # Create the TradingView provider adapter that implements all required methods
            tv_provider = get_adapted_tradingview_provider({
                "enabled": True,
                "cache_expiry": 60,
                "use_premium": False,
                "timeout": 30
            }, self.message_bus)
            
            # Register the provider
            await self.register_provider(
                provider=tv_provider,
                provider_type=DataProviderType.PRIMARY,
                priority=100,
                name="tradingview"
            )
            
            logger.info("Adapted TradingView provider registered successfully")
            
            # List all registered providers
            providers = list(self.providers.keys())
            logger.info(f"Registered providers: {providers}")
            
            return True
        except Exception as e:
            logger.error(f"Error registering TradingView provider: {str(e)}")
    
    return result

# Replace the initialize method
MarketDataFacade.initialize = patched_initialize 