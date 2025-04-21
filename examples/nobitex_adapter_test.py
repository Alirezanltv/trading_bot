#!/usr/bin/env python
"""
Nobitex Exchange Adapter Test

Simple test script for the NobitexExchangeAdapter class.
"""

import os
import sys
import logging
from dotenv import load_dotenv

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("adapter_test")

def test_adapter_structure():
    """Test the NobitexExchangeAdapter structure without initialization."""
    try:
        from trading_system.exchange.nobitex_adapter import NobitexExchangeAdapter
        from trading_system.exchange.adapter import ExchangeAdapter
        
        logger.info("Successfully imported adapter classes")
        logger.info(f"NobitexExchangeAdapter inherits from ExchangeAdapter: {issubclass(NobitexExchangeAdapter, ExchangeAdapter)}")
        
        # Create a minimal instance to verify structure
        minimal_config = {"api_key": None, "api_secret": None, "cache_dir": "./cache"}
        adapter = NobitexExchangeAdapter(minimal_config)
        
        # Check initialization without calling methods
        logger.info(f"Adapter instance created with name: {adapter.name}")
        logger.info(f"Adapter has logger: {hasattr(adapter, 'logger')}")
        
        # List available methods 
        methods = [method for method in dir(adapter) if callable(getattr(adapter, method)) and not method.startswith('_')]
        logger.info(f"Available public methods: {', '.join(methods[:10])}...")
        
        return True
    except Exception as e:
        logger.error(f"Error in adapter structure test: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    logger.info("Starting Nobitex adapter structure test")
    
    # Test adapter structure
    success = test_adapter_structure()
    logger.info(f"Adapter structure test {'passed' if success else 'failed'}") 