#!/usr/bin/env python
"""
Nobitex Exchange Adapter Structure Test

Test script that examines the structure of the NobitexExchangeAdapter class
without calling problematic initialization code.
"""

import os
import sys
import logging
import inspect
from typing import Dict, Any

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
    """Test the structure of NobitexExchangeAdapter without initialization."""
    try:
        # Import the adapter class
        from trading_system.exchange.adapter import ExchangeAdapter
        
        # Get the source code of the ExchangeAdapter.__init__ method
        init_source = inspect.getsource(ExchangeAdapter.__init__)
        logger.info(f"ExchangeAdapter.__init__ source code:")
        for line in init_source.splitlines():
            logger.info(f"  {line}")
        
        # Now import the NobitexExchangeAdapter class
        from trading_system.exchange.nobitex_adapter import NobitexExchangeAdapter
        
        # Print inheritance hierarchy
        logger.info(f"NobitexExchangeAdapter inherits from: {NobitexExchangeAdapter.__mro__}")
        
        # Create our own customized testing adapter that doesn't call _initialize
        class TestAdapter(NobitexExchangeAdapter):
            def __init__(self, config: Dict[str, Any]):
                # Minimal setup to bypass parent's initialization
                self.name = "nobitex"
                self.config = config
                self.logger = logging.getLogger("test_adapter")
                
                # Extract required fields
                self.api_key = config.get("api_key")
                self.api_secret = config.get("api_secret")
                self.timeout = config.get("timeout", 15)
                self.max_retries = config.get("max_retries", 3)
                self.cache_dir = config.get("cache_dir", "./cache")
                
                # Nobitex client initialization will not happen
                self.client = None
                
                # Initialize caches
                self.markets_info = {}
                self.symbol_mapping = {}
                self.reverse_symbol_mapping = {}
        
        # Create an instance
        config = {"api_key": None, "api_secret": None, "cache_dir": "./cache"}
        adapter = TestAdapter(config)
        
        # Log structure
        logger.info(f"Successfully created adapter instance")
        logger.info(f"Adapter attributes: {', '.join([attr for attr in dir(adapter) if not attr.startswith('__')])}")
        logger.info(f"Adapter methods: {', '.join([method for method in dir(adapter) if callable(getattr(adapter, method)) and not method.startswith('__')])}")
        
        return True
    except Exception as e:
        logger.error(f"Error testing adapter structure: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    logger.info("Starting Nobitex adapter structure test")
    
    # Test adapter structure
    success = test_adapter_structure()
    logger.info(f"Adapter structure test {'passed' if success else 'failed'}") 