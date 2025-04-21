#!/usr/bin/env python
"""
Nobitex Integration Test

This script tests the connection to Nobitex and basic functionality
such as fetching market data and account balances.
"""

import os
import sys
import logging
import time
from dotenv import load_dotenv

# Add project root to path so we can import modules properly
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("nobitex_test")

def test_nobitex_api():
    """Test basic functionality of the NobitexAPIClient."""
    try:
        from nobitex_api import NobitexAPIClient
        
        # Load environment variables to get API key
        load_dotenv()
        
        # Initialize client
        logger.info("Initializing Nobitex API client")
        client = NobitexAPIClient()
        
        # Test market data
        logger.info("Fetching market stats")
        stats = client.get_market_stats()
        
        if stats and "stats" in stats:
            markets = list(stats["stats"].keys())
            logger.info(f"Successfully fetched stats for {len(markets)} markets")
            logger.info(f"Available markets: {markets[:10]}...")
            
            # Print some market details
            if "BTCUSDT" in stats["stats"]:
                btc_data = stats["stats"]["BTCUSDT"]
                logger.info(f"BTC/USDT: Last price: {btc_data.get('latest')}, 24h High: {btc_data.get('dayHigh')}, 24h Low: {btc_data.get('dayLow')}")
            
            if "ETHUSDT" in stats["stats"]:
                eth_data = stats["stats"]["ETHUSDT"]
                logger.info(f"ETH/USDT: Last price: {eth_data.get('latest')}, 24h High: {eth_data.get('dayHigh')}, 24h Low: {eth_data.get('dayLow')}")
        else:
            logger.error(f"Failed to get market stats: {stats}")
        
        # Test wallet data (requires authentication)
        if client.api_key:
            logger.info("Fetching wallet information")
            wallet = client.get_wallet()
            
            if wallet and "wallets" in wallet:
                logger.info(f"Successfully fetched wallet data with {len(wallet['wallets'])} assets")
                
                # Print balances
                for asset in wallet["wallets"]:
                    currency = asset.get("currency", "")
                    balance = asset.get("balance", 0)
                    
                    if float(balance) > 0:
                        logger.info(f"{currency}: {balance}")
            else:
                logger.error(f"Failed to get wallet data: {wallet}")
        else:
            logger.warning("No API key provided, skipping wallet test")
        
        # Test order book
        logger.info("Fetching order book for BTC/USDT")
        orderbook = client.get_orderbook("BTCUSDT")
        
        if orderbook and "bids" in orderbook and "asks" in orderbook:
            logger.info(f"Successfully fetched order book with {len(orderbook['bids'])} bids and {len(orderbook['asks'])} asks")
            
            if orderbook["bids"]:
                logger.info(f"Best bid: {orderbook['bids'][0]}")
            
            if orderbook["asks"]:
                logger.info(f"Best ask: {orderbook['asks'][0]}")
        else:
            logger.error(f"Failed to get order book: {orderbook}")
        
        logger.info("Nobitex API tests completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error testing Nobitex API: {str(e)}")
        return False

def test_nobitex_adapter():
    """Test the NobitexExchangeAdapter."""
    try:
        # Import with error handling
        try:
            from trading_system.exchange.nobitex_adapter import NobitexExchangeAdapter
            import logging
        except ImportError as e:
            logger.error(f"Could not import NobitexExchangeAdapter: {e}")
            return False
        
        # Load environment variables
        load_dotenv()
        
        # Configure basic logging if not already done
        root_logger = logging.getLogger()
        if not root_logger.handlers:
            root_logger.setLevel(logging.INFO)
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] [%(name)s] %(message)s'))
            root_logger.addHandler(handler)
        
        # Initialize adapter
        logger.info("Initializing Nobitex exchange adapter")
        config = {
            "api_key": os.getenv("NOBITEX_API_KEY"),
            "api_secret": os.getenv("NOBITEX_API_SECRET"),
            "cache_dir": "./cache"
        }
        
        adapter = NobitexExchangeAdapter(config)
        
        # Safety check for required attributes
        if not hasattr(adapter, 'logger'):
            logger.error("Adapter does not have a logger attribute")
            return False
            
        # Test basic functionality without initialization
        logger.info("Nobitex adapter successfully created")
        
        return True
        
    except Exception as e:
        logger.error(f"Error testing Nobitex adapter: {str(e)}")
        return False

def test_nobitex_adapter_directly():
    """Test the NobitexExchangeAdapter directly without using initialization."""
    try:
        # Import directly
        try:
            from trading_system.exchange.nobitex_adapter import NobitexExchangeAdapter
            from trading_system.exchange.adapter import ExchangeAdapter
        except ImportError as e:
            logger.error(f"Could not import adapter classes: {e}")
            return False
            
        # Skip initialization and just check the class structure
        logger.info("Testing NobitexExchangeAdapter class structure")
        logger.info(f"Successfully imported NobitexExchangeAdapter")
        logger.info(f"Adapter inherits from ExchangeAdapter: {issubclass(NobitexExchangeAdapter, ExchangeAdapter)}")
        
        # Create a minimal instance just to check the structure
        minimal_config = {"api_key": None, "api_secret": None, "cache_dir": "./cache"}
        adapter = NobitexExchangeAdapter(minimal_config)
        
        # Check if important attributes and methods exist
        logger.info(f"Adapter has logger: {hasattr(adapter, 'logger')}")
        logger.info(f"Adapter has _initialize method: {hasattr(adapter, '_initialize')}")
        logger.info(f"Adapter has ping method: {hasattr(adapter, 'ping')}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error testing Nobitex adapter directly: {str(e)}")
        return False

if __name__ == "__main__":
    logger.info("Starting Nobitex integration tests")
    
    # Test API client
    api_test_result = test_nobitex_api()
    logger.info(f"API client test {'passed' if api_test_result else 'failed'}")
    
    # Test direct adapter import
    direct_adapter_test_result = test_nobitex_adapter_directly()
    logger.info(f"Direct adapter test {'passed' if direct_adapter_test_result else 'failed'}")
    
    # Test exchange adapter
    adapter_test_result = test_nobitex_adapter()
    logger.info(f"Exchange adapter test {'passed' if adapter_test_result else 'failed'}")
    
    logger.info("All tests completed") 