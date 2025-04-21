"""
Legacy bridge module to connect existing implementation with the new architecture.
"""

import os
import time
import json
import dotenv
from typing import Dict, Any, List, Optional, Union, Tuple

from trading_system.core import get_logger, api_key_manager
from trading_system.exchanges import NobitexExchangeAdapter
from trading_system.market_data.tradingview import TradingViewDataProvider, TradingViewCredentials

logger = get_logger("adapters.legacy_bridge")

class LegacyBridge:
    """
    Bridge between existing code and new architecture.
    
    This class provides compatibility with the existing codebase while
    leveraging the new high-reliability architecture.
    """
    
    def __init__(self):
        """Initialize the legacy bridge."""
        # Load environment variables
        self._load_env_variables()
        
        # Initialize connections to external services
        self._init_api_keys()
        
        # Create adapter instances
        self.nobitex_adapter = None
        self.tradingview_provider = None
        
    def _load_env_variables(self):
        """Load environment variables from .env file."""
        try:
            # Load .env file
            dotenv.load_dotenv()
            
            # Required API credentials
            self.nobitex_api_key = os.getenv("NOBITEX_API_KEY")
            self.nobitex_api_secret = os.getenv("NOBITEX_API_SECRET")
            self.tradingview_secret = os.getenv("TRADINGVIEW_SECRET")
            
            # Trading parameters
            self.min_profit_threshold = float(os.getenv("MIN_PROFIT_THRESHOLD", 1.5))
            self.max_spread_pct = float(os.getenv("MAX_SPREAD_PCT", 0.3))
            self.position_size = float(os.getenv("POSITION_SIZE", 0.15))
            self.stop_loss_pct = float(os.getenv("STOP_LOSS_PCT", 0.8))
            self.take_profit_pct = float(os.getenv("TAKE_PROFIT_PCT", 2.5))
            self.min_24h_volume = float(os.getenv("MIN_24H_VOLUME", 1000000000))
            
            logger.info("Environment variables loaded successfully")
            
        except Exception as e:
            logger.error(f"Error loading environment variables: {str(e)}", exc_info=True)
            raise
    
    def _init_api_keys(self):
        """Initialize API keys in the key manager."""
        try:
            # Add Nobitex API keys
            api_key_manager.set_keys(
                exchange="nobitex",
                api_key=self.nobitex_api_key,
                secret_key=self.nobitex_api_secret,
                additional_params={
                    "cache_dir": "./cache",
                    "api_timeout": 15
                }
            )
            
            logger.info("API keys initialized in key manager")
            
        except Exception as e:
            logger.error(f"Error initializing API keys: {str(e)}", exc_info=True)
            raise
    
    def initialize(self) -> bool:
        """Initialize the legacy bridge."""
        try:
            # Create and initialize Nobitex adapter
            self.nobitex_adapter = NobitexExchangeAdapter()
            if not self.nobitex_adapter.initialize():
                logger.error("Failed to initialize Nobitex adapter")
                return False
            
            # Create and initialize TradingView provider
            # Note: TradingView initialization is async, so we'll do it lazily
            credentials = TradingViewCredentials(session_token=self.tradingview_secret)
            self.tradingview_provider = TradingViewDataProvider(credentials)
            
            logger.info("Legacy bridge initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing legacy bridge: {str(e)}", exc_info=True)
            return False
    
    def get_wallet_balance(self, currency: str = 'rls') -> float:
        """
        Get wallet balance for a currency.
        
        Args:
            currency: Currency code (default: 'rls')
            
        Returns:
            Balance amount
        """
        try:
            if not self.nobitex_adapter:
                logger.error("Nobitex adapter not initialized")
                return 0.0
            
            account_balance = self.nobitex_adapter.get_account_balance()
            balances = account_balance.get('balances', {})
            
            currency_balance = balances.get(currency.lower(), {})
            return float(currency_balance.get('free', 0.0))
            
        except Exception as e:
            logger.error(f"Error getting wallet balance: {str(e)}", exc_info=True)
            return 0.0
    
    def get_market_price(self, symbol: str) -> float:
        """
        Get current market price for a symbol.
        
        Args:
            symbol: Trading pair symbol (e.g., 'btc-rls')
            
        Returns:
            Current market price
        """
        try:
            if not self.nobitex_adapter:
                logger.error("Nobitex adapter not initialized")
                return 0.0
            
            ticker = self.nobitex_adapter.get_ticker(symbol)
            return float(ticker.get('last', 0.0))
            
        except Exception as e:
            logger.error(f"Error getting market price: {str(e)}", exc_info=True)
            return 0.0
    
    def place_order(self, symbol: str, side: str, quantity: float, price: float = None) -> Dict[str, Any]:
        """
        Place an order on Nobitex.
        
        Args:
            symbol: Trading pair symbol (e.g., 'btc-rls')
            side: Order side ('buy' or 'sell')
            quantity: Order quantity
            price: Order price (optional for market orders)
            
        Returns:
            Order information
        """
        try:
            if not self.nobitex_adapter:
                logger.error("Nobitex adapter not initialized")
                return {}
            
            # Map order parameters to adapter format
            from trading_system.core import OrderType, OrderSide
            
            order_type = OrderType.MARKET if price is None else OrderType.LIMIT
            order_side = OrderSide.BUY if side.lower() == 'buy' else OrderSide.SELL
            
            # Apply precision to quantity and price
            quantity = self.nobitex_adapter.convert_amount_to_precision(symbol, quantity)
            if price is not None:
                price = self.nobitex_adapter.convert_price_to_precision(symbol, price)
            
            # Place order
            order_info = self.nobitex_adapter.create_order(
                symbol=symbol,
                order_type=order_type,
                side=order_side,
                amount=quantity,
                price=price
            )
            
            return order_info
            
        except Exception as e:
            logger.error(f"Error placing order: {str(e)}", exc_info=True)
            return {}
    
    def get_order_status(self, order_id: str) -> Dict[str, Any]:
        """
        Get order status.
        
        Args:
            order_id: Order ID
            
        Returns:
            Order status information
        """
        try:
            if not self.nobitex_adapter:
                logger.error("Nobitex adapter not initialized")
                return {}
            
            return self.nobitex_adapter.get_order(order_id)
            
        except Exception as e:
            logger.error(f"Error getting order status: {str(e)}", exc_info=True)
            return {}
    
    def cancel_order(self, order_id: str) -> bool:
        """
        Cancel an order.
        
        Args:
            order_id: Order ID
            
        Returns:
            Success status
        """
        try:
            if not self.nobitex_adapter:
                logger.error("Nobitex adapter not initialized")
                return False
            
            result = self.nobitex_adapter.cancel_order(order_id)
            return result.get('success', False)
            
        except Exception as e:
            logger.error(f"Error cancelling order: {str(e)}", exc_info=True)
            return False
    
    def get_open_orders(self, symbol: str = None) -> List[Dict[str, Any]]:
        """
        Get open orders.
        
        Args:
            symbol: Trading pair symbol (optional)
            
        Returns:
            List of open orders
        """
        try:
            if not self.nobitex_adapter:
                logger.error("Nobitex adapter not initialized")
                return []
            
            return self.nobitex_adapter.get_open_orders(symbol)
            
        except Exception as e:
            logger.error(f"Error getting open orders: {str(e)}", exc_info=True)
            return []
    
    def get_market_candles(self, symbol: str, resolution: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get historical market candles.
        
        Args:
            symbol: Trading pair symbol (e.g., 'btc-rls')
            resolution: Candle resolution ('1m', '5m', '1h', '1d', etc.)
            limit: Number of candles to return
            
        Returns:
            List of candles
        """
        try:
            if not self.nobitex_adapter:
                logger.error("Nobitex adapter not initialized")
                return []
            
            return self.nobitex_adapter.get_historical_ohlcv(
                symbol=symbol,
                timeframe=resolution,
                limit=limit
            )
            
        except Exception as e:
            logger.error(f"Error getting market candles: {str(e)}", exc_info=True)
            return []


# Global legacy bridge instance
legacy_bridge = LegacyBridge() 