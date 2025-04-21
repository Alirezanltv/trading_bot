"""
Base Exchange Adapter

This module defines the base class for exchange adapters that provide
a consistent interface for interacting with different exchanges.
"""

import logging
import time
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Tuple

from trading_system.models.order import Order
from trading_system.models.position import Position

logger = logging.getLogger(__name__)


class ExchangeAdapter(ABC):
    """
    Base class for exchange adapters.
    
    Exchange adapters provide a common interface for interacting with different exchanges,
    allowing the trading system to be exchange-agnostic.
    """
    
    def __init__(
        self, 
        exchange_id: str, 
        api_key: str = None, 
        api_secret: str = None,
        additional_params: Dict[str, Any] = None
    ):
        """
        Initialize exchange adapter.
        
        Args:
            exchange_id: Unique identifier for the exchange
            api_key: API key for authentication
            api_secret: API secret for authentication
            additional_params: Additional parameters for exchange-specific configuration
        """
        self.exchange_id = exchange_id
        self.api_key = api_key
        self.api_secret = api_secret
        self.additional_params = additional_params or {}
        self._connected = False
        self._last_connection_check = 0
        self._connection_check_interval = 60  # seconds
    
    @abstractmethod
    def connect(self) -> bool:
        """
        Connect to the exchange.
        
        Returns:
            True if connection successful, False otherwise
        """
        pass
    
    @abstractmethod
    def disconnect(self) -> bool:
        """
        Disconnect from the exchange.
        
        Returns:
            True if disconnection successful, False otherwise
        """
        pass
    
    def is_connected(self) -> bool:
        """
        Check if the adapter is connected to the exchange.
        
        Returns:
            True if connected, False otherwise
        """
        current_time = time.time()
        if current_time - self._last_connection_check > self._connection_check_interval:
            self._connected = self._check_connection()
            self._last_connection_check = current_time
        
        return self._connected
    
    @abstractmethod
    def _check_connection(self) -> bool:
        """
        Check the connection status with the exchange.
        
        Returns:
            True if connected, False otherwise
        """
        pass
    
    @abstractmethod
    def validate_order(self, order: Order) -> Dict[str, Any]:
        """
        Validate an order before submission.
        
        Args:
            order: Order to validate
            
        Returns:
            Dictionary containing validation result:
            {
                "valid": bool,
                "message": str,
                "details": Dict[str, Any]  # Optional additional validation details
            }
        """
        pass
    
    @abstractmethod
    def submit_order(self, order: Order) -> Dict[str, Any]:
        """
        Submit an order to the exchange.
        
        Args:
            order: Order to submit
            
        Returns:
            Dictionary containing submission result:
            {
                "success": bool,
                "message": str,
                "exchange_order_id": str,
                "execution_id": str,
                "filled_quantity": float,
                "fill_price": float,
                "fees": float,
                "details": Dict[str, Any]  # Optional additional execution details
            }
        """
        pass
    
    @abstractmethod
    def cancel_order(self, order: Order) -> Dict[str, Any]:
        """
        Cancel an order on the exchange.
        
        Args:
            order: Order to cancel
            
        Returns:
            Dictionary containing cancellation result:
            {
                "success": bool,
                "message": str,
                "details": Dict[str, Any]  # Optional additional cancellation details
            }
        """
        pass
    
    @abstractmethod
    def check_order_status(self, order: Order) -> Dict[str, Any]:
        """
        Check the status of an order on the exchange.
        
        Args:
            order: Order to check
            
        Returns:
            Dictionary containing order status:
            {
                "success": bool,
                "message": str,
                "exists": bool,
                "status": str,
                "filled_quantity": float,
                "fill_price": float,
                "details": Dict[str, Any]  # Optional additional status details
            }
        """
        pass
    
    @abstractmethod
    def get_market_data(self, symbol: str, data_type: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Get market data from the exchange.
        
        Args:
            symbol: Symbol to get data for
            data_type: Type of data to get (e.g., "ticker", "orderbook", "trades")
            params: Additional parameters for the data request
            
        Returns:
            Dictionary containing market data:
            {
                "success": bool,
                "message": str,
                "data": Any,  # The requested market data
                "timestamp": float  # Timestamp of the data
            }
        """
        pass
    
    @abstractmethod
    def get_account_balance(self) -> Dict[str, Any]:
        """
        Get account balance from the exchange.
        
        Returns:
            Dictionary containing account balance:
            {
                "success": bool,
                "message": str,
                "balances": Dict[str, float],  # Asset -> Balance mapping
                "timestamp": float  # Timestamp of the data
            }
        """
        pass
    
    @abstractmethod
    def get_positions(self) -> Dict[str, Any]:
        """
        Get current positions from the exchange.
        
        Returns:
            Dictionary containing positions:
            {
                "success": bool,
                "message": str,
                "positions": List[Dict[str, Any]],  # List of position dictionaries
                "timestamp": float  # Timestamp of the data
            }
        """
        pass
    
    def get_exchange_info(self) -> Dict[str, Any]:
        """
        Get exchange information.
        
        Returns:
            Dictionary containing exchange information:
            {
                "exchange_id": str,
                "name": str,
                "supported_order_types": List[str],
                "trading_fees": Dict[str, float],
                "withdrawal_fees": Dict[str, float],
                "minimum_order_sizes": Dict[str, float],
                "price_precision": Dict[str, int],
                "quantity_precision": Dict[str, int]
            }
        """
        # Default implementation - subclasses should override with exchange-specific info
        return {
            "exchange_id": self.exchange_id,
            "name": self.exchange_id,
            "supported_order_types": ["market", "limit"],
            "trading_fees": {},
            "withdrawal_fees": {},
            "minimum_order_sizes": {},
            "price_precision": {},
            "quantity_precision": {}
        }
    
    def handle_error(self, error: Exception, context: str = "") -> Dict[str, Any]:
        """
        Handle an error.
        
        Args:
            error: Exception that occurred
            context: Context in which the error occurred
            
        Returns:
            Dictionary containing error information:
            {
                "success": False,
                "message": str,
                "error_type": str,
                "error_details": str
            }
        """
        error_msg = f"{context}: {str(error)}" if context else str(error)
        logger.error(f"Exchange adapter error: {error_msg}")
        return {
            "success": False,
            "message": error_msg,
            "error_type": type(error).__name__,
            "error_details": str(error)
        } 