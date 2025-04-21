"""
Exchange adapter interface for high-reliability trading system.

This module defines the interface for exchange adapters and common data types.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Union, Tuple
from dataclasses import dataclass
import time
from enum import Enum


@dataclass
class OrderResult:
    """Result of an order operation."""
    success: bool
    exchange_order_id: Optional[str] = None
    client_order_id: Optional[str] = None
    status: Optional[str] = None
    filled_quantity: float = 0.0
    average_price: Optional[float] = None
    timestamp: int = 0
    error: Optional[str] = None
    raw_response: Optional[Dict[str, Any]] = None


@dataclass
class OrderInfo:
    """Information about an order."""
    exchange_order_id: str
    client_order_id: Optional[str]
    symbol: str
    side: str
    order_type: str
    quantity: float
    price: Optional[float]
    stop_price: Optional[float]
    filled_quantity: float
    average_price: Optional[float]
    status: str
    time_in_force: Optional[str]
    created_time: int
    updated_time: int
    error: Optional[str] = None
    raw_data: Optional[Dict[str, Any]] = None


@dataclass
class MarketInfo:
    """Information about a market/trading pair."""
    symbol: str
    base_asset: str
    quote_asset: str
    min_price: float
    max_price: float
    price_increment: float
    min_quantity: float
    max_quantity: float
    quantity_increment: float
    min_notional: float
    status: str
    raw_data: Optional[Dict[str, Any]] = None


class ExchangeAdapter(ABC):
    """
    Abstract interface for exchange adapters.
    
    Exchange adapters are responsible for:
    - Connecting to exchanges
    - Implementing exchange-specific API calls
    - Translating between exchange-specific formats and common formats
    - Error handling and retry logic
    """
    
    def __init__(self, name: str, config: Dict[str, Any]):
        """
        Initialize the exchange adapter.
        
        Args:
            name: Exchange name
            config: Configuration parameters
        """
        self.name = name
        self.config = config
        
        # Initialize connection
        self._initialize()
    
    @abstractmethod
    def _initialize(self) -> None:
        """Initialize exchange connection."""
        pass
    
    @abstractmethod
    def get_markets(self) -> Dict[str, MarketInfo]:
        """
        Get all available markets.
        
        Returns:
            Dict of symbol -> MarketInfo
        """
        pass
    
    @abstractmethod
    def get_ticker(self, symbol: str) -> Dict[str, Any]:
        """
        Get current ticker data for a symbol.
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            Dict containing ticker data
        """
        pass
    
    @abstractmethod
    def get_order_book(self, symbol: str, limit: int = 20) -> Dict[str, Any]:
        """
        Get order book for a symbol.
        
        Args:
            symbol: Trading pair symbol
            limit: Maximum number of bids/asks to return
            
        Returns:
            Dict containing order book data
        """
        pass
    
    @abstractmethod
    def get_recent_trades(self, symbol: str, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get recent trades for a symbol.
        
        Args:
            symbol: Trading pair symbol
            limit: Maximum number of trades to return
            
        Returns:
            List of trades
        """
        pass
    
    @abstractmethod
    def get_candles(self, symbol: str, interval: str, start_time: Optional[int] = None, 
                   end_time: Optional[int] = None, limit: int = 500) -> List[Dict[str, Any]]:
        """
        Get candlestick data for a symbol.
        
        Args:
            symbol: Trading pair symbol
            interval: Candlestick interval (e.g. '1m', '1h', '1d')
            start_time: Start time in milliseconds
            end_time: End time in milliseconds
            limit: Maximum number of candles to return
            
        Returns:
            List of candles
        """
        pass
    
    @abstractmethod
    def get_balances(self) -> Dict[str, Dict[str, float]]:
        """
        Get account balances.
        
        Returns:
            Dict of asset -> balance info (free, locked, total)
        """
        pass
    
    @abstractmethod
    def place_order(self, symbol: str, side: str, order_type: str, quantity: float,
                   price: Optional[float] = None, stop_price: Optional[float] = None,
                   time_in_force: str = 'GTC', client_order_id: Optional[str] = None,
                   **kwargs) -> OrderResult:
        """
        Place an order.
        
        Args:
            symbol: Trading pair symbol
            side: Order side ('buy' or 'sell')
            order_type: Order type ('market', 'limit', etc.)
            quantity: Order quantity
            price: Order price (for limit orders)
            stop_price: Stop price (for stop orders)
            time_in_force: Time in force ('GTC', 'IOC', 'FOK')
            client_order_id: Client-generated order ID
            **kwargs: Additional exchange-specific parameters
            
        Returns:
            OrderResult containing result of order placement
        """
        pass
    
    @abstractmethod
    def cancel_order(self, symbol: str, order_id: Optional[str] = None, 
                    client_order_id: Optional[str] = None) -> OrderResult:
        """
        Cancel an order.
        
        Args:
            symbol: Trading pair symbol
            order_id: Exchange order ID
            client_order_id: Client order ID
            
        Returns:
            OrderResult containing result of cancellation
        """
        pass
    
    @abstractmethod
    def get_order(self, symbol: str, order_id: Optional[str] = None,
                 client_order_id: Optional[str] = None) -> OrderResult:
        """
        Get order information.
        
        Args:
            symbol: Trading pair symbol
            order_id: Exchange order ID
            client_order_id: Client order ID
            
        Returns:
            OrderResult containing order information
        """
        pass
    
    @abstractmethod
    def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderInfo]:
        """
        Get all open orders.
        
        Args:
            symbol: Trading pair symbol (if None, get all open orders)
            
        Returns:
            List of OrderInfo
        """
        pass
    
    @abstractmethod
    def get_order_history(self, symbol: Optional[str] = None, 
                         limit: int = 100) -> List[OrderInfo]:
        """
        Get order history.
        
        Args:
            symbol: Trading pair symbol (if None, get all order history)
            limit: Maximum number of orders to return
            
        Returns:
            List of OrderInfo
        """
        pass
    
    @abstractmethod
    def get_trade_history(self, symbol: Optional[str] = None, 
                         limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get trade history.
        
        Args:
            symbol: Trading pair symbol (if None, get all trade history)
            limit: Maximum number of trades to return
            
        Returns:
            List of trades
        """
        pass
    
    @abstractmethod
    def get_server_time(self) -> int:
        """
        Get exchange server time.
        
        Returns:
            Server time in milliseconds
        """
        pass
    
    @abstractmethod
    def ping(self) -> bool:
        """
        Ping exchange to check connectivity.
        
        Returns:
            True if successful, False otherwise
        """
        pass 