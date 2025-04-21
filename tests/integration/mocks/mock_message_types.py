"""
Mock Message Types

This module defines message types used in the integration tests.
"""

from enum import Enum, auto


class MessageTypes(Enum):
    """Message types used in the trading system."""
    
    # Core system messages
    SYSTEM_NOTIFICATION = auto()
    SYSTEM_STATUS = auto()
    SYSTEM_COMMAND = auto()
    COMPONENT_STATUS = auto()
    
    # Market data messages
    MARKET_DATA_TICKER_UPDATE = auto()
    MARKET_DATA_ORDERBOOK_UPDATE = auto()
    MARKET_DATA_TRADE_UPDATE = auto()
    MARKET_DATA_BAR_UPDATE = auto()
    MARKET_DATA_INSTRUMENT_INFO = auto()
    
    # Strategy messages
    STRATEGY_SIGNAL = auto()
    
    # Order messages
    ORDER_REQUEST = auto()
    ORDER_VALIDATED = auto()
    ORDER_REJECTED = auto()
    ORDER_UPDATE = auto()
    ORDER_CANCEL_REQUEST = auto()
    FILL = auto()
    
    # Position and portfolio messages
    POSITION_UPDATE = auto()
    PORTFOLIO_UPDATE = auto()
    
    # Risk messages
    RISK_NOTIFICATION = auto()
    RISK_LIMIT_UPDATE = auto()
    
    # Custom test messages
    TEST_EVENT = auto()
    TEST_RESULT = auto()


# Export the MessageTypes class at module level
__all__ = ['MessageTypes'] 