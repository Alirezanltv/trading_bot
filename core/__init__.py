"""
Core module for Trading System

This module contains the basic components and utilities used throughout the trading system.
"""

# Import components
from .component import Component, ComponentStatus

# Import other core utilities
from .logging import initialize_logging as setup_logging, get_logger
from .config import ConfigManager
from .message_bus import MessageBus
from .rabbitmq_broker import DeliveryMode
# from .event import Event, EventType  # Comment out as this module doesn't exist

from enum import Enum, auto

class MessageTypes(Enum):
    """Message types used in the message bus."""
    MARKET_DATA = auto()
    TICKER = auto()
    CANDLE = auto()
    TRADE = auto()
    ORDERBOOK = auto()
    SIGNAL = auto()
    ORDER = auto()
    EXECUTION = auto()
    ERROR = auto()
    INFO = auto()
    SYSTEM = auto() 