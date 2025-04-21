"""
Base Strategy Module

This module provides the base classes for implementing trading strategies.
"""

import uuid
from enum import Enum
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass
from datetime import datetime

class SignalType(Enum):
    """Enum for strategy signal types."""
    BUY = "buy"         # Buy signal (enter long position)
    SELL = "sell"       # Sell signal (enter short position)
    EXIT = "exit"       # Exit signal (close position)
    NEUTRAL = "neutral" # Neutral signal (no action)

class BaseStrategy:
    """
    Base class for all trading strategies.
    
    All strategy implementations should inherit from this class
    and implement the required methods.
    """
    
    def __init__(self, 
                name: str,
                symbols: List[str] = None,
                timeframes: List[str] = None):
        """
        Initialize base strategy.
        
        Args:
            name: Strategy name
            symbols: List of symbols to monitor
            timeframes: List of timeframes to monitor
        """
        self.name = name
        self.strategy_id = str(uuid.uuid4())
        self.symbols = symbols or []
        self.timeframes = timeframes or []
    
    async def initialize(self) -> bool:
        """
        Initialize the strategy.
        
        Returns:
            Initialization success
        """
        raise NotImplementedError("Subclasses must implement initialize()")
    
    async def update(self, market_data: Dict[str, Any]) -> None:
        """
        Update the strategy with new market data.
        
        Args:
            market_data: Market data update
        """
        raise NotImplementedError("Subclasses must implement update()")
    
    async def get_signal(self, symbol: str, timeframe: str) -> Optional[Dict[str, Any]]:
        """
        Get the current signal for a symbol and timeframe.
        
        Args:
            symbol: Trading symbol
            timeframe: Chart timeframe
            
        Returns:
            Signal data or None if no signal
        """
        raise NotImplementedError("Subclasses must implement get_signal()")
    
    async def save_state(self) -> bool:
        """
        Save strategy state to disk.
        
        Returns:
            Save success
        """
        raise NotImplementedError("Subclasses must implement save_state()")
    
    async def load_state(self) -> bool:
        """
        Load strategy state from disk.
        
        Returns:
            Load success
        """
        raise NotImplementedError("Subclasses must implement load_state()")

# Alias BaseStrategy as Strategy for compatibility
Strategy = BaseStrategy

@dataclass
class StrategySignal:
    """Strategy signal data class."""
    strategy_name: str
    symbol: str
    timeframe: str
    signal_type: SignalType
    timestamp: float
    confidence: float = 1.0  # Signal confidence level (0.0 to 1.0)
    price: Optional[float] = None
    volume: Optional[float] = None
    metadata: Dict[str, Any] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert signal to dictionary."""
        return {
            "strategy_name": self.strategy_name,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "signal_type": self.signal_type.value,
            "timestamp": self.timestamp,
            "confidence": self.confidence,
            "price": self.price,
            "volume": self.volume,
            "metadata": self.metadata or {}
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'StrategySignal':
        """Create signal from dictionary."""
        signal_type = data.get("signal_type")
        if isinstance(signal_type, str):
            signal_type = SignalType(signal_type)
            
        return cls(
            strategy_name=data.get("strategy_name", "unknown"),
            symbol=data.get("symbol", "unknown"),
            timeframe=data.get("timeframe", "unknown"),
            signal_type=signal_type,
            timestamp=data.get("timestamp", datetime.now().timestamp()),
            confidence=data.get("confidence", 1.0),
            price=data.get("price"),
            volume=data.get("volume"),
            metadata=data.get("metadata", {})
        )

@dataclass
class StrategyResult:
    """Strategy execution result."""
    strategy_name: str
    symbol: str
    timeframe: str
    signals: List[StrategySignal]
    timestamp: float
    execution_time: float
    success: bool
    error: Optional[str] = None
    metadata: Dict[str, Any] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "strategy_name": self.strategy_name,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "signals": [s.to_dict() for s in self.signals],
            "timestamp": self.timestamp,
            "execution_time": self.execution_time,
            "success": self.success,
            "error": self.error,
            "metadata": self.metadata or {}
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'StrategyResult':
        """Create result from dictionary."""
        signals_data = data.get("signals", [])
        signals = [StrategySignal.from_dict(s) for s in signals_data]
            
        return cls(
            strategy_name=data.get("strategy_name", "unknown"),
            symbol=data.get("symbol", "unknown"),
            timeframe=data.get("timeframe", "unknown"),
            signals=signals,
            timestamp=data.get("timestamp", datetime.now().timestamp()),
            execution_time=data.get("execution_time", 0.0),
            success=data.get("success", False),
            error=data.get("error"),
            metadata=data.get("metadata", {})
        )
