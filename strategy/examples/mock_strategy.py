"""
Mock Strategy Module

This module provides a simplified mock strategy for testing without external dependencies.
"""

import os
import json
import logging
import asyncio
import random
from datetime import datetime
from typing import Dict, List, Any, Optional, Union

from trading_system.core.logging import get_logger
from trading_system.strategy.base import BaseStrategy, SignalType

logger = get_logger("mock_strategy")

class MockStrategy(BaseStrategy):
    """
    A simple mock strategy that generates random signals for testing.
    """
    
    def __init__(self, 
                name: str = "Mock_Strategy",
                symbols: List[str] = None,
                timeframes: List[str] = None,
                state_dir: str = "./data/strategy_state",
                performance_dir: str = "./data/strategy_performance",
                signal_probability: float = 0.2):
        """
        Initialize the mock strategy.
        
        Args:
            name: Strategy name
            symbols: List of symbols to monitor
            timeframes: List of timeframes to monitor
            state_dir: Directory for state persistence
            performance_dir: Directory for performance data
            signal_probability: Probability of generating a non-neutral signal (0-1)
        """
        super().__init__(
            name=name,
            symbols=symbols or ["BTC/USDT", "ETH/USDT"],
            timeframes=timeframes or ["1m", "5m", "15m"]
        )
        
        self.state_dir = state_dir
        self.performance_dir = performance_dir
        self.signal_probability = max(0.0, min(1.0, signal_probability))
        
        # Make directories if they don't exist
        os.makedirs(self.state_dir, exist_ok=True)
        os.makedirs(self.performance_dir, exist_ok=True)
        
        # Market data cache
        self.market_data_cache = {}
        
        # Last generated signals
        self.last_signals = {}
        
        logger.info(f"Mock strategy '{name}' initialized")
    
    async def initialize(self) -> bool:
        """
        Initialize the strategy.
        
        Returns:
            Initialization success
        """
        logger.info(f"Initializing mock strategy '{self.name}'")
        return True
    
    async def update(self, market_data: Dict[str, Any]) -> None:
        """
        Update the strategy with new market data.
        
        Args:
            market_data: Market data update
        """
        # Extract symbol and timeframe
        symbol = market_data.get("symbol")
        timeframe = market_data.get("timeframe")
        
        if not symbol or not timeframe:
            return
        
        # Update market data cache
        key = f"{symbol}_{timeframe}"
        self.market_data_cache[key] = market_data
        
        # Generate a new signal based on probability
        if random.random() < self.signal_probability:
            # Generate a random signal type (buy, sell, exit)
            signal_type = random.choice([SignalType.BUY, SignalType.SELL, SignalType.EXIT])
            
            # Generate a random confidence between 0.5 and 1.0
            confidence = 0.5 + (random.random() * 0.5)
            
            # Store the signal
            self.last_signals[key] = {
                "signal": signal_type,
                "confidence": confidence,
                "timestamp": datetime.now().timestamp()
            }
            
            logger.info(f"Generated {signal_type.value} signal for {symbol} {timeframe} with confidence {confidence:.2f}")
        else:
            # Generate a neutral signal
            self.last_signals[key] = {
                "signal": SignalType.NEUTRAL,
                "confidence": 0.0,
                "timestamp": datetime.now().timestamp()
            }
    
    async def get_signal(self, symbol: str, timeframe: str) -> Optional[Dict[str, Any]]:
        """
        Get the current signal for a symbol and timeframe.
        
        Args:
            symbol: Trading symbol
            timeframe: Chart timeframe
            
        Returns:
            Signal data or None if no signal
        """
        key = f"{symbol}_{timeframe}"
        
        # If we have a signal, format and return it
        if key in self.last_signals:
            signal_data = self.last_signals[key]
            
            # Only return non-neutral signals
            if signal_data["signal"] != SignalType.NEUTRAL:
                return {
                    "strategy": self.name,
                    "strategy_id": self.strategy_id,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "signal": signal_data["signal"].value,
                    "confidence": signal_data["confidence"],
                    "timestamp": signal_data["timestamp"],
                    "metadata": {
                        "reason": "Random mock signal for testing",
                        "generated_at": datetime.now().isoformat()
                    }
                }
        
        return None
    
    async def save_state(self) -> bool:
        """
        Save strategy state to disk.
        
        Returns:
            Save success
        """
        try:
            # Create a state object
            state = {
                "name": self.name,
                "strategy_id": self.strategy_id,
                "symbols": self.symbols,
                "timeframes": self.timeframes,
                "last_signals": {k: {
                    "signal": v["signal"].value if hasattr(v["signal"], "value") else v["signal"],
                    "confidence": v["confidence"],
                    "timestamp": v["timestamp"]
                } for k, v in self.last_signals.items()},
                "saved_at": datetime.now().isoformat()
            }
            
            # Save to file
            filename = os.path.join(self.state_dir, f"{self.strategy_id}.json")
            with open(filename, 'w') as f:
                json.dump(state, f, indent=2)
                
            return True
        except Exception as e:
            logger.error(f"Error saving strategy state: {e}")
            return False
    
    async def load_state(self) -> bool:
        """
        Load strategy state from disk.
        
        Returns:
            Load success
        """
        try:
            filename = os.path.join(self.state_dir, f"{self.strategy_id}.json")
            
            # Check if file exists
            if not os.path.exists(filename):
                return False
                
            # Load from file
            with open(filename, 'r') as f:
                state = json.load(f)
                
            # Update strategy state
            self.last_signals = {k: {
                "signal": SignalType(v["signal"]) if isinstance(v["signal"], str) else SignalType(v["signal"]),
                "confidence": v["confidence"],
                "timestamp": v["timestamp"]
            } for k, v in state.get("last_signals", {}).items()}
            
            return True
        except Exception as e:
            logger.error(f"Error loading strategy state: {e}")
            return False 