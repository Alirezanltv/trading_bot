"""
RSI Strategy Implementation

This module implements a Relative Strength Index (RSI) based trading strategy.
"""

import asyncio
import math
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

from core.logging import get_logger
from strategy.base import Strategy, SignalType, SignalConfidence


class RSIStrategy(Strategy):
    """
    RSI-based trading strategy.
    
    Generates buy signals when RSI is oversold and sell signals when it's overbought.
    Implements trend filtering and signal smoothing for better performance.
    """
    
    def __init__(self, config: Dict[str, Any], name: Optional[str] = None):
        """
        Initialize RSI strategy.
        
        Args:
            config: Strategy configuration
            name: Human-readable strategy name
        """
        # Call parent constructor
        super().__init__(config, name or "RSI Strategy")
        
        # Get parameters from config
        parameters = config.get("parameters", {})
        
        # RSI parameters
        self.rsi_period = parameters.get("rsi_period", 14)
        self.overbought_threshold = parameters.get("overbought_threshold", 70)
        self.oversold_threshold = parameters.get("oversold_threshold", 30)
        self.exit_threshold = parameters.get("exit_threshold", 50)
        self.signal_smoothing = parameters.get("signal_smoothing", 2)
        
        # Initialize logger
        self.logger = get_logger(f"strategy.rsi")
        
        # Log initialization
        self.logger.info(
            f"RSI Strategy initialized with parameters: "
            f"period={self.rsi_period}, "
            f"overbought={self.overbought_threshold}, "
            f"oversold={self.oversold_threshold}, "
            f"exit={self.exit_threshold}, "
            f"smoothing={self.signal_smoothing}"
        )
        
        # Initialize state for each symbol
        self.state["prices"] = {}
        self.state["rsi_values"] = {}
        self.state["signal_counts"] = {}
    
    def _calculate_rsi(self, price_changes: List[float]) -> float:
        """
        Calculate Relative Strength Index.
        
        Args:
            price_changes: List of price changes
            
        Returns:
            RSI value (0-100)
        """
        if len(price_changes) < self.rsi_period:
            return 50.0  # Default to neutral if not enough data
        
        # Get the most recent period of changes
        changes = price_changes[-self.rsi_period:]
        
        # Calculate gains and losses
        gains = [max(0, change) for change in changes]
        losses = [max(0, -change) for change in changes]
        
        # Calculate average gain and loss
        avg_gain = sum(gains) / self.rsi_period
        avg_loss = sum(losses) / self.rsi_period
        
        # Calculate RS
        if avg_loss == 0:
            return 100.0  # All gains, no losses
        
        rs = avg_gain / avg_loss
        
        # Calculate RSI
        rsi = 100.0 - (100.0 / (1.0 + rs))
        
        return rsi
    
    async def _update_indicators(self, symbol: str, timeframe: str, market_data: Dict[str, Any]) -> None:
        """
        Update RSI indicator for a symbol.
        
        Args:
            symbol: Trading symbol
            timeframe: Chart timeframe
            market_data: Market data
        """
        # Get current price
        current_price = market_data.get("last", 0.0)
        if current_price <= 0:
            return
        
        # Get price history for this symbol and timeframe
        key = f"{symbol}_{timeframe}"
        
        # Initialize price history if needed
        if key not in self.state["prices"]:
            self.state["prices"][key] = []
            self.state["rsi_values"][key] = []
            self.state["signal_counts"][key] = {
                SignalType.BUY.value: 0,
                SignalType.SELL.value: 0,
                SignalType.EXIT.value: 0,
                SignalType.NEUTRAL.value: 0
            }
        
        # Add current price to history
        self.state["prices"][key].append(current_price)
        
        # Keep only the necessary history for RSI calculation
        history_needed = max(100, self.rsi_period * 3)  # Keep more for trend analysis
        if len(self.state["prices"][key]) > history_needed:
            self.state["prices"][key] = self.state["prices"][key][-history_needed:]
        
        # Calculate price changes
        price_history = self.state["prices"][key]
        if len(price_history) < 2:
            return
        
        price_changes = [price_history[i] - price_history[i-1] for i in range(1, len(price_history))]
        
        # Calculate RSI
        rsi = self._calculate_rsi(price_changes)
        
        # Add RSI to history
        self.state["rsi_values"][key].append(rsi)
        
        # Keep only recent RSI values
        if len(self.state["rsi_values"][key]) > history_needed:
            self.state["rsi_values"][key] = self.state["rsi_values"][key][-history_needed:]
    
    async def generate_signal(self, symbol: str, timeframe: str) -> Tuple[SignalType, float, Optional[Dict[str, Any]]]:
        """
        Generate trading signal based on RSI.
        
        Args:
            symbol: Trading symbol
            timeframe: Chart timeframe
            
        Returns:
            Signal type, confidence, and metadata
        """
        # Check if we have enough data
        key = f"{symbol}_{timeframe}"
        if key not in self.state["rsi_values"] or len(self.state["rsi_values"][key]) < 3:
            return SignalType.NEUTRAL, 0.0, None
        
        # Get current RSI and price
        rsi_values = self.state["rsi_values"][key]
        current_rsi = rsi_values[-1]
        
        # Get price for metadata
        prices = self.state["prices"][key]
        current_price = prices[-1] if prices else 0.0
        
        # Calculate RSI trend
        rsi_trend = 0
        if len(rsi_values) >= 3:
            # Simple trend calculation
            if rsi_values[-1] > rsi_values[-2] and rsi_values[-2] > rsi_values[-3]:
                rsi_trend = 1  # Uptrend
            elif rsi_values[-1] < rsi_values[-2] and rsi_values[-2] < rsi_values[-3]:
                rsi_trend = -1  # Downtrend
        
        # Get signal counts
        signal_counts = self.state["signal_counts"][key]
        
        # Determine signal type
        signal_type = SignalType.NEUTRAL
        confidence = 0.0
        metadata = None
        
        # Check for buy signal
        if current_rsi <= self.oversold_threshold and rsi_trend >= 0:
            # RSI is oversold and trend is flat or up
            signal_type = SignalType.BUY
            
            # Calculate confidence based on how oversold it is
            oversold_range = self.oversold_threshold  # e.g., 30
            confidence = min(0.9, (self.oversold_threshold - current_rsi) / oversold_range + 0.5)
            
            metadata = {
                "rsi": current_rsi,
                "trend": "up" if rsi_trend > 0 else "flat",
                "price": current_price,
                "reason": "RSI oversold condition"
            }
            
        # Check for sell signal
        elif current_rsi >= self.overbought_threshold and rsi_trend <= 0:
            # RSI is overbought and trend is flat or down
            signal_type = SignalType.SELL
            
            # Calculate confidence based on how overbought it is
            overbought_range = 100 - self.overbought_threshold  # e.g., 30
            confidence = min(0.9, (current_rsi - self.overbought_threshold) / overbought_range + 0.5)
            
            metadata = {
                "rsi": current_rsi,
                "trend": "down" if rsi_trend < 0 else "flat",
                "price": current_price,
                "reason": "RSI overbought condition"
            }
            
        # Check for exit signal
        elif (
            (current_rsi >= self.exit_threshold and signal_counts.get(SignalType.BUY.value, 0) > 0) or
            (current_rsi <= self.exit_threshold and signal_counts.get(SignalType.SELL.value, 0) > 0)
        ):
            signal_type = SignalType.EXIT
            
            # Calculate confidence based on distance from exit threshold
            distance_from_exit = abs(current_rsi - self.exit_threshold) / 50.0  # Normalize to 0-1 range
            confidence = min(0.8, 0.5 + distance_from_exit)
            
            metadata = {
                "rsi": current_rsi,
                "price": current_price,
                "reason": "RSI crossed exit threshold"
            }
        
        # Apply signal smoothing if enabled
        if self.signal_smoothing > 0 and signal_type != SignalType.NEUTRAL:
            # Count how many times we've seen this signal type
            signal_counts[signal_type.value] += 1
            
            # Only return signal if we've seen it enough times
            if signal_counts[signal_type.value] < self.signal_smoothing:
                # Not enough signal repetitions yet
                signal_type = SignalType.NEUTRAL
                confidence = 0.0
                metadata = None
            else:
                # Reset counter for other signal types
                for signal_key in signal_counts:
                    if signal_key != signal_type.value:
                        signal_counts[signal_key] = 0
        
        return signal_type, confidence, metadata 