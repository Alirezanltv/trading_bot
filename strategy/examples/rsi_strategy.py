"""
RSI Strategy Example

This module provides an example implementation of a trading strategy
based on the Relative Strength Index (RSI) indicator.
"""

import asyncio
import json
import time
from typing import Dict, Any, Optional, List

import numpy as np
import pandas as pd
import talib

from trading_system.core.logging import get_logger
from trading_system.strategy.base import BaseStrategy, SignalType, SignalConfidence

logger = get_logger("strategy.examples.rsi")


class RSIStrategy(BaseStrategy):
    """
    RSI Strategy
    
    A strategy that generates buy signals when RSI is oversold and
    sell signals when RSI is overbought.
    
    Features:
    - Configurable RSI period and thresholds
    - Signal smoothing with confirmation bars
    - Optional trend filter
    - Configurable exit conditions
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the RSI strategy.
        
        Args:
            config: Strategy configuration
        """
        # Default configuration
        default_config = {
            "name": "RSI Strategy",
            "description": "Trades based on RSI oversold and overbought conditions",
            "version": "1.0.0",
            "rsi_period": 14,
            "overbought_threshold": 70,
            "oversold_threshold": 30,
            "exit_threshold": 50,
            "confirmation_bars": 1,
            "use_trend_filter": False,
            "trend_period": 200,
            "state_save_interval": 60,  # in seconds
            "performance_check_interval": 3600,  # in seconds
        }
        
        # Merge with provided config
        config = {**default_config, **(config or {})}
        
        # Call parent constructor
        super().__init__(config)
        
        # Extract specific RSI parameters
        self.rsi_period = self.config.get("rsi_period", 14)
        self.overbought_threshold = self.config.get("overbought_threshold", 70)
        self.oversold_threshold = self.config.get("oversold_threshold", 30)
        self.exit_threshold = self.config.get("exit_threshold", 50)
        self.confirmation_bars = self.config.get("confirmation_bars", 1)
        self.use_trend_filter = self.config.get("use_trend_filter", False)
        self.trend_period = self.config.get("trend_period", 200)
        
        # Initialize market data and indicators
        self.market_data = {}  # Symbol -> Timeframe -> DataFrame
        self.indicators = {}   # Symbol -> Timeframe -> Dict of indicators
        self.signals = {}      # Symbol -> Timeframe -> List of recent signals
        
        # Signal tracking
        self.last_signal_time = {}  # Symbol -> Timeframe -> Last signal time
        self.confirmation_count = {}  # Symbol -> Timeframe -> Confirmation count
        
        logger.info(
            f"Initialized {self.name} with RSI({self.rsi_period}), "
            f"Overbought: {self.overbought_threshold}, "
            f"Oversold: {self.oversold_threshold}, "
            f"Exit: {self.exit_threshold}"
        )
    
    async def initialize(self) -> bool:
        """
        Initialize the strategy.
        
        Returns:
            Success flag
        """
        # Call parent initialize method
        result = await super().initialize()
        
        # Log successful initialization
        if result:
            logger.info(f"Strategy {self.name} initialized successfully")
        
        return result
    
    def _update_market_data(self, symbol: str, timeframe: str, data: Dict[str, Any]) -> None:
        """
        Update market data for the given symbol and timeframe.
        
        Args:
            symbol: Trading symbol
            timeframe: Chart timeframe
            data: Market data point
        """
        # Initialize containers if needed
        if symbol not in self.market_data:
            self.market_data[symbol] = {}
            self.indicators[symbol] = {}
            self.signals[symbol] = {}
            self.last_signal_time[symbol] = {}
            self.confirmation_count[symbol] = {}
        
        if timeframe not in self.market_data[symbol]:
            self.market_data[symbol][timeframe] = pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])
            self.indicators[symbol][timeframe] = {"rsi": [], "sma": []}
            self.signals[symbol][timeframe] = []
            self.last_signal_time[symbol][timeframe] = 0
            self.confirmation_count[symbol][timeframe] = 0
        
        # Append data to DataFrame
        df = self.market_data[symbol][timeframe]
        
        # Check if we already have this candle
        if data["time"] in df["time"].values:
            # Update existing candle
            idx = df.index[df["time"] == data["time"]][0]
            df.loc[idx] = [
                data["time"],
                data["open"],
                data["high"],
                data["low"],
                data["close"],
                data["volume"]
            ]
        else:
            # Append new candle
            new_row = pd.DataFrame([[
                data["time"],
                data["open"],
                data["high"],
                data["low"],
                data["close"],
                data["volume"]
            ]], columns=df.columns)
            
            self.market_data[symbol][timeframe] = pd.concat([df, new_row], ignore_index=True)
        
        # Ensure data is sorted by time
        self.market_data[symbol][timeframe] = self.market_data[symbol][timeframe].sort_values("time").reset_index(drop=True)
        
        # Limit data size (keep last 1000 candles)
        max_candles = max(1000, self.rsi_period * 3, self.trend_period * 2)
        if len(self.market_data[symbol][timeframe]) > max_candles:
            self.market_data[symbol][timeframe] = self.market_data[symbol][timeframe].iloc[-max_candles:]
    
    def _calculate_indicators(self, symbol: str, timeframe: str) -> None:
        """
        Calculate indicators for the given symbol and timeframe.
        
        Args:
            symbol: Trading symbol
            timeframe: Chart timeframe
        """
        # Get market data
        df = self.market_data[symbol][timeframe]
        
        # Ensure we have enough data
        if len(df) < self.rsi_period + 1:
            return
        
        # Calculate RSI
        close_prices = df["close"].values
        rsi = talib.RSI(close_prices, timeperiod=self.rsi_period)
        
        # Calculate SMA for trend filter if enabled
        sma = None
        if self.use_trend_filter and len(df) >= self.trend_period:
            sma = talib.SMA(close_prices, timeperiod=self.trend_period)
        
        # Store indicators
        self.indicators[symbol][timeframe]["rsi"] = rsi
        self.indicators[symbol][timeframe]["sma"] = sma
    
    def _check_for_signal(self, symbol: str, timeframe: str) -> Optional[Dict[str, Any]]:
        """
        Check for new signals based on indicators.
        
        Args:
            symbol: Trading symbol
            timeframe: Chart timeframe
            
        Returns:
            Signal dictionary or None
        """
        # Get indicator values
        rsi_values = self.indicators[symbol][timeframe]["rsi"]
        sma_values = self.indicators[symbol][timeframe]["sma"]
        
        # Get market data
        df = self.market_data[symbol][timeframe]
        
        # Ensure we have enough data
        if len(rsi_values) < 2 or pd.isna(rsi_values[-1]):
            return None
        
        # Get current and previous RSI values
        current_rsi = rsi_values[-1]
        previous_rsi = rsi_values[-2]
        
        # Get current price
        current_price = df["close"].iloc[-1]
        
        # Check for signal
        signal_type = None
        confidence = SignalConfidence.LOW
        
        # Check trend if filter is enabled
        trend_up = True
        if self.use_trend_filter and sma_values is not None and not pd.isna(sma_values[-1]):
            trend_up = current_price > sma_values[-1]
        
        # Oversold condition (buy signal)
        if (previous_rsi < self.oversold_threshold and current_rsi > self.oversold_threshold and 
            (not self.use_trend_filter or trend_up)):
            
            signal_type = SignalType.BUY
            
            # Determine confidence level
            if current_rsi > self.oversold_threshold + 5:
                confidence = SignalConfidence.HIGH
            elif current_rsi > self.oversold_threshold + 2:
                confidence = SignalConfidence.MEDIUM
            
            # Update confirmation count
            self.confirmation_count[symbol][timeframe] += 1
            
            # Check if we have enough confirmation bars
            if self.confirmation_count[symbol][timeframe] >= self.confirmation_bars:
                self.confirmation_count[symbol][timeframe] = 0
            else:
                signal_type = None  # Not enough confirmation
        
        # Overbought condition (sell signal)
        elif (previous_rsi > self.overbought_threshold and current_rsi < self.overbought_threshold):
            
            signal_type = SignalType.SELL
            
            # Determine confidence level
            if current_rsi < self.overbought_threshold - 5:
                confidence = SignalConfidence.HIGH
            elif current_rsi < self.overbought_threshold - 2:
                confidence = SignalConfidence.MEDIUM
            
            # Update confirmation count
            self.confirmation_count[symbol][timeframe] += 1
            
            # Check if we have enough confirmation bars
            if self.confirmation_count[symbol][timeframe] >= self.confirmation_bars:
                self.confirmation_count[symbol][timeframe] = 0
            else:
                signal_type = None  # Not enough confirmation
        
        # Exit condition
        elif (
            # Exit long position if RSI crosses above exit threshold
            (previous_rsi < self.exit_threshold and current_rsi > self.exit_threshold) or
            # Exit short position if RSI crosses below exit threshold
            (previous_rsi > self.exit_threshold and current_rsi < self.exit_threshold)
        ):
            signal_type = SignalType.EXIT
            confidence = SignalConfidence.MEDIUM
        
        # Return signal if we have one
        if signal_type:
            # Create signal data
            signal = {
                "type": signal_type,
                "confidence": confidence,
                "time": int(time.time() * 1000),
                "price": current_price,
                "symbol": symbol,
                "timeframe": timeframe,
                "source": self.name,
                "metadata": {
                    "rsi": current_rsi,
                    "rsi_period": self.rsi_period,
                    "overbought": self.overbought_threshold,
                    "oversold": self.oversold_threshold
                }
            }
            
            # Store signal
            self.signals[symbol][timeframe].append(signal)
            
            # Limit signal history
            if len(self.signals[symbol][timeframe]) > 100:
                self.signals[symbol][timeframe] = self.signals[symbol][timeframe][-100:]
            
            # Update last signal time
            self.last_signal_time[symbol][timeframe] = signal["time"]
            
            return signal
        
        return None
    
    async def update(self, market_data: Dict[str, Any]) -> bool:
        """
        Update the strategy with new market data.
        
        Args:
            market_data: Market data point
            
        Returns:
            Success flag
        """
        try:
            # Extract data
            symbol = market_data.get("symbol")
            timeframe = market_data.get("timeframe")
            
            if not symbol or not timeframe:
                logger.warning("Missing symbol or timeframe in market data")
                return False
            
            # Update market data
            self._update_market_data(symbol, timeframe, market_data)
            
            # Calculate indicators
            self._calculate_indicators(symbol, timeframe)
            
            # Call parent update method (handles state saving)
            return await super().update(market_data)
            
        except Exception as e:
            logger.error(f"Error updating {self.name}: {e}")
            return False
    
    async def get_signal(self, symbol: str, timeframe: str) -> Optional[Dict[str, Any]]:
        """
        Get the current signal for the symbol and timeframe.
        
        Args:
            symbol: Trading symbol
            timeframe: Chart timeframe
            
        Returns:
            Signal dictionary or None
        """
        try:
            # Check if we have data for this symbol and timeframe
            if (symbol not in self.market_data or 
                timeframe not in self.market_data[symbol] or 
                len(self.market_data[symbol][timeframe]) < self.rsi_period + 1):
                return None
            
            # Check for new signal
            signal = self._check_for_signal(symbol, timeframe)
            
            if signal:
                # Log signal
                logger.info(
                    f"{self.name} generated {signal['type']} signal for {symbol} {timeframe} "
                    f"at price {signal['price']:.2f} (RSI: {signal['metadata']['rsi']:.1f})"
                )
                
                # Update performance metrics
                await self._update_performance_metrics(signal)
            
            return signal
            
        except Exception as e:
            logger.error(f"Error getting signal from {self.name}: {e}")
            return None
    
    async def _update_performance_metrics(self, signal: Dict[str, Any]) -> None:
        """
        Update performance metrics for the strategy.
        
        Args:
            signal: Generated signal
        """
        # Add signal to performance tracking
        self.performance.add_signal(
            symbol=signal["symbol"],
            time=signal["time"],
            signal_type=signal["type"],
            price=signal["price"],
            confidence=signal["confidence"]
        )
        
        # Save performance data if needed
        if (time.time() - self.last_performance_save) > self.performance_check_interval:
            await self._save_performance()
            self.last_performance_save = time.time()
    
    def get_state(self) -> Dict[str, Any]:
        """
        Get the current state of the strategy.
        
        Returns:
            State dictionary
        """
        # Call parent method to get base state
        state = super().get_state()
        
        # Add strategy-specific state
        state.update({
            "confirmation_count": self.confirmation_count,
            "last_signal_time": self.last_signal_time
        })
        
        return state
    
    def restore_state(self, state: Dict[str, Any]) -> bool:
        """
        Restore the strategy state from a saved state.
        
        Args:
            state: Saved state
            
        Returns:
            Success flag
        """
        try:
            # Call parent method to restore base state
            result = super().restore_state(state)
            
            if not result:
                return False
            
            # Restore strategy-specific state
            if "confirmation_count" in state:
                self.confirmation_count = state["confirmation_count"]
                
            if "last_signal_time" in state:
                self.last_signal_time = state["last_signal_time"]
            
            return True
            
        except Exception as e:
            logger.error(f"Error restoring state for {self.name}: {e}")
            return False 