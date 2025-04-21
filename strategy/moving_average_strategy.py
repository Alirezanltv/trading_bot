"""
Moving Average Strategy

This module implements a moving average crossover trading strategy for the trading system.

Features:
- Configurable short and long moving average periods
- Entry and exit threshold parameters
- Optional short position support
- Volume-based filtering
"""

import logging
import numpy as np
from typing import List, Dict, Any, Optional, Deque
from collections import deque
from datetime import datetime

from trading_system.strategy.strategy_engine import (
    Strategy, 
    StrategyParameters,
    TradingSignal,
    SignalType,
    SignalDirection
)
from trading_system.market_data.market_data_types import MarketData

# Get logger
logger = logging.getLogger(__name__)

class MovingAverageStrategy(Strategy):
    """
    Moving average crossover strategy implementation.
    
    This strategy generates entry signals when the short moving average crosses
    above the long moving average, and exit signals when it crosses below.
    """
    
    def __init__(
        self,
        strategy_id: str,
        name: str = "Moving Average Crossover",
        parameters: StrategyParameters = None
    ):
        """Initialize moving average strategy.
        
        Args:
            strategy_id: Unique ID for the strategy
            name: Display name for the strategy
            parameters: Strategy parameters
        """
        super().__init__(strategy_id, name, parameters)
        
        # Set default parameters if not provided
        if not parameters:
            self.parameters = StrategyParameters({
                "short_window": 10,
                "long_window": 30,
                "entry_threshold": 0.0002,
                "exit_threshold": 0.0001,
                "min_volume": 1.0,
                "enable_short": False
            })
        
        # Price history for each ticker
        self.price_data = {}
        self.volume_data = {}
        self.signal_state = {}  # Tracks whether we're in a position for each ticker
        
    async def initialize(self) -> None:
        """Initialize the strategy."""
        # Get parameters
        self.short_window = self.parameters.get("short_window", 10)
        self.long_window = self.parameters.get("long_window", 30)
        self.entry_threshold = self.parameters.get("entry_threshold", 0.0002)
        self.exit_threshold = self.parameters.get("exit_threshold", 0.0001)
        self.min_volume = self.parameters.get("min_volume", 1.0)
        self.enable_short = self.parameters.get("enable_short", False)
        
        logger.info(f"Initialized MovingAverageStrategy - short_window: {self.short_window}, long_window: {self.long_window}")
        
    def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the supported list.
        
        Args:
            ticker: Ticker symbol to add
        """
        if ticker not in self.supported_tickers:
            self.supported_tickers.append(ticker)
            self.price_data[ticker] = deque(maxlen=self.long_window)
            self.volume_data[ticker] = deque(maxlen=self.long_window)
            self.signal_state[ticker] = {
                "long_position": False,
                "short_position": False,
                "last_signal_time": None,
                "last_price": None
            }
            logger.info(f"Added ticker {ticker} to MovingAverageStrategy")
    
    async def process_market_data(self, market_data: MarketData) -> List[TradingSignal]:
        """Process market data and generate trading signals.
        
        Args:
            market_data: Market data to process
            
        Returns:
            List of generated trading signals
        """
        signals = []
        ticker = market_data.ticker
        
        # Check if we support this ticker
        if ticker not in self.supported_tickers:
            return signals
        
        # Extract candle data - we're assuming market_data.data contains candle information
        try:
            candle = market_data.data
            close_price = float(candle.get("close", 0))
            volume = float(candle.get("volume", 0))
            timestamp = market_data.timestamp
            
            # Skip processing if missing data
            if close_price <= 0:
                return signals
                
            # Update data
            self.price_data[ticker].append(close_price)
            self.volume_data[ticker].append(volume)
            
            # Need enough data for both MAs
            if len(self.price_data[ticker]) < self.long_window:
                return signals
                
            # Calculate moving averages
            short_ma = self._calculate_ma(self.price_data[ticker], self.short_window)
            long_ma = self._calculate_ma(self.price_data[ticker], self.long_window)
            
            # Calculate average volume
            avg_volume = self._calculate_ma(self.volume_data[ticker], self.short_window)
            
            # Get current state
            state = self.signal_state[ticker]
            state["last_price"] = close_price
            
            # Check for signal conditions
            ma_diff = short_ma - long_ma
            ma_diff_pct = ma_diff / long_ma if long_ma > 0 else 0
            
            # Long signal conditions
            if not state["long_position"] and ma_diff_pct > self.entry_threshold and avg_volume >= self.min_volume:
                # Generate entry signal for long
                signal = self.create_signal(
                    ticker=ticker,
                    signal_type=SignalType.ENTRY,
                    direction=SignalDirection.LONG,
                    params={
                        "price": close_price,
                        "ma_diff_pct": ma_diff_pct,
                        "short_ma": short_ma,
                        "long_ma": long_ma,
                        "volume": volume
                    }
                )
                signals.append(signal)
                state["long_position"] = True
                state["last_signal_time"] = timestamp
                logger.info(f"Generated LONG entry signal for {ticker} at {close_price}, ma_diff_pct: {ma_diff_pct:.4f}")
                
            # Long exit conditions
            elif state["long_position"] and ma_diff_pct < -self.exit_threshold:
                # Generate exit signal for long
                signal = self.create_signal(
                    ticker=ticker,
                    signal_type=SignalType.EXIT,
                    direction=SignalDirection.LONG,
                    params={
                        "price": close_price,
                        "ma_diff_pct": ma_diff_pct,
                        "short_ma": short_ma,
                        "long_ma": long_ma,
                        "volume": volume
                    }
                )
                signals.append(signal)
                state["long_position"] = False
                state["last_signal_time"] = timestamp
                logger.info(f"Generated LONG exit signal for {ticker} at {close_price}, ma_diff_pct: {ma_diff_pct:.4f}")
            
            # Short signal conditions (if enabled)
            if self.enable_short:
                if not state["short_position"] and ma_diff_pct < -self.entry_threshold and avg_volume >= self.min_volume:
                    # Generate entry signal for short
                    signal = self.create_signal(
                        ticker=ticker,
                        signal_type=SignalType.ENTRY,
                        direction=SignalDirection.SHORT,
                        params={
                            "price": close_price,
                            "ma_diff_pct": ma_diff_pct,
                            "short_ma": short_ma,
                            "long_ma": long_ma,
                            "volume": volume
                        }
                    )
                    signals.append(signal)
                    state["short_position"] = True
                    state["last_signal_time"] = timestamp
                    logger.info(f"Generated SHORT entry signal for {ticker} at {close_price}, ma_diff_pct: {ma_diff_pct:.4f}")
                    
                # Short exit conditions
                elif state["short_position"] and ma_diff_pct > self.exit_threshold:
                    # Generate exit signal for short
                    signal = self.create_signal(
                        ticker=ticker,
                        signal_type=SignalType.EXIT,
                        direction=SignalDirection.SHORT,
                        params={
                            "price": close_price,
                            "ma_diff_pct": ma_diff_pct,
                            "short_ma": short_ma,
                            "long_ma": long_ma,
                            "volume": volume
                        }
                    )
                    signals.append(signal)
                    state["short_position"] = False
                    state["last_signal_time"] = timestamp
                    logger.info(f"Generated SHORT exit signal for {ticker} at {close_price}, ma_diff_pct: {ma_diff_pct:.4f}")
                
            return signals
            
        except Exception as e:
            logger.error(f"Error processing market data for {ticker}: {str(e)}")
            return signals
    
    def _calculate_ma(self, data, window):
        """Calculate the moving average for the given window.
        
        Args:
            data: Data points
            window: Window size
            
        Returns:
            Moving average value
        """
        return np.mean(list(data)[-window:])
    
    async def on_signal_executed(self, signal: TradingSignal, result: Dict[str, Any]) -> None:
        """Handle signal execution result.
        
        Args:
            signal: Trading signal that was executed
            result: Execution result
        """
        await super().on_signal_executed(signal, result)
        
        # Additional handling specific to this strategy could be added here
        logger.info(f"Signal executed: {signal.signal_id} for {signal.ticker} - result: {result}") 