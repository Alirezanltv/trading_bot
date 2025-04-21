"""
Simple MACD Strategy Implementation

This module implements a simple MACD (Moving Average Convergence Divergence)
strategy for trading with additional confirmation from RSI.
"""

import time
import asyncio
import numpy as np
from typing import Dict, Any, List, Optional, Tuple

from trading_system.core import get_logger
from trading_system.strategy.base import (
    Strategy, StrategySignal, SignalDirection, BacktestResult
)

logger = get_logger("strategy.simple_macd")

class SimpleMACD(Strategy):
    """
    Simple MACD Strategy 
    
    This strategy generates buy/sell signals based on MACD crosses
    with additional confirmation from RSI.
    """
    
    def __init__(self, strategy_id: str, config: Dict[str, Any]):
        """
        Initialize the SimpleMACD strategy.
        
        Args:
            strategy_id: Unique strategy identifier
            config: Strategy configuration
        """
        super().__init__(strategy_id, config)
        
        # MACD parameters
        self.fast_length = config.get("parameters", {}).get("fast_length", 12)
        self.slow_length = config.get("parameters", {}).get("slow_length", 26)
        self.signal_length = config.get("parameters", {}).get("signal_length", 9)
        
        # RSI parameters
        self.rsi_length = config.get("parameters", {}).get("rsi_length", 14)
        self.rsi_overbought = config.get("parameters", {}).get("rsi_overbought", 70)
        self.rsi_oversold = config.get("parameters", {}).get("rsi_oversold", 30)
        
        # Additional parameters
        self.volume_factor = config.get("parameters", {}).get("volume_factor", 1.5)
        
        # Signal tracking
        self._last_signal: Dict[str, Dict[str, Any]] = {}
        
        logger.info(
            f"SimpleMACD strategy initialized with: "
            f"fast={self.fast_length}, slow={self.slow_length}, signal={self.signal_length}, "
            f"rsi_length={self.rsi_length}, rsi_ob={self.rsi_overbought}, rsi_os={self.rsi_oversold}"
        )
        
    async def analyze(self, symbol: str, market_data: Dict[str, Any]) -> Optional[StrategySignal]:
        """
        Analyze market data and generate trading signals.
        
        Args:
            symbol: Trading symbol
            market_data: Market data including OHLCV and indicators
            
        Returns:
            Trading signal or None
        """
        try:
            # Extract required data from market data
            ohlcv = market_data.get("ohlcv", [])
            if not ohlcv or len(ohlcv) < max(self.slow_length, self.rsi_length) + self.signal_length:
                logger.warning(f"Insufficient data points for {symbol} analysis, need at least {max(self.slow_length, self.rsi_length) + self.signal_length}")
                return None
                
            # Calculate indicators if not provided
            if "indicators" not in market_data:
                indicators = self._calculate_indicators(ohlcv)
            else:
                indicators = market_data["indicators"]
                
            # Extract close prices and volumes
            closes = np.array([candle[4] for candle in ohlcv])
            volumes = np.array([candle[5] for candle in ohlcv])
            
            # Get indicator values
            macd_values = indicators.get("macd", {})
            macd_line = macd_values.get("macd_line", [])
            signal_line = macd_values.get("signal_line", [])
            histogram = macd_values.get("histogram", [])
            
            rsi_values = indicators.get("rsi", [])
            
            # Check for empty indicator values
            if not macd_line or not signal_line or not histogram or not rsi_values:
                logger.warning(f"Missing indicator values for {symbol}")
                return None
                
            # Get the latest values
            current_macd = macd_line[-1]
            current_signal = signal_line[-1]
            current_histogram = histogram[-1]
            previous_histogram = histogram[-2] if len(histogram) > 1 else 0
            
            current_rsi = rsi_values[-1]
            previous_rsi = rsi_values[-2] if len(rsi_values) > 1 else 50
            
            # Get current price and volume
            current_price = closes[-1]
            current_volume = volumes[-1]
            
            # Calculate average volume for volume confirmation
            avg_volume = np.mean(volumes[-20:])
            
            # Generate signal
            signal = None
            
            # Buy signal conditions
            buy_conditions = [
                current_histogram > 0,  # Positive histogram
                current_histogram > previous_histogram,  # Increasing histogram
                current_macd > current_signal,  # MACD above signal line
                current_rsi < 50,  # RSI below midpoint
                current_rsi > self.rsi_oversold,  # RSI not oversold (avoid catching falling knife)
                current_rsi > previous_rsi,  # RSI increasing
                current_volume > avg_volume * self.volume_factor  # Volume confirmation
            ]
            
            # Sell signal conditions
            sell_conditions = [
                current_histogram < 0,  # Negative histogram
                current_histogram < previous_histogram,  # Decreasing histogram
                current_macd < current_signal,  # MACD below signal line
                current_rsi > 50,  # RSI above midpoint
                current_rsi < self.rsi_overbought,  # RSI not overbought (avoid selling too early)
                current_rsi < previous_rsi,  # RSI decreasing
                current_volume > avg_volume * self.volume_factor  # Volume confirmation
            ]
            
            # Buy signal
            if all(buy_conditions):
                # Avoid duplicate signals
                if not self._is_duplicate_signal(symbol, SignalDirection.BUY):
                    signal = StrategySignal(
                        strategy_id=self.strategy_id,
                        symbol=symbol,
                        timeframe=self.timeframe,
                        direction=SignalDirection.BUY,
                        price=current_price,
                        amount=self._calculate_position_size(symbol, current_price),
                        timestamp=int(time.time() * 1000),
                        metadata={
                            "reason": "MACD bullish cross with RSI confirmation",
                            "confidence": self._calculate_confidence(buy_conditions, current_rsi, current_histogram),
                            "indicators": {
                                "macd": {
                                    "histogram": current_histogram,
                                    "macd_line": current_macd,
                                    "signal_line": current_signal
                                },
                                "rsi": current_rsi
                            }
                        }
                    )
                    
                    # Track signal to avoid duplicates
                    self._last_signal[symbol] = {
                        "direction": SignalDirection.BUY,
                        "price": current_price,
                        "timestamp": int(time.time() * 1000)
                    }
                    
                    logger.info(f"Generated BUY signal for {symbol} at {current_price}")
            
            # Sell signal
            elif all(sell_conditions):
                # Avoid duplicate signals
                if not self._is_duplicate_signal(symbol, SignalDirection.SELL):
                    signal = StrategySignal(
                        strategy_id=self.strategy_id,
                        symbol=symbol,
                        timeframe=self.timeframe,
                        direction=SignalDirection.SELL,
                        price=current_price,
                        amount=self._calculate_position_size(symbol, current_price),
                        timestamp=int(time.time() * 1000),
                        metadata={
                            "reason": "MACD bearish cross with RSI confirmation",
                            "confidence": self._calculate_confidence(sell_conditions, current_rsi, abs(current_histogram)),
                            "indicators": {
                                "macd": {
                                    "histogram": current_histogram,
                                    "macd_line": current_macd,
                                    "signal_line": current_signal
                                },
                                "rsi": current_rsi
                            }
                        }
                    )
                    
                    # Track signal to avoid duplicates
                    self._last_signal[symbol] = {
                        "direction": SignalDirection.SELL,
                        "price": current_price,
                        "timestamp": int(time.time() * 1000)
                    }
                    
                    logger.info(f"Generated SELL signal for {symbol} at {current_price}")
            
            return signal
            
        except Exception as e:
            logger.error(f"Error analyzing {symbol}: {str(e)}", exc_info=True)
            return None
            
    async def backtest(self, symbol: str, market_data: Dict[str, Any]) -> BacktestResult:
        """
        Perform backtesting for the strategy.
        
        Args:
            symbol: Trading symbol
            market_data: Historical market data
            
        Returns:
            Backtest result
        """
        try:
            # Extract required data
            ohlcv = market_data.get("ohlcv", [])
            
            if not ohlcv or len(ohlcv) < max(self.slow_length, self.rsi_length) + self.signal_length:
                logger.warning(f"Insufficient data points for {symbol} backtest")
                return BacktestResult(
                    strategy_id=self.strategy_id,
                    symbol=symbol,
                    profit_loss=0.0,
                    win_rate=0.0,
                    trades=[],
                    metrics={},
                    timestamp=int(time.time() * 1000)
                )
                
            # Calculate indicators
            indicators = self._calculate_indicators(ohlcv)
            
            # Extract indicator values
            macd_values = indicators.get("macd", {})
            macd_line = macd_values.get("macd_line", [])
            signal_line = macd_values.get("signal_line", [])
            histogram = macd_values.get("histogram", [])
            
            rsi_values = indicators.get("rsi", [])
            
            # Extract close prices and timestamps
            closes = [candle[4] for candle in ohlcv]
            timestamps = [candle[0] for candle in ohlcv]
            volumes = [candle[5] for candle in ohlcv]
            
            # Skip the warm-up period
            warmup = max(self.slow_length, self.rsi_length) + self.signal_length
            
            # Initialize backtest variables
            position = 0
            entry_price = 0
            trades = []
            cash = 10000  # Start with 10,000 (arbitrary initial capital)
            equity = cash
            equities = []
            
            # Calculate average volume for first 20 periods (may be less if not enough data)
            avg_volume_window = min(20, len(volumes) - warmup)
            
            # Backtest over historical data
            for i in range(warmup, len(ohlcv)):
                current_price = closes[i]
                current_macd = macd_line[i - warmup]
                current_signal = signal_line[i - warmup]
                current_histogram = histogram[i - warmup]
                previous_histogram = histogram[i - warmup - 1] if i > warmup else 0
                
                current_rsi = rsi_values[i - warmup]
                previous_rsi = rsi_values[i - warmup - 1] if i > warmup else 50
                
                current_timestamp = timestamps[i]
                current_volume = volumes[i]
                
                # Calculate rolling average volume
                start_idx = max(0, i - 20)
                avg_volume = sum(volumes[start_idx:i]) / (i - start_idx)
                
                # Update equity
                if position != 0:
                    equity = cash + position * current_price
                else:
                    equity = cash
                    
                equities.append(equity)
                
                # Buy signal conditions
                buy_conditions = [
                    position == 0,  # Not in position
                    current_histogram > 0,  # Positive histogram
                    current_histogram > previous_histogram,  # Increasing histogram
                    current_macd > current_signal,  # MACD above signal line
                    current_rsi < 50,  # RSI below midpoint
                    current_rsi > self.rsi_oversold,  # RSI not oversold
                    current_rsi > previous_rsi,  # RSI increasing
                    current_volume > avg_volume * self.volume_factor  # Volume confirmation
                ]
                
                # Sell signal conditions
                sell_conditions = [
                    position > 0,  # In position
                    current_histogram < 0,  # Negative histogram
                    current_histogram < previous_histogram,  # Decreasing histogram
                    current_macd < current_signal,  # MACD below signal line
                    current_rsi > 50,  # RSI above midpoint
                    current_rsi < self.rsi_overbought,  # RSI not overbought
                    current_rsi < previous_rsi,  # RSI decreasing
                    current_volume > avg_volume * self.volume_factor  # Volume confirmation
                ]
                
                # Execute buy
                if all(buy_conditions):
                    # Calculate position size (simplified, using percentage of cash)
                    size = cash * 0.95 / current_price  # 95% of cash
                    
                    # Update position and cash
                    position = size
                    entry_price = current_price
                    cash -= position * current_price
                    
                    # Record trade
                    trades.append({
                        "type": "buy",
                        "timestamp": current_timestamp,
                        "price": current_price,
                        "amount": position,
                        "value": position * current_price,
                        "indicators": {
                            "macd": {
                                "histogram": current_histogram,
                                "macd_line": current_macd,
                                "signal_line": current_signal
                            },
                            "rsi": current_rsi
                        }
                    })
                    
                # Execute sell
                elif all(sell_conditions):
                    # Calculate profit/loss
                    profit = position * (current_price - entry_price)
                    
                    # Update cash
                    cash += position * current_price
                    
                    # Record trade
                    trades.append({
                        "type": "sell",
                        "timestamp": current_timestamp,
                        "price": current_price,
                        "amount": position,
                        "value": position * current_price,
                        "profit": profit,
                        "profit_percentage": (current_price - entry_price) / entry_price * 100,
                        "indicators": {
                            "macd": {
                                "histogram": current_histogram,
                                "macd_line": current_macd,
                                "signal_line": current_signal
                            },
                            "rsi": current_rsi
                        }
                    })
                    
                    # Reset position
                    position = 0
                    entry_price = 0
            
            # Calculate backtest metrics
            win_trades = [t for t in trades if t.get("type") == "sell" and t.get("profit", 0) > 0]
            loss_trades = [t for t in trades if t.get("type") == "sell" and t.get("profit", 0) <= 0]
            
            total_trades = len(win_trades) + len(loss_trades)
            win_rate = len(win_trades) / total_trades if total_trades > 0 else 0
            
            total_profit = sum(t.get("profit", 0) for t in trades)
            profit_percentage = (equity - 10000) / 10000 * 100
            
            max_drawdown = 0
            peak = 10000  # Initial equity
            
            for current_equity in equities:
                if current_equity > peak:
                    peak = current_equity
                drawdown = (peak - current_equity) / peak * 100
                max_drawdown = max(max_drawdown, drawdown)
            
            # Create result
            result = BacktestResult(
                strategy_id=self.strategy_id,
                symbol=symbol,
                profit_loss=total_profit,
                win_rate=win_rate,
                trades=trades,
                metrics={
                    "profit_percentage": profit_percentage,
                    "total_trades": total_trades,
                    "win_trades": len(win_trades),
                    "loss_trades": len(loss_trades),
                    "max_drawdown": max_drawdown,
                    "final_equity": equity
                },
                timestamp=int(time.time() * 1000)
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error backtesting {symbol}: {str(e)}", exc_info=True)
            return BacktestResult(
                strategy_id=self.strategy_id,
                symbol=symbol,
                profit_loss=0.0,
                win_rate=0.0,
                trades=[],
                metrics={
                    "error": str(e)
                },
                timestamp=int(time.time() * 1000)
            )
    
    def _calculate_indicators(self, ohlcv: List[List[float]]) -> Dict[str, Any]:
        """
        Calculate indicators from OHLCV data.
        
        Args:
            ohlcv: OHLCV data
            
        Returns:
            Dictionary of calculated indicators
        """
        # Extract close prices
        closes = np.array([candle[4] for candle in ohlcv])
        
        # Calculate EMA values for MACD
        ema_fast = self._calculate_ema(closes, self.fast_length)
        ema_slow = self._calculate_ema(closes, self.slow_length)
        
        # Calculate MACD line
        macd_line = ema_fast - ema_slow
        
        # Calculate signal line
        signal_line = self._calculate_ema(macd_line, self.signal_length)
        
        # Calculate histogram
        histogram = macd_line - signal_line
        
        # Calculate RSI
        rsi = self._calculate_rsi(closes, self.rsi_length)
        
        return {
            "macd": {
                "macd_line": macd_line.tolist(),
                "signal_line": signal_line.tolist(),
                "histogram": histogram.tolist()
            },
            "rsi": rsi.tolist()
        }
    
    def _calculate_ema(self, data: np.ndarray, length: int) -> np.ndarray:
        """
        Calculate Exponential Moving Average.
        
        Args:
            data: Input data
            length: EMA period
            
        Returns:
            EMA values
        """
        alpha = 2 / (length + 1)
        ema = np.zeros_like(data)
        ema[0] = data[0]
        
        for i in range(1, len(data)):
            ema[i] = data[i] * alpha + ema[i-1] * (1 - alpha)
            
        return ema
    
    def _calculate_rsi(self, data: np.ndarray, length: int) -> np.ndarray:
        """
        Calculate Relative Strength Index.
        
        Args:
            data: Input data
            length: RSI period
            
        Returns:
            RSI values
        """
        # Calculate price changes
        delta = np.zeros_like(data)
        delta[1:] = data[1:] - data[:-1]
        
        # Separate gains and losses
        gains = delta.copy()
        losses = delta.copy()
        
        gains[gains < 0] = 0
        losses[losses > 0] = 0
        losses = abs(losses)
        
        # Initialize averages
        avg_gain = np.zeros_like(data)
        avg_loss = np.zeros_like(data)
        
        # First average is simple average
        avg_gain[length] = np.mean(gains[1:length+1])
        avg_loss[length] = np.mean(losses[1:length+1])
        
        # Calculate smoothed averages
        for i in range(length+1, len(data)):
            avg_gain[i] = (avg_gain[i-1] * (length-1) + gains[i]) / length
            avg_loss[i] = (avg_loss[i-1] * (length-1) + losses[i]) / length
        
        # Calculate RS and RSI
        rs = np.zeros_like(data)
        rsi = np.zeros_like(data)
        
        for i in range(length, len(data)):
            if avg_loss[i] == 0:
                rs[i] = 100.0
            else:
                rs[i] = avg_gain[i] / avg_loss[i]
                
            rsi[i] = 100 - (100 / (1 + rs[i]))
        
        return rsi
    
    def _calculate_position_size(self, symbol: str, price: float) -> float:
        """
        Calculate position size for a trade.
        
        Args:
            symbol: Trading symbol
            price: Current price
            
        Returns:
            Position size
        """
        # Simple fixed position size for now
        # In a real implementation, this would include risk management
        if "BTC" in symbol:
            return 0.01  # 0.01 BTC
        elif "ETH" in symbol:
            return 0.1  # 0.1 ETH
        else:
            return 1.0
    
    def _calculate_confidence(self, conditions: List[bool], rsi: float, histogram_value: float) -> float:
        """
        Calculate confidence level for a signal.
        
        Args:
            conditions: List of conditions met
            rsi: Current RSI value
            histogram_value: Current MACD histogram value
            
        Returns:
            Confidence score between 0 and 1
        """
        # Base confidence on:
        # 1. Number of conditions met
        # 2. Strength of RSI (distance from 50)
        # 3. Strength of MACD histogram
        
        condition_score = sum(conditions) / len(conditions)
        
        # RSI confidence (0-1)
        rsi_distance = abs(rsi - 50) / 50  # Normalize to 0-1
        
        # Histogram confidence (0-1)
        # Normalize histogram (typical values might be around -2 to 2)
        histogram_norm = min(1.0, abs(histogram_value) / 2.0)
        
        # Weighted average
        confidence = (
            0.4 * condition_score +
            0.3 * rsi_distance +
            0.3 * histogram_norm
        )
        
        return round(confidence, 2)
    
    def _is_duplicate_signal(self, symbol: str, direction: SignalDirection) -> bool:
        """
        Check if the signal is a duplicate of the last signal.
        
        Args:
            symbol: Trading symbol
            direction: Signal direction
            
        Returns:
            True if the signal is a duplicate
        """
        if symbol not in self._last_signal:
            return False
            
        last_signal = self._last_signal[symbol]
        last_direction = last_signal.get("direction")
        last_timestamp = last_signal.get("timestamp", 0)
        
        # If same direction and less than 1 hour passed, consider it a duplicate
        if (direction == last_direction and 
            (int(time.time() * 1000) - last_timestamp) < 3600000):
            return True
            
        return False 