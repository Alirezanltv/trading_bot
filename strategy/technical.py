"""
Technical strategy implementation for the high-reliability trading system.

This strategy uses common technical indicators to generate trading signals.
"""

from typing import Dict, Any, List, Optional, Tuple
import time
import pandas as pd
import numpy as np
from pandas import DataFrame

from trading_system.core import get_logger
from trading_system.strategy.base import (
    Strategy, StrategySignal, StrategyResult, BacktestResult, SignalType
)

logger = get_logger("strategy.technical")


class TechnicalStrategy(Strategy):
    """
    Technical analysis based trading strategy.
    
    This strategy generates signals based on a combination of technical indicators:
    - MACD (Moving Average Convergence Divergence)
    - RSI (Relative Strength Index)
    - Bollinger Bands
    - Moving Averages (Simple and Exponential)
    """
    
    def __init__(self, name: str = "TechnicalStrategy", parameters: Dict[str, Any] = None):
        """
        Initialize the technical strategy.
        
        Args:
            name: Strategy name
            parameters: Strategy parameters
        """
        # Default parameters
        default_parameters = {
            # MACD parameters
            "macd_fast": 12,
            "macd_slow": 26,
            "macd_signal": 9,
            "macd_weight": 0.3,
            
            # RSI parameters
            "rsi_period": 14,
            "rsi_overbought": 70,
            "rsi_oversold": 30,
            "rsi_weight": 0.2,
            
            # Bollinger Bands parameters
            "bb_period": 20,
            "bb_std_dev": 2,
            "bb_weight": 0.2,
            
            # Moving Average parameters
            "ma_fast": 10,
            "ma_slow": 30,
            "ma_weight": 0.3,
            
            # Signal parameters
            "signal_threshold_buy": 0.6,
            "signal_threshold_sell": -0.6,
            "signal_threshold_strong_buy": 0.8,
            "signal_threshold_strong_sell": -0.8
        }
        
        # Merge default parameters with provided parameters
        if parameters:
            default_parameters.update(parameters)
        
        super().__init__(name=name, parameters=default_parameters)
    
    def _validate_parameters(self, parameters: Dict[str, Any]) -> bool:
        """
        Validate strategy parameters.
        
        Args:
            parameters: Parameters to validate
            
        Returns:
            True if parameters are valid
            
        Raises:
            ValueError: If parameters are invalid
        """
        # Validate MACD parameters
        if "macd_fast" in parameters and parameters["macd_fast"] >= parameters.get("macd_slow", self.parameters["macd_slow"]):
            raise ValueError("MACD fast period must be less than slow period")
        
        if "macd_slow" in parameters and parameters["macd_slow"] <= parameters.get("macd_fast", self.parameters["macd_fast"]):
            raise ValueError("MACD slow period must be greater than fast period")
        
        # Validate RSI parameters
        if "rsi_overbought" in parameters and parameters["rsi_overbought"] <= parameters.get("rsi_oversold", self.parameters["rsi_oversold"]):
            raise ValueError("RSI overbought level must be greater than oversold level")
        
        if "rsi_oversold" in parameters and parameters["rsi_oversold"] >= parameters.get("rsi_overbought", self.parameters["rsi_overbought"]):
            raise ValueError("RSI oversold level must be less than overbought level")
        
        # Validate Moving Average parameters
        if "ma_fast" in parameters and parameters["ma_fast"] >= parameters.get("ma_slow", self.parameters["ma_slow"]):
            raise ValueError("Fast MA period must be less than slow MA period")
        
        if "ma_slow" in parameters and parameters["ma_slow"] <= parameters.get("ma_fast", self.parameters["ma_fast"]):
            raise ValueError("Slow MA period must be greater than fast MA period")
        
        # Validate weights
        weights = [
            parameters.get("macd_weight", self.parameters["macd_weight"]),
            parameters.get("rsi_weight", self.parameters["rsi_weight"]),
            parameters.get("bb_weight", self.parameters["bb_weight"]),
            parameters.get("ma_weight", self.parameters["ma_weight"])
        ]
        
        if any(w < 0 or w > 1 for w in weights):
            raise ValueError("Indicator weights must be between 0 and 1")
        
        if abs(sum(weights) - 1.0) > 0.01:
            raise ValueError("Sum of indicator weights must be approximately 1.0")
        
        return True
    
    def generate_signals(self, data: Dict[str, Any]) -> StrategyResult:
        """
        Generate trading signals based on technical indicators.
        
        Args:
            data: Market data dictionary containing OHLCV data
            
        Returns:
            StrategyResult containing generated signals
        """
        signals = []
        performance_metrics = {}
        
        try:
            # Process each symbol
            for symbol, symbol_data in data.items():
                # Extract OHLCV data
                df = self._preprocess_data(symbol_data)
                
                if df is None or len(df) < 50:  # Need enough data for indicators
                    logger.warning(f"Insufficient data for symbol {symbol}")
                    continue
                
                # Calculate indicators
                df = self._calculate_indicators(df)
                
                # Generate signal
                signal_value, signal_metadata = self._calculate_signal_strength(df)
                
                # Determine signal type
                signal_type = self._determine_signal_type(signal_value)
                
                # Only generate signals for buy/sell/strong buy/strong sell
                if signal_type != SignalType.HOLD:
                    # Get the current price
                    current_price = df.iloc[-1]['close']
                    
                    # Create signal
                    signal = StrategySignal(
                        symbol=symbol,
                        signal_type=signal_type,
                        price=current_price,
                        confidence=abs(signal_value),
                        metadata=signal_metadata
                    )
                    
                    signals.append(signal)
                    
                # Add performance metrics
                symbol_metrics = {
                    "signal_value": signal_value,
                    "last_indicators": {
                        "macd": df.iloc[-1].get('macd', 0),
                        "macd_signal": df.iloc[-1].get('macd_signal', 0),
                        "rsi": df.iloc[-1].get('rsi', 0),
                        "bb_upper": df.iloc[-1].get('bb_upper', 0),
                        "bb_lower": df.iloc[-1].get('bb_lower', 0),
                        "ma_fast": df.iloc[-1].get('ma_fast', 0),
                        "ma_slow": df.iloc[-1].get('ma_slow', 0)
                    }
                }
                
                performance_metrics[symbol] = symbol_metrics
                
            return StrategyResult(
                signals=signals,
                performance_metrics=performance_metrics
            )
                
        except Exception as e:
            logger.error(f"Error generating signals: {str(e)}", exc_info=True)
            return StrategyResult(
                status="error",
                error=str(e)
            )
    
    def _preprocess_data(self, symbol_data: Dict[str, Any]) -> Optional[DataFrame]:
        """
        Preprocess symbol data into DataFrame.
        
        Args:
            symbol_data: Symbol data dictionary
            
        Returns:
            Preprocessed DataFrame or None if data is invalid
        """
        try:
            # Check if data is already a DataFrame
            if isinstance(symbol_data, pd.DataFrame):
                df = symbol_data.copy()
            else:
                # Convert dictionary data to DataFrame
                if "ohlcv" in symbol_data:
                    ohlcv_data = symbol_data["ohlcv"]
                    
                    # Check data format
                    if isinstance(ohlcv_data, list) and len(ohlcv_data) > 0:
                        # Convert list of dictionaries to DataFrame
                        df = pd.DataFrame(ohlcv_data)
                    else:
                        logger.warning(f"Invalid OHLCV data format")
                        return None
                else:
                    # Try to convert directly
                    df = pd.DataFrame(symbol_data)
            
            # Ensure required columns exist
            required_columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            
            # Check if all required columns exist
            if not all(col in df.columns for col in required_columns):
                logger.warning(f"Missing required columns in data. Required: {required_columns}, Found: {df.columns}")
                return None
            
            # Ensure timestamp is the index
            if 'timestamp' in df.columns:
                df = df.set_index('timestamp')
            
            # Convert columns to numeric
            for col in ['open', 'high', 'low', 'close', 'volume']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Sort by index
            df = df.sort_index()
            
            # Remove duplicates
            df = df[~df.index.duplicated(keep='last')]
            
            # Remove NaN values
            df = df.dropna(subset=['open', 'high', 'low', 'close'])
            
            return df
            
        except Exception as e:
            logger.error(f"Error preprocessing data: {str(e)}", exc_info=True)
            return None
    
    def _calculate_indicators(self, df: DataFrame) -> DataFrame:
        """
        Calculate technical indicators.
        
        Args:
            df: DataFrame with OHLCV data
            
        Returns:
            DataFrame with added technical indicators
        """
        # Extract parameters
        macd_fast = self.parameters["macd_fast"]
        macd_slow = self.parameters["macd_slow"]
        macd_signal = self.parameters["macd_signal"]
        rsi_period = self.parameters["rsi_period"]
        bb_period = self.parameters["bb_period"]
        bb_std_dev = self.parameters["bb_std_dev"]
        ma_fast_period = self.parameters["ma_fast"]
        ma_slow_period = self.parameters["ma_slow"]
        
        # Calculate MACD
        try:
            ema_fast = df['close'].ewm(span=macd_fast, adjust=False).mean()
            ema_slow = df['close'].ewm(span=macd_slow, adjust=False).mean()
            df['macd'] = ema_fast - ema_slow
            df['macd_signal'] = df['macd'].ewm(span=macd_signal, adjust=False).mean()
            df['macd_hist'] = df['macd'] - df['macd_signal']
        except Exception as e:
            logger.error(f"Error calculating MACD: {str(e)}", exc_info=True)
            df['macd'] = np.nan
            df['macd_signal'] = np.nan
            df['macd_hist'] = np.nan
        
        # Calculate RSI
        try:
            delta = df['close'].diff()
            gain = delta.where(delta > 0, 0)
            loss = -delta.where(delta < 0, 0)
            
            avg_gain = gain.rolling(window=rsi_period).mean()
            avg_loss = loss.rolling(window=rsi_period).mean()
            
            rs = avg_gain / avg_loss
            df['rsi'] = 100 - (100 / (1 + rs))
        except Exception as e:
            logger.error(f"Error calculating RSI: {str(e)}", exc_info=True)
            df['rsi'] = np.nan
        
        # Calculate Bollinger Bands
        try:
            df['ma_bb'] = df['close'].rolling(window=bb_period).mean()
            df['bb_std'] = df['close'].rolling(window=bb_period).std()
            df['bb_upper'] = df['ma_bb'] + (df['bb_std'] * bb_std_dev)
            df['bb_lower'] = df['ma_bb'] - (df['bb_std'] * bb_std_dev)
            df['bb_width'] = (df['bb_upper'] - df['bb_lower']) / df['ma_bb']
        except Exception as e:
            logger.error(f"Error calculating Bollinger Bands: {str(e)}", exc_info=True)
            df['ma_bb'] = np.nan
            df['bb_std'] = np.nan
            df['bb_upper'] = np.nan
            df['bb_lower'] = np.nan
            df['bb_width'] = np.nan
        
        # Calculate Moving Averages
        try:
            df['ma_fast'] = df['close'].rolling(window=ma_fast_period).mean()
            df['ma_slow'] = df['close'].rolling(window=ma_slow_period).mean()
        except Exception as e:
            logger.error(f"Error calculating Moving Averages: {str(e)}", exc_info=True)
            df['ma_fast'] = np.nan
            df['ma_slow'] = np.nan
        
        return df
    
    def _calculate_signal_strength(self, df: DataFrame) -> Tuple[float, Dict[str, Any]]:
        """
        Calculate overall signal strength.
        
        Args:
            df: DataFrame with technical indicators
            
        Returns:
            Tuple of (signal_value, signal_metadata)
        """
        # Extract last row
        last_row = df.iloc[-1]
        
        # Extract parameters
        macd_weight = self.parameters["macd_weight"]
        rsi_weight = self.parameters["rsi_weight"]
        bb_weight = self.parameters["bb_weight"]
        ma_weight = self.parameters["ma_weight"]
        rsi_overbought = self.parameters["rsi_overbought"]
        rsi_oversold = self.parameters["rsi_oversold"]
        
        # Initialize signal components
        macd_signal = 0
        rsi_signal = 0
        bb_signal = 0
        ma_signal = 0
        
        # Calculate MACD signal (-1 to 1)
        try:
            if not pd.isna(last_row['macd']) and not pd.isna(last_row['macd_signal']):
                # Normalize MACD
                macd_diff = last_row['macd'] - last_row['macd_signal']
                macd_signal = min(1, max(-1, macd_diff / abs(last_row['macd_signal']) if abs(last_row['macd_signal']) > 0 else 0))
        except (KeyError, Exception) as e:
            logger.warning(f"Error calculating MACD signal: {str(e)}")
        
        # Calculate RSI signal (-1 to 1)
        try:
            if not pd.isna(last_row['rsi']):
                rsi = last_row['rsi']
                if rsi > rsi_overbought:
                    rsi_signal = -1 * min(1, (rsi - rsi_overbought) / (100 - rsi_overbought))
                elif rsi < rsi_oversold:
                    rsi_signal = min(1, (rsi_oversold - rsi) / rsi_oversold)
                else:
                    # Normalize RSI between oversold and overbought
                    rsi_range = rsi_overbought - rsi_oversold
                    rsi_normalized = (rsi - rsi_oversold) / rsi_range
                    rsi_signal = 1 - 2 * rsi_normalized  # Convert to -1 to 1 range
        except (KeyError, Exception) as e:
            logger.warning(f"Error calculating RSI signal: {str(e)}")
        
        # Calculate Bollinger Bands signal (-1 to 1)
        try:
            if not (pd.isna(last_row['close']) or pd.isna(last_row['bb_upper']) or pd.isna(last_row['bb_lower']) or pd.isna(last_row['ma_bb'])):
                close = last_row['close']
                bb_upper = last_row['bb_upper']
                bb_lower = last_row['bb_lower']
                ma_bb = last_row['ma_bb']
                
                # Calculate distance from price to bands
                if close > ma_bb:
                    # Price above middle band
                    bb_signal = -1 * min(1, (close - ma_bb) / (bb_upper - ma_bb) if bb_upper > ma_bb else 0)
                else:
                    # Price below middle band
                    bb_signal = min(1, (ma_bb - close) / (ma_bb - bb_lower) if ma_bb > bb_lower else 0)
        except (KeyError, Exception) as e:
            logger.warning(f"Error calculating Bollinger Bands signal: {str(e)}")
        
        # Calculate Moving Average signal (-1 to 1)
        try:
            if not (pd.isna(last_row['ma_fast']) or pd.isna(last_row['ma_slow'])):
                ma_fast = last_row['ma_fast']
                ma_slow = last_row['ma_slow']
                
                # Calculate MA crossover
                ma_diff = (ma_fast - ma_slow) / ma_slow if ma_slow > 0 else 0
                ma_signal = min(1, max(-1, ma_diff * 10))  # Scale the difference
        except (KeyError, Exception) as e:
            logger.warning(f"Error calculating MA signal: {str(e)}")
        
        # Calculate weighted signal
        total_weight = macd_weight + rsi_weight + bb_weight + ma_weight
        signal_value = 0
        
        if total_weight > 0:
            signal_value = (
                (macd_signal * macd_weight) +
                (rsi_signal * rsi_weight) +
                (bb_signal * bb_weight) +
                (ma_signal * ma_weight)
            ) / total_weight
        
        # Create signal metadata
        signal_metadata = {
            "macd_signal": macd_signal,
            "rsi_signal": rsi_signal,
            "bb_signal": bb_signal,
            "ma_signal": ma_signal,
            "rsi": last_row.get('rsi', None),
            "macd": last_row.get('macd', None),
            "macd_hist": last_row.get('macd_hist', None),
            "bb_width": last_row.get('bb_width', None),
            "price": last_row.get('close', None)
        }
        
        return signal_value, signal_metadata
    
    def _determine_signal_type(self, signal_value: float) -> SignalType:
        """
        Determine signal type based on signal value.
        
        Args:
            signal_value: Signal value (-1 to 1)
            
        Returns:
            SignalType
        """
        # Extract thresholds
        threshold_buy = self.parameters["signal_threshold_buy"]
        threshold_sell = self.parameters["signal_threshold_sell"]
        threshold_strong_buy = self.parameters["signal_threshold_strong_buy"]
        threshold_strong_sell = self.parameters["signal_threshold_strong_sell"]
        
        # Determine signal type
        if signal_value >= threshold_strong_buy:
            return SignalType.STRONG_BUY
        elif signal_value >= threshold_buy:
            return SignalType.BUY
        elif signal_value <= threshold_strong_sell:
            return SignalType.STRONG_SELL
        elif signal_value <= threshold_sell:
            return SignalType.SELL
        else:
            return SignalType.HOLD
    
    def backtest(self, historical_data: Dict[str, Any]) -> BacktestResult:
        """
        Run backtest on historical data.
        
        Args:
            historical_data: Historical market data dictionary
            
        Returns:
            BacktestResult containing backtest results
        """
        signals = []
        trades = []
        all_equity_curves = {}
        start_equity = historical_data.get('initial_equity', 10000)
        
        try:
            # Process each symbol
            for symbol, symbol_data in historical_data.items():
                if symbol == 'initial_equity':
                    continue
                    
                # Skip if not a valid symbol data dictionary
                if not isinstance(symbol_data, dict) and not isinstance(symbol_data, pd.DataFrame):
                    continue
                
                # Preprocess data
                df = self._preprocess_data(symbol_data)
                
                if df is None or len(df) < 50:
                    logger.warning(f"Insufficient data for symbol {symbol} in backtest")
                    continue
                
                # Calculate indicators for the entire dataset
                df = self._calculate_indicators(df)
                
                # Generate signals for each candle
                symbol_signals = []
                equity = start_equity
                position = 0
                equity_curve = []
                current_trade = None
                
                for i in range(50, len(df)):  # Skip the first candles that lack indicators
                    candle_df = df.iloc[:i+1]
                    signal_value, signal_metadata = self._calculate_signal_strength(candle_df)
                    signal_type = self._determine_signal_type(signal_value)
                    
                    # Record signal
                    if signal_type != SignalType.HOLD:
                        signal = StrategySignal(
                            symbol=symbol,
                            signal_type=signal_type,
                            price=df.iloc[i]['close'],
                            timestamp=df.index[i],
                            confidence=abs(signal_value),
                            metadata=signal_metadata
                        )
                        
                        symbol_signals.append(signal)
                        signals.append(signal)
                    
                    # Execute trade logic
                    current_price = df.iloc[i]['close']
                    
                    # Record equity
                    equity_curve.append({
                        'timestamp': df.index[i],
                        'equity': equity,
                        'position': position,
                        'price': current_price
                    })
                    
                    # Check for trade entry/exit
                    if signal_type in [SignalType.BUY, SignalType.STRONG_BUY] and position <= 0:
                        # Buy signal
                        if position < 0:
                            # Close short position
                            pnl = position * (entry_price - current_price)
                            equity += pnl
                            
                            # Record trade exit
                            if current_trade:
                                current_trade['exit_price'] = current_price
                                current_trade['exit_timestamp'] = df.index[i]
                                current_trade['pnl'] = pnl
                                current_trade['pnl_pct'] = pnl / abs(position * entry_price)
                                trades.append(current_trade)
                        
                        # Open long position
                        position = equity * 0.95 / current_price  # Use 95% of equity
                        entry_price = current_price
                        
                        # Record trade entry
                        current_trade = {
                            'symbol': symbol,
                            'type': 'long',
                            'entry_price': entry_price,
                            'entry_timestamp': df.index[i],
                            'signal_confidence': abs(signal_value),
                            'position_size': position
                        }
                        
                    elif signal_type in [SignalType.SELL, SignalType.STRONG_SELL] and position >= 0:
                        # Sell signal
                        if position > 0:
                            # Close long position
                            pnl = position * (current_price - entry_price)
                            equity += pnl
                            
                            # Record trade exit
                            if current_trade:
                                current_trade['exit_price'] = current_price
                                current_trade['exit_timestamp'] = df.index[i]
                                current_trade['pnl'] = pnl
                                current_trade['pnl_pct'] = pnl / (position * entry_price)
                                trades.append(current_trade)
                        
                        # Open short position
                        position = -equity * 0.95 / current_price  # Use 95% of equity
                        entry_price = current_price
                        
                        # Record trade entry
                        current_trade = {
                            'symbol': symbol,
                            'type': 'short',
                            'entry_price': entry_price,
                            'entry_timestamp': df.index[i],
                            'signal_confidence': abs(signal_value),
                            'position_size': position
                        }
                
                # Close any open position at the end
                if position != 0:
                    current_price = df.iloc[-1]['close']
                    
                    if position > 0:
                        # Close long position
                        pnl = position * (current_price - entry_price)
                    else:
                        # Close short position
                        pnl = position * (entry_price - current_price)
                    
                    equity += pnl
                    
                    # Record trade exit
                    if current_trade:
                        current_trade['exit_price'] = current_price
                        current_trade['exit_timestamp'] = df.index[-1]
                        current_trade['pnl'] = pnl
                        current_trade['pnl_pct'] = pnl / abs(position * entry_price)
                        trades.append(current_trade)
                
                # Store equity curve
                all_equity_curves[symbol] = pd.DataFrame(equity_curve)
            
            # Combine equity curves
            if all_equity_curves:
                # Use the first symbol's timestamps as reference
                first_symbol = list(all_equity_curves.keys())[0]
                combined_equity = all_equity_curves[first_symbol][['timestamp', 'equity']].copy()
                combined_equity.columns = ['timestamp', first_symbol]
                
                # Add other symbols
                for symbol, df in all_equity_curves.items():
                    if symbol != first_symbol:
                        df_temp = df[['timestamp', 'equity']].copy()
                        df_temp.columns = ['timestamp', symbol]
                        combined_equity = pd.merge(combined_equity, df_temp, on='timestamp', how='outer')
                
                # Fill missing values
                combined_equity = combined_equity.fillna(method='ffill')
                
                # Calculate total equity
                combined_equity['total_equity'] = combined_equity.iloc[:, 1:].sum(axis=1)
                
                # Calculate drawdown
                combined_equity['peak'] = combined_equity['total_equity'].cummax()
                combined_equity['drawdown'] = (combined_equity['total_equity'] - combined_equity['peak']) / combined_equity['peak']
                
                # Calculate performance metrics
                final_equity = combined_equity['total_equity'].iloc[-1]
                max_drawdown = combined_equity['drawdown'].min()
                returns = final_equity / start_equity - 1
                
                win_trades = [t for t in trades if t['pnl'] > 0]
                loss_trades = [t for t in trades if t['pnl'] <= 0]
                
                performance_metrics = {
                    'final_equity': final_equity,
                    'returns': returns,
                    'max_drawdown': max_drawdown,
                    'total_trades': len(trades),
                    'win_trades': len(win_trades),
                    'loss_trades': len(loss_trades),
                    'win_rate': len(win_trades) / len(trades) if trades else 0,
                    'profit_factor': sum(t['pnl'] for t in win_trades) / abs(sum(t['pnl'] for t in loss_trades)) if loss_trades else float('inf'),
                    'avg_profit': sum(t['pnl'] for t in trades) / len(trades) if trades else 0,
                    'avg_win': sum(t['pnl'] for t in win_trades) / len(win_trades) if win_trades else 0,
                    'avg_loss': sum(t['pnl'] for t in loss_trades) / len(loss_trades) if loss_trades else 0
                }
                
                return BacktestResult(
                    signals=signals,
                    trades=trades,
                    performance_metrics=performance_metrics,
                    equity_curve=combined_equity,
                    drawdown=combined_equity[['timestamp', 'drawdown']]
                )
            
            return BacktestResult(
                signals=signals,
                trades=trades,
                performance_metrics={'error': 'No valid symbols for backtest'}
            )
                
        except Exception as e:
            logger.error(f"Error running backtest: {str(e)}", exc_info=True)
            return BacktestResult(
                performance_metrics={'error': str(e)}
            ) 