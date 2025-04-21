"""
Backtesting Engine

This module provides the backtesting framework for evaluating trading strategies
using historical market data.
"""

import asyncio
import json
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union, Set, Tuple, Callable

import numpy as np
import pandas as pd

from trading_system.core.logging import get_logger
from trading_system.market_data.timeseries_db import TimeSeriesDB, get_timeseries_db
from trading_system.strategy.base import BaseStrategy, SignalType

logger = get_logger("strategy.backtest")


class BacktestConfig:
    """Configuration for a backtest."""
    
    def __init__(self, 
                symbol: str,
                timeframe: str,
                start_time: Union[str, datetime],
                end_time: Union[str, datetime],
                initial_capital: float = 10000.0,
                commission: float = 0.001,
                slippage: float = 0.0005,
                use_market_orders: bool = True,
                position_sizing: str = "fixed",
                position_size: float = 1.0,
                allow_short: bool = True,
                include_fees: bool = True,
                metrics: List[str] = None,
                data_source: str = None,
                warmup_period: int = 0,
                quote_currency: str = "USDT"):
        """
        Initialize the backtest configuration.
        
        Args:
            symbol: Trading symbol (e.g., "BTC/USDT")
            timeframe: Chart timeframe (e.g., "1h", "1d")
            start_time: Start time for the backtest
            end_time: End time for the backtest
            initial_capital: Initial capital for the backtest
            commission: Commission rate as a decimal (e.g., 0.001 for 0.1%)
            slippage: Slippage as a decimal (e.g., 0.0005 for 0.05%)
            use_market_orders: Whether to simulate market orders (True) or limit orders (False)
            position_sizing: Position sizing method ("fixed", "percent", "risk")
            position_size: Position size (interpretation depends on position_sizing)
            allow_short: Whether to allow short positions
            include_fees: Whether to include fees in calculations
            metrics: List of metrics to calculate
            data_source: Source of historical data
            warmup_period: Number of candles to warm up strategy before trading
            quote_currency: Quote currency for the trading pair
        """
        self.symbol = symbol
        self.timeframe = timeframe
        
        # Parse start and end times
        if isinstance(start_time, str):
            self.start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        else:
            self.start_time = start_time
            
        if isinstance(end_time, str):
            self.end_time = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
        else:
            self.end_time = end_time
        
        self.initial_capital = initial_capital
        self.commission = commission
        self.slippage = slippage
        self.use_market_orders = use_market_orders
        self.position_sizing = position_sizing
        self.position_size = position_size
        self.allow_short = allow_short
        self.include_fees = include_fees
        
        # Default metrics if none provided
        self.metrics = metrics or [
            "total_return", "annualized_return", "max_drawdown", 
            "sharpe_ratio", "win_rate", "profit_factor", "trades"
        ]
        
        self.data_source = data_source
        self.warmup_period = warmup_period
        self.quote_currency = quote_currency
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "initial_capital": self.initial_capital,
            "commission": self.commission,
            "slippage": self.slippage,
            "use_market_orders": self.use_market_orders,
            "position_sizing": self.position_sizing,
            "position_size": self.position_size,
            "allow_short": self.allow_short,
            "include_fees": self.include_fees,
            "metrics": self.metrics,
            "data_source": self.data_source,
            "warmup_period": self.warmup_period,
            "quote_currency": self.quote_currency
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BacktestConfig':
        """Create configuration from dictionary."""
        return cls(
            symbol=data.get("symbol", "BTC/USDT"),
            timeframe=data.get("timeframe", "1h"),
            start_time=data.get("start_time", (datetime.now() - timedelta(days=30)).isoformat()),
            end_time=data.get("end_time", datetime.now().isoformat()),
            initial_capital=data.get("initial_capital", 10000.0),
            commission=data.get("commission", 0.001),
            slippage=data.get("slippage", 0.0005),
            use_market_orders=data.get("use_market_orders", True),
            position_sizing=data.get("position_sizing", "fixed"),
            position_size=data.get("position_size", 1.0),
            allow_short=data.get("allow_short", True),
            include_fees=data.get("include_fees", True),
            metrics=data.get("metrics"),
            data_source=data.get("data_source"),
            warmup_period=data.get("warmup_period", 0),
            quote_currency=data.get("quote_currency", "USDT")
        )


class Trade:
    """Represents a trade in a backtest."""
    
    def __init__(self, 
                entry_time: datetime, 
                symbol: str,
                direction: str,
                entry_price: float,
                position_size: float,
                stop_loss: Optional[float] = None,
                take_profit: Optional[float] = None):
        """
        Initialize a trade.
        
        Args:
            entry_time: Entry time
            symbol: Trading symbol
            direction: Trade direction ("buy" or "sell")
            entry_price: Entry price
            position_size: Position size in base currency
            stop_loss: Optional stop loss price
            take_profit: Optional take profit price
        """
        self.entry_time = entry_time
        self.symbol = symbol
        self.direction = direction
        self.entry_price = entry_price
        self.position_size = position_size
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        
        # Exit information
        self.exit_price = None
        self.exit_time = None
        self.exit_reason = None
        
        # Performance
        self.pnl = 0.0
        self.pnl_percent = 0.0
        self.fees = 0.0
    
    def close(self, exit_time: datetime, exit_price: float, reason: str, commission: float = 0.0) -> None:
        """
        Close the trade.
        
        Args:
            exit_time: Exit time
            exit_price: Exit price
            reason: Exit reason
            commission: Commission rate
        """
        self.exit_time = exit_time
        self.exit_price = exit_price
        self.exit_reason = reason
        
        # Calculate PnL
        if self.direction == "buy":
            price_diff = exit_price - self.entry_price
        else:
            price_diff = self.entry_price - exit_price
            
        self.pnl = price_diff * self.position_size
        
        # Calculate fees
        entry_fee = self.entry_price * self.position_size * commission
        exit_fee = exit_price * self.position_size * commission
        self.fees = entry_fee + exit_fee
        
        # Adjust PnL for fees
        self.pnl -= self.fees
        
        # Calculate percent PnL
        if self.direction == "buy":
            self.pnl_percent = (exit_price / self.entry_price) - 1.0
        else:
            self.pnl_percent = 1.0 - (exit_price / self.entry_price)
            
        # Adjust percent PnL for fees
        self.pnl_percent -= (self.fees / (self.entry_price * self.position_size))
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert trade to dictionary."""
        return {
            "entry_time": self.entry_time.isoformat(),
            "symbol": self.symbol,
            "direction": self.direction,
            "entry_price": self.entry_price,
            "position_size": self.position_size,
            "stop_loss": self.stop_loss,
            "take_profit": self.take_profit,
            "exit_price": self.exit_price,
            "exit_time": self.exit_time.isoformat() if self.exit_time else None,
            "exit_reason": self.exit_reason,
            "pnl": self.pnl,
            "pnl_percent": self.pnl_percent,
            "fees": self.fees
        }


class BacktestResult:
    """Holds the results of a backtest."""
    
    def __init__(self, 
                strategy_name: str,
                config: BacktestConfig,
                trades: List[Trade],
                equity_curve: List[float],
                timestamps: List[datetime]):
        """
        Initialize the backtest result.
        
        Args:
            strategy_name: Name of the strategy
            config: Backtest configuration
            trades: List of trades
            equity_curve: Equity curve (account value over time)
            timestamps: Timestamps for the equity curve
        """
        self.strategy_name = strategy_name
        self.config = config
        self.trades = trades
        self.equity_curve = equity_curve
        self.timestamps = timestamps
        
        # Calculate metrics
        self.metrics = self._calculate_metrics()
    
    def _calculate_metrics(self) -> Dict[str, Any]:
        """Calculate performance metrics."""
        metrics = {}
        
        if not self.trades or not self.equity_curve:
            return {
                "total_return": 0.0,
                "annualized_return": 0.0,
                "max_drawdown": 0.0,
                "sharpe_ratio": 0.0,
                "win_rate": 0.0,
                "profit_factor": 0.0,
                "trades": 0
            }
        
        # Convert to numpy arrays
        equity = np.array(self.equity_curve)
        
        # Basic return metrics
        metrics["initial_capital"] = self.config.initial_capital
        metrics["final_capital"] = self.equity_curve[-1]
        metrics["total_return"] = (metrics["final_capital"] / self.config.initial_capital) - 1.0
        
        # Annualized return
        days = (self.config.end_time - self.config.start_time).days
        if days > 0:
            metrics["annualized_return"] = pow(1.0 + metrics["total_return"], 365 / days) - 1.0
        else:
            metrics["annualized_return"] = 0.0
        
        # Max drawdown
        peak = np.maximum.accumulate(equity)
        drawdown = (peak - equity) / peak
        metrics["max_drawdown"] = drawdown.max()
        
        # Trade metrics
        metrics["trades"] = len(self.trades)
        
        winning_trades = [t for t in self.trades if t.pnl > 0]
        losing_trades = [t for t in self.trades if t.pnl <= 0]
        
        metrics["winning_trades"] = len(winning_trades)
        metrics["losing_trades"] = len(losing_trades)
        
        if metrics["trades"] > 0:
            metrics["win_rate"] = metrics["winning_trades"] / metrics["trades"]
        else:
            metrics["win_rate"] = 0.0
        
        # Profit factor
        total_profit = sum(t.pnl for t in winning_trades)
        total_loss = sum(abs(t.pnl) for t in losing_trades)
        
        if total_loss > 0:
            metrics["profit_factor"] = total_profit / total_loss
        else:
            metrics["profit_factor"] = float('inf') if total_profit > 0 else 0.0
        
        # Average trade metrics
        if metrics["trades"] > 0:
            metrics["avg_trade_pnl"] = sum(t.pnl for t in self.trades) / metrics["trades"]
            metrics["avg_trade_pnl_percent"] = sum(t.pnl_percent for t in self.trades) / metrics["trades"]
        else:
            metrics["avg_trade_pnl"] = 0.0
            metrics["avg_trade_pnl_percent"] = 0.0
        
        if metrics["winning_trades"] > 0:
            metrics["avg_winning_trade"] = sum(t.pnl for t in winning_trades) / metrics["winning_trades"]
        else:
            metrics["avg_winning_trade"] = 0.0
        
        if metrics["losing_trades"] > 0:
            metrics["avg_losing_trade"] = sum(t.pnl for t in losing_trades) / metrics["losing_trades"]
        else:
            metrics["avg_losing_trade"] = 0.0
        
        # Sharpe ratio (assuming daily returns)
        if len(equity) > 1:
            daily_returns = np.diff(equity) / equity[:-1]
            if len(daily_returns) > 0 and np.std(daily_returns) > 0:
                metrics["sharpe_ratio"] = (np.mean(daily_returns) / np.std(daily_returns)) * np.sqrt(252)
            else:
                metrics["sharpe_ratio"] = 0.0
        else:
            metrics["sharpe_ratio"] = 0.0
        
        return metrics
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "strategy_name": self.strategy_name,
            "config": self.config.to_dict(),
            "metrics": self.metrics,
            "trades": [t.to_dict() for t in self.trades],
            "equity_curve": self.equity_curve,
            "timestamps": [ts.isoformat() for ts in self.timestamps]
        }
    
    def save_to_file(self, file_path: str) -> bool:
        """
        Save the result to a file.
        
        Args:
            file_path: Path to save file
            
        Returns:
            Success flag
        """
        try:
            # Create directory if it doesn't exist
            directory = os.path.dirname(file_path)
            if directory and not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)
            
            # Convert to dictionary and save as JSON
            data = self.to_dict()
            
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to save backtest result: {e}")
            return False
    
    @classmethod
    def load_from_file(cls, file_path: str) -> Optional['BacktestResult']:
        """
        Load a result from a file.
        
        Args:
            file_path: Path to load file
            
        Returns:
            Backtest result or None if failed
        """
        try:
            if not os.path.exists(file_path):
                return None
            
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Parse data
            strategy_name = data.get("strategy_name", "Unknown")
            config = BacktestConfig.from_dict(data.get("config", {}))
            
            # Parse trades
            trades = []
            for trade_data in data.get("trades", []):
                trade = Trade(
                    entry_time=datetime.fromisoformat(trade_data["entry_time"].replace('Z', '+00:00')),
                    symbol=trade_data["symbol"],
                    direction=trade_data["direction"],
                    entry_price=trade_data["entry_price"],
                    position_size=trade_data["position_size"],
                    stop_loss=trade_data.get("stop_loss"),
                    take_profit=trade_data.get("take_profit")
                )
                
                if trade_data.get("exit_time"):
                    trade.exit_time = datetime.fromisoformat(trade_data["exit_time"].replace('Z', '+00:00'))
                    trade.exit_price = trade_data["exit_price"]
                    trade.exit_reason = trade_data["exit_reason"]
                    trade.pnl = trade_data["pnl"]
                    trade.pnl_percent = trade_data["pnl_percent"]
                    trade.fees = trade_data.get("fees", 0.0)
                
                trades.append(trade)
            
            # Parse equity curve and timestamps
            equity_curve = data.get("equity_curve", [])
            timestamps = [
                datetime.fromisoformat(ts.replace('Z', '+00:00'))
                for ts in data.get("timestamps", [])
            ]
            
            # Create result
            result = cls(strategy_name, config, trades, equity_curve, timestamps)
            
            # Override metrics if they exist in the data
            if "metrics" in data:
                result.metrics = data["metrics"]
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to load backtest result: {e}")
            return None


class BacktestEngine:
    """
    Backtesting engine for evaluating trading strategies.
    
    Features:
    - Historical data retrieval from time-series database
    - Strategy initialization and signal generation
    - Trade execution simulation
    - Performance metrics calculation
    """
    
    def __init__(self, config: BacktestConfig = None):
        """
        Initialize the backtest engine.
        
        Args:
            config: Backtest configuration
        """
        self.config = config or BacktestConfig(
            symbol="BTC/USDT",
            timeframe="1h",
            start_time=datetime.now() - timedelta(days=30),
            end_time=datetime.now()
        )
        
        # Database connection for historical data
        self.db = get_timeseries_db()
        
        # State for the backtest
        self.current_time = None
        self.current_price = None
        self.current_balance = 0.0
        self.current_position = 0.0
        self.position_value = 0.0
        
        # Current strategy being tested
        self.strategy = None
        
        # Result tracking
        self.trades = []
        self.equity_curve = []
        self.timestamps = []
        self.open_trade = None
    
    async def load_market_data(self) -> pd.DataFrame:
        """
        Load historical market data for the backtest.
        
        Returns:
            DataFrame with historical market data
        """
        try:
            # Convert times to milliseconds for the database
            start_ms = int(self.config.start_time.timestamp() * 1000)
            end_ms = int(self.config.end_time.timestamp() * 1000)
            
            # Get data from time-series database
            data = await self.db.get_market_data(
                symbol=self.config.symbol,
                timeframe=self.config.timeframe,
                start_time=start_ms,
                end_time=end_ms,
                source=self.config.data_source
            )
            
            if not data:
                logger.error(f"No historical data found for {self.config.symbol} {self.config.timeframe}")
                return pd.DataFrame()
                
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Ensure required columns exist
            required_columns = ["time", "open", "high", "low", "close", "volume"]
            for col in required_columns:
                if col not in df.columns:
                    logger.error(f"Required column {col} not found in historical data")
                    return pd.DataFrame()
            
            # Convert time to datetime
            df["time"] = pd.to_datetime(df["time"], unit="ms")
            
            # Set time as index
            df.set_index("time", inplace=True)
            
            # Sort by time
            df.sort_index(inplace=True)
            
            logger.info(f"Loaded {len(df)} candles for {self.config.symbol} {self.config.timeframe}")
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to load historical data: {e}")
            return pd.DataFrame()
    
    def _format_market_data(self, row: pd.Series) -> Dict[str, Any]:
        """
        Format a row of market data for strategy updates.
        
        Args:
            row: DataFrame row
            
        Returns:
            Formatted market data dictionary
        """
        # Create market data dictionary compatible with strategy.update()
        data = {
            "symbol": self.config.symbol,
            "timeframe": self.config.timeframe,
            "time": int(row.name.timestamp() * 1000),
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": float(row["volume"])
        }
        
        # Add any additional columns
        for col in row.index:
            if col not in ["open", "high", "low", "close", "volume"]:
                data[col] = row[col]
        
        return data
    
    async def _update_strategy(self, row: pd.Series) -> None:
        """
        Update the strategy with new market data.
        
        Args:
            row: DataFrame row with market data
        """
        if not self.strategy:
            logger.error("No strategy set for backtest")
            return
        
        # Format market data
        data = self._format_market_data(row)
        
        # Update strategy
        await self.strategy.update(data)
    
    def _calculate_position_size(self, price: float, signal_type: str) -> float:
        """
        Calculate position size based on configuration.
        
        Args:
            price: Current price
            signal_type: Signal type
            
        Returns:
            Position size in base currency
        """
        if self.config.position_sizing == "fixed":
            # Fixed position size in base currency
            return self.config.position_size
            
        elif self.config.position_sizing == "percent":
            # Percent of portfolio value
            portfolio_value = self.current_balance
            if self.open_trade:
                portfolio_value += self.position_value
                
            position_value = portfolio_value * self.config.position_size
            return position_value / price
            
        elif self.config.position_sizing == "risk":
            # Risk-based position sizing requires stop loss
            if signal_type == "buy":
                # For long positions
                stop_level = 0.95  # Default 5% stop loss if not provided in signal
                risk_amount = self.current_balance * self.config.position_size
                risk_per_unit = price * (1 - stop_level)
                
                if risk_per_unit > 0:
                    return risk_amount / risk_per_unit
                
            else:
                # For short positions
                stop_level = 1.05  # Default 5% stop loss if not provided in signal
                risk_amount = self.current_balance * self.config.position_size
                risk_per_unit = price * (stop_level - 1)
                
                if risk_per_unit > 0:
                    return risk_amount / risk_per_unit
        
        # Default fallback
        return 1.0
    
    def _execute_trade(self, time: datetime, signal_type: str, price: float) -> None:
        """
        Execute a trade based on a signal.
        
        Args:
            time: Current time
            signal_type: Signal type (buy, sell, exit)
            price: Current price
        """
        # Adjust price for slippage
        if signal_type == "buy":
            adjusted_price = price * (1 + self.config.slippage)
        else:
            adjusted_price = price * (1 - self.config.slippage)
        
        # Handle existing trade
        if self.open_trade:
            # Check if signal is to exit current position
            if ((self.open_trade.direction == "buy" and signal_type == "sell") or 
                (self.open_trade.direction == "sell" and signal_type == "buy") or
                signal_type == "exit"):
                
                # Close the trade
                self.open_trade.close(
                    exit_time=time,
                    exit_price=adjusted_price,
                    reason=f"Signal {signal_type}",
                    commission=self.config.commission if self.config.include_fees else 0.0
                )
                
                # Update balance
                self.current_balance += self.open_trade.pnl
                
                # Add to completed trades
                self.trades.append(self.open_trade)
                
                # Clear current trade
                self.current_position = 0.0
                self.position_value = 0.0
                self.open_trade = None
                
                logger.debug(
                    f"Closed {self.trades[-1].direction} trade at {adjusted_price}: "
                    f"PnL = {self.trades[-1].pnl:.2f} ({self.trades[-1].pnl_percent:.2%})"
                )
        
        # Open new position if not exit signal
        if signal_type in ["buy", "sell"] and not self.open_trade:
            # Calculate position size
            position_size = self._calculate_position_size(adjusted_price, signal_type)
            
            # Check if we have enough balance
            position_value = position_size * adjusted_price
            
            if position_value <= self.current_balance:
                # Open new trade
                self.open_trade = Trade(
                    entry_time=time,
                    symbol=self.config.symbol,
                    direction=signal_type,
                    entry_price=adjusted_price,
                    position_size=position_size
                )
                
                # Update position
                self.current_position = position_size if signal_type == "buy" else -position_size
                self.position_value = position_value
                
                # Update balance (subtract initial position value)
                self.current_balance -= position_value
                
                # Calculate initial fees
                if self.config.include_fees:
                    fee = position_value * self.config.commission
                    self.current_balance -= fee
                    self.open_trade.fees = fee
                
                logger.debug(f"Opened {signal_type} trade at {adjusted_price}: size = {position_size:.6f}")
    
    def _update_position_value(self, price: float) -> None:
        """
        Update the current position value.
        
        Args:
            price: Current price
        """
        if self.open_trade:
            if self.open_trade.direction == "buy":
                self.position_value = self.open_trade.position_size * price
            else:
                # For short positions, value increases as price decreases
                factor = self.open_trade.entry_price / price if price > 0 else 1
                self.position_value = self.open_trade.position_size * self.open_trade.entry_price * factor
    
    def _calculate_equity(self) -> float:
        """
        Calculate current equity (balance + position value).
        
        Returns:
            Current equity
        """
        return self.current_balance + self.position_value
    
    async def run_backtest(self, strategy: BaseStrategy) -> BacktestResult:
        """
        Run a backtest for a strategy.
        
        Args:
            strategy: Strategy to test
            
        Returns:
            Backtest result
        """
        try:
            # Initialize
            self.strategy = strategy
            self.current_balance = self.config.initial_capital
            self.current_position = 0.0
            self.position_value = 0.0
            self.trades = []
            self.equity_curve = []
            self.timestamps = []
            self.open_trade = None
            
            # Initialize strategy
            await strategy.initialize()
            
            # Load historical data
            data = await self.load_market_data()
            
            if data.empty:
                logger.error("No historical data available for backtest")
                return None
            
            # Warm up period
            warmup_data = data.iloc[:self.config.warmup_period]
            
            logger.info(f"Warming up strategy with {len(warmup_data)} candles")
            
            for idx, row in warmup_data.iterrows():
                # Update strategy with historical data
                await self._update_strategy(row)
                
                # Update current time and price
                self.current_time = idx.to_pydatetime()
                self.current_price = row["close"]
            
            # Main backtest loop
            logger.info(f"Running backtest for {strategy.name} on {self.config.symbol} {self.config.timeframe}")
            
            # Skip warmup period for main loop
            main_data = data.iloc[self.config.warmup_period:]
            
            for idx, row in main_data.iterrows():
                # Update current time and price
                self.current_time = idx.to_pydatetime()
                self.current_price = row["close"]
                
                # Update strategy with new data
                await self._update_strategy(row)
                
                # Get signal from strategy
                signal = await strategy.get_signal(self.config.symbol, self.config.timeframe)
                
                # Process signal
                if signal:
                    signal_type = signal.get("type")
                    self._execute_trade(self.current_time, signal_type, self.current_price)
                
                # Update position value
                self._update_position_value(self.current_price)
                
                # Update equity curve
                equity = self._calculate_equity()
                self.equity_curve.append(equity)
                self.timestamps.append(self.current_time)
            
            # Close any open trade at the end
            if self.open_trade:
                self.open_trade.close(
                    exit_time=self.current_time,
                    exit_price=self.current_price,
                    reason="End of backtest",
                    commission=self.config.commission if self.config.include_fees else 0.0
                )
                
                # Update balance
                self.current_balance += self.open_trade.pnl
                
                # Add to completed trades
                self.trades.append(self.open_trade)
                
                # Clear current trade
                self.current_position = 0.0
                self.position_value = 0.0
                self.open_trade = None
                
                # Update final equity
                equity = self._calculate_equity()
                if self.equity_curve:
                    self.equity_curve[-1] = equity
            
            # Create backtest result
            result = BacktestResult(
                strategy_name=strategy.name,
                config=self.config,
                trades=self.trades,
                equity_curve=self.equity_curve,
                timestamps=self.timestamps
            )
            
            # Report summary
            logger.info(f"Backtest completed for {strategy.name}:")
            logger.info(f"Total Return: {result.metrics['total_return']:.2%}")
            logger.info(f"Annualized Return: {result.metrics['annualized_return']:.2%}")
            logger.info(f"Max Drawdown: {result.metrics['max_drawdown']:.2%}")
            logger.info(f"Sharpe Ratio: {result.metrics['sharpe_ratio']:.2f}")
            logger.info(f"Trades: {result.metrics['trades']}")
            logger.info(f"Win Rate: {result.metrics['win_rate']:.2%}")
            logger.info(f"Profit Factor: {result.metrics['profit_factor']:.2f}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error running backtest: {e}")
            return None
    
    def save_result(self, result: BacktestResult, file_path: Optional[str] = None) -> bool:
        """
        Save backtest result to file.
        
        Args:
            result: Backtest result
            file_path: Optional file path (default: data/backtest/strategy_symbol_timeframe.json)
            
        Returns:
            Success flag
        """
        if not result:
            return False
        
        if not file_path:
            # Create default file path
            symbol = result.config.symbol.replace("/", "_")
            file_path = f"data/backtest/{result.strategy_name}_{symbol}_{result.config.timeframe}.json"
        
        return result.save_to_file(file_path)
    
    @classmethod
    async def load_and_run(cls, strategy: BaseStrategy, config_file: str) -> Optional[BacktestResult]:
        """
        Load configuration from file and run backtest.
        
        Args:
            strategy: Strategy to test
            config_file: Path to configuration file
            
        Returns:
            Backtest result or None if failed
        """
        try:
            if not os.path.exists(config_file):
                logger.error(f"Configuration file not found: {config_file}")
                return None
            
            with open(config_file, 'r') as f:
                config_data = json.load(f)
            
            config = BacktestConfig.from_dict(config_data)
            engine = cls(config)
            
            return await engine.run_backtest(strategy)
            
        except Exception as e:
            logger.error(f"Failed to load and run backtest: {e}")
            return None


async def run_backtest(strategy_class_name: str, config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Run a backtest with the given strategy and configuration.
    
    Args:
        strategy_class_name: Fully qualified class name of the strategy
        config: Backtest configuration
        
    Returns:
        Backtest result as a dictionary or None if failed
    """
    try:
        # Import strategy class
        parts = strategy_class_name.split(".")
        module_name = ".".join(parts[:-1])
        class_name = parts[-1]
        
        module = __import__(module_name, fromlist=[class_name])
        strategy_cls = getattr(module, class_name)
        
        # Create strategy instance
        strategy_config = config.get("strategy_config", {})
        strategy = strategy_cls(strategy_config)
        
        # Create backtest config
        backtest_config = BacktestConfig.from_dict(config)
        
        # Create and run backtest
        engine = BacktestEngine(backtest_config)
        result = await engine.run_backtest(strategy)
        
        if result:
            # Save result if path provided
            if "result_path" in config:
                engine.save_result(result, config["result_path"])
            
            # Return result as dictionary
            return result.to_dict()
        
        return None
        
    except Exception as e:
        logger.error(f"Failed to run backtest: {e}")
        return None 