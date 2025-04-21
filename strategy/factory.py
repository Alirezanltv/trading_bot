"""
Strategy Factory Module

This module provides the strategy factory for the trading system.
"""

import os
import sys
import importlib
import inspect
import asyncio
import logging
from typing import Dict, Any, List, Type, Optional, Union, Tuple

from trading_system.core.component import Component, ComponentStatus
from trading_system.core.message_bus import MessageBus, MessageTypes
from trading_system.core.logging import get_logger

logger = get_logger("strategy.factory")

# Global factory instance
_factory_instance = None

def strategy_factory(config: Dict[str, Any] = None, message_bus: MessageBus = None) -> 'StrategyFactory':
    """
    Get or create the strategy factory.
    
    Args:
        config: Configuration for the factory
        message_bus: Message bus instance
        
    Returns:
        Strategy factory instance
    """
    global _factory_instance
    
    if _factory_instance is None:
        _factory_instance = StrategyFactory(config, message_bus)
        
    return _factory_instance

class Strategy:
    """
    Base Strategy Class
    
    All trading strategies should inherit from this base class.
    """
    
    def __init__(self, strategy_id: str, config: Dict[str, Any] = None):
        """
        Initialize the strategy.
        
        Args:
            strategy_id: Strategy ID
            config: Strategy configuration
        """
        self.strategy_id = strategy_id
        self.config = config or {}
        self.name = self.config.get("name", strategy_id)
        
        # Operational state
        self.enabled = False
        self.status = "INITIALIZED"
        
        # Market data
        self.market_data = {}
        
        # Trading parameters
        self.symbols = self.config.get("symbols", [])
        self.timeframes = self.config.get("timeframes", ["1h"])
        
    def initialize(self) -> bool:
        """
        Initialize the strategy.
        
        Returns:
            Initialization success
        """
        self.status = "OPERATIONAL"
        self.enabled = True
        return True
    
    def start(self) -> bool:
        """
        Start the strategy.
        
        Returns:
            Start success
        """
        self.enabled = True
        logger.info(f"Started strategy: {self.strategy_id}")
        return True
    
    def stop(self) -> bool:
        """
        Stop the strategy.
        
        Returns:
            Stop success
        """
        self.enabled = False
        logger.info(f"Stopped strategy: {self.strategy_id}")
        return True
    
    def update_market_data(self, market_data: Dict[str, Any]) -> None:
        """
        Update market data.
        
        Args:
            market_data: Market data
        """
        symbol = market_data.get("symbol")
        timeframe = market_data.get("timeframe")
        
        if not symbol or not timeframe:
            return
        
        if symbol not in self.market_data:
            self.market_data[symbol] = {}
        
        self.market_data[symbol][timeframe] = market_data
    
    def analyze(self, symbol: str, timeframe: str) -> Dict[str, Any]:
        """
        Analyze market data and generate signals.
        
        Args:
            symbol: Symbol to analyze
            timeframe: Timeframe to analyze
            
        Returns:
            Analysis result
        """
        # This method should be implemented by subclasses
        return {
            "strategy_id": self.strategy_id,
            "symbol": symbol,
            "timeframe": timeframe,
            "signal": None,
            "timestamp": None
        }
    
    def backtest(self, symbol: str, timeframe: str, market_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Backtest the strategy.
        
        Args:
            symbol: Symbol to backtest
            timeframe: Timeframe to backtest
            market_data: Market data for backtesting
            
        Returns:
            Backtest result
        """
        # This method should be implemented by subclasses
        return {
            "strategy_id": self.strategy_id,
            "symbol": symbol,
            "timeframe": timeframe,
            "start_time": None,
            "end_time": None,
            "trades": [],
            "profit_loss": 0.0,
            "win_rate": 0.0,
            "max_drawdown": 0.0
        }

class SimpleMACD(Strategy):
    """
    Simple MACD Strategy
    
    This strategy uses the MACD indicator to generate trading signals.
    """
    
    def __init__(self, strategy_id: str, config: Dict[str, Any] = None):
        """
        Initialize the strategy.
        
        Args:
            strategy_id: Strategy ID
            config: Strategy configuration
        """
        super().__init__(strategy_id, config)
        
        # MACD parameters
        self.fast_length = self.config.get("fast_length", 12)
        self.slow_length = self.config.get("slow_length", 26)
        self.signal_length = self.config.get("signal_length", 9)
        
        # Trading parameters
        self.buy_threshold = self.config.get("buy_threshold", 0.0)
        self.sell_threshold = self.config.get("sell_threshold", 0.0)
    
    def analyze(self, symbol: str, timeframe: str) -> Dict[str, Any]:
        """
        Analyze market data and generate signals.
        
        Args:
            symbol: Symbol to analyze
            timeframe: Timeframe to analyze
            
        Returns:
            Analysis result
        """
        try:
            # Check if market data is available
            if (symbol not in self.market_data or 
                timeframe not in self.market_data[symbol]):
                return {
                    "strategy_id": self.strategy_id,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "signal": None,
                    "timestamp": None
                }
            
            # Get market data
            market_data = self.market_data[symbol][timeframe]
            
            # Check if indicators are available
            if "indicators" not in market_data:
                return {
                    "strategy_id": self.strategy_id,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "signal": None,
                    "timestamp": None
                }
            
            # Get indicators
            indicators = market_data["indicators"]
            
            # Check if MACD is available
            if "macd" not in indicators:
                return {
                    "strategy_id": self.strategy_id,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "signal": None,
                    "timestamp": None
                }
            
            # Get MACD
            macd = indicators["macd"]
            
            # Check if MACD components are available
            if ("macd_line" not in macd or 
                "signal_line" not in macd or 
                "histogram" not in macd):
                return {
                    "strategy_id": self.strategy_id,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "signal": None,
                    "timestamp": None
                }
            
            # Get MACD components
            macd_line = macd["macd_line"]
            signal_line = macd["signal_line"]
            histogram = macd["histogram"]
            
            # Check if there's enough data
            if len(macd_line) < 2 or len(signal_line) < 2 or len(histogram) < 2:
                return {
                    "strategy_id": self.strategy_id,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "signal": None,
                    "timestamp": None
                }
            
            # Get latest values
            latest_macd = macd_line[-1]
            prev_macd = macd_line[-2]
            latest_signal = signal_line[-1]
            prev_signal = signal_line[-2]
            latest_histogram = histogram[-1]
            prev_histogram = histogram[-2]
            
            # Generate signal
            signal = None
            
            # MACD crosses signal line from below (bullish)
            if prev_macd < prev_signal and latest_macd > latest_signal:
                signal = "buy"
            
            # MACD crosses signal line from above (bearish)
            elif prev_macd > prev_signal and latest_macd < latest_signal:
                signal = "sell"
            
            # Histogram turns positive (bullish)
            elif prev_histogram < 0 and latest_histogram > 0:
                signal = "buy"
            
            # Histogram turns negative (bearish)
            elif prev_histogram > 0 and latest_histogram < 0:
                signal = "sell"
            
            # Get current price
            current_price = None
            if "ohlcv" in market_data and len(market_data["ohlcv"]) > 0:
                current_price = market_data["ohlcv"][-1][4]  # Close price
            
            # Get timestamp
            timestamp = None
            if "ohlcv" in market_data and len(market_data["ohlcv"]) > 0:
                timestamp = market_data["ohlcv"][-1][0]  # Timestamp
            
            return {
                "strategy_id": self.strategy_id,
                "symbol": symbol,
                "timeframe": timeframe,
                "signal": signal,
                "price": current_price,
                "timestamp": timestamp,
                "indicators": {
                    "macd": latest_macd,
                    "signal": latest_signal,
                    "histogram": latest_histogram
                }
            }
            
        except Exception as e:
            logger.error(f"Error analyzing market data: {str(e)}", exc_info=True)
            return {
                "strategy_id": self.strategy_id,
                "symbol": symbol,
                "timeframe": timeframe,
                "signal": None,
                "timestamp": None,
                "error": str(e)
            }
    
    def backtest(self, symbol: str, timeframe: str, market_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Backtest the strategy.
        
        Args:
            symbol: Symbol to backtest
            timeframe: Timeframe to backtest
            market_data: Market data for backtesting
            
        Returns:
            Backtest result
        """
        try:
            # Check if market data is valid
            if not market_data or "ohlcv" not in market_data:
                return {
                    "strategy_id": self.strategy_id,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "error": "Invalid market data"
                }
            
            # Get OHLCV data
            ohlcv = market_data["ohlcv"]
            
            # Check if there's enough data
            if len(ohlcv) < self.slow_length + self.signal_length:
                return {
                    "strategy_id": self.strategy_id,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "error": "Not enough data for MACD calculation"
                }
            
            # Calculate MACD
            close_prices = [candle[4] for candle in ohlcv]
            timestamps = [candle[0] for candle in ohlcv]
            
            # Calculate EMAs
            fast_ema = self._calculate_ema(close_prices, self.fast_length)
            slow_ema = self._calculate_ema(close_prices, self.slow_length)
            
            # Calculate MACD line
            macd_line = [fast - slow for fast, slow in zip(fast_ema, slow_ema)]
            
            # Calculate signal line
            signal_line = self._calculate_ema(macd_line, self.signal_length)
            
            # Calculate histogram
            histogram = [macd - signal for macd, signal in zip(macd_line, signal_line)]
            
            # Generate signals
            signals = []
            trades = []
            positions = []
            
            position = None
            
            for i in range(self.slow_length + self.signal_length, len(close_prices)):
                # Check for signals
                current_macd = macd_line[i]
                prev_macd = macd_line[i-1]
                current_signal = signal_line[i]
                prev_signal = signal_line[i-1]
                current_histogram = histogram[i]
                prev_histogram = histogram[i-1]
                
                signal = None
                
                # MACD crosses signal line from below (bullish)
                if prev_macd < prev_signal and current_macd > current_signal:
                    signal = "buy"
                
                # MACD crosses signal line from above (bearish)
                elif prev_macd > prev_signal and current_macd < current_signal:
                    signal = "sell"
                
                # Histogram turns positive (bullish)
                elif prev_histogram < 0 and current_histogram > 0:
                    signal = "buy"
                
                # Histogram turns negative (bearish)
                elif prev_histogram > 0 and current_histogram < 0:
                    signal = "sell"
                
                if signal:
                    signals.append({
                        "timestamp": timestamps[i],
                        "price": close_prices[i],
                        "signal": signal,
                        "macd": current_macd,
                        "signal_line": current_signal,
                        "histogram": current_histogram
                    })
                    
                    # Execute trades based on signals
                    if signal == "buy" and position is None:
                        # Open long position
                        position = {
                            "type": "long",
                            "entry_time": timestamps[i],
                            "entry_price": close_prices[i],
                            "amount": 1.0
                        }
                        positions.append(position)
                    
                    elif signal == "sell" and position is not None and position["type"] == "long":
                        # Close long position
                        exit_price = close_prices[i]
                        profit_loss = (exit_price - position["entry_price"]) / position["entry_price"]
                        
                        trades.append({
                            "entry_time": position["entry_time"],
                            "entry_price": position["entry_price"],
                            "exit_time": timestamps[i],
                            "exit_price": exit_price,
                            "amount": position["amount"],
                            "profit_loss": profit_loss,
                            "type": "long"
                        })
                        
                        position = None
            
            # Close any open position at the end
            if position is not None:
                exit_price = close_prices[-1]
                profit_loss = (exit_price - position["entry_price"]) / position["entry_price"]
                
                trades.append({
                    "entry_time": position["entry_time"],
                    "entry_price": position["entry_price"],
                    "exit_time": timestamps[-1],
                    "exit_price": exit_price,
                    "amount": position["amount"],
                    "profit_loss": profit_loss,
                    "type": "long"
                })
            
            # Calculate performance metrics
            total_profit_loss = sum(trade["profit_loss"] for trade in trades) if trades else 0.0
            winning_trades = sum(1 for trade in trades if trade["profit_loss"] > 0) if trades else 0
            losing_trades = sum(1 for trade in trades if trade["profit_loss"] <= 0) if trades else 0
            win_rate = winning_trades / len(trades) * 100 if trades else 0.0
            
            # Calculate max drawdown
            max_drawdown = 0.0
            peak = 0.0
            
            for trade in trades:
                if trade["profit_loss"] > 0:
                    peak = max(peak, trade["profit_loss"])
                else:
                    drawdown = peak - trade["profit_loss"]
                    max_drawdown = max(max_drawdown, drawdown)
            
            return {
                "strategy_id": self.strategy_id,
                "symbol": symbol,
                "timeframe": timeframe,
                "start_time": timestamps[0],
                "end_time": timestamps[-1],
                "trades": trades,
                "signals": signals,
                "trade_count": len(trades),
                "winning_trades": winning_trades,
                "losing_trades": losing_trades,
                "profit_loss": total_profit_loss,
                "win_rate": win_rate,
                "max_drawdown": max_drawdown
            }
            
        except Exception as e:
            logger.error(f"Error backtesting strategy: {str(e)}", exc_info=True)
            return {
                "strategy_id": self.strategy_id,
                "symbol": symbol,
                "timeframe": timeframe,
                "error": str(e)
            }
    
    def _calculate_ema(self, prices: List[float], period: int) -> List[float]:
        """
        Calculate EMA for a list of prices.
        
        Args:
            prices: List of prices
            period: EMA period
            
        Returns:
            List of EMA values
        """
        ema = []
        
        # Initialize with SMA
        sma = sum(prices[:period]) / period
        multiplier = 2 / (period + 1)
        
        ema.append(sma)
        
        # Calculate EMA
        for i in range(1, len(prices) - period + 1):
            ema_value = (prices[i + period - 1] - ema[i-1]) * multiplier + ema[i-1]
            ema.append(ema_value)
        
        # Pad the beginning with None values
        return [None] * (period - 1) + ema

class StrategyFactory(Component):
    """
    Strategy Factory
    
    This class manages the creation and operation of trading strategies.
    """
    
    def __init__(self, config: Dict[str, Any] = None, message_bus: MessageBus = None, market_data_manager=None):
        """
        Initialize the strategy factory.
        
        Args:
            config: Strategy configuration
            message_bus: Message bus for communication
            market_data_manager: Market data manager for retrieving market data
        """
        super().__init__(name="StrategyFactory")
        
        # Configuration
        self.config = config or {}
        self.message_bus = message_bus
        self.market_data_manager = market_data_manager
        
        # Strategy registry
        self.strategy_registry = {}
        
        # Active strategies
        self.strategies = {}
        
        # Register built-in strategies
        self.register_strategy_class("SimpleMACD", SimpleMACD)
    
    async def initialize(self) -> bool:
        """
        Initialize the strategy factory.
        
        Returns:
            Initialization success
        """
        try:
            self._status = ComponentStatus.INITIALIZING
            logger.info("Initializing strategy factory")
            
            # Load strategies from configuration
            await self._load_strategies_from_config()
            
            # Subscribe to market data updates if message bus is available
            if self.message_bus is not None:
                try:
                    await self.message_bus.subscribe(MessageTypes.MARKET_DATA_UPDATE, self._handle_market_data_update)
                except Exception as e:
                    logger.warning(f"Failed to subscribe to message bus: {str(e)}")
            else:
                logger.warning("No message bus provided to strategy factory")
            
            self._status = ComponentStatus.INITIALIZED
            logger.info(f"Strategy factory initialized with {len(self.strategies)} strategies")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing strategy factory: {str(e)}", exc_info=True)
            self._status = ComponentStatus.ERROR
            self._error = str(e)
            return False
    
    async def start(self) -> bool:
        """
        Start the strategy factory.
        
        Returns:
            Start success
        """
        try:
            logger.info("Starting strategy factory")
            
            # Start all strategies
            for strategy_id, strategy in self.strategies.items():
                strategy.start()
            
            self._status = ComponentStatus.OPERATIONAL
            logger.info("Strategy factory started successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error starting strategy factory: {str(e)}", exc_info=True)
            self._status = ComponentStatus.ERROR
            self._error = str(e)
            return False
    
    async def stop(self) -> bool:
        """
        Stop the strategy factory.
        
        Returns:
            Stop success
        """
        try:
            logger.info("Stopping strategy factory")
            
            # Stop all strategies
            for strategy_id, strategy in self.strategies.items():
                strategy.stop()
            
            self._status = ComponentStatus.STOPPED
            logger.info("Strategy factory stopped successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping strategy factory: {str(e)}", exc_info=True)
            self._status = ComponentStatus.ERROR
            self._error = str(e)
            return False
    
    def register_strategy_class(self, strategy_type: str, strategy_class: Type[Strategy]) -> None:
        """
        Register a strategy class.
        
        Args:
            strategy_type: Strategy type identifier
            strategy_class: Strategy class
        """
        self.strategy_registry[strategy_type] = strategy_class
        logger.debug(f"Registered strategy type: {strategy_type}")
    
    async def create_strategy(self, strategy_id: str, strategy_type: str, config: Dict[str, Any] = None) -> Optional[Strategy]:
        """
        Create a new strategy instance.
        
        Args:
            strategy_id: Strategy ID
            strategy_type: Strategy type
            config: Strategy configuration
            
        Returns:
            Strategy instance or None if creation failed
        """
        try:
            # Check if strategy type is registered
            if strategy_type not in self.strategy_registry:
                logger.error(f"Unknown strategy type: {strategy_type}")
                return None
            
            # Check if strategy ID already exists
            if strategy_id in self.strategies:
                logger.warning(f"Strategy {strategy_id} already exists")
                return self.strategies[strategy_id]
            
            # Create configuration
            if config is None:
                config = {}
            
            config["id"] = strategy_id
            config["type"] = strategy_type
            
            # Create strategy instance
            strategy_class = self.strategy_registry[strategy_type]
            strategy = strategy_class(strategy_id, config)
            
            # Initialize strategy
            strategy.initialize()
            
            # Add to active strategies
            self.strategies[strategy_id] = strategy
            
            logger.info(f"Created strategy: {strategy_id} (type: {strategy_type})")
            return strategy
            
        except Exception as e:
            logger.error(f"Error creating strategy: {str(e)}", exc_info=True)
            return None
    
    async def delete_strategy(self, strategy_id: str) -> bool:
        """
        Delete a strategy.
        
        Args:
            strategy_id: Strategy ID
            
        Returns:
            Delete success
        """
        try:
            # Check if strategy exists
            if strategy_id not in self.strategies:
                logger.warning(f"Strategy {strategy_id} not found")
                return False
            
            # Stop strategy
            strategy = self.strategies[strategy_id]
            strategy.stop()
            
            # Remove from active strategies
            del self.strategies[strategy_id]
            
            logger.info(f"Deleted strategy: {strategy_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting strategy: {str(e)}", exc_info=True)
            return False
    
    async def get_strategy(self, strategy_id: str) -> Optional[Strategy]:
        """
        Get a strategy instance.
        
        Args:
            strategy_id: Strategy ID
            
        Returns:
            Strategy instance or None if not found
        """
        return self.strategies.get(strategy_id)
    
    async def get_all_strategies(self) -> Dict[str, Strategy]:
        """
        Get all strategies.
        
        Returns:
            Dictionary of strategy ID to strategy instance
        """
        return dict(self.strategies)
    
    async def _load_strategies_from_config(self) -> None:
        """
        Load strategies from configuration.
        """
        try:
            # Get strategies configuration
            strategies_config = self.config.get("strategies", [])
            
            for strategy_config in strategies_config:
                strategy_id = strategy_config.get("id")
                strategy_type = strategy_config.get("type")
                
                if not strategy_id or not strategy_type:
                    logger.warning("Strategy ID or type not provided in configuration")
                    continue
                
                await self.create_strategy(strategy_id, strategy_type, strategy_config)
            
        except Exception as e:
            logger.error(f"Error loading strategies from configuration: {str(e)}", exc_info=True)
    
    async def _handle_market_data_update(self, message_type: MessageTypes, data: Dict[str, Any]) -> None:
        """
        Handle market data update.
        
        Args:
            message_type: Message type
            data: Message data
        """
        try:
            # Check if market data is valid
            if not data or "symbol" not in data or "timeframe" not in data:
                return
            
            symbol = data["symbol"]
            timeframe = data["timeframe"]
            
            # Update market data for all strategies
            for strategy_id, strategy in self.strategies.items():
                # Check if strategy is interested in this symbol and timeframe
                if (not strategy.symbols or symbol in strategy.symbols) and (not strategy.timeframes or timeframe in strategy.timeframes):
                    # Update market data
                    strategy.update_market_data(data)
                    
                    # Analyze market data
                    analysis = strategy.analyze(symbol, timeframe)
                    
                    # If there's a signal, publish it
                    if analysis and analysis.get("signal") and self.message_bus:
                        await self.message_bus.publish(
                            MessageTypes.STRATEGY_SIGNAL,
                            analysis
                        )
            
        except Exception as e:
            logger.error(f"Error handling market data update: {str(e)}", exc_info=True)
    
    async def run_backtest(self, strategy_id: str, symbol: str, timeframe: str, market_data: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Run backtest for a strategy.
        
        Args:
            strategy_id: Strategy ID
            symbol: Symbol to backtest
            timeframe: Timeframe to backtest
            market_data: Market data for backtesting
            
        Returns:
            Backtest result
        """
        try:
            # Check if strategy exists
            strategy = await self.get_strategy(strategy_id)
            if not strategy:
                return {"error": f"Strategy {strategy_id} not found"}
            
            # If market data is not provided, get it from market data manager
            if not market_data and self.market_data_manager:
                market_data = await self.market_data_manager.get_market_data(symbol, timeframe)
            
            # Run backtest
            result = strategy.backtest(symbol, timeframe, market_data)
            
            # Publish backtest result
            if self.message_bus:
                await self.message_bus.publish(
                    MessageTypes.BACKTEST_RESULT,
                    {
                        "strategy_id": strategy_id,
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "result": result
                    }
                )
            
            return result
            
        except Exception as e:
            logger.error(f"Error running backtest: {str(e)}", exc_info=True)
            return {"error": str(e)} 