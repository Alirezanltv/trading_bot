"""
Strategy manager for high-reliability trading system.
Manages multiple strategies, handles real-time signals, and implements fallback mechanisms.
"""

import asyncio
import time
import logging
from typing import Dict, Any, List, Optional, Union, Callable
from datetime import datetime, timedelta
import threading

import pandas as pd

from trading_system.core.component import Component, ComponentStatus
from trading_system.core.logging import get_logger
from trading_system.core.message_bus import MessageBus, MessageTypes
from trading_system.strategy.base import Strategy, StrategySignal, StrategyResult
from trading_system.strategy.factory import strategy_factory

logger = get_logger("strategy.manager")

class StrategyManager(Component):
    """
    Strategy manager component.
    
    Manages multiple trading strategies, coordinates strategy execution,
    and implements fallback mechanisms for strategy degradation.
    """
    
    def __init__(self, config: Dict[str, Any] = None, message_bus: MessageBus = None):
        """
        Initialize the strategy manager.
        
        Args:
            config: Strategy manager configuration
            message_bus: Message bus instance
        """
        super().__init__(name="StrategyManager")
        
        # Store message bus
        self.message_bus = message_bus
        
        # Default configuration
        self.config = {
            "signal_aggregation_method": "weighted",  # weighted, majority, priority
            "enable_fallback": True,
            "performance_threshold": 0.5,  # Required performance to avoid fallback
            "fallback_cooldown": 300,  # 5 minutes in seconds
            "execution_timeout": 10,  # Maximum time a strategy can run in seconds
            "max_signal_age": 60,  # Maximum age of signal in seconds
            "polling_interval": 1,  # Interval to check for new market data in seconds
        }
        
        # Update with provided config
        if config:
            self.config.update(config)
        
        # Strategy instances
        self._strategies: Dict[str, Strategy] = {}
        self._active_strategies: Dict[str, bool] = {}
        self._strategy_priorities: Dict[str, int] = {}  # Lower number = higher priority
        self._strategy_weights: Dict[str, float] = {}  # Strategy weights for aggregation
        
        # Strategy performance
        self._performance_metrics: Dict[str, Dict[str, Any]] = {}
        self._fallback_status: Dict[str, bool] = {}
        self._fallback_until: Dict[str, int] = {}  # Timestamp until strategy stays in fallback
        
        # Signal tracking
        self._recent_signals: Dict[str, Dict[str, List[StrategySignal]]] = {}  # symbol -> strategy -> signals
        self._aggregated_signals: Dict[str, StrategySignal] = {}  # symbol -> aggregated signal
        
        # Market data cache
        self._market_data_cache: Dict[str, Dict[str, pd.DataFrame]] = {}  # symbol -> timeframe -> data
        self._market_data_timestamps: Dict[str, Dict[str, int]] = {}  # symbol -> timeframe -> timestamp
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Running status
        self.running = False
        self._stop_event = threading.Event()
        self._polling_thread = None
    
    def register_strategy(self, strategy: Strategy, priority: int = 0, weight: float = 1.0, active: bool = True) -> None:
        """
        Register a strategy with the manager.
        
        Args:
            strategy: Strategy instance
            priority: Strategy priority (lower number = higher priority)
            weight: Strategy weight for signal aggregation
            active: Whether the strategy is active initially
        """
        with self._lock:
            name = strategy.name
            self._strategies[name] = strategy
            self._active_strategies[name] = active
            self._strategy_priorities[name] = priority
            self._strategy_weights[name] = weight
            self._fallback_status[name] = False
            self._fallback_until[name] = 0
            self._performance_metrics[name] = {
                "execution_count": 0,
                "success_count": 0,
                "error_count": 0,
                "timeout_count": 0,
                "avg_execution_time": 0,
                "success_rate": 1.0,
                "last_execution_time": 0,
                "last_execution_duration": 0,
                "last_execution_status": "none"
            }
            
            logger.info(f"Registered strategy {name} with priority {priority} and weight {weight}")
    
    def unregister_strategy(self, name: str) -> bool:
        """
        Unregister a strategy from the manager.
        
        Args:
            name: Strategy name
            
        Returns:
            Success status
        """
        with self._lock:
            if name in self._strategies:
                self._strategies.pop(name)
                self._active_strategies.pop(name, None)
                self._strategy_priorities.pop(name, None)
                self._strategy_weights.pop(name, None)
                self._fallback_status.pop(name, None)
                self._fallback_until.pop(name, None)
                self._performance_metrics.pop(name, None)
                
                # Clean up signals
                for symbol in self._recent_signals:
                    self._recent_signals[symbol].pop(name, None)
                
                logger.info(f"Unregistered strategy {name}")
                return True
                
            return False
    
    def get_strategy(self, name: str) -> Optional[Strategy]:
        """
        Get a strategy by name.
        
        Args:
            name: Strategy name
            
        Returns:
            Strategy instance or None if not found
        """
        return self._strategies.get(name)
    
    def get_all_strategies(self) -> Dict[str, Strategy]:
        """
        Get all registered strategies.
        
        Returns:
            Dictionary of strategy instances
        """
        return dict(self._strategies)
    
    def get_active_strategies(self) -> Dict[str, Strategy]:
        """
        Get all active strategies.
        
        Returns:
            Dictionary of active strategy instances
        """
        return {name: self._strategies[name] for name in self._active_strategies if self._active_strategies[name]}
    
    def activate_strategy(self, name: str) -> bool:
        """
        Activate a strategy.
        
        Args:
            name: Strategy name
            
        Returns:
            Success status
        """
        with self._lock:
            if name in self._strategies:
                self._active_strategies[name] = True
                logger.info(f"Activated strategy {name}")
                return True
            return False
    
    def deactivate_strategy(self, name: str) -> bool:
        """
        Deactivate a strategy.
        
        Args:
            name: Strategy name
            
        Returns:
            Success status
        """
        with self._lock:
            if name in self._strategies:
                self._active_strategies[name] = False
                logger.info(f"Deactivated strategy {name}")
                return True
            return False
    
    def update_strategy_priority(self, name: str, priority: int) -> bool:
        """
        Update a strategy's priority.
        
        Args:
            name: Strategy name
            priority: New priority
            
        Returns:
            Success status
        """
        with self._lock:
            if name in self._strategies:
                self._strategy_priorities[name] = priority
                logger.info(f"Updated strategy {name} priority to {priority}")
                return True
            return False
    
    def update_strategy_weight(self, name: str, weight: float) -> bool:
        """
        Update a strategy's weight.
        
        Args:
            name: Strategy name
            weight: New weight
            
        Returns:
            Success status
        """
        with self._lock:
            if name in self._strategies:
                self._strategy_weights[name] = weight
                logger.info(f"Updated strategy {name} weight to {weight}")
                return True
            return False
    
    def set_fallback_status(self, name: str, fallback: bool, duration: int = None) -> bool:
        """
        Set a strategy's fallback status.
        
        Args:
            name: Strategy name
            fallback: Whether to put the strategy in fallback mode
            duration: Duration in seconds to stay in fallback mode (default: use config value)
            
        Returns:
            Success status
        """
        with self._lock:
            if name in self._strategies:
                self._fallback_status[name] = fallback
                
                if fallback and duration is not None:
                    self._fallback_until[name] = int(time.time()) + duration
                elif fallback:
                    self._fallback_until[name] = int(time.time()) + self.config["fallback_cooldown"]
                else:
                    self._fallback_until[name] = 0
                
                logger.info(f"Set strategy {name} fallback status to {fallback}")
                return True
            return False
    
    def get_fallback_status(self, name: str) -> bool:
        """
        Get a strategy's fallback status.
        
        Args:
            name: Strategy name
            
        Returns:
            Whether the strategy is in fallback mode
        """
        with self._lock:
            if name in self._fallback_status:
                # Check if fallback period has expired
                if self._fallback_status[name] and self._fallback_until[name] < int(time.time()):
                    self._fallback_status[name] = False
                    logger.info(f"Strategy {name} fallback period expired")
                
                return self._fallback_status[name]
            return False
    
    def get_performance_metrics(self, name: str = None) -> Union[Dict[str, Any], Dict[str, Dict[str, Any]]]:
        """
        Get performance metrics for a strategy or all strategies.
        
        Args:
            name: Strategy name (optional)
            
        Returns:
            Strategy performance metrics
        """
        with self._lock:
            if name:
                return dict(self._performance_metrics.get(name, {}))
            else:
                return {k: dict(v) for k, v in self._performance_metrics.items()}
    
    def update_market_data(self, symbol: str, timeframe: str, data: pd.DataFrame) -> None:
        """
        Update market data for a symbol and timeframe.
        
        Args:
            symbol: Trading symbol
            timeframe: Timeframe
            data: Market data DataFrame
        """
        with self._lock:
            # Ensure dictionaries are initialized
            if symbol not in self._market_data_cache:
                self._market_data_cache[symbol] = {}
                self._market_data_timestamps[symbol] = {}
            
            # Update data and timestamp
            self._market_data_cache[symbol][timeframe] = data
            self._market_data_timestamps[symbol][timeframe] = int(time.time())
            
            # Trigger strategy execution
            self._execute_strategies_for_market_data(symbol, timeframe, data)
    
    def _should_execute_strategy(self, name: str, symbol: str, timeframe: str) -> bool:
        """
        Determine if a strategy should be executed.
        
        Args:
            name: Strategy name
            symbol: Trading symbol
            timeframe: Timeframe
            
        Returns:
            Whether the strategy should be executed
        """
        # Check if strategy is active
        if not self._active_strategies.get(name, False):
            return False
        
        # Check if strategy is supported for this symbol/timeframe
        strategy = self._strategies.get(name)
        if not strategy:
            return False
        
        # Check strategy timeframes
        if hasattr(strategy, 'timeframes') and timeframe not in getattr(strategy, 'timeframes', []):
            return False
        
        # Check strategy symbols
        if hasattr(strategy, 'symbols') and symbol not in getattr(strategy, 'symbols', []):
            return False
        
        # Check fallback status
        if self.get_fallback_status(name):
            logger.debug(f"Strategy {name} in fallback mode, skipping execution")
            return False
        
        # Check last execution time
        metrics = self._performance_metrics.get(name, {})
        last_exec_time = metrics.get('last_execution_time', 0)
        
        # If executed recently, skip
        if time.time() - last_exec_time < 5:  # Minimum 5 seconds between executions
            return False
        
        return True
    
    def _execute_strategies_for_market_data(self, symbol: str, timeframe: str, data: pd.DataFrame) -> None:
        """
        Execute strategies for updated market data.
        
        Args:
            symbol: Trading symbol
            timeframe: Timeframe
            data: Market data DataFrame
        """
        try:
            strategies_to_execute = []
            
            # Find eligible strategies
            for name, strategy in self._strategies.items():
                if self._should_execute_strategy(name, symbol, timeframe):
                    strategies_to_execute.append((name, strategy))
            
            # Sort by priority
            strategies_to_execute.sort(key=lambda x: self._strategy_priorities.get(x[0], 999))
            
            # Execute each strategy
            for name, strategy in strategies_to_execute:
                self._execute_strategy_with_timeout(name, strategy, symbol, timeframe, data)
            
            # Aggregate signals if we have new results
            if strategies_to_execute:
                self._aggregate_signals_for_symbol(symbol)
        
        except Exception as e:
            logger.error(f"Error executing strategies for {symbol} {timeframe}: {str(e)}", exc_info=True)
    
    def _execute_strategy_with_timeout(self, name: str, strategy: Strategy, symbol: str, timeframe: str, data: pd.DataFrame) -> None:
        """
        Execute a strategy with timeout protection.
        
        Args:
            name: Strategy name
            strategy: Strategy instance
            symbol: Trading symbol
            timeframe: Timeframe
            data: Market data DataFrame
        """
        try:
            # Prepare market data for strategy
            market_data = {symbol: data}
            
            # Update metrics
            metrics = self._performance_metrics[name]
            metrics['execution_count'] += 1
            metrics['last_execution_time'] = int(time.time())
            
            # Set up thread for execution
            result_container = []
            error_container = []
            
            def execute_strategy():
                try:
                    start_time = time.time()
                    result = strategy.generate_signals(market_data)
                    duration = time.time() - start_time
                    
                    result_container.append((result, duration))
                except Exception as e:
                    error_container.append(str(e))
            
            # Start execution thread
            thread = threading.Thread(target=execute_strategy)
            thread.daemon = True
            thread.start()
            
            # Wait for thread to complete with timeout
            timeout = self.config['execution_timeout']
            thread.join(timeout)
            
            # Check results
            if result_container:
                # Thread completed successfully
                result, duration = result_container[0]
                
                # Update metrics
                metrics['success_count'] += 1
                metrics['last_execution_duration'] = duration
                metrics['last_execution_status'] = "success"
                metrics['avg_execution_time'] = (metrics['avg_execution_time'] * (metrics['success_count'] - 1) + duration) / metrics['success_count']
                metrics['success_rate'] = metrics['success_count'] / metrics['execution_count']
                
                # Process signals
                self._process_strategy_signals(name, symbol, result)
                
                # Check for poor performance for fallback
                if self.config['enable_fallback'] and duration > timeout * 0.8:
                    logger.warning(f"Strategy {name} execution time ({duration:.2f}s) approaching timeout, consider fallback")
                    if metrics['avg_execution_time'] > timeout * 0.7:
                        self.set_fallback_status(name, True)
                
            elif error_container:
                # Thread completed with error
                error = error_container[0]
                
                # Update metrics
                metrics['error_count'] += 1
                metrics['last_execution_status'] = "error"
                metrics['success_rate'] = metrics['success_count'] / metrics['execution_count']
                
                logger.error(f"Error executing strategy {name}: {error}")
                
                # Check for fallback
                if self.config['enable_fallback'] and metrics['error_count'] > 3:
                    logger.warning(f"Strategy {name} had multiple errors, entering fallback mode")
                    self.set_fallback_status(name, True)
                
            else:
                # Thread timed out
                # Update metrics
                metrics['timeout_count'] += 1
                metrics['last_execution_status'] = "timeout"
                metrics['success_rate'] = metrics['success_count'] / metrics['execution_count']
                
                logger.warning(f"Strategy {name} execution timed out after {timeout} seconds")
                
                # Always fallback on timeout
                if self.config['enable_fallback']:
                    logger.warning(f"Strategy {name} execution timed out, entering fallback mode")
                    self.set_fallback_status(name, True)
        
        except Exception as e:
            logger.error(f"Error in strategy execution wrapper for {name}: {str(e)}", exc_info=True)
    
    def _process_strategy_signals(self, strategy_name: str, symbol: str, result: StrategyResult) -> None:
        """
        Process signals from a strategy execution.
        
        Args:
            strategy_name: Strategy name
            symbol: Trading symbol (used to filter signals)
            result: Strategy execution result
        """
        try:
            # Skip if no signals or bad result
            if not result or result.status != "success" or not result.signals:
                return
            
            # Filter signals for this symbol
            symbol_signals = [signal for signal in result.signals if signal.symbol == symbol]
            
            if not symbol_signals:
                return
            
            # Add signals to recent signals
            with self._lock:
                # Initialize if needed
                if symbol not in self._recent_signals:
                    self._recent_signals[symbol] = {}
                
                if strategy_name not in self._recent_signals[symbol]:
                    self._recent_signals[symbol][strategy_name] = []
                
                # Add new signals
                self._recent_signals[symbol][strategy_name].extend(symbol_signals)
                
                # Remove old signals
                max_age = self.config['max_signal_age']
                current_time = int(time.time() * 1000)
                
                self._recent_signals[symbol][strategy_name] = [
                    signal for signal in self._recent_signals[symbol][strategy_name]
                    if current_time - signal.timestamp < max_age * 1000
                ]
        
        except Exception as e:
            logger.error(f"Error processing signals from {strategy_name}: {str(e)}", exc_info=True)
    
    def _aggregate_signals_for_symbol(self, symbol: str) -> Optional[StrategySignal]:
        """
        Aggregate signals from multiple strategies for a symbol.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Aggregated signal or None
        """
        try:
            with self._lock:
                if symbol not in self._recent_signals:
                    return None
                
                # Collect all recent signals for this symbol
                all_signals = []
                for strategy_name, signals in self._recent_signals[symbol].items():
                    all_signals.extend(signals)
                
                if not all_signals:
                    return None
                
                # Use appropriate aggregation method
                method = self.config['signal_aggregation_method']
                
                if method == 'weighted':
                    aggregated = self._aggregate_signals_weighted(symbol, all_signals)
                elif method == 'majority':
                    aggregated = self._aggregate_signals_majority(symbol, all_signals)
                elif method == 'priority':
                    aggregated = self._aggregate_signals_priority(symbol, all_signals)
                else:
                    logger.error(f"Unknown aggregation method: {method}")
                    return None
                
                if aggregated:
                    # Store aggregated signal
                    self._aggregated_signals[symbol] = aggregated
                    
                    # Publish signal
                    self._publish_signal(aggregated)
                
                return aggregated
        
        except Exception as e:
            logger.error(f"Error aggregating signals for {symbol}: {str(e)}", exc_info=True)
            return None
    
    def _aggregate_signals_weighted(self, symbol: str, signals: List[StrategySignal]) -> Optional[StrategySignal]:
        """
        Aggregate signals using weighted average.
        
        Args:
            symbol: Trading symbol
            signals: List of signals to aggregate
            
        Returns:
            Aggregated signal or None
        """
        if not signals:
            return None
        
        # Count signals by type
        signal_scores = {
            'buy': 0.0,
            'sell': 0.0,
            'hold': 0.0,
            'strong_buy': 0.0,
            'strong_sell': 0.0,
            'exit': 0.0
        }
        
        total_weight = 0.0
        latest_timestamp = 0
        latest_price = 0.0
        
        # Calculate weighted scores
        for signal in signals:
            strategy_name = signal.metadata.get('strategy', '')
            weight = self._strategy_weights.get(strategy_name, 1.0)
            
            # Convert signal type to score
            signal_type = signal.signal_type.value if hasattr(signal.signal_type, 'value') else signal.signal_type
            signal_scores[signal_type] += weight * signal.confidence
            
            total_weight += weight
            
            # Track latest signal for timestamp and price
            if signal.timestamp > latest_timestamp:
                latest_timestamp = signal.timestamp
                latest_price = signal.price
        
        if total_weight == 0:
            return None
        
        # Normalize scores
        for key in signal_scores:
            signal_scores[key] /= total_weight
        
        # Calculate buy/sell scores
        buy_score = signal_scores['buy'] + 1.5 * signal_scores['strong_buy']
        sell_score = signal_scores['sell'] + 1.5 * signal_scores['strong_sell']
        hold_score = signal_scores['hold']
        exit_score = signal_scores['exit']
        
        # Determine signal type and confidence
        if exit_score > 0.5:
            signal_type = 'exit'
            confidence = exit_score
        elif buy_score > sell_score and buy_score > hold_score:
            if buy_score > 0.8:
                signal_type = 'strong_buy'
                confidence = buy_score
            else:
                signal_type = 'buy'
                confidence = buy_score
        elif sell_score > buy_score and sell_score > hold_score:
            if sell_score > 0.8:
                signal_type = 'strong_sell'
                confidence = sell_score
            else:
                signal_type = 'sell'
                confidence = sell_score
        else:
            signal_type = 'hold'
            confidence = max(hold_score, 0.5)
        
        # Create aggregated signal
        from trading_system.strategy.base import SignalType, StrategySignal
        
        aggregated = StrategySignal(
            symbol=symbol,
            signal_type=SignalType(signal_type),
            price=latest_price,
            timestamp=latest_timestamp,
            confidence=confidence,
            metadata={
                'aggregation_method': 'weighted',
                'source_count': len(signals),
                'scores': signal_scores
            }
        )
        
        return aggregated
    
    def _aggregate_signals_majority(self, symbol: str, signals: List[StrategySignal]) -> Optional[StrategySignal]:
        """
        Aggregate signals using majority vote.
        
        Args:
            symbol: Trading symbol
            signals: List of signals to aggregate
            
        Returns:
            Aggregated signal or None
        """
        if not signals:
            return None
        
        # Count signals by type
        signal_counts = {
            'buy': 0,
            'sell': 0,
            'hold': 0,
            'strong_buy': 0,
            'strong_sell': 0,
            'exit': 0
        }
        
        latest_timestamp = 0
        latest_price = 0.0
        total_confidence = 0.0
        
        # Count signals
        for signal in signals:
            signal_type = signal.signal_type.value if hasattr(signal.signal_type, 'value') else signal.signal_type
            signal_counts[signal_type] += 1
            total_confidence += signal.confidence
            
            # Track latest signal for timestamp and price
            if signal.timestamp > latest_timestamp:
                latest_timestamp = signal.timestamp
                latest_price = signal.price
        
        # Combine strong and regular signals
        buy_count = signal_counts['buy'] + signal_counts['strong_buy']
        sell_count = signal_counts['sell'] + signal_counts['strong_sell']
        hold_count = signal_counts['hold']
        exit_count = signal_counts['exit']
        
        # Determine winner
        max_count = max(buy_count, sell_count, hold_count, exit_count)
        
        if max_count == 0:
            return None
        
        # Determine signal type and confidence
        if exit_count == max_count:
            signal_type = 'exit'
            confidence = exit_count / len(signals)
        elif buy_count == max_count:
            if signal_counts['strong_buy'] > signal_counts['buy']:
                signal_type = 'strong_buy'
            else:
                signal_type = 'buy'
            confidence = buy_count / len(signals)
        elif sell_count == max_count:
            if signal_counts['strong_sell'] > signal_counts['sell']:
                signal_type = 'strong_sell'
            else:
                signal_type = 'sell'
            confidence = sell_count / len(signals)
        else:
            signal_type = 'hold'
            confidence = hold_count / len(signals)
        
        # Create aggregated signal
        from trading_system.strategy.base import SignalType, StrategySignal
        
        aggregated = StrategySignal(
            symbol=symbol,
            signal_type=SignalType(signal_type),
            price=latest_price,
            timestamp=latest_timestamp,
            confidence=confidence,
            metadata={
                'aggregation_method': 'majority',
                'source_count': len(signals),
                'vote_counts': signal_counts,
                'average_confidence': total_confidence / len(signals)
            }
        )
        
        return aggregated
    
    def _aggregate_signals_priority(self, symbol: str, signals: List[StrategySignal]) -> Optional[StrategySignal]:
        """
        Aggregate signals using priority (highest priority strategy wins).
        
        Args:
            symbol: Trading symbol
            signals: List of signals to aggregate
            
        Returns:
            Aggregated signal or None
        """
        if not signals:
            return None
        
        # Group signals by strategy
        strategy_signals = {}
        for signal in signals:
            strategy_name = signal.metadata.get('strategy', '')
            if strategy_name not in strategy_signals:
                strategy_signals[strategy_name] = []
            strategy_signals[strategy_name].append(signal)
        
        # Sort strategies by priority
        sorted_strategies = sorted(
            strategy_signals.keys(),
            key=lambda s: self._strategy_priorities.get(s, 999)
        )
        
        if not sorted_strategies:
            return None
        
        # Get highest priority strategy that has signals
        highest_priority = sorted_strategies[0]
        highest_signals = strategy_signals[highest_priority]
        
        # Get latest signal from highest priority strategy
        latest_signal = max(highest_signals, key=lambda s: s.timestamp)
        
        # Add aggregation metadata
        latest_signal.metadata['aggregation_method'] = 'priority'
        latest_signal.metadata['source_count'] = len(signals)
        latest_signal.metadata['priority_strategy'] = highest_priority
        
        return latest_signal
    
    def _publish_signal(self, signal: StrategySignal) -> None:
        """
        Publish an aggregated signal to the message bus.
        
        Args:
            signal: Signal to publish
        """
        try:
            # Create message payload
            payload = {
                'signal': signal.to_dict(),
                'timestamp': int(time.time() * 1000),
                'source': 'strategy_manager'
            }
            
            # Publish to message bus
            self.message_bus.publish_async({
                'type': MessageTypes.STRATEGY_SIGNAL.value,
                'source': 'strategy_manager',
                'payload': payload
            })
            
            logger.info(f"Published aggregated {signal.signal_type.value} signal for {signal.symbol} with confidence {signal.confidence:.2f}")
            
        except Exception as e:
            logger.error(f"Error publishing signal: {str(e)}", exc_info=True)
    
    def get_recent_signals(self, symbol: str = None, strategy: str = None) -> Union[Dict[str, Dict[str, List[StrategySignal]]], List[StrategySignal]]:
        """
        Get recent signals.
        
        Args:
            symbol: Trading symbol (optional)
            strategy: Strategy name (optional)
            
        Returns:
            Dictionary of signals or list of signals if both symbol and strategy are specified
        """
        with self._lock:
            if symbol and strategy:
                if symbol in self._recent_signals and strategy in self._recent_signals[symbol]:
                    return list(self._recent_signals[symbol][strategy])
                return []
            elif symbol:
                if symbol in self._recent_signals:
                    return {k: list(v) for k, v in self._recent_signals[symbol].items()}
                return {}
            elif strategy:
                result = {}
                for sym, strats in self._recent_signals.items():
                    if strategy in strats:
                        result[sym] = list(strats[strategy])
                return result
            else:
                result = {}
                for sym, strats in self._recent_signals.items():
                    result[sym] = {k: list(v) for k, v in strats.items()}
                return result
    
    def get_aggregated_signals(self, symbol: str = None) -> Union[Dict[str, StrategySignal], Optional[StrategySignal]]:
        """
        Get aggregated signals.
        
        Args:
            symbol: Trading symbol (optional)
            
        Returns:
            Dictionary of aggregated signals or single signal if symbol is specified
        """
        with self._lock:
            if symbol:
                return self._aggregated_signals.get(symbol)
            else:
                return dict(self._aggregated_signals)
    
    def _polling_loop(self) -> None:
        """
        Background polling loop for strategy execution.
        
        This loop periodically checks for new market data and triggers strategy execution.
        """
        logger.info("Strategy manager polling loop started")
        
        try:
            while not self._stop_event.is_set():
                try:
                    # Check for fallback expirations
                    self._check_fallback_expirations()
                    
                    # Sleep for polling interval
                    self._stop_event.wait(self.config['polling_interval'])
                    
                except Exception as e:
                    logger.error(f"Error in strategy manager polling loop: {str(e)}", exc_info=True)
                    time.sleep(1)  # Avoid tight loop on error
        
        except Exception as e:
            logger.error(f"Fatal error in strategy manager polling loop: {str(e)}", exc_info=True)
        
        logger.info("Strategy manager polling loop stopped")
    
    def _check_fallback_expirations(self) -> None:
        """Check for fallback expirations and reset status."""
        current_time = int(time.time())
        
        with self._lock:
            for name in list(self._fallback_status.keys()):
                if self._fallback_status[name] and self._fallback_until[name] < current_time:
                    self._fallback_status[name] = False
                    logger.info(f"Strategy {name} fallback period expired")
    
    def _handle_market_data_update(self, message: Dict[str, Any]) -> None:
        """
        Handle market data update message from message bus.
        
        Args:
            message: Market data update message
        """
        try:
            payload = message.payload
            
            symbol = payload.get('symbol')
            timeframe = payload.get('timeframe')
            data = payload.get('data')
            
            if not symbol or not timeframe or not isinstance(data, pd.DataFrame):
                logger.warning("Invalid market data update message")
                return
            
            # Update market data
            self.update_market_data(symbol, timeframe, data)
            
        except Exception as e:
            logger.error(f"Error handling market data update: {str(e)}", exc_info=True)
    
    def start(self) -> bool:
        """
        Start the strategy manager.
        
        Returns:
            Success status
        """
        try:
            if self.running:
                logger.warning("Strategy manager already running")
                return True
            
            # Reset stop event
            self._stop_event.clear()
            
            # Start polling thread
            self._polling_thread = threading.Thread(target=self._polling_loop)
            self._polling_thread.daemon = True
            self._polling_thread.start()
            
            # Subscribe to market data updates
            self.message_bus.subscribe(MessageTypes.MARKET_DATA_UPDATE, self._handle_market_data_update)
            
            self.running = True
            self.status = ComponentStatus.OPERATIONAL
            
            logger.info("Strategy manager started")
            return True
            
        except Exception as e:
            logger.error(f"Error starting strategy manager: {str(e)}", exc_info=True)
            self.status = ComponentStatus.ERROR
            return False
    
    def stop(self) -> bool:
        """
        Stop the strategy manager.
        
        Returns:
            Success status
        """
        try:
            if not self.running:
                logger.warning("Strategy manager not running")
                return True
            
            # Signal polling thread to stop
            self._stop_event.set()
            
            # Wait for polling thread to stop
            if self._polling_thread and self._polling_thread.is_alive():
                self._polling_thread.join(timeout=5.0)
            
            self.running = False
            self.status = ComponentStatus.STOPPED
            
            logger.info("Strategy manager stopped")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping strategy manager: {str(e)}", exc_info=True)
            self.status = ComponentStatus.ERROR
            return False
    
    async def initialize(self) -> bool:
        """
        Initialize the strategy manager.
        
        Returns:
            Success status
        """
        try:
            # Initialize from configurations
            strategies = strategy_factory.create_strategies_from_configurations()
            
            for name, strategy in strategies.items():
                # Use default priorities based on name order
                self.register_strategy(strategy, priority=len(self._strategies))
            
            # Start manager
            success = self.start()
            
            if success:
                self.status = ComponentStatus.OPERATIONAL
            else:
                self.status = ComponentStatus.ERROR
            
            return success
            
        except Exception as e:
            logger.error(f"Error initializing strategy manager: {str(e)}", exc_info=True)
            self.status = ComponentStatus.ERROR
            return False

# Create strategy manager singleton
strategy_manager = StrategyManager() 