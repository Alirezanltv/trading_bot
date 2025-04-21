"""
Strategy Engine Module

This module provides the core strategy engine for managing multiple trading
strategies, strategy execution, signal aggregation, and performance monitoring.
"""

import asyncio
import logging
import os
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union, Set, Tuple, Callable
from concurrent.futures import ThreadPoolExecutor

from trading_system.core.component import Component, ComponentStatus
from trading_system.core.logging import get_logger
from trading_system.core.message_bus import MessageBus
from trading_system.market_data.data_facade import MarketDataFacade
from trading_system.strategy.base import BaseStrategy, SignalType

logger = get_logger("strategy.engine")


class StrategyEngine(Component):
    """
    Strategy Engine manages multiple trading strategies.
    
    Features:
    - Strategy registration, activation, and deactivation
    - Signal aggregation from multiple strategies
    - Performance monitoring and degradation handling
    - Real-time strategy execution
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the strategy engine.
        
        Args:
            config: Engine configuration
        """
        super().__init__(name="StrategyEngine")
        
        # Default configuration
        self.config = {
            "signal_aggregation_method": "weighted",  # weighted, majority, priority
            "signal_cache_size": 1000,                # Number of signals to cache
            "performance_check_interval": 3600,       # Check performance every hour
            "strategy_save_interval": 300,            # Save strategy state every 5 minutes
            "degradation_threshold": 0.3,             # Performance below this triggers degradation
            "degradation_cooldown": 86400,            # Cooldown period after degradation (24h)
            "default_symbols": ["BTC/USDT"],          # Default symbols to monitor
            "default_timeframes": ["1h", "4h", "1d"], # Default timeframes
            "max_strategy_timeout": 10,               # Maximum time for strategy execution in seconds
            "thread_pool_size": 4                     # Size of thread pool for strategy execution
        }
        
        # Update with provided config
        if config:
            self.config.update(config)
        
        # Registered strategies
        self.strategies: Dict[str, BaseStrategy] = {}
        self.active_strategies: Set[str] = set()
        
        # Strategy metadata
        self.strategy_priorities: Dict[str, int] = {}  # Lower is higher priority
        self.strategy_weights: Dict[str, float] = {}   # For weighted aggregation
        
        # Performance tracking
        self.strategy_performance: Dict[str, Dict[str, Any]] = {}
        self.degraded_strategies: Dict[str, datetime] = {}  # Strategy -> degradation time
        
        # Signal tracking by symbol and timeframe
        self.signal_cache: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}  # symbol_tf -> strategy -> signals
        
        # Market data
        self.market_data: Dict[str, Dict[str, Any]] = {}  # symbol_tf -> latest market data
        
        # Message bus for communication
        self.message_bus: Optional[MessageBus] = None
        
        # Market data facade for data access
        self.market_data_facade: Optional[MarketDataFacade] = None
        
        # Thread pool for background tasks
        self.thread_pool = ThreadPoolExecutor(max_workers=self.config["thread_pool_size"])
        
        # Background tasks
        self.background_tasks: List[asyncio.Task] = []
        
        # Running status
        self.running = False
        self.shutdown_event = asyncio.Event()
    
    async def initialize(self) -> bool:
        """
        Initialize the strategy engine.
        
        Returns:
            Success flag
        """
        try:
            logger.info("Initializing strategy engine")
            
            # Initialize message bus connection
            self.message_bus = MessageBus()
            
            # Initialize market data facade
            if self.market_data_facade is None:
                from trading_system.market_data.data_facade import get_market_data_facade
                self.market_data_facade = get_market_data_facade()
            
            # Register message handlers
            await self._register_message_handlers()
            
            # Initialize signal cache
            for symbol in self.config["default_symbols"]:
                for timeframe in self.config["default_timeframes"]:
                    symbol_tf = f"{symbol}_{timeframe}"
                    if symbol_tf not in self.signal_cache:
                        self.signal_cache[symbol_tf] = {}
            
            self.status = ComponentStatus.OPERATIONAL
            logger.info("Strategy engine initialized successfully")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize strategy engine: {e}")
            self.status = ComponentStatus.ERROR
            return False
    
    async def _register_message_handlers(self) -> None:
        """Register message handlers for market data and commands."""
        if not self.message_bus:
            return
        
        # Register market data handler
        await self.message_bus.subscribe("market_data", self._handle_market_data)
        
        # Register command handler
        await self.message_bus.subscribe("strategy_command", self._handle_command)
    
    async def _handle_market_data(self, data: Dict[str, Any]) -> None:
        """
        Handle incoming market data.
        
        Args:
            data: Market data message
        """
        if not self.running:
            return
        
        try:
            # Extract symbol and timeframe
            symbol = data.get("symbol")
            timeframe = data.get("timeframe")
            
            if not symbol or not timeframe:
                return
            
            # Create symbol_timeframe key
            symbol_tf = f"{symbol}_{timeframe}"
            
            # Store market data
            self.market_data[symbol_tf] = data
            
            # Update active strategies with new data
            for strategy_id in self.active_strategies:
                strategy = self.strategies.get(strategy_id)
                if not strategy:
                    continue
                
                # Check if strategy is interested in this symbol/timeframe
                if (not strategy.symbols or symbol in strategy.symbols) and \
                   (not strategy.timeframes or timeframe in strategy.timeframes):
                    # Schedule strategy update
                    asyncio.create_task(self._update_strategy(strategy, data))
            
        except Exception as e:
            logger.error(f"Error handling market data: {e}")
    
    async def _update_strategy(self, strategy: BaseStrategy, market_data: Dict[str, Any]) -> None:
        """
        Update a strategy with new market data.
        
        Args:
            strategy: Strategy to update
            market_data: Market data to update with
        """
        try:
            start_time = time.time()
            
            # Update strategy with new data
            await strategy.update(market_data)
            
            # Get signal if update completed without error
            symbol = market_data.get("symbol")
            timeframe = market_data.get("timeframe")
            
            if symbol and timeframe:
                # Get signal from strategy
                signal = await strategy.get_signal(symbol, timeframe)
                
                # If we have a signal, store it and notify listeners
                if signal:
                    # Store in signal cache
                    symbol_tf = f"{symbol}_{timeframe}"
                    if symbol_tf not in self.signal_cache:
                        self.signal_cache[symbol_tf] = {}
                    
                    if strategy.strategy_id not in self.signal_cache[symbol_tf]:
                        self.signal_cache[symbol_tf][strategy.strategy_id] = []
                    
                    # Add signal to cache
                    self.signal_cache[symbol_tf][strategy.strategy_id].append(signal)
                    
                    # Limit cache size
                    cache_size = self.config["signal_cache_size"]
                    if len(self.signal_cache[symbol_tf][strategy.strategy_id]) > cache_size:
                        self.signal_cache[symbol_tf][strategy.strategy_id] = \
                            self.signal_cache[symbol_tf][strategy.strategy_id][-cache_size:]
                    
                    # Notify via message bus
                    if self.message_bus:
                        await self.message_bus.publish("strategy_signal", signal)
            
            # Update execution time in performance metrics
            execution_time = time.time() - start_time
            self._update_strategy_performance(strategy.strategy_id, execution_time, True)
            
        except Exception as e:
            logger.error(f"Error updating strategy {strategy.name}: {e}")
            self._update_strategy_performance(strategy.strategy_id, 0, False)
    
    def _update_strategy_performance(self, strategy_id: str, execution_time: float, success: bool) -> None:
        """
        Update strategy performance metrics.
        
        Args:
            strategy_id: Strategy ID
            execution_time: Execution time in seconds
            success: Whether the execution was successful
        """
        if strategy_id not in self.strategy_performance:
            self.strategy_performance[strategy_id] = {
                "execution_count": 0,
                "success_count": 0,
                "error_count": 0,
                "avg_execution_time": 0,
                "last_execution_time": 0,
                "success_rate": 1.0
            }
        
        perf = self.strategy_performance[strategy_id]
        perf["execution_count"] += 1
        perf["last_execution_time"] = execution_time
        
        if success:
            perf["success_count"] += 1
        else:
            perf["error_count"] += 1
        
        # Update average execution time
        perf["avg_execution_time"] = (
            (perf["avg_execution_time"] * (perf["execution_count"] - 1)) + execution_time
        ) / perf["execution_count"]
        
        # Update success rate
        perf["success_rate"] = perf["success_count"] / perf["execution_count"]
        
        # Check if strategy needs to be degraded
        self._check_strategy_degradation(strategy_id)
    
    def _check_strategy_degradation(self, strategy_id: str) -> None:
        """
        Check if a strategy needs to be degraded based on performance.
        
        Args:
            strategy_id: Strategy ID to check
        """
        if strategy_id not in self.strategy_performance:
            return
        
        perf = self.strategy_performance[strategy_id]
        degradation_threshold = self.config["degradation_threshold"]
        
        # Check if we need to degrade (success rate below threshold)
        if perf["success_rate"] < degradation_threshold and strategy_id in self.active_strategies:
            # Deactivate strategy
            self.active_strategies.remove(strategy_id)
            
            # Record degradation time
            self.degraded_strategies[strategy_id] = datetime.now()
            
            # Log degradation
            logger.warning(
                f"Strategy {strategy_id} degraded due to poor performance: "
                f"success rate {perf['success_rate']:.2f} below threshold {degradation_threshold}"
            )
    
    def _check_degradation_restoration(self) -> None:
        """Check if degraded strategies can be restored."""
        now = datetime.now()
        cooldown = timedelta(seconds=self.config["degradation_cooldown"])
        
        strategies_to_restore = []
        
        for strategy_id, degraded_time in self.degraded_strategies.items():
            if (now - degraded_time) > cooldown:
                # Cooldown period over, restore strategy
                strategies_to_restore.append(strategy_id)
        
        # Restore strategies
        for strategy_id in strategies_to_restore:
            # Reactivate if strategy still exists
            if strategy_id in self.strategies:
                self.active_strategies.add(strategy_id)
                logger.info(f"Restored strategy {strategy_id} after degradation cooldown")
            
            # Remove from degraded list
            self.degraded_strategies.pop(strategy_id)
    
    async def _handle_command(self, command: Dict[str, Any]) -> None:
        """
        Handle strategy command messages.
        
        Args:
            command: Command message
        """
        try:
            cmd_type = command.get("type")
            if not cmd_type:
                return
            
            if cmd_type == "register_strategy":
                # Register a new strategy
                strategy_id = command.get("strategy_id")
                strategy_class = command.get("strategy_class")
                config = command.get("config", {})
                
                if strategy_id and strategy_class:
                    self.register_strategy_by_class(strategy_id, strategy_class, config)
            
            elif cmd_type == "unregister_strategy":
                # Unregister a strategy
                strategy_id = command.get("strategy_id")
                if strategy_id:
                    self.unregister_strategy(strategy_id)
            
            elif cmd_type == "activate_strategy":
                # Activate a strategy
                strategy_id = command.get("strategy_id")
                if strategy_id:
                    self.activate_strategy(strategy_id)
            
            elif cmd_type == "deactivate_strategy":
                # Deactivate a strategy
                strategy_id = command.get("strategy_id")
                if strategy_id:
                    self.deactivate_strategy(strategy_id)
            
            elif cmd_type == "get_signals":
                # Get signals for a symbol and timeframe
                symbol = command.get("symbol")
                timeframe = command.get("timeframe")
                strategy_id = command.get("strategy_id")
                callback_topic = command.get("callback_topic")
                
                if symbol and timeframe and callback_topic:
                    signals = self.get_signals(symbol, timeframe, strategy_id)
                    # Send signals back on callback topic
                    await self.message_bus.publish(callback_topic, {
                        "signals": signals,
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "strategy_id": strategy_id
                    })
            
            elif cmd_type == "get_performance":
                # Get strategy performance
                strategy_id = command.get("strategy_id")
                callback_topic = command.get("callback_topic")
                
                if callback_topic:
                    if strategy_id:
                        # Get performance for specific strategy
                        performance = self.get_strategy_performance(strategy_id)
                        await self.message_bus.publish(callback_topic, {
                            "performance": performance,
                            "strategy_id": strategy_id
                        })
                    else:
                        # Get performance for all strategies
                        performance = self.get_all_performance()
                        await self.message_bus.publish(callback_topic, {
                            "performance": performance
                        })
                        
        except Exception as e:
            logger.error(f"Error handling strategy command: {e}")
    
    def register_strategy(self, strategy: BaseStrategy, priority: int = 0, weight: float = 1.0) -> None:
        """
        Register a strategy with the engine.
        
        Args:
            strategy: Strategy instance
            priority: Priority for signal aggregation (lower is higher priority)
            weight: Weight for weighted signal aggregation
        """
        try:
            # Initialize strategy if not already initialized
            if strategy.status == ComponentStatus.CREATED:
                asyncio.create_task(strategy.initialize())
            
            # Register strategy
            self.strategies[strategy.strategy_id] = strategy
            self.strategy_priorities[strategy.strategy_id] = priority
            self.strategy_weights[strategy.strategy_id] = weight
            
            # Activate by default
            self.active_strategies.add(strategy.strategy_id)
            
            logger.info(
                f"Registered strategy {strategy.name} (ID: {strategy.strategy_id}) "
                f"with priority {priority} and weight {weight}"
            )
            
        except Exception as e:
            logger.error(f"Error registering strategy: {e}")
    
    def register_strategy_by_class(self, strategy_id: str, strategy_class: str, config: Dict[str, Any],
                                  priority: int = 0, weight: float = 1.0) -> Optional[BaseStrategy]:
        """
        Register a strategy by class name.
        
        Args:
            strategy_id: Strategy ID
            strategy_class: Fully qualified class name
            config: Strategy configuration
            priority: Priority for signal aggregation
            weight: Weight for weighted signal aggregation
            
        Returns:
            Registered strategy instance or None if failed
        """
        try:
            # Ensure config has strategy ID
            if "id" not in config:
                config["id"] = strategy_id
            
            # Import and create strategy instance
            parts = strategy_class.split(".")
            module_name = ".".join(parts[:-1])
            class_name = parts[-1]
            
            module = __import__(module_name, fromlist=[class_name])
            strategy_cls = getattr(module, class_name)
            
            # Create strategy instance
            strategy = strategy_cls(config)
            
            # Register strategy
            self.register_strategy(strategy, priority, weight)
            
            return strategy
            
        except Exception as e:
            logger.error(f"Error registering strategy by class: {e}")
            return None
    
    def unregister_strategy(self, strategy_id: str) -> bool:
        """
        Unregister a strategy from the engine.
        
        Args:
            strategy_id: Strategy ID
            
        Returns:
            Success flag
        """
        try:
            if strategy_id not in self.strategies:
                return False
            
            # Get strategy
            strategy = self.strategies[strategy_id]
            
            # Deactivate first
            if strategy_id in self.active_strategies:
                self.active_strategies.remove(strategy_id)
            
            # Shutdown strategy
            asyncio.create_task(strategy.shutdown())
            
            # Remove from collections
            self.strategies.pop(strategy_id)
            self.strategy_priorities.pop(strategy_id, None)
            self.strategy_weights.pop(strategy_id, None)
            
            # Clean up signal cache
            for symbol_tf in self.signal_cache:
                if strategy_id in self.signal_cache[symbol_tf]:
                    self.signal_cache[symbol_tf].pop(strategy_id)
            
            logger.info(f"Unregistered strategy {strategy.name} (ID: {strategy_id})")
            return True
            
        except Exception as e:
            logger.error(f"Error unregistering strategy: {e}")
            return False
    
    def activate_strategy(self, strategy_id: str) -> bool:
        """
        Activate a strategy.
        
        Args:
            strategy_id: Strategy ID
            
        Returns:
            Success flag
        """
        if strategy_id in self.strategies:
            self.active_strategies.add(strategy_id)
            logger.info(f"Activated strategy {strategy_id}")
            return True
        return False
    
    def deactivate_strategy(self, strategy_id: str) -> bool:
        """
        Deactivate a strategy.
        
        Args:
            strategy_id: Strategy ID
            
        Returns:
            Success flag
        """
        if strategy_id in self.active_strategies:
            self.active_strategies.remove(strategy_id)
            logger.info(f"Deactivated strategy {strategy_id}")
            return True
        return False
    
    def get_signals(self, symbol: str, timeframe: str, strategy_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get signals for a symbol and timeframe.
        
        Args:
            symbol: Trading symbol
            timeframe: Chart timeframe
            strategy_id: Optional strategy ID to filter by
            
        Returns:
            List of signals
        """
        symbol_tf = f"{symbol}_{timeframe}"
        
        if symbol_tf not in self.signal_cache:
            return []
        
        if strategy_id:
            # Get signals for specific strategy
            return self.signal_cache[symbol_tf].get(strategy_id, [])
        else:
            # Aggregate signals from all strategies
            return self._aggregate_signals(symbol_tf)
    
    def _aggregate_signals(self, symbol_tf: str) -> List[Dict[str, Any]]:
        """
        Aggregate signals from all active strategies for a symbol and timeframe.
        
        Args:
            symbol_tf: Symbol and timeframe key
            
        Returns:
            List of aggregated signals
        """
        if symbol_tf not in self.signal_cache:
            return []
        
        all_signals = []
        
        # Collect signals from active strategies
        for strategy_id in self.active_strategies:
            if strategy_id in self.signal_cache[symbol_tf]:
                signals = self.signal_cache[symbol_tf][strategy_id]
                if signals:
                    all_signals.extend(signals)
        
        # Sort by timestamp (newest first)
        all_signals.sort(key=lambda s: s.get("timestamp", 0), reverse=True)
        
        return all_signals[:self.config["signal_cache_size"]]
    
    def get_strategy_performance(self, strategy_id: str) -> Dict[str, Any]:
        """
        Get performance metrics for a strategy.
        
        Args:
            strategy_id: Strategy ID
            
        Returns:
            Performance metrics
        """
        # Get engine metrics
        engine_metrics = self.strategy_performance.get(strategy_id, {})
        
        # Get strategy metrics
        strategy = self.strategies.get(strategy_id)
        if strategy:
            strategy_metrics = strategy.get_performance()
        else:
            strategy_metrics = {}
        
        # Merge metrics
        metrics = {**engine_metrics, **strategy_metrics}
        
        # Add degradation status
        metrics["degraded"] = strategy_id in self.degraded_strategies
        if metrics["degraded"]:
            metrics["degraded_since"] = self.degraded_strategies[strategy_id].isoformat()
        
        return metrics
    
    def get_all_performance(self) -> Dict[str, Dict[str, Any]]:
        """
        Get performance metrics for all strategies.
        
        Returns:
            Dictionary of strategy ID -> performance metrics
        """
        performance = {}
        
        for strategy_id in self.strategies:
            performance[strategy_id] = self.get_strategy_performance(strategy_id)
        
        return performance
    
    async def start(self) -> bool:
        """
        Start the strategy engine.
        
        Returns:
            Success flag
        """
        try:
            logger.info("Starting strategy engine")
            
            # Check if already running
            if self.running:
                logger.warning("Strategy engine already running")
                return True
            
            # Initialize if not initialized
            if self.status == ComponentStatus.CREATED:
                await self.initialize()
            
            # Reset shutdown event
            self.shutdown_event.clear()
            
            # Start background tasks
            self._start_background_tasks()
            
            # Set running flag
            self.running = True
            self.status = ComponentStatus.OPERATIONAL
            
            logger.info("Strategy engine started successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start strategy engine: {e}")
            self.status = ComponentStatus.ERROR
            return False
    
    def _start_background_tasks(self) -> None:
        """Start background tasks for performance monitoring and state saving."""
        # Performance monitoring task
        perf_task = asyncio.create_task(self._performance_monitoring_task())
        self.background_tasks.append(perf_task)
        
        # Strategy save task
        save_task = asyncio.create_task(self._strategy_save_task())
        self.background_tasks.append(save_task)
    
    async def _performance_monitoring_task(self) -> None:
        """Background task for monitoring strategy performance."""
        interval = self.config["performance_check_interval"]
        
        while not self.shutdown_event.is_set():
            try:
                # Check if degraded strategies can be restored
                self._check_degradation_restoration()
                
                # Wait for next check
                try:
                    await asyncio.wait_for(self.shutdown_event.wait(), timeout=interval)
                except asyncio.TimeoutError:
                    # This is expected, just continue
                    pass
                
            except Exception as e:
                logger.error(f"Error in performance monitoring task: {e}")
                # Sleep a bit to avoid tight loop on error
                await asyncio.sleep(10)
    
    async def _strategy_save_task(self) -> None:
        """Background task for saving strategy state."""
        interval = self.config["strategy_save_interval"]
        
        while not self.shutdown_event.is_set():
            try:
                # Save state for all strategies
                for strategy in self.strategies.values():
                    if strategy.status == ComponentStatus.OPERATIONAL:
                        await strategy._save_state()
                
                # Wait for next save
                try:
                    await asyncio.wait_for(self.shutdown_event.wait(), timeout=interval)
                except asyncio.TimeoutError:
                    # This is expected, just continue
                    pass
                
            except Exception as e:
                logger.error(f"Error in strategy save task: {e}")
                # Sleep a bit to avoid tight loop on error
                await asyncio.sleep(10)
    
    async def stop(self) -> bool:
        """
        Stop the strategy engine.
        
        Returns:
            Success flag
        """
        try:
            logger.info("Stopping strategy engine")
            
            # Check if already stopped
            if not self.running:
                logger.warning("Strategy engine already stopped")
                return True
            
            # Set shutdown event to stop background tasks
            self.shutdown_event.set()
            
            # Wait for background tasks to complete
            if self.background_tasks:
                await asyncio.gather(*self.background_tasks, return_exceptions=True)
                self.background_tasks.clear()
            
            # Stop all strategies
            for strategy in self.strategies.values():
                await strategy.shutdown()
            
            # Clear running flag
            self.running = False
            self.status = ComponentStatus.STOPPED
            
            logger.info("Strategy engine stopped successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to stop strategy engine: {e}")
            self.status = ComponentStatus.ERROR
            return False
    
    async def shutdown(self) -> bool:
        """
        Shutdown the strategy engine.
        
        Returns:
            Success flag
        """
        try:
            logger.info("Shutting down strategy engine")
            
            # Stop engine if running
            if self.running:
                await self.stop()
            
            # Unregister message handlers
            if self.message_bus:
                await self.message_bus.unsubscribe("market_data", self._handle_market_data)
                await self.message_bus.unsubscribe("strategy_command", self._handle_command)
            
            # Shutdown thread pool
            self.thread_pool.shutdown()
            
            # Clear collections
            self.strategies.clear()
            self.active_strategies.clear()
            self.strategy_priorities.clear()
            self.strategy_weights.clear()
            self.signal_cache.clear()
            
            self.status = ComponentStatus.STOPPED
            logger.info("Strategy engine shutdown complete")
            return True
            
        except Exception as e:
            logger.error(f"Failed to shutdown strategy engine: {e}")
            self.status = ComponentStatus.ERROR
            return False


# Singleton instance
_instance = None


def get_strategy_engine(config: Dict[str, Any] = None) -> StrategyEngine:
    """
    Get or create the strategy engine instance.
    
    Args:
        config: Optional engine configuration
        
    Returns:
        Strategy engine instance
    """
    global _instance
    
    if _instance is None:
        _instance = StrategyEngine(config)
    
    return _instance 