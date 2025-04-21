"""
Trading System Controller.

This module implements the main controller for orchestrating all subsystems
and handling the main trading loop. It coordinates interactions between
the strategy, execution engine, position manager, and market data providers.
"""

import json
import os
import signal
import sys
import time
import threading
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, List, Any, Optional, Set, Tuple, Type

from trading_system.core.component import Component, ComponentStatus
from trading_system.core.logging import get_logger
from trading_system.execution.engine import ExecutionEngine
from trading_system.execution.orders import Order, OrderStatus, OrderSide, OrderType
from trading_system.position.position_manager import PositionManager, PositionType
from trading_system.strategy.base import Strategy, SignalType, SignalConfidence


class TradingController:
    """
    Main trading system controller.
    
    Coordinates all subsystems and handles the main trading loop.
    This is the central component that manages the flow of information
    and actions between different parts of the system.
    """
    
    def __init__(self, config_path: str):
        """
        Initialize trading controller.
        
        Args:
            config_path: Path to configuration file
        """
        # Initialize logger
        self.logger = get_logger("controller")
        
        # Load configuration
        self.config = self._load_config(config_path)
        
        # Trading state
        self.running = False
        self.shutdown_requested = False
        self.main_thread = None
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
        
        # Initialize subsystems
        self.execution_engine = None
        self.position_manager = None
        self.strategies = {}
        self.market_data_provider = None
        
        # Runtime metrics
        self.metrics = {
            "signals_processed": 0,
            "orders_placed": 0,
            "positions_opened": 0,
            "positions_closed": 0,
            "cycles_completed": 0,
            "errors_encountered": 0,
            "last_cycle_time": 0,
            "avg_cycle_time": 0,
            "start_time": None,
            "uptime_seconds": 0
        }
        
        self.logger.info("Trading controller initialized")
    
    def start(self) -> None:
        """Start the trading controller and all subsystems."""
        if self.running:
            self.logger.warning("Trading controller already running")
            return
        
        self.logger.info("Starting trading controller")
        
        try:
            # Initialize subsystems
            self._initialize_subsystems()
            
            # Start execution engine
            if self.execution_engine:
                self.execution_engine.start()
            
            # Start position manager
            if self.position_manager:
                self.position_manager.start()
            
            # Initialize strategies
            self._initialize_strategies()
            
            # Start market data provider
            if self.market_data_provider:
                self.market_data_provider.start()
            
            # Start main loop
            self.running = True
            self.shutdown_requested = False
            self.metrics["start_time"] = time.time()
            self.main_thread = threading.Thread(target=self._main_loop, daemon=True)
            self.main_thread.start()
            
            self.logger.info("Trading controller started")
            
        except Exception as e:
            self.logger.exception(f"Error starting trading controller: {str(e)}")
            self.stop()
    
    def stop(self) -> None:
        """Stop the trading controller and all subsystems."""
        if not self.running:
            return
        
        self.logger.info("Stopping trading controller")
        
        # Signal shutdown
        self.shutdown_requested = True
        
        # Wait for main thread to complete
        if self.main_thread and self.main_thread.is_alive():
            self.main_thread.join(timeout=30.0)
            if self.main_thread.is_alive():
                self.logger.warning("Main thread did not terminate gracefully")
        
        # Stop market data provider
        if self.market_data_provider:
            try:
                self.market_data_provider.stop()
            except Exception as e:
                self.logger.error(f"Error stopping market data provider: {str(e)}")
        
        # Stop strategies
        for strategy in self.strategies.values():
            try:
                strategy.stop()
            except Exception as e:
                self.logger.error(f"Error stopping strategy {strategy.name}: {str(e)}")
        
        # Stop position manager
        if self.position_manager:
            try:
                self.position_manager.stop()
            except Exception as e:
                self.logger.error(f"Error stopping position manager: {str(e)}")
        
        # Stop execution engine
        if self.execution_engine:
            try:
                self.execution_engine.stop()
            except Exception as e:
                self.logger.error(f"Error stopping execution engine: {str(e)}")
        
        self.running = False
        self.logger.info("Trading controller stopped")
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """
        Load configuration from file.
        
        Args:
            config_path: Path to configuration file
            
        Returns:
            Configuration dictionary
        """
        self.logger.info(f"Loading configuration from {config_path}")
        
        try:
            with open(config_path, "r") as f:
                config = json.load(f)
            
            self.logger.info("Configuration loaded successfully")
            return config
            
        except Exception as e:
            self.logger.exception(f"Error loading configuration: {str(e)}")
            return {}
    
    def _initialize_subsystems(self) -> None:
        """Initialize trading subsystems."""
        self.logger.info("Initializing subsystems")
        
        # Initialize execution engine
        self.logger.info("Initializing execution engine")
        self.execution_engine = ExecutionEngine(self.config)
        
        # Initialize position manager
        self.logger.info("Initializing position manager")
        self.position_manager = PositionManager(self.config)
        
        # Initialize market data provider
        self.logger.info("Initializing market data provider")
        
        # Import the appropriate market data provider
        market_data_type = self.config.get("market_data", {}).get("provider", "tradingview")
        
        if market_data_type == "tradingview":
            from trading_system.market_data.tradingview import TradingViewProvider
            self.market_data_provider = TradingViewProvider(self.config)
        else:
            self.logger.error(f"Unsupported market data provider: {market_data_type}")
    
    def _initialize_strategies(self) -> None:
        """Initialize trading strategies from configuration."""
        self.logger.info("Initializing strategies")
        
        # Get strategy configurations
        strategy_configs = self.config.get("strategies", [])
        
        for strategy_config in strategy_configs:
            try:
                # Get strategy type
                strategy_type = strategy_config.get("type")
                if not strategy_type:
                    self.logger.error("Missing strategy type in configuration")
                    continue
                
                # Import the strategy class
                strategy_class = self._get_strategy_class(strategy_type)
                if not strategy_class:
                    self.logger.error(f"Unknown strategy type: {strategy_type}")
                    continue
                
                # Create strategy instance
                strategy_id = strategy_config.get("id", str(hash(f"{strategy_type}_{time.time()}")))
                strategy = strategy_class(strategy_config, strategy_id)
                
                # Initialize and store strategy
                strategy.start()
                self.strategies[strategy_id] = strategy
                
                self.logger.info(f"Strategy {strategy_id} ({strategy_type}) initialized")
                
            except Exception as e:
                self.logger.exception(f"Error initializing strategy: {str(e)}")
    
    def _get_strategy_class(self, strategy_type: str) -> Optional[Type[Strategy]]:
        """
        Get strategy class by type.
        
        Args:
            strategy_type: Strategy type name
            
        Returns:
            Strategy class or None if not found
        """
        try:
            if strategy_type == "rsi":
                from trading_system.strategy.rsi_strategy import RSIStrategy
                return RSIStrategy
            # Add more strategy types here
            
            return None
            
        except Exception as e:
            self.logger.exception(f"Error importing strategy {strategy_type}: {str(e)}")
            return None
    
    def _main_loop(self) -> None:
        """Main trading loop."""
        self.logger.info("Starting main trading loop")
        
        cycle_times = []
        
        while not self.shutdown_requested:
            try:
                start_time = time.time()
                
                # Process one cycle
                self._process()
                
                # Update metrics
                cycle_time = time.time() - start_time
                self.metrics["last_cycle_time"] = cycle_time
                self.metrics["cycles_completed"] += 1
                self.metrics["uptime_seconds"] = time.time() - self.metrics["start_time"]
                
                # Store cycle times for average calculation
                cycle_times.append(cycle_time)
                if len(cycle_times) > 100:
                    cycle_times = cycle_times[-100:]
                self.metrics["avg_cycle_time"] = sum(cycle_times) / len(cycle_times)
                
                # Sleep to maintain cycle rate
                target_cycle_time = self.config.get("controller", {}).get("cycle_time_seconds", 1.0)
                sleep_time = max(0.1, target_cycle_time - cycle_time)
                time.sleep(sleep_time)
                
            except Exception as e:
                self.logger.exception(f"Error in main loop: {str(e)}")
                self.metrics["errors_encountered"] += 1
                time.sleep(1.0)  # Sleep briefly to avoid tight error loops
        
        self.logger.info("Main trading loop exited")
    
    def _process(self) -> None:
        """Process one cycle of the trading loop."""
        # Update market data
        self._update_market_data()
        
        # Check trading signals
        self._check_signals()
        
        # Process pending orders
        self._process_orders()
        
        # Update positions
        self._update_positions()
        
        # Perform periodic tasks
        self._periodic_tasks()
    
    def _update_market_data(self) -> None:
        """Update market data for all trading symbols."""
        if not self.market_data_provider:
            return
        
        # Get the list of symbols to monitor
        symbols = self._get_monitored_symbols()
        
        # Update market data for each symbol
        for symbol in symbols:
            try:
                self.market_data_provider.update(symbol)
            except Exception as e:
                self.logger.error(f"Error updating market data for {symbol}: {str(e)}")
    
    def _get_monitored_symbols(self) -> Set[str]:
        """
        Get the set of symbols to monitor.
        
        Returns:
            Set of symbols
        """
        # Start with symbols from configuration
        symbols = set(self.config.get("symbols", []))
        
        # Add symbols from strategies
        for strategy in self.strategies.values():
            if hasattr(strategy, "symbols"):
                symbols.update(strategy.symbols)
        
        # Add symbols from open positions
        if self.position_manager:
            for position in self.position_manager.get_open_positions():
                symbols.add(position.symbol)
        
        return symbols
    
    def _check_signals(self) -> None:
        """Check trading signals from all strategies."""
        # Get the list of symbols to check
        symbols = self._get_monitored_symbols()
        
        # Check signals for each symbol
        for symbol in symbols:
            try:
                self._check_symbol_signals(symbol)
            except Exception as e:
                self.logger.error(f"Error checking signals for {symbol}: {str(e)}")
    
    def _check_symbol_signals(self, symbol: str) -> None:
        """
        Check trading signals for a specific symbol.
        
        Args:
            symbol: Trading symbol
        """
        # Get market data
        if not self.market_data_provider:
            return
        
        market_data = self.market_data_provider.get_data(symbol)
        if not market_data:
            return
        
        # Get current price
        current_price = market_data.get("last", 0.0)
        if current_price <= 0:
            return
        
        # Check signals from each strategy
        for strategy_id, strategy in self.strategies.items():
            try:
                # Skip if the strategy doesn't handle this symbol
                if hasattr(strategy, "symbols") and symbol not in strategy.symbols:
                    continue
                
                # Get trading timeframe
                timeframe = strategy.config.get("timeframe", "1h")
                
                # Generate signal
                signal_type, confidence, metadata = strategy.generate_signal(symbol, timeframe)
                
                # Process actionable signals
                if signal_type != SignalType.NEUTRAL and confidence > 0.0:
                    self._process_signal(strategy_id, symbol, signal_type, confidence, current_price, metadata)
                
            except Exception as e:
                self.logger.error(f"Error generating signal for {symbol} using strategy {strategy_id}: {str(e)}")
    
    def _process_signal(self, strategy_id: str, symbol: str, signal_type: SignalType, 
                      confidence: float, current_price: float, metadata: Optional[Dict[str, Any]]) -> None:
        """
        Process an actionable trading signal.
        
        Args:
            strategy_id: Strategy ID
            symbol: Trading symbol
            signal_type: Signal type
            confidence: Signal confidence
            current_price: Current price
            metadata: Signal metadata
        """
        self.logger.info(
            f"Signal received: {symbol} {signal_type.name} with {confidence:.2f} confidence "
            f"at {current_price} from strategy {strategy_id}"
        )
        
        self.metrics["signals_processed"] += 1
        
        # Check existing positions
        existing_positions = self.position_manager.get_open_positions(symbol) if self.position_manager else []
        
        # Process based on signal type
        if signal_type == SignalType.BUY:
            # Check if we already have a long position
            has_long = any(p.position_type == PositionType.LONG for p in existing_positions)
            
            if not has_long:
                # Create new long position
                self._open_position(strategy_id, symbol, PositionType.LONG, current_price, confidence, metadata)
            
        elif signal_type == SignalType.SELL:
            # Check if we already have a short position
            has_short = any(p.position_type == PositionType.SHORT for p in existing_positions)
            
            if not has_short:
                # Create new short position
                self._open_position(strategy_id, symbol, PositionType.SHORT, current_price, confidence, metadata)
            
        elif signal_type == SignalType.EXIT:
            # Close all positions for this symbol
            for position in existing_positions:
                self._close_position(position.position_id, current_price, metadata)
    
    def _open_position(self, strategy_id: str, symbol: str, position_type: PositionType, 
                     price: float, confidence: float, metadata: Optional[Dict[str, Any]]) -> None:
        """
        Open a new position.
        
        Args:
            strategy_id: Strategy ID
            symbol: Trading symbol
            position_type: Position type
            price: Entry price
            confidence: Signal confidence
            metadata: Signal metadata
        """
        if not self.position_manager or not self.execution_engine:
            self.logger.error("Cannot open position: position manager or execution engine not initialized")
            return
        
        try:
            # Calculate quantity
            quantity = self._calculate_position_size(symbol, price, confidence)
            if quantity <= 0:
                self.logger.warning(f"Calculated zero position size for {symbol}")
                return
            
            # Create order
            order_side = OrderSide.BUY if position_type == PositionType.LONG else OrderSide.SELL
            order = Order(
                symbol=symbol,
                side=order_side,
                order_type=OrderType.MARKET,
                quantity=quantity,
                strategy_id=strategy_id
            )
            
            # Add metadata
            order.metadata["signal_confidence"] = confidence
            if metadata:
                order.metadata["signal_metadata"] = metadata
            
            # Submit order
            order_id = self.execution_engine.submit_order(order)
            
            # Create position
            position_id = self.position_manager.create_position(
                symbol=symbol,
                position_type=position_type,
                entry_price=price,
                quantity=quantity,
                strategy_id=strategy_id,
                entry_order=order,
                metadata={
                    "order_id": order_id,
                    "signal_confidence": confidence,
                    "signal_metadata": metadata or {}
                }
            )
            
            self.logger.info(
                f"Opened {position_type.value} position {position_id} for {symbol}: "
                f"{quantity} @ {price}"
            )
            
            self.metrics["positions_opened"] += 1
            self.metrics["orders_placed"] += 1
            
        except Exception as e:
            self.logger.exception(f"Error opening position for {symbol}: {str(e)}")
    
    def _close_position(self, position_id: str, price: float, metadata: Optional[Dict[str, Any]]) -> None:
        """
        Close an existing position.
        
        Args:
            position_id: Position ID
            price: Exit price
            metadata: Signal metadata
        """
        if not self.position_manager or not self.execution_engine:
            self.logger.error("Cannot close position: position manager or execution engine not initialized")
            return
        
        try:
            # Get position
            position = self.position_manager.get_position(position_id)
            if not position:
                self.logger.warning(f"Position {position_id} not found")
                return
            
            # Create order
            order_side = OrderSide.SELL if position.position_type == PositionType.LONG else OrderSide.BUY
            order = Order(
                symbol=position.symbol,
                side=order_side,
                order_type=OrderType.MARKET,
                quantity=position.remaining_quantity,
                position_id=position_id,
                strategy_id=position.strategy_id
            )
            
            # Add metadata
            if metadata:
                order.metadata["signal_metadata"] = metadata
            
            # Submit order
            order_id = self.execution_engine.submit_order(order)
            
            # Close position
            success = self.position_manager.close_position(
                position_id=position_id,
                exit_price=price,
                exit_order=order
            )
            
            if success:
                self.logger.info(
                    f"Closed {position.position_type.value} position {position_id} for {position.symbol}: "
                    f"{position.remaining_quantity} @ {price}"
                )
                
                self.metrics["positions_closed"] += 1
                self.metrics["orders_placed"] += 1
            
        except Exception as e:
            self.logger.exception(f"Error closing position {position_id}: {str(e)}")
    
    def _calculate_position_size(self, symbol: str, price: float, confidence: float) -> float:
        """
        Calculate position size.
        
        Args:
            symbol: Trading symbol
            price: Current price
            confidence: Signal confidence
            
        Returns:
            Position size
        """
        # Get risk settings
        risk_settings = self.config.get("risk_management", {})
        
        # Get portfolio value (account balance)
        portfolio_value = self._get_portfolio_value()
        
        # Calculate risk amount
        risk_per_trade_pct = risk_settings.get("risk_per_trade_percent", 1.0)
        
        # Adjust risk based on confidence
        adjusted_risk_pct = risk_per_trade_pct * confidence
        
        # Calculate risk amount
        risk_amount = portfolio_value * (adjusted_risk_pct / 100.0)
        
        # Get position sizing method
        sizing_method = risk_settings.get("position_sizing", "fixed_value")
        
        if sizing_method == "fixed_value":
            # Fixed value sizing
            max_position_value = risk_settings.get("max_position_value", 1000.0)
            position_value = min(risk_amount, max_position_value)
            
            # Calculate quantity
            if price > 0:
                quantity = position_value / price
            else:
                quantity = 0.0
                
        elif sizing_method == "fixed_quantity":
            # Fixed quantity sizing
            quantity = float(risk_settings.get("fixed_quantity", 0.1))
            
        else:
            # Default to fixed value
            position_value = risk_amount
            if price > 0:
                quantity = position_value / price
            else:
                quantity = 0.0
        
        # Apply minimum and maximum constraints
        min_quantity = self._get_min_quantity(symbol)
        max_quantity = self._get_max_quantity(symbol, portfolio_value)
        
        quantity = max(min_quantity, min(quantity, max_quantity))
        
        # Round to appropriate precision
        precision = self._get_quantity_precision(symbol)
        quantity = round(quantity, precision)
        
        return quantity
    
    def _get_portfolio_value(self) -> float:
        """
        Get total portfolio value.
        
        Returns:
            Portfolio value
        """
        # This is a simplification - in a real system, this would query the exchange
        return float(self.config.get("account", {}).get("balance", 10000.0))
    
    def _get_min_quantity(self, symbol: str) -> float:
        """
        Get minimum order quantity for a symbol.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Minimum quantity
        """
        # Get from config or use default
        symbol_settings = self._get_symbol_settings(symbol)
        return float(symbol_settings.get("min_quantity", 0.001))
    
    def _get_max_quantity(self, symbol: str, portfolio_value: float) -> float:
        """
        Get maximum order quantity for a symbol.
        
        Args:
            symbol: Trading symbol
            portfolio_value: Total portfolio value
            
        Returns:
            Maximum quantity
        """
        # Get from config or derive from portfolio value
        symbol_settings = self._get_symbol_settings(symbol)
        
        # Use max percentage if configured
        max_pct = float(symbol_settings.get("max_position_percent", 25.0))
        derived_max = portfolio_value * (max_pct / 100.0)
        
        # Convert to quantity
        current_price = self._get_current_price(symbol)
        if current_price <= 0:
            return 0.0
        
        derived_max_qty = derived_max / current_price
        
        # Check against hard limit if configured
        hard_max = float(symbol_settings.get("max_quantity", float("inf")))
        
        return min(derived_max_qty, hard_max)
    
    def _get_quantity_precision(self, symbol: str) -> int:
        """
        Get quantity precision for a symbol.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Decimal precision
        """
        # Get from config or use default
        symbol_settings = self._get_symbol_settings(symbol)
        return int(symbol_settings.get("quantity_precision", 6))
    
    def _get_symbol_settings(self, symbol: str) -> Dict[str, Any]:
        """
        Get settings for a specific symbol.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Symbol settings
        """
        # Get from config or use default
        symbol_settings = self.config.get("symbols_settings", {}).get(symbol, {})
        return symbol_settings
    
    def _get_current_price(self, symbol: str) -> float:
        """
        Get current price for a symbol.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Current price
        """
        if not self.market_data_provider:
            return 0.0
        
        market_data = self.market_data_provider.get_data(symbol)
        if not market_data:
            return 0.0
        
        return market_data.get("last", 0.0)
    
    def _process_orders(self) -> None:
        """Process orders and update positions."""
        # Nothing to do if execution engine or position manager not initialized
        if not self.execution_engine or not self.position_manager:
            return
        
        # Get active orders
        active_orders = self.execution_engine.get_active_orders()
        
        # Process each order
        for order in active_orders:
            try:
                # Skip if order hasn't changed
                if order.status in (OrderStatus.CREATED, OrderStatus.VALIDATING, OrderStatus.READY, OrderStatus.SUBMITTING):
                    continue
                
                # Update position with order
                self.position_manager.process_order(order)
                
            except Exception as e:
                self.logger.error(f"Error processing order {order.order_id}: {str(e)}")
    
    def _update_positions(self) -> None:
        """Update positions with current market data."""
        # Nothing to do if position manager not initialized
        if not self.position_manager or not self.market_data_provider:
            return
        
        # Get open positions
        open_positions = self.position_manager.get_open_positions()
        
        # Update each position
        for position in open_positions:
            try:
                # Get current price
                current_price = self._get_current_price(position.symbol)
                if current_price <= 0:
                    continue
                
                # Update position
                self.position_manager.update_position(position.position_id, current_price)
                
            except Exception as e:
                self.logger.error(f"Error updating position {position.position_id}: {str(e)}")
    
    def _periodic_tasks(self) -> None:
        """Perform periodic tasks."""
        # Determine if it's time to perform each task
        current_time = time.time()
        
        # Reconcile exchange positions
        if (self.position_manager and 
            current_time % self.config.get("controller", {}).get("reconciliation_interval_seconds", 300) < 1.0):
            self._reconcile_exchange_positions()
    
    def _reconcile_exchange_positions(self) -> None:
        """Reconcile internal positions with exchange positions."""
        if not self.position_manager:
            return
        
        try:
            # In a real system, this would query the exchange for current positions
            exchange_positions = self._get_exchange_positions()
            
            # Reconcile positions
            self.position_manager.reconcile_exchange_positions(exchange_positions)
            
        except Exception as e:
            self.logger.error(f"Error reconciling exchange positions: {str(e)}")
    
    def _get_exchange_positions(self) -> List[Dict[str, Any]]:
        """
        Get positions from exchange.
        
        Returns:
            List of positions from exchange
        """
        # This is a placeholder - in a real system, this would query the exchange
        return []
    
    def _handle_signal(self, signum: int, frame) -> None:
        """
        Handle process signal.
        
        Args:
            signum: Signal number
            frame: Stack frame
        """
        self.logger.info(f"Received signal {signum}, shutting down")
        self.shutdown_requested = True


def main(config_path: Optional[str] = None) -> None:
    """
    Main entry point.
    
    Args:
        config_path: Path to configuration file
    """
    # Get config path
    if not config_path:
        # Use default config path
        config_path = "config/trading_config.json"
    
    # Create controller
    controller = TradingController(config_path)
    
    try:
        # Start controller
        controller.start()
        
        # Wait for shutdown
        print("Trading system started. Press Ctrl+C to exit.")
        while controller.running:
            time.sleep(1.0)
            
    except KeyboardInterrupt:
        print("\nShutting down...")
    
    finally:
        # Ensure clean shutdown
        controller.stop()


if __name__ == "__main__":
    # Get config path from command line args
    config_path = sys.argv[1] if len(sys.argv) > 1 else None
    
    # Run main function
    main(config_path) 