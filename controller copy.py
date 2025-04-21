"""
Trading System Controller

This module implements the main controller that coordinates the strategy, execution,
and position management subsystems.
"""

import os
import asyncio
import time
from typing import Dict, List, Any, Optional, Set, Tuple
from datetime import datetime, timedelta
import traceback
import signal
import json

from core.logging import get_logger, initialize_logging
from core.component import Component, ComponentStatus
from strategy.base import Strategy, SignalType, SignalConfidence
from execution.engine import ExecutionEngine
from execution.orders import (
    Order, OrderStatus, OrderSide, OrderType, create_market_order, create_limit_order
)
from position.position_manager import PositionManager, Position, PositionType
from market_data.tradingview import TradingViewDataProvider


class TradingController(Component):
    """
    Main controller for the trading system.
    
    Coordinates all subsystems and handles the main trading loop.
    """
    
    def __init__(self, config_file: str):
        """
        Initialize trading controller.
        
        Args:
            config_file: Path to configuration file
        """
        super().__init__(component_id="trading_controller")
        
        # Initialize logging
        initialize_logging()
        self.logger = get_logger("controller")
        
        # Load configuration
        self.config_file = config_file
        self.config = self._load_config(config_file)
        
        # Subsystem instances
        self.strategies: Dict[str, Strategy] = {}
        self.execution_engine: Optional[ExecutionEngine] = None
        self.position_manager: Optional[PositionManager] = None
        self.market_data_provider: Optional[TradingViewDataProvider] = None
        
        # Signal handling
        self._shutdown_requested = False
        self._shutdown_complete = asyncio.Event()
        
        # Trading state
        self.last_signal_check = {}
        self.signal_history: Dict[str, List[Dict[str, Any]]] = {}
        self.market_data_cache: Dict[str, Dict[str, Any]] = {}
        
        # Trading parameters
        self.signal_check_interval = timedelta(seconds=self.config.get("signal_check_interval_seconds", 60))
        self.max_signal_history = self.config.get("max_signal_history", 100)
        self.symbols = self.config.get("symbols", [])
        
        # Runtime metrics
        self.start_time = datetime.now()
        self.process_count = 0
        self.last_process_time = None
        self.error_count = 0
        
        self.logger.info("Trading controller initialized")
    
    def _load_config(self, config_file: str) -> Dict[str, Any]:
        """
        Load configuration from file.
        
        Args:
            config_file: Path to configuration file
            
        Returns:
            Configuration dictionary
        """
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
            
            self.logger.info(f"Loaded configuration from {config_file}")
            return config
            
        except Exception as e:
            self.logger.error(f"Error loading configuration: {str(e)}")
            # Return default configuration
            return {
                "signal_check_interval_seconds": 60,
                "max_signal_history": 100,
                "symbols": ["btc-rls", "eth-rls"],
                "timeframe": "1h",
                "log_level": "info"
            }
    
    async def initialize(self) -> bool:
        """
        Initialize trading controller and all subsystems.
        
        Returns:
            Success flag
        """
        try:
            self.logger.info("Initializing trading controller")
            
            # Set component status
            self._update_status(ComponentStatus.INITIALIZING)
            
            # Initialize market data provider
            self.logger.info("Initializing market data provider")
            self.market_data_provider = TradingViewDataProvider(
                {
                    "credentials": self.config.get("tradingview_credentials", {}),
                    "symbols": self.symbols,
                    "timeframes": [self.config.get("timeframe", "1h")]
                }
            )
            
            if not await self.market_data_provider.initialize():
                self.logger.error("Failed to initialize market data provider")
                self._update_status(ComponentStatus.ERROR)
                return False
            
            # Initialize execution engine
            self.logger.info("Initializing execution engine")
            self.execution_engine = ExecutionEngine(
                self.config.get("execution_engine", {})
            )
            
            if not await self.execution_engine.initialize():
                self.logger.error("Failed to initialize execution engine")
                self._update_status(ComponentStatus.ERROR)
                return False
            
            # Initialize position manager
            self.logger.info("Initializing position manager")
            self.position_manager = PositionManager(
                self.config.get("position_manager", {})
            )
            
            if not await self.position_manager.initialize():
                self.logger.error("Failed to initialize position manager")
                self._update_status(ComponentStatus.ERROR)
                return False
            
            # Connect position manager to execution engine
            self.position_manager.set_execution_engine(self.execution_engine)
            
            # Initialize strategies
            await self._initialize_strategies()
            
            # Register signal handlers (for clean shutdown)
            self._register_signal_handlers()
            
            # Set status to operational
            self._update_status(ComponentStatus.OPERATIONAL)
            
            self.logger.info("Trading controller initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize trading controller: {str(e)}")
            traceback.print_exc()
            self._update_status(ComponentStatus.ERROR)
            return False
    
    async def _initialize_strategies(self) -> None:
        """Initialize trading strategies from configuration."""
        strategy_configs = self.config.get("strategies", [])
        
        if not strategy_configs:
            self.logger.warning("No strategies configured")
            return
        
        for strategy_config in strategy_configs:
            strategy_type = strategy_config.get("type")
            strategy_name = strategy_config.get("name")
            
            if not strategy_type:
                self.logger.warning(f"Missing strategy type in configuration: {strategy_config}")
                continue
            
            try:
                # Import the strategy class
                if strategy_type == "RSI":
                    from strategy.rsi_strategy import RSIStrategy
                    strategy = RSIStrategy(strategy_config, name=strategy_name)
                else:
                    self.logger.warning(f"Unknown strategy type: {strategy_type}")
                    continue
                
                # Initialize strategy
                if not await strategy.initialize():
                    self.logger.error(f"Failed to initialize strategy {strategy_name}")
                    continue
                
                # Store strategy
                self.strategies[strategy.component_id] = strategy
                
                self.logger.info(f"Initialized strategy {strategy.name} ({strategy.component_id})")
                
            except Exception as e:
                self.logger.error(f"Error initializing strategy {strategy_name}: {str(e)}")
                traceback.print_exc()
    
    async def run(self) -> None:
        """Run the main trading loop."""
        self.logger.info("Starting trading controller")
        
        # Reset shutdown flag
        self._shutdown_requested = False
        self._shutdown_complete.clear()
        
        try:
            while not self._shutdown_requested:
                try:
                    # Process trading logic
                    await self._process()
                    
                    # Increment process count
                    self.process_count += 1
                    self.last_process_time = datetime.now()
                    
                    # Check for shutdown request
                    if self._shutdown_requested:
                        break
                    
                    # Sleep until next cycle
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    self.logger.error(f"Error in trading loop: {str(e)}")
                    traceback.print_exc()
                    self.error_count += 1
                    
                    # Sleep before retry
                    await asyncio.sleep(5)
            
            # Perform cleanup
            await self._shutdown()
            
        except asyncio.CancelledError:
            self.logger.info("Trading controller task cancelled")
            await self._shutdown()
            
        except Exception as e:
            self.logger.error(f"Unhandled error in trading controller: {str(e)}")
            traceback.print_exc()
            await self._shutdown()
        
        finally:
            # Signal that shutdown is complete
            self._shutdown_complete.set()
    
    async def _process(self) -> None:
        """Process one cycle of the trading loop."""
        current_time = datetime.now()
        
        # Process market data updates
        await self._update_market_data()
        
        # Process pending orders
        await self.execution_engine.process_orders()
        
        # Process position updates
        await self.position_manager.process_updates()
        
        # Check for trading signals
        for symbol in self.symbols:
            # Skip if within signal check interval
            last_check = self.last_signal_check.get(symbol)
            if last_check and (current_time - last_check) < self.signal_check_interval:
                continue
            
            # Check signals for this symbol
            await self._check_signals(symbol)
            self.last_signal_check[symbol] = current_time
    
    async def _update_market_data(self) -> None:
        """Update market data for all symbols."""
        for symbol in self.symbols:
            try:
                # Get market data
                market_data = await self.market_data_provider.get_market_data(
                    symbol, self.config.get("timeframe", "1h")
                )
                
                if not market_data:
                    continue
                
                # Cache market data
                self.market_data_cache[symbol] = market_data
                
                # Update positions with current price
                if "last" in market_data:
                    current_price = market_data["last"]
                    self.position_manager.update_prices(symbol, current_price)
                
                # Update strategies with new market data
                for strategy in self.strategies.values():
                    await strategy.update(market_data)
                
            except Exception as e:
                self.logger.error(f"Error updating market data for {symbol}: {str(e)}")
    
    async def _check_signals(self, symbol: str) -> None:
        """
        Check for trading signals for a symbol.
        
        Args:
            symbol: Trading symbol
        """
        # Skip if no market data
        if symbol not in self.market_data_cache:
            return
        
        market_data = self.market_data_cache[symbol]
        
        # Check signals from all strategies
        signals = []
        
        for strategy in self.strategies.values():
            try:
                # Skip if symbol not in strategy's symbols
                if symbol not in strategy.symbols:
                    continue
                
                # Get signal
                signal = await strategy.get_signal(symbol, self.config.get("timeframe", "1h"))
                
                if signal:
                    # Add to signals list
                    signals.append(signal)
                    
                    # Add to signal history
                    if symbol not in self.signal_history:
                        self.signal_history[symbol] = []
                    
                    self.signal_history[symbol].append(signal)
                    
                    # Trim signal history
                    if len(self.signal_history[symbol]) > self.max_signal_history:
                        self.signal_history[symbol] = self.signal_history[symbol][-self.max_signal_history:]
                
            except Exception as e:
                self.logger.error(f"Error getting signal from {strategy.name} for {symbol}: {str(e)}")
        
        # Process actionable signals
        for signal in signals:
            signal_type = signal.get("type")
            confidence = signal.get("confidence", 0)
            
            # Skip neutral and low confidence signals
            if signal_type == SignalType.NEUTRAL.value:
                continue
            
            if confidence < SignalConfidence.MEDIUM.value:
                continue
            
            # Process actionable signal
            await self._process_signal(signal)
    
    async def _process_signal(self, signal: Dict[str, Any]) -> None:
        """
        Process a trading signal.
        
        Args:
            signal: Trading signal
        """
        signal_type = signal.get("type")
        symbol = signal.get("symbol")
        price = signal.get("price", 0.0)
        strategy = signal.get("strategy")
        confidence = signal.get("confidence", 0)
        
        self.logger.info(
            f"Processing signal: {symbol} {signal_type} from {strategy} "
            f"(confidence: {confidence}, price: {price})"
        )
        
        # Get current positions for this symbol
        positions = self.position_manager.get_positions_by_symbol(symbol, open_only=True)
        
        # Process based on signal type
        if signal_type == SignalType.BUY.value:
            # Skip if already have long position
            if any(p.position_type == PositionType.LONG for p in positions):
                self.logger.info(f"Already have long position for {symbol}, skipping buy signal")
                return
            
            # Create and submit buy order
            await self._create_buy_order(symbol, price, signal)
            
        elif signal_type == SignalType.SELL.value:
            # Close long positions or open short position
            if any(p.position_type == PositionType.LONG for p in positions):
                # Close long positions
                for position in positions:
                    if position.position_type == PositionType.LONG:
                        await self._close_position(position, price, signal)
            else:
                # Create short position
                await self._create_sell_order(symbol, price, signal)
            
        elif signal_type == SignalType.EXIT.value:
            # Close all positions
            for position in positions:
                await self._close_position(position, price, signal)
    
    async def _create_buy_order(self, symbol: str, price: float, signal: Dict[str, Any]) -> None:
        """
        Create and submit a buy order.
        
        Args:
            symbol: Trading symbol
            price: Signal price
            signal: Full signal data
        """
        try:
            # Calculate order quantity
            risk_amount = self.config.get("risk_per_trade", 0.02)  # 2% risk
            order_quantity = self._calculate_position_size(symbol, risk_amount)
            
            if order_quantity <= 0:
                self.logger.warning(f"Calculated zero quantity for {symbol}, skipping order")
                return
            
            # Create order metadata
            metadata = {
                "signal": signal,
                "strategy": signal.get("strategy"),
                "confidence": signal.get("confidence")
            }
            
            # Create market order (can also use limit orders here)
            order = create_market_order(
                symbol=symbol,
                side=OrderSide.BUY,
                quantity=order_quantity,
                metadata=metadata,
                strategy_id=signal.get("strategy_id")
            )
            
            # Submit order
            self.execution_engine.submit_order(order)
            
            self.logger.info(
                f"Submitted buy order for {symbol}: {order_quantity} @ market "
                f"(order ID: {order.order_id})"
            )
            
        except Exception as e:
            self.logger.error(f"Error creating buy order for {symbol}: {str(e)}")
            traceback.print_exc()
    
    async def _create_sell_order(self, symbol: str, price: float, signal: Dict[str, Any]) -> None:
        """
        Create and submit a sell order.
        
        Args:
            symbol: Trading symbol
            price: Signal price
            signal: Full signal data
        """
        try:
            # Calculate order quantity
            risk_amount = self.config.get("risk_per_trade", 0.02)  # 2% risk
            order_quantity = self._calculate_position_size(symbol, risk_amount)
            
            if order_quantity <= 0:
                self.logger.warning(f"Calculated zero quantity for {symbol}, skipping order")
                return
            
            # Create order metadata
            metadata = {
                "signal": signal,
                "strategy": signal.get("strategy"),
                "confidence": signal.get("confidence")
            }
            
            # Create market order (can also use limit orders here)
            order = create_market_order(
                symbol=symbol,
                side=OrderSide.SELL,
                quantity=order_quantity,
                metadata=metadata,
                strategy_id=signal.get("strategy_id")
            )
            
            # Submit order
            self.execution_engine.submit_order(order)
            
            self.logger.info(
                f"Submitted sell order for {symbol}: {order_quantity} @ market "
                f"(order ID: {order.order_id})"
            )
            
        except Exception as e:
            self.logger.error(f"Error creating sell order for {symbol}: {str(e)}")
            traceback.print_exc()
    
    async def _close_position(self, position: Position, price: float, signal: Dict[str, Any]) -> None:
        """
        Close a position.
        
        Args:
            position: Position to close
            price: Signal price
            signal: Full signal data
        """
        try:
            # Create order side (opposite of position type)
            side = OrderSide.SELL if position.position_type == PositionType.LONG else OrderSide.BUY
            
            # Create order metadata
            metadata = {
                "signal": signal,
                "strategy": signal.get("strategy"),
                "confidence": signal.get("confidence"),
                "position_id": position.position_id
            }
            
            # Create market order
            order = create_market_order(
                symbol=position.symbol,
                side=side,
                quantity=position.quantity,
                metadata=metadata,
                strategy_id=signal.get("strategy_id"),
                position_id=position.position_id
            )
            
            # Submit order
            self.execution_engine.submit_order(order)
            
            # Register callback to close position when order is filled
            self.execution_engine.register_order_callback(
                order.order_id, 
                lambda o: self._on_position_exit_order_update(o, position)
            )
            
            self.logger.info(
                f"Submitted order to close position {position.position_id}: {position.quantity} @ market "
                f"(order ID: {order.order_id})"
            )
            
        except Exception as e:
            self.logger.error(f"Error closing position {position.position_id}: {str(e)}")
            traceback.print_exc()
    
    def _on_position_exit_order_update(self, order: Order, position: Position) -> None:
        """
        Handle position exit order updates.
        
        Args:
            order: Updated order
            position: Position being closed
        """
        if order.status == OrderStatus.FILLED:
            # Close position with exit price
            asyncio.create_task(
                self._update_position_on_exit(position, order)
            )
    
    async def _update_position_on_exit(self, position: Position, order: Order) -> None:
        """
        Update position when exit order is filled.
        
        Args:
            position: Position being closed
            order: Filled exit order
        """
        try:
            # Close position
            self.position_manager.close_position(
                position_id=position.position_id,
                exit_price=order.average_fill_price,
                exit_quantity=order.filled_quantity,
                order_id=order.order_id
            )
            
            # Update strategy if available
            if position.strategy_id and position.strategy_id in self.strategies:
                strategy = self.strategies[position.strategy_id]
                
                # Record trade in strategy performance
                strategy.record_trade(
                    symbol=position.symbol,
                    entry_price=position.entry_price,
                    exit_price=order.average_fill_price,
                    quantity=position.quantity,
                    entry_time=position.open_time,
                    exit_time=datetime.now(),
                    pnl=position.realized_pnl,
                    metadata={
                        "position_id": position.position_id,
                        "entry_orders": position.entry_orders,
                        "exit_orders": position.exit_orders
                    }
                )
            
            self.logger.info(
                f"Closed position {position.position_id} with realized P&L: {position.realized_pnl:.2f}"
            )
            
        except Exception as e:
            self.logger.error(f"Error updating position {position.position_id} on exit: {str(e)}")
            traceback.print_exc()
    
    def _calculate_position_size(self, symbol: str, risk_percentage: float) -> float:
        """
        Calculate position size based on risk percentage.
        
        Args:
            symbol: Trading symbol
            risk_percentage: Risk percentage (0-1)
            
        Returns:
            Position size
        """
        # Get available balance (simplified - normally would check account)
        available_balance = 1000.0  # Example value, replace with actual balance
        
        # Calculate position size
        position_size = available_balance * risk_percentage
        
        # Get current price
        if symbol in self.market_data_cache and "last" in self.market_data_cache[symbol]:
            current_price = self.market_data_cache[symbol]["last"]
            
            # Convert currency amount to quantity
            quantity = position_size / current_price
            
            # Apply minimum and maximum constraints
            min_quantity = 0.001
            max_quantity = available_balance / current_price
            
            quantity = max(min_quantity, min(quantity, max_quantity))
            
            return quantity
        
        return 0.0
    
    def _register_signal_handlers(self) -> None:
        """Register signal handlers for graceful shutdown."""
        # Register signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, sig, frame) -> None:
        """
        Handle termination signals.
        
        Args:
            sig: Signal number
            frame: Current stack frame
        """
        self.logger.info(f"Received signal {sig}, initiating shutdown")
        self._shutdown_requested = True
    
    async def shutdown(self) -> None:
        """Initiate graceful shutdown."""
        self.logger.info("Initiating shutdown")
        self._shutdown_requested = True
        
        # Wait for shutdown to complete
        await self._shutdown_complete.wait()
    
    async def _shutdown(self) -> None:
        """Perform shutdown procedures."""
        self.logger.info("Performing shutdown")
        
        try:
            # Shutdown strategies
            for strategy in self.strategies.values():
                strategy.stop()
            
            # Shutdown execution engine
            if self.execution_engine:
                await self.execution_engine.shutdown()
            
            # Shutdown position manager
            if self.position_manager:
                await self.position_manager.shutdown()
            
            # Shutdown market data provider
            if self.market_data_provider:
                await self.market_data_provider.shutdown()
            
            self.logger.info("Trading controller shutdown complete")
            
        except Exception as e:
            self.logger.error(f"Error during shutdown: {str(e)}")
            traceback.print_exc()
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get controller status.
        
        Returns:
            Status information
        """
        # Calculate runtime
        runtime = datetime.now() - self.start_time
        runtime_str = str(runtime).split('.')[0]  # Format as HH:MM:SS
        
        return {
            "status": self._status.value,
            "start_time": self.start_time.isoformat(),
            "runtime": runtime_str,
            "process_count": self.process_count,
            "error_count": self.error_count,
            "last_process_time": self.last_process_time.isoformat() if self.last_process_time else None,
            "strategies": len(self.strategies),
            "symbols": self.symbols,
            "active_positions": len(self.position_manager.get_open_positions()) if self.position_manager else 0,
            "active_orders": len(self.execution_engine.get_active_orders()) if self.execution_engine else 0
        }


async def main():
    """Main entry point for the trading system."""
    # Default config file path
    config_file = "config/trading_config.json"
    
    # Override with environment variable if set
    if os.environ.get("TRADING_CONFIG"):
        config_file = os.environ.get("TRADING_CONFIG")
    
    # Create controller
    controller = TradingController(config_file)
    
    # Initialize controller
    success = await controller.initialize()
    
    if not success:
        print("Failed to initialize trading controller")
        return
    
    # Run until interrupted
    try:
        await controller.run()
    except KeyboardInterrupt:
        print("Keyboard interrupt received, shutting down")
        await controller.shutdown()


if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main()) 