"""
Integrated Trading Pipeline

This module connects all the components of the trading system into a working
end-to-end pipeline using mock market data for testing.
"""

import os
import sys
import time
import json
import uuid
import logging
import threading
import asyncio
from datetime import datetime
from typing import Dict, List, Any, Optional, Set, Tuple

# Import core components
from trading_system.core.logging import initialize_logging, get_logger
from trading_system.core.component import ComponentStatus

# Import subsystems
from trading_system.market_data.simulated_data import SimulatedDataProvider
from trading_system.strategy.rsi_strategy import RSIStrategy
from trading_system.strategy.base import SignalType, SignalConfidence
from trading_system.execution.engine import ExecutionEngine
from trading_system.execution.orders import Order, OrderStatus, OrderSide, OrderType, OrderTimeInForce
from trading_system.position.position_manager import PositionManager, Position, PositionType, PositionStatus

class IntegratedTradingPipeline:
    """
    Connects all trading system components into a working pipeline.
    
    This pipeline:
    1. Gets market data from a simulated provider
    2. Feeds data to a strategy
    3. Converts signals to orders
    4. Updates positions
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the integrated pipeline.
        
        Args:
            config_path: Path to configuration file (optional)
        """
        # Setup logging
        self.logger = get_logger("integrated_pipeline")
        
        # Load configuration
        self.config = self._load_config(config_path)
        
        # Initialize components
        self.market_data = SimulatedDataProvider(self.config.get("market_data", {}))
        self.strategy = RSIStrategy(self.config.get("strategy", {}))
        self.execution_engine = ExecutionEngine(self.config)
        self.position_manager = PositionManager(self.config)
        
        # Monitored symbols
        self.symbols = self.config.get("symbols", ["btc-usdt", "eth-usdt"])
        
        # Pipeline state
        self.running = False
        self.thread = None
        
        # Initialize component statuses
        self.component_statuses = {
            "market_data": False,
            "strategy": False,
            "execution": False,
            "position": False
        }
        
        self.logger.info("Integrated trading pipeline initialized")
    
    def _load_config(self, config_path: Optional[str]) -> Dict[str, Any]:
        """
        Load configuration from file or use defaults.
        
        Args:
            config_path: Path to configuration file
            
        Returns:
            Configuration dictionary
        """
        if not config_path:
            # Use default configuration
            return {
                "symbols": ["btc-usdt", "eth-usdt"],
                "market_data": {
                    "update_interval": 5,  # seconds
                },
                "strategy": {
                    "rsi_period": 14,
                    "overbought_threshold": 70,
                    "oversold_threshold": 30
                },
                "position_manager": {
                    "persistence_enabled": True,
                    "persistence_path": "data/positions"
                },
                "execution_engine": {
                    "persistence_enabled": True,
                    "persistence_path": "data/orders",
                    "circuit_breaker_enabled": True
                }
            }
        
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Error loading configuration: {str(e)}")
            return {}
    
    def start(self):
        """Start the integrated pipeline."""
        if self.running:
            self.logger.warning("Pipeline already running")
            return
        
        self.logger.info("Starting integrated trading pipeline")
        
        # Create necessary directories
        os.makedirs("data/positions", exist_ok=True)
        os.makedirs("data/orders", exist_ok=True)
        
        # Start position manager
        self.position_manager.start()
        self.component_statuses["position"] = True
        
        # Start execution engine
        self.execution_engine.start()
        self.component_statuses["execution"] = True
        
        # Start strategy
        self.strategy.start()
        self.component_statuses["strategy"] = True
        
        # Start pipeline processing thread
        self.running = True
        self.thread = threading.Thread(target=self._run_pipeline, daemon=True)
        self.thread.start()
        
        self.logger.info("Integrated trading pipeline started")
    
    def stop(self):
        """Stop the integrated pipeline."""
        if not self.running:
            return
        
        self.logger.info("Stopping integrated trading pipeline")
        
        # Stop processing
        self.running = False
        if self.thread:
            self.thread.join(timeout=5.0)
        
        # Stop components in reverse order
        if self.component_statuses["strategy"]:
            self.strategy.stop()
            self.component_statuses["strategy"] = False
        
        if self.component_statuses["execution"]:
            self.execution_engine.stop()
            self.component_statuses["execution"] = False
        
        if self.component_statuses["position"]:
            self.position_manager.stop()
            self.component_statuses["position"] = False
        
        self.logger.info("Integrated trading pipeline stopped")
    
    def _run_pipeline(self):
        """Main pipeline processing loop."""
        self.logger.info("Pipeline processing thread started")
        
        update_interval = self.config.get("market_data", {}).get("update_interval", 5)
        
        while self.running:
            try:
                # Process each monitored symbol
                for symbol in self.symbols:
                    self._process_symbol(symbol)
                
                # Process active orders and update positions
                self._update_positions()
                
                # Wait for next update
                time.sleep(update_interval)
                
            except Exception as e:
                self.logger.error(f"Error in pipeline processing: {str(e)}")
                time.sleep(1)  # Avoid tight loop on error
    
    def _process_symbol(self, symbol: str):
        """
        Process a single symbol through the pipeline.
        
        Args:
            symbol: Trading symbol
        """
        try:
            # 1. Get market data
            candle = self.market_data.generate_candles(symbol, timeframe="5m", count=1)[0]
            ticker = self.market_data.get_ticker(symbol)
            
            # 2. Update strategy with market data
            market_data = {
                "symbol": symbol,
                "candle": candle,
                "ticker": ticker,
                "timeframe": "5m"
            }
            
            # Create and run event loop for async methods
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # Update strategy with market data
            loop.run_until_complete(self.strategy._update_indicators(market_data))
            
            # Generate signal
            signal_type, confidence, metadata = loop.run_until_complete(
                self.strategy.generate_signal(symbol, "5m")
            )
            
            loop.close()
            
            # 4. Process signal if actionable
            if signal_type in [SignalType.BUY, SignalType.SELL, SignalType.EXIT]:
                self._process_signal(symbol, signal_type.value, confidence.value, ticker["last_price"], metadata)
                
        except Exception as e:
            self.logger.error(f"Error processing symbol {symbol}: {str(e)}")
    
    def _process_signal(self, symbol: str, signal_type: str, confidence: int, 
                       price: float, metadata: Dict[str, Any]):
        """
        Process a trading signal.
        
        Args:
            symbol: Trading symbol
            signal_type: Type of signal (BUY, SELL, EXIT)
            confidence: Signal confidence level
            price: Current price
            metadata: Additional signal information
        """
        self.logger.info(f"Processing signal: {symbol} {signal_type} (confidence: {confidence}, price: {price})")
        
        # Get active positions for this symbol
        positions = self.position_manager.get_positions_by_symbol(symbol)
        
        if signal_type == SignalType.BUY.value:
            # If we already have an open long position, don't open another
            if any(p.position_type == PositionType.LONG and p.status == PositionStatus.OPEN for p in positions):
                self.logger.info(f"Long position already exists for {symbol}, skipping")
                return
            
            # Calculate position size based on confidence
            quantity = self._calculate_position_size(symbol, price, confidence)
            
            # Create a new position
            position_id = str(uuid.uuid4())
            position = Position(
                position_id=position_id,
                symbol=symbol,
                position_type=PositionType.LONG,
                entry_price=price,
                quantity=quantity,
                strategy_id=self.strategy.name
            )
            
            # Create order for position
            order = Order(
                symbol=symbol,
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=quantity,
                time_in_force=OrderTimeInForce.GTC,
                position_id=position_id,
                strategy_id=self.strategy.name
            )
            
            # Submit order and store position
            self.execution_engine.submit_order(order)
            self.position_manager.store_position(position)
            
        elif signal_type == SignalType.SELL.value:
            # If we already have an open short position, don't open another
            if any(p.position_type == PositionType.SHORT and p.status == PositionStatus.OPEN for p in positions):
                self.logger.info(f"Short position already exists for {symbol}, skipping")
                return
            
            # Calculate position size based on confidence
            quantity = self._calculate_position_size(symbol, price, confidence)
            
            # Create a new position
            position_id = str(uuid.uuid4())
            position = Position(
                position_id=position_id,
                symbol=symbol,
                position_type=PositionType.SHORT,
                entry_price=price,
                quantity=quantity,
                strategy_id=self.strategy.name
            )
            
            # Create order for position
            order = Order(
                symbol=symbol,
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,
                quantity=quantity,
                time_in_force=OrderTimeInForce.GTC,
                position_id=position_id,
                strategy_id=self.strategy.name
            )
            
            # Submit order and store position
            self.execution_engine.submit_order(order)
            self.position_manager.store_position(position)
            
        elif signal_type == SignalType.EXIT.value:
            # Close all positions for this symbol
            for position in positions:
                if position.status == PositionStatus.OPEN:
                    self._close_position(position, price)
    
    def _close_position(self, position: Position, price: float):
        """
        Close a position.
        
        Args:
            position: Position to close
            price: Exit price
        """
        # Determine order side based on position type
        side = OrderSide.SELL if position.position_type == PositionType.LONG else OrderSide.BUY
        
        # Create exit order
        order = Order(
            symbol=position.symbol,
            side=side,
            order_type=OrderType.MARKET,
            quantity=position.remaining_quantity,
            time_in_force=OrderTimeInForce.GTC,
            position_id=position.position_id,
            strategy_id=position.strategy_id
        )
        
        # Submit order
        self.execution_engine.submit_order(order)
        self.logger.info(f"Closing position {position.position_id} at {price}")
    
    def _calculate_position_size(self, symbol: str, price: float, confidence: int) -> float:
        """
        Calculate appropriate position size based on risk parameters.
        
        Args:
            symbol: Trading symbol
            price: Current price
            confidence: Signal confidence level
            
        Returns:
            Position quantity
        """
        # For simplicity, use fixed sizes for now
        base_size = 0.01  # Base position size (BTC)
        
        # Adjust based on confidence
        multiplier = 1.0
        if confidence >= 3:  # High confidence
            multiplier = 1.5
        elif confidence <= 1:  # Low confidence
            multiplier = 0.5
            
        # Different symbols may have different position sizes
        if "eth" in symbol.lower():
            base_size = 0.1  # Higher quantity for ETH
            
        return round(base_size * multiplier, 8)
    
    def _update_positions(self):
        """Update positions based on order executions."""
        # Get filled orders that need processing
        for order_id, order in self.execution_engine.orders.items():
            if order.status == OrderStatus.FILLED and order.position_id:
                position = self.position_manager.get_position(order.position_id)
                
                if position:
                    # Process the order update
                    self.position_manager.process_order(order)


def main():
    """Run the integrated trading pipeline."""
    # Initialize logging
    logs_dir = os.path.join(os.getcwd(), "logs")
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    initialize_logging(log_level=logging.INFO)
    
    # Create and start pipeline
    pipeline = IntegratedTradingPipeline()
    
    try:
        pipeline.start()
        
        # Run for a specified time or until keyboard interrupt
        print("Press Ctrl+C to stop the trading pipeline")
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nStopping trading pipeline...")
    finally:
        pipeline.stop()


if __name__ == "__main__":
    main() 