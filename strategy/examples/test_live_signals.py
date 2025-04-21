#!/usr/bin/env python
"""
Real-time Signal Tester Script

This script tests the real-time signal generation capabilities of trading strategies
by connecting them to the Market Data Facade and monitoring signals generated in
a live environment.
"""

import asyncio
import argparse
import os
import sys
import signal
import time
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Set

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

# Import and apply patches to fix compatibility issues
from trading_system.strategy.examples.enum_fix import *
from trading_system.strategy.examples.message_bus_patch import *
from trading_system.strategy.examples.provider_fix import *
from trading_system.strategy.examples.engine_fix import *

from trading_system.market_data.market_data_facade import MarketDataFacade
from trading_system.strategy.engine import StrategyEngine
from trading_system.strategy.examples.mock_strategy import MockStrategy
from trading_system.strategy.monitor import StrategyMonitor
from trading_system.core.message_bus import MessageBus
from trading_system.core.logging import initialize_logging, get_logger

# Setup logging
initialize_logging()
logger = get_logger("live_signal_test")


class LiveSignalTester:
    """
    Tests real-time signal generation by connecting strategies to live market data.
    
    This class initializes the market data facade, strategy engine, and monitors signals
    generated in response to real-time market data.
    """
    
    def __init__(
        self,
        symbols: List[str] = None,
        timeframes: List[str] = None,
        strategies: List[Dict[str, Any]] = None,
        data_source: str = "tradingview",
        duration_seconds: int = 3600,
        report_interval: int = 300
    ):
        """
        Initialize the live signal test.
        
        Args:
            symbols: List of symbols to test
            timeframes: List of timeframes to test
            strategies: List of strategy configurations
            data_source: Market data source to use
            duration_seconds: Test duration in seconds
            report_interval: Report interval in seconds
        """
        # Set default values
        self.symbols = symbols or ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
        self.timeframes = timeframes or ["1m", "5m", "15m"]
        self.data_source = data_source
        self.duration_seconds = duration_seconds
        self.report_interval = report_interval
        
        # Initialize components
        self.message_bus = None
        self.market_data = None
        self.strategy_engine = None
        
        # Signal trackers
        self.signals_received = 0
        self.last_signal_time = None
        self.signals_by_strategy = {}
        self.signals_by_symbol = {}
        
        # Monitors
        self.strategy_monitors = {}
        
        # Test status
        self.running = False
        self.start_time = None

    async def initialize(self):
        """Initialize the message bus, market data facade, and strategy engine."""
        logger.info("Initializing live signal test environment...")
        
        # Initialize message bus
        self.message_bus = MessageBus()
        
        # Create a config dictionary for the market data facade
        market_data_config = {
            "validation_level": "basic",
            "message_bus": self.message_bus,
            "cache_size": 1000,
            "data_sources": {
                "tradingview": {
                    "enabled": True
                },
                "nobitex": {
                    "api_key": os.environ.get("NOBITEX_API_KEY", ""),
                    "api_secret": os.environ.get("NOBITEX_API_SECRET", ""),
                    "enabled": True if os.environ.get("NOBITEX_API_KEY") else False
                }
            }
        }
        
        # Initialize market data facade
        self.market_data = MarketDataFacade(market_data_config)
        await self.market_data.initialize()
        
        # Register message handlers - properly await this if it's async
        if asyncio.iscoroutinefunction(self.message_bus.subscribe):
            await self.message_bus.subscribe("market_data", self.handle_market_data)
            await self.message_bus.subscribe("strategy_signal", self.handle_strategy_signal)
        else:
            self.message_bus.subscribe("market_data", self.handle_market_data)
            self.message_bus.subscribe("strategy_signal", self.handle_strategy_signal)
        
        # Initialize strategy engine
        self.strategy_engine = StrategyEngine(self.message_bus)
        await self.strategy_engine.initialize()
        
        # Initialize strategies
        await self.initialize_strategies()
        
        # Initialize monitors
        self.initialize_monitors()
        
        logger.info("Live signal test environment initialized")

    async def initialize_strategies(self):
        """Initialize and register trading strategies."""
        logger.info("Initializing strategies...")
        
        # Create Mock Strategy
        mock_strategy = MockStrategy(
            name="Mock_Live_Test",
            symbols=self.symbols,
            timeframes=self.timeframes,
            state_dir="./data/strategy_state",
            performance_dir="./data/strategy_performance"
        )
        
        # Register strategy with engine
        self.strategy_engine.register_strategy(mock_strategy)
        
        # Initialize strategy
        await mock_strategy.initialize()
        
        # Track strategy
        self.signals_by_strategy[mock_strategy.name] = {
            "buy": 0,
            "sell": 0,
            "exit": 0,
            "neutral": 0
        }
        
        logger.info(f"Strategy {mock_strategy.name} initialized")

    def initialize_monitors(self):
        """Initialize performance monitors for each strategy."""
        logger.info("Initializing strategy monitors...")
        
        # For each strategy in the engine
        for strategy_name, strategy in self.strategy_engine.strategies.items():
            # Create monitor
            monitor = StrategyMonitor(
                strategy_name=strategy_name,
                storage_path=f"./data/strategy_monitors/{strategy_name}"
            )
            
            # Configure metrics
            monitor.add_metric("win_rate", 0.0, threshold_min=0.4, alert_on_threshold=True)
            monitor.add_metric("profit_factor", 0.0, threshold_min=1.2, alert_on_threshold=True)
            monitor.add_metric("max_drawdown", 0.0, threshold_max=0.2, alert_on_threshold=True)
            monitor.add_metric("signal_accuracy", 0.0, threshold_min=0.5, alert_on_threshold=True)
            
            # Set alert callback
            monitor.on_alert = self.handle_monitor_alert
            
            # Store monitor
            self.strategy_monitors[strategy_name] = monitor
            
            logger.info(f"Monitor for {strategy_name} initialized")

    def handle_market_data(self, message):
        """
        Handle incoming market data messages.
        
        Args:
            message: The market data message
        """
        # Check if message is a proper message object with data attribute
        if hasattr(message, 'data'):
            data = message.data
        else:
            # If message itself is the data
            data = message
            
        if not isinstance(data, dict):
            return
            
        # Log receipt of data
        symbol = data.get("symbol", "unknown")
        timeframe = data.get("timeframe", "unknown")
        
        # Initialize symbol tracking if needed
        if symbol not in self.signals_by_symbol:
            self.signals_by_symbol[symbol] = {
                "data_updates": 0,
                "buy": 0,
                "sell": 0,
                "exit": 0,
                "neutral": 0
            }
            
        # Update counts
        self.signals_by_symbol[symbol]["data_updates"] += 1
        
        # Only log occasionally to avoid spam
        if self.signals_by_symbol[symbol]["data_updates"] % 10 == 0:
            logger.debug(f"Received market data: {symbol} {timeframe}")

    def handle_strategy_signal(self, message):
        """
        Handle strategy signal messages.
        
        Args:
            message: The strategy signal message
        """
        # Check if message is a proper message object with data attribute
        if hasattr(message, 'data'):
            signal_data = message.data
        else:
            # If message itself is the data
            signal_data = message
            
        if not isinstance(signal_data, dict):
            return
            
        # Extract signal information
        strategy_name = signal_data.get("strategy", "unknown")
        symbol = signal_data.get("symbol", "unknown")
        timeframe = signal_data.get("timeframe", "unknown")
        signal_type = signal_data.get("signal", "neutral")
        confidence = signal_data.get("confidence", 0.0)
        
        # Update signal counts
        if strategy_name in self.signals_by_strategy:
            self.signals_by_strategy[strategy_name][signal_type] += 1
            
        if symbol in self.signals_by_symbol:
            self.signals_by_symbol[symbol][signal_type] += 1
        
        # Update total signal count
        self.signals_received += 1
        self.last_signal_time = datetime.now()
        
        # Log the signal
        logger.info(f"Signal: {strategy_name} | {symbol} {timeframe} | {signal_type.upper()} | Confidence: {confidence:.2f}")
        
        # Update monitor if signal is not neutral
        if signal_type != "neutral" and strategy_name in self.strategy_monitors:
            self.strategy_monitors[strategy_name].update_metric("signal_accuracy", confidence)

    def handle_monitor_alert(self, metric_name: str, current_value: float, threshold: float, is_below: bool):
        """
        Handle alerts from strategy monitors.
        
        Args:
            metric_name: Name of the metric triggering the alert
            current_value: Current value of the metric
            threshold: Threshold that was crossed
            is_below: True if the alert is for value below threshold, False if above
        """
        direction = "below" if is_below else "above"
        logger.warning(f"ALERT: {metric_name} is {direction} threshold! Current: {current_value:.4f}, Threshold: {threshold:.4f}")

    async def start_market_data(self):
        """Start polling for market data for all symbols and timeframes."""
        logger.info(f"Starting market data polling for {len(self.symbols)} symbols and {len(self.timeframes)} timeframes")
        
        # Create cancellable polling task
        self.polling_task = asyncio.create_task(self._poll_market_data())
        logger.info("Market data polling started")
        
    async def _poll_market_data(self):
        """Poll for market data periodically."""
        try:
            while self.running:
                for symbol in self.symbols:
                    for timeframe in self.timeframes:
                        try:
                            # Get latest market data
                            data = await self.market_data.get_market_data(
                                symbol=symbol,
                                timeframe=timeframe,
                                limit=1
                            )
                            
                            if data and isinstance(data, list) and len(data) > 0:
                                # Process the data as if it came through the message bus
                                candle = data[0]
                                message_data = {
                                    "symbol": symbol,
                                    "timeframe": timeframe,
                                    "data": candle,
                                    "source": self.data_source
                                }
                                await asyncio.to_thread(self.handle_market_data, message_data)
                        except Exception as e:
                            logger.error(f"Error polling {symbol} {timeframe}: {str(e)}")
                
                # Wait before polling again
                await asyncio.sleep(5)  # Poll every 5 seconds
        except asyncio.CancelledError:
            logger.info("Market data polling cancelled")
        except Exception as e:
            logger.error(f"Error in market data polling: {str(e)}")

    async def stop_market_data(self):
        """Stop polling for market data."""
        logger.info("Stopping market data polling")
        
        if hasattr(self, 'polling_task') and self.polling_task:
            self.polling_task.cancel()
            try:
                await self.polling_task
            except asyncio.CancelledError:
                pass
            
        logger.info("Market data polling stopped")

    def generate_report(self) -> Dict[str, Any]:
        """
        Generate a test summary report.
        
        Returns:
            Dict containing test statistics and results
        """
        duration = time.time() - self.start_time if self.start_time else 0
        
        report = {
            "test_duration_seconds": duration,
            "signals_received": self.signals_received,
            "signals_per_minute": (self.signals_received / duration * 60) if duration > 0 else 0,
            "last_signal_time": self.last_signal_time.isoformat() if self.last_signal_time else None,
            "signals_by_strategy": self.signals_by_strategy,
            "signals_by_symbol": self.signals_by_symbol
        }
        
        # Add monitor metrics
        report["strategy_metrics"] = {}
        for strategy_name, monitor in self.strategy_monitors.items():
            report["strategy_metrics"][strategy_name] = monitor.get_metrics_as_dict()
            
        return report

    def save_report(self, report: Dict[str, Any], filename: str = None):
        """
        Save the test report to a JSON file.
        
        Args:
            report: The report to save
            filename: Optional filename to save to
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"./data/reports/live_test_report_{timestamp}.json"
            
        # Ensure directory exists
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        # Save report
        with open(filename, 'w') as f:
            json.dump(report, f, indent=2)
            
        logger.info(f"Test report saved to {filename}")

    async def run(self, duration_seconds: int = 300):
        """
        Run the live signal test for the specified duration.
        
        Args:
            duration_seconds: Test duration in seconds
        """
        self.running = True
        self.start_time = time.time()
        
        logger.info(f"Starting live signal test for {duration_seconds} seconds")
        
        # Windows doesn't support signal handling in asyncio
        # Just catch KeyboardInterrupt in the try/except block
        
        try:
            # Initialize components
            await self.initialize()
            
            # Start market data
            await self.start_market_data()
            
            # Create directories for data
            os.makedirs("./data/strategy_state", exist_ok=True)
            os.makedirs("./data/strategy_performance", exist_ok=True)
            os.makedirs("./data/strategy_monitors", exist_ok=True)
            os.makedirs("./data/reports", exist_ok=True)
            
            # Run for specified duration
            end_time = self.start_time + duration_seconds
            
            # Progress updates
            report_interval = min(60, max(10, duration_seconds // 10))  # Report at least 10 times
            next_report = time.time() + report_interval
            
            while self.running and time.time() < end_time:
                current_time = time.time()
                remaining = end_time - current_time
                
                # Report progress
                if current_time >= next_report:
                    elapsed = current_time - self.start_time
                    progress = elapsed / duration_seconds * 100
                    signals_per_min = self.signals_received / (elapsed / 60) if elapsed > 0 else 0
                    
                    logger.info(f"Progress: {progress:.1f}% | Remaining: {remaining:.1f}s | Signals: {self.signals_received} ({signals_per_min:.1f}/min)")
                    next_report = current_time + report_interval
                
                # Small delay to prevent CPU hogging
                await asyncio.sleep(1)
                
            # Generate final report
            logger.info("Test completed, generating report...")
            report = self.generate_report()
            self.save_report(report)
            
            # Display summary
            logger.info(f"Test Summary: {report['signals_received']} signals received over {report['test_duration_seconds']:.1f} seconds")
            logger.info(f"Signals per minute: {report['signals_per_minute']:.2f}")
            
            for strategy_name, signals in report['signals_by_strategy'].items():
                logger.info(f"Strategy {strategy_name}: Buy: {signals['buy']}, Sell: {signals['sell']}, Exit: {signals['exit']}")
                
        finally:
            # Ensure cleanup happens
            await self.shutdown()

    async def shutdown(self):
        """Shutdown all components and stop the test."""
        if not self.running:
            return
            
        self.running = False
        logger.info("Shutting down live signal test...")
        
        # Stop market data
        await self.stop_market_data()
        
        # Stop strategy engine
        if self.strategy_engine:
            await self.strategy_engine.shutdown()
            
        # Stop market data facade
        if self.market_data:
            await self.market_data.shutdown()
            
        logger.info("Live signal test shutdown complete")


async def main():
    """Main entry point for the live signal test script."""
    parser = argparse.ArgumentParser(description="Test real-time signal generation with live market data")
    parser.add_argument("--symbols", type=str, default="BTC/USDT,ETH/USDT,SOL/USDT", 
                       help="Comma-separated list of symbols to test (default: BTC/USDT,ETH/USDT,SOL/USDT)")
    parser.add_argument("--timeframes", type=str, default="1m,5m,15m",
                       help="Comma-separated list of timeframes to test (default: 1m,5m,15m)")
    parser.add_argument("--duration", type=int, default=3600,
                       help="Test duration in seconds (default: 3600)")
    parser.add_argument("--source", type=str, default="tradingview",
                       help="Market data source (default: tradingview)")
    
    args = parser.parse_args()
    
    # Parse arguments
    symbols = args.symbols.split(",")
    timeframes = args.timeframes.split(",")
    
    # Display configuration
    logger.info(f"Starting live signal test with configuration:")
    logger.info(f"Symbols: {symbols}")
    logger.info(f"Timeframes: {timeframes}")
    logger.info(f"Duration: {args.duration} seconds")
    logger.info(f"Data source: {args.source}")
    
    # Create and run tester
    tester = LiveSignalTester(
        symbols=symbols,
        timeframes=timeframes,
        data_source=args.source,
        duration_seconds=args.duration
    )
    
    await tester.run(args.duration)


if __name__ == "__main__":
    asyncio.run(main()) 