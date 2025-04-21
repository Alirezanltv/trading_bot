#!/usr/bin/env python
"""
Strategy subsystem test script.

This script demonstrates how to use the StrategyFactory and StrategyManager components.
"""

import os
import sys
import time
import json
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Any

import pandas as pd
import numpy as np

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from trading_system.core import setup_logging, get_logger
from trading_system.core import message_bus, MessageTypes
from trading_system.strategy.factory import strategy_factory
from trading_system.strategy.manager import strategy_manager
from trading_system.strategy.base import SimpleStrategy, RSIStrategy, SignalType

# Set up logging
setup_logging()
logger = get_logger("strategy_test")

def generate_sample_market_data(symbol: str, periods: int = 100) -> pd.DataFrame:
    """
    Generate sample market data for testing.

    Args:
        symbol: Trading symbol
        periods: Number of periods to generate

    Returns:
        DataFrame with OHLCV data
    """
    now = datetime.now()
    dates = [now - timedelta(minutes=i) for i in range(periods)]
    dates.reverse()

    # Create base price with random walk
    np.random.seed(42)  # For reproducibility
    price = 1000
    closes = [price]
    
    for _ in range(periods - 1):
        change_percent = np.random.normal(0, 1) / 100  # Mean 0%, std 1%
        price = price * (1 + change_percent)
        closes.append(price)

    # Generate OHLCV data
    data = {
        'timestamp': dates,
        'open': [],
        'high': [],
        'low': [],
        'close': closes,
        'volume': []
    }

    # Generate open, high, low based on close
    for i in range(periods):
        close = closes[i]
        
        # Calculate a random open within 0.5% of close
        open_price = close * (1 + np.random.uniform(-0.005, 0.005))
        
        # High is the max of open and close plus a random amount
        high = max(open_price, close) * (1 + abs(np.random.normal(0, 0.003)))
        
        # Low is the min of open and close minus a random amount
        low = min(open_price, close) * (1 - abs(np.random.normal(0, 0.003)))
        
        # Random volume
        volume = np.random.uniform(500, 1500)
        
        data['open'].append(open_price)
        data['high'].append(high)
        data['low'].append(low)
        data['volume'].append(volume)
    
    # Create DataFrame
    df = pd.DataFrame(data)
    df.set_index('timestamp', inplace=True)
    
    return df

def test_strategy_factory():
    """Test StrategyFactory functionality."""
    logger.info("Testing StrategyFactory...")
    
    # Register built-in strategies
    strategy_factory.register_strategy_class(SimpleStrategy)
    strategy_factory.register_strategy_class(RSIStrategy)
    
    # Create strategies from configurations
    simple_config = {
        "name": "MyMACrossover",
        "parameters": {
            "fast_ma_period": 5,
            "slow_ma_period": 20
        },
        "symbols": ["BTC/USD", "ETH/USD"],
        "timeframes": ["1m", "5m"]
    }
    
    rsi_config = {
        "name": "MyRSIStrategy",
        "parameters": {
            "rsi_period": 14,
            "overbought": 75,
            "oversold": 25
        },
        "symbols": ["BTC/USD"],
        "timeframes": ["1m", "5m", "15m"]
    }
    
    # Create strategy instances
    simple_strategy = strategy_factory.create_strategy("SimpleStrategy", simple_config)
    rsi_strategy = strategy_factory.create_strategy("RSIStrategy", rsi_config)
    
    # Save strategy configurations
    strategy_factory.save_strategy_configuration(simple_strategy)
    strategy_factory.save_strategy_configuration(rsi_strategy)
    
    # Load strategy configurations
    loaded_configs = strategy_factory.load_all_strategy_configurations()
    logger.info(f"Loaded {len(loaded_configs)} strategy configurations")
    
    # Create strategies from loaded configurations
    strategies = strategy_factory.create_strategies_from_configurations()
    logger.info(f"Created {len(strategies)} strategies from configurations")
    
    logger.info("StrategyFactory test completed")
    return strategies

def test_strategies_signal_generation(strategies: Dict[str, Any]):
    """
    Test signal generation for each strategy.
    
    Args:
        strategies: Dictionary of strategy instances
    """
    logger.info("Testing strategy signal generation...")
    
    # Generate sample market data
    symbols = ["BTC/USD", "ETH/USD"]
    market_data = {symbol: generate_sample_market_data(symbol) for symbol in symbols}
    
    # Test each strategy
    for name, strategy in strategies.items():
        logger.info(f"Testing strategy: {name}")
        
        # Generate signals
        result = strategy.generate_signals(market_data)
        
        # Log results
        logger.info(f"  Status: {result.status}")
        logger.info(f"  Execution time: {result.execution_time:.4f} seconds")
        logger.info(f"  Generated {len(result.signals)} signals")
        
        # Log signals
        for signal in result.signals:
            logger.info(f"  Signal: {signal.symbol} - {signal.signal_type.value} @ {signal.price:.2f} (conf: {signal.confidence:.2f})")
    
    logger.info("Strategy signal generation test completed")

def test_strategy_manager(strategies: Dict[str, Any]):
    """
    Test StrategyManager functionality.
    
    Args:
        strategies: Dictionary of strategy instances
    """
    logger.info("Testing StrategyManager...")
    
    # Register strategies with manager
    for i, (name, strategy) in enumerate(strategies.items()):
        strategy_manager.register_strategy(strategy, priority=i, weight=1.0)
    
    # Start strategy manager
    strategy_manager.start()
    
    logger.info(f"Registered {len(strategies)} strategies with StrategyManager")
    
    # Generate sample market data
    symbols = ["BTC/USD", "ETH/USD"]
    timeframes = ["1m", "5m"]
    
    # Update market data
    for symbol in symbols:
        for timeframe in timeframes:
            data = generate_sample_market_data(symbol)
            strategy_manager.update_market_data(symbol, timeframe, data)
            logger.info(f"Updated market data for {symbol} {timeframe}")
    
    # Wait for strategies to execute
    logger.info("Waiting for strategies to execute...")
    time.sleep(3)
    
    # Get recent signals
    recent_signals = strategy_manager.get_recent_signals()
    
    for symbol, strategy_signals in recent_signals.items():
        logger.info(f"Recent signals for {symbol}:")
        for strategy_name, signals in strategy_signals.items():
            logger.info(f"  Strategy {strategy_name}: {len(signals)} signals")
            for signal in signals:
                logger.info(f"    {signal.signal_type.value} @ {signal.price:.2f} (conf: {signal.confidence:.2f})")
    
    # Get aggregated signals
    aggregated_signals = strategy_manager.get_aggregated_signals()
    
    for symbol, signal in aggregated_signals.items():
        logger.info(f"Aggregated signal for {symbol}: {signal.signal_type.value} @ {signal.price:.2f} (conf: {signal.confidence:.2f})")
    
    # Get performance metrics
    metrics = strategy_manager.get_performance_metrics()
    
    for strategy_name, strategy_metrics in metrics.items():
        logger.info(f"Performance metrics for {strategy_name}:")
        for metric, value in strategy_metrics.items():
            logger.info(f"  {metric}: {value}")
    
    # Stop strategy manager
    strategy_manager.stop()
    
    logger.info("StrategyManager test completed")

async def test_message_bus_integration():
    """Test message bus integration with strategy signals."""
    logger.info("Testing message bus integration...")
    
    # Subscribe to strategy signals
    received_signals = []
    
    def signal_handler(message):
        logger.info(f"Received strategy signal: {message['payload']['signal']['signal_type']}")
        received_signals.append(message)
    
    # Subscribe to strategy signals
    message_bus.subscribe(MessageTypes.STRATEGY_SIGNAL, signal_handler)
    
    # Register strategies with manager
    simple_strategy = SimpleStrategy("TestMA", {
        "parameters": {"fast_ma_period": 3, "slow_ma_period": 7},
        "symbols": ["BTC/USD"]
    })
    
    rsi_strategy = RSIStrategy("TestRSI", {
        "parameters": {"rsi_period": 5, "overbought": 70, "oversold": 30},
        "symbols": ["BTC/USD"]
    })
    
    strategy_manager.register_strategy(simple_strategy, priority=0, weight=1.0)
    strategy_manager.register_strategy(rsi_strategy, priority=1, weight=1.0)
    
    # Start strategy manager
    strategy_manager.start()
    
    # Update market data
    data = generate_sample_market_data("BTC/USD")
    strategy_manager.update_market_data("BTC/USD", "1m", data)
    
    # Wait for signals to be published
    await asyncio.sleep(3)
    
    # Check received signals
    logger.info(f"Received {len(received_signals)} signals via message bus")
    
    # Stop strategy manager
    strategy_manager.stop()
    
    logger.info("Message bus integration test completed")

def main():
    """Run all tests."""
    logger.info("Starting strategy subsystem tests")
    
    # Test StrategyFactory
    strategies = test_strategy_factory()
    
    # Test strategy signal generation
    test_strategies_signal_generation(strategies)
    
    # Test StrategyManager
    test_strategy_manager(strategies)
    
    # Test message bus integration 
    asyncio.run(test_message_bus_integration())
    
    logger.info("All tests completed")

if __name__ == "__main__":
    main() 