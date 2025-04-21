"""
Strategy Engine Example

This example demonstrates how to use the Strategy Engine with the Moving
Average Strategy to process market data and generate trading signals.

Features:
- Strategy Engine initialization
- Strategy registration and configuration
- Market data processing
- Signal handling and execution
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any

from trading_system.strategy.strategy_engine import (
    StrategyEngine, 
    StrategyParameters,
    SignalType,
    SignalDirection
)
from trading_system.strategy.moving_average_strategy import MovingAverageStrategy
from trading_system.market_data.market_data_types import MarketData
from trading_system.core.logging import setup_logging, get_logger
from trading_system.core.message_bus import MessageBus

# Setup logging
setup_logging(level=logging.INFO)
logger = get_logger("examples.strategy")

class MockExecution:
    """Mock execution engine for demonstration."""
    
    def __init__(self):
        self.orders = []
        
    async def execute_signal(self, signal):
        """Mock execute a trading signal."""
        logger.info(f"Executing signal: {signal.signal_type.value} {signal.direction.value} for {signal.ticker}")
        
        # Simulate execution delay
        await asyncio.sleep(0.5)
        
        # Create mock order result
        order_id = f"order-{len(self.orders) + 1}"
        self.orders.append({
            "order_id": order_id,
            "ticker": signal.ticker,
            "type": signal.signal_type.value,
            "direction": signal.direction.value,
            "timestamp": signal.timestamp.isoformat(),
            "status": "FILLED"
        })
        
        result = {
            "order_id": order_id,
            "status": "FILLED",
            "fill_price": signal.params.get("price", 0) * (0.99 if signal.direction == SignalDirection.SHORT else 1.01),
            "timestamp": datetime.now().isoformat()
        }
        
        return result

class MarketDataSimulator:
    """Simulates market data for testing."""
    
    def __init__(self, ticker: str, start_price: float = 100.0):
        self.ticker = ticker
        self.current_price = start_price
        self.timestamp = datetime.now()
        self.trend = 0  # 0: sideways, 1: uptrend, -1: downtrend
        self.trend_duration = 0
        self.volatility = 0.002
        self.max_trend_duration = 20
    
    def next_candle(self) -> MarketData:
        """Generate next candle data."""
        # Update trend if needed
        if self.trend_duration <= 0:
            # Change trend
            options = [-1, 0, 1]
            if self.trend in options:
                options.remove(self.trend)
            self.trend = options[int(asyncio.get_event_loop().time() * 10) % 2]
            self.trend_duration = int(asyncio.get_event_loop().time() * 100) % self.max_trend_duration + 5
        
        # Reduce trend duration
        self.trend_duration -= 1
        
        # Calculate price movement
        base_change = self.volatility * (asyncio.get_event_loop().time() * 1000 % 10) / 10
        trend_change = self.trend * self.volatility * 1.5
        change = base_change + trend_change
        
        # Update price
        self.current_price *= (1 + change)
        
        # Create candle data
        open_price = self.current_price * (1 - self.volatility/2)
        high_price = max(open_price, self.current_price) * (1 + self.volatility/2)
        low_price = min(open_price, self.current_price) * (1 - self.volatility/2)
        volume = 10 + (asyncio.get_event_loop().time() * 100 % 90)
        
        # Update timestamp
        self.timestamp += timedelta(minutes=1)
        
        # Create MarketData object
        candle_data = {
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": self.current_price,
            "volume": volume
        }
        
        return MarketData(
            ticker=self.ticker,
            data_type="candle",
            timestamp=self.timestamp,
            data=candle_data
        )

async def run_strategy_example():
    """Run the strategy engine example."""
    logger.info("Starting strategy engine example")
    
    # Initialize message bus
    message_bus = MessageBus()
    await message_bus.initialize()
    
    # Initialize strategy engine
    strategy_engine = StrategyEngine(message_bus=message_bus)
    await strategy_engine.initialize()
    
    # Create moving average strategy
    ma_params = StrategyParameters(
        params={
            "short_window": 5,        # Short window for faster simulation
            "long_window": 15,        # Long window for faster simulation
            "entry_threshold": 0.001, # Entry threshold (0.1%)
            "exit_threshold": 0.0005, # Exit threshold (0.05%)
            "min_volume": 5.0,        # Min volume
            "enable_short": True      # Enable short positions
        }
    )
    
    # Create and register strategy
    ma_strategy = MovingAverageStrategy(
        strategy_id="ma_crossover_1",
        name="MA Crossover (5,15)",
        parameters=ma_params
    )
    ma_strategy.supported_tickers = ["BTC/USDT", "ETH/USDT"]
    
    # Register strategy with engine
    strategy_engine.register_strategy(ma_strategy)
    
    # Create mock execution engine
    execution = MockExecution()
    
    # Create data simulators
    btc_simulator = MarketDataSimulator("BTC/USDT", 40000.0)
    eth_simulator = MarketDataSimulator("ETH/USDT", 2500.0)
    
    # Start strategy engine
    await strategy_engine.start()
    
    # Connect signal handlers
    async def signal_handler(topic, signal):
        if topic == "signals.new":
            logger.info(f"Received signal: {signal.signal_type.value} {signal.direction.value} for {signal.ticker}")
            # Execute signal
            result = await execution.execute_signal(signal)
            # Send back result
            await strategy_engine.on_signal_executed(signal, result)
    
    # Subscribe to signal topics
    await message_bus.subscribe("signals.#", signal_handler)
    
    # Run simulation for 200 minutes
    try:
        logger.info("Running market data simulation...")
        for _ in range(200):
            # Generate candle data
            btc_data = btc_simulator.next_candle()
            eth_data = eth_simulator.next_candle()
            
            # Process market data
            await strategy_engine.process_market_data(btc_data)
            await strategy_engine.process_market_data(eth_data)
            
            # Sleep to simulate time passing
            await asyncio.sleep(0.1)
            
        # Print summary of signals generated
        for strategy_id, strategy in strategy_engine.strategies.items():
            logger.info(f"Strategy {strategy.name} generated {len(strategy.signals)} signals")
            
            # Group signals by ticker and type
            signal_summary = {}
            for signal in strategy.signals:
                key = f"{signal.ticker}_{signal.signal_type.value}_{signal.direction.value}"
                if key not in signal_summary:
                    signal_summary[key] = 0
                signal_summary[key] += 1
            
            # Print summary
            for key, count in signal_summary.items():
                ticker, signal_type, direction = key.split('_')
                logger.info(f"  {ticker}: {signal_type} {direction} - {count} signals")
    
    finally:
        # Stop strategy engine
        await strategy_engine.stop()
        
        # Close message bus
        await message_bus.shutdown()

def main():
    """Main entry point."""
    print("Strategy Engine Example")
    print("======================")
    print("This example demonstrates using the Strategy Engine with")
    print("a Moving Average Crossover strategy to process market data")
    print("and generate trading signals.")
    print("\nFeatures demonstrated:")
    print("- Strategy registration and configuration")
    print("- Market data simulation and processing")
    print("- Signal generation based on moving average crossovers")
    print("- Signal execution and callback handling")
    print("- Message bus integration for signal distribution")
    print("\nRunning example...\n")
    
    # Run the example
    asyncio.run(run_strategy_example())

if __name__ == "__main__":
    main() 