"""
Mock Strategy Engine

This module provides a simulated strategy engine for integration testing.
"""

import asyncio
import datetime
import logging
import random
from typing import Dict, List, Any, Optional

from trading_system.core.component import Component
from trading_system.core.message_bus import (
    message_bus, get_async_message_bus
)
from trading_system.tests.integration.mocks.mock_message_types import MessageTypes
from trading_system.tests.integration.mocks.mock_component_status import ComponentStatus

# Instead of importing, define mock types directly
class SignalType:
    """Signal type enumeration"""
    LONG = "long"
    SHORT = "short"
    NEUTRAL = "neutral"

class Signal:
    """Mock signal data class"""
    def __init__(self, id, timestamp, strategy_id, symbol, type, price, confidence, metadata=None):
        self.id = id
        self.timestamp = timestamp
        self.strategy_id = strategy_id
        self.symbol = symbol
        self.type = type
        self.price = price
        self.confidence = confidence
        self.metadata = metadata or {}

# Import our mock order types instead of real ones
from trading_system.tests.integration.mocks.mock_order import Order, OrderType, OrderSide, TimeInForce

logger = logging.getLogger("integration.mock_strategy")


class MockStrategyEngine(Component):
    """
    Mock strategy engine for integration testing.
    
    This component simulates a strategy engine that generates trading signals
    based on configurable patterns for testing system behavior.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the mock strategy engine.
        
        Args:
            config: Configuration settings
        """
        super().__init__("mock_strategy")
        self.config = config or {}
        
        # Strategy state
        self.symbols = self.config.get("symbols", ["BTC/USDT", "ETH/USDT"])
        self.active = False
        self.signal_generation_interval = self.config.get("signal_interval", 10)  # seconds
        
        # Signal generation patterns
        self.signal_patterns = self.config.get("signal_patterns", {
            "alternating": True,  # Alternate between buy and sell
            "trend_following": False,  # Follow price trends
            "mean_reversion": False,  # Mean reversion strategy
            "random": False  # Random signals
        })
        
        # Current market state tracked by the strategy
        self.market_state = {}
        for symbol in self.symbols:
            self.market_state[symbol] = {
                "current_price": None,
                "prices": [],  # Historical prices
                "last_signal": None,  # Last signal generated
                "last_signal_time": None,  # Time of last signal
                "position": 0  # Current simulated position
            }
        
        # Strategy performance metrics
        self.metrics = {
            "signals_generated": 0,
            "long_signals": 0,
            "short_signals": 0,
            "neutral_signals": 0
        }
        
        # Runtime variables
        self.signal_task = None
        self.async_message_bus = None
        
        # Signal handlers
        self.market_data_handlers = {
            MessageTypes.MARKET_DATA_TICKER_UPDATE: self._handle_ticker_update,
            MessageTypes.MARKET_DATA_TRADE_UPDATE: self._handle_trade_update,
            MessageTypes.MARKET_DATA_BAR_UPDATE: self._handle_bar_update,
            MessageTypes.MARKET_DATA_ORDERBOOK_UPDATE: self._handle_orderbook_update
        }
    
    async def initialize(self):
        """Initialize the mock strategy engine."""
        logger.info("Initializing mock strategy engine")
        self.async_message_bus = get_async_message_bus()
        
        # Subscribe to market data updates
        for message_type, handler in self.market_data_handlers.items():
            await self.async_message_bus.subscribe(message_type, handler)
        
        self.status = ComponentStatus.INITIALIZED
        return self.status
    
    async def start(self):
        """Start the mock strategy engine."""
        logger.info("Starting mock strategy engine")
        
        # Start the signal generation task
        self.active = True
        self.signal_task = asyncio.create_task(self._signal_generation_loop())
        
        self.status = ComponentStatus.OPERATIONAL
        return self.status
    
    async def stop(self):
        """Stop the mock strategy engine."""
        logger.info("Stopping mock strategy engine")
        
        # Stop the signal generation task
        self.active = False
        if self.signal_task:
            self.signal_task.cancel()
            try:
                await self.signal_task
            except asyncio.CancelledError:
                pass
            self.signal_task = None
        
        self.status = ComponentStatus.SHUTDOWN
        return self.status
    
    async def get_status(self):
        """Get the current status of the mock strategy engine."""
        return self.status
    
    async def _signal_generation_loop(self):
        """Main loop for generating trading signals."""
        try:
            while self.active:
                # Generate signals based on current market state
                for symbol in self.symbols:
                    if self.market_state[symbol]["current_price"] is not None:
                        await self._generate_signal(symbol)
                
                # Sleep for the signal generation interval
                await asyncio.sleep(self.signal_generation_interval)
                
        except asyncio.CancelledError:
            logger.info("Signal generation loop cancelled")
            raise
        except Exception as e:
            logger.error(f"Error in signal generation loop: {str(e)}", exc_info=True)
            self.status = ComponentStatus.ERROR
    
    async def _generate_signal(self, symbol: str):
        """
        Generate a trading signal for the given symbol.
        
        Args:
            symbol: Instrument symbol
        """
        signal_type = await self._determine_signal_type(symbol)
        
        if signal_type == SignalType.NEUTRAL:
            # No trading signal
            return
        
        # Get current price
        current_price = self.market_state[symbol]["current_price"]
        
        # Create signal
        signal = Signal(
            id=f"mock-signal-{self.metrics['signals_generated'] + 1}",
            timestamp=datetime.datetime.now(),
            strategy_id="mock_strategy",
            symbol=symbol,
            type=signal_type,
            price=current_price,
            confidence=random.uniform(0.6, 0.9),
            metadata={
                "reason": "mock signal for testing",
                "indicators": {
                    "simulation": True
                }
            }
        )
        
        # Update last signal
        self.market_state[symbol]["last_signal"] = signal_type
        self.market_state[symbol]["last_signal_time"] = datetime.datetime.now()
        
        # Update metrics
        self.metrics["signals_generated"] += 1
        if signal_type == SignalType.LONG:
            self.metrics["long_signals"] += 1
        elif signal_type == SignalType.SHORT:
            self.metrics["short_signals"] += 1
        else:
            self.metrics["neutral_signals"] += 1
        
        # Publish signal
        await self._publish_signal(signal)
        
        # If configured, also generate an order
        if self.config.get("generate_orders", True):
            await self._generate_order_from_signal(signal)
    
    async def _determine_signal_type(self, symbol: str) -> SignalType:
        """
        Determine the signal type based on the current state and configured patterns.
        
        Args:
            symbol: Instrument symbol
            
        Returns:
            Signal type
        """
        # Get current market state
        state = self.market_state[symbol]
        
        # If we don't have enough price history, return neutral
        if len(state["prices"]) < 10:
            return SignalType.NEUTRAL
        
        # Default signal type
        signal_type = SignalType.NEUTRAL
        
        # Generate a signal based on the configured patterns
        if self.signal_patterns.get("alternating", False):
            # Alternate between long and short
            if state["last_signal"] == SignalType.LONG:
                signal_type = SignalType.SHORT
            elif state["last_signal"] == SignalType.SHORT:
                signal_type = SignalType.LONG
            else:
                # First signal or last was neutral
                signal_type = SignalType.LONG if random.random() > 0.5 else SignalType.SHORT
        
        elif self.signal_patterns.get("trend_following", False):
            # Simple trend following based on recent price direction
            recent_prices = state["prices"][-5:]
            price_change = recent_prices[-1] - recent_prices[0]
            
            if price_change > 0:
                signal_type = SignalType.LONG
            elif price_change < 0:
                signal_type = SignalType.SHORT
        
        elif self.signal_patterns.get("mean_reversion", False):
            # Simple mean reversion
            recent_prices = state["prices"][-10:]
            avg_price = sum(recent_prices) / len(recent_prices)
            current_price = state["current_price"]
            
            # If current price is significantly above average, go short
            if current_price > avg_price * 1.02:
                signal_type = SignalType.SHORT
            # If current price is significantly below average, go long
            elif current_price < avg_price * 0.98:
                signal_type = SignalType.LONG
        
        elif self.signal_patterns.get("random", False):
            # Generate random signals with some probability
            if random.random() < 0.3:  # 30% chance of generating a signal
                signal_type = random.choice([SignalType.LONG, SignalType.SHORT])
        
        # Random chance of no signal
        if random.random() < 0.4:  # 40% chance of no signal
            signal_type = SignalType.NEUTRAL
        
        return signal_type
    
    async def _publish_signal(self, signal: Signal):
        """
        Publish a trading signal.
        
        Args:
            signal: Trading signal
        """
        try:
            await self.async_message_bus.publish(
                MessageTypes.STRATEGY_SIGNAL,
                signal
            )
            logger.info(f"Published {signal.type} signal for {signal.symbol} at {signal.price}")
        except Exception as e:
            logger.error(f"Error publishing signal: {str(e)}", exc_info=True)
    
    async def _generate_order_from_signal(self, signal: Signal):
        """
        Generate an order from a trading signal.
        
        Args:
            signal: Trading signal
        """
        # Only generate orders for non-neutral signals
        if signal.type == SignalType.NEUTRAL:
            return
        
        # Determine order side
        side = OrderSide.BUY if signal.type == SignalType.LONG else OrderSide.SELL
        
        # Generate random quantity
        quantity = random.uniform(0.1, 1.0)
        
        # Create order
        order = Order(
            symbol=signal.symbol,
            side=side,
            type=OrderType.MARKET,
            quantity=quantity,
            price=signal.price,  # For market orders, this is just an estimate
            time_in_force=TimeInForce.GTC,
            strategy_id=signal.strategy_id,
            signal_id=signal.id
        )
        
        # Publish the order request
        try:
            await self.async_message_bus.publish(
                MessageTypes.ORDER_REQUEST,
                order
            )
            logger.info(f"Generated {side.value} order for {signal.symbol}, qty: {quantity}")
        except Exception as e:
            logger.error(f"Error publishing order request: {str(e)}", exc_info=True)
    
    # Market data handlers
    
    async def _handle_ticker_update(self, message_type, data):
        """
        Handle ticker updates.
        
        Args:
            message_type: Message type
            data: Ticker data
        """
        symbol = data.symbol
        
        # Store current price
        if symbol in self.market_state:
            self.market_state[symbol]["current_price"] = data.price
            self.market_state[symbol]["prices"].append(data.price)
            
            # Keep only recent prices
            if len(self.market_state[symbol]["prices"]) > 100:
                self.market_state[symbol]["prices"] = self.market_state[symbol]["prices"][-100:]
    
    async def _handle_trade_update(self, message_type, data):
        """
        Handle trade updates.
        
        Args:
            message_type: Message type
            data: Trade data
        """
        # Not used in basic strategy simulation
        pass
    
    async def _handle_bar_update(self, message_type, data):
        """
        Handle bar updates.
        
        Args:
            message_type: Message type
            data: Bar data
        """
        # Not used in basic strategy simulation
        pass
    
    async def _handle_orderbook_update(self, message_type, data):
        """
        Handle order book updates.
        
        Args:
            message_type: Message type
            data: Order book data
        """
        # Not used in basic strategy simulation
        pass
    
    # Simulation control methods
    
    def set_signal_pattern(self, pattern_name: str, enabled: bool):
        """
        Set a signal generation pattern.
        
        Args:
            pattern_name: Pattern name
            enabled: Whether the pattern is enabled
        """
        if pattern_name in self.signal_patterns:
            self.signal_patterns[pattern_name] = enabled
            logger.info(f"Set signal pattern {pattern_name} to {enabled}")
    
    def set_signal_interval(self, interval_seconds: int):
        """
        Set the signal generation interval.
        
        Args:
            interval_seconds: Interval in seconds
        """
        self.signal_generation_interval = interval_seconds
        logger.info(f"Set signal generation interval to {interval_seconds} seconds")
    
    def get_metrics(self):
        """Get strategy metrics."""
        return self.metrics 