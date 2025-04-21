"""
Mock Market Data Subsystem

This module provides a simulated market data subsystem for integration testing.
"""

import asyncio
import logging
import random
import datetime
import time
from typing import Dict, List, Any, Optional

from trading_system.core.component import Component
from trading_system.core.message_bus import (
    message_bus, get_async_message_bus
)
from trading_system.tests.integration.mocks.mock_message_types import MessageTypes
from trading_system.tests.integration.mocks.mock_component_status import ComponentStatus

logger = logging.getLogger("integration.mock_market_data")

# Mock market data types - defined here to avoid dependencies on real implementations
class MarketDataType:
    """Market data types enumeration"""
    TICKER = "ticker"
    TRADE = "trade"
    ORDER_BOOK = "orderbook"
    BAR = "bar"
    INSTRUMENT_INFO = "instrument_info"

class TickerUpdate:
    """Mock ticker update data class"""
    def __init__(self, symbol, timestamp, price, volume_24h, change_24h, high_24h, low_24h):
        self.symbol = symbol
        self.timestamp = timestamp
        self.price = price
        self.volume_24h = volume_24h
        self.change_24h = change_24h
        self.high_24h = high_24h
        self.low_24h = low_24h

class OrderBookUpdate:
    """Mock order book update data class"""
    def __init__(self, symbol, timestamp, asks, bids):
        self.symbol = symbol
        self.timestamp = timestamp
        self.asks = asks  # List of (price, quantity) tuples
        self.bids = bids  # List of (price, quantity) tuples

class TradeUpdate:
    """Mock trade update data class"""
    def __init__(self, symbol, timestamp, price, quantity, side, trade_id):
        self.symbol = symbol
        self.timestamp = timestamp
        self.price = price
        self.quantity = quantity
        self.side = side
        self.trade_id = trade_id

class BarUpdate:
    """Mock bar update data class"""
    def __init__(self, symbol, timestamp, interval, open, high, low, close, volume):
        self.symbol = symbol
        self.timestamp = timestamp
        self.interval = interval
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume

class InstrumentInfo:
    """Mock instrument info data class"""
    def __init__(self, symbol, base_currency, quote_currency, price_precision, quantity_precision, min_quantity):
        self.symbol = symbol
        self.base_currency = base_currency
        self.quote_currency = quote_currency
        self.price_precision = price_precision
        self.quantity_precision = quantity_precision
        self.min_quantity = min_quantity


class MockMarketDataSubsystem(Component):
    """
    Mock market data subsystem for integration testing.
    
    This component simulates a market data provider with controllable behaviors
    for testing system responses to various market conditions.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the mock market data subsystem.
        
        Args:
            config: Configuration settings
        """
        super().__init__("mock_market_data")
        self.config = config or {}
        
        # Market data state
        self.instruments = {
            "BTC/USDT": {
                "base_price": 40000.0,
                "current_price": 40000.0,
                "volatility": 0.002,  # 0.2% volatility per step
                "symbol": "BTC/USDT",
                "base_currency": "BTC",
                "quote_currency": "USDT",
                "min_quantity": 0.001,
                "max_quantity": 100.0,
                "price_decimals": 2,
                "quantity_decimals": 6,
                "order_book": {
                    "asks": [(40010.0, 0.5), (40020.0, 1.0), (40030.0, 2.0)],
                    "bids": [(39990.0, 0.5), (39980.0, 1.0), (39970.0, 2.0)]
                },
                "last_trade": {
                    "price": 40000.0,
                    "quantity": 0.1,
                    "timestamp": datetime.datetime.now()
                }
            },
            "ETH/USDT": {
                "base_price": 2800.0,
                "current_price": 2800.0,
                "volatility": 0.003,  # 0.3% volatility per step
                "symbol": "ETH/USDT",
                "base_currency": "ETH",
                "quote_currency": "USDT",
                "min_quantity": 0.01,
                "max_quantity": 1000.0,
                "price_decimals": 2,
                "quantity_decimals": 4,
                "order_book": {
                    "asks": [(2801.0, 5.0), (2802.0, 10.0), (2803.0, 20.0)],
                    "bids": [(2799.0, 5.0), (2798.0, 10.0), (2797.0, 20.0)]
                },
                "last_trade": {
                    "price": 2800.0,
                    "quantity": 1.0,
                    "timestamp": datetime.datetime.now()
                }
            }
        }
        
        # Add additional symbols from config
        for symbol in self.config.get("symbols", []):
            if symbol not in self.instruments:
                # Create a new instrument with default values
                base_currency, quote_currency = symbol.split("/")
                base_price = 1000.0 if base_currency == "ETH" else 40000.0 if base_currency == "BTC" else 100.0
                
                self.instruments[symbol] = {
                    "base_price": base_price,
                    "current_price": base_price,
                    "volatility": 0.002,
                    "symbol": symbol,
                    "base_currency": base_currency,
                    "quote_currency": quote_currency,
                    "min_quantity": 0.001,
                    "max_quantity": 100.0,
                    "price_decimals": 2,
                    "quantity_decimals": 6,
                    "order_book": {
                        "asks": [(base_price * 1.001, 0.5), (base_price * 1.002, 1.0), (base_price * 1.003, 2.0)],
                        "bids": [(base_price * 0.999, 0.5), (base_price * 0.998, 1.0), (base_price * 0.997, 2.0)]
                    },
                    "last_trade": {
                        "price": base_price,
                        "quantity": 0.1,
                        "timestamp": datetime.datetime.now()
                    }
                }
        
        # Timings and control flags
        self.update_interval = self.config.get("update_interval", 1.0)  # seconds
        self.connected = True
        self.data_quality = 1.0  # 1.0 = perfect, 0.0 = completely missing
        self.latency_ms = 0  # Additional latency in milliseconds
        self.price_trend = self.config.get("price_trend", "random")  # random, up, down
        self.volatility_override = self.config.get("price_volatility", None)
        
        # Runtime variables
        self.update_task = None
        self.async_message_bus = None
        self.status = ComponentStatus.UNINITIALIZED
    
    async def initialize(self):
        """Initialize the mock market data subsystem."""
        logger.info("Initializing mock market data subsystem")
        self.async_message_bus = get_async_message_bus()
        
        # Simulate network connection delay
        await asyncio.sleep(0.1)
        
        self.status = ComponentStatus.INITIALIZED
        return self.status
    
    async def start(self):
        """Start the mock market data subsystem."""
        logger.info("Starting mock market data subsystem")
        
        # Simulate startup sequence
        await asyncio.sleep(0.1)
        
        # Start the update task
        self.update_task = asyncio.create_task(self._update_loop())
        
        self.status = ComponentStatus.OPERATIONAL
        return self.status
    
    async def stop(self):
        """Stop the mock market data subsystem."""
        logger.info("Stopping mock market data subsystem")
        
        # Cancel the update task
        if self.update_task:
            self.update_task.cancel()
            try:
                await self.update_task
            except asyncio.CancelledError:
                pass
            self.update_task = None
        
        self.status = ComponentStatus.SHUTDOWN
        return self.status
    
    async def get_status(self):
        """Get the current status of the mock market data subsystem."""
        # If not connected, return degraded status
        if not self.connected:
            return ComponentStatus.DEGRADED
        
        return self.status
    
    async def _update_loop(self):
        """Main update loop for generating market data."""
        try:
            while True:
                # Only send updates if connected
                if self.connected:
                    # Update prices
                    await self._update_prices()
                    
                    # Generate and publish market data
                    await self._publish_market_data()
                    
                    # Simulate latency if configured
                    if self.latency_ms > 0:
                        await asyncio.sleep(self.latency_ms / 1000.0)
                
                # Wait for the next update
                await asyncio.sleep(self.update_interval)
                
        except asyncio.CancelledError:
            logger.info("Market data update loop cancelled")
            raise
        except Exception as e:
            logger.error(f"Error in market data update loop: {str(e)}", exc_info=True)
            self.status = ComponentStatus.ERROR
    
    async def _update_prices(self):
        """Update instrument prices based on configured behaviors."""
        for symbol, instrument in self.instruments.items():
            # Determine volatility
            volatility = self.volatility_override if self.volatility_override is not None else instrument["volatility"]
            
            # Generate price change
            price_change = 0.0
            
            if self.price_trend == "up":
                # Upward trend
                price_change = volatility * random.uniform(0.1, 1.0)
            elif self.price_trend == "down":
                # Downward trend
                price_change = -volatility * random.uniform(0.1, 1.0)
            else:
                # Random movement
                price_change = volatility * random.normalvariate(0, 1.0)
            
            # Apply price change
            new_price = instrument["current_price"] * (1 + price_change)
            
            # Ensure price doesn't deviate too far from base price
            max_deviation = 0.3  # Maximum 30% deviation from base price
            base_price = instrument["base_price"]
            min_price = base_price * (1 - max_deviation)
            max_price = base_price * (1 + max_deviation)
            
            new_price = max(min_price, min(max_price, new_price))
            
            # Round to price precision
            new_price = round(new_price, instrument["price_decimals"])
            
            # Update current price
            instrument["current_price"] = new_price
            
            # Update order book
            self._update_order_book(symbol, new_price)
    
    def _update_order_book(self, symbol: str, price: float):
        """
        Update the order book for an instrument.
        
        Args:
            symbol: Instrument symbol
            price: Current price
        """
        instrument = self.instruments[symbol]
        
        # Update the order book with the new price
        spread = price * 0.001  # 0.1% spread
        
        # Generate new asks (higher than price)
        asks = []
        for i in range(5):
            ask_price = price + spread * (i + 1)
            ask_price = round(ask_price, instrument["price_decimals"])
            ask_quantity = random.uniform(0.1, 2.0) * (1 / (i + 1))
            ask_quantity = round(ask_quantity, instrument["quantity_decimals"])
            asks.append((ask_price, ask_quantity))
        
        # Generate new bids (lower than price)
        bids = []
        for i in range(5):
            bid_price = price - spread * (i + 1)
            bid_price = round(bid_price, instrument["price_decimals"])
            bid_quantity = random.uniform(0.1, 2.0) * (1 / (i + 1))
            bid_quantity = round(bid_quantity, instrument["quantity_decimals"])
            bids.append((bid_price, bid_quantity))
        
        # Update the order book
        instrument["order_book"] = {
            "asks": asks,
            "bids": bids
        }
    
    async def _publish_market_data(self):
        """Publish market data updates for all instruments."""
        for symbol, instrument in self.instruments.items():
            # Simulate data quality issues
            if random.random() > self.data_quality:
                continue
            
            # Publish ticker update
            await self._publish_ticker(symbol)
            
            # Randomly publish other types of data
            rand = random.random()
            
            if rand < 0.3:
                # 30% chance of order book update
                await self._publish_order_book(symbol)
            elif rand < 0.6:
                # 30% chance of trade
                await self._generate_trade(symbol)
            elif rand < 0.7:
                # 10% chance of bar update
                await self._publish_bar(symbol)
    
    async def _publish_ticker(self, symbol: str):
        """
        Publish a ticker update for an instrument.
        
        Args:
            symbol: Instrument symbol
        """
        instrument = self.instruments[symbol]
        current_price = instrument["current_price"]
        base_price = instrument["base_price"]
        
        # Create ticker data
        ticker = TickerUpdate(
            symbol=symbol,
            timestamp=datetime.datetime.now(),
            price=current_price,
            volume_24h=random.uniform(100, 1000),
            change_24h=(current_price - base_price) / base_price * 100,
            high_24h=current_price * 1.02,
            low_24h=current_price * 0.98
        )
        
        # Publish ticker update
        try:
            await self.async_message_bus.publish(
                MessageTypes.MARKET_DATA_TICKER_UPDATE,
                ticker
            )
            logger.debug(f"Published ticker for {symbol}: {current_price}")
        except Exception as e:
            logger.error(f"Error publishing ticker: {str(e)}", exc_info=True)
    
    async def _publish_order_book(self, symbol: str):
        """
        Publish an order book update for an instrument.
        
        Args:
            symbol: Instrument symbol
        """
        instrument = self.instruments[symbol]
        order_book = instrument["order_book"]
        
        # Create order book data
        order_book_update = OrderBookUpdate(
            symbol=symbol,
            timestamp=datetime.datetime.now(),
            asks=order_book["asks"],
            bids=order_book["bids"]
        )
        
        # Publish order book update
        try:
            await self.async_message_bus.publish(
                MessageTypes.MARKET_DATA_ORDERBOOK_UPDATE,
                order_book_update
            )
            logger.debug(f"Published order book for {symbol}")
        except Exception as e:
            logger.error(f"Error publishing order book: {str(e)}", exc_info=True)
    
    async def _generate_trade(self, symbol: str):
        """
        Generate a random trade for an instrument.
        
        Args:
            symbol: Instrument symbol
        """
        instrument = self.instruments[symbol]
        current_price = instrument["current_price"]
        
        # Random price around current price
        price = current_price * (1 + random.normalvariate(0, 0.0001))
        price = round(price, instrument["price_decimals"])
        
        # Random quantity
        quantity = random.uniform(
            instrument["min_quantity"],
            min(instrument["max_quantity"] * 0.01, 1.0)  # Small trades most of the time
        )
        quantity = round(quantity, instrument["quantity_decimals"])
        
        # Create trade
        trade = {
            "price": price,
            "quantity": quantity,
            "timestamp": datetime.datetime.now()
        }
        
        # Update last trade
        instrument["last_trade"] = trade
        
        # Publish trade
        await self._publish_trade(symbol, trade)
    
    async def _publish_trade(self, symbol: str, trade: Dict):
        """
        Publish a trade update for an instrument.
        
        Args:
            symbol: Instrument symbol
            trade: Trade data
        """
        # Create trade update
        trade_update = TradeUpdate(
            symbol=symbol,
            timestamp=trade["timestamp"],
            price=trade["price"],
            quantity=trade["quantity"],
            side="buy" if random.random() > 0.5 else "sell",
            trade_id=str(int(time.time() * 1000))
        )
        
        # Publish trade update
        try:
            await self.async_message_bus.publish(
                MessageTypes.MARKET_DATA_TRADE_UPDATE,
                trade_update
            )
            logger.debug(f"Published trade for {symbol}: {trade['quantity']} @ {trade['price']}")
        except Exception as e:
            logger.error(f"Error publishing trade: {str(e)}", exc_info=True)
    
    async def _publish_bar(self, symbol: str):
        """
        Publish a bar update for an instrument.
        
        Args:
            symbol: Instrument symbol
        """
        instrument = self.instruments[symbol]
        current_price = instrument["current_price"]
        
        # Create random OHLC values around current price
        open_price = current_price * (1 + random.normalvariate(0, 0.001))
        high_price = max(open_price, current_price) * (1 + abs(random.normalvariate(0, 0.001)))
        low_price = min(open_price, current_price) * (1 - abs(random.normalvariate(0, 0.001)))
        close_price = current_price
        
        # Round prices
        open_price = round(open_price, instrument["price_decimals"])
        high_price = round(high_price, instrument["price_decimals"])
        low_price = round(low_price, instrument["price_decimals"])
        close_price = round(close_price, instrument["price_decimals"])
        
        # Create bar update
        bar_update = BarUpdate(
            symbol=symbol,
            timestamp=datetime.datetime.now(),
            interval="1m",
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
            volume=random.uniform(10, 100)
        )
        
        # Publish bar update
        try:
            await self.async_message_bus.publish(
                MessageTypes.MARKET_DATA_BAR_UPDATE,
                bar_update
            )
            logger.debug(f"Published bar for {symbol}")
        except Exception as e:
            logger.error(f"Error publishing bar: {str(e)}", exc_info=True)
    
    # Simulation control methods
    
    def set_data_quality(self, quality: float):
        """
        Set the data quality.
        
        Args:
            quality: Data quality (0.0 to 1.0)
        """
        self.data_quality = max(0.0, min(1.0, quality))
        logger.info(f"Set data quality to {self.data_quality}")
    
    def set_connected(self, connected: bool):
        """
        Set the connection state.
        
        Args:
            connected: Whether the connection is active
        """
        self.connected = connected
        logger.info(f"Set connected to {connected}")
    
    def set_latency(self, latency_ms: int):
        """
        Set the latency.
        
        Args:
            latency_ms: Latency in milliseconds
        """
        self.latency_ms = max(0, latency_ms)
        logger.info(f"Set latency to {self.latency_ms} ms")
    
    def set_price_trend(self, trend: str):
        """
        Set the price trend.
        
        Args:
            trend: Price trend (random, up, down)
        """
        if trend in ["random", "up", "down"]:
            self.price_trend = trend
            logger.info(f"Set price trend to {self.price_trend}")
        else:
            logger.warning(f"Invalid price trend: {trend}")
    
    def set_update_interval(self, interval: float):
        """
        Set the update interval.
        
        Args:
            interval: Update interval in seconds
        """
        self.update_interval = max(0.1, interval)
        logger.info(f"Set update interval to {self.update_interval} seconds")
    
    def set_price(self, symbol: str, price: float):
        """
        Set the price for an instrument.
        
        Args:
            symbol: Instrument symbol
            price: Price to set
        """
        if symbol in self.instruments:
            instrument = self.instruments[symbol]
            instrument["current_price"] = price
            self._update_order_book(symbol, price)
            logger.info(f"Set price for {symbol} to {price}")
        else:
            logger.warning(f"Unknown symbol: {symbol}")
    
    def trigger_price_spike(self, symbol: str, percent: float):
        """
        Trigger a price spike for an instrument.
        
        Args:
            symbol: Instrument symbol
            percent: Percentage change
        """
        if symbol in self.instruments:
            instrument = self.instruments[symbol]
            current_price = instrument["current_price"]
            new_price = current_price * (1 + percent / 100.0)
            instrument["current_price"] = new_price
            self._update_order_book(symbol, new_price)
            logger.info(f"Triggered price spike for {symbol}: {percent}% to {new_price}")
        else:
            logger.warning(f"Unknown symbol: {symbol}")
    
    def get_current_prices(self):
        """Get current prices for all instruments."""
        return {symbol: instrument["current_price"] for symbol, instrument in self.instruments.items()}
    
    def get_instrument_info(self, symbol: str):
        """
        Get information about an instrument.
        
        Args:
            symbol: Instrument symbol
            
        Returns:
            Instrument information
        """
        if symbol in self.instruments:
            instrument = self.instruments[symbol]
            return {
                "symbol": symbol,
                "base_currency": instrument["base_currency"],
                "quote_currency": instrument["quote_currency"],
                "price_decimals": instrument["price_decimals"],
                "quantity_decimals": instrument["quantity_decimals"],
                "min_quantity": instrument["min_quantity"],
                "max_quantity": instrument["max_quantity"],
                "current_price": instrument["current_price"]
            }
        else:
            return None 