"""
Simulated Exchange Module

This module provides a simulated exchange for testing.
"""

import os
import sys
import time
import random
import asyncio
import logging
import json
from typing import Dict, Any, List, Optional, Union, Tuple
from datetime import datetime, timedelta

from trading_system.core.logging import get_logger
from trading_system.exchange.base import (
    BaseExchange, 
    Order, 
    OrderType, 
    OrderSide, 
    OrderStatus,
    Balance,
    Ticker,
    OrderBook,
    MarketInfo
)

logger = get_logger("exchange.simulated")

class SimulatedExchange(BaseExchange):
    """
    Simulated Exchange
    
    This exchange simulates trading for testing purposes.
    """
    
    def __init__(self, name: str, config: Dict[str, Any] = None):
        """
        Initialize the simulated exchange.
        
        Args:
            name: Exchange name
            config: Exchange configuration
        """
        super().__init__(name, config)
        
        # Simulation settings
        self.simulation_mode = True
        self.latency = self.config.get("latency", 100)  # ms
        self.fill_ratio = self.config.get("fill_ratio", 0.98)  # 98% chance of fill
        self.volatility = self.config.get("volatility", 0.002)  # 0.2% price volatility
        
        # Simulated state
        self._balances: Dict[str, Balance] = {}
        self._orders: Dict[str, Order] = {}
        self._pending_orders: Dict[str, Order] = {}
        self._tickers: Dict[str, Ticker] = {}
        self._order_books: Dict[str, OrderBook] = {}
        self._markets: Dict[str, MarketInfo] = {}
        
        # Initial balances
        self._init_balances()
        
        # Update loop
        self._update_task = None
        self._update_interval = self.config.get("update_interval", 1.0)  # seconds
        self._running = False
    
    async def initialize(self) -> bool:
        """
        Initialize the simulated exchange.
        
        Returns:
            Initialization success
        """
        try:
            logger.info(f"Initializing simulated exchange: {self.name}")
            
            # Initialize default markets
            self._init_markets()
            
            # Initialize tickers and order books
            self._init_tickers()
            self._init_order_books()
            
            logger.info(f"Simulated exchange {self.name} initialized with {len(self._markets)} markets")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing simulated exchange: {str(e)}", exc_info=True)
            return False
    
    async def connect(self) -> bool:
        """
        Connect to the simulated exchange.
        
        Returns:
            Connection success
        """
        try:
            # Simulate connection delay
            await asyncio.sleep(self.latency / 1000)
            
            self.connected = True
            self._running = True
            
            # Start update loop
            self._update_task = asyncio.create_task(self._update_loop())
            
            logger.info(f"Connected to simulated exchange: {self.name}")
            return True
            
        except Exception as e:
            logger.error(f"Error connecting to simulated exchange: {str(e)}", exc_info=True)
            return False
    
    async def disconnect(self) -> bool:
        """
        Disconnect from the simulated exchange.
        
        Returns:
            Disconnect success
        """
        try:
            # Stop update loop
            self._running = False
            
            if self._update_task:
                try:
                    self._update_task.cancel()
                    await self._update_task
                except asyncio.CancelledError:
                    pass
                
                self._update_task = None
            
            self.connected = False
            
            logger.info(f"Disconnected from simulated exchange: {self.name}")
            return True
            
        except Exception as e:
            logger.error(f"Error disconnecting from simulated exchange: {str(e)}", exc_info=True)
            return False
    
    async def fetch_balance(self) -> Dict[str, Balance]:
        """
        Fetch account balance.
        
        Returns:
            Account balance by asset
        """
        # Simulate latency
        await asyncio.sleep(self.latency / 1000)
        
        return dict(self._balances)
    
    async def fetch_markets(self) -> Dict[str, MarketInfo]:
        """
        Fetch markets information.
        
        Returns:
            Markets information by symbol
        """
        # Simulate latency
        await asyncio.sleep(self.latency / 1000)
        
        return dict(self._markets)
    
    async def fetch_ticker(self, symbol: str) -> Ticker:
        """
        Fetch ticker for a symbol.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Ticker information
        """
        # Simulate latency
        await asyncio.sleep(self.latency / 1000)
        
        if symbol not in self._tickers:
            raise ValueError(f"Symbol not found: {symbol}")
        
        return self._tickers[symbol]
    
    async def fetch_order_book(self, symbol: str, limit: int = 20) -> OrderBook:
        """
        Fetch order book for a symbol.
        
        Args:
            symbol: Trading symbol
            limit: Maximum number of bids/asks
            
        Returns:
            Order book
        """
        # Simulate latency
        await asyncio.sleep(self.latency / 1000)
        
        if symbol not in self._order_books:
            raise ValueError(f"Symbol not found: {symbol}")
        
        order_book = self._order_books[symbol]
        
        # Limit the number of entries
        limited_order_book = OrderBook(
            symbol=order_book.symbol,
            bids=order_book.bids[:limit],
            asks=order_book.asks[:limit],
            timestamp=int(time.time() * 1000)
        )
        
        return limited_order_book
    
    async def create_order(self, symbol: str, order_type: OrderType, side: OrderSide, 
                         amount: float, price: Optional[float] = None, 
                         params: Dict[str, Any] = None) -> Order:
        """
        Create a new order.
        
        Args:
            symbol: Trading symbol
            order_type: Order type
            side: Order side
            amount: Order amount
            price: Order price (required for limit orders)
            params: Additional parameters
            
        Returns:
            Created order
        """
        # Simulate latency
        await asyncio.sleep(self.latency / 1000)
        
        if symbol not in self._markets:
            raise ValueError(f"Symbol not found: {symbol}")
        
        # Get current price if not provided
        if price is None or order_type == OrderType.MARKET:
            price = self._tickers[symbol].last
        
        # Generate order ID
        order_id = f"sim_order_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
        
        # Create order
        order = Order(
            id=order_id,
            symbol=symbol,
            type=order_type,
            side=side,
            price=price,
            amount=amount,
            status=OrderStatus.NEW,
            timestamp=int(time.time() * 1000),
            client_order_id=params.get("client_order_id") if params else None
        )
        
        # Add to pending orders
        self._pending_orders[order_id] = order
        
        # Process market orders immediately
        if order_type == OrderType.MARKET:
            await self._process_order(order)
        
        # Add to orders
        self._orders[order_id] = order
        
        return order
    
    async def cancel_order(self, order_id: str, symbol: str, params: Dict[str, Any] = None) -> Order:
        """
        Cancel an order.
        
        Args:
            order_id: Order ID
            symbol: Trading symbol
            params: Additional parameters
            
        Returns:
            Canceled order
        """
        # Simulate latency
        await asyncio.sleep(self.latency / 1000)
        
        if order_id not in self._orders:
            raise ValueError(f"Order not found: {order_id}")
        
        order = self._orders[order_id]
        
        # Check if order can be canceled
        if order.status in [OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED]:
            return order
        
        # Cancel order
        order.status = OrderStatus.CANCELED
        order.update_timestamp = int(time.time() * 1000)
        
        # Remove from pending orders
        if order_id in self._pending_orders:
            del self._pending_orders[order_id]
        
        return order
    
    async def fetch_order(self, order_id: str, symbol: str, params: Dict[str, Any] = None) -> Order:
        """
        Fetch an order.
        
        Args:
            order_id: Order ID
            symbol: Trading symbol
            params: Additional parameters
            
        Returns:
            Order information
        """
        # Simulate latency
        await asyncio.sleep(self.latency / 1000)
        
        if order_id not in self._orders:
            raise ValueError(f"Order not found: {order_id}")
        
        return self._orders[order_id]
    
    async def fetch_open_orders(self, symbol: Optional[str] = None, 
                               params: Dict[str, Any] = None) -> List[Order]:
        """
        Fetch open orders.
        
        Args:
            symbol: Trading symbol (optional)
            params: Additional parameters
            
        Returns:
            List of open orders
        """
        # Simulate latency
        await asyncio.sleep(self.latency / 1000)
        
        # Filter by symbol and status
        open_orders = []
        for order in self._orders.values():
            if (symbol is None or order.symbol == symbol) and order.status in [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED]:
                open_orders.append(order)
        
        return open_orders
    
    def _init_balances(self) -> None:
        """Initialize default balances."""
        default_balances = {
            "BTC": Balance(
                asset="BTC",
                total=1.0,
                available=1.0
            ),
            "ETH": Balance(
                asset="ETH",
                total=10.0,
                available=10.0
            ),
            "USDT": Balance(
                asset="USDT",
                total=10000.0,
                available=10000.0
            ),
            "BNB": Balance(
                asset="BNB",
                total=100.0,
                available=100.0
            )
        }
        
        self._balances = default_balances
    
    def _init_markets(self) -> None:
        """Initialize default markets."""
        default_markets = {
            "BTCUSDT": MarketInfo(
                symbol="BTCUSDT",
                base_asset="BTC",
                quote_asset="USDT",
                min_order_size=0.001,
                max_order_size=100.0,
                tick_size=0.01,
                min_price=1000.0,
                max_price=100000.0,
                price_precision=2,
                amount_precision=6,
                status="active"
            ),
            "ETHUSDT": MarketInfo(
                symbol="ETHUSDT",
                base_asset="ETH",
                quote_asset="USDT",
                min_order_size=0.01,
                max_order_size=1000.0,
                tick_size=0.01,
                min_price=100.0,
                max_price=10000.0,
                price_precision=2,
                amount_precision=5,
                status="active"
            ),
            "ETHBTC": MarketInfo(
                symbol="ETHBTC",
                base_asset="ETH",
                quote_asset="BTC",
                min_order_size=0.01,
                max_order_size=1000.0,
                tick_size=0.00001,
                min_price=0.001,
                max_price=1.0,
                price_precision=5,
                amount_precision=5,
                status="active"
            ),
            "BNBUSDT": MarketInfo(
                symbol="BNBUSDT",
                base_asset="BNB",
                quote_asset="USDT",
                min_order_size=0.1,
                max_order_size=10000.0,
                tick_size=0.01,
                min_price=1.0,
                max_price=1000.0,
                price_precision=2,
                amount_precision=4,
                status="active"
            )
        }
        
        self._markets = default_markets
        self.markets = default_markets
    
    def _init_tickers(self) -> None:
        """Initialize default tickers."""
        default_tickers = {
            "BTCUSDT": Ticker(
                symbol="BTCUSDT",
                bid=35000.0,
                ask=35010.0,
                last=35005.0,
                volume=1000.0,
                high=36000.0,
                low=34000.0,
                open=34500.0,
                close=35005.0,
                timestamp=int(time.time() * 1000)
            ),
            "ETHUSDT": Ticker(
                symbol="ETHUSDT",
                bid=2000.0,
                ask=2001.0,
                last=2000.5,
                volume=5000.0,
                high=2100.0,
                low=1900.0,
                open=1950.0,
                close=2000.5,
                timestamp=int(time.time() * 1000)
            ),
            "ETHBTC": Ticker(
                symbol="ETHBTC",
                bid=0.06,
                ask=0.0601,
                last=0.0605,
                volume=200.0,
                high=0.062,
                low=0.058,
                open=0.059,
                close=0.0605,
                timestamp=int(time.time() * 1000)
            ),
            "BNBUSDT": Ticker(
                symbol="BNBUSDT",
                bid=300.0,
                ask=301.0,
                last=300.5,
                volume=10000.0,
                high=310.0,
                low=290.0,
                open=295.0,
                close=300.5,
                timestamp=int(time.time() * 1000)
            )
        }
        
        self._tickers = default_tickers
        self.tickers = default_tickers
    
    def _init_order_books(self) -> None:
        """Initialize default order books."""
        default_order_books = {}
        
        for symbol, ticker in self._tickers.items():
            # Generate bids (lower than last price)
            bids = []
            for i in range(20):
                price = ticker.last * (1 - 0.0001 * (i + 1))
                amount = random.uniform(0.1, 10.0)
                bids.append((price, amount))
            
            # Generate asks (higher than last price)
            asks = []
            for i in range(20):
                price = ticker.last * (1 + 0.0001 * (i + 1))
                amount = random.uniform(0.1, 10.0)
                asks.append((price, amount))
            
            # Create order book
            order_book = OrderBook(
                symbol=symbol,
                bids=bids,
                asks=asks,
                timestamp=int(time.time() * 1000)
            )
            
            default_order_books[symbol] = order_book
        
        self._order_books = default_order_books
        self.order_books = default_order_books
    
    async def _update_loop(self) -> None:
        """Update loop for simulated exchange."""
        try:
            while self._running:
                # Update tickers
                await self._update_tickers()
                
                # Update order books
                await self._update_order_books()
                
                # Process pending orders
                await self._process_pending_orders()
                
                # Wait for next update
                await asyncio.sleep(self._update_interval)
                
        except asyncio.CancelledError:
            logger.info(f"Update loop canceled for simulated exchange: {self.name}")
        except Exception as e:
            logger.error(f"Error in update loop for simulated exchange: {str(e)}", exc_info=True)
    
    async def _update_tickers(self) -> None:
        """Update tickers with simulated price movements."""
        for symbol, ticker in self._tickers.items():
            # Generate random price movement
            price_change = ticker.last * self.volatility * random.uniform(-1, 1)
            new_last = ticker.last + price_change
            
            # Ensure price is within limits
            market = self._markets[symbol]
            new_last = max(market.min_price, min(market.max_price, new_last))
            
            # Update ticker
            new_ticker = Ticker(
                symbol=symbol,
                bid=new_last * 0.9998,  # Slightly lower than last
                ask=new_last * 1.0002,  # Slightly higher than last
                last=new_last,
                volume=ticker.volume * random.uniform(0.95, 1.05),  # Random volume change
                high=max(ticker.high, new_last),
                low=min(ticker.low, new_last),
                open=ticker.open,
                close=new_last,
                timestamp=int(time.time() * 1000)
            )
            
            self._tickers[symbol] = new_ticker
    
    async def _update_order_books(self) -> None:
        """Update order books with simulated changes."""
        for symbol, order_book in self._order_books.items():
            ticker = self._tickers[symbol]
            
            # Generate new bids (lower than last price)
            bids = []
            for i in range(20):
                price = ticker.last * (1 - 0.0001 * (i + 1))
                amount = random.uniform(0.1, 10.0)
                bids.append((price, amount))
            
            # Generate new asks (higher than last price)
            asks = []
            for i in range(20):
                price = ticker.last * (1 + 0.0001 * (i + 1))
                amount = random.uniform(0.1, 10.0)
                asks.append((price, amount))
            
            # Create new order book
            new_order_book = OrderBook(
                symbol=symbol,
                bids=bids,
                asks=asks,
                timestamp=int(time.time() * 1000)
            )
            
            self._order_books[symbol] = new_order_book
    
    async def _process_pending_orders(self) -> None:
        """Process pending orders."""
        # Make a copy to avoid modification during iteration
        pending_orders = dict(self._pending_orders)
        
        for order_id, order in pending_orders.items():
            await self._process_order(order)
    
    async def _process_order(self, order: Order) -> None:
        """
        Process an order.
        
        Args:
            order: Order to process
        """
        # Skip if order is already processed
        if order.status not in [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED]:
            return
        
        # Random fill chance based on fill ratio
        should_fill = random.random() < self.fill_ratio
        
        if should_fill:
            # Get current price
            ticker = self._tickers[order.symbol]
            
            # Determine fill price
            if order.type == OrderType.MARKET:
                # Market orders fill at current price
                fill_price = ticker.last
            else:
                # Limit orders fill at limit price or better
                if order.side == OrderSide.BUY:
                    fill_price = min(ticker.ask, order.price)
                else:
                    fill_price = max(ticker.bid, order.price)
            
            # Calculate remaining amount to fill
            remaining = order.amount - order.filled
            
            # Determine fill amount
            fill_amount = remaining * random.uniform(0.1, 1.0)
            
            # Update order
            order.filled += fill_amount
            order.cost += fill_amount * fill_price
            order.fee = order.cost * 0.001  # 0.1% fee
            order.fee_asset = order.symbol.split('USDT')[0] if 'USDT' in order.symbol else "USDT"
            
            # Update status
            if order.filled >= order.amount * 0.99:  # Consider filled if >= 99% filled
                order.filled = order.amount  # Adjust to exact amount
                order.status = OrderStatus.FILLED
                
                # Remove from pending orders
                if order.id in self._pending_orders:
                    del self._pending_orders[order.id]
            else:
                order.status = OrderStatus.PARTIALLY_FILLED
            
            order.update_timestamp = int(time.time() * 1000)
            
            # Update balances
            await self._update_balances(order, fill_amount, fill_price)
        
        # Random reject chance
        elif random.random() < 0.01:  # 1% chance of rejection
            order.status = OrderStatus.REJECTED
            order.update_timestamp = int(time.time() * 1000)
            
            # Remove from pending orders
            if order.id in self._pending_orders:
                del self._pending_orders[order.id]
    
    async def _update_balances(self, order: Order, fill_amount: float, fill_price: float) -> None:
        """
        Update balances after order execution.
        
        Args:
            order: Executed order
            fill_amount: Amount filled
            fill_price: Fill price
        """
        # Get base and quote assets
        market = self._markets[order.symbol]
        base_asset = market.base_asset
        quote_asset = market.quote_asset
        
        # Calculate total cost
        cost = fill_amount * fill_price
        
        # Update balances
        if order.side == OrderSide.BUY:
            # Reduce quote asset (e.g., USDT)
            if quote_asset in self._balances:
                self._balances[quote_asset].total -= cost
                self._balances[quote_asset].available -= cost
            
            # Increase base asset (e.g., BTC)
            if base_asset in self._balances:
                self._balances[base_asset].total += fill_amount
                self._balances[base_asset].available += fill_amount
            else:
                self._balances[base_asset] = Balance(
                    asset=base_asset,
                    total=fill_amount,
                    available=fill_amount
                )
        else:  # SELL
            # Reduce base asset (e.g., BTC)
            if base_asset in self._balances:
                self._balances[base_asset].total -= fill_amount
                self._balances[base_asset].available -= fill_amount
            
            # Increase quote asset (e.g., USDT)
            if quote_asset in self._balances:
                self._balances[quote_asset].total += cost
                self._balances[quote_asset].available += cost
            else:
                self._balances[quote_asset] = Balance(
                    asset=quote_asset,
                    total=cost,
                    available=cost
                ) 