"""
High-Performance Order Engine Bridge

This module provides a bridge between the Python trading system and the
high-performance Rust order matching engine.

It handles:
1. Initialization of the Rust engine
2. Type conversion between Python and Rust
3. Error handling and recovery
4. Performance monitoring
"""

import time
import logging
import asyncio
import threading
import uuid
from typing import Dict, List, Any, Optional, Tuple, Union
from decimal import Decimal
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from trading_system.core.component import Component, ComponentStatus
from trading_system.core.logging import get_logger
from trading_system.execution.orders import Order, OrderFill, OrderSide, OrderType, OrderStatus

# Set up logging
logger = get_logger("high_performance.order_engine")

# Attempt to import the Rust module
try:
    import order_engine
    RUST_ENGINE_AVAILABLE = True
    logger.info(f"Rust order engine loaded successfully (version: {order_engine.version()})")
except ImportError:
    RUST_ENGINE_AVAILABLE = False
    logger.warning("Rust order engine not available, using Python fallback")


class HighPerformanceOrderEngine(Component):
    """
    High-performance order matching engine using Rust.
    
    This class provides a bridge between the Python trading system and the
    Rust-implemented order matching engine, with fallback to a Python
    implementation if the Rust module is not available.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the high-performance order engine.
        
        Args:
            config: Configuration dictionary
        """
        super().__init__(name="HighPerformanceOrderEngine")
        
        self.config = config or {}
        self.order_books: Dict[str, Any] = {}  # Symbol -> OrderBook
        self.thread_pool = ThreadPoolExecutor(
            max_workers=self.config.get("max_workers", 4),
            thread_name_prefix="order_engine_"
        )
        
        # Performance metrics
        self.perf_metrics = {
            "orders_processed": 0,
            "fills_generated": 0,
            "processing_time_ns": 0,
            "avg_processing_time_ns": 0,
        }
        
        # Lock for thread safety
        self._lock = threading.RLock()
        
        # Python fallback implementation for when Rust is not available
        self._py_order_books = {}
        
    async def initialize(self) -> bool:
        """
        Initialize the order engine.
        
        Returns:
            bool: Initialization success
        """
        try:
            self._status = ComponentStatus.INITIALIZING
            logger.info("Initializing high-performance order engine")
            
            if not RUST_ENGINE_AVAILABLE:
                logger.warning("Using Python fallback implementation")
            
            # Initialize any symbols specified in config
            symbols = self.config.get("symbols", [])
            for symbol in symbols:
                self.get_or_create_order_book(symbol)
            
            self._status = ComponentStatus.INITIALIZED
            logger.info(f"High-performance order engine initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing order engine: {str(e)}", exc_info=True)
            self._status = ComponentStatus.ERROR
            return False
    
    async def start(self) -> bool:
        """
        Start the order engine.
        
        Returns:
            bool: Success flag
        """
        self._status = ComponentStatus.OPERATIONAL
        logger.info("High-performance order engine started")
        return True
    
    async def stop(self) -> bool:
        """
        Stop the order engine.
        
        Returns:
            bool: Success flag
        """
        self.thread_pool.shutdown(wait=True)
        self._status = ComponentStatus.STOPPED
        logger.info("High-performance order engine stopped")
        return True
    
    def get_or_create_order_book(self, symbol: str) -> Any:
        """
        Get or create an order book for a symbol.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            OrderBook: Order book instance
        """
        with self._lock:
            if symbol not in self.order_books:
                if RUST_ENGINE_AVAILABLE:
                    self.order_books[symbol] = order_engine.create_order_book(symbol)
                else:
                    # Python fallback
                    self._py_order_books[symbol] = PythonOrderBook(symbol)
                    self.order_books[symbol] = self._py_order_books[symbol]
                
                logger.info(f"Created order book for {symbol}")
            
            return self.order_books[symbol]
    
    async def process_order(self, order: Order) -> List[OrderFill]:
        """
        Process an order in the high-performance engine.
        
        Args:
            order: Order to process
            
        Returns:
            List[OrderFill]: List of fills generated
        """
        start_time = time.time_ns()
        
        # Get the order book
        order_book = self.get_or_create_order_book(order.symbol)
        
        # Convert Python order to Rust format
        if RUST_ENGINE_AVAILABLE:
            # Create a Rust order
            rust_order = self._convert_to_rust_order(order)
            
            # Use thread pool to not block the event loop
            fills_list = await asyncio.get_event_loop().run_in_executor(
                self.thread_pool,
                lambda: order_book.add_order(rust_order)
            )
            
            # Convert fills back to Python
            fills = [self._convert_from_rust_fill(fill, order) for fill in fills_list]
        else:
            # Use Python fallback
            fills = await self._py_order_books[order.symbol].add_order(order)
        
        # Update performance metrics
        end_time = time.time_ns()
        processing_time = end_time - start_time
        
        with self._lock:
            self.perf_metrics["orders_processed"] += 1
            self.perf_metrics["fills_generated"] += len(fills)
            self.perf_metrics["processing_time_ns"] += processing_time
            
            if self.perf_metrics["orders_processed"] > 0:
                self.perf_metrics["avg_processing_time_ns"] = (
                    self.perf_metrics["processing_time_ns"] / 
                    self.perf_metrics["orders_processed"]
                )
        
        logger.debug(f"Processed order {order.order_id} in {processing_time/1000:.2f} μs, generated {len(fills)} fills")
        return fills
    
    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        """
        Cancel an order.
        
        Args:
            order_id: Order ID
            symbol: Trading symbol
            
        Returns:
            bool: Cancel success
        """
        # Get the order book
        if symbol not in self.order_books:
            logger.warning(f"Cannot cancel order {order_id}, no order book for {symbol}")
            return False
        
        order_book = self.order_books[symbol]
        
        # Cancel the order
        if RUST_ENGINE_AVAILABLE:
            result = await asyncio.get_event_loop().run_in_executor(
                self.thread_pool,
                lambda: order_book.cancel_order(order_id)
            )
        else:
            # Use Python fallback
            result = await self._py_order_books[symbol].cancel_order(order_id)
        
        logger.debug(f"Canceled order {order_id}: {result}")
        return result
    
    async def get_order_book_depth(self, symbol: str, levels: int = 10) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        """
        Get order book depth.
        
        Args:
            symbol: Trading symbol
            levels: Number of price levels to return
            
        Returns:
            Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]: (bids, asks)
        """
        # Get the order book
        if symbol not in self.order_books:
            logger.warning(f"No order book for {symbol}")
            return ([], [])
        
        order_book = self.order_books[symbol]
        
        # Get book state
        if RUST_ENGINE_AVAILABLE:
            bids, asks = await asyncio.get_event_loop().run_in_executor(
                self.thread_pool,
                order_book.get_book_state
            )
        else:
            # Use Python fallback
            bids, asks = await self._py_order_books[symbol].get_book_state()
        
        # Limit to requested levels
        return (bids[:levels], asks[:levels])
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """
        Get performance metrics.
        
        Returns:
            Dict[str, Any]: Performance metrics
        """
        with self._lock:
            metrics = self.perf_metrics.copy()
        
        # Convert nanoseconds to microseconds for readability
        if "avg_processing_time_ns" in metrics:
            metrics["avg_processing_time_us"] = metrics["avg_processing_time_ns"] / 1000
        
        return metrics
    
    def _convert_to_rust_order(self, order: Order) -> Any:
        """
        Convert a Python Order to a Rust Order.
        
        Args:
            order: Python Order
            
        Returns:
            Any: Rust Order
        """
        # Create a new Rust order
        side = "Buy" if order.side == OrderSide.BUY else "Sell"
        order_type = "Market" if order.order_type == OrderType.MARKET else "Limit"
        
        rust_order = order_engine.Order(
            symbol=order.symbol,
            side=side,
            order_type=order_type,
            quantity=float(order.quantity),
            price=float(order.price) if order.price is not None else None
        )
        
        return rust_order
    
    def _convert_from_rust_fill(self, rust_fill: Any, original_order: Order) -> OrderFill:
        """
        Convert a Rust OrderFill to a Python OrderFill.
        
        Args:
            rust_fill: Rust OrderFill
            original_order: Original Python Order
            
        Returns:
            OrderFill: Python OrderFill
        """
        # Create a Python OrderFill from the Rust fill
        fill = OrderFill(
            fill_id=rust_fill.id,
            timestamp=rust_fill.timestamp / 1000,  # Convert from ms to seconds
            price=rust_fill.price,
            quantity=rust_fill.quantity,
            fee=0.0,  # Fee would be calculated elsewhere
            fee_asset=None
        )
        
        return fill


class PythonOrderBook:
    """
    Python implementation of order book for fallback.
    
    This is a simplified version used when the Rust engine is not available.
    """
    
    def __init__(self, symbol: str):
        """
        Initialize the order book.
        
        Args:
            symbol: Trading symbol
        """
        self.symbol = symbol
        self.buy_orders = []  # List of (price, order) sorted by price desc
        self.sell_orders = []  # List of (price, order) sorted by price asc
        self.orders = {}  # order_id -> Order
        self.last_price = None
        self.last_updated = time.time()
    
    async def add_order(self, order: Order) -> List[OrderFill]:
        """
        Add an order to the book.
        
        Args:
            order: Order to add
            
        Returns:
            List[OrderFill]: Generated fills
        """
        fills = []
        
        # Store the order
        self.orders[order.order_id] = order
        
        # Handle market orders immediately
        if order.order_type == OrderType.MARKET:
            return await self._match_market_order(order)
        
        # Add to the appropriate order list
        if order.side == OrderSide.BUY:
            # Insert maintaining price-time priority (price desc)
            price = float(order.price)
            i = 0
            while i < len(self.buy_orders) and self.buy_orders[i][0] > price:
                i += 1
            self.buy_orders.insert(i, (price, order))
        else:
            # Insert maintaining price-time priority (price asc)
            price = float(order.price)
            i = 0
            while i < len(self.sell_orders) and self.sell_orders[i][0] < price:
                i += 1
            self.sell_orders.insert(i, (price, order))
        
        # Try to match orders
        fills = await self._match_orders()
        
        self.last_updated = time.time()
        return fills
    
    async def cancel_order(self, order_id: str) -> bool:
        """
        Cancel an order.
        
        Args:
            order_id: Order ID
            
        Returns:
            bool: Cancel success
        """
        if order_id not in self.orders:
            return False
        
        order = self.orders[order_id]
        order.status = OrderStatus.CANCELED
        
        # Remove from order lists
        if order.side == OrderSide.BUY:
            self.buy_orders = [(p, o) for p, o in self.buy_orders 
                             if o.order_id != order_id]
        else:
            self.sell_orders = [(p, o) for p, o in self.sell_orders 
                              if o.order_id != order_id]
        
        self.last_updated = time.time()
        return True
    
    async def get_book_state(self) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        """
        Get the current order book state.
        
        Returns:
            Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]: (bids, asks)
        """
        # Aggregate by price level
        bids = {}
        asks = {}
        
        for price, order in self.buy_orders:
            if order.status != OrderStatus.CANCELED:
                remaining = order.quantity - order.executed_quantity
                if remaining > 0:
                    if price not in bids:
                        bids[price] = 0
                    bids[price] += remaining
        
        for price, order in self.sell_orders:
            if order.status != OrderStatus.CANCELED:
                remaining = order.quantity - order.executed_quantity
                if remaining > 0:
                    if price not in asks:
                        asks[price] = 0
                    asks[price] += remaining
        
        # Convert to lists
        bid_list = [(price, qty) for price, qty in bids.items()]
        ask_list = [(price, qty) for price, qty in asks.items()]
        
        # Sort by price
        bid_list.sort(key=lambda x: x[0], reverse=True)  # Descending
        ask_list.sort(key=lambda x: x[0])  # Ascending
        
        return (bid_list, ask_list)
    
    async def _match_market_order(self, order: Order) -> List[OrderFill]:
        """
        Match a market order against the book.
        
        Args:
            order: Market order
            
        Returns:
            List[OrderFill]: Generated fills
        """
        fills = []
        remaining = order.quantity
        
        if order.side == OrderSide.BUY:
            # Match against sell orders
            for i, (price, sell_order) in enumerate(self.sell_orders):
                if remaining <= 0:
                    break
                    
                if sell_order.status == OrderStatus.CANCELED:
                    continue
                
                # Calculate fill quantity
                sell_remaining = sell_order.quantity - sell_order.executed_quantity
                fill_qty = min(remaining, sell_remaining)
                
                if fill_qty <= 0:
                    continue
                
                # Create fill
                fill = OrderFill(
                    fill_id=str(uuid.uuid4()),
                    timestamp=time.time(),
                    price=price,
                    quantity=fill_qty,
                    fee=0.0,
                    fee_asset=None
                )
                
                # Update order quantities
                order.executed_quantity += fill_qty
                sell_order.executed_quantity += fill_qty
                
                # Update statuses
                if order.executed_quantity >= order.quantity:
                    order.status = OrderStatus.FILLED
                else:
                    order.status = OrderStatus.PARTIALLY_FILLED
                    
                if sell_order.executed_quantity >= sell_order.quantity:
                    sell_order.status = OrderStatus.FILLED
                else:
                    sell_order.status = OrderStatus.PARTIALLY_FILLED
                
                # Add fill
                fills.append(fill)
                
                # Update remaining
                remaining -= fill_qty
                
                # Update last price
                self.last_price = price
        else:
            # Match against buy orders
            for i, (price, buy_order) in enumerate(self.buy_orders):
                if remaining <= 0:
                    break
                    
                if buy_order.status == OrderStatus.CANCELED:
                    continue
                
                # Calculate fill quantity
                buy_remaining = buy_order.quantity - buy_order.executed_quantity
                fill_qty = min(remaining, buy_remaining)
                
                if fill_qty <= 0:
                    continue
                
                # Create fill
                fill = OrderFill(
                    fill_id=str(uuid.uuid4()),
                    timestamp=time.time(),
                    price=price,
                    quantity=fill_qty,
                    fee=0.0,
                    fee_asset=None
                )
                
                # Update order quantities
                order.executed_quantity += fill_qty
                buy_order.executed_quantity += fill_qty
                
                # Update statuses
                if order.executed_quantity >= order.quantity:
                    order.status = OrderStatus.FILLED
                else:
                    order.status = OrderStatus.PARTIALLY_FILLED
                    
                if buy_order.executed_quantity >= buy_order.quantity:
                    buy_order.status = OrderStatus.FILLED
                else:
                    buy_order.status = OrderStatus.PARTIALLY_FILLED
                
                # Add fill
                fills.append(fill)
                
                # Update remaining
                remaining -= fill_qty
                
                # Update last price
                self.last_price = price
        
        # Clean up filled orders
        self.buy_orders = [(p, o) for p, o in self.buy_orders 
                         if o.status != OrderStatus.FILLED]
        self.sell_orders = [(p, o) for p, o in self.sell_orders 
                          if o.status != OrderStatus.FILLED]
        
        self.last_updated = time.time()
        return fills
    
    async def _match_orders(self) -> List[OrderFill]:
        """
        Match resting orders in the book.
        
        Returns:
            List[OrderFill]: Generated fills
        """
        fills = []
        
        # While we have matching orders
        while self.buy_orders and self.sell_orders:
            # Get the best prices
            best_bid = self.buy_orders[0][0] if self.buy_orders else 0
            best_ask = self.sell_orders[0][0] if self.sell_orders else float('inf')
            
            # Check if they cross
            if best_bid < best_ask:
                break
            
            # Get the orders
            _, buy_order = self.buy_orders[0]
            _, sell_order = self.sell_orders[0]
            
            # Skip canceled orders
            if buy_order.status == OrderStatus.CANCELED:
                self.buy_orders.pop(0)
                continue
                
            if sell_order.status == OrderStatus.CANCELED:
                self.sell_orders.pop(0)
                continue
            
            # Calculate fill quantity
            buy_remaining = buy_order.quantity - buy_order.executed_quantity
            sell_remaining = sell_order.quantity - sell_order.executed_quantity
            fill_qty = min(buy_remaining, sell_remaining)
            
            if fill_qty <= 0:
                break
            
            # Create fill
            fill = OrderFill(
                fill_id=str(uuid.uuid4()),
                timestamp=time.time(),
                price=best_ask,  # Use seller's price
                quantity=fill_qty,
                fee=0.0,
                fee_asset=None
            )
            
            # Update order quantities
            buy_order.executed_quantity += fill_qty
            sell_order.executed_quantity += fill_qty
            
            # Update statuses
            if buy_order.executed_quantity >= buy_order.quantity:
                buy_order.status = OrderStatus.FILLED
                self.buy_orders.pop(0)
            else:
                buy_order.status = OrderStatus.PARTIALLY_FILLED
                
            if sell_order.executed_quantity >= sell_order.quantity:
                sell_order.status = OrderStatus.FILLED
                self.sell_orders.pop(0)
            else:
                sell_order.status = OrderStatus.PARTIALLY_FILLED
            
            # Add fill
            fills.append(fill)
            
            # Update last price
            self.last_price = best_ask
        
        self.last_updated = time.time()
        return fills


def get_high_performance_order_engine(config: Dict[str, Any] = None) -> HighPerformanceOrderEngine:
    """
    Get the high-performance order engine instance.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        HighPerformanceOrderEngine: Engine instance
    """
    return HighPerformanceOrderEngine(config) 