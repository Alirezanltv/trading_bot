"""
Simple Trading Pipeline

A streamlined version of the trading pipeline for testing and demonstration.
"""

import os
import sys
import time
import json
import uuid
import logging
import threading
import asyncio
import random
from datetime import datetime
from enum import Enum
from typing import Dict, List, Any, Optional, Set, Tuple
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

# Load environment variables
load_dotenv()

logger = logging.getLogger("simple_pipeline")

# Define enums and types
class OrderSide(Enum):
    BUY = "buy"
    SELL = "sell"

class OrderType(Enum):
    MARKET = "market"
    LIMIT = "limit"

class OrderStatus(Enum):
    CREATED = "created"
    SUBMITTED = "submitted"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"

class PositionType(Enum):
    LONG = "long"
    SHORT = "short"

class PositionStatus(Enum):
    OPEN = "open"
    PARTIALLY_CLOSED = "partially_closed"
    CLOSED = "closed"

class SignalType(Enum):
    BUY = "buy"
    SELL = "sell"
    EXIT = "exit"
    NEUTRAL = "neutral"

# Simple position class
class Position:
    def __init__(self, position_id: str, symbol: str, position_type: PositionType, 
                 entry_price: float, quantity: float, strategy_id: str):
        self.position_id = position_id
        self.symbol = symbol
        self.position_type = position_type
        self.entry_price = entry_price
        self.quantity = quantity
        self.remaining_quantity = quantity
        self.initial_quantity = quantity
        self.strategy_id = strategy_id
        self.status = PositionStatus.OPEN
        self.entry_orders = []
        self.exit_orders = []
        self.exit_price = None
        self.realized_pnl = 0.0
        self.created_at = time.time()
        self.closed_at = None
        
    def add_entry_order(self, order_id: str):
        self.entry_orders.append(order_id)
        
    def add_exit_order(self, order_id: str):
        self.exit_orders.append(order_id)
        
    def update_exit(self, price: float, quantity: float):
        # Calculate realized P&L
        if self.position_type == PositionType.LONG:
            pnl = (price - self.entry_price) * quantity
        else:
            pnl = (self.entry_price - price) * quantity
            
        self.realized_pnl += pnl
        self.remaining_quantity -= quantity
        
        # Update exit price (weighted average if multiple exits)
        if self.exit_price is None:
            self.exit_price = price
        else:
            closed_quantity = self.initial_quantity - self.remaining_quantity - quantity
            self.exit_price = ((closed_quantity * self.exit_price) + (quantity * price)) / (closed_quantity + quantity)
        
        # Update status
        if self.remaining_quantity <= 0:
            self.status = PositionStatus.CLOSED
            self.closed_at = time.time()
        elif self.remaining_quantity < self.initial_quantity:
            self.status = PositionStatus.PARTIALLY_CLOSED

# Simple order class
class Order:
    def __init__(self, order_id: str, symbol: str, side: OrderSide, order_type: OrderType,
                 quantity: float, price: Optional[float] = None, position_id: Optional[str] = None,
                 strategy_id: Optional[str] = None):
        self.order_id = order_id
        self.symbol = symbol
        self.side = side
        self.order_type = order_type
        self.quantity = quantity
        self.price = price
        self.position_id = position_id
        self.strategy_id = strategy_id
        self.status = OrderStatus.CREATED
        self.created_at = time.time()
        self.updated_at = time.time()
        self.filled_at = None
        self.executed_quantity = 0.0
        self.fill_price = None
        
    def update_status(self, status: OrderStatus):
        self.status = status
        self.updated_at = time.time()
        
        if status == OrderStatus.FILLED and self.filled_at is None:
            self.filled_at = time.time()

# Simple mock market data
class MockMarketData:
    def __init__(self):
        self.prices = {
            "btc-usdt": 60000.0,
            "eth-usdt": 3000.0
        }
        self.logger = logging.getLogger("mock_market")
        
    def get_price(self, symbol: str) -> float:
        # Add small random price movement (±1%)
        current_price = self.prices.get(symbol, 1000.0)
        new_price = current_price * (1 + random.uniform(-0.01, 0.01))
        self.prices[symbol] = new_price
        return new_price
    
    def get_candle(self, symbol: str) -> Dict[str, Any]:
        price = self.get_price(symbol)
        return {
            "timestamp": int(time.time()),
            "open": price * 0.995,
            "high": price * 1.01,
            "low": price * 0.99,
            "close": price,
            "volume": random.uniform(10, 100)
        }

# Simple RSI strategy
class SimpleRSIStrategy:
    def __init__(self):
        self.prices = {}
        self.rsi_values = {}
        self.rsi_period = 14
        self.overbought = 70
        self.oversold = 30
        self.logger = logging.getLogger("rsi_strategy")
        
    def update(self, symbol: str, price: float):
        # Initialize price history for this symbol if needed
        if symbol not in self.prices:
            self.prices[symbol] = []
            self.rsi_values[symbol] = []
        
        # Add price to history
        self.prices[symbol].append(price)
        
        # Keep only the necessary history
        max_history = max(100, self.rsi_period * 3)
        if len(self.prices[symbol]) > max_history:
            self.prices[symbol] = self.prices[symbol][-max_history:]
            
        # Calculate RSI if we have enough data
        if len(self.prices[symbol]) > self.rsi_period:
            rsi = self._calculate_rsi(self.prices[symbol])
            self.rsi_values[symbol].append(rsi)
            
            # Limit RSI history
            if len(self.rsi_values[symbol]) > max_history:
                self.rsi_values[symbol] = self.rsi_values[symbol][-max_history:]
    
    def _calculate_rsi(self, prices: List[float]) -> float:
        # Calculate price changes
        deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
        
        # Separate gains and losses
        gains = [delta if delta > 0 else 0 for delta in deltas]
        losses = [-delta if delta < 0 else 0 for delta in deltas]
        
        # Calculate average gain and loss
        avg_gain = sum(gains[-self.rsi_period:]) / self.rsi_period
        avg_loss = sum(losses[-self.rsi_period:]) / self.rsi_period
        
        # Calculate RS and RSI
        if avg_loss == 0:
            return 100.0
            
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    def get_signal(self, symbol: str) -> Tuple[SignalType, float]:
        # Default signal
        if symbol not in self.rsi_values or not self.rsi_values[symbol]:
            return SignalType.NEUTRAL, 0.0
            
        # Get current RSI
        rsi = self.rsi_values[symbol][-1]
        
        # Generate signal based on RSI
        if rsi <= self.oversold:
            return SignalType.BUY, rsi
        elif rsi >= self.overbought:
            return SignalType.SELL, rsi
        
        return SignalType.NEUTRAL, rsi

# Simple mock exchange
class MockExchange:
    def __init__(self):
        self.orders = {}
        self.balances = {
            "btc": 1.0,
            "eth": 10.0,
            "usdt": 50000.0
        }
        self.prices = {
            "btc-usdt": 60000.0,
            "eth-usdt": 3000.0
        }
        self.logger = logging.getLogger("mock_exchange")
        
    def submit_order(self, order: Order) -> bool:
        self.logger.info(f"Submitting order: {order.symbol} {order.side.value} {order.quantity}")
        
        # Simulate execution delay
        time.sleep(random.uniform(0.1, 0.3))
        
        # Validate order
        if order.quantity <= 0:
            self.logger.warning(f"Invalid quantity: {order.quantity}")
            order.update_status(OrderStatus.REJECTED)
            return False
            
        # For market orders, execute immediately
        if order.order_type == OrderType.MARKET:
            # Get current price
            price = self.prices.get(order.symbol, 50000.0)
            
            # Add random slippage
            execution_price = price * (1 + random.uniform(-0.002, 0.002))
            
            # Update order
            order.fill_price = execution_price
            order.executed_quantity = order.quantity
            order.update_status(OrderStatus.FILLED)
            
            # Update balances
            base_currency, quote_currency = order.symbol.split("-")
            
            if order.side == OrderSide.BUY:
                # Buying base with quote
                self.balances[base_currency] = self.balances.get(base_currency, 0) + order.quantity
                self.balances[quote_currency] = self.balances.get(quote_currency, 0) - (order.quantity * execution_price)
            else:
                # Selling base for quote
                self.balances[base_currency] = self.balances.get(base_currency, 0) - order.quantity
                self.balances[quote_currency] = self.balances.get(quote_currency, 0) + (order.quantity * execution_price)
                
            self.logger.info(f"Order executed: {order.symbol} {order.side.value} {order.quantity} @ {execution_price}")
            
            # Store order
            self.orders[order.order_id] = order
            return True
        
        # For limit orders, just store and mark as submitted
        order.update_status(OrderStatus.SUBMITTED)
        self.orders[order.order_id] = order
        
        self.logger.info(f"Limit order submitted: {order.symbol} {order.side.value} {order.quantity} @ {order.price}")
        return True

# Simple position manager
class SimplePositionManager:
    def __init__(self):
        self.positions = {}
        self.open_positions = set()
        self.closed_positions = set()
        self.logger = logging.getLogger("position_manager")
        
    def create_position(self, symbol: str, position_type: PositionType, entry_price: float, 
                        quantity: float, strategy_id: str) -> str:
        position_id = str(uuid.uuid4())
        
        position = Position(
            position_id=position_id,
            symbol=symbol,
            position_type=position_type,
            entry_price=entry_price,
            quantity=quantity,
            strategy_id=strategy_id
        )
        
        self.positions[position_id] = position
        self.open_positions.add(position_id)
        
        self.logger.info(f"Created {position_type.value} position {position_id} for {symbol}: {quantity} @ {entry_price}")
        
        return position_id
    
    def process_order(self, order: Order):
        # Check if order belongs to a position
        if not order.position_id or order.position_id not in self.positions:
            self.logger.warning(f"Order {order.order_id} not linked to a valid position")
            return
            
        # Get position
        position = self.positions[order.position_id]
        
        # Check if order is filled
        if order.status != OrderStatus.FILLED:
            return
            
        # Process entry or exit
        is_entry = (order.side == OrderSide.BUY and position.position_type == PositionType.LONG) or \
                  (order.side == OrderSide.SELL and position.position_type == PositionType.SHORT)
                  
        if is_entry:
            position.add_entry_order(order.order_id)
        else:
            # Process exit order
            position.add_exit_order(order.order_id)
            position.update_exit(order.fill_price, order.executed_quantity)
            
            # Update position status
            if position.status == PositionStatus.CLOSED:
                self.open_positions.discard(position.position_id)
                self.closed_positions.add(position.position_id)
                
                self.logger.info(f"Closed {position.position_type.value} position {position.position_id} for {position.symbol}: {position.quantity} @ {position.exit_price} PnL: {position.realized_pnl}")
    
    def get_position(self, position_id: str) -> Optional[Position]:
        return self.positions.get(position_id)
        
    def get_positions_by_symbol(self, symbol: str) -> List[Position]:
        return [p for p in self.positions.values() if p.symbol == symbol]
    
    def get_open_positions_by_symbol(self, symbol: str) -> List[Position]:
        return [p for p in self.positions.values() 
                if p.symbol == symbol and p.position_id in self.open_positions]

# Simplified trading pipeline
class SimpleTradingPipeline:
    def __init__(self, config_path: Optional[str] = None):
        self.logger = logging.getLogger("simple_pipeline")
        
        # Load configuration
        self.config = self._load_config(config_path)
        
        # Initialize components
        self.market_data = MockMarketData()
        self.strategy = SimpleRSIStrategy()
        
        # Determine which exchange to use
        exchange_type = self.config.get("exchange", {}).get("type", "mock")
        
        if exchange_type.lower() == "nobitex":
            try:
                self.logger.info("Initializing Nobitex exchange")
                from nobitex_api import NobitexAPIClient
                
                # Get API credentials
                api_key = self.config.get("exchange", {}).get("api_key") or os.getenv("NOBITEX_API_KEY")
                
                if not api_key:
                    self.logger.warning("No Nobitex API key found, falling back to mock exchange")
                    self.exchange = MockExchange()
                else:
                    self.exchange = NobitexExchange(api_key=api_key)
                    self.logger.info("Nobitex exchange initialized successfully")
            except Exception as e:
                self.logger.error(f"Failed to initialize Nobitex exchange: {str(e)}")
                self.logger.warning("Falling back to mock exchange")
                self.exchange = MockExchange()
        else:
            self.logger.info("Using mock exchange")
            self.exchange = MockExchange()
        
        self.position_manager = SimplePositionManager()
        
        # Monitored symbols
        self.symbols = self.config.get("symbols", ["btc-usdt", "eth-usdt"])
        
        # Pipeline state
        self.running = False
        self.thread = None
        
        # Dry run mode (simulated execution)
        self.dry_run = self.config.get("dry_run", True)
        if self.dry_run:
            self.logger.warning("Running in DRY RUN mode - orders will not be executed on exchange")
        
        self.logger.info("Simple trading pipeline initialized")
    
    def _load_config(self, config_path: Optional[str]) -> Dict[str, Any]:
        if not config_path:
            # Use default configuration
            return {
                "symbols": ["btc-usdt", "eth-usdt"],
                "update_interval": 5,  # seconds
                "position_size": {
                    "btc-usdt": 0.001,  # Reduced for safety
                    "eth-usdt": 0.01    # Reduced for safety
                },
                "risk_per_trade": 0.01,  # 1% of balance
                "dry_run": True,  # Default to dry run for safety
                "exchange": {
                    "type": "nobitex",
                    "api_key": None  # Will use from environment if not set
                }
            }
        
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Error loading configuration: {str(e)}")
            return {}
    
    def start(self):
        if self.running:
            self.logger.warning("Pipeline already running")
            return
        
        self.logger.info("Starting trading pipeline")
        
        # Start pipeline thread
        self.running = True
        self.thread = threading.Thread(target=self._run_pipeline, daemon=True)
        self.thread.start()
        
        self.logger.info("Trading pipeline started")
    
    def stop(self):
        if not self.running:
            return
        
        self.logger.info("Stopping trading pipeline")
        self.running = False
        
        if self.thread:
            self.thread.join(timeout=5.0)
        
        self.logger.info("Trading pipeline stopped")
    
    def _run_pipeline(self):
        self.logger.info("Pipeline processing thread started")
        
        update_interval = self.config.get("update_interval", 5)
        
        while self.running:
            try:
                # Process each monitored symbol
                for symbol in self.symbols:
                    self._process_symbol(symbol)
                
                # Wait for next update
                time.sleep(update_interval)
                
            except Exception as e:
                self.logger.error(f"Error in pipeline processing: {str(e)}")
                time.sleep(1)  # Avoid tight loop on error
    
    def _process_symbol(self, symbol: str):
        try:
            # 1. Get market data
            price = self.market_data.get_price(symbol)
            candle = self.market_data.get_candle(symbol)
            
            # 2. Update strategy with market data
            self.strategy.update(symbol, price)
            
            # 3. Get signal from strategy
            signal_type, rsi = self.strategy.get_signal(symbol)
            
            # 4. Process signal if actionable
            if signal_type != SignalType.NEUTRAL:
                self.logger.info(f"Signal generated for {symbol}: {signal_type.value} (RSI: {rsi:.2f})")
                self._process_signal(symbol, signal_type, price, rsi)
                
        except Exception as e:
            self.logger.error(f"Error processing symbol {symbol}: {str(e)}")
    
    def _process_signal(self, symbol: str, signal_type: SignalType, price: float, rsi: float):
        # Get active positions for this symbol
        positions = self.position_manager.get_open_positions_by_symbol(symbol)
        
        if signal_type == SignalType.BUY:
            # If we already have an open long position, don't open another
            if any(p.position_type == PositionType.LONG for p in positions):
                self.logger.info(f"Long position already exists for {symbol}, skipping")
                return
            
            # Calculate position size
            quantity = self._calculate_position_size(symbol, price)
            
            # Create position
            position_id = self.position_manager.create_position(
                symbol=symbol,
                position_type=PositionType.LONG,
                entry_price=price,
                quantity=quantity,
                strategy_id="SimpleRSI"
            )
            
            # Create and submit order
            order_id = str(uuid.uuid4())
            order = Order(
                order_id=order_id,
                symbol=symbol,
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=quantity,
                position_id=position_id,
                strategy_id="SimpleRSI"
            )
            
            # Submit order
            if self.exchange.submit_order(order):
                # Process filled order
                self.position_manager.process_order(order)
            
        elif signal_type == SignalType.SELL:
            # If we already have an open short position, don't open another
            if any(p.position_type == PositionType.SHORT for p in positions):
                self.logger.info(f"Short position already exists for {symbol}, skipping")
                return
            
            # Calculate position size
            quantity = self._calculate_position_size(symbol, price)
            
            # Create position
            position_id = self.position_manager.create_position(
                symbol=symbol,
                position_type=PositionType.SHORT,
                entry_price=price,
                quantity=quantity,
                strategy_id="SimpleRSI"
            )
            
            # Create and submit order
            order_id = str(uuid.uuid4())
            order = Order(
                order_id=order_id,
                symbol=symbol,
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,
                quantity=quantity,
                position_id=position_id,
                strategy_id="SimpleRSI"
            )
            
            # Submit order
            if self.exchange.submit_order(order):
                # Process filled order
                self.position_manager.process_order(order)
            
        elif signal_type == SignalType.EXIT:
            # Close all positions for this symbol
            for position in positions:
                self._close_position(position, price)
    
    def _close_position(self, position: Position, price: float):
        # Determine order side
        side = OrderSide.SELL if position.position_type == PositionType.LONG else OrderSide.BUY
        
        # Create exit order
        order_id = str(uuid.uuid4())
        order = Order(
            order_id=order_id,
            symbol=position.symbol,
            side=side,
            order_type=OrderType.MARKET,
            quantity=position.remaining_quantity,
            position_id=position.position_id,
            strategy_id=position.strategy_id
        )
        
        # Submit order
        if self.exchange.submit_order(order):
            # Process filled order
            self.position_manager.process_order(order)
    
    def _calculate_position_size(self, symbol: str, price: float) -> float:
        # Use configured position sizes
        position_sizes = self.config.get("position_size", {})
        base_size = position_sizes.get(symbol, 0.01)
        
        # Different symbols may have different position sizes
        if "eth" in symbol.lower() and symbol not in position_sizes:
            base_size = 0.1  # Higher quantity for ETH
            
        return round(base_size, 8)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get pipeline statistics."""
        open_positions = {}
        for pos_id in self.position_manager.open_positions:
            pos = self.position_manager.get_position(pos_id)
            if pos:
                open_positions[pos_id] = {
                    "symbol": pos.symbol,
                    "type": pos.position_type.value,
                    "entry_price": pos.entry_price,
                    "quantity": pos.quantity,
                    "current_price": self.market_data.get_price(pos.symbol),
                    "unrealized_pnl": self._calculate_unrealized_pnl(pos)
                }
                
        closed_positions = {}
        for pos_id in self.position_manager.closed_positions:
            pos = self.position_manager.get_position(pos_id)
            if pos:
                closed_positions[pos_id] = {
                    "symbol": pos.symbol,
                    "type": pos.position_type.value,
                    "entry_price": pos.entry_price,
                    "exit_price": pos.exit_price,
                    "quantity": pos.quantity,
                    "realized_pnl": pos.realized_pnl
                }
                
        return {
            "open_positions": open_positions,
            "closed_positions": closed_positions,
            "balances": self.exchange.balances,
            "prices": {symbol: self.market_data.get_price(symbol) for symbol in self.symbols}
        }
    
    def _calculate_unrealized_pnl(self, position: Position) -> float:
        current_price = self.market_data.get_price(position.symbol)
        
        if position.position_type == PositionType.LONG:
            return (current_price - position.entry_price) * position.remaining_quantity
        else:
            return (position.entry_price - current_price) * position.remaining_quantity

# Nobitex exchange implementation
class NobitexExchange:
    def __init__(self, api_key=None, cache_dir="./cache"):
        self.logger = logging.getLogger("nobitex_exchange")
        
        # Initialize Nobitex API client
        from nobitex_api import NobitexAPIClient
        self.client = NobitexAPIClient(api_key=api_key, cache_dir=cache_dir)
        
        # Cache market data
        self.prices = {}
        self.balances = {}
        self.last_balance_update = 0
        self.balance_cache_time = 60  # 60 seconds
        self.orders = {}
        
        # Initialize balances
        self._update_balances()
        
        self.logger.info("Nobitex exchange initialized")
    
    def _update_balances(self):
        try:
            # Only update every minute to avoid excessive API calls
            current_time = time.time()
            if current_time - self.last_balance_update < self.balance_cache_time:
                return
            
            wallet_data = self.client.get_wallet()
            
            if wallet_data and "wallets" in wallet_data:
                self.balances = {}
                for wallet in wallet_data.get("wallets", []):
                    currency = wallet.get("currency", "").lower()
                    balance = float(wallet.get("balance", 0))
                    self.balances[currency] = balance
                
                self.last_balance_update = current_time
                self.logger.info(f"Updated balances: {self.balances}")
        except Exception as e:
            self.logger.error(f"Error updating balances: {str(e)}")
    
    def get_price(self, symbol: str) -> float:
        try:
            # Convert from standardized format (btc-usdt) to Nobitex format (BTCUSDT)
            nobitex_symbol = self._format_symbol(symbol)
            
            # Get market stats
            stats = self.client.get_market_stats()
            
            if stats and "stats" in stats and nobitex_symbol in stats["stats"]:
                price = float(stats["stats"][nobitex_symbol].get("latest", 0))
                self.prices[symbol] = price
                return price
            
            # Fallback to cached price or default
            return self.prices.get(symbol, 50000.0 if "btc" in symbol.lower() else 3000.0)
        except Exception as e:
            self.logger.error(f"Error getting price for {symbol}: {str(e)}")
            return self.prices.get(symbol, 50000.0 if "btc" in symbol.lower() else 3000.0)
    
    def _format_symbol(self, symbol: str) -> str:
        """Convert from standardized format (btc-usdt) to Nobitex format (BTCUSDT)"""
        parts = symbol.split('-')
        if len(parts) != 2:
            return symbol.upper()
        
        return f"{parts[0].upper()}{parts[1].upper()}"
    
    def submit_order(self, order: Order) -> bool:
        self.logger.info(f"Submitting order: {order.symbol} {order.side.value} {order.quantity}")
        
        try:
            # Update balances before submitting order
            self._update_balances()
            
            # Validate order
            if order.quantity <= 0:
                self.logger.warning(f"Invalid quantity: {order.quantity}")
                order.update_status(OrderStatus.REJECTED)
                return False
            
            # Check if we have enough balance
            base_currency, quote_currency = order.symbol.split("-")
            
            current_price = self.get_price(order.symbol)
            
            if order.side == OrderSide.BUY:
                required_quote = order.quantity * current_price
                if quote_currency not in self.balances or self.balances[quote_currency] < required_quote:
                    self.logger.warning(f"Insufficient {quote_currency} balance for buy: {self.balances.get(quote_currency, 0)} < {required_quote}")
                    order.update_status(OrderStatus.REJECTED)
                    return False
            else:  # SELL
                if base_currency not in self.balances or self.balances[base_currency] < order.quantity:
                    self.logger.warning(f"Insufficient {base_currency} balance for sell: {self.balances.get(base_currency, 0)} < {order.quantity}")
                    order.update_status(OrderStatus.REJECTED)
                    return False
            
            # Prepare order parameters
            order_type = "buy" if order.side == OrderSide.BUY else "sell"
            nobitex_symbol = self._format_symbol(order.symbol)
            execution_price = current_price
            
            # For market orders, execute immediately on Nobitex
            if order.order_type == OrderType.MARKET:
                # Place the order
                order_result = self.client.place_order(
                    symbol=nobitex_symbol,
                    type=order_type,
                    amount=order.quantity,
                    price=execution_price if order.order_type == OrderType.LIMIT else None
                )
                
                if not order_result or "status" not in order_result or order_result["status"] != "ok":
                    error_message = order_result.get("message", "Unknown error") if order_result else "Failed to place order"
                    self.logger.error(f"Order submission failed: {error_message}")
                    order.update_status(OrderStatus.REJECTED)
                    return False
                
                # Get the order ID
                order_id = order_result.get("order", {}).get("id")
                
                if not order_id:
                    self.logger.error("Order submitted but no order ID received")
                    order.update_status(OrderStatus.REJECTED)
                    return False
                
                # Store order and mark as filled (since we're treating market orders as immediately filled)
                order.fill_price = execution_price
                order.executed_quantity = order.quantity
                order.update_status(OrderStatus.FILLED)
                self.orders[order.order_id] = order
                
                # Update balances after order execution
                if order.side == OrderSide.BUY:
                    # Buying base with quote
                    self.balances[base_currency] = self.balances.get(base_currency, 0) + order.quantity
                    self.balances[quote_currency] = self.balances.get(quote_currency, 0) - (order.quantity * execution_price)
                else:
                    # Selling base for quote
                    self.balances[base_currency] = self.balances.get(base_currency, 0) - order.quantity
                    self.balances[quote_currency] = self.balances.get(quote_currency, 0) + (order.quantity * execution_price)
                
                self.logger.info(f"Order executed: {order.symbol} {order.side.value} {order.quantity} @ {execution_price}")
                
                return True
            
            # For limit orders, submit and mark as pending
            if order.order_type == OrderType.LIMIT and order.price is not None:
                # Place the order
                order_result = self.client.place_order(
                    symbol=nobitex_symbol,
                    type=order_type,
                    amount=order.quantity,
                    price=order.price
                )
                
                if not order_result or "status" not in order_result or order_result["status"] != "ok":
                    error_message = order_result.get("message", "Unknown error") if order_result else "Failed to place order"
                    self.logger.error(f"Limit order submission failed: {error_message}")
                    order.update_status(OrderStatus.REJECTED)
                    return False
                
                # Store order info
                order.update_status(OrderStatus.SUBMITTED)
                self.orders[order.order_id] = order
                
                self.logger.info(f"Limit order submitted: {order.symbol} {order.side.value} {order.quantity} @ {order.price}")
                
                return True
            
            self.logger.error(f"Unsupported order type: {order.order_type}")
            order.update_status(OrderStatus.REJECTED)
            return False
            
        except Exception as e:
            self.logger.error(f"Error submitting order: {str(e)}")
            order.update_status(OrderStatus.REJECTED)
            return False

def main():
    """Run the simple trading pipeline."""
    # Create and start pipeline
    pipeline = SimpleTradingPipeline()
    
    try:
        pipeline.start()
        
        # Run until keyboard interrupt
        print("Pipeline running... Press Ctrl+C to stop")
        print("Monitoring the following symbols:", ", ".join(pipeline.symbols))
        
        while True:
            try:
                cmd = input("\nEnter 'status' for current state, 'positions' for open positions, or 'q' to quit: ")
                
                if cmd.lower() == 'q':
                    break
                elif cmd.lower() == 'status':
                    stats = pipeline.get_stats()
                    print("\nCurrent Status:")
                    print(f"Prices: {stats['prices']}")
                    print(f"Balances: {stats['balances']}")
                    print(f"Open positions: {len(stats['open_positions'])}")
                    print(f"Closed positions: {len(stats['closed_positions'])}")
                elif cmd.lower() == 'positions':
                    stats = pipeline.get_stats()
                    
                    if not stats['open_positions']:
                        print("No open positions")
                    else:
                        print(f"\nOpen positions ({len(stats['open_positions'])}):")
                        for pos_id, pos in stats['open_positions'].items():
                            print(f"  {pos['symbol']} {pos['type']}: {pos['quantity']} @ {pos['entry_price']} | PnL: {pos['unrealized_pnl']:.2f}")
                    
                    if stats['closed_positions']:
                        print(f"\nClosed positions ({len(stats['closed_positions'])}):")
                        for pos_id, pos in stats['closed_positions'].items():
                            print(f"  {pos['symbol']} {pos['type']}: {pos['quantity']} @ {pos['entry_price']} -> {pos['exit_price']} | PnL: {pos['realized_pnl']:.2f}")
                else:
                    print("Unknown command")
                    
            except EOFError:
                break
            
    except KeyboardInterrupt:
        print("\nStopping pipeline...")
    finally:
        pipeline.stop()
        print("Pipeline stopped")

if __name__ == "__main__":
    main() 