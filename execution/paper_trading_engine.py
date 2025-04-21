"""
Paper Trading Execution Engine

This module implements a simulated execution engine for paper trading.
It follows the same interface as the real execution engine but simulates order
execution without actually placing orders on the exchange.
"""

import os
import time
import json
import random
import logging
import threading
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from enum import Enum
from typing import Dict, List, Optional, Any, Set, Callable, Union
from uuid import uuid4

from trading_system.core.component import Component, ComponentStatus
from trading_system.core.logging import get_logger
from trading_system.core.message_bus import MessageBus, MessageTypes
from trading_system.execution.orders import Order, OrderStatus, OrderSide, OrderType, OrderTimeInForce, OrderFill


class PaperOrderResult:
    """Simulated order execution result."""
    
    def __init__(self, order_id: str, success: bool = True):
        self.order_id = order_id
        self.success = success
        self.status = OrderStatus.NEW
        self.fills = []
        self.executed_quantity = 0.0
        self.executed_price = 0.0
        self.avg_price = 0.0
        self.commission = 0.0
        self.commission_asset = ""
        self.execution_time_ms = 0
        self.error = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "order_id": self.order_id,
            "success": self.success,
            "status": self.status.value if isinstance(self.status, OrderStatus) else self.status,
            "executed_quantity": self.executed_quantity,
            "executed_price": self.executed_price,
            "avg_price": self.avg_price,
            "commission": self.commission,
            "commission_asset": self.commission_asset,
            "execution_time_ms": self.execution_time_ms,
            "error": self.error,
            "fills": [f.to_dict() if hasattr(f, "to_dict") else f for f in self.fills]
        }


class PaperTradingEngine(Component):
    """
    Paper Trading Execution Engine
    
    This engine simulates order execution without actually placing orders on the exchange.
    It follows the same interface as the real execution engine but uses simulated prices
    and fills to update virtual positions and balances.
    
    Features:
    - Simulates order execution latency and fills
    - Calculates fees based on real exchange rates
    - Handles partial fills and slippage
    - Logs all simulated transactions
    - Maintains virtual balance and positions
    """
    
    def __init__(self, config: Dict[str, Any], message_bus: Optional[MessageBus] = None):
        """
        Initialize the paper trading execution engine.
        
        Args:
            config: Configuration dictionary
            message_bus: Optional message bus for publishing events
        """
        super().__init__("paper_trading_engine")
        
        # Set up logger
        self.logger = get_logger("execution.paper")
        
        # Store configuration
        self.config = config
        self.message_bus = message_bus
        
        # Virtual balance and positions
        self.virtual_balance = config.get("virtual_balance", {
            "USDT": 10000.0,
            "IRT": 5000000.0,
            "BTC": 0.1
        })
        
        # Order tracking
        self.orders: Dict[str, Order] = {}
        self.active_orders: Set[str] = set()
        self.execution_lock = threading.Lock()
        
        # Simulation parameters
        sim_config = config.get("simulation", {})
        self.min_execution_delay_ms = sim_config.get("min_execution_delay_ms", 50)
        self.max_execution_delay_ms = sim_config.get("max_execution_delay_ms", 500)
        self.partial_fill_probability = sim_config.get("partial_fill_probability", 0.2)
        self.rejection_probability = sim_config.get("rejection_probability", 0.02)
        self.slippage_range = sim_config.get("slippage_range", (-0.001, 0.001))  # -0.1% to +0.1%
        self.commission_rate = sim_config.get("commission_rate", 0.001)  # 0.1%
        
        # Market data client for price lookup (will be initialized later)
        self.market_data_client = None
        
        # Last known prices
        self.last_prices = {}
        
        # Data storage
        self.data_dir = Path(config.get("data_dir", "data/paper_trading"))
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Session management
        self.session_id = config.get("session_id", f"paper_{int(time.time())}")
        self.session_dir = self.data_dir / self.session_id
        self.session_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize order file
        self.orders_file = self.session_dir / "orders.jsonl"
        self.positions_file = self.session_dir / "positions.jsonl"
        self.balance_file = self.session_dir / "balance.jsonl"
        
        # Save initial balance
        self._log_balance_update("INITIAL", {})
        
        # Order processor task
        self.order_processor_running = False
        self.order_processor_task = None
        
        # Statistics
        self.stats = {
            "total_orders": 0,
            "successful_orders": 0,
            "failed_orders": 0,
            "partial_fills": 0,
            "execution_times_ms": [],
            "avg_execution_time_ms": 0
        }
        
        self.logger.info("Paper Trading Engine initialized")
    
    async def initialize(self) -> bool:
        """
        Initialize the paper trading engine.
        
        Returns:
            Initialization success
        """
        try:
            self.logger.info("Initializing Paper Trading Engine")
            
            # Update component status
            self._status = ComponentStatus.INITIALIZING
            
            # Subscribe to market data updates if message bus is available
            if self.message_bus:
                await self.message_bus.subscribe(
                    topic=MessageTypes.MARKET_DATA_UPDATE,
                    callback=self._handle_market_data_update
                )
                self.logger.info("Subscribed to market data updates")
            
            # Save initial configuration
            config_file = self.session_dir / "config.json"
            with open(config_file, "w") as f:
                json.dump(self.config, f, indent=2)
            
            self._status = ComponentStatus.INITIALIZED
            self.logger.info("Paper Trading Engine initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Paper Trading Engine: {str(e)}", exc_info=True)
            self._status = ComponentStatus.ERROR
            return False
    
    async def start(self) -> bool:
        """
        Start the paper trading engine.
        
        Returns:
            Start success
        """
        try:
            if self._status != ComponentStatus.INITIALIZED:
                self.logger.error("Cannot start: Engine not initialized")
                return False
                
            self.logger.info("Starting Paper Trading Engine")
            
            # Start order processor
            self.order_processor_running = True
            self.order_processor_task = asyncio.create_task(self._process_orders())
            
            self._status = ComponentStatus.OPERATIONAL
            self.logger.info("Paper Trading Engine started successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to start Paper Trading Engine: {str(e)}", exc_info=True)
            self._status = ComponentStatus.ERROR
            return False
    
    async def stop(self) -> bool:
        """
        Stop the paper trading engine.
        
        Returns:
            Stop success
        """
        try:
            self.logger.info("Stopping Paper Trading Engine")
            
            # Stop order processor
            self.order_processor_running = False
            if self.order_processor_task:
                try:
                    await asyncio.wait_for(self.order_processor_task, timeout=5.0)
                except asyncio.TimeoutError:
                    self.logger.warning("Order processor task did not stop within timeout")
            
            # Update statistics
            if self.stats["execution_times_ms"]:
                self.stats["avg_execution_time_ms"] = (
                    sum(self.stats["execution_times_ms"]) / len(self.stats["execution_times_ms"])
                )
            
            # Log final statistics
            self.logger.info(f"Paper Trading Engine statistics: {json.dumps(self.stats, indent=2)}")
            
            # Save final balance
            self._log_balance_update("FINAL", {})
            
            self._status = ComponentStatus.SHUTDOWN
            self.logger.info("Paper Trading Engine stopped successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to stop Paper Trading Engine: {str(e)}", exc_info=True)
            self._status = ComponentStatus.ERROR
            return False
    
    async def execute_order(self, order: Order) -> PaperOrderResult:
        """
        Execute an order in paper trading mode.
        
        Args:
            order: Order to execute
            
        Returns:
            Simulated order result
        """
        order_id = order.order_id or str(uuid4())
        order.order_id = order_id
        
        self.logger.info(f"Executing paper order: {order_id} - {order.symbol} {order.side.value} {order.quantity}")
        
        # Create result
        result = PaperOrderResult(order_id, success=True)
        
        try:
            # Update statistics
            self.stats["total_orders"] += 1
            
            # Add order to tracking
            with self.execution_lock:
                self.orders[order_id] = order
                self.active_orders.add(order_id)
            
            # Log order
            self._log_order(order, "NEW")
            
            # Simulate order validation
            validation_result = await self._validate_order(order)
            
            if not validation_result["valid"]:
                result.success = False
                result.status = OrderStatus.REJECTED
                result.error = validation_result["reason"]
                self.stats["failed_orders"] += 1
                
                # Update order status
                order.status = OrderStatus.REJECTED
                
                # Log rejection
                self._log_order(order, "REJECTED", error=validation_result["reason"])
                
                # Publish update
                await self._publish_order_update(order)
                
                self.logger.warning(f"Order {order_id} rejected: {validation_result['reason']}")
                
                return result
            
            # Random chance of rejection (simulating exchange issues)
            if random.random() < self.rejection_probability:
                result.success = False
                result.status = OrderStatus.REJECTED
                result.error = "Simulated exchange rejection"
                self.stats["failed_orders"] += 1
                
                # Update order status
                order.status = OrderStatus.REJECTED
                
                # Log rejection
                self._log_order(order, "REJECTED", error="Simulated exchange rejection")
                
                # Publish update
                await self._publish_order_update(order)
                
                self.logger.warning(f"Order {order_id} rejected: Simulated exchange rejection")
                
                return result
            
            # Simulate order accepted
            order.status = OrderStatus.NEW
            
            # Publish order update
            await self._publish_order_update(order)
            
            self.logger.info(f"Paper order {order_id} accepted, waiting for execution")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error executing paper order {order_id}: {str(e)}", exc_info=True)
            
            result.success = False
            result.status = OrderStatus.FAILED
            result.error = str(e)
            
            # Update order status
            order.status = OrderStatus.FAILED
            
            # Log failure
            self._log_order(order, "FAILED", error=str(e))
            
            # Publish update
            await self._publish_order_update(order)
            
            return result
    
    async def _process_orders(self) -> None:
        """Process active orders in the background."""
        try:
            self.logger.info("Order processor started")
            
            while self.order_processor_running:
                # Process each active order
                active_order_ids = list(self.active_orders)
                
                for order_id in active_order_ids:
                    try:
                        with self.execution_lock:
                            if order_id not in self.orders:
                                self.active_orders.discard(order_id)
                                continue
                                
                            order = self.orders[order_id]
                            
                            # Skip already completed orders
                            if order.status in [OrderStatus.FILLED, OrderStatus.REJECTED, 
                                               OrderStatus.CANCELED, OrderStatus.FAILED]:
                                self.active_orders.discard(order_id)
                                continue
                        
                        # Process the order
                        await self._process_order(order)
                        
                    except Exception as e:
                        self.logger.error(f"Error processing order {order_id}: {str(e)}", exc_info=True)
                
                # Sleep before next iteration
                await asyncio.sleep(0.1)
                
        except asyncio.CancelledError:
            self.logger.info("Order processor cancelled")
        except Exception as e:
            self.logger.error(f"Order processor error: {str(e)}", exc_info=True)
    
    async def _process_order(self, order: Order) -> None:
        """
        Process a single order.
        
        Args:
            order: Order to process
        """
        # Simulate execution delay
        execution_delay_ms = random.uniform(self.min_execution_delay_ms, self.max_execution_delay_ms)
        await asyncio.sleep(execution_delay_ms / 1000)
        
        # Get current price
        price = await self._get_current_price(order.symbol)
        
        if price is None:
            self.logger.warning(f"Cannot execute order {order.order_id}: No price available for {order.symbol}")
            return
        
        # Apply slippage
        slippage = random.uniform(self.slippage_range[0], self.slippage_range[1])
        
        # For buy orders, slippage increases price; for sell orders, it decreases price
        if order.side == OrderSide.BUY:
            execution_price = price * (1 + slippage)
        else:
            execution_price = price * (1 - slippage)
        
        # Round to reasonable precision
        execution_price = round(execution_price, 8)
        
        # Check if order can be filled based on price
        can_fill = True
        
        if order.type == OrderType.LIMIT:
            if order.side == OrderSide.BUY and execution_price > order.price:
                can_fill = False
            elif order.side == OrderSide.SELL and execution_price < order.price:
                can_fill = False
        
        if not can_fill:
            # Order remains active, will be checked again later
            return
        
        # Decide whether to do partial fill
        do_partial_fill = random.random() < self.partial_fill_probability
        
        # Calculate fill quantity
        if do_partial_fill:
            # Fill between 30% and 70% of the order
            fill_ratio = random.uniform(0.3, 0.7)
            fill_quantity = order.quantity * fill_ratio
            fill_quantity = round(fill_quantity, 8)
            self.stats["partial_fills"] += 1
        else:
            fill_quantity = order.quantity
        
        # Create fill
        timestamp = datetime.now().isoformat()
        commission = fill_quantity * execution_price * self.commission_rate
        
        # Get quote currency for commission
        quote_currency = order.symbol.split('-')[1].upper() if '-' in order.symbol else "USDT"
        
        fill = OrderFill(
            timestamp=timestamp,
            quantity=fill_quantity,
            price=execution_price,
            commission=commission,
            commission_asset=quote_currency
        )
        
        # Update order
        order.add_fill(fill)
        
        # Update order status
        if order.filled_quantity >= order.quantity:
            order.status = OrderStatus.FILLED
            self.stats["successful_orders"] += 1
            
            # Remove from active orders
            with self.execution_lock:
                self.active_orders.discard(order.order_id)
        else:
            order.status = OrderStatus.PARTIALLY_FILLED
        
        # Calculate total fill value
        fill_value = fill_quantity * execution_price
        
        # Update virtual balance
        if order.side == OrderSide.BUY:
            # Deduct quote currency, add base currency
            base_currency = order.symbol.split('-')[0].upper() if '-' in order.symbol else "BTC"
            
            # Deduct quote amount + commission
            self.virtual_balance[quote_currency] = self.virtual_balance.get(quote_currency, 0) - fill_value - commission
            
            # Add base amount
            self.virtual_balance[base_currency] = self.virtual_balance.get(base_currency, 0) + fill_quantity
        else:  # SELL
            # Add quote currency, deduct base currency
            base_currency = order.symbol.split('-')[0].upper() if '-' in order.symbol else "BTC"
            
            # Add quote amount - commission
            self.virtual_balance[quote_currency] = self.virtual_balance.get(quote_currency, 0) + fill_value - commission
            
            # Deduct base amount
            self.virtual_balance[base_currency] = self.virtual_balance.get(base_currency, 0) - fill_quantity
        
        # Log updates
        self._log_order(order, order.status.value)
        self._log_fill(order, fill)
        self._log_balance_update(order.order_id, {
            "symbol": order.symbol,
            "side": order.side.value,
            "quantity": fill_quantity,
            "price": execution_price,
            "value": fill_value,
            "commission": commission
        })
        
        # Record execution time
        execution_time_ms = execution_delay_ms
        self.stats["execution_times_ms"].append(execution_time_ms)
        
        # Publish order update
        await self._publish_order_update(order)
        
        # Publish fill
        await self._publish_fill(order, fill)
        
        self.logger.info(
            f"Paper order {order.order_id} {order.status.value}: {fill_quantity} @ {execution_price} "
            f"(slippage: {slippage:.2%})"
        )
    
    async def cancel_order(self, order_id: str) -> bool:
        """
        Cancel a paper order.
        
        Args:
            order_id: ID of the order to cancel
            
        Returns:
            Cancellation success status
        """
        with self.execution_lock:
            if order_id not in self.orders:
                self.logger.warning(f"Cannot cancel order {order_id}: Order not found")
                return False
                
            order = self.orders[order_id]
            
            # Check if order can be cancelled
            if order.status in [OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.FAILED]:
                self.logger.warning(f"Cannot cancel order {order_id}: Order status is {order.status.value}")
                return False
                
            # Cancel the order
            order.status = OrderStatus.CANCELED
            
            # Remove from active orders
            self.active_orders.discard(order_id)
        
        # Log cancellation
        self._log_order(order, "CANCELED")
        
        # Publish update
        await self._publish_order_update(order)
        
        self.logger.info(f"Paper order {order_id} cancelled")
        
        return True
    
    async def get_order(self, order_id: str) -> Optional[Order]:
        """
        Get order by ID.
        
        Args:
            order_id: Order ID
            
        Returns:
            Order if found, None otherwise
        """
        with self.execution_lock:
            return self.orders.get(order_id)
    
    async def get_all_orders(self) -> List[Order]:
        """
        Get all orders.
        
        Returns:
            List of all orders
        """
        with self.execution_lock:
            return list(self.orders.values())
    
    async def get_open_orders(self) -> List[Order]:
        """
        Get all open orders.
        
        Returns:
            List of open orders
        """
        with self.execution_lock:
            return [
                order for order in self.orders.values()
                if order.status not in [OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.FAILED]
            ]
    
    async def _validate_order(self, order: Order) -> Dict[str, Any]:
        """
        Validate a paper order.
        
        Args:
            order: Order to validate
            
        Returns:
            Validation result
        """
        result = {
            "valid": True,
            "reason": None
        }
        
        # Check required fields
        if not order.symbol:
            result["valid"] = False
            result["reason"] = "Missing symbol"
            return result
        
        if not order.side:
            result["valid"] = False
            result["reason"] = "Missing side"
            return result
        
        if not order.type:
            result["valid"] = False
            result["reason"] = "Missing order type"
            return result
        
        if order.quantity <= 0:
            result["valid"] = False
            result["reason"] = "Invalid quantity"
            return result
        
        # Check if limit order has price
        if order.type == OrderType.LIMIT and (order.price is None or order.price <= 0):
            result["valid"] = False
            result["reason"] = "Limit order requires price"
            return result
        
        # Check symbol format
        if '-' not in order.symbol:
            result["valid"] = False
            result["reason"] = "Invalid symbol format"
            return result
        
        # Extract base and quote currency
        base_currency, quote_currency = order.symbol.split('-')
        base_currency = base_currency.upper()
        quote_currency = quote_currency.upper()
        
        # Check balances
        if order.side == OrderSide.BUY:
            # Check if enough quote currency
            estimated_price = order.price if order.type == OrderType.LIMIT else await self._get_current_price(order.symbol)
            
            if estimated_price is None:
                result["valid"] = False
                result["reason"] = f"Cannot estimate price for {order.symbol}"
                return result
                
            estimated_value = order.quantity * estimated_price
            estimated_commission = estimated_value * self.commission_rate
            total_required = estimated_value + estimated_commission
            
            if self.virtual_balance.get(quote_currency, 0) < total_required:
                result["valid"] = False
                result["reason"] = f"Insufficient {quote_currency} balance"
                return result
                
        else:  # SELL
            # Check if enough base currency
            if self.virtual_balance.get(base_currency, 0) < order.quantity:
                result["valid"] = False
                result["reason"] = f"Insufficient {base_currency} balance"
                return result
        
        return result
    
    async def _get_current_price(self, symbol: str) -> Optional[float]:
        """
        Get current price for a symbol.
        
        Args:
            symbol: Symbol to get price for
            
        Returns:
            Current price if available, None otherwise
        """
        # First check if we have a recent price in cache
        if symbol in self.last_prices:
            return self.last_prices[symbol]
        
        # If market data client is available, use it
        if self.market_data_client and hasattr(self.market_data_client, "get_ticker"):
            try:
                ticker = await self.market_data_client.get_ticker(symbol)
                price = ticker["price"]
                self.last_prices[symbol] = price
                return price
            except Exception as e:
                self.logger.warning(f"Error getting price for {symbol}: {str(e)}")
        
        # Default fallback values for common symbols during testing
        fallback_prices = {
            "btc-usdt": 40000.0,
            "eth-usdt": 2200.0,
            "ltc-usdt": 90.0,
            "bnb-usdt": 500.0,
            "xrp-usdt": 0.5,
            "btc-irt": 2000000000.0,
            "eth-irt": 110000000.0,
        }
        
        # Try to find a fallback price
        lower_symbol = symbol.lower()
        if lower_symbol in fallback_prices:
            price = fallback_prices[lower_symbol]
            # Add some random variation to simulate price movement
            variation = random.uniform(-0.005, 0.005)  # ±0.5%
            price = price * (1 + variation)
            self.last_prices[symbol] = price
            return price
        
        self.logger.warning(f"No price available for {symbol}")
        return None
    
    async def _handle_market_data_update(self, topic: str, message: Dict[str, Any]) -> None:
        """
        Handle market data updates.
        
        Args:
            topic: Message topic
            message: Message content
        """
        try:
            # Only process ticker updates
            if message.get("type") != "ticker":
                return
                
            symbol = message.get("symbol")
            price = message.get("price")
            
            if symbol and price:
                # Update last known price
                self.last_prices[symbol] = price
                
        except Exception as e:
            self.logger.error(f"Error handling market data update: {str(e)}", exc_info=True)
    
    async def _publish_order_update(self, order: Order) -> None:
        """
        Publish order update to message bus.
        
        Args:
            order: Updated order
        """
        if not self.message_bus:
            return
            
        try:
            message = order.to_dict()
            message["_source"] = "paper_trading_engine"
            
            await self.message_bus.publish(
                topic=MessageTypes.ORDER_UPDATE,
                message=message
            )
            
        except Exception as e:
            self.logger.error(f"Error publishing order update: {str(e)}", exc_info=True)
    
    async def _publish_fill(self, order: Order, fill: OrderFill) -> None:
        """
        Publish fill to message bus.
        
        Args:
            order: Order that was filled
            fill: Fill details
        """
        if not self.message_bus:
            return
            
        try:
            fill_message = {
                "order_id": order.order_id,
                "symbol": order.symbol,
                "side": order.side.value,
                "order_type": order.type.value,
                "fill": fill.to_dict() if hasattr(fill, "to_dict") else fill,
                "_source": "paper_trading_engine"
            }
            
            await self.message_bus.publish(
                topic=MessageTypes.FILL,
                message=fill_message
            )
            
        except Exception as e:
            self.logger.error(f"Error publishing fill: {str(e)}", exc_info=True)
    
    def _log_order(self, order: Order, status: str, error: Optional[str] = None) -> None:
        """
        Log order to orders file.
        
        Args:
            order: Order to log
            status: Current status
            error: Optional error message
        """
        try:
            log_entry = {
                "timestamp": datetime.now().isoformat(),
                "order_id": order.order_id,
                "symbol": order.symbol,
                "side": order.side.value,
                "type": order.type.value,
                "quantity": order.quantity,
                "price": order.price,
                "status": status,
                "filled_quantity": order.filled_quantity
            }
            
            if error:
                log_entry["error"] = error
                
            with open(self.orders_file, "a") as f:
                f.write(json.dumps(log_entry) + "\n")
                
        except Exception as e:
            self.logger.error(f"Error logging order: {str(e)}", exc_info=True)
    
    def _log_fill(self, order: Order, fill: OrderFill) -> None:
        """
        Log fill to orders file.
        
        Args:
            order: Order that was filled
            fill: Fill details
        """
        try:
            log_entry = {
                "timestamp": datetime.now().isoformat(),
                "order_id": order.order_id,
                "symbol": order.symbol,
                "side": order.side.value,
                "fill_time": fill.timestamp,
                "fill_quantity": fill.quantity,
                "fill_price": fill.price,
                "commission": fill.commission,
                "commission_asset": fill.commission_asset
            }
                
            with open(self.orders_file, "a") as f:
                f.write(json.dumps(log_entry) + "\n")
                
        except Exception as e:
            self.logger.error(f"Error logging fill: {str(e)}", exc_info=True)
    
    def _log_balance_update(self, order_id: str, transaction_details: Dict[str, Any]) -> None:
        """
        Log balance update to balance file.
        
        Args:
            order_id: Order ID that caused the update (or special values like "INITIAL", "FINAL")
            transaction_details: Details of the transaction
        """
        try:
            log_entry = {
                "timestamp": datetime.now().isoformat(),
                "order_id": order_id,
                "transaction": transaction_details,
                "balance": {k: v for k, v in self.virtual_balance.items()}
            }
                
            with open(self.balance_file, "a") as f:
                f.write(json.dumps(log_entry) + "\n")
                
        except Exception as e:
            self.logger.error(f"Error logging balance: {str(e)}", exc_info=True)
    
    def get_virtual_balance(self) -> Dict[str, float]:
        """
        Get current virtual balance.
        
        Returns:
            Current virtual balance
        """
        return {k: v for k, v in self.virtual_balance.items()}
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get engine statistics.
        
        Returns:
            Engine statistics
        """
        # Calculate average execution time if available
        if self.stats["execution_times_ms"]:
            self.stats["avg_execution_time_ms"] = (
                sum(self.stats["execution_times_ms"]) / len(self.stats["execution_times_ms"])
            )
            
        return {k: v for k, v in self.stats.items()} 