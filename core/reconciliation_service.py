"""
Trade Reconciliation Service

This module provides a service for reconciling trades and positions between the trading system
and exchange. It works with the transaction verification system to ensure data consistency
and identify discrepancies.
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Dict, List, Optional, Set, Tuple, Any

from trading_system.core.transaction_verification import (
    TransactionVerifier, VerificationStatus, VerificationOutcome
)


class ReconciliationStatus(Enum):
    """Status of a reconciliation run."""
    PENDING = auto()
    IN_PROGRESS = auto()
    COMPLETED = auto()
    FAILED = auto()


class ReconciliationPriority(Enum):
    """Priority levels for reconciliation tasks."""
    LOW = auto()
    MEDIUM = auto()
    HIGH = auto()
    CRITICAL = auto()


class ReconciliationService:
    """
    Service for reconciling trades and positions between the system and exchange.
    
    This service performs periodic reconciliation of:
    - Open orders
    - Recent trades
    - Account balances
    - Current positions
    
    It detects and resolves discrepancies to ensure data consistency.
    """
    
    def __init__(self, 
                 execution_subsystem,
                 market_data_subsystem,
                 position_manager,
                 transaction_verifier: TransactionVerifier,
                 reconciliation_interval: int = 60,  # seconds
                 balance_threshold: float = 0.001,  # 0.1% threshold for balance discrepancies
                 position_threshold: float = 0.001,  # 0.1% threshold for position discrepancies
                 max_retries: int = 3):
        """
        Initialize the reconciliation service.
        
        Args:
            execution_subsystem: The system's execution subsystem
            market_data_subsystem: The market data subsystem
            position_manager: The position management subsystem
            transaction_verifier: Transaction verification service
            reconciliation_interval: Interval in seconds between reconciliation runs
            balance_threshold: Threshold for flagging balance discrepancies (as fraction)
            position_threshold: Threshold for flagging position discrepancies (as fraction)
            max_retries: Maximum number of retries for failed reconciliation tasks
        """
        self.execution = execution_subsystem
        self.market_data = market_data_subsystem
        self.position_manager = position_manager
        self.transaction_verifier = transaction_verifier
        self.reconciliation_interval = reconciliation_interval
        self.balance_threshold = balance_threshold
        self.position_threshold = position_threshold
        self.max_retries = max_retries
        
        self.logger = logging.getLogger(__name__)
        self._reconciliation_task = None
        self._running = False
        self._last_reconciliation = None
        self._reconciliation_stats = {
            "total_runs": 0,
            "successful_runs": 0,
            "failed_runs": 0,
            "discrepancies_found": 0,
            "discrepancies_resolved": 0,
        }
        
        # Pending reconciliation tasks queue
        self._pending_tasks = []
        
    async def initialize(self):
        """Initialize the reconciliation service."""
        self.logger.info("Initializing reconciliation service")
        # Initialize any resources needed
        self._last_reconciliation = None
        self._running = False
    
    async def start(self):
        """Start the reconciliation service."""
        if self._running:
            self.logger.warning("Reconciliation service already running")
            return
            
        self.logger.info("Starting reconciliation service")
        self._running = True
        self._reconciliation_task = asyncio.create_task(self._reconciliation_loop())
        
    async def stop(self):
        """Stop the reconciliation service."""
        if not self._running:
            self.logger.warning("Reconciliation service not running")
            return
            
        self.logger.info("Stopping reconciliation service")
        self._running = False
        if self._reconciliation_task:
            self._reconciliation_task.cancel()
            try:
                await self._reconciliation_task
            except asyncio.CancelledError:
                pass
            self._reconciliation_task = None
    
    async def _reconciliation_loop(self):
        """Main reconciliation loop that runs periodically."""
        self.logger.info("Reconciliation loop started")
        
        while self._running:
            try:
                self.logger.debug("Starting reconciliation run")
                await self._perform_reconciliation()
                self._last_reconciliation = datetime.now()
                self._reconciliation_stats["total_runs"] += 1
                self._reconciliation_stats["successful_runs"] += 1
                
                # Process any pending high-priority reconciliation tasks
                await self._process_pending_tasks()
                
                # Wait until next scheduled reconciliation
                await asyncio.sleep(self.reconciliation_interval)
                
            except Exception as e:
                self.logger.error(f"Error in reconciliation loop: {str(e)}", exc_info=True)
                self._reconciliation_stats["failed_runs"] += 1
                # Shorter sleep on failure
                await asyncio.sleep(max(5, self.reconciliation_interval // 4))
    
    async def _perform_reconciliation(self):
        """Perform a full reconciliation of all data."""
        start_time = time.time()
        self.logger.info("Starting full reconciliation")
        
        # Step 1: Reconcile open orders
        await self._reconcile_open_orders()
        
        # Step 2: Reconcile recent trades
        await self._reconcile_recent_trades()
        
        # Step 3: Reconcile account balances
        await self._reconcile_balances()
        
        # Step 4: Reconcile positions
        await self._reconcile_positions()
        
        duration = time.time() - start_time
        self.logger.info(f"Reconciliation completed in {duration:.2f} seconds")
    
    async def _reconcile_open_orders(self):
        """Reconcile open orders between system and exchange."""
        self.logger.debug("Reconciling open orders")
        
        # Get open orders from the system's perspective
        system_orders = await self.execution.get_open_orders()
        
        # Get open orders directly from the exchange
        exchange_orders = await self.execution.get_exchange_open_orders()
        
        # Build sets of order IDs for comparison
        system_order_ids = {order.order_id for order in system_orders}
        exchange_order_ids = {order['id'] for order in exchange_orders}
        
        # Find discrepancies
        missing_from_system = exchange_order_ids - system_order_ids
        missing_from_exchange = system_order_ids - exchange_order_ids
        
        if missing_from_system:
            self.logger.warning(f"Found {len(missing_from_system)} orders on exchange but not in system")
            self._reconciliation_stats["discrepancies_found"] += len(missing_from_system)
            await self._handle_missing_system_orders(missing_from_system, exchange_orders)
        
        if missing_from_exchange:
            self.logger.warning(f"Found {len(missing_from_exchange)} orders in system but not on exchange")
            self._reconciliation_stats["discrepancies_found"] += len(missing_from_exchange)
            await self._handle_missing_exchange_orders(missing_from_exchange, system_orders)
    
    async def _reconcile_recent_trades(self):
        """Reconcile recent trades between system and exchange."""
        self.logger.debug("Reconciling recent trades")
        
        # Get recent trades from system (last 24 hours)
        since_time = datetime.now() - timedelta(days=1)
        system_trades = await self.execution.get_recent_fills(since=since_time)
        
        # Get recent trades from exchange
        exchange_trades = await self.execution.get_exchange_recent_trades(since=since_time)
        
        # Compare trades
        system_trade_ids = {trade.fill_id for trade in system_trades}
        exchange_trade_ids = {trade['id'] for trade in exchange_trades}
        
        missing_from_system = exchange_trade_ids - system_trade_ids
        
        if missing_from_system:
            self.logger.warning(f"Found {len(missing_from_system)} trades on exchange but not in system")
            self._reconciliation_stats["discrepancies_found"] += len(missing_from_system)
            await self._handle_missing_trades(missing_from_system, exchange_trades)
    
    async def _reconcile_balances(self):
        """Reconcile account balances between system and exchange."""
        self.logger.debug("Reconciling account balances")
        
        # Get balances from system
        system_balances = await self.position_manager.get_account_balances()
        
        # Get balances from exchange
        exchange_balances = await self.execution.get_exchange_balances()
        
        # Compare balances with threshold for floating point differences
        discrepancies = []
        
        for asset, system_amount in system_balances.items():
            if asset in exchange_balances:
                exchange_amount = exchange_balances[asset]
                # Check if difference exceeds threshold
                if system_amount == 0:
                    if exchange_amount != 0:
                        discrepancies.append((asset, system_amount, exchange_amount))
                elif abs((system_amount - exchange_amount) / system_amount) > self.balance_threshold:
                    discrepancies.append((asset, system_amount, exchange_amount))
            elif system_amount > 0:
                # Asset in system but not on exchange with non-zero balance
                discrepancies.append((asset, system_amount, 0))
        
        # Check for assets on exchange but not in system
        for asset, exchange_amount in exchange_balances.items():
            if asset not in system_balances and exchange_amount > 0:
                discrepancies.append((asset, 0, exchange_amount))
        
        if discrepancies:
            self.logger.warning(f"Found {len(discrepancies)} balance discrepancies")
            self._reconciliation_stats["discrepancies_found"] += len(discrepancies)
            await self._handle_balance_discrepancies(discrepancies)
    
    async def _reconcile_positions(self):
        """Reconcile positions between system and exchange."""
        self.logger.debug("Reconciling positions")
        
        # Get positions from system
        system_positions = await self.position_manager.get_all_positions()
        
        # Get positions from exchange
        exchange_positions = await self.execution.get_exchange_positions()
        
        # Compare positions
        discrepancies = []
        
        for symbol, system_pos in system_positions.items():
            system_size = system_pos.size
            
            if symbol in exchange_positions:
                exchange_size = exchange_positions[symbol]['size']
                # Check if difference exceeds threshold
                if system_size == 0:
                    if exchange_size != 0:
                        discrepancies.append((symbol, system_size, exchange_size))
                elif abs((system_size - exchange_size) / system_size) > self.position_threshold:
                    discrepancies.append((symbol, system_size, exchange_size))
            elif system_size != 0:
                # Position in system but not on exchange with non-zero size
                discrepancies.append((symbol, system_size, 0))
        
        # Check for positions on exchange but not in system
        for symbol, position in exchange_positions.items():
            exchange_size = position['size']
            if symbol not in system_positions and exchange_size != 0:
                discrepancies.append((symbol, 0, exchange_size))
        
        if discrepancies:
            self.logger.warning(f"Found {len(discrepancies)} position discrepancies")
            self._reconciliation_stats["discrepancies_found"] += len(discrepancies)
            await self._handle_position_discrepancies(discrepancies)
    
    async def _handle_missing_system_orders(self, missing_order_ids, exchange_orders):
        """Handle orders that exist on exchange but not in system."""
        for order_id in missing_order_ids:
            # Find the order details from exchange data
            order_data = next((o for o in exchange_orders if o['id'] == order_id), None)
            if not order_data:
                continue
                
            self.logger.info(f"Importing missing order from exchange: {order_id}")
            
            try:
                # Import the order into the system
                await self.execution.import_exchange_order(order_data)
                self._reconciliation_stats["discrepancies_resolved"] += 1
            except Exception as e:
                self.logger.error(f"Failed to import order {order_id}: {str(e)}")
                # Add to pending tasks for retry
                self._add_pending_task("import_order", order_data, ReconciliationPriority.HIGH)
    
    async def _handle_missing_exchange_orders(self, missing_order_ids, system_orders):
        """Handle orders that exist in system but not on exchange."""
        for order_id in missing_order_ids:
            # Find the order from system data
            order = next((o for o in system_orders if o.order_id == order_id), None)
            if not order:
                continue
                
            self.logger.info(f"Handling system order missing from exchange: {order_id}")
            
            try:
                # Check if order should have been executed
                if order.is_working():
                    # Order should be active but isn't on exchange - could have been filled or canceled
                    # Try to fetch order history
                    order_history = await self.execution.get_exchange_order_history(order_id)
                    
                    if order_history:
                        # Order was on exchange but is now completed/canceled
                        await self.execution.update_order_from_exchange(order_id, order_history)
                    else:
                        # Order was never successfully placed - mark as failed
                        await self.execution.mark_order_as_failed(order_id, "Order not found on exchange")
                        
                    self._reconciliation_stats["discrepancies_resolved"] += 1
            except Exception as e:
                self.logger.error(f"Failed to reconcile missing exchange order {order_id}: {str(e)}")
                # Add to pending tasks for retry
                self._add_pending_task("handle_missing_exchange_order", order.to_dict(), ReconciliationPriority.HIGH)
    
    async def _handle_missing_trades(self, missing_trade_ids, exchange_trades):
        """Handle trades that exist on exchange but not in system."""
        for trade_id in missing_trade_ids:
            # Find trade details from exchange data
            trade_data = next((t for t in exchange_trades if t['id'] == trade_id), None)
            if not trade_data:
                continue
                
            self.logger.info(f"Importing missing trade from exchange: {trade_id}")
            
            try:
                # Import the trade into the system
                await self.execution.import_exchange_trade(trade_data)
                
                # Update position based on the trade
                await self.position_manager.process_fill(trade_data)
                
                self._reconciliation_stats["discrepancies_resolved"] += 1
            except Exception as e:
                self.logger.error(f"Failed to import trade {trade_id}: {str(e)}")
                # Add to pending tasks for retry
                self._add_pending_task("import_trade", trade_data, ReconciliationPriority.HIGH)
    
    async def _handle_balance_discrepancies(self, discrepancies):
        """Handle discrepancies in account balances."""
        for asset, system_amount, exchange_amount in discrepancies:
            self.logger.warning(
                f"Balance discrepancy for {asset}: system={system_amount}, exchange={exchange_amount}, "
                f"diff={exchange_amount - system_amount}"
            )
            
            try:
                # Update system balance to match exchange
                await self.position_manager.correct_balance(asset, exchange_amount)
                self._reconciliation_stats["discrepancies_resolved"] += 1
                
                # Log adjustment for audit
                self.logger.info(f"Adjusted {asset} balance from {system_amount} to {exchange_amount}")
            except Exception as e:
                self.logger.error(f"Failed to reconcile {asset} balance: {str(e)}")
                # Add to pending tasks for retry
                self._add_pending_task(
                    "correct_balance", 
                    {"asset": asset, "amount": exchange_amount}, 
                    ReconciliationPriority.MEDIUM
                )
    
    async def _handle_position_discrepancies(self, discrepancies):
        """Handle discrepancies in positions."""
        for symbol, system_size, exchange_size in discrepancies:
            self.logger.warning(
                f"Position discrepancy for {symbol}: system={system_size}, exchange={exchange_size}, "
                f"diff={exchange_size - system_size}"
            )
            
            try:
                # Get current market price for position valuation
                price_data = await self.market_data.get_ticker(symbol)
                current_price = price_data.last_price if price_data else None
                
                if current_price is None:
                    self.logger.warning(f"Cannot reconcile {symbol} position without price data")
                    continue
                
                # Update system position to match exchange
                await self.position_manager.correct_position(symbol, exchange_size, current_price)
                self._reconciliation_stats["discrepancies_resolved"] += 1
                
                # Log adjustment for audit
                self.logger.info(f"Adjusted {symbol} position from {system_size} to {exchange_size}")
            except Exception as e:
                self.logger.error(f"Failed to reconcile {symbol} position: {str(e)}")
                # Add to pending tasks for retry
                self._add_pending_task(
                    "correct_position", 
                    {"symbol": symbol, "size": exchange_size}, 
                    ReconciliationPriority.MEDIUM
                )
    
    def _add_pending_task(self, task_type, task_data, priority):
        """Add a task to the pending reconciliation tasks queue."""
        self._pending_tasks.append({
            "type": task_type,
            "data": task_data,
            "priority": priority,
            "added_time": datetime.now(),
            "retries": 0
        })
        
        # Sort pending tasks by priority (highest first)
        self._pending_tasks.sort(key=lambda x: x["priority"].value, reverse=True)
    
    async def _process_pending_tasks(self):
        """Process pending reconciliation tasks."""
        if not self._pending_tasks:
            return
            
        self.logger.info(f"Processing {len(self._pending_tasks)} pending reconciliation tasks")
        
        # Process tasks in priority order
        remaining_tasks = []
        
        for task in self._pending_tasks:
            if task["retries"] >= self.max_retries:
                self.logger.warning(
                    f"Task {task['type']} for {task['data']} exceeded max retries "
                    f"({task['retries']}/{self.max_retries})"
                )
                continue
                
            try:
                if task["type"] == "import_order":
                    await self.execution.import_exchange_order(task["data"])
                elif task["type"] == "handle_missing_exchange_order":
                    await self.execution.mark_order_as_failed(
                        task["data"]["order_id"], 
                        "Order not found on exchange (from reconciliation)"
                    )
                elif task["type"] == "import_trade":
                    await self.execution.import_exchange_trade(task["data"])
                    await self.position_manager.process_fill(task["data"])
                elif task["type"] == "correct_balance":
                    await self.position_manager.correct_balance(
                        task["data"]["asset"], 
                        task["data"]["amount"]
                    )
                elif task["type"] == "correct_position":
                    # Get current price
                    price_data = await self.market_data.get_ticker(task["data"]["symbol"])
                    current_price = price_data.last_price if price_data else None
                    
                    if current_price is None:
                        # Cannot complete without price, keep in queue
                        task["retries"] += 1
                        remaining_tasks.append(task)
                        continue
                        
                    await self.position_manager.correct_position(
                        task["data"]["symbol"], 
                        task["data"]["size"], 
                        current_price
                    )
                
                self._reconciliation_stats["discrepancies_resolved"] += 1
                self.logger.info(f"Successfully processed pending task: {task['type']}")
                
            except Exception as e:
                self.logger.error(f"Failed to process task {task['type']}: {str(e)}")
                task["retries"] += 1
                remaining_tasks.append(task)
        
        self._pending_tasks = remaining_tasks
    
    def force_reconciliation(self):
        """Force an immediate reconciliation."""
        if not self._running:
            self.logger.warning("Cannot force reconciliation while service is not running")
            return False
            
        if self._reconciliation_task:
            # Cancel current task if running
            self._reconciliation_task.cancel()
            
        # Start a new reconciliation task
        self._reconciliation_task = asyncio.create_task(self._perform_reconciliation())
        return True
    
    def get_reconciliation_stats(self):
        """Get statistics about reconciliation runs."""
        stats = self._reconciliation_stats.copy()
        stats["last_reconciliation"] = self._last_reconciliation
        stats["is_running"] = self._running
        
        if self._pending_tasks:
            stats["pending_tasks"] = len(self._pending_tasks)
            
            # Group tasks by type and priority
            task_breakdown = {}
            for task in self._pending_tasks:
                task_type = task["type"]
                priority = task["priority"].name
                
                if task_type not in task_breakdown:
                    task_breakdown[task_type] = {}
                    
                if priority not in task_breakdown[task_type]:
                    task_breakdown[task_type][priority] = 0
                    
                task_breakdown[task_type][priority] += 1
                
            stats["pending_task_breakdown"] = task_breakdown
            
        return stats 