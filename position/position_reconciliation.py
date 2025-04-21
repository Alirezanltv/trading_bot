"""
Position Reconciliation Module

This module implements position reconciliation between internal positions
and exchange positions, detecting and resolving discrepancies.
"""

import asyncio
import logging
import time
from typing import Dict, List, Any, Optional, Tuple, Set
from decimal import Decimal
from datetime import datetime, timedelta

from trading_system.core.component import Component, ComponentStatus
from trading_system.position.shadow_accounting import ShadowPositionManager
from trading_system.position.position_manager import PositionManager
from trading_system.position.position_types import Position, PositionStatus
from trading_system.core.logging import get_logger

logger = get_logger("position.reconciliation")

class ReconciliationResult:
    """Container for reconciliation result data."""
    
    def __init__(self):
        self.timestamp = datetime.now()
        self.duration_ms = 0
        self.exchanges_checked = 0
        self.positions_checked = 0
        self.matches = 0
        self.mismatches = 0
        self.corrections = 0
        self.errors = 0
        self.details = []
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "duration_ms": self.duration_ms,
            "exchanges_checked": self.exchanges_checked,
            "positions_checked": self.positions_checked,
            "matches": self.matches,
            "mismatches": self.mismatches,
            "corrections": self.corrections,
            "errors": self.errors,
            "details": self.details
        }
    
    def add_detail(self, exchange: str, symbol: str, detail: Dict[str, Any]) -> None:
        """Add a detail record."""
        self.details.append({
            "exchange": exchange,
            "symbol": symbol,
            **detail
        })
    
    def add_error(self, exchange: str, error_message: str) -> None:
        """Add an error record."""
        self.errors += 1
        self.details.append({
            "exchange": exchange,
            "error": error_message,
            "type": "error"
        })


class PositionReconciliation(Component):
    """
    Position reconciliation component.
    
    This component periodically reconciles positions between:
    1. Position Manager (in-memory positions)
    2. Shadow Accounting (database-backed positions)
    3. Exchange positions (from API)
    
    It detects discrepancies and can apply corrections based on configuration.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize position reconciliation.
        
        Args:
            config: Configuration dictionary
        """
        super().__init__(name="position_reconciliation")
        
        # Configuration
        self.config = config or {}
        self.reconciliation_interval = self.config.get("reconciliation_interval", 3600)  # seconds
        self.auto_correct = self.config.get("auto_correct", True)
        self.tolerance = Decimal(str(self.config.get("tolerance", "0.001")))
        self.critical_threshold = Decimal(str(self.config.get("critical_threshold", "0.05")))
        
        # Components
        self.position_manager = None
        self.shadow_manager = None
        
        # Exchange clients
        self.exchange_clients = {}
        
        # Reconciliation task
        self.reconciliation_task = None
        self.last_reconciliation = None
        
        # Locked positions
        self.locked_positions = set()
        
        # Statistics
        self.reconciliation_stats = {
            "total": 0,
            "matches": 0,
            "mismatches": 0,
            "corrections": 0,
            "errors": 0
        }
    
    async def initialize(self) -> bool:
        """
        Initialize the reconciliation component.
        
        Returns:
            Initialization success
        """
        try:
            logger.info("Initializing position reconciliation")
            self._status = ComponentStatus.INITIALIZING
            
            # Start reconciliation task
            if self.reconciliation_interval > 0:
                self.reconciliation_task = asyncio.create_task(self._reconciliation_loop())
            
            self._status = ComponentStatus.INITIALIZED
            logger.info("Position reconciliation initialized")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing position reconciliation: {str(e)}", exc_info=True)
            self._status = ComponentStatus.ERROR
            self._error = str(e)
            return False
    
    async def stop(self) -> bool:
        """
        Stop the reconciliation component.
        
        Returns:
            Stop success
        """
        try:
            logger.info("Stopping position reconciliation")
            
            # Stop reconciliation task
            if self.reconciliation_task:
                self.reconciliation_task.cancel()
                try:
                    await self.reconciliation_task
                except asyncio.CancelledError:
                    pass
                self.reconciliation_task = None
            
            self._status = ComponentStatus.STOPPED
            logger.info("Position reconciliation stopped")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping position reconciliation: {str(e)}", exc_info=True)
            self._status = ComponentStatus.ERROR
            self._error = str(e)
            return False
    
    def register_position_manager(self, position_manager: PositionManager) -> None:
        """
        Register the position manager.
        
        Args:
            position_manager: Position manager instance
        """
        self.position_manager = position_manager
        logger.info("Registered position manager")
    
    def register_shadow_manager(self, shadow_manager: ShadowPositionManager) -> None:
        """
        Register the shadow position manager.
        
        Args:
            shadow_manager: Shadow position manager instance
        """
        self.shadow_manager = shadow_manager
        logger.info("Registered shadow position manager")
    
    def register_exchange_client(self, exchange_id: str, client: Any) -> None:
        """
        Register an exchange client.
        
        Args:
            exchange_id: Exchange identifier
            client: Exchange client instance with get_positions method
        """
        self.exchange_clients[exchange_id] = client
        logger.info(f"Registered exchange client for {exchange_id}")
    
    def unregister_exchange_client(self, exchange_id: str) -> None:
        """
        Unregister an exchange client.
        
        Args:
            exchange_id: Exchange identifier
        """
        if exchange_id in self.exchange_clients:
            del self.exchange_clients[exchange_id]
            logger.info(f"Unregistered exchange client for {exchange_id}")
    
    async def reconcile_positions(self, exchange_id: Optional[str] = None, symbol: Optional[str] = None,
                                 force_correction: bool = False) -> ReconciliationResult:
        """
        Reconcile positions between position manager, shadow accounting, and exchanges.
        
        Args:
            exchange_id: Exchange to reconcile (all exchanges if None)
            symbol: Symbol to reconcile (all symbols if None)
            force_correction: Force correction even if auto_correct is disabled
            
        Returns:
            ReconciliationResult: Reconciliation results
        """
        # Initialize result
        result = ReconciliationResult()
        start_time = time.time()
        
        # Get exchanges to check
        exchanges = [exchange_id] if exchange_id else list(self.exchange_clients.keys())
        result.exchanges_checked = len(exchanges)
        
        # Check required components
        if not self.position_manager:
            logger.error("Position manager not registered")
            result.add_error("all", "Position manager not registered")
            return result
        
        if not self.shadow_manager:
            logger.warning("Shadow manager not registered, will only reconcile with exchanges")
        
        # Process each exchange
        for exchange in exchanges:
            try:
                # Get exchange client
                client = self.exchange_clients.get(exchange)
                if not client:
                    logger.warning(f"No client for exchange {exchange}, skipping reconciliation")
                    result.add_error(exchange, "No client available")
                    continue
                
                # Get exchange positions
                try:
                    if hasattr(client, "get_positions"):
                        exchange_positions = await client.get_positions(symbol=symbol)
                    elif hasattr(client, "get_all_positions"):
                        exchange_positions = await client.get_all_positions(symbol=symbol)
                    else:
                        logger.error(f"Exchange client for {exchange} has no position retrieval method")
                        result.add_error(exchange, "Client has no position retrieval method")
                        continue
                except Exception as e:
                    logger.error(f"Error getting positions from exchange {exchange}: {str(e)}", exc_info=True)
                    result.add_error(exchange, f"Failed to retrieve positions: {str(e)}")
                    continue
                
                # Convert exchange positions to a lookup dict
                exchange_positions_dict = {}
                for pos in exchange_positions:
                    # Extract key fields
                    ex_symbol = pos.get("symbol")
                    ex_quantity = Decimal(str(pos.get("quantity", 0)))
                    
                    if ex_symbol:
                        exchange_positions_dict[ex_symbol] = {
                            "quantity": ex_quantity,
                            "raw_data": pos
                        }
                
                # Get position manager positions for this exchange
                manager_positions = {}
                if symbol:
                    positions = self.position_manager.get_positions_by_symbol(symbol)
                    for pos in positions:
                        if pos.exchange == exchange and pos.status != PositionStatus.CLOSED:
                            manager_positions[pos.symbol] = pos
                else:
                    # Get all open positions for this exchange
                    for pos in self.position_manager.get_all_open_positions():
                        if pos.exchange == exchange:
                            manager_positions[pos.symbol] = pos
                
                # Get shadow positions if available
                shadow_positions = {}
                if self.shadow_manager:
                    try:
                        shadow_data = await self.shadow_manager.get_all_positions(exchange)
                        
                        # Handle different return types (dict or list)
                        if isinstance(shadow_data, dict):
                            # Process dictionary format
                            for symbol_key, pos in shadow_data.items():
                                # Skip by symbol filter if provided
                                pos_symbol = pos.get("symbol")
                                if symbol and symbol != pos_symbol:
                                    continue
                                
                                if pos_symbol:  # Only add if symbol exists
                                    shadow_positions[pos_symbol] = {
                                        "quantity": Decimal(str(pos.get("quantity", 0))),
                                        "raw_data": pos
                                    }
                        else:
                            # Process list format
                            for pos in shadow_data:
                                # Skip by symbol filter if provided
                                pos_symbol = pos.get("symbol")
                                if symbol and symbol != pos_symbol:
                                    continue
                                
                                if pos_symbol:  # Only add if symbol exists
                                    shadow_positions[pos_symbol] = {
                                        "quantity": Decimal(str(pos.get("quantity", 0))),
                                        "raw_data": pos
                                    }
                    except Exception as e:
                        logger.error(f"Error getting shadow positions: {str(e)}")
                        result.errors += 1
                        result.details.append({
                            "exchange": exchange,
                            "error": f"Shadow position error: {str(e)}",
                            "status": "error"
                        })
                
                # Collect all unique symbols
                all_symbols = set(list(exchange_positions_dict.keys()) + 
                                  list(manager_positions.keys()) + 
                                  list(shadow_positions.keys()))
                
                # Update positions checked count
                result.positions_checked += len(all_symbols)
                
                # Process each symbol
                for sym in all_symbols:
                    # Skip if filtering by symbol and doesn't match
                    if symbol and sym != symbol:
                        continue
                    
                    # Get positions from each source
                    exchange_pos = exchange_positions_dict.get(sym, {"quantity": Decimal("0")})
                    manager_pos = manager_positions.get(sym)
                    shadow_pos = shadow_positions.get(sym, {"quantity": Decimal("0")})
                    
                    # Extract quantities
                    exchange_qty = exchange_pos["quantity"]
                    manager_qty = Decimal(str(manager_pos.quantity)) if manager_pos else Decimal("0")
                    shadow_qty = shadow_pos["quantity"]
                    
                    # Determine positions match
                    exchange_manager_match = abs(exchange_qty - manager_qty) <= self.tolerance
                    exchange_shadow_match = abs(exchange_qty - shadow_qty) <= self.tolerance
                    manager_shadow_match = abs(manager_qty - shadow_qty) <= self.tolerance
                    
                    # All match within tolerance
                    if exchange_manager_match and exchange_shadow_match and manager_shadow_match:
                        result.matches += 1
                        
                        # Add detail for non-zero positions
                        if exchange_qty != 0 or manager_qty != 0 or shadow_qty != 0:
                            result.add_detail(
                                exchange, sym, {
                                    "exchange_quantity": float(exchange_qty),
                                    "manager_quantity": float(manager_qty),
                                    "shadow_quantity": float(shadow_qty),
                                    "status": "matched",
                                    "notes": "All positions match within tolerance"
                                }
                            )
                        
                        continue
                    
                    # Position mismatch
                    result.mismatches += 1
                    
                    # Calculate differences
                    exchange_manager_diff = exchange_qty - manager_qty
                    exchange_shadow_diff = exchange_qty - shadow_qty
                    manager_shadow_diff = manager_qty - shadow_qty
                    
                    # Create mismatch detail
                    mismatch_data = {
                        "exchange_quantity": float(exchange_qty),
                        "manager_quantity": float(manager_qty),
                        "shadow_quantity": float(shadow_qty),
                        "exchange_manager_diff": float(exchange_manager_diff),
                        "exchange_shadow_diff": float(exchange_shadow_diff),
                        "manager_shadow_diff": float(manager_shadow_diff),
                        "status": "mismatched",
                        "correction_applied": False,
                        "notes": "Position quantities differ"
                    }
                    
                    # Check if this is a critical mismatch
                    is_critical = False
                    if exchange_qty != 0:
                        if abs(exchange_manager_diff / exchange_qty) >= self.critical_threshold:
                            is_critical = True
                            mismatch_data["notes"] += " - CRITICAL MISMATCH"
                            logger.warning(f"CRITICAL POSITION MISMATCH: {exchange}:{sym} - Exchange: {exchange_qty}, Internal: {manager_qty}")
                    
                    # Apply correction if enabled or forced
                    if self.auto_correct or force_correction or is_critical:
                        # Skip if position is already locked
                        position_key = f"{exchange}:{sym}"
                        if position_key in self.locked_positions:
                            mismatch_data["notes"] += " - Position locked, correction skipped"
                            result.add_detail(exchange, sym, mismatch_data)
                            continue
                        
                        # Lock position for reconciliation
                        self.locked_positions.add(position_key)
                        
                        try:
                            # Determine correct quantity
                            # Trust exchange quantity first
                            correct_qty = exchange_qty
                            
                            # Update position manager if needed
                            if abs(manager_qty - correct_qty) > self.tolerance:
                                if manager_pos:
                                    # Update existing position
                                    await self.position_manager.update_position_quantity(
                                        position_id=manager_pos.position_id,
                                        new_quantity=float(correct_qty),
                                        reason="Reconciliation with exchange",
                                        metadata={
                                            "reconciliation": True,
                                            "previous_quantity": float(manager_qty),
                                            "exchange_quantity": float(correct_qty),
                                            "difference": float(correct_qty - manager_qty)
                                        }
                                    )
                                    mismatch_data["notes"] += " - Updated position manager quantity"
                                elif correct_qty != 0:
                                    # Create new position if not found
                                    price_info = exchange_pos.get("raw_data", {}).get("price") or exchange_pos.get("raw_data", {}).get("entry_price", 0)
                                    
                                    # Create new position
                                    await self.position_manager.create_position(
                                        exchange=exchange,
                                        symbol=sym,
                                        quantity=float(correct_qty),
                                        entry_price=float(price_info) if price_info else 0,
                                        metadata={
                                            "reconciliation": True,
                                            "reason": "Position found in exchange but not in position manager"
                                        }
                                    )
                                    mismatch_data["notes"] += " - Created new position in position manager"
                            
                            # Update shadow accounting if needed
                            if self.shadow_manager and abs(shadow_qty - correct_qty) > self.tolerance:
                                await self.shadow_manager.update_position(
                                    exchange_id=exchange,
                                    symbol=sym,
                                    quantity=float(correct_qty),
                                    price=float(exchange_pos.get("raw_data", {}).get("price", 0)),
                                    source=self.shadow_manager.PositionSource.RECONCILIATION,
                                    metadata={
                                        "reconciliation": True,
                                        "previous_quantity": float(shadow_qty),
                                        "exchange_quantity": float(correct_qty),
                                        "difference": float(correct_qty - shadow_qty)
                                    }
                                )
                                mismatch_data["notes"] += " - Updated shadow accounting"
                            
                            # Record correction
                            mismatch_data["status"] = "corrected"
                            mismatch_data["correction_applied"] = True
                            result.corrections += 1
                            
                        except Exception as e:
                            logger.error(f"Error correcting position {position_key}: {str(e)}", exc_info=True)
                            mismatch_data["notes"] += f" - Correction failed: {str(e)}"
                            result.errors += 1
                        
                        finally:
                            # Unlock position
                            if position_key in self.locked_positions:
                                self.locked_positions.remove(position_key)
                    
                    # Add mismatch detail
                    result.add_detail(exchange, sym, mismatch_data)
                
            except Exception as e:
                logger.error(f"Error reconciling positions for exchange {exchange}: {str(e)}", exc_info=True)
                result.add_error(exchange, str(e))
        
        # Calculate duration
        end_time = time.time()
        result.duration_ms = int((end_time - start_time) * 1000)
        
        # Update reconciliation stats and time
        self.reconciliation_stats["total"] += 1
        self.reconciliation_stats["matches"] += result.matches
        self.reconciliation_stats["mismatches"] += result.mismatches
        self.reconciliation_stats["corrections"] += result.corrections
        self.reconciliation_stats["errors"] += result.errors
        self.last_reconciliation = datetime.now()
        
        # Log summary
        logger.info(
            f"Reconciliation completed in {result.duration_ms}ms: "
            f"{result.matches} matches, {result.mismatches} mismatches, "
            f"{result.corrections} corrections, {result.errors} errors"
        )
        
        return result
    
    async def get_reconciliation_stats(self) -> Dict[str, Any]:
        """
        Get reconciliation statistics.
        
        Returns:
            Dict[str, Any]: Reconciliation statistics
        """
        return {
            **self.reconciliation_stats,
            "last_reconciliation": self.last_reconciliation.isoformat() if self.last_reconciliation else None
        }
    
    async def _reconciliation_loop(self) -> None:
        """Background task for automatic reconciliation."""
        try:
            while True:
                try:
                    # Wait between reconciliations
                    await asyncio.sleep(self.reconciliation_interval)
                    
                    # Skip if not initialized
                    if self._status != ComponentStatus.INITIALIZED and self._status != ComponentStatus.HEALTHY:
                        continue
                    
                    # Perform reconciliation
                    logger.info("Starting automated position reconciliation")
                    await self.reconcile_positions()
                    
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"Error in reconciliation loop: {str(e)}", exc_info=True)
                    await asyncio.sleep(60)  # Wait a bit longer on error
                
        except asyncio.CancelledError:
            logger.info("Reconciliation task cancelled") 