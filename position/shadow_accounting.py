"""
Shadow Accounting System

This module implements a high-reliability shadow accounting system for position tracking
and reconciliation with exchange data, providing a double-entry verification mechanism.
"""

import json
import logging
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Set, Any, Tuple, Union

from trading_system.models.order import Order, OrderStatus, OrderSide
from trading_system.models.position import Position, PositionType, PositionStatus
from trading_system.models.fill import Fill
from trading_system.monitoring.alert_system import get_alert_system, AlertSeverity
from trading_system.utils.config import Config

# Configure logging
logger = logging.getLogger(__name__)

class ReconciliationStatus(Enum):
    """Status of position reconciliation with exchange."""
    MATCHED = "matched"
    DISCREPANCY = "discrepancy"
    ERROR = "error"
    NOT_CHECKED = "not_checked"

class AccountingEntryType(Enum):
    """Types of shadow accounting entries."""
    POSITION_OPEN = "position_open"
    POSITION_INCREASE = "position_increase"
    POSITION_DECREASE = "position_decrease"
    POSITION_CLOSE = "position_close"
    ORDER_EXECUTED = "order_executed"
    FILL_RECEIVED = "fill_received"
    MANUAL_ADJUSTMENT = "manual_adjustment"
    RECONCILIATION = "reconciliation"

@dataclass
class AccountingEntry:
    """Shadow accounting entry for position tracking."""
    entry_id: str
    timestamp: float
    entry_type: AccountingEntryType
    symbol: str
    quantity: float
    price: Optional[float] = None
    position_id: Optional[str] = None
    order_id: Optional[str] = None
    fill_id: Optional[str] = None
    transaction_id: Optional[str] = None
    notes: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "entry_id": self.entry_id,
            "timestamp": self.timestamp,
            "entry_type": self.entry_type.value,
            "symbol": self.symbol,
            "quantity": self.quantity,
            "price": self.price,
            "position_id": self.position_id,
            "order_id": self.order_id,
            "fill_id": self.fill_id,
            "transaction_id": self.transaction_id,
            "notes": self.notes,
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AccountingEntry':
        """Create from dictionary."""
        return cls(
            entry_id=data["entry_id"],
            timestamp=data["timestamp"],
            entry_type=AccountingEntryType(data["entry_type"]),
            symbol=data["symbol"],
            quantity=data["quantity"],
            price=data.get("price"),
            position_id=data.get("position_id"),
            order_id=data.get("order_id"),
            fill_id=data.get("fill_id"),
            transaction_id=data.get("transaction_id"),
            notes=data.get("notes", ""),
            metadata=data.get("metadata", {})
        )

@dataclass
class ReconciliationResult:
    """Result of position reconciliation."""
    position_id: str
    symbol: str
    timestamp: float
    status: ReconciliationStatus
    shadow_quantity: float
    exchange_quantity: float
    discrepancy: float
    correction_applied: bool = False
    error_message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "position_id": self.position_id,
            "symbol": self.symbol,
            "timestamp": self.timestamp,
            "status": self.status.value,
            "shadow_quantity": self.shadow_quantity,
            "exchange_quantity": self.exchange_quantity,
            "discrepancy": self.discrepancy,
            "correction_applied": self.correction_applied,
            "error_message": self.error_message,
            "details": self.details
        }

class ShadowAccountingSystem:
    """
    High-reliability shadow accounting system.
    
    This system maintains an independent record of all positions and trades,
    providing a double-entry verification mechanism to reconcile with exchange data.
    
    Features:
    - Double-entry accounting for position tracking
    - Periodic reconciliation with exchange data
    - Discrepancy detection and alerting
    - Automatic correction of small discrepancies
    - Comprehensive audit trail
    - Database persistence
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the shadow accounting system.
        
        Args:
            config: Shadow accounting configuration
        """
        self.config = config or Config().get("shadow_accounting", {})
        
        # Database settings
        self.db_path = self.config.get("db_path", "shadow_accounting.db")
        self.auto_reconcile = self.config.get("auto_reconcile", True)
        self.reconciliation_interval = self.config.get("reconciliation_interval", 3600)  # seconds
        self.auto_correction_threshold = self.config.get("auto_correction_threshold", 0.01)  # quantity
        self.alert_threshold = self.config.get("alert_threshold", 0.001)  # quantity
        
        # Thread safety
        self.lock = threading.Lock()
        
        # Alert system
        self.alert_system = get_alert_system()
        
        # Initialize database
        self._initialize_database()
        
        # Load positions
        self.positions: Dict[str, Position] = {}
        self._load_positions()
        
        # Reconciliation state
        self.last_reconciliation = time.time()
        self.reconciliation_results: Dict[str, ReconciliationResult] = {}
        
        logger.info("Shadow accounting system initialized")
    
    def _initialize_database(self) -> None:
        """Initialize the SQLite database."""
        db_path = Path(self.db_path)
        db_dir = db_path.parent
        
        # Create parent directory if it doesn't exist
        db_dir.mkdir(parents=True, exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Create positions table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS positions (
                    position_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    position_type TEXT NOT NULL,
                    quantity REAL NOT NULL,
                    entry_price REAL NOT NULL,
                    status TEXT NOT NULL,
                    timestamp_opened REAL NOT NULL,
                    timestamp_updated REAL NOT NULL,
                    timestamp_closed REAL,
                    exchange_id TEXT,
                    metadata TEXT
                )
            ''')
            
            # Create accounting entries table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS accounting_entries (
                    entry_id TEXT PRIMARY KEY,
                    timestamp REAL NOT NULL,
                    entry_type TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    quantity REAL NOT NULL,
                    price REAL,
                    position_id TEXT,
                    order_id TEXT,
                    fill_id TEXT,
                    transaction_id TEXT,
                    notes TEXT,
                    metadata TEXT
                )
            ''')
            
            # Create reconciliation results table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS reconciliation_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    position_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    status TEXT NOT NULL,
                    shadow_quantity REAL NOT NULL,
                    exchange_quantity REAL NOT NULL,
                    discrepancy REAL NOT NULL,
                    correction_applied INTEGER NOT NULL,
                    error_message TEXT,
                    details TEXT
                )
            ''')
            
            # Create indexes
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions (symbol)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_entries_position_id ON accounting_entries (position_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_entries_order_id ON accounting_entries (order_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_entries_timestamp ON accounting_entries (timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_reconciliation_position_id ON reconciliation_results (position_id)')
            
            conn.commit()
            
        logger.info(f"Shadow accounting database initialized at {self.db_path}")
    
    def _load_positions(self) -> None:
        """Load positions from database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM positions WHERE status != "closed"')
            rows = cursor.fetchall()
            
            for row in rows:
                # Parse metadata
                metadata = json.loads(row['metadata']) if row['metadata'] else {}
                
                # Create position object
                position = Position(
                    position_id=row['position_id'],
                    symbol=row['symbol'],
                    position_type=PositionType(row['position_type']),
                    quantity=row['quantity'],
                    entry_price=row['entry_price'],
                    status=PositionStatus(row['status']),
                    timestamp_opened=row['timestamp_opened'],
                    timestamp_updated=row['timestamp_updated'],
                    timestamp_closed=row['timestamp_closed'],
                    exchange_id=row['exchange_id']
                )
                
                # Add to positions dict
                self.positions[position.position_id] = position
        
        logger.info(f"Loaded {len(self.positions)} active positions from database")
    
    def create_position(self, position: Position) -> bool:
        """
        Create a new position in the shadow accounting system.
        
        Args:
            position: Position to create
            
        Returns:
            Success flag
        """
        with self.lock:
            position_id = position.position_id
            
            # Check if position already exists
            if position_id in self.positions:
                logger.warning(f"Position {position_id} already exists in shadow accounting")
                return False
            
            try:
                # Store position in database
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    
                    cursor.execute('''
                        INSERT INTO positions (
                            position_id, symbol, position_type, quantity, entry_price,
                            status, timestamp_opened, timestamp_updated, timestamp_closed,
                            exchange_id, metadata
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        position.position_id,
                        position.symbol,
                        position.position_type.value,
                        position.quantity,
                        position.entry_price,
                        position.status.value,
                        position.timestamp_opened,
                        position.timestamp_updated,
                        position.timestamp_closed,
                        position.exchange_id,
                        json.dumps({})
                    ))
                    
                    conn.commit()
                
                # Add to positions dict
                self.positions[position_id] = position
                
                # Create accounting entry
                entry = AccountingEntry(
                    entry_id=f"pe_{int(time.time())}_{position_id}",
                    timestamp=time.time(),
                    entry_type=AccountingEntryType.POSITION_OPEN,
                    symbol=position.symbol,
                    quantity=position.quantity,
                    price=position.entry_price,
                    position_id=position_id,
                    notes=f"Position opened: {position.symbol} ({position.position_type.value})"
                )
                
                self._add_accounting_entry(entry)
                
                logger.info(f"Created position in shadow accounting: {position_id}")
                return True
                
            except Exception as e:
                logger.error(f"Error creating position in shadow accounting: {str(e)}")
                return False
    
    def update_position(self, position: Position) -> bool:
        """
        Update an existing position in the shadow accounting system.
        
        Args:
            position: Updated position
            
        Returns:
            Success flag
        """
        with self.lock:
            position_id = position.position_id
            
            # Check if position exists
            if position_id not in self.positions:
                logger.warning(f"Position {position_id} not found in shadow accounting")
                return self.create_position(position)
            
            try:
                old_position = self.positions[position_id]
                quantity_delta = position.quantity - old_position.quantity
                
                # Update in database
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    
                    cursor.execute('''
                        UPDATE positions
                        SET quantity = ?, entry_price = ?, status = ?,
                            timestamp_updated = ?, timestamp_closed = ?
                        WHERE position_id = ?
                    ''', (
                        position.quantity,
                        position.entry_price,
                        position.status.value,
                        position.timestamp_updated,
                        position.timestamp_closed,
                        position_id
                    ))
                    
                    conn.commit()
                
                # Update in memory
                self.positions[position_id] = position
                
                # Create accounting entry if quantity changed
                if abs(quantity_delta) > 0.000001:  # Floating point comparison
                    entry_type = None
                    notes = ""
                    
                    if quantity_delta > 0:
                        # Position increased
                        entry_type = AccountingEntryType.POSITION_INCREASE
                        notes = f"Position increased: {position.symbol} (+{quantity_delta})"
                    else:
                        # Position decreased
                        entry_type = AccountingEntryType.POSITION_DECREASE
                        notes = f"Position decreased: {position.symbol} ({quantity_delta})"
                    
                    entry = AccountingEntry(
                        entry_id=f"pu_{int(time.time())}_{position_id}",
                        timestamp=time.time(),
                        entry_type=entry_type,
                        symbol=position.symbol,
                        quantity=abs(quantity_delta),
                        price=position.entry_price,
                        position_id=position_id,
                        notes=notes
                    )
                    
                    self._add_accounting_entry(entry)
                
                # Handle position closure
                if position.status == PositionStatus.CLOSED and old_position.status != PositionStatus.CLOSED:
                    # Create position close entry
                    entry = AccountingEntry(
                        entry_id=f"pc_{int(time.time())}_{position_id}",
                        timestamp=time.time(),
                        entry_type=AccountingEntryType.POSITION_CLOSE,
                        symbol=position.symbol,
                        quantity=old_position.quantity,
                        price=position.current_price,
                        position_id=position_id,
                        notes=f"Position closed: {position.symbol}"
                    )
                    
                    self._add_accounting_entry(entry)
                
                logger.info(f"Updated position in shadow accounting: {position_id}")
                return True
                
            except Exception as e:
                logger.error(f"Error updating position in shadow accounting: {str(e)}")
                return False
    
    def record_order_execution(self, order: Order) -> bool:
        """
        Record an order execution in the shadow accounting system.
        
        Args:
            order: Executed order
            
        Returns:
            Success flag
        """
        if order.status != OrderStatus.FILLED and order.status != OrderStatus.PARTIALLY_FILLED:
            return True  # Nothing to record
        
        try:
            # Create accounting entry
            entry = AccountingEntry(
                entry_id=f"oe_{int(time.time())}_{order.order_id}",
                timestamp=time.time(),
                entry_type=AccountingEntryType.ORDER_EXECUTED,
                symbol=order.symbol,
                quantity=order.filled_quantity,
                price=order.average_price or order.price,
                order_id=order.order_id,
                position_id=order.position_id,
                notes=f"Order executed: {order.symbol} ({order.side.value})"
            )
            
            self._add_accounting_entry(entry)
            
            logger.info(f"Recorded order execution in shadow accounting: {order.order_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error recording order execution in shadow accounting: {str(e)}")
            return False
    
    def record_fill(self, fill: Fill) -> bool:
        """
        Record a fill in the shadow accounting system.
        
        Args:
            fill: Trade fill
            
        Returns:
            Success flag
        """
        try:
            # Create accounting entry
            entry = AccountingEntry(
                entry_id=f"fi_{int(time.time())}_{fill.fill_id}",
                timestamp=time.time(),
                entry_type=AccountingEntryType.FILL_RECEIVED,
                symbol=fill.symbol,
                quantity=fill.quantity,
                price=fill.price,
                order_id=fill.order_id,
                fill_id=fill.fill_id,
                notes=f"Fill received: {fill.symbol} ({fill.quantity} @ {fill.price})"
            )
            
            self._add_accounting_entry(entry)
            
            logger.info(f"Recorded fill in shadow accounting: {fill.fill_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error recording fill in shadow accounting: {str(e)}")
            return False
    
    def manual_adjustment(self, 
                        symbol: str,
                        quantity: float,
                        position_id: Optional[str] = None,
                        notes: str = "",
                        metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Make a manual adjustment to a position.
        
        Args:
            symbol: Trading symbol
            quantity: Adjustment quantity (positive to increase, negative to decrease)
            position_id: Optional position ID
            notes: Adjustment notes
            metadata: Optional additional metadata
            
        Returns:
            Success flag
        """
        try:
            # Find position if not specified
            if not position_id and symbol in self.positions:
                for pos in self.positions.values():
                    if pos.symbol == symbol and pos.status == PositionStatus.OPEN:
                        position_id = pos.position_id
                        break
            
            # Create accounting entry
            entry = AccountingEntry(
                entry_id=f"ma_{int(time.time())}_{symbol}",
                timestamp=time.time(),
                entry_type=AccountingEntryType.MANUAL_ADJUSTMENT,
                symbol=symbol,
                quantity=abs(quantity),
                position_id=position_id,
                notes=f"Manual adjustment: {symbol} ({'+'if quantity > 0 else '-'}{abs(quantity)}). {notes}",
                metadata=metadata or {}
            )
            
            self._add_accounting_entry(entry)
            
            # Update position if specified
            if position_id and position_id in self.positions:
                position = self.positions[position_id]
                position.quantity += quantity
                
                self.update_position(position)
            
            logger.info(f"Made manual adjustment in shadow accounting: {symbol} ({quantity})")
            return True
            
        except Exception as e:
            logger.error(f"Error making manual adjustment in shadow accounting: {str(e)}")
            return False
    
    def reconcile_with_exchange(self, 
                              exchange_positions: Dict[str, Any],
                              position_id: Optional[str] = None,
                              symbol: Optional[str] = None) -> List[ReconciliationResult]:
        """
        Reconcile shadow accounting positions with exchange data.
        
        Args:
            exchange_positions: Positions from exchange
            position_id: Optional specific position to reconcile
            symbol: Optional specific symbol to reconcile
            
        Returns:
            List of reconciliation results
        """
        with self.lock:
            results = []
            
            try:
                # Filter positions to reconcile
                positions_to_check = {}
                
                if position_id:
                    # Reconcile specific position
                    if position_id in self.positions:
                        positions_to_check[position_id] = self.positions[position_id]
                    else:
                        logger.warning(f"Position {position_id} not found in shadow accounting")
                        return []
                elif symbol:
                    # Reconcile all positions for a symbol
                    for pos_id, pos in self.positions.items():
                        if pos.symbol == symbol and pos.status == PositionStatus.OPEN:
                            positions_to_check[pos_id] = pos
                else:
                    # Reconcile all open positions
                    for pos_id, pos in self.positions.items():
                        if pos.status == PositionStatus.OPEN:
                            positions_to_check[pos_id] = pos
                
                # Reconcile each position
                for pos_id, shadow_position in positions_to_check.items():
                    symbol = shadow_position.symbol
                    exchange_position = None
                    
                    # Find matching exchange position
                    for pos in exchange_positions:
                        if pos.get("symbol") == symbol:
                            exchange_position = pos
                            break
                    
                    result = self._reconcile_position(shadow_position, exchange_position)
                    results.append(result)
                    
                    # Store result
                    self.reconciliation_results[pos_id] = result
                    
                    # Record result in database
                    self._store_reconciliation_result(result)
                    
                    # Apply correction if needed and allowed
                    if (result.status == ReconciliationStatus.DISCREPANCY and 
                        self.auto_reconcile and 
                        abs(result.discrepancy) <= self.auto_correction_threshold):
                        self._apply_reconciliation_correction(result)
                        result.correction_applied = True
                    
                    # Alert on discrepancies
                    if result.status == ReconciliationStatus.DISCREPANCY and abs(result.discrepancy) > self.alert_threshold:
                        severity = AlertSeverity.WARNING
                        if abs(result.discrepancy) > self.auto_correction_threshold:
                            severity = AlertSeverity.ERROR
                        
                        self.alert_system.alert(
                            "Position Reconciliation Discrepancy",
                            f"Discrepancy detected for {symbol} position: shadow {result.shadow_quantity}, "
                            f"exchange {result.exchange_quantity}, diff {result.discrepancy}",
                            severity=severity,
                            component="ShadowAccounting",
                            tags={"position_id": pos_id, "symbol": symbol}
                        )
                
                # Update last reconciliation timestamp
                self.last_reconciliation = time.time()
                
                logger.info(f"Reconciled {len(results)} positions with exchange data")
                return results
                
            except Exception as e:
                logger.error(f"Error reconciling positions with exchange: {str(e)}")
                
                # Create error result
                error_result = ReconciliationResult(
                    position_id=position_id or "all",
                    symbol=symbol or "all",
                    timestamp=time.time(),
                    status=ReconciliationStatus.ERROR,
                    shadow_quantity=0,
                    exchange_quantity=0,
                    discrepancy=0,
                    error_message=str(e)
                )
                
                results.append(error_result)
                return results
    
    def _reconcile_position(self, 
                          shadow_position: Position, 
                          exchange_position: Optional[Dict[str, Any]]) -> ReconciliationResult:
        """
        Reconcile a single position with exchange data.
        
        Args:
            shadow_position: Position from shadow accounting
            exchange_position: Position data from exchange
            
        Returns:
            Reconciliation result
        """
        position_id = shadow_position.position_id
        symbol = shadow_position.symbol
        shadow_quantity = shadow_position.quantity
        
        # Default values
        exchange_quantity = 0
        status = ReconciliationStatus.NOT_CHECKED
        discrepancy = 0
        details = {}
        
        if exchange_position is None:
            # Position not found on exchange
            if shadow_quantity > 0:
                status = ReconciliationStatus.DISCREPANCY
                discrepancy = shadow_quantity
                details = {"reason": "Position exists in shadow accounting but not on exchange"}
            else:
                status = ReconciliationStatus.MATCHED
                details = {"reason": "Position has zero quantity in shadow accounting and doesn't exist on exchange"}
        else:
            # Position found on exchange
            exchange_quantity = float(exchange_position.get("quantity", 0))
            
            if abs(shadow_quantity - exchange_quantity) < 0.000001:  # Floating point comparison
                status = ReconciliationStatus.MATCHED
                details = {"reason": "Quantities match"}
            else:
                status = ReconciliationStatus.DISCREPANCY
                discrepancy = shadow_quantity - exchange_quantity
                details = {
                    "reason": "Quantity mismatch",
                    "shadow_type": shadow_position.position_type.value,
                    "exchange_type": exchange_position.get("type", "unknown")
                }
        
        # Create result
        result = ReconciliationResult(
            position_id=position_id,
            symbol=symbol,
            timestamp=time.time(),
            status=status,
            shadow_quantity=shadow_quantity,
            exchange_quantity=exchange_quantity,
            discrepancy=discrepancy,
            details=details
        )
        
        return result
    
    def _apply_reconciliation_correction(self, result: ReconciliationResult) -> bool:
        """
        Apply a correction based on reconciliation result.
        
        Args:
            result: Reconciliation result
            
        Returns:
            Success flag
        """
        position_id = result.position_id
        discrepancy = result.discrepancy
        
        if position_id not in self.positions:
            logger.warning(f"Cannot apply correction: Position {position_id} not found")
            return False
        
        # Get position
        position = self.positions[position_id]
        
        # Create adjustment entry
        entry = AccountingEntry(
            entry_id=f"rc_{int(time.time())}_{position_id}",
            timestamp=time.time(),
            entry_type=AccountingEntryType.RECONCILIATION,
            symbol=position.symbol,
            quantity=abs(discrepancy),
            position_id=position_id,
            notes=f"Reconciliation adjustment: {position.symbol} ({'+'if discrepancy < 0 else '-'}{abs(discrepancy)})"
        )
        
        self._add_accounting_entry(entry)
        
        # Update position
        position.quantity -= discrepancy  # Adjust to match exchange
        self.update_position(position)
        
        logger.info(f"Applied reconciliation correction to {position_id}: {-discrepancy}")
        
        # Log the correction
        self.alert_system.info(
            "Reconciliation Correction Applied",
            f"Applied automatic correction to {position.symbol} position: adjusted by {-discrepancy}",
            component="ShadowAccounting",
            tags={"position_id": position_id, "symbol": position.symbol}
        )
        
        return True
    
    def _add_accounting_entry(self, entry: AccountingEntry) -> bool:
        """
        Add an accounting entry to the database.
        
        Args:
            entry: Accounting entry
            
        Returns:
            Success flag
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT INTO accounting_entries (
                        entry_id, timestamp, entry_type, symbol, quantity,
                        price, position_id, order_id, fill_id, transaction_id,
                        notes, metadata
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    entry.entry_id,
                    entry.timestamp,
                    entry.entry_type.value,
                    entry.symbol,
                    entry.quantity,
                    entry.price,
                    entry.position_id,
                    entry.order_id,
                    entry.fill_id,
                    entry.transaction_id,
                    entry.notes,
                    json.dumps(entry.metadata)
                ))
                
                conn.commit()
            
            return True
            
        except Exception as e:
            logger.error(f"Error adding accounting entry: {str(e)}")
            return False
    
    def _store_reconciliation_result(self, result: ReconciliationResult) -> bool:
        """
        Store reconciliation result in database.
        
        Args:
            result: Reconciliation result
            
        Returns:
            Success flag
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT INTO reconciliation_results (
                        position_id, symbol, timestamp, status,
                        shadow_quantity, exchange_quantity, discrepancy,
                        correction_applied, error_message, details
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    result.position_id,
                    result.symbol,
                    result.timestamp,
                    result.status.value,
                    result.shadow_quantity,
                    result.exchange_quantity,
                    result.discrepancy,
                    1 if result.correction_applied else 0,
                    result.error_message,
                    json.dumps(result.details)
                ))
                
                conn.commit()
            
            return True
            
        except Exception as e:
            logger.error(f"Error storing reconciliation result: {str(e)}")
            return False
    
    def get_position_history(self, 
                           position_id: str,
                           limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get history of entries for a position.
        
        Args:
            position_id: Position ID
            limit: Maximum number of entries
            
        Returns:
            List of accounting entries
        """
        entries = []
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT * FROM accounting_entries
                    WHERE position_id = ?
                    ORDER BY timestamp DESC
                    LIMIT ?
                ''', (position_id, limit))
                
                rows = cursor.fetchall()
                
                for row in rows:
                    metadata = json.loads(row['metadata']) if row['metadata'] else {}
                    
                    entry = {
                        "entry_id": row['entry_id'],
                        "timestamp": row['timestamp'],
                        "entry_type": row['entry_type'],
                        "symbol": row['symbol'],
                        "quantity": row['quantity'],
                        "price": row['price'],
                        "position_id": row['position_id'],
                        "order_id": row['order_id'],
                        "fill_id": row['fill_id'],
                        "transaction_id": row['transaction_id'],
                        "notes": row['notes'],
                        "metadata": metadata
                    }
                    
                    entries.append(entry)
            
            return entries
            
        except Exception as e:
            logger.error(f"Error getting position history: {str(e)}")
            return []
    
    def get_all_positions(self) -> Dict[str, Position]:
        """
        Get all positions in shadow accounting.
        
        Returns:
            Dictionary of positions
        """
        return self.positions.copy()
    
    def get_position(self, position_id: str) -> Optional[Position]:
        """
        Get a specific position.
        
        Args:
            position_id: Position ID
            
        Returns:
            Position or None if not found
        """
        return self.positions.get(position_id)
    
    def get_positions_by_symbol(self, symbol: str) -> List[Position]:
        """
        Get all positions for a symbol.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            List of positions
        """
        return [p for p in self.positions.values() if p.symbol == symbol]
    
    def get_reconciliation_results(self, 
                                 limit: int = 10, 
                                 position_id: Optional[str] = None,
                                 symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get recent reconciliation results.
        
        Args:
            limit: Maximum number of results
            position_id: Optional filter by position ID
            symbol: Optional filter by symbol
            
        Returns:
            List of reconciliation results
        """
        results = []
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                query = '''
                    SELECT * FROM reconciliation_results
                    WHERE 1=1
                '''
                
                params = []
                
                if position_id:
                    query += " AND position_id = ?"
                    params.append(position_id)
                
                if symbol:
                    query += " AND symbol = ?"
                    params.append(symbol)
                
                query += " ORDER BY timestamp DESC LIMIT ?"
                params.append(limit)
                
                cursor.execute(query, tuple(params))
                
                rows = cursor.fetchall()
                
                for row in rows:
                    details = json.loads(row['details']) if row['details'] else {}
                    
                    result = {
                        "position_id": row['position_id'],
                        "symbol": row['symbol'],
                        "timestamp": row['timestamp'],
                        "status": row['status'],
                        "shadow_quantity": row['shadow_quantity'],
                        "exchange_quantity": row['exchange_quantity'],
                        "discrepancy": row['discrepancy'],
                        "correction_applied": bool(row['correction_applied']),
                        "error_message": row['error_message'],
                        "details": details
                    }
                    
                    results.append(result)
            
            return results
            
        except Exception as e:
            logger.error(f"Error getting reconciliation results: {str(e)}")
            return []
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get shadow accounting statistics.
        
        Returns:
            Statistics dictionary
        """
        stats = {
            "total_positions": len(self.positions),
            "open_positions": sum(1 for p in self.positions.values() if p.status == PositionStatus.OPEN),
            "last_reconciliation": self.last_reconciliation,
            "reconciliation_age": time.time() - self.last_reconciliation,
            "auto_reconcile": self.auto_reconcile,
            "auto_correction_threshold": self.auto_correction_threshold,
            "alert_threshold": self.alert_threshold
        }
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Count entries
                cursor.execute('SELECT COUNT(*) FROM accounting_entries')
                stats["total_entries"] = cursor.fetchone()[0]
                
                # Count reconciliation results
                cursor.execute('SELECT COUNT(*) FROM reconciliation_results')
                stats["total_reconciliations"] = cursor.fetchone()[0]
                
                # Count discrepancies
                cursor.execute('SELECT COUNT(*) FROM reconciliation_results WHERE status = "discrepancy"')
                stats["total_discrepancies"] = cursor.fetchone()[0]
                
                # Count corrections
                cursor.execute('SELECT COUNT(*) FROM reconciliation_results WHERE correction_applied = 1')
                stats["total_corrections"] = cursor.fetchone()[0]
                
        except Exception as e:
            logger.error(f"Error getting statistics: {str(e)}")
        
        return stats

# Global instance
_instance = None

def get_shadow_accounting(config: Optional[Dict[str, Any]] = None) -> ShadowAccountingSystem:
    """
    Get or create the global ShadowAccountingSystem instance.
    
    Args:
        config: Optional configuration (only used on first call)
        
    Returns:
        ShadowAccountingSystem instance
    """
    global _instance
    if _instance is None:
        _instance = ShadowAccountingSystem(config)
    return _instance 