"""
Position Storage Module

This module provides persistent storage for positions, orders, and portfolio data
using SQLite database.
"""

import logging
import os
import json
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Any, Union, Tuple

from trading_system.position.position_types import (
    Position, Order, PositionTrigger, PositionStatus, 
    PositionType, OrderStatus, OrderType, OrderAction,
    PositionSource, TriggerType, RiskLevel, PortfolioSummary
)

logger = logging.getLogger(__name__)


class PositionStorage:
    """
    SQLite-based storage for position management data.
    
    This class handles:
    - Database schema initialization
    - CRUD operations for positions, orders, and triggers
    - Query operations for position data
    - Portfolio snapshot storage and retrieval
    """
    
    def __init__(self, db_path: str = "data/positions/positions.db"):
        """
        Initialize the position storage.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # Initialize connection and tables
        self.conn = self._create_connection()
        self._create_tables()
        
        logger.info(f"Position storage initialized with database at {db_path}")
    
    def _create_connection(self) -> sqlite3.Connection:
        """Create database connection."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # Access rows as dictionaries
            return conn
        except sqlite3.Error as e:
            logger.error(f"Error connecting to database: {e}")
            raise
    
    def _create_tables(self) -> None:
        """Create required database tables if they don't exist."""
        try:
            cursor = self.conn.cursor()
            
            # Positions table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS positions (
                position_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                position_type TEXT NOT NULL,
                status TEXT NOT NULL,
                quantity REAL NOT NULL,
                entry_price REAL,
                current_price REAL,
                exit_price REAL,
                created_at TEXT NOT NULL,
                opened_at TEXT,
                closed_at TEXT,
                pnl REAL,
                pnl_percentage REAL,
                fees REAL,
                strategy_id TEXT,
                source TEXT,
                risk_level TEXT,
                exchange TEXT,
                metadata TEXT,
                tags TEXT,
                notes TEXT
            )
            ''')
            
            # Orders table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS orders (
                order_id TEXT PRIMARY KEY,
                exchange_order_id TEXT,
                position_id TEXT,
                symbol TEXT NOT NULL,
                order_type TEXT NOT NULL,
                action TEXT NOT NULL,
                quantity REAL NOT NULL,
                price REAL,
                stop_price REAL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                filled_quantity REAL,
                average_fill_price REAL,
                fees REAL,
                fee_currency TEXT,
                error_message TEXT,
                exchange TEXT,
                metadata TEXT,
                FOREIGN KEY (position_id) REFERENCES positions (position_id)
            )
            ''')
            
            # Position triggers table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS triggers (
                trigger_id TEXT PRIMARY KEY,
                position_id TEXT NOT NULL,
                trigger_type TEXT NOT NULL,
                price REAL,
                percentage REAL,
                trail_value REAL,
                expiration_time TEXT,
                is_active INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                triggered_at TEXT,
                metadata TEXT,
                FOREIGN KEY (position_id) REFERENCES positions (position_id)
            )
            ''')
            
            # Position-order relation
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS position_orders (
                position_id TEXT,
                order_id TEXT,
                order_type TEXT,
                PRIMARY KEY (position_id, order_id),
                FOREIGN KEY (position_id) REFERENCES positions (position_id),
                FOREIGN KEY (order_id) REFERENCES orders (order_id)
            )
            ''')
            
            # Portfolio snapshots
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS portfolio_snapshots (
                snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                total_equity REAL,
                available_balance REAL,
                open_position_count INTEGER,
                total_position_value REAL,
                total_pnl REAL,
                total_pnl_percentage REAL,
                total_fees REAL,
                current_exposure REAL,
                exposure_data TEXT
            )
            ''')
            
            # Create indexes
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions (symbol)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_positions_status ON positions (status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_positions_strategy ON positions (strategy_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_orders_symbol ON orders (symbol)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_orders_status ON orders (status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_orders_position ON orders (position_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_triggers_position ON triggers (position_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_triggers_active ON triggers (is_active)')
            
            self.conn.commit()
            logger.debug("Database tables created/verified")
        
        except sqlite3.Error as e:
            logger.error(f"Error creating database tables: {e}")
            raise
    
    def close(self) -> None:
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.debug("Database connection closed")
    
    def _dict_to_json(self, data: Dict) -> str:
        """Convert dictionary to JSON string."""
        return json.dumps(data) if data else "{}"
    
    def _json_to_dict(self, json_str: str) -> Dict:
        """Convert JSON string to dictionary."""
        return json.loads(json_str) if json_str else {}
    
    def _convert_list_to_json(self, data_list: List) -> str:
        """Convert list to JSON string."""
        return json.dumps(data_list) if data_list else "[]"
    
    def _convert_json_to_list(self, json_str: str) -> List:
        """Convert JSON string to list."""
        return json.loads(json_str) if json_str else []
    
    def save_position(self, position: Position) -> None:
        """
        Save a position to the database.
        
        Args:
            position: Position object to save
        """
        try:
            cursor = self.conn.cursor()
            
            # Convert lists and dictionaries to JSON
            metadata_json = self._dict_to_json(position.metadata)
            tags_json = self._convert_list_to_json(position.tags)
            
            # Prepare data for insertion/update
            position_data = (
                position.position_id,
                position.symbol,
                position.position_type.value,
                position.status.value,
                position.quantity,
                position.entry_price,
                position.current_price,
                position.exit_price,
                position.created_at.isoformat(),
                position.opened_at.isoformat() if position.opened_at else None,
                position.closed_at.isoformat() if position.closed_at else None,
                position.pnl,
                position.pnl_percentage,
                position.fees,
                position.strategy_id,
                position.source.value,
                position.risk_level.value,
                position.exchange,
                metadata_json,
                tags_json,
                position.notes
            )
            
            # Upsert position
            cursor.execute('''
            INSERT INTO positions (
                position_id, symbol, position_type, status, quantity,
                entry_price, current_price, exit_price, created_at, opened_at,
                closed_at, pnl, pnl_percentage, fees, strategy_id, source,
                risk_level, exchange, metadata, tags, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(position_id) DO UPDATE SET
                symbol = excluded.symbol,
                position_type = excluded.position_type,
                status = excluded.status,
                quantity = excluded.quantity,
                entry_price = excluded.entry_price,
                current_price = excluded.current_price,
                exit_price = excluded.exit_price,
                opened_at = excluded.opened_at,
                closed_at = excluded.closed_at,
                pnl = excluded.pnl,
                pnl_percentage = excluded.pnl_percentage,
                fees = excluded.fees,
                strategy_id = excluded.strategy_id,
                source = excluded.source,
                risk_level = excluded.risk_level,
                exchange = excluded.exchange,
                metadata = excluded.metadata,
                tags = excluded.tags,
                notes = excluded.notes
            ''', position_data)
            
            # Update position_orders link table for open_orders and close_orders
            cursor.execute('DELETE FROM position_orders WHERE position_id = ?', (position.position_id,))
            
            for order_id in position.open_orders:
                cursor.execute('INSERT INTO position_orders (position_id, order_id, order_type) VALUES (?, ?, ?)',
                             (position.position_id, order_id, 'open'))
            
            for order_id in position.close_orders:
                cursor.execute('INSERT INTO position_orders (position_id, order_id, order_type) VALUES (?, ?, ?)',
                             (position.position_id, order_id, 'close'))
            
            self.conn.commit()
            logger.debug(f"Saved position {position.position_id}")
        
        except sqlite3.Error as e:
            self.conn.rollback()
            logger.error(f"Error saving position {position.position_id}: {e}")
            raise
    
    def get_position(self, position_id: str) -> Optional[Position]:
        """
        Get a position by ID.
        
        Args:
            position_id: Position ID
            
        Returns:
            Position object or None if not found
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('SELECT * FROM positions WHERE position_id = ?', (position_id,))
            row = cursor.fetchone()
            
            if not row:
                return None
            
            # Get order lists from position_orders
            cursor.execute('SELECT order_id, order_type FROM position_orders WHERE position_id = ?', (position_id,))
            order_rows = cursor.fetchall()
            
            open_orders = [r['order_id'] for r in order_rows if r['order_type'] == 'open']
            close_orders = [r['order_id'] for r in order_rows if r['order_type'] == 'close']
            
            # Get triggers list
            cursor.execute('SELECT trigger_id FROM triggers WHERE position_id = ?', (position_id,))
            trigger_rows = cursor.fetchall()
            triggers = [r['trigger_id'] for r in trigger_rows]
            
            # Convert row to Position object
            position = Position(
                position_id=row['position_id'],
                symbol=row['symbol'],
                position_type=PositionType(row['position_type']),
                status=PositionStatus(row['status']),
                quantity=row['quantity'],
                entry_price=row['entry_price'],
                current_price=row['current_price'],
                exit_price=row['exit_price'],
                created_at=datetime.fromisoformat(row['created_at']),
                opened_at=datetime.fromisoformat(row['opened_at']) if row['opened_at'] else None,
                closed_at=datetime.fromisoformat(row['closed_at']) if row['closed_at'] else None,
                pnl=row['pnl'],
                pnl_percentage=row['pnl_percentage'],
                fees=row['fees'],
                strategy_id=row['strategy_id'],
                source=PositionSource(row['source']),
                risk_level=RiskLevel(row['risk_level']),
                exchange=row['exchange'],
                metadata=self._json_to_dict(row['metadata']),
                tags=self._convert_json_to_list(row['tags']),
                notes=row['notes'],
                open_orders=open_orders,
                close_orders=close_orders,
                triggers=triggers
            )
            
            return position
        
        except Exception as e:
            logger.error(f"Error retrieving position {position_id}: {e}")
            return None
    
    def delete_position(self, position_id: str) -> bool:
        """
        Delete a position and related data.
        
        Args:
            position_id: Position ID
            
        Returns:
            True if deleted, False otherwise
        """
        try:
            cursor = self.conn.cursor()
            
            # Delete related records first
            cursor.execute('DELETE FROM position_orders WHERE position_id = ?', (position_id,))
            cursor.execute('DELETE FROM triggers WHERE position_id = ?', (position_id,))
            cursor.execute('DELETE FROM orders WHERE position_id = ?', (position_id,))
            
            # Delete position
            cursor.execute('DELETE FROM positions WHERE position_id = ?', (position_id,))
            
            self.conn.commit()
            return cursor.rowcount > 0
        
        except sqlite3.Error as e:
            self.conn.rollback()
            logger.error(f"Error deleting position {position_id}: {e}")
            return False
    
    def get_positions(self, 
                   status: Optional[Union[str, List[str]]] = None,
                   symbol: Optional[str] = None,
                   strategy_id: Optional[str] = None,
                   limit: int = 1000,
                   offset: int = 0) -> List[Position]:
        """
        Get positions based on filter criteria.
        
        Args:
            status: Filter by status
            symbol: Filter by symbol
            strategy_id: Filter by strategy ID
            limit: Maximum number of positions to return
            offset: Offset for pagination
            
        Returns:
            List of Position objects
        """
        try:
            cursor = self.conn.cursor()
            
            # Build query conditions
            conditions = []
            params = []
            
            if status:
                if isinstance(status, list):
                    placeholders = ', '.join(['?'] * len(status))
                    conditions.append(f"status IN ({placeholders})")
                    params.extend(status)
                else:
                    conditions.append("status = ?")
                    params.append(status)
            
            if symbol:
                conditions.append("symbol = ?")
                params.append(symbol)
            
            if strategy_id:
                conditions.append("strategy_id = ?")
                params.append(strategy_id)
            
            # Construct the query
            query = "SELECT position_id FROM positions"
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            
            query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
            params.extend([limit, offset])
            
            # Execute query
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            # Get positions by ID
            positions = []
            for row in rows:
                position = self.get_position(row['position_id'])
                if position:
                    positions.append(position)
            
            return positions
        
        except sqlite3.Error as e:
            logger.error(f"Error retrieving positions: {e}")
            return []
    
    def count_positions(self, 
                      status: Optional[Union[str, List[str]]] = None,
                      symbol: Optional[str] = None,
                      strategy_id: Optional[str] = None) -> int:
        """
        Count positions based on filter criteria.
        
        Args:
            status: Filter by status
            symbol: Filter by symbol
            strategy_id: Filter by strategy ID
            
        Returns:
            Number of matching positions
        """
        try:
            cursor = self.conn.cursor()
            
            # Build query conditions
            conditions = []
            params = []
            
            if status:
                if isinstance(status, list):
                    placeholders = ', '.join(['?'] * len(status))
                    conditions.append(f"status IN ({placeholders})")
                    params.extend(status)
                else:
                    conditions.append("status = ?")
                    params.append(status)
            
            if symbol:
                conditions.append("symbol = ?")
                params.append(symbol)
            
            if strategy_id:
                conditions.append("strategy_id = ?")
                params.append(strategy_id)
            
            # Construct the query
            query = "SELECT COUNT(*) as count FROM positions"
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            
            # Execute query
            cursor.execute(query, params)
            result = cursor.fetchone()
            
            return result['count'] if result else 0
        
        except sqlite3.Error as e:
            logger.error(f"Error counting positions: {e}")
            return 0
    
    def save_order(self, order: Order) -> None:
        """
        Save an order to the database.
        
        Args:
            order: Order object to save
        """
        try:
            cursor = self.conn.cursor()
            
            # Convert metadata to JSON
            metadata_json = self._dict_to_json(order.metadata)
            
            # Prepare data for insertion/update
            order_data = (
                order.order_id,
                order.exchange_order_id,
                order.position_id,
                order.symbol,
                order.order_type.value,
                order.action.value,
                order.quantity,
                order.price,
                order.stop_price,
                order.status.value,
                order.created_at.isoformat(),
                order.updated_at.isoformat(),
                order.filled_quantity,
                order.average_fill_price,
                order.fees,
                order.fee_currency,
                order.error_message,
                order.exchange,
                metadata_json
            )
            
            # Upsert order
            cursor.execute('''
            INSERT INTO orders (
                order_id, exchange_order_id, position_id, symbol,
                order_type, action, quantity, price, stop_price,
                status, created_at, updated_at, filled_quantity,
                average_fill_price, fees, fee_currency, error_message,
                exchange, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(order_id) DO UPDATE SET
                exchange_order_id = excluded.exchange_order_id,
                position_id = excluded.position_id,
                symbol = excluded.symbol,
                order_type = excluded.order_type,
                action = excluded.action,
                quantity = excluded.quantity,
                price = excluded.price,
                stop_price = excluded.stop_price,
                status = excluded.status,
                updated_at = excluded.updated_at,
                filled_quantity = excluded.filled_quantity,
                average_fill_price = excluded.average_fill_price,
                fees = excluded.fees,
                fee_currency = excluded.fee_currency,
                error_message = excluded.error_message,
                exchange = excluded.exchange,
                metadata = excluded.metadata
            ''', order_data)
            
            self.conn.commit()
            logger.debug(f"Saved order {order.order_id}")
        
        except sqlite3.Error as e:
            self.conn.rollback()
            logger.error(f"Error saving order {order.order_id}: {e}")
            raise
    
    def get_order(self, order_id: str) -> Optional[Order]:
        """
        Get an order by ID.
        
        Args:
            order_id: Order ID
            
        Returns:
            Order object or None if not found
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('SELECT * FROM orders WHERE order_id = ?', (order_id,))
            row = cursor.fetchone()
            
            if not row:
                return None
            
            # Convert row to Order object
            order = Order(
                order_id=row['order_id'],
                exchange_order_id=row['exchange_order_id'],
                position_id=row['position_id'],
                symbol=row['symbol'],
                order_type=OrderType(row['order_type']),
                action=OrderAction(row['action']),
                quantity=row['quantity'],
                price=row['price'],
                stop_price=row['stop_price'],
                status=OrderStatus(row['status']),
                created_at=datetime.fromisoformat(row['created_at']),
                updated_at=datetime.fromisoformat(row['updated_at']),
                filled_quantity=row['filled_quantity'],
                average_fill_price=row['average_fill_price'],
                fees=row['fees'],
                fee_currency=row['fee_currency'],
                error_message=row['error_message'],
                exchange=row['exchange'],
                metadata=self._json_to_dict(row['metadata'])
            )
            
            return order
        
        except Exception as e:
            logger.error(f"Error retrieving order {order_id}: {e}")
            return None
    
    def get_orders(self, 
                 status: Optional[Union[str, List[str]]] = None,
                 symbol: Optional[str] = None,
                 position_id: Optional[str] = None,
                 limit: int = 1000,
                 offset: int = 0) -> List[Order]:
        """
        Get orders based on filter criteria.
        
        Args:
            status: Filter by status
            symbol: Filter by symbol
            position_id: Filter by position ID
            limit: Maximum number of orders to return
            offset: Offset for pagination
            
        Returns:
            List of Order objects
        """
        try:
            cursor = self.conn.cursor()
            
            # Build query conditions
            conditions = []
            params = []
            
            if status:
                if isinstance(status, list):
                    placeholders = ', '.join(['?'] * len(status))
                    conditions.append(f"status IN ({placeholders})")
                    params.extend(status)
                else:
                    conditions.append("status = ?")
                    params.append(status)
            
            if symbol:
                conditions.append("symbol = ?")
                params.append(symbol)
            
            if position_id:
                conditions.append("position_id = ?")
                params.append(position_id)
            
            # Construct the query
            query = "SELECT * FROM orders"
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            
            query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
            params.extend([limit, offset])
            
            # Execute query
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            # Convert rows to Order objects
            orders = []
            for row in rows:
                order = Order(
                    order_id=row['order_id'],
                    exchange_order_id=row['exchange_order_id'],
                    position_id=row['position_id'],
                    symbol=row['symbol'],
                    order_type=OrderType(row['order_type']),
                    action=OrderAction(row['action']),
                    quantity=row['quantity'],
                    price=row['price'],
                    stop_price=row['stop_price'],
                    status=OrderStatus(row['status']),
                    created_at=datetime.fromisoformat(row['created_at']),
                    updated_at=datetime.fromisoformat(row['updated_at']),
                    filled_quantity=row['filled_quantity'],
                    average_fill_price=row['average_fill_price'],
                    fees=row['fees'],
                    fee_currency=row['fee_currency'],
                    error_message=row['error_message'],
                    exchange=row['exchange'],
                    metadata=self._json_to_dict(row['metadata'])
                )
                orders.append(order)
            
            return orders
        
        except sqlite3.Error as e:
            logger.error(f"Error retrieving orders: {e}")
            return []
    
    def save_trigger(self, trigger: PositionTrigger) -> None:
        """
        Save a position trigger to the database.
        
        Args:
            trigger: PositionTrigger object to save
        """
        try:
            cursor = self.conn.cursor()
            
            # Convert metadata to JSON
            metadata_json = self._dict_to_json(trigger.metadata)
            
            # Prepare data for insertion/update
            trigger_data = (
                trigger.trigger_id,
                trigger.position_id,
                trigger.trigger_type.value,
                trigger.price,
                trigger.percentage,
                trigger.trail_value,
                trigger.expiration_time.isoformat() if trigger.expiration_time else None,
                1 if trigger.is_active else 0,
                trigger.created_at.isoformat(),
                trigger.triggered_at.isoformat() if trigger.triggered_at else None,
                metadata_json
            )
            
            # Upsert trigger
            cursor.execute('''
            INSERT INTO triggers (
                trigger_id, position_id, trigger_type, price, percentage,
                trail_value, expiration_time, is_active, created_at,
                triggered_at, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(trigger_id) DO UPDATE SET
                position_id = excluded.position_id,
                trigger_type = excluded.trigger_type,
                price = excluded.price,
                percentage = excluded.percentage,
                trail_value = excluded.trail_value,
                expiration_time = excluded.expiration_time,
                is_active = excluded.is_active,
                triggered_at = excluded.triggered_at,
                metadata = excluded.metadata
            ''', trigger_data)
            
            self.conn.commit()
            logger.debug(f"Saved trigger {trigger.trigger_id}")
        
        except sqlite3.Error as e:
            self.conn.rollback()
            logger.error(f"Error saving trigger {trigger.trigger_id}: {e}")
            raise
    
    def get_trigger(self, trigger_id: str) -> Optional[PositionTrigger]:
        """
        Get a trigger by ID.
        
        Args:
            trigger_id: Trigger ID
            
        Returns:
            PositionTrigger object or None if not found
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('SELECT * FROM triggers WHERE trigger_id = ?', (trigger_id,))
            row = cursor.fetchone()
            
            if not row:
                return None
            
            # Convert row to PositionTrigger object
            trigger = PositionTrigger(
                trigger_id=row['trigger_id'],
                position_id=row['position_id'],
                trigger_type=TriggerType(row['trigger_type']),
                price=row['price'],
                percentage=row['percentage'],
                trail_value=row['trail_value'],
                expiration_time=datetime.fromisoformat(row['expiration_time']) if row['expiration_time'] else None,
                is_active=bool(row['is_active']),
                created_at=datetime.fromisoformat(row['created_at']),
                triggered_at=datetime.fromisoformat(row['triggered_at']) if row['triggered_at'] else None,
                metadata=self._json_to_dict(row['metadata'])
            )
            
            return trigger
        
        except Exception as e:
            logger.error(f"Error retrieving trigger {trigger_id}: {e}")
            return None
    
    def get_triggers(self, 
                   position_id: Optional[str] = None,
                   is_active: Optional[bool] = None,
                   limit: int = 1000,
                   offset: int = 0) -> List[PositionTrigger]:
        """
        Get triggers based on filter criteria.
        
        Args:
            position_id: Filter by position ID
            is_active: Filter by active status
            limit: Maximum number of triggers to return
            offset: Offset for pagination
            
        Returns:
            List of PositionTrigger objects
        """
        try:
            cursor = self.conn.cursor()
            
            # Build query conditions
            conditions = []
            params = []
            
            if position_id:
                conditions.append("position_id = ?")
                params.append(position_id)
            
            if is_active is not None:
                conditions.append("is_active = ?")
                params.append(1 if is_active else 0)
            
            # Construct the query
            query = "SELECT * FROM triggers"
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            
            query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
            params.extend([limit, offset])
            
            # Execute query
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            # Convert rows to PositionTrigger objects
            triggers = []
            for row in rows:
                trigger = PositionTrigger(
                    trigger_id=row['trigger_id'],
                    position_id=row['position_id'],
                    trigger_type=TriggerType(row['trigger_type']),
                    price=row['price'],
                    percentage=row['percentage'],
                    trail_value=row['trail_value'],
                    expiration_time=datetime.fromisoformat(row['expiration_time']) if row['expiration_time'] else None,
                    is_active=bool(row['is_active']),
                    created_at=datetime.fromisoformat(row['created_at']),
                    triggered_at=datetime.fromisoformat(row['triggered_at']) if row['triggered_at'] else None,
                    metadata=self._json_to_dict(row['metadata'])
                )
                triggers.append(trigger)
            
            return triggers
        
        except sqlite3.Error as e:
            logger.error(f"Error retrieving triggers: {e}")
            return []
    
    def save_portfolio_snapshot(self, portfolio: PortfolioSummary) -> int:
        """
        Save a portfolio snapshot.
        
        Args:
            portfolio: PortfolioSummary object
            
        Returns:
            Snapshot ID
        """
        try:
            cursor = self.conn.cursor()
            
            # Combine exposure data
            exposure_data = {
                'exposure_per_symbol': portfolio.exposure_per_symbol,
                'exposure_per_strategy': portfolio.exposure_per_strategy
            }
            exposure_json = self._dict_to_json(exposure_data)
            
            # Prepare data for insertion
            snapshot_data = (
                portfolio.updated_at.isoformat(),
                portfolio.total_equity,
                portfolio.available_balance,
                portfolio.open_position_count,
                portfolio.total_position_value,
                portfolio.total_pnl,
                portfolio.total_pnl_percentage,
                portfolio.total_fees,
                portfolio.current_exposure,
                exposure_json
            )
            
            # Insert snapshot
            cursor.execute('''
            INSERT INTO portfolio_snapshots (
                timestamp, total_equity, available_balance, open_position_count,
                total_position_value, total_pnl, total_pnl_percentage,
                total_fees, current_exposure, exposure_data
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', snapshot_data)
            
            self.conn.commit()
            snapshot_id = cursor.lastrowid
            logger.debug(f"Saved portfolio snapshot {snapshot_id}")
            
            return snapshot_id
        
        except sqlite3.Error as e:
            self.conn.rollback()
            logger.error(f"Error saving portfolio snapshot: {e}")
            raise
    
    def get_latest_portfolio_snapshot(self) -> Optional[PortfolioSummary]:
        """
        Get the latest portfolio snapshot.
        
        Returns:
            PortfolioSummary object or None if not found
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('SELECT * FROM portfolio_snapshots ORDER BY timestamp DESC LIMIT 1')
            row = cursor.fetchone()
            
            if not row:
                return None
            
            # Convert exposure data
            exposure_data = self._json_to_dict(row['exposure_data'])
            
            # Create PortfolioSummary object
            portfolio = PortfolioSummary(
                total_equity=row['total_equity'],
                available_balance=row['available_balance'],
                open_position_count=row['open_position_count'],
                total_position_value=row['total_position_value'],
                total_pnl=row['total_pnl'],
                total_pnl_percentage=row['total_pnl_percentage'],
                total_fees=row['total_fees'],
                current_exposure=row['current_exposure'],
                exposure_per_symbol=exposure_data.get('exposure_per_symbol', {}),
                exposure_per_strategy=exposure_data.get('exposure_per_strategy', {}),
                updated_at=datetime.fromisoformat(row['timestamp'])
            )
            
            return portfolio
        
        except Exception as e:
            logger.error(f"Error retrieving latest portfolio snapshot: {e}")
            return None
    
    def get_portfolio_snapshots(self, 
                              start_time: Optional[datetime] = None,
                              end_time: Optional[datetime] = None,
                              limit: int = 100,
                              offset: int = 0) -> List[PortfolioSummary]:
        """
        Get portfolio snapshots for a time range.
        
        Args:
            start_time: Filter by start time
            end_time: Filter by end time
            limit: Maximum number of snapshots to return
            offset: Offset for pagination
            
        Returns:
            List of PortfolioSummary objects
        """
        try:
            cursor = self.conn.cursor()
            
            # Build query conditions
            conditions = []
            params = []
            
            if start_time:
                conditions.append("timestamp >= ?")
                params.append(start_time.isoformat())
            
            if end_time:
                conditions.append("timestamp <= ?")
                params.append(end_time.isoformat())
            
            # Construct the query
            query = "SELECT * FROM portfolio_snapshots"
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            
            query += " ORDER BY timestamp DESC LIMIT ? OFFSET ?"
            params.extend([limit, offset])
            
            # Execute query
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            # Convert rows to PortfolioSummary objects
            portfolios = []
            for row in rows:
                # Convert exposure data
                exposure_data = self._json_to_dict(row['exposure_data'])
                
                # Create PortfolioSummary object
                portfolio = PortfolioSummary(
                    total_equity=row['total_equity'],
                    available_balance=row['available_balance'],
                    open_position_count=row['open_position_count'],
                    total_position_value=row['total_position_value'],
                    total_pnl=row['total_pnl'],
                    total_pnl_percentage=row['total_pnl_percentage'],
                    total_fees=row['total_fees'],
                    current_exposure=row['current_exposure'],
                    exposure_per_symbol=exposure_data.get('exposure_per_symbol', {}),
                    exposure_per_strategy=exposure_data.get('exposure_per_strategy', {}),
                    updated_at=datetime.fromisoformat(row['timestamp'])
                )
                portfolios.append(portfolio)
            
            return portfolios
        
        except sqlite3.Error as e:
            logger.error(f"Error retrieving portfolio snapshots: {e}")
            return [] 