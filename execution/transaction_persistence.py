"""
Transaction State Persistence

This module provides transaction state persistence with SQLite for durability.
It enables reliable recovery of transaction state after system failures.
"""

import os
import json
import sqlite3
import logging
import asyncio
from enum import Enum
from typing import Dict, Any, List, Optional, Set, Union, Tuple
from datetime import datetime, timedelta

from trading_system.execution.transaction import TransactionContext, TransactionStatus, TransactionPhase
from trading_system.core.logging import get_logger

logger = get_logger("execution.transaction_persistence")

class TransactionStore:
    """
    Transaction state persistence store.
    
    This class provides persistence for transaction state using SQLite,
    enabling reliable recovery after system failures.
    """
    
    def __init__(self, db_path: str = "data/transactions.db"):
        """
        Initialize transaction store.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self._conn = None
        self._lock = asyncio.Lock()
        
        # Ensure directory exists if not using in-memory database
        if db_path != ":memory:" and db_path:
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # Initialize database
        self._init_db()
        
        logger.info(f"Transaction store initialized with database at {db_path}")
    
    def _init_db(self) -> None:
        """Initialize database schema."""
        conn = self._get_connection()
        
        try:
            cursor = conn.cursor()
            
            # Create transactions table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                id TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                symbol TEXT NOT NULL,
                amount REAL NOT NULL,
                price REAL,
                exchange TEXT NOT NULL,
                status TEXT NOT NULL,
                phase TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                timeout INTEGER NOT NULL,
                retries INTEGER NOT NULL,
                max_retries INTEGER NOT NULL,
                order_id TEXT,
                client_order_id TEXT,
                "order" TEXT,
                verification_attempts INTEGER NOT NULL,
                max_verification_attempts INTEGER NOT NULL,
                verification_delay INTEGER NOT NULL,
                metadata TEXT
            )
            ''')
            
            # Create index on status for faster querying
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status)')
            
            # Create index on exchange and symbol for faster querying
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_exchange_symbol ON transactions(exchange, symbol)')
            
            # Create index on updated_at for faster cleanup
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_updated_at ON transactions(updated_at)')
            
            # Create transaction logs table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS transaction_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                transaction_id TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                status TEXT NOT NULL,
                phase TEXT NOT NULL,
                details TEXT,
                FOREIGN KEY (transaction_id) REFERENCES transactions(id)
            )
            ''')
            
            # Create index on transaction_id for faster querying
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_transaction_logs_transaction_id ON transaction_logs(transaction_id)')
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"Error initializing transaction database: {e}", exc_info=True)
            conn.rollback()
            raise
    
    def _get_connection(self) -> sqlite3.Connection:
        """
        Get SQLite connection.
        
        Returns:
            sqlite3.Connection: Database connection
        """
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path)
            # Enable foreign key constraints
            self._conn.execute("PRAGMA foreign_keys = ON")
            # Configure connection
            self._conn.row_factory = sqlite3.Row
            
        return self._conn
    
    async def save_transaction(self, transaction: TransactionContext) -> bool:
        """
        Save transaction state to database.
        
        Args:
            transaction: Transaction context to save
            
        Returns:
            bool: True if saved successfully, False otherwise
        """
        async with self._lock:
            conn = self._get_connection()
            
            try:
                data = transaction.to_dict()
                
                # Debug log
                logger.info(f"Transaction data before conversion: {data}")
                
                # Convert complex objects to JSON strings
                if data.get("order"):
                    data["order"] = json.dumps(data["order"])
                
                if data.get("metadata"):
                    data["metadata"] = json.dumps(data["metadata"])
                
                # Ensure all fields are valid SQLite types
                # Convert any Enum values to strings if they still exist
                for key, value in list(data.items()):
                    if hasattr(value, 'value'):  # Check if it's an Enum
                        data[key] = value.value
                    # Explicitly handle None values for numeric fields
                    elif key in ['price'] and value is None:
                        # SQLite can store NULL directly
                        pass
                    # Handle dict or list that might not have been converted to JSON
                    elif isinstance(value, (dict, list)):
                        data[key] = json.dumps(value)
                
                # Debug log after conversion
                logger.info(f"Transaction data after conversion: {data}")
                
                cursor = conn.cursor()
                
                # Check if transaction already exists
                cursor.execute("SELECT id FROM transactions WHERE id = ?", (transaction.id,))
                exists = cursor.fetchone() is not None
                
                if exists:
                    # Update existing transaction
                    params = (
                        data["type"],
                        data["symbol"],
                        data["amount"],
                        data["price"],
                        data["exchange"],
                        data["status"],
                        data["phase"],
                        data["updated_at"],
                        data["timeout"],
                        data["retries"],
                        data["max_retries"],
                        data["order_id"],
                        data["client_order_id"],
                        data["order"],
                        data["verification_attempts"],
                        data["max_verification_attempts"],
                        data["verification_delay"],
                        data["metadata"],
                        data["id"]
                    )
                    
                    # Debug log parameter types
                    logger.info(f"Update parameters: {[(i, type(p), p) for i, p in enumerate(params)]}")
                    
                    cursor.execute('''
                    UPDATE transactions SET
                        type = ?,
                        symbol = ?,
                        amount = ?,
                        price = ?,
                        exchange = ?,
                        status = ?,
                        phase = ?,
                        updated_at = ?,
                        timeout = ?,
                        retries = ?,
                        max_retries = ?,
                        order_id = ?,
                        client_order_id = ?,
                        "order" = ?,
                        verification_attempts = ?,
                        max_verification_attempts = ?,
                        verification_delay = ?,
                        metadata = ?
                    WHERE id = ?
                    ''', params)
                else:
                    # Insert new transaction
                    params = (
                        data["id"],
                        data["type"],
                        data["symbol"],
                        data["amount"],
                        data["price"],
                        data["exchange"],
                        data["status"],
                        data["phase"],
                        data["created_at"],
                        data["updated_at"],
                        data["timeout"],
                        data["retries"],
                        data["max_retries"],
                        data["order_id"],
                        data["client_order_id"],
                        data["order"],
                        data["verification_attempts"],
                        data["max_verification_attempts"],
                        data["verification_delay"],
                        data["metadata"]
                    )
                    
                    # Debug log parameter types
                    logger.info(f"Insert parameters: {[(i, type(p), p) for i, p in enumerate(params)]}")
                    
                    cursor.execute('''
                    INSERT INTO transactions VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    ''', params)
                
                # Add log entry
                cursor.execute('''
                INSERT INTO transaction_logs (
                    transaction_id, timestamp, status, phase, details
                ) VALUES (?, ?, ?, ?, ?)
                ''', (
                    data["id"],
                    data["updated_at"],
                    data["status"],
                    data["phase"],
                    json.dumps({
                        "order_id": data["order_id"],
                        "retries": data["retries"],
                        "verification_attempts": data["verification_attempts"]
                    })
                ))
                
                conn.commit()
                return True
                
            except Exception as e:
                logger.error(f"Error saving transaction {transaction.id}: {e}", exc_info=True)
                conn.rollback()
                return False
    
    async def load_transaction(self, transaction_id: str) -> Optional[TransactionContext]:
        """
        Load transaction by ID.
        
        Args:
            transaction_id: Transaction ID to load
            
        Returns:
            TransactionContext: Loaded transaction context or None if not found
        """
        async with self._lock:
            conn = self._get_connection()
            
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM transactions WHERE id = ?", (transaction_id,))
                row = cursor.fetchone()
                
                if not row:
                    return None
                
                # Convert to dict
                data = dict(row)
                
                # Parse JSON fields
                if data["order"]:
                    data["order"] = json.loads(data["order"])
                
                if data["metadata"]:
                    data["metadata"] = json.loads(data["metadata"])
                
                # Create transaction context
                return TransactionContext.from_dict(data)
                
            except Exception as e:
                logger.error(f"Error loading transaction {transaction_id}: {e}", exc_info=True)
                return None
    
    async def delete_transaction(self, transaction_id: str) -> bool:
        """
        Delete transaction by ID.
        
        Args:
            transaction_id: Transaction ID to delete
            
        Returns:
            bool: True if deleted successfully, False otherwise
        """
        async with self._lock:
            conn = self._get_connection()
            
            try:
                cursor = conn.cursor()
                
                # Delete transaction logs first (due to foreign key constraint)
                cursor.execute("DELETE FROM transaction_logs WHERE transaction_id = ?", (transaction_id,))
                
                # Delete transaction
                cursor.execute("DELETE FROM transactions WHERE id = ?", (transaction_id,))
                
                conn.commit()
                return cursor.rowcount > 0
                
            except Exception as e:
                logger.error(f"Error deleting transaction {transaction_id}: {e}", exc_info=True)
                conn.rollback()
                return False
    
    async def get_transaction_logs(self, transaction_id: str) -> List[Dict[str, Any]]:
        """
        Get transaction logs by transaction ID.
        
        Args:
            transaction_id: Transaction ID
            
        Returns:
            List[Dict[str, Any]]: List of transaction log entries
        """
        async with self._lock:
            conn = self._get_connection()
            
            try:
                cursor = conn.cursor()
                cursor.execute('''
                SELECT * FROM transaction_logs 
                WHERE transaction_id = ? 
                ORDER BY timestamp ASC
                ''', (transaction_id,))
                
                logs = []
                for row in cursor.fetchall():
                    log = dict(row)
                    if log["details"]:
                        log["details"] = json.loads(log["details"])
                    logs.append(log)
                
                return logs
                
            except Exception as e:
                logger.error(f"Error getting logs for transaction {transaction_id}: {e}", exc_info=True)
                return []
    
    async def get_transactions_by_status(self, status: TransactionStatus) -> List[TransactionContext]:
        """
        Get transactions by status.
        
        Args:
            status: Transaction status to filter by
            
        Returns:
            List[TransactionContext]: List of matching transactions
        """
        async with self._lock:
            conn = self._get_connection()
            
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM transactions WHERE status = ?", (status.value,))
                
                transactions = []
                for row in cursor.fetchall():
                    data = dict(row)
                    
                    # Parse JSON fields
                    if data["order"]:
                        data["order"] = json.loads(data["order"])
                    
                    if data["metadata"]:
                        data["metadata"] = json.loads(data["metadata"])
                    
                    # Create transaction context
                    transactions.append(TransactionContext.from_dict(data))
                
                return transactions
                
            except Exception as e:
                logger.error(f"Error getting transactions with status {status.value}: {e}", exc_info=True)
                return []
    
    async def get_pending_transactions(self) -> List[TransactionContext]:
        """
        Get all pending transactions.
        
        Returns:
            List[TransactionContext]: List of pending transactions
        """
        # Combine in-progress and pending transactions
        in_progress = await self.get_transactions_by_status(TransactionStatus.IN_PROGRESS)
        pending = await self.get_transactions_by_status(TransactionStatus.PENDING)
        
        return in_progress + pending
    
    async def get_transactions_by_exchange_symbol(self, exchange: str, symbol: str) -> List[TransactionContext]:
        """
        Get transactions by exchange and symbol.
        
        Args:
            exchange: Exchange name
            symbol: Symbol name
            
        Returns:
            List[TransactionContext]: List of matching transactions
        """
        async with self._lock:
            conn = self._get_connection()
            
            try:
                cursor = conn.cursor()
                cursor.execute('''
                SELECT * FROM transactions 
                WHERE exchange = ? AND symbol = ?
                ORDER BY created_at DESC
                ''', (exchange, symbol))
                
                transactions = []
                for row in cursor.fetchall():
                    data = dict(row)
                    
                    # Parse JSON fields
                    if data["order"]:
                        data["order"] = json.loads(data["order"])
                    
                    if data["metadata"]:
                        data["metadata"] = json.loads(data["metadata"])
                    
                    # Create transaction context
                    transactions.append(TransactionContext.from_dict(data))
                
                return transactions
                
            except Exception as e:
                logger.error(f"Error getting transactions for {exchange}/{symbol}: {e}", exc_info=True)
                return []
    
    async def cleanup_old_transactions(self, max_age_seconds: int = 86400) -> int:
        """
        Clean up old completed/failed transactions.
        
        Args:
            max_age_seconds: Maximum age in seconds to keep transactions
            
        Returns:
            int: Number of transactions deleted
        """
        async with self._lock:
            conn = self._get_connection()
            
            try:
                # Calculate cutoff time
                cutoff_time = int(datetime.now().timestamp() * 1000) - max_age_seconds * 1000
                
                cursor = conn.cursor()
                
                # Get IDs of transactions to delete
                cursor.execute('''
                SELECT id FROM transactions 
                WHERE updated_at < ? 
                AND status IN (?, ?, ?, ?)
                ''', (
                    cutoff_time,
                    TransactionStatus.COMPLETED.value,
                    TransactionStatus.FAILED.value,
                    TransactionStatus.ABORTED.value,
                    TransactionStatus.ROLLED_BACK.value
                ))
                
                to_delete = [row[0] for row in cursor.fetchall()]
                deleted = 0
                
                # Delete each transaction
                for transaction_id in to_delete:
                    # Delete transaction logs first (due to foreign key constraint)
                    cursor.execute("DELETE FROM transaction_logs WHERE transaction_id = ?", (transaction_id,))
                    
                    # Delete transaction
                    cursor.execute("DELETE FROM transactions WHERE id = ?", (transaction_id,))
                    deleted += 1
                
                conn.commit()
                
                if deleted > 0:
                    logger.info(f"Cleaned up {deleted} old transactions")
                
                return deleted
                
            except Exception as e:
                logger.error(f"Error cleaning up old transactions: {e}", exc_info=True)
                conn.rollback()
                return 0
    
    async def close(self) -> None:
        """Close the database connection."""
        async with self._lock:
            if self._conn:
                self._conn.close()
                self._conn = None 