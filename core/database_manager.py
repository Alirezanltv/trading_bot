"""
Database Manager for Trading System

This module provides database management functionality for the trading system.
"""

import os
import sqlite3
import json
import time
import logging
import asyncio
from typing import Dict, Any, List, Optional, Tuple, Union
from concurrent.futures import ThreadPoolExecutor

from trading_system.core.component import Component, ComponentStatus
from trading_system.core.logging import get_logger

logger = get_logger("core.database")

class DatabaseManager(Component):
    """
    Database Manager
    
    Handles database operations for the trading system.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the database manager.
        
        Args:
            config: Database configuration
        """
        super().__init__(name="DatabaseManager")
        
        self.config = config or {}
        
        # Database connection
        self.db_path = self.config.get("db_path", "trading_system.db")
        self.connection = None
        self.cursor = None
        
        # Thread pool for async operations
        self.thread_pool = ThreadPoolExecutor(max_workers=4)
        
        # Schema definition
        self.tables = {
            "trades": """
                CREATE TABLE IF NOT EXISTS trades (
                    id TEXT PRIMARY KEY,
                    strategy_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    entry_price REAL,
                    exit_price REAL,
                    entry_time INTEGER,
                    exit_time INTEGER,
                    amount REAL,
                    profit_loss REAL,
                    status TEXT,
                    metadata TEXT,
                    created_at INTEGER,
                    updated_at INTEGER
                )
            """,
            "orders": """
                CREATE TABLE IF NOT EXISTS orders (
                    id TEXT PRIMARY KEY,
                    trade_id TEXT,
                    exchange_id TEXT,
                    symbol TEXT NOT NULL,
                    type TEXT NOT NULL,
                    side TEXT NOT NULL,
                    amount REAL,
                    price REAL,
                    status TEXT,
                    filled_amount REAL,
                    average_price REAL,
                    fee REAL,
                    created_at INTEGER,
                    updated_at INTEGER,
                    FOREIGN KEY (trade_id) REFERENCES trades(id)
                )
            """,
            "market_data": """
                CREATE TABLE IF NOT EXISTS market_data (
                    id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL,
                    indicators TEXT,
                    created_at INTEGER
                )
            """,
            "strategies": """
                CREATE TABLE IF NOT EXISTS strategies (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT,
                    config TEXT,
                    status TEXT,
                    created_at INTEGER,
                    updated_at INTEGER
                )
            """,
            "backtest_results": """
                CREATE TABLE IF NOT EXISTS backtest_results (
                    id TEXT PRIMARY KEY,
                    strategy_id TEXT NOT NULL,
                    start_time INTEGER,
                    end_time INTEGER,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    trades_count INTEGER,
                    winning_trades INTEGER,
                    losing_trades INTEGER,
                    profit_loss REAL,
                    win_rate REAL,
                    max_drawdown REAL,
                    sharpe_ratio REAL,
                    results TEXT,
                    created_at INTEGER,
                    FOREIGN KEY (strategy_id) REFERENCES strategies(id)
                )
            """,
            "system_logs": """
                CREATE TABLE IF NOT EXISTS system_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp INTEGER NOT NULL,
                    level TEXT NOT NULL,
                    component TEXT,
                    message TEXT,
                    details TEXT
                )
            """,
            "exchange_info": """
                CREATE TABLE IF NOT EXISTS exchange_info (
                    id TEXT PRIMARY KEY,
                    exchange_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    status TEXT,
                    symbols TEXT,
                    fees TEXT,
                    limits TEXT,
                    updated_at INTEGER
                )
            """
        }
    
    async def initialize(self) -> bool:
        """
        Initialize the database manager.
        
        Returns:
            Initialization success
        """
        try:
            self._update_status(ComponentStatus.INITIALIZING)
            logger.info(f"Initializing DatabaseManager with {self.db_path}")
            
            # Ensure directory exists
            db_dir = os.path.dirname(self.db_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir)
            
            # Connect to database
            await self._connect()
            
            # Create tables
            for table_name, create_sql in self.tables.items():
                await self._execute(create_sql)
            
            self._update_status(ComponentStatus.INITIALIZED)
            logger.info("DatabaseManager initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing DatabaseManager: {e}", exc_info=True)
            self._update_status(ComponentStatus.ERROR, str(e))
            return False
    
    async def start(self) -> bool:
        """
        Start the database manager.
        
        Returns:
            Start success
        """
        try:
            logger.info("Starting DatabaseManager")
            self._update_status(ComponentStatus.OPERATIONAL)
            return True
            
        except Exception as e:
            logger.error(f"Error starting DatabaseManager: {e}", exc_info=True)
            self._update_status(ComponentStatus.ERROR, str(e))
            return False
    
    async def stop(self) -> bool:
        """
        Stop the database manager.
        
        Returns:
            Stop success
        """
        try:
            logger.info("Stopping DatabaseManager")
            
            # Close database connection
            if self.connection:
                await self._close()
            
            # Shutdown thread pool
            self.thread_pool.shutdown()
            
            self._update_status(ComponentStatus.STOPPED)
            return True
            
        except Exception as e:
            logger.error(f"Error stopping DatabaseManager: {e}", exc_info=True)
            self._update_status(ComponentStatus.ERROR, str(e))
            return False
    
    async def _connect(self) -> None:
        """Connect to the database."""
        def _connect_sync():
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            return conn
        
        self.connection = await asyncio.get_event_loop().run_in_executor(
            self.thread_pool, _connect_sync
        )
        self.cursor = self.connection.cursor()
        logger.debug(f"Connected to database: {self.db_path}")
    
    async def _close(self) -> None:
        """Close the database connection."""
        if self.connection:
            def _close_sync(conn):
                conn.close()
            
            await asyncio.get_event_loop().run_in_executor(
                self.thread_pool, _close_sync, self.connection
            )
            self.connection = None
            self.cursor = None
            logger.debug("Database connection closed")
    
    async def _execute(self, query: str, params: tuple = ()) -> Any:
        """
        Execute a SQL query.
        
        Args:
            query: SQL query
            params: Query parameters
            
        Returns:
            Query result
        """
        if not self.connection:
            await self._connect()
        
        def _execute_sync(conn, query, params):
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            return cursor
        
        cursor = await asyncio.get_event_loop().run_in_executor(
            self.thread_pool, _execute_sync, self.connection, query, params
        )
        return cursor
    
    async def _fetch_one(self, query: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
        """
        Fetch a single row from the database.
        
        Args:
            query: SQL query
            params: Query parameters
            
        Returns:
            Row data or None
        """
        cursor = await self._execute(query, params)
        
        def _fetchone_sync(cursor):
            row = cursor.fetchone()
            if row:
                return dict(row)
            return None
        
        result = await asyncio.get_event_loop().run_in_executor(
            self.thread_pool, _fetchone_sync, cursor
        )
        return result
    
    async def _fetch_all(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """
        Fetch all rows from the database.
        
        Args:
            query: SQL query
            params: Query parameters
            
        Returns:
            List of row data
        """
        cursor = await self._execute(query, params)
        
        def _fetchall_sync(cursor):
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        
        results = await asyncio.get_event_loop().run_in_executor(
            self.thread_pool, _fetchall_sync, cursor
        )
        return results
    
    async def save_trade(self, trade_data: Dict[str, Any]) -> bool:
        """
        Save trade data to the database.
        
        Args:
            trade_data: Trade data
            
        Returns:
            Success status
        """
        try:
            trade_id = trade_data.get("id")
            if not trade_id:
                logger.error("Trade data missing ID")
                return False
            
            # Convert metadata to JSON string
            metadata = trade_data.get("metadata", {})
            if metadata and isinstance(metadata, dict):
                trade_data["metadata"] = json.dumps(metadata)
            
            # Set timestamps
            current_time = int(time.time() * 1000)
            if "created_at" not in trade_data:
                trade_data["created_at"] = current_time
            trade_data["updated_at"] = current_time
            
            # Check if trade exists
            existing_trade = await self._fetch_one(
                "SELECT id FROM trades WHERE id = ?", 
                (trade_id,)
            )
            
            if existing_trade:
                # Update existing trade
                fields = []
                values = []
                
                for key, value in trade_data.items():
                    if key != "id":
                        fields.append(f"{key} = ?")
                        values.append(value)
                
                values.append(trade_id)
                
                query = f"UPDATE trades SET {', '.join(fields)} WHERE id = ?"
                await self._execute(query, tuple(values))
                logger.debug(f"Updated trade: {trade_id}")
            else:
                # Insert new trade
                fields = list(trade_data.keys())
                placeholders = ["?"] * len(fields)
                
                query = f"INSERT INTO trades ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
                await self._execute(query, tuple(trade_data.values()))
                logger.debug(f"Inserted new trade: {trade_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving trade: {e}", exc_info=True)
            return False
    
    async def get_trade(self, trade_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a trade from the database.
        
        Args:
            trade_id: Trade ID
            
        Returns:
            Trade data or None
        """
        try:
            trade = await self._fetch_one(
                "SELECT * FROM trades WHERE id = ?", 
                (trade_id,)
            )
            
            if trade and "metadata" in trade and trade["metadata"]:
                try:
                    trade["metadata"] = json.loads(trade["metadata"])
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON in trade metadata: {trade_id}")
            
            return trade
            
        except Exception as e:
            logger.error(f"Error getting trade: {e}", exc_info=True)
            return None
    
    async def get_trades(self, filters: Dict[str, Any] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get trades from the database.
        
        Args:
            filters: Query filters
            limit: Maximum number of results
            
        Returns:
            List of trade data
        """
        try:
            query = "SELECT * FROM trades"
            params = []
            
            if filters:
                where_clauses = []
                
                for key, value in filters.items():
                    if key in ["strategy_id", "symbol", "direction", "status"]:
                        where_clauses.append(f"{key} = ?")
                        params.append(value)
                
                if where_clauses:
                    query += f" WHERE {' AND '.join(where_clauses)}"
            
            query += f" ORDER BY created_at DESC LIMIT {limit}"
            
            trades = await self._fetch_all(query, tuple(params))
            
            # Parse metadata JSON
            for trade in trades:
                if "metadata" in trade and trade["metadata"]:
                    try:
                        trade["metadata"] = json.loads(trade["metadata"])
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON in trade metadata: {trade['id']}")
            
            return trades
            
        except Exception as e:
            logger.error(f"Error getting trades: {e}", exc_info=True)
            return []
    
    async def save_order(self, order_data: Dict[str, Any]) -> bool:
        """
        Save order data to the database.
        
        Args:
            order_data: Order data
            
        Returns:
            Success status
        """
        try:
            order_id = order_data.get("id")
            if not order_id:
                logger.error("Order data missing ID")
                return False
            
            # Set timestamps
            current_time = int(time.time() * 1000)
            if "created_at" not in order_data:
                order_data["created_at"] = current_time
            order_data["updated_at"] = current_time
            
            # Check if order exists
            existing_order = await self._fetch_one(
                "SELECT id FROM orders WHERE id = ?", 
                (order_id,)
            )
            
            if existing_order:
                # Update existing order
                fields = []
                values = []
                
                for key, value in order_data.items():
                    if key != "id":
                        fields.append(f"{key} = ?")
                        values.append(value)
                
                values.append(order_id)
                
                query = f"UPDATE orders SET {', '.join(fields)} WHERE id = ?"
                await self._execute(query, tuple(values))
                logger.debug(f"Updated order: {order_id}")
            else:
                # Insert new order
                fields = list(order_data.keys())
                placeholders = ["?"] * len(fields)
                
                query = f"INSERT INTO orders ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
                await self._execute(query, tuple(order_data.values()))
                logger.debug(f"Inserted new order: {order_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving order: {e}", exc_info=True)
            return False
    
    async def get_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        """
        Get an order from the database.
        
        Args:
            order_id: Order ID
            
        Returns:
            Order data or None
        """
        try:
            order = await self._fetch_one(
                "SELECT * FROM orders WHERE id = ?", 
                (order_id,)
            )
            return order
            
        except Exception as e:
            logger.error(f"Error getting order: {e}", exc_info=True)
            return None
    
    async def get_orders(self, filters: Dict[str, Any] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get orders from the database.
        
        Args:
            filters: Query filters
            limit: Maximum number of results
            
        Returns:
            List of order data
        """
        try:
            query = "SELECT * FROM orders"
            params = []
            
            if filters:
                where_clauses = []
                
                for key, value in filters.items():
                    if key in ["trade_id", "exchange_id", "symbol", "type", "side", "status"]:
                        where_clauses.append(f"{key} = ?")
                        params.append(value)
                
                if where_clauses:
                    query += f" WHERE {' AND '.join(where_clauses)}"
            
            query += f" ORDER BY created_at DESC LIMIT {limit}"
            
            orders = await self._fetch_all(query, tuple(params))
            return orders
            
        except Exception as e:
            logger.error(f"Error getting orders: {e}", exc_info=True)
            return []
    
    async def save_market_data(self, market_data: Dict[str, Any]) -> bool:
        """
        Save market data to the database.
        
        Args:
            market_data: Market data
            
        Returns:
            Success status
        """
        try:
            # Required fields
            symbol = market_data.get("symbol")
            timeframe = market_data.get("timeframe")
            timestamp = market_data.get("timestamp")
            
            if not all([symbol, timeframe, timestamp]):
                logger.error("Market data missing required fields")
                return False
            
            # Generate ID if not provided
            if "id" not in market_data:
                market_data["id"] = f"{symbol}_{timeframe}_{timestamp}"
            
            # Convert indicators to JSON string
            indicators = market_data.get("indicators", {})
            if indicators and isinstance(indicators, dict):
                market_data["indicators"] = json.dumps(indicators)
            
            # Set creation timestamp
            current_time = int(time.time() * 1000)
            if "created_at" not in market_data:
                market_data["created_at"] = current_time
            
            # Check if data exists
            existing_data = await self._fetch_one(
                "SELECT id FROM market_data WHERE symbol = ? AND timeframe = ? AND timestamp = ?", 
                (symbol, timeframe, timestamp)
            )
            
            if existing_data:
                # Update existing data
                fields = []
                values = []
                
                for key, value in market_data.items():
                    if key not in ["symbol", "timeframe", "timestamp"]:
                        fields.append(f"{key} = ?")
                        values.append(value)
                
                values.extend([symbol, timeframe, timestamp])
                
                query = f"UPDATE market_data SET {', '.join(fields)} WHERE symbol = ? AND timeframe = ? AND timestamp = ?"
                await self._execute(query, tuple(values))
                logger.debug(f"Updated market data: {symbol}/{timeframe}/{timestamp}")
            else:
                # Insert new data
                fields = list(market_data.keys())
                placeholders = ["?"] * len(fields)
                
                query = f"INSERT INTO market_data ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
                await self._execute(query, tuple(market_data.values()))
                logger.debug(f"Inserted new market data: {symbol}/{timeframe}/{timestamp}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving market data: {e}", exc_info=True)
            return False
    
    async def get_market_data(self, symbol: str, timeframe: str, start_time: int = None, end_time: int = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get market data from the database.
        
        Args:
            symbol: Symbol
            timeframe: Timeframe
            start_time: Start timestamp (milliseconds)
            end_time: End timestamp (milliseconds)
            limit: Maximum number of results
            
        Returns:
            List of market data
        """
        try:
            query = "SELECT * FROM market_data WHERE symbol = ? AND timeframe = ?"
            params = [symbol, timeframe]
            
            if start_time:
                query += " AND timestamp >= ?"
                params.append(start_time)
            
            if end_time:
                query += " AND timestamp <= ?"
                params.append(end_time)
            
            query += " ORDER BY timestamp ASC LIMIT ?"
            params.append(limit)
            
            data = await self._fetch_all(query, tuple(params))
            
            # Parse indicators JSON
            for item in data:
                if "indicators" in item and item["indicators"]:
                    try:
                        item["indicators"] = json.loads(item["indicators"])
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON in market data indicators: {item['id']}")
            
            return data
            
        except Exception as e:
            logger.error(f"Error getting market data: {e}", exc_info=True)
            return []
    
    async def save_backtest_result(self, result_data: Dict[str, Any]) -> bool:
        """
        Save backtest result to the database.
        
        Args:
            result_data: Backtest result data
            
        Returns:
            Success status
        """
        try:
            result_id = result_data.get("id")
            if not result_id:
                logger.error("Backtest result data missing ID")
                return False
            
            # Convert results to JSON string
            results = result_data.get("results", {})
            if results and isinstance(results, dict):
                result_data["results"] = json.dumps(results)
            
            # Set creation timestamp
            current_time = int(time.time() * 1000)
            if "created_at" not in result_data:
                result_data["created_at"] = current_time
            
            # Check if result exists
            existing_result = await self._fetch_one(
                "SELECT id FROM backtest_results WHERE id = ?", 
                (result_id,)
            )
            
            if existing_result:
                # Update existing result
                fields = []
                values = []
                
                for key, value in result_data.items():
                    if key != "id":
                        fields.append(f"{key} = ?")
                        values.append(value)
                
                values.append(result_id)
                
                query = f"UPDATE backtest_results SET {', '.join(fields)} WHERE id = ?"
                await self._execute(query, tuple(values))
                logger.debug(f"Updated backtest result: {result_id}")
            else:
                # Insert new result
                fields = list(result_data.keys())
                placeholders = ["?"] * len(fields)
                
                query = f"INSERT INTO backtest_results ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
                await self._execute(query, tuple(result_data.values()))
                logger.debug(f"Inserted new backtest result: {result_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving backtest result: {e}", exc_info=True)
            return False
    
    async def get_backtest_results(self, strategy_id: str = None, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get backtest results from the database.
        
        Args:
            strategy_id: Strategy ID
            limit: Maximum number of results
            
        Returns:
            List of backtest results
        """
        try:
            query = "SELECT * FROM backtest_results"
            params = []
            
            if strategy_id:
                query += " WHERE strategy_id = ?"
                params.append(strategy_id)
            
            query += f" ORDER BY created_at DESC LIMIT {limit}"
            
            results = await self._fetch_all(query, tuple(params))
            
            # Parse results JSON
            for result in results:
                if "results" in result and result["results"]:
                    try:
                        result["results"] = json.loads(result["results"])
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON in backtest results: {result['id']}")
            
            return results
            
        except Exception as e:
            logger.error(f"Error getting backtest results: {e}", exc_info=True)
            return []
    
    async def log_system_event(self, level: str, component: str, message: str, details: Dict[str, Any] = None) -> bool:
        """
        Log a system event to the database.
        
        Args:
            level: Log level
            component: Component name
            message: Log message
            details: Additional details
            
        Returns:
            Success status
        """
        try:
            current_time = int(time.time() * 1000)
            
            # Convert details to JSON string
            details_json = None
            if details:
                details_json = json.dumps(details)
            
            await self._execute(
                "INSERT INTO system_logs (timestamp, level, component, message, details) VALUES (?, ?, ?, ?, ?)",
                (current_time, level, component, message, details_json)
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error logging system event: {e}", exc_info=True)
            return False
    
    async def get_system_logs(self, level: str = None, component: str = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get system logs from the database.
        
        Args:
            level: Log level filter
            component: Component filter
            limit: Maximum number of logs
            
        Returns:
            List of system logs
        """
        try:
            query = "SELECT * FROM system_logs"
            params = []
            
            where_clauses = []
            
            if level:
                where_clauses.append("level = ?")
                params.append(level)
            
            if component:
                where_clauses.append("component = ?")
                params.append(component)
            
            if where_clauses:
                query += f" WHERE {' AND '.join(where_clauses)}"
            
            query += f" ORDER BY timestamp DESC LIMIT {limit}"
            
            logs = await self._fetch_all(query, tuple(params))
            
            # Parse details JSON
            for log in logs:
                if "details" in log and log["details"]:
                    try:
                        log["details"] = json.loads(log["details"])
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON in log details: {log['id']}")
            
            return logs
            
        except Exception as e:
            logger.error(f"Error getting system logs: {e}", exc_info=True)
            return [] 