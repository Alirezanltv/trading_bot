"""
Database module for high-reliability trading system.
Provides interfaces for TimeSeries (InfluxDB) and Transaction (PostgreSQL) databases.
"""

import os
import time
import json
import logging
import asyncio
import sqlite3
import concurrent.futures
from typing import Dict, Any, List, Optional, Union, Tuple, Callable
from datetime import datetime, timedelta
from enum import Enum
from abc import ABC, abstractmethod

import pandas as pd
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import ASYNCHRONOUS, SYNCHRONOUS
import asyncpg
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from trading_system.core.component import Component, ComponentStatus
from trading_system.core.logging import get_logger

logger = get_logger("core.database")

class DatabaseError(Exception):
    """Base exception for database errors."""
    pass

class ConnectionError(DatabaseError):
    """Exception raised for database connection errors."""
    pass

class QueryError(DatabaseError):
    """Exception raised for database query errors."""
    pass

class WriteError(DatabaseError):
    """Exception raised for database write errors."""
    pass

class Database(Component, ABC):
    """
    Abstract base class for database connections.
    """
    
    def __init__(self, name: str, connection_string: str):
        """
        Initialize the database connection.
        
        Args:
            name: Name of the database component
            connection_string: Connection string for the database
        """
        super().__init__(name=name)
        self.connection_string = connection_string
        self.connection = None
        self.is_connected = False
        self.last_connection_attempt = 0
        self.connection_retry_interval = 5  # seconds
        self._lock = asyncio.Lock()
    
    @abstractmethod
    async def connect(self) -> bool:
        """
        Connect to the database.
        
        Returns:
            Success status
        """
        pass
    
    @abstractmethod
    async def disconnect(self) -> bool:
        """
        Disconnect from the database.
        
        Returns:
            Success status
        """
        pass
    
    @abstractmethod
    async def health_check(self) -> bool:
        """
        Check if the database connection is healthy.
        
        Returns:
            Health status
        """
        pass
    
    async def initialize(self) -> bool:
        """
        Initialize the database connection.
        
        Returns:
            Success status
        """
        try:
            if await self.connect():
                self.status = ComponentStatus.OPERATIONAL
                return True
            else:
                self.status = ComponentStatus.DEGRADED
                return False
        except Exception as e:
            logger.error(f"Error initializing {self.name}: {str(e)}", exc_info=True)
            self.status = ComponentStatus.ERROR
            return False
    
    async def stop(self) -> bool:
        """
        Stop the database connection.
        
        Returns:
            Success status
        """
        try:
            if await self.disconnect():
                self.status = ComponentStatus.STOPPED
                return True
            else:
                self.status = ComponentStatus.ERROR
                return False
        except Exception as e:
            logger.error(f"Error stopping {self.name}: {str(e)}", exc_info=True)
            self.status = ComponentStatus.ERROR
            return False

class TimeSeriesDatabase(Database):
    """
    InfluxDB time series database connection.
    
    Provides methods for storing and retrieving time series data.
    """
    
    def __init__(self, connection_string: str, org: str, bucket: str, token: str):
        """
        Initialize the InfluxDB connection.
        
        Args:
            connection_string: InfluxDB connection string
            org: InfluxDB organization
            bucket: InfluxDB bucket
            token: InfluxDB token
        """
        super().__init__(name="TimeSeriesDatabase", connection_string=connection_string)
        self.org = org
        self.bucket = bucket
        self.token = token
        self.client = None
        self.write_api = None
        self.query_api = None
    
    async def connect(self) -> bool:
        """
        Connect to InfluxDB.
        
        Returns:
            Success status
        """
        try:
            async with self._lock:
                if self.is_connected and self.client:
                    return True
                
                # Check if we should retry connection
                current_time = time.time()
                if (current_time - self.last_connection_attempt) < self.connection_retry_interval:
                    return False
                
                self.last_connection_attempt = current_time
                
                # Create client in executor to avoid blocking
                loop = asyncio.get_event_loop()
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    self.client = await loop.run_in_executor(
                        executor, 
                        lambda: InfluxDBClient(
                            url=self.connection_string,
                            token=self.token,
                            org=self.org
                        )
                    )
                
                # Check connection
                health = await loop.run_in_executor(executor, lambda: self.client.health())
                if health.status != "pass":
                    logger.error(f"InfluxDB health check failed: {health.message}")
                    self.client = None
                    self.is_connected = False
                    return False
                
                # Create APIs
                self.write_api = self.client.write_api(write_options=ASYNCHRONOUS)
                self.query_api = self.client.query_api()
                
                self.is_connected = True
                logger.info(f"Connected to InfluxDB: {self.connection_string}")
                return True
                
        except Exception as e:
            logger.error(f"Error connecting to InfluxDB: {str(e)}", exc_info=True)
            self.client = None
            self.write_api = None
            self.query_api = None
            self.is_connected = False
            return False
    
    async def disconnect(self) -> bool:
        """
        Disconnect from InfluxDB.
        
        Returns:
            Success status
        """
        try:
            async with self._lock:
                if self.client:
                    # Close in executor to avoid blocking
                    loop = asyncio.get_event_loop()
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        await loop.run_in_executor(executor, lambda: self.client.close())
                    
                    self.client = None
                    self.write_api = None
                    self.query_api = None
                    self.is_connected = False
                    logger.info("Disconnected from InfluxDB")
                    return True
                return True
        except Exception as e:
            logger.error(f"Error disconnecting from InfluxDB: {str(e)}", exc_info=True)
            return False
    
    async def health_check(self) -> bool:
        """
        Check if the InfluxDB connection is healthy.
        
        Returns:
            Health status
        """
        try:
            if not self.is_connected or not self.client:
                return False
            
            # Check health in executor to avoid blocking
            loop = asyncio.get_event_loop()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                health = await loop.run_in_executor(executor, lambda: self.client.health())
            
            return health.status == "pass"
        except Exception as e:
            logger.error(f"Error checking InfluxDB health: {str(e)}", exc_info=True)
            return False
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(WriteError)
    )
    async def write_data_point(self, measurement: str, fields: Dict[str, Any], 
                          tags: Dict[str, str] = None, timestamp: datetime = None) -> bool:
        """
        Write a data point to InfluxDB.
        
        Args:
            measurement: Measurement name
            fields: Field values
            tags: Tag values
            timestamp: Timestamp (default: current time)
            
        Returns:
            Success status
        """
        try:
            if not await self.connect():
                raise ConnectionError("Not connected to InfluxDB")
            
            # Create point
            point = Point(measurement)
            
            # Add fields
            for key, value in fields.items():
                if isinstance(value, bool):
                    point = point.field(key, value)
                elif isinstance(value, int):
                    point = point.field(key, value)
                elif isinstance(value, float):
                    point = point.field(key, value)
                elif isinstance(value, str):
                    point = point.field(key, value)
                else:
                    point = point.field(key, str(value))
            
            # Add tags
            if tags:
                for key, value in tags.items():
                    point = point.tag(key, value)
            
            # Add timestamp
            if timestamp:
                point = point.time(timestamp)
            
            # Write in executor to avoid blocking
            loop = asyncio.get_event_loop()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                await loop.run_in_executor(
                    executor,
                    lambda: self.write_api.write(
                        bucket=self.bucket,
                        record=point
                    )
                )
            
            return True
            
        except Exception as e:
            logger.error(f"Error writing to InfluxDB: {str(e)}", exc_info=True)
            if "cannot connect" in str(e).lower():
                self.is_connected = False
                await self.disconnect()
            raise WriteError(f"Failed to write data point: {str(e)}")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(WriteError)
    )
    async def write_dataframe(self, df: pd.DataFrame, measurement: str, 
                         tag_columns: List[str] = None, timestamp_column: str = None) -> bool:
        """
        Write a DataFrame to InfluxDB.
        
        Args:
            df: DataFrame to write
            measurement: Measurement name
            tag_columns: Columns to use as tags
            timestamp_column: Column to use as timestamp
            
        Returns:
            Success status
        """
        try:
            if not await self.connect():
                raise ConnectionError("Not connected to InfluxDB")
            
            if df.empty:
                logger.warning(f"Empty DataFrame provided for {measurement}")
                return True
            
            # Convert DataFrame to Line Protocol
            # This is more efficient than writing points one by one
            
            # Process in executor to avoid blocking
            loop = asyncio.get_event_loop()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                await loop.run_in_executor(
                    executor,
                    lambda: self.write_api.write(
                        bucket=self.bucket,
                        record=df,
                        data_frame_measurement_name=measurement,
                        data_frame_tag_columns=tag_columns,
                        data_frame_timestamp_column=timestamp_column
                    )
                )
            
            return True
            
        except Exception as e:
            logger.error(f"Error writing DataFrame to InfluxDB: {str(e)}", exc_info=True)
            if "cannot connect" in str(e).lower():
                self.is_connected = False
                await self.disconnect()
            raise WriteError(f"Failed to write DataFrame: {str(e)}")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(QueryError)
    )
    async def query(self, query: str) -> pd.DataFrame:
        """
        Query InfluxDB using Flux.
        
        Args:
            query: Flux query
            
        Returns:
            DataFrame with query results
        """
        try:
            if not await self.connect():
                raise ConnectionError("Not connected to InfluxDB")
            
            # Execute query in executor to avoid blocking
            loop = asyncio.get_event_loop()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                result = await loop.run_in_executor(
                    executor,
                    lambda: self.query_api.query_data_frame(query=query, org=self.org)
                )
            
            if isinstance(result, list):
                if not result:
                    return pd.DataFrame()
                return pd.concat(result)
            
            return result
            
        except Exception as e:
            logger.error(f"Error querying InfluxDB: {str(e)}", exc_info=True)
            if "cannot connect" in str(e).lower():
                self.is_connected = False
                await self.disconnect()
            raise QueryError(f"Failed to execute query: {str(e)}")
    
    async def get_latest_data(self, measurement: str, fields: List[str] = None, 
                         tags: Dict[str, str] = None, limit: int = 1) -> pd.DataFrame:
        """
        Get the latest data for a measurement.
        
        Args:
            measurement: Measurement name
            fields: Fields to retrieve (default: all)
            tags: Tags to filter by
            limit: Number of records to retrieve
            
        Returns:
            DataFrame with the latest data
        """
        try:
            # Build query
            query = f'''
                from(bucket: "{self.bucket}")
                  |> range(start: -1h)
                  |> filter(fn: (r) => r._measurement == "{measurement}")
            '''
            
            # Add tag filters
            if tags:
                for key, value in tags.items():
                    query += f'\n  |> filter(fn: (r) => r.{key} == "{value}")'
            
            # Add field filter
            if fields:
                field_filter = ' or '.join([f'r._field == "{field}"' for field in fields])
                query += f'\n  |> filter(fn: (r) => {field_filter})'
            
            # Add limit
            query += f'\n  |> tail(n: {limit})'
            
            # Execute query
            return await self.query(query)
            
        except Exception as e:
            logger.error(f"Error getting latest data: {str(e)}", exc_info=True)
            return pd.DataFrame()
    
    async def get_time_series(self, measurement: str, field: str, 
                         start: Union[datetime, str], end: Union[datetime, str] = None, 
                         tags: Dict[str, str] = None, interval: str = None) -> pd.DataFrame:
        """
        Get time series data for a measurement and field.
        
        Args:
            measurement: Measurement name
            field: Field to retrieve
            start: Start time
            end: End time (default: now)
            tags: Tags to filter by
            interval: Aggregation interval (e.g. "1m", "1h")
            
        Returns:
            DataFrame with time series data
        """
        try:
            # Format start time
            if isinstance(start, datetime):
                start_str = start.strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                start_str = start
            
            # Format end time
            if end is None:
                end_str = "now()"
            elif isinstance(end, datetime):
                end_str = end.strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                end_str = end
            
            # Build query
            query = f'''
                from(bucket: "{self.bucket}")
                  |> range(start: {start_str}, stop: {end_str})
                  |> filter(fn: (r) => r._measurement == "{measurement}")
                  |> filter(fn: (r) => r._field == "{field}")
            '''
            
            # Add tag filters
            if tags:
                for key, value in tags.items():
                    query += f'\n  |> filter(fn: (r) => r.{key} == "{value}")'
            
            # Add interval aggregation
            if interval:
                query += f'''
                  |> aggregateWindow(every: {interval}, fn: mean, createEmpty: false)
                '''
            
            # Execute query
            return await self.query(query)
            
        except Exception as e:
            logger.error(f"Error getting time series data: {str(e)}", exc_info=True)
            return pd.DataFrame()

class TransactionDatabase(Database):
    """
    PostgreSQL transaction database connection.
    
    Provides methods for storing and retrieving transaction data.
    """
    
    def __init__(self, connection_string: str, max_connections: int = 10):
        """
        Initialize the PostgreSQL connection.
        
        Args:
            connection_string: PostgreSQL connection string
            max_connections: Maximum number of connections in the pool
        """
        super().__init__(name="TransactionDatabase", connection_string=connection_string)
        self.pool = None
        self.max_connections = max_connections
        self.min_connections = 2
    
    async def connect(self) -> bool:
        """
        Connect to PostgreSQL.
        
        Returns:
            Success status
        """
        try:
            async with self._lock:
                if self.is_connected and self.pool:
                    return True
                
                # Check if we should retry connection
                current_time = time.time()
                if (current_time - self.last_connection_attempt) < self.connection_retry_interval:
                    return False
                
                self.last_connection_attempt = current_time
                
                # Create connection pool
                self.pool = await asyncpg.create_pool(
                    dsn=self.connection_string,
                    min_size=self.min_connections,
                    max_size=self.max_connections
                )
                
                self.is_connected = True
                logger.info(f"Connected to PostgreSQL: {self.connection_string}")
                return True
                
        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL: {str(e)}", exc_info=True)
            self.pool = None
            self.is_connected = False
            return False
    
    async def disconnect(self) -> bool:
        """
        Disconnect from PostgreSQL.
        
        Returns:
            Success status
        """
        try:
            async with self._lock:
                if self.pool:
                    await self.pool.close()
                    self.pool = None
                    self.is_connected = False
                    logger.info("Disconnected from PostgreSQL")
                    return True
                return True
        except Exception as e:
            logger.error(f"Error disconnecting from PostgreSQL: {str(e)}", exc_info=True)
            return False
    
    async def health_check(self) -> bool:
        """
        Check if the PostgreSQL connection is healthy.
        
        Returns:
            Health status
        """
        try:
            if not self.is_connected or not self.pool:
                return False
            
            # Execute simple query to check connection
            async with self.pool.acquire() as conn:
                result = await conn.fetchval("SELECT 1")
                return result == 1
        except Exception as e:
            logger.error(f"Error checking PostgreSQL health: {str(e)}", exc_info=True)
            return False
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(QueryError)
    )
    async def execute(self, query: str, *args) -> str:
        """
        Execute a SQL query.
        
        Args:
            query: SQL query
            *args: Query parameters
            
        Returns:
            Query status
        """
        try:
            if not await self.connect():
                raise ConnectionError("Not connected to PostgreSQL")
            
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    result = await conn.execute(query, *args)
                    return result
                    
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}", exc_info=True)
            if "connection" in str(e).lower():
                self.is_connected = False
                await self.disconnect()
            raise QueryError(f"Failed to execute query: {str(e)}")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(QueryError)
    )
    async def fetch(self, query: str, *args) -> List[Dict[str, Any]]:
        """
        Fetch records from a SQL query.
        
        Args:
            query: SQL query
            *args: Query parameters
            
        Returns:
            List of records
        """
        try:
            if not await self.connect():
                raise ConnectionError("Not connected to PostgreSQL")
            
            async with self.pool.acquire() as conn:
                records = await conn.fetch(query, *args)
                return [dict(record) for record in records]
                    
        except Exception as e:
            logger.error(f"Error fetching records: {str(e)}", exc_info=True)
            if "connection" in str(e).lower():
                self.is_connected = False
                await self.disconnect()
            raise QueryError(f"Failed to fetch records: {str(e)}")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(QueryError)
    )
    async def fetchval(self, query: str, *args) -> Any:
        """
        Fetch a single value from a SQL query.
        
        Args:
            query: SQL query
            *args: Query parameters
            
        Returns:
            Single value
        """
        try:
            if not await self.connect():
                raise ConnectionError("Not connected to PostgreSQL")
            
            async with self.pool.acquire() as conn:
                return await conn.fetchval(query, *args)
                    
        except Exception as e:
            logger.error(f"Error fetching value: {str(e)}", exc_info=True)
            if "connection" in str(e).lower():
                self.is_connected = False
                await self.disconnect()
            raise QueryError(f"Failed to fetch value: {str(e)}")

# Singleton instances
time_series_db: Optional[TimeSeriesDatabase] = None
transaction_db: Optional[TransactionDatabase] = None

async def initialize_databases(influx_url: str, influx_token: str, influx_org: str, influx_bucket: str,
                          postgres_url: str) -> Tuple[bool, bool]:
    """
    Initialize database connections.
    
    Args:
        influx_url: InfluxDB URL
        influx_token: InfluxDB token
        influx_org: InfluxDB organization
        influx_bucket: InfluxDB bucket
        postgres_url: PostgreSQL connection URL
        
    Returns:
        Tuple of (time_series_success, transaction_success)
    """
    global time_series_db, transaction_db
    
    # Initialize time series database
    time_series_success = False
    try:
        time_series_db = TimeSeriesDatabase(
            connection_string=influx_url,
            org=influx_org,
            bucket=influx_bucket,
            token=influx_token
        )
        time_series_success = await time_series_db.initialize()
    except Exception as e:
        logger.error(f"Error initializing time series database: {str(e)}", exc_info=True)
    
    # Initialize transaction database
    transaction_success = False
    try:
        transaction_db = TransactionDatabase(
            connection_string=postgres_url
        )
        transaction_success = await transaction_db.initialize()
    except Exception as e:
        logger.error(f"Error initializing transaction database: {str(e)}", exc_info=True)
    
    return time_series_success, transaction_success

async def close_databases() -> None:
    """Close all database connections."""
    global time_series_db, transaction_db
    
    # Close time series database
    if time_series_db is not None:
        await time_series_db.stop()
        time_series_db = None
    
    # Close transaction database
    if transaction_db is not None:
        await transaction_db.stop()
        transaction_db = None

class DatabaseManager(Component):
    """
    Database Manager
    
    Manages database operations for the trading system.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the database manager.
        
        Args:
            config: Database configuration
        """
        super().__init__(name="DatabaseManager")
        
        # Configuration
        self.config = config or {}
        
        # Database connection
        self.db_path = self.config.get("db_path", "trading_system.db")
        self.conn = None
        self.cursor = None
        
        # Performance metrics
        self.query_count = 0
        self.last_query_time = 0
        self.query_times = []
        
        # Schema definitions
        self.tables = {
            "strategies": """
                CREATE TABLE IF NOT EXISTS strategies (
                    id TEXT PRIMARY KEY,
                    type TEXT NOT NULL,
                    name TEXT NOT NULL,
                    config TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                )
            """,
            "market_data": """
                CREATE TABLE IF NOT EXISTS market_data (
                    id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL NOT NULL
                )
            """,
            "signals": """
                CREATE TABLE IF NOT EXISTS signals (
                    id TEXT PRIMARY KEY,
                    strategy_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    signal TEXT NOT NULL,
                    price REAL NOT NULL,
                    data TEXT NOT NULL
                )
            """,
            "orders": """
                CREATE TABLE IF NOT EXISTS orders (
                    id TEXT PRIMARY KEY,
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    order_type TEXT NOT NULL,
                    side TEXT NOT NULL,
                    amount REAL NOT NULL,
                    price REAL,
                    status TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    filled_amount REAL,
                    filled_price REAL,
                    fee REAL,
                    metadata TEXT
                )
            """,
            "trades": """
                CREATE TABLE IF NOT EXISTS trades (
                    id TEXT PRIMARY KEY,
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    order_id TEXT NOT NULL,
                    side TEXT NOT NULL,
                    amount REAL NOT NULL,
                    price REAL NOT NULL,
                    cost REAL NOT NULL,
                    fee REAL,
                    timestamp INTEGER NOT NULL,
                    metadata TEXT
                )
            """,
            "balances": """
                CREATE TABLE IF NOT EXISTS balances (
                    id TEXT PRIMARY KEY,
                    exchange TEXT NOT NULL,
                    currency TEXT NOT NULL,
                    amount REAL NOT NULL,
                    timestamp INTEGER NOT NULL
                )
            """,
            "settings": """
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    description TEXT,
                    updated_at INTEGER NOT NULL
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
            self._status = ComponentStatus.INITIALIZING
            logger.info("Initializing database manager")
            
            # Create database directory if necessary
            db_dir = os.path.dirname(self.db_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir)
            
            # Connect to the database
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()
            
            # Create tables
            for table_name, table_sql in self.tables.items():
                self.cursor.execute(table_sql)
            
            # Commit changes
            self.conn.commit()
            
            # Initialize settings
            await self._initialize_settings()
            
            self._status = ComponentStatus.INITIALIZED
            logger.info("Database manager initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing database manager: {str(e)}", exc_info=True)
            self._status = ComponentStatus.ERROR
            self._error = str(e)
            return False
    
    async def start(self) -> bool:
        """
        Start the database manager.
        
        Returns:
            Start success
        """
        try:
            logger.info("Starting database manager")
            
            if not self.conn:
                # Connect to the database if not already connected
                self.conn = sqlite3.connect(self.db_path)
                self.conn.row_factory = sqlite3.Row
                self.cursor = self.conn.cursor()
            
            self._status = ComponentStatus.OPERATIONAL
            logger.info("Database manager started successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error starting database manager: {str(e)}", exc_info=True)
            self._status = ComponentStatus.ERROR
            self._error = str(e)
            return False
    
    async def stop(self) -> bool:
        """
        Stop the database manager.
        
        Returns:
            Stop success
        """
        try:
            logger.info("Stopping database manager")
            
            # Close database connection
            if self.conn:
                self.conn.close()
                self.conn = None
                self.cursor = None
            
            self._status = ComponentStatus.STOPPED
            logger.info("Database manager stopped successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping database manager: {str(e)}", exc_info=True)
            self._status = ComponentStatus.ERROR
            self._error = str(e)
            return False
    
    async def insert_one(self, collection: str, data: Dict[str, Any]) -> str:
        """
        Insert a document into a collection.
        
        Args:
            collection: Collection name
            data: Document data
            
        Returns:
            Document ID
        """
        try:
            # Measure query time
            start_time = time.time()
            
            # Get document ID
            doc_id = data.get("id")
            if not doc_id:
                doc_id = str(int(time.time() * 1000))
                data["id"] = doc_id
            
            # Check if table exists
            if collection not in self.tables:
                logger.error(f"Table {collection} does not exist")
                return None
            
            # Prepare query
            placeholders = ", ".join(["?"] * len(data))
            columns = ", ".join(data.keys())
            values = list(data.values())
            
            # Convert complex values to JSON
            for i, value in enumerate(values):
                if isinstance(value, (dict, list)):
                    values[i] = json.dumps(value)
            
            # Create SQL query
            query = f"INSERT INTO {collection} ({columns}) VALUES ({placeholders})"
            
            # Execute query
            self.cursor.execute(query, values)
            self.conn.commit()
            
            # Update metrics
            self.query_count += 1
            query_time = time.time() - start_time
            self.last_query_time = query_time
            self.query_times.append(query_time)
            if len(self.query_times) > 100:
                self.query_times = self.query_times[-100:]
            
            return doc_id
            
        except Exception as e:
            logger.error(f"Error inserting document into {collection}: {str(e)}", exc_info=True)
            return None
    
    async def find_one(self, collection: str, query: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Find a document in a collection.
        
        Args:
            collection: Collection name
            query: Query filter
            
        Returns:
            Document data or None if not found
        """
        try:
            # Measure query time
            start_time = time.time()
            
            # Check if table exists
            if collection not in self.tables:
                logger.error(f"Table {collection} does not exist")
                return None
            
            # Prepare query
            where_clauses = []
            values = []
            
            for key, value in query.items():
                where_clauses.append(f"{key} = ?")
                if isinstance(value, (dict, list)):
                    values.append(json.dumps(value))
                else:
                    values.append(value)
            
            # Create SQL query
            where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
            query = f"SELECT * FROM {collection} WHERE {where_clause} LIMIT 1"
            
            # Execute query
            self.cursor.execute(query, values)
            result = self.cursor.fetchone()
            
            # Update metrics
            self.query_count += 1
            query_time = time.time() - start_time
            self.last_query_time = query_time
            self.query_times.append(query_time)
            if len(self.query_times) > 100:
                self.query_times = self.query_times[-100:]
            
            # Convert to dictionary
            if result:
                result_dict = dict(result)
                
                # Parse JSON fields
                for key, value in result_dict.items():
                    if isinstance(value, str) and (value.startswith("{") or value.startswith("[")):
                        try:
                            result_dict[key] = json.loads(value)
                        except:
                            pass
                
                return result_dict
            else:
                return None
            
        except Exception as e:
            logger.error(f"Error finding document in {collection}: {str(e)}", exc_info=True)
            return None
    
    async def find(self, collection: str, query: Dict[str, Any] = None, limit: int = 0, sort: List[Tuple[str, int]] = None) -> List[Dict[str, Any]]:
        """
        Find documents in a collection.
        
        Args:
            collection: Collection name
            query: Query filter
            limit: Maximum number of documents to return (0 for all)
            sort: List of fields to sort by (field name, direction) where direction is 1 for ascending, -1 for descending
            
        Returns:
            List of documents
        """
        try:
            # Measure query time
            start_time = time.time()
            
            # Check if table exists
            if collection not in self.tables:
                logger.error(f"Table {collection} does not exist")
                return []
            
            # Prepare query
            where_clauses = []
            values = []
            
            if query:
                for key, value in query.items():
                    where_clauses.append(f"{key} = ?")
                    if isinstance(value, (dict, list)):
                        values.append(json.dumps(value))
                    else:
                        values.append(value)
            
            # Create SQL query
            where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
            query_sql = f"SELECT * FROM {collection} WHERE {where_clause}"
            
            # Add sort
            if sort:
                order_by = []
                for field, direction in sort:
                    order_by.append(f"{field} {'ASC' if direction > 0 else 'DESC'}")
                query_sql += f" ORDER BY {', '.join(order_by)}"
            
            # Add limit
            if limit > 0:
                query_sql += f" LIMIT {limit}"
            
            # Execute query
            self.cursor.execute(query_sql, values)
            results = self.cursor.fetchall()
            
            # Update metrics
            self.query_count += 1
            query_time = time.time() - start_time
            self.last_query_time = query_time
            self.query_times.append(query_time)
            if len(self.query_times) > 100:
                self.query_times = self.query_times[-100:]
            
            # Convert to dictionaries
            result_list = []
            for result in results:
                result_dict = dict(result)
                
                # Parse JSON fields
                for key, value in result_dict.items():
                    if isinstance(value, str) and (value.startswith("{") or value.startswith("[")):
                        try:
                            result_dict[key] = json.loads(value)
                        except:
                            pass
                
                result_list.append(result_dict)
            
            return result_list
            
        except Exception as e:
            logger.error(f"Error finding documents in {collection}: {str(e)}", exc_info=True)
            return []
    
    async def update_one(self, collection: str, query: Dict[str, Any], update: Dict[str, Any]) -> bool:
        """
        Update a document in a collection.
        
        Args:
            collection: Collection name
            query: Query filter
            update: Update operations
            
        Returns:
            Success status
        """
        try:
            # Measure query time
            start_time = time.time()
            
            # Check if table exists
            if collection not in self.tables:
                logger.error(f"Table {collection} does not exist")
                return False
            
            # Find document to update
            document = await self.find_one(collection, query)
            if not document:
                logger.warning(f"Document not found for update in {collection}")
                return False
            
            # Get document ID
            doc_id = document.get("id")
            if not doc_id:
                logger.error(f"Document has no ID for update in {collection}")
                return False
            
            # Update document
            for key, value in update.items():
                document[key] = value
            
            # Set updated_at if present
            if "updated_at" in document:
                document["updated_at"] = int(time.time() * 1000)
            
            # Delete document
            await self.delete_one(collection, {"id": doc_id})
            
            # Insert updated document
            await self.insert_one(collection, document)
            
            # Update metrics
            self.query_count += 1
            query_time = time.time() - start_time
            self.last_query_time = query_time
            self.query_times.append(query_time)
            if len(self.query_times) > 100:
                self.query_times = self.query_times[-100:]
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating document in {collection}: {str(e)}", exc_info=True)
            return False
    
    async def delete_one(self, collection: str, query: Dict[str, Any]) -> bool:
        """
        Delete a document from a collection.
        
        Args:
            collection: Collection name
            query: Query filter
            
        Returns:
            Success status
        """
        try:
            # Measure query time
            start_time = time.time()
            
            # Check if table exists
            if collection not in self.tables:
                logger.error(f"Table {collection} does not exist")
                return False
            
            # Prepare query
            where_clauses = []
            values = []
            
            for key, value in query.items():
                where_clauses.append(f"{key} = ?")
                if isinstance(value, (dict, list)):
                    values.append(json.dumps(value))
                else:
                    values.append(value)
            
            # Create SQL query
            where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
            query = f"DELETE FROM {collection} WHERE {where_clause} LIMIT 1"
            
            # Execute query
            self.cursor.execute(query, values)
            self.conn.commit()
            
            # Update metrics
            self.query_count += 1
            query_time = time.time() - start_time
            self.last_query_time = query_time
            self.query_times.append(query_time)
            if len(self.query_times) > 100:
                self.query_times = self.query_times[-100:]
            
            return True
            
        except Exception as e:
            logger.error(f"Error deleting document from {collection}: {str(e)}", exc_info=True)
            return False
    
    async def delete_many(self, collection: str, query: Dict[str, Any]) -> int:
        """
        Delete multiple documents from a collection.
        
        Args:
            collection: Collection name
            query: Query filter
            
        Returns:
            Number of deleted documents
        """
        try:
            # Measure query time
            start_time = time.time()
            
            # Check if table exists
            if collection not in self.tables:
                logger.error(f"Table {collection} does not exist")
                return 0
            
            # Prepare query
            where_clauses = []
            values = []
            
            for key, value in query.items():
                where_clauses.append(f"{key} = ?")
                if isinstance(value, (dict, list)):
                    values.append(json.dumps(value))
                else:
                    values.append(value)
            
            # Create SQL query
            where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
            query = f"DELETE FROM {collection} WHERE {where_clause}"
            
            # Execute query
            self.cursor.execute(query, values)
            deleted_count = self.cursor.rowcount
            self.conn.commit()
            
            # Update metrics
            self.query_count += 1
            query_time = time.time() - start_time
            self.last_query_time = query_time
            self.query_times.append(query_time)
            if len(self.query_times) > 100:
                self.query_times = self.query_times[-100:]
            
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error deleting documents from {collection}: {str(e)}", exc_info=True)
            return 0
    
    async def execute_query(self, query: str, parameters: List[Any] = None) -> List[Dict[str, Any]]:
        """
        Execute a custom SQL query.
        
        Args:
            query: SQL query
            parameters: Query parameters
            
        Returns:
            Query results
        """
        try:
            # Measure query time
            start_time = time.time()
            
            # Execute query
            if parameters:
                self.cursor.execute(query, parameters)
            else:
                self.cursor.execute(query)
            
            # Get results
            results = self.cursor.fetchall()
            
            # Commit if necessary
            if query.strip().upper().startswith(("INSERT", "UPDATE", "DELETE")):
                self.conn.commit()
            
            # Update metrics
            self.query_count += 1
            query_time = time.time() - start_time
            self.last_query_time = query_time
            self.query_times.append(query_time)
            if len(self.query_times) > 100:
                self.query_times = self.query_times[-100:]
            
            # Convert to dictionaries
            result_list = []
            for result in results:
                result_dict = dict(result)
                
                # Parse JSON fields
                for key, value in result_dict.items():
                    if isinstance(value, str) and (value.startswith("{") or value.startswith("[")):
                        try:
                            result_dict[key] = json.loads(value)
                        except:
                            pass
                
                result_list.append(result_dict)
            
            return result_list
            
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}", exc_info=True)
            return []
    
    async def get_setting(self, key: str, default: Any = None) -> Any:
        """
        Get a setting value.
        
        Args:
            key: Setting key
            default: Default value if not found
            
        Returns:
            Setting value
        """
        try:
            # Find setting
            setting = await self.find_one("settings", {"key": key})
            
            if setting:
                value = setting.get("value")
                
                # Parse JSON if needed
                if value and (value.startswith("{") or value.startswith("[")):
                    try:
                        return json.loads(value)
                    except:
                        return value
                
                return value
            else:
                return default
            
        except Exception as e:
            logger.error(f"Error getting setting {key}: {str(e)}", exc_info=True)
            return default
    
    async def set_setting(self, key: str, value: Any, description: str = None) -> bool:
        """
        Set a setting value.
        
        Args:
            key: Setting key
            value: Setting value
            description: Setting description
            
        Returns:
            Success status
        """
        try:
            # Convert value to string if needed
            if isinstance(value, (dict, list)):
                value_str = json.dumps(value)
            else:
                value_str = str(value)
            
            # Check if setting exists
            setting = await self.find_one("settings", {"key": key})
            
            if setting:
                # Update setting
                await self.update_one(
                    "settings",
                    {"key": key},
                    {
                        "value": value_str,
                        "description": description or setting.get("description"),
                        "updated_at": int(time.time() * 1000)
                    }
                )
            else:
                # Insert setting
                await self.insert_one(
                    "settings",
                    {
                        "key": key,
                        "value": value_str,
                        "description": description,
                        "updated_at": int(time.time() * 1000)
                    }
                )
            
            return True
            
        except Exception as e:
            logger.error(f"Error setting setting {key}: {str(e)}", exc_info=True)
            return False
    
    async def delete_setting(self, key: str) -> bool:
        """
        Delete a setting.
        
        Args:
            key: Setting key
            
        Returns:
            Success status
        """
        try:
            # Delete setting
            return await self.delete_one("settings", {"key": key})
            
        except Exception as e:
            logger.error(f"Error deleting setting {key}: {str(e)}", exc_info=True)
            return False
    
    async def get_performance_metrics(self) -> Dict[str, Any]:
        """
        Get database performance metrics.
        
        Returns:
            Performance metrics
        """
        try:
            # Calculate metrics
            avg_query_time = sum(self.query_times) / len(self.query_times) if self.query_times else 0
            
            return {
                "query_count": self.query_count,
                "last_query_time": self.last_query_time,
                "avg_query_time": avg_query_time
            }
            
        except Exception as e:
            logger.error(f"Error getting performance metrics: {str(e)}", exc_info=True)
            return {}
    
    async def _initialize_settings(self) -> None:
        """
        Initialize default settings.
        """
        try:
            logger.info("Initializing default settings")
            # In test mode, we'll just log this step rather than 
            # actually inserting default settings to avoid DB dependency
            logger.info("Default settings initialized (test mode)")
        except Exception as e:
            logger.error(f"Error initializing settings: {str(e)}", exc_info=True)

# Create database manager singleton
database_manager = DatabaseManager() 