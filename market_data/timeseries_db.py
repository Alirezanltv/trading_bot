"""
Time-Series Database Module

This module provides a high-reliability interface to InfluxDB for storing
and retrieving market data. It includes connection pooling, automatic retry,
and circuit breaker patterns to ensure robust database operations.
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Any, Tuple

import influxdb
import pandas as pd
from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError, InfluxDBServerError

from trading_system.core.circuit_breaker import CircuitBreaker
from trading_system.utils.retry import retry_with_backoff
from trading_system.utils.config import Config

logger = logging.getLogger(__name__)

class TimeSeriesDB:
    """
    High-reliability interface to InfluxDB for market data storage and retrieval.
    
    Features:
    - Connection pooling for database connections
    - Automatic retry with exponential backoff
    - Circuit breaker to prevent cascading failures
    - Efficient bulk operations for write performance
    - Query caching for improved read performance
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize the TimeSeriesDB with configuration.
        
        Args:
            config: Dictionary containing InfluxDB configuration
        """
        self.config = config or Config().get("timeseries_db", {})
        self.host = self.config.get("host", "localhost")
        self.port = self.config.get("port", 8086)
        self.username = self.config.get("username", "")
        self.password = self.config.get("password", "")
        self.database = self.config.get("database", "market_data")
        self.retention_policy = self.config.get("retention_policy", "autogen")
        self.batch_size = self.config.get("batch_size", 5000)
        self.timeout = self.config.get("timeout", 10)
        self.query_cache_size = self.config.get("query_cache_size", 100)
        self.query_cache_ttl = self.config.get("query_cache_ttl", 60)  # seconds
        
        # Connection pool size
        self.pool_size = self.config.get("pool_size", 5)
        self.pool = []
        
        # Circuit breaker for write operations
        self.write_circuit_breaker = CircuitBreaker(
            name="influxdb_write",
            failure_threshold=5,
            reset_timeout=30,
            success_threshold=3
        )
        
        # Circuit breaker for read operations
        self.read_circuit_breaker = CircuitBreaker(
            name="influxdb_read",
            failure_threshold=5,
            reset_timeout=30,
            success_threshold=3
        )
        
        # Query cache
        self.query_cache = {}
        self.query_cache_timestamps = {}
        
        # Initialize the connection pool
        self._initialize_pool()
        
        # Ensure database exists
        self._ensure_database_exists()
    
    def _initialize_pool(self) -> None:
        """Initialize the connection pool."""
        successful_connections = 0
        connection_errors = []

        for i in range(self.pool_size):
            try:
                client = InfluxDBClient(
                    host=self.host,
                    port=self.port,
                    username=self.username,
                    password=self.password,
                    database=self.database,
                    timeout=self.timeout
                )
                
                # Test the connection
                client.ping()
                
                self.pool.append(client)
                successful_connections += 1
                logger.debug(f"Successfully initialized InfluxDB connection {i+1}/{self.pool_size}")
            except (InfluxDBClientError, InfluxDBServerError) as e:
                error_msg = str(e)
                connection_errors.append(error_msg)
                logger.warning(f"Failed to initialize InfluxDB connection {i+1}/{self.pool_size}: {error_msg}")
        
        if successful_connections == 0:
            error_details = "; ".join(connection_errors[:3])
            logger.error(f"Failed to initialize any InfluxDB connections. Errors: {error_details}")
            logger.info("Please run 'py -3.10 setup_influxdb.py' to set up InfluxDB")
        else:
            logger.info(f"Initialized {successful_connections}/{self.pool_size} InfluxDB connections")

    def _get_connection(self) -> InfluxDBClient:
        """
        Get a connection from the pool.
        
        Returns:
            InfluxDB client connection
        
        Raises:
            RuntimeError: If no connections are available
        """
        if not self.pool:
            logger.warning("No connections in pool, attempting to reinitialize")
            self._initialize_pool()
            if not self.pool:
                raise RuntimeError("Failed to initialize InfluxDB connection pool")
        
        # Simple round-robin pool with connection checking
        for _ in range(len(self.pool)):
            client = self.pool.pop(0)
            self.pool.append(client)
            
            try:
                # Verify connection is still valid with a quick ping
                client.ping()
                return client
            except (InfluxDBClientError, InfluxDBServerError) as e:
                logger.warning(f"Connection in pool is invalid, will try another: {str(e)}")
                continue
        
        # If we've tried all connections and none worked, reinitialize pool
        logger.warning("All connections in pool are invalid, reinitializing")
        self.pool = []
        self._initialize_pool()
        
        if not self.pool:
            raise RuntimeError("All InfluxDB connections failed")
            
        return self.pool[0]
    
    def _ensure_database_exists(self) -> None:
        """Ensure the configured database exists, creating it if necessary."""
        for attempt in range(3):  # Try up to 3 times
            try:
                client = None
                
                try:
                    client = self._get_connection()
                except RuntimeError as e:
                    logger.error(f"Could not get database connection: {str(e)}")
                    time.sleep(1)
                    continue
                    
                databases = [db['name'] for db in client.get_list_database()]
                
                if self.database not in databases:
                    logger.info(f"Creating database {self.database}")
                    client.create_database(self.database)
                    
                    # Set up retention policies if needed
                    if self.config.get("custom_retention_policies"):
                        for policy in self.config["custom_retention_policies"]:
                            client.create_retention_policy(
                                name=policy["name"],
                                duration=policy["duration"],
                                replication=policy.get("replication", 1),
                                database=self.database,
                                default=policy.get("default", False)
                            )
                    
                    # Create default retention policy if not using custom ones
                    else:
                        client.create_retention_policy(
                            name="default_policy",
                            duration="INF",
                            replication=1,
                            database=self.database,
                            default=True
                        )
                        logger.info(f"Created default retention policy for {self.database}")
                
                logger.info(f"Connected to database: {self.database}")
                return
                    
            except Exception as e:
                logger.error(f"Error ensuring database exists (attempt {attempt+1}/3): {str(e)}")
                if attempt < 2:  # Wait before retrying, but not on the last attempt
                    time.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s
        
        logger.error(f"Failed to ensure database exists after multiple attempts")
        logger.info("Please run 'py -3.10 setup_influxdb.py' to set up InfluxDB")
    
    @retry_with_backoff(max_retries=3, initial_wait=0.5, backoff_factor=2)
    def write_points(self, points: List[Dict], 
                     measurement: Optional[str] = None,
                     tags: Optional[Dict] = None,
                     time_precision: str = 's',
                     batch_size: Optional[int] = None) -> bool:
        """
        Write data points to InfluxDB with automatic retries and circuit breaker.
        
        Args:
            points: List of data points to write
            measurement: Optional measurement name if not included in points
            tags: Optional tags to add to all points
            time_precision: Precision of the time values
            batch_size: Optional custom batch size
            
        Returns:
            True if write operation succeeded, False otherwise
        """
        if not points:
            return True
        
        # Add measurement and tags if provided
        if measurement or tags:
            for point in points:
                if measurement and 'measurement' not in point:
                    point['measurement'] = measurement
                if tags:
                    point_tags = point.get('tags', {})
                    point_tags.update(tags)
                    point['tags'] = point_tags
        
        # Use the write circuit breaker
        with self.write_circuit_breaker:
            client = self._get_connection()
            batch_size = batch_size or self.batch_size
            
            # Write in batches to prevent oversized requests
            for i in range(0, len(points), batch_size):
                batch = points[i:i + batch_size]
                try:
                    client.write_points(
                        batch,
                        time_precision=time_precision,
                        retention_policy=self.retention_policy
                    )
                except (InfluxDBClientError, InfluxDBServerError) as e:
                    logger.error(f"Error writing to InfluxDB: {str(e)}")
                    raise
            
            return True
    
    def write_dataframe(self, df: pd.DataFrame, 
                        measurement: str,
                        tag_columns: Optional[List[str]] = None,
                        field_columns: Optional[List[str]] = None,
                        time_column: str = 'time') -> bool:
        """
        Write pandas DataFrame to InfluxDB.
        
        Args:
            df: DataFrame containing the data
            measurement: Measurement name
            tag_columns: Columns to use as tags
            field_columns: Columns to use as fields
            time_column: Column to use as the timestamp
            
        Returns:
            True if write operation succeeded, False otherwise
        """
        if df.empty:
            return True
        
        tag_columns = tag_columns or []
        field_columns = field_columns or list(set(df.columns) - set(tag_columns) - {time_column})
        
        points = []
        for _, row in df.iterrows():
            point = {
                "measurement": measurement,
                "tags": {tag: row[tag] for tag in tag_columns if tag in row},
                "fields": {field: row[field] for field in field_columns if field in row},
            }
            
            # Convert timestamp to proper format
            if time_column in row:
                if isinstance(row[time_column], (int, float)):
                    point["time"] = int(row[time_column])
                else:
                    point["time"] = pd.Timestamp(row[time_column]).value // 10**9  # nanoseconds to seconds
            
            points.append(point)
        
        return self.write_points(points)
    
    @retry_with_backoff(max_retries=3, initial_wait=0.5, backoff_factor=2)
    def query(self, query: str, 
              bind_params: Optional[Dict] = None,
              epoch: Optional[str] = None,
              as_dataframe: bool = True,
              use_cache: bool = True) -> Union[List[Dict], pd.DataFrame]:
        """
        Query data from InfluxDB with automatic retries and circuit breaker.
        
        Args:
            query: InfluxDB query string
            bind_params: Parameters to bind to the query
            epoch: Return time data in specified epoch format
            as_dataframe: Return results as pandas DataFrame
            use_cache: Use query cache if available
            
        Returns:
            Query results as a list of dictionaries or DataFrame
        """
        # Check the cache if enabled
        cache_key = f"{query}:{str(bind_params)}:{epoch}:{as_dataframe}"
        if use_cache and cache_key in self.query_cache:
            cache_time = self.query_cache_timestamps.get(cache_key, 0)
            if time.time() - cache_time <= self.query_cache_ttl:
                return self.query_cache[cache_key]
        
        # Use the read circuit breaker
        with self.read_circuit_breaker:
            client = self._get_connection()
            try:
                result = client.query(query, bind_params=bind_params, epoch=epoch)
                
                if as_dataframe:
                    # Convert to DataFrame
                    dataframes = []
                    for (measurement, tags), points in result.items():
                        df = pd.DataFrame(points)
                        if not df.empty:
                            if tags:
                                for tag_key, tag_value in tags.items():
                                    df[tag_key] = tag_value
                            df['measurement'] = measurement
                            dataframes.append(df)
                    
                    if dataframes:
                        result_df = pd.concat(dataframes, ignore_index=True)
                        
                        # Cache the result if caching is enabled
                        if use_cache:
                            self.query_cache[cache_key] = result_df
                            self.query_cache_timestamps[cache_key] = time.time()
                            
                            # Prune cache if it exceeds size limit
                            if len(self.query_cache) > self.query_cache_size:
                                oldest_key = min(self.query_cache_timestamps.items(), key=lambda x: x[1])[0]
                                del self.query_cache[oldest_key]
                                del self.query_cache_timestamps[oldest_key]
                        
                        return result_df
                    return pd.DataFrame()
                
                # Return raw result if not converting to DataFrame
                return result
                
            except (InfluxDBClientError, InfluxDBServerError) as e:
                logger.error(f"Error querying InfluxDB: {str(e)}")
                raise
    
    def get_candlestick_data(self, symbol: str, 
                           interval: str, 
                           start_time: Union[datetime, int, str],
                           end_time: Optional[Union[datetime, int, str]] = None,
                           limit: Optional[int] = None) -> pd.DataFrame:
        """
        Retrieve candlestick (OHLCV) data for a symbol.
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTC/USDT')
            interval: Candle interval (e.g., '1m', '1h', '1d')
            start_time: Start time as datetime, timestamp, or string
            end_time: End time as datetime, timestamp, or string
            limit: Maximum number of candles to return
            
        Returns:
            DataFrame containing candlestick data
        """
        # Convert start_time and end_time to proper format
        if isinstance(start_time, datetime):
            start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        elif isinstance(start_time, int):
            start_time_str = datetime.fromtimestamp(start_time).strftime('%Y-%m-%dT%H:%M:%SZ')
        else:
            start_time_str = start_time
        
        if end_time is None:
            end_time_str = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
        elif isinstance(end_time, datetime):
            end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        elif isinstance(end_time, int):
            end_time_str = datetime.fromtimestamp(end_time).strftime('%Y-%m-%dT%H:%M:%SZ')
        else:
            end_time_str = end_time
        
        # Build the query
        query = f'''
            SELECT time, open, high, low, close, volume
            FROM ohlcv
            WHERE symbol = $symbol
                AND interval = $interval
                AND time >= $start_time
                AND time <= $end_time
            ORDER BY time ASC
        '''
        
        if limit:
            query += f" LIMIT {limit}"
        
        bind_params = {
            "symbol": symbol,
            "interval": interval,
            "start_time": start_time_str,
            "end_time": end_time_str
        }
        
        return self.query(query, bind_params=bind_params)
    
    def get_latest_price(self, symbol: str) -> Optional[float]:
        """
        Get the most recent price for a symbol.
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTC/USDT')
            
        Returns:
            Latest price or None if no data available
        """
        query = '''
            SELECT last(close) as price
            FROM ohlcv
            WHERE symbol = $symbol
        '''
        
        result = self.query(query, bind_params={"symbol": symbol}, as_dataframe=True)
        if not result.empty and 'price' in result.columns:
            return float(result.iloc[0]['price'])
        return None
    
    def store_trade(self, trade: Dict) -> bool:
        """
        Store a trade in the database.
        
        Args:
            trade: Dictionary containing trade information
            
        Returns:
            True if storage succeeded, False otherwise
        """
        point = {
            "measurement": "trades",
            "tags": {
                "symbol": trade.get("symbol", "unknown"),
                "side": trade.get("side", "unknown"),
                "order_id": trade.get("order_id", "unknown")
            },
            "fields": {
                "price": float(trade.get("price", 0)),
                "amount": float(trade.get("amount", 0)),
                "cost": float(trade.get("price", 0)) * float(trade.get("amount", 0)),
                "fee": float(trade.get("fee", {}).get("cost", 0)) if isinstance(trade.get("fee"), dict) else 0,
                "fee_currency": trade.get("fee", {}).get("currency", "") if isinstance(trade.get("fee"), dict) else ""
            },
            "time": int(trade.get("timestamp", time.time() * 1000) / 1000)  # milliseconds to seconds
        }
        
        return self.write_points([point])
    
    def store_order(self, order: Dict) -> bool:
        """
        Store an order in the database.
        
        Args:
            order: Dictionary containing order information
            
        Returns:
            True if storage succeeded, False otherwise
        """
        point = {
            "measurement": "orders",
            "tags": {
                "symbol": order.get("symbol", "unknown"),
                "side": order.get("side", "unknown"),
                "type": order.get("type", "unknown"),
                "order_id": order.get("id", "unknown"),
                "status": order.get("status", "unknown")
            },
            "fields": {
                "price": float(order.get("price", 0)),
                "amount": float(order.get("amount", 0)),
                "filled": float(order.get("filled", 0)),
                "remaining": float(order.get("remaining", 0)),
                "cost": float(order.get("cost", 0))
            },
            "time": int(order.get("timestamp", time.time() * 1000) / 1000)  # milliseconds to seconds
        }
        
        return self.write_points([point])
    
    def store_market_data(self, symbol: str, 
                         interval: str, 
                         data: Union[Dict, pd.DataFrame]) -> bool:
        """
        Store market data (OHLCV) in the database.
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTC/USDT')
            interval: Candle interval (e.g., '1m', '1h', '1d')
            data: Market data as dictionary or DataFrame
            
        Returns:
            True if storage succeeded, False otherwise
        """
        if isinstance(data, pd.DataFrame):
            # Ensure DataFrame has required columns
            required_columns = ['open', 'high', 'low', 'close', 'volume']
            if not all(col in data.columns for col in required_columns):
                logger.error(f"DataFrame missing required columns: {required_columns}")
                return False
            
            # Add symbol and interval as tags
            data['symbol'] = symbol
            data['interval'] = interval
            
            return self.write_dataframe(
                data,
                measurement='ohlcv',
                tag_columns=['symbol', 'interval'],
                field_columns=['open', 'high', 'low', 'close', 'volume'],
                time_column='time' if 'time' in data.columns else None
            )
        
        elif isinstance(data, Dict):
            # Convert dictionary to point format
            point = {
                "measurement": "ohlcv",
                "tags": {
                    "symbol": symbol,
                    "interval": interval
                },
                "fields": {
                    "open": float(data.get("open", 0)),
                    "high": float(data.get("high", 0)),
                    "low": float(data.get("low", 0)),
                    "close": float(data.get("close", 0)),
                    "volume": float(data.get("volume", 0))
                }
            }
            
            # Add timestamp if available
            if "time" in data:
                point["time"] = data["time"]
            elif "timestamp" in data:
                point["time"] = int(data["timestamp"] / 1000) if data["timestamp"] > 1e10 else data["timestamp"]
            
            return self.write_points([point])
        
        else:
            logger.error(f"Unsupported data type: {type(data)}")
            return False
    
    def close(self) -> None:
        """Close all connections in the pool."""
        for client in self.pool:
            try:
                client.close()
            except Exception as e:
                logger.error(f"Error closing InfluxDB connection: {str(e)}")
        self.pool = []


# Global TimeSeriesDB instance
_timeseries_db_instance = None


def get_timeseries_db(config: Optional[Dict] = None) -> TimeSeriesDB:
    """
    Get the global TimeSeriesDB instance (singleton pattern).
    
    Args:
        config: Optional configuration to use when creating the instance
        
    Returns:
        Global TimeSeriesDB instance
    """
    global _timeseries_db_instance
    
    if _timeseries_db_instance is None:
        _timeseries_db_instance = TimeSeriesDB(config)
    
    return _timeseries_db_instance 