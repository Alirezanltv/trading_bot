"""
Time Series Database Adapter for Market Data

This module provides a high-performance adapter for storing and retrieving market data
using time-series databases. It supports multiple backends including InfluxDB and TimescaleDB
with automatic failover and batched operations for efficiency.

Features:
- Multi-database backend support (InfluxDB, TimescaleDB)
- Automatic batching and efficient writes
- Connection pooling and automatic reconnection
- Circuit breaker pattern for fault tolerance
- Query optimization for high-frequency data
- Caching layer for frequently accessed data
- Background health monitoring
"""

import asyncio
import datetime
import enum
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

import influxdb_client
from influxdb_client.client.write_api import ASYNCHRONOUS
import pandas as pd
import psycopg2
import pybreaker
from redis import Redis
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class TimeSeriesBackend(enum.Enum):
    """Supported time-series database backends."""
    INFLUXDB = "influxdb"
    TIMESCALEDB = "timescaledb"
    MEMORY = "memory"  # For testing


@dataclass
class DataPoint:
    """A single time-series data point."""
    timestamp: datetime.datetime
    measurement: str
    tags: Dict[str, str]
    fields: Dict[str, Union[float, int, str, bool]]
    
    def to_influx_point(self) -> dict:
        """Convert to InfluxDB point format."""
        return {
            "measurement": self.measurement,
            "tags": self.tags,
            "fields": self.fields,
            "time": self.timestamp
        }
    
    def to_timescale_format(self) -> Tuple[str, List]:
        """Convert to TimescaleDB SQL and parameters."""
        columns = ["time"]
        values = [self.timestamp]
        placeholders = ["%s"]
        
        # Add tags as columns
        for tag_name, tag_value in self.tags.items():
            columns.append(f"tag_{tag_name}")
            values.append(tag_value)
            placeholders.append("%s")
        
        # Add fields as columns
        for field_name, field_value in self.fields.items():
            columns.append(f"field_{field_name}")
            values.append(field_value)
            placeholders.append("%s")
        
        sql = f"""
        INSERT INTO {self.measurement} ({', '.join(columns)})
        VALUES ({', '.join(placeholders)})
        """
        
        return sql, values


class TimeSeriesAdapter:
    """
    Adapter for time-series databases with high performance and reliability features.
    
    Supports multiple backends with automatic failover, batched writes, and
    optimized queries for market data storage and retrieval.
    """
    
    def __init__(
        self,
        config: Dict[str, Any],
        primary_backend: TimeSeriesBackend = TimeSeriesBackend.INFLUXDB,
        fallback_backend: Optional[TimeSeriesBackend] = TimeSeriesBackend.MEMORY,
        batch_size: int = 1000,
        flush_interval: float = 1.0,
        max_pending_batches: int = 10,
        retry_attempts: int = 3,
        cache_enabled: bool = True,
        cache_ttl: int = 60,  # seconds
    ):
        """
        Initialize the time-series adapter.
        
        Args:
            config: Configuration dictionary containing connection parameters
            primary_backend: Primary database backend
            fallback_backend: Fallback backend if primary fails
            batch_size: Maximum number of points to batch before writing
            flush_interval: Maximum time to wait before flushing batch (seconds)
            max_pending_batches: Maximum number of batches to queue if writes are failing
            retry_attempts: Number of retry attempts for failed operations
            cache_enabled: Whether to enable Redis caching for frequent queries
            cache_ttl: Time-to-live for cached data (seconds)
        """
        self.config = config
        self.primary_backend = primary_backend
        self.fallback_backend = fallback_backend
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.max_pending_batches = max_pending_batches
        self.retry_attempts = retry_attempts
        self.cache_enabled = cache_enabled
        self.cache_ttl = cache_ttl
        
        # Initialize connections
        self.influx_client = None
        self.influx_write_api = None
        self.timescale_conn = None
        self.redis_client = None
        
        # Batch management
        self.pending_points = []
        self.flush_task = None
        self.flush_lock = asyncio.Lock()
        self.is_running = False
        
        # Circuit breakers
        self.influx_breaker = pybreaker.CircuitBreaker(
            fail_max=5,
            reset_timeout=30,
            name="influxdb_circuit"
        )
        
        self.timescale_breaker = pybreaker.CircuitBreaker(
            fail_max=5,
            reset_timeout=30,
            name="timescaledb_circuit"
        )
        
        # In-memory fallback (for testing or when all DBs are down)
        self.memory_storage = {}
        
        # Initialize connections
        self._initialize_connections()
        
        if cache_enabled:
            self._initialize_cache()
    
    def _initialize_connections(self):
        """Initialize database connections based on configuration."""
        # Initialize InfluxDB if it's primary or fallback
        if (self.primary_backend == TimeSeriesBackend.INFLUXDB or 
            self.fallback_backend == TimeSeriesBackend.INFLUXDB):
            try:
                self.influx_client = influxdb_client.InfluxDBClient(
                    url=self.config.get("influxdb_url", "http://localhost:8086"),
                    token=self.config.get("influxdb_token", ""),
                    org=self.config.get("influxdb_org", ""),
                    timeout=10000
                )
                self.influx_write_api = self.influx_client.write_api(write_options=ASYNCHRONOUS)
                logger.info("InfluxDB connection initialized")
            except Exception as e:
                logger.error(f"Failed to initialize InfluxDB connection: {e}")
                if self.primary_backend == TimeSeriesBackend.INFLUXDB:
                    logger.warning("Primary backend initialization failed, will try fallback")
        
        # Initialize TimescaleDB if it's primary or fallback
        if (self.primary_backend == TimeSeriesBackend.TIMESCALEDB or 
            self.fallback_backend == TimeSeriesBackend.TIMESCALEDB):
            try:
                self.timescale_conn = psycopg2.connect(
                    dbname=self.config.get("timescale_dbname", "market_data"),
                    user=self.config.get("timescale_user", "postgres"),
                    password=self.config.get("timescale_password", ""),
                    host=self.config.get("timescale_host", "localhost"),
                    port=self.config.get("timescale_port", 5432)
                )
                logger.info("TimescaleDB connection initialized")
            except Exception as e:
                logger.error(f"Failed to initialize TimescaleDB connection: {e}")
                if self.primary_backend == TimeSeriesBackend.TIMESCALEDB:
                    logger.warning("Primary backend initialization failed, will try fallback")
    
    def _initialize_cache(self):
        """Initialize Redis cache for query results."""
        try:
            self.redis_client = Redis(
                host=self.config.get("redis_host", "localhost"),
                port=self.config.get("redis_port", 6379),
                db=self.config.get("redis_db", 0),
                password=self.config.get("redis_password", None),
            )
            logger.info("Redis cache initialized")
        except Exception as e:
            logger.warning(f"Failed to initialize Redis cache, caching disabled: {e}")
            self.cache_enabled = False
    
    async def start(self):
        """Start the adapter and background tasks."""
        if self.is_running:
            return
        
        self.is_running = True
        self.flush_task = asyncio.create_task(self._periodic_flush())
        logger.info("Time series adapter started")
    
    async def stop(self):
        """Stop the adapter and clean up resources."""
        if not self.is_running:
            return
        
        self.is_running = False
        if self.flush_task:
            self.flush_task.cancel()
            try:
                await self.flush_task
            except asyncio.CancelledError:
                pass
        
        # Flush any remaining points
        await self.flush()
        
        # Close connections
        if self.influx_client:
            self.influx_client.close()
        
        if self.timescale_conn:
            self.timescale_conn.close()
        
        if self.redis_client:
            self.redis_client.close()
        
        logger.info("Time series adapter stopped")
    
    async def write_point(self, point: DataPoint):
        """
        Write a single data point to the time-series database.
        
        The point will be added to a batch and written asynchronously.
        """
        self.pending_points.append(point)
        
        # If batch size threshold is reached, trigger a flush
        if len(self.pending_points) >= self.batch_size:
            asyncio.create_task(self.flush())
    
    async def write_points(self, points: List[DataPoint]):
        """Write multiple data points as a batch."""
        self.pending_points.extend(points)
        
        # If batch size threshold is reached, trigger a flush
        if len(self.pending_points) >= self.batch_size:
            asyncio.create_task(self.flush())
    
    async def _periodic_flush(self):
        """Background task to periodically flush pending points."""
        while self.is_running:
            try:
                await asyncio.sleep(self.flush_interval)
                if self.pending_points:
                    await self.flush()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic flush: {e}")
    
    async def flush(self):
        """Flush all pending points to the database."""
        async with self.flush_lock:
            if not self.pending_points:
                return
            
            points_to_write = self.pending_points.copy()
            self.pending_points = []
            
            await self._write_to_backend(points_to_write)
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True
    )
    async def _write_to_backend(self, points: List[DataPoint]):
        """Write points to the configured backend with retry logic."""
        if not points:
            return
        
        success = False
        
        # Try primary backend first
        if self.primary_backend == TimeSeriesBackend.INFLUXDB:
            try:
                success = await self._write_to_influxdb(points)
            except Exception as e:
                logger.error(f"Error writing to InfluxDB: {e}")
                success = False
        
        elif self.primary_backend == TimeSeriesBackend.TIMESCALEDB:
            try:
                success = await self._write_to_timescaledb(points)
            except Exception as e:
                logger.error(f"Error writing to TimescaleDB: {e}")
                success = False
        
        # If primary fails and we have a fallback, try it
        if not success and self.fallback_backend:
            logger.warning(f"Primary backend write failed, trying fallback: {self.fallback_backend}")
            
            if self.fallback_backend == TimeSeriesBackend.INFLUXDB:
                try:
                    success = await self._write_to_influxdb(points)
                except Exception as e:
                    logger.error(f"Error writing to fallback InfluxDB: {e}")
                    success = False
            
            elif self.fallback_backend == TimeSeriesBackend.TIMESCALEDB:
                try:
                    success = await self._write_to_timescaledb(points)
                except Exception as e:
                    logger.error(f"Error writing to fallback TimescaleDB: {e}")
                    success = False
            
            elif self.fallback_backend == TimeSeriesBackend.MEMORY:
                # Memory fallback never fails
                self._write_to_memory(points)
                success = True
        
        # If all backends fail, store to memory as last resort
        if not success:
            logger.error("All database backends failed, storing to in-memory fallback")
            self._write_to_memory(points)
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def _write_to_influxdb(self, points: List[DataPoint]) -> bool:
        """Write points to InfluxDB."""
        if not self.influx_client or not self.influx_write_api:
            return False
        
        try:
            # Use circuit breaker to prevent hammering a failing database
            @self.influx_breaker
            async def _write_with_breaker():
                batch = [point.to_influx_point() for point in points]
                bucket = self.config.get("influxdb_bucket", "market_data")
                
                # Convert to InfluxDB line protocol and write
                await self.influx_write_api.write(bucket=bucket, record=batch)
                return True
            
            return await _write_with_breaker()
        except pybreaker.CircuitBreakerError:
            logger.error("InfluxDB circuit breaker open, skipping write")
            return False
        except Exception as e:
            logger.error(f"Error writing to InfluxDB: {e}")
            return False
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def _write_to_timescaledb(self, points: List[DataPoint]) -> bool:
        """Write points to TimescaleDB."""
        if not self.timescale_conn:
            return False
        
        try:
            # Use circuit breaker to prevent hammering a failing database
            @self.timescale_breaker
            def _write_with_breaker():
                # Group points by measurement (table)
                points_by_measurement = {}
                for point in points:
                    if point.measurement not in points_by_measurement:
                        points_by_measurement[point.measurement] = []
                    points_by_measurement[point.measurement].append(point)
                
                with self.timescale_conn.cursor() as cursor:
                    for measurement, measurement_points in points_by_measurement.items():
                        # Prepare batch insert
                        for point in measurement_points:
                            sql, values = point.to_timescale_format()
                            cursor.execute(sql, values)
                
                self.timescale_conn.commit()
                return True
            
            return _write_with_breaker()
        except pybreaker.CircuitBreakerError:
            logger.error("TimescaleDB circuit breaker open, skipping write")
            return False
        except Exception as e:
            logger.error(f"Error writing to TimescaleDB: {e}")
            # Try to reconnect if connection is closed
            if "connection is closed" in str(e):
                try:
                    self._initialize_connections()
                except Exception as conn_err:
                    logger.error(f"Failed to reconnect to TimescaleDB: {conn_err}")
            return False
    
    def _write_to_memory(self, points: List[DataPoint]):
        """Write points to in-memory storage (fallback)."""
        for point in points:
            if point.measurement not in self.memory_storage:
                self.memory_storage[point.measurement] = []
            
            self.memory_storage[point.measurement].append({
                "timestamp": point.timestamp,
                "tags": point.tags,
                "fields": point.fields
            })
            
            # Limit memory storage size to prevent OOM
            max_points = self.config.get("memory_max_points", 100000)
            if len(self.memory_storage[point.measurement]) > max_points:
                # Remove oldest points
                self.memory_storage[point.measurement] = self.memory_storage[point.measurement][-max_points:]
    
    async def query(
        self,
        measurement: str,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        tags: Optional[Dict[str, str]] = None,
        fields: Optional[List[str]] = None,
        aggregation: Optional[str] = None,
        interval: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Query time-series data and return as pandas DataFrame.
        
        Args:
            measurement: The measurement (table) to query
            start_time: Start time for the query range
            end_time: End time for the query range
            tags: Optional tag filters as key-value pairs
            fields: Optional list of fields to return (default: all)
            aggregation: Optional aggregation function (mean, sum, min, max)
            interval: Optional time bucket for aggregation (e.g., '1h', '5m')
            limit: Optional limit on number of results
            
        Returns:
            DataFrame containing the query results
        """
        # Generate cache key if caching is enabled
        cache_key = None
        if self.cache_enabled and self.redis_client:
            cache_key = self._generate_cache_key(
                measurement, start_time, end_time, tags, fields, aggregation, interval, limit
            )
            # Try to get from cache first
            cached_result = self._get_from_cache(cache_key)
            if cached_result is not None:
                return cached_result
        
        # Not in cache or caching disabled, query database
        result = None
        
        # Try primary backend first
        if self.primary_backend == TimeSeriesBackend.INFLUXDB:
            try:
                result = await self._query_influxdb(
                    measurement, start_time, end_time, tags, fields, aggregation, interval, limit
                )
            except Exception as e:
                logger.error(f"Error querying InfluxDB: {e}")
                result = None
        
        elif self.primary_backend == TimeSeriesBackend.TIMESCALEDB:
            try:
                result = await self._query_timescaledb(
                    measurement, start_time, end_time, tags, fields, aggregation, interval, limit
                )
            except Exception as e:
                logger.error(f"Error querying TimescaleDB: {e}")
                result = None
        
        # If primary fails and we have a fallback, try it
        if result is None and self.fallback_backend:
            logger.warning(f"Primary backend query failed, trying fallback: {self.fallback_backend}")
            
            if self.fallback_backend == TimeSeriesBackend.INFLUXDB:
                try:
                    result = await self._query_influxdb(
                        measurement, start_time, end_time, tags, fields, aggregation, interval, limit
                    )
                except Exception as e:
                    logger.error(f"Error querying fallback InfluxDB: {e}")
                    result = None
            
            elif self.fallback_backend == TimeSeriesBackend.TIMESCALEDB:
                try:
                    result = await self._query_timescaledb(
                        measurement, start_time, end_time, tags, fields, aggregation, interval, limit
                    )
                except Exception as e:
                    logger.error(f"Error querying fallback TimescaleDB: {e}")
                    result = None
            
            elif self.fallback_backend == TimeSeriesBackend.MEMORY:
                result = self._query_memory(
                    measurement, start_time, end_time, tags, fields, aggregation, interval, limit
                )
        
        # If all backends fail, try memory storage
        if result is None:
            logger.warning("All database backends failed, querying in-memory fallback")
            result = self._query_memory(
                measurement, start_time, end_time, tags, fields, aggregation, interval, limit
            )
        
        # Cache the result if caching is enabled
        if self.cache_enabled and self.redis_client and cache_key and result is not None:
            self._store_in_cache(cache_key, result)
        
        return result if result is not None else pd.DataFrame()
    
    async def _query_influxdb(
        self,
        measurement: str,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        tags: Optional[Dict[str, str]] = None,
        fields: Optional[List[str]] = None,
        aggregation: Optional[str] = None,
        interval: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Optional[pd.DataFrame]:
        """Query data from InfluxDB."""
        if not self.influx_client:
            return None
        
        try:
            # Use circuit breaker to prevent hammering a failing database
            @self.influx_breaker
            async def _query_with_breaker():
                # Build Flux query
                query = f'from(bucket:"{self.config.get("influxdb_bucket", "market_data")}")'
                query += f'\n  |> range(start: {start_time.isoformat()}, stop: {end_time.isoformat()})'
                query += f'\n  |> filter(fn: (r) => r._measurement == "{measurement}")'
                
                # Add tag filters
                if tags:
                    for tag_name, tag_value in tags.items():
                        query += f'\n  |> filter(fn: (r) => r.{tag_name} == "{tag_value}")'
                
                # Add field filters
                if fields:
                    field_conditions = " or ".join([f'r._field == "{field}"' for field in fields])
                    query += f'\n  |> filter(fn: (r) => {field_conditions})'
                
                # Add aggregation if specified
                if aggregation and interval:
                    query += f'\n  |> aggregateWindow(every: {interval}, fn: {aggregation}, createEmpty: false)'
                
                # Add limit if specified
                if limit:
                    query += f'\n  |> limit(n: {limit})'
                
                # Execute query
                query_api = self.influx_client.query_api()
                result = await query_api.query_data_frame(query)
                
                # Process result into a clean DataFrame
                if isinstance(result, list) and len(result) > 0:
                    result = pd.concat(result)
                
                if not result.empty:
                    # Rename columns and clean up
                    result = result.rename(columns={'_time': 'time', '_value': 'value'})
                    result = result.drop(columns=['result', 'table', '_start', '_stop', '_measurement'], errors='ignore')
                    
                    # Pivot to get fields as columns if we have multiple fields
                    if '_field' in result.columns:
                        result = result.pivot(index=['time'] + list(set(result.columns) - {'time', '_field', 'value'}),
                                            columns='_field',
                                            values='value').reset_index()
                
                return result
            
            return await _query_with_breaker()
        except pybreaker.CircuitBreakerError:
            logger.error("InfluxDB circuit breaker open, skipping query")
            return None
        except Exception as e:
            logger.error(f"Error querying InfluxDB: {e}")
            return None
    
    async def _query_timescaledb(
        self,
        measurement: str,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        tags: Optional[Dict[str, str]] = None,
        fields: Optional[List[str]] = None,
        aggregation: Optional[str] = None,
        interval: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Optional[pd.DataFrame]:
        """Query data from TimescaleDB."""
        if not self.timescale_conn:
            return None
        
        try:
            # Use circuit breaker to prevent hammering a failing database
            @self.timescale_breaker
            def _query_with_breaker():
                # Start building SQL query
                field_clause = "*"
                if fields:
                    field_list = [f"field_{field}" for field in fields]
                    field_clause = "time, " + ", ".join(field_list)
                
                sql = f"SELECT {field_clause} FROM {measurement} WHERE time >= %s AND time <= %s"
                params = [start_time, end_time]
                
                # Add tag filters
                if tags:
                    for tag_name, tag_value in tags.items():
                        sql += f" AND tag_{tag_name} = %s"
                        params.append(tag_value)
                
                # Add aggregation if specified
                if aggregation and interval:
                    agg_map = {
                        "mean": "AVG",
                        "sum": "SUM",
                        "min": "MIN",
                        "max": "MAX",
                        "count": "COUNT",
                    }
                    sql_agg = agg_map.get(aggregation.lower(), "AVG")
                    
                    # Convert time bucket syntax
                    time_bucket = interval
                    if interval.endswith('m'):
                        minutes = int(interval[:-1])
                        time_bucket = f"'{minutes} minutes'"
                    elif interval.endswith('h'):
                        hours = int(interval[:-1])
                        time_bucket = f"'{hours} hours'"
                    elif interval.endswith('d'):
                        days = int(interval[:-1])
                        time_bucket = f"'{days} days'"
                    
                    # Rewrite query for aggregation
                    field_aggs = []
                    group_by = ["time_bucket"]
                    
                    if fields:
                        for field in fields:
                            field_aggs.append(f"{sql_agg}(field_{field}) AS field_{field}")
                    else:
                        # Get all field columns
                        cursor = self.timescale_conn.cursor()
                        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{measurement}' AND column_name LIKE 'field_%'")
                        field_columns = [row[0] for row in cursor.fetchall()]
                        cursor.close()
                        
                        for col in field_columns:
                            field_aggs.append(f"{sql_agg}({col}) AS {col}")
                    
                    sql = f"""
                    SELECT time_bucket({time_bucket}, time) AS time_bucket, 
                           {', '.join(field_aggs)}
                    FROM {measurement} 
                    WHERE time >= %s AND time <= %s
                    """
                    
                    # Add tag filters and group by
                    if tags:
                        for tag_name, tag_value in tags.items():
                            sql += f" AND tag_{tag_name} = %s"
                            group_by.append(f"tag_{tag_name}")
                    
                    sql += f" GROUP BY {', '.join(group_by)}"
                    sql += " ORDER BY time_bucket"
                
                else:
                    # Regular query, just add ordering
                    sql += " ORDER BY time"
                
                # Add limit if specified
                if limit:
                    sql += f" LIMIT %s"
                    params.append(limit)
                
                # Execute query
                cursor = self.timescale_conn.cursor()
                cursor.execute(sql, params)
                
                # Fetch results
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                cursor.close()
                
                # Convert to DataFrame
                df = pd.DataFrame(rows, columns=columns)
                
                # Rename time_bucket column to time if it exists
                if 'time_bucket' in df.columns:
                    df = df.rename(columns={'time_bucket': 'time'})
                
                return df
            
            return _query_with_breaker()
        except pybreaker.CircuitBreakerError:
            logger.error("TimescaleDB circuit breaker open, skipping query")
            return None
        except Exception as e:
            logger.error(f"Error querying TimescaleDB: {e}")
            # Try to reconnect if connection is closed
            if "connection is closed" in str(e):
                try:
                    self._initialize_connections()
                except Exception as conn_err:
                    logger.error(f"Failed to reconnect to TimescaleDB: {conn_err}")
            return None
    
    def _query_memory(
        self,
        measurement: str,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        tags: Optional[Dict[str, str]] = None,
        fields: Optional[List[str]] = None,
        aggregation: Optional[str] = None,
        interval: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Query data from in-memory storage."""
        if measurement not in self.memory_storage:
            return pd.DataFrame()
        
        # Filter by time range
        data = self.memory_storage[measurement]
        filtered_data = [
            point for point in data
            if start_time <= point["timestamp"] <= end_time
        ]
        
        # Filter by tags
        if tags:
            filtered_data = [
                point for point in filtered_data
                if all(point["tags"].get(tag_name) == tag_value for tag_name, tag_value in tags.items())
            ]
        
        # Convert to DataFrame
        if not filtered_data:
            return pd.DataFrame()
        
        # Create DataFrame
        records = []
        for point in filtered_data:
            record = {"time": point["timestamp"]}
            record.update({f"tag_{k}": v for k, v in point["tags"].items()})
            record.update({f"field_{k}": v for k, v in point["fields"].items()})
            records.append(record)
        
        df = pd.DataFrame(records)
        
        # Filter fields if specified
        if fields and not df.empty:
            field_columns = [f"field_{field}" for field in fields]
            keep_columns = ["time"] + [col for col in df.columns if col.startswith("tag_")] + field_columns
            df = df[[col for col in keep_columns if col in df.columns]]
        
        # Apply aggregation if specified
        if aggregation and interval and not df.empty:
            # Convert interval string to pandas frequency string
            freq = None
            if interval.endswith('m'):
                freq = f"{interval[:-1]}min"
            elif interval.endswith('h'):
                freq = f"{interval[:-1]}H"
            elif interval.endswith('d'):
                freq = f"{interval[:-1]}D"
            
            if freq:
                # Group by time interval
                df['time'] = pd.to_datetime(df['time'])
                df.set_index('time', inplace=True)
                
                # Group by all tag columns
                tag_columns = [col for col in df.columns if col.startswith("tag_")]
                
                # Apply aggregation
                agg_map = {
                    "mean": "mean",
                    "sum": "sum",
                    "min": "min",
                    "max": "max",
                    "count": "count",
                }
                agg_func = agg_map.get(aggregation.lower(), "mean")
                
                field_columns = [col for col in df.columns if col.startswith("field_")]
                
                if tag_columns:
                    df = df.groupby([pd.Grouper(freq=freq)] + tag_columns)[field_columns].agg(agg_func)
                else:
                    df = df.groupby(pd.Grouper(freq=freq))[field_columns].agg(agg_func)
                
                df.reset_index(inplace=True)
        
        # Apply limit if specified
        if limit and not df.empty:
            df = df.head(limit)
        
        return df
    
    def _generate_cache_key(self, *args) -> str:
        """Generate a deterministic cache key from query parameters."""
        # Convert all args to strings and join with separator
        key_parts = []
        for arg in args:
            if arg is None:
                key_parts.append("null")
            elif isinstance(arg, (datetime.datetime, datetime.date)):
                key_parts.append(arg.isoformat())
            elif isinstance(arg, dict):
                # Sort dict items for deterministic key
                sorted_items = sorted(arg.items())
                key_parts.append(str(sorted_items))
            elif isinstance(arg, list):
                # Sort list items for deterministic key
                key_parts.append(str(sorted(arg)))
            else:
                key_parts.append(str(arg))
        
        key = "tsdb:" + ":".join(key_parts)
        return key
    
    def _get_from_cache(self, key: str) -> Optional[pd.DataFrame]:
        """Retrieve data from Redis cache."""
        if not self.cache_enabled or not self.redis_client:
            return None
        
        try:
            cached_data = self.redis_client.get(key)
            if cached_data:
                # Deserialize from JSON
                json_data = json.loads(cached_data)
                df = pd.DataFrame(json_data)
                
                # Convert time column to datetime if it exists
                if 'time' in df.columns and not df.empty:
                    df['time'] = pd.to_datetime(df['time'])
                
                return df
        except Exception as e:
            logger.warning(f"Error retrieving from cache: {e}")
        
        return None
    
    def _store_in_cache(self, key: str, df: pd.DataFrame):
        """Store data in Redis cache."""
        if not self.cache_enabled or not self.redis_client or df.empty:
            return
        
        try:
            # Convert to JSON-compatible format
            json_data = df.to_json(orient="records", date_format="iso")
            
            # Store in Redis with TTL
            self.redis_client.setex(key, self.cache_ttl, json_data)
        except Exception as e:
            logger.warning(f"Error storing in cache: {e}")
    
    async def health_check(self) -> Dict[str, bool]:
        """
        Check the health of all database connections.
        
        Returns:
            Dictionary with health status for each backend
        """
        health = {
            "influxdb": False,
            "timescaledb": False,
            "memory": True,  # Memory storage is always healthy
            "redis": False if self.cache_enabled else None,
        }
        
        # Check InfluxDB
        if self.influx_client:
            try:
                health_api = self.influx_client.health()
                health["influxdb"] = health_api.status == "pass"
            except Exception:
                health["influxdb"] = False
        
        # Check TimescaleDB
        if self.timescale_conn:
            try:
                cursor = self.timescale_conn.cursor()
                cursor.execute("SELECT 1")
                health["timescaledb"] = cursor.fetchone()[0] == 1
                cursor.close()
            except Exception:
                health["timescaledb"] = False
        
        # Check Redis
        if self.cache_enabled and self.redis_client:
            try:
                health["redis"] = self.redis_client.ping()
            except Exception:
                health["redis"] = False
        
        return health
    
    @property
    def stats(self) -> Dict[str, Any]:
        """
        Get statistics about the time-series adapter.
        
        Returns:
            Dictionary with statistics
        """
        return {
            "pending_points": len(self.pending_points),
            "memory_storage_size": {
                measurement: len(points)
                for measurement, points in self.memory_storage.items()
            },
            "primary_backend": self.primary_backend.value,
            "fallback_backend": self.fallback_backend.value if self.fallback_backend else None,
            "influxdb_circuit": {
                "open": self.influx_breaker.current_state == "open",
                "failure_count": self.influx_breaker.failure_count,
            } if self.influx_client else None,
            "timescaledb_circuit": {
                "open": self.timescale_breaker.current_state == "open",
                "failure_count": self.timescale_breaker.failure_count,
            } if self.timescale_conn else None,
            "cache_enabled": self.cache_enabled,
        } 