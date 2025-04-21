"""
Time Series Database Schema Initializer

This module handles schema creation and management for time-series databases
used in the trading system. It supports both InfluxDB and TimescaleDB,
creating the necessary tables, buckets, and permissions.

Features:
- Automatic schema creation and updates
- Database migration handling
- Creation of specialized tables for different market data types
- Setup of retention policies and continuous aggregates
- Index optimization for common query patterns
"""

import asyncio
import logging
import os
import sys
from typing import Dict, List, Optional, Any, Tuple

import influxdb_client
from influxdb_client.client.bucket_api import BucketApi
from influxdb_client.domain.bucket import Bucket
from influxdb_client.domain.bucket_retention_rules import BucketRetentionRules
import psycopg2
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class TimeSeriesSchemaManager:
    """
    Manages time-series database schemas for market data storage.
    
    Handles creation and maintenance of database schemas for both
    InfluxDB and TimescaleDB backends, ensuring optimal performance
    for market data operations.
    """
    
    def __init__(
        self,
        config: Dict[str, Any]
    ):
        """
        Initialize the schema manager.
        
        Args:
            config: Configuration dictionary containing database connection parameters
        """
        self.config = config
        self.influx_client = None
        self.timescale_conn = None
        
        # Standard market data measurements/tables
        self.standard_measurements = [
            "price_candles",       # OHLCV candles
            "trades",              # Individual trades
            "order_book",          # Order book snapshots
            "ticker",              # Latest price tickers
            "indicators",          # Technical indicators
            "signals",             # Trading signals
            "system_metrics"       # Performance metrics
        ]
        
        # Schema versions for migration management
        self.current_influxdb_schema_version = 1
        self.current_timescaledb_schema_version = 1
        
        # Initialize connections
        self._initialize_connections()
    
    def _initialize_connections(self):
        """Initialize database connections."""
        # Initialize InfluxDB connection
        if self.config.get("use_influxdb", True):
            try:
                self.influx_client = influxdb_client.InfluxDBClient(
                    url=self.config.get("influxdb_url", "http://localhost:8086"),
                    token=self.config.get("influxdb_token", ""),
                    org=self.config.get("influxdb_org", ""),
                    timeout=30000
                )
                logger.info("InfluxDB connection initialized for schema management")
            except Exception as e:
                logger.error(f"Failed to initialize InfluxDB connection: {e}")
        
        # Initialize TimescaleDB connection
        if self.config.get("use_timescaledb", False):
            try:
                self.timescale_conn = psycopg2.connect(
                    dbname=self.config.get("timescale_dbname", "market_data"),
                    user=self.config.get("timescale_user", "postgres"),
                    password=self.config.get("timescale_password", ""),
                    host=self.config.get("timescale_host", "localhost"),
                    port=self.config.get("timescale_port", 5432)
                )
                logger.info("TimescaleDB connection initialized for schema management")
            except Exception as e:
                logger.error(f"Failed to initialize TimescaleDB connection: {e}")
    
    def close(self):
        """Close all database connections."""
        if self.influx_client:
            self.influx_client.close()
        
        if self.timescale_conn:
            self.timescale_conn.close()
    
    async def setup_schemas(self) -> bool:
        """
        Set up all required database schemas.
        
        Returns:
            Success status
        """
        success = True
        
        if self.config.get("use_influxdb", True) and self.influx_client:
            influx_success = await self.setup_influxdb_schema()
            if not influx_success:
                logger.error("Failed to set up InfluxDB schema")
                success = False
        
        if self.config.get("use_timescaledb", False) and self.timescale_conn:
            timescale_success = await self.setup_timescaledb_schema()
            if not timescale_success:
                logger.error("Failed to set up TimescaleDB schema")
                success = False
        
        return success
    
    async def setup_influxdb_schema(self) -> bool:
        """
        Set up InfluxDB schema with buckets and retention policies.
        
        Returns:
            Success status
        """
        logger.info("Setting up InfluxDB schema")
        
        try:
            # Create or update buckets
            bucket_api = self.influx_client.buckets_api()
            org_id = self._get_influxdb_org_id()
            
            if not org_id:
                logger.error("Failed to get InfluxDB organization ID")
                return False
            
            # Create standard buckets with different retention policies
            buckets_to_create = [
                {
                    "name": self.config.get("influxdb_bucket", "market_data"),
                    "description": "Primary market data bucket",
                    "retention_days": self.config.get("default_retention_days", 30)
                },
                {
                    "name": "market_data_long_term",
                    "description": "Long-term market data storage",
                    "retention_days": self.config.get("long_term_retention_days", 365)
                },
                {
                    "name": "system_metrics",
                    "description": "Trading system performance metrics",
                    "retention_days": self.config.get("metrics_retention_days", 7)
                }
            ]
            
            for bucket_config in buckets_to_create:
                bucket_name = bucket_config["name"]
                retention_days = bucket_config["retention_days"]
                description = bucket_config["description"]
                
                # Check if bucket exists
                existing_bucket = self._find_bucket_by_name(bucket_api, bucket_name)
                
                if existing_bucket:
                    logger.info(f"Bucket '{bucket_name}' already exists, updating")
                    # Update retention policy if needed
                    retention_seconds = retention_days * 86400
                    rules = BucketRetentionRules(type="expire", every_seconds=retention_seconds)
                    existing_bucket.retention_rules = [rules]
                    bucket_api.update_bucket(existing_bucket)
                else:
                    logger.info(f"Creating bucket '{bucket_name}'")
                    retention_seconds = retention_days * 86400
                    rules = BucketRetentionRules(type="expire", every_seconds=retention_seconds)
                    bucket = Bucket(
                        name=bucket_name,
                        description=description,
                        org_id=org_id,
                        retention_rules=[rules]
                    )
                    bucket_api.create_bucket(bucket)
            
            # Create tasks for continuous queries / downsampling if needed
            if self.config.get("setup_downsampling", True):
                await self._setup_influxdb_downsampling()
            
            logger.info("InfluxDB schema setup completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error setting up InfluxDB schema: {e}")
            return False
    
    def _get_influxdb_org_id(self) -> Optional[str]:
        """Get the organization ID for InfluxDB."""
        try:
            org_name = self.config.get("influxdb_org", "")
            organizations_api = self.influx_client.organizations_api()
            orgs = organizations_api.find_organizations()
            
            for org in orgs:
                if org.name == org_name:
                    return org.id
            
            logger.warning(f"Organization '{org_name}' not found")
            return None
            
        except Exception as e:
            logger.error(f"Error getting organization ID: {e}")
            return None
    
    def _find_bucket_by_name(self, bucket_api: BucketApi, name: str) -> Optional[Bucket]:
        """Find a bucket by name."""
        try:
            buckets = bucket_api.find_buckets().buckets
            for bucket in buckets:
                if bucket.name == name:
                    return bucket
            return None
        except Exception as e:
            logger.error(f"Error finding bucket '{name}': {e}")
            return None
    
    async def _setup_influxdb_downsampling(self) -> bool:
        """
        Set up InfluxDB downsampling tasks for data aggregation.
        
        Returns:
            Success status
        """
        logger.info("Setting up InfluxDB downsampling tasks")
        
        try:
            # These would be Flux language tasks to downsample data
            # Create tasks for different aggregation intervals
            tasks_api = self.influx_client.tasks_api()
            org_name = self.config.get("influxdb_org", "")
            source_bucket = self.config.get("influxdb_bucket", "market_data")
            target_bucket = "market_data_long_term"
            
            # Define downsampling tasks
            downsample_tasks = [
                {
                    "name": "downsample_price_candles_1h",
                    "interval": "1h",
                    "measurement": "price_candles",
                    "fields": ["open", "high", "low", "close", "volume"],
                    "retention": "30d"
                },
                {
                    "name": "downsample_price_candles_1d",
                    "interval": "1d",
                    "measurement": "price_candles",
                    "fields": ["open", "high", "low", "close", "volume"],
                    "retention": "365d"
                }
            ]
            
            for task_config in downsample_tasks:
                # Create flux script for downsampling
                measurement = task_config["measurement"]
                interval = task_config["interval"]
                fields_list = task_config["fields"]
                
                # Build Flux script
                flux_script = f'''
                option task = {{name: "{task_config["name"]}", every: {interval}}}
                
                from(bucket: "{source_bucket}")
                    |> range(start: -{interval}, stop: now())
                    |> filter(fn: (r) => r._measurement == "{measurement}")
                    |> filter(fn: (r) => {" or ".join([f'r._field == "{field}"' for field in fields_list])})
                    |> aggregateWindow(every: {interval}, fn: last)
                    |> to(bucket: "{target_bucket}", org: "{org_name}")
                '''
                
                # Check if task exists and update or create
                existing_tasks = tasks_api.find_tasks(name=task_config["name"])
                
                if existing_tasks and len(existing_tasks.tasks) > 0:
                    logger.info(f"Task '{task_config['name']}' already exists, updating")
                    task = existing_tasks.tasks[0]
                    task.flux = flux_script
                    tasks_api.update_task(task)
                else:
                    logger.info(f"Creating task '{task_config['name']}'")
                    tasks_api.create_task_every(task_config["name"], flux_script, 
                                              task_config["interval"], org_name)
            
            logger.info("InfluxDB downsampling setup completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error setting up InfluxDB downsampling: {e}")
            return False
    
    async def setup_timescaledb_schema(self) -> bool:
        """
        Set up TimescaleDB schema with hypertables and indexes.
        
        Returns:
            Success status
        """
        logger.info("Setting up TimescaleDB schema")
        
        try:
            if not self.timescale_conn:
                logger.error("TimescaleDB connection not initialized")
                return False
            
            # Check if TimescaleDB extension is installed
            with self.timescale_conn.cursor() as cursor:
                cursor.execute("SELECT extname FROM pg_extension WHERE extname = 'timescaledb'")
                if not cursor.fetchone():
                    logger.error("TimescaleDB extension not installed in database")
                    return False
            
            # Setup schema version tracking table
            self._setup_schema_version_table()
            
            # Get current schema version
            current_version = self._get_timescaledb_schema_version()
            
            # Apply migrations if needed
            if current_version < self.current_timescaledb_schema_version:
                self._apply_timescaledb_migrations(current_version)
            
            # Set up tables for each measurement
            for measurement in self.standard_measurements:
                success = await self._setup_timescaledb_table(measurement)
                if not success:
                    logger.error(f"Failed to set up table for measurement: {measurement}")
                    return False
            
            # Create continuous aggregates for downsampling
            if self.config.get("setup_downsampling", True):
                await self._setup_timescaledb_continuous_aggregates()
            
            logger.info("TimescaleDB schema setup completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error setting up TimescaleDB schema: {e}")
            return False
    
    def _setup_schema_version_table(self):
        """Set up schema version tracking table."""
        try:
            with self.timescale_conn.cursor() as cursor:
                # Create schema version table if it doesn't exist
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS schema_version (
                        id SERIAL PRIMARY KEY,
                        version INTEGER NOT NULL,
                        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        description TEXT
                    )
                """)
                
                # Check if we need to insert initial version
                cursor.execute("SELECT COUNT(*) FROM schema_version")
                count = cursor.fetchone()[0]
                
                if count == 0:
                    cursor.execute("""
                        INSERT INTO schema_version (version, description)
                        VALUES (1, 'Initial schema version')
                    """)
                
                self.timescale_conn.commit()
                
        except Exception as e:
            logger.error(f"Error setting up schema version table: {e}")
            self.timescale_conn.rollback()
    
    def _get_timescaledb_schema_version(self) -> int:
        """Get the current TimescaleDB schema version."""
        try:
            with self.timescale_conn.cursor() as cursor:
                cursor.execute("SELECT MAX(version) FROM schema_version")
                result = cursor.fetchone()
                return result[0] if result and result[0] else 0
                
        except Exception as e:
            logger.error(f"Error getting schema version: {e}")
            return 0
    
    def _apply_timescaledb_migrations(self, current_version: int):
        """
        Apply schema migrations from current version to latest.
        
        Args:
            current_version: Current schema version
        """
        logger.info(f"Applying TimescaleDB migrations from version {current_version}")
        
        migrations = []
        
        # Define migrations as list of (version, description, commands)
        # Each migration moves from version N to N+1
        migrations = [
            # Migration from version 0 to 1 (initial setup)
            (1, "Initial schema setup", [
                # No explicit commands here since we create tables separately
            ]),
            
            # Add more migrations as needed
            # (2, "Added new fields", [...]),
            # (3, "Added new indexes", [...]),
        ]
        
        try:
            for version, description, commands in migrations:
                if version > current_version:
                    logger.info(f"Applying migration to version {version}: {description}")
                    
                    with self.timescale_conn.cursor() as cursor:
                        for command in commands:
                            cursor.execute(command)
                        
                        # Update schema version
                        cursor.execute("""
                            INSERT INTO schema_version (version, description)
                            VALUES (%s, %s)
                        """, (version, description))
                    
                    self.timescale_conn.commit()
                    logger.info(f"Migration to version {version} completed")
            
        except Exception as e:
            logger.error(f"Error applying migrations: {e}")
            self.timescale_conn.rollback()
    
    async def _setup_timescaledb_table(self, measurement: str) -> bool:
        """
        Set up a TimescaleDB table for a specific measurement.
        
        Args:
            measurement: Name of the measurement
        
        Returns:
            Success status
        """
        logger.info(f"Setting up TimescaleDB table for measurement: {measurement}")
        
        # Define schema based on measurement type
        schema_definitions = {
            "price_candles": """
                CREATE TABLE IF NOT EXISTS price_candles (
                    time TIMESTAMPTZ NOT NULL,
                    tag_symbol TEXT NOT NULL,
                    tag_exchange TEXT NOT NULL,
                    tag_timeframe TEXT NOT NULL,
                    field_open DOUBLE PRECISION NOT NULL,
                    field_high DOUBLE PRECISION NOT NULL,
                    field_low DOUBLE PRECISION NOT NULL,
                    field_close DOUBLE PRECISION NOT NULL,
                    field_volume DOUBLE PRECISION NOT NULL
                )
            """,
            
            "trades": """
                CREATE TABLE IF NOT EXISTS trades (
                    time TIMESTAMPTZ NOT NULL,
                    tag_symbol TEXT NOT NULL,
                    tag_exchange TEXT NOT NULL,
                    tag_side TEXT NOT NULL,
                    field_price DOUBLE PRECISION NOT NULL,
                    field_amount DOUBLE PRECISION NOT NULL,
                    field_trade_id TEXT
                )
            """,
            
            "order_book": """
                CREATE TABLE IF NOT EXISTS order_book (
                    time TIMESTAMPTZ NOT NULL,
                    tag_symbol TEXT NOT NULL,
                    tag_exchange TEXT NOT NULL,
                    tag_level TEXT NOT NULL,
                    tag_side TEXT NOT NULL,
                    field_price DOUBLE PRECISION NOT NULL,
                    field_amount DOUBLE PRECISION NOT NULL
                )
            """,
            
            "ticker": """
                CREATE TABLE IF NOT EXISTS ticker (
                    time TIMESTAMPTZ NOT NULL,
                    tag_symbol TEXT NOT NULL,
                    tag_exchange TEXT NOT NULL,
                    field_bid DOUBLE PRECISION,
                    field_ask DOUBLE PRECISION,
                    field_last DOUBLE PRECISION,
                    field_volume DOUBLE PRECISION
                )
            """,
            
            "indicators": """
                CREATE TABLE IF NOT EXISTS indicators (
                    time TIMESTAMPTZ NOT NULL,
                    tag_symbol TEXT NOT NULL,
                    tag_indicator TEXT NOT NULL,
                    tag_timeframe TEXT NOT NULL,
                    tag_params TEXT,
                    field_value DOUBLE PRECISION NOT NULL,
                    field_signal TEXT
                )
            """,
            
            "signals": """
                CREATE TABLE IF NOT EXISTS signals (
                    time TIMESTAMPTZ NOT NULL,
                    tag_symbol TEXT NOT NULL,
                    tag_strategy TEXT NOT NULL,
                    tag_timeframe TEXT NOT NULL,
                    tag_signal_type TEXT NOT NULL,
                    field_price DOUBLE PRECISION,
                    field_strength DOUBLE PRECISION,
                    field_direction TEXT,
                    field_metadata JSONB
                )
            """,
            
            "system_metrics": """
                CREATE TABLE IF NOT EXISTS system_metrics (
                    time TIMESTAMPTZ NOT NULL,
                    tag_component TEXT NOT NULL,
                    tag_metric_type TEXT NOT NULL,
                    tag_instance TEXT,
                    field_value DOUBLE PRECISION NOT NULL,
                    field_unit TEXT,
                    field_metadata JSONB
                )
            """
        }
        
        # Default schema for any measurement not specifically defined
        default_schema = f"""
            CREATE TABLE IF NOT EXISTS {measurement} (
                time TIMESTAMPTZ NOT NULL
            )
        """
        
        try:
            schema_sql = schema_definitions.get(measurement, default_schema)
            
            with self.timescale_conn.cursor() as cursor:
                # Create the table
                cursor.execute(schema_sql)
                
                # Convert to hypertable
                cursor.execute(f"""
                    SELECT create_hypertable('{measurement}', 'time', 
                        if_not_exists => TRUE, 
                        create_default_indexes => TRUE)
                """)
                
                # Create additional indexes based on measurement type
                await self._create_indexes_for_measurement(cursor, measurement)
                
                # Set up retention policy
                retention_days = self.config.get("timescale_retention_days", 30)
                cursor.execute(f"""
                    SELECT add_retention_policy('{measurement}', 
                        INTERVAL '{retention_days} days',
                        if_not_exists => TRUE)
                """)
                
                self.timescale_conn.commit()
                
            logger.info(f"TimescaleDB table for {measurement} created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error creating TimescaleDB table for {measurement}: {e}")
            self.timescale_conn.rollback()
            return False
    
    async def _create_indexes_for_measurement(self, cursor, measurement: str):
        """
        Create indexes for a measurement based on its type.
        
        Args:
            cursor: Database cursor
            measurement: Measurement name
        """
        # Common indexes for all tables
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{measurement}_time ON {measurement} (time DESC)")
        
        # Specific indexes based on measurement type
        if measurement == "price_candles":
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{measurement}_symbol ON {measurement} (tag_symbol, tag_timeframe)")
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{measurement}_exchange ON {measurement} (tag_exchange)")
            
        elif measurement == "trades":
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{measurement}_symbol ON {measurement} (tag_symbol)")
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{measurement}_side ON {measurement} (tag_side)")
            
        elif measurement == "order_book":
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{measurement}_symbol ON {measurement} (tag_symbol, tag_level, tag_side)")
            
        elif measurement == "signals":
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{measurement}_symbol ON {measurement} (tag_symbol, tag_strategy)")
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{measurement}_signal_type ON {measurement} (tag_signal_type)")
            
        elif measurement == "system_metrics":
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{measurement}_component ON {measurement} (tag_component, tag_metric_type)")
    
    async def _setup_timescaledb_continuous_aggregates(self) -> bool:
        """
        Set up TimescaleDB continuous aggregates for downsampling.
        
        Returns:
            Success status
        """
        logger.info("Setting up TimescaleDB continuous aggregates")
        
        # Define continuous aggregates for different measurements
        continuous_aggregates = [
            # Price candles aggregated to 1-hour intervals
            {
                "name": "price_candles_1h",
                "source_table": "price_candles",
                "time_interval": "1 hour",
                "refresh_interval": "1 hour",
                "query": """
                    SELECT
                        time_bucket('1 hour', time) AS bucket,
                        tag_symbol,
                        tag_exchange,
                        'hour' AS tag_timeframe,
                        first(field_open, time) AS field_open,
                        max(field_high) AS field_high,
                        min(field_low) AS field_low,
                        last(field_close, time) AS field_close,
                        sum(field_volume) AS field_volume
                    FROM price_candles
                    GROUP BY bucket, tag_symbol, tag_exchange
                """
            },
            
            # Price candles aggregated to 1-day intervals
            {
                "name": "price_candles_1d",
                "source_table": "price_candles",
                "time_interval": "1 day",
                "refresh_interval": "6 hours",
                "query": """
                    SELECT
                        time_bucket('1 day', time) AS bucket,
                        tag_symbol,
                        tag_exchange,
                        'day' AS tag_timeframe,
                        first(field_open, time) AS field_open,
                        max(field_high) AS field_high,
                        min(field_low) AS field_low,
                        last(field_close, time) AS field_close,
                        sum(field_volume) AS field_volume
                    FROM price_candles
                    GROUP BY bucket, tag_symbol, tag_exchange
                """
            },
            
            # System metrics aggregated hourly
            {
                "name": "system_metrics_1h",
                "source_table": "system_metrics",
                "time_interval": "1 hour",
                "refresh_interval": "15 minutes",
                "query": """
                    SELECT
                        time_bucket('1 hour', time) AS bucket,
                        tag_component,
                        tag_metric_type,
                        tag_instance,
                        avg(field_value) AS field_value,
                        min(field_value) AS field_min,
                        max(field_value) AS field_max,
                        count(*) AS field_count,
                        first(field_unit, time) AS field_unit
                    FROM system_metrics
                    GROUP BY bucket, tag_component, tag_metric_type, tag_instance
                """
            }
        ]
        
        try:
            with self.timescale_conn.cursor() as cursor:
                for agg in continuous_aggregates:
                    # Check if continuous aggregate view exists
                    cursor.execute(f"""
                        SELECT * FROM pg_catalog.pg_matviews
                        WHERE matviewname = '{agg["name"]}'
                    """)
                    
                    if cursor.fetchone():
                        logger.info(f"Continuous aggregate '{agg['name']}' already exists")
                        continue
                    
                    # Create continuous aggregate view
                    logger.info(f"Creating continuous aggregate '{agg['name']}'")
                    
                    cursor.execute(f"""
                        CREATE MATERIALIZED VIEW {agg["name"]}
                        WITH (timescaledb.continuous) AS
                        {agg["query"]}
                    """)
                    
                    # Set refresh policy
                    cursor.execute(f"""
                        SELECT add_continuous_aggregate_policy('{agg["name"]}',
                            start_offset => INTERVAL '3 days',
                            end_offset => INTERVAL '1 hour',
                            schedule_interval => INTERVAL '{agg["refresh_interval"]}')
                    """)
                    
                    # Create indexes on the view
                    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{agg['name']}_bucket ON {agg['name']} (bucket DESC)")
                    
                    if "tag_symbol" in agg["query"]:
                        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{agg['name']}_symbol ON {agg['name']} (tag_symbol)")
                    
                    if "tag_component" in agg["query"]:
                        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{agg['name']}_component ON {agg['name']} (tag_component)")
                
                self.timescale_conn.commit()
                logger.info("TimescaleDB continuous aggregates setup completed successfully")
                return True
                
        except Exception as e:
            logger.error(f"Error setting up TimescaleDB continuous aggregates: {e}")
            self.timescale_conn.rollback()
            return False


async def initialize_database_schemas(config: Dict[str, Any]) -> bool:
    """
    Initialize all time-series database schemas.
    
    Args:
        config: Configuration dictionary with database connection details
        
    Returns:
        Success status
    """
    schema_manager = TimeSeriesSchemaManager(config)
    
    try:
        success = await schema_manager.setup_schemas()
        return success
    finally:
        schema_manager.close()


if __name__ == "__main__":
    """
    Standalone script for initializing database schemas.
    
    Usage:
        python -m trading_system.market_data.timeseries_schema [config_file]
    """
    import json
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Load configuration from file or environment
    config_file = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("CONFIG_FILE")
    
    if config_file:
        with open(config_file, 'r') as f:
            config = json.load(f)
    else:
        # Default configuration
        config = {
            "use_influxdb": True,
            "use_timescaledb": False,
            "influxdb_url": "http://localhost:8086",
            "influxdb_token": os.environ.get("INFLUXDB_TOKEN", ""),
            "influxdb_org": os.environ.get("INFLUXDB_ORG", "trading_system"),
            "influxdb_bucket": "market_data",
            "default_retention_days": 30,
            "long_term_retention_days": 365,
            "setup_downsampling": True
        }
    
    # Run schema initialization
    asyncio.run(initialize_database_schemas(config)) 