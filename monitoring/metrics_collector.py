"""
Metrics Collector System

This module provides functionality for collecting, storing, and analyzing 
metrics from various components of the trading system.
"""

import time
import asyncio
import statistics
from enum import Enum
from typing import Dict, List, Optional, Any, Union, Callable, Set
from datetime import datetime, timedelta
import json
import logging
import os
import threading
from collections import defaultdict, deque

from trading_system.core.component import Component, ComponentStatus
from trading_system.core.logging import get_logger

logger = get_logger("monitoring.metrics_collector")


class MetricType(Enum):
    """Types of metrics."""
    COUNTER = "counter"          # Monotonically increasing counter
    GAUGE = "gauge"              # Value that can go up and down
    HISTOGRAM = "histogram"      # Distribution of values
    TIMER = "timer"              # Duration measurements
    EVENT = "event"              # Discrete events


class MetricLabel(Enum):
    """Common metric labels."""
    COMPONENT = "component"      # Component name
    OPERATION = "operation"      # Operation name
    STATUS = "status"            # Operation status
    INSTANCE = "instance"        # Instance identifier
    SOURCE = "source"            # Data source
    TARGET = "target"            # Data target
    VERSION = "version"          # Component version
    EXCHANGE = "exchange"        # Exchange name
    ASSET = "asset"              # Asset name
    STRATEGY = "strategy"        # Strategy name
    PRIORITY = "priority"        # Priority level


class MetricValue:
    """Value container for a metric with metadata."""
    
    def __init__(self, 
                 value: Union[int, float, str, bool],
                 metric_type: MetricType,
                 timestamp: Optional[float] = None,
                 labels: Optional[Dict[str, str]] = None):
        """
        Initialize metric value.
        
        Args:
            value: The metric value
            metric_type: Type of metric
            timestamp: Timestamp of the metric (defaults to current time)
            labels: Labels associated with this value
        """
        self.value = value
        self.metric_type = metric_type
        self.timestamp = timestamp or time.time()
        self.labels = labels or {}
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary.
        
        Returns:
            Dict representation
        """
        return {
            "value": self.value,
            "type": self.metric_type.value,
            "timestamp": self.timestamp,
            "formatted_time": datetime.fromtimestamp(self.timestamp).isoformat(),
            "labels": self.labels
        }


class Metric:
    """Container for a metric with its values history."""
    
    def __init__(self, 
                 name: str, 
                 description: str,
                 metric_type: MetricType,
                 unit: str = "",
                 max_history: int = 1000):
        """
        Initialize metric.
        
        Args:
            name: Metric name
            description: Metric description
            metric_type: Type of metric
            unit: Unit of measurement
            max_history: Maximum number of values to keep
        """
        self.name = name
        self.description = description
        self.metric_type = metric_type
        self.unit = unit
        self.max_history = max_history
        
        # Values history
        self.values: List[MetricValue] = []
        
        # Current value (for gauges and counters)
        self.current_value: Optional[Union[int, float, str, bool]] = None
        
        # Statistics (for histograms)
        self.sum = 0.0
        self.min = None
        self.max = None
        self.count = 0
        
        # Lock for thread safety
        self._lock = threading.RLock()
    
    def add_value(self, 
                  value: Union[int, float, str, bool], 
                  timestamp: Optional[float] = None,
                  labels: Optional[Dict[str, str]] = None) -> None:
        """
        Add a value to the metric.
        
        Args:
            value: Value to add
            timestamp: Timestamp (defaults to current time)
            labels: Labels associated with this value
        """
        with self._lock:
            # Create metric value
            metric_value = MetricValue(
                value=value,
                metric_type=self.metric_type,
                timestamp=timestamp,
                labels=labels
            )
            
            # Add to history
            self.values.append(metric_value)
            
            # Trim history if needed
            if len(self.values) > self.max_history:
                self.values = self.values[-self.max_history:]
            
            # Update current value
            self.current_value = value
            
            # Update statistics for numeric values
            if isinstance(value, (int, float)):
                self.count += 1
                self.sum += value
                
                if self.min is None or value < self.min:
                    self.min = value
                
                if self.max is None or value > self.max:
                    self.max = value
    
    def get_values(self, 
                   start_time: Optional[float] = None,
                   end_time: Optional[float] = None,
                   label_filter: Optional[Dict[str, str]] = None) -> List[MetricValue]:
        """
        Get values within time range and matching labels.
        
        Args:
            start_time: Start timestamp (None for no limit)
            end_time: End timestamp (None for no limit)
            label_filter: Filter by labels
            
        Returns:
            List of matching metric values
        """
        with self._lock:
            filtered_values = []
            
            for value in self.values:
                # Time range filter
                if start_time is not None and value.timestamp < start_time:
                    continue
                
                if end_time is not None and value.timestamp > end_time:
                    continue
                
                # Label filter
                if label_filter:
                    match = True
                    for k, v in label_filter.items():
                        if k not in value.labels or value.labels[k] != v:
                            match = False
                            break
                    
                    if not match:
                        continue
                
                filtered_values.append(value)
            
            return filtered_values
    
    def get_statistics(self, 
                       start_time: Optional[float] = None,
                       end_time: Optional[float] = None,
                       label_filter: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Get statistics for values within time range and matching labels.
        
        Args:
            start_time: Start timestamp (None for no limit)
            end_time: End timestamp (None for no limit)
            label_filter: Filter by labels
            
        Returns:
            Dictionary of statistics
        """
        values = self.get_values(start_time, end_time, label_filter)
        
        # Extract numeric values
        numeric_values = [v.value for v in values if isinstance(v.value, (int, float))]
        
        result = {
            "count": len(numeric_values),
            "sum": sum(numeric_values) if numeric_values else 0,
            "min": min(numeric_values) if numeric_values else None,
            "max": max(numeric_values) if numeric_values else None,
            "avg": sum(numeric_values) / len(numeric_values) if numeric_values else None
        }
        
        # Add more statistics if enough values
        if len(numeric_values) >= 2:
            result["median"] = statistics.median(numeric_values)
            result["stddev"] = statistics.stdev(numeric_values)
            
            # Percentiles
            numeric_values.sort()
            result["p50"] = numeric_values[int(len(numeric_values) * 0.5)]
            result["p90"] = numeric_values[int(len(numeric_values) * 0.9)]
            result["p95"] = numeric_values[int(len(numeric_values) * 0.95)]
            result["p99"] = numeric_values[int(len(numeric_values) * 0.99)] if len(numeric_values) >= 100 else None
        
        return result
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary.
        
        Returns:
            Dict representation
        """
        with self._lock:
            return {
                "name": self.name,
                "description": self.description,
                "type": self.metric_type.value,
                "unit": self.unit,
                "current_value": self.current_value,
                "count": self.count,
                "sum": self.sum,
                "min": self.min,
                "max": self.max,
                "avg": self.sum / self.count if self.count > 0 else None
            }


class MetricsCollector(Component):
    """
    Metrics Collector
    
    This component collects, stores, and provides access to metrics
    from various parts of the trading system.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize metrics collector.
        
        Args:
            config: Configuration dictionary
        """
        super().__init__(name="metrics_collector", config=config)
        
        # Configuration
        self.collection_interval = self.config.get("collection_interval", 10.0)
        self.storage_path = self.config.get("storage_path", "data/metrics")
        self.max_history = self.config.get("max_history", 10000)
        self.enable_persistence = self.config.get("enable_persistence", True)
        self.persist_interval = self.config.get("persist_interval", 300.0)  # 5 minutes
        
        # Metrics registry
        self.metrics: Dict[str, Metric] = {}
        
        # Collection tasks
        self.collection_tasks: Dict[str, asyncio.Task] = {}
        
        # Registered collectors
        self.collectors: Dict[str, Callable] = {}
        
        # Persistence task
        self.persistence_task = None
        
        # Running state
        self.running = False
        
        # Initialize status
        self._update_status(ComponentStatus.INITIALIZED)
        logger.info("Metrics collector initialized")
        
        # Register built-in metrics
        self._register_builtin_metrics()
    
    async def start(self) -> bool:
        """
        Start the metrics collector.
        
        Returns:
            bool: True if started successfully, False otherwise
        """
        try:
            if self.running:
                logger.warning("Metrics collector already running")
                return True
            
            # Update status
            self._update_status(ComponentStatus.INITIALIZING)
            
            # Ensure storage directory exists
            if self.enable_persistence:
                os.makedirs(self.storage_path, exist_ok=True)
                
                # Load persisted metrics
                await self._load_metrics()
            
            # Start collection tasks
            self.running = True
            
            # Start persistence task if enabled
            if self.enable_persistence:
                self.persistence_task = asyncio.create_task(self._persistence_loop())
            
            # Update status
            self._update_status(ComponentStatus.OPERATIONAL)
            logger.info("Metrics collector started")
            return True
            
        except Exception as e:
            self._update_status(ComponentStatus.ERROR, f"Error starting: {str(e)}")
            logger.error(f"Error starting metrics collector: {str(e)}", exc_info=True)
            return False
    
    async def stop(self) -> bool:
        """
        Stop the metrics collector.
        
        Returns:
            bool: True if stopped successfully, False otherwise
        """
        try:
            if not self.running:
                return True
            
            # Update status
            self._update_status(ComponentStatus.STOPPING)
            
            # Stop running flag
            self.running = False
            
            # Cancel collection tasks
            for name, task in self.collection_tasks.items():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            
            self.collection_tasks.clear()
            
            # Cancel persistence task
            if self.persistence_task:
                self.persistence_task.cancel()
                try:
                    await self.persistence_task
                except asyncio.CancelledError:
                    pass
            
            # Persist metrics before stopping
            if self.enable_persistence:
                await self._persist_metrics()
            
            # Update status
            self._update_status(ComponentStatus.STOPPED)
            logger.info("Metrics collector stopped")
            return True
            
        except Exception as e:
            self._update_status(ComponentStatus.ERROR, f"Error stopping: {str(e)}")
            logger.error(f"Error stopping metrics collector: {str(e)}", exc_info=True)
            return False
    
    def register_metric(self, 
                        name: str, 
                        description: str,
                        metric_type: MetricType,
                        unit: str = "",
                        max_history: Optional[int] = None) -> Metric:
        """
        Register a new metric.
        
        Args:
            name: Metric name
            description: Metric description
            metric_type: Type of metric
            unit: Unit of measurement
            max_history: Maximum number of values to keep (defaults to global setting)
            
        Returns:
            Registered metric
        """
        if name in self.metrics:
            logger.warning(f"Metric {name} already registered, returning existing instance")
            return self.metrics[name]
        
        # Create metric
        metric = Metric(
            name=name,
            description=description,
            metric_type=metric_type,
            unit=unit,
            max_history=max_history or self.max_history
        )
        
        # Register metric
        self.metrics[name] = metric
        logger.info(f"Registered metric: {name} ({metric_type.value})")
        
        return metric
    
    def get_metric(self, name: str) -> Optional[Metric]:
        """
        Get a metric by name.
        
        Args:
            name: Metric name
            
        Returns:
            Metric or None if not found
        """
        return self.metrics.get(name)
    
    def update_metric(self, 
                      name: str, 
                      value: Union[int, float, str, bool],
                      labels: Optional[Dict[str, str]] = None,
                      timestamp: Optional[float] = None) -> None:
        """
        Update a metric value.
        
        Args:
            name: Metric name
            value: Metric value
            labels: Labels associated with this value
            timestamp: Timestamp (defaults to current time)
        """
        metric = self.get_metric(name)
        if not metric:
            logger.warning(f"Attempted to update unknown metric: {name}")
            return
        
        metric.add_value(value, timestamp, labels)
    
    def increment_counter(self, 
                          name: str, 
                          amount: Union[int, float] = 1,
                          labels: Optional[Dict[str, str]] = None) -> None:
        """
        Increment a counter metric.
        
        Args:
            name: Metric name
            amount: Amount to increment by
            labels: Labels associated with this value
        """
        metric = self.get_metric(name)
        if not metric:
            logger.warning(f"Attempted to increment unknown counter: {name}")
            return
        
        if metric.metric_type != MetricType.COUNTER:
            logger.warning(f"Metric {name} is not a counter (type: {metric.metric_type.value})")
            return
        
        # Get current value or start from 0
        current = metric.current_value or 0
        
        # Increment and update
        new_value = current + amount
        metric.add_value(new_value, labels=labels)
    
    def time_operation(self, 
                       name: str, 
                       labels: Optional[Dict[str, str]] = None) -> Callable:
        """
        Create a context manager for timing operations.
        
        Args:
            name: Timer metric name
            labels: Labels associated with this value
            
        Returns:
            Context manager for timing operations
        """
        metric = self.get_metric(name)
        if not metric:
            logger.warning(f"Attempted to time operation with unknown metric: {name}")
            
            class DummyTimer:
                def __enter__(self):
                    return self
                
                def __exit__(self, exc_type, exc_val, exc_tb):
                    pass
            
            return DummyTimer()
        
        if metric.metric_type != MetricType.TIMER:
            logger.warning(f"Metric {name} is not a timer (type: {metric.metric_type.value})")
            
            class DummyTimer:
                def __enter__(self):
                    return self
                
                def __exit__(self, exc_type, exc_val, exc_tb):
                    pass
            
            return DummyTimer()
        
        class Timer:
            def __init__(self, metric, labels):
                self.metric = metric
                self.labels = labels
                self.start_time = None
            
            def __enter__(self):
                self.start_time = time.time()
                return self
            
            def __exit__(self, exc_type, exc_val, exc_tb):
                if self.start_time is not None:
                    duration = time.time() - self.start_time
                    self.metric.add_value(duration, labels=self.labels)
        
        return Timer(metric, labels)
    
    def register_collector(self, 
                           name: str, 
                           collector_func: Callable,
                           interval: Optional[float] = None) -> None:
        """
        Register a collector function.
        
        Args:
            name: Collector name
            collector_func: Collector function
            interval: Collection interval in seconds (defaults to global setting)
        """
        if name in self.collectors:
            logger.warning(f"Collector {name} already registered, replacing")
        
        # Register collector
        self.collectors[name] = collector_func
        logger.info(f"Registered metrics collector: {name}")
        
        # Start collection task if running
        if self.running:
            if name in self.collection_tasks:
                self.collection_tasks[name].cancel()
            
            task_interval = interval or self.collection_interval
            self.collection_tasks[name] = asyncio.create_task(
                self._collection_loop(name, collector_func, task_interval)
            )
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """
        Get summary of all metrics.
        
        Returns:
            Dictionary with metrics summary
        """
        return {
            name: metric.to_dict() for name, metric in self.metrics.items()
        }
    
    def get_metrics_by_type(self, metric_type: MetricType) -> Dict[str, Metric]:
        """
        Get metrics by type.
        
        Args:
            metric_type: Metric type
            
        Returns:
            Dictionary of metrics of the requested type
        """
        return {
            name: metric for name, metric in self.metrics.items()
            if metric.metric_type == metric_type
        }
    
    def _register_builtin_metrics(self) -> None:
        """Register built-in metrics."""
        # System metrics
        self.register_metric(
            name="system.cpu.usage",
            description="CPU usage percentage",
            metric_type=MetricType.GAUGE,
            unit="percent"
        )
        
        self.register_metric(
            name="system.memory.usage",
            description="Memory usage percentage",
            metric_type=MetricType.GAUGE,
            unit="percent"
        )
        
        self.register_metric(
            name="system.disk.usage",
            description="Disk usage percentage",
            metric_type=MetricType.GAUGE,
            unit="percent"
        )
        
        # Component metrics
        self.register_metric(
            name="component.status",
            description="Component status",
            metric_type=MetricType.GAUGE
        )
        
        # Operation metrics
        self.register_metric(
            name="operation.latency",
            description="Operation latency",
            metric_type=MetricType.TIMER,
            unit="seconds"
        )
        
        self.register_metric(
            name="operation.count",
            description="Operation count",
            metric_type=MetricType.COUNTER,
            unit="operations"
        )
        
        self.register_metric(
            name="operation.errors",
            description="Operation errors",
            metric_type=MetricType.COUNTER,
            unit="errors"
        )
        
        # Trading metrics
        self.register_metric(
            name="trading.orders.created",
            description="Orders created",
            metric_type=MetricType.COUNTER,
            unit="orders"
        )
        
        self.register_metric(
            name="trading.orders.filled",
            description="Orders filled",
            metric_type=MetricType.COUNTER,
            unit="orders"
        )
        
        self.register_metric(
            name="trading.orders.canceled",
            description="Orders canceled",
            metric_type=MetricType.COUNTER,
            unit="orders"
        )
        
        self.register_metric(
            name="trading.pnl",
            description="Profit and loss",
            metric_type=MetricType.GAUGE,
            unit="currency"
        )
    
    async def _collection_loop(self, name: str, collector_func: Callable, interval: float) -> None:
        """
        Background task to run a collector function periodically.
        
        Args:
            name: Collector name
            collector_func: Collector function
            interval: Collection interval in seconds
        """
        try:
            logger.info(f"Starting metrics collection for {name} (interval: {interval}s)")
            
            while self.running:
                try:
                    # Run collector
                    if asyncio.iscoroutinefunction(collector_func):
                        await collector_func(self)
                    else:
                        collector_func(self)
                    
                except Exception as e:
                    logger.error(f"Error in metrics collector {name}: {str(e)}", exc_info=True)
                
                # Wait before next collection
                await asyncio.sleep(interval)
                
        except asyncio.CancelledError:
            logger.info(f"Metrics collection for {name} cancelled")
            raise
            
        except Exception as e:
            logger.error(f"Error in metrics collection loop for {name}: {str(e)}", exc_info=True)
    
    async def _persistence_loop(self) -> None:
        """Background task to persist metrics periodically."""
        try:
            logger.info(f"Starting metrics persistence (interval: {self.persist_interval}s)")
            
            while self.running:
                try:
                    # Persist metrics
                    await self._persist_metrics()
                    
                except Exception as e:
                    logger.error(f"Error persisting metrics: {str(e)}", exc_info=True)
                
                # Wait before next persistence
                await asyncio.sleep(self.persist_interval)
                
        except asyncio.CancelledError:
            logger.info("Metrics persistence cancelled")
            raise
            
        except Exception as e:
            logger.error(f"Error in metrics persistence loop: {str(e)}", exc_info=True)
    
    async def _persist_metrics(self) -> None:
        """Persist metrics to storage."""
        try:
            # Get timestamp
            timestamp = int(time.time())
            
            # Create metrics data
            metrics_data = {
                "timestamp": timestamp,
                "formatted_time": datetime.fromtimestamp(timestamp).isoformat(),
                "metrics": self.get_metrics_summary()
            }
            
            # Create filename
            filename = f"{self.storage_path}/metrics_{timestamp}.json"
            
            # Write to file
            with open(filename, 'w') as f:
                json.dump(metrics_data, f, indent=2)
            
            logger.info(f"Persisted metrics to {filename}")
            
            # Cleanup old files
            self._cleanup_old_metric_files()
            
        except Exception as e:
            logger.error(f"Error persisting metrics: {str(e)}", exc_info=True)
    
    async def _load_metrics(self) -> None:
        """Load persisted metrics from storage."""
        try:
            # Check if storage directory exists
            if not os.path.exists(self.storage_path):
                logger.info(f"Metrics storage directory {self.storage_path} does not exist, skipping load")
                return
            
            # Find latest metrics file
            files = [f for f in os.listdir(self.storage_path) if f.startswith("metrics_") and f.endswith(".json")]
            if not files:
                logger.info("No persisted metrics found")
                return
            
            # Sort by timestamp (descending)
            files.sort(reverse=True)
            latest_file = os.path.join(self.storage_path, files[0])
            
            # Load metrics
            with open(latest_file, 'r') as f:
                metrics_data = json.load(f)
            
            # Process metrics
            for name, metric_data in metrics_data.get("metrics", {}).items():
                # Skip if metric already exists
                if name in self.metrics:
                    continue
                
                # Create metric
                metric_type = MetricType(metric_data.get("type", "gauge"))
                metric = self.register_metric(
                    name=name,
                    description=metric_data.get("description", ""),
                    metric_type=metric_type,
                    unit=metric_data.get("unit", "")
                )
                
                # Set current value
                current_value = metric_data.get("current_value")
                if current_value is not None:
                    metric.add_value(current_value)
            
            logger.info(f"Loaded persisted metrics from {latest_file}")
            
        except Exception as e:
            logger.error(f"Error loading persisted metrics: {str(e)}", exc_info=True)
    
    def _cleanup_old_metric_files(self) -> None:
        """Clean up old metric files."""
        try:
            # Get retention period from config (default: 7 days)
            retention_days = self.config.get("retention_days", 7)
            retention_seconds = retention_days * 24 * 60 * 60
            
            # Calculate cutoff timestamp
            cutoff_time = time.time() - retention_seconds
            
            # List files
            files = [f for f in os.listdir(self.storage_path) if f.startswith("metrics_") and f.endswith(".json")]
            
            # Check each file
            for filename in files:
                try:
                    # Extract timestamp from filename
                    timestamp_str = filename.replace("metrics_", "").replace(".json", "")
                    timestamp = int(timestamp_str)
                    
                    # Check if older than retention period
                    if timestamp < cutoff_time:
                        file_path = os.path.join(self.storage_path, filename)
                        os.remove(file_path)
                        logger.debug(f"Deleted old metrics file: {filename}")
                        
                except Exception as e:
                    logger.warning(f"Error processing metrics file {filename}: {str(e)}")
            
        except Exception as e:
            logger.error(f"Error cleaning up old metric files: {str(e)}")


async def create_metrics_collector(config: Dict[str, Any]) -> MetricsCollector:
    """
    Create and start a metrics collector.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        MetricsCollector instance
    """
    collector = MetricsCollector(config)
    await collector.start()
    return collector 