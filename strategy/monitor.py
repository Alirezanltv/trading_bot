"""
Strategy Monitor Module

This module provides a performance monitoring system for trading strategies.
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional, Callable, List, Tuple

from trading_system.core.logging import get_logger

logger = get_logger("strategy.monitor")

class StrategyMonitor:
    """
    Monitors strategy performance and triggers alerts when metrics cross thresholds.
    """
    
    def __init__(self, strategy_name: str, storage_path: str = None):
        """
        Initialize the strategy monitor.
        
        Args:
            strategy_name: Name of the strategy to monitor
            storage_path: Path to store monitor data
        """
        self.strategy_name = strategy_name
        self.storage_path = storage_path
        
        # Performance metrics
        self.metrics: Dict[str, Dict[str, Any]] = {}
        
        # Alert handler
        self.on_alert: Optional[Callable[[str, float, float, bool], None]] = None
        
        # Make storage directory if needed
        if self.storage_path:
            os.makedirs(self.storage_path, exist_ok=True)
    
    def add_metric(self, 
                 name: str, 
                 initial_value: float = 0.0, 
                 threshold_min: Optional[float] = None,
                 threshold_max: Optional[float] = None,
                 alert_on_threshold: bool = False) -> None:
        """
        Add a metric to monitor.
        
        Args:
            name: Metric name
            initial_value: Initial metric value
            threshold_min: Minimum value threshold
            threshold_max: Maximum value threshold
            alert_on_threshold: Whether to trigger alert when threshold crossed
        """
        self.metrics[name] = {
            "current": initial_value,
            "history": [(datetime.now().timestamp(), initial_value)],
            "threshold_min": threshold_min,
            "threshold_max": threshold_max,
            "alert_on_threshold": alert_on_threshold,
            "last_alert_time": None
        }
        
        logger.info(f"Added metric '{name}' for {self.strategy_name} with initial value {initial_value}")
    
    def update_metric(self, name: str, value: float) -> None:
        """
        Update a metric value.
        
        Args:
            name: Metric name
            value: New metric value
        """
        if name not in self.metrics:
            self.add_metric(name, value)
            return
        
        # Update value and history
        self.metrics[name]["current"] = value
        self.metrics[name]["history"].append((datetime.now().timestamp(), value))
        
        # Limit history length
        if len(self.metrics[name]["history"]) > 1000:
            self.metrics[name]["history"] = self.metrics[name]["history"][-1000:]
        
        # Check thresholds
        self._check_thresholds(name, value)
    
    def _check_thresholds(self, name: str, value: float) -> None:
        """
        Check if metric value crosses any thresholds.
        
        Args:
            name: Metric name
            value: Current metric value
        """
        metric = self.metrics[name]
        
        # Skip if alerts not enabled
        if not metric["alert_on_threshold"]:
            return
        
        # Check minimum threshold
        if metric["threshold_min"] is not None and value < metric["threshold_min"]:
            logger.warning(f"Metric '{name}' below threshold: {value} < {metric['threshold_min']}")
            
            if self.on_alert:
                self.on_alert(name, value, metric["threshold_min"], True)
                
            metric["last_alert_time"] = datetime.now().timestamp()
        
        # Check maximum threshold
        if metric["threshold_max"] is not None and value > metric["threshold_max"]:
            logger.warning(f"Metric '{name}' above threshold: {value} > {metric['threshold_max']}")
            
            if self.on_alert:
                self.on_alert(name, value, metric["threshold_max"], False)
                
            metric["last_alert_time"] = datetime.now().timestamp()
    
    def get_metric(self, name: str) -> Optional[float]:
        """
        Get current value of a metric.
        
        Args:
            name: Metric name
            
        Returns:
            Current metric value or None if not found
        """
        return self.metrics.get(name, {}).get("current")
    
    def get_metrics_as_dict(self) -> Dict[str, Any]:
        """
        Get all metrics as a dictionary.
        
        Returns:
            Dictionary of all metrics
        """
        return {
            name: {
                "current": metric["current"],
                "threshold_min": metric["threshold_min"],
                "threshold_max": metric["threshold_max"],
                "last_updated": metric["history"][-1][0] if metric["history"] else None,
                "history_points": len(metric["history"]) 
            }
            for name, metric in self.metrics.items()
        }
    
    def save_metrics(self, filename: Optional[str] = None) -> bool:
        """
        Save metrics to a file.
        
        Args:
            filename: Custom filename to save to
            
        Returns:
            Save success
        """
        if not self.storage_path:
            return False
            
        try:
            if not filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = os.path.join(self.storage_path, f"{self.strategy_name}_metrics_{timestamp}.json")
            elif not os.path.isabs(filename):
                filename = os.path.join(self.storage_path, filename)
                
            # Convert data for serialization
            data = {
                "strategy_name": self.strategy_name,
                "timestamp": datetime.now().isoformat(),
                "metrics": {
                    name: {
                        "current": metric["current"],
                        "threshold_min": metric["threshold_min"],
                        "threshold_max": metric["threshold_max"],
                        "history": [(ts, val) for ts, val in metric["history"]]
                    }
                    for name, metric in self.metrics.items()
                }
            }
            
            # Save to file
            with open(filename, 'w') as f:
                json.dump(data, f, indent=2)
                
            logger.info(f"Saved metrics for {self.strategy_name} to {filename}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving metrics: {e}")
            return False
    
    def load_metrics(self, filename: str) -> bool:
        """
        Load metrics from a file.
        
        Args:
            filename: Filename to load from
            
        Returns:
            Load success
        """
        try:
            if not os.path.isabs(filename) and self.storage_path:
                filename = os.path.join(self.storage_path, filename)
                
            if not os.path.exists(filename):
                return False
                
            # Load data
            with open(filename, 'r') as f:
                data = json.load(f)
                
            # Update metrics
            for name, metric_data in data.get("metrics", {}).items():
                if name not in self.metrics:
                    self.add_metric(
                        name, 
                        initial_value=metric_data.get("current", 0.0),
                        threshold_min=metric_data.get("threshold_min"),
                        threshold_max=metric_data.get("threshold_max")
                    )
                
                self.metrics[name]["current"] = metric_data.get("current", 0.0)
                self.metrics[name]["history"] = [(ts, val) for ts, val in metric_data.get("history", [])]
                
            logger.info(f"Loaded metrics for {self.strategy_name} from {filename}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading metrics: {e}")
            return False 