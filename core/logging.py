"""
Logging Module

This module provides logging functionality for the trading system.
"""

import os
import sys
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from typing import Dict, Optional, Union, Any


# Default log directory
DEFAULT_LOG_DIR = "logs"

# Default log format
DEFAULT_LOG_FORMAT = "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"

# Global logger registry
_loggers: Dict[str, logging.Logger] = {}

# Configuration
_config = {
    "initialized": False,
    "log_level": logging.INFO,
    "console_logging": True,
    "file_logging": True,
    "log_dir": DEFAULT_LOG_DIR,
    "log_format": DEFAULT_LOG_FORMAT,
    "max_file_size_mb": 10,
    "backup_count": 5
}


def initialize_logging(
    log_level: Union[int, str] = logging.INFO,
    console_logging: bool = True,
    file_logging: bool = True,
    log_dir: str = DEFAULT_LOG_DIR,
    log_format: str = DEFAULT_LOG_FORMAT,
    max_file_size_mb: int = 10,
    backup_count: int = 5
) -> None:
    """
    Initialize logging system.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        console_logging: Enable console logging
        file_logging: Enable file logging
        log_dir: Log directory
        log_format: Log format
        max_file_size_mb: Maximum log file size in MB
        backup_count: Number of backup log files
    """
    # Convert string log level to int if needed
    if isinstance(log_level, str):
        log_level = getattr(logging, log_level.upper())
    
    # Update configuration
    _config["log_level"] = log_level
    _config["console_logging"] = console_logging
    _config["file_logging"] = file_logging
    _config["log_dir"] = log_dir
    _config["log_format"] = log_format
    _config["max_file_size_mb"] = max_file_size_mb
    _config["backup_count"] = backup_count
    
    # Create log directory if it doesn't exist
    if file_logging and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Remove existing handlers
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)
    
    # Create formatter
    formatter = logging.Formatter(log_format)
    
    # Add console handler
    if console_logging:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
    
    # Add file handler
    if file_logging:
        current_date = datetime.now().strftime("%Y-%m-%d")
        log_file = os.path.join(log_dir, f"trading_{current_date}.log")
        
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=max_file_size_mb * 1024 * 1024,
            backupCount=backup_count
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    # Mark as initialized
    _config["initialized"] = True
    
    # Log initialization
    root_logger.info(f"Logging initialized (level={logging.getLevelName(log_level)})")


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance.
    
    Args:
        name: Logger name
        
    Returns:
        Logger instance
    """
    # Initialize logging if not already initialized
    if not _config["initialized"]:
        initialize_logging()
    
    # Return cached logger if available
    if name in _loggers:
        return _loggers[name]
    
    # Create and cache logger
    logger = logging.getLogger(name)
    logger.setLevel(_config["log_level"])
    _loggers[name] = logger
    
    return logger


def set_log_level(level: Union[int, str], logger_name: Optional[str] = None) -> None:
    """
    Set log level for a specific logger or all loggers.
    
    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        logger_name: Logger name or None for all loggers
    """
    # Convert string log level to int if needed
    if isinstance(level, str):
        level = getattr(logging, level.upper())
    
    if logger_name is None:
        # Update configuration
        _config["log_level"] = level
        
        # Update root logger
        logging.getLogger().setLevel(level)
        
        # Update all existing loggers
        for logger in _loggers.values():
            logger.setLevel(level)
    else:
        # Update specific logger
        logger = get_logger(logger_name)
        logger.setLevel(level)


def get_log_config() -> Dict[str, Any]:
    """
    Get current logging configuration.
    
    Returns:
        Logging configuration
    """
    return {
        "log_level": logging.getLevelName(_config["log_level"]),
        "console_logging": _config["console_logging"],
        "file_logging": _config["file_logging"],
        "log_dir": _config["log_dir"],
        "log_format": _config["log_format"],
        "max_file_size_mb": _config["max_file_size_mb"],
        "backup_count": _config["backup_count"],
        "initialized": _config["initialized"]
    } 