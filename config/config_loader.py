"""
Configuration Loader for Trading System

This module loads and validates configuration files for the trading system.
It supports JSON and YAML formats with environment variable interpolation.
"""

import os
import re
import json
import yaml
import logging
from pathlib import Path
from typing import Dict, Any, Optional, Union

# Use dotenv for environment variable loading
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger("config.loader")

def _interpolate_env_vars(config_str: str) -> str:
    """
    Replace environment variable placeholders in the configuration string.
    
    Args:
        config_str: Configuration string with placeholders like ${VAR_NAME}
        
    Returns:
        String with environment variables replaced
    """
    pattern = r'\${([^}]+)}'
    
    def replace_env_var(match):
        env_var = match.group(1)
        value = os.environ.get(env_var)
        if value is None:
            logger.warning(f"Environment variable '{env_var}' not found")
            return f"${{{env_var}}}"  # Keep the placeholder if not found
        return value
    
    return re.sub(pattern, replace_env_var, config_str)

def load_config(config_path: Union[str, Path]) -> Dict[str, Any]:
    """
    Load configuration from a file with environment variable interpolation.
    
    Args:
        config_path: Path to the configuration file (JSON or YAML)
        
    Returns:
        Configuration dictionary
    
    Raises:
        FileNotFoundError: If the configuration file doesn't exist
        ValueError: If the file format is not supported or the content is invalid
    """
    config_path = Path(config_path)
    
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    try:
        # Read the file content
        with open(config_path, 'r', encoding='utf-8') as f:
            config_str = f.read()
        
        # Interpolate environment variables
        config_str = _interpolate_env_vars(config_str)
        
        # Parse based on file extension
        file_ext = config_path.suffix.lower()
        if file_ext == '.json':
            config = json.loads(config_str)
        elif file_ext in ('.yaml', '.yml'):
            config = yaml.safe_load(config_str)
        else:
            raise ValueError(f"Unsupported configuration file format: {file_ext}")
        
        logger.info(f"Loaded configuration from {config_path}")
        return config
        
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in configuration file: {e}")
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML in configuration file: {e}")
    except Exception as e:
        raise ValueError(f"Error loading configuration: {e}")

def get_config_value(config: Dict[str, Any], path: str, default: Any = None) -> Any:
    """
    Get a value from a nested configuration dictionary using dot notation.
    
    Args:
        config: Configuration dictionary
        path: Path to the value using dot notation (e.g., "database.host")
        default: Default value to return if the path is not found
        
    Returns:
        The value at the specified path or the default value
    """
    keys = path.split('.')
    result = config
    
    for key in keys:
        if isinstance(result, dict) and key in result:
            result = result[key]
        else:
            return default
    
    return result

def validate_required_configs(config: Dict[str, Any], required_paths: list) -> bool:
    """
    Validate that required configuration values are present.
    
    Args:
        config: Configuration dictionary
        required_paths: List of required configuration paths using dot notation
        
    Returns:
        True if all required configurations are present, False otherwise
    """
    missing = []
    
    for path in required_paths:
        value = get_config_value(config, path)
        if value is None:
            missing.append(path)
    
    if missing:
        logger.error(f"Missing required configuration values: {', '.join(missing)}")
        return False
    
    return True

def save_config(config: Dict[str, Any], config_path: Union[str, Path]) -> bool:
    """
    Save configuration to a file.
    
    Args:
        config: Configuration dictionary
        config_path: Path to save the configuration file
        
    Returns:
        True if the configuration was saved successfully, False otherwise
    """
    config_path = Path(config_path)
    
    try:
        # Create parent directories if they don't exist
        config_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write based on file extension
        file_ext = config_path.suffix.lower()
        if file_ext == '.json':
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2)
        elif file_ext in ('.yaml', '.yml'):
            with open(config_path, 'w', encoding='utf-8') as f:
                yaml.dump(config, f, default_flow_style=False)
        else:
            raise ValueError(f"Unsupported configuration file format: {file_ext}")
        
        logger.info(f"Saved configuration to {config_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error saving configuration: {e}")
        return False 