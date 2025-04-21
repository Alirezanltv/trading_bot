"""
Configuration Utility

This module provides configuration management functionality for the trading system.
"""

import os
import json
import yaml
import logging
from typing import Dict, Any, Optional, List, Union
from dotenv import load_dotenv

from trading_system.core.logging import get_logger

# Load environment variables
load_dotenv()

logger = get_logger("utils.config")


class Config:
    """Configuration manager for the trading system."""
    
    def __init__(self, config_file: Optional[str] = None, env_prefix: str = "APP_"):
        """
        Initialize configuration manager.
        
        Args:
            config_file: Path to configuration file (JSON or YAML)
            env_prefix: Environment variable prefix
        """
        self.config_file = config_file
        self.env_prefix = env_prefix
        self.config_data: Dict[str, Any] = {}
        
        # Load configuration
        self._load_config()
    
    def _load_config(self) -> None:
        """Load configuration from file and environment variables."""
        # Load from configuration file if specified
        if self.config_file and os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as f:
                    if self.config_file.endswith('.json'):
                        self.config_data = json.load(f)
                    elif self.config_file.endswith(('.yaml', '.yml')):
                        import yaml
                        self.config_data = yaml.safe_load(f)
                    else:
                        logger.warning(f"Unsupported config file format: {self.config_file}")
                
                logger.info(f"Loaded configuration from {self.config_file}")
                
            except Exception as e:
                logger.error(f"Error loading configuration from {self.config_file}: {str(e)}")
        
        # Load environment variables with prefix
        env_vars = {k: v for k, v in os.environ.items() if k.startswith(self.env_prefix)}
        for key, value in env_vars.items():
            # Remove prefix and convert to lowercase
            config_key = key[len(self.env_prefix):].lower()
            
            # Handle nested keys (using double underscore as separator)
            if '__' in config_key:
                parts = config_key.split('__')
                current = self.config_data
                for part in parts[:-1]:
                    if part not in current:
                        current[part] = {}
                    current = current[part]
                current[parts[-1]] = self._parse_env_value(value)
            else:
                self.config_data[config_key] = self._parse_env_value(value)
    
    def _parse_env_value(self, value: str) -> Any:
        """
        Parse environment variable value to appropriate type.
        
        Args:
            value: String value from environment variable
            
        Returns:
            Parsed value
        """
        # Check for boolean values
        if value.lower() in ('true', 'yes', '1'):
            return True
        if value.lower() in ('false', 'no', '0'):
            return False
        
        # Check for numeric values
        try:
            if '.' in value:
                return float(value)
            else:
                return int(value)
        except ValueError:
            pass
        
        # Check for JSON/list values
        if value.startswith('[') and value.endswith(']'):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                pass
        
        if value.startswith('{') and value.endswith('}'):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                pass
        
        # Return as string
        return value
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value.
        
        Args:
            key: Configuration key (dot notation for nested keys)
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        parts = key.split('.')
        current = self.config_data
        
        try:
            for part in parts:
                current = current[part]
            return current
        except (KeyError, TypeError):
            return default
    
    def get_int(self, key: str, default: int = 0) -> int:
        """
        Get integer configuration value.
        
        Args:
            key: Configuration key
            default: Default value if key not found
            
        Returns:
            Integer configuration value
        """
        value = self.get(key, default)
        if isinstance(value, int):
            return value
        try:
            return int(value)
        except (ValueError, TypeError):
            return default
    
    def get_float(self, key: str, default: float = 0.0) -> float:
        """
        Get float configuration value.
        
        Args:
            key: Configuration key
            default: Default value if key not found
            
        Returns:
            Float configuration value
        """
        value = self.get(key, default)
        if isinstance(value, float):
            return value
        try:
            return float(value)
        except (ValueError, TypeError):
            return default
    
    def get_bool(self, key: str, default: bool = False) -> bool:
        """
        Get boolean configuration value.
        
        Args:
            key: Configuration key
            default: Default value if key not found
            
        Returns:
            Boolean configuration value
        """
        value = self.get(key, default)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ('true', 'yes', '1')
        return bool(value)
    
    def get_list(self, key: str, default: Optional[List[Any]] = None) -> List[Any]:
        """
        Get list configuration value.
        
        Args:
            key: Configuration key
            default: Default value if key not found
            
        Returns:
            List configuration value
        """
        if default is None:
            default = []
            
        value = self.get(key, default)
        if isinstance(value, list):
            return value
        if isinstance(value, str) and value.startswith('[') and value.endswith(']'):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                pass
        return default
    
    def get_dict(self, key: str, default: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Get dictionary configuration value.
        
        Args:
            key: Configuration key
            default: Default value if key not found
            
        Returns:
            Dictionary configuration value
        """
        if default is None:
            default = {}
            
        value = self.get(key, default)
        if isinstance(value, dict):
            return value
        if isinstance(value, str) and value.startswith('{') and value.endswith('}'):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                pass
        return default
    
    def set(self, key: str, value: Any) -> None:
        """
        Set configuration value.
        
        Args:
            key: Configuration key (dot notation for nested keys)
            value: Configuration value
        """
        parts = key.split('.')
        current = self.config_data
        
        # Navigate to the right level
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        
        # Set the value
        current[parts[-1]] = value
    
    def save(self, file_path: Optional[str] = None) -> bool:
        """
        Save configuration to file.
        
        Args:
            file_path: Path to save configuration file (defaults to self.config_file)
            
        Returns:
            Success flag
        """
        file_path = file_path or self.config_file
        if not file_path:
            logger.error("No configuration file specified")
            return False
        
        try:
            with open(file_path, 'w') as f:
                if file_path.endswith('.json'):
                    json.dump(self.config_data, f, indent=2)
                elif file_path.endswith(('.yaml', '.yml')):
                    import yaml
                    yaml.dump(self.config_data, f)
                else:
                    logger.error(f"Unsupported config file format: {file_path}")
                    return False
            
            logger.info(f"Saved configuration to {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving configuration to {file_path}: {str(e)}")
            return False
    
    def reload(self) -> None:
        """Reload configuration from file and environment variables."""
        self.config_data = {}
        self._load_config()
        logger.info("Configuration reloaded")


# Global configuration instance
_config_instance = None


def get_config(config_file: Optional[str] = None) -> Config:
    """
    Get global configuration instance.
    
    Args:
        config_file: Path to configuration file
        
    Returns:
        Configuration instance
    """
    global _config_instance
    if _config_instance is None:
        _config_instance = Config(config_file)
    return _config_instance 