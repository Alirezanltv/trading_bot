"""
Configuration Management Module

This module handles configuration management for the trading system.
"""

import os
import json
import logging
from typing import Dict, Any, Optional, List

from trading_system.core.logging import get_logger

logger = get_logger("core.config")

class ConfigManager:
    """
    Configuration Manager
    
    This class handles loading, saving, and accessing configuration settings
    for the trading system.
    """
    
    def __init__(self, config_file: Optional[str] = None):
        """
        Initialize configuration manager.
        
        Args:
            config_file: Path to configuration file (optional)
        """
        self._config: Dict[str, Any] = {}
        self._config_file = config_file
        self._initialized = False
        
    async def initialize(self) -> bool:
        """
        Initialize the configuration manager.
        
        Returns:
            Initialization success
        """
        if self._initialized:
            logger.warning("ConfigManager already initialized")
            return True
            
        # Load from file if provided
        if self._config_file and os.path.exists(self._config_file):
            success = self.load_from_file(self._config_file)
        else:
            # Set default configuration
            self._config = {
                "system": {
                    "initialized": False,
                    "test_mode": False,
                    "log_level": "INFO"
                },
                "exchange": {
                    "default": "simulated"
                },
                "market_data": {
                    "default_provider": "local"
                }
            }
            success = True
            
        self._initialized = success
        if success:
            logger.info("ConfigManager initialized successfully")
        else:
            logger.error("Failed to initialize ConfigManager")
            
        return success
            
    def get_config(self, section: str, default: Any = None) -> Any:
        """
        Get configuration for a section.
        
        Args:
            section: Configuration section
            default: Default value if section does not exist
            
        Returns:
            Configuration for the section
        """
        return self._config.get(section, default)
        
    def set_config(self, section: str, config: Any) -> None:
        """
        Set configuration for a section.
        
        Args:
            section: Configuration section
            config: Configuration value
        """
        self._config[section] = config
        
    def update_config(self, section: str, config: Dict[str, Any]) -> None:
        """
        Update configuration for a section.
        
        Args:
            section: Configuration section
            config: Configuration updates
        """
        if section not in self._config:
            self._config[section] = {}
            
        if isinstance(self._config[section], dict) and isinstance(config, dict):
            self._config[section].update(config)
        else:
            self._config[section] = config
            
    def get_all_config(self) -> Dict[str, Any]:
        """
        Get all configuration.
        
        Returns:
            All configuration
        """
        return dict(self._config)
        
    def load_from_file(self, file_path: str) -> bool:
        """
        Load configuration from file.
        
        Args:
            file_path: Path to configuration file
            
        Returns:
            Load success
        """
        try:
            with open(file_path, 'r') as f:
                config = json.load(f)
                
            self._config = config
            logger.info(f"Loaded configuration from {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading configuration from {file_path}: {str(e)}", exc_info=True)
            return False
            
    def save_to_file(self, file_path: str) -> bool:
        """
        Save configuration to file.
        
        Args:
            file_path: Path to configuration file
            
        Returns:
            Save success
        """
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            with open(file_path, 'w') as f:
                json.dump(self._config, f, indent=4)
                
            logger.info(f"Saved configuration to {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving configuration to {file_path}: {str(e)}", exc_info=True)
            return False
            
    def get_nested_config(self, section: str, key: str, default: Any = None) -> Any:
        """
        Get a nested configuration value.
        
        Args:
            section: Configuration section
            key: Key within the section
            default: Default value if key does not exist
            
        Returns:
            Configuration value
        """
        section_config = self.get_config(section, {})
        if isinstance(section_config, dict):
            return section_config.get(key, default)
        return default
        
    def set_nested_config(self, section: str, key: str, value: Any) -> None:
        """
        Set a nested configuration value.
        
        Args:
            section: Configuration section
            key: Key within the section
            value: Configuration value
        """
        if section not in self._config:
            self._config[section] = {}
            
        if isinstance(self._config[section], dict):
            self._config[section][key] = value
            
    async def get(self, path: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation path.
        
        Args:
            path: Configuration path (e.g., "system.test_mode")
            default: Default value if path does not exist
            
        Returns:
            Configuration value
        """
        parts = path.split('.')
        if len(parts) == 1:
            return self.get_config(parts[0], default)
        elif len(parts) == 2:
            return self.get_nested_config(parts[0], parts[1], default)
        else:
            # Handle deeper nesting if needed
            current = self._config
            for part in parts:
                if isinstance(current, dict) and part in current:
                    current = current[part]
                else:
                    return default
            return current
            
    async def set(self, path: str, value: Any) -> None:
        """
        Set configuration value using dot notation path.
        
        Args:
            path: Configuration path (e.g., "system.test_mode")
            value: Configuration value
        """
        parts = path.split('.')
        if len(parts) == 1:
            self.set_config(parts[0], value)
        elif len(parts) == 2:
            self.set_nested_config(parts[0], parts[1], value)
        else:
            # Handle deeper nesting if needed
            current = self._config
            for i, part in enumerate(parts[:-1]):
                if part not in current:
                    current[part] = {}
                if i < len(parts) - 2 and not isinstance(current[part], dict):
                    current[part] = {}
                current = current[part]
            current[parts[-1]] = value


# Global config instance
config = ConfigManager() 