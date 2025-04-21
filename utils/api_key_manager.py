"""
API Key Manager

This module provides secure handling of API keys and sensitive credentials
for exchange integrations and other external services.
"""

import os
import json
import base64
import logging
import hashlib
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv

# Configure logger
logger = logging.getLogger("utils.api_key_manager")


class ApiKeyManager:
    """
    Secure API key manager for handling sensitive credentials.
    
    Features:
    - Load API keys from environment variables or secure files
    - Basic obfuscation for logging and display
    - Verification of required credentials
    - Support for multiple exchanges and API types
    """
    
    def __init__(self, env_file: Optional[str] = None):
        """
        Initialize the API key manager.
        
        Args:
            env_file: Path to .env file (optional, defaults to project .env)
        """
        # Load environment variables
        if env_file and os.path.exists(env_file):
            load_dotenv(env_file)
        else:
            # Try to find .env in common locations
            possible_locations = [
                ".env",
                "../.env",
                "../../.env",
                os.path.join(os.path.dirname(__file__), "../.env"),
                os.path.join(os.path.dirname(__file__), "../../.env"),
            ]
            
            for location in possible_locations:
                if os.path.exists(location):
                    load_dotenv(location)
                    break
        
        # Credential cache
        self.credentials: Dict[str, Dict[str, str]] = {}
        
        logger.debug("API Key Manager initialized")
    
    def get_exchange_credentials(self, exchange: str) -> Dict[str, str]:
        """
        Get credentials for a specific exchange.
        
        Args:
            exchange: Exchange name (e.g., 'nobitex', 'binance')
            
        Returns:
            Dictionary containing API credentials
            
        Raises:
            ValueError: If required credentials are not found
        """
        # Check cache first
        if exchange in self.credentials:
            return self.credentials[exchange]
        
        exchange = exchange.lower()
        
        # Standard keys to look for
        credential_keys = ["api_key", "api_secret"]
        extra_keys = ["passphrase", "password", "token"]
        
        result = {}
        
        # Try to load from environment variables
        for key in credential_keys:
            env_key = f"{exchange.upper()}_{key.upper()}"
            value = os.getenv(env_key)
            
            if value:
                result[key] = value
            elif key in credential_keys:  # Only required for standard keys
                logger.warning(f"Required credential '{key}' not found for {exchange}")
        
        # Load optional extra credentials
        for key in extra_keys:
            env_key = f"{exchange.upper()}_{key.upper()}"
            value = os.getenv(env_key)
            
            if value:
                result[key] = value
        
        # Verify we have the minimum required credentials
        if exchange == "nobitex":
            if "api_key" not in result:
                raise ValueError(f"Missing required API key for {exchange}")
        
        # Cache for future use
        self.credentials[exchange] = result
        
        logger.debug(f"Loaded credentials for {exchange}")
        return result
    
    def get_credential(self, service: str, key: str) -> Optional[str]:
        """
        Get a specific credential for a service.
        
        Args:
            service: Service name
            key: Credential key
            
        Returns:
            Credential value if found, None otherwise
        """
        env_key = f"{service.upper()}_{key.upper()}"
        return os.getenv(env_key)
    
    def get_obfuscated_key(self, key: str) -> str:
        """
        Get an obfuscated version of a key for logging.
        
        Args:
            key: API key or secret to obfuscate
            
        Returns:
            Obfuscated key (first 4 chars + **** + last 4 chars)
        """
        if not key or len(key) < 8:
            return "****"
        
        return key[:4] + "****" + key[-4:]
    
    def verify_credentials(self, exchange: str) -> bool:
        """
        Verify that all required credentials for an exchange are present.
        
        Args:
            exchange: Exchange name
            
        Returns:
            True if all required credentials are present, False otherwise
        """
        try:
            creds = self.get_exchange_credentials(exchange)
            
            # Check requirements based on exchange
            if exchange.lower() == "nobitex":
                return "api_key" in creds
                
            # General case - most exchanges need both key and secret
            return "api_key" in creds and "api_secret" in creds
            
        except Exception as e:
            logger.error(f"Error verifying credentials for {exchange}: {str(e)}")
            return False
    
    def load_credentials_file(self, file_path: str, password: Optional[str] = None) -> bool:
        """
        Load credentials from a JSON file.
        
        Args:
            file_path: Path to credentials file
            password: Optional password to decrypt file
            
        Returns:
            True if loaded successfully, False otherwise
        """
        try:
            file_path = Path(file_path)
            
            if not file_path.exists():
                logger.error(f"Credentials file not found: {file_path}")
                return False
            
            with open(file_path, "r") as f:
                data = json.load(f)
            
            # Simple decryption if password provided
            if password:
                # In a real implementation, we would use proper encryption here
                pass
            
            # Update credentials cache
            for exchange, creds in data.items():
                self.credentials[exchange.lower()] = creds
            
            logger.info(f"Loaded credentials from {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading credentials file: {str(e)}")
            return False


# Global instance for easy access
api_key_manager = ApiKeyManager()


def get_credentials(exchange: str) -> Dict[str, str]:
    """
    Helper function to get credentials for an exchange.
    
    Args:
        exchange: Exchange name
        
    Returns:
        Dictionary with credentials
    """
    return api_key_manager.get_exchange_credentials(exchange)


def verify_credentials(exchange: str) -> bool:
    """
    Helper function to verify credentials.
    
    Args:
        exchange: Exchange name
        
    Returns:
        True if credentials are valid
    """
    return api_key_manager.verify_credentials(exchange) 