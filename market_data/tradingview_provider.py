"""
TradingView Data Provider

This module provides integration with TradingView alerts as a market data source.
It validates, processes, and distributes TradingView signals.
"""

import os
import json
import time
import logging
import asyncio
import hashlib
from enum import Enum
from typing import Dict, List, Any, Optional, Union, Callable
from datetime import datetime

from trading_system.core.component import Component, ComponentStatus
from trading_system.core.logging import get_logger
from trading_system.core.message_bus import MessageBus, MessageTypes
from trading_system.market_data.tradingview_validator import TradingViewValidator

logger = get_logger("market_data.tradingview")

class TradingViewSignalType(Enum):
    """Types of TradingView signals."""
    STRATEGY_ALERT = "strategy_alert"
    INDICATOR_ALERT = "indicator_alert"
    PRICE_ALERT = "price_alert"
    VOLUME_ALERT = "volume_alert"
    CUSTOM_ALERT = "custom_alert"

class TradingViewProvider(Component):
    """
    TradingView Provider handles incoming TradingView signals.
    
    This component:
    1. Receives TradingView webhook alerts
    2. Validates signals for authenticity and relevance
    3. Normalizes data format
    4. Distributes signals to subscribers via the message bus
    5. Implements redundancy and cross-validation
    """
    
    def __init__(self, config: Dict[str, Any], message_bus: Optional[MessageBus] = None):
        """
        Initialize the TradingView provider.
        
        Args:
            config: Configuration dictionary
            message_bus: Message bus instance
        """
        super().__init__(name="TradingViewProvider")
        
        # Configuration
        self.config = config or {}
        self.message_bus = message_bus
        
        # Signal validation
        self.validator = None
        
        # Signal processing
        self.signal_processors = {}
        self.default_processor = self._default_signal_processor
        
        # Signal history
        self.signal_history = []
        self.max_history_size = self.config.get("max_history_size", 1000)
        
        # Statistics
        self.stats = {
            "total_signals": 0,
            "valid_signals": 0,
            "invalid_signals": 0,
            "processed_signals": 0,
            "rejected_signals": 0,
            "signal_types": {}
        }
    
    async def initialize(self) -> bool:
        """
        Initialize the TradingView provider.
        
        Returns:
            Initialization success
        """
        try:
            self._status = ComponentStatus.INITIALIZING
            logger.info("Initializing TradingView Provider")
            
            # Initialize validator
            validator_config = self.config.get("validator", {})
            self.validator = TradingViewValidator(validator_config)
            if not await self.validator.initialize():
                logger.warning("Failed to initialize TradingView validator, continuing without validation")
            
            # Register message handlers
            if self.message_bus:
                await self.message_bus.subscribe(
                    topic="tradingview.signal",
                    callback=self._handle_tradingview_signal
                )
                logger.info("Subscribed to TradingView signals")
            
            self._status = ComponentStatus.INITIALIZED
            logger.info("TradingView Provider initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing TradingView Provider: {str(e)}", exc_info=True)
            self._status = ComponentStatus.ERROR
            self._error = str(e)
            return False
    
    async def register_signal_processor(self, signal_type: Union[str, TradingViewSignalType], processor: Callable) -> bool:
        """
        Register a signal processor for a specific signal type.
        
        Args:
            signal_type: Signal type to process
            processor: Callback function to process signals
            
        Returns:
            Registration success
        """
        try:
            # Convert enum to string if needed
            if isinstance(signal_type, TradingViewSignalType):
                signal_type = signal_type.value
            
            self.signal_processors[signal_type] = processor
            logger.info(f"Registered signal processor for {signal_type}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error registering signal processor: {str(e)}", exc_info=True)
            return False
    
    async def unregister_signal_processor(self, signal_type: Union[str, TradingViewSignalType]) -> bool:
        """
        Unregister a signal processor.
        
        Args:
            signal_type: Signal type to unregister
            
        Returns:
            Unregistration success
        """
        try:
            # Convert enum to string if needed
            if isinstance(signal_type, TradingViewSignalType):
                signal_type = signal_type.value
            
            if signal_type in self.signal_processors:
                del self.signal_processors[signal_type]
                logger.info(f"Unregistered signal processor for {signal_type}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error unregistering signal processor: {str(e)}", exc_info=True)
            return False
    
    async def process_signal(self, signal: Dict[str, Any]) -> bool:
        """
        Process a TradingView signal.
        
        Args:
            signal: TradingView signal data
            
        Returns:
            Processing success
        """
        try:
            self.stats["total_signals"] += 1
            
            # Validate signal
            if self.validator and self.validator._status == ComponentStatus.INITIALIZED:
                validation_result = await self.validator.validate_signal(signal)
                
                if validation_result.is_valid:
                    self.stats["valid_signals"] += 1
                    logger.info(f"Signal validated: {validation_result.confidence.name}")
                else:
                    self.stats["invalid_signals"] += 1
                    logger.warning(f"Invalid signal: {validation_result.error}")
                    return False
            
            # Add to history
            self._add_to_history(signal)
            
            # Extract signal type
            signal_type = self._get_signal_type(signal)
            
            # Update stats
            if signal_type not in self.stats["signal_types"]:
                self.stats["signal_types"][signal_type] = 0
            self.stats["signal_types"][signal_type] += 1
            
            # Process signal
            if signal_type in self.signal_processors:
                processor = self.signal_processors[signal_type]
                result = await processor(signal)
                
                if result:
                    self.stats["processed_signals"] += 1
                    logger.info(f"Signal processed: {signal.get('symbol', 'Unknown')}")
                else:
                    self.stats["rejected_signals"] += 1
                    logger.warning(f"Signal rejected by processor: {signal.get('symbol', 'Unknown')}")
                
                return result
            else:
                # Use default processor
                result = await self.default_processor(signal)
                
                if result:
                    self.stats["processed_signals"] += 1
                else:
                    self.stats["rejected_signals"] += 1
                
                return result
            
        except Exception as e:
            logger.error(f"Error processing signal: {str(e)}", exc_info=True)
            return False
    
    def _get_signal_type(self, signal: Dict[str, Any]) -> str:
        """
        Extract signal type from the signal data.
        
        Args:
            signal: TradingView signal data
            
        Returns:
            Signal type
        """
        # Check if type is explicitly provided
        if "signal_type" in signal:
            return signal["signal_type"]
        
        # Check if strategy alert
        if "strategy" in signal or ("meta" in signal and "strategy" in signal["meta"]):
            return TradingViewSignalType.STRATEGY_ALERT.value
        
        # Check if indicator alert
        if "indicator" in signal or ("meta" in signal and "indicator" in signal["meta"]):
            return TradingViewSignalType.INDICATOR_ALERT.value
        
        # Check if price alert
        if "price" in signal and ("action" in signal or "condition" in signal):
            return TradingViewSignalType.PRICE_ALERT.value
        
        # Check if volume alert
        if "volume" in signal or "vol" in signal:
            return TradingViewSignalType.VOLUME_ALERT.value
        
        # Default to custom alert
        return TradingViewSignalType.CUSTOM_ALERT.value
    
    def _add_to_history(self, signal: Dict[str, Any]) -> None:
        """
        Add signal to history.
        
        Args:
            signal: TradingView signal data
        """
        # Add timestamp if not present
        if "timestamp" not in signal:
            signal["timestamp"] = int(datetime.now().timestamp() * 1000)
        
        # Add to history
        self.signal_history.append(signal)
        
        # Trim history if needed
        if len(self.signal_history) > self.max_history_size:
            self.signal_history = self.signal_history[-self.max_history_size:]
    
    async def _default_signal_processor(self, signal: Dict[str, Any]) -> bool:
        """
        Default signal processor.
        
        Args:
            signal: TradingView signal data
            
        Returns:
            Processing success
        """
        try:
            # Normalize signal
            normalized = self._normalize_signal(signal)
            
            # Broadcast on message bus if available
            if self.message_bus:
                signal_type = self._get_signal_type(signal)
                
                await self.message_bus.publish(
                    topic=f"tradingview.{signal_type}",
                    message=normalized
                )
                
                # Also publish to a general tradingview topic
                await self.message_bus.publish(
                    topic="tradingview.signal.processed",
                    message=normalized
                )
                
                logger.debug(f"Published signal to message bus: {signal.get('symbol', 'Unknown')}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error in default signal processor: {str(e)}", exc_info=True)
            return False
    
    def _normalize_signal(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize the signal to a standard format.
        
        Args:
            signal: TradingView signal data
            
        Returns:
            Normalized signal
        """
        # Create a copy of the signal
        normalized = signal.copy()
        
        # Add timestamp if not present
        if "timestamp" not in normalized:
            normalized["timestamp"] = int(datetime.now().timestamp() * 1000)
        
        # Add signal_id if not present
        if "signal_id" not in normalized:
            # Create a unique ID from signal content
            signal_str = json.dumps(signal, sort_keys=True)
            signal_hash = hashlib.md5(signal_str.encode()).hexdigest()
            normalized["signal_id"] = signal_hash
        
        # Add signal_type if not present
        if "signal_type" not in normalized:
            normalized["signal_type"] = self._get_signal_type(signal)
        
        # Ensure symbol is normalized
        if "symbol" in normalized:
            # Ensure it's uppercase
            normalized["symbol"] = normalized["symbol"].upper()
            
            # Remove any exchange prefix if needed
            if ":" in normalized["symbol"]:
                parts = normalized["symbol"].split(":")
                if len(parts) == 2:
                    normalized["exchange"] = parts[0]
                    normalized["symbol"] = parts[1]
        
        return normalized
    
    async def _handle_tradingview_signal(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle TradingView signal from message bus.
        
        Args:
            message: Signal message
            
        Returns:
            Response message
        """
        try:
            # Process signal
            signal = message.get("signal", {})
            result = await self.process_signal(signal)
            
            return {"success": result}
            
        except Exception as e:
            logger.error(f"Error handling TradingView signal: {str(e)}", exc_info=True)
            return {"success": False, "error": str(e)}
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get provider statistics.
        
        Returns:
            Dictionary of statistics
        """
        return self.stats
    
    def get_signal_history(self, limit: int = 10, signal_type: str = None) -> List[Dict[str, Any]]:
        """
        Get signal history.
        
        Args:
            limit: Maximum number of signals to return
            signal_type: Filter by signal type
            
        Returns:
            List of signals
        """
        if signal_type:
            # Filter by signal type
            filtered = [s for s in self.signal_history if self._get_signal_type(s) == signal_type]
            return filtered[-limit:]
        else:
            return self.signal_history[-limit:]

# Singleton instance
_instance = None

def get_tradingview_provider(config: Dict[str, Any] = None, message_bus: Optional[MessageBus] = None) -> TradingViewProvider:
    """
    Get or create the TradingViewProvider instance.
    
    Args:
        config: Configuration dictionary
        message_bus: Message bus instance
        
    Returns:
        TradingViewProvider instance
    """
    global _instance
    if _instance is None:
        if config is None:
            config = {}
        _instance = TradingViewProvider(config, message_bus)
    return _instance 