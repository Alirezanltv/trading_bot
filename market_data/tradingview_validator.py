"""
TradingView Signal Validator

This module provides validation and redundancy features for TradingView signals,
ensuring that trading signals are legitimate and reliable before acting on them.
"""

import os
import json
import time
import math
import logging
import asyncio
import re
import hashlib
from enum import Enum
from typing import Dict, List, Any, Optional, Union, Callable, Set
from datetime import datetime, timedelta

from trading_system.core.component import Component, ComponentStatus
from trading_system.core.logging import get_logger
from trading_system.core.message_bus import MessageBus, MessageTypes
from trading_system.market_data.timeseries_db import get_timeseries_db

logger = get_logger("market_data.tradingview_validator")

class SignalConfidence(Enum):
    """Signal confidence levels."""
    HIGH = "high"       # Signal is highly reliable (multiple confirmations)
    MEDIUM = "medium"   # Signal is moderately reliable (some confirmations)
    LOW = "low"         # Signal has minimal confirmation
    REJECTED = "rejected"  # Signal was explicitly rejected

class SignalSource(Enum):
    """Signal source types."""
    WEBHOOK = "webhook"             # Direct webhook from TradingView
    ALERT_EMAIL = "alert_email"     # Email notification from TradingView
    SCRAPER = "scraper"             # Web scraping from TradingView
    EXTERNAL_API = "external_api"   # External API providing signals
    MOCK = "mock"                   # Mock signal for testing

class ValidationRule(Enum):
    """Signal validation rules."""
    PRICE_THRESHOLD = "price_threshold"  # Price must be within X% of current price
    VOLUME_THRESHOLD = "volume_threshold"  # Volume must be above threshold
    TIME_WINDOW = "time_window"   # Signal must be within time window
    DUPLICATE_CHECK = "duplicate_check"  # Check for duplicate signals
    CROSS_REFERENCE = "cross_reference"  # Cross-reference with other sources
    STRATEGY_PARAMS = "strategy_params"  # Validate strategy parameters

class ValidationResult:
    """Result of a signal validation."""
    
    def __init__(self, is_valid: bool, confidence: SignalConfidence, 
                 reason: str = None, details: Dict[str, Any] = None):
        """
        Initialize validation result.
        
        Args:
            is_valid: Whether the signal is valid
            confidence: Confidence level in the signal
            reason: Reason for validation result
            details: Additional details about validation
        """
        self.is_valid = is_valid
        self.confidence = confidence
        self.reason = reason
        self.details = details or {}
        self.timestamp = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "is_valid": self.is_valid,
            "confidence": self.confidence.value,
            "reason": self.reason,
            "details": self.details,
            "timestamp": self.timestamp.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ValidationResult':
        """Create from dictionary."""
        return cls(
            is_valid=data.get("is_valid", False),
            confidence=SignalConfidence(data.get("confidence", "low")),
            reason=data.get("reason"),
            details=data.get("details", {})
        )

class TradingViewValidator(Component):
    """
    TradingView Signal Validator.
    
    Validates trading signals from TradingView to ensure reliability,
    using redundancy and cross-referencing techniques.
    """
    
    def __init__(self, config: Dict[str, Any], message_bus: Optional[MessageBus] = None):
        """
        Initialize the TradingView signal validator.
        
        Args:
            config: Configuration dictionary
            message_bus: Message bus instance
        """
        super().__init__(name="TradingViewValidator")
        
        # Configuration
        self.config = config or {}
        self.message_bus = message_bus
        
        # Validation rules
        self.rules = {}
        self._load_validation_rules()
        
        # Signal history for duplicate detection
        self.signal_history = {}
        self.max_history_size = self.config.get("max_history_size", 1000)
        self.history_expiry = self.config.get("history_expiry", 86400)  # 24 hours
        
        # Price verification sources
        self.price_sources = []
        
        # Validation callbacks
        self.validation_callbacks: Dict[str, List[Callable]] = {}
        
        # Time-series database for persistent storage
        self.db = None
        
        # Statistics
        self.stats = {
            "total_signals": 0,
            "valid_signals": 0,
            "rejected_signals": 0,
            "high_confidence": 0,
            "medium_confidence": 0,
            "low_confidence": 0,
            "last_validation_time": None
        }
    
    def _load_validation_rules(self) -> None:
        """Load validation rules from configuration."""
        rules_config = self.config.get("validation_rules", {})
        
        # Default rules
        defaults = {
            ValidationRule.PRICE_THRESHOLD.value: {
                "enabled": True,
                "threshold_percent": 5.0  # 5% price difference
            },
            ValidationRule.VOLUME_THRESHOLD.value: {
                "enabled": True,
                "min_volume": 1.0  # Minimum volume
            },
            ValidationRule.TIME_WINDOW.value: {
                "enabled": True,
                "window_seconds": 300  # 5 minutes
            },
            ValidationRule.DUPLICATE_CHECK.value: {
                "enabled": True,
                "window_seconds": 300  # 5 minutes
            },
            ValidationRule.CROSS_REFERENCE.value: {
                "enabled": True,
                "min_sources": 1  # Minimum number of confirming sources
            },
            ValidationRule.STRATEGY_PARAMS.value: {
                "enabled": True,
                "required_params": ["strategy", "symbol", "price"]
            }
        }
        
        # Combine defaults with config
        for rule, default in defaults.items():
            self.rules[rule] = {**default, **rules_config.get(rule, {})}
            
        logger.info(f"Loaded {len(self.rules)} validation rules")
    
    async def initialize(self) -> bool:
        """
        Initialize the TradingView validator.
        
        Returns:
            Initialization success
        """
        try:
            self._status = ComponentStatus.INITIALIZING
            logger.info("Initializing TradingView validator")
            
            # Initialize time-series database
            db_config = self.config.get("timeseries_db", {})
            if db_config.get("enabled", True):
                self.db = get_timeseries_db(db_config)
                
                # Initialize database
                if not self.db._status == ComponentStatus.INITIALIZED:
                    if not await self.db.initialize():
                        logger.warning("Failed to initialize time-series database, continuing without persistence")
            
            # Register for signal messages
            if self.message_bus:
                await self.message_bus.subscribe(
                    topic="signal.tradingview.#",
                    callback=self._handle_signal_message
                )
                logger.info("Subscribed to TradingView signal messages")
            
            self._status = ComponentStatus.INITIALIZED
            logger.info("TradingView validator initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing TradingView validator: {str(e)}", exc_info=True)
            self._status = ComponentStatus.ERROR
            self._error = str(e)
            return False
    
    async def _handle_signal_message(self, message: Dict[str, Any]) -> None:
        """
        Handle signal message from message bus.
        
        Args:
            message: Signal message
        """
        try:
            message_type = message.get("type")
            data = message.get("data", {})
            
            if message_type == MessageTypes.SIGNAL:
                # Validate signal
                validation_result = await self.validate_signal(data)
                
                # Add validation result to signal
                data["validation"] = validation_result.to_dict()
                
                # Publish validated signal
                if validation_result.is_valid:
                    await self.message_bus.publish(
                        topic=f"signal.validated.{data.get('type', 'unknown')}",
                        message={
                            "type": MessageTypes.SIGNAL,
                            "data": data
                        }
                    )
                    logger.info(f"Published validated signal: {data.get('type')} for {data.get('symbol')}")
                else:
                    await self.message_bus.publish(
                        topic="signal.rejected",
                        message={
                            "type": MessageTypes.SIGNAL,
                            "data": data
                        }
                    )
                    logger.warning(f"Published rejected signal: {data.get('type')} for {data.get('symbol')}")
            
        except Exception as e:
            logger.error(f"Error handling signal message: {str(e)}", exc_info=True)
    
    async def validate_signal(self, signal: Dict[str, Any]) -> ValidationResult:
        """
        Validate a TradingView signal.
        
        Args:
            signal: Trading signal data
            
        Returns:
            Validation result
        """
        try:
            self.stats["total_signals"] += 1
            self.stats["last_validation_time"] = datetime.now().isoformat()
            
            logger.info(f"Validating signal: {signal.get('type')} for {signal.get('symbol')}")
            
            # Track failures
            failures = []
            
            # Apply validation rules
            rule_results = {}
            
            # 1. Basic parameter check
            if self.rules.get(ValidationRule.STRATEGY_PARAMS.value, {}).get("enabled", True):
                params_result = self._validate_required_params(signal)
                rule_results[ValidationRule.STRATEGY_PARAMS.value] = params_result
                
                if not params_result["valid"]:
                    failures.append(f"Missing required parameters: {params_result['missing']}")
            
            # 2. Duplicate check
            if self.rules.get(ValidationRule.DUPLICATE_CHECK.value, {}).get("enabled", True):
                duplicate_result = await self._validate_not_duplicate(signal)
                rule_results[ValidationRule.DUPLICATE_CHECK.value] = duplicate_result
                
                if not duplicate_result["valid"]:
                    failures.append(f"Duplicate signal: {duplicate_result['details']}")
            
            # 3. Time window check
            if self.rules.get(ValidationRule.TIME_WINDOW.value, {}).get("enabled", True):
                time_result = self._validate_time_window(signal)
                rule_results[ValidationRule.TIME_WINDOW.value] = time_result
                
                if not time_result["valid"]:
                    failures.append(f"Signal outside time window: {time_result['details']}")
            
            # 4. Price verification
            if self.rules.get(ValidationRule.PRICE_THRESHOLD.value, {}).get("enabled", True):
                price_result = await self._validate_price(signal)
                rule_results[ValidationRule.PRICE_THRESHOLD.value] = price_result
                
                if not price_result["valid"]:
                    failures.append(f"Price validation failed: {price_result['details']}")
            
            # 5. Volume verification
            if self.rules.get(ValidationRule.VOLUME_THRESHOLD.value, {}).get("enabled", True):
                volume_result = await self._validate_volume(signal)
                rule_results[ValidationRule.VOLUME_THRESHOLD.value] = volume_result
                
                if not volume_result["valid"]:
                    failures.append(f"Volume validation failed: {volume_result['details']}")
            
            # 6. Cross-reference with other sources
            if self.rules.get(ValidationRule.CROSS_REFERENCE.value, {}).get("enabled", True):
                cross_result = await self._validate_cross_reference(signal)
                rule_results[ValidationRule.CROSS_REFERENCE.value] = cross_result
                
                if not cross_result["valid"]:
                    failures.append(f"Cross-reference validation failed: {cross_result['details']}")
            
            # Calculate confidence level based on rule results
            is_valid = len(failures) == 0
            confidence = self._calculate_confidence(rule_results)
            
            # Create validation result
            reason = "All validation checks passed" if is_valid else "; ".join(failures)
            result = ValidationResult(
                is_valid=is_valid,
                confidence=confidence,
                reason=reason,
                details={
                    "rule_results": rule_results,
                    "validation_time": datetime.now().isoformat()
                }
            )
            
            # Update statistics
            if is_valid:
                self.stats["valid_signals"] += 1
                if confidence == SignalConfidence.HIGH:
                    self.stats["high_confidence"] += 1
                elif confidence == SignalConfidence.MEDIUM:
                    self.stats["medium_confidence"] += 1
                elif confidence == SignalConfidence.LOW:
                    self.stats["low_confidence"] += 1
            else:
                self.stats["rejected_signals"] += 1
            
            # Update signal history
            await self._update_signal_history(signal, result)
            
            # Invoke validation callbacks
            await self._invoke_validation_callbacks(signal, result)
            
            logger.info(f"Signal validation result: valid={is_valid}, confidence={confidence.value}, reason='{reason}'")
            return result
            
        except Exception as e:
            logger.error(f"Error validating signal: {str(e)}", exc_info=True)
            
            # Create failure result
            return ValidationResult(
                is_valid=False,
                confidence=SignalConfidence.REJECTED,
                reason=f"Validation error: {str(e)}",
                details={"error": str(e)}
            )
    
    def _validate_required_params(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate that signal has required parameters.
        
        Args:
            signal: Trading signal data
            
        Returns:
            Validation result
        """
        required_params = self.rules.get(ValidationRule.STRATEGY_PARAMS.value, {}).get("required_params", [])
        missing = [param for param in required_params if param not in signal or signal.get(param) is None]
        
        return {
            "valid": len(missing) == 0,
            "missing": missing
        }
    
    async def _validate_not_duplicate(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate that signal is not a duplicate.
        
        Args:
            signal: Trading signal data
            
        Returns:
            Validation result
        """
        # Create signature for this signal
        signature = self._create_signal_signature(signal)
        
        # Check in-memory history
        if signature in self.signal_history:
            previous = self.signal_history[signature]
            age_seconds = (datetime.now() - previous["timestamp"]).total_seconds()
            
            window_seconds = self.rules.get(ValidationRule.DUPLICATE_CHECK.value, {}).get("window_seconds", 300)
            
            if age_seconds < window_seconds:
                return {
                    "valid": False,
                    "details": f"Duplicate signal within {age_seconds:.1f} seconds (window: {window_seconds}s)"
                }
        
        # Check database if available
        if self.db and self.db._status == ComponentStatus.INITIALIZED:
            # Query for duplicate signals
            symbol = signal.get("symbol", "")
            signal_type = signal.get("type", "")
            
            # Only check if we have symbol and type
            if symbol and signal_type:
                start_time = datetime.now() - timedelta(seconds=self.rules.get(ValidationRule.DUPLICATE_CHECK.value, {}).get("window_seconds", 300))
                
                # Query for recent signals of same type and symbol
                signals = await self.db.get_signals(
                    symbol=symbol,
                    signal_type=signal_type,
                    start_time=start_time,
                    limit=10
                )
                
                # Check for duplicates
                for s in signals:
                    s_signature = self._create_signal_signature(s)
                    if s_signature == signature:
                        return {
                            "valid": False,
                            "details": f"Duplicate signal found in database (id: {s.get('id', '')})"
                        }
        
        return {
            "valid": True,
            "details": "No duplicates found"
        }
    
    def _validate_time_window(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate that signal is within acceptable time window.
        
        Args:
            signal: Trading signal data
            
        Returns:
            Validation result
        """
        # Get signal timestamp
        timestamp = signal.get("timestamp", signal.get("time", time.time()))
        if isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp()
            except ValueError:
                try:
                    timestamp = float(timestamp)
                except ValueError:
                    timestamp = time.time()
        
        # Convert to seconds if in milliseconds
        if timestamp > 1e12:  # Likely milliseconds
            timestamp = timestamp / 1000.0
        
        # Calculate age
        now = time.time()
        age_seconds = now - timestamp
        
        # Get allowed window
        window_seconds = self.rules.get(ValidationRule.TIME_WINDOW.value, {}).get("window_seconds", 300)
        
        # Check if within window
        if age_seconds > window_seconds:
            return {
                "valid": False,
                "details": f"Signal age ({age_seconds:.1f}s) exceeds window ({window_seconds}s)"
            }
        
        if age_seconds < -10:  # Allow for small clock skew
            return {
                "valid": False,
                "details": f"Signal from future ({-age_seconds:.1f}s ahead)"
            }
        
        return {
            "valid": True,
            "details": f"Signal age ({age_seconds:.1f}s) within window ({window_seconds}s)"
        }
    
    async def _validate_price(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate signal price against current market price.
        
        Args:
            signal: Trading signal data
            
        Returns:
            Validation result
        """
        try:
            # Get signal price
            signal_price = float(signal.get("price", 0))
            if signal_price <= 0:
                return {
                    "valid": False,
                    "details": "Invalid price in signal"
                }
            
            # Get symbol
            symbol = signal.get("symbol", "")
            if not symbol:
                return {
                    "valid": False,
                    "details": "Missing symbol in signal"
                }
            
            # Get current price from available sources
            current_price = await self._get_current_price(symbol)
            if current_price <= 0:
                return {
                    "valid": True,  # Consider valid if we can't get current price
                    "details": "Could not verify price (no current price available)"
                }
            
            # Calculate price difference
            if current_price > 0:
                diff_percent = abs(signal_price - current_price) / current_price * 100.0
                
                # Get threshold
                threshold_percent = self.rules.get(ValidationRule.PRICE_THRESHOLD.value, {}).get("threshold_percent", 5.0)
                
                # Check if within threshold
                if diff_percent > threshold_percent:
                    return {
                        "valid": False,
                        "details": f"Price difference ({diff_percent:.2f}%) exceeds threshold ({threshold_percent}%)"
                    }
                
                return {
                    "valid": True,
                    "details": f"Price difference ({diff_percent:.2f}%) within threshold ({threshold_percent}%)"
                }
            
            return {
                "valid": True,
                "details": "Could not compare prices (current price unavailable)"
            }
            
        except Exception as e:
            logger.error(f"Error validating price: {str(e)}", exc_info=True)
            return {
                "valid": True,  # Consider valid on error to avoid blocking signals
                "details": f"Error validating price: {str(e)}"
            }
    
    async def _validate_volume(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate signal against minimum volume requirements.
        
        Args:
            signal: Trading signal data
            
        Returns:
            Validation result
        """
        try:
            # Get volume from signal
            volume = float(signal.get("volume", 0))
            
            # Get minimum volume threshold
            min_volume = self.rules.get(ValidationRule.VOLUME_THRESHOLD.value, {}).get("min_volume", 1.0)
            
            # Check volume
            if volume < min_volume:
                return {
                    "valid": False,
                    "details": f"Volume ({volume}) below minimum ({min_volume})"
                }
            
            return {
                "valid": True,
                "details": f"Volume ({volume}) meets minimum ({min_volume})"
            }
            
        except Exception as e:
            logger.error(f"Error validating volume: {str(e)}", exc_info=True)
            return {
                "valid": True,  # Consider valid on error to avoid blocking signals
                "details": f"Error validating volume: {str(e)}"
            }
    
    async def _validate_cross_reference(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate signal by cross-referencing with other sources.
        
        Args:
            signal: Trading signal data
            
        Returns:
            Validation result
        """
        try:
            # Get minimum number of confirming sources
            min_sources = self.rules.get(ValidationRule.CROSS_REFERENCE.value, {}).get("min_sources", 1)
            
            # For now, just consider the signal itself as a source
            # In a real implementation, this would check other signal sources
            confirming_sources = 1
            
            # Check if meets minimum
            if confirming_sources < min_sources:
                return {
                    "valid": False,
                    "details": f"Only {confirming_sources} confirming sources (need {min_sources})"
                }
            
            return {
                "valid": True,
                "details": f"Found {confirming_sources} confirming sources (need {min_sources})"
            }
            
        except Exception as e:
            logger.error(f"Error cross-referencing signal: {str(e)}", exc_info=True)
            return {
                "valid": True,  # Consider valid on error to avoid blocking signals
                "details": f"Error cross-referencing signal: {str(e)}"
            }
    
    def _calculate_confidence(self, rule_results: Dict[str, Any]) -> SignalConfidence:
        """
        Calculate confidence level based on validation results.
        
        Args:
            rule_results: Results of validation rules
            
        Returns:
            Confidence level
        """
        # Count total rules and valid rules
        total_rules = len(rule_results)
        valid_rules = sum(1 for result in rule_results.values() if result.get("valid", False))
        
        # Calculate validation score (0-100%)
        if total_rules == 0:
            return SignalConfidence.LOW
        
        validation_score = valid_rules / total_rules * 100.0
        
        # Determine confidence level
        if validation_score == 100.0:
            return SignalConfidence.HIGH
        elif validation_score >= 75.0:
            return SignalConfidence.MEDIUM
        elif validation_score >= 50.0:
            return SignalConfidence.LOW
        else:
            return SignalConfidence.REJECTED
    
    def _create_signal_signature(self, signal: Dict[str, Any]) -> str:
        """
        Create a unique signature for a signal.
        
        Args:
            signal: Trading signal data
            
        Returns:
            Signal signature
        """
        # Extract key fields
        symbol = signal.get("symbol", "")
        signal_type = signal.get("type", "")
        price = str(signal.get("price", ""))
        strategy = signal.get("strategy", "")
        
        # Create signature string
        signature_str = f"{symbol}:{signal_type}:{price}:{strategy}"
        
        # Create hash
        return hashlib.md5(signature_str.encode()).hexdigest()
    
    async def _update_signal_history(self, signal: Dict[str, Any], result: ValidationResult) -> None:
        """
        Update signal history for duplicate detection.
        
        Args:
            signal: Trading signal data
            result: Validation result
        """
        # Create signature
        signature = self._create_signal_signature(signal)
        
        # Add to history
        self.signal_history[signature] = {
            "signal": signal,
            "result": result.to_dict(),
            "timestamp": datetime.now()
        }
        
        # Prune old entries
        await self._prune_signal_history()
        
        # Store in database if available
        if self.db and self.db._status == ComponentStatus.INITIALIZED and result.is_valid:
            try:
                # Add validation result to signal
                signal_with_validation = {**signal, "validation": result.to_dict()}
                
                # Store signal
                await self.db.store_signal(signal_with_validation)
            except Exception as e:
                logger.error(f"Error storing signal in database: {str(e)}", exc_info=True)
    
    async def _prune_signal_history(self) -> None:
        """Prune old entries from signal history."""
        now = datetime.now()
        
        # Remove expired entries
        expired = []
        for signature, entry in self.signal_history.items():
            age = (now - entry["timestamp"]).total_seconds()
            if age > self.history_expiry:
                expired.append(signature)
        
        for signature in expired:
            del self.signal_history[signature]
        
        # If still too many entries, remove oldest
        if len(self.signal_history) > self.max_history_size:
            # Sort by timestamp
            sorted_entries = sorted(
                self.signal_history.items(),
                key=lambda x: x[1]["timestamp"]
            )
            
            # Remove oldest entries
            to_remove = len(self.signal_history) - self.max_history_size
            for i in range(to_remove):
                if i < len(sorted_entries):
                    signature = sorted_entries[i][0]
                    del self.signal_history[signature]
    
    async def _get_current_price(self, symbol: str) -> float:
        """
        Get current price for a symbol.
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            Current price or 0 if unavailable
        """
        try:
            # Try to get price from the message bus first
            if self.message_bus:
                # Request current price from market data manager
                response = await self.message_bus.request(
                    topic="market_data.price",
                    message={
                        "symbol": symbol
                    },
                    timeout=2.0  # 2 second timeout
                )
                
                if response and "price" in response:
                    return float(response["price"])
            
            # Fall back to database
            if self.db and self.db._status == ComponentStatus.INITIALIZED:
                # Get latest candle
                candles = await self.db.get_market_data(
                    symbol=symbol,
                    timeframe="1m",
                    start_time="-5m",
                    limit=1
                )
                
                if candles and len(candles) > 0:
                    return float(candles[0].get("close", 0))
            
            return 0.0
            
        except Exception as e:
            logger.error(f"Error getting current price for {symbol}: {str(e)}", exc_info=True)
            return 0.0
    
    async def register_validation_callback(self, callback: Callable[[Dict[str, Any], ValidationResult], None]) -> None:
        """
        Register a callback for signal validation events.
        
        Args:
            callback: Callback function
        """
        if "validation" not in self.validation_callbacks:
            self.validation_callbacks["validation"] = []
        
        self.validation_callbacks["validation"].append(callback)
        logger.info("Registered validation callback")
    
    async def _invoke_validation_callbacks(self, signal: Dict[str, Any], result: ValidationResult) -> None:
        """
        Invoke validation callbacks.
        
        Args:
            signal: Trading signal data
            result: Validation result
        """
        if "validation" in self.validation_callbacks:
            for callback in self.validation_callbacks["validation"]:
                try:
                    await callback(signal, result)
                except Exception as e:
                    logger.error(f"Error in validation callback: {str(e)}", exc_info=True)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get validator statistics.
        
        Returns:
            Statistics dictionary
        """
        return {
            **self.stats,
            "signal_history_size": len(self.signal_history)
        }

# Singleton instance
_instance = None

def get_tradingview_validator(config: Dict[str, Any] = None, message_bus: Optional[MessageBus] = None) -> TradingViewValidator:
    """
    Get or create the TradingViewValidator instance.
    
    Args:
        config: Configuration dictionary
        message_bus: Message bus instance
        
    Returns:
        TradingViewValidator instance
    """
    global _instance
    if _instance is None:
        if config is None:
            config = {}
        _instance = TradingViewValidator(config, message_bus)
    return _instance 