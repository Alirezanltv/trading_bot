"""
TradingView Webhook Handler

This module provides a webhook server to receive alerts from TradingView.
It validates incoming alerts, processes them into structured signals,
and distributes them to the trading system.

Features:
- Webhook server for receiving TradingView alerts
- Signature validation for security
- Alert normalization and transformation
- Multiple alert formats support
- Redundancy with alternative sources
"""

import os
import json
import time
import asyncio
import hmac
import hashlib
import logging
from enum import Enum
from typing import Dict, List, Any, Optional, Union, Callable
from datetime import datetime

import aiohttp
from aiohttp import web
from dotenv import load_dotenv

from trading_system.core.component import Component, ComponentStatus
from trading_system.core.logging import get_logger
from trading_system.core.message_bus import MessageBus, MessageTypes

# Load environment variables
load_dotenv()

logger = get_logger("market_data.tradingview_webhook")

class AlertType(Enum):
    """Types of TradingView alerts."""
    STRATEGY = "strategy"
    INDICATOR = "indicator"
    PRICE = "price"
    VOLUME = "volume"
    CUSTOM = "custom"

class SignalStatus(Enum):
    """Signal processing status."""
    RECEIVED = "received"
    VALIDATED = "validated"
    INVALID = "invalid"
    PROCESSED = "processed"
    ERROR = "error"

class ValidationResult:
    """Result of signal validation."""
    
    def __init__(self, is_valid: bool, confidence: float = 0.0, error: str = None):
        """
        Initialize validation result.
        
        Args:
            is_valid: Whether the signal is valid
            confidence: Confidence score (0.0 to 1.0)
            error: Error message if invalid
        """
        self.is_valid = is_valid
        self.confidence = confidence
        self.error = error

class TradingViewWebhookHandler(Component):
    """
    TradingView Webhook Handler for receiving and processing alerts.
    
    This component:
    1. Runs a webhook server to receive TradingView alerts
    2. Validates the alerts using HMAC signatures
    3. Normalizes alert formats into standard signals
    4. Distributes signals to the trading system
    5. Provides redundancy with alternative data sources
    """
    
    def __init__(self, config: Dict[str, Any], message_bus: Optional[MessageBus] = None):
        """
        Initialize the TradingView webhook handler.
        
        Args:
            config: Configuration dictionary containing:
                - port: Webhook server port (default: 8080)
                - host: Webhook server host (default: 0.0.0.0)
                - endpoint: Webhook endpoint (default: "/webhook/tradingview")
                - secret_key: Webhook secret key for signature validation
                - signature_header: Header containing the signature (default: "X-Tradingview-Signature")
                - signature_type: Signature type (default: "sha256")
                - require_signature: Whether to require signatures (default: True)
                - max_payload_size: Maximum payload size in bytes (default: 1024 * 1024)
                - timeout: Webhook server timeout in seconds (default: 60)
            message_bus: Message bus instance
        """
        super().__init__(name="TradingViewWebhookHandler")
        
        # Configuration
        self.config = config or {}
        self.port = self.config.get("port", 8080)
        self.host = self.config.get("host", "0.0.0.0")
        self.endpoint = self.config.get("endpoint", "/webhook/tradingview")
        self.secret_key = self.config.get("secret_key") or os.getenv("TRADINGVIEW_SECRET")
        self.signature_header = self.config.get("signature_header", "X-Tradingview-Signature")
        self.signature_type = self.config.get("signature_type", "sha256")
        self.require_signature = self.config.get("require_signature", True)
        self.max_payload_size = self.config.get("max_payload_size", 1024 * 1024)  # 1MB
        self.timeout = self.config.get("timeout", 60)
        
        # Message bus
        self.message_bus = message_bus
        
        # HTTP server
        self.app = None
        self.runner = None
        self.site = None
        
        # Signal processors
        self.signal_processors = {}
        self.default_processor = self._default_signal_processor
        
        # Statistics
        self.stats = {
            "total_alerts": 0,
            "valid_alerts": 0,
            "invalid_alerts": 0,
            "processed_alerts": 0,
            "error_alerts": 0,
            "alert_types": {}
        }
        
        # Signal history
        self.signal_history = []
        self.max_history_size = self.config.get("max_history_size", 1000)
    
    async def initialize(self) -> bool:
        """
        Initialize the TradingView webhook handler.
        
        Creates and starts the webhook server.
        
        Returns:
            Initialization success
        """
        try:
            self._status = ComponentStatus.INITIALIZING
            logger.info("Initializing TradingView Webhook Handler")
            
            # Create webhook server
            self.app = web.Application(client_max_size=self.max_payload_size)
            self.app.add_routes([
                web.post(self.endpoint, self._handle_webhook)
            ])
            
            # Start server
            self.runner = web.AppRunner(self.app)
            await self.runner.setup()
            self.site = web.TCPSite(self.runner, self.host, self.port)
            await self.site.start()
            
            logger.info(f"TradingView webhook server started at http://{self.host}:{self.port}{self.endpoint}")
            
            self._status = ComponentStatus.INITIALIZED
            logger.info("TradingView Webhook Handler initialized")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing TradingView Webhook Handler: {str(e)}", exc_info=True)
            self._status = ComponentStatus.ERROR
            self._error = str(e)
            return False
    
    async def stop(self) -> bool:
        """
        Stop the TradingView webhook handler.
        
        Stops the webhook server.
        
        Returns:
            Stop success
        """
        try:
            logger.info("Stopping TradingView Webhook Handler")
            
            # Stop server
            if self.site:
                await self.site.stop()
                self.site = None
            
            if self.runner:
                await self.runner.cleanup()
                self.runner = None
            
            self.app = None
            
            self._status = ComponentStatus.STOPPED
            logger.info("TradingView Webhook Handler stopped")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping TradingView Webhook Handler: {str(e)}", exc_info=True)
            self._status = ComponentStatus.ERROR
            self._error = str(e)
            return False
    
    async def register_signal_processor(self, alert_type: Union[str, AlertType], processor: Callable) -> bool:
        """
        Register a signal processor for a specific alert type.
        
        Args:
            alert_type: Alert type to process
            processor: Callback function to process signals
            
        Returns:
            Registration success
        """
        try:
            # Convert enum to string if needed
            if isinstance(alert_type, AlertType):
                alert_type = alert_type.value
            
            self.signal_processors[alert_type] = processor
            logger.info(f"Registered signal processor for {alert_type}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error registering signal processor: {str(e)}", exc_info=True)
            return False
    
    async def unregister_signal_processor(self, alert_type: Union[str, AlertType]) -> bool:
        """
        Unregister a signal processor.
        
        Args:
            alert_type: Alert type to unregister
            
        Returns:
            Unregistration success
        """
        try:
            # Convert enum to string if needed
            if isinstance(alert_type, AlertType):
                alert_type = alert_type.value
            
            if alert_type in self.signal_processors:
                del self.signal_processors[alert_type]
                logger.info(f"Unregistered signal processor for {alert_type}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error unregistering signal processor: {str(e)}", exc_info=True)
            return False
    
    async def _handle_webhook(self, request: web.Request) -> web.Response:
        """
        Handle incoming webhook requests.
        
        Args:
            request: HTTP request
            
        Returns:
            HTTP response
        """
        self.stats["total_alerts"] += 1
        
        try:
            # Check content type
            content_type = request.headers.get("Content-Type", "")
            
            if "application/json" not in content_type:
                self.stats["invalid_alerts"] += 1
                logger.warning(f"Invalid content type: {content_type}")
                return web.json_response(
                    {"status": "error", "message": "Invalid content type, expected application/json"},
                    status=400
                )
            
            # Get payload
            try:
                payload = await request.json()
            except Exception as e:
                self.stats["invalid_alerts"] += 1
                logger.warning(f"Invalid JSON payload: {str(e)}")
                return web.json_response(
                    {"status": "error", "message": "Invalid JSON payload"},
                    status=400
                )
            
            # Validate signature if required
            if self.require_signature and self.secret_key:
                try:
                    signature = request.headers.get(self.signature_header)
                    
                    if not signature:
                        self.stats["invalid_alerts"] += 1
                        logger.warning("Missing signature header")
                        return web.json_response(
                            {"status": "error", "message": "Missing signature header"},
                            status=401
                        )
                    
                    # Verify signature
                    payload_bytes = await request.read()
                    expected_signature = self._generate_signature(payload_bytes)
                    
                    if signature != expected_signature:
                        self.stats["invalid_alerts"] += 1
                        logger.warning(f"Invalid signature: {signature} != {expected_signature}")
                        return web.json_response(
                            {"status": "error", "message": "Invalid signature"},
                            status=401
                        )
                    
                except Exception as e:
                    self.stats["invalid_alerts"] += 1
                    logger.warning(f"Error validating signature: {str(e)}")
                    return web.json_response(
                        {"status": "error", "message": "Error validating signature"},
                        status=500
                    )
            
            # Process the alert
            asyncio.create_task(self._process_alert(payload))
            
            return web.json_response({"status": "success"})
            
        except Exception as e:
            self.stats["error_alerts"] += 1
            logger.error(f"Error handling webhook: {str(e)}", exc_info=True)
            return web.json_response(
                {"status": "error", "message": "Internal server error"},
                status=500
            )
    
    def _generate_signature(self, payload: bytes) -> str:
        """
        Generate HMAC signature for payload verification.
        
        Args:
            payload: Payload bytes
            
        Returns:
            HMAC signature as hex string
        """
        if not self.secret_key:
            return ""
        
        h = hmac.new(
            key=self.secret_key.encode("utf-8"),
            msg=payload,
            digestmod=getattr(hashlib, self.signature_type)
        )
        
        return h.hexdigest()
    
    async def _process_alert(self, alert: Dict[str, Any]) -> bool:
        """
        Process a TradingView alert.
        
        Args:
            alert: TradingView alert data
            
        Returns:
            Processing success
        """
        signal_status = SignalStatus.RECEIVED
        
        try:
            logger.info(f"Processing TradingView alert: {alert.get('ticker', 'unknown')}")
            
            # Extract alert type
            alert_type = self._get_alert_type(alert)
            
            # Update stats
            if alert_type not in self.stats["alert_types"]:
                self.stats["alert_types"][alert_type] = 0
            self.stats["alert_types"][alert_type] += 1
            
            # Validate alert
            validation_result = self._validate_alert(alert)
            
            if not validation_result.is_valid:
                signal_status = SignalStatus.INVALID
                self.stats["invalid_alerts"] += 1
                logger.warning(f"Invalid alert: {validation_result.error}")
                return False
            
            signal_status = SignalStatus.VALIDATED
            self.stats["valid_alerts"] += 1
            
            # Normalize alert
            signal = self._normalize_alert(alert)
            
            # Add to history
            self._add_to_history(signal)
            
            # Process signal
            processor = self.signal_processors.get(alert_type, self.default_processor)
            
            success = await processor(signal)
            
            if success:
                signal_status = SignalStatus.PROCESSED
                self.stats["processed_alerts"] += 1
                
                # Forward to message bus if available
                if self.message_bus:
                    await self.message_bus.publish(
                        topic="tradingview.signal",
                        message={
                            "signal": signal,
                            "timestamp": datetime.now().isoformat(),
                            "source": "tradingview_webhook"
                        }
                    )
                
                logger.info(f"Successfully processed TradingView alert: {signal.get('ticker', 'unknown')}")
                return True
            else:
                signal_status = SignalStatus.ERROR
                self.stats["error_alerts"] += 1
                logger.warning(f"Failed to process TradingView alert: {signal.get('ticker', 'unknown')}")
                return False
            
        except Exception as e:
            signal_status = SignalStatus.ERROR
            self.stats["error_alerts"] += 1
            logger.error(f"Error processing TradingView alert: {str(e)}", exc_info=True)
            return False
    
    def _validate_alert(self, alert: Dict[str, Any]) -> ValidationResult:
        """
        Validate a TradingView alert.
        
        Args:
            alert: TradingView alert data
            
        Returns:
            Validation result
        """
        # Check for minimum required fields
        if not alert:
            return ValidationResult(False, 0.0, "Empty alert")
        
        # Try to find ticker/symbol information
        ticker = None
        
        if "ticker" in alert:
            ticker = alert["ticker"]
        elif "symbol" in alert:
            ticker = alert["symbol"]
        elif "chart" in alert and "symbol" in alert["chart"]:
            ticker = alert["chart"]["symbol"]
        elif "text" in alert:
            # Try to extract from text
            text = alert["text"]
            
            # Common format: {{ticker}} or symbol={{ticker}}
            if "{{" in text and "}}" in text:
                start = text.find("{{") + 2
                end = text.find("}}", start)
                
                if start > 1 and end > start:
                    ticker = text[start:end].strip()
        
        if not ticker:
            return ValidationResult(False, 0.0, "Missing ticker/symbol information")
        
        # Validate ticker format
        if not self._is_valid_ticker_format(ticker):
            return ValidationResult(
                False, 
                0.0, 
                f"Invalid ticker format: {ticker}"
            )
        
        # Check for alert message or action
        has_message = "message" in alert or "text" in alert
        has_action = (
            "action" in alert or 
            "position" in alert or 
            ("strategy" in alert and "position" in alert["strategy"])
        )
        
        if not has_message and not has_action:
            return ValidationResult(
                False,
                0.2,
                "Missing alert message or action"
            )
        
        # Validate based on alert type
        alert_type = self._get_alert_type(alert)
        
        if alert_type == AlertType.STRATEGY.value:
            # Strategy alerts should have position information
            if not has_action:
                return ValidationResult(
                    False,
                    0.3,
                    "Strategy alert missing position/action information"
                )
        
        elif alert_type == AlertType.PRICE.value:
            # Price alerts should have price information
            if "price" not in alert and "close" not in alert and "last" not in alert:
                return ValidationResult(
                    False,
                    0.3,
                    "Price alert missing price information"
                )
        
        # If we got here, the alert is valid
        confidence = 0.8  # Default confidence
        
        # Increase confidence based on additional checks
        if "timestamp" in alert:
            confidence += 0.05
        
        if "timeframe" in alert or "interval" in alert:
            confidence += 0.05
        
        if "exchange" in alert:
            confidence += 0.05
        
        if "indicator" in alert and isinstance(alert["indicator"], dict):
            confidence += 0.05
        
        return ValidationResult(True, min(1.0, confidence), None)
    
    def _is_valid_ticker_format(self, ticker: str) -> bool:
        """
        Check if a ticker has a valid format.
        
        Args:
            ticker: Ticker to check
            
        Returns:
            Whether the ticker has a valid format
        """
        if not ticker:
            return False
        
        # Common formats: BTC/USDT, BTCUSDT, BTC-USDT
        if "/" in ticker or "-" in ticker:
            parts = ticker.replace("/", "-").split("-")
            return len(parts) == 2 and all(part.strip() for part in parts)
        
        # If no separator, common format is like BTCUSDT where we need to identify the parts
        common_quote_currencies = ["USDT", "USD", "BTC", "ETH", "IRT", "RLS"]
        
        for quote in common_quote_currencies:
            if ticker.upper().endswith(quote) and len(ticker) > len(quote):
                base = ticker[:-len(quote)]
                return bool(base.strip())
        
        # If we can't identify the format, return True and let further validation decide
        return True
    
    def _get_alert_type(self, alert: Dict[str, Any]) -> str:
        """
        Determine the alert type from the payload.
        
        Args:
            alert: TradingView alert data
            
        Returns:
            Alert type string
        """
        # Check if explicitly specified
        if "alert_type" in alert:
            return alert["alert_type"]
        
        # Check message for keywords
        message = alert.get("message", "").lower() or alert.get("text", "").lower()
        
        if "strategy" in message:
            return AlertType.STRATEGY.value
        elif "indicator" in message:
            return AlertType.INDICATOR.value
        elif "price" in message or "close" in message:
            return AlertType.PRICE.value
        elif "volume" in message:
            return AlertType.VOLUME.value
        
        # Try to infer from other fields
        if "strategy" in alert:
            return AlertType.STRATEGY.value
        elif "study" in alert or "indicator" in alert:
            return AlertType.INDICATOR.value
        
        # Default to custom
        return AlertType.CUSTOM.value
    
    def _normalize_alert(self, alert: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize TradingView alert into a standard signal format.
        
        Args:
            alert: TradingView alert data
            
        Returns:
            Normalized signal dictionary
        """
        # Standard fields
        signal = {
            "id": alert.get("id") or str(hash(json.dumps(alert))),
            "timestamp": alert.get("timestamp") or datetime.now().isoformat(),
            "type": self._get_alert_type(alert),
            "source": "tradingview",
            "raw_data": alert
        }
        
        # Extract ticker/symbol
        ticker = None
        
        if "ticker" in alert:
            ticker = alert["ticker"]
        elif "symbol" in alert:
            ticker = alert["symbol"]
        elif "chart" in alert and "symbol" in alert["chart"]:
            ticker = alert["chart"]["symbol"]
        elif "text" in alert:
            # Try to extract from text
            text = alert["text"]
            
            # Common format: {{ticker}} or symbol={{ticker}}
            if "{{" in text and "}}" in text:
                start = text.find("{{") + 2
                end = text.find("}}", start)
                
                if start > 1 and end > start:
                    ticker = text[start:end].strip()
        
        signal["ticker"] = ticker
        
        # Extract price information
        price = None
        
        if "price" in alert:
            price = alert["price"]
        elif "close" in alert:
            price = alert["close"]
        elif "last" in alert:
            price = alert["last"]
        
        signal["price"] = price
        
        # Extract time frame
        timeframe = None
        
        if "timeframe" in alert:
            timeframe = alert["timeframe"]
        elif "interval" in alert:
            timeframe = alert["interval"]
        
        signal["timeframe"] = timeframe
        
        # Extract strategy/indicator specific information
        if signal["type"] == AlertType.STRATEGY.value:
            # Strategy alerts typically include position information
            position = None
            
            if "position" in alert:
                position = alert["position"]
            elif "action" in alert:
                position = alert["action"]
            elif "text" in alert:
                text = alert["text"].lower()
                
                if "buy" in text or "long" in text:
                    position = "long"
                elif "sell" in text or "short" in text:
                    position = "short"
                elif "exit" in text or "close" in text:
                    position = "exit"
            
            signal["position"] = position
        
        elif signal["type"] == AlertType.INDICATOR.value:
            # Indicator alerts typically include indicator values
            value = None
            
            if "value" in alert:
                value = alert["value"]
            elif "indicator" in alert and "value" in alert["indicator"]:
                value = alert["indicator"]["value"]
            
            signal["value"] = value
        
        # Extract message
        message = None
        
        if "message" in alert:
            message = alert["message"]
        elif "text" in alert:
            message = alert["text"]
            
        signal["message"] = message
        
        # Add any other fields
        for key, value in alert.items():
            if key not in signal and key != "raw_data":
                signal[key] = value
        
        return signal
    
    async def _default_signal_processor(self, signal: Dict[str, Any]) -> bool:
        """
        Default signal processor when no specific processor is registered.
        
        Args:
            signal: Signal data
            
        Returns:
            Processing success
        """
        # For the default processor, we just log the signal
        logger.info(f"Default processor handling signal for {signal.get('ticker')}: {signal.get('type')}")
        return True
    
    def _add_to_history(self, signal: Dict[str, Any]) -> None:
        """
        Add a signal to the history.
        
        Args:
            signal: Signal data
        """
        # Add timestamp if not present
        if "timestamp" not in signal:
            signal["timestamp"] = datetime.now().isoformat()
        
        # Add to history
        self.signal_history.append(signal)
        
        # Trim history if needed
        if len(self.signal_history) > self.max_history_size:
            self.signal_history = self.signal_history[-self.max_history_size:]
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get component statistics.
        
        Returns:
            Statistics dictionary
        """
        return self.stats
    
    def get_signal_history(self, limit: int = 10, signal_type: str = None) -> List[Dict[str, Any]]:
        """
        Get signal history, optionally filtered by type.
        
        Args:
            limit: Maximum number of signals to return
            signal_type: Optional signal type to filter by
            
        Returns:
            List of signal dictionaries
        """
        if signal_type:
            filtered_history = [
                signal for signal in self.signal_history
                if signal.get("type") == signal_type
            ]
        else:
            filtered_history = self.signal_history
        
        # Return most recent signals first
        return list(reversed(filtered_history[-limit:]))

def get_tradingview_webhook_handler(config: Dict[str, Any] = None, message_bus: Optional[MessageBus] = None) -> TradingViewWebhookHandler:
    """
    Get a TradingView webhook handler.
    
    Args:
        config: Configuration dictionary
        message_bus: Message bus instance
        
    Returns:
        TradingViewWebhookHandler instance
    """
    handler = TradingViewWebhookHandler(config or {}, message_bus)
    return handler 