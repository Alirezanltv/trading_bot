"""
TradingView webhook handler for receiving trading signals.

This module provides a webhook server to receive TradingView alerts and convert them
into trading signals. It allows the system to react to TradingView chart events.
"""

import json
import asyncio
import logging
import time
import hmac
import hashlib
import base64
from typing import Dict, Any, List, Optional, Union, Callable
from datetime import datetime
from aiohttp import web

from trading_system.core import Component, ComponentStatus, get_logger
from trading_system.core import message_bus, MessageTypes, database_manager

logger = get_logger("market_data.tradingview_webhook")

class TradingViewWebhookHandler(Component):
    """
    TradingView webhook handler for receiving trading signals.
    
    Provides a web server to handle webhook requests from TradingView alerts.
    """
    
    def __init__(self, host: str = "0.0.0.0", port: int = 8080, 
                 webhook_path: str = "/webhook/tradingview",
                 api_key: str = None):
        """
        Initialize the TradingView webhook handler.
        
        Args:
            host: Webhook server host
            port: Webhook server port
            webhook_path: Path for the webhook endpoint
            api_key: Secret key for webhook authentication
        """
        super().__init__(name="TradingViewWebhook")
        
        self.host = host
        self.port = port
        self.webhook_path = webhook_path
        self.api_key = api_key
        
        # Server
        self.app = None
        self.runner = None
        self.site = None
        
        # Signal handlers
        self.signal_handlers: Dict[str, List[Callable]] = {}
        
        # Statistics
        self.stats = {
            "total_requests": 0,
            "valid_requests": 0,
            "invalid_requests": 0,
            "last_request_time": None
        }
    
    async def _handle_webhook(self, request: web.Request) -> web.Response:
        """
        Handle webhook request from TradingView.
        
        Args:
            request: HTTP request
        
        Returns:
            HTTP response
        """
        try:
            # Update stats
            self.stats["total_requests"] += 1
            self.stats["last_request_time"] = datetime.now().isoformat()
            
            # Check authentication
            if self.api_key:
                auth_header = request.headers.get("X-TradingView-Key")
                if not auth_header or auth_header != self.api_key:
                    logger.warning("Invalid authentication for webhook request")
                    self.stats["invalid_requests"] += 1
                    return web.Response(text="Unauthorized", status=401)
            
            # Parse request body
            try:
                payload = await request.json()
            except json.JSONDecodeError:
                logger.warning("Invalid JSON payload in webhook request")
                self.stats["invalid_requests"] += 1
                return web.Response(text="Invalid JSON", status=400)
            
            # Process signal
            asyncio.create_task(self._process_signal(payload))
            
            # Update stats
            self.stats["valid_requests"] += 1
            
            # Send success response
            return web.Response(text="OK")
            
        except Exception as e:
            logger.error(f"Error handling webhook request: {str(e)}", exc_info=True)
            self.stats["invalid_requests"] += 1
            return web.Response(text="Internal Server Error", status=500)
    
    async def _process_signal(self, payload: Dict[str, Any]) -> None:
        """
        Process a trading signal from TradingView.
        
        Args:
            payload: Signal payload
        """
        try:
            logger.info(f"Received TradingView signal: {json.dumps(payload)}")
            
            # Extract signal information
            signal_type = payload.get("strategy", {}).get("action", "unknown")
            symbol = payload.get("ticker", "")
            price = payload.get("price", 0.0)
            timestamp = payload.get("time", datetime.now().timestamp())
            
            # Additional information
            strategy_name = payload.get("strategy", {}).get("name", "unknown")
            interval = payload.get("interval", "")
            exchange = payload.get("exchange", "")
            
            # Create normalized signal
            signal = {
                "source": "tradingview",
                "type": signal_type,
                "symbol": symbol,
                "price": price,
                "timestamp": timestamp,
                "strategy": strategy_name,
                "interval": interval,
                "exchange": exchange,
                "raw_data": payload
            }
            
            # Store signal in database
            if database_manager.is_connected():
                await database_manager.insert_one("signals", signal)
            
            # Publish signal to message bus
            await message_bus.publish(
                topic=f"signal.tradingview.{signal_type}",
                message={
                    "type": MessageTypes.SIGNAL,
                    "data": signal
                }
            )
            
            # Invoke signal handlers
            if signal_type in self.signal_handlers:
                for handler in self.signal_handlers[signal_type]:
                    try:
                        asyncio.create_task(handler(signal))
                    except Exception as e:
                        logger.error(f"Error in signal handler: {str(e)}", exc_info=True)
            
            logger.info(f"Processed TradingView signal: {signal_type} for {symbol}")
            
        except Exception as e:
            logger.error(f"Error processing TradingView signal: {str(e)}", exc_info=True)
    
    def register_signal_handler(self, signal_type: str, handler: Callable) -> None:
        """
        Register a handler for a specific signal type.
        
        Args:
            signal_type: Signal type (e.g., "buy", "sell", "alert")
            handler: Signal handler function
        """
        if signal_type not in self.signal_handlers:
            self.signal_handlers[signal_type] = []
        
        self.signal_handlers[signal_type].append(handler)
        logger.info(f"Registered handler for TradingView signal type: {signal_type}")
    
    def unregister_signal_handler(self, signal_type: str, handler: Callable) -> bool:
        """
        Unregister a signal handler.
        
        Args:
            signal_type: Signal type
            handler: Signal handler function
            
        Returns:
            Success status
        """
        if signal_type not in self.signal_handlers:
            return False
        
        try:
            self.signal_handlers[signal_type].remove(handler)
            logger.info(f"Unregistered handler for TradingView signal type: {signal_type}")
            return True
        except ValueError:
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get webhook handler statistics.
        
        Returns:
            Statistics dictionary
        """
        return self.stats
    
    async def initialize(self) -> bool:
        """
        Initialize the webhook handler.
        
        Returns:
            Success status
        """
        try:
            # Create web application
            self.app = web.Application()
            
            # Configure routes
            self.app.router.add_post(self.webhook_path, self._handle_webhook)
            
            # Create runner
            self.runner = web.AppRunner(self.app)
            await self.runner.setup()
            
            # Set component status
            self.status = ComponentStatus.INITIALIZED
            logger.info("TradingView webhook handler initialized")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing TradingView webhook handler: {str(e)}", exc_info=True)
            return False
    
    async def start(self) -> bool:
        """
        Start the webhook handler.
        
        Returns:
            Success status
        """
        try:
            # Start server
            self.site = web.TCPSite(self.runner, self.host, self.port)
            await self.site.start()
            
            # Set component status
            self.status = ComponentStatus.RUNNING
            logger.info(f"TradingView webhook handler started on http://{self.host}:{self.port}{self.webhook_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error starting TradingView webhook handler: {str(e)}", exc_info=True)
            return False
    
    async def stop(self) -> bool:
        """
        Stop the webhook handler.
        
        Returns:
            Success status
        """
        try:
            # Stop server
            if self.site:
                await self.site.stop()
            
            # Clean up runner
            if self.runner:
                await self.runner.cleanup()
            
            # Reset resources
            self.app = None
            self.runner = None
            self.site = None
            
            # Set component status
            self.status = ComponentStatus.STOPPED
            logger.info("TradingView webhook handler stopped")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping TradingView webhook handler: {str(e)}", exc_info=True)
            return False

# Singleton instance
webhook_handler = TradingViewWebhookHandler() 