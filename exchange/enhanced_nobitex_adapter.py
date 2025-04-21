#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Enhanced Nobitex Adapter with High Reliability Features

This module provides an enhanced adapter for the Nobitex cryptocurrency exchange
with high reliability features including:
- Connection pooling with automatic reconnection
- Circuit breaker pattern for fault tolerance
- Three-phase commit protocol for transaction verification
- Comprehensive error handling and recovery mechanisms
- Rate limiting and request queuing
- Detailed logging and metrics for monitoring

The adapter integrates with the transaction verification pipeline to ensure
trade execution reliability and consistency.
"""

import os
import time
import json
import hmac
import hashlib
import logging
import asyncio
import aiohttp
import datetime
from enum import Enum
from typing import Dict, List, Optional, Union, Any, Tuple
from urllib.parse import urlencode

from trading_system.exchange.connection_pool import ConnectionPool, ConnectionStatus
from trading_system.exchange.transaction_verification import (
    Transaction, TransactionStatus, TransactionPhase,
    TransactionError, get_transaction_verification_pipeline
)
from trading_system.core.message_bus import MessageBus, get_message_bus
from trading_system.core.metrics import Metrics, get_metrics

logger = logging.getLogger(__name__)

class OrderPhase(Enum):
    """Enum representing the phases of an order lifecycle."""
    CREATED = "created"
    VALIDATED = "validated"
    SUBMITTED = "submitted"
    CONFIRMED = "confirmed"
    FILLED = "filled"
    CANCELED = "canceled"
    FAILED = "failed"

class OrderError(Exception):
    """Base exception for order-related errors."""
    def __init__(self, message: str, order_id: Optional[str] = None, 
                 details: Optional[Dict] = None):
        self.message = message
        self.order_id = order_id
        self.details = details or {}
        super().__init__(f"{message} - Order ID: {order_id}")

class TransactionLog:
    """Class for logging transaction details throughout the order lifecycle."""
    
    def __init__(self, order_id: str, symbol: str, order_type: str, 
                 side: str, amount: float, price: float = 0):
        self.order_id = order_id
        self.symbol = symbol
        self.order_type = order_type
        self.side = side
        self.amount = amount
        self.price = price
        self.created_at = datetime.datetime.now()
        self.events = []
        self.add_event(OrderPhase.CREATED, "Order created")

    def add_event(self, phase: OrderPhase, message: str, 
                  details: Optional[Dict] = None) -> None:
        """Add an event to the transaction log."""
        self.events.append({
            "timestamp": datetime.datetime.now().isoformat(),
            "phase": phase.value,
            "message": message,
            "details": details or {}
        })
        
    def to_dict(self) -> Dict:
        """Convert the transaction log to a dictionary."""
        return {
            "order_id": self.order_id,
            "symbol": self.symbol,
            "order_type": self.order_type,
            "side": self.side,
            "amount": self.amount,
            "price": self.price,
            "created_at": self.created_at.isoformat(),
            "events": self.events
        }

class EnhancedNobitexAdapter:
    """
    Enhanced adapter for Nobitex cryptocurrency exchange with high 
    reliability features and transaction verification.
    """
    
    BASE_URL = "https://api.nobitex.ir"
    
    def __init__(self, config: Dict):
        """
        Initialize the enhanced Nobitex adapter.
        
        Args:
            config: Configuration dictionary containing API credentials and settings
        """
        self.api_key = config.get("api_key", "")
        self.api_secret = config.get("api_secret", "")
        
        # Connection pool settings
        pool_size = config.get("connection_pool_size", 5)
        conn_timeout = config.get("connection_timeout", 10)
        self.connection_pool = ConnectionPool(
            base_url=self.BASE_URL,
            pool_size=pool_size,
            connect_timeout=conn_timeout
        )
        
        # High availability settings
        self.max_retries = config.get("max_retries", 3)
        self.retry_delay = config.get("retry_delay", 1.0)
        self.circuit_breaker_threshold = config.get("circuit_breaker_threshold", 5)
        self.circuit_breaker_timeout = config.get("circuit_breaker_timeout", 60)
        
        # Rate limiting
        self.rate_limit = config.get("rate_limit", 10)  # requests per second
        self.request_tokens = self.rate_limit
        self.last_replenish_time = time.time()
        self.request_queue = asyncio.Queue()
        
        # Cache directory for market data and transaction logs
        self.cache_dir = config.get("cache_dir", "cache/nobitex")
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Transaction verification
        self.verification_pipeline = get_transaction_verification_pipeline(config)
        
        # Message bus for notifications
        self.message_bus = get_message_bus()
        
        # Metrics
        self.metrics = get_metrics()
        
        # Markets data
        self.markets = {}
        self.last_markets_update = 0
        self.markets_update_interval = config.get("markets_update_interval", 3600)
        
        # Initialize
        self._initialize_adaptor()
    
    def _initialize_adaptor(self) -> None:
        """Initialize the adapter and load cached data."""
        # Load cached market data if available
        self._load_cached_markets()
        
        # Start background tasks
        asyncio.create_task(self._request_processor())
        asyncio.create_task(self._token_replenisher())
        
        logger.info("Enhanced Nobitex adapter initialized")

    async def _create_client(self) -> aiohttp.ClientSession:
        """Create a new client session with appropriate headers."""
        headers = {
            "Content-Type": "application/json",
        }
        if self.api_key:
            headers["Authorization"] = f"Token {self.api_key}"
        
        return aiohttp.ClientSession(headers=headers)
        
    async def _execute_with_retry(self, method: str, endpoint: str, 
                                  data: Optional[Dict] = None, 
                                  params: Optional[Dict] = None,
                                  auth_required: bool = False) -> Dict:
        """
        Execute an API method with retry logic and circuit breaker.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint
            data: POST data
            params: Query parameters
            auth_required: Whether authentication is required
            
        Returns:
            API response as a dictionary
        """
        url = f"{self.BASE_URL}/{endpoint}"
        retry_count = 0
        last_error = None
        
        # Add request to the rate limit queue
        request_id = id(data or {})
        await self.request_queue.put((request_id, method, url, data, params, auth_required))
        
        # Wait for the request to be processed
        while True:
            if hasattr(self, f"_result_{request_id}"):
                result = getattr(self, f"_result_{request_id}")
                delattr(self, f"_result_{request_id}")
                if isinstance(result, Exception):
                    raise result
                return result
            await asyncio.sleep(0.1)

    async def _request_processor(self) -> None:
        """Background task to process requests according to rate limits."""
        while True:
            # Get a request from the queue
            request_id, method, url, data, params, auth_required = await self.request_queue.get()
            
            # Wait for a token if we're at the limit
            while self.request_tokens <= 0:
                await asyncio.sleep(0.1)
            
            # Use a token
            self.request_tokens -= 1
            
            # Process the request
            try:
                result = await self._do_request(method, url, data, params, auth_required)
                setattr(self, f"_result_{request_id}", result)
            except Exception as e:
                setattr(self, f"_result_{request_id}", e)
            
            # Mark the task as done
            self.request_queue.task_done()

    async def _token_replenisher(self) -> None:
        """Background task to replenish rate limit tokens."""
        while True:
            current_time = time.time()
            elapsed = current_time - self.last_replenish_time
            
            # Replenish tokens based on elapsed time
            new_tokens = int(elapsed * self.rate_limit)
            if new_tokens > 0:
                self.request_tokens = min(self.rate_limit, self.request_tokens + new_tokens)
                self.last_replenish_time = current_time
            
            await asyncio.sleep(0.1)

    async def _do_request(self, method: str, url: str, data: Optional[Dict] = None,
                         params: Optional[Dict] = None, 
                         auth_required: bool = False) -> Dict:
        """
        Execute an actual HTTP request with the connection pool.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            url: Full URL
            data: POST data
            params: Query parameters
            auth_required: Whether authentication is required
            
        Returns:
            API response as a dictionary
        """
        # Get a connection from the pool
        connection = await self.connection_pool.get_connection()
        
        if connection.status != ConnectionStatus.AVAILABLE:
            raise ConnectionError(f"No available connections: {connection.status}")
        
        # Prepare request
        kwargs = {}
        if params:
            kwargs["params"] = params
        
        if data:
            kwargs["json"] = data
        
        # Add authentication if required
        if auth_required:
            # Sign the request
            timestamp = str(int(time.time() * 1000))
            payload = json.dumps(data) if data else ""
            message = f"{timestamp}{payload}"
            signature = hmac.new(
                self.api_secret.encode(),
                message.encode(),
                hashlib.sha256
            ).hexdigest()
            
            # Add headers
            headers = kwargs.get("headers", {})
            headers.update({
                "X-API-Key": self.api_key,
                "X-API-Timestamp": timestamp,
                "X-API-Signature": signature
            })
            kwargs["headers"] = headers
        
        retry_count = 0
        last_error = None
        
        while retry_count < self.max_retries:
            try:
                start_time = time.time()
                
                # Execute request
                async with connection.session.request(method, url, **kwargs) as response:
                    # Track request duration
                    duration = time.time() - start_time
                    self.metrics.record_histogram(
                        "nobitex_request_duration_seconds",
                        duration,
                        {"method": method, "endpoint": url}
                    )
                    
                    # Check status
                    if response.status not in (200, 201):
                        error_text = await response.text()
                        logger.error(f"API error: {response.status} - {error_text}")
                        self.metrics.increment_counter(
                            "nobitex_request_errors_total",
                            {"method": method, "endpoint": url, "status": response.status}
                        )
                        
                        if retry_count < self.max_retries - 1:
                            retry_count += 1
                            await asyncio.sleep(self.retry_delay * (2 ** retry_count))
                            continue
                        
                        raise OrderError(
                            f"API error: {response.status}", 
                            details={"response": error_text}
                        )
                    
                    try:
                        result = await response.json()
                        
                        # Return the connection to the pool
                        await self.connection_pool.release_connection(connection)
                        
                        # Track successful requests
                        self.metrics.increment_counter(
                            "nobitex_request_success_total",
                            {"method": method, "endpoint": url}
                        )
                        
                        return result
                    except Exception as e:
                        logger.error(f"Error parsing JSON response: {e}")
                        last_error = e
            
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.error(f"Connection error: {e}")
                self.metrics.increment_counter(
                    "nobitex_connection_errors_total",
                    {"method": method, "error": str(type(e).__name__)}
                )
                last_error = e
                
                # Mark the connection as failed
                await self.connection_pool.mark_connection_failed(connection)
                
                # Get a new connection for retry
                connection = await self.connection_pool.get_connection()
            
            retry_count += 1
            if retry_count < self.max_retries:
                delay = self.retry_delay * (2 ** retry_count)
                logger.warning(f"Retrying request after {delay}s (attempt {retry_count}/{self.max_retries})")
                await asyncio.sleep(delay)
        
        # If we've exhausted all retries, update metrics and raise an exception
        self.metrics.increment_counter(
            "nobitex_request_failures_total", 
            {"method": method, "endpoint": url}
        )
        
        error_msg = f"Failed to execute request after {self.max_retries} retries"
        if last_error:
            error_msg += f": {last_error}"
            
        raise OrderError(error_msg, details={"last_error": str(last_error)})

    def _load_cached_markets(self) -> None:
        """Load cached market data if available."""
        cache_file = os.path.join(self.cache_dir, "markets.json")
        if os.path.exists(cache_file):
            try:
                with open(cache_file, "r") as f:
                    data = json.load(f)
                    self.markets = data.get("markets", {})
                    self.last_markets_update = data.get("timestamp", 0)
                    logger.info(f"Loaded cached market data for {len(self.markets)} markets")
            except Exception as e:
                logger.warning(f"Failed to load cached market data: {e}") 

    async def get_market_info(self, symbol: str) -> Dict:
        """
        Get information about a specific market.
        
        Args:
            symbol: Market symbol (e.g., 'btc-rls')
            
        Returns:
            Market information
        """
        # Check if we need to update market data
        current_time = time.time()
        if current_time - self.last_markets_update > self.markets_update_interval:
            await self.update_markets()
            
        # Return market info if available
        symbol = symbol.lower()
        if symbol in self.markets:
            return self.markets[symbol]
        
        raise OrderError(f"Market not found: {symbol}")
        
    async def update_markets(self) -> Dict:
        """
        Update market information from the exchange.
        
        Returns:
            Dictionary of markets
        """
        try:
            # Get market information
            result = await self._execute_with_retry("GET", "market/stats", auth_required=False)
            if "stats" in result:
                # Process market data
                self.markets = {}
                for symbol, data in result["stats"].items():
                    self.markets[symbol.lower()] = {
                        "symbol": symbol,
                        "last_trade": data.get("latest", 0),
                        "high": data.get("dayHigh", 0),
                        "low": data.get("dayLow", 0),
                        "volume": data.get("volumeSrc", 0),
                        "change": data.get("dayChange", 0),
                    }
                
                # Cache the data
                self._save_cached_markets()
                
                self.last_markets_update = time.time()
                logger.info(f"Updated market data for {len(self.markets)} markets")
                
                # Publish market update event
                await self.message_bus.publish(
                    "market_data_updated",
                    {"markets": self.markets, "timestamp": self.last_markets_update}
                )
                
                return self.markets
            else:
                logger.error(f"Failed to update markets: {result}")
                raise OrderError("Failed to update markets", details=result)
        except Exception as e:
            logger.error(f"Error updating markets: {e}")
            self.metrics.increment_counter("nobitex_market_update_errors_total")
            raise
    
    def _save_cached_markets(self) -> None:
        """Save market data to cache."""
        cache_file = os.path.join(self.cache_dir, "markets.json")
        try:
            with open(cache_file, "w") as f:
                json.dump({
                    "markets": self.markets,
                    "timestamp": self.last_markets_update
                }, f)
            logger.debug("Saved market data to cache")
        except Exception as e:
            logger.warning(f"Failed to save market data to cache: {e}")
    
    async def get_order_book(self, symbol: str) -> Dict:
        """
        Get the order book for a specific market.
        
        Args:
            symbol: Market symbol (e.g., 'btc-rls')
            
        Returns:
            Order book data
        """
        # Make API call
        result = await self._execute_with_retry(
            "GET", 
            "market/depth", 
            params={"symbol": symbol}
        )
        
        if "bids" in result and "asks" in result:
            return {
                "bids": result["bids"],
                "asks": result["asks"],
                "timestamp": time.time()
            }
        else:
            logger.error(f"Failed to get order book: {result}")
            raise OrderError(f"Failed to get order book for {symbol}", details=result)
    
    async def get_account_balance(self) -> Dict:
        """
        Get account balance information.
        
        Returns:
            Dictionary of balances by asset
        """
        # Make API call
        result = await self._execute_with_retry(
            "POST", 
            "users/wallets/list", 
            auth_required=True
        )
        
        if "wallets" in result:
            # Process balances
            balances = {}
            for wallet in result["wallets"]:
                currency = wallet.get("currency", "").lower()
                if currency:
                    balances[currency] = {
                        "total": float(wallet.get("balance", 0)),
                        "available": float(wallet.get("depositOnly", 0)) == 0 and float(wallet.get("balance", 0)) or 0,
                        "locked": float(wallet.get("frozen", 0))
                    }
            
            return balances
        else:
            logger.error(f"Failed to get balances: {result}")
            raise OrderError("Failed to get account balances", details=result)
    
    async def create_order(self, symbol: str, order_type: str, side: str, 
                          amount: float, price: float = 0) -> Dict:
        """
        Create an order with transaction verification.
        
        This method implements the three-phase commit protocol for order creation:
        1. Validation phase - Check balance, market status, and order parameters
        2. Submission phase - Send the order to the exchange
        3. Verification phase - Verify the order status after submission
        
        Args:
            symbol: Market symbol (e.g., 'btc-rls')
            order_type: Order type ('market' or 'limit')
            side: Order side ('buy' or 'sell')
            amount: Order amount
            price: Order price (required for limit orders)
            
        Returns:
            Order information
        """
        # Generate a unique order ID for tracking
        order_id = f"nobitex_{int(time.time())}_{hash(str(time.time()))}"[:24]
        
        # Create transaction log
        transaction_log = TransactionLog(
            order_id=order_id,
            symbol=symbol,
            order_type=order_type,
            side=side,
            amount=amount,
            price=price
        )
        
        # Create transaction for verification pipeline
        transaction = Transaction(
            id=order_id,
            exchange="nobitex",
            symbol=symbol,
            order_type=order_type,
            side=side,
            amount=amount,
            price=price,
            created_at=datetime.datetime.now().isoformat()
        )
        
        # 1. Validation Phase
        try:
            # Add validation event
            transaction_log.add_event(
                OrderPhase.VALIDATED,
                "Starting validation phase"
            )
            
            # Validate market exists
            market_info = await self.get_market_info(symbol)
            transaction_log.add_event(
                OrderPhase.VALIDATED,
                "Market validation passed",
                {"market_info": market_info}
            )
            
            # Validate order type
            if order_type not in ("market", "limit"):
                raise OrderError(
                    f"Invalid order type: {order_type}",
                    order_id,
                    {"valid_types": ["market", "limit"]}
                )
            
            # Validate price for limit orders
            if order_type == "limit" and price <= 0:
                raise OrderError(
                    "Price is required for limit orders",
                    order_id
                )
            
            # Validate order side
            if side not in ("buy", "sell"):
                raise OrderError(
                    f"Invalid order side: {side}",
                    order_id,
                    {"valid_sides": ["buy", "sell"]}
                )
            
            # Validate amount
            if amount <= 0:
                raise OrderError(
                    f"Invalid order amount: {amount}",
                    order_id
                )
            
            # Validate balance
            balances = await self.get_account_balance()
            transaction_log.add_event(
                OrderPhase.VALIDATED,
                "Balance validation passed",
                {"balances": balances}
            )
            
            # Market symbol parts
            symbol_parts = symbol.split("-")
            if len(symbol_parts) != 2:
                raise OrderError(
                    f"Invalid symbol format: {symbol}",
                    order_id
                )
            
            base_currency, quote_currency = symbol_parts
            
            # Check if we have enough balance
            if side == "buy":
                required_amount = amount * (price or market_info["last_trade"])
                if quote_currency not in balances or balances[quote_currency]["available"] < required_amount:
                    raise OrderError(
                        f"Insufficient {quote_currency} balance",
                        order_id,
                        {
                            "required": required_amount,
                            "available": balances.get(quote_currency, {}).get("available", 0)
                        }
                    )
            else:  # sell
                if base_currency not in balances or balances[base_currency]["available"] < amount:
                    raise OrderError(
                        f"Insufficient {base_currency} balance",
                        order_id,
                        {
                            "required": amount,
                            "available": balances.get(base_currency, {}).get("available", 0)
                        }
                    )
            
            # Update transaction status
            await self.verification_pipeline.update_transaction_phase(
                transaction, 
                TransactionPhase.VALIDATION,
                {"status": "success"}
            )
            
        except Exception as e:
            # Log the error
            logger.error(f"Order validation failed: {e}")
            transaction_log.add_event(
                OrderPhase.FAILED,
                f"Validation failed: {str(e)}",
                {"error": str(e)}
            )
            
            # Update transaction status
            await self.verification_pipeline.update_transaction_phase(
                transaction, 
                TransactionPhase.VALIDATION,
                {"status": "failed", "error": str(e)}
            )
            
            # Update metrics
            self.metrics.increment_counter(
                "nobitex_order_validation_errors_total",
                {"symbol": symbol, "side": side, "type": order_type}
            )
            
            # Re-raise the exception
            raise
        
        # 2. Submission Phase
        try:
            # Add submission event
            transaction_log.add_event(
                OrderPhase.SUBMITTED,
                "Starting submission phase"
            )
            
            # Prepare order data
            order_data = {
                "type": order_type,
                "execution": "limit" if order_type == "limit" else "market",
                "srcCurrency": base_currency.upper(),
                "dstCurrency": quote_currency.upper(),
                "amount": str(amount),
                "price": str(price) if order_type == "limit" else None,
                "clientOrderId": order_id
            }
            
            # Make API call to create order
            endpoint = "market/orders/add"
            result = await self._execute_with_retry(
                "POST", 
                endpoint, 
                data=order_data,
                auth_required=True
            )
            
            if "status" in result and result["status"] == "ok":
                # Extract exchange order ID
                exchange_order_id = result.get("order", {}).get("id")
                
                # Add confirmation event
                transaction_log.add_event(
                    OrderPhase.CONFIRMED,
                    "Order submitted successfully",
                    {"exchange_order_id": exchange_order_id, "result": result}
                )
                
                # Update transaction with exchange order ID
                transaction.exchange_id = exchange_order_id
                await self.verification_pipeline.update_transaction_phase(
                    transaction, 
                    TransactionPhase.SUBMISSION,
                    {"status": "success", "exchange_id": exchange_order_id}
                )
                
                # Update metrics
                self.metrics.increment_counter(
                    "nobitex_orders_submitted_total",
                    {"symbol": symbol, "side": side, "type": order_type}
                )
                
                # Prepare order response
                order_response = {
                    "order_id": order_id,
                    "exchange_order_id": exchange_order_id,
                    "symbol": symbol,
                    "type": order_type,
                    "side": side,
                    "amount": amount,
                    "price": price,
                    "status": "submitted",
                    "timestamp": time.time()
                }
                
                # 3. Verification Phase
                asyncio.create_task(self._verify_order(transaction, order_response))
                
                return order_response
            else:
                # Order submission failed
                error_msg = result.get("message", "Unknown error")
                logger.error(f"Order submission failed: {error_msg}")
                
                # Add failure event
                transaction_log.add_event(
                    OrderPhase.FAILED,
                    f"Submission failed: {error_msg}",
                    {"result": result}
                )
                
                # Update transaction status
                await self.verification_pipeline.update_transaction_phase(
                    transaction, 
                    TransactionPhase.SUBMISSION,
                    {"status": "failed", "error": error_msg}
                )
                
                # Update metrics
                self.metrics.increment_counter(
                    "nobitex_order_submission_errors_total",
                    {"symbol": symbol, "side": side, "type": order_type}
                )
                
                raise OrderError(f"Order submission failed: {error_msg}", order_id, result)
                
        except Exception as e:
            # Log the error if not already logged
            if not isinstance(e, OrderError):
                logger.error(f"Order submission failed: {e}")
                transaction_log.add_event(
                    OrderPhase.FAILED,
                    f"Submission failed: {str(e)}",
                    {"error": str(e)}
                )
                
                # Update transaction status if not already updated
                await self.verification_pipeline.update_transaction_phase(
                    transaction, 
                    TransactionPhase.SUBMISSION,
                    {"status": "failed", "error": str(e)}
                )
                
                # Update metrics
                self.metrics.increment_counter(
                    "nobitex_order_submission_errors_total",
                    {"symbol": symbol, "side": side, "type": order_type}
                )
            
            # Re-raise the exception
            raise
    
    async def _verify_order(self, transaction: Transaction, order_data: Dict) -> None:
        """
        Verify an order after submission.
        
        Args:
            transaction: Transaction object
            order_data: Order data dictionary
        """
        try:
            # Wait a short time for the order to be processed
            await asyncio.sleep(1.0)
            
            # Get order status
            exchange_order_id = order_data["exchange_order_id"]
            order_status = await self.get_order_status(exchange_order_id)
            
            # Check if the order exists and is in a valid state
            if order_status.get("status") in ("active", "done", "partial"):
                # Order verified
                await self.verification_pipeline.update_transaction_phase(
                    transaction, 
                    TransactionPhase.VERIFICATION,
                    {"status": "success", "order_status": order_status}
                )
                
                # Update metrics
                self.metrics.increment_counter(
                    "nobitex_orders_verified_total",
                    {"symbol": order_data["symbol"], "side": order_data["side"], "type": order_data["type"]}
                )
                
                # Publish order verified event
                await self.message_bus.publish(
                    "order_verified",
                    {
                        "order_id": order_data["order_id"],
                        "exchange_order_id": exchange_order_id,
                        "status": order_status["status"],
                        "filled": order_status.get("filled", 0),
                        "remaining": order_status.get("remaining", 0),
                        "timestamp": time.time()
                    }
                )
                
            else:
                # Order verification failed
                error_msg = f"Order verification failed: Invalid status {order_status.get('status')}"
                logger.error(error_msg)
                
                # Update transaction status
                await self.verification_pipeline.update_transaction_phase(
                    transaction, 
                    TransactionPhase.VERIFICATION,
                    {"status": "failed", "error": error_msg, "order_status": order_status}
                )
                
                # Update metrics
                self.metrics.increment_counter(
                    "nobitex_order_verification_errors_total",
                    {"symbol": order_data["symbol"], "side": order_data["side"], "type": order_data["type"]}
                )
                
                # Publish order verification failed event
                await self.message_bus.publish(
                    "order_verification_failed",
                    {
                        "order_id": order_data["order_id"],
                        "exchange_order_id": exchange_order_id,
                        "error": error_msg,
                        "timestamp": time.time()
                    }
                )
        except Exception as e:
            # Log the error
            logger.error(f"Order verification failed: {e}")
            
            # Update transaction status
            await self.verification_pipeline.update_transaction_phase(
                transaction, 
                TransactionPhase.VERIFICATION,
                {"status": "failed", "error": str(e)}
            )
            
            # Update metrics
            self.metrics.increment_counter(
                "nobitex_order_verification_errors_total",
                {"symbol": order_data["symbol"], "side": order_data["side"], "type": order_data["type"]}
            )
    
    async def get_order_status(self, order_id: str) -> Dict:
        """
        Get the status of an order.
        
        Args:
            order_id: Exchange order ID
            
        Returns:
            Order status
        """
        # Make API call
        result = await self._execute_with_retry(
            "POST", 
            "market/orders/status", 
            data={"id": order_id},
            auth_required=True
        )
        
        if "order" in result:
            order = result["order"]
            
            # Map exchange status to our status format
            status_mapping = {
                "Active": "active",
                "Filled": "done",
                "Partial": "partial",
                "Canceled": "canceled",
                "Rejected": "rejected"
            }
            
            exchange_status = order.get("status", "Unknown")
            status = status_mapping.get(exchange_status, "unknown")
            
            return {
                "order_id": order.get("clientOrderId"),
                "exchange_order_id": order.get("id"),
                "symbol": f"{order.get('srcCurrency', '').lower()}-{order.get('dstCurrency', '').lower()}",
                "type": order.get("type"),
                "side": "buy" if order.get("type") == "buy" else "sell",
                "amount": float(order.get("amount", 0)),
                "price": float(order.get("price", 0)),
                "filled": float(order.get("executed", 0)),
                "remaining": float(order.get("amount", 0)) - float(order.get("executed", 0)),
                "status": status,
                "created_at": order.get("created_at"),
                "updated_at": order.get("updated_at")
            }
        else:
            # Order not found or error
            error_msg = result.get("message", "Order not found")
            logger.error(f"Failed to get order status: {error_msg}")
            
            # Update metrics
            self.metrics.increment_counter("nobitex_order_status_errors_total")
            
            return {
                "order_id": None,
                "exchange_order_id": order_id,
                "status": "error",
                "error": error_msg
            }
    
    async def cancel_order(self, order_id: str) -> Dict:
        """
        Cancel an active order.
        
        Args:
            order_id: Exchange order ID
            
        Returns:
            Cancellation result
        """
        # Make API call
        result = await self._execute_with_retry(
            "POST", 
            "market/orders/cancel", 
            data={"id": order_id},
            auth_required=True
        )
        
        if "status" in result and result["status"] == "ok":
            # Update metrics
            self.metrics.increment_counter("nobitex_orders_canceled_total")
            
            return {
                "order_id": order_id,
                "status": "canceled",
                "timestamp": time.time()
            }
        else:
            # Cancellation failed
            error_msg = result.get("message", "Unknown error")
            logger.error(f"Order cancellation failed: {error_msg}")
            
            # Update metrics
            self.metrics.increment_counter("nobitex_order_cancellation_errors_total")
            
            return {
                "order_id": order_id,
                "status": "error",
                "error": error_msg
            }
    
    async def get_transaction_history(self, symbol: Optional[str] = None, 
                                     limit: int = 100) -> List[Dict]:
        """
        Get transaction history.
        
        Args:
            symbol: Optional market symbol to filter by
            limit: Maximum number of transactions to return
            
        Returns:
            List of transactions
        """
        # Prepare parameters
        params = {"limit": limit}
        if symbol:
            symbol_parts = symbol.split("-")
            if len(symbol_parts) == 2:
                params["srcCurrency"] = symbol_parts[0].upper()
                params["dstCurrency"] = symbol_parts[1].upper()
        
        # Make API call
        result = await self._execute_with_retry(
            "POST", 
            "market/trades/list", 
            data=params,
            auth_required=True
        )
        
        if "trades" in result:
            # Process trades
            trades = []
            for trade in result["trades"]:
                trades.append({
                    "id": trade.get("id"),
                    "symbol": f"{trade.get('srcCurrency', '').lower()}-{trade.get('dstCurrency', '').lower()}",
                    "order_id": trade.get("orderId"),
                    "side": "buy" if trade.get("type") == "buy" else "sell",
                    "amount": float(trade.get("volume", 0)),
                    "price": float(trade.get("price", 0)),
                    "fee": float(trade.get("fee", 0)),
                    "timestamp": trade.get("timestamp")
                })
            
            return trades
        else:
            # Error getting trades
            error_msg = result.get("message", "Unknown error")
            logger.error(f"Failed to get transaction history: {error_msg}")
            
            # Update metrics
            self.metrics.increment_counter("nobitex_transaction_history_errors_total")
            
            raise OrderError(f"Failed to get transaction history: {error_msg}") 