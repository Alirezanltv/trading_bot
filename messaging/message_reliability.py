"""
Message Reliability Components

This module provides components for enhancing message reliability:
- Dead-letter queue handling
- Message retry with exponential backoff
- Message persistence tracking

These components work with the RabbitMQ adapter to provide a highly reliable
messaging infrastructure for critical trading operations.
"""

import time
import json
import random
import logging
import asyncio
import uuid
from enum import Enum
from typing import Dict, Any, List, Optional, Set, Union, Callable, Tuple
from datetime import datetime, timedelta

from trading_system.core.logging import get_logger

logger = get_logger("messaging.reliability")

class MessageStatus(Enum):
    """Message status enumeration."""
    PENDING = "pending"
    DELIVERED = "delivered"
    FAILED = "failed"
    REDELIVERED = "redelivered"
    DEAD_LETTERED = "dead_lettered"
    ACKNOWLEDGED = "acknowledged"


class RetryStrategy(Enum):
    """Message retry strategy enumeration."""
    NONE = "none"  # No retry
    FIXED = "fixed"  # Fixed retry interval
    EXPONENTIAL = "exponential"  # Exponential backoff
    LINEAR = "linear"  # Linear backoff


class DeadLetterHandler:
    """
    Dead-letter message handler.
    
    This class handles messages that have failed processing and been sent to
    a dead-letter queue. It provides mechanisms for:
    - Routing messages to appropriate handlers
    - Retry processing with different strategies
    - Logging and monitoring of dead-lettered messages
    """
    
    def __init__(self, retry_strategy: RetryStrategy = RetryStrategy.EXPONENTIAL, 
                max_retries: int = 3, initial_delay: float = 1.0):
        """
        Initialize dead-letter handler.
        
        Args:
            retry_strategy: Retry strategy to use
            max_retries: Maximum number of retry attempts
            initial_delay: Initial delay between retries in seconds
        """
        self.retry_strategy = retry_strategy
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        
        # Handlers for different message types
        self.handlers: Dict[str, Callable] = {}
        
        # Statistics for monitoring
        self.stats = {
            "processed": 0,
            "retried": 0,
            "failed": 0,
            "succeeded": 0
        }
        
        logger.info(f"Dead-letter handler initialized with {retry_strategy.value} retry strategy")
    
    def register_handler(self, routing_key: str, handler: Callable) -> None:
        """
        Register a handler for a specific routing key pattern.
        
        Args:
            routing_key: Routing key pattern to match
            handler: Handler function to call for matching messages
        """
        self.handlers[routing_key] = handler
        logger.info(f"Registered dead-letter handler for routing key: {routing_key}")
    
    async def handle_message(self, message: Dict[str, Any], headers: Dict[str, Any]) -> bool:
        """
        Handle a dead-lettered message.
        
        Args:
            message: Message content
            headers: Message headers
            
        Returns:
            bool: True if message was handled successfully, False otherwise
        """
        routing_key = headers.get("x-original-routing-key", "unknown")
        exchange = headers.get("x-original-exchange", "unknown")
        reason = headers.get("x-death", [{}])[0].get("reason", "unknown")
        count = headers.get("x-death", [{}])[0].get("count", 0)
        
        logger.info(f"Handling dead-lettered message from {exchange}/{routing_key} (reason: {reason}, count: {count})")
        
        self.stats["processed"] += 1
        
        # Check if we've exceeded the retry limit
        if count > self.max_retries:
            logger.warning(f"Message exceeded retry limit ({self.max_retries}): {routing_key}")
            self.stats["failed"] += 1
            return await self._handle_permanent_failure(message, headers)
        
        # Find a handler for this routing key
        handler = self._find_handler(routing_key)
        if not handler:
            logger.warning(f"No handler found for routing key: {routing_key}")
            self.stats["failed"] += 1
            return False
        
        # Calculate retry delay based on strategy
        delay = self._calculate_retry_delay(count)
        
        # Wait before retrying
        if delay > 0:
            logger.info(f"Waiting {delay:.2f}s before retry {count} for {routing_key}")
            await asyncio.sleep(delay)
        
        # Retry processing
        try:
            self.stats["retried"] += 1
            result = await handler(message, headers)
            
            if result:
                logger.info(f"Successfully reprocessed message: {routing_key}")
                self.stats["succeeded"] += 1
            else:
                logger.warning(f"Failed to reprocess message: {routing_key}")
                self.stats["failed"] += 1
                
            return result
            
        except Exception as e:
            logger.error(f"Error reprocessing message {routing_key}: {str(e)}", exc_info=True)
            self.stats["failed"] += 1
            return False
    
    def _find_handler(self, routing_key: str) -> Optional[Callable]:
        """
        Find an appropriate handler for a routing key.
        
        Args:
            routing_key: Routing key to match
            
        Returns:
            Callable: Handler function or None if no match found
        """
        for pattern, handler in self.handlers.items():
            # Simple wildcard matching
            if pattern == "#" or pattern == routing_key:
                return handler
            
            # Handle wildcards
            if "#" in pattern or "*" in pattern:
                pattern_parts = pattern.split(".")
                key_parts = routing_key.split(".")
                
                if self._match_pattern(pattern_parts, key_parts):
                    return handler
                    
        return None
    
    def _match_pattern(self, pattern_parts: List[str], key_parts: List[str]) -> bool:
        """
        Match a routing key pattern against a specific key.
        
        Args:
            pattern_parts: Parts of the pattern, split by '.'
            key_parts: Parts of the routing key, split by '.'
            
        Returns:
            bool: True if pattern matches key, False otherwise
        """
        # Handle # wildcard (matches zero or more words)
        if len(pattern_parts) == 1 and pattern_parts[0] == "#":
            return True
            
        if len(pattern_parts) > len(key_parts) and "#" not in pattern_parts:
            return False
            
        for i, p_part in enumerate(pattern_parts):
            # Handle end of key parts
            if i >= len(key_parts):
                # Only valid if remaining pattern is #
                return p_part == "#"
                
            k_part = key_parts[i]
            
            # Exact match
            if p_part == k_part:
                continue
                
            # * wildcard (matches exactly one word)
            if p_part == "*":
                continue
                
            # # wildcard (matches zero or more words)
            if p_part == "#":
                return True
                
            # No match
            return False
            
        # If we've matched all pattern parts, we're good
        return len(pattern_parts) == len(key_parts)
    
    def _calculate_retry_delay(self, retry_count: int) -> float:
        """
        Calculate delay before retry based on strategy.
        
        Args:
            retry_count: Current retry attempt count
            
        Returns:
            float: Delay in seconds
        """
        if self.retry_strategy == RetryStrategy.NONE:
            return 0.0
            
        if self.retry_strategy == RetryStrategy.FIXED:
            return self.initial_delay
            
        if self.retry_strategy == RetryStrategy.EXPONENTIAL:
            # Exponential backoff with jitter
            delay = self.initial_delay * (2 ** (retry_count - 1))
            # Add some jitter (±15%)
            jitter = delay * 0.3 * (0.5 - (random.random()))
            return max(0, delay + jitter)
            
        if self.retry_strategy == RetryStrategy.LINEAR:
            # Linear backoff
            return self.initial_delay * retry_count
            
        return self.initial_delay  # Default
    
    async def _handle_permanent_failure(self, message: Dict[str, Any], headers: Dict[str, Any]) -> bool:
        """
        Handle a message that has permanently failed.
        
        Args:
            message: Message content
            headers: Message headers
            
        Returns:
            bool: True if handled, False otherwise
        """
        routing_key = headers.get("x-original-routing-key", "unknown")
        
        # Log the permanent failure
        logger.error(f"Permanent failure for message {routing_key}: {message}")
        
        # TODO: Add permanent failure handling (e.g., write to DB, alert)
        
        return False
    
    def get_stats(self) -> Dict[str, int]:
        """
        Get handler statistics.
        
        Returns:
            Dict[str, int]: Handler statistics
        """
        return self.stats.copy()


class MessageTracker:
    """
    Message delivery tracking.
    
    This class provides tracking for message delivery status to ensure
    reliable delivery and enable recovery from failures.
    """
    
    def __init__(self, expiry_time: int = 86400):
        """
        Initialize message tracker.
        
        Args:
            expiry_time: Time in seconds to keep message records
        """
        self.messages: Dict[str, Dict[str, Any]] = {}
        self.expiry_time = expiry_time
        
        # Statistics
        self.stats = {
            "published": 0,
            "acknowledged": 0,
            "failed": 0,
            "in_flight": 0
        }
        
        # Start cleanup task
        self.cleanup_task = asyncio.create_task(self._cleanup_loop())
        
        logger.info(f"Message tracker initialized with {expiry_time}s expiry time")
    
    def track_message(self, message_id: str, exchange: str, routing_key: str, 
                     message_data: Any) -> Dict[str, Any]:
        """
        Track a new message.
        
        Args:
            message_id: Unique message ID
            exchange: Exchange the message was published to
            routing_key: Routing key used for the message
            message_data: Original message data
            
        Returns:
            Dict[str, Any]: Message tracking record
        """
        now = int(time.time())
        
        message_record = {
            "id": message_id,
            "exchange": exchange,
            "routing_key": routing_key,
            "status": MessageStatus.PENDING.value,
            "published_at": now,
            "updated_at": now,
            "expires_at": now + self.expiry_time,
            "acknowledged_at": None,
            "retry_count": 0,
            "data": message_data
        }
        
        self.messages[message_id] = message_record
        self.stats["published"] += 1
        self.stats["in_flight"] += 1
        
        return message_record
    
    def update_status(self, message_id: str, status: MessageStatus, 
                     metadata: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Update status for a tracked message.
        
        Args:
            message_id: Message ID
            status: New message status
            metadata: Additional metadata to store
            
        Returns:
            Dict[str, Any]: Updated message record or None if not found
        """
        if message_id not in self.messages:
            return None
            
        message = self.messages[message_id]
        
        # Update status and timestamp
        message["status"] = status.value
        message["updated_at"] = int(time.time())
        
        # Update acknowledgment time if relevant
        if status == MessageStatus.ACKNOWLEDGED:
            message["acknowledged_at"] = int(time.time())
            self.stats["acknowledged"] += 1
            self.stats["in_flight"] -= 1
            
        # Track failures
        if status == MessageStatus.FAILED or status == MessageStatus.DEAD_LETTERED:
            self.stats["failed"] += 1
            self.stats["in_flight"] -= 1
            
        # Store additional metadata
        if metadata:
            if "metadata" not in message:
                message["metadata"] = {}
            message["metadata"].update(metadata)
            
        return message
    
    def increment_retry(self, message_id: str) -> Optional[int]:
        """
        Increment retry count for a message.
        
        Args:
            message_id: Message ID
            
        Returns:
            int: New retry count or None if message not found
        """
        if message_id not in self.messages:
            return None
            
        message = self.messages[message_id]
        message["retry_count"] += 1
        message["updated_at"] = int(time.time())
        
        return message["retry_count"]
    
    def get_message(self, message_id: str) -> Optional[Dict[str, Any]]:
        """
        Get message record by ID.
        
        Args:
            message_id: Message ID
            
        Returns:
            Dict[str, Any]: Message record or None if not found
        """
        return self.messages.get(message_id)
    
    def get_unacknowledged_messages(self, age_seconds: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get all unacknowledged messages.
        
        Args:
            age_seconds: Only include messages older than this many seconds
            
        Returns:
            List[Dict[str, Any]]: List of unacknowledged message records
        """
        now = int(time.time())
        min_time = now - (age_seconds or 0)
        
        return [
            msg for msg in self.messages.values()
            if msg["status"] == MessageStatus.PENDING.value
            and msg["published_at"] < min_time
        ]
    
    def get_stats(self) -> Dict[str, int]:
        """
        Get tracking statistics.
        
        Returns:
            Dict[str, int]: Tracking statistics
        """
        return self.stats.copy()
    
    def cleanup_expired(self) -> int:
        """
        Remove expired message records.
        
        Returns:
            int: Number of records removed
        """
        now = int(time.time())
        to_remove = []
        
        # Find expired messages
        for message_id, message in self.messages.items():
            if message["expires_at"] <= now:
                to_remove.append(message_id)
                
        # Remove them
        for message_id in to_remove:
            del self.messages[message_id]
            
        return len(to_remove)
    
    async def _cleanup_loop(self) -> None:
        """Background task for automatic cleanup of expired messages."""
        try:
            while True:
                await asyncio.sleep(300)  # Run every 5 minutes
                removed = self.cleanup_expired()
                if removed > 0:
                    logger.info(f"Cleaned up {removed} expired message records")
                    
        except asyncio.CancelledError:
            logger.info("Message tracker cleanup task cancelled")
    
    async def stop(self) -> None:
        """Stop the message tracker."""
        if self.cleanup_task:
            self.cleanup_task.cancel()
            try:
                await self.cleanup_task
            except asyncio.CancelledError:
                pass 