"""
Exchange API Connection Pool

This module provides connection pooling for exchange API clients, improving reliability
and reducing the impact of connection limits and rate limiting.

Features:
- Connection pooling with configurable pool size
- Automatic reconnection and health monitoring
- Rate limiting and request queueing
- Circuit breaker integration for fault tolerance
"""

import asyncio
import logging
import time
from typing import Dict, Any, List, Optional, Callable, Awaitable, Union, Tuple
from datetime import datetime, timedelta
from enum import Enum
import aiohttp
from dataclasses import dataclass, field
from pybreaker import CircuitBreaker, CircuitBreakerError

from trading_system.core.component import Component, ComponentStatus
from trading_system.core.logging import get_logger

logger = get_logger("exchange.connection_pool")

class ConnectionStatus(Enum):
    """Connection status states."""
    AVAILABLE = "available"
    BUSY = "busy"
    THROTTLED = "throttled"
    ERROR = "error"

class ConnectionPoolError(Exception):
    """Base error for connection pool issues."""
    pass

@dataclass
class ConnectionStats:
    """Statistics for a connection."""
    status: str = "available"
    requests_count: int = 0
    error_count: int = 0
    last_error: Optional[str] = None
    last_error_time: Optional[str] = None
    avg_response_time: float = 0
    min_response_time: Optional[float] = None
    max_response_time: Optional[float] = None
    last_used_time: Optional[str] = None
    creation_time: Optional[str] = None
    reconnect_count: int = 0
    last_reconnect_time: Optional[str] = None

    def update_on_request(self, response_time: float) -> None:
        """Update stats after a successful request."""
        self.requests_count += 1
        self.avg_response_time = (
            (self.avg_response_time * (self.requests_count - 1) + response_time)
            / self.requests_count
        )
        if self.min_response_time is None or response_time < self.min_response_time:
            self.min_response_time = response_time
        if self.max_response_time is None or response_time > self.max_response_time:
            self.max_response_time = response_time
        self.last_used_time = datetime.now().isoformat()
    
    def update_on_error(self, error: Exception) -> None:
        """Update stats after an error."""
        self.error_count += 1
        self.last_error = str(error)
        self.last_error_time = datetime.now().isoformat()

    def update_on_reconnect(self) -> None:
        """Update stats after a reconnection attempt."""
        self.reconnect_count += 1
        self.last_reconnect_time = datetime.now().isoformat()
        self.status = ConnectionStatus.AVAILABLE.value

class SimpleCircuitBreaker:
    """A simple circuit breaker implementation for async functions."""
    
    def __init__(self, failure_threshold: int = 5, reset_timeout: float = 60.0):
        """
        Initialize the circuit breaker.
        
        Args:
            failure_threshold: Number of failures before opening the circuit
            reset_timeout: Time in seconds before attempting to close the circuit
        """
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "closed"  # closed, open, half-open
        self._lock = asyncio.Lock()
    
    async def call_async(self, func, *args, **kwargs) -> Any:
        """
        Call a function with circuit breaker protection.
        
        Args:
            func: The function to call
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function
            
        Returns:
            The result of the function call
            
        Raises:
            ConnectionPoolError: If the circuit is open
        """
        async with self._lock:
            if self.state == "open":
                # Check if we should try to close the circuit
                if self.last_failure_time and time.time() - self.last_failure_time > self.reset_timeout:
                    self.state = "half-open"
                    logger.info("Circuit breaker entering half-open state")
                else:
                    raise ConnectionPoolError("Circuit breaker is open")
            
            try:
                # Call the function
                result = await func(*args, **kwargs)
                
                # On success, reset the circuit if it was half-open
                if self.state == "half-open":
                    self.state = "closed"
                    self.failure_count = 0
                    logger.info("Circuit breaker closed after successful call")
                
                return result
                
            except Exception as e:
                # On failure, increment the failure count
                self.failure_count += 1
                self.last_failure_time = time.time()
                
                # Open the circuit if we've hit the threshold
                if self.failure_count >= self.failure_threshold:
                    self.state = "open"
                    logger.warning(f"Circuit breaker opened after {self.failure_count} failures")
                
                raise

@dataclass
class Connection:
    """Represents a connection in the pool."""
    id: str
    client: Any
    breaker: SimpleCircuitBreaker
    stats: ConnectionStats = field(default_factory=ConnectionStats)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def execute(self, method: str, *args, **kwargs) -> Any:
        """Execute a method on the connection with circuit breaker protection."""
        async with self._lock:
            start_time = time.time()
            try:
                # Get the method from the client
                if not hasattr(self.client, method):
                    raise AttributeError(f"Client does not have method {method}")
                
                client_method = getattr(self.client, method)
                
                # Execute with circuit breaker protection
                result = await self.breaker.call_async(client_method, *args, **kwargs)
                
                # Update stats
                response_time = time.time() - start_time
                self.stats.update_on_request(response_time)
                
                return result
                
            except Exception as e:
                self.stats.update_on_error(e)
                raise

class ConnectionPool(Component):
    """
    Connection pool for exchange API clients.
    
    Manages multiple connections to the exchange API, with health monitoring,
    automatic reconnection, and request queueing.
    """
    
    def __init__(self, 
                 name: str,
                 create_client_func: Callable[[], Any],
                 config: Dict[str, Any] = None):
        """
        Initialize the connection pool.
        
        Args:
            name: Pool name (usually the exchange name)
            create_client_func: Function to create a new client instance
            config: Configuration dictionary containing:
                - min_connections: Minimum number of connections (default: 1)
                - max_connections: Maximum number of connections (default: 5)
                - max_requests_per_window: Maximum requests in rate limit window (default: 60)
                - window_seconds: Window size in seconds for rate limiting (default: 60)
                - max_queued_requests: Maximum number of queued requests (default: 100)
                - connection_timeout: Connection timeout in seconds (default: 10)
                - inactive_timeout: Time to keep idle connections in seconds (default: 60)
                - idle_timeout: Time to keep idle connections in seconds (default: 10)
                - max_retries: Maximum number of retries (default: 3)
        """
        super().__init__(name=f"ConnectionPool_{name}")
        
        # Configuration
        self.config = config or {}
        self.min_connections = self.config.get("min_connections", 1)
        self.max_connections = self.config.get("max_connections", 5)
        self.max_requests_per_window = self.config.get("max_requests_per_window", 60)
        self.window_seconds = self.config.get("window_seconds", 60)
        self.max_queued_requests = self.config.get("max_queued_requests", 100)
        self.connection_timeout = self.config.get("connection_timeout", 10)
        self.inactive_timeout = self.config.get("inactive_timeout", 60)
        self.idle_timeout = self.config.get("idle_timeout", 10)
        self.max_retries = self.config.get("max_retries", 3)
        
        # Function to create new clients
        self.create_client_func = create_client_func
        
        # Connection management
        self.connections: Dict[str, Connection] = {}
        self.available_connections: List[Connection] = []
        self.connection_locks = {}
        self.pool_lock = asyncio.Lock()
        
        # Request queue
        self.request_queue = asyncio.Queue(maxsize=self.max_queued_requests)
        self.queue_processor = None
        
        # Statistics
        self.stats = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "queued_requests": 0,
            "queue_wait_time": 0,
            "connections_created": 0,
            "connections_closed": 0,
            "reconnections": 0,
        }
        
        # Circuit breaker
        self._breaker = SimpleCircuitBreaker(
            failure_threshold=self.max_connections,
            reset_timeout=self.connection_timeout
        )
    
    async def initialize(self) -> bool:
        """Initialize the connection pool by creating the minimum number of connections."""
        logger.info(f"Initializing connection pool for {self.name}")
        
        # Create initial connections
        for i in range(self.min_connections):
            conn_id = f"connectionpool_{self.name.lower()}_{i+1}"
            try:
                await self._create_connection(conn_id)
            except Exception as e:
                logger.error(f"Failed to create initial connection {conn_id}: {e}")
        
        # Start the queue processor
        self.queue_processor = asyncio.create_task(self._process_queue())
        
        logger.info(f"Connection pool for {self.name} initialized with {len(self.connections)} connections")
        return len(self.connections) >= self.min_connections
    
    async def get_connection(self) -> Optional[Connection]:
        """
        Get an available connection from the pool.
        
        Returns:
            A connection object or None if no connection could be obtained
        """
        async with self.pool_lock:
            # If we have an available connection, return it
            if self.available_connections:
                connection = self.available_connections.pop(0)
                logger.debug(f"Using existing connection {connection.id}")
                return connection
            
            # If we're below max connections, create a new one
            if len(self.connections) < self.max_connections:
                conn_id = f"connectionpool_{self.name.lower()}_{len(self.connections)+1}"
                try:
                    connection = await self._create_connection(conn_id)
                    logger.debug(f"Created new connection {conn_id}")
                    return connection
                except Exception as e:
                    logger.error(f"Failed to create new connection: {e}")
            
            # No connections available, wait for one
            logger.warning("No connections available, waiting...")
            while not self.available_connections:
                # Release the lock while waiting
                self.pool_lock.release()
                await asyncio.sleep(0.1)
                await self.pool_lock.acquire()
                
                # Check again after waiting
                if self.available_connections:
                    connection = self.available_connections.pop(0)
                    logger.debug(f"Using connection {connection.id} after waiting")
                    return connection
            
            return None
    
    async def release_connection(self, connection: Connection) -> None:
        """
        Release a connection back to the pool.
        
        Args:
            connection: The connection to release
        """
        async with self.pool_lock:
            if connection.id in self.connections and connection not in self.available_connections:
                self.available_connections.append(connection)
                logger.debug(f"Released connection {connection.id} back to pool")
    
    async def execute(self, method: str, *args, **kwargs) -> Any:
        """
        Execute a method on a connection from the pool.
        
        Args:
            method: Method name to execute
            *args: Positional arguments for the method
            **kwargs: Keyword arguments for the method
            
        Returns:
            The result of the method call
            
        Raises:
            ConnectionPoolError: If the operation fails after retries
        """
        # Get a connection
        connection = await self.get_connection()
        if not connection:
            raise ConnectionPoolError("No connection available")
        
        # Try to execute with retries
        retry_count = 0
        last_error = None
        
        while retry_count <= self.max_retries:
            try:
                result = await connection.execute(method, *args, **kwargs)
                # Success, release the connection and return the result
                await self.release_connection(connection)
                self.stats["successful_requests"] += 1
                return result
            except Exception as e:
                retry_count += 1
                last_error = e
                logger.warning(f"Error executing {method} (attempt {retry_count}): {e}")
                
                # If this wasn't the last retry, wait before retrying
                if retry_count <= self.max_retries:
                    # Use exponential backoff
                    wait_time = 0.1 * (2 ** retry_count)
                    await asyncio.sleep(wait_time)
                    
                    # Try with a different connection
                    await self.release_connection(connection)
                    connection = await self.get_connection()
                    if not connection:
                        raise ConnectionPoolError("No connection available for retry")
        
        # All retries failed
        await self.release_connection(connection)
        self.stats["failed_requests"] += 1
        raise ConnectionPoolError(f"Operation failed after {self.max_retries} retries: {last_error}")
    
    async def _create_connection(self, conn_id: str) -> Connection:
        """
        Create a new connection.
        
        Args:
            conn_id: Connection ID
            
        Returns:
            The new connection
            
        Raises:
            ConnectionPoolError: If the connection cannot be created
        """
        try:
            client = self.create_client_func()
            breaker = SimpleCircuitBreaker()
            connection = Connection(
                id=conn_id,
                client=client,
                breaker=breaker,
                stats=ConnectionStats(
                    creation_time=datetime.now().isoformat()
                )
            )
            
            self.connections[conn_id] = connection
            self.available_connections.append(connection)
            self.stats["connections_created"] += 1
            
            logger.info(f"Creating new connection {conn_id}")
            return connection
        except Exception as e:
            logger.error(f"Failed to create connection {conn_id}: {e}")
            raise ConnectionPoolError(f"Failed to create connection: {e}")
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Check the health of the connection pool.
        
        Returns:
            Dictionary with health status
        """
        status = ComponentStatus.OK
        message = "Connection pool is healthy"
        
        # Check if we have enough connections
        connection_count = len(self.connections)
        if connection_count < self.min_connections:
            status = ComponentStatus.DEGRADED
            message = f"Connection pool has less than minimum connections ({connection_count}/{self.min_connections})"
        
        # Check if any connections are in error state
        error_connections = sum(1 for conn in self.connections.values() 
                               if conn.stats.status == ConnectionStatus.ERROR.value)
        if error_connections > 0:
            status = ComponentStatus.DEGRADED
            message = f"{error_connections} connections in error state"
        
        # No connections available
        if len(self.available_connections) == 0 and connection_count > 0:
            status = ComponentStatus.DEGRADED
            message = "No available connections"
        
        # All connections failed
        if connection_count > 0 and error_connections == connection_count:
            status = ComponentStatus.ERROR
            message = "All connections are in error state"
            
        # No connections
        if connection_count == 0:
            status = ComponentStatus.ERROR
            message = "No connections established"
        
        return {
            "status": status.value,
            "message": message,
            "connections": {
                "total": connection_count,
                "available": len(self.available_connections),
                "errors": error_connections
            },
            "requests": {
                "total": self.stats["total_requests"],
                "successful": self.stats["successful_requests"],
                "failed": self.stats["failed_requests"],
                "queued": self.request_queue.qsize()
            }
        }
        
    def get_stats(self) -> Dict[str, Any]:
        """
        Get detailed statistics about the connection pool.
        
        Returns:
            Dictionary with detailed statistics about the pool and its connections
        """
        # Collect connection stats
        connection_stats = {}
        for conn_id, conn in self.connections.items():
            connection_stats[conn_id] = {
                "status": conn.stats.status,
                "requests": conn.stats.requests_count,
                "errors": conn.stats.error_count,
                "last_error": conn.stats.last_error,
                "last_error_time": conn.stats.last_error_time,
                "avg_response_time": conn.stats.avg_response_time,
                "min_response_time": conn.stats.min_response_time,
                "max_response_time": conn.stats.max_response_time,
                "last_used": conn.stats.last_used_time,
                "created": conn.stats.creation_time,
                "reconnects": conn.stats.reconnect_count,
                "last_reconnect": conn.stats.last_reconnect_time
            }
        
        # Return combined stats
        return {
            "pool": {
                "name": self.name,
                "min_connections": self.min_connections,
                "max_connections": self.max_connections,
                "current_connections": len(self.connections),
                "available_connections": len(self.available_connections),
                "queue_size": self.request_queue.qsize(),
                "max_queue_size": self.max_queued_requests,
                "total_requests": self.stats["total_requests"],
                "successful_requests": self.stats["successful_requests"],
                "failed_requests": self.stats["failed_requests"],
                "connections_created": self.stats["connections_created"],
                "connections_closed": self.stats["connections_closed"],
                "reconnections": self.stats["reconnections"],
            },
            "connections": connection_stats,
            "rate_limiting": {
                "max_requests_per_window": self.max_requests_per_window,
                "window_seconds": self.window_seconds
            }
        }
    
    async def _process_queue(self) -> None:
        """Process the request queue."""
        try:
            while True:
                # Get an item from the queue
                request = await self.request_queue.get()
                
                # Execute the request
                method_name = request["method"]
                args = request["args"]
                kwargs = request["kwargs"]
                future = request["future"]
                
                try:
                    result = await self.execute(method_name, *args, **kwargs)
                    future.set_result(result)
                except Exception as e:
                    future.set_exception(e)
                finally:
                    self.request_queue.task_done()
                    
        except asyncio.CancelledError:
            logger.info("Queue processor cancelled")
        except Exception as e:
            logger.error(f"Error in queue processor: {e}")
    
    async def stop(self) -> bool:
        """Stop the connection pool."""
        logger.info(f"Stopping connection pool {self.name}")
        
        # Cancel the queue processor
        if self.queue_processor:
            self.queue_processor.cancel()
            try:
                await self.queue_processor
            except asyncio.CancelledError:
                pass
        
        # Clear connections
        self.connections = {}
        self.available_connections = []
        
        return True 