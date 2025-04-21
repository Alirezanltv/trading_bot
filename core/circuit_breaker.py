"""
Circuit Breaker Pattern Implementation

This module provides a circuit breaker implementation for high-reliability applications.
The circuit breaker pattern prevents cascading failures by detecting when a service is 
unavailable and failing fast until the service recovers.

Features:
- Three circuit states: CLOSED, OPEN, HALF_OPEN
- Configurable failure thresholds, timeout periods, and retry attempts
- Support for both synchronous and asynchronous operations
- Detailed metrics tracking for monitoring
- Health check reporting
- Event notifications via callbacks
"""

import asyncio
import functools
import logging
import time
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar, Union, cast

logger = logging.getLogger(__name__)

# Type definitions
T = TypeVar('T')  # Return type for functions
F = TypeVar('F', bound=Callable[..., Any])  # Function type


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"  # Normal operation - requests pass through
    OPEN = "open"  # Circuit is open - requests fail fast
    HALF_OPEN = "half_open"  # Testing if service is back - limited requests pass


class CircuitBreakerError(Exception):
    """Base class for circuit breaker exceptions."""
    pass


class CircuitOpenError(CircuitBreakerError):
    """Exception raised when circuit is open."""
    
    def __init__(self, name: str, opened_at: float, reset_timeout: float):
        """
        Initialize CircuitOpenError.
        
        Args:
            name: Circuit breaker name
            opened_at: Timestamp when the circuit was opened
            reset_timeout: Time in seconds until the circuit will be half-open
        """
        self.name = name
        self.opened_at = opened_at
        self.reset_timeout = reset_timeout
        
        # Calculate time remaining until reset
        elapsed = time.time() - opened_at
        remaining = max(0, reset_timeout - elapsed)
        
        super().__init__(
            f"Circuit '{name}' is OPEN - failing fast. "
            f"Opened {elapsed:.2f} seconds ago, will reset in {remaining:.2f} seconds."
        )


class CircuitHalfOpenError(CircuitBreakerError):
    """Exception raised when circuit is half-open and request limit reached."""
    
    def __init__(self, name: str, allowed_requests: int):
        """
        Initialize CircuitHalfOpenError.
        
        Args:
            name: Circuit breaker name
            allowed_requests: Number of allowed requests in half-open state
        """
        self.name = name
        self.allowed_requests = allowed_requests
        
        super().__init__(
            f"Circuit '{name}' is HALF-OPEN - maximum test requests ({allowed_requests}) reached."
        )


class CircuitMetrics:
    """Metrics tracking for a circuit breaker."""
    
    def __init__(self):
        """Initialize circuit metrics."""
        # Total counts
        self.success_count = 0
        self.failure_count = 0
        self.timeout_count = 0
        self.rejected_count = 0
        
        # Current window counts
        self.current_success = 0
        self.current_failure = 0
        self.consecutive_failures = 0
        self.consecutive_successes = 0
        
        # Timing
        self.last_failure_time: Optional[float] = None
        self.last_success_time: Optional[float] = None
        self.last_state_change_time: float = time.time()
        self.last_reset_time: float = time.time()
        
        # State history
        self.open_circuits = 0
        self.state_changes: List[Dict[str, Any]] = []
    
    def record_success(self) -> None:
        """Record a successful operation."""
        self.success_count += 1
        self.current_success += 1
        self.consecutive_successes += 1
        self.consecutive_failures = 0
        self.last_success_time = time.time()
    
    def record_failure(self) -> None:
        """Record a failed operation."""
        self.failure_count += 1
        self.current_failure += 1
        self.consecutive_failures += 1
        self.consecutive_successes = 0
        self.last_failure_time = time.time()
    
    def record_timeout(self) -> None:
        """Record a timeout."""
        self.timeout_count += 1
        self.record_failure()
    
    def record_rejection(self) -> None:
        """Record a rejected request due to open circuit."""
        self.rejected_count += 1
    
    def record_state_change(self, from_state: CircuitState, to_state: CircuitState, reason: str) -> None:
        """
        Record a state change.
        
        Args:
            from_state: Previous state
            to_state: New state
            reason: Reason for state change
        """
        timestamp = time.time()
        self.state_changes.append({
            "timestamp": timestamp,
            "from_state": from_state.value,
            "to_state": to_state.value,
            "reason": reason
        })
        
        self.last_state_change_time = timestamp
        
        if to_state == CircuitState.OPEN:
            self.open_circuits += 1
    
    def reset_window(self) -> None:
        """Reset the current window counters."""
        self.current_success = 0
        self.current_failure = 0
        self.last_reset_time = time.time()
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get circuit metrics as a dictionary.
        
        Returns:
            Circuit metrics
        """
        return {
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "timeout_count": self.timeout_count,
            "rejected_count": self.rejected_count,
            "current_success": self.current_success,
            "current_failure": self.current_failure,
            "consecutive_failures": self.consecutive_failures,
            "consecutive_successes": self.consecutive_successes,
            "last_failure_time": self.last_failure_time,
            "last_success_time": self.last_success_time,
            "last_state_change_time": self.last_state_change_time,
            "last_reset_time": self.last_reset_time,
            "open_circuits": self.open_circuits,
            "state_changes": self.state_changes[-10:]  # Last 10 state changes
        }


class CircuitBreaker:
    """
    Circuit breaker implementation.
    
    This circuit breaker implements the pattern with three states:
    - CLOSED: Normal operation, all requests pass through
    - OPEN: Failing fast for all requests until a timeout
    - HALF-OPEN: Testing with a limited number of requests
    """
    
    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        reset_timeout: float = 60.0,
        half_open_max_requests: int = 3,
        success_threshold: int = 3,
        window_size: float = 60.0,
        exclude_exceptions: Optional[List[Type[Exception]]] = None
    ):
        """
        Initialize circuit breaker.
        
        Args:
            name: Circuit breaker name
            failure_threshold: Number of failures until circuit opens
            reset_timeout: Seconds until circuit transitions from open to half-open
            half_open_max_requests: Maximum number of requests allowed in half-open state
            success_threshold: Number of successes to close the circuit in half-open state
            window_size: Time window in seconds for tracking failures
            exclude_exceptions: Exceptions to exclude from failure counting
        """
        self.name = name
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.half_open_max_requests = half_open_max_requests
        self.success_threshold = success_threshold
        self.window_size = window_size
        self.exclude_exceptions = exclude_exceptions or []
        
        # State management
        self._state = CircuitState.CLOSED
        self.opened_at: Optional[float] = None
        self.half_open_requests = 0
        
        # Lock for thread safety
        self._lock = asyncio.Lock()
        
        # Metrics
        self.metrics = CircuitMetrics()
        
        # Event handlers
        self._on_state_change: List[Callable[[CircuitState, CircuitState, str], None]] = []
        self._on_success: List[Callable[[], None]] = []
        self._on_failure: List[Callable[[Exception], None]] = []
        self._on_rejection: List[Callable[[], None]] = []
        
        logger.info(f"Initialized circuit breaker '{name}'")
    
    @property
    def state(self) -> CircuitState:
        """Get the current circuit state."""
        # Auto-transition from OPEN to HALF-OPEN if reset timeout passed
        if (self._state == CircuitState.OPEN and 
            self.opened_at is not None and 
            time.time() - self.opened_at >= self.reset_timeout):
            
            # Avoid directly calling self.state = ... inside the property
            # Instead, schedule the transition to happen outside this property
            asyncio.create_task(self._transition_state(
                CircuitState.HALF_OPEN, 
                "Reset timeout expired"
            ))
        
        return self._state
    
    @state.setter
    def state(self, value: CircuitState) -> None:
        """Set the circuit state directly (for testing and internal use)."""
        self._state = value
    
    async def _check_state_transition(self) -> None:
        """Check if circuit needs to transition to a different state based on timers."""
        if self.state == CircuitState.OPEN:
            # Check if reset timeout has expired
            if self.opened_at and (time.time() - self.opened_at) >= self.reset_timeout:
                # Transition to half-open
                await self._transition_state(
                    CircuitState.HALF_OPEN,
                    "Reset timeout expired"
                )
                self.half_open_requests = 0
    
    async def _transition_state(self, new_state: CircuitState, reason: str) -> None:
        """
        Transition the circuit to a new state.
        
        Args:
            new_state: The new state
            reason: Reason for the transition
        """
        if self.state == new_state:
            return
            
        async with self._lock:
            if self.state == new_state:
                return
                
            old_state = self.state
            
            # Update timestamps and metrics
            if new_state == CircuitState.OPEN:
                self.opened_at = time.time()
                self.metrics.open_circuits += 1
            else:
                # Reset consecutive failures when transitioning out of OPEN
                if old_state == CircuitState.OPEN:
                    self.metrics.consecutive_failures = 0
            
            self.state = new_state
            
            # Record state change in metrics
            self.metrics.record_state_change(old_state, new_state, reason)
            
            # Log the transition
            logger.info(f"Circuit '{self.name}' state changed from {old_state.value} to {new_state.value}: {reason}")
            
            # Notify listeners
            for listener in self._on_state_change:
                try:
                    listener(old_state, new_state, reason)
                except Exception as e:
                    logger.error(f"Error in state change listener: {e}")
    
    def _record_success(self) -> None:
        """Record a successful operation."""
        self.metrics.record_success()
        
        # Notify event handlers
        for handler in self._on_success:
            try:
                handler()
            except Exception as e:
                logger.error(f"Error in success handler: {str(e)}")
    
    def _record_failure(self, exception: Exception) -> None:
        """
        Record a failed operation.
        
        Args:
            exception: Exception that caused the failure
        """
        self.metrics.record_failure()
        
        # Notify event handlers
        for handler in self._on_failure:
            try:
                handler(exception)
            except Exception as e:
                logger.error(f"Error in failure handler: {str(e)}")
    
    def _record_rejection(self) -> None:
        """Record a rejected request due to open circuit."""
        self.metrics.record_rejection()
        
        # Notify event handlers
        for handler in self._on_rejection:
            try:
                handler()
            except Exception as e:
                logger.error(f"Error in rejection handler: {str(e)}")
    
    async def _handle_success(self) -> None:
        """Handle a successful operation."""
        self._record_success()
        
        # If in half-open state and success threshold reached, close the circuit
        if (self._state == CircuitState.HALF_OPEN and 
            self.metrics.consecutive_successes >= self.success_threshold):
            
            await self._transition_state(
                CircuitState.CLOSED,
                f"Success threshold reached ({self.success_threshold})"
            )
    
    async def _handle_failure(self, exception: Exception) -> None:
        """
        Handle a failed operation.
        
        Args:
            exception: Exception that caused the failure
        """
        # Check if this exception should be excluded from failure counting
        if any(isinstance(exception, exc_type) for exc_type in self.exclude_exceptions):
            logger.debug(f"Excluded exception type {type(exception).__name__} from failure counting")
            return
        
        self._record_failure(exception)
        
        # If in closed state and failure threshold reached, open the circuit
        if (self._state == CircuitState.CLOSED and 
            self.metrics.consecutive_failures >= self.failure_threshold):
            
            await self._transition_state(
                CircuitState.OPEN,
                f"Failure threshold reached ({self.failure_threshold})"
            )
        
        # If in half-open state, any failure should open the circuit
        elif self._state == CircuitState.HALF_OPEN:
            await self._transition_state(
                CircuitState.OPEN,
                "Failure in half-open state"
            )
    
    async def execute(self, func, *args, **kwargs) -> Any:
        """
        Execute a function with circuit breaker protection.
        
        Args:
            func: Function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments
            
        Returns:
            Function result
            
        Raises:
            CircuitOpenError: If circuit is open
            CircuitHalfOpenError: If circuit is half-open and request limit reached
            Exception: Any exception raised by the function
        """
        # Check for state transitions before processing
        await self._check_state_transition()
        
        current_state = self.state
        
        # Check if circuit is open
        if current_state == CircuitState.OPEN:
            self._record_rejection()
            raise CircuitOpenError(
                self.name, 
                cast(float, self.opened_at), 
                self.reset_timeout
            )
        
        # Check if circuit is half-open and request limit reached
        if current_state == CircuitState.HALF_OPEN:
            async with self._lock:
                if self.half_open_requests >= self.half_open_max_requests:
                    self._record_rejection()
                    raise CircuitHalfOpenError(self.name, self.half_open_max_requests)
                
                self.half_open_requests += 1
        
        try:
            # Execute the function (handle both sync and async functions)
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
            
            # Handle successful execution
            await self._handle_success()
            
            return result
            
        except Exception as e:
            # Handle failure
            await self._handle_failure(e)
            raise
    
    def add_state_change_listener(self, listener: Callable[[CircuitState, CircuitState, str], None]) -> None:
        """
        Add a state change event listener.
        
        Args:
            listener: Callback function when circuit state changes
        """
        if listener not in self._on_state_change:
            self._on_state_change.append(listener)
    
    def add_success_listener(self, listener: Callable[[], None]) -> None:
        """
        Add a success event listener.
        
        Args:
            listener: Callback function when operation succeeds
        """
        if listener not in self._on_success:
            self._on_success.append(listener)
    
    def add_failure_listener(self, listener: Callable[[Exception], None]) -> None:
        """
        Add a failure event listener.
        
        Args:
            listener: Callback function when operation fails
        """
        if listener not in self._on_failure:
            self._on_failure.append(listener)
    
    def add_rejection_listener(self, listener: Callable[[], None]) -> None:
        """
        Add a rejection event listener.
        
        Args:
            listener: Callback function when request is rejected
        """
        if listener not in self._on_rejection:
            self._on_rejection.append(listener)
    
    def reset(self) -> None:
        """Reset the circuit to closed state."""
        asyncio.create_task(self._transition_state(
            CircuitState.CLOSED,
            "Manual reset"
        ))
    
    def trip(self, reason: str = "Manual trip") -> None:
        """
        Trip the circuit to open state.
        
        Args:
            reason: Reason for tripping the circuit
        """
        asyncio.create_task(self._transition_state(
            CircuitState.OPEN,
            reason
        ))
    
    def get_health(self) -> Dict[str, Any]:
        """
        Get circuit health status.
        
        Returns:
            Health status
        """
        current_state = self.state
        metrics = self.metrics
        
        health_status = "healthy"
        if current_state == CircuitState.OPEN:
            health_status = "unhealthy"
        elif current_state == CircuitState.HALF_OPEN:
            health_status = "degraded"
        
        return {
            "name": self.name,
            "state": current_state.value,
            "status": health_status,
            "failure_rate": (
                metrics.failure_count / (metrics.success_count + metrics.failure_count)
                if (metrics.success_count + metrics.failure_count) > 0 else 0
            ),
            "consecutive_failures": metrics.consecutive_failures,
            "consecutive_successes": metrics.consecutive_successes,
            "opened_at": self.opened_at,
            "open_circuits_count": metrics.open_circuits,
            "rejection_count": metrics.rejected_count,
            "last_state_change": metrics.last_state_change_time
        }


def circuit_breaker(
    name_or_breaker: Union[str, CircuitBreaker],
    failure_threshold: int = 5,
    reset_timeout: float = 60.0,
    half_open_max_requests: int = 3,
    success_threshold: int = 3,
    window_size: float = 60.0,
    exclude_exceptions: Optional[List[Type[Exception]]] = None
) -> Callable[[F], F]:
    """
    Circuit breaker decorator.
    
    Can be used with either a circuit breaker instance or configuration parameters.
    
    Args:
        name_or_breaker: Circuit breaker name or instance
        failure_threshold: Number of failures until circuit opens
        reset_timeout: Seconds until circuit transitions from open to half-open
        half_open_max_requests: Maximum number of requests allowed in half-open state
        success_threshold: Number of successes to close the circuit in half-open state
        window_size: Time window in seconds for tracking failures
        exclude_exceptions: Exceptions to exclude from failure counting
        
    Returns:
        Decorator function
    """
    if isinstance(name_or_breaker, CircuitBreaker):
        breaker = name_or_breaker
    else:
        breaker = CircuitBreaker(
            name=name_or_breaker,
            failure_threshold=failure_threshold,
            reset_timeout=reset_timeout,
            half_open_max_requests=half_open_max_requests,
            success_threshold=success_threshold,
            window_size=window_size,
            exclude_exceptions=exclude_exceptions
        )
    
    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            return await breaker.execute(func, *args, **kwargs)
        
        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            return asyncio.run(breaker.execute(func, *args, **kwargs))
        
        if asyncio.iscoroutinefunction(func):
            return cast(F, async_wrapper)
        else:
            return cast(F, sync_wrapper)
    
    return decorator


class CircuitBreakerRegistry:
    """
    Registry for circuit breakers.
    
    Provides a centralized place to access and monitor all circuit breakers in the system.
    """
    
    _instance = None
    
    def __new__(cls):
        """Singleton implementation."""
        if cls._instance is None:
            cls._instance = super(CircuitBreakerRegistry, cls).__new__(cls)
            cls._instance.breakers = {}
        return cls._instance
    
    def register(self, breaker: CircuitBreaker) -> None:
        """
        Register a circuit breaker.
        
        Args:
            breaker: Circuit breaker to register
        """
        self.breakers[breaker.name] = breaker
        logger.debug(f"Registered circuit breaker '{breaker.name}'")
    
    def unregister(self, name: str) -> None:
        """
        Unregister a circuit breaker.
        
        Args:
            name: Name of circuit breaker to unregister
        """
        if name in self.breakers:
            del self.breakers[name]
            logger.debug(f"Unregistered circuit breaker '{name}'")
    
    def get(self, name: str) -> Optional[CircuitBreaker]:
        """
        Get a circuit breaker by name.
        
        Args:
            name: Circuit breaker name
            
        Returns:
            Circuit breaker instance or None if not found
        """
        return self.breakers.get(name)
    
    def get_all(self) -> Dict[str, CircuitBreaker]:
        """
        Get all registered circuit breakers.
        
        Returns:
            Dictionary of circuit breaker name to instance
        """
        return self.breakers.copy()
    
    def get_health(self) -> Dict[str, Dict[str, Any]]:
        """
        Get health status of all circuit breakers.
        
        Returns:
            Dictionary of circuit breaker name to health status
        """
        return {name: breaker.get_health() for name, breaker in self.breakers.items()}
    
    def reset_all(self) -> None:
        """Reset all circuit breakers to closed state."""
        for breaker in self.breakers.values():
            breaker.reset()
        logger.info("Reset all circuit breakers")


# Create a singleton registry
registry = CircuitBreakerRegistry() 