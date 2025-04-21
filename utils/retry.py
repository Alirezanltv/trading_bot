"""
Retry Utility

This module provides retry functionality with exponential backoff.
"""

import asyncio
import functools
import logging
import random
import time
from typing import Any, Callable, Optional, Type, TypeVar, cast

from trading_system.core.logging import get_logger

logger = get_logger("utils.retry")

# Type variable for function return type
T = TypeVar('T')


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    backoff_factor: float = 2.0,
    jitter: bool = True,
    exceptions: tuple = (Exception,),
    initial_wait: float = None  # For backward compatibility
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Retry decorator with exponential backoff.
    
    Args:
        max_retries: Maximum number of retries
        base_delay: Initial delay between retries in seconds
        initial_wait: Alias for base_delay (for backward compatibility)
        max_delay: Maximum delay between retries in seconds
        backoff_factor: Multiplicative factor for backoff
        jitter: Whether to add random jitter to delays
        exceptions: Tuple of exceptions to catch
        
    Returns:
        Decorator function
    """
    # Handle backward compatibility - use initial_wait if provided
    if initial_wait is not None:
        base_delay = initial_wait
    
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            attempt = 0
            last_exception = None
            
            while attempt <= max_retries:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    attempt += 1
                    last_exception = e
                    
                    if attempt > max_retries:
                        logger.error(f"Function {func.__name__} failed after {max_retries} retries. Last error: {str(e)}")
                        raise
                    
                    # Calculate delay with exponential backoff
                    delay = min(base_delay * (backoff_factor ** (attempt - 1)), max_delay)
                    
                    # Add jitter if enabled
                    if jitter:
                        delay = delay * (0.5 + random.random())
                    
                    logger.warning(
                        f"Retry {attempt}/{max_retries} for {func.__name__} in {delay:.2f} seconds. Error: {str(e)}"
                    )
                    
                    # Wait before retrying
                    time.sleep(delay)
            
            # This should never happen due to the raise inside the loop
            assert last_exception is not None
            raise last_exception
        
        return cast(Callable[..., T], wrapper)
    
    return decorator


def async_retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    backoff_factor: float = 2.0,
    jitter: bool = True,
    exceptions: tuple = (Exception,),
    initial_wait: float = None  # For backward compatibility
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Async retry decorator with exponential backoff.
    
    Args:
        max_retries: Maximum number of retries
        base_delay: Initial delay between retries in seconds
        initial_wait: Alias for base_delay (for backward compatibility)
        max_delay: Maximum delay between retries in seconds
        backoff_factor: Multiplicative factor for backoff
        jitter: Whether to add random jitter to delays
        exceptions: Tuple of exceptions to catch
        
    Returns:
        Decorator function
    """
    # Handle backward compatibility - use initial_wait if provided
    if initial_wait is not None:
        base_delay = initial_wait
        
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            attempt = 0
            last_exception = None
            
            while attempt <= max_retries:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    attempt += 1
                    last_exception = e
                    
                    if attempt > max_retries:
                        logger.error(f"Function {func.__name__} failed after {max_retries} retries. Last error: {str(e)}")
                        raise
                    
                    # Calculate delay with exponential backoff
                    delay = min(base_delay * (backoff_factor ** (attempt - 1)), max_delay)
                    
                    # Add jitter if enabled
                    if jitter:
                        delay = delay * (0.5 + random.random())
                    
                    logger.warning(
                        f"Retry {attempt}/{max_retries} for {func.__name__} in {delay:.2f} seconds. Error: {str(e)}"
                    )
                    
                    # Wait before retrying
                    await asyncio.sleep(delay)
            
            # This should never happen due to the raise inside the loop
            assert last_exception is not None
            raise last_exception
        
        return cast(Callable[..., T], wrapper)
    
    return decorator


class RetryException(Exception):
    """Exception raised when all retries have been exhausted."""
    
    def __init__(self, original_exception: Exception, attempts: int):
        """
        Initialize retry exception.
        
        Args:
            original_exception: Original exception that caused the failure
            attempts: Number of attempts made
        """
        self.original_exception = original_exception
        self.attempts = attempts
        super().__init__(f"Failed after {attempts} attempts. Last error: {str(original_exception)}")


class RetryContextManager:
    """Context manager for retry with backoff."""
    
    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        backoff_factor: float = 2.0,
        jitter: bool = True,
        exceptions: tuple = (Exception,),
        on_retry: Optional[Callable[[Exception, int], None]] = None,
        initial_wait: float = None  # For backward compatibility
    ):
        """
        Initialize retry context manager.
        
        Args:
            max_retries: Maximum number of retries
            base_delay: Initial delay between retries in seconds
            initial_wait: Alias for base_delay (for backward compatibility)
            max_delay: Maximum delay between retries in seconds
            backoff_factor: Multiplicative factor for backoff
            jitter: Whether to add random jitter to delays
            exceptions: Tuple of exceptions to catch
            on_retry: Optional callback for retry events
        """
        self.max_retries = max_retries
        self.base_delay = initial_wait if initial_wait is not None else base_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.jitter = jitter
        self.exceptions = exceptions
        self.on_retry = on_retry
        self.attempt = 0
    
    def __enter__(self):
        """Enter context."""
        self.attempt = 0
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit context.
        
        Args:
            exc_type: Exception type
            exc_val: Exception value
            exc_tb: Exception traceback
            
        Returns:
            True if exception was handled, False otherwise
        """
        if exc_type is None:
            return False
        
        # Check if exception is one we should catch
        if not issubclass(exc_type, self.exceptions):
            return False
        
        self.attempt += 1
        
        # If we've exceeded retries, re-raise the exception
        if self.attempt > self.max_retries:
            logger.error(f"Failed after {self.max_retries} retries. Last error: {str(exc_val)}")
            return False
        
        # Calculate delay with exponential backoff
        delay = min(self.base_delay * (self.backoff_factor ** (self.attempt - 1)), self.max_delay)
        
        # Add jitter if enabled
        if self.jitter:
            delay = delay * (0.5 + random.random())
        
        logger.warning(
            f"Retry {self.attempt}/{self.max_retries} in {delay:.2f} seconds. Error: {str(exc_val)}"
        )
        
        # Call on_retry callback if provided
        if self.on_retry:
            self.on_retry(exc_val, self.attempt)
        
        # Wait before retrying
        time.sleep(delay)
        
        # Indicate that we handled the exception
        return True


class AsyncRetryContextManager:
    """Async context manager for retry with backoff."""
    
    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        backoff_factor: float = 2.0,
        jitter: bool = True,
        exceptions: tuple = (Exception,),
        on_retry: Optional[Callable[[Exception, int], None]] = None,
        initial_wait: float = None  # For backward compatibility
    ):
        """
        Initialize async retry context manager.
        
        Args:
            max_retries: Maximum number of retries
            base_delay: Initial delay between retries in seconds
            initial_wait: Alias for base_delay (for backward compatibility)
            max_delay: Maximum delay between retries in seconds
            backoff_factor: Multiplicative factor for backoff
            jitter: Whether to add random jitter to delays
            exceptions: Tuple of exceptions to catch
            on_retry: Optional callback for retry events
        """
        self.max_retries = max_retries
        self.base_delay = initial_wait if initial_wait is not None else base_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.jitter = jitter
        self.exceptions = exceptions
        self.on_retry = on_retry
        self.attempt = 0
    
    async def __aenter__(self):
        """Enter async context."""
        self.attempt = 0
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Exit async context.
        
        Args:
            exc_type: Exception type
            exc_val: Exception value
            exc_tb: Exception traceback
            
        Returns:
            True if exception was handled, False otherwise
        """
        if exc_type is None:
            return False
        
        # Check if exception is one we should catch
        if not issubclass(exc_type, self.exceptions):
            return False
        
        self.attempt += 1
        
        # If we've exceeded retries, re-raise the exception
        if self.attempt > self.max_retries:
            logger.error(f"Failed after {self.max_retries} retries. Last error: {str(exc_val)}")
            return False
        
        # Calculate delay with exponential backoff
        delay = min(self.base_delay * (self.backoff_factor ** (self.attempt - 1)), self.max_delay)
        
        # Add jitter if enabled
        if self.jitter:
            delay = delay * (0.5 + random.random())
        
        logger.warning(
            f"Retry {self.attempt}/{self.max_retries} in {delay:.2f} seconds. Error: {str(exc_val)}"
        )
        
        # Call on_retry callback if provided
        if self.on_retry:
            self.on_retry(exc_val, self.attempt)
        
        # Wait before retrying
        await asyncio.sleep(delay)
        
        # Indicate that we handled the exception
        return True 