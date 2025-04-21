# High-Reliability Trading System Components

This README provides a guide on how to use the high-reliability components implemented in our trading system.

## Installation

### Prerequisites

- Python 3.10 or higher
- pip package manager

### Quick Install

Install the core high-reliability components:

```bash
# Windows
py -3.10 -m pip install aiohttp asyncio pika pybreaker tenacity

# Linux/Mac
python3.10 -m pip install aiohttp asyncio pika pybreaker tenacity
```

For a full installation with all dependencies:

```bash
# Windows
py -3.10 -m trading_system.install_high_reliability

# Linux/Mac
python3.10 -m trading_system.install_high_reliability
```

## Core Components

### Circuit Breaker

The circuit breaker pattern prevents cascading failures by detecting when a service is unavailable and failing fast until it recovers.

```python
from trading_system.core.circuit_breaker import CircuitBreaker, circuit_breaker

# Method 1: Using the decorator
@circuit_breaker(name="api_operations", failure_threshold=3, reset_timeout=60.0)
async def call_api():
    # Your API call here
    pass

# Method 2: Using the class directly
breaker = CircuitBreaker(
    name="exchange_api",
    failure_threshold=5,
    reset_timeout=30.0,
    half_open_max_requests=2,
    success_threshold=3
)

async def make_request():
    try:
        result = await breaker.execute(api_client.get_data)
        return result
    except Exception as e:
        # Handle the exception
        pass
```

### Connection Pool

The connection pool manages multiple connections to external services, enhancing reliability through connection reuse and automatic reconnection.

```python
from trading_system.exchange.connection_pool import ConnectionPool

# Create a client factory function
def create_client():
    # Return a new client instance
    return ApiClient()

# Initialize the pool
pool = ConnectionPool(
    name="exchange_pool",
    create_client_func=create_client,
    config={
        "min_connections": 2,
        "max_connections": 10,
        "connection_timeout": 10,
        "idle_timeout": 60
    }
)

# Initialize the pool
await pool.initialize()

# Execute operations through the pool
result = await pool.execute("get_market_data", "BTC/USDT")
```

### Transaction Verification

Ensures transaction integrity through a three-phase commit protocol.

```python
from trading_system.exchange.transaction_verification import TransactionVerificationPipeline
from trading_system.exchange.enhanced_nobitex_adapter import EnhancedNobitexAdapter

# Create the pipeline
verifier = TransactionVerificationPipeline(
    exchange_adapter=exchange_adapter,
    message_bus=message_bus,
    verification_timeout=30,
    max_retries=3
)

# Create a transaction
transaction = await verifier.create_transaction(
    type="market_buy",
    symbol="BTC/USDT",
    amount=0.001,
    exchange="nobitex"
)

# Execute the transaction with verification
success = await verifier.execute_transaction(transaction)
```

### Alert System

Provides comprehensive monitoring and notification capabilities.

```python
from trading_system.monitoring.alert_system import AlertSystem, AlertLevel, AlertSource

# Initialize the alert system
alert_system = AlertSystem({
    "channels": {
        "console": True,
        "email": False,
        "telegram": False
    },
    "level_thresholds": {
        "console": "info"
    }
})

# Start the alert system
await alert_system.start()

# Send alerts
await alert_system.send_alert(
    source=AlertSource.EXCHANGE,
    level=AlertLevel.WARNING,
    title="API Rate Limit Approaching",
    message="Exchange API rate limit is at 80% capacity",
    context={"exchange": "nobitex", "rate_limit": 80}
)
```

## Integration Example

Here's how to use multiple components together:

```python
import asyncio
from trading_system.core.circuit_breaker import CircuitBreaker
from trading_system.exchange.connection_pool import ConnectionPool
from trading_system.exchange.enhanced_nobitex_adapter import EnhancedNobitexAdapter
from trading_system.exchange.transaction_verification import TransactionVerificationPipeline
from trading_system.monitoring.alert_system import AlertSystem, AlertLevel, AlertSource

async def main():
    # Initialize components
    alert_system = AlertSystem(config={"channels": {"console": True}})
    await alert_system.start()
    
    connection_pool = ConnectionPool(
        name="nobitex_pool",
        create_client_func=lambda: NobitexClient(),
        config={"min_connections": 2, "max_connections": 5}
    )
    await connection_pool.initialize()
    
    exchange_adapter = EnhancedNobitexAdapter(
        config={
            "api_key": "your_api_key",
            "api_secret": "your_api_secret",
            "connection_pool": connection_pool
        }
    )
    await exchange_adapter.initialize()
    
    verifier = TransactionVerificationPipeline(
        exchange_adapter=exchange_adapter,
        verification_timeout=30
    )
    
    # Create a circuit breaker
    breaker = CircuitBreaker(
        name="trading_operations",
        failure_threshold=3,
        reset_timeout=60.0
    )
    
    # Execute operation with multiple reliability features
    try:
        # Use circuit breaker to protect the operation
        transaction = await breaker.execute(
            verifier.create_transaction,
            type="market_buy",
            symbol="BTC/USDT",
            amount=0.001
        )
        
        # Execute with verification
        success = await verifier.execute_transaction(transaction)
        
        if success:
            await alert_system.send_alert(
                source=AlertSource.EXECUTION,
                level=AlertLevel.INFO,
                title="Transaction Successful",
                message=f"Successfully executed transaction {transaction.id}"
            )
        
    except Exception as e:
        await alert_system.send_alert(
            source=AlertSource.EXECUTION,
            level=AlertLevel.ERROR,
            title="Transaction Failed",
            message=f"Failed to execute transaction: {str(e)}"
        )
    
    # Shutdown components
    await connection_pool.stop()
    await alert_system.stop()

if __name__ == "__main__":
    asyncio.run(main())
```

## Running the Demo

To see all components working together, run the high-reliability demo:

```bash
# Windows
py -3.10 -m trading_system.examples.high_reliability_demo

# Linux/Mac
python3.10 -m trading_system.examples.high_reliability_demo
```

## Testing

Run the high-reliability tests:

```bash
# Windows
py -3.10 -m trading_system.test_high_reliability

# Linux/Mac
python3.10 -m trading_system.test_high_reliability
```

## Documentation

For more detailed information, see the following documentation:

- `HIGH_RELIABILITY_ARCHITECTURE.md` - Comprehensive architecture overview
- `HIGH_RELIABILITY_SUMMARY.md` - Summary of implemented components
- Code-level documentation in each module

## Support

For issues or questions, please contact me. 