# High-Reliability Trading System Components

This package provides high-reliability components for the trading system, ensuring fault tolerance, resilience, and robustness.

## Installation

Ensure you have Python 3.7 or higher installed. Then install the required dependencies:

```bash
# Using pip (Windows)
py -3.10 -m pip install -r requirements.txt

# Using pip (Linux/Mac)
python3.10 -m pip install -r requirements.txt
```

## Key Components

The high-reliability system includes:

1. **Circuit Breaker** - Prevents cascading failures by detecting when services are unavailable and failing fast until recovery.
2. **Connection Pool** - Manages multiple connections to external services with automatic reconnection and health monitoring.
3. **Message Bus** - Provides reliable inter-component communication.
4. **Transaction Verification** - Ensures transaction integrity using a three-phase commit protocol.
5. **Shadow Accounting** - Maintains high-integrity position tracking with reconciliation.
6. **Alert System** - Provides comprehensive monitoring and notifications.

## Running Examples

### Quick Start Example

This example demonstrates the core high-reliability features:

```bash
# Windows
py -3.10 -m trading_system.examples.reliability_quick_start

# Linux/Mac
python3.10 -m trading_system.examples.reliability_quick_start
```

### High Reliability Demo

A more comprehensive demonstration of all features:

```bash
# Windows
py -3.10 -m trading_system.examples.high_reliability_demo

# Linux/Mac
python3.10 -m trading_system.examples.high_reliability_demo
```

### Verifying Installation

To verify that all components are installed correctly:

```bash
# Windows
py -3.10 -m trading_system.verify_installation

# Linux/Mac
python3.10 -m trading_system.verify_installation
```

## Testing

Run the test suite to validate the high-reliability components:

```bash
# Windows
py -3.10 -m trading_system.test_high_reliability

# Linux/Mac
python3.10 -m trading_system.test_high_reliability
```

## Individual Component Tests

Test individual components:

### Circuit Breaker

```bash
py -3.10 -m trading_system.test_circuit_breaker
```

## Configuration

The high-reliability components can be configured via environment variables or configuration files:

### Environment Variables

- `TRADING_SYSTEM_MESSAGE_BUS_MODE` - Message bus mode (`in_memory` or `rabbitmq`)
- `TRADING_SYSTEM_LOG_LEVEL` - Logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`)

### Configuration Files

Configuration files are stored in the `trading_system/config` directory:

- `high_reliability_config.yaml` - Main configuration file for high-reliability components

## Architecture

For detailed information about the high-reliability architecture, see:

- [HIGH_RELIABILITY_ARCHITECTURE.md](./HIGH_RELIABILITY_ARCHITECTURE.md) - Comprehensive architecture overview
- [HIGH_RELIABILITY_SUMMARY.md](./HIGH_RELIABILITY_SUMMARY.md) - Summary of implemented components

## Usage in Your Code

### Circuit Breaker

```python
from trading_system.core.circuit_breaker import CircuitBreaker

# Create a circuit breaker
breaker = CircuitBreaker(
    name="my_service",
    failure_threshold=5,    # Open after 5 failures
    reset_timeout=30.0,     # Try again after 30 seconds
    half_open_max_requests=2,  # Allow 2 test requests
    success_threshold=2     # Close after 2 successes
)

# Use the circuit breaker
async def get_data():
    try:
        result = await breaker.execute(my_api_client.fetch_data)
        return result
    except Exception as e:
        # Handle the exception
        return None
```

### Connection Pool

```python
from trading_system.exchange.connection_pool import ConnectionPool

# Create a connection pool
pool = ConnectionPool(
    name="api_pool",
    create_client_func=lambda: MyApiClient(),
    config={
        "min_connections": 2,
        "max_connections": 10,
        "connection_timeout": 30,
        "idle_timeout": 300
    }
)

# Initialize the pool
await pool.initialize()

# Execute an operation
result = await pool.execute("get_market_data", "BTC/USDT")

# Shutdown the pool when done
await pool.stop()
```

### Message Bus

```python
from trading_system.core import message_bus

# Initialize the message bus
message_bus.initialize(in_memory=True)

# Subscribe to messages
def handle_market_data(data):
    print(f"Received market data: {data}")

message_bus.subscribe("market_data", handle_market_data)

# Publish a message
message_bus.publish("market_data", {
    "symbol": "BTC/USDT",
    "price": 50000,
    "timestamp": time.time()
})
```

## Troubleshooting

If you encounter issues:

1. Check that all dependencies are installed correctly
2. Verify that Python 3.7+ is being used
3. Look at the log files in the `logs` directory
4. Run the verification script to check component functionality

## Support

For questions or issues, check the documentation or open an issue on the repository. 