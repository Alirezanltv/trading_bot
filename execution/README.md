# Execution Engine Subsystem

The Execution Engine is responsible for reliably executing trades on cryptocurrency exchanges, implementing robust error handling, retry mechanisms, and maintaining detailed transaction logs for auditing and debugging.

## Features

- **Reliable Order Execution**: Implements a three-phase commit model for order processing:
  - **Pre-Commit**: Validates signals and checks available funds
  - **Commit**: Sends orders to the exchange
  - **Post-Commit**: Confirms execution and updates system state

- **Error Handling**:
  - Retry mechanism with exponential backoff
  - Circuit breaker pattern to prevent cascading failures
  - Detailed error logging and reporting

- **Transaction Logging**:
  - Comprehensive audit trail of all order lifecycle events
  - JSON-based structured logging for easy analysis
  - Separate logs for orders, fills, and errors

- **Order Status Tracking**:
  - Background monitoring of active orders
  - Detection of partial fills and order state changes
  - Automated status reconciliation

- **Secure API Key Management**:
  - Environment-based credential loading
  - Obfuscation for logging purposes
  - API key validation

## Components

### 1. NobitexExecutionEngine

Production-grade execution engine for the Nobitex exchange, implementing:
- Three-phase commit workflow
- Robust error handling
- Active order tracking
- Circuit breaker protection

### 2. Transaction Logger

Detailed logging system for all transaction-related events:
- Order creation, updates, and fills
- Execution phases
- Errors and exceptions
- Retry attempts

### 3. API Key Manager

Secure handling of exchange API credentials:
- Loading from environment variables
- Validation and verification
- Safe logging practices

## Usage Example

```python
from trading_system.execution.nobitex_engine import NobitexExecutionEngine
from trading_system.execution.orders import Order, OrderSide, OrderType, OrderTimeInForce

# Create configuration
config = {
    "execution_engine": {
        "exchange_adapter": "nobitex",
        "max_retries": 3,
        "retry_delay": 2.0,
        "circuit_breaker_enabled": True
    },
    "exchange": {
        "api_key": os.getenv("NOBITEX_API_KEY"),
        "api_secret": os.getenv("NOBITEX_API_SECRET")
    }
}

# Initialize execution engine
engine = NobitexExecutionEngine(config)
engine.start()

# Create a market buy order
order = Order(
    order_id=str(uuid.uuid4()),
    symbol="btc-usdt",
    side=OrderSide.BUY,
    order_type=OrderType.MARKET,
    quantity=0.001,
    price=None,  # Market order
    time_in_force=OrderTimeInForce.IOC,
    timestamp=int(time.time() * 1000)
)

# Execute order
result = engine.execute_order(order)

# Check result
if result.success:
    print(f"Order executed successfully: {result.exchange_order_id}")
else:
    print(f"Order execution failed: {result.error_message} (phase: {result.phase.value})")
```

## Configuration Options

The execution engine accepts the following configuration parameters:

```python
{
    "execution_engine": {
        # Exchange adapter to use
        "exchange_adapter": "nobitex",  # or "mock" for testing
        
        # Retry settings
        "max_retries": 3,
        "retry_delay": 2.0,  # Initial delay before retrying (seconds)
        
        # Order tracking
        "order_update_interval": 5.0,  # Interval for checking order status (seconds)
        
        # Transaction logging
        "transaction_log_path": "data/transactions",
        
        # Circuit breaker
        "circuit_breaker_enabled": True,
        "failure_threshold": 3,  # Number of consecutive failures before opening
        "circuit_open_timeout": 300  # Time in seconds to keep circuit open
    },
    "exchange": {
        # Exchange-specific settings
        "api_key": "your_api_key",
        "api_secret": "your_api_secret",
        "timeout": 15,  # API request timeout (seconds)
        "max_retries": 3  # API request retries
    }
}
```

## Transaction Logging

The execution engine maintains detailed logs of all transactions in JSON format:

- **Orders Log**: Records all order lifecycle events (creation, updates, submissions)
- **Fills Log**: Records all order fills (full and partial)
- **Errors Log**: Records all errors and exceptions

Logs are stored in the `data/transactions` directory with daily rotation.

## Integration with Other Subsystems

The execution engine integrates with:

1. **Position Management**: Updates position status based on order execution
2. **Risk Management**: Respects risk limits and position sizing rules
3. **Monitoring**: Provides execution metrics and performance data
4. **Exchange Adapter**: Interfaces with specific exchange APIs

## Example Script

See `trading_system/execution/examples/nobitex_execution_example.py` for a complete
example of using the NobitexExecutionEngine to place orders on the Nobitex exchange. 