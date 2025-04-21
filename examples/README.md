# High Reliability Trading System Examples

This directory contains example scripts that demonstrate key features of the high reliability trading system.

## High Reliability Demo

The `high_reliability_demo.py` script demonstrates multiple high-reliability components working together to provide fault tolerance in a trading system:

- **Circuit Breakers**: Prevent cascading failures by stopping operations when services repeatedly fail
- **Connection Pooling**: Manage multiple connections for redundancy and performance
- **Transaction Verification**: Use a multi-phase commit approach to ensure trading transactions are verified
- **Alert System**: Monitor system health and notify on important events

### How to Run the Demo

```bash
# From the project root directory
python -m trading_system.examples.high_reliability_demo
```

The demo simulates trading operations with a mock exchange that randomly fails (30% failure rate), showing how the high reliability components handle these failures gracefully.

### Sample Output

The output will show various operations, including:

- Connection pool initialization
- Balance checks protected by circuit breakers
- Transaction creation, execution, and verification
- Alert notifications for various events
- Circuit breakers opening when failure thresholds are reached

### Learning from the Demo

Review the code to understand:

1. How circuit breakers prevent cascading failures
2. How connection pools manage multiple connections
3. How transactions are verified using a multi-phase approach
4. How alerts provide visibility into system operations

## Other Examples

- **High Availability Example**: Demonstrates component failover and health monitoring
- **Reliability Example**: Shows basic reliability features in action

Each example is fully documented with comments to explain the concepts being demonstrated. 