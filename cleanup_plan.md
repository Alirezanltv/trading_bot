# Trading System Cleanup Plan

## Identified Redundancies

### TradingView Implementation Duplication
1. **Redundant Files:**
   - `market_data/tradingview.py`
   - `market_data/tradingview_realtime.py` 
   - `market_data/tradingview_simple.py`
   - `monitoring/tradingview_fetcher.py`
   - `monitoring/tradingview_monitor.py`

   **Action:** Consolidate into a single, robust TradingView integration in `market_data/tradingview.py` with our enhanced fallback mechanisms

### Nobitex API Duplication
1. **Redundant Files:**
   - `exchange/nobitex.py`
   - `exchanges/nobitex.py`
   - `nobitex_client.py`
   - `monitoring/account_monitor.py` (partial functionality)

   **Action:** Consolidate into a single implementation in `exchange/nobitex.py`

### Test Files Scattered Across Codebase
1. **Redundant/Scattered Test Files:**
   - `test_*.py` files in root directory
   - `*_test.py` files in root directory
   - Example scripts in `examples/` directory that serve as tests

   **Action:** Create a proper `tests/` directory with organized test files

### Logging Implementation Duplication
1. **Redundant Files:**
   - `core/logging.py`
   - `core/logging_setup.py`

   **Action:** Consolidate into a single logging module

## Directory Structure Issues

1. **Duplicate Directories:**
   - `exchange/` vs `exchanges/`
   - `adapters/` with minimal content

   **Action:** Consolidate into single `exchange/` directory, move adapter functionality if needed

2. **Monitoring Structure:**
   - `monitoring/` contains a mix of market data and account monitoring

   **Action:** Reorganize: move relevant market data to `market_data/`, keep account monitoring in `monitoring/`

## Cleanup Steps

### 1. Consolidate TradingView Implementation
- Keep the enhanced `tradingview_fetcher.py` functionality
- Migrate it to `market_data/tradingview.py`
- Remove redundant implementations 
- Update imports across codebase

### 2. Consolidate Nobitex Implementation
- Consolidate `exchange/nobitex.py` and `nobitex_client.py`
- Remove `exchanges/nobitex.py`
- Update imports across codebase

### 3. Organize Test Files
- Create `tests/` directory
- Move all test files there with consistent naming
- Separate unit tests from integration tests

### 4. Consolidate Logging
- Merge `core/logging.py` and `core/logging_setup.py`
- Ensure consistent usage across codebase

### 5. Streamline Directory Structure
- Remove `exchanges/` directory after consolidation
- Evaluate and potentially remove `adapters/` if functionality is minimal
- Ensure proper imports after restructuring

### 6. Cleaning Test/Debug Files
- Remove temporary test files and logs
- Organize remaining logs in a structured way

## Final Structure

```
trading_system/
├── core/                # Core system components
│   ├── component.py
│   ├── config.py
│   ├── database.py
│   ├── logging.py      # Consolidated logging
│   └── message_bus.py
├── exchange/            # All exchange adapters
│   ├── adapter.py
│   ├── base.py
│   ├── nobitex.py      # Consolidated Nobitex adapter
│   └── simulated.py
├── market_data/         # Market data providers
│   ├── manager.py
│   ├── tradingview.py  # Consolidated TradingView implementation
│   └── websocket_client.py
├── monitoring/          # Monitoring and visualization tools
│   ├── account_monitor.py
│   └── alert_manager.py 
├── strategy/            # Trading strategies
├── execution/           # Execution components
├── risk/                # Risk management
├── position/            # Position management
├── tests/               # Organized tests
│   ├── unit/           # Unit tests
│   └── integration/    # Integration tests
├── examples/            # Example usage only (not tests)
└── __init__.py
```

## Implementation Timeline

1. **Phase 1:** Consolidate TradingView implementation
2. **Phase 2:** Organize test files
3. **Phase 3:** Consolidate Nobitex implementation
4. **Phase 4:** Streamline directory structure
5. **Phase 5:** Consolidate logging
6. **Phase 6:** Final cleanup and verification 