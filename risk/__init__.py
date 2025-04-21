"""
Risk management module for high-reliability trading system.

This module provides risk management capabilities for the trading system, including:
- Position sizing
- Stop loss management
- Risk limits enforcement
- Drawdown protection
- Portfolio risk controls
"""

from trading_system.risk.risk_manager import RiskManager

__all__ = [
    'RiskManager',
] 