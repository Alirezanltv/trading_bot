"""
This package contains trading strategy implementations and related components.
"""

from .factory import StrategyFactory, Strategy, SimpleMACD

__all__ = [
    'StrategyFactory',
    'Strategy',
    'SimpleMACD'
] 