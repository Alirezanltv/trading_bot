"""
Position Sizing

This module provides position sizing strategies to determine the appropriate position size
based on risk parameters, account equity, and market conditions.
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import math

logger = logging.getLogger(__name__)


class PositionSizer(ABC):
    """Base class for position sizing strategies."""
    
    @abstractmethod
    def calculate_position_size(self,
                              symbol: str,
                              entry_price: float,
                              stop_loss_price: Optional[float],
                              account_balance: float,
                              **kwargs) -> float:
        """
        Calculate the position size based on the strategy.
        
        Args:
            symbol: Trading symbol
            entry_price: Entry price
            stop_loss_price: Stop loss price (if applicable)
            account_balance: Available account balance
            **kwargs: Additional strategy-specific parameters
            
        Returns:
            Position size in base currency units
        """
        pass


class FixedRiskPositionSizer(PositionSizer):
    """
    Position sizer that risks a fixed percentage of account equity per trade.
    
    This is one of the most common position sizing methods. It ensures that
    each trade risks the same percentage of the account, maintaining consistent
    risk exposure as the account grows or shrinks.
    """
    
    def __init__(self, risk_percentage: float = 0.01, min_position_size: float = 0.0, max_position_size: Optional[float] = None):
        """
        Initialize with risk parameters.
        
        Args:
            risk_percentage: Percentage of account to risk per trade (0.01 = 1%)
            min_position_size: Minimum position size allowed
            max_position_size: Maximum position size allowed (None for no limit)
        """
        self.risk_percentage = risk_percentage
        self.min_position_size = min_position_size
        self.max_position_size = max_position_size
    
    def calculate_position_size(self,
                              symbol: str,
                              entry_price: float,
                              stop_loss_price: Optional[float],
                              account_balance: float,
                              **kwargs) -> float:
        """
        Calculate position size based on fixed percentage risk.
        
        Args:
            symbol: Trading symbol
            entry_price: Entry price
            stop_loss_price: Stop loss price
            account_balance: Available account balance
            **kwargs: Additional parameters (unused)
            
        Returns:
            Position size in base currency units
        """
        # Validate inputs
        if entry_price <= 0:
            logger.warning(f"Invalid entry price for {symbol}: {entry_price}")
            return 0.0
        
        if account_balance <= 0:
            logger.warning("Account balance is zero or negative")
            return 0.0
        
        # Calculate risk amount
        risk_amount = account_balance * self.risk_percentage
        
        # If stop loss is specified, calculate based on risk per unit
        if stop_loss_price and stop_loss_price > 0:
            # Calculate risk per unit
            price_difference = abs(entry_price - stop_loss_price)
            if price_difference <= 0:
                logger.warning(f"Invalid price difference between entry and stop loss for {symbol}")
                return 0.0
            
            # Calculate position size to risk exactly risk_amount
            position_size = risk_amount / price_difference
        else:
            # If no stop loss, use a default risk (2% price movement)
            default_risk_percent = kwargs.get('default_risk_percent', 0.02)
            position_size = risk_amount / (entry_price * default_risk_percent)
        
        # Apply min/max constraints
        if self.min_position_size > 0:
            position_size = max(position_size, self.min_position_size)
        
        if self.max_position_size is not None:
            position_size = min(position_size, self.max_position_size)
        
        # Round to appropriate precision for the asset
        decimals = kwargs.get('decimals', 8)  # Default to 8 decimals for crypto
        position_size = round(position_size, decimals)
        
        logger.debug(f"Fixed risk position size for {symbol}: {position_size} units (entry: {entry_price}, stop: {stop_loss_price})")
        
        return position_size


class FixedSizePositionSizer(PositionSizer):
    """
    Position sizer that uses a fixed position size.
    
    This is a simple strategy that always uses the same position size
    regardless of account balance or market conditions.
    """
    
    def __init__(self, fixed_size: float):
        """
        Initialize with fixed size.
        
        Args:
            fixed_size: Fixed position size to use
        """
        self.fixed_size = fixed_size
    
    def calculate_position_size(self,
                              symbol: str,
                              entry_price: float,
                              stop_loss_price: Optional[float],
                              account_balance: float,
                              **kwargs) -> float:
        """
        Return the fixed position size.
        
        Args:
            symbol: Trading symbol (unused)
            entry_price: Entry price (unused)
            stop_loss_price: Stop loss price (unused)
            account_balance: Available account balance (unused)
            **kwargs: Additional parameters (unused)
            
        Returns:
            Fixed position size
        """
        # Ensure we don't return more than the account can afford
        if entry_price > 0:
            max_affordable = account_balance / entry_price
            actual_size = min(self.fixed_size, max_affordable)
            
            if actual_size < self.fixed_size:
                logger.warning(f"Reduced position size from {self.fixed_size} to {actual_size} due to account balance")
            
            # Round to appropriate precision
            decimals = kwargs.get('decimals', 8)
            actual_size = round(actual_size, decimals)
            
            return actual_size
        
        return 0.0


class PercentageOfEquityPositionSizer(PositionSizer):
    """
    Position sizer that uses a percentage of total equity.
    
    This strategy allocates a fixed percentage of the account
    to each position regardless of the stop loss distance.
    """
    
    def __init__(self, percentage: float = 0.05, min_position_size: float = 0.0, max_position_size: Optional[float] = None):
        """
        Initialize with percentage.
        
        Args:
            percentage: Percentage of equity to allocate (0.05 = 5%)
            min_position_size: Minimum position size allowed
            max_position_size: Maximum position size allowed (None for no limit)
        """
        self.percentage = percentage
        self.min_position_size = min_position_size
        self.max_position_size = max_position_size
    
    def calculate_position_size(self,
                              symbol: str,
                              entry_price: float,
                              stop_loss_price: Optional[float],
                              account_balance: float,
                              **kwargs) -> float:
        """
        Calculate position size based on percentage of equity.
        
        Args:
            symbol: Trading symbol
            entry_price: Entry price
            stop_loss_price: Stop loss price (unused)
            account_balance: Available account balance
            **kwargs: Additional parameters (unused)
            
        Returns:
            Position size in base currency units
        """
        # Validate inputs
        if entry_price <= 0:
            logger.warning(f"Invalid entry price for {symbol}: {entry_price}")
            return 0.0
        
        if account_balance <= 0:
            logger.warning("Account balance is zero or negative")
            return 0.0
        
        # Calculate position value (in quote currency)
        position_value = account_balance * self.percentage
        
        # Convert to base currency units
        position_size = position_value / entry_price
        
        # Apply min/max constraints
        if self.min_position_size > 0:
            position_size = max(position_size, self.min_position_size)
        
        if self.max_position_size is not None:
            position_size = min(position_size, self.max_position_size)
        
        # Round to appropriate precision
        decimals = kwargs.get('decimals', 8)
        position_size = round(position_size, decimals)
        
        logger.debug(f"Percentage of equity position size for {symbol}: {position_size} units (entry: {entry_price})")
        
        return position_size


class KellyPositionSizer(PositionSizer):
    """
    Position sizer based on the Kelly Criterion.
    
    The Kelly Criterion calculates the optimal position size based on
    the win rate and the reward-to-risk ratio of the trading system.
    """
    
    def __init__(self, 
                win_rate: float = 0.5, 
                reward_risk_ratio: float = 2.0,
                kelly_fraction: float = 0.5,  # Only use a fraction of the Kelly bet
                min_position_size: float = 0.0,
                max_position_size: Optional[float] = None):
        """
        Initialize with Kelly parameters.
        
        Args:
            win_rate: Historical win rate (0.0-1.0)
            reward_risk_ratio: Ratio of average win to average loss
            kelly_fraction: Fraction of Kelly to use (usually 0.5 for half-Kelly)
            min_position_size: Minimum position size allowed
            max_position_size: Maximum position size allowed (None for no limit)
        """
        self.win_rate = win_rate
        self.reward_risk_ratio = reward_risk_ratio
        self.kelly_fraction = kelly_fraction
        self.min_position_size = min_position_size
        self.max_position_size = max_position_size
    
    def calculate_position_size(self,
                              symbol: str,
                              entry_price: float,
                              stop_loss_price: Optional[float],
                              account_balance: float,
                              **kwargs) -> float:
        """
        Calculate position size based on Kelly criterion.
        
        Args:
            symbol: Trading symbol
            entry_price: Entry price
            stop_loss_price: Stop loss price
            account_balance: Available account balance
            **kwargs: Additional parameters
                - win_rate: Override default win rate
                - reward_risk_ratio: Override default reward/risk ratio
            
        Returns:
            Position size in base currency units
        """
        # Allow overriding parameters per trade
        win_rate = kwargs.get('win_rate', self.win_rate)
        reward_risk_ratio = kwargs.get('reward_risk_ratio', self.reward_risk_ratio)
        
        # Validate inputs
        if entry_price <= 0:
            logger.warning(f"Invalid entry price for {symbol}: {entry_price}")
            return 0.0
        
        if account_balance <= 0:
            logger.warning("Account balance is zero or negative")
            return 0.0
        
        # Calculate Kelly percentage
        # Kelly formula: f* = (bp - q) / b
        # where f* is the fraction of the account to bet
        # p is the probability of winning, q is the probability of losing (1-p)
        # b is the odds received on the bet (how much you win per unit bet)
        
        # For trading: b = reward/risk ratio
        b = reward_risk_ratio
        p = win_rate
        q = 1.0 - p
        
        # Calculate optimal Kelly fraction
        kelly = (b * p - q) / b
        
        # Apply Kelly fraction to reduce risk
        kelly = kelly * self.kelly_fraction
        
        # Ensure kelly is non-negative
        kelly = max(0.0, kelly)
        
        # Calculate position value in quote currency
        position_value = account_balance * kelly
        
        # If stop loss is specified, calculate risk-adjusted position size
        if stop_loss_price and stop_loss_price > 0:
            # Calculate risk per unit
            price_difference = abs(entry_price - stop_loss_price)
            if price_difference <= 0:
                logger.warning(f"Invalid price difference between entry and stop loss for {symbol}")
                return 0.0
            
            # Risk amount based on Kelly percentage
            risk_amount = account_balance * kelly / reward_risk_ratio
            
            # Position size based on risk amount and stop distance
            position_size = risk_amount / price_difference
        else:
            # If no stop loss, use position value / entry price
            position_size = position_value / entry_price
        
        # Apply min/max constraints
        if self.min_position_size > 0:
            position_size = max(position_size, self.min_position_size)
        
        if self.max_position_size is not None:
            position_size = min(position_size, self.max_position_size)
        
        # Round to appropriate precision
        decimals = kwargs.get('decimals', 8)
        position_size = round(position_size, decimals)
        
        logger.debug(f"Kelly position size for {symbol}: {position_size} units (kelly: {kelly:.2%})")
        
        return position_size


class VolatilityPositionSizer(PositionSizer):
    """
    Position sizer that adjusts position size based on market volatility.
    
    This strategy reduces position size in highly volatile markets
    and increases it in low volatility conditions.
    """
    
    def __init__(self, 
                base_risk_percentage: float = 0.01,
                volatility_lookback: int = 20,
                volatility_scale_factor: float = 1.0,
                min_position_size: float = 0.0,
                max_position_size: Optional[float] = None):
        """
        Initialize with volatility parameters.
        
        Args:
            base_risk_percentage: Base percentage of account to risk (0.01 = 1%)
            volatility_lookback: Number of periods to calculate volatility
            volatility_scale_factor: Scaling factor for volatility adjustment
            min_position_size: Minimum position size allowed
            max_position_size: Maximum position size allowed (None for no limit)
        """
        self.base_risk_percentage = base_risk_percentage
        self.volatility_lookback = volatility_lookback
        self.volatility_scale_factor = volatility_scale_factor
        self.min_position_size = min_position_size
        self.max_position_size = max_position_size
        
        # Store historical volatility values
        self.historical_volatility = {}
    
    def calculate_position_size(self,
                              symbol: str,
                              entry_price: float,
                              stop_loss_price: Optional[float],
                              account_balance: float,
                              **kwargs) -> float:
        """
        Calculate position size based on market volatility.
        
        Args:
            symbol: Trading symbol
            entry_price: Entry price
            stop_loss_price: Stop loss price
            account_balance: Available account balance
            **kwargs: Additional parameters
                - current_volatility: Current volatility value
                - average_volatility: Average volatility for normalization
                - price_history: List of historical prices for volatility calculation
            
        Returns:
            Position size in base currency units
        """
        # Get volatility information
        current_volatility = kwargs.get('current_volatility')
        average_volatility = kwargs.get('average_volatility')
        price_history = kwargs.get('price_history')
        
        # If volatility not provided, calculate from price history if available
        if current_volatility is None and price_history is not None and len(price_history) > self.volatility_lookback:
            # Simple std dev calculation for volatility
            recent_prices = price_history[-self.volatility_lookback:]
            returns = [recent_prices[i] / recent_prices[i-1] - 1 for i in range(1, len(recent_prices))]
            current_volatility = math.sqrt(sum(r*r for r in returns) / len(returns))
            
            # Store for future reference
            self.historical_volatility[symbol] = current_volatility
        elif current_volatility is None and symbol in self.historical_volatility:
            # Use stored volatility if available
            current_volatility = self.historical_volatility.get(symbol)
        elif current_volatility is None:
            # Default to neutral volatility
            current_volatility = 0.02  # 2% daily volatility as default
        
        # If average volatility not provided, use current or default
        if average_volatility is None:
            average_volatility = current_volatility
        
        # Normalize volatility (1.0 = average)
        normalized_volatility = current_volatility / average_volatility if average_volatility > 0 else 1.0
        
        # Adjust risk percentage based on volatility
        # Lower volatility = higher position size, higher volatility = lower position size
        volatility_factor = 1.0 / (normalized_volatility ** self.volatility_scale_factor)
        adjusted_risk = self.base_risk_percentage * volatility_factor
        
        # Calculate risk amount
        risk_amount = account_balance * adjusted_risk
        
        # If stop loss is specified, calculate based on risk per unit
        if stop_loss_price and stop_loss_price > 0:
            # Calculate risk per unit
            price_difference = abs(entry_price - stop_loss_price)
            if price_difference <= 0:
                logger.warning(f"Invalid price difference between entry and stop loss for {symbol}")
                return 0.0
            
            # Calculate position size to risk exactly risk_amount
            position_size = risk_amount / price_difference
        else:
            # If no stop loss, use a default risk (based on volatility)
            default_risk = current_volatility * 2  # Default to 2x daily volatility
            position_size = risk_amount / (entry_price * default_risk)
        
        # Apply min/max constraints
        if self.min_position_size > 0:
            position_size = max(position_size, self.min_position_size)
        
        if self.max_position_size is not None:
            position_size = min(position_size, self.max_position_size)
        
        # Round to appropriate precision
        decimals = kwargs.get('decimals', 8)
        position_size = round(position_size, decimals)
        
        logger.debug(f"Volatility-adjusted position size for {symbol}: {position_size} units "
                     f"(volatility: {current_volatility:.2%}, adjustment: {volatility_factor:.2f})")
        
        return position_size


class PositionSizingFactory:
    """Factory for creating position sizers."""
    
    @staticmethod
    def create_position_sizer(sizing_type: str, config: Dict[str, Any]) -> PositionSizer:
        """
        Create a position sizer based on type and configuration.
        
        Args:
            sizing_type: Type of position sizer to create
            config: Configuration parameters
            
        Returns:
            PositionSizer instance
        """
        if sizing_type == "fixed_risk":
            return FixedRiskPositionSizer(
                risk_percentage=config.get('risk_percentage', 0.01),
                min_position_size=config.get('min_position_size', 0.0),
                max_position_size=config.get('max_position_size')
            )
        elif sizing_type == "fixed_size":
            return FixedSizePositionSizer(
                fixed_size=config.get('fixed_size', 0.01)
            )
        elif sizing_type == "percentage_of_equity":
            return PercentageOfEquityPositionSizer(
                percentage=config.get('percentage', 0.05),
                min_position_size=config.get('min_position_size', 0.0),
                max_position_size=config.get('max_position_size')
            )
        elif sizing_type == "kelly":
            return KellyPositionSizer(
                win_rate=config.get('win_rate', 0.5),
                reward_risk_ratio=config.get('reward_risk_ratio', 2.0),
                kelly_fraction=config.get('kelly_fraction', 0.5),
                min_position_size=config.get('min_position_size', 0.0),
                max_position_size=config.get('max_position_size')
            )
        elif sizing_type == "volatility":
            return VolatilityPositionSizer(
                base_risk_percentage=config.get('base_risk_percentage', 0.01),
                volatility_lookback=config.get('volatility_lookback', 20),
                volatility_scale_factor=config.get('volatility_scale_factor', 1.0),
                min_position_size=config.get('min_position_size', 0.0),
                max_position_size=config.get('max_position_size')
            )
        else:
            logger.warning(f"Unknown position sizing type: {sizing_type}, using default fixed risk")
            return FixedRiskPositionSizer(
                risk_percentage=config.get('risk_percentage', 0.01)
            ) 