"""
Multi-Level Stop-Loss System

This module implements a comprehensive multi-level stop-loss system with 
multiple tiers of protection for risk management.
"""

import logging
import time
from typing import Dict, List, Any, Optional, Union, Callable, Set, Tuple
from datetime import datetime, timedelta
from enum import Enum

from trading_system.core.logging import get_logger
from trading_system.core.component import Component, ComponentStatus
from trading_system.core.message_bus import MessageTypes

logger = get_logger("risk.multi_level_stoploss")

class StopLossType(Enum):
    """Types of stop-loss mechanism."""
    FIXED = "fixed"           # Fixed price stop-loss
    TRAILING = "trailing"     # Trailing stop with percentage distance
    TIME = "time"             # Time-based stop-loss
    VOLATILITY = "volatility" # Volatility-based stop
    MARKET = "market"         # Market conditions stop
    EMERGENCY = "emergency"   # Emergency/hardware stop


class StopLossTier(Enum):
    """Tiers for the multi-level stop-loss system."""
    PRIMARY = "primary"       # First level stop-loss (normal market conditions)
    SECONDARY = "secondary"   # Second level (backup if first fails)
    TERTIARY = "tertiary"     # Third level (additional protection)
    EMERGENCY = "emergency"   # Emergency/hardware level (last resort)


class StopLossAction(Enum):
    """Actions to take when stop-loss is triggered."""
    CLOSE_POSITION = "close"             # Close the specific position
    REDUCE_POSITION = "reduce"           # Reduce position size
    HEDGE_POSITION = "hedge"             # Create a hedge
    CLOSE_ALL_POSITIONS = "close_all"    # Close all positions
    SUSPEND_TRADING = "suspend"          # Suspend all trading
    EMERGENCY_SHUTDOWN = "shutdown"      # Shut down the system


class StopLossTrigger:
    """
    Stop-Loss Trigger definition.
    
    Defines when and how a stop-loss is triggered, along with the required action.
    """
    
    def __init__(self, 
                 stop_type: StopLossType,
                 tier: StopLossTier,
                 action: StopLossAction,
                 trigger_value: float,
                 trigger_percentage: Optional[float] = None,
                 description: str = "",
                 symbol: Optional[str] = None,
                 position_id: Optional[str] = None):
        """
        Initialize a stop-loss trigger.
        
        Args:
            stop_type: Type of stop-loss
            tier: Tier level
            action: Action to take when triggered
            trigger_value: Price or value to trigger (depends on stop_type)
            trigger_percentage: Percentage for trailing stops
            description: Human-readable description
            symbol: Trading symbol (if applicable)
            position_id: Position ID (if applicable)
        """
        self.stop_type = stop_type
        self.tier = tier
        self.action = action
        self.trigger_value = trigger_value
        self.trigger_percentage = trigger_percentage
        self.description = description
        self.symbol = symbol
        self.position_id = position_id
        
        # Tracking fields
        self.created_at = datetime.now().isoformat()
        self.updated_at = self.created_at
        self.triggered = False
        self.triggered_at = None
        self.triggered_price = None
        self.id = f"{stop_type.value}_{tier.value}_{int(time.time() * 1000)}"
        
        # For trailing stops
        self.highest_price = trigger_value if stop_type == StopLossType.TRAILING else None
        self.lowest_price = trigger_value if stop_type == StopLossType.TRAILING else None
    
    def update(self, current_price: float) -> bool:
        """
        Update the stop-loss with current price information.
        
        Args:
            current_price: Current price to check against
            
        Returns:
            bool: True if triggered, False otherwise
        """
        if self.triggered:
            return True
            
        triggered = False
        
        # Update timestamp
        self.updated_at = datetime.now().isoformat()
        
        # Check based on stop type
        if self.stop_type == StopLossType.FIXED:
            # Fixed price stop-loss
            if self.position_direction() == "long":
                triggered = current_price <= self.trigger_value
            else:
                triggered = current_price >= self.trigger_value
                
        elif self.stop_type == StopLossType.TRAILING:
            # Update highest/lowest price
            if self.position_direction() == "long":
                self.highest_price = max(self.highest_price or 0, current_price)
                stop_price = self.highest_price * (1 - (self.trigger_percentage or 0) / 100)
                triggered = current_price <= stop_price
            else:
                self.lowest_price = min(self.lowest_price or float('inf'), current_price)
                stop_price = self.lowest_price * (1 + (self.trigger_percentage or 0) / 100)
                triggered = current_price >= stop_price
        
        # Update triggered status
        if triggered:
            self.triggered = True
            self.triggered_at = datetime.now().isoformat()
            self.triggered_price = current_price
            
        return triggered
    
    def position_direction(self) -> str:
        """
        Get position direction (long/short).
        
        Returns:
            str: "long" or "short"
        """
        # In a real implementation, this would be determined from the position data
        # For now, we assume long if trigger_value is below initial price (for fixed stops)
        return "long"
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for serialization.
        
        Returns:
            Dict: Serializable representation
        """
        return {
            "id": self.id,
            "stop_type": self.stop_type.value,
            "tier": self.tier.value,
            "action": self.action.value,
            "trigger_value": self.trigger_value,
            "trigger_percentage": self.trigger_percentage,
            "description": self.description,
            "symbol": self.symbol,
            "position_id": self.position_id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "triggered": self.triggered,
            "triggered_at": self.triggered_at,
            "triggered_price": self.triggered_price,
            "highest_price": self.highest_price,
            "lowest_price": self.lowest_price
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'StopLossTrigger':
        """
        Create from dictionary.
        
        Args:
            data: Dictionary representation
            
        Returns:
            StopLossTrigger: Instance
        """
        instance = cls(
            stop_type=StopLossType(data.get("stop_type", "fixed")),
            tier=StopLossTier(data.get("tier", "primary")),
            action=StopLossAction(data.get("action", "close")),
            trigger_value=data.get("trigger_value", 0.0),
            trigger_percentage=data.get("trigger_percentage"),
            description=data.get("description", ""),
            symbol=data.get("symbol"),
            position_id=data.get("position_id")
        )
        
        # Restore tracking fields
        instance.id = data.get("id", instance.id)
        instance.created_at = data.get("created_at", instance.created_at)
        instance.updated_at = data.get("updated_at", instance.updated_at)
        instance.triggered = data.get("triggered", False)
        instance.triggered_at = data.get("triggered_at")
        instance.triggered_price = data.get("triggered_price")
        instance.highest_price = data.get("highest_price")
        instance.lowest_price = data.get("lowest_price")
        
        return instance


class MultiLevelStopLoss(Component):
    """
    Multi-Level Stop-Loss System
    
    Manages multiple tiers of stop-loss triggers with different actions and
    protection mechanisms, providing layered risk management.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the multi-level stop-loss system.
        
        Args:
            config: Configuration dictionary
        """
        super().__init__(name="multi_level_stoploss", config=config)
        
        # Stop-loss registry
        self.stop_losses: Dict[str, StopLossTrigger] = {}  # stop_id -> StopLossTrigger
        self.position_stops: Dict[str, List[str]] = {}  # position_id -> [stop_ids]
        self.symbol_stops: Dict[str, List[str]] = {}  # symbol -> [stop_ids]
        self.emergency_stops: List[str] = []  # stop_ids for emergency stops
        
        # Configuration
        self.check_interval = self.config.get("check_interval", 1.0)  # seconds
        self.price_buffer = self.config.get("price_buffer", 0.1)  # percentage
        self.default_trailing_percentage = self.config.get("default_trailing_percentage", 2.0)
        self.verification_required = self.config.get("verification_required", True)
        
        # Default config for each stop tier
        self.tier_config = self.config.get("tier_config", {
            StopLossTier.PRIMARY.value: {
                "enabled": True,
                "default_type": StopLossType.TRAILING.value,
                "default_action": StopLossAction.CLOSE_POSITION.value,
                "default_percentage": 2.0
            },
            StopLossTier.SECONDARY.value: {
                "enabled": True,
                "default_type": StopLossType.TRAILING.value,
                "default_action": StopLossAction.CLOSE_POSITION.value,
                "default_percentage": 3.0
            },
            StopLossTier.TERTIARY.value: {
                "enabled": True,
                "default_type": StopLossType.FIXED.value,
                "default_action": StopLossAction.CLOSE_POSITION.value,
                "default_percentage": 5.0
            },
            StopLossTier.EMERGENCY.value: {
                "enabled": True,
                "default_type": StopLossType.FIXED.value,
                "default_action": StopLossAction.CLOSE_ALL_POSITIONS.value,
                "default_percentage": 10.0
            }
        })
        
        # Task for checking stops
        self.check_task = None
        
        # State tracking
        self.running = False
        self.last_price_update = {}  # symbol -> {price, timestamp}
        self.triggered_stops = []  # List of recently triggered stops
        
        # Emergency shutdown flag
        self.emergency_shutdown_active = False
        
        # Message bus reference
        self.message_bus = None
        
        # Callbacks
        self.on_stop_triggered: Dict[StopLossTier, List[Callable]] = {
            tier: [] for tier in StopLossTier
        }
        
        self._update_status(ComponentStatus.INITIALIZED)
        logger.info("Multi-level stop-loss system initialized")
    
    async def start(self) -> bool:
        """
        Start the stop-loss monitoring system.
        
        Returns:
            bool: True if started successfully, False otherwise
        """
        try:
            if self.running:
                logger.warning("Stop-loss system already running")
                return True
                
            # Update status
            self._update_status(ComponentStatus.INITIALIZING)
            
            # Start check task
            self.running = True
            self.check_task = asyncio.create_task(self._check_stops_loop())
            
            # Update status
            self._update_status(ComponentStatus.OPERATIONAL)
            logger.info("Multi-level stop-loss system started")
            return True
            
        except Exception as e:
            self._update_status(ComponentStatus.ERROR, f"Error starting: {str(e)}")
            logger.error(f"Error starting multi-level stop-loss system: {str(e)}", exc_info=True)
            return False
    
    async def stop(self) -> bool:
        """
        Stop the stop-loss monitoring system.
        
        Returns:
            bool: True if stopped successfully, False otherwise
        """
        try:
            if not self.running:
                return True
                
            # Update status
            self._update_status(ComponentStatus.STOPPING)
            
            # Stop running flag
            self.running = False
            
            # Cancel check task
            if self.check_task:
                self.check_task.cancel()
                try:
                    await self.check_task
                except asyncio.CancelledError:
                    pass
                
            # Update status
            self._update_status(ComponentStatus.STOPPED)
            logger.info("Multi-level stop-loss system stopped")
            return True
            
        except Exception as e:
            self._update_status(ComponentStatus.ERROR, f"Error stopping: {str(e)}")
            logger.error(f"Error stopping multi-level stop-loss system: {str(e)}", exc_info=True)
            return False
    
    def set_message_bus(self, message_bus) -> None:
        """
        Set the message bus for event communication.
        
        Args:
            message_bus: Message bus instance
        """
        self.message_bus = message_bus
    
    def add_stop_loss(self, stop_loss: StopLossTrigger) -> str:
        """
        Add a stop-loss trigger to the system.
        
        Args:
            stop_loss: Stop-loss trigger to add
            
        Returns:
            str: Stop-loss ID
        """
        # Add to registry
        self.stop_losses[stop_loss.id] = stop_loss
        
        # Add to position index
        if stop_loss.position_id:
            if stop_loss.position_id not in self.position_stops:
                self.position_stops[stop_loss.position_id] = []
            self.position_stops[stop_loss.position_id].append(stop_loss.id)
        
        # Add to symbol index
        if stop_loss.symbol:
            if stop_loss.symbol not in self.symbol_stops:
                self.symbol_stops[stop_loss.symbol] = []
            self.symbol_stops[stop_loss.symbol].append(stop_loss.id)
        
        # Add to emergency index
        if stop_loss.tier == StopLossTier.EMERGENCY:
            self.emergency_stops.append(stop_loss.id)
            
        logger.info(f"Added {stop_loss.tier.value} {stop_loss.stop_type.value} stop-loss for " + 
                   f"{stop_loss.symbol or stop_loss.position_id or 'system'}")
        return stop_loss.id
    
    def remove_stop_loss(self, stop_id: str) -> bool:
        """
        Remove a stop-loss trigger.
        
        Args:
            stop_id: Stop-loss ID to remove
            
        Returns:
            bool: True if removed, False if not found
        """
        if stop_id not in self.stop_losses:
            return False
            
        # Get stop loss
        stop_loss = self.stop_losses[stop_id]
        
        # Remove from position index
        if stop_loss.position_id and stop_loss.position_id in self.position_stops:
            if stop_id in self.position_stops[stop_loss.position_id]:
                self.position_stops[stop_loss.position_id].remove(stop_id)
                
        # Remove from symbol index
        if stop_loss.symbol and stop_loss.symbol in self.symbol_stops:
            if stop_id in self.symbol_stops[stop_loss.symbol]:
                self.symbol_stops[stop_loss.symbol].remove(stop_id)
                
        # Remove from emergency index
        if stop_id in self.emergency_stops:
            self.emergency_stops.remove(stop_id)
            
        # Remove from registry
        del self.stop_losses[stop_id]
        
        logger.info(f"Removed stop-loss {stop_id}")
        return True
    
    def get_stop_loss(self, stop_id: str) -> Optional[StopLossTrigger]:
        """
        Get a stop-loss trigger by ID.
        
        Args:
            stop_id: Stop-loss ID
            
        Returns:
            StopLossTrigger or None if not found
        """
        return self.stop_losses.get(stop_id)
    
    def get_position_stops(self, position_id: str) -> List[StopLossTrigger]:
        """
        Get all stop-loss triggers for a position.
        
        Args:
            position_id: Position ID
            
        Returns:
            List of StopLossTrigger objects
        """
        if position_id not in self.position_stops:
            return []
            
        return [self.stop_losses[stop_id] for stop_id in self.position_stops[position_id]
                if stop_id in self.stop_losses]
    
    def get_symbol_stops(self, symbol: str) -> List[StopLossTrigger]:
        """
        Get all stop-loss triggers for a symbol.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            List of StopLossTrigger objects
        """
        if symbol not in self.symbol_stops:
            return []
            
        return [self.stop_losses[stop_id] for stop_id in self.symbol_stops[symbol]
                if stop_id in self.stop_losses]
    
    def update_price(self, symbol: str, price: float) -> List[StopLossTrigger]:
        """
        Update price information and check for triggered stops.
        
        Args:
            symbol: Trading symbol
            price: Current price
            
        Returns:
            List of triggered StopLossTrigger objects
        """
        # Update price cache
        self.last_price_update[symbol] = {
            "price": price,
            "timestamp": time.time()
        }
        
        # Check for symbol stops
        triggered = []
        if symbol in self.symbol_stops:
            for stop_id in self.symbol_stops[symbol]:
                if stop_id in self.stop_losses:
                    stop = self.stop_losses[stop_id]
                    
                    # Skip already triggered stops
                    if stop.triggered:
                        continue
                    
                    # Update stop and check if triggered
                    if stop.update(price):
                        triggered.append(stop)
                        
        return triggered
    
    def create_stop_loss_set(self, position_id: str, symbol: str, entry_price: float, 
                           direction: str = "long", risk_percentage: float = 2.0) -> List[str]:
        """
        Create a complete set of multi-level stop-losses for a position.
        
        Args:
            position_id: Position ID
            symbol: Trading symbol
            entry_price: Entry price
            direction: Position direction ("long" or "short")
            risk_percentage: Risk percentage for primary stop
            
        Returns:
            List of created stop-loss IDs
        """
        stop_ids = []
        
        # Factor for converting between long/short
        direction_factor = -1 if direction == "long" else 1
        
        # Create primary stop (trailing)
        primary_config = self.tier_config.get(StopLossTier.PRIMARY.value, {})
        if primary_config.get("enabled", True):
            primary_stop = StopLossTrigger(
                stop_type=StopLossType.TRAILING,
                tier=StopLossTier.PRIMARY,
                action=StopLossAction(primary_config.get("default_action", StopLossAction.CLOSE_POSITION.value)),
                trigger_value=entry_price,
                trigger_percentage=risk_percentage,
                description=f"Primary trailing stop ({risk_percentage}%)",
                symbol=symbol,
                position_id=position_id
            )
            stop_ids.append(self.add_stop_loss(primary_stop))
        
        # Create secondary stop (trailing with wider margin)
        secondary_config = self.tier_config.get(StopLossTier.SECONDARY.value, {})
        if secondary_config.get("enabled", True):
            secondary_percentage = risk_percentage * 1.5
            secondary_stop = StopLossTrigger(
                stop_type=StopLossType.TRAILING,
                tier=StopLossTier.SECONDARY,
                action=StopLossAction(secondary_config.get("default_action", StopLossAction.CLOSE_POSITION.value)),
                trigger_value=entry_price,
                trigger_percentage=secondary_percentage,
                description=f"Secondary trailing stop ({secondary_percentage}%)",
                symbol=symbol,
                position_id=position_id
            )
            stop_ids.append(self.add_stop_loss(secondary_stop))
        
        # Create tertiary stop (fixed)
        tertiary_config = self.tier_config.get(StopLossTier.TERTIARY.value, {})
        if tertiary_config.get("enabled", True):
            tertiary_percentage = risk_percentage * 2.0
            # Calculate fixed stop price
            stop_price = entry_price * (1 - direction_factor * tertiary_percentage / 100)
            tertiary_stop = StopLossTrigger(
                stop_type=StopLossType.FIXED,
                tier=StopLossTier.TERTIARY,
                action=StopLossAction(tertiary_config.get("default_action", StopLossAction.CLOSE_POSITION.value)),
                trigger_value=stop_price,
                description=f"Tertiary fixed stop ({tertiary_percentage}%)",
                symbol=symbol,
                position_id=position_id
            )
            stop_ids.append(self.add_stop_loss(tertiary_stop))
        
        # Emergency stop for the position
        emergency_config = self.tier_config.get(StopLossTier.EMERGENCY.value, {})
        if emergency_config.get("enabled", True):
            emergency_percentage = risk_percentage * 3.0
            # Calculate fixed emergency stop price
            stop_price = entry_price * (1 - direction_factor * emergency_percentage / 100)
            emergency_stop = StopLossTrigger(
                stop_type=StopLossType.FIXED,
                tier=StopLossTier.EMERGENCY,
                action=StopLossAction(emergency_config.get("default_action", StopLossAction.CLOSE_ALL_POSITIONS.value)),
                trigger_value=stop_price,
                description=f"Emergency stop ({emergency_percentage}%)",
                symbol=symbol,
                position_id=position_id
            )
            stop_ids.append(self.add_stop_loss(emergency_stop))
        
        return stop_ids
    
    def register_stop_handler(self, tier: StopLossTier, handler: Callable) -> None:
        """
        Register a handler for stop-loss events.
        
        Args:
            tier: Stop-loss tier
            handler: Handler function
        """
        if tier not in self.on_stop_triggered:
            self.on_stop_triggered[tier] = []
            
        self.on_stop_triggered[tier].append(handler)
        logger.info(f"Registered handler for {tier.value} stop-loss events")
    
    async def _check_stops_loop(self) -> None:
        """Background task to periodically check stop-losses."""
        try:
            while self.running:
                # Check all stops
                await self._check_all_stops()
                
                # Sleep until next check
                await asyncio.sleep(self.check_interval)
                
        except asyncio.CancelledError:
            logger.info("Stop-loss check loop cancelled")
            raise
            
        except Exception as e:
            logger.error(f"Error in stop-loss check loop: {str(e)}", exc_info=True)
            self._update_status(ComponentStatus.ERROR, f"Check loop error: {str(e)}")
    
    async def _check_all_stops(self) -> None:
        """Check all registered stop-losses against current prices."""
        # Process symbols with price updates
        for symbol, price_data in self.last_price_update.items():
            # Skip old price data
            if time.time() - price_data["timestamp"] > 60:  # 60 second cache lifetime
                continue
                
            # Check stops for this symbol
            triggered = self.update_price(symbol, price_data["price"])
            
            # Process triggered stops
            for stop in triggered:
                # Keep track of recently triggered stops
                self.triggered_stops.append(stop.id)
                
                # Execute stop-loss action
                await self._execute_stop_loss(stop)
                
                # Call handlers for this tier
                if stop.tier in self.on_stop_triggered:
                    for handler in self.on_stop_triggered[stop.tier]:
                        try:
                            await handler(stop)
                        except Exception as e:
                            logger.error(f"Error in stop-loss handler: {str(e)}", exc_info=True)
    
    async def _execute_stop_loss(self, stop: StopLossTrigger) -> None:
        """
        Execute a triggered stop-loss.
        
        Args:
            stop: Triggered stop-loss
        """
        logger.warning(f"STOP-LOSS TRIGGERED: {stop.description} at {stop.triggered_price}")
        
        # Skip if message bus not available
        if not self.message_bus:
            logger.error("Cannot execute stop-loss: Message bus not available")
            return
            
        try:
            # Handle different actions
            if stop.action == StopLossAction.CLOSE_POSITION:
                # Close specific position
                if stop.position_id:
                    await self.message_bus.publish(
                        MessageTypes.POSITION_CLOSE_REQUEST,
                        {
                            "position_id": stop.position_id,
                            "reason": f"Stop-loss triggered: {stop.description}",
                            "stop_loss_id": stop.id,
                            "price": stop.triggered_price,
                            "emergency": stop.tier == StopLossTier.EMERGENCY
                        }
                    )
                else:
                    logger.error(f"Cannot close position: No position ID specified in stop-loss {stop.id}")
                    
            elif stop.action == StopLossAction.REDUCE_POSITION:
                # Reduce position size (e.g., by 50%)
                if stop.position_id:
                    await self.message_bus.publish(
                        MessageTypes.POSITION_REDUCE_REQUEST,
                        {
                            "position_id": stop.position_id,
                            "reduction_percentage": 50.0,  # Default to 50%
                            "reason": f"Stop-loss triggered: {stop.description}",
                            "stop_loss_id": stop.id,
                            "price": stop.triggered_price
                        }
                    )
                else:
                    logger.error(f"Cannot reduce position: No position ID specified in stop-loss {stop.id}")
                    
            elif stop.action == StopLossAction.CLOSE_ALL_POSITIONS:
                # Close all positions
                await self.message_bus.publish(
                    MessageTypes.ALL_POSITIONS_CLOSE_REQUEST,
                    {
                        "reason": f"Stop-loss triggered: {stop.description}",
                        "stop_loss_id": stop.id,
                        "emergency": stop.tier == StopLossTier.EMERGENCY
                    }
                )
                
            elif stop.action == StopLossAction.SUSPEND_TRADING:
                # Suspend trading
                await self.message_bus.publish(
                    MessageTypes.TRADING_SUSPEND_REQUEST,
                    {
                        "reason": f"Stop-loss triggered: {stop.description}",
                        "stop_loss_id": stop.id,
                        "duration": 3600  # Default to 1 hour suspension
                    }
                )
                
            elif stop.action == StopLossAction.EMERGENCY_SHUTDOWN:
                # Emergency shutdown
                if not self.emergency_shutdown_active:
                    self.emergency_shutdown_active = True
                    await self.message_bus.publish(
                        MessageTypes.EMERGENCY_SHUTDOWN_REQUEST,
                        {
                            "reason": f"Emergency stop-loss triggered: {stop.description}",
                            "stop_loss_id": stop.id
                        }
                    )
                    
            elif stop.action == StopLossAction.HEDGE_POSITION:
                # Create hedge position
                if stop.position_id and stop.symbol:
                    await self.message_bus.publish(
                        MessageTypes.HEDGE_POSITION_REQUEST,
                        {
                            "position_id": stop.position_id,
                            "symbol": stop.symbol,
                            "reason": f"Stop-loss triggered: {stop.description}",
                            "stop_loss_id": stop.id,
                            "price": stop.triggered_price
                        }
                    )
                else:
                    logger.error(f"Cannot create hedge: Insufficient details in stop-loss {stop.id}")
                    
        except Exception as e:
            logger.error(f"Error executing stop-loss action: {str(e)}", exc_info=True)
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get status information.
        
        Returns:
            Dict: Status information
        """
        return {
            "running": self.running,
            "active_stops": len(self.stop_losses),
            "symbols_monitored": len(self.symbol_stops),
            "positions_monitored": len(self.position_stops),
            "emergency_stops": len(self.emergency_stops),
            "triggered_count": len([s for s in self.stop_losses.values() if s.triggered]),
            "triggered_recent": self.triggered_stops[-10:] if self.triggered_stops else []
        }


async def create_multi_level_stoploss(config: Dict[str, Any]) -> MultiLevelStopLoss:
    """
    Create and initialize a multi-level stop-loss system.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        Initialized MultiLevelStopLoss instance
    """
    stop_loss_system = MultiLevelStopLoss(config)
    await stop_loss_system.start()
    return stop_loss_system 