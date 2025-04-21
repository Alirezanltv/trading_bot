"""
Position Service Module

This module implements the Position Service, which acts as a bridge between strategies
and the position management subsystem. It handles the creation, modification, and monitoring
of positions based on strategy signals.
"""

import logging
import asyncio
import uuid
from typing import Dict, List, Optional, Any, Union, Callable
from datetime import datetime
import threading

from trading_system.position.position_types import (
    Position, Order, PositionTrigger, PositionStatus, 
    PositionType, OrderAction, OrderStatus, TriggerType
)
from trading_system.position.position_manager import PositionManager
from trading_system.position.position_sizing import PositionSizer, PositionSizingFactory

logger = logging.getLogger(__name__)


class PositionService:
    """
    Service for position creation and management based on strategy signals.
    
    This service:
    - Processes strategy signals to create/modify positions
    - Applies position sizing rules
    - Creates and tracks position triggers (stop-loss, take-profit)
    - Monitors positions and updates their status
    - Interfaces with the execution service for order management
    - Provides position information to other system components
    """
    
    def __init__(self, 
                position_manager: PositionManager,
                config: Dict[str, Any]):
        """
        Initialize the Position Service.
        
        Args:
            position_manager: Position Manager instance
            config: Configuration dictionary
        """
        self.position_manager = position_manager
        self.config = config
        
        # Configure position sizing strategies
        sizing_config = config.get('position_sizing', {})
        self.default_position_sizer = PositionSizingFactory.create_position_sizer(
            sizing_config.get('default_type', 'fixed_risk'),
            sizing_config.get('default_config', {'risk_percentage': 0.01})
        )
        
        # Strategy-specific position sizers
        self.strategy_position_sizers: Dict[str, PositionSizer] = {}
        for strategy_id, strategy_config in sizing_config.get('strategies', {}).items():
            self.strategy_position_sizers[strategy_id] = PositionSizingFactory.create_position_sizer(
                strategy_config.get('type', 'fixed_risk'),
                strategy_config.get('config', {})
            )
        
        # Signal handlers and callbacks
        self.signal_handlers: Dict[str, Callable] = {}
        
        # Position monitoring
        self.monitoring_interval = config.get('monitoring_interval', 60)  # seconds
        self.monitor_running = False
        self.monitor_thread = None
        
        # Position reconciliation
        self.reconciliation_interval = config.get('reconciliation_interval', 3600)  # seconds
        self.last_reconciliation = datetime.now()
        
        # Default stop-loss and take-profit settings
        self.default_stop_loss = config.get('default_stop_loss', 0.05)  # 5%
        self.default_take_profit = config.get('default_take_profit', 0.1)  # 10%
        
        logger.info("Position Service initialized")
    
    def start(self) -> None:
        """Start position monitoring and services."""
        if not self.monitor_running:
            self.monitor_running = True
            self.monitor_thread = threading.Thread(target=self._position_monitor_loop, daemon=True)
            self.monitor_thread.start()
            logger.info("Position monitoring started")
    
    def stop(self) -> None:
        """Stop position monitoring and services."""
        self.monitor_running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=2.0)
            self.monitor_thread = None
            logger.info("Position monitoring stopped")
    
    def _position_monitor_loop(self) -> None:
        """Background thread for position monitoring."""
        while self.monitor_running:
            try:
                # Monitor open positions
                self._monitor_positions()
                
                # Check if reconciliation is needed
                now = datetime.now()
                if (now - self.last_reconciliation).total_seconds() >= self.reconciliation_interval:
                    self._reconcile_positions()
                    self.last_reconciliation = now
                
                # Sleep until next check
                time.sleep(self.monitoring_interval)
            
            except Exception as e:
                logger.error(f"Error in position monitor loop: {e}")
                # Continue monitoring despite errors
                time.sleep(10)  # Short sleep before retry
    
    def _monitor_positions(self) -> None:
        """Monitor and update open positions."""
        try:
            # Get all open positions
            open_positions = self.position_manager.get_open_positions()
            
            if not open_positions:
                return
            
            logger.debug(f"Monitoring {len(open_positions)} open positions")
            
            # Check each position
            for position in open_positions:
                # Skip positions without current price
                if position.current_price is None:
                    continue
                
                # Check for active triggers that might have been triggered
                triggers = self.position_manager.get_triggers(
                    position_id=position.position_id,
                    is_active=True
                )
                
                for trigger in triggers:
                    # Skip triggers without price
                    if trigger.price is None:
                        continue
                    
                    # Check if trigger should be activated
                    triggered = False
                    
                    if trigger.trigger_type == TriggerType.STOP_LOSS:
                        # Stop loss
                        if position.position_type == PositionType.LONG:
                            triggered = position.current_price <= trigger.price
                        else:  # SHORT
                            triggered = position.current_price >= trigger.price
                    
                    elif trigger.trigger_type == TriggerType.TAKE_PROFIT:
                        # Take profit
                        if position.position_type == PositionType.LONG:
                            triggered = position.current_price >= trigger.price
                        else:  # SHORT
                            triggered = position.current_price <= trigger.price
                    
                    elif trigger.trigger_type == TriggerType.TRAILING_STOP:
                        # Trailing stop - complex logic handled in position manager
                        pass  # Position manager handles this in process_market_update
                    
                    elif trigger.trigger_type == TriggerType.TIME_LIMIT:
                        # Time-based exit
                        if trigger.expiration_time and datetime.now() >= trigger.expiration_time:
                            triggered = True
                    
                    # Process triggered trigger
                    if triggered:
                        logger.info(f"Trigger {trigger.trigger_id} ({trigger.trigger_type.value}) "
                                   f"activated for position {position.position_id}")
                        
                        # Mark trigger as triggered
                        self.position_manager.update_trigger(
                            trigger.trigger_id,
                            is_active=False,
                            triggered_at=datetime.now()
                        )
                        
                        # Call appropriate handler
                        self._handle_triggered_position(position, trigger)
            
        except Exception as e:
            logger.error(f"Error monitoring positions: {e}")
    
    def _handle_triggered_position(self, position: Position, trigger: PositionTrigger) -> None:
        """
        Handle a triggered position.
        
        Args:
            position: Position that has a triggered condition
            trigger: The trigger that was activated
        """
        try:
            # Based on trigger type, take appropriate action
            if trigger.trigger_type == TriggerType.STOP_LOSS:
                # Generate signal to close position at market
                logger.info(f"Stop loss triggered for position {position.position_id} at price {trigger.price}")
                
                # Call signal handlers if registered
                if 'stop_loss' in self.signal_handlers:
                    self.signal_handlers['stop_loss'](position, trigger)
                
                # Default behavior: close position
                self._close_position_on_trigger(position, trigger)
            
            elif trigger.trigger_type == TriggerType.TAKE_PROFIT:
                # Generate signal to close position at market
                logger.info(f"Take profit triggered for position {position.position_id} at price {trigger.price}")
                
                # Call signal handlers if registered
                if 'take_profit' in self.signal_handlers:
                    self.signal_handlers['take_profit'](position, trigger)
                
                # Default behavior: close position
                self._close_position_on_trigger(position, trigger)
            
            elif trigger.trigger_type == TriggerType.TRAILING_STOP:
                # Generate signal to close position at market
                logger.info(f"Trailing stop triggered for position {position.position_id} at price {trigger.price}")
                
                # Call signal handlers if registered
                if 'trailing_stop' in self.signal_handlers:
                    self.signal_handlers['trailing_stop'](position, trigger)
                
                # Default behavior: close position
                self._close_position_on_trigger(position, trigger)
            
            elif trigger.trigger_type == TriggerType.TIME_LIMIT:
                # Generate signal to close position at market
                logger.info(f"Time limit triggered for position {position.position_id}")
                
                # Call signal handlers if registered
                if 'time_limit' in self.signal_handlers:
                    self.signal_handlers['time_limit'](position, trigger)
                
                # Default behavior: close position
                self._close_position_on_trigger(position, trigger)
        
        except Exception as e:
            logger.error(f"Error handling triggered position {position.position_id}: {e}")
    
    def _close_position_on_trigger(self, position: Position, trigger: PositionTrigger) -> None:
        """
        Close a position based on a triggered condition.
        
        Args:
            position: Position to close
            trigger: Trigger that caused the close
        """
        try:
            # Update position status to pending close
            self.position_manager.update_position(
                position.position_id,
                status=PositionStatus.PENDING_CLOSE,
                metadata={
                    **position.metadata,
                    'close_reason': f"{trigger.trigger_type.value} trigger",
                    'trigger_id': trigger.trigger_id
                }
            )
            
            # Create an order for closing
            order = Order(
                position_id=position.position_id,
                symbol=position.symbol,
                order_type=OrderType.MARKET,
                action=OrderAction.SELL if position.position_type == PositionType.LONG else OrderAction.BUY,
                quantity=position.quantity,
                exchange=position.exchange,
                metadata={
                    'trigger_id': trigger.trigger_id,
                    'trigger_type': trigger.trigger_type.value
                }
            )
            
            # Add order to system
            self.position_manager.add_order(order)
            
            # Call 'close_order_created' signal handler if registered
            if 'close_order_created' in self.signal_handlers:
                self.signal_handlers['close_order_created'](position, order)
            
            logger.info(f"Created order {order.order_id} to close position {position.position_id} "
                       f"triggered by {trigger.trigger_type.value}")
        
        except Exception as e:
            logger.error(f"Error closing position {position.position_id} on trigger: {e}")
    
    def _reconcile_positions(self) -> None:
        """Reconcile positions with exchange data."""
        try:
            logger.info("Starting position reconciliation with exchange")
            
            # This would typically call an exchange API to get positions
            # For now, we'll just log that reconciliation was attempted
            
            # TODO: Implement actual exchange integration
            # exchange_positions = exchange_client.get_positions()
            # self.position_manager.reconcile_with_exchange(exchange_positions)
            
            logger.info("Position reconciliation completed")
        
        except Exception as e:
            logger.error(f"Error reconciling positions: {e}")
    
    def register_signal_handler(self, signal_type: str, handler: Callable) -> None:
        """
        Register a handler for position signals.
        
        Args:
            signal_type: Type of signal to handle
            handler: Callback function for the signal
        """
        self.signal_handlers[signal_type] = handler
        logger.debug(f"Registered handler for {signal_type} signals")
    
    def process_strategy_signal(self, 
                              strategy_id: str,
                              signal_data: Dict[str, Any]) -> Optional[Position]:
        """
        Process a strategy signal to create or modify a position.
        
        Args:
            strategy_id: ID of the strategy generating the signal
            signal_data: Signal data containing action, symbol, etc.
            
        Returns:
            Created or modified Position, or None if signal couldn't be processed
        """
        try:
            action = signal_data.get('action')
            symbol = signal_data.get('symbol')
            
            if not action or not symbol:
                logger.warning(f"Invalid strategy signal from {strategy_id}: missing action or symbol")
                return None
            
            if action.lower() == 'buy':
                return self._process_buy_signal(strategy_id, signal_data)
            elif action.lower() == 'sell':
                return self._process_sell_signal(strategy_id, signal_data)
            elif action.lower() == 'exit':
                return self._process_exit_signal(strategy_id, signal_data)
            else:
                logger.warning(f"Unknown action in strategy signal from {strategy_id}: {action}")
                return None
        
        except Exception as e:
            logger.error(f"Error processing strategy signal from {strategy_id}: {e}")
            return None
    
    def _process_buy_signal(self, 
                          strategy_id: str,
                          signal_data: Dict[str, Any]) -> Optional[Position]:
        """
        Process a buy signal from a strategy.
        
        Args:
            strategy_id: ID of the strategy generating the signal
            signal_data: Signal data containing details
            
        Returns:
            Created Position or None if signal couldn't be processed
        """
        symbol = signal_data.get('symbol')
        price = signal_data.get('price')
        
        if not price:
            logger.warning(f"Buy signal from {strategy_id} missing price")
            return None
        
        # Get position sizer for this strategy
        position_sizer = self.strategy_position_sizers.get(strategy_id, self.default_position_sizer)
        
        # Get stop loss price if provided
        stop_loss_price = signal_data.get('stop_loss')
        
        # If no stop loss provided, calculate based on default percentage
        if not stop_loss_price and self.default_stop_loss > 0:
            stop_loss_price = price * (1 - self.default_stop_loss)
        
        # Get portfolio information for sizing
        portfolio = self.position_manager.get_portfolio_summary()
        
        # Calculate position size
        quantity = position_sizer.calculate_position_size(
            symbol=symbol,
            entry_price=price,
            stop_loss_price=stop_loss_price,
            account_balance=portfolio.available_balance,
            # Additional parameters can be passed here
            **signal_data.get('sizing_params', {})
        )
        
        if quantity <= 0:
            logger.warning(f"Invalid position size calculated for {symbol}: {quantity}")
            return None
        
        # Check for maximum positions per strategy
        max_positions = self.config.get('max_positions_per_strategy', 10)
        current_positions = self.position_manager.get_positions(
            status=PositionStatus.OPEN,
            strategy_id=strategy_id
        )
        
        if len(current_positions) >= max_positions:
            logger.warning(f"Strategy {strategy_id} has reached maximum positions ({max_positions})")
            return None
        
        # Check for existing position in same symbol
        existing_positions = self.position_manager.get_positions(
            status=PositionStatus.OPEN,
            symbol=symbol,
            strategy_id=strategy_id
        )
        
        if existing_positions:
            logger.info(f"Strategy {strategy_id} already has an open position for {symbol}")
            # Either return existing or add to position depending on configuration
            if self.config.get('allow_position_increase', False):
                # Add to existing position
                existing = existing_positions[0]
                self.position_manager.update_position(
                    existing.position_id,
                    quantity=existing.quantity + quantity,
                    # Recalculate average entry price
                    entry_price=((existing.entry_price * existing.quantity) + (price * quantity)) / 
                               (existing.quantity + quantity) if existing.entry_price else price
                )
                return existing
            else:
                # Use existing position
                return existing_positions[0]
        
        # Create new position
        position = self.position_manager.create_position(
            symbol=symbol,
            position_type=PositionType.LONG,
            quantity=quantity,
            entry_price=price,
            strategy_id=strategy_id,
            exchange=signal_data.get('exchange', ''),
            metadata={
                'signal_id': signal_data.get('signal_id', str(uuid.uuid4())),
                'signal_time': signal_data.get('timestamp', datetime.now().isoformat()),
                'timeframe': signal_data.get('timeframe', ''),
                'indicators': signal_data.get('indicators', {})
            },
            tags=signal_data.get('tags', [])
        )
        
        # Create market order
        order = Order(
            position_id=position.position_id,
            symbol=symbol,
            order_type=OrderType.MARKET,
            action=OrderAction.BUY,
            quantity=quantity,
            price=price,
            exchange=signal_data.get('exchange', ''),
            metadata={
                'strategy_id': strategy_id,
                'signal_id': signal_data.get('signal_id', ''),
            }
        )
        
        # Add order to system
        self.position_manager.add_order(order)
        
        # Add stop loss if configured
        if stop_loss_price:
            self._add_stop_loss(position, stop_loss_price)
        
        # Add take profit if provided or configured
        take_profit_price = signal_data.get('take_profit')
        if not take_profit_price and self.default_take_profit > 0:
            take_profit_price = price * (1 + self.default_take_profit)
        
        if take_profit_price:
            self._add_take_profit(position, take_profit_price)
        
        # Add trailing stop if configured
        trailing_stop = signal_data.get('trailing_stop')
        if trailing_stop:
            self._add_trailing_stop(position, trailing_stop)
        
        logger.info(f"Created position {position.position_id} for {symbol} based on buy signal from {strategy_id}")
        
        return position
    
    def _process_sell_signal(self, 
                           strategy_id: str,
                           signal_data: Dict[str, Any]) -> Optional[Position]:
        """
        Process a sell signal from a strategy.
        
        Args:
            strategy_id: ID of the strategy generating the signal
            signal_data: Signal data containing details
            
        Returns:
            Created Position or None if signal couldn't be processed
        """
        symbol = signal_data.get('symbol')
        price = signal_data.get('price')
        
        if not price:
            logger.warning(f"Sell signal from {strategy_id} missing price")
            return None
        
        # Get position sizer for this strategy
        position_sizer = self.strategy_position_sizers.get(strategy_id, self.default_position_sizer)
        
        # Get stop loss price if provided
        stop_loss_price = signal_data.get('stop_loss')
        
        # If no stop loss provided, calculate based on default percentage
        if not stop_loss_price and self.default_stop_loss > 0:
            stop_loss_price = price * (1 + self.default_stop_loss)
        
        # Get portfolio information for sizing
        portfolio = self.position_manager.get_portfolio_summary()
        
        # Calculate position size
        quantity = position_sizer.calculate_position_size(
            symbol=symbol,
            entry_price=price,
            stop_loss_price=stop_loss_price,
            account_balance=portfolio.available_balance,
            # Additional parameters can be passed here
            **signal_data.get('sizing_params', {})
        )
        
        if quantity <= 0:
            logger.warning(f"Invalid position size calculated for {symbol}: {quantity}")
            return None
        
        # Check for maximum positions per strategy
        max_positions = self.config.get('max_positions_per_strategy', 10)
        current_positions = self.position_manager.get_positions(
            status=PositionStatus.OPEN,
            strategy_id=strategy_id
        )
        
        if len(current_positions) >= max_positions:
            logger.warning(f"Strategy {strategy_id} has reached maximum positions ({max_positions})")
            return None
        
        # Check for existing position in same symbol
        existing_positions = self.position_manager.get_positions(
            status=PositionStatus.OPEN,
            symbol=symbol,
            strategy_id=strategy_id
        )
        
        if existing_positions:
            logger.info(f"Strategy {strategy_id} already has an open position for {symbol}")
            # Either return existing or add to position depending on configuration
            if self.config.get('allow_position_increase', False):
                # Add to existing position
                existing = existing_positions[0]
                self.position_manager.update_position(
                    existing.position_id,
                    quantity=existing.quantity + quantity,
                    # Recalculate average entry price
                    entry_price=((existing.entry_price * existing.quantity) + (price * quantity)) / 
                               (existing.quantity + quantity) if existing.entry_price else price
                )
                return existing
            else:
                # Use existing position
                return existing_positions[0]
        
        # Create new position
        position = self.position_manager.create_position(
            symbol=symbol,
            position_type=PositionType.SHORT,
            quantity=quantity,
            entry_price=price,
            strategy_id=strategy_id,
            exchange=signal_data.get('exchange', ''),
            metadata={
                'signal_id': signal_data.get('signal_id', str(uuid.uuid4())),
                'signal_time': signal_data.get('timestamp', datetime.now().isoformat()),
                'timeframe': signal_data.get('timeframe', ''),
                'indicators': signal_data.get('indicators', {})
            },
            tags=signal_data.get('tags', [])
        )
        
        # Create market order
        order = Order(
            position_id=position.position_id,
            symbol=symbol,
            order_type=OrderType.MARKET,
            action=OrderAction.SELL,
            quantity=quantity,
            price=price,
            exchange=signal_data.get('exchange', ''),
            metadata={
                'strategy_id': strategy_id,
                'signal_id': signal_data.get('signal_id', ''),
            }
        )
        
        # Add order to system
        self.position_manager.add_order(order)
        
        # Add stop loss if configured
        if stop_loss_price:
            self._add_stop_loss(position, stop_loss_price)
        
        # Add take profit if provided or configured
        take_profit_price = signal_data.get('take_profit')
        if not take_profit_price and self.default_take_profit > 0:
            take_profit_price = price * (1 - self.default_take_profit)
        
        if take_profit_price:
            self._add_take_profit(position, take_profit_price)
        
        # Add trailing stop if configured
        trailing_stop = signal_data.get('trailing_stop')
        if trailing_stop:
            self._add_trailing_stop(position, trailing_stop)
        
        logger.info(f"Created position {position.position_id} for {symbol} based on sell signal from {strategy_id}")
        
        return position
    
    def _process_exit_signal(self, 
                           strategy_id: str,
                           signal_data: Dict[str, Any]) -> Optional[Position]:
        """
        Process an exit signal from a strategy.
        
        Args:
            strategy_id: ID of the strategy generating the signal
            signal_data: Signal data containing details
            
        Returns:
            Closed Position or None if signal couldn't be processed
        """
        symbol = signal_data.get('symbol')
        price = signal_data.get('price')
        position_id = signal_data.get('position_id')
        
        # Find the position to exit
        if position_id:
            # Exit specific position
            position = self.position_manager.get_position(position_id)
            if not position:
                logger.warning(f"Exit signal for unknown position {position_id}")
                return None
            
            if position.status != PositionStatus.OPEN and position.status != PositionStatus.PARTIALLY_CLOSED:
                logger.warning(f"Cannot exit position {position_id} with status {position.status}")
                return None
        else:
            # Find position by symbol and strategy
            positions = self.position_manager.get_positions(
                status=[PositionStatus.OPEN, PositionStatus.PARTIALLY_CLOSED],
                symbol=symbol,
                strategy_id=strategy_id
            )
            
            if not positions:
                logger.warning(f"No open position found for {symbol} from strategy {strategy_id}")
                return None
            
            position = positions[0]
        
        # Exit the position
        exit_price = price or position.current_price
        if not exit_price:
            logger.warning(f"No exit price available for position {position.position_id}")
            return None
        
        # Create market order for exit
        order = Order(
            position_id=position.position_id,
            symbol=position.symbol,
            order_type=OrderType.MARKET,
            action=OrderAction.SELL if position.position_type == PositionType.LONG else OrderAction.BUY,
            quantity=position.quantity,
            price=exit_price,
            exchange=position.exchange,
            metadata={
                'strategy_id': strategy_id,
                'signal_id': signal_data.get('signal_id', ''),
                'exit_reason': signal_data.get('reason', 'strategy_exit')
            }
        )
        
        # Add order to system
        self.position_manager.add_order(order)
        
        # Update position status
        self.position_manager.update_position(
            position.position_id,
            status=PositionStatus.PENDING_CLOSE,
            metadata={
                **position.metadata,
                'exit_signal_id': signal_data.get('signal_id', ''),
                'exit_reason': signal_data.get('reason', 'strategy_exit')
            }
        )
        
        logger.info(f"Exiting position {position.position_id} based on signal from {strategy_id}")
        
        return position
    
    def _add_stop_loss(self, position: Position, price: float) -> PositionTrigger:
        """
        Add a stop loss to a position.
        
        Args:
            position: Position to add stop loss to
            price: Stop loss price
            
        Returns:
            Created PositionTrigger
        """
        trigger = PositionTrigger(
            position_id=position.position_id,
            trigger_type=TriggerType.STOP_LOSS,
            price=price,
            percentage=(price / position.entry_price - 1) * 100 if position.entry_price else None
        )
        
        self.position_manager.add_trigger(trigger)
        
        logger.info(f"Added stop loss at {price} for position {position.position_id}")
        
        return trigger
    
    def _add_take_profit(self, position: Position, price: float) -> PositionTrigger:
        """
        Add a take profit to a position.
        
        Args:
            position: Position to add take profit to
            price: Take profit price
            
        Returns:
            Created PositionTrigger
        """
        trigger = PositionTrigger(
            position_id=position.position_id,
            trigger_type=TriggerType.TAKE_PROFIT,
            price=price,
            percentage=(price / position.entry_price - 1) * 100 if position.entry_price else None
        )
        
        self.position_manager.add_trigger(trigger)
        
        logger.info(f"Added take profit at {price} for position {position.position_id}")
        
        return trigger
    
    def _add_trailing_stop(self, position: Position, trail_value: float) -> PositionTrigger:
        """
        Add a trailing stop to a position.
        
        Args:
            position: Position to add trailing stop to
            trail_value: Trail value (amount or percentage to trail by)
            
        Returns:
            Created PositionTrigger
        """
        # Start trailing stop at current price minus trail value
        initial_price = None
        if position.current_price:
            if position.position_type == PositionType.LONG:
                initial_price = position.current_price - trail_value
            else:  # SHORT
                initial_price = position.current_price + trail_value
        elif position.entry_price:
            if position.position_type == PositionType.LONG:
                initial_price = position.entry_price - trail_value
            else:  # SHORT
                initial_price = position.entry_price + trail_value
        
        trigger = PositionTrigger(
            position_id=position.position_id,
            trigger_type=TriggerType.TRAILING_STOP,
            price=initial_price,
            trail_value=trail_value
        )
        
        self.position_manager.add_trigger(trigger)
        
        logger.info(f"Added trailing stop with trail value {trail_value} for position {position.position_id}")
        
        return trigger
    
    def update_position_price(self, symbol: str, price: float) -> List[Position]:
        """
        Update price for all positions with the given symbol.
        
        Args:
            symbol: Symbol to update
            price: Current price
            
        Returns:
            List of updated positions
        """
        # Process market update through position manager
        self.position_manager.process_market_update(symbol, price)
        
        # Return updated positions
        return self.position_manager.get_positions(
            status=[PositionStatus.OPEN, PositionStatus.PARTIALLY_CLOSED],
            symbol=symbol
        )
    
    def get_open_positions(self, 
                         strategy_id: Optional[str] = None,
                         symbol: Optional[str] = None) -> List[Position]:
        """
        Get open positions, optionally filtered.
        
        Args:
            strategy_id: Filter by strategy ID
            symbol: Filter by symbol
            
        Returns:
            List of open positions
        """
        return self.position_manager.get_positions(
            status=[PositionStatus.OPEN, PositionStatus.PARTIALLY_CLOSED],
            strategy_id=strategy_id,
            symbol=symbol
        )
    
    def get_position_by_id(self, position_id: str) -> Optional[Position]:
        """
        Get a position by ID.
        
        Args:
            position_id: Position ID
            
        Returns:
            Position or None if not found
        """
        return self.position_manager.get_position(position_id)
    
    def get_orders_for_position(self, position_id: str) -> List[Order]:
        """
        Get all orders for a position.
        
        Args:
            position_id: Position ID
            
        Returns:
            List of orders
        """
        return self.position_manager.get_orders(position_id=position_id)
    
    def get_triggers_for_position(self, position_id: str) -> List[PositionTrigger]:
        """
        Get all triggers for a position.
        
        Args:
            position_id: Position ID
            
        Returns:
            List of triggers
        """
        return self.position_manager.get_triggers(position_id=position_id)
    
    def get_portfolio_summary(self) -> Dict[str, Any]:
        """
        Get current portfolio summary.
        
        Returns:
            Dictionary with portfolio summary data
        """
        portfolio = self.position_manager.get_portfolio_summary()
        return portfolio.to_dict()
    
    def modify_position_stop_loss(self, 
                                position_id: str,
                                price: float) -> Optional[PositionTrigger]:
        """
        Modify a position's stop loss.
        
        Args:
            position_id: Position ID
            price: New stop loss price
            
        Returns:
            Updated PositionTrigger or None if not found
        """
        position = self.position_manager.get_position(position_id)
        if not position:
            logger.warning(f"Position {position_id} not found")
            return None
        
        # Get existing stop loss
        triggers = self.position_manager.get_triggers(
            position_id=position_id,
            is_active=True
        )
        
        existing_stop = next((t for t in triggers if t.trigger_type == TriggerType.STOP_LOSS), None)
        
        if existing_stop:
            # Update existing stop loss
            self.position_manager.update_trigger(
                existing_stop.trigger_id,
                price=price,
                percentage=(price / position.entry_price - 1) * 100 if position.entry_price else None
            )
            
            logger.info(f"Updated stop loss to {price} for position {position_id}")
            
            return self.position_manager.get_trigger(existing_stop.trigger_id)
        else:
            # Create new stop loss
            return self._add_stop_loss(position, price)
    
    def modify_position_take_profit(self, 
                                  position_id: str,
                                  price: float) -> Optional[PositionTrigger]:
        """
        Modify a position's take profit.
        
        Args:
            position_id: Position ID
            price: New take profit price
            
        Returns:
            Updated PositionTrigger or None if not found
        """
        position = self.position_manager.get_position(position_id)
        if not position:
            logger.warning(f"Position {position_id} not found")
            return None
        
        # Get existing take profit
        triggers = self.position_manager.get_triggers(
            position_id=position_id,
            is_active=True
        )
        
        existing_tp = next((t for t in triggers if t.trigger_type == TriggerType.TAKE_PROFIT), None)
        
        if existing_tp:
            # Update existing take profit
            self.position_manager.update_trigger(
                existing_tp.trigger_id,
                price=price,
                percentage=(price / position.entry_price - 1) * 100 if position.entry_price else None
            )
            
            logger.info(f"Updated take profit to {price} for position {position_id}")
            
            return self.position_manager.get_trigger(existing_tp.trigger_id)
        else:
            # Create new take profit
            return self._add_take_profit(position, price)
    
    def modify_position_trailing_stop(self, 
                                    position_id: str,
                                    trail_value: float) -> Optional[PositionTrigger]:
        """
        Modify a position's trailing stop.
        
        Args:
            position_id: Position ID
            trail_value: New trail value
            
        Returns:
            Updated PositionTrigger or None if not found
        """
        position = self.position_manager.get_position(position_id)
        if not position:
            logger.warning(f"Position {position_id} not found")
            return None
        
        # Get existing trailing stop
        triggers = self.position_manager.get_triggers(
            position_id=position_id,
            is_active=True
        )
        
        existing_ts = next((t for t in triggers if t.trigger_type == TriggerType.TRAILING_STOP), None)
        
        if existing_ts:
            # Initialize new price based on current price
            new_price = None
            if position.current_price:
                if position.position_type == PositionType.LONG:
                    new_price = position.current_price - trail_value
                else:  # SHORT
                    new_price = position.current_price + trail_value
            
            # Update existing trailing stop
            self.position_manager.update_trigger(
                existing_ts.trigger_id,
                trail_value=trail_value,
                price=new_price if new_price else existing_ts.price
            )
            
            logger.info(f"Updated trailing stop to {trail_value} for position {position_id}")
            
            return self.position_manager.get_trigger(existing_ts.trigger_id)
        else:
            # Create new trailing stop
            return self._add_trailing_stop(position, trail_value) 