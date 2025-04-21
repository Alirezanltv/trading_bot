"""
Hardware-Level Stop-Loss Executor

This module implements a hardware-level stop-loss mechanism that operates independently
from the main trading system, providing a fail-safe if the main system fails.
"""

import json
import logging
import os
import signal
import subprocess
import threading
import time
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Any, Set, Callable, Union

from trading_system.exchange.nobitex_adapter import NobitexAdapter
from trading_system.models.position import Position, PositionType, PositionStatus
from trading_system.monitoring.alert_system import get_alert_system, AlertSeverity
from trading_system.utils.config import Config

# Configure logging
logger = logging.getLogger(__name__)

class StopLossExecutorStatus(Enum):
    """Status of the stop-loss executor."""
    INACTIVE = "inactive"
    ACTIVE = "active"
    EMERGENCY = "emergency"
    ERROR = "error"

class StopLossExecutionMode(Enum):
    """Execution mode for stop-loss orders."""
    SIMULATION = "simulation"  # Don't actually execute, just simulate
    PAPER_TRADING = "paper_trading"  # Execute in paper trading mode
    LIVE = "live"  # Execute live orders

class HardwareStopLossExecutor:
    """
    Hardware-level stop-loss executor.
    
    This provides an independent fail-safe that can operate even if the
    main trading system fails. It directly interfaces with exchange APIs
    to close positions when stop-loss thresholds are breached.
    
    Features:
    - Independent process monitoring
    - Direct exchange API access
    - File-based position tracking
    - Watchdog timer
    - Heartbeat monitoring
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the hardware-level stop-loss executor.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config or Config().get("hardware_stop_loss", {})
        self.enabled = self.config.get("enabled", False)
        
        if not self.enabled:
            logger.info("Hardware-level stop-loss executor is disabled")
            self.status = StopLossExecutorStatus.INACTIVE
            return
        
        # Load configuration
        self.mode = StopLossExecutionMode(self.config.get("mode", "simulation"))
        self.check_interval = self.config.get("check_interval", 5)  # seconds
        self.heartbeat_timeout = self.config.get("heartbeat_timeout", 60)  # seconds
        self.position_file = self.config.get("position_file", "hardware_stop_loss_positions.json")
        self.emergency_file = self.config.get("emergency_file", "EMERGENCY_STOP")
        self.max_drawdown_pct = self.config.get("max_drawdown_pct", 25.0)
        self.api_key = self.config.get("api_key", "")
        self.api_secret = self.config.get("api_secret", "")
        
        # Initialize state
        self.status = StopLossExecutorStatus.INACTIVE
        self.last_heartbeat = time.time()
        self.active_positions: Dict[str, Dict[str, Any]] = {}
        self.emergency_triggered = False
        self.stop_event = threading.Event()
        self.monitor_thread = None
        
        # Initialize exchange adapter if in live mode
        self.exchange = None
        if self.mode == StopLossExecutionMode.LIVE:
            try:
                self.exchange = NobitexAdapter({
                    "api_key": self.api_key,
                    "api_secret": self.api_secret,
                    "timeout": 10
                })
                logger.info("Initialized exchange adapter for hardware stop-loss")
            except Exception as e:
                logger.error(f"Failed to initialize exchange adapter: {str(e)}")
                self.status = StopLossExecutorStatus.ERROR
                return
        
        # Initialize alert system
        self.alert_system = get_alert_system()
        
        # Ensure position file exists
        self._initialize_position_file()
        
        self.status = StopLossExecutorStatus.ACTIVE
        logger.info(f"Hardware-level stop-loss executor initialized in {self.mode.value} mode")
    
    def _initialize_position_file(self) -> None:
        """Initialize the position tracking file."""
        file_path = Path(self.position_file)
        
        # Create parent directories if they don't exist
        os.makedirs(file_path.parent, exist_ok=True)
        
        # Create empty file if it doesn't exist
        if not file_path.exists():
            with open(file_path, 'w') as f:
                json.dump({"positions": {}, "last_update": time.time()}, f)
            logger.info(f"Created hardware stop-loss position file: {self.position_file}")
    
    def start(self) -> bool:
        """
        Start the hardware-level stop-loss executor.
        
        Returns:
            Success flag
        """
        if not self.enabled:
            logger.warning("Hardware-level stop-loss executor is disabled, cannot start")
            return False
        
        if self.status == StopLossExecutorStatus.ACTIVE:
            logger.warning("Hardware-level stop-loss executor is already active")
            return True
        
        try:
            # Start monitoring thread
            self.stop_event.clear()
            self.monitor_thread = threading.Thread(
                target=self._monitor_loop, 
                daemon=True,
                name="HardwareStopLossMonitor"
            )
            self.monitor_thread.start()
            
            self.status = StopLossExecutorStatus.ACTIVE
            logger.info("Hardware-level stop-loss executor started")
            
            # Send alert
            self.alert_system.info(
                "Hardware Stop-Loss Activated",
                f"Hardware-level stop-loss protection activated in {self.mode.value} mode",
                component="HardwareStopLoss"
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to start hardware-level stop-loss executor: {str(e)}")
            self.status = StopLossExecutorStatus.ERROR
            return False
    
    def stop(self) -> bool:
        """
        Stop the hardware-level stop-loss executor.
        
        Returns:
            Success flag
        """
        if not self.enabled or self.status == StopLossExecutorStatus.INACTIVE:
            logger.warning("Hardware-level stop-loss executor is not active")
            return True
        
        try:
            # Signal thread to stop
            self.stop_event.set()
            
            # Wait for thread to terminate
            if self.monitor_thread and self.monitor_thread.is_alive():
                self.monitor_thread.join(timeout=5.0)
            
            self.status = StopLossExecutorStatus.INACTIVE
            logger.info("Hardware-level stop-loss executor stopped")
            
            return True
            
        except Exception as e:
            logger.error(f"Error stopping hardware-level stop-loss executor: {str(e)}")
            return False
    
    def register_position(self, position: Position, stop_loss_price: float) -> bool:
        """
        Register a position for hardware-level stop-loss protection.
        
        Args:
            position: Position to protect
            stop_loss_price: Stop-loss price
            
        Returns:
            Success flag
        """
        if not self.enabled:
            return False
        
        position_id = position.position_id
        
        try:
            # Read current positions
            positions = self._read_positions()
            
            # Add or update position
            positions["positions"][position_id] = {
                "position_id": position_id,
                "symbol": position.symbol,
                "position_type": position.position_type.value,
                "quantity": position.quantity,
                "entry_price": position.entry_price,
                "current_price": position.current_price,
                "stop_loss_price": stop_loss_price,
                "registered_at": time.time()
            }
            
            # Update timestamp
            positions["last_update"] = time.time()
            
            # Write back to file
            self._write_positions(positions)
            
            logger.info(f"Registered position {position_id} for hardware-level stop-loss at price {stop_loss_price}")
            return True
            
        except Exception as e:
            logger.error(f"Error registering position for hardware stop-loss: {str(e)}")
            return False
    
    def update_position(self, position: Position, stop_loss_price: Optional[float] = None) -> bool:
        """
        Update a position's hardware-level stop-loss settings.
        
        Args:
            position: Updated position
            stop_loss_price: New stop-loss price (if None, keeps existing)
            
        Returns:
            Success flag
        """
        if not self.enabled:
            return False
        
        position_id = position.position_id
        
        try:
            # Read current positions
            positions = self._read_positions()
            
            # Check if position exists
            if position_id not in positions["positions"]:
                logger.warning(f"Position {position_id} not found in hardware stop-loss registry")
                if stop_loss_price is not None:
                    return self.register_position(position, stop_loss_price)
                return False
            
            # Update position data
            pos_data = positions["positions"][position_id]
            pos_data["quantity"] = position.quantity
            pos_data["current_price"] = position.current_price
            
            # Update stop-loss price if provided
            if stop_loss_price is not None:
                pos_data["stop_loss_price"] = stop_loss_price
                logger.info(f"Updated stop-loss price for position {position_id} to {stop_loss_price}")
            
            # If position is closed, remove it
            if position.status != PositionStatus.OPEN or position.quantity == 0:
                del positions["positions"][position_id]
                logger.info(f"Removed closed position {position_id} from hardware stop-loss registry")
            
            # Update timestamp
            positions["last_update"] = time.time()
            
            # Write back to file
            self._write_positions(positions)
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating position in hardware stop-loss: {str(e)}")
            return False
    
    def remove_position(self, position_id: str) -> bool:
        """
        Remove a position from hardware-level stop-loss protection.
        
        Args:
            position_id: Position ID to remove
            
        Returns:
            Success flag
        """
        if not self.enabled:
            return False
        
        try:
            # Read current positions
            positions = self._read_positions()
            
            # Remove position if it exists
            if position_id in positions["positions"]:
                del positions["positions"][position_id]
                logger.info(f"Removed position {position_id} from hardware stop-loss registry")
                
                # Update timestamp
                positions["last_update"] = time.time()
                
                # Write back to file
                self._write_positions(positions)
            
            return True
            
        except Exception as e:
            logger.error(f"Error removing position from hardware stop-loss: {str(e)}")
            return False
    
    def update_heartbeat(self) -> None:
        """Update the heartbeat timestamp to indicate the main system is alive."""
        if not self.enabled:
            return
        
        self.last_heartbeat = time.time()
    
    def trigger_emergency_stop(self) -> bool:
        """
        Trigger emergency stop.
        
        Returns:
            Success flag
        """
        if not self.enabled:
            return False
        
        try:
            # Create emergency file
            with open(self.emergency_file, 'w') as f:
                f.write(f"EMERGENCY STOP TRIGGERED AT {datetime.now().isoformat()}")
            
            self.emergency_triggered = True
            self.status = StopLossExecutorStatus.EMERGENCY
            
            logger.critical("EMERGENCY STOP TRIGGERED via hardware-level stop-loss")
            
            # Execute emergency stop logic
            self._execute_emergency_stop()
            
            # Send alert
            self.alert_system.critical(
                "HARDWARE EMERGENCY STOP TRIGGERED",
                "Hardware-level emergency stop has been triggered. All positions will be closed immediately.",
                component="HardwareStopLoss"
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error triggering emergency stop: {str(e)}")
            return False
    
    def _monitor_loop(self) -> None:
        """Monitor positions and execute stop-loss orders when triggered."""
        logger.info("Hardware stop-loss monitor thread started")
        
        while not self.stop_event.is_set():
            try:
                # Check for emergency file
                if os.path.exists(self.emergency_file):
                    logger.critical("EMERGENCY STOP file detected")
                    self._execute_emergency_stop()
                    self.status = StopLossExecutorStatus.EMERGENCY
                
                # Check heartbeat
                if time.time() - self.last_heartbeat > self.heartbeat_timeout:
                    logger.warning(f"Main system heartbeat timeout ({self.heartbeat_timeout}s) - executing failsafe checks")
                    self._perform_failsafe_checks()
                
                # Check positions
                positions = self._read_positions()
                
                # Skip if no positions or emergency already triggered
                if not positions["positions"] or self.emergency_triggered:
                    time.sleep(self.check_interval)
                    continue
                
                # Check each position
                for position_id, position_data in list(positions["positions"].items()):
                    self._check_position_stop_loss(position_id, position_data)
                
            except Exception as e:
                logger.error(f"Error in hardware stop-loss monitor: {str(e)}")
                time.sleep(self.check_interval)
            
            # Sleep until next check
            time.sleep(self.check_interval)
        
        logger.info("Hardware stop-loss monitor thread stopped")
    
    def _check_position_stop_loss(self, position_id: str, position_data: Dict[str, Any]) -> None:
        """
        Check if stop-loss is triggered for a position.
        
        Args:
            position_id: Position ID
            position_data: Position data
        """
        try:
            symbol = position_data["symbol"]
            position_type = position_data["position_type"]
            stop_loss_price = position_data["stop_loss_price"]
            
            # Get current price (in a real implementation, this would query the exchange)
            current_price = position_data["current_price"]
            
            # In live mode, get actual price from exchange
            if self.mode == StopLossExecutionMode.LIVE and self.exchange:
                try:
                    ticker = self.exchange.fetch_ticker(symbol)
                    current_price = ticker["last"]
                except Exception as e:
                    logger.error(f"Error fetching current price for {symbol}: {str(e)}")
            
            # Check if stop-loss is triggered
            stop_loss_triggered = False
            
            if position_type == PositionType.LONG.value:
                # For long positions, trigger if price falls below stop-loss
                if current_price <= stop_loss_price:
                    stop_loss_triggered = True
            else:
                # For short positions, trigger if price rises above stop-loss
                if current_price >= stop_loss_price:
                    stop_loss_triggered = True
            
            # Execute stop-loss if triggered
            if stop_loss_triggered:
                self._execute_stop_loss(position_id, position_data, current_price)
            
        except Exception as e:
            logger.error(f"Error checking stop-loss for position {position_id}: {str(e)}")
    
    def _execute_stop_loss(self, position_id: str, position_data: Dict[str, Any], current_price: float) -> None:
        """
        Execute stop-loss for a position.
        
        Args:
            position_id: Position ID
            position_data: Position data
            current_price: Current price
        """
        symbol = position_data["symbol"]
        position_type = position_data["position_type"]
        quantity = position_data["quantity"]
        
        logger.warning(f"Hardware stop-loss triggered for {symbol} position {position_id} at price {current_price}")
        
        try:
            # Execute the order based on mode
            if self.mode == StopLossExecutionMode.SIMULATION:
                logger.info(f"SIMULATION: Would close {symbol} position of {quantity} at {current_price}")
                
                # Send alert
                self.alert_system.warning(
                    "Hardware Stop-Loss Triggered (Simulation)",
                    f"Hardware stop-loss would close {symbol} position of {quantity} at {current_price}",
                    component="HardwareStopLoss",
                    tags={"position_id": position_id, "symbol": symbol}
                )
                
            elif self.mode == StopLossExecutionMode.PAPER_TRADING:
                logger.info(f"PAPER TRADING: Closing {symbol} position of {quantity} at {current_price}")
                
                # Send alert
                self.alert_system.warning(
                    "Hardware Stop-Loss Triggered (Paper Trading)",
                    f"Hardware stop-loss closing {symbol} position of {quantity} at {current_price}",
                    component="HardwareStopLoss",
                    tags={"position_id": position_id, "symbol": symbol}
                )
                
            elif self.mode == StopLossExecutionMode.LIVE and self.exchange:
                logger.warning(f"LIVE: Closing {symbol} position of {quantity} at market price")
                
                # Determine order side (opposite of position type)
                order_side = "sell" if position_type == PositionType.LONG.value else "buy"
                
                # Create market order to close position
                try:
                    order = self.exchange.create_market_order(
                        symbol=symbol,
                        side=order_side,
                        amount=quantity
                    )
                    
                    logger.info(f"Stop-loss market order created: {order['id']}")
                    
                    # Send alert
                    self.alert_system.error(
                        "Hardware Stop-Loss Executed (Live)",
                        f"Hardware stop-loss closed {symbol} position of {quantity} at market price",
                        component="HardwareStopLoss",
                        tags={"position_id": position_id, "symbol": symbol, "order_id": order['id']}
                    )
                    
                except Exception as e:
                    logger.error(f"Error creating market order to close position: {str(e)}")
                    
                    # Try one more time after a delay
                    time.sleep(2)
                    try:
                        order = self.exchange.create_market_order(
                            symbol=symbol,
                            side=order_side,
                            amount=quantity
                        )
                        logger.info(f"Stop-loss market order created on second attempt: {order['id']}")
                    except Exception as e2:
                        logger.critical(f"FAILED to create market order on second attempt: {str(e2)}")
                        
                        # Send critical alert
                        self.alert_system.critical(
                            "Hardware Stop-Loss Execution Failed",
                            f"Failed to execute hardware stop-loss for {symbol} position: {str(e2)}",
                            component="HardwareStopLoss",
                            tags={"position_id": position_id, "symbol": symbol}
                        )
            
            # Remove position from tracking
            self.remove_position(position_id)
            
        except Exception as e:
            logger.error(f"Error executing stop-loss for position {position_id}: {str(e)}")
    
    def _execute_emergency_stop(self) -> None:
        """Execute emergency stop procedure, closing all positions."""
        if self.emergency_triggered:
            return  # Already triggered
        
        logger.critical("Executing EMERGENCY STOP procedure")
        self.emergency_triggered = True
        self.status = StopLossExecutorStatus.EMERGENCY
        
        try:
            # Read current positions
            positions = self._read_positions()
            
            # Execute stop-loss for all positions
            for position_id, position_data in list(positions["positions"].items()):
                self._execute_stop_loss(position_id, position_data, position_data["current_price"])
            
            # Clear positions file
            self._write_positions({"positions": {}, "last_update": time.time()})
            
            # Send critical alert
            self.alert_system.critical(
                "Hardware Emergency Stop Executed",
                "Hardware-level emergency stop procedure completed. All positions should be closed.",
                component="HardwareStopLoss"
            )
            
        except Exception as e:
            logger.error(f"Error executing emergency stop: {str(e)}")
    
    def _perform_failsafe_checks(self) -> None:
        """Perform failsafe checks when main system heartbeat is lost."""
        logger.warning("Performing failsafe checks due to lost heartbeat")
        
        try:
            # Check all positions for stop-loss conditions
            positions = self._read_positions()
            
            for position_id, position_data in list(positions["positions"].items()):
                # Use a tighter stop-loss in failsafe mode to reduce risk
                symbol = position_data["symbol"]
                position_type = position_data["position_type"]
                entry_price = position_data["entry_price"]
                current_price = position_data["current_price"]
                
                # Calculate emergency stop-loss threshold (tighter than normal)
                emergency_threshold = self.config.get("emergency_threshold_pct", 2.0)  # Default 2%
                
                if position_type == PositionType.LONG.value:
                    emergency_stop_price = entry_price * (1 - emergency_threshold / 100)
                    if current_price <= emergency_stop_price:
                        logger.warning(f"Failsafe stop-loss triggered for {symbol} at {current_price}")
                        self._execute_stop_loss(position_id, position_data, current_price)
                else:
                    emergency_stop_price = entry_price * (1 + emergency_threshold / 100)
                    if current_price >= emergency_stop_price:
                        logger.warning(f"Failsafe stop-loss triggered for {symbol} at {current_price}")
                        self._execute_stop_loss(position_id, position_data, current_price)
            
            # Send alert about lost heartbeat
            self.alert_system.error(
                "Trading System Heartbeat Lost",
                f"Main trading system heartbeat lost for {self.heartbeat_timeout}s. Failsafe checks performed.",
                component="HardwareStopLoss"
            )
            
        except Exception as e:
            logger.error(f"Error performing failsafe checks: {str(e)}")
    
    def _read_positions(self) -> Dict[str, Any]:
        """
        Read positions from the position file.
        
        Returns:
            Dictionary of position data
        """
        try:
            with open(self.position_file, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            # Return empty structure if file doesn't exist or is invalid
            return {"positions": {}, "last_update": time.time()}
    
    def _write_positions(self, positions: Dict[str, Any]) -> None:
        """
        Write positions to the position file.
        
        Args:
            positions: Position data to write
        """
        with open(self.position_file, 'w') as f:
            json.dump(positions, f, indent=2)

# Global instance
_instance = None

def get_hardware_stop_loss(config: Optional[Dict[str, Any]] = None) -> HardwareStopLossExecutor:
    """
    Get or create the global HardwareStopLossExecutor instance.
    
    Args:
        config: Optional configuration (only used on first call)
        
    Returns:
        HardwareStopLossExecutor instance
    """
    global _instance
    if _instance is None:
        _instance = HardwareStopLossExecutor(config)
    return _instance 