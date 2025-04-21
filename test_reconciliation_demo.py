#!/usr/bin/env python3
"""
Position Reconciliation Demo

This script demonstrates the position reconciliation process with:
1. Position Manager (in-memory positions)
2. Shadow Accounting (database-backed positions)
3. Exchange positions (real or simulated)

Usage:
    py -3.10 test_reconciliation_demo.py
"""

import os
import sys
import asyncio
import logging
import random
import json
from decimal import Decimal
from datetime import datetime

# Add parent directory to path if needed
if os.path.basename(os.getcwd()) == "trading_system":
    sys.path.insert(0, os.path.abspath(".."))
else:
    sys.path.insert(0, os.path.abspath("."))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"reconciliation_demo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    ]
)

logger = logging.getLogger("reconciliation_demo")

# Import components
from trading_system.position.position_reconciliation import PositionReconciliation
from trading_system.position.shadow_accounting import ShadowPositionManager
from trading_system.position.position_manager import PositionManager


class SimulatedExchangeClient:
    """Simulated exchange client for testing reconciliation."""
    
    def __init__(self, exchange_id="simulated_exchange", simulate_errors=True):
        """
        Initialize simulated exchange.
        
        Args:
            exchange_id: Exchange identifier
            simulate_errors: Whether to simulate errors/discrepancies
        """
        self.exchange_id = exchange_id
        self.positions = {}
        self.simulate_errors = simulate_errors
        self.error_probability = 0.2  # 20% chance of error
        self.max_error_percent = 0.1  # Up to 10% error
        self.api_error_probability = 0.05  # 5% chance of API error
    
    def add_position(self, symbol, quantity, price=None):
        """Add a position to the exchange."""
        self.positions[symbol] = {
            "symbol": symbol,
            "quantity": quantity,
            "price": price or random.uniform(100, 50000),
            "timestamp": datetime.now().isoformat()
        }
    
    def remove_position(self, symbol):
        """Remove a position from the exchange."""
        if symbol in self.positions:
            del self.positions[symbol]
    
    async def get_positions(self, symbol=None):
        """
        Get positions from the exchange.
        
        Args:
            symbol: Symbol to get positions for (or None for all)
            
        Returns:
            List of positions
        """
        # Simulate API error
        if self.simulate_errors and random.random() < self.api_error_probability:
            raise Exception("Simulated API error: Could not retrieve positions")
        
        # Get positions
        if symbol:
            if symbol in self.positions:
                return [self._maybe_modify_position(self.positions[symbol])]
            return []
        
        # Return all positions with possible modifications
        return [self._maybe_modify_position(pos) for pos in self.positions.values()]
    
    def _maybe_modify_position(self, position):
        """Potentially modify a position to simulate errors."""
        if not self.simulate_errors or random.random() > self.error_probability:
            return position.copy()
        
        # Make a modified copy
        modified = position.copy()
        
        # Modify quantity with small error
        error_factor = 1.0 + random.uniform(-self.max_error_percent, self.max_error_percent)
        modified["quantity"] = position["quantity"] * error_factor
        
        logger.info(
            f"Simulated position error: {position['symbol']} "
            f"quantity {position['quantity']} -> {modified['quantity']}"
        )
        
        return modified


async def run_demo():
    """Run the reconciliation demo."""
    logger.info("=" * 70)
    logger.info("STARTING POSITION RECONCILIATION DEMO")
    logger.info("=" * 70)
    
    # Create temporary directory for the demo
    import tempfile
    import shutil
    temp_dir = tempfile.mkdtemp()
    logger.info(f"Created temporary directory: {temp_dir}")
    
    try:
        # Create components
        logger.info("Initializing components...")
        
        # Shadow accounting
        shadow_config = {
            "db_path": os.path.join(temp_dir, "shadow_positions.db"),
            "backup_dir": os.path.join(temp_dir, "backups"),
            "backup_interval": 3600,  # 1 hour
            "reconciliation_interval": 0,  # Disable automatic reconciliation
            "transaction_log_path": os.path.join(temp_dir, "transaction_log.json")
        }
        shadow_manager = ShadowPositionManager(shadow_config)
        
        # Position manager
        position_manager = PositionManager({
            "state_file": os.path.join(temp_dir, "positions.json")
        })
        
        # Exchange client
        exchange = SimulatedExchangeClient("demo_exchange")
        
        # Position reconciliation
        reconciliation_config = {
            "reconciliation_interval": 0,  # Disable automatic reconciliation
            "auto_correct": True,
            "tolerance": Decimal("0.001"),
            "critical_threshold": Decimal("0.05")
        }
        reconciliation = PositionReconciliation(reconciliation_config)
        
        # Initialize components
        await shadow_manager.initialize()
        await position_manager.initialize()
        await reconciliation.initialize()
        
        # Register components
        reconciliation.register_position_manager(position_manager)
        reconciliation.register_shadow_manager(shadow_manager)
        reconciliation.register_exchange_client("demo_exchange", exchange)
        
        # Create some positions
        logger.info("Creating positions...")
        test_positions = [
            {"symbol": "BTC/USDT", "quantity": 0.5, "price": 50000},
            {"symbol": "ETH/USDT", "quantity": 5.0, "price": 3000},
            {"symbol": "SOL/USDT", "quantity": 20.0, "price": 100},
            {"symbol": "DOGE/USDT", "quantity": 1000.0, "price": 0.1}
        ]
        
        # Add positions to exchange
        for pos in test_positions:
            exchange.add_position(pos["symbol"], pos["quantity"], pos["price"])
        
        # Add positions to position manager and shadow accounting with slight variations
        for i, pos in enumerate(test_positions):
            # Introduce small variations in some positions to test reconciliation
            variation = 1.0
            if i % 2 == 1:  # Every other position
                variation = random.uniform(0.95, 1.05)  # +/- 5% variation
            
            # Create position in position manager
            position_id = await position_manager.create_position(
                exchange="demo_exchange",
                symbol=pos["symbol"],
                quantity=pos["quantity"] * variation,
                entry_price=pos["price"],
                metadata={"demo": True}
            )
            
            # Create position in shadow accounting with different variation
            if i % 3 == 2:  # Every third position
                variation = random.uniform(0.96, 1.04)  # +/- 4% variation
            
            await shadow_manager.update_position(
                exchange_id="demo_exchange",
                symbol=pos["symbol"],
                quantity=pos["quantity"] * variation,
                price=pos["price"],
                source=shadow_manager.PositionSource.INTERNAL
            )
        
        # Show initial positions
        logger.info("Initial positions:")
        
        # Exchange positions
        exchange_positions = await exchange.get_positions()
        logger.info(f"Exchange positions: {len(exchange_positions)}")
        for pos in exchange_positions:
            logger.info(f"  {pos['symbol']}: {pos['quantity']} @ {pos['price']}")
        
        # Position manager positions
        manager_positions = position_manager.get_all_open_positions()
        logger.info(f"Position manager positions: {len(manager_positions)}")
        for pos in manager_positions:
            logger.info(f"  {pos.symbol}: {pos.quantity} @ {pos.entry_price}")
        
        # Shadow accounting positions
        shadow_positions = await shadow_manager.get_all_positions("demo_exchange")
        logger.info(f"Shadow accounting positions: {len(shadow_positions)}")
        for symbol, pos in shadow_positions.items():
            logger.info(f"  {symbol}: {pos['quantity']} @ {pos['price']}")
        
        # Run reconciliation
        logger.info("\nRunning position reconciliation...")
        result = await reconciliation.reconcile_positions()
        
        # Show reconciliation result
        logger.info("\nReconciliation result:")
        logger.info(f"  Duration: {result.duration_ms}ms")
        logger.info(f"  Exchanges checked: {result.exchanges_checked}")
        logger.info(f"  Positions checked: {result.positions_checked}")
        logger.info(f"  Matches: {result.matches}")
        logger.info(f"  Mismatches: {result.mismatches}")
        logger.info(f"  Corrections: {result.corrections}")
        logger.info(f"  Errors: {result.errors}")
        
        # Show details for each position
        logger.info("\nDetailed results:")
        for detail in result.details:
            logger.info(f"  {detail['exchange']} - {detail['symbol']}:")
            for key, value in detail.items():
                if key not in ['exchange', 'symbol']:
                    logger.info(f"    {key}: {value}")
            logger.info("")
        
        # Show final positions after reconciliation
        logger.info("\nFinal positions after reconciliation:")
        
        # Position manager positions
        manager_positions = position_manager.get_all_open_positions()
        logger.info(f"Position manager positions: {len(manager_positions)}")
        for pos in manager_positions:
            logger.info(f"  {pos.symbol}: {pos.quantity} @ {pos.entry_price}")
        
        # Shadow accounting positions
        shadow_positions = await shadow_manager.get_all_positions("demo_exchange")
        logger.info(f"Shadow accounting positions: {len(shadow_positions)}")
        for symbol, pos in shadow_positions.items():
            logger.info(f"  {symbol}: {pos['quantity']} @ {pos['price']}")
        
        # Show reconciliation stats
        stats = await reconciliation.get_reconciliation_stats()
        logger.info("\nReconciliation stats:")
        for key, value in stats.items():
            logger.info(f"  {key}: {value}")
        
        logger.info("\nDemo completed successfully")
    
    except Exception as e:
        logger.error(f"Error in demo: {str(e)}", exc_info=True)
    
    finally:
        # Cleanup
        await shadow_manager.stop()
        await reconciliation.stop()
        
        # Remove temporary directory
        shutil.rmtree(temp_dir)
        logger.info(f"Removed temporary directory: {temp_dir}")
        logger.info("=" * 70)


if __name__ == "__main__":
    asyncio.run(run_demo()) 