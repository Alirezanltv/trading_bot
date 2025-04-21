#!/usr/bin/env python3
"""
Test script for Phase 1 components:
- Enhanced Shadow Accounting with database persistence
- Multi-level stop-loss mechanisms
- Position reconciliation with exchanges
"""

import os
import sys
import asyncio
import unittest
import tempfile
import logging
import shutil
from decimal import Decimal
from datetime import datetime
from typing import Dict, Any, List, Optional

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from trading_system.position.shadow_accounting import ShadowPositionManager, PositionSource, ReconciliationStatus
from trading_system.position.position_reconciliation import PositionReconciliation, ReconciliationResult
from trading_system.risk.risk_manager import RiskManager, StopLossType, StopLossTier, MultiLevelStopLoss, StopLossConfig
from trading_system.position.position_types import Position, PositionStatus


# Mock Exchange Client
class MockExchangeClient:
    """Mock exchange client for testing position reconciliation."""
    
    def __init__(self, exchange_id="mock_exchange"):
        self.exchange_id = exchange_id
        self.positions = {}
        
    def add_position(self, symbol, quantity, price=100.0):
        """Add a position to the mock exchange."""
        self.positions[symbol] = {
            "symbol": symbol,
            "quantity": quantity,
            "price": price,
            "timestamp": datetime.now().isoformat()
        }
        
    def remove_position(self, symbol):
        """Remove a position from the mock exchange."""
        if symbol in self.positions:
            del self.positions[symbol]
    
    async def get_positions(self, symbol=None):
        """Get positions from the mock exchange."""
        if symbol:
            if symbol in self.positions:
                return [self.positions[symbol]]
            return []
        return list(self.positions.values())


# Mock Position Manager
class MockPositionManager:
    """Mock position manager for testing position reconciliation."""
    
    def __init__(self):
        self.positions = {}
        self.position_counter = 1
        
    def get_all_open_positions(self):
        """Get all open positions."""
        return [p for p in self.positions.values() if p.status != PositionStatus.CLOSED]
    
    def get_positions_by_symbol(self, symbol):
        """Get positions by symbol."""
        return [p for p in self.positions.values() if p.symbol == symbol]
    
    async def update_position_quantity(self, position_id, new_quantity, reason=None, metadata=None):
        """Update position quantity."""
        if position_id in self.positions:
            self.positions[position_id].quantity = new_quantity
            return True
        return False
    
    async def create_position(self, exchange, symbol, quantity, entry_price, metadata=None):
        """Create a new position."""
        position_id = f"pos_{self.position_counter}"
        self.position_counter += 1
        
        position = Position(
            position_id=position_id,
            exchange=exchange,
            symbol=symbol,
            quantity=quantity,
            entry_price=entry_price,
            status=PositionStatus.OPEN,
            opened_at=datetime.now(),
            metadata=metadata or {}
        )
        
        self.positions[position_id] = position
        return position_id


class TestShadowAccountingEnhanced(unittest.TestCase):
    """Test the enhanced shadow accounting system."""
    
    def setUp(self):
        # Create a temporary directory for the database
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "shadow_positions.db")
        self.backup_dir = os.path.join(self.temp_dir, "backups")
        
        # Configure shadow accounting
        self.config = {
            "db_path": self.db_path,
            "backup_dir": self.backup_dir,
            "backup_interval": 3600,  # 1 hour
            "max_backups": 5,
            "reconciliation_interval": 300,  # 5 minutes
            "auto_correct_discrepancies": True,
            "transaction_log_path": os.path.join(self.temp_dir, "transaction_log.json")
        }
        
        # Create shadow position manager
        self.shadow_manager = ShadowPositionManager(self.config)
        
        # Mock exchange client
        self.mock_exchange = MockExchangeClient("mock_exchange")
        
        # Setup event loop
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
    def tearDown(self):
        # Clean up tasks
        tasks = asyncio.all_tasks(self.loop)
        for task in tasks:
            task.cancel()
        
        # Run cleanup
        self.loop.run_until_complete(self.shadow_manager.stop())
        
        # Close the loop
        self.loop.close()
        
        # Remove temporary directory
        shutil.rmtree(self.temp_dir)
    
    def test_position_persistence(self):
        """Test position persistence and retrieval."""
        async def test_coro():
            # Initialize shadow accounting
            await self.shadow_manager.initialize()
            
            # Register exchange client
            await self.shadow_manager.register_exchange_client("mock_exchange", self.mock_exchange)
            
            # Add some positions
            pos1 = await self.shadow_manager.update_position(
                exchange_id="mock_exchange",
                symbol="BTC/USDT",
                quantity=0.5,
                price=50000.0
            )
            
            pos2 = await self.shadow_manager.update_position(
                exchange_id="mock_exchange",
                symbol="ETH/USDT",
                quantity=5.0,
                price=3000.0
            )
            
            # Retrieve positions
            positions = await self.shadow_manager.get_all_positions("mock_exchange")
            self.assertEqual(len(positions), 2)
            
            # Verify position data
            btc_pos = await self.shadow_manager.get_position("mock_exchange", "BTC/USDT")
            eth_pos = await self.shadow_manager.get_position("mock_exchange", "ETH/USDT")
            
            self.assertEqual(btc_pos["symbol"], "BTC/USDT")
            self.assertEqual(float(btc_pos["quantity"]), 0.5)
            self.assertEqual(float(btc_pos["price"]), 50000.0)
            
            self.assertEqual(eth_pos["symbol"], "ETH/USDT")
            self.assertEqual(float(eth_pos["quantity"]), 5.0)
            self.assertEqual(float(eth_pos["price"]), 3000.0)
            
            # Test position history
            btc_history = await self.shadow_manager.get_position_history("mock_exchange", "BTC/USDT")
            self.assertGreaterEqual(len(btc_history), 1)
            
            # Test creating backup
            success = await self.shadow_manager._create_backup("test")
            self.assertTrue(success)
            
            # Update a position
            updated_pos = await self.shadow_manager.update_position(
                exchange_id="mock_exchange",
                symbol="BTC/USDT",
                quantity=0.7,
                price=51000.0
            )
            
            # Verify update
            btc_pos = await self.shadow_manager.get_position("mock_exchange", "BTC/USDT")
            self.assertEqual(float(btc_pos["quantity"]), 0.7)
            self.assertEqual(float(btc_pos["price"]), 51000.0)
            
            # Test position history after update
            btc_history = await self.shadow_manager.get_position_history("mock_exchange", "BTC/USDT")
            self.assertGreaterEqual(len(btc_history), 2)
        
        self.loop.run_until_complete(test_coro())
    
    def test_reconciliation(self):
        """Test position reconciliation capability."""
        async def test_coro():
            # Initialize shadow accounting
            await self.shadow_manager.initialize()
            
            # Register exchange client
            await self.shadow_manager.register_exchange_client("mock_exchange", self.mock_exchange)
            
            # Setup positions in shadow accounting
            await self.shadow_manager.update_position(
                exchange_id="mock_exchange",
                symbol="BTC/USDT",
                quantity=0.5,
                price=50000.0
            )
            
            # Setup positions in mock exchange
            self.mock_exchange.add_position("BTC/USDT", 0.6, 51000.0)
            
            # Run reconciliation
            result = await self.shadow_manager.reconcile_positions("mock_exchange")
            
            # Verify reconciliation result
            self.assertIn("matched", result)
            self.assertIn("mismatched", result)
            self.assertIn("corrected", result)
            
            # Verify position was corrected
            btc_pos = await self.shadow_manager.get_position("mock_exchange", "BTC/USDT")
            self.assertEqual(float(btc_pos["quantity"]), 0.6)  # Should match exchange quantity
            
            # Test reconciliation history
            history = await self.shadow_manager.get_reconciliation_history()
            self.assertGreaterEqual(len(history), 1)
        
        self.loop.run_until_complete(test_coro())
    
    def test_database_corruption_recovery(self):
        """Test recovery from database corruption."""
        async def test_coro():
            # Initialize shadow accounting
            await self.shadow_manager.initialize()
            
            # Add some positions
            await self.shadow_manager.update_position(
                exchange_id="mock_exchange",
                symbol="BTC/USDT",
                quantity=0.5,
                price=50000.0
            )
            
            # Create backup
            await self.shadow_manager._create_backup("test")
            
            # Stop shadow accounting
            await self.shadow_manager.stop()
            
            # Corrupt the database
            with open(self.db_path, 'w') as f:
                f.write("CORRUPTED DATABASE")
            
            # Reinitialize with recovery
            await self.shadow_manager.initialize()
            
            # Verify position was recovered from backup
            btc_pos = await self.shadow_manager.get_position("mock_exchange", "BTC/USDT")
            self.assertIsNotNone(btc_pos)
            self.assertEqual(btc_pos["symbol"], "BTC/USDT")
            self.assertEqual(float(btc_pos["quantity"]), 0.5)
        
        self.loop.run_until_complete(test_coro())


class TestRiskManagerMultiLevel(unittest.TestCase):
    """Test the enhanced risk manager with multi-level stop-loss."""
    
    def setUp(self):
        # Configure risk manager
        self.config = {
            "default_risk_profile": "conservative",
            "risk_profiles": {
                "test_profile": {
                    "description": "Test profile",
                    "target_risk_per_trade_pct": 1.0,
                    "position_sizing_method": "fixed_risk",
                    "stop_loss_type": "fixed",
                    "stop_loss_pct": 2.0,
                    "take_profit_type": "fixed",
                    "take_profit_pct": 4.0,
                    "risk_limits": {
                        "max_position_size": 0.1,  # 10% of account
                        "max_position_value": 1000.0,
                        "max_positions_per_asset": 2,
                        "max_total_positions": 5,
                        "max_daily_drawdown_pct": 5.0,
                        "max_total_drawdown_pct": 10.0,
                        "max_risk_per_trade_pct": 1.0,
                        "max_leverage": 1.0,
                        "max_concentration_pct": 20.0,
                        "emergency_stop_drawdown_pct": 8.0
                    }
                }
            }
        }
        
        # Create risk manager
        self.risk_manager = RiskManager(self.config)
        
        # Start risk manager
        self.risk_manager.start()
        
        # Set account info
        self.risk_manager.update_account_info(10000.0, 10000.0)
    
    def tearDown(self):
        # Stop risk manager
        self.risk_manager.stop()
    
    def test_multi_level_stop_loss(self):
        """Test multi-level stop-loss calculation."""
        # Calculate multi-level stops for a long position
        stops = self.risk_manager.calculate_multi_level_stops(
            symbol="BTC/USDT",
            entry_price=50000.0,
            side="buy",
            profile_name="test_profile"
        )
        
        # Verify all stop levels are present
        self.assertIn(StopLossTier.PRIMARY.value, stops)
        self.assertIn(StopLossTier.SECONDARY.value, stops)
        self.assertIn(StopLossTier.DISASTER.value, stops)
        
        # Verify stop levels are in the correct order (for long position)
        primary_stop = stops[StopLossTier.PRIMARY.value]
        secondary_stop = stops[StopLossTier.SECONDARY.value]
        disaster_stop = stops[StopLossTier.DISASTER.value]
        
        # Primary should be closest to entry (highest price for long)
        self.assertLess(disaster_stop, secondary_stop)
        self.assertLess(secondary_stop, primary_stop)
        self.assertLess(primary_stop, 50000.0)
        
        # Do the same for a short position
        stops = self.risk_manager.calculate_multi_level_stops(
            symbol="BTC/USDT",
            entry_price=50000.0,
            side="sell",
            profile_name="test_profile"
        )
        
        # Verify stop levels are in the correct order (for short position)
        primary_stop = stops[StopLossTier.PRIMARY.value]
        secondary_stop = stops[StopLossTier.SECONDARY.value]
        disaster_stop = stops[StopLossTier.DISASTER.value]
        
        # Primary should be closest to entry (lowest price for short)
        self.assertGreater(disaster_stop, secondary_stop)
        self.assertGreater(secondary_stop, primary_stop)
        self.assertGreater(primary_stop, 50000.0)
    
    def test_stop_loss_registration_and_checking(self):
        """Test registration and checking of stop loss levels."""
        # Calculate stops
        stops = self.risk_manager.calculate_multi_level_stops(
            symbol="BTC/USDT",
            entry_price=50000.0,
            side="buy",
            profile_name="test_profile"
        )
        
        # Register stops for a position
        position_id = "test_position_1"
        self.risk_manager.register_position_stop_losses(position_id, stops)
        
        # Get registered stops
        registered_stops = self.risk_manager.get_position_stop_losses(position_id)
        
        # Verify stops match
        for tier in [StopLossTier.PRIMARY.value, StopLossTier.SECONDARY.value, StopLossTier.DISASTER.value]:
            self.assertIn(tier, registered_stops)
            self.assertEqual(stops[tier], registered_stops[tier])
        
        # Check if stop loss triggers (price above all stops)
        triggered, tier, price = self.risk_manager.check_stop_loss_triggers(
            position_id=position_id,
            current_price=49000.0,  # Above all stops
            side="buy"
        )
        self.assertFalse(triggered)
        
        # Check if primary stop loss triggers
        primary_stop = stops[StopLossTier.PRIMARY.value]
        triggered, tier, price = self.risk_manager.check_stop_loss_triggers(
            position_id=position_id,
            current_price=primary_stop - 1.0,  # Just below primary stop
            side="buy"
        )
        self.assertTrue(triggered)
        self.assertEqual(tier, StopLossTier.PRIMARY.value)
        
        # Check if secondary stop loss triggers
        secondary_stop = stops[StopLossTier.SECONDARY.value]
        triggered, tier, price = self.risk_manager.check_stop_loss_triggers(
            position_id=position_id,
            current_price=secondary_stop - 1.0,  # Below secondary stop
            side="buy"
        )
        self.assertTrue(triggered)
        
        # Check if disaster stop loss triggers
        disaster_stop = stops[StopLossTier.DISASTER.value]
        triggered, tier, price = self.risk_manager.check_stop_loss_triggers(
            position_id=position_id,
            current_price=disaster_stop - 1.0,  # Below disaster stop
            side="buy"
        )
        self.assertTrue(triggered)
        
        # Remove stop losses
        self.risk_manager.remove_position_stop_losses(position_id)
        
        # Verify stops are removed
        registered_stops = self.risk_manager.get_position_stop_losses(position_id)
        self.assertEqual(registered_stops, {})
    
    def test_trailing_stop_update(self):
        """Test updating of trailing stop loss."""
        # Calculate stops with trailing stop
        stops = self.risk_manager.calculate_multi_level_stops(
            symbol="BTC/USDT",
            entry_price=50000.0,
            side="buy",
            profile_name="test_profile"
        )
        
        # Add trailing stop if not present
        if StopLossTier.TRAILING.value not in stops:
            stops[StopLossTier.TRAILING.value] = 49000.0
        
        # Register stops for a position
        position_id = "test_position_2"
        self.risk_manager.register_position_stop_losses(position_id, stops)
        
        # Initial trailing stop
        initial_trailing = stops[StopLossTier.TRAILING.value]
        
        # Update trailing stop with higher price (for long position)
        updated_stop = self.risk_manager.update_trailing_stop(
            position_id=position_id,
            current_price=51000.0,  # Higher than entry
            side="buy"
        )
        
        # Verify trailing stop was updated (moved higher)
        self.assertIsNotNone(updated_stop)
        self.assertGreater(updated_stop, initial_trailing)
        
        # Get updated stops
        updated_stops = self.risk_manager.get_position_stop_losses(position_id)
        self.assertGreater(updated_stops[StopLossTier.TRAILING.value], initial_trailing)
        
        # Check if trailing stop triggers
        triggered, tier, price = self.risk_manager.check_stop_loss_triggers(
            position_id=position_id,
            current_price=updated_stops[StopLossTier.TRAILING.value] - 1.0,
            side="buy"
        )
        self.assertTrue(triggered)
        self.assertEqual(tier, StopLossTier.TRAILING.value)


class TestPositionReconciliation(unittest.TestCase):
    """Test the position reconciliation system."""
    
    def setUp(self):
        # Configure position reconciliation
        self.config = {
            "reconciliation_interval": 3600,  # 1 hour
            "auto_correct": True,
            "tolerance": 0.001,
            "critical_threshold": 0.05  # 5%
        }
        
        # Create components
        self.position_reconciliation = PositionReconciliation(self.config)
        self.position_manager = MockPositionManager()
        self.shadow_manager = ShadowPositionManager({
            "db_path": ":memory:",  # In-memory SQLite database for testing
            "backup_interval": 0,  # Disable backups for testing
            "reconciliation_interval": 0  # Disable automatic reconciliation
        })
        self.exchange_client = MockExchangeClient("test_exchange")
        
        # Setup event loop
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        # Initialize components
        self.loop.run_until_complete(self.shadow_manager.initialize())
        self.loop.run_until_complete(self.position_reconciliation.initialize())
        
        # Register components
        self.position_reconciliation.register_position_manager(self.position_manager)
        self.position_reconciliation.register_shadow_manager(self.shadow_manager)
        self.position_reconciliation.register_exchange_client("test_exchange", self.exchange_client)
    
    def tearDown(self):
        # Clean up tasks
        tasks = asyncio.all_tasks(self.loop)
        for task in tasks:
            task.cancel()
        
        # Run cleanup
        self.loop.run_until_complete(self.shadow_manager.stop())
        self.loop.run_until_complete(self.position_reconciliation.stop())
        
        # Close the loop
        self.loop.close()
    
    def test_reconciliation_matching_positions(self):
        """Test reconciliation with matching positions."""
        async def test_coro():
            # Setup matching positions
            symbol = "BTC/USDT"
            quantity = 0.5
            price = 50000.0
            
            # Add to position manager
            position_id = await self.position_manager.create_position(
                exchange="test_exchange",
                symbol=symbol,
                quantity=quantity,
                entry_price=price
            )
            
            # Add to shadow accounting
            await self.shadow_manager.update_position(
                exchange_id="test_exchange",
                symbol=symbol,
                quantity=quantity,
                price=price
            )
            
            # Add to exchange
            self.exchange_client.add_position(symbol, quantity, price)
            
            # Run reconciliation
            result = await self.position_reconciliation.reconcile_positions()
            
            # Verify reconciliation result
            self.assertEqual(result.mismatches, 0)
            self.assertEqual(result.matches, 1)
            self.assertEqual(result.corrections, 0)
            self.assertEqual(result.errors, 0)
        
        self.loop.run_until_complete(test_coro())
    
    def test_reconciliation_mismatched_positions(self):
        """Test reconciliation with mismatched positions."""
        async def test_coro():
            # Setup mismatched positions
            symbol = "ETH/USDT"
            
            # Different quantities in each system
            manager_qty = 1.0
            shadow_qty = 1.1
            exchange_qty = 1.2
            price = 3000.0
            
            # Add to position manager
            position_id = await self.position_manager.create_position(
                exchange="test_exchange",
                symbol=symbol,
                quantity=manager_qty,
                entry_price=price
            )
            
            # Add to shadow accounting
            await self.shadow_manager.update_position(
                exchange_id="test_exchange",
                symbol=symbol,
                quantity=shadow_qty,
                price=price
            )
            
            # Add to exchange
            self.exchange_client.add_position(symbol, exchange_qty, price)
            
            # Run reconciliation
            result = await self.position_reconciliation.reconcile_positions()
            
            # Verify reconciliation result
            self.assertEqual(result.mismatches, 1)
            self.assertEqual(result.matches, 0)
            self.assertEqual(result.corrections, 1)  # Auto-correct is enabled
            self.assertEqual(result.errors, 0)
            
            # Verify positions were corrected to match exchange
            shadow_pos = (await self.shadow_manager.get_position("test_exchange", symbol))
            self.assertAlmostEqual(float(shadow_pos["quantity"]), exchange_qty, places=3)
            
            # Position manager should be updated too
            manager_pos = self.position_manager.get_positions_by_symbol(symbol)[0]
            self.assertAlmostEqual(manager_pos.quantity, exchange_qty, places=3)
        
        self.loop.run_until_complete(test_coro())
    
    def test_reconciliation_missing_positions(self):
        """Test reconciliation with positions missing in some systems."""
        async def test_coro():
            # Setup a position that exists only in the exchange
            symbol = "LTC/USDT"
            quantity = 10.0
            price = 200.0
            
            # Add only to exchange
            self.exchange_client.add_position(symbol, quantity, price)
            
            # Run reconciliation
            result = await self.position_reconciliation.reconcile_positions()
            
            # Verify reconciliation result
            self.assertEqual(result.mismatches, 1)
            self.assertEqual(result.matches, 0)
            self.assertEqual(result.corrections, 1)  # Auto-correct is enabled
            
            # Verify position was created in position manager
            manager_positions = self.position_manager.get_positions_by_symbol(symbol)
            self.assertEqual(len(manager_positions), 1)
            self.assertAlmostEqual(manager_positions[0].quantity, quantity, places=3)
            
            # Verify position was created in shadow accounting
            shadow_pos = (await self.shadow_manager.get_position("test_exchange", symbol))
            self.assertIsNotNone(shadow_pos)
            self.assertAlmostEqual(float(shadow_pos["quantity"]), quantity, places=3)
        
        self.loop.run_until_complete(test_coro())


if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)
    unittest.main() 