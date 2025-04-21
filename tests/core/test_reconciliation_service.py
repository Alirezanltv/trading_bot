"""
Test Reconciliation Service

This module contains tests for the reconciliation service implementation.
"""

import unittest
import asyncio
import sys
from unittest.mock import MagicMock, patch, AsyncMock
from datetime import datetime, timedelta
from decimal import Decimal
import enum

# Create mock modules to avoid import errors
class MockMessageBus:
    def __init__(self):
        pass
        
    async def publish(self, *args, **kwargs):
        pass
        
    def publish_sync(self, *args, **kwargs):
        pass

# Add mock modules to sys.modules
sys.modules['trading_system.core.message_bus'] = MagicMock()
sys.modules['trading_system.core.rabbitmq_broker'] = MagicMock()
sys.modules['trading_system.core.async_rabbitmq_broker'] = MagicMock()

from trading_system.core.reconciliation_service import (
    ReconciliationService,
    ReconciliationStatus,
    ReconciliationPriority
)
from trading_system.core.transaction_verification import (
    TransactionVerifier, 
    VerificationStatus, 
    VerificationOutcome
)
from trading_system.core.logging import get_logger

# Set up logger
logger = get_logger("tests.core.reconciliation_service")


class MockOrder:
    """Mock order for testing."""
    
    def __init__(self, order_id, symbol, status="open"):
        self.order_id = order_id
        self.symbol = symbol
        self.status = status
        self.price = Decimal("100.0")
        self.quantity = Decimal("1.0")
        self.side = "buy"
        
    def is_working(self):
        """Check if order is in working state."""
        return self.status == "open"
        
    def to_dict(self):
        """Convert to dictionary."""
        return {
            "order_id": self.order_id,
            "symbol": self.symbol,
            "status": self.status,
            "price": float(self.price),
            "quantity": float(self.quantity),
            "side": self.side
        }


class MockFill:
    """Mock fill for testing."""
    
    def __init__(self, fill_id, order_id, symbol, price=Decimal("100.0"), quantity=Decimal("1.0")):
        self.fill_id = fill_id
        self.order_id = order_id
        self.symbol = symbol
        self.price = price
        self.quantity = quantity
        self.timestamp = datetime.now()


class MockPosition:
    """Mock position for testing."""
    
    def __init__(self, symbol, size, avg_price=Decimal("100.0")):
        self.symbol = symbol
        self.size = size
        self.avg_price = avg_price
        self.realized_pnl = Decimal("0")
        self.unrealized_pnl = Decimal("0")


class TestReconciliationService(unittest.IsolatedAsyncioTestCase):
    """Test case for the ReconciliationService class."""
    
    async def asyncSetUp(self):
        """Set up test environment."""
        # Create mock subsystems
        self.mock_execution = AsyncMock()
        self.mock_market_data = AsyncMock()
        self.mock_position_manager = AsyncMock()
        self.mock_transaction_verifier = AsyncMock()
        
        # Configure mock returns
        self.configure_mocks()
        
        # Create reconciliation service
        self.reconciliation_service = ReconciliationService(
            execution_subsystem=self.mock_execution,
            market_data_subsystem=self.mock_market_data,
            position_manager=self.mock_position_manager,
            transaction_verifier=self.mock_transaction_verifier,
            reconciliation_interval=1,  # Use short interval for testing
            balance_threshold=0.001,
            position_threshold=0.001
        )
        
        # Initialize service
        await self.reconciliation_service.initialize()
        
    def configure_mocks(self):
        """Configure mock return values."""
        # Mock execution subsystem
        self.mock_execution.get_open_orders.return_value = [
            MockOrder("order1", "BTC/USDT"),
            MockOrder("order2", "ETH/USDT")
        ]
        
        self.mock_execution.get_exchange_open_orders.return_value = [
            {"id": "order1", "symbol": "BTC/USDT", "status": "open"},
            {"id": "order3", "symbol": "XRP/USDT", "status": "open"}
        ]
        
        self.mock_execution.get_recent_fills.return_value = [
            MockFill("fill1", "order1", "BTC/USDT"),
            MockFill("fill2", "order2", "ETH/USDT")
        ]
        
        self.mock_execution.get_exchange_recent_trades.return_value = [
            {"id": "fill1", "order_id": "order1", "symbol": "BTC/USDT"},
            {"id": "fill3", "order_id": "order3", "symbol": "XRP/USDT"}
        ]
        
        self.mock_execution.get_exchange_balances.return_value = {
            "BTC": Decimal("1.0"),
            "ETH": Decimal("10.0"),
            "USDT": Decimal("5000.0"),
            "XRP": Decimal("1000.0")
        }
        
        self.mock_execution.get_exchange_positions.return_value = {
            "BTC/USDT": {"symbol": "BTC/USDT", "size": Decimal("1.0")},
            "ETH/USDT": {"symbol": "ETH/USDT", "size": Decimal("10.0")},
            "XRP/USDT": {"symbol": "XRP/USDT", "size": Decimal("1000.0")}
        }
        
        # Mock position manager
        self.mock_position_manager.get_account_balances.return_value = {
            "BTC": Decimal("1.0"),
            "ETH": Decimal("10.0"),
            "USDT": Decimal("5000.0")
        }
        
        self.mock_position_manager.get_all_positions.return_value = {
            "BTC/USDT": MockPosition("BTC/USDT", Decimal("1.0")),
            "ETH/USDT": MockPosition("ETH/USDT", Decimal("10.0")),
            "LTC/USDT": MockPosition("LTC/USDT", Decimal("5.0"))
        }
        
        # Mock market data
        self.mock_market_data.get_ticker.return_value = MagicMock(
            last_price=Decimal("50000.0")
        )
        
    async def asyncTearDown(self):
        """Clean up after tests."""
        await self.reconciliation_service.stop()
    
    async def test_initialize(self):
        """Test initialization."""
        # Check initial state
        self.assertFalse(self.reconciliation_service._running)
        self.assertIsNone(self.reconciliation_service._last_reconciliation)
        
        # Initialize again
        await self.reconciliation_service.initialize()
        
        # Check state is still correct
        self.assertFalse(self.reconciliation_service._running)
        self.assertIsNone(self.reconciliation_service._last_reconciliation)
    
    async def test_start_stop(self):
        """Test starting and stopping the service."""
        # Start service
        await self.reconciliation_service.start()
        
        # Check running state
        self.assertTrue(self.reconciliation_service._running)
        self.assertIsNotNone(self.reconciliation_service._reconciliation_task)
        
        # Stop service
        await self.reconciliation_service.stop()
        
        # Check stopped state
        self.assertFalse(self.reconciliation_service._running)
        self.assertIsNone(self.reconciliation_service._reconciliation_task)
    
    async def test_reconcile_open_orders(self):
        """Test reconciliation of open orders."""
        # Run reconciliation
        await self.reconciliation_service._reconcile_open_orders()
        
        # Verify calls
        self.mock_execution.get_open_orders.assert_called_once()
        self.mock_execution.get_exchange_open_orders.assert_called_once()
        
        # Check that the import method was called for the missing order
        self.mock_execution.import_exchange_order.assert_called_once()
        call_args = self.mock_execution.import_exchange_order.call_args[0][0]
        self.assertEqual(call_args["id"], "order3")
        
        # Check that the missing exchange order was handled
        self.mock_execution.get_exchange_order_history.assert_called_once_with("order2")
    
    async def test_reconcile_recent_trades(self):
        """Test reconciliation of recent trades."""
        # Run reconciliation
        await self.reconciliation_service._reconcile_recent_trades()
        
        # Verify calls
        self.mock_execution.get_recent_fills.assert_called_once()
        self.mock_execution.get_exchange_recent_trades.assert_called_once()
        
        # Check that the import method was called for the missing trade
        self.mock_execution.import_exchange_trade.assert_called_once()
        call_args = self.mock_execution.import_exchange_trade.call_args[0][0]
        self.assertEqual(call_args["id"], "fill3")
        
        # Check that the position was updated
        self.mock_position_manager.process_fill.assert_called_once()
    
    async def test_reconcile_balances(self):
        """Test reconciliation of account balances."""
        # Run reconciliation
        await self.reconciliation_service._reconcile_balances()
        
        # Verify calls
        self.mock_position_manager.get_account_balances.assert_called_once()
        self.mock_execution.get_exchange_balances.assert_called_once()
        
        # Check that the balance correction was called for the missing asset
        self.mock_position_manager.correct_balance.assert_called_once_with("XRP", Decimal("1000.0"))
    
    async def test_reconcile_positions(self):
        """Test reconciliation of positions."""
        # Run reconciliation
        await self.reconciliation_service._reconcile_positions()
        
        # Verify calls
        self.mock_position_manager.get_all_positions.assert_called_once()
        self.mock_execution.get_exchange_positions.assert_called_once()
        
        # Verify position corrections
        calls = self.mock_position_manager.correct_position.call_args_list
        self.assertEqual(len(calls), 2)
        
        # Check that both positions were corrected
        # Note: Order of calls might vary, so create a set of symbols
        corrected_symbols = {calls[0][0][0], calls[1][0][0]}
        self.assertIn("LTC/USDT", corrected_symbols)
        self.assertIn("XRP/USDT", corrected_symbols)
        
        # Verify correct values were passed
        for call in calls:
            symbol, size, price = call[0]
            if symbol == "LTC/USDT":
                self.assertEqual(size, Decimal("0"))
            elif symbol == "XRP/USDT":
                self.assertEqual(size, Decimal("1000.0"))
    
    async def test_perform_reconciliation(self):
        """Test full reconciliation process."""
        # Run reconciliation
        await self.reconciliation_service._perform_reconciliation()
        
        # Verify all reconciliation methods were called
        self.mock_execution.get_open_orders.assert_called_once()
        self.mock_execution.get_recent_fills.assert_called_once()
        self.mock_position_manager.get_account_balances.assert_called_once()
        self.mock_position_manager.get_all_positions.assert_called_once()
    
    async def test_reconciliation_loop(self):
        """Test the main reconciliation loop."""
        # Start service but then stop it immediately
        self.reconciliation_service._running = True
        
        # Create a task to run the loop for a short time
        loop_task = asyncio.create_task(self.reconciliation_service._reconciliation_loop())
        
        # Allow some time for the loop to run
        await asyncio.sleep(0.1)
        
        # Stop the service
        self.reconciliation_service._running = False
        loop_task.cancel()
        
        try:
            await loop_task
        except asyncio.CancelledError:
            pass
        
        # Check that the reconciliation was performed
        self.mock_execution.get_open_orders.assert_called()
    
    async def test_pending_tasks(self):
        """Test handling of pending tasks."""
        # Add some pending tasks
        self.reconciliation_service._add_pending_task(
            "import_order", {"id": "test_order"}, ReconciliationPriority.HIGH
        )
        self.reconciliation_service._add_pending_task(
            "correct_balance", {"asset": "BTC", "amount": Decimal("2.0")}, ReconciliationPriority.MEDIUM
        )
        
        # Process tasks
        await self.reconciliation_service._process_pending_tasks()
        
        # Verify tasks were processed
        self.mock_execution.import_exchange_order.assert_called_with({"id": "test_order"})
        self.mock_position_manager.correct_balance.assert_called_with("BTC", Decimal("2.0"))
    
    async def test_force_reconciliation(self):
        """Test forcing reconciliation."""
        # Set up running state
        self.reconciliation_service._running = True
        
        # Force reconciliation (in async context)
        self.assertTrue(self.reconciliation_service.force_reconciliation())
        
        # Check that a task was created
        self.assertIsNotNone(self.reconciliation_service._reconciliation_task)
        
        # Clean up task to avoid warnings
        self.reconciliation_service._reconciliation_task.cancel()
        try:
            await self.reconciliation_service._reconciliation_task
        except asyncio.CancelledError:
            pass
    
    def test_get_reconciliation_stats(self):
        """Test getting reconciliation statistics."""
        # Add some test data
        self.reconciliation_service._reconciliation_stats["total_runs"] = 5
        self.reconciliation_service._reconciliation_stats["discrepancies_found"] = 10
        self.reconciliation_service._last_reconciliation = datetime.now()
        
        # Add pending tasks
        self.reconciliation_service._add_pending_task(
            "import_order", {"id": "test_order"}, ReconciliationPriority.HIGH
        )
        
        # Get stats
        stats = self.reconciliation_service.get_reconciliation_stats()
        
        # Check stats
        self.assertEqual(stats["total_runs"], 5)
        self.assertEqual(stats["discrepancies_found"], 10)
        self.assertEqual(stats["pending_tasks"], 1)
        self.assertIn("pending_task_breakdown", stats)
        self.assertIn("import_order", stats["pending_task_breakdown"])
        self.assertEqual(stats["pending_task_breakdown"]["import_order"]["HIGH"], 1)


if __name__ == "__main__":
    unittest.main() 