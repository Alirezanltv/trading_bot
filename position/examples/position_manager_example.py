"""
Position Management Example

This script demonstrates the usage of the Position Management subsystem.
It creates sample positions, orders, and triggers, and shows how to manage
portfolio state.
"""

import os
import sys
import logging
import time
from datetime import datetime, timedelta
import random
from typing import Dict, Any

# Add parent directory to path to allow imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from trading_system.position.position_manager import PositionManager
from trading_system.position.position_types import (
    Position, Order, PositionTrigger, PositionStatus, 
    PositionType, OrderAction, OrderStatus, OrderType,
    TriggerType, PositionSource, RiskLevel
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("position_example")


def create_sample_config() -> Dict[str, Any]:
    """Create a sample configuration for the Position Manager."""
    return {
        "position_management": {
            "max_exposure": 0.8,  # Maximum 80% of portfolio in positions
            "max_symbol_exposure": 0.25,  # Maximum 25% in a single symbol
            "max_strategy_exposure": 0.5,  # Maximum 50% in a single strategy
            "default_stop_loss": 0.05,  # Default 5% stop loss
            "default_take_profit": 0.1,  # Default 10% take profit
            "monitoring_interval": 10,  # Check triggers every 10 seconds
            "reconciliation_interval": 3600  # Reconcile with exchange every hour
        },
        "position_sizing": {
            "default_type": "fixed_risk",
            "default_config": {
                "risk_percentage": 0.01  # Risk 1% per trade
            },
            "strategies": {
                "trend_following": {
                    "type": "percentage_of_equity",
                    "config": {
                        "percentage": 0.05  # Allocate 5% per trade
                    }
                },
                "mean_reversion": {
                    "type": "volatility",
                    "config": {
                        "base_risk_percentage": 0.01,
                        "volatility_scale_factor": 1.5
                    }
                }
            }
        }
    }


def create_sample_positions(position_manager: PositionManager) -> Dict[str, Position]:
    """Create sample positions."""
    logger.info("Creating sample positions...")
    
    positions = {}
    
    # Long BTC position
    positions["btc_long"] = position_manager.create_position(
        symbol="BTC/USDT",
        position_type=PositionType.LONG,
        quantity=0.1,
        entry_price=50000.0,
        strategy_id="trend_following",
        exchange="binance",
        metadata={"signal_source": "moving_average_crossover"},
        tags=["crypto", "bitcoin", "trend"]
    )
    
    # Add a stop loss to BTC position
    btc_stop = PositionTrigger(
        position_id=positions["btc_long"].position_id,
        trigger_type=TriggerType.STOP_LOSS,
        price=47500.0,  # 5% below entry
        percentage=-5.0
    )
    position_manager.add_trigger(btc_stop)
    
    # Add a take profit to BTC position
    btc_tp = PositionTrigger(
        position_id=positions["btc_long"].position_id,
        trigger_type=TriggerType.TAKE_PROFIT,
        price=55000.0,  # 10% above entry
        percentage=10.0
    )
    position_manager.add_trigger(btc_tp)
    
    # Short ETH position
    positions["eth_short"] = position_manager.create_position(
        symbol="ETH/USDT",
        position_type=PositionType.SHORT,
        quantity=1.0,
        entry_price=3000.0,
        strategy_id="mean_reversion",
        exchange="binance",
        metadata={"signal_source": "rsi_overbought"},
        tags=["crypto", "ethereum", "mean_reversion"]
    )
    
    # Add a stop loss to ETH position
    eth_stop = PositionTrigger(
        position_id=positions["eth_short"].position_id,
        trigger_type=TriggerType.STOP_LOSS,
        price=3150.0,  # 5% above entry
        percentage=5.0
    )
    position_manager.add_trigger(eth_stop)
    
    # Add a trailing stop to ETH position
    eth_trail = PositionTrigger(
        position_id=positions["eth_short"].position_id,
        trigger_type=TriggerType.TRAILING_STOP,
        price=3150.0,  # Initial stop at 5% above entry
        trail_value=50.0  # Trail by $50
    )
    position_manager.add_trigger(eth_trail)
    
    # Long SOL position
    positions["sol_long"] = position_manager.create_position(
        symbol="SOL/USDT",
        position_type=PositionType.LONG,
        quantity=10.0,
        entry_price=100.0,
        strategy_id="trend_following",
        exchange="binance",
        metadata={"signal_source": "support_bounce"},
        tags=["crypto", "solana", "trend"]
    )
    
    # Time-based exit for SOL position
    sol_time = PositionTrigger(
        position_id=positions["sol_long"].position_id,
        trigger_type=TriggerType.TIME_LIMIT,
        expiration_time=datetime.now() + timedelta(days=7)  # 7-day time limit
    )
    position_manager.add_trigger(sol_time)
    
    logger.info(f"Created {len(positions)} sample positions")
    return positions


def create_sample_orders(position_manager: PositionManager, positions: Dict[str, Position]) -> None:
    """Create sample orders for positions."""
    logger.info("Creating sample orders...")
    
    # Create a market order for BTC position
    btc_order = Order(
        position_id=positions["btc_long"].position_id,
        symbol="BTC/USDT",
        order_type=OrderType.MARKET,
        action=OrderAction.BUY,
        quantity=0.1,
        price=50000.0,
        exchange="binance",
        status=OrderStatus.FILLED,
        filled_quantity=0.1,
        average_fill_price=50000.0,
        metadata={"signal_id": "bt_123"}
    )
    position_manager.add_order(btc_order)
    
    # Create a limit order for ETH position
    eth_order = Order(
        position_id=positions["eth_short"].position_id,
        symbol="ETH/USDT",
        order_type=OrderType.LIMIT,
        action=OrderAction.SELL,
        quantity=1.0,
        price=3000.0,
        exchange="binance",
        status=OrderStatus.FILLED,
        filled_quantity=1.0,
        average_fill_price=3000.0,
        metadata={"signal_id": "eth_456"}
    )
    position_manager.add_order(eth_order)
    
    # Create a market order for SOL position
    sol_order = Order(
        position_id=positions["sol_long"].position_id,
        symbol="SOL/USDT",
        order_type=OrderType.MARKET,
        action=OrderAction.BUY,
        quantity=10.0,
        price=100.0,
        exchange="binance",
        status=OrderStatus.FILLED,
        filled_quantity=10.0,
        average_fill_price=100.0,
        metadata={"signal_id": "sol_789"}
    )
    position_manager.add_order(sol_order)
    
    # Update positions to OPEN status since orders are filled
    for pos_key, position in positions.items():
        position_manager.update_position(
            position.position_id,
            status=PositionStatus.OPEN,
            opened_at=datetime.now()
        )
    
    logger.info("Created and filled sample orders")


def simulate_market_updates(position_manager: PositionManager, positions: Dict[str, Position]) -> None:
    """Simulate market updates and position monitoring."""
    logger.info("Starting market update simulation...")
    
    # Initial portfolio balance
    position_manager.update_portfolio_balance(100000.0, 80000.0)
    
    # Simulate 10 price updates
    for i in range(1, 11):
        logger.info(f"Market update {i}/10")
        
        # Generate random price movements
        btc_price = 50000.0 + random.uniform(-2000, 2000)
        eth_price = 3000.0 + random.uniform(-150, 150)
        sol_price = 100.0 + random.uniform(-5, 5)
        
        # Update position prices
        position_manager.process_market_update("BTC/USDT", btc_price)
        position_manager.process_market_update("ETH/USDT", eth_price)
        position_manager.process_market_update("SOL/USDT", sol_price)
        
        # Get updated portfolio
        portfolio = position_manager.get_portfolio_summary()
        logger.info(f"Portfolio status: {portfolio.open_position_count} open positions, "
                  f"{portfolio.current_exposure:.2%} exposure, "
                  f"${portfolio.total_pnl:.2f} PnL")
        
        # Check if any positions were closed by triggers
        closed_positions = position_manager.get_positions(status=PositionStatus.CLOSED)
        for pos in closed_positions:
            logger.info(f"Position {pos.position_id} ({pos.symbol}) was closed with PnL: ${pos.pnl:.2f}")
        
        # Simulate time passing
        time.sleep(1)


def close_remaining_positions(position_manager: PositionManager) -> None:
    """Close any remaining open positions."""
    logger.info("Closing remaining open positions...")
    
    open_positions = position_manager.get_open_positions()
    
    for position in open_positions:
        # Generate a closing price (current price or random)
        exit_price = position.current_price or (
            position.entry_price * (1 + random.uniform(-0.05, 0.1))
        )
        
        # Close the position
        position_manager.close_position(
            position_id=position.position_id,
            exit_price=exit_price
        )
        
        logger.info(f"Closed position {position.position_id} ({position.symbol}) at price {exit_price}")
    
    # Get final portfolio state
    portfolio = position_manager.get_portfolio_summary()
    logger.info(f"Final portfolio: ${portfolio.total_equity:.2f} equity, "
              f"${portfolio.total_pnl:.2f} total PnL")


def analyze_positions(position_manager: PositionManager) -> None:
    """Analyze closed positions and performance."""
    logger.info("Analyzing positions...")
    
    # Get all positions
    all_positions = position_manager.get_positions()
    
    # Analyze by strategy
    strategy_pnl = {}
    strategy_count = {}
    
    for position in all_positions:
        strategy = position.strategy_id or "unknown"
        
        if strategy not in strategy_pnl:
            strategy_pnl[strategy] = 0.0
            strategy_count[strategy] = 0
        
        strategy_pnl[strategy] += position.pnl
        strategy_count[strategy] += 1
    
    # Print strategy analysis
    logger.info("Strategy performance:")
    for strategy, pnl in strategy_pnl.items():
        count = strategy_count[strategy]
        avg_pnl = pnl / count if count > 0 else 0
        logger.info(f"  {strategy}: {count} positions, ${pnl:.2f} total PnL, ${avg_pnl:.2f} avg PnL")
    
    # Analyze by symbol
    symbol_pnl = {}
    
    for position in all_positions:
        symbol = position.symbol
        
        if symbol not in symbol_pnl:
            symbol_pnl[symbol] = 0.0
        
        symbol_pnl[symbol] += position.pnl
    
    # Print symbol analysis
    logger.info("Symbol performance:")
    for symbol, pnl in symbol_pnl.items():
        logger.info(f"  {symbol}: ${pnl:.2f} PnL")


def main():
    """Main function to run the position management example."""
    # Create data directory
    os.makedirs("./data/positions", exist_ok=True)
    
    # Create configuration
    config = create_sample_config()
    
    # Create position manager
    position_manager = PositionManager(
        config=config,
        data_dir="./data/positions",
        max_exposure=config["position_management"]["max_exposure"],
        max_symbol_exposure=config["position_management"]["max_symbol_exposure"],
        max_strategy_exposure=config["position_management"]["max_strategy_exposure"]
    )
    
    try:
        # Create sample positions and orders
        positions = create_sample_positions(position_manager)
        create_sample_orders(position_manager, positions)
        
        # Simulate market updates
        simulate_market_updates(position_manager, positions)
        
        # Close remaining positions
        close_remaining_positions(position_manager)
        
        # Analyze positions
        analyze_positions(position_manager)
        
        logger.info("Position management example completed successfully")
    
    except Exception as e:
        logger.error(f"Error in position management example: {e}")
        raise
    
    finally:
        # Clean up resources
        pass


if __name__ == "__main__":
    main() 