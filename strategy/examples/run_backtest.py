#!/usr/bin/env python
"""
Strategy Backtest Runner

This script runs a backtest for a trading strategy and displays the results.
"""

import asyncio
import argparse
import json
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))

from trading_system.strategy.backtest import BacktestConfig, BacktestEngine
from trading_system.strategy.examples.rsi_strategy import RSIStrategy
from trading_system.core.logging import setup_logging, get_logger

# Setup logging
setup_logging()
logger = get_logger("backtest_runner")


async def run_strategy_backtest(config_file: Optional[str] = None) -> None:
    """
    Run a backtest for a strategy using the specified configuration file.
    
    Args:
        config_file: Path to configuration file (JSON)
    """
    try:
        # Load configuration from file or use defaults
        config = None
        if config_file and os.path.exists(config_file):
            logger.info(f"Loading configuration from {config_file}")
            with open(config_file, 'r') as f:
                config_data = json.load(f)
                config = BacktestConfig.from_dict(config_data)
        else:
            # Use default configuration
            logger.info("Using default configuration")
            config = BacktestConfig(
                symbol="BTC/USDT",
                timeframe="1h",
                start_time=datetime.now() - timedelta(days=30),
                end_time=datetime.now(),
                initial_capital=10000.0,
                commission=0.001,
                slippage=0.0005,
                use_market_orders=True,
                position_sizing="fixed",
                position_size=1.0,
                allow_short=True,
                include_fees=True
            )
        
        # Create strategy
        logger.info("Creating RSI strategy")
        strategy = RSIStrategy({
            "rsi_period": 14,
            "overbought_threshold": 70,
            "oversold_threshold": 30,
            "exit_threshold": 50,
            "confirmation_bars": 1,
            "use_trend_filter": True,
            "trend_period": 200
        })
        
        # Create backtest engine
        logger.info("Creating backtest engine")
        engine = BacktestEngine(config)
        
        # Run backtest
        logger.info(f"Running backtest for {strategy.name} on {config.symbol} {config.timeframe}")
        logger.info(f"Period: {config.start_time.date()} to {config.end_time.date()}")
        
        result = await engine.run_backtest(strategy)
        
        if result:
            # Display results
            display_results(result)
            
            # Save results
            output_file = f"backtest_results_{strategy.name.replace(' ', '_')}_{config.symbol.replace('/', '_')}_{config.timeframe}.json"
            if engine.save_result(result, output_file):
                logger.info(f"Results saved to {output_file}")
        else:
            logger.error("Backtest failed to produce results")
    
    except Exception as e:
        logger.error(f"Error running backtest: {e}")


def display_results(result) -> None:
    """
    Display backtest results.
    
    Args:
        result: Backtest result
    """
    # Print summary
    print("\n" + "=" * 60)
    print(f"BACKTEST RESULTS: {result.strategy_name}")
    print("=" * 60)
    print(f"Symbol: {result.config.symbol} | Timeframe: {result.config.timeframe}")
    print(f"Period: {result.config.start_time.date()} to {result.config.end_time.date()}")
    print(f"Initial Capital: ${result.config.initial_capital:.2f}")
    print("-" * 60)
    print(f"Final Capital: ${result.metrics['final_capital']:.2f}")
    print(f"Total Return: {result.metrics['total_return']:.2%}")
    print(f"Annualized Return: {result.metrics['annualized_return']:.2%}")
    print(f"Max Drawdown: {result.metrics['max_drawdown']:.2%}")
    print(f"Sharpe Ratio: {result.metrics['sharpe_ratio']:.2f}")
    print("-" * 60)
    print(f"Total Trades: {result.metrics['trades']}")
    print(f"Win Rate: {result.metrics['win_rate']:.2%}")
    print(f"Profit Factor: {result.metrics['profit_factor']:.2f}")
    print(f"Avg. Trade: {result.metrics.get('avg_trade_pnl_percent', 0):.2%}")
    
    # Display trades
    if result.trades:
        print("\nTRADES:")
        print("-" * 60)
        print(f"{'Direction':<8} {'Entry Time':<20} {'Exit Time':<20} {'Entry':<10} {'Exit':<10} {'PnL':<10} {'PnL %':<10}")
        print("-" * 60)
        
        for i, trade in enumerate(result.trades[:10]):  # Show first 10 trades
            print(f"{trade.direction:<8} {trade.entry_time.strftime('%Y-%m-%d %H:%M'):<20} "
                  f"{trade.exit_time.strftime('%Y-%m-%d %H:%M') if trade.exit_time else 'Open':<20} "
                  f"${trade.entry_price:<10.2f} ${trade.exit_price:<10.2f} "
                  f"${trade.pnl:<10.2f} {trade.pnl_percent:<10.2%}")
        
        if len(result.trades) > 10:
            print(f"... and {len(result.trades) - 10} more trades")
    
    # Plot equity curve
    if result.equity_curve and result.timestamps:
        try:
            plt.figure(figsize=(12, 6))
            plt.plot(result.timestamps, result.equity_curve)
            plt.title(f"{result.strategy_name} - Equity Curve")
            plt.xlabel("Date")
            plt.ylabel("Equity ($)")
            plt.grid(True)
            plt.tight_layout()
            
            # Save plot
            plt.savefig(f"backtest_equity_{result.strategy_name.replace(' ', '_')}.png")
            logger.info(f"Equity curve saved as backtest_equity_{result.strategy_name.replace(' ', '_')}.png")
            
            # Show plot
            plt.show()
        except Exception as e:
            logger.error(f"Error plotting equity curve: {e}")


def parse_args():
    """
    Parse command line arguments.
    
    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(description="Run a backtest for a trading strategy")
    parser.add_argument("-c", "--config", help="Path to configuration file (JSON)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(run_strategy_backtest(args.config)) 