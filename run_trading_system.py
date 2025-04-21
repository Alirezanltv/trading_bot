#!/usr/bin/env python
"""
High-Reliability Trading System Runner

This script runs the trading system with Nobitex integration for live trading.
It can be configured to run in dry-run mode initially for testing.
"""

import os
import sys
import time
import json
import logging
import argparse
from dotenv import load_dotenv
from trading_system.simple_pipeline import SimpleTradingPipeline

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("trading_system.log"),
    ]
)

logger = logging.getLogger("trading_system")

def parse_arguments():
    parser = argparse.ArgumentParser(description="High-Reliability Trading System Runner")
    
    parser.add_argument("--config", type=str, default=None, 
                        help="Path to configuration file (default: use built-in configuration)")
    
    parser.add_argument("--dry-run", action="store_true", default=False,
                        help="Run in dry-run mode (no real trades)")
    
    parser.add_argument("--exchange", type=str, choices=["mock", "nobitex"], default="nobitex",
                        help="Exchange to use (default: nobitex)")
    
    parser.add_argument("--symbols", type=str, nargs="+", default=["btc-usdt", "eth-usdt"],
                        help="Symbols to trade (default: btc-usdt eth-usdt)")
    
    parser.add_argument("--interval", type=int, default=5,
                        help="Update interval in seconds (default: 5)")
    
    return parser.parse_args()

def load_or_create_config(args):
    """Load existing config or create a new one from arguments."""
    if args.config and os.path.exists(args.config):
        try:
            with open(args.config, 'r') as f:
                config = json.load(f)
                logger.info(f"Loaded configuration from {args.config}")
                return config
        except Exception as e:
            logger.error(f"Error loading configuration from {args.config}: {str(e)}")
    
    # Create new configuration from arguments
    config = {
        "symbols": args.symbols,
        "update_interval": args.interval,
        "position_size": {
            "btc-usdt": 0.001,  # Small size for safety
            "eth-usdt": 0.01,   # Small size for safety
            "btc-irt": 0.001,   # Small size for safety
        },
        "risk_per_trade": 0.01,  # 1% of balance
        "dry_run": args.dry_run,
        "exchange": {
            "type": args.exchange
        }
    }
    
    # Save configuration for future use
    config_path = "trading_config.json"
    try:
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
            logger.info(f"Saved configuration to {config_path}")
    except Exception as e:
        logger.warning(f"Could not save configuration: {str(e)}")
    
    return config

def main():
    """Main entry point for the trading system."""
    # Load environment variables
    load_dotenv()
    
    # Parse command line arguments
    args = parse_arguments()
    
    # Load or create configuration
    config = load_or_create_config(args)
    
    # Override dry run setting from command line if specified
    if args.dry_run:
        config["dry_run"] = True
    
    # Display settings
    logger.info("Starting trading system with the following settings:")
    logger.info(f"Exchange: {config['exchange']['type']}")
    logger.info(f"Symbols: {config['symbols']}")
    logger.info(f"Update interval: {config['update_interval']} seconds")
    logger.info(f"Dry run: {config['dry_run']}")
    
    # Check API keys
    if config['exchange']['type'] == 'nobitex' and not os.getenv('NOBITEX_API_KEY'):
        logger.warning("No Nobitex API key found in environment variables!")
        if not config['dry_run']:
            logger.error("Cannot run in live mode without API keys. Exiting.")
            return 1
        logger.warning("Continuing in dry-run mode only.")
        config['dry_run'] = True
    
    # Create the trading pipeline
    pipeline = SimpleTradingPipeline(config_path=args.config if args.config else None)
    
    # Override config with command line args if config file wasn't provided
    if not args.config:
        pipeline.config = config
        pipeline.symbols = config['symbols']
        pipeline.dry_run = config['dry_run']
    
    try:
        # Start the pipeline
        pipeline.start()
        
        # Interactive command loop
        print("\nTrading system running. Available commands:")
        print("  stats    - Show current statistics")
        print("  positions - Show open positions")
        print("  balance  - Show account balances")
        print("  exit     - Exit the program")
        
        while True:
            try:
                command = input("\nEnter command: ").strip().lower()
                
                if command == "exit" or command == "quit" or command == "q":
                    break
                elif command == "stats":
                    stats = pipeline.get_stats()
                    print("\nCurrent Statistics:")
                    print(f"Monitored symbols: {pipeline.symbols}")
                    print(f"Prices: {stats['prices']}")
                    print(f"Open positions: {len(stats['open_positions'])}")
                    print(f"Closed positions: {len(stats['closed_positions'])}")
                    print(f"Running in {'dry run' if pipeline.dry_run else 'live'} mode")
                elif command == "positions" or command == "pos":
                    stats = pipeline.get_stats()
                    
                    if not stats['open_positions']:
                        print("No open positions")
                    else:
                        print(f"\nOpen positions ({len(stats['open_positions'])}):")
                        for pos_id, pos in stats['open_positions'].items():
                            print(f"  {pos['symbol']} {pos['type']}: {pos['quantity']} @ {pos['entry_price']} | PnL: {pos['unrealized_pnl']:.2f}")
                    
                    if stats['closed_positions']:
                        print(f"\nClosed positions ({len(stats['closed_positions'])}):")
                        total_pnl = 0
                        for pos_id, pos in stats['closed_positions'].items():
                            print(f"  {pos['symbol']} {pos['type']}: {pos['quantity']} @ {pos['entry_price']} -> {pos['exit_price']} | PnL: {pos['realized_pnl']:.2f}")
                            total_pnl += pos['realized_pnl']
                        print(f"\nTotal realized PnL: {total_pnl:.2f}")
                elif command == "balance":
                    stats = pipeline.get_stats()
                    print("\nAccount Balances:")
                    for asset, amount in stats['balances'].items():
                        if amount > 0:
                            print(f"  {asset.upper()}: {amount}")
                else:
                    print("Unknown command")
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Error processing command: {str(e)}")
        
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logger.exception(f"Error running trading system: {str(e)}")
        return 1
    finally:
        # Stop the pipeline
        pipeline.stop()
        logger.info("Trading system stopped")
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 