#!/usr/bin/env python
"""
TradingView Monitor Runner

This script provides a simple interface to run the TradingView monitor.
"""

import sys
import os
import asyncio
import argparse

# Ensure the repository root is in the Python path
script_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(script_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from trading_system.monitoring.tradingview_monitor import TradingViewMonitor

def print_banner():
    """Print a banner for the TradingView monitor."""
    print("=" * 60)
    print(" " * 15 + "TRADINGVIEW MARKET MONITOR")
    print("=" * 60)
    print("Real-time cryptocurrency market data and trading signals")
    print("=" * 60)
    print()

async def main():
    """Run the TradingView monitor."""
    print_banner()
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run the TradingView market monitor")
    parser.add_argument("--symbols", type=str, 
                      help="Comma-separated list of symbols to monitor (e.g., 'BTC/USDT,ETH/USDT,SOL/USDT')")
    parser.add_argument("--interval", type=int, default=60,
                      help="Update interval in seconds (default: 60)")
    parser.add_argument("--timeframe", type=str, default="1D", choices=["1m", "5m", "15m", "1h", "4h", "1D", "1W"],
                      help="Timeframe for analysis (default: 1D)")
    args = parser.parse_args()
    
    # Get symbols from arguments or use defaults
    symbols = args.symbols.split(",") if args.symbols else None
    
    # Display configuration
    print(f"Update interval: {args.interval} seconds")
    print(f"Analysis timeframe: {args.timeframe}")
    print(f"Symbols: {'Default selection' if symbols is None else ', '.join(symbols)}")
    print("\nStarting TradingView monitor...")
    print("Press Ctrl+C to exit\n")
    
    # Initialize monitor
    monitor = TradingViewMonitor(symbols=symbols, update_interval=args.interval)
    monitor.main_timeframe = args.timeframe
    
    try:
        # Start monitor
        if await monitor.start():
            # Keep running until Ctrl+C
            while monitor.running:
                await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        # Ensure monitor is stopped
        await monitor.stop()
        print("\nTradingView monitor shut down.")

if __name__ == "__main__":
    try:
        # Check required packages
        import pandas
        import tabulate
        import colorama
        import websockets
        import aiohttp
        
        # Run the main function
        asyncio.run(main())
    except ImportError as e:
        print(f"Error: {e}")
        print("\nPlease install required packages with:")
        print("pip install pandas tabulate colorama websockets aiohttp")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1) 