"""
TradingView Market Monitor

This script provides real-time monitoring of crypto assets using the TradingViewFetcher.
It displays current prices, technical indicators, and potential trading signals.
"""

import os
import sys
import time
import json
import asyncio
import signal
import pandas as pd
import dotenv
from datetime import datetime
from typing import Dict, List, Any, Optional
from tabulate import tabulate
from colorama import init, Fore, Back, Style

# Add the repository root to the Python path
script_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(os.path.dirname(script_dir))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# Import the TradingViewFetcher
from trading_system.monitoring.tradingview_fetcher import TradingViewFetcher

# Load environment variables
dotenv.load_dotenv()

# Initialize colorama
init(autoreset=True)

# Import logging
from trading_system.core.logging import initialize_logging, get_logger
initialize_logging(log_level="INFO", log_file=os.path.join(os.getcwd(), "logs", "tradingview_monitor.log"))
logger = get_logger("tradingview_monitor")

class TradingViewMonitor:
    """
    TradingView Market Monitor
    
    This class provides real-time monitoring of crypto assets
    using the TradingViewFetcher.
    """
    
    def __init__(self, symbols: List[str] = None, update_interval: int = 60):
        """
        Initialize the TradingView market monitor.
        
        Args:
            symbols: List of symbols to monitor (e.g., ["BTC/USDT", "ETH/USDT"])
            update_interval: Update interval in seconds
        """
        # Default symbols if none provided
        if symbols is None:
            symbols = [
                "BTC/USDT",  # Bitcoin
                "ETH/USDT",  # Ethereum
                "SOL/USDT",  # Solana
                "BNB/USDT",  # Binance Coin
                "XRP/USDT",  # XRP
                "ADA/USDT",  # Cardano
                "DOGE/USDT", # Dogecoin
                "LINK/USDT", # Chainlink
                "AVAX/USDT", # Avalanche
                "DOT/USDT"   # Polkadot
            ]
        
        self.symbols = symbols
        self.update_interval = update_interval
        self.fetcher = None
        self.market_data = {}
        self.indicators_data = {}
        self.signals = {}
        self.last_prices = {}
        self.price_changes = {}
        self.running = False
        self.update_task = None
        self.timeframes = ["1D", "4h", "1h", "15m"]
        self.main_timeframe = "1D"  # Default timeframe for indicators
        
        logger.info(f"TradingView monitor initialized with {len(symbols)} symbols")
    
    async def initialize(self):
        """Initialize the TradingView fetcher and connect to TradingView."""
        try:
            logger.info("Initializing TradingView fetcher")
            self.fetcher = TradingViewFetcher(use_fallback=True)
            
            # Connect to TradingView
            if not await self.fetcher.connect():
                logger.error("Failed to connect to TradingView")
                return False
            
            logger.info("TradingView fetcher initialized and connected")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing TradingView monitor: {str(e)}", exc_info=True)
            return False
    
    async def start(self):
        """Start the monitor."""
        if not self.fetcher:
            success = await self.initialize()
            if not success:
                logger.error("Failed to initialize TradingView fetcher")
                return False
        
        self.running = True
        
        # Register signal handlers for graceful shutdown
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self._signal_handler)
        
        # Start update task
        self.update_task = asyncio.create_task(self._update_loop())
        
        logger.info("TradingView monitor started")
        return True
    
    def _signal_handler(self, sig, frame):
        """Handle signals for graceful shutdown."""
        logger.info(f"Received signal {sig}, shutting down...")
        self.running = False
        
        # Wait for tasks to complete
        if self.update_task and not self.update_task.done():
            self.update_task.cancel()
        
        print("\nShutting down TradingView monitor...")
    
    async def stop(self):
        """Stop the monitor."""
        self.running = False
        
        # Cancel update task
        if self.update_task and not self.update_task.done():
            self.update_task.cancel()
            try:
                await self.update_task
            except asyncio.CancelledError:
                pass
        
        # Close fetcher connection
        if self.fetcher:
            await self.fetcher.close()
        
        logger.info("TradingView monitor stopped")
    
    async def _update_loop(self):
        """Update loop for fetching market data."""
        try:
            first_run = True
            
            while self.running:
                try:
                    logger.info("Updating market data")
                    
                    # Show updating message on console (except first run)
                    if not first_run:
                        print("\nUpdating market data...", end="", flush=True)
                    
                    # First, get current prices for all symbols
                    await self._update_prices()
                    
                    # Then, get indicators and chart data for analysis
                    await self._update_indicators()
                    
                    # Generate trading signals
                    self._generate_signals()
                    
                    # Clear screen and display data
                    if not first_run:
                        # Clear the "Updating..." message
                        print("\r" + " " * 25 + "\r", end="", flush=True)
                    else:
                        first_run = False
                    
                    # Display market data
                    self._display_market_data()
                    
                    # Log update completion
                    logger.info("Market data update completed")
                    
                    # Wait for next update
                    for _ in range(self.update_interval):
                        if not self.running:
                            break
                        await asyncio.sleep(1)
                        
                except Exception as e:
                    logger.error(f"Error in update loop: {str(e)}", exc_info=True)
                    await asyncio.sleep(5)  # Short retry delay on error
                    
        except asyncio.CancelledError:
            logger.info("Update loop cancelled")
        except Exception as e:
            logger.error(f"Unexpected error in update loop: {str(e)}", exc_info=True)
    
    async def _update_prices(self):
        """Update current prices for all symbols."""
        for symbol in self.symbols:
            try:
                # Get current price
                price = await self.fetcher.get_price(symbol)
                
                if price is not None:
                    # Calculate price change if we have a previous price
                    if symbol in self.market_data:
                        prev_price = self.market_data[symbol].get("price", 0)
                        if prev_price > 0:
                            price_change = (price / prev_price - 1) * 100
                        else:
                            price_change = 0
                    else:
                        price_change = 0
                    
                    # Update market data
                    self.market_data[symbol] = {
                        "price": price,
                        "price_change": price_change,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    
                    logger.debug(f"Updated price for {symbol}: ${price:,.2f}")
            except Exception as e:
                logger.error(f"Error updating price for {symbol}: {str(e)}")
    
    async def _update_indicators(self):
        """Update technical indicators for all symbols."""
        for symbol in self.symbols:
            try:
                # Define indicators to fetch
                indicators = [
                    {"name": "RSI", "params": [14]},
                    {"name": "MACD", "params": [12, 26, 9]},
                    {"name": "BB", "params": [20, 2]},  # Bollinger Bands
                    {"name": "EMA", "params": [50]},
                    {"name": "EMA", "params": [200]}
                ]
                
                # Get indicators for main timeframe
                ind_data = await self.fetcher.get_indicators(symbol, timeframe=self.main_timeframe, indicators=indicators)
                
                if ind_data:
                    # Extract latest indicator values
                    indicator_values = {}
                    
                    # Process RSI
                    if "RSI@tv-basicstudies" in ind_data:
                        rsi_data = ind_data["RSI@tv-basicstudies"]
                        if "RSI" in rsi_data and rsi_data["RSI"]:
                            indicator_values["RSI"] = rsi_data["RSI"][-1]
                    
                    # Process MACD
                    if "MACD@tv-basicstudies" in ind_data:
                        macd_data = ind_data["MACD@tv-basicstudies"]
                        if "MACD" in macd_data and "Signal" in macd_data and macd_data["MACD"] and macd_data["Signal"]:
                            indicator_values["MACD"] = macd_data["MACD"][-1]
                            indicator_values["MACD_Signal"] = macd_data["Signal"][-1]
                            indicator_values["MACD_Histogram"] = macd_data["Histogram"][-1] if "Histogram" in macd_data else 0
                    
                    # Process EMAs
                    if "EMA@tv-basicstudies" in ind_data:
                        ema_data = ind_data["EMA@tv-basicstudies"]
                        if "EMA" in ema_data and ema_data["EMA"]:
                            indicator_values["EMA50"] = ema_data["EMA"][-1]
                    
                    # Store indicators
                    self.indicators_data[symbol] = indicator_values
                    
                    # Get daily chart data for additional analysis
                    candles = await self.fetcher.get_chart_data(symbol, timeframe=self.main_timeframe, count=30)
                    if candles:
                        # Store last 5 candles
                        self.market_data[symbol]["candles"] = candles[-5:]
                    
                    logger.debug(f"Updated indicators for {symbol}")
                
            except Exception as e:
                logger.error(f"Error updating indicators for {symbol}: {str(e)}")
    
    def _generate_signals(self):
        """Generate trading signals based on indicators."""
        for symbol in self.symbols:
            try:
                if symbol not in self.indicators_data or symbol not in self.market_data:
                    continue
                
                indicators = self.indicators_data[symbol]
                current_price = self.market_data[symbol].get("price", 0)
                
                signals = []
                
                # RSI signals
                if "RSI" in indicators:
                    rsi = indicators["RSI"]
                    if rsi < 30:
                        signals.append({"type": "BUY", "strength": 2, "reason": f"RSI oversold ({rsi:.1f})"})
                    elif rsi < 40:
                        signals.append({"type": "BUY", "strength": 1, "reason": f"RSI bullish ({rsi:.1f})"})
                    elif rsi > 70:
                        signals.append({"type": "SELL", "strength": 2, "reason": f"RSI overbought ({rsi:.1f})"})
                    elif rsi > 60:
                        signals.append({"type": "SELL", "strength": 1, "reason": f"RSI bearish ({rsi:.1f})"})
                
                # MACD signals
                if all(k in indicators for k in ["MACD", "MACD_Signal", "MACD_Histogram"]):
                    macd = indicators["MACD"]
                    signal = indicators["MACD_Signal"]
                    hist = indicators["MACD_Histogram"]
                    
                    # Crossover detection - current vs previous (approximation since we only have latest values)
                    if 0 < (macd - signal) < 0.02 * abs(macd):
                        signals.append({"type": "BUY", "strength": 2, "reason": "MACD bullish crossover"})
                    elif 0 > (macd - signal) > -0.02 * abs(macd):
                        signals.append({"type": "SELL", "strength": 2, "reason": "MACD bearish crossover"})
                    
                    # MACD above/below zero
                    if macd > 0 and signal > 0:
                        signals.append({"type": "BUY", "strength": 1, "reason": "MACD bullish momentum"})
                    elif macd < 0 and signal < 0:
                        signals.append({"type": "SELL", "strength": 1, "reason": "MACD bearish momentum"})
                
                # EMA crossovers and price relative to EMAs
                if "EMA50" in indicators and current_price > 0:
                    ema50 = indicators["EMA50"]
                    
                    # Price vs EMA50
                    if 0 < (current_price / ema50 - 1) < 0.02:
                        signals.append({"type": "BUY", "strength": 1, "reason": "Price crossed above EMA50"})
                    elif 0 > (current_price / ema50 - 1) > -0.02:
                        signals.append({"type": "SELL", "strength": 1, "reason": "Price crossed below EMA50"})
                
                # Analyze recent candles for patterns
                if "candles" in self.market_data[symbol]:
                    candles = self.market_data[symbol]["candles"]
                    if len(candles) >= 3:
                        # Check for strong trend patterns
                        consecutive_up = 0
                        consecutive_down = 0
                        
                        for i in range(1, len(candles)):
                            if candles[i]["close"] > candles[i-1]["close"]:
                                consecutive_up += 1
                                consecutive_down = 0
                            elif candles[i]["close"] < candles[i-1]["close"]:
                                consecutive_down += 1
                                consecutive_up = 0
                        
                        if consecutive_up >= 3:
                            signals.append({"type": "BUY", "strength": 1 + consecutive_up // 2, 
                                            "reason": f"{consecutive_up} consecutive bullish candles"})
                        elif consecutive_down >= 3:
                            signals.append({"type": "SELL", "strength": 1 + consecutive_down // 2, 
                                            "reason": f"{consecutive_down} consecutive bearish candles"})
                
                # Aggregate signals
                buy_strength = sum(s["strength"] for s in signals if s["type"] == "BUY")
                sell_strength = sum(s["strength"] for s in signals if s["type"] == "SELL")
                
                # Final signal determination
                if buy_strength > sell_strength:
                    signal_type = "BUY"
                    signal_strength = min(buy_strength - sell_strength, 5)  # Cap at 5
                elif sell_strength > buy_strength:
                    signal_type = "SELL"
                    signal_strength = min(sell_strength - buy_strength, 5)  # Cap at 5
                else:
                    signal_type = "NEUTRAL"
                    signal_strength = 0
                
                # Store signal
                self.signals[symbol] = {
                    "type": signal_type,
                    "strength": signal_strength,
                    "details": signals
                }
                
            except Exception as e:
                logger.error(f"Error generating signals for {symbol}: {str(e)}")
                self.signals[symbol] = {"type": "ERROR", "strength": 0, "details": []}
    
    def _display_market_data(self):
        """Display market data in a formatted table."""
        try:
            # Clear screen (platform independent)
            os.system('cls' if os.name == 'nt' else 'clear')
            
            # Print header
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n{Style.BRIGHT}{Fore.CYAN}=== TRADINGVIEW MARKET MONITOR ==={Style.RESET_ALL}")
            print(f"Time: {current_time}  |  Update Interval: {self.update_interval}s  |  Timeframe: {self.main_timeframe}")
            print(f"Monitoring {len(self.symbols)} assets\n")
            
            # Prepare table data
            table_data = []
            headers = ["Symbol", "Price", "24h Change", "RSI", "MACD", "Signal", "Strength"]
            
            for symbol in sorted(self.symbols, key=lambda s: (
                    # Sort order: Signal strength (DESC), Symbol name (ASC)
                    -1 * self.signals.get(symbol, {}).get("strength", 0),
                    symbol
            )):
                if symbol not in self.market_data:
                    continue
                
                # Get data
                price = self.market_data[symbol].get("price", 0)
                price_change = self.market_data[symbol].get("price_change", 0)
                
                # Format price with color based on change
                if price_change > 0:
                    price_str = f"{Fore.GREEN}${price:,.2f} (+{price_change:.2f}%){Style.RESET_ALL}"
                elif price_change < 0:
                    price_str = f"{Fore.RED}${price:,.2f} ({price_change:.2f}%){Style.RESET_ALL}"
                else:
                    price_str = f"${price:,.2f} (0.00%)"
                
                # Get indicator values
                indicators = self.indicators_data.get(symbol, {})
                rsi = indicators.get("RSI", None)
                macd = indicators.get("MACD", None)
                
                # Format RSI with color
                if rsi is not None:
                    if rsi < 30:
                        rsi_str = f"{Fore.GREEN}{rsi:.1f}{Style.RESET_ALL}"
                    elif rsi > 70:
                        rsi_str = f"{Fore.RED}{rsi:.1f}{Style.RESET_ALL}"
                    else:
                        rsi_str = f"{rsi:.1f}"
                else:
                    rsi_str = "N/A"
                
                # Format MACD
                macd_str = f"{macd:.4f}" if macd is not None else "N/A"
                
                # Get signal
                signal = self.signals.get(symbol, {"type": "NEUTRAL", "strength": 0})
                signal_type = signal["type"]
                signal_strength = signal["strength"]
                
                # Format signal with color and strength indicators
                if signal_type == "BUY":
                    signal_str = f"{Fore.GREEN}{signal_type}{Style.RESET_ALL}"
                    strength_str = Fore.GREEN + "●" * signal_strength + "○" * (5 - signal_strength) + Style.RESET_ALL
                elif signal_type == "SELL":
                    signal_str = f"{Fore.RED}{signal_type}{Style.RESET_ALL}"
                    strength_str = Fore.RED + "●" * signal_strength + "○" * (5 - signal_strength) + Style.RESET_ALL
                else:
                    signal_str = signal_type
                    strength_str = "○○○○○"
                
                # Add row to table
                table_data.append([
                    symbol,
                    price_str,
                    f"{price_change:.2f}%",
                    rsi_str,
                    macd_str,
                    signal_str,
                    strength_str
                ])
            
            # Print table
            if table_data:
                print(tabulate(table_data, headers=headers, tablefmt="pretty"))
            else:
                print("No data available yet. Please wait for data to be fetched.")
            
            # Print signal details for top asset
            if table_data:
                top_symbol = table_data[0][0]  # First column of first row
                print(f"\n{Style.BRIGHT}Signal details for {top_symbol}:{Style.RESET_ALL}")
                
                signal_details = self.signals.get(top_symbol, {}).get("details", [])
                if signal_details:
                    for signal in signal_details:
                        signal_type = signal["type"]
                        strength = signal["strength"]
                        reason = signal["reason"]
                        
                        if signal_type == "BUY":
                            print(f"  {Fore.GREEN}[BUY {strength}]{Style.RESET_ALL} {reason}")
                        elif signal_type == "SELL":
                            print(f"  {Fore.RED}[SELL {strength}]{Style.RESET_ALL} {reason}")
                else:
                    print("  No detailed signals available")
            
            # Print footer with instructions
            print(f"\n{Style.BRIGHT}Press Ctrl+C to exit{Style.RESET_ALL} | Next update in {self.update_interval} seconds")
            
        except Exception as e:
            logger.error(f"Error displaying market data: {str(e)}", exc_info=True)
            print(f"Error displaying market data: {str(e)}")

# Main function for direct execution
async def main():
    """Main function to run the TradingView monitor."""
    try:
        # Parse command line arguments
        import argparse
        parser = argparse.ArgumentParser(description="TradingView Market Monitor")
        parser.add_argument("--symbols", type=str, help="Comma-separated list of symbols to monitor")
        parser.add_argument("--interval", type=int, default=60, help="Update interval in seconds")
        parser.add_argument("--timeframe", type=str, default="1D", help="Main timeframe for analysis")
        args = parser.parse_args()
        
        # Get symbols from arguments or use defaults
        symbols = args.symbols.split(",") if args.symbols else None
        
        # Initialize monitor
        monitor = TradingViewMonitor(symbols=symbols, update_interval=args.interval)
        monitor.main_timeframe = args.timeframe
        
        # Start monitor
        if await monitor.start():
            # Keep running until Ctrl+C
            while monitor.running:
                await asyncio.sleep(1)
        
    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        logger.error(f"Error in main: {str(e)}", exc_info=True)
        print(f"Error: {str(e)}")
    finally:
        # Ensure monitor is stopped
        if 'monitor' in locals():
            await monitor.stop()

if __name__ == "__main__":
    # Import required modules for standalone execution
    try:
        import argparse
    except ImportError:
        print("Missing required packages. Please install with:")
        print("pip install tabulate colorama pandas")
        sys.exit(1)
    
    # Run the main function
    asyncio.run(main()) 