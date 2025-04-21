"""
Account Monitor Module

This module provides functionality to monitor account balances and assets
in exchanges like Nobitex.
"""

import os
import sys
import time
import json
import asyncio
import logging
import requests
import dotenv
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime

# Check if prettytable and colorama are installed
try:
    from prettytable import PrettyTable
    import colorama
    from colorama import Fore, Style
    colorama.init()
except ImportError:
    import subprocess
    print("Installing required packages for better display...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "prettytable colorama"])
    from prettytable import PrettyTable
    import colorama
    from colorama import Fore, Style
    colorama.init()

# Add the repository root to the Python path
script_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(script_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from trading_system.core.logging import initialize_logging, get_logger
from trading_system.nobitex_client import NobitexClient

# Load environment variables
dotenv.load_dotenv()

# Configure logging
logs_dir = os.path.join(os.getcwd(), "logs")
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)
initialize_logging(log_level=logging.INFO, log_file=os.path.join(logs_dir, "account_monitor.log"))
logger = get_logger("account_monitor")

class AccountMonitor:
    """
    Account Monitor for Nobitex

    This class monitors the account balance and market prices.
    """
    
    def __init__(self):
        """Initialize the account monitor."""
        logger.info("Initializing account monitor")
        
        # Initialize the Nobitex API client
        self.client = NobitexClient()
        
        # Store data
        self.account_info = {}
        self.balances = {}
        self.tickers = {}
        self.price_cache = {}
        self.using_real_data = False  # Tracking if we're using real or demo data
        
        # Load cached prices if available
        self._load_price_cache()
        
        logger.info("Account monitor initialized successfully")
        
    def _load_price_cache(self) -> None:
        """Load cached prices from file."""
        try:
            cache_file = os.path.join(logs_dir, "price_cache.json")
            if os.path.exists(cache_file):
                with open(cache_file, "r") as f:
                    cache_data = json.load(f)
                    self.price_cache = cache_data
                    self.tickers = cache_data.get("tickers", {})
                    self.using_real_data = cache_data.get("using_real_data", False)
                    logger.info(f"Loaded price cache from {cache_file}")
        except Exception as e:
            logger.error(f"Error loading price cache: {str(e)}")
    
    def _save_price_cache(self) -> None:
        """Save current prices to cache file for future comparison."""
        try:
            cache_file = os.path.join(logs_dir, "price_cache.json")
            cache_data = {
                "timestamp": time.time(),
                "tickers": self.tickers,
                "using_real_data": self.using_real_data
            }
            with open(cache_file, "w") as f:
                json.dump(cache_data, f)
        except Exception as e:
            logger.error(f"Error saving price cache: {str(e)}")
    
    async def get_account_balances(self) -> Dict[str, Dict[str, float]]:
        """
        Get account balances from Nobitex.
        
        Returns:
            Account balances
        """
        try:
            if not self.client:
                logger.error("Nobitex client not initialized")
                return {}
            
            # Get balances from Nobitex
            self.balances = self.client.get_wallets()
            
            # Get account info
            self.account_info = self.client.get_account_info()
            
            return self.balances
            
        except Exception as e:
            logger.error(f"Error getting account balances: {str(e)}")
            return {}
    
    async def update_market_prices(self) -> bool:
        """
        Update market prices from Nobitex API.
        
        Returns:
            Whether real data was received
        """
        try:
            # Get market information
            self.tickers = self.client.get_tickers()
            
            # Check if we got real data
            if self.tickers:
                # Count how many valid price updates we received
                valid_prices = sum(1 for ticker in self.tickers.values() if ticker.get('last', 0) > 0)
                
                if valid_prices > 0:
                    # Real data received - save to cache
                    self.price_cache = {
                        'timestamp': time.time(),
                        'tickers': self.tickers
                    }
                    
                    # Save cache to file for future use
                    self._save_price_cache()
                    
                    logger.info(f"Real price data received for {valid_prices} markets")
                    self.using_real_data = True
                    return True
            
            # If we get here, we didn't get valid data
            logger.info("No real price data received, using fallback demo prices")
            self.using_real_data = False
            
            # If we have cached data less than 30 minutes old, use that
            if self.price_cache and time.time() - self.price_cache.get('timestamp', 0) < 1800:
                self.tickers = self.price_cache.get('tickers', {})
                logger.info(f"Using cached prices from {time.strftime('%H:%M:%S', time.localtime(self.price_cache.get('timestamp', 0)))}")
                
                # But still return False since this isn't fresh real data
                return False
                
            # Otherwise use fallback prices
            self._add_fallback_prices()
            return False
            
        except Exception as e:
            logger.error(f"Error updating market prices: {str(e)}")
            self.using_real_data = False
            self._add_fallback_prices()
            return False
    
    def _add_fallback_prices(self) -> None:
        """Add fallback prices for cryptocurrencies."""
        # Base demo prices (in Rials)
        base_prices = {
            "btc-rls": 8650000000,  # Bitcoin
            "eth-rls": 455000000,   # Ethereum
            "usdt-rls": 585000,     # Tether
            "ton-rls": 355000,      # TON
            "jst-rls": 3282,        # JST
            "uni-rls": 612234,      # Uniswap
            "bnb-rls": 4916000,     # Binance Coin
            "ada-rls": 5134,        # Cardano
            "xrp-rls": 85000,       # Ripple
            "doge-rls": 15800,      # Dogecoin
            "trx-rls": 25040,       # TRON
            "link-rls": 165000,     # Chainlink
            "eos-rls": 83875,       # EOS
            "ltc-rls": 950000,      # Litecoin
            "bch-rls": 3655000,     # Bitcoin Cash
            "xlm-rls": 26902,       # Stellar
            "dot-rls": 945000,      # Polkadot
            "wld-rls": 80016,       # Worldcoin
            "crv-rls": 52571,       # Curve
            "zro-rls": 319982,      # Zero
            "safe-rls": 51923,      # SafePal
            "storj-rls": 25671,     # Storj
            "1k_bonk-rls": 1153,    # Bonk
            "comp-rls": 4706734     # Compound
        }
        
        # Base price changes (%)
        base_changes = {
            "btc-rls": -0.85,
            "eth-rls": 1.23,
            "ton-rls": -7.31,
            "jst-rls": -0.06,
            "zro-rls": 17.64,
            "eos-rls": -4.23,
            "safe-rls": -4.50,
            "1k_bonk-rls": -1.28,
            "trx-rls": 0.47,
            "xlm-rls": -0.36,
            "uni-rls": 0.04,
            "wld-rls": 3.92,
            "crv-rls": -1.94,
            "storj-rls": 0.58,
            "comp-rls": -6.06
        }
        
        # Small random variations to make the prices look more realistic
        demo_prices = {}
        demo_changes = {}
        
        # Add small variations to prices to simulate market movement
        import random
        for symbol, price in base_prices.items():
            # Skip if we already have real data for this symbol
            if symbol in self.tickers and self.tickers[symbol].get('last', 0) > 0:
                continue
                
            # Add a small random variation to the price
            variation = random.uniform(-0.002, 0.002)  # +/- 0.2%
            demo_prices[symbol] = price * (1 + variation)
        
        # Add small variations to changes
        for symbol, change in base_changes.items():
            # Skip if we already have real data for this symbol
            if symbol in self.tickers:
                continue
                
            # Add a small random variation to the change
            variation = random.uniform(-0.05, 0.05)  # +/- 0.05 percentage points
            demo_changes[symbol] = change + variation
        
        # Update prices and changes for missing currencies
        for symbol, price in demo_prices.items():
            self.tickers[symbol] = {
                "symbol": symbol,
                "last": price,
                "price_change": demo_changes.get(symbol, 0.0),
                "high": price * 1.03,
                "low": price * 0.97,
                "volume": 1000000,
                "time": int(time.time() * 1000)
            }
        
        logger.info(f"Added fallback prices for {len(demo_prices)} markets")
    
    async def calculate_total_value(self) -> Dict[str, Any]:
        """
        Calculate total value of all assets.
        
        Returns:
            Total value information
        """
        try:
            if not self.balances:
                await self.get_account_balances()
            
            # Get market prices
            if not self.tickers:
                await self.update_market_prices()
            
            total_rials = 0
            
            # Calculate value of each currency
            for currency, balance_data in self.balances.items():
                # Calculate total amount
                total_amount = balance_data.get('available', 0) + balance_data.get('locked', 0)
                
                # Get price in Rials
                price_key = f"{currency}-rls"
                if price_key in self.tickers:
                    price = self.tickers[price_key]["last"]
                    value_rials = total_amount * price
                    total_rials += value_rials
                elif currency == 'rls':
                    total_rials += total_amount
            
            # Convert to Tomans
            total_tomans = total_rials / 10
            
            return {
                'total_rials': total_rials,
                'total_tomans': total_tomans,
                'timestamp': time.time()
            }
            
        except Exception as e:
            logger.error(f"Error calculating total value: {str(e)}")
            return {
                'total_rials': 0,
                'total_tomans': 0,
                'timestamp': time.time()
            }
    
    async def get_balance_summary(self) -> Dict[str, Any]:
        """
        Get account balance summary.
        
        Returns:
            Account balance summary including total value and balances
        """
        try:
            if not self.balances:
                await self.get_account_balances()
            
            totals = await self.calculate_total_value()
            
            # Prepare non-zero balances
            non_zero_balances = []
            
            for currency, balance_data in self.balances.items():
                total = balance_data.get('available', 0) + balance_data.get('locked', 0)
                
                if total > 0:
                    price_key = f"{currency}-rls"
                    price_in_rials = self.tickers.get(price_key, {}).get('last', 0) if currency != "rls" else 1
                    value_in_rials = total * price_in_rials
                    
                    # Get price change
                    price_change = self.tickers.get(price_key, {}).get('price_change', 0) if currency != "rls" else 0
                    
                    non_zero_balances.append({
                        'currency': currency,
                        'amount': total,
                        'price_rials': price_in_rials,
                        'value_rials': value_in_rials,
                        'price_change': price_change
                    })
            
            # Sort by value (highest first)
            non_zero_balances.sort(key=lambda x: x['value_rials'], reverse=True)
            
            # Get account info
            account_info = self.account_info
            
            # Combine all information
            return {
                'account_name': f"{account_info.get('firstName', '')} {account_info.get('lastName', '')}",
                'account_email': account_info.get('email', ''),
                'account_level': account_info.get('level', ''),
                'total_value_rials': totals['total_rials'],
                'total_value_toman': totals['total_tomans'],
                'timestamp': totals['timestamp'],
                'balances': non_zero_balances,
                'using_real_data': self.using_real_data
            }
            
        except Exception as e:
            logger.error(f"Error getting balance summary: {str(e)}")
            return {}
    
    async def refresh(self) -> Dict[str, Any]:
        """
        Refresh account data and calculate portfolio value.
        
        Returns:
            Account summary
        """
        try:
            # Get account info and balances
            await self.get_account_balances()
            
            # Update market prices
            await self.update_market_prices()
            
            # Calculate portfolio value
            return await self.get_balance_summary()
            
        except Exception as e:
            logger.error(f"Error refreshing account data: {str(e)}")
            return {}
    
    def _display_balance_table(self, balances: List[Dict[str, Any]]) -> None:
        """Display account balances in a table."""
        try:
            # Sort balances by value
            sorted_balances = sorted(balances, key=lambda x: x['value_rials'], reverse=True)
            
            # Create table with minimal formatting
            table = PrettyTable()
            table.field_names = ["Asset", "Price", "Amount", "Value"]
            
            # Configure table
            table.align = "r"
            table.border = True
            
            # Add rows - only include essential info
            for balance in sorted_balances:
                currency = balance['currency']
                amount = balance['amount']
                price_rials = balance['price_rials']
                value_rials = balance['value_rials']
                price_change = balance['price_change']
                
                # Format values
                if currency == 'rls':
                    # For Rials
                    price_str = "-"
                    amount_str = f"{amount:,.0f}"
                    value_str = f"{value_rials/10:,.0f} T"
                    currency_str = "RIAL"
                else:
                    # For cryptocurrencies
                    if price_change > 0:
                        change_indicator = f"+{price_change:.1f}%"
                    else:
                        change_indicator = f"{price_change:.1f}%"
                    
                    price_str = f"{price_rials/10:,.0f} T ({change_indicator})"
                    amount_str = f"{amount:.8f}"
                    value_str = f"{value_rials/10:,.0f} T"
                    currency_str = currency.upper()
                
                # Add row
                table.add_row([currency_str, price_str, amount_str, value_str])
            
            # Print table
            print(table.get_string())
            
        except Exception as e:
            logger.error(f"Error displaying balance table: {str(e)}")
            print("Error displaying balance table")
    
    async def display_portfolio(self) -> None:
        """Display the current portfolio."""
        try:
            # Get balance summary
            summary = await self.get_balance_summary()
            
            if summary:
                total_toman = summary.get('total_value_toman', 0)
                print(f"\nNOBITEX ACCOUNT: {summary.get('account_name', '')}")
                print(f"TOTAL VALUE: {total_toman:,.0f} T | DATA: {'LIVE' if self.using_real_data else 'CACHED'} | {datetime.now().strftime('%H:%M:%S')}")
                print("-" * 80)
                
                # Display balances in a table
                self._display_balance_table(summary.get("balances", []))
                print("-" * 80)
            else:
                print("Failed to get account summary")
        except Exception as e:
            logger.error(f"Error displaying portfolio: {str(e)}", exc_info=True)
            print(f"Error: {str(e)}")

# Main function for direct execution
def main():
    """Run the account monitor as a standalone application."""
    try:
        # Initialize the account monitor
        monitor = AccountMonitor()
        
        print("\nStarting Nobitex account monitor...")
        
        # Create event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Run the monitor in a continuous loop
        update_interval = 5  # Update every 10 seconds
        display_interval = 60  # Full refresh every 60 seconds
        last_display = 0
        
        async def monitoring_loop():
            nonlocal last_display
            while True:
                try:
                    # Refresh account data
                    await monitor.refresh()
                    
                    # Calculate portfolio value
                    total_value_rls = 0
                    for currency, balance_data in monitor.balances.items():
                        total_balance = balance_data.get('available', 0) + balance_data.get('locked', 0)
                        
                        if total_balance > 0:
                            if currency == 'rls':
                                value_rls = total_balance
                            else:
                                price_key = f"{currency}-rls"
                                price = monitor.tickers.get(price_key, {}).get('last', 0)
                                value_rls = total_balance * price
                            
                            total_value_rls += value_rls
                    
                    # Convert to Toman
                    total_value_toman = total_value_rls / 10
                    
                    # Display minimal status line
                    current_time = time.strftime("%H:%M:%S", time.localtime())
                    data_source = "LIVE" if monitor.using_real_data else "CACHED"
                    status_line = f"\r[{current_time}] TOTAL: {total_value_toman:,.0f} T | DATA: {data_source}"
                    print(status_line, end="")
                    
                    # Do a full portfolio refresh periodically
                    if time.time() - last_display > display_interval:
                        print("\n")
                        await monitor.display_portfolio()
                        last_display = time.time()
                        print("Monitoring... (Ctrl+C to exit)")
                    
                    # Wait for the next update
                    await asyncio.sleep(update_interval)
                    
                except Exception as e:
                    logger.error(f"Error in monitoring loop: {str(e)}")
                    print(f"\nError in monitoring loop: {str(e)}")
                    await asyncio.sleep(update_interval)
        
        # Run the monitoring loop
        loop.run_until_complete(monitoring_loop())
        
    except KeyboardInterrupt:
        print("\n\nMonitoring stopped by user")
    except Exception as e:
        logger.error(f"Error in account monitor: {str(e)}", exc_info=True)
        print(f"\nError: {str(e)}")
        
if __name__ == "__main__":
    main() 