"""
Nobitex API Client

This module provides a dedicated client for interacting with the Nobitex API.
"""

import os
import sys
import time
import json
import logging
import asyncio
import requests
import hmac
import hashlib
import dotenv
from typing import Dict, Any, List, Optional

# Add the repository root to the Python path
script_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(script_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from trading_system.core.logging import initialize_logging, get_logger

# Load environment variables
dotenv.load_dotenv()

# Configure logging
logs_dir = os.path.join(os.getcwd(), "logs")
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)
initialize_logging(log_level=logging.INFO, log_file=os.path.join(logs_dir, "nobitex_client.log"))
logger = get_logger("nobitex_client")

class NobitexClient:
    """
    Nobitex API Client
    
    This class provides methods to interact with the Nobitex API.
    """
    
    def __init__(self, api_key: str = None, api_secret: str = None):
        """
        Initialize the Nobitex API client.
        
        Args:
            api_key: Nobitex API key
            api_secret: Nobitex API secret
        """
        self.api_key = api_key or os.getenv("NOBITEX_API_KEY")
        self.api_secret = api_secret or os.getenv("NOBITEX_API_SECRET")
        
        if not self.api_key:
            raise ValueError("Nobitex API key is required")
            
        # API endpoints
        self.base_url = "https://api.nobitex.ir"
        
        # Request settings
        self.timeout = 30
        self.max_retries = 3
        
        # Cache
        self.markets_cache = {}
        self.ticker_cache = {}
        self.cache_expiry = 60  # 60 seconds
        self.last_cache_update = 0
        
        logger.info("Nobitex API client initialized")
    
    def ping(self) -> bool:
        """
        Test the API connection.
        
        Returns:
            Connection success
        """
        try:
            response = self._make_request("GET", "/market/stats", auth_required=False)
            return response is not None and response.get("status") == "ok"
        except Exception as e:
            logger.error(f"Error pinging Nobitex API: {str(e)}")
            return False
    
    def get_account_info(self) -> Dict[str, Any]:
        """
        Get account information.
        
        Returns:
            Account information
        """
        try:
            # According to the docs, this is the correct profile endpoint
            response = self._make_request("POST", "/users/profile", auth_required=True)
            
            if response and response.get("status") == "ok":
                return response.get("profile", {})
            else:
                logger.error(f"Error getting account info: {response}")
                return {}
                
        except Exception as e:
            logger.error(f"Error getting account info: {str(e)}")
            return {}
    
    def get_wallets(self) -> Dict[str, Dict[str, float]]:
        """
        Get wallet balances.
        
        Returns:
            Wallet balances
        """
        try:
            # Using the correct wallets endpoint from docs
            response = self._make_request("POST", "/users/wallets/list", auth_required=True)
            
            if response and response.get("status") == "ok":
                wallets = {}
                wallet_list = response.get("wallets", [])
                
                for wallet in wallet_list:
                    currency = wallet.get("currency", "").lower()
                    balance = {
                        "available": float(wallet.get("balance", 0)),
                        "locked": float(wallet.get("blocked", 0)),
                        "pending_deposit": float(wallet.get("pending", 0)),
                        "pending_withdraw": float(wallet.get("pending_withdraws", 0)),
                        "address": wallet.get("depositAddress", "")
                    }
                    wallets[currency] = balance
                
                return wallets
            else:
                logger.error(f"Error getting wallets: {response}")
                return {}
                
        except Exception as e:
            logger.error(f"Error getting wallets: {str(e)}")
            return {}
    
    def get_transactions(self, count: int = 100) -> List[Dict[str, Any]]:
        """
        Get recent transactions.
        
        Args:
            count: Number of transactions to get
            
        Returns:
            List of transactions
        """
        try:
            response = self._make_request(
                "POST", 
                "/users/wallets/transactions/list", 
                data={"size": count},
                auth_required=True
            )
            
            if response and response.get("status") == "ok":
                return response.get("transactions", [])
            else:
                logger.error(f"Error getting transactions: {response}")
                return []
                
        except Exception as e:
            logger.error(f"Error getting transactions: {str(e)}")
            return []
    
    def get_orders(self, status: str = "all", count: int = 100) -> List[Dict[str, Any]]:
        """
        Get orders.
        
        Args:
            status: Order status (all, open, done)
            count: Number of orders to get
            
        Returns:
            List of orders
        """
        try:
            params = {"size": count}
            
            if status in ["open", "done"]:
                params["status"] = status
            
            response = self._make_request(
                "POST", 
                "/market/orders/list", 
                data=params,
                auth_required=True
            )
            
            if response and response.get("status") == "ok":
                return response.get("orders", [])
            else:
                logger.error(f"Error getting orders: {response}")
                return []
                
        except Exception as e:
            logger.error(f"Error getting orders: {str(e)}")
            return []
    
    def get_markets(self) -> Dict[str, Dict[str, Any]]:
        """
        Get market information.
        
        Returns:
            Market information
        """
        try:
            # Check cache
            if self.markets_cache and time.time() - self.last_cache_update < self.cache_expiry:
                return self.markets_cache
            
            # Use the orderbook endpoint as documented in the API docs
            response = self._make_request("GET", "/market/orders/depth", auth_required=False)
            
            if response and response.get("status") == "ok":
                markets = {}
                
                # Format market data
                for market_data in response.get("orders", {}).values():
                    # Get the symbol
                    symbol = market_data.get("symbol", "")
                    
                    # Extract currency pair (e.g., BTCIRT -> btc-rls)
                    if len(symbol) >= 3:
                        # Most symbols follow the pattern BTCIRT
                        base = symbol[:-3].lower()
                        quote = symbol[-3:].lower()
                        # Convert IRT to rls
                        if quote == "irt":
                            quote = "rls"
                        market_id = f"{base}-{quote}"
                        
                        # Get market data
                        asks = market_data.get("asks", [])
                        bids = market_data.get("bids", [])
                        
                        markets[market_id] = {
                            "symbol": symbol,
                            "base_currency": base,
                            "quote_currency": quote,
                            "best_ask": float(asks[0][0]) if asks else 0,
                            "best_bid": float(bids[0][0]) if bids else 0,
                            "last_price": float(market_data.get("lastTradePrice", 0)),
                            "ask_volume": float(asks[0][1]) if asks else 0,
                            "bid_volume": float(bids[0][1]) if bids else 0
                        }
                
                # Update cache
                self.markets_cache = markets
                self.last_cache_update = time.time()
                
                return markets
            else:
                logger.error(f"Error getting markets: {response}")
                return {}
                
        except Exception as e:
            logger.error(f"Error getting markets: {str(e)}")
            return {}
    
    def get_tickers(self) -> Dict[str, Dict[str, Any]]:
        """
        Get ticker information for all markets.
        
        Returns:
            Ticker information
        """
        try:
            # Check cache
            if self.ticker_cache and time.time() - self.last_cache_update < self.cache_expiry:
                return self.ticker_cache
            
            # Get market stats for ticker information
            stats_response = self._make_request("GET", "/market/stats", auth_required=False)
            
            if stats_response and stats_response.get("status") == "ok":
                tickers = {}
                stats_data = stats_response.get("stats", {})
                
                # Process market data from stats
                for symbol, stats_item in stats_data.items():
                    # Convert to standardized format (e.g., BTCIRT -> btc-rls)
                    std_symbol = self._standardize_symbol(symbol)
                    
                    # Create ticker data with available information
                    tickers[std_symbol] = {
                        "symbol": std_symbol,
                        "last": float(stats_item.get("latest", 0)),
                        "high": float(stats_item.get("dayHigh", 0)),
                        "low": float(stats_item.get("dayLow", 0)),
                        "volume": float(stats_item.get("volumeSrc", 0)),
                        "quote_volume": float(stats_item.get("volumeDst", 0)),
                        "price_change": float(stats_item.get("dayChange", 0)),
                        "time": int(time.time() * 1000)  # Current time in milliseconds
                    }
                
                # Update cache
                self.ticker_cache = tickers
                self.last_cache_update = time.time()
                
                return tickers
            else:
                logger.error(f"Error getting ticker data: {stats_response}")
                return {}
                
        except Exception as e:
            logger.error(f"Error getting tickers: {str(e)}")
            return {}
    
    def get_ticker(self, symbol: str) -> Dict[str, Any]:
        """
        Get ticker information for a specific market.
        
        Args:
            symbol: Market symbol (e.g., btc-rls)
            
        Returns:
            Ticker information
        """
        try:
            # Get all tickers
            tickers = self.get_tickers()
            
            # Return specific ticker
            return tickers.get(symbol, {})
                
        except Exception as e:
            logger.error(f"Error getting ticker for {symbol}: {str(e)}")
            return {}
    
    def _standardize_symbol(self, nobitex_symbol: str) -> str:
        """
        Convert Nobitex symbol format to standard format.
        
        Args:
            nobitex_symbol: Symbol in Nobitex format (e.g., BTCIRT)
            
        Returns:
            Symbol in standard format (e.g., btc-rls)
        """
        # Common quote currencies
        quote_currencies = ["IRT", "USDT", "BTC", "ETH"]
        
        # Default result if we can't parse
        result = nobitex_symbol.lower()
        
        # Try to find a known quote currency
        for quote in quote_currencies:
            if nobitex_symbol.endswith(quote):
                base = nobitex_symbol[:-len(quote)]
                quote_mapped = "rls" if quote == "IRT" else quote.lower()
                return f"{base.lower()}-{quote_mapped}"
        
        # If no known quote found, return as is
        return result
    
    def _make_request(self, method: str, endpoint: str, params: Dict[str, Any] = None, 
                     data: Dict[str, Any] = None, headers: Dict[str, str] = None, 
                     auth_required: bool = True) -> Dict[str, Any]:
        """
        Make a request to the Nobitex API.
        
        Args:
            method: HTTP method (GET, POST, DELETE, etc.)
            endpoint: API endpoint
            params: Query parameters
            data: Request body data
            headers: Request headers
            auth_required: Whether authentication is required
            
        Returns:
            API response as dictionary
        """
        try:
            url = f"{self.base_url}{endpoint}"
            
            # Prepare headers
            if headers is None:
                headers = {}
            
            # Set content type correctly according to docs
            headers['Content-Type'] = 'application/json'
            
            # Add authentication token if required - per the docs, token goes in the header
            if auth_required and self.api_key:
                headers['Authorization'] = f"Token {self.api_key}"
            
            # Make the request with retries
            for retry in range(self.max_retries):
                try:
                    if method == 'GET':
                        response = requests.get(url, params=params, headers=headers, timeout=self.timeout)
                    elif method == 'POST':
                        response = requests.post(url, json=data, headers=headers, timeout=self.timeout)
                    elif method == 'DELETE':
                        response = requests.delete(url, json=data, headers=headers, timeout=self.timeout)
                    else:
                        logger.error(f"Unsupported HTTP method: {method}")
                        return {}
                    
                    # Check for HTTP errors
                    response.raise_for_status()
                    
                    # Parse JSON response
                    result = response.json()
                    
                    # Check for API errors
                    if isinstance(result, dict) and result.get('status') == 'failed':
                        error_msg = result.get('message', 'Unknown API error')
                        logger.error(f"Nobitex API error: {error_msg}")
                        
                        # Return the error response
                        return result
                    
                    return result
                    
                except (requests.exceptions.RequestException, requests.exceptions.HTTPError) as e:
                    if retry < self.max_retries - 1:
                        # Exponential backoff
                        wait_time = (2 ** retry) + (time.time() % 1)
                        logger.warning(f"Request failed, retrying in {wait_time:.2f} seconds: {str(e)}")
                        time.sleep(wait_time)
                    else:
                        # Last retry failed
                        logger.error(f"Request failed after {self.max_retries} retries: {str(e)}")
                        raise
            
            return {}
            
        except Exception as e:
            logger.error(f"Error making request: {str(e)}")
            return {}

# Main function for direct execution
async def main():
    """Main function to test the Nobitex API client."""
    try:
        client = NobitexClient()
        
        # Test connection
        if not client.ping():
            logger.error("Failed to connect to Nobitex API")
            return
        
        print("\n=== NOBITEX ACCOUNT SUMMARY ===")
        
        # Get account info
        account_info = client.get_account_info()
        if account_info:
            print(f"User: {account_info.get('firstName', '')} {account_info.get('lastName', '')}")
            print(f"Email: {account_info.get('email', '')}")
            print(f"Account Level: {account_info.get('level', '')}")
        
        # Get wallet balances
        wallets = client.get_wallets()
        
        # Get market information
        markets = client.get_markets()
        tickers = client.get_tickers()
        
        # Calculate total value in Rials
        total_value_rials = 0
        non_zero_wallets = {}
        
        for currency, wallet in wallets.items():
            total_balance = wallet.get('available', 0) + wallet.get('locked', 0)
            
            if total_balance > 0:
                # Calculate value in Rials
                value_rials = 0
                
                if currency == 'rls':
                    value_rials = total_balance
                else:
                    market_id = f"{currency}-rls"
                    ticker = tickers.get(market_id, {})
                    if ticker:
                        price = ticker.get('last', 0)
                        value_rials = total_balance * price
                
                # Add to total
                total_value_rials += value_rials
                
                # Store non-zero wallet
                non_zero_wallets[currency] = {
                    "balance": total_balance,
                    "value_rials": value_rials
                }
        
        # Display total value
        print(f"\nTotal Value: {total_value_rials:,.0f} Rials")
        
        # Display assets
        print("\nAssets:")
        
        # Sort by value
        sorted_wallets = sorted(
            non_zero_wallets.items(), 
            key=lambda x: x[1]['value_rials'], 
            reverse=True
        )
        
        for currency, data in sorted_wallets:
            balance = data['balance']
            value_rials = data['value_rials']
            percentage = (value_rials / total_value_rials * 100) if total_value_rials > 0 else 0
            
            print(f"{currency.upper()}: {balance:.8f} ({value_rials:,.0f} Rials, {percentage:.2f}%)")
        
        # Get recent orders
        orders = client.get_orders(status="open", count=5)
        if orders:
            print("\nRecent Open Orders:")
            for order in orders:
                print(f"  {order.get('type')} {order.get('srcCurrency')}/{order.get('dstCurrency')} @ {order.get('price')} ({order.get('status')})")
        
        # Get recent transactions
        transactions = client.get_transactions(count=5)
        if transactions:
            print("\nRecent Transactions:")
            for tx in transactions:
                print(f"  {tx.get('currency')} {tx.get('type')} of {tx.get('amount')} ({tx.get('status')})")
        
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main()) 