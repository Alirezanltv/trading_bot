"""
InfluxDB Setup Script for Trading System

This script helps set up InfluxDB for the trading system by:
1. Checking if InfluxDB is installed and running
2. Creating the required database
3. Setting up retention policies
4. Verifying the connection

Usage:
    py -3.10 trading_system/scripts/setup_influxdb.py
"""

import sys
import time
import logging
import subprocess
import platform
import json
from typing import Dict, Any, Optional, List, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("influxdb_setup")

# Try to import influxdb
try:
    from influxdb import InfluxDBClient
    from influxdb.exceptions import InfluxDBClientError, InfluxDBServerError
except ImportError:
    logger.error("InfluxDB client not installed. Installing required packages...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "influxdb"])
    
    # Retry import
    from influxdb import InfluxDBClient
    from influxdb.exceptions import InfluxDBClientError, InfluxDBServerError

# Default InfluxDB configuration
DEFAULT_CONFIG = {
    "host": "localhost",
    "port": 8086,
    "username": "admin",
    "password": "admin",
    "database": "trading_data",
    "retention_policies": [
        {
            "name": "default_policy",
            "duration": "INF",
            "replication": 1,
            "default": True
        },
        {
            "name": "short_term",
            "duration": "7d",
            "replication": 1,
            "default": False
        }
    ]
}

def check_influxdb_installed() -> bool:
    """Check if InfluxDB is installed on the system."""
    logger.info("Checking if InfluxDB is installed...")
    
    system = platform.system().lower()
    try:
        if system == "windows":
            # Try to find InfluxDB in Program Files
            import os
            influx_paths = [
                os.path.join(os.environ.get("ProgramFiles", "C:\\Program Files"), "InfluxDB"),
                os.path.join(os.environ.get("ProgramFiles(x86)", "C:\\Program Files (x86)"), "InfluxDB")
            ]
            
            for path in influx_paths:
                if os.path.exists(path):
                    logger.info(f"InfluxDB found at: {path}")
                    return True
                    
            # Try to run influxd version
            result = subprocess.run(["where", "influxd"], capture_output=True, text=True)
            if result.returncode == 0:
                logger.info("InfluxDB is installed and in PATH")
                return True
        else:
            # For Linux/Mac, check if influxd is in PATH
            result = subprocess.run(["which", "influxd"], capture_output=True, text=True)
            if result.returncode == 0:
                logger.info("InfluxDB is installed and in PATH")
                return True
    
    except Exception as e:
        logger.error(f"Error checking if InfluxDB is installed: {str(e)}")
    
    logger.warning("InfluxDB does not appear to be installed")
    return False

def check_influxdb_running(host: str = "localhost", port: int = 8086) -> bool:
    """Check if InfluxDB is running."""
    logger.info(f"Checking if InfluxDB is running on {host}:{port}...")
    
    try:
        client = InfluxDBClient(host=host, port=port)
        client.ping()
        
        # Get version info
        version = client.request('ping', expected_response_code=204).headers.get('X-Influxdb-Version', 'unknown')
        logger.info(f"InfluxDB is running (version: {version})")
        return True
        
    except Exception as e:
        logger.error(f"InfluxDB is not running: {str(e)}")
        return False

def start_influxdb() -> bool:
    """Attempt to start InfluxDB if it's not running."""
    logger.info("Attempting to start InfluxDB...")
    
    system = platform.system().lower()
    try:
        if system == "windows":
            # Try to start InfluxDB as a service
            subprocess.run(["net", "start", "influxdb"], capture_output=True)
        elif system == "linux":
            # Try systemctl first
            try:
                subprocess.run(["systemctl", "start", "influxdb"], capture_output=True)
            except FileNotFoundError:
                # Try service command as fallback
                subprocess.run(["service", "influxdb", "start"], capture_output=True)
        elif system == "darwin":  # macOS
            subprocess.run(["brew", "services", "start", "influxdb"], capture_output=True)
        
        # Wait a moment for the service to start
        time.sleep(5)
        
        # Check if it's now running
        return check_influxdb_running()
        
    except Exception as e:
        logger.error(f"Error starting InfluxDB: {str(e)}")
        return False

def create_influxdb_database(config: Dict[str, Any]) -> bool:
    """Create InfluxDB database and retention policies."""
    logger.info(f"Creating database '{config['database']}'...")
    
    try:
        # Connect to InfluxDB
        client = InfluxDBClient(
            host=config["host"],
            port=config["port"],
            username=config["username"],
            password=config["password"]
        )
        
        # Get existing databases
        databases = [db["name"] for db in client.get_list_database()]
        
        if config["database"] in databases:
            logger.info(f"Database '{config['database']}' already exists")
        else:
            client.create_database(config["database"])
            logger.info(f"Created database '{config['database']}'")
        
        # Set current database
        client.switch_database(config["database"])
        
        # Set up retention policies
        if "retention_policies" in config:
            for policy in config["retention_policies"]:
                logger.info(f"Creating retention policy '{policy['name']}'...")
                
                # Check if policy already exists
                existing_policies = client.get_list_retention_policies()
                policy_exists = any(p["name"] == policy["name"] for p in existing_policies)
                
                if policy_exists:
                    logger.info(f"Retention policy '{policy['name']}' already exists")
                    
                    # Update existing policy
                    client.alter_retention_policy(
                        name=policy["name"],
                        database=config["database"],
                        duration=policy["duration"],
                        replication=policy.get("replication", 1),
                        default=policy.get("default", False)
                    )
                    logger.info(f"Updated retention policy '{policy['name']}'")
                else:
                    # Create new policy
                    client.create_retention_policy(
                        name=policy["name"],
                        duration=policy["duration"],
                        replication=policy.get("replication", 1),
                        database=config["database"],
                        default=policy.get("default", False)
                    )
                    logger.info(f"Created retention policy '{policy['name']}'")
        
        # Test write
        logger.info("Testing database write...")
        current_time = int(time.time() * 1000)
        
        points = [
            {
                "measurement": "test_measurement",
                "tags": {
                    "setup": "initial"
                },
                "time": current_time,
                "fields": {
                    "value": 1.0
                }
            }
        ]
        
        client.write_points(points)
        logger.info("Test write successful")
        
        # Test read
        logger.info("Testing database read...")
        results = client.query('SELECT * FROM "test_measurement" LIMIT 1')
        points = list(results.get_points())
        
        if points:
            logger.info("Test read successful")
            return True
        else:
            logger.warning("Test read returned no results")
            return False
        
    except Exception as e:
        logger.error(f"Error creating database: {str(e)}")
        return False

def read_config() -> Dict[str, Any]:
    """Read configuration from config file or use defaults."""
    config_file = "trading_system/config/timeseries_db.json"
    
    try:
        with open(config_file, "r") as f:
            config = json.load(f)
            logger.info(f"Loaded configuration from {config_file}")
            return config
    except FileNotFoundError:
        logger.info(f"Configuration file {config_file} not found, using defaults")
        return DEFAULT_CONFIG
    except json.JSONDecodeError:
        logger.warning(f"Error parsing {config_file}, using defaults")
        return DEFAULT_CONFIG

def save_config(config: Dict[str, Any]) -> None:
    """Save configuration to config file."""
    config_file = "trading_system/config/timeseries_db.json"
    
    try:
        # Ensure directory exists
        import os
        os.makedirs(os.path.dirname(config_file), exist_ok=True)
        
        with open(config_file, "w") as f:
            json.dump(config, f, indent=4)
            logger.info(f"Saved configuration to {config_file}")
    except Exception as e:
        logger.error(f"Error saving configuration: {str(e)}")

def get_user_config() -> Dict[str, Any]:
    """Get configuration from user input or use defaults."""
    logger.info("Setting up InfluxDB configuration...")
    
    # Start with defaults
    config = DEFAULT_CONFIG.copy()
    
    try:
        # Ask if user wants to use defaults
        use_defaults = input("Use default InfluxDB configuration? (Y/n): ").strip().lower() != "n"
        
        if not use_defaults:
            # Get custom configuration
            config["host"] = input(f"InfluxDB host [{config['host']}]: ").strip() or config["host"]
            
            port_input = input(f"InfluxDB port [{config['port']}]: ").strip()
            if port_input:
                config["port"] = int(port_input)
            
            config["username"] = input(f"InfluxDB username [{config['username']}]: ").strip() or config["username"]
            config["password"] = input(f"InfluxDB password [{config['password']}]: ").strip() or config["password"]
            config["database"] = input(f"Database name [{config['database']}]: ").strip() or config["database"]
    
    except KeyboardInterrupt:
        logger.info("\nUser interrupted, using default configuration")
    
    return config

def main() -> None:
    """Main entry point."""
    logger.info("Starting InfluxDB setup for trading system...")
    
    # Check if InfluxDB is installed
    if not check_influxdb_installed():
        logger.error("""
InfluxDB is not installed. Please install InfluxDB first:
- Windows: Download from https://portal.influxdata.com/downloads/
- Linux: sudo apt-get install influxdb (or equivalent for your distro)
- macOS: brew install influxdb
""")
        sys.exit(1)
    
    # Check if InfluxDB is running
    if not check_influxdb_running():
        logger.warning("InfluxDB is not running")
        
        # Try to start InfluxDB
        if not start_influxdb():
            logger.error("""
InfluxDB is installed but not running. Please start InfluxDB:
- Windows: Start the InfluxDB service from Services or run 'net start influxdb'
- Linux: sudo systemctl start influxdb
- macOS: brew services start influxdb
""")
            sys.exit(1)
    
    # Get configuration
    config = get_user_config()
    
    # Create database and retention policies
    if create_influxdb_database(config):
        logger.info("Successfully set up InfluxDB for trading system")
        
        # Save configuration
        save_config(config)
        
        logger.info("""
InfluxDB setup complete. You can now run the trading system.

To test the database connection:
py -3.10 test_db.py
""")
    else:
        logger.error("Failed to set up InfluxDB for trading system")
        sys.exit(1)

if __name__ == "__main__":
    main() 