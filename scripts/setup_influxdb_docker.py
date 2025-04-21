"""
InfluxDB Docker Setup Script for Trading System

This script sets up InfluxDB using Docker:
1. Checks if Docker is installed
2. Pulls and runs the InfluxDB Docker container
3. Creates the required database
4. Verifies the connection

Usage:
    py -3.10 trading_system/scripts/setup_influxdb_docker.py
    
    Or use the wrapper script:
    py -3.10 setup_db.py --docker
"""

import sys
import time
import logging
import subprocess
import os
import platform

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("influxdb_docker_setup")

# InfluxDB Configuration
INFLUXDB_HOST = "localhost"
INFLUXDB_PORT = 8086
INFLUXDB_DATABASE = "trading_data"
INFLUXDB_USER = "admin"
INFLUXDB_PASSWORD = "admin123"
INFLUXDB_CONTAINER_NAME = "trading_influxdb"

def check_docker_installed() -> bool:
    """Check if Docker is installed on the system."""
    try:
        result = subprocess.run(
            ["docker", "--version"], 
            capture_output=True, 
            text=True,
            check=False
        )
        logger.info(f"Docker version: {result.stdout.strip()}")
        return result.returncode == 0
    except FileNotFoundError:
        logger.error("Docker is not installed or not in the PATH")
        return False

def check_container_running(container_name: str) -> bool:
    """Check if a container with the given name is already running."""
    try:
        result = subprocess.run(
            ["docker", "ps", "-q", "-f", f"name={container_name}"],
            capture_output=True,
            text=True,
            check=True
        )
        return bool(result.stdout.strip())
    except subprocess.CalledProcessError:
        logger.error("Failed to check if InfluxDB container is running")
        return False

def start_influxdb_container():
    """Start InfluxDB Docker container."""
    if check_container_running(INFLUXDB_CONTAINER_NAME):
        logger.info(f"InfluxDB container '{INFLUXDB_CONTAINER_NAME}' is already running")
        return True
    
    # Check if the container exists but is not running
    try:
        result = subprocess.run(
            ["docker", "ps", "-a", "-q", "-f", f"name={INFLUXDB_CONTAINER_NAME}"],
            capture_output=True,
            text=True,
            check=True
        )
        if result.stdout.strip():
            logger.info(f"Starting existing InfluxDB container: {INFLUXDB_CONTAINER_NAME}")
            subprocess.run(
                ["docker", "start", INFLUXDB_CONTAINER_NAME],
                check=True
            )
            return True
    except subprocess.CalledProcessError:
        logger.error("Failed to check if InfluxDB container exists")
    
    # Create a new container
    logger.info("Creating and starting new InfluxDB container")
    try:
        subprocess.run([
            "docker", "run", "-d",
            "--name", INFLUXDB_CONTAINER_NAME,
            "-p", f"{INFLUXDB_PORT}:{INFLUXDB_PORT}",
            "-e", f"INFLUXDB_DB={INFLUXDB_DATABASE}",
            "-e", f"INFLUXDB_ADMIN_USER={INFLUXDB_USER}",
            "-e", f"INFLUXDB_ADMIN_PASSWORD={INFLUXDB_PASSWORD}",
            "influxdb:1.8"
        ], check=True)
        logger.info("InfluxDB container created successfully")
        
        # Wait for the container to fully start
        time.sleep(5)  
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to create InfluxDB container: {e}")
        return False

def verify_influxdb_connection():
    """Verify connection to InfluxDB."""
    try:
        # Import here to avoid issues if the package is not installed
        from influxdb import InfluxDBClient
        from influxdb.exceptions import InfluxDBClientError

        # Create a client
        client = InfluxDBClient(
            host=INFLUXDB_HOST,
            port=INFLUXDB_PORT,
            username=INFLUXDB_USER,
            password=INFLUXDB_PASSWORD,
            database=INFLUXDB_DATABASE
        )
        
        # Check connection and create database if needed
        databases = client.get_list_database()
        db_exists = any(db['name'] == INFLUXDB_DATABASE for db in databases)
        
        if not db_exists:
            logger.info(f"Creating database: {INFLUXDB_DATABASE}")
            client.create_database(INFLUXDB_DATABASE)
        
        # Create retention policies
        logger.info("Setting up retention policies")
        client.create_retention_policy(
            name='trading_data_30d',
            duration='30d',
            replication='1',
            database=INFLUXDB_DATABASE,
            default=True
        )
        
        # Test write and read
        logger.info("Testing write and read operations")
        test_point = [{
            "measurement": "test_measurement",
            "tags": {
                "test_tag": "test_value"
            },
            "fields": {
                "test_field": 1.0
            },
            "time": int(time.time() * 1000000000)  # nanoseconds
        }]
        
        client.write_points(test_point)
        result = client.query('SELECT * FROM test_measurement')
        
        if result:
            logger.info("Successfully wrote and read test data")
            return True
        else:
            logger.error("Failed to read test data")
            return False
            
    except ImportError:
        logger.error("influxdb package is not installed. Installing...")
        try:
            subprocess.run([sys.executable, "-m", "pip", "install", "influxdb"], check=True)
            logger.info("influxdb package installed successfully")
            return verify_influxdb_connection()  # Retry after installation
        except subprocess.CalledProcessError:
            logger.error("Failed to install influxdb package")
            return False
    except InfluxDBClientError as e:
        logger.error(f"InfluxDB client error: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False

def update_config_file():
    """Update the configuration file with InfluxDB settings."""
    try:
        import yaml
        
        config_path = "config/config.yaml"
        
        # Create directories if they don't exist
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        
        # Load existing config if it exists
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f) or {}
        else:
            config = {}
        
        # Update with InfluxDB settings
        if 'influxdb' not in config:
            config['influxdb'] = {}
            
        config['influxdb'].update({
            'host': INFLUXDB_HOST,
            'port': INFLUXDB_PORT,
            'database': INFLUXDB_DATABASE,
            'username': INFLUXDB_USER,
            'password': INFLUXDB_PASSWORD,
            'timeout': 10,
            'retries': 3,
            'use_ssl': False,
            'verify_ssl': False,
            'retention_policy': 'trading_data_30d'
        })
        
        # Save the updated config
        with open(config_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False)
            
        logger.info(f"Updated config file: {config_path}")
        return True
    except ImportError:
        logger.error("PyYAML package is not installed. Installing...")
        try:
            subprocess.run([sys.executable, "-m", "pip", "install", "PyYAML"], check=True)
            logger.info("PyYAML package installed successfully")
            return update_config_file()  # Retry after installation
        except subprocess.CalledProcessError:
            logger.error("Failed to install PyYAML package")
            return False
    except Exception as e:
        logger.error(f"Failed to update config file: {e}")
        return False

def main():
    """Main function to setup InfluxDB via Docker."""
    logger.info("Starting InfluxDB Docker setup...")
    
    # Check if Docker is installed
    if not check_docker_installed():
        logger.error("Docker is required but not installed. Please install Docker and try again.")
        return False
    
    # Start InfluxDB container
    if not start_influxdb_container():
        logger.error("Failed to start InfluxDB container")
        return False
    
    # Verify connection and setup
    if not verify_influxdb_connection():
        logger.error("Failed to verify InfluxDB connection")
        return False
    
    # Update config file
    if not update_config_file():
        logger.error("Failed to update configuration file")
        return False
    
    logger.info("InfluxDB setup completed successfully!")
    logger.info(f"InfluxDB is running at {INFLUXDB_HOST}:{INFLUXDB_PORT}")
    logger.info(f"Database: {INFLUXDB_DATABASE}")
    logger.info(f"Username: {INFLUXDB_USER}")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 