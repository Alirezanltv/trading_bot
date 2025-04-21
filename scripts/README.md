# Trading System Setup Scripts

This directory contains setup scripts for configuring the trading system environment.

## InfluxDB Setup Scripts

### Native InfluxDB Setup

`setup_influxdb.py` - Sets up a native InfluxDB installation:

- Checks if InfluxDB is installed and running
- Creates the required database and retention policies
- Verifies connection and writes test data
- Saves configuration to the trading system

Usage:
```
py -3.10 trading_system/scripts/setup_influxdb.py
```

Or use the wrapper script from the root directory:
```
py -3.10 setup_db.py
```

### Docker-based InfluxDB Setup

`setup_influxdb_docker.py` - Sets up InfluxDB using Docker:

- Checks if Docker is installed and running
- Pulls and runs the InfluxDB Docker container
- Creates the database and retention policies
- Verifies connection

Usage:
```
py -3.10 trading_system/scripts/setup_influxdb_docker.py
```

Or use the wrapper script from the root directory:
```
py -3.10 setup_db.py --docker
```

## Testing Database Connection

After setting up InfluxDB, you can test the database connection:

```
py -3.10 test_db.py
```

This will run the TimeSeriesDB tests to verify that the trading system can connect to InfluxDB and perform basic operations. 