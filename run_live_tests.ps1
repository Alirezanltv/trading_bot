#!/usr/bin/env pwsh
# Run Live Signal Tests with Market Data

$ErrorActionPreference = "Stop"

# Set up directory paths
$scriptDir = $PSScriptRoot
$pythonScript = Join-Path $scriptDir "trading_system\strategy\examples\test_live_signals.py"

# Configuration
$symbols = "BTC/USDT,ETH/USDT"
$timeframes = "1m,5m,15m,1h"
$duration = 300  # seconds
$source = "tradingview"  # Changed from binance to tradingview

# Print configuration
Write-Host "Running Live Signal Tests"
Write-Host "======================="
Write-Host "Configuration:"
Write-Host "Symbols:    $symbols"
Write-Host "Timeframes: $timeframes"
Write-Host "Duration:   $duration seconds"
Write-Host "Source:     $source"
Write-Host ""

# Create necessary directories
if (-not (Test-Path "./data/reports")) {
    New-Item -ItemType Directory -Force -Path "./data/reports" | Out-Null
    Write-Host "Created reports directory"
}

if (-not (Test-Path "./data/strategy_state")) {
    New-Item -ItemType Directory -Force -Path "./data/strategy_state" | Out-Null
    Write-Host "Created strategy state directory"
}

if (-not (Test-Path "./data/strategy_monitors")) {
    New-Item -ItemType Directory -Force -Path "./data/strategy_monitors" | Out-Null
    Write-Host "Created strategy monitors directory"
}

# Set PYTHONPATH to include current directory
$env:PYTHONPATH = $scriptDir

# Convert to proper path format for Python
$normalizedPythonScript = $pythonScript -replace '\\', '/'

# Import the required modules directly
Write-Host "Starting live signal tests..."
try {
    # Run the Python command directly with all the patches
    py -3.10 -c "
import sys, os
sys.path.insert(0, os.path.abspath('$scriptDir'))
from trading_system.strategy.examples.db_fix import *
from trading_system.strategy.examples.enum_fix import *
from trading_system.strategy.examples.message_bus_patch import *
from trading_system.strategy.examples.provider_fix import *
from trading_system.strategy.examples.engine_fix import *
import argparse
import asyncio
import sys
sys.argv = ['$normalizedPythonScript', '--symbols', '$symbols', '--timeframes', '$timeframes', '--duration', '$duration', '--source', '$source']
with open('$normalizedPythonScript', 'r') as f:
    code = compile(f.read(), '$normalizedPythonScript', 'exec')
    exec(code, {})
"
}
catch {
    Write-Host "Error running live signal tests: $_" -ForegroundColor Red
}
