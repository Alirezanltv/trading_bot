#!/usr/bin/env pwsh
# Simplified Live Signal Tests Runner

$ErrorActionPreference = "Stop"

# Set up directory paths
$rootDir = $PSScriptRoot
$fixDir = Join-Path $rootDir "trading_system\strategy\examples"

# Configuration
$symbols = "BTC/USDT,ETH/USDT"
$timeframes = "1m,5m,15m,1h"
$duration = 60  # shorter duration for testing
$source = "tradingview"

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

# Change to the directory where the test script is located
Set-Location -Path "$fixDir"

# Set PYTHONPATH to include project root
$env:PYTHONPATH = $rootDir

Write-Host "Starting live signal tests..."
try {
    # Run the test with our fixes applied directly
    py -3.10 -c "
import sys, os
sys.path.insert(0, os.path.abspath('$rootDir'))

# Import our mocks and fixes
from db_fix import *
from enum_fix import *
from message_bus_patch import *
from provider_fix import *
from engine_fix import *

# Now import and run the test
import test_live_signals
from test_live_signals import main

# Set up arguments
sys.argv = ['test_live_signals.py', '--symbols', '$symbols', '--timeframes', '$timeframes', '--duration', '$duration', '--source', '$source']
import asyncio
asyncio.run(main())
"
    Write-Host "Test completed successfully!" -ForegroundColor Green
}
catch {
    Write-Host "Error running live signal tests: $_" -ForegroundColor Red
}
finally {
    # Return to original directory
    Set-Location -Path $rootDir
} 