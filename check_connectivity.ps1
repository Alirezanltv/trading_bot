Write-Host "Market Data Connectivity Check" -ForegroundColor Cyan
Write-Host "=============================" -ForegroundColor Cyan

# Default parameters
$Symbol = "BTC/USDT"
$Timeframe = "1m"
$Duration = 60

# Display configuration
Write-Host "Configuration:" -ForegroundColor Yellow
Write-Host "Symbol:    $Symbol"
Write-Host "Timeframe: $Timeframe"
Write-Host "Duration:  $Duration seconds"
Write-Host ""

# Debug info
Write-Host "Debug Information:" -ForegroundColor Magenta
Write-Host "Current Directory: $(Get-Location)"

# Use Python 3.10 specifically
$pythonCmd = "py -3.10"
Write-Host "Python Command: $pythonCmd"
$pythonVersion = & py -3.10 --version
Write-Host "Python Version: $pythonVersion"

# Set PYTHONPATH to include the project root
$env:PYTHONPATH = "$((Get-Location).Path)"
Write-Host "PYTHONPATH set to: $env:PYTHONPATH"
Write-Host ""

# Navigate to the trading system directory
$tradingSystemPath = "trading_system"
if (Test-Path -Path $tradingSystemPath) {
    Set-Location -Path $tradingSystemPath
    Write-Host "Changed directory to: $(Get-Location)" -ForegroundColor Gray
} else {
    Write-Host "ERROR: Trading system directory not found: $tradingSystemPath" -ForegroundColor Red
    Write-Host "Please run this script from the root of the project." -ForegroundColor Red
    Write-Host "Press any key to exit..." -ForegroundColor Yellow
    $null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
    exit 1
}

# Run the connectivity check script
Write-Host "Starting connectivity check..." -ForegroundColor Green
try {
    & py -3.10 strategy/examples/check_market_connectivity.py --symbol $Symbol --timeframe $Timeframe --duration $Duration
    $scriptSuccess = $?
} catch {
    $scriptSuccess = $false
    Write-Host "ERROR: An exception occurred:" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
}

# Return to original directory
Set-Location -Path ".."

# Result
if ($scriptSuccess) {
    Write-Host "Connectivity check completed successfully." -ForegroundColor Green
} else {
    Write-Host "Connectivity check failed." -ForegroundColor Red
    Write-Host "Please check that the module structure and imports are correct." -ForegroundColor Red
}

Write-Host ""
Write-Host "Press any key to exit..." -ForegroundColor Yellow
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
