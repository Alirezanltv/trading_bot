#!/usr/bin/env python
"""
Normal Trading Test

This scenario simulates normal trading conditions to verify basic system functionality.
"""

import asyncio
import logging
import os
import sys

# Add the project root directory to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../.."))
sys.path.append(project_root)

from trading_system.tests.integration.simulation_framework import SimulationFramework, ScenarioType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("normal_trading_test")

async def run_normal_trading_test():
    """
    Run a normal trading test scenario.
    
    This test verifies basic functionality under normal market conditions:
    - Components initialize and start properly
    - Market data flows through the system
    - Strategy generates signals
    - Orders are created and executed
    - Positions are tracked
    - Risk limits are enforced
    """
    logger.info("Starting normal trading test")
    
    # Create and configure simulation
    sim = SimulationFramework(ScenarioType.NORMAL_TRADING)
    
    # Add normal trading events
    await sim.setup()
    
    # Set some scheduled events for testing
    await sim.schedule_event("MARKET_DATA_UPDATE_QUALITY", 10)  # Data quality change at 10s
    await sim.schedule_event("MARKET_PRICE_SHOCK", 30)         # Price shock at 30s
    await sim.schedule_event("API_TIMEOUT", 45)                # API timeout at 45s
    await sim.schedule_event("API_RECOVERY", 50)               # API recovery at 50s
    
    # Run the simulation for 60 seconds
    success, details = await sim.run(duration=60)
    
    # Print results
    if success:
        logger.info("PASSED: Normal trading test completed successfully")
    else:
        logger.error("FAILED: Normal trading test did not complete successfully")
    
    logger.info(f"Details: {details}")
    return success

if __name__ == "__main__":
    asyncio.run(run_normal_trading_test()) 