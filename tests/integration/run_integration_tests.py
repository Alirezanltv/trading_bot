#!/usr/bin/env python
"""
Integration Test Runner

This script executes system-wide integration tests to validate
how all trading system components interact under various scenarios.
"""

import asyncio
import argparse
import datetime
import logging
import os
import sys
import json
from typing import Dict, List, Any

# Add trading system to path if needed
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))

from trading_system.tests.integration.simulation_framework import (
    SimulationFramework, ScenarioType, SystemEvent
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("integration_test_results.log")
    ]
)

logger = logging.getLogger("integration.runner")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Run integration tests for the trading system")
    
    # Test selection
    parser.add_argument(
        "--scenarios", 
        type=str, 
        nargs="+", 
        choices=[s.value for s in ScenarioType],
        default=["normal_trading"],
        help="Scenarios to run (default: normal_trading)"
    )
    
    # Test configuration
    parser.add_argument(
        "--duration", 
        type=int, 
        default=120,
        help="Test duration in seconds (default: 120)"
    )
    
    parser.add_argument(
        "--config", 
        type=str, 
        default=None,
        help="Path to test configuration JSON file"
    )
    
    # Output options
    parser.add_argument(
        "--report", 
        type=str, 
        default="test_report.json",
        help="Output report file path (default: test_report.json)"
    )
    
    parser.add_argument(
        "--verbose", 
        action="store_true",
        help="Enable verbose output"
    )
    
    return parser.parse_args()


async def run_scenario(scenario_name: str, config: Dict[str, Any], duration: int) -> Dict[str, Any]:
    """
    Run a single test scenario.
    
    Args:
        scenario_name: Name of the scenario to run
        config: Test configuration
        duration: Test duration in seconds
        
    Returns:
        Test results
    """
    logger.info(f"=== Running scenario: {scenario_name} ===")
    
    # Convert string to enum
    scenario_type = ScenarioType(scenario_name)
    
    # Create simulation framework
    sim = SimulationFramework(scenario_type, config.get(scenario_name, {}))
    
    # Setup simulation
    setup_success = await sim.setup()
    if not setup_success:
        logger.error(f"Failed to set up simulation for {scenario_name}")
        return {
            "scenario": scenario_name,
            "success": False,
            "error": "Failed to set up simulation"
        }
    
    # Run simulation
    try:
        results = await sim.run(duration_seconds=duration)
        logger.info(f"Scenario {scenario_name} complete: {'PASSED' if results['passed'] else 'FAILED'}")
        
        # Add scenario information to results
        results["scenario"] = scenario_name
        results["timestamp"] = datetime.datetime.now().isoformat()
        
        return results
        
    except Exception as e:
        logger.error(f"Error running scenario {scenario_name}: {str(e)}", exc_info=True)
        return {
            "scenario": scenario_name,
            "success": False,
            "error": str(e),
            "timestamp": datetime.datetime.now().isoformat()
        }


async def run_all_scenarios(args):
    """
    Run all selected test scenarios.
    
    Args:
        args: Command line arguments
    """
    logger.info(f"Starting integration tests at {datetime.datetime.now().isoformat()}")
    logger.info(f"Selected scenarios: {', '.join(args.scenarios)}")
    
    # Load configuration
    config = {}
    if args.config:
        try:
            with open(args.config, 'r') as f:
                config = json.load(f)
                logger.info(f"Loaded configuration from {args.config}")
        except Exception as e:
            logger.warning(f"Error loading configuration: {str(e)}")
    
    # Run each scenario
    results = []
    for scenario in args.scenarios:
        scenario_results = await run_scenario(scenario, config, args.duration)
        results.append(scenario_results)
    
    # Generate summary report
    summary = {
        "timestamp": datetime.datetime.now().isoformat(),
        "scenarios_run": len(results),
        "scenarios_passed": sum(1 for r in results if r.get("passed", False)),
        "scenarios_failed": sum(1 for r in results if not r.get("passed", False)),
        "total_duration": sum(r.get("time_elapsed", 0) for r in results),
        "detailed_results": results
    }
    
    # Save report
    with open(args.report, 'w') as f:
        json.dump(summary, f, indent=2)
    
    logger.info(f"Test report saved to {args.report}")
    
    # Print summary
    logger.info(f"=== Integration Test Summary ===")
    logger.info(f"Scenarios run: {summary['scenarios_run']}")
    logger.info(f"Scenarios passed: {summary['scenarios_passed']}")
    logger.info(f"Scenarios failed: {summary['scenarios_failed']}")
    logger.info(f"Total duration: {summary['total_duration']:.2f} seconds")
    
    # Return overall success
    return summary['scenarios_failed'] == 0


def main():
    """
    Main entry point.
    """
    # Parse arguments
    args = parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Run tests
    success = asyncio.run(run_all_scenarios(args))
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main() 