#!/usr/bin/env python3
"""
Integrated Trading Pipeline Runner

This script runs the integrated trading pipeline with the specified configuration.
"""

import os
import sys
import argparse
import logging
from pathlib import Path

# Add parent directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from trading_system.integrated_trading_pipeline import IntegratedTradingPipeline
from trading_system.core.logging import initialize_logging

def main():
    """Run the integrated trading pipeline."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run the integrated trading pipeline")
    parser.add_argument("--config", "-c", 
                        default="trading_system/config/pipeline_config.json",
                        help="Path to configuration file")
    parser.add_argument("--log-level", "-l", 
                        default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Logging level")
    args = parser.parse_args()
    
    # Initialize logging
    logs_dir = os.path.join(os.getcwd(), "logs")
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    
    log_level = getattr(logging, args.log_level)
    initialize_logging(log_level=log_level)
    
    # Create data directories
    Path("data/positions").mkdir(parents=True, exist_ok=True)
    Path("data/orders").mkdir(parents=True, exist_ok=True)
    
    # Check if config file exists
    if not os.path.exists(args.config):
        print(f"Config file not found: {args.config}")
        return 1
    
    # Create and start pipeline
    print(f"Starting integrated trading pipeline with config: {args.config}")
    pipeline = IntegratedTradingPipeline(args.config)
    
    try:
        pipeline.start()
        
        # Run until keyboard interrupt
        print("Pipeline running... Press Ctrl+C to stop")
        print("Monitoring the following symbols:", ", ".join(pipeline.symbols))
        while True:
            try:
                cmd = input("\nEnter 'status' for current state, 'positions' for open positions, or 'q' to quit: ")
                
                if cmd.lower() == 'q':
                    break
                elif cmd.lower() == 'status':
                    print(f"Pipeline status: {'Running' if pipeline.running else 'Stopped'}")
                    print(f"Components: {pipeline.component_statuses}")
                elif cmd.lower() == 'positions':
                    # Get all open positions
                    open_positions = pipeline.position_manager.get_open_positions()
                    
                    if not open_positions:
                        print("No open positions")
                    else:
                        print(f"Open positions ({len(open_positions)}):")
                        for pos_id, pos in open_positions.items():
                            print(f"  {pos.symbol} {pos.position_type.name}: {pos.quantity} @ {pos.entry_price}")
                else:
                    print("Unknown command")
                    
            except EOFError:
                break
            
    except KeyboardInterrupt:
        print("\nStopping pipeline...")
    finally:
        pipeline.stop()
        print("Pipeline stopped")
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 