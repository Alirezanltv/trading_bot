"""
Reliability Example

This script demonstrates the reliability features of the trading system,
including high availability, fault tolerance, and resilience mechanisms.

Features demonstrated:
- Connection pooling and automatic reconnection
- Circuit breaker pattern for fault tolerance
- Transaction verification with three-phase commit
- Shadow accounting and position reconciliation
- Automatic failover between market data providers
- Health monitoring and status dashboard
"""

import asyncio
import logging
import random
import time
from datetime import datetime
from typing import Dict, Any, List, Optional

from trading_system.core.health_monitor import HealthMonitor, ComponentHealth, HealthStatus
from trading_system.exchange.connection_pool import ConnectionPool
from trading_system.exchange.enhanced_nobitex_adapter import EnhancedNobitexAdapter
from trading_system.exchange.transaction_verification import TransactionVerificationPipeline
from trading_system.market_data.tradingview_webhook_handler import TradingViewWebhookHandler
from trading_system.position.shadow_accounting import ShadowPositionManager
from trading_system.strategy.strategy_engine import StrategyEngine
from trading_system.strategy.moving_average_strategy import MovingAverageStrategy
from trading_system.dashboard.health_dashboard import HealthDashboard
from trading_system.bus.message_bus import MessageBus

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("reliability_example")

class ReliabilityExample:
    """Example demonstrating reliability features of the trading system."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the reliability example.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.running = False
        self.message_bus = None
        self.health_monitor = None
        self.health_dashboard = None
        self.exchange_adapter = None
        self.transaction_verifier = None
        self.shadow_accounting = None
        self.strategy_engine = None
        self.market_data_handler = None
        
    async def initialize(self) -> bool:
        """Initialize all components of the reliability example.
        
        Returns:
            Initialization success status
        """
        try:
            logger.info("Initializing reliability example components...")
            
            # Initialize message bus for component communication
            self.message_bus = MessageBus(self.config.get("message_bus", {}))
            await self.message_bus.initialize()
            
            # Initialize health monitoring
            self.health_monitor = HealthMonitor(
                update_interval=self.config.get("health_monitor", {}).get("update_interval", 5)
            )
            
            # Initialize health dashboard
            dashboard_config = self.config.get("health_dashboard", {})
            self.health_dashboard = HealthDashboard(
                host=dashboard_config.get("host", "0.0.0.0"),
                port=dashboard_config.get("port", 8080),
                health_monitor=self.health_monitor,
                message_bus=self.message_bus
            )
            
            # Initialize exchange connection pool
            connection_pool = ConnectionPool(
                base_url=self.config.get("exchange", {}).get("base_url", "https://api.nobitex.ir"),
                max_connections=self.config.get("exchange", {}).get("max_connections", 10),
                connection_timeout=self.config.get("exchange", {}).get("connection_timeout", 10),
                idle_timeout=self.config.get("exchange", {}).get("idle_timeout", 60),
                max_retries=self.config.get("exchange", {}).get("max_retries", 3)
            )
            
            # Initialize enhanced exchange adapter
            exchange_config = self.config.get("exchange", {})
            self.exchange_adapter = EnhancedNobitexAdapter(
                config={
                    "api_key": exchange_config.get("api_key", ""),
                    "api_secret": exchange_config.get("api_secret", ""),
                    "connection_pool": connection_pool,
                    "max_retries": exchange_config.get("max_retries", 3),
                    "retry_delay": exchange_config.get("retry_delay", 1.0),
                    "circuit_breaker_threshold": exchange_config.get("circuit_breaker_threshold", 5),
                    "circuit_breaker_timeout": exchange_config.get("circuit_breaker_timeout", 30),
                    "cache_dir": exchange_config.get("cache_dir", "./cache")
                }
            )
            await self.exchange_adapter.initialize()
            
            # Register exchange adapter with health monitor
            self.health_monitor.register_component(
                component_id="exchange_adapter",
                name="Nobitex Exchange Adapter",
                health_check=self.exchange_adapter.health_check
            )
            
            # Initialize transaction verification
            self.transaction_verifier = TransactionVerificationPipeline(
                exchange_adapter=self.exchange_adapter,
                message_bus=self.message_bus,
                verification_timeout=exchange_config.get("verification_timeout", 30),
                max_retries=exchange_config.get("verification_retries", 3)
            )
            
            # Initialize shadow accounting
            accounting_config = self.config.get("position", {})
            self.shadow_accounting = ShadowPositionManager(
                db_path=accounting_config.get("db_path", "./positions.db"),
                exchange_adapter=self.exchange_adapter,
                reconciliation_interval=accounting_config.get("reconciliation_interval", 300),
                tolerance=accounting_config.get("tolerance", 0.01),
                max_history=accounting_config.get("max_history", 100),
                auto_correct=accounting_config.get("auto_correct", True)
            )
            await self.shadow_accounting.initialize()
            
            # Register shadow accounting with health monitor
            self.health_monitor.register_component(
                component_id="shadow_accounting",
                name="Shadow Position Accounting",
                health_check=self.shadow_accounting.health_check
            )
            
            # Initialize strategy engine
            self.strategy_engine = StrategyEngine(
                message_bus=self.message_bus,
                exchange_adapter=self.exchange_adapter,
                transaction_verifier=self.transaction_verifier,
                position_manager=self.shadow_accounting
            )
            await self.strategy_engine.initialize()
            
            # Register strategy engine with health monitor
            self.health_monitor.register_component(
                component_id="strategy_engine",
                name="Strategy Engine",
                health_check=self.strategy_engine.health_check
            )
            
            # Initialize market data webhook handler
            market_data_config = self.config.get("market_data", {})
            self.market_data_handler = TradingViewWebhookHandler(
                port=market_data_config.get("webhook_port", 8081),
                host=market_data_config.get("webhook_host", "0.0.0.0"),
                endpoint=market_data_config.get("webhook_endpoint", "/tradingview"),
                secret_key=market_data_config.get("webhook_secret", "your_webhook_secret"),
                message_bus=self.message_bus
            )
            await self.market_data_handler.initialize()
            
            # Register market data handler with health monitor
            self.health_monitor.register_component(
                component_id="market_data",
                name="TradingView Market Data Handler",
                health_check=self.market_data_handler.health_check
            )
            
            # Create and register a moving average strategy
            ma_strategy = MovingAverageStrategy(
                strategy_id="ma_crossover_btc",
                name="BTC Moving Average Crossover",
                parameters={
                    "short_window": 10,
                    "long_window": 30,
                    "entry_threshold": 0.0005,
                    "exit_threshold": 0.0003,
                    "min_volume": 1.0,
                    "enable_short": False
                }
            )
            ma_strategy.add_ticker("BTCUSDT")
            await self.strategy_engine.register_strategy(ma_strategy)
            
            # Start health dashboard
            await self.health_dashboard.start()
            
            logger.info("Reliability example components initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing reliability example: {str(e)}", exc_info=True)
            return False
    
    async def start(self) -> bool:
        """Start the reliability example.
        
        Returns:
            Start success status
        """
        try:
            logger.info("Starting reliability example...")
            
            # Start health monitor
            await self.health_monitor.start()
            
            # Start exchange adapter
            await self.exchange_adapter.start()
            
            # Start shadow accounting
            await self.shadow_accounting.start()
            
            # Start strategy engine
            await self.strategy_engine.start()
            
            # Start market data handler
            await self.market_data_handler.start()
            
            self.running = True
            logger.info("Reliability example started successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error starting reliability example: {str(e)}", exc_info=True)
            return False
    
    async def stop(self) -> bool:
        """Stop the reliability example.
        
        Returns:
            Stop success status
        """
        try:
            logger.info("Stopping reliability example...")
            self.running = False
            
            # Stop components in reverse order
            if self.market_data_handler:
                await self.market_data_handler.stop()
                
            if self.strategy_engine:
                await self.strategy_engine.stop()
                
            if self.shadow_accounting:
                await self.shadow_accounting.stop()
                
            if self.exchange_adapter:
                await self.exchange_adapter.stop()
                
            if self.health_monitor:
                await self.health_monitor.stop()
                
            if self.health_dashboard:
                await self.health_dashboard.stop()
                
            if self.message_bus:
                await self.message_bus.stop()
                
            logger.info("Reliability example stopped successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping reliability example: {str(e)}", exc_info=True)
            return False
    
    async def simulate_failures(self) -> None:
        """Simulate various failure scenarios to demonstrate recovery mechanisms."""
        while self.running:
            try:
                # Randomly choose a component to simulate failure
                component = random.choice([
                    "exchange_adapter",
                    "market_data",
                    "strategy_engine",
                    "shadow_accounting"
                ])
                
                # Simulate different types of failures
                failure_type = random.choice([
                    "connection_loss",
                    "timeout",
                    "rate_limit",
                    "inconsistent_data"
                ])
                
                logger.info(f"Simulating {failure_type} failure for {component}")
                
                # Report degraded health for the component
                self.health_monitor.update_component_health(
                    component_id=component,
                    health=ComponentHealth(
                        status=HealthStatus.DEGRADED,
                        message=f"Simulated {failure_type} failure",
                        details={"failure_type": failure_type, "timestamp": datetime.now().isoformat()}
                    )
                )
                
                # Wait for recovery mechanisms to kick in
                await asyncio.sleep(10)
                
                # Restore health
                if component == "exchange_adapter":
                    health = await self.exchange_adapter.health_check()
                    self.health_monitor.update_component_health(
                        component_id=component,
                        health=health
                    )
                elif component == "market_data":
                    health = await self.market_data_handler.health_check()
                    self.health_monitor.update_component_health(
                        component_id=component,
                        health=health
                    )
                elif component == "strategy_engine":
                    health = await self.strategy_engine.health_check()
                    self.health_monitor.update_component_health(
                        component_id=component,
                        health=health
                    )
                elif component == "shadow_accounting":
                    health = await self.shadow_accounting.health_check()
                    self.health_monitor.update_component_health(
                        component_id=component,
                        health=health
                    )
                
                # Wait before next simulation
                await asyncio.sleep(random.randint(30, 60))
                
            except Exception as e:
                logger.error(f"Error in failure simulation: {str(e)}")
                await asyncio.sleep(5)

async def run_example():
    """Run the reliability example."""
    # Load configuration (in a real scenario, this would be loaded from a file)
    config = {
        "exchange": {
            "api_key": "your_api_key",
            "api_secret": "your_api_secret",
            "base_url": "https://api.nobitex.ir",
            "max_connections": 10,
            "connection_timeout": 10,
            "idle_timeout": 60,
            "max_retries": 3,
            "retry_delay": 1.0,
            "circuit_breaker_threshold": 5,
            "circuit_breaker_timeout": 30,
            "verification_timeout": 30,
            "verification_retries": 3,
            "cache_dir": "./cache"
        },
        "position": {
            "db_path": "./positions.db",
            "reconciliation_interval": 300,
            "tolerance": 0.01,
            "max_history": 100,
            "auto_correct": True
        },
        "market_data": {
            "webhook_port": 8081,
            "webhook_host": "0.0.0.0",
            "webhook_endpoint": "/tradingview",
            "webhook_secret": "your_webhook_secret"
        },
        "health_monitor": {
            "update_interval": 5
        },
        "health_dashboard": {
            "host": "0.0.0.0",
            "port": 8080
        },
        "message_bus": {
            "max_queue_size": 1000,
            "worker_count": 2
        }
    }
    
    # Create and start the reliability example
    example = ReliabilityExample(config)
    
    # Initialize and start components
    success = await example.initialize()
    if not success:
        logger.error("Failed to initialize reliability example")
        return
    
    success = await example.start()
    if not success:
        logger.error("Failed to start reliability example")
        await example.stop()
        return
    
    # Print information about the demo
    print("\n=== Reliability Example Running ===")
    print("This example demonstrates the following reliability features:")
    print("1. Connection pooling with automatic reconnection")
    print("2. Circuit breaker pattern for fault tolerance")
    print("3. Three-phase commit for transaction verification")
    print("4. Shadow accounting and position reconciliation")
    print("5. Health monitoring with automatic recovery")
    print("6. Component status dashboard (http://localhost:8080)")
    print("\nThe example will simulate random failures to demonstrate recovery mechanisms.")
    print("Press Ctrl+C to stop the example.\n")
    
    # Start the failure simulator
    simulator_task = asyncio.create_task(example.simulate_failures())
    
    try:
        # Run for a specified amount of time or until interrupted
        await asyncio.sleep(3600)  # Run for 1 hour
    except asyncio.CancelledError:
        logger.info("Example cancelled")
    finally:
        # Cancel the simulator task
        simulator_task.cancel()
        try:
            await simulator_task
        except asyncio.CancelledError:
            pass
        
        # Stop the example
        await example.stop()
        logger.info("Example stopped")

def main():
    """Main entry point for the reliability example."""
    try:
        # Run the example
        asyncio.run(run_example())
    except KeyboardInterrupt:
        print("\nExample terminated by user")
    except Exception as e:
        logger.error(f"Error running reliability example: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main() 