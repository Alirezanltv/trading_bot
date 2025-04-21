"""
Market Data Subsystem

This package provides a comprehensive market data collection, validation, storage,
and distribution system. It integrates multiple data sources including
exchanges, TradingView, and real-time WebSocket feeds with time-series
database storage and validation.

Components:
- Data Facade: Provides unified access to market data with failover
- Data Unifier: Normalizes data from different sources
- TradingView Integration: Processes and validates TradingView signals
- WebSocket Clients: Real-time market data streams
- Time-Series DB: Storage for historical market data
"""

from typing import Dict, Any, Optional

# Core components
from trading_system.core.message_bus import MessageBus

# Import main components
try:
    from trading_system.market_data.market_data_facade import MarketDataFacade, get_market_data_facade
    from trading_system.market_data.data_unifier import DataUnifier, get_data_unifier
    from trading_system.market_data.timeseries_db import TimeSeriesDB, get_timeseries_db
    from trading_system.market_data.tradingview_validator import TradingViewValidator
    from trading_system.market_data.tradingview_provider import TradingViewProvider, get_tradingview_provider
except ImportError as e:
    import logging
    logging.warning(f"Error importing market data components: {str(e)}")

# Import WebSocket clients
try:
    from trading_system.market_data.nobitex_websocket import NobitexWebSocketClient
except ImportError:
    pass


class MarketDataSubsystem:
    """
    Market Data Subsystem integrates all market data components.
    
    This is the main entry point for the market data subsystem, providing
    centralized initialization and configuration for all market data components.
    """
    
    def __init__(self, config: Dict[str, Any], message_bus: Optional[MessageBus] = None):
        """
        Initialize the market data subsystem.
        
        Args:
            config: Configuration dictionary
            message_bus: Message bus instance
        """
        self.config = config or {}
        self.message_bus = message_bus
        
        # Initialize components
        self.facade = None
        self.unifier = None
        self.db = None
        self.tradingview = None
        self.websocket_clients = {}
        
        # Component initialization status
        self.component_status = {}
    
    async def initialize(self) -> bool:
        """
        Initialize all market data components.
        
        Returns:
            Initialization success
        """
        success = True
        
        try:
            # Initialize time-series database
            db_config = self.config.get("timeseries_db", {})
            if db_config.get("enabled", True):
                try:
                    self.db = get_timeseries_db(db_config)
                    db_success = await self.db.initialize()
                    self.component_status["timeseries_db"] = db_success
                    if not db_success:
                        success = False
                except Exception as e:
                    import logging
                    logging.error(f"Error initializing time-series database: {str(e)}", exc_info=True)
                    self.component_status["timeseries_db"] = False
                    success = False
            
            # Initialize data unifier
            unifier_config = self.config.get("data_unifier", {})
            try:
                self.unifier = get_data_unifier(unifier_config)
                self.component_status["data_unifier"] = True
            except Exception as e:
                import logging
                logging.error(f"Error initializing data unifier: {str(e)}", exc_info=True)
                self.component_status["data_unifier"] = False
                success = False
            
            # Initialize TradingView provider
            tv_config = self.config.get("tradingview", {})
            if tv_config.get("enabled", True):
                try:
                    self.tradingview = get_tradingview_provider(tv_config, self.message_bus)
                    tv_success = await self.tradingview.initialize()
                    self.component_status["tradingview"] = tv_success
                    if not tv_success:
                        success = False
                except Exception as e:
                    import logging
                    logging.error(f"Error initializing TradingView provider: {str(e)}", exc_info=True)
                    self.component_status["tradingview"] = False
                    success = False
            
            # Initialize WebSocket clients
            ws_config = self.config.get("websocket", {})
            
            # Initialize Nobitex WebSocket client
            nobitex_ws_config = ws_config.get("nobitex", {})
            if nobitex_ws_config.get("enabled", True):
                try:
                    from trading_system.market_data.nobitex_websocket import NobitexWebSocketClient
                    nobitex_ws = NobitexWebSocketClient(nobitex_ws_config, self.message_bus)
                    nobitex_success = await nobitex_ws.initialize()
                    self.websocket_clients["nobitex"] = nobitex_ws
                    self.component_status["nobitex_websocket"] = nobitex_success
                    if not nobitex_success:
                        success = False
                except Exception as e:
                    import logging
                    logging.error(f"Error initializing Nobitex WebSocket client: {str(e)}", exc_info=True)
                    self.component_status["nobitex_websocket"] = False
                    success = False
            
            # Initialize market data facade (last, so it can use other components)
            facade_config = self.config.get("facade", {})
            try:
                self.facade = get_market_data_facade(facade_config, self.message_bus)
                facade_success = await self.facade.initialize()
                self.component_status["market_data_facade"] = facade_success
                
                # Register providers with the facade
                if facade_success:
                    # Register TradingView provider
                    if self.tradingview and self.component_status.get("tradingview", False):
                        from trading_system.market_data.market_data_facade import DataProviderType
                        await self.facade.register_provider(
                            provider=self.tradingview,
                            provider_type=DataProviderType.SECONDARY,
                            priority=50,
                            name="tradingview"
                        )
                    
                    # Register WebSocket clients
                    for name, client in self.websocket_clients.items():
                        if self.component_status.get(f"{name}_websocket", False):
                            from trading_system.market_data.market_data_facade import DataProviderType
                            await self.facade.register_provider(
                                provider=client,
                                provider_type=DataProviderType.PRIMARY, 
                                priority=90,
                                name=f"{name}_websocket"
                            )
                
                if not facade_success:
                    success = False
                    
            except Exception as e:
                import logging
                logging.error(f"Error initializing market data facade: {str(e)}", exc_info=True)
                self.component_status["market_data_facade"] = False
                success = False
            
            return success
            
        except Exception as e:
            import logging
            logging.error(f"Error initializing market data subsystem: {str(e)}", exc_info=True)
            return False
    
    async def shutdown(self) -> bool:
        """
        Shutdown all market data components.
        
        Returns:
            Shutdown success
        """
        success = True
        
        try:
            # Shutdown WebSocket clients
            for name, client in self.websocket_clients.items():
                try:
                    await client.shutdown()
                except Exception as e:
                    import logging
                    logging.error(f"Error shutting down {name} WebSocket client: {str(e)}", exc_info=True)
                    success = False
            
            # Shutdown TradingView provider
            if self.tradingview:
                try:
                    await self.tradingview.shutdown()
                except Exception as e:
                    import logging
                    logging.error(f"Error shutting down TradingView provider: {str(e)}", exc_info=True)
                    success = False
            
            # Shutdown market data facade
            if self.facade:
                try:
                    await self.facade.shutdown()
                except Exception as e:
                    import logging
                    logging.error(f"Error shutting down market data facade: {str(e)}", exc_info=True)
                    success = False
            
            # Shutdown time-series database
            if self.db:
                try:
                    await self.db.shutdown()
                except Exception as e:
                    import logging
                    logging.error(f"Error shutting down time-series database: {str(e)}", exc_info=True)
                    success = False
            
            return success
            
        except Exception as e:
            import logging
            logging.error(f"Error shutting down market data subsystem: {str(e)}", exc_info=True)
            return False

# Singleton instance
_instance = None

def get_market_data_subsystem(config: Dict[str, Any] = None, message_bus: Optional[MessageBus] = None) -> MarketDataSubsystem:
    """
    Get or create the MarketDataSubsystem instance.
    
    Args:
        config: Configuration dictionary
        message_bus: Message bus instance
        
    Returns:
        MarketDataSubsystem instance
    """
    global _instance
    if _instance is None:
        if config is None:
            config = {}
        _instance = MarketDataSubsystem(config, message_bus)
    return _instance


__all__ = [
    "MarketDataSubsystem",
    "get_market_data_subsystem",
    "MarketDataFacade",
    "get_market_data_facade",
    "DataUnifier",
    "get_data_unifier",
    "TimeSeriesDB",
    "get_timeseries_db",
    "TradingViewValidator",
    "TradingViewProvider",
    "get_tradingview_provider"
] 