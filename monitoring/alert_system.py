"""
Alert System for High-Reliability Trading

This module provides a comprehensive alerting and notification system for the trading platform.
It monitors various aspects of the system and sends alerts through multiple channels when issues are detected.

Features:
- Multiple notification channels (console, email, SMS, Telegram)
- Alert levels (info, warning, error, critical)
- Configurable thresholds for different metrics
- Alert aggregation to prevent alert storms
- Alert history and reporting
- Circuit breaker for notification channels
"""

import asyncio
import json
import logging
import os
import smtplib
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import aiohttp
import requests

from trading_system.core.component import Component, ComponentStatus
from trading_system.core import message_bus, MessageTypes
from trading_system.core.circuit_breaker import CircuitBreaker
from trading_system.utils.config import Config

logger = logging.getLogger(__name__)


class AlertLevel(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertSource(Enum):
    """Sources that can generate alerts."""
    SYSTEM = "system"
    MARKET_DATA = "market_data"
    EXCHANGE = "exchange"
    STRATEGY = "strategy"
    EXECUTION = "execution"
    POSITION = "position"
    RISK = "risk"
    HEALTH = "health"
    USER = "user"


class NotificationChannel(Enum):
    """Available notification channels."""
    CONSOLE = "console"
    EMAIL = "email"
    SMS = "sms"
    TELEGRAM = "telegram"
    WEBHOOK = "webhook"
    DATABASE = "database"


@dataclass
class Alert:
    """Alert data container."""
    id: str
    source: AlertSource
    level: AlertLevel
    title: str
    message: str
    timestamp: float = field(default_factory=time.time)
    context: Dict[str, Any] = field(default_factory=dict)
    acknowledged: bool = False
    acknowledged_by: Optional[str] = None
    acknowledged_time: Optional[float] = None
    sent_channels: List[NotificationChannel] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert alert to dictionary."""
        return {
            "id": self.id,
            "source": self.source.value,
            "level": self.level.value,
            "title": self.title,
            "message": self.message,
            "timestamp": self.timestamp,
            "context": self.context,
            "acknowledged": self.acknowledged,
            "acknowledged_by": self.acknowledged_by,
            "acknowledged_time": self.acknowledged_time,
            "sent_channels": [c.value for c in self.sent_channels]
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Alert':
        """Create alert from dictionary."""
        sent_channels = [NotificationChannel(c) for c in data.get("sent_channels", [])]
        return cls(
            id=data["id"],
            source=AlertSource(data["source"]),
            level=AlertLevel(data["level"]),
            title=data["title"],
            message=data["message"],
            timestamp=data["timestamp"],
            context=data.get("context", {}),
            acknowledged=data.get("acknowledged", False),
            acknowledged_by=data.get("acknowledged_by"),
            acknowledged_time=data.get("acknowledged_time"),
            sent_channels=sent_channels
        )


class ChannelStatus(Enum):
    """Notification channel status."""
    OPERATIONAL = "operational"
    DEGRADED = "degraded"
    FAILED = "failed"
    CIRCUIT_OPEN = "circuit_open"


@dataclass
class ChannelStats:
    """Notification channel statistics."""
    total_sent: int = 0
    failed_attempts: int = 0
    consecutive_failures: int = 0
    last_success: Optional[float] = None
    last_failure: Optional[float] = None
    circuit_breaker_open: bool = False
    circuit_open_time: Optional[float] = None
    
    def record_success(self) -> None:
        """Record a successful notification."""
        self.total_sent += 1
        self.consecutive_failures = 0
        self.last_success = time.time()
    
    def record_failure(self) -> None:
        """Record a failed notification."""
        self.failed_attempts += 1
        self.consecutive_failures += 1
        self.last_failure = time.time()
    
    def open_circuit(self) -> None:
        """Open the circuit breaker."""
        self.circuit_breaker_open = True
        self.circuit_open_time = time.time()
    
    def close_circuit(self) -> None:
        """Close the circuit breaker."""
        self.circuit_breaker_open = False
        self.circuit_open_time = None
    
    def get_status(self) -> ChannelStatus:
        """Get the channel status."""
        if self.circuit_breaker_open:
            return ChannelStatus.CIRCUIT_OPEN
        
        if self.consecutive_failures > 5:
            return ChannelStatus.FAILED
        
        if self.consecutive_failures > 2:
            return ChannelStatus.DEGRADED
        
        return ChannelStatus.OPERATIONAL


class AlertSystem(Component):
    """
    Alert system for trading platform.
    
    Monitors various aspects of the system and sends alerts through
    multiple channels when issues are detected.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize alert system.
        
        Args:
            config: Configuration dictionary
        """
        super().__init__(name="alert_system", config=config)
        
        # Alert history
        self.alerts: List[Alert] = []
        self.max_history = config.get("max_history", 1000)
        
        # Alert deduplication
        self.recent_alert_ids: Set[str] = set()
        self.deduplication_window = config.get("deduplication_window", 300)  # 5 minutes
        
        # Channel configuration
        self.channels_config = config.get("channels", {})
        self.enabled_channels: List[NotificationChannel] = []
        self._configure_channels()
        
        # Channel statistics and circuit breakers
        self.channel_stats: Dict[NotificationChannel, ChannelStats] = {}
        self.circuit_breaker_threshold = config.get("circuit_breaker_threshold", 5)
        self.circuit_breaker_reset_time = config.get("circuit_breaker_reset_time", 300)  # 5 minutes
        
        # Alert level minimum thresholds by channel
        self.level_thresholds: Dict[NotificationChannel, AlertLevel] = {}
        self._configure_level_thresholds()
        
        # HTTP session for webhooks and Telegram
        self.http_session: Optional[aiohttp.ClientSession] = None
        
        # Create alert loggers
        self._setup_alert_logging()
        
        # Setup message bus subscriptions
        self._setup_subscriptions()
    
    def _configure_channels(self) -> None:
        """Configure notification channels from config."""
        for channel_name, enabled in self.channels_config.items():
            try:
                if isinstance(enabled, bool) and enabled:
                    channel = NotificationChannel(channel_name)
                    self.enabled_channels.append(channel)
                    self.channel_stats[channel] = ChannelStats()
                    logger.info(f"Enabled notification channel: {channel.value}")
            except ValueError:
                logger.warning(f"Unknown notification channel: {channel_name}")
    
    def _configure_level_thresholds(self) -> None:
        """Configure alert level thresholds for each channel."""
        thresholds = self.config.get("level_thresholds", {})
        
        for channel in self.enabled_channels:
            channel_threshold = thresholds.get(channel.value, "info")
            try:
                self.level_thresholds[channel] = AlertLevel(channel_threshold)
            except ValueError:
                logger.warning(f"Invalid alert level threshold '{channel_threshold}' for {channel.value}, defaulting to INFO")
                self.level_thresholds[channel] = AlertLevel.INFO
    
    def _setup_alert_logging(self) -> None:
        """Set up separate loggers for each alert level."""
        for level in AlertLevel:
            level_name = level.value.upper()
            alert_logger = logging.getLogger(f"alert.{level.value}")
            alert_logger.setLevel(logging.INFO)
            
            # Avoid duplicate handlers
            if not alert_logger.handlers:
                handler = logging.FileHandler(f"alerts_{level.value}.log")
                formatter = logging.Formatter('%(asctime)s - %(message)s')
                handler.setFormatter(formatter)
                alert_logger.addHandler(handler)
    
    def _setup_subscriptions(self) -> None:
        """Set up message bus subscriptions."""
        if message_bus.is_available():
            message_bus.subscribe(MessageTypes.COMPONENT_STATUS, self._handle_component_status)
            message_bus.subscribe(MessageTypes.EXCHANGE_ERROR, self._handle_exchange_error)
            message_bus.subscribe(MessageTypes.POSITION_UPDATE, self._handle_position_update)
            message_bus.subscribe(MessageTypes.TRANSACTION_STATUS, self._handle_transaction_status)
            message_bus.subscribe(MessageTypes.STRATEGY_SIGNAL, self._handle_strategy_signal)
            message_bus.subscribe(MessageTypes.MARKET_DATA, self._handle_market_data)
            message_bus.subscribe(MessageTypes.ALERT, self._handle_alert_message)
    
    async def start(self) -> bool:
        """
        Start the alert system.
        
        Returns:
            Success status
        """
        try:
            logger.info("Starting alert system...")
            self.http_session = aiohttp.ClientSession()
            
            # Send startup alert
            await self.send_alert(
                source=AlertSource.SYSTEM,
                level=AlertLevel.INFO,
                title="System Started",
                message="Alert system has been started successfully",
                context={"startup_time": time.time()}
            )
            
            # Start circuit breaker check task
            asyncio.create_task(self._circuit_breaker_check_loop())
            
            self.status = ComponentStatus.RUNNING
            logger.info("Alert system started successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to start alert system: {str(e)}")
            self.status = ComponentStatus.ERROR
            return False
    
    async def stop(self) -> bool:
        """
        Stop the alert system.
        
        Returns:
            Success status
        """
        try:
            logger.info("Stopping alert system...")
            
            # Send shutdown alert
            await self.send_alert(
                source=AlertSource.SYSTEM,
                level=AlertLevel.INFO,
                title="System Shutdown",
                message="Alert system is shutting down",
                context={"shutdown_time": time.time()}
            )
            
            # Close HTTP session
            if self.http_session:
                await self.http_session.close()
                self.http_session = None
            
            self.status = ComponentStatus.STOPPED
            logger.info("Alert system stopped successfully")
            return True
        except Exception as e:
            logger.error(f"Error stopping alert system: {str(e)}")
            self.status = ComponentStatus.ERROR
            return False
    
    async def send_alert(self, source: AlertSource, level: AlertLevel, 
                          title: str, message: str, context: Optional[Dict[str, Any]] = None) -> Optional[Alert]:
        """
        Send an alert through configured channels.
        
        Args:
            source: Alert source
            level: Alert severity level
            title: Alert title
            message: Alert message
            context: Additional context data
            
        Returns:
            Created alert or None if duplicated
        """
        # Generate alert ID and check for duplicates
        alert_id = f"{source.value}_{level.value}_{hash(title + message)}"
        if alert_id in self.recent_alert_ids:
            logger.debug(f"Duplicate alert suppressed: {title}")
            return None
        
        # Create alert
        alert = Alert(
            id=alert_id,
            source=source,
            level=level,
            title=title,
            message=message,
            timestamp=time.time(),
            context=context or {}
        )
        
        # Add to deduplication cache
        self.recent_alert_ids.add(alert_id)
        
        # Schedule removal from deduplication cache
        asyncio.create_task(self._remove_from_deduplication(alert_id))
        
        # Add to history
        self.alerts.append(alert)
        if len(self.alerts) > self.max_history:
            self.alerts = self.alerts[-self.max_history:]
        
        # Log the alert
        alert_logger = logging.getLogger(f"alert.{level.value}")
        alert_logger.info(f"[{source.value.upper()}] {title}: {message}")
        
        # Send through channels
        tasks = []
        for channel in self.enabled_channels:
            # Check if alert level meets the threshold for this channel
            if self._should_send_to_channel(level, channel):
                task = asyncio.create_task(self._send_to_channel(alert, channel))
                tasks.append(task)
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        # Publish to message bus
        if message_bus.is_available():
            message_bus.publish(MessageTypes.ALERT, alert.to_dict())
        
        return alert
    
    def _should_send_to_channel(self, level: AlertLevel, channel: NotificationChannel) -> bool:
        """Check if alert level meets the threshold for the channel."""
        threshold = self.level_thresholds.get(channel, AlertLevel.INFO)
        
        # Order of severity: INFO < WARNING < ERROR < CRITICAL
        level_values = {
            AlertLevel.INFO: 0,
            AlertLevel.WARNING: 1,
            AlertLevel.ERROR: 2,
            AlertLevel.CRITICAL: 3
        }
        
        # Send if alert level is equal or higher than threshold
        return level_values[level] >= level_values[threshold]
    
    async def _send_to_channel(self, alert: Alert, channel: NotificationChannel) -> bool:
        """
        Send alert to a specific notification channel.
        
        Args:
            alert: Alert to send
            channel: Channel to send through
            
        Returns:
            Success status
        """
        # Check circuit breaker
        stats = self.channel_stats[channel]
        if stats.circuit_breaker_open:
            logger.warning(f"Circuit breaker open for {channel.value}, alert not sent")
            return False
        
        try:
            success = False
            
            if channel == NotificationChannel.CONSOLE:
                success = await self._send_console(alert)
            elif channel == NotificationChannel.EMAIL:
                success = await self._send_email(alert)
            elif channel == NotificationChannel.SMS:
                success = await self._send_sms(alert)
            elif channel == NotificationChannel.TELEGRAM:
                success = await self._send_telegram(alert)
            elif channel == NotificationChannel.WEBHOOK:
                success = await self._send_webhook(alert)
            elif channel == NotificationChannel.DATABASE:
                success = await self._store_in_database(alert)
            
            if success:
                stats.record_success()
                alert.sent_channels.append(channel)
                return True
            else:
                stats.record_failure()
                
                # Check if we need to open circuit breaker
                if stats.consecutive_failures >= self.circuit_breaker_threshold:
                    stats.open_circuit()
                    await self.send_alert(
                        source=AlertSource.SYSTEM,
                        level=AlertLevel.ERROR,
                        title=f"{channel.value.capitalize()} Channel Failed",
                        message=f"Circuit breaker opened for {channel.value} after {stats.consecutive_failures} failures",
                        context={"channel": channel.value}
                    )
                
                return False
                
        except Exception as e:
            logger.error(f"Error sending alert through {channel.value}: {str(e)}")
            stats.record_failure()
            return False
    
    async def _send_console(self, alert: Alert) -> bool:
        """Send alert to console."""
        level_colors = {
            AlertLevel.INFO: "\033[94m",  # Blue
            AlertLevel.WARNING: "\033[93m",  # Yellow
            AlertLevel.ERROR: "\033[91m",  # Red
            AlertLevel.CRITICAL: "\033[97m\033[41m",  # White text on red background
        }
        reset = "\033[0m"
        
        color = level_colors.get(alert.level, "")
        timestamp = datetime.fromtimestamp(alert.timestamp).strftime("%Y-%m-%d %H:%M:%S")
        
        print(f"{color}[{timestamp}] [{alert.level.value.upper()}] [{alert.source.value}] {alert.title}{reset}")
        print(f"{color}{alert.message}{reset}")
        
        if alert.context:
            print(f"{color}Context: {json.dumps(alert.context, indent=2)}{reset}")
        
        print("-" * 80)
        return True
    
    async def _send_email(self, alert: Alert) -> bool:
        """Send alert via email."""
        config = self.channels_config.get("email", {})
        if not config.get("enabled", False):
            return False
        
        smtp_server = config.get("smtp_server")
        smtp_port = config.get("smtp_port", 587)
        username = config.get("username")
        password = config.get("password")
        sender = config.get("sender")
        recipients = config.get("recipients", [])
        
        if not all([smtp_server, username, password, sender, recipients]):
            logger.error("Email configuration incomplete")
            return False
        
        try:
            # Create message
            msg = MIMEMultipart()
            msg['From'] = sender
            msg['To'] = ", ".join(recipients)
            msg['Subject'] = f"[{alert.level.value.upper()}] {alert.title}"
            
            # Message body
            body = f"""
            <html>
            <head>
                <style>
                    body {{ font-family: Arial, sans-serif; }}
                    .alert-info {{ color: #0066cc; }}
                    .alert-warning {{ color: #ff9900; }}
                    .alert-error {{ color: #cc0000; }}
                    .alert-critical {{ background-color: #cc0000; color: white; padding: 5px; }}
                    .context {{ background-color: #f5f5f5; padding: 10px; }}
                </style>
            </head>
            <body>
                <h2 class="alert-{alert.level.value}">{alert.title}</h2>
                <p><strong>Source:</strong> {alert.source.value}</p>
                <p><strong>Level:</strong> {alert.level.value.upper()}</p>
                <p><strong>Time:</strong> {datetime.fromtimestamp(alert.timestamp).strftime("%Y-%m-%d %H:%M:%S")}</p>
                <p><strong>Message:</strong> {alert.message}</p>
                
                <div class="context">
                <p><strong>Context:</strong></p>
                <pre>{json.dumps(alert.context, indent=2)}</pre>
                </div>
            </body>
            </html>
            """
            
            msg.attach(MIMEText(body, 'html'))
            
            # Connect to server and send
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(username, password)
                server.send_message(msg)
            
            return True
        
        except Exception as e:
            logger.error(f"Failed to send email alert: {str(e)}")
            return False
    
    async def _send_sms(self, alert: Alert) -> bool:
        """Send alert via SMS."""
        config = self.channels_config.get("sms", {})
        if not config.get("enabled", False):
            return False
        
        # This is a placeholder. In a real system, you would integrate with an SMS API
        # such as Twilio, Nexmo, etc.
        logger.info(f"SMS alert would be sent: {alert.title}")
        return True
    
    async def _send_telegram(self, alert: Alert) -> bool:
        """Send alert via Telegram."""
        config = self.channels_config.get("telegram", {})
        if not config.get("enabled", False) or not self.http_session:
            return False
        
        bot_token = config.get("bot_token")
        chat_ids = config.get("chat_ids", [])
        
        if not bot_token or not chat_ids:
            logger.error("Telegram configuration incomplete")
            return False
        
        # Format message
        level_emojis = {
            AlertLevel.INFO: "ℹ️",
            AlertLevel.WARNING: "⚠️",
            AlertLevel.ERROR: "❌",
            AlertLevel.CRITICAL: "🚨",
        }
        
        emoji = level_emojis.get(alert.level, "")
        timestamp = datetime.fromtimestamp(alert.timestamp).strftime("%Y-%m-%d %H:%M:%S")
        
        message = f"""{emoji} *{alert.title}*
        
*Level:* {alert.level.value.upper()}
*Source:* {alert.source.value}
*Time:* {timestamp}
        
{alert.message}
        
*Context:*
```
{json.dumps(alert.context, indent=2)}
```"""
        
        # Send to each chat ID
        success = True
        api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        
        for chat_id in chat_ids:
            try:
                async with self.http_session.post(api_url, json={
                    "chat_id": chat_id,
                    "text": message,
                    "parse_mode": "Markdown"
                }) as response:
                    if response.status != 200:
                        text = await response.text()
                        logger.error(f"Telegram API error: {text}")
                        success = False
            except Exception as e:
                logger.error(f"Error sending Telegram message: {str(e)}")
                success = False
        
        return success
    
    async def _send_webhook(self, alert: Alert) -> bool:
        """Send alert via webhook."""
        config = self.channels_config.get("webhook", {})
        if not config.get("enabled", False) or not self.http_session:
            return False
        
        url = config.get("url")
        headers = config.get("headers", {})
        
        if not url:
            logger.error("Webhook configuration incomplete")
            return False
        
        try:
            payload = alert.to_dict()
            async with self.http_session.post(url, json=payload, headers=headers) as response:
                if response.status < 200 or response.status >= 300:
                    text = await response.text()
                    logger.error(f"Webhook error: {response.status} - {text}")
                    return False
            return True
        except Exception as e:
            logger.error(f"Error sending webhook: {str(e)}")
            return False
    
    async def _store_in_database(self, alert: Alert) -> bool:
        """Store alert in database."""
        # This is a placeholder. In a real system, you would store in a database.
        # For now, we'll just count this as successful since we're already storing in memory.
        return True
    
    async def _remove_from_deduplication(self, alert_id: str) -> None:
        """Remove alert ID from deduplication cache after window expires."""
        await asyncio.sleep(self.deduplication_window)
        self.recent_alert_ids.discard(alert_id)
    
    async def _circuit_breaker_check_loop(self) -> None:
        """Periodically check and reset circuit breakers."""
        while self.status == ComponentStatus.RUNNING:
            try:
                for channel, stats in self.channel_stats.items():
                    if stats.circuit_breaker_open and stats.circuit_open_time:
                        # Check if it's time to reset the circuit breaker
                        elapsed = time.time() - stats.circuit_open_time
                        if elapsed >= self.circuit_breaker_reset_time:
                            stats.close_circuit()
                            logger.info(f"Circuit breaker closed for {channel.value}")
                            
                            # Send notification
                            await self.send_alert(
                                source=AlertSource.SYSTEM,
                                level=AlertLevel.INFO,
                                title=f"{channel.value.capitalize()} Channel Restored",
                                message=f"Circuit breaker closed for {channel.value} after {int(elapsed)} seconds",
                                context={"channel": channel.value}
                            )
            except Exception as e:
                logger.error(f"Error in circuit breaker check loop: {str(e)}")
            
            await asyncio.sleep(30)  # Check every 30 seconds
    
    def acknowledge_alert(self, alert_id: str, user: str) -> bool:
        """
        Acknowledge an alert.
        
        Args:
            alert_id: ID of alert to acknowledge
            user: User who acknowledged the alert
            
        Returns:
            Success status
        """
        for alert in self.alerts:
            if alert.id == alert_id and not alert.acknowledged:
                alert.acknowledged = True
                alert.acknowledged_by = user
                alert.acknowledged_time = time.time()
                
                # Log acknowledgment
                logger.info(f"Alert {alert_id} acknowledged by {user}")
                return True
        
        return False
    
    def get_alerts(self, source: Optional[AlertSource] = None, 
                   level: Optional[AlertLevel] = None,
                   start_time: Optional[float] = None,
                   end_time: Optional[float] = None,
                   acknowledged: Optional[bool] = None,
                   limit: int = 100) -> List[Alert]:
        """
        Get filtered alerts.
        
        Args:
            source: Filter by alert source
            level: Filter by alert level
            start_time: Filter by start time (timestamp)
            end_time: Filter by end time (timestamp)
            acknowledged: Filter by acknowledgment status
            limit: Maximum number of alerts to return
            
        Returns:
            Filtered alerts
        """
        filtered = self.alerts
        
        if source:
            filtered = [a for a in filtered if a.source == source]
        
        if level:
            filtered = [a for a in filtered if a.level == level]
        
        if start_time is not None:
            filtered = [a for a in filtered if a.timestamp >= start_time]
        
        if end_time is not None:
            filtered = [a for a in filtered if a.timestamp <= end_time]
        
        if acknowledged is not None:
            filtered = [a for a in filtered if a.acknowledged == acknowledged]
        
        # Sort by timestamp (newest first) and limit
        filtered.sort(key=lambda a: a.timestamp, reverse=True)
        return filtered[:limit]
    
    def get_alert_stats(self) -> Dict[str, Any]:
        """
        Get alert statistics.
        
        Returns:
            Alert statistics
        """
        # Count alerts by level
        level_counts = {level.value: 0 for level in AlertLevel}
        for alert in self.alerts:
            level_counts[alert.level.value] += 1
        
        # Count alerts by source
        source_counts = {source.value: 0 for source in AlertSource}
        for alert in self.alerts:
            source_counts[alert.source.value] += 1
        
        # Count acknowledged vs unacknowledged
        acknowledged = sum(1 for a in self.alerts if a.acknowledged)
        unacknowledged = len(self.alerts) - acknowledged
        
        # Get channel stats
        channel_stats = {}
        for channel, stats in self.channel_stats.items():
            channel_stats[channel.value] = {
                "total_sent": stats.total_sent,
                "failed_attempts": stats.failed_attempts,
                "consecutive_failures": stats.consecutive_failures,
                "last_success": stats.last_success,
                "last_failure": stats.last_failure,
                "circuit_breaker_open": stats.circuit_breaker_open,
                "status": stats.get_status().value
            }
        
        return {
            "total_alerts": len(self.alerts),
            "by_level": level_counts,
            "by_source": source_counts,
            "acknowledged": acknowledged,
            "unacknowledged": unacknowledged,
            "channels": channel_stats
        }
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Perform health check.
        
        Returns:
            Health status
        """
        # Check channel health
        channels_health = {}
        for channel, stats in self.channel_stats.items():
            status = stats.get_status()
            channels_health[channel.value] = {
                "status": status.value,
                "circuit_breaker_open": stats.circuit_breaker_open,
                "consecutive_failures": stats.consecutive_failures
            }
        
        # Overall health
        healthy = all(
            ch["status"] == ChannelStatus.OPERATIONAL.value 
            for ch in channels_health.values()
        )
        
        return {
            "status": "healthy" if healthy else "degraded",
            "channels": channels_health,
            "alerts_count": len(self.alerts),
            "unacknowledged_critical": sum(
                1 for a in self.alerts 
                if a.level == AlertLevel.CRITICAL and not a.acknowledged
            )
        }
    
    # Message bus handlers
    
    async def _handle_component_status(self, data: Dict[str, Any]) -> None:
        """Handle component status updates."""
        component_id = data.get("component_id")
        status = data.get("status")
        
        if not component_id or not status:
            return
        
        if status == "error":
            await self.send_alert(
                source=AlertSource.HEALTH,
                level=AlertLevel.ERROR,
                title=f"Component Error: {component_id}",
                message=f"Component {component_id} has reported an error status",
                context=data
            )
        elif status == "warning":
            await self.send_alert(
                source=AlertSource.HEALTH,
                level=AlertLevel.WARNING,
                title=f"Component Warning: {component_id}",
                message=f"Component {component_id} has reported a warning status",
                context=data
            )
    
    async def _handle_exchange_error(self, data: Dict[str, Any]) -> None:
        """Handle exchange errors."""
        exchange = data.get("exchange", "unknown")
        error = data.get("error", "Unknown error")
        error_code = data.get("error_code")
        method = data.get("method")
        
        level = AlertLevel.ERROR
        if data.get("critical", False):
            level = AlertLevel.CRITICAL
        
        title = f"Exchange Error: {exchange}"
        message = f"Error in exchange {exchange}"
        if method:
            message += f" during {method}"
        message += f": {error}"
        
        await self.send_alert(
            source=AlertSource.EXCHANGE,
            level=level,
            title=title,
            message=message,
            context=data
        )
    
    async def _handle_position_update(self, data: Dict[str, Any]) -> None:
        """Handle position updates."""
        # Only alert on position changes that involve significant P&L
        if "pnl_percent" in data:
            pnl_percent = data["pnl_percent"]
            symbol = data.get("symbol", "unknown")
            
            # Alert on significant profit or loss
            if pnl_percent <= -10:
                await self.send_alert(
                    source=AlertSource.POSITION,
                    level=AlertLevel.WARNING,
                    title=f"Significant Loss: {symbol}",
                    message=f"Position for {symbol} is down {pnl_percent:.2f}%",
                    context=data
                )
            elif pnl_percent >= 20:
                await self.send_alert(
                    source=AlertSource.POSITION,
                    level=AlertLevel.INFO,
                    title=f"Significant Profit: {symbol}",
                    message=f"Position for {symbol} is up {pnl_percent:.2f}%",
                    context=data
                )
    
    async def _handle_transaction_status(self, data: Dict[str, Any]) -> None:
        """Handle transaction status updates."""
        transaction_id = data.get("id", "unknown")
        status = data.get("status")
        type_ = data.get("type", "unknown")
        symbol = data.get("symbol", "unknown")
        
        if status == "failed":
            await self.send_alert(
                source=AlertSource.EXECUTION,
                level=AlertLevel.ERROR,
                title=f"Transaction Failed: {symbol}",
                message=f"Transaction {transaction_id} of type {type_} for {symbol} has failed",
                context=data
            )
        elif status == "timeout":
            await self.send_alert(
                source=AlertSource.EXECUTION,
                level=AlertLevel.WARNING,
                title=f"Transaction Timeout: {symbol}",
                message=f"Transaction {transaction_id} of type {type_} for {symbol} has timed out",
                context=data
            )
    
    async def _handle_strategy_signal(self, data: Dict[str, Any]) -> None:
        """Handle strategy signals."""
        strategy_id = data.get("strategy_id", "unknown")
        signal_type = data.get("signal_type", "unknown")
        symbol = data.get("symbol", "unknown")
        
        if signal_type in ["buy", "sell"]:
            await self.send_alert(
                source=AlertSource.STRATEGY,
                level=AlertLevel.INFO,
                title=f"Strategy Signal: {strategy_id}",
                message=f"Strategy {strategy_id} generated a {signal_type.upper()} signal for {symbol}",
                context=data
            )
    
    async def _handle_market_data(self, data: Dict[str, Any]) -> None:
        """Handle market data updates."""
        # Only alert on significant price movements
        if "price_change_percent" in data:
            change_percent = data["price_change_percent"]
            symbol = data.get("symbol", "unknown")
            
            # Alert on significant price movements
            if abs(change_percent) >= 5:
                level = AlertLevel.INFO
                if abs(change_percent) >= 10:
                    level = AlertLevel.WARNING
                
                direction = "up" if change_percent > 0 else "down"
                await self.send_alert(
                    source=AlertSource.MARKET_DATA,
                    level=level,
                    title=f"Price Movement: {symbol}",
                    message=f"Price of {symbol} moved {direction} by {abs(change_percent):.2f}%",
                    context=data
                )
    
    async def _handle_alert_message(self, data: Dict[str, Any]) -> None:
        """Handle direct alert messages."""
        # Convert from dict to alert object if needed
        if isinstance(data, dict) and "source" in data and "level" in data:
            try:
                source = AlertSource(data["source"])
                level = AlertLevel(data["level"])
                title = data.get("title", "Custom Alert")
                message = data.get("message", "No message provided")
                context = data.get("context", {})
                
                # Don't process alerts that originated from this system (would cause duplication)
                if context.get("generated_by") == "alert_system":
                    return
                
                await self.send_alert(
                    source=source,
                    level=level,
                    title=title,
                    message=message,
                    context=context
                )
            except (ValueError, KeyError) as e:
                logger.error(f"Invalid alert message format: {str(e)}")

# Global instance
_instance = None

def get_alert_system(config: Optional[Dict[str, Any]] = None) -> AlertSystem:
    """
    Get or create the global AlertSystem instance.
    
    Args:
        config: Optional configuration (only used on first call)
        
    Returns:
        AlertSystem instance
    """
    global _instance
    if _instance is None:
        _instance = AlertSystem(config)
    return _instance 