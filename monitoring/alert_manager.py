"""
Alert manager for high-reliability trading system.

This module provides alert management capabilities for detecting
and handling system issues and critical events.
"""

import time
import logging
import smtplib
import json
from enum import Enum, auto
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass
from email.mime.text import MIMEText
from threading import Lock, Thread
from queue import Queue
import os
from datetime import datetime

from trading_system.core.component import Component, ComponentStatus


class AlertLevel(Enum):
    """Alert severity levels."""
    INFO = auto()
    WARNING = auto()
    ERROR = auto()
    CRITICAL = auto()


@dataclass
class Alert:
    """Alert data structure."""
    id: str
    timestamp: int
    level: AlertLevel
    source: str
    message: str
    details: Optional[Dict[str, Any]] = None
    acknowledged: bool = False
    acknowledged_by: Optional[str] = None
    acknowledged_at: Optional[int] = None


class AlertManager(Component):
    """
    Alert manager for handling system alerts.
    
    Responsible for:
    - Collecting alerts from system components
    - Processing and filtering alerts
    - Notifying administrators via configured channels
    - Tracking alert status and acknowledgements
    - Implementing alert escalation policies
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the alert manager.
        
        Args:
            config: Configuration parameters
        """
        super().__init__(name="alert_manager", config=config)
        
        # Alert configuration
        self.enable_email = config.get("enable_email", False)
        self.enable_sms = config.get("enable_sms", False)
        self.enable_console = config.get("enable_console", True)
        self.enable_file = config.get("enable_file", True)
        
        # Notification settings
        self.email_config = config.get("email", {})
        self.sms_config = config.get("sms", {})
        self.notification_cooldown = config.get("notification_cooldown", 300)  # seconds
        
        # Alert storage
        self.alerts = []
        self.alert_history_limit = config.get("alert_history_limit", 1000)
        self.alert_file = config.get("alert_file", "alerts.json")
        
        # Alert processing
        self.alert_queue = Queue()
        self.processor_thread = None
        self.running = False
        self.last_notifications = {}  # alert_id -> timestamp
        
        # Custom alert handlers
        self.alert_handlers = {}
        
        # Lock for thread safety
        self.lock = Lock()
        
        # Load previous alerts if available
        self._load_alerts()
        
        logging.info("Alert manager initialized")
    
    def start(self) -> bool:
        """
        Start the alert manager.
        
        Returns:
            bool: True if started successfully, False otherwise
        """
        try:
            with self.lock:
                if self.running:
                    logging.warning("Alert manager is already running")
                    return True
                
                self.running = True
                self.processor_thread = Thread(target=self._alert_processor, daemon=True)
                self.processor_thread.start()
                
                self._update_status(ComponentStatus.HEALTHY)
                logging.info("Alert manager started")
                return True
                
        except Exception as e:
            logging.error(f"Error starting alert manager: {str(e)}")
            self._update_status(ComponentStatus.ERROR, str(e))
            return False
    
    def stop(self) -> bool:
        """
        Stop the alert manager.
        
        Returns:
            bool: True if stopped successfully, False otherwise
        """
        try:
            with self.lock:
                if not self.running:
                    logging.warning("Alert manager is not running")
                    return True
                
                self.running = False
                if self.processor_thread and self.processor_thread.is_alive():
                    self.processor_thread.join(timeout=2.0)
                
                self._update_status(ComponentStatus.STOPPED)
                logging.info("Alert manager stopped")
                return True
                
        except Exception as e:
            logging.error(f"Error stopping alert manager: {str(e)}")
            return False
    
    def add_alert(self, level: AlertLevel, source: str, message: str, 
                 details: Optional[Dict[str, Any]] = None) -> str:
        """
        Add a new alert to the system.
        
        Args:
            level: Alert severity level
            source: Component that generated the alert
            message: Alert message
            details: Additional alert details
            
        Returns:
            str: Alert ID
        """
        # Generate alert ID
        alert_id = f"{source}-{int(time.time())}"
        
        # Create alert object
        alert = Alert(
            id=alert_id,
            timestamp=int(time.time()),
            level=level,
            source=source,
            message=message,
            details=details,
            acknowledged=False
        )
        
        # Add to processing queue
        self.alert_queue.put(alert)
        
        # Log alert
        log_level = logging.INFO if level == AlertLevel.INFO else (
            logging.WARNING if level == AlertLevel.WARNING else logging.ERROR
        )
        logging.log(log_level, f"Alert [{level.name}] from {source}: {message}")
        
        return alert_id
    
    def get_active_alerts(self) -> List[Alert]:
        """
        Get list of active (unacknowledged) alerts.
        
        Returns:
            List[Alert]: List of active alerts
        """
        with self.lock:
            return [alert for alert in self.alerts if not alert.acknowledged]
    
    def get_all_alerts(self) -> List[Alert]:
        """
        Get all alerts in the system.
        
        Returns:
            List[Alert]: List of all alerts
        """
        with self.lock:
            return self.alerts.copy()
    
    def acknowledge_alert(self, alert_id: str, acknowledged_by: str) -> bool:
        """
        Acknowledge an alert.
        
        Args:
            alert_id: Alert ID to acknowledge
            acknowledged_by: Name of person acknowledging the alert
            
        Returns:
            bool: True if acknowledged successfully, False otherwise
        """
        with self.lock:
            for alert in self.alerts:
                if alert.id == alert_id and not alert.acknowledged:
                    alert.acknowledged = True
                    alert.acknowledged_by = acknowledged_by
                    alert.acknowledged_at = int(time.time())
                    logging.info(f"Alert {alert_id} acknowledged by {acknowledged_by}")
                    
                    # Save alerts to file
                    self._save_alerts()
                    
                    return True
            
            logging.warning(f"Alert {alert_id} not found or already acknowledged")
            return False
    
    def register_alert_handler(self, level: AlertLevel, handler: Callable[[Alert], None]) -> None:
        """
        Register a custom alert handler function for a specific alert level.
        
        Args:
            level: Alert level to handle
            handler: Handler function that takes an Alert object
        """
        with self.lock:
            if level not in self.alert_handlers:
                self.alert_handlers[level] = []
            
            self.alert_handlers[level].append(handler)
            logging.info(f"Registered new alert handler for level {level.name}")
    
    def _alert_processor(self) -> None:
        """Alert processing thread."""
        logging.info("Alert processor thread started")
        
        while self.running:
            try:
                # Wait for alert with timeout to allow clean shutdown
                try:
                    alert = self.alert_queue.get(timeout=1.0)
                except:
                    continue
                
                # Process the alert
                self._process_alert(alert)
                
                # Mark task as done
                self.alert_queue.task_done()
                
            except Exception as e:
                logging.error(f"Error in alert processor: {str(e)}")
    
    def _process_alert(self, alert: Alert) -> None:
        """
        Process a single alert.
        
        Args:
            alert: Alert to process
        """
        with self.lock:
            # Add to alerts list
            self.alerts.append(alert)
            
            # Trim alert history if needed
            if len(self.alerts) > self.alert_history_limit:
                # Remove oldest acknowledged alerts first
                self.alerts.sort(key=lambda a: (a.acknowledged, a.timestamp))
                self.alerts = self.alerts[-self.alert_history_limit:]
            
            # Save alerts to file
            self._save_alerts()
        
        # Call custom handlers if registered
        self._call_custom_handlers(alert)
        
        # Send notifications based on alert level
        self._send_notifications(alert)
    
    def _call_custom_handlers(self, alert: Alert) -> None:
        """
        Call registered custom handlers for an alert.
        
        Args:
            alert: Alert to handle
        """
        with self.lock:
            handlers = self.alert_handlers.get(alert.level, [])
            
        # Call handlers outside of lock
        for handler in handlers:
            try:
                handler(alert)
            except Exception as e:
                logging.error(f"Error in alert handler: {str(e)}")
    
    def _send_notifications(self, alert: Alert) -> None:
        """
        Send notifications for an alert.
        
        Args:
            alert: Alert to send notifications for
        """
        # Check notification cooldown
        current_time = time.time()
        if alert.id in self.last_notifications:
            time_since_last = current_time - self.last_notifications[alert.id]
            if time_since_last < self.notification_cooldown:
                logging.info(f"Skipping notification for alert {alert.id} due to cooldown")
                return
        
        # Update last notification time
        self.last_notifications[alert.id] = current_time
        
        # Console notification
        if self.enable_console:
            self._send_console_notification(alert)
        
        # File notification
        if self.enable_file:
            self._send_file_notification(alert)
        
        # Email notification for WARNING, ERROR, and CRITICAL alerts
        if self.enable_email and alert.level in [AlertLevel.WARNING, AlertLevel.ERROR, AlertLevel.CRITICAL]:
            self._send_email_notification(alert)
        
        # SMS notification for ERROR and CRITICAL alerts
        if self.enable_sms and alert.level in [AlertLevel.ERROR, AlertLevel.CRITICAL]:
            self._send_sms_notification(alert)
    
    def _send_console_notification(self, alert: Alert) -> None:
        """
        Send console notification for an alert.
        
        Args:
            alert: Alert to send notification for
        """
        try:
            level_str = alert.level.name
            timestamp_str = datetime.fromtimestamp(alert.timestamp).strftime('%Y-%m-%d %H:%M:%S')
            
            # Format based on level
            if alert.level == AlertLevel.INFO:
                print(f"\033[94m[INFO]\033[0m {timestamp_str} - {alert.source}: {alert.message}")
            elif alert.level == AlertLevel.WARNING:
                print(f"\033[93m[WARNING]\033[0m {timestamp_str} - {alert.source}: {alert.message}")
            elif alert.level == AlertLevel.ERROR:
                print(f"\033[91m[ERROR]\033[0m {timestamp_str} - {alert.source}: {alert.message}")
            elif alert.level == AlertLevel.CRITICAL:
                print(f"\033[1;91m[CRITICAL]\033[0m {timestamp_str} - {alert.source}: {alert.message}")
                
            # Print details if available
            if alert.details:
                print(f"  Details: {json.dumps(alert.details, indent=2)}")
                
        except Exception as e:
            logging.error(f"Error sending console notification: {str(e)}")
    
    def _send_file_notification(self, alert: Alert) -> None:
        """
        Send file notification for an alert.
        
        Args:
            alert: Alert to send notification for
        """
        try:
            log_dir = "logs"
            os.makedirs(log_dir, exist_ok=True)
            
            log_file = os.path.join(log_dir, "alerts.log")
            timestamp_str = datetime.fromtimestamp(alert.timestamp).strftime('%Y-%m-%d %H:%M:%S')
            
            with open(log_file, "a") as f:
                f.write(f"[{timestamp_str}] {alert.level.name}: {alert.source} - {alert.message}\n")
                if alert.details:
                    f.write(f"  Details: {json.dumps(alert.details)}\n")
                    
        except Exception as e:
            logging.error(f"Error sending file notification: {str(e)}")
    
    def _send_email_notification(self, alert: Alert) -> None:
        """
        Send email notification for an alert.
        
        Args:
            alert: Alert to send notification for
        """
        try:
            # Check if email is configured
            if not self.email_config:
                logging.warning("Email notification skipped: No email configuration")
                return
            
            # Extract email config
            smtp_server = self.email_config.get("smtp_server")
            smtp_port = self.email_config.get("smtp_port", 587)
            username = self.email_config.get("username")
            password = self.email_config.get("password")
            from_addr = self.email_config.get("from_addr")
            to_addrs = self.email_config.get("to_addrs", [])
            
            if not (smtp_server and username and password and from_addr and to_addrs):
                logging.warning("Email notification skipped: Incomplete email configuration")
                return
            
            # Format email subject and body
            timestamp_str = datetime.fromtimestamp(alert.timestamp).strftime('%Y-%m-%d %H:%M:%S')
            subject = f"[{alert.level.name}] Alert from {alert.source}"
            body = f"""
            Alert Details:
            --------------
            Timestamp: {timestamp_str}
            Level: {alert.level.name}
            Source: {alert.source}
            Message: {alert.message}
            """
            
            if alert.details:
                body += f"\nAdditional Details:\n{json.dumps(alert.details, indent=4)}"
            
            # Create email message
            msg = MIMEText(body)
            msg['Subject'] = subject
            msg['From'] = from_addr
            msg['To'] = ", ".join(to_addrs)
            
            # Send email
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(username, password)
                server.send_message(msg)
                
            logging.info(f"Email notification sent for alert {alert.id}")
            
        except Exception as e:
            logging.error(f"Error sending email notification: {str(e)}")
    
    def _send_sms_notification(self, alert: Alert) -> None:
        """
        Send SMS notification for an alert.
        
        Args:
            alert: Alert to send notification for
        """
        try:
            # Check if SMS is configured
            if not self.sms_config:
                logging.warning("SMS notification skipped: No SMS configuration")
                return
            
            # In a real implementation, this would connect to an SMS gateway service
            # For demo purposes, just log the SMS that would be sent
            phones = self.sms_config.get("phone_numbers", [])
            message = f"[{alert.level.name}] {alert.source}: {alert.message}"
            
            logging.info(f"SMS notification would be sent to {len(phones)} recipients: {message}")
            
        except Exception as e:
            logging.error(f"Error sending SMS notification: {str(e)}")
    
    def _save_alerts(self) -> None:
        """Save alerts to file."""
        try:
            # Serialize alerts to JSON
            alerts_data = []
            for alert in self.alerts:
                alerts_data.append({
                    "id": alert.id,
                    "timestamp": alert.timestamp,
                    "level": alert.level.name,
                    "source": alert.source,
                    "message": alert.message,
                    "details": alert.details,
                    "acknowledged": alert.acknowledged,
                    "acknowledged_by": alert.acknowledged_by,
                    "acknowledged_at": alert.acknowledged_at
                })
            
            # Save to file
            with open(self.alert_file, 'w') as f:
                json.dump(alerts_data, f, indent=2)
                
        except Exception as e:
            logging.error(f"Error saving alerts to file: {str(e)}")
    
    def _load_alerts(self) -> None:
        """Load alerts from file."""
        try:
            if not os.path.exists(self.alert_file):
                logging.info(f"Alerts file {self.alert_file} not found")
                return
            
            with open(self.alert_file, 'r') as f:
                alerts_data = json.load(f)
            
            # Deserialize alerts from JSON
            with self.lock:
                self.alerts = []
                for alert_data in alerts_data:
                    alert = Alert(
                        id=alert_data["id"],
                        timestamp=alert_data["timestamp"],
                        level=AlertLevel[alert_data["level"]],
                        source=alert_data["source"],
                        message=alert_data["message"],
                        details=alert_data.get("details"),
                        acknowledged=alert_data.get("acknowledged", False),
                        acknowledged_by=alert_data.get("acknowledged_by"),
                        acknowledged_at=alert_data.get("acknowledged_at")
                    )
                    self.alerts.append(alert)
                
                logging.info(f"Loaded {len(self.alerts)} alerts from file")
                
        except Exception as e:
            logging.error(f"Error loading alerts from file: {str(e)}")
    
    def get_status_report(self) -> Dict[str, Any]:
        """
        Get status report with alert metrics.
        
        Returns:
            Dict: Status report
        """
        with self.lock:
            # Count alerts by level
            active_by_level = {level.name: 0 for level in AlertLevel}
            total_by_level = {level.name: 0 for level in AlertLevel}
            
            for alert in self.alerts:
                total_by_level[alert.level.name] += 1
                if not alert.acknowledged:
                    active_by_level[alert.level.name] += 1
            
            # Calculate total active alerts
            total_active = sum(active_by_level.values())
            
            status = {
                "active_alerts": total_active,
                "active_by_level": active_by_level,
                "total_alerts": len(self.alerts),
                "total_by_level": total_by_level,
                "timestamp": int(time.time())
            }
            
            return status 