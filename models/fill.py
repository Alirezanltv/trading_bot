"""
Fill Model Module

This module defines the Fill data model representing a fill/execution of an order.
"""

from typing import Dict, Any, Optional
import time
import json
import uuid


class Fill:
    """
    Fill model representing an execution/fill of an order.
    """
    
    def __init__(self,
                 order_id: str,
                 quantity: float,
                 price: float,
                 timestamp: Optional[float] = None,
                 venue: Optional[str] = None,
                 venue_fill_id: Optional[str] = None,
                 commission: float = 0.0,
                 fill_id: Optional[str] = None):
        """
        Initialize fill.
        
        Args:
            order_id: Order ID
            quantity: Filled quantity
            price: Fill price
            timestamp: Fill timestamp (defaults to current time)
            venue: Trading venue/exchange
            venue_fill_id: Venue-specific fill ID
            commission: Commission paid
            fill_id: Fill ID (generated if not provided)
        """
        self.id = fill_id or str(uuid.uuid4())
        self.order_id = order_id
        self.quantity = float(quantity)
        self.price = float(price)
        self.timestamp = timestamp or time.time()
        self.venue = venue
        self.venue_fill_id = venue_fill_id
        self.commission = float(commission)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert fill to dictionary.
        
        Returns:
            Fill as dictionary
        """
        return {
            "id": self.id,
            "order_id": self.order_id,
            "quantity": self.quantity,
            "price": self.price,
            "timestamp": self.timestamp,
            "venue": self.venue,
            "venue_fill_id": self.venue_fill_id,
            "commission": self.commission
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Fill':
        """
        Create fill from dictionary.
        
        Args:
            data: Fill data dictionary
            
        Returns:
            Fill instance
        """
        return cls(
            order_id=data["order_id"],
            quantity=data["quantity"],
            price=data["price"],
            timestamp=data.get("timestamp"),
            venue=data.get("venue"),
            venue_fill_id=data.get("venue_fill_id"),
            commission=data.get("commission", 0.0),
            fill_id=data.get("id")
        )
    
    def to_json(self) -> str:
        """
        Convert fill to JSON string.
        
        Returns:
            Fill as JSON string
        """
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Fill':
        """
        Create fill from JSON string.
        
        Args:
            json_str: JSON string
            
        Returns:
            Fill instance
        """
        data = json.loads(json_str)
        return cls.from_dict(data)
    
    def __str__(self) -> str:
        """String representation of the fill."""
        return (f"Fill(id={self.id}, order_id={self.order_id}, "
                f"quantity={self.quantity}, price={self.price})") 