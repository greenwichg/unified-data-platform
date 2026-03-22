"""
Python dataclass equivalents of the Java OrderEvent and MenuEvent POJOs.

Converted from:
  - com.zomato.pipeline1.events.OrderEvent
  - com.zomato.pipeline1.events.MenuEvent
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum
from typing import Any, Dict, Optional


# ---------------------------------------------------------------------------
# OrderEvent enums
# ---------------------------------------------------------------------------

class OrderStatus(Enum):
    PLACED = "PLACED"
    CONFIRMED = "CONFIRMED"
    PREPARING = "PREPARING"
    READY_FOR_PICKUP = "READY_FOR_PICKUP"
    DISPATCHED = "DISPATCHED"
    IN_TRANSIT = "IN_TRANSIT"
    DELIVERED = "DELIVERED"
    CANCELLED = "CANCELLED"
    REFUNDED = "REFUNDED"
    FAILED = "FAILED"
    PARTIALLY_DELIVERED = "PARTIALLY_DELIVERED"

    @classmethod
    def from_string(cls, value: Optional[str]) -> Optional["OrderStatus"]:
        """Parse a status string case-insensitively, returning ``None`` for unknown values."""
        if not value:
            return None
        try:
            return cls(value.strip().upper())
        except (ValueError, KeyError):
            return None

    def is_terminal(self) -> bool:
        return self in (
            OrderStatus.DELIVERED,
            OrderStatus.CANCELLED,
            OrderStatus.REFUNDED,
            OrderStatus.FAILED,
        )

    def is_cancellation(self) -> bool:
        return self in (OrderStatus.CANCELLED, OrderStatus.REFUNDED)


class OrderEventType(Enum):
    ORDER_CREATED = "ORDER_CREATED"
    ORDER_UPDATED = "ORDER_UPDATED"
    ORDER_STATUS_CHANGED = "ORDER_STATUS_CHANGED"
    ORDER_AMOUNT_MODIFIED = "ORDER_AMOUNT_MODIFIED"
    ORDER_ADDRESS_CHANGED = "ORDER_ADDRESS_CHANGED"
    ORDER_PAYMENT_UPDATED = "ORDER_PAYMENT_UPDATED"
    ORDER_PROMO_APPLIED = "ORDER_PROMO_APPLIED"
    ORDER_ITEM_ADDED = "ORDER_ITEM_ADDED"
    ORDER_ITEM_REMOVED = "ORDER_ITEM_REMOVED"
    ORDER_CANCELLED = "ORDER_CANCELLED"
    ORDER_DELIVERED = "ORDER_DELIVERED"

    @classmethod
    def from_string(cls, value: Optional[str]) -> Optional["OrderEventType"]:
        if not value:
            return None
        try:
            return cls(value.strip().upper())
        except (ValueError, KeyError):
            return None


# ---------------------------------------------------------------------------
# OrderEvent dataclass
# ---------------------------------------------------------------------------

@dataclass
class OrderEvent:
    """Dataclass representing an order event consumed from Kafka."""

    # Core fields
    order_id: Optional[str] = None
    user_id: Optional[str] = None
    restaurant_id: Optional[str] = None
    order_amount: Optional[Decimal] = None
    order_status: Optional[OrderStatus] = None
    delivery_address: Optional[str] = None
    city: Optional[str] = None
    zone: Optional[str] = None
    payment_method: Optional[str] = None
    item_count: int = 0
    promo_code: Optional[str] = None
    timestamp: int = 0
    event_type: Optional[OrderEventType] = None

    # Extended fields retained from original schema
    status: Optional[str] = None
    subtotal: Optional[Decimal] = None
    tax: Optional[Decimal] = None
    delivery_fee: Optional[Decimal] = None
    total_amount: Optional[Decimal] = None
    latitude: float = 0.0
    longitude: float = 0.0
    created_at: int = 0
    updated_at: int = 0
    op_type: Optional[str] = None

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OrderEvent":
        """Construct an ``OrderEvent`` from a JSON-decoded dictionary.

        Handles both camelCase (Java-style) and snake_case keys.
        """

        def _get(snake: str, camel: str | None = None) -> Any:
            """Return value for *snake* or *camel* key."""
            val = data.get(snake)
            if val is None and camel:
                val = data.get(camel)
            return val

        def _decimal(val: Any) -> Optional[Decimal]:
            if val is None:
                return None
            return Decimal(str(val))

        status_raw = _get("status")
        order_status_raw = _get("order_status", "orderStatus") or status_raw

        evt = cls(
            order_id=_get("order_id", "orderId"),
            user_id=_get("user_id", "userId"),
            restaurant_id=_get("restaurant_id", "restaurantId"),
            order_amount=_decimal(_get("order_amount", "orderAmount") or _get("total_amount", "totalAmount")),
            order_status=OrderStatus.from_string(order_status_raw),
            delivery_address=_get("delivery_address", "deliveryAddress"),
            city=_get("city"),
            zone=_get("zone"),
            payment_method=_get("payment_method", "paymentMethod"),
            item_count=int(_get("item_count", "itemCount") or 0),
            promo_code=_get("promo_code", "promoCode"),
            timestamp=int(_get("timestamp") or _get("updated_at", "updatedAt") or 0),
            event_type=OrderEventType.from_string(_get("event_type", "eventType")),
            status=status_raw,
            subtotal=_decimal(_get("subtotal")),
            tax=_decimal(_get("tax")),
            delivery_fee=_decimal(_get("delivery_fee", "deliveryFee")),
            total_amount=_decimal(_get("total_amount", "totalAmount")),
            latitude=float(_get("latitude") or 0.0),
            longitude=float(_get("longitude") or 0.0),
            created_at=int(_get("created_at", "createdAt") or 0),
            updated_at=int(_get("updated_at", "updatedAt") or 0),
            op_type=_get("op_type", "opType"),
        )
        return evt

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a plain dictionary (camelCase keys for Kafka compatibility)."""
        return {
            "orderId": self.order_id,
            "userId": self.user_id,
            "restaurantId": self.restaurant_id,
            "orderAmount": str(self.order_amount) if self.order_amount is not None else None,
            "orderStatus": self.order_status.value if self.order_status else None,
            "deliveryAddress": self.delivery_address,
            "city": self.city,
            "zone": self.zone,
            "paymentMethod": self.payment_method,
            "itemCount": self.item_count,
            "promoCode": self.promo_code,
            "timestamp": self.timestamp,
            "eventType": self.event_type.value if self.event_type else None,
            "status": self.status,
            "subtotal": str(self.subtotal) if self.subtotal is not None else None,
            "tax": str(self.tax) if self.tax is not None else None,
            "deliveryFee": str(self.delivery_fee) if self.delivery_fee is not None else None,
            "totalAmount": str(self.total_amount) if self.total_amount is not None else None,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
            "opType": self.op_type,
        }

    # ------------------------------------------------------------------
    # Derived helpers (mirrors Java OrderEvent)
    # ------------------------------------------------------------------

    def get_event_time_ms(self) -> int:
        """Return event timestamp in epoch milliseconds."""
        return self.timestamp if self.timestamp > 0 else self.updated_at

    def is_cancellation(self) -> bool:
        if self.order_status is not None and self.order_status.is_cancellation():
            return True
        return (self.status or "").upper() == "CANCELLED"

    def is_terminal(self) -> bool:
        return self.order_status is not None and self.order_status.is_terminal()

    def get_effective_amount(self) -> Optional[Decimal]:
        return self.order_amount if self.order_amount is not None else self.total_amount


# ---------------------------------------------------------------------------
# MenuEvent enums
# ---------------------------------------------------------------------------

class MenuEventType(Enum):
    MENU_ITEM_CREATED = "MENU_ITEM_CREATED"
    MENU_ITEM_UPDATED = "MENU_ITEM_UPDATED"
    MENU_ITEM_PRICE_CHANGED = "MENU_ITEM_PRICE_CHANGED"
    MENU_ITEM_AVAILABILITY_CHANGED = "MENU_ITEM_AVAILABILITY_CHANGED"
    MENU_ITEM_DELETED = "MENU_ITEM_DELETED"
    MENU_ITEM_CATEGORY_CHANGED = "MENU_ITEM_CATEGORY_CHANGED"
    MENU_ITEM_IMAGE_UPDATED = "MENU_ITEM_IMAGE_UPDATED"

    @classmethod
    def from_string(cls, value: Optional[str]) -> Optional["MenuEventType"]:
        if not value:
            return None
        try:
            return cls(value.strip().upper())
        except (ValueError, KeyError):
            return None


# ---------------------------------------------------------------------------
# MenuEvent dataclass
# ---------------------------------------------------------------------------

@dataclass
class MenuEvent:
    """Dataclass representing a menu event consumed from Kafka."""

    # Core fields
    menu_id: Optional[str] = None
    restaurant_id: Optional[str] = None
    item_name: Optional[str] = None
    category: Optional[str] = None
    cuisine: Optional[str] = None
    price: Optional[Decimal] = None
    is_available: bool = False
    last_updated: int = 0
    event_type: Optional[MenuEventType] = None

    # Extended fields retained from original schema
    item_id: Optional[str] = None
    name: Optional[str] = None
    cuisine_type: Optional[str] = None
    previous_price: Optional[Decimal] = None
    vegetarian: bool = False
    vegan: bool = False
    available: bool = False
    preparation_time_mins: int = 0
    rating: float = 0.0
    num_ratings: int = 0
    image_url: Optional[str] = None
    description: Optional[str] = None
    created_at: int = 0
    updated_at: int = 0
    op_type: Optional[str] = None

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MenuEvent":
        """Construct a ``MenuEvent`` from a JSON-decoded dictionary."""

        def _get(snake: str, camel: str | None = None) -> Any:
            val = data.get(snake)
            if val is None and camel:
                val = data.get(camel)
            return val

        def _decimal(val: Any) -> Optional[Decimal]:
            if val is None:
                return None
            return Decimal(str(val))

        return cls(
            menu_id=_get("menu_id", "menuId") or _get("item_id", "itemId"),
            restaurant_id=_get("restaurant_id", "restaurantId"),
            item_name=_get("item_name", "itemName") or _get("name"),
            category=_get("category"),
            cuisine=_get("cuisine") or _get("cuisine_type", "cuisineType"),
            price=_decimal(_get("price")),
            is_available=bool(_get("is_available", "isAvailable") or _get("available") or False),
            last_updated=int(_get("last_updated", "lastUpdated") or _get("updated_at", "updatedAt") or 0),
            event_type=MenuEventType.from_string(_get("event_type", "eventType")),
            item_id=_get("item_id", "itemId"),
            name=_get("name"),
            cuisine_type=_get("cuisine_type", "cuisineType"),
            previous_price=_decimal(_get("previous_price", "previousPrice")),
            vegetarian=bool(_get("vegetarian") or False),
            vegan=bool(_get("vegan") or False),
            available=bool(_get("available") or False),
            preparation_time_mins=int(_get("preparation_time_mins", "preparationTimeMins") or 0),
            rating=float(_get("rating") or 0.0),
            num_ratings=int(_get("num_ratings", "numRatings") or 0),
            image_url=_get("image_url", "imageUrl"),
            description=_get("description"),
            created_at=int(_get("created_at", "createdAt") or 0),
            updated_at=int(_get("updated_at", "updatedAt") or 0),
            op_type=_get("op_type", "opType"),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a plain dictionary (camelCase keys)."""
        return {
            "menuId": self.menu_id,
            "restaurantId": self.restaurant_id,
            "itemName": self.item_name,
            "category": self.category,
            "cuisine": self.cuisine,
            "price": str(self.price) if self.price is not None else None,
            "isAvailable": self.is_available,
            "lastUpdated": self.last_updated,
            "eventType": self.event_type.value if self.event_type else None,
            "itemId": self.item_id,
            "name": self.name,
            "cuisineType": self.cuisine_type,
            "previousPrice": str(self.previous_price) if self.previous_price is not None else None,
            "vegetarian": self.vegetarian,
            "vegan": self.vegan,
            "available": self.available,
            "preparationTimeMins": self.preparation_time_mins,
            "rating": self.rating,
            "numRatings": self.num_ratings,
            "imageUrl": self.image_url,
            "description": self.description,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
            "opType": self.op_type,
        }

    # ------------------------------------------------------------------
    # Derived helpers (mirrors Java MenuEvent)
    # ------------------------------------------------------------------

    def get_event_time_ms(self) -> int:
        """Return event timestamp in epoch milliseconds."""
        return self.last_updated if self.last_updated > 0 else self.updated_at

    def has_price_changed(self) -> bool:
        """Return ``True`` when a price change is detected."""
        return (
            self.previous_price is not None
            and self.price is not None
            and self.previous_price != self.price
        )

    def get_price_change_percent(self) -> float:
        """Return the percentage change in price. Positive means price increased."""
        if (
            self.previous_price is None
            or self.price is None
            or self.previous_price == Decimal("0")
        ):
            return 0.0
        change = (self.price - self.previous_price) / self.previous_price
        return float(change.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)) * 100.0

    def became_unavailable(self) -> bool:
        """Return ``True`` if the item just became unavailable."""
        return (
            self.event_type == MenuEventType.MENU_ITEM_AVAILABILITY_CHANGED
            and not self.is_available
        )
