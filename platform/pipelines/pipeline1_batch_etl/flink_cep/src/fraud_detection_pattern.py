"""
PyFlink equivalent of FraudDetectionPattern.java.

Detects potential fraud when the same user places multiple orders from
different delivery addresses within a 5-minute window.

Since PyFlink does not expose the Java CEP library, this implementation
uses a ``KeyedProcessFunction`` with ``ListState`` and ``ValueState``
to replicate the same detection logic:
  - Key by user_id
  - Maintain a list of recent orders (within 5-min window) in ListState
  - On each new order check for 2+ distinct delivery addresses
  - Timer-based cleanup to expire old orders

Converted from: com.zomato.pipeline1.patterns.FraudDetectionPattern
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

from pyflink.common import Types
from pyflink.datastream import DataStream, KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor, ValueStateDescriptor

from events import OrderEvent

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants (same as Java version)
# ---------------------------------------------------------------------------

DETECTION_WINDOW_MS: int = 5 * 60 * 1000      # 5 minutes
MIN_DISTINCT_ADDRESSES: int = 2
MIN_ORDERS: int = 3


# ---------------------------------------------------------------------------
# Alert dataclass
# ---------------------------------------------------------------------------

@dataclass
class FraudAlert:
    """Alert emitted when multi-address fraud is detected for a user."""

    user_id: str = ""
    order_ids: List[str] = field(default_factory=list)
    distinct_addresses: Set[str] = field(default_factory=set)
    alert_type: str = "MULTI_ADDRESS_FRAUD"
    severity: str = ""
    detected_at: int = 0
    window_start_ms: int = 0
    window_end_ms: int = 0

    def to_dict(self) -> dict:
        return {
            "user_id": self.user_id,
            "order_ids": list(self.order_ids),
            "distinct_addresses": list(self.distinct_addresses),
            "alert_type": self.alert_type,
            "severity": self.severity,
            "detected_at": self.detected_at,
            "window_start_ms": self.window_start_ms,
            "window_end_ms": self.window_end_ms,
        }

    def __str__(self) -> str:
        return (
            f"FraudAlert{{userId='{self.user_id}', "
            f"orders={len(self.order_ids)}, "
            f"addresses={len(self.distinct_addresses)}, "
            f"severity='{self.severity}'}}"
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize_address(address: Optional[str]) -> str:
    """Normalize an address for comparison: lowercase, trim, collapse whitespace."""
    if not address:
        return ""
    return re.sub(r"\s+", " ", address.lower().strip())


def _determine_severity(distinct_addresses: int, order_count: int) -> str:
    """Determine alert severity based on distinct address count and order count."""
    if distinct_addresses >= 4 or order_count >= 6:
        return "CRITICAL"
    elif distinct_addresses >= 3 or order_count >= 4:
        return "HIGH"
    elif distinct_addresses >= MIN_DISTINCT_ADDRESSES or order_count >= MIN_ORDERS:
        return "MEDIUM"
    return "LOW"


# ---------------------------------------------------------------------------
# Internal: lightweight order record stored in ListState
# ---------------------------------------------------------------------------

@dataclass
class _OrderRecord:
    """Minimal record stored in state for each recent order."""
    order_id: str = ""
    delivery_address: str = ""
    timestamp_ms: int = 0


# ---------------------------------------------------------------------------
# Keyed process function: fraud detection via state + timers
# ---------------------------------------------------------------------------

class _FraudDetectionProcessFunction(KeyedProcessFunction):
    """Maintains recent orders per user in ``ListState`` and checks for
    multiple distinct delivery addresses within a 5-minute window.

    Uses event-time timers to clean up expired order entries.
    """

    def __init__(self) -> None:
        self._orders_state = None   # type: ignore[assignment]
        self._cleanup_timer_state = None  # type: ignore[assignment]

    def open(self, runtime_context: RuntimeContext) -> None:
        # ListState holding serialized order records as tuples (order_id, address, ts)
        self._orders_state = runtime_context.get_list_state(
            ListStateDescriptor(
                "recent-orders",
                Types.PICKLED_BYTE_ARRAY(),
            )
        )
        # ValueState holding the next cleanup timer timestamp
        self._cleanup_timer_state = runtime_context.get_state(
            ValueStateDescriptor("cleanup-timer", Types.LONG())
        )

    def process_element(self, value: OrderEvent, ctx: KeyedProcessFunction.Context):
        # Only consider PLACED orders with a delivery address
        if (
            not value.delivery_address
            or (value.status or "").upper() != "PLACED"
        ):
            return

        current_ts: int = value.get_event_time_ms()
        new_record = _OrderRecord(
            order_id=value.order_id or "",
            delivery_address=_normalize_address(value.delivery_address),
            timestamp_ms=current_ts,
        )

        # Expire old records and collect current ones
        cutoff: int = current_ts - DETECTION_WINDOW_MS
        active_records: List[_OrderRecord] = []

        for record in self._orders_state.get():
            if record.timestamp_ms >= cutoff:
                active_records.append(record)

        # Add the new record
        active_records.append(new_record)

        # Persist updated list
        self._orders_state.clear()
        for record in active_records:
            self._orders_state.add(record)

        # Register a cleanup timer for window expiration
        cleanup_time: int = current_ts + DETECTION_WINDOW_MS
        existing_timer = self._cleanup_timer_state.value()
        if existing_timer is None or cleanup_time > existing_timer:
            if existing_timer is not None:
                ctx.timer_service().delete_event_time_timer(existing_timer)
            ctx.timer_service().register_event_time_timer(cleanup_time)
            self._cleanup_timer_state.update(cleanup_time)

        # Check for fraud: 2+ distinct addresses in the window
        addresses: Set[str] = set()
        order_ids: List[str] = []
        min_ts: int = current_ts
        max_ts: int = 0

        for record in active_records:
            addresses.add(record.delivery_address)
            order_ids.append(record.order_id)
            min_ts = min(min_ts, record.timestamp_ms)
            max_ts = max(max_ts, record.timestamp_ms)

        if len(addresses) >= MIN_DISTINCT_ADDRESSES:
            severity = _determine_severity(len(addresses), len(order_ids))
            alert = FraudAlert(
                user_id=value.user_id or "",
                order_ids=order_ids,
                distinct_addresses=addresses,
                alert_type="MULTI_ADDRESS_FRAUD",
                severity=severity,
                detected_at=int(time.time() * 1000),
                window_start_ms=min_ts,
                window_end_ms=max_ts,
            )
            logger.warning(
                "Fraud detected: userId=%s, orders=%d, addresses=%d, severity=%s",
                alert.user_id,
                len(order_ids),
                len(addresses),
                severity,
            )
            yield alert

    def on_timer(self, timestamp: int, ctx: KeyedProcessFunction.OnTimerContext):
        """Timer callback to clean up expired order records."""
        cutoff: int = timestamp - DETECTION_WINDOW_MS
        active_records: List[_OrderRecord] = []

        for record in self._orders_state.get():
            if record.timestamp_ms >= cutoff:
                active_records.append(record)

        self._orders_state.clear()
        for record in active_records:
            self._orders_state.add(record)

        # If there are still active records, register a new cleanup timer
        if active_records:
            max_ts = max(r.timestamp_ms for r in active_records)
            next_cleanup = max_ts + DETECTION_WINDOW_MS
            ctx.timer_service().register_event_time_timer(next_cleanup)
            self._cleanup_timer_state.update(next_cleanup)
        else:
            self._cleanup_timer_state.clear()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def apply(order_stream: DataStream) -> DataStream:
    """Apply the fraud detection pattern to *order_stream*.

    Parameters
    ----------
    order_stream:
        DataStream of ``OrderEvent`` instances.

    Returns
    -------
    DataStream of ``FraudAlert`` instances.
    """
    return (
        order_stream
        .key_by(lambda e: e.user_id)
        .process(_FraudDetectionProcessFunction())
    )
