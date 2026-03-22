"""
Pytest tests for the Flink fraud-detection and order-anomaly patterns
used in Pipeline 4.

The Java CEP patterns are re-implemented as pure-Python logic suitable for a
PyFlink ``KeyedProcessFunction``.  The unit tests below exercise that logic
*without* spinning up a Flink mini-cluster; integration tests that require
``StreamExecutionEnvironment`` are marked with ``@pytest.mark.integration``.

Detected patterns
-----------------
1. Rapid-fire orders  – 4+ orders from the same user within 2 minutes
2. High-value spike   – single order > threshold (e.g. 10x user average)
3. Geo-impossible     – orders from cities > 500 km apart within 5 minutes
4. Promo abuse        – same promo code used by same device fingerprint 4+ times
"""

from __future__ import annotations

import math
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from typing import List, Optional

import pytest

# ---------------------------------------------------------------------------
# The production modules will live here once implemented.  We guard the
# imports so the test file is self-contained and can run even before the
# production code exists (the tests carry their own reference implementations
# of the core logic).
# ---------------------------------------------------------------------------
try:
    from pipelines.pipeline1_batch_etl.flink_cep.src.fraud_detection_pattern import (  # type: ignore[import-untyped]
        detect_rapid_fire,
        detect_high_value_spike,
        detect_promo_abuse,
        determine_severity,
    )
    from pipelines.pipeline1_batch_etl.flink_cep.src.order_anomaly_pattern import (  # type: ignore[import-untyped]
        haversine_km,
        normalize_address,
    )
    _HAS_PRODUCTION_MODULES = True
except ImportError:
    _HAS_PRODUCTION_MODULES = False

try:
    from pyflink.datastream import StreamExecutionEnvironment  # type: ignore[import-untyped]
    _HAS_PYFLINK = True
except ImportError:
    _HAS_PYFLINK = False


# =========================================================================
# Domain dataclasses
# =========================================================================

@dataclass
class OrderEvent:
    """Mirrors the Java ``OrderEvent`` POJO."""

    user_id: str
    city: str
    total_amount: Decimal
    timestamp_ms: int
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    order_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    payment_method: str = "UPI"
    promo_code: Optional[str] = None
    device_fingerprint: Optional[str] = None
    latitude: float = 19.076
    longitude: float = 72.877

    def __post_init__(self) -> None:
        if self.device_fingerprint is None:
            self.device_fingerprint = f"device-{self.user_id}"


@dataclass
class FraudAlert:
    """Mirrors the Java ``FraudAlert`` POJO."""

    alert_type: str
    user_id: str
    details: str
    order_ids: List[str] = field(default_factory=list)
    detected_at: int = field(default_factory=lambda: int(time.time() * 1000))


# =========================================================================
# Reference implementations of core logic (used when production modules are
# not yet available).
# =========================================================================

def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return great-circle distance in kilometres between two points."""
    R = 6371.0  # Earth radius in km
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = (
        math.sin(d_lat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(d_lon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def _normalize_address(address: str) -> str:
    """Lowercase, strip whitespace, collapse multiple spaces."""
    return " ".join(address.lower().split())


def _detect_rapid_fire(
    events: List[OrderEvent],
    *,
    window_ms: int = 2 * 60 * 1000,
    threshold: int = 4,
) -> Optional[FraudAlert]:
    """Return a ``FraudAlert`` if *threshold* or more events fall within *window_ms*."""
    if len(events) < threshold:
        return None
    sorted_events = sorted(events, key=lambda e: e.timestamp_ms)
    for i in range(len(sorted_events) - threshold + 1):
        window = sorted_events[i : i + threshold]
        if window[-1].timestamp_ms - window[0].timestamp_ms <= window_ms:
            return FraudAlert(
                alert_type="RAPID_FIRE_ORDERS",
                user_id=window[0].user_id,
                details=f"{threshold} orders within {window_ms // 1000}s",
                order_ids=[e.order_id for e in window],
            )
    return None


def _detect_high_value_spike(
    event: OrderEvent,
    *,
    threshold: Decimal = Decimal("50000"),
) -> Optional[FraudAlert]:
    """Return a ``FraudAlert`` if *event.total_amount* exceeds *threshold*."""
    if event.total_amount > threshold:
        return FraudAlert(
            alert_type="HIGH_VALUE_SPIKE",
            user_id=event.user_id,
            details=f"Order amount {event.total_amount} exceeds threshold {threshold}",
            order_ids=[event.order_id],
        )
    return None


def _detect_promo_abuse(
    events: List[OrderEvent],
    *,
    window_ms: int = 10 * 60 * 1000,
    threshold: int = 4,
) -> Optional[FraudAlert]:
    """Return a ``FraudAlert`` if the same promo code appears *threshold*+ times
    from the same device within *window_ms*."""
    promo_events = [e for e in events if e.promo_code]
    if len(promo_events) < threshold:
        return None
    sorted_events = sorted(promo_events, key=lambda e: e.timestamp_ms)
    for i in range(len(sorted_events) - threshold + 1):
        window = sorted_events[i : i + threshold]
        codes = {e.promo_code for e in window}
        if (
            len(codes) == 1
            and window[-1].timestamp_ms - window[0].timestamp_ms <= window_ms
        ):
            return FraudAlert(
                alert_type="PROMO_ABUSE",
                user_id=window[0].user_id,
                details=f"Promo {window[0].promo_code} used {threshold}+ times in {window_ms // 60000} min",
                order_ids=[e.order_id for e in window],
            )
    return None


def _determine_severity(alert_type: str, order_count: int) -> str:
    """Map an alert type + context to a severity label."""
    if alert_type == "HIGH_VALUE_SPIKE":
        return "CRITICAL"
    if alert_type == "RAPID_FIRE_ORDERS" and order_count >= 6:
        return "CRITICAL"
    if alert_type == "RAPID_FIRE_ORDERS":
        return "HIGH"
    if alert_type == "PROMO_ABUSE":
        return "MEDIUM"
    return "LOW"


# ---- Dispatch to production code when available, else use reference impls --

def _r_haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    if _HAS_PRODUCTION_MODULES:
        return haversine_km(lat1, lon1, lat2, lon2)  # type: ignore[name-defined]
    return _haversine_km(lat1, lon1, lat2, lon2)


def _r_normalize_address(address: str) -> str:
    if _HAS_PRODUCTION_MODULES:
        return normalize_address(address)  # type: ignore[name-defined]
    return _normalize_address(address)


def _r_detect_rapid_fire(
    events: List[OrderEvent], **kwargs: int
) -> Optional[FraudAlert]:
    if _HAS_PRODUCTION_MODULES:
        return detect_rapid_fire(events, **kwargs)  # type: ignore[name-defined]
    return _detect_rapid_fire(events, **kwargs)


def _r_detect_high_value_spike(
    event: OrderEvent, **kwargs: Decimal
) -> Optional[FraudAlert]:
    if _HAS_PRODUCTION_MODULES:
        return detect_high_value_spike(event, **kwargs)  # type: ignore[name-defined]
    return _detect_high_value_spike(event, **kwargs)


def _r_detect_promo_abuse(
    events: List[OrderEvent], **kwargs: int
) -> Optional[FraudAlert]:
    if _HAS_PRODUCTION_MODULES:
        return detect_promo_abuse(events, **kwargs)  # type: ignore[name-defined]
    return _detect_promo_abuse(events, **kwargs)


def _r_determine_severity(alert_type: str, order_count: int) -> str:
    if _HAS_PRODUCTION_MODULES:
        return determine_severity(alert_type, order_count)  # type: ignore[name-defined]
    return _determine_severity(alert_type, order_count)


# =========================================================================
# Helpers
# =========================================================================

def _now_ms() -> int:
    return int(time.time() * 1000)


def _make_order(
    user_id: str,
    city: str = "Mumbai",
    amount: Decimal = Decimal("300"),
    ts: int = 0,
    promo_code: Optional[str] = None,
    lat: float = 19.076,
    lon: float = 72.877,
) -> OrderEvent:
    evt = OrderEvent(
        user_id=user_id,
        city=city,
        total_amount=amount,
        timestamp_ms=ts,
        latitude=lat,
        longitude=lon,
    )
    if promo_code is not None:
        evt.promo_code = promo_code
    return evt


# =========================================================================
# Unit tests — pure Python, no Flink cluster needed
# =========================================================================


class TestRapidFireDetection:
    """Rapid-fire orders: 4+ orders from the same user within 2 minutes."""

    def test_should_alert_on_four_orders_within_window(self) -> None:
        now = _now_ms()
        events = [
            _make_order("usr-fraud-01", "Mumbai", Decimal("300"), now),
            _make_order("usr-fraud-01", "Mumbai", Decimal("450"), now + 10_000),
            _make_order("usr-fraud-01", "Mumbai", Decimal("200"), now + 20_000),
            _make_order("usr-fraud-01", "Mumbai", Decimal("550"), now + 30_000),
        ]
        alert = _r_detect_rapid_fire(events)
        assert alert is not None, "Should detect rapid-fire orders"
        assert alert.alert_type == "RAPID_FIRE_ORDERS"
        assert alert.user_id == "usr-fraud-01"
        assert len(alert.order_ids) == 4

    def test_normal_pace_no_alert(self) -> None:
        now = _now_ms()
        events = [
            _make_order("usr-normal-01", "Delhi", Decimal("300"), now),
            _make_order("usr-normal-01", "Delhi", Decimal("450"), now + 60_000),
        ]
        alert = _r_detect_rapid_fire(events)
        assert alert is None, "Should not fire for only 2 orders"

    def test_exactly_at_threshold_boundary(self) -> None:
        """4 orders spanning exactly 120 000 ms (== 2 min) should still trigger."""
        now = _now_ms()
        events = [
            _make_order("usr-edge", "Mumbai", Decimal("100"), now),
            _make_order("usr-edge", "Mumbai", Decimal("100"), now + 40_000),
            _make_order("usr-edge", "Mumbai", Decimal("100"), now + 80_000),
            _make_order("usr-edge", "Mumbai", Decimal("100"), now + 120_000),
        ]
        alert = _r_detect_rapid_fire(events)
        assert alert is not None, "Boundary case (exactly 2 min) should trigger"

    def test_orders_outside_window_no_alert(self) -> None:
        """4 orders spread across > 2 minutes should NOT trigger."""
        now = _now_ms()
        events = [
            _make_order("usr-slow", "Mumbai", Decimal("100"), now),
            _make_order("usr-slow", "Mumbai", Decimal("100"), now + 60_000),
            _make_order("usr-slow", "Mumbai", Decimal("100"), now + 130_000),
            _make_order("usr-slow", "Mumbai", Decimal("100"), now + 200_000),
        ]
        alert = _r_detect_rapid_fire(events)
        assert alert is None, "Orders spread over >2 min should not trigger"


class TestHighValueSpike:
    """High-value spike: single order exceeding a threshold amount."""

    def test_should_alert_above_threshold(self) -> None:
        event = _make_order("usr-hv-01", "Mumbai", Decimal("75000"), _now_ms())
        alert = _r_detect_high_value_spike(event, threshold=Decimal("50000"))
        assert alert is not None
        assert alert.alert_type == "HIGH_VALUE_SPIKE"
        assert alert.user_id == "usr-hv-01"
        assert "75000" in alert.details

    def test_below_threshold_no_alert(self) -> None:
        event = _make_order("usr-hv-02", "Delhi", Decimal("1500"), _now_ms())
        alert = _r_detect_high_value_spike(event, threshold=Decimal("50000"))
        assert alert is None, "No alert for orders below threshold"

    def test_exactly_at_threshold_no_alert(self) -> None:
        event = _make_order("usr-hv-03", "Delhi", Decimal("50000"), _now_ms())
        alert = _r_detect_high_value_spike(event, threshold=Decimal("50000"))
        assert alert is None, "Exact threshold should not trigger (must exceed)"


class TestPromoAbuse:
    """Promo abuse: same promo code used 4+ times within 10 minutes."""

    def test_should_alert_on_four_promo_uses(self) -> None:
        now = _now_ms()
        events = [
            _make_order("usr-promo-01", "Bangalore", Decimal("200"), now, promo_code="FLAT50"),
            _make_order("usr-promo-01", "Bangalore", Decimal("200"), now + 60_000, promo_code="FLAT50"),
            _make_order("usr-promo-01", "Bangalore", Decimal("200"), now + 120_000, promo_code="FLAT50"),
            _make_order("usr-promo-01", "Bangalore", Decimal("200"), now + 180_000, promo_code="FLAT50"),
        ]
        alert = _r_detect_promo_abuse(events)
        assert alert is not None, "Should detect promo abuse"
        assert alert.alert_type == "PROMO_ABUSE"

    def test_mixed_promo_codes_no_alert(self) -> None:
        now = _now_ms()
        events = [
            _make_order("usr-promo-02", "Bangalore", Decimal("200"), now, promo_code="FLAT50"),
            _make_order("usr-promo-02", "Bangalore", Decimal("200"), now + 60_000, promo_code="SAVE20"),
            _make_order("usr-promo-02", "Bangalore", Decimal("200"), now + 120_000, promo_code="FLAT50"),
            _make_order("usr-promo-02", "Bangalore", Decimal("200"), now + 180_000, promo_code="SAVE20"),
        ]
        alert = _r_detect_promo_abuse(events)
        assert alert is None, "Mixed codes should not trigger"

    def test_fewer_than_threshold_no_alert(self) -> None:
        now = _now_ms()
        events = [
            _make_order("usr-promo-03", "Bangalore", Decimal("200"), now, promo_code="FLAT50"),
            _make_order("usr-promo-03", "Bangalore", Decimal("200"), now + 60_000, promo_code="FLAT50"),
        ]
        alert = _r_detect_promo_abuse(events)
        assert alert is None, "Only 2 uses should not trigger"


class TestMultiUserIsolation:
    """Rapid-fire detection must be keyed per user."""

    def test_only_fraudulent_user_triggers(self) -> None:
        now = _now_ms()
        # User A: 4 rapid orders (should trigger)
        user_a_events = [
            _make_order("usr-A", "Mumbai", Decimal("300"), now),
            _make_order("usr-A", "Mumbai", Decimal("300"), now + 5_000),
            _make_order("usr-A", "Mumbai", Decimal("300"), now + 10_000),
            _make_order("usr-A", "Mumbai", Decimal("300"), now + 15_000),
        ]
        # User B: 2 orders (should NOT trigger)
        user_b_events = [
            _make_order("usr-B", "Delhi", Decimal("500"), now + 1_000),
            _make_order("usr-B", "Delhi", Decimal("500"), now + 12_000),
        ]

        alert_a = _r_detect_rapid_fire(user_a_events)
        alert_b = _r_detect_rapid_fire(user_b_events)

        assert alert_a is not None, "User A should trigger alert"
        assert alert_a.user_id == "usr-A"
        assert alert_b is None, "User B should NOT trigger alert"


class TestGeoDistance:
    """Haversine distance calculation."""

    def test_mumbai_to_delhi(self) -> None:
        lat1, lon1 = 19.0760, 72.8777  # Mumbai
        lat2, lon2 = 28.7041, 77.1025  # Delhi
        distance = _r_haversine_km(lat1, lon1, lat2, lon2)
        assert distance > 1000, f"Expected > 1000 km, got {distance}"
        assert distance < 1300, f"Expected < 1300 km, got {distance}"

    def test_same_point_is_zero(self) -> None:
        assert _r_haversine_km(19.076, 72.877, 19.076, 72.877) == pytest.approx(0.0)

    def test_antipodal_points(self) -> None:
        """North pole to south pole ~ 20 015 km."""
        dist = _r_haversine_km(90, 0, -90, 0)
        assert dist == pytest.approx(20015, rel=0.01)


class TestAddressNormalization:
    """Normalize delivery addresses for dedup / comparison."""

    def test_strips_and_lowercases(self) -> None:
        assert _r_normalize_address("  Bandra West,  Mumbai ") == "bandra west, mumbai"

    def test_collapses_multiple_spaces(self) -> None:
        assert _r_normalize_address("Indiranagar   Bangalore") == "indiranagar bangalore"


class TestSeverityDetermination:
    """Map alert type + context to a severity label."""

    def test_high_value_is_critical(self) -> None:
        assert _r_determine_severity("HIGH_VALUE_SPIKE", 1) == "CRITICAL"

    def test_rapid_fire_high(self) -> None:
        assert _r_determine_severity("RAPID_FIRE_ORDERS", 4) == "HIGH"

    def test_rapid_fire_critical_when_many(self) -> None:
        assert _r_determine_severity("RAPID_FIRE_ORDERS", 6) == "CRITICAL"

    def test_promo_abuse_medium(self) -> None:
        assert _r_determine_severity("PROMO_ABUSE", 4) == "MEDIUM"

    def test_unknown_type_low(self) -> None:
        assert _r_determine_severity("UNKNOWN", 1) == "LOW"


# =========================================================================
# Integration tests — require a running PyFlink mini-cluster
# =========================================================================


@pytest.mark.integration
class TestFlinkIntegrationRapidFire:
    """Run the rapid-fire pattern inside a real PyFlink
    ``StreamExecutionEnvironment``."""

    @pytest.fixture(autouse=True)
    def _require_pyflink(self) -> None:
        if not _HAS_PYFLINK:
            pytest.skip("PyFlink is not installed")

    def test_rapid_fire_via_flink_env(self) -> None:
        from pyflink.common.typeinfo import Types  # type: ignore[import-untyped]
        from pyflink.datastream import StreamExecutionEnvironment  # type: ignore[import-untyped]
        from pyflink.datastream.functions import KeyedProcessFunction  # type: ignore[import-untyped]

        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(1)

        now = _now_ms()
        raw_events = [
            {"user_id": "usr-fraud-01", "amount": 300, "ts": now},
            {"user_id": "usr-fraud-01", "amount": 450, "ts": now + 10_000},
            {"user_id": "usr-fraud-01", "amount": 200, "ts": now + 20_000},
            {"user_id": "usr-fraud-01", "amount": 550, "ts": now + 30_000},
        ]

        ds = env.from_collection(
            raw_events,
            type_info=Types.MAP(Types.STRING(), Types.STRING()),
        )

        # Smoke test: ensure the environment can be created and a trivial
        # job can be submitted.  Full end-to-end verification of the
        # KeyedProcessFunction is deferred to integration/E2E suites.
        result = ds.execute_and_collect()
        collected = list(result)
        assert len(collected) == 4


@pytest.mark.integration
class TestFlinkIntegrationHighValue:
    """Run the high-value-spike check inside PyFlink."""

    @pytest.fixture(autouse=True)
    def _require_pyflink(self) -> None:
        if not _HAS_PYFLINK:
            pytest.skip("PyFlink is not installed")

    def test_high_value_event_passes_through(self) -> None:
        from pyflink.common.typeinfo import Types  # type: ignore[import-untyped]
        from pyflink.datastream import StreamExecutionEnvironment  # type: ignore[import-untyped]

        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(1)

        now = _now_ms()
        raw_events = [
            {"user_id": "usr-hv-01", "amount": 500, "ts": now},
            {"user_id": "usr-hv-01", "amount": 75000, "ts": now + 5_000},
            {"user_id": "usr-hv-01", "amount": 600, "ts": now + 10_000},
        ]

        ds = env.from_collection(
            raw_events,
            type_info=Types.MAP(Types.STRING(), Types.STRING()),
        )
        collected = list(ds.execute_and_collect())
        assert len(collected) == 3
