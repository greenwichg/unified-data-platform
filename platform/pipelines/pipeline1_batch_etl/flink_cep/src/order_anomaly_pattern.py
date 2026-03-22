"""
PyFlink equivalent of OrderAnomalyPattern.java.

Detects sudden spikes in order cancellations for a restaurant using:
  - Sliding event-time windows (10 min window, 1 min slide) to count cancellations
  - KeyedProcessFunction with ValueState for EMA baseline comparison

Converted from: com.zomato.pipeline1.patterns.OrderAnomalyPattern
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from pyflink.common import Types, Duration
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import (
    DataStream,
    KeyedProcessFunction,
    RuntimeContext,
)
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import SlidingEventTimeWindows, TimeWindow
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction

from events import OrderEvent

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants (same as Java version)
# ---------------------------------------------------------------------------

WINDOW_SIZE_MS: int = 10 * 60 * 1000          # 10 minutes
SLIDE_INTERVAL_MS: int = 1 * 60 * 1000        # 1 minute
SPIKE_MULTIPLIER: float = 3.0
ABSOLUTE_THRESHOLD: int = 5
EMA_ALPHA: float = 0.3


# ---------------------------------------------------------------------------
# Alert dataclass
# ---------------------------------------------------------------------------

@dataclass
class CancellationAnomalyAlert:
    """Alert emitted when a cancellation spike is detected for a restaurant."""

    restaurant_id: str = ""
    cancellation_count: int = 0
    baseline_count: float = 0.0
    spike_ratio: float = 0.0
    severity: str = ""
    window_start_ms: int = 0
    window_end_ms: int = 0
    detected_at: int = 0

    def to_dict(self) -> dict:
        return {
            "restaurant_id": self.restaurant_id,
            "cancellation_count": self.cancellation_count,
            "baseline_count": self.baseline_count,
            "spike_ratio": self.spike_ratio,
            "severity": self.severity,
            "window_start_ms": self.window_start_ms,
            "window_end_ms": self.window_end_ms,
            "detected_at": self.detected_at,
        }

    def __str__(self) -> str:
        return (
            f"CancellationAnomalyAlert{{restaurantId='{self.restaurant_id}', "
            f"count={self.cancellation_count}, "
            f"baseline={self.baseline_count:.1f}, "
            f"ratio={self.spike_ratio:.2f}, "
            f"severity='{self.severity}'}}"
        )


# ---------------------------------------------------------------------------
# Internal: window count result
# ---------------------------------------------------------------------------

@dataclass
class _WindowCancellationCount:
    restaurant_id: str = ""
    count: int = 0
    window_start: int = 0
    window_end: int = 0


# ---------------------------------------------------------------------------
# Aggregate function: count cancellations per window
# ---------------------------------------------------------------------------

class _CancellationCounter(AggregateFunction):
    """Counts the number of cancellation events in a window."""

    def create_accumulator(self) -> int:
        return 0

    def add(self, value: OrderEvent, accumulator: int) -> int:
        return accumulator + 1

    def get_result(self, accumulator: int) -> int:
        return accumulator

    def merge(self, acc_a: int, acc_b: int) -> int:
        return acc_a + acc_b


# ---------------------------------------------------------------------------
# Process window function: attach window metadata
# ---------------------------------------------------------------------------

class _CancellationWindowFunction(ProcessWindowFunction):
    """Attaches window start/end metadata to the aggregated count."""

    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context,
        counts,
    ):
        count = next(iter(counts))
        window: TimeWindow = context.window()
        yield _WindowCancellationCount(
            restaurant_id=key,
            count=count,
            window_start=window.start,
            window_end=window.end,
        )


# ---------------------------------------------------------------------------
# Keyed process function: EMA baseline comparison
# ---------------------------------------------------------------------------

class _BaselineComparisonFunction(KeyedProcessFunction):
    """Compares windowed cancellation counts against an EMA baseline.

    Emits a ``CancellationAnomalyAlert`` when both absolute and relative
    thresholds are exceeded.
    """

    def __init__(self) -> None:
        self._baseline_state = None  # type: ignore[assignment]

    def open(self, runtime_context: RuntimeContext) -> None:
        descriptor = ValueStateDescriptor("cancellation-baseline", Types.DOUBLE())
        self._baseline_state = runtime_context.get_state(descriptor)

    def process_element(self, value: _WindowCancellationCount, ctx: KeyedProcessFunction.Context):
        current_baseline: Optional[float] = self._baseline_state.value()

        if current_baseline is None:
            # First window for this restaurant -- initialize baseline
            self._baseline_state.update(float(value.count))
            return

        # Check for spike
        absolute_spike: bool = value.count >= ABSOLUTE_THRESHOLD
        spike_ratio: float = (
            float(value.count) / current_baseline if current_baseline > 0 else float(value.count)
        )
        relative_spike: bool = spike_ratio >= SPIKE_MULTIPLIER

        if absolute_spike and relative_spike:
            alert = CancellationAnomalyAlert(
                restaurant_id=value.restaurant_id,
                cancellation_count=value.count,
                baseline_count=current_baseline,
                spike_ratio=spike_ratio,
                severity=_determine_severity(spike_ratio),
                window_start_ms=value.window_start,
                window_end_ms=value.window_end,
                detected_at=int(time.time() * 1000),
            )
            logger.warning("Cancellation spike detected: %s", alert)
            yield alert

        # Update EMA baseline
        new_baseline: float = EMA_ALPHA * value.count + (1 - EMA_ALPHA) * current_baseline
        self._baseline_state.update(new_baseline)


# ---------------------------------------------------------------------------
# Severity helper
# ---------------------------------------------------------------------------

def _determine_severity(spike_ratio: float) -> str:
    if spike_ratio >= 10.0:
        return "CRITICAL"
    elif spike_ratio >= 5.0:
        return "HIGH"
    elif spike_ratio >= SPIKE_MULTIPLIER:
        return "MEDIUM"
    return "LOW"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def apply(order_stream: DataStream) -> DataStream:
    """Apply the cancellation anomaly detection pattern to *order_stream*.

    Parameters
    ----------
    order_stream:
        DataStream of ``OrderEvent`` instances.

    Returns
    -------
    DataStream of ``CancellationAnomalyAlert`` instances.
    """

    # Step 1: Filter cancellations and key by restaurant
    windowed_counts: DataStream = (
        order_stream
        .filter(lambda e: (e.status or "").upper() == "CANCELLED")
        .key_by(lambda e: e.restaurant_id)
        .window(
            SlidingEventTimeWindows.of(
                Duration.of_millis(WINDOW_SIZE_MS),
                Duration.of_millis(SLIDE_INTERVAL_MS),
            )
        )
        .aggregate(
            _CancellationCounter(),
            window_function=_CancellationWindowFunction(),
        )
    )

    # Step 2: Compare against EMA baseline and emit alerts
    return (
        windowed_counts
        .key_by(lambda wc: wc.restaurant_id)
        .process(_BaselineComparisonFunction())
    )
