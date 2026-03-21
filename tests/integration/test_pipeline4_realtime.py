"""
Integration test for Pipeline 4: Real-time Events (Kafka) -> S3 output + Druid ingestion path.

Tests the event production, Kafka round-trip, and validates that events
destined for S3 and Druid ingestion are correctly shaped.
"""

import io
import json
import time
import uuid
from datetime import datetime

import pyarrow as pa
import pyarrow.orc as orc
import pytest


# ---------------------------------------------------------------------------
# Event generators (mirrors event_producer.py)
# ---------------------------------------------------------------------------
def _make_event(
    event_type: str,
    user_id: str,
    city: str = "Mumbai",
    properties: dict | None = None,
) -> dict:
    """Build a Zomato platform event matching the ZomatoEvent schema."""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "source": "mobile_app",
        "user_id": user_id,
        "session_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        "properties": properties or {},
        "device": {
            "platform": "android",
            "app_version": "17.5.2",
            "os_version": "14",
        },
        "location": {
            "latitude": "19.0760",
            "longitude": "72.8777",
            "city": city,
        },
    }


def _make_order_placed_event(user_id: str, city: str = "Mumbai", amount: float = 750.0) -> dict:
    return _make_event(
        event_type="ORDER_PLACED",
        user_id=user_id,
        city=city,
        properties={
            "order_id": str(uuid.uuid4()),
            "restaurant_id": f"rest_{uuid.uuid4().hex[:8]}",
            "total_amount": str(amount),
            "payment_method": "UPI",
        },
    )


# Topic routing (must match pipeline4 EVENT_TOPIC_MAPPING)
EVENT_TOPIC_MAPPING = {
    "ORDER_PLACED": "orders",
    "ORDER_DELIVERED": "orders",
    "USER_SIGNUP": "users",
    "USER_LOGIN": "users",
    "MENU_VIEWED": "menu",
    "PROMO_APPLIED": "promo",
    "APP_OPENED": "topics",
    "PAGE_VIEWED": "topics",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _produce_events(producer, events: list[dict]) -> None:
    """Route events to the correct Kafka topic and produce them."""
    for event in events:
        topic = EVENT_TOPIC_MAPPING.get(event["event_type"], "topics")
        producer.produce(
            topic=topic,
            key=event["user_id"].encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
        )
    producer.flush(timeout=10)


def _consume_all(consumer, expected: int, timeout: float = 20.0) -> list[dict]:
    """Consume messages until expected count or timeout."""
    messages = []
    deadline = time.time() + timeout
    while len(messages) < expected and time.time() < deadline:
        msg = consumer.poll(timeout=1.0)
        if msg is None or msg.error():
            continue
        messages.append(json.loads(msg.value().decode("utf-8")))
    return messages


def _transform_event_for_s3(event: dict) -> dict:
    """Transform a raw event into the S3 ORC sink schema (mirrors Flink SQL)."""
    return {
        "event_id": event["event_id"],
        "event_type": event["event_type"],
        "source": event["source"],
        "user_id": event["user_id"],
        "session_id": event["session_id"],
        "event_timestamp": event["timestamp"],
        "city": event.get("location", {}).get("city", ""),
        "latitude": float(event.get("location", {}).get("latitude", 0)),
        "longitude": float(event.get("location", {}).get("longitude", 0)),
        "platform": event.get("device", {}).get("platform", ""),
        "order_id": event.get("properties", {}).get("order_id", ""),
        "total_amount": float(event.get("properties", {}).get("total_amount", 0)),
        "dt": datetime.utcnow().strftime("%Y-%m-%d"),
        "hour": datetime.utcnow().hour,
    }


def _transform_event_for_druid(event: dict) -> dict:
    """Transform a raw event into the Druid ingestion schema."""
    return {
        "event_id": event["event_id"],
        "event_type": event["event_type"],
        "source": event["source"],
        "user_id": event["user_id"],
        "city": event.get("location", {}).get("city", ""),
        "order_id": event.get("properties", {}).get("order_id", ""),
        "total_amount": float(event.get("properties", {}).get("total_amount", 0)),
        "event_count": 1,
        "event_timestamp": event["timestamp"],
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestPipeline4Realtime:
    """Test real-time event pipeline: produce -> Kafka -> S3 + Druid path."""

    def test_order_events_routed_to_orders_topic(
        self, kafka_producer, kafka_consumer_factory, kafka_topics,
    ):
        """Verify ORDER_PLACED events land on the 'orders' topic."""
        event = _make_order_placed_event("usr-rt-001", city="Mumbai", amount=500.0)
        _produce_events(kafka_producer, [event])

        consumer = kafka_consumer_factory(["orders"], group_id="test-rt-order-route")
        messages = _consume_all(consumer, expected=1, timeout=15)

        matched = [m for m in messages if m["event_id"] == event["event_id"]]
        assert len(matched) == 1
        assert matched[0]["event_type"] == "ORDER_PLACED"

    def test_user_events_routed_to_users_topic(
        self, kafka_producer, kafka_consumer_factory, kafka_topics,
    ):
        """Verify USER_SIGNUP events land on the 'users' topic."""
        event = _make_event("USER_SIGNUP", user_id="usr-rt-002", city="Delhi")
        _produce_events(kafka_producer, [event])

        consumer = kafka_consumer_factory(["users"], group_id="test-rt-user-route")
        messages = _consume_all(consumer, expected=1, timeout=15)

        matched = [m for m in messages if m["event_id"] == event["event_id"]]
        assert len(matched) == 1

    def test_multi_topic_event_batch(
        self, kafka_producer, kafka_consumer_factory, kafka_topics,
    ):
        """Produce events of multiple types and verify cross-topic delivery."""
        events = [
            _make_order_placed_event("usr-batch-01", city="Bangalore"),
            _make_event("USER_LOGIN", user_id="usr-batch-02"),
            _make_event("MENU_VIEWED", user_id="usr-batch-03"),
            _make_event("PROMO_APPLIED", user_id="usr-batch-04"),
            _make_event("APP_OPENED", user_id="usr-batch-05"),
        ]
        _produce_events(kafka_producer, events)

        # Consume from all topics
        consumer = kafka_consumer_factory(
            ["orders", "users", "menu", "promo", "topics"],
            group_id="test-rt-multitopic",
        )
        messages = _consume_all(consumer, expected=5, timeout=20)

        event_ids = {e["event_id"] for e in events}
        received_ids = {m["event_id"] for m in messages if m["event_id"] in event_ids}
        assert received_ids == event_ids

    def test_s3_orc_output_format(self, upload_test_data, s3_client, s3_buckets):
        """Validate that events transformed for S3 produce valid ORC files."""
        events = [
            _make_order_placed_event(f"usr-orc-{i}", city="Hyderabad", amount=300.0 + i * 50)
            for i in range(25)
        ]
        transformed = [_transform_event_for_s3(e) for e in events]

        # Build ORC from transformed events
        schema = pa.schema([
            ("event_id", pa.string()),
            ("event_type", pa.string()),
            ("source", pa.string()),
            ("user_id", pa.string()),
            ("session_id", pa.string()),
            ("event_timestamp", pa.string()),
            ("city", pa.string()),
            ("latitude", pa.float64()),
            ("longitude", pa.float64()),
            ("platform", pa.string()),
            ("order_id", pa.string()),
            ("total_amount", pa.float64()),
            ("dt", pa.string()),
            ("hour", pa.int32()),
        ])
        arrays = [pa.array([r[f.name] for r in transformed]) for f in schema]
        table = pa.table(dict(zip([f.name for f in schema], arrays)), schema=schema)

        buf = io.BytesIO()
        orc.write_table(table, buf)
        orc_bytes = buf.getvalue()

        dt = datetime.utcnow().strftime("%Y-%m-%d")
        hour = datetime.utcnow().hour
        key = f"pipeline4-realtime/flink-output/dt={dt}/hour={hour}/part-00000.orc"
        upload_test_data("raw", key, orc_bytes)

        # Read back and validate
        resp = s3_client.get_object(Bucket=s3_buckets["raw"], Key=key)
        read_table = orc.read_table(io.BytesIO(resp["Body"].read()))

        assert read_table.num_rows == 25
        assert set(read_table.column("city").to_pylist()) == {"Hyderabad"}
        assert all(v == "ORDER_PLACED" for v in read_table.column("event_type").to_pylist())

    def test_druid_ingestion_event_format(self, kafka_producer, kafka_consumer_factory, kafka_topics):
        """Validate the Druid ingestion event shape written to the druid-ingestion-events topic."""
        event = _make_order_placed_event("usr-druid-001", city="Chennai", amount=999.0)
        druid_record = _transform_event_for_druid(event)

        kafka_producer.produce(
            topic="druid-ingestion-events",
            key=event["user_id"].encode("utf-8"),
            value=json.dumps(druid_record).encode("utf-8"),
        )
        kafka_producer.flush(timeout=10)

        consumer = kafka_consumer_factory(
            ["druid-ingestion-events"], group_id="test-druid-ingestion",
        )
        messages = _consume_all(consumer, expected=1, timeout=15)

        matched = [m for m in messages if m["event_id"] == event["event_id"]]
        assert len(matched) == 1

        record = matched[0]
        assert record["city"] == "Chennai"
        assert record["total_amount"] == 999.0
        assert record["event_count"] == 1
        assert "event_timestamp" in record

    def test_druid_ingestion_batch_aggregation(self, upload_test_data, s3_client, s3_buckets):
        """
        Simulate a batch of events written to S3 for Druid deep storage,
        and verify the aggregation shape.
        """
        events = []
        cities = ["Mumbai", "Delhi", "Bangalore"]
        for i in range(30):
            events.append(
                _make_order_placed_event(
                    f"usr-druid-batch-{i}",
                    city=cities[i % 3],
                    amount=100.0 * (i + 1),
                )
            )

        druid_records = [_transform_event_for_druid(e) for e in events]

        # Write as NDJSON (Druid deep storage format)
        ndjson = "\n".join(json.dumps(r) for r in druid_records)
        dt = datetime.utcnow().strftime("%Y-%m-%d")
        key = f"pipeline4-realtime/druid-deep-storage/dt={dt}/events.json"
        upload_test_data("raw", key, ndjson, content_type="application/json")

        resp = s3_client.get_object(Bucket=s3_buckets["raw"], Key=key)
        lines = resp["Body"].read().decode("utf-8").strip().split("\n")
        records = [json.loads(line) for line in lines]

        assert len(records) == 30

        # Verify city distribution
        city_counts = {}
        for r in records:
            city_counts[r["city"]] = city_counts.get(r["city"], 0) + 1
        assert city_counts == {"Mumbai": 10, "Delhi": 10, "Bangalore": 10}

    def test_high_volume_event_throughput(
        self, kafka_producer, kafka_consumer_factory, kafka_topics,
    ):
        """Stress test: produce 500 events and verify all are received."""
        event_count = 500
        events = [
            _make_order_placed_event(
                f"usr-hv-{i}",
                city=["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai", "Pune"][i % 6],
                amount=100.0 + i,
            )
            for i in range(event_count)
        ]
        event_ids = {e["event_id"] for e in events}

        _produce_events(kafka_producer, events)

        consumer = kafka_consumer_factory(["orders"], group_id="test-rt-high-volume")
        messages = _consume_all(consumer, expected=event_count, timeout=60)

        received_ids = {m["event_id"] for m in messages if m["event_id"] in event_ids}
        assert len(received_ids) == event_count
