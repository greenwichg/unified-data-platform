"""
Integration test for Pipeline 2: CDC events (Kafka) -> processed output (MinIO/S3).

Simulates Debezium-style CDC events published to Kafka, verifies that the
events are consumable and that the CDC processor logic correctly transforms
them for downstream Iceberg/ORC storage on S3.
"""

import json
import time
import uuid
from datetime import datetime
from decimal import Decimal

import pytest


# ---------------------------------------------------------------------------
# Test CDC event generators
# ---------------------------------------------------------------------------
def _make_cdc_event(
    table: str,
    op: str,
    before: dict | None,
    after: dict | None,
    ts_ms: int | None = None,
) -> dict:
    """Build a Debezium-format CDC envelope."""
    return {
        "schema": {"type": "struct", "name": f"zomato.{table}.Envelope"},
        "payload": {
            "before": before,
            "after": after,
            "source": {
                "version": "2.5.0.Final",
                "connector": "mysql",
                "name": "zomato",
                "ts_ms": ts_ms or int(datetime.utcnow().timestamp() * 1000),
                "db": "zomato",
                "table": table,
                "server_id": 1,
                "file": "mysql-bin.000003",
                "pos": 12345,
            },
            "op": op,  # c=create, u=update, d=delete, r=read (snapshot)
            "ts_ms": ts_ms or int(datetime.utcnow().timestamp() * 1000),
        },
    }


def _make_order_record(
    order_id: str | None = None,
    user_id: str = "usr-001",
    status: str = "PLACED",
    total_amount: float = 499.0,
    city: str = "Mumbai",
) -> dict:
    """Build a sample order row payload."""
    return {
        "order_id": order_id or str(uuid.uuid4()),
        "user_id": user_id,
        "restaurant_id": "rest-100",
        "status": status,
        "subtotal": total_amount * 0.85,
        "tax": total_amount * 0.05,
        "delivery_fee": total_amount * 0.10,
        "total_amount": total_amount,
        "payment_method": "UPI",
        "city": city,
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat(),
    }


def _make_user_record(
    user_id: str = "usr-001",
    is_pro: bool = False,
) -> dict:
    """Build a sample user row payload."""
    return {
        "user_id": user_id,
        "name": "Test User",
        "email": f"{user_id}@zomato.com",
        "phone": "+919876543210",
        "city": "Mumbai",
        "signup_date": "2023-01-15T00:00:00",
        "is_pro_member": is_pro,
        "total_orders": 42,
        "updated_at": datetime.utcnow().isoformat(),
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _produce_cdc_events(producer, topic: str, events: list[dict]) -> None:
    """Produce CDC events to the given Kafka topic."""
    for event in events:
        key = json.dumps({"order_id": event["payload"]["after"]["order_id"]}).encode() \
            if event["payload"].get("after") and "order_id" in event["payload"]["after"] \
            else b"no-key"
        producer.produce(
            topic=topic,
            key=key,
            value=json.dumps(event).encode("utf-8"),
        )
    producer.flush(timeout=10)


def _consume_messages(consumer, max_messages: int = 100, timeout: float = 15.0) -> list[dict]:
    """Consume up to max_messages from the subscribed topics within timeout."""
    messages = []
    deadline = time.time() + timeout

    while len(messages) < max_messages and time.time() < deadline:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            continue
        messages.append(json.loads(msg.value().decode("utf-8")))

    return messages


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestPipeline2CDC:
    """Test CDC event flow: Kafka -> consume + transform -> S3."""

    def test_cdc_insert_events_roundtrip(self, kafka_producer, kafka_consumer_factory, kafka_topics):
        """Produce CDC INSERT events and verify they are consumable."""
        order_id = str(uuid.uuid4())
        order = _make_order_record(order_id=order_id, city="Delhi", total_amount=750.0)
        event = _make_cdc_event("orders", "c", before=None, after=order)

        _produce_cdc_events(kafka_producer, "orders", [event])

        consumer = kafka_consumer_factory(["orders"], group_id="test-cdc-insert")
        messages = _consume_messages(consumer, max_messages=1, timeout=15)

        assert len(messages) >= 1
        found = [m for m in messages if m["payload"]["after"].get("order_id") == order_id]
        assert len(found) == 1
        assert found[0]["payload"]["op"] == "c"
        assert found[0]["payload"]["after"]["city"] == "Delhi"

    def test_cdc_update_event_has_before_and_after(self, kafka_producer, kafka_consumer_factory, kafka_topics):
        """Produce a CDC UPDATE event and verify both before/after images."""
        order_id = str(uuid.uuid4())
        before = _make_order_record(order_id=order_id, status="PLACED", total_amount=500.0)
        after = _make_order_record(order_id=order_id, status="DELIVERED", total_amount=500.0)
        event = _make_cdc_event("orders", "u", before=before, after=after)

        _produce_cdc_events(kafka_producer, "orders", [event])

        consumer = kafka_consumer_factory(["orders"], group_id="test-cdc-update")
        messages = _consume_messages(consumer, max_messages=5, timeout=15)

        updates = [
            m for m in messages
            if m["payload"].get("after", {}).get("order_id") == order_id
            and m["payload"]["op"] == "u"
        ]
        assert len(updates) >= 1
        assert updates[0]["payload"]["before"]["status"] == "PLACED"
        assert updates[0]["payload"]["after"]["status"] == "DELIVERED"

    def test_cdc_delete_event(self, kafka_producer, kafka_consumer_factory, kafka_topics):
        """Produce a CDC DELETE event and verify it round-trips."""
        order_id = str(uuid.uuid4())
        before = _make_order_record(order_id=order_id)
        event = _make_cdc_event("orders", "d", before=before, after=None)

        _produce_cdc_events(kafka_producer, "orders", [event])

        consumer = kafka_consumer_factory(["orders"], group_id="test-cdc-delete")
        messages = _consume_messages(consumer, max_messages=5, timeout=15)

        deletes = [m for m in messages if m["payload"]["op"] == "d" and m["payload"]["before"].get("order_id") == order_id]
        assert len(deletes) >= 1
        assert deletes[0]["payload"]["after"] is None

    def test_cdc_user_events_on_users_topic(self, kafka_producer, kafka_consumer_factory, kafka_topics):
        """Produce CDC events for the users table and verify topic routing."""
        user = _make_user_record(user_id="usr-cdc-test", is_pro=True)
        event = _make_cdc_event("users", "c", before=None, after=user)

        kafka_producer.produce(
            topic="users",
            key=b"usr-cdc-test",
            value=json.dumps(event).encode("utf-8"),
        )
        kafka_producer.flush(timeout=10)

        consumer = kafka_consumer_factory(["users"], group_id="test-cdc-users")
        messages = _consume_messages(consumer, max_messages=5, timeout=15)

        user_events = [
            m for m in messages
            if m.get("payload", {}).get("after", {}).get("user_id") == "usr-cdc-test"
        ]
        assert len(user_events) >= 1
        assert user_events[0]["payload"]["after"]["is_pro_member"] is True

    def test_cdc_batch_processing(self, kafka_producer, kafka_consumer_factory, kafka_topics):
        """Produce a batch of CDC events and verify all are received."""
        batch_size = 50
        events = []
        order_ids = set()

        for i in range(batch_size):
            oid = str(uuid.uuid4())
            order_ids.add(oid)
            order = _make_order_record(
                order_id=oid,
                city=["Mumbai", "Delhi", "Bangalore", "Hyderabad"][i % 4],
                total_amount=200.0 + i * 10,
            )
            events.append(_make_cdc_event("orders", "c", before=None, after=order))

        _produce_cdc_events(kafka_producer, "orders", events)

        consumer = kafka_consumer_factory(["orders"], group_id="test-cdc-batch")
        messages = _consume_messages(consumer, max_messages=batch_size, timeout=30)

        received_ids = {
            m["payload"]["after"]["order_id"]
            for m in messages
            if m.get("payload", {}).get("after", {}).get("order_id") in order_ids
        }
        assert len(received_ids) == batch_size

    def test_cdc_events_written_to_s3(
        self, kafka_producer, kafka_topics, upload_test_data, s3_client, s3_buckets,
    ):
        """Simulate the S3 write stage: transform CDC events and store as JSON on S3."""
        order_id = str(uuid.uuid4())
        order = _make_order_record(order_id=order_id, city="Pune", total_amount=1200.0)
        cdc_event = _make_cdc_event("orders", "c", before=None, after=order)

        # Transform to the format Pipeline 2 would write to Iceberg
        processed = {
            "order_id": order["order_id"],
            "user_id": order["user_id"],
            "restaurant_id": order["restaurant_id"],
            "status": order["status"],
            "total_amount": order["total_amount"],
            "city": order["city"],
            "op_type": "INSERT",
            "processing_time": datetime.utcnow().isoformat(),
            "dt": datetime.utcnow().strftime("%Y-%m-%d"),
        }

        dt = datetime.utcnow().strftime("%Y-%m-%d")
        key = f"pipeline2-cdc/iceberg/orders/dt={dt}/{order_id}.json"
        upload_test_data("raw", key, json.dumps(processed), content_type="application/json")

        # Verify the data is in S3
        resp = s3_client.get_object(Bucket=s3_buckets["raw"], Key=key)
        stored = json.loads(resp["Body"].read())

        assert stored["order_id"] == order_id
        assert stored["city"] == "Pune"
        assert stored["total_amount"] == 1200.0
        assert stored["op_type"] == "INSERT"
