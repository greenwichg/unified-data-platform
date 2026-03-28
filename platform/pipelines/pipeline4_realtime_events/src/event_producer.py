"""
Pipeline 4 - Custom Event Producer: Microservices/Web/Mobile → Kafka

Custom Kafka producer that collects events from Zomato's microservices,
web application, and mobile apps, and publishes them to Kafka topics.

Topics: orders, users, menu, promo
"""

import json
import logging
import os
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum

from confluent_kafka import Producer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
    StringSerializer,
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("pipeline4_event_producer")


class EventType(str, Enum):
    ORDER_PLACED = "ORDER_PLACED"
    ORDER_UPDATED = "ORDER_UPDATED"
    ORDER_CANCELLED = "ORDER_CANCELLED"
    ORDER_DELIVERED = "ORDER_DELIVERED"
    USER_SIGNUP = "USER_SIGNUP"
    USER_LOGIN = "USER_LOGIN"
    USER_LOCATION_UPDATE = "USER_LOCATION_UPDATE"
    MENU_VIEWED = "MENU_VIEWED"
    MENU_ITEM_ADDED = "MENU_ITEM_ADDED"
    RESTAURANT_SEARCH = "RESTAURANT_SEARCH"
    PROMO_APPLIED = "PROMO_APPLIED"
    PROMO_REDEEMED = "PROMO_REDEEMED"
    PAYMENT_INITIATED = "PAYMENT_INITIATED"
    PAYMENT_COMPLETED = "PAYMENT_COMPLETED"
    DELIVERY_ASSIGNED = "DELIVERY_ASSIGNED"
    DELIVERY_PICKED_UP = "DELIVERY_PICKED_UP"
    DELIVERY_COMPLETED = "DELIVERY_COMPLETED"
    APP_OPENED = "APP_OPENED"
    PAGE_VIEWED = "PAGE_VIEWED"
    RATING_SUBMITTED = "RATING_SUBMITTED"


@dataclass
class ZomatoEvent:
    """Base event structure for all Zomato platform events."""

    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = ""
    source: str = ""
    user_id: str = ""
    session_id: str = ""
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    properties: dict = field(default_factory=dict)
    device: dict = field(default_factory=dict)
    location: dict = field(default_factory=dict)


# Topic routing based on event type
EVENT_TOPIC_MAPPING = {
    EventType.ORDER_PLACED: "orders",
    EventType.ORDER_UPDATED: "orders",
    EventType.ORDER_CANCELLED: "orders",
    EventType.ORDER_DELIVERED: "orders",
    EventType.PAYMENT_INITIATED: "orders",
    EventType.PAYMENT_COMPLETED: "orders",
    EventType.DELIVERY_ASSIGNED: "orders",
    EventType.DELIVERY_PICKED_UP: "orders",
    EventType.DELIVERY_COMPLETED: "orders",
    EventType.USER_SIGNUP: "users",
    EventType.USER_LOGIN: "users",
    EventType.USER_LOCATION_UPDATE: "users",
    EventType.RATING_SUBMITTED: "users",
    EventType.MENU_VIEWED: "menu",
    EventType.MENU_ITEM_ADDED: "menu",
    EventType.RESTAURANT_SEARCH: "menu",
    EventType.PROMO_APPLIED: "promo",
    EventType.PROMO_REDEEMED: "promo",
    EventType.APP_OPENED: "users",
    EventType.PAGE_VIEWED: "users",
}


class ZomatoEventProducer:
    """Kafka producer for Zomato platform events."""

    def __init__(
        self,
        bootstrap_servers: str,
        schema_registry_url: str | None = None,
        use_avro: bool = False,
    ):
        self.producer_config = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": f"zomato-event-producer-{uuid.uuid4().hex[:8]}",
            "acks": "all",
            "retries": 5,
            "retry.backoff.ms": 1000,
            "linger.ms": 50,
            "batch.size": 65536,
            "compression.type": "lz4",
            "max.in.flight.requests.per.connection": 5,
            "enable.idempotence": True,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "AWS_MSK_IAM",
            "sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
            "sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
        }
        self.producer = Producer(self.producer_config)
        self.string_serializer = StringSerializer("utf_8")
        self.use_avro = use_avro

        if use_avro and schema_registry_url:
            self.schema_registry = SchemaRegistryClient({"url": schema_registry_url})
        else:
            self.schema_registry = None

        self._delivery_count = 0
        self._error_count = 0

    def _delivery_callback(self, err, msg):
        """Callback for message delivery confirmation."""
        if err is not None:
            self._error_count += 1
            logger.error("Message delivery failed: %s", err)
        else:
            self._delivery_count += 1
            if self._delivery_count % 10000 == 0:
                logger.info(
                    "Delivered %d messages (errors: %d)",
                    self._delivery_count,
                    self._error_count,
                )

    def send_event(self, event: ZomatoEvent) -> None:
        """Send a single event to the appropriate Kafka topic."""
        try:
            event_type = EventType(event.event_type)
            topic = EVENT_TOPIC_MAPPING.get(event_type, "users")
        except ValueError:
            topic = "users"

        key = event.user_id or event.event_id
        value = json.dumps(asdict(event)).encode("utf-8")

        self.producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=value,
            callback=self._delivery_callback,
        )
        self.producer.poll(0)

    def send_batch(self, events: list[ZomatoEvent]) -> None:
        """Send a batch of events."""
        for event in events:
            self.send_event(event)
        self.producer.flush()

    def close(self) -> None:
        """Flush remaining messages and close the producer."""
        remaining = self.producer.flush(timeout=30)
        if remaining > 0:
            logger.warning("%d messages were not delivered", remaining)
        logger.info(
            "Producer closed. Total delivered: %d, errors: %d",
            self._delivery_count,
            self._error_count,
        )


def create_sample_order_event(user_id: str, city: str = "Mumbai") -> ZomatoEvent:
    """Create a sample order placed event for testing."""
    return ZomatoEvent(
        event_type=EventType.ORDER_PLACED.value,
        source="mobile_app",
        user_id=user_id,
        session_id=str(uuid.uuid4()),
        properties={
            "order_id": str(uuid.uuid4()),
            "restaurant_id": f"rest_{uuid.uuid4().hex[:8]}",
            "items": [
                {"name": "Biryani", "quantity": 2, "price": 350.0},
                {"name": "Raita", "quantity": 1, "price": 50.0},
            ],
            "total_amount": 750.0,
            "payment_method": "UPI",
            "estimated_delivery_mins": 35,
        },
        device={
            "platform": "android",
            "app_version": "17.5.2",
            "os_version": "14",
        },
        location={
            "latitude": 19.0760,
            "longitude": 72.8777,
            "city": city,
        },
    )


if __name__ == "__main__":
    bootstrap = os.environ.get("MSK_BOOTSTRAP")
    if not bootstrap:
        logger.error("MSK_BOOTSTRAP environment variable is required")
        sys.exit(1)
    schema_registry = os.environ.get("SCHEMA_REGISTRY_URL")

    producer = ZomatoEventProducer(
        bootstrap_servers=bootstrap,
        schema_registry_url=schema_registry,
    )

    try:
        # Send sample events
        cities = ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai", "Pune"]
        for i in range(100):
            city = cities[i % len(cities)]
            event = create_sample_order_event(f"user_{i}", city)
            producer.send_event(event)

            if i % 10 == 0:
                time.sleep(0.1)
    finally:
        producer.close()
