"""
Root conftest for the Zomato Data Platform test suite.

Provides shared fixtures used across unit, integration, and e2e tests:
  - Mock S3 (via moto)
  - Mock Kafka producer/consumer
  - Test data generators for orders, users, menus, promos, and deliveries
  - Temporary file helpers for state/config files
"""

import json
import os
import tempfile
import uuid
from datetime import datetime, timedelta
from typing import Generator

import pytest

# ---------------------------------------------------------------------------
# Try importing moto for S3 mocking; skip gracefully if unavailable
# ---------------------------------------------------------------------------
try:
    import boto3
    from moto import mock_aws
except ImportError:
    boto3 = None
    mock_aws = None


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TEST_S3_BUCKET_RAW = "zomato-data-platform-test-raw-data-lake"
TEST_S3_BUCKET_PROCESSED = "zomato-data-platform-test-processed"
TEST_MSK_BOOTSTRAP = "localhost:9098"  # MSK (Amazon Managed Streaming for Kafka) test default
TEST_GLUE_SCHEMA_REGISTRY_ENDPOINT = "https://glue.us-east-1.amazonaws.com"
TEST_JDBC_URL = "jdbc:mysql://localhost:3306/zomato_test"

ZOMATO_CITIES = ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai", "Pune"]
CUISINE_TYPES = ["North Indian", "South Indian", "Chinese", "Italian", "Mexican", "Thai"]
PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "Wallet", "Cash"]


# ---------------------------------------------------------------------------
# Mock S3 fixtures (moto-based, no network required)
# ---------------------------------------------------------------------------
@pytest.fixture()
def aws_credentials():
    """Set dummy AWS credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    yield
    for key in [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SECURITY_TOKEN",
        "AWS_SESSION_TOKEN",
    ]:
        os.environ.pop(key, None)


@pytest.fixture()
def mock_s3(aws_credentials):
    """Provide a mocked S3 client with pre-created test buckets."""
    if mock_aws is None:
        pytest.skip("moto is not installed")

    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=TEST_S3_BUCKET_RAW)
        client.create_bucket(Bucket=TEST_S3_BUCKET_PROCESSED)
        yield client


# ---------------------------------------------------------------------------
# Mock Kafka fixtures
# ---------------------------------------------------------------------------
class MockKafkaProducer:
    """In-memory Kafka producer stand-in for unit tests."""

    def __init__(self):
        self.messages: list[dict] = []
        self._flushed = False

    def produce(self, topic: str, key: bytes | None = None, value: bytes | None = None, callback=None):
        msg = {"topic": topic, "key": key, "value": value}
        self.messages.append(msg)
        if callback is not None:
            callback(None, msg)

    def poll(self, timeout: float = 0):
        return 0

    def flush(self, timeout: float = 30):
        self._flushed = True
        return 0

    def get_messages_for_topic(self, topic: str) -> list[dict]:
        return [m for m in self.messages if m["topic"] == topic]


class MockKafkaConsumer:
    """In-memory Kafka consumer stand-in for unit tests."""

    def __init__(self, messages: list[dict] | None = None):
        self._messages = list(messages or [])
        self._position = 0
        self._subscribed_topics: list[str] = []

    def subscribe(self, topics: list[str]):
        self._subscribed_topics = topics

    def poll(self, timeout: float = 1.0):
        if self._position < len(self._messages):
            msg = self._messages[self._position]
            self._position += 1
            return msg
        return None

    def close(self):
        pass

    def commit(self):
        pass


@pytest.fixture()
def mock_kafka_producer() -> MockKafkaProducer:
    """Return a mock Kafka producer."""
    return MockKafkaProducer()


@pytest.fixture()
def mock_kafka_consumer() -> MockKafkaConsumer:
    """Return a mock Kafka consumer with no pre-loaded messages."""
    return MockKafkaConsumer()


# ---------------------------------------------------------------------------
# Temporary file helpers
# ---------------------------------------------------------------------------
@pytest.fixture()
def tmp_state_file(tmp_path) -> str:
    """Return a path to a temporary state file."""
    return str(tmp_path / "spark_jdbc_state.json")


@pytest.fixture()
def tmp_config_dir(tmp_path) -> str:
    """Return a temporary directory for config output."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    return str(config_dir)


# ---------------------------------------------------------------------------
# Test data generators
# ---------------------------------------------------------------------------
class TestDataGenerator:
    """Generates realistic Zomato test data for all entity types."""

    @staticmethod
    def order_event(
        order_id: str | None = None,
        user_id: str | None = None,
        restaurant_id: str | None = None,
        status: str = "PLACED",
        city: str = "Mumbai",
        total_amount: float = 750.0,
    ) -> dict:
        return {
            "order_id": order_id or f"ord_{uuid.uuid4().hex[:12]}",
            "user_id": user_id or f"usr_{uuid.uuid4().hex[:8]}",
            "restaurant_id": restaurant_id or f"rest_{uuid.uuid4().hex[:8]}",
            "status": status,
            "items": [
                {"item_id": "item_001", "name": "Biryani", "quantity": 2, "unit_price": 350.0},
                {"item_id": "item_002", "name": "Raita", "quantity": 1, "unit_price": 50.0},
            ],
            "subtotal": total_amount - 80.0,
            "tax": 50.0,
            "delivery_fee": 30.0,
            "total_amount": total_amount,
            "payment_method": "UPI",
            "delivery_address": {
                "latitude": 19.0760,
                "longitude": 72.8777,
                "address_line": "123 Test Street",
                "city": city,
                "pincode": "400001",
            },
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "event_type": "INSERT",
            "source_pipeline": "pipeline4",
        }

    @staticmethod
    def user_event(
        user_id: str | None = None,
        city: str = "Mumbai",
        is_pro: bool = False,
    ) -> dict:
        return {
            "user_id": user_id or f"usr_{uuid.uuid4().hex[:8]}",
            "name": "Test User",
            "email": "test@example.com",
            "phone": "+919876543210",
            "city": city,
            "signup_date": datetime.utcnow().isoformat(),
            "is_pro_member": is_pro,
            "preferred_cuisine": ["North Indian", "Chinese"],
            "total_orders": 42,
            "average_order_value": 550.0,
            "last_order_at": datetime.utcnow().isoformat(),
            "event_type": "INSERT",
            "event_timestamp": datetime.utcnow().isoformat(),
        }

    @staticmethod
    def menu_event(
        item_id: str | None = None,
        restaurant_id: str | None = None,
        price: float = 350.0,
    ) -> dict:
        return {
            "item_id": item_id or f"item_{uuid.uuid4().hex[:8]}",
            "restaurant_id": restaurant_id or f"rest_{uuid.uuid4().hex[:8]}",
            "name": "Chicken Biryani",
            "description": "Fragrant basmati rice with tender chicken",
            "category": "Main Course",
            "cuisine_type": "North Indian",
            "price": price,
            "is_vegetarian": False,
            "is_available": True,
            "preparation_time_mins": 25,
            "rating": 4.5,
            "total_orders": 1200,
            "image_url": "https://img.zomato.com/biryani.jpg",
            "event_type": "INSERT",
            "event_timestamp": datetime.utcnow().isoformat(),
        }

    @staticmethod
    def promo_event(
        promo_id: str | None = None,
        discount_type: str = "PERCENTAGE",
        discount_value: float = 20.0,
    ) -> dict:
        now = datetime.utcnow()
        return {
            "promo_id": promo_id or f"promo_{uuid.uuid4().hex[:8]}",
            "promo_code": "TESTPROMO20",
            "description": "20% off on orders above 500",
            "discount_type": discount_type,
            "discount_value": discount_value,
            "min_order_value": 500.0,
            "max_discount": 150.0,
            "valid_from": now.isoformat(),
            "valid_until": (now + timedelta(days=30)).isoformat(),
            "applicable_restaurants": [],
            "applicable_cities": ["Mumbai", "Delhi", "Bangalore"],
            "usage_count": 0,
            "max_usage": 10000,
            "is_active": True,
            "event_type": "INSERT",
            "event_timestamp": now.isoformat(),
        }

    @staticmethod
    def delivery_event(
        delivery_id: str | None = None,
        order_id: str | None = None,
        status: str = "ASSIGNED",
    ) -> dict:
        now = datetime.utcnow()
        return {
            "delivery_id": delivery_id or f"del_{uuid.uuid4().hex[:8]}",
            "order_id": order_id or f"ord_{uuid.uuid4().hex[:12]}",
            "rider_id": f"rider_{uuid.uuid4().hex[:8]}",
            "status": status,
            "pickup_location": {"latitude": 19.0760, "longitude": 72.8777},
            "dropoff_location": {"latitude": 19.0850, "longitude": 72.8900},
            "estimated_time_mins": 30,
            "actual_time_mins": None if status == "ASSIGNED" else 28,
            "distance_km": 3.5,
            "assigned_at": now.isoformat(),
            "picked_up_at": None if status == "ASSIGNED" else (now + timedelta(minutes=10)).isoformat(),
            "delivered_at": None if status != "DELIVERED" else (now + timedelta(minutes=28)).isoformat(),
            "event_type": "INSERT",
            "event_timestamp": now.isoformat(),
        }

    @staticmethod
    def dynamodb_stream_record(
        table_name: str = "orders",
        event_name: str = "INSERT",
        data: dict | None = None,
    ) -> dict:
        """Generate a DynamoDB Streams event record in the raw Lambda format."""
        if data is None:
            data = {"order_id": {"S": "ord_test123"}, "status": {"S": "PLACED"}}

        return {
            "eventID": str(uuid.uuid4()),
            "eventName": event_name,
            "eventVersion": "1.1",
            "eventSource": "aws:dynamodb",
            "awsRegion": "us-east-1",
            "eventSourceARN": f"arn:aws:dynamodb:us-east-1:123456789012:table/{table_name}/stream/2024-01-01T00:00:00.000",
            "dynamodb": {
                "ApproximateCreationDateTime": int(datetime.utcnow().timestamp()),
                "Keys": {"order_id": {"S": data.get("order_id", {}).get("S", "test")}},
                "NewImage": data,
                "SequenceNumber": "111",
                "SizeBytes": 256,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
        }


@pytest.fixture()
def data_generator() -> TestDataGenerator:
    """Return an instance of the test data generator."""
    return TestDataGenerator()


@pytest.fixture()
def sample_order(data_generator) -> dict:
    return data_generator.order_event()


@pytest.fixture()
def sample_user(data_generator) -> dict:
    return data_generator.user_event()


@pytest.fixture()
def sample_menu(data_generator) -> dict:
    return data_generator.menu_event()


@pytest.fixture()
def sample_promo(data_generator) -> dict:
    return data_generator.promo_event()


@pytest.fixture()
def sample_delivery(data_generator) -> dict:
    return data_generator.delivery_event()
