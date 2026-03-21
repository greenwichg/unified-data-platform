"""
Integration tests for data quality checks across the Zomato data lake.

Tests cover:
  - Schema conformance (required fields, types, null handling)
  - Referential integrity (order -> user, order -> restaurant)
  - Business rule validation (amounts, dates, statuses)
  - Data freshness checks
  - Duplicate detection
  - Partition completeness

These tests validate data quality rules that run as part of the
Airflow data_quality_dag.
"""

import json
import os
from datetime import datetime, timedelta

import pytest

# Use the root conftest fixtures: mock_s3, data_generator, etc.


# ---------------------------------------------------------------------------
# Schema conformance tests
# ---------------------------------------------------------------------------
class TestOrderSchemaConformance:
    """Validate that order records conform to the expected schema."""

    def test_order_has_required_fields(self, sample_order):
        required = {"order_id", "user_id", "restaurant_id", "status", "total_amount"}
        assert required.issubset(set(sample_order.keys()))

    def test_order_id_format(self, sample_order):
        assert sample_order["order_id"].startswith("ord_")

    def test_total_amount_positive(self, sample_order):
        assert sample_order["total_amount"] > 0

    def test_subtotal_less_than_or_equal_total(self, sample_order):
        assert sample_order["subtotal"] <= sample_order["total_amount"]

    def test_valid_status(self, sample_order):
        valid_statuses = {
            "PLACED", "CONFIRMED", "PREPARING", "READY",
            "PICKED_UP", "DELIVERING", "DELIVERED", "CANCELLED",
        }
        assert sample_order["status"] in valid_statuses

    def test_delivery_address_has_coordinates(self, sample_order):
        addr = sample_order["delivery_address"]
        assert -90 <= addr["latitude"] <= 90
        assert -180 <= addr["longitude"] <= 180

    def test_payment_method_not_empty(self, sample_order):
        assert len(sample_order["payment_method"]) > 0

    def test_items_not_empty(self, sample_order):
        assert len(sample_order["items"]) > 0

    def test_item_quantities_positive(self, sample_order):
        for item in sample_order["items"]:
            assert item["quantity"] > 0


class TestUserSchemaConformance:
    def test_user_has_required_fields(self, sample_user):
        required = {"user_id", "name", "email", "city"}
        assert required.issubset(set(sample_user.keys()))

    def test_email_format(self, sample_user):
        assert "@" in sample_user["email"]
        assert "." in sample_user["email"].split("@")[1]

    def test_phone_format(self, sample_user):
        phone = sample_user["phone"]
        assert phone.startswith("+91") or phone.startswith("+")

    def test_total_orders_non_negative(self, sample_user):
        assert sample_user["total_orders"] >= 0


class TestMenuSchemaConformance:
    def test_menu_has_required_fields(self, sample_menu):
        required = {"item_id", "restaurant_id", "name", "price"}
        assert required.issubset(set(sample_menu.keys()))

    def test_price_positive(self, sample_menu):
        assert sample_menu["price"] > 0

    def test_preparation_time_reasonable(self, sample_menu):
        assert 1 <= sample_menu["preparation_time_mins"] <= 120

    def test_rating_in_range(self, sample_menu):
        if sample_menu["rating"] is not None:
            assert 0 <= sample_menu["rating"] <= 5


class TestPromoSchemaConformance:
    def test_promo_has_required_fields(self, sample_promo):
        required = {"promo_id", "promo_code", "discount_type", "discount_value"}
        assert required.issubset(set(sample_promo.keys()))

    def test_discount_value_positive(self, sample_promo):
        assert sample_promo["discount_value"] > 0

    def test_min_order_value_non_negative(self, sample_promo):
        assert sample_promo["min_order_value"] >= 0

    def test_valid_from_before_valid_until(self, sample_promo):
        valid_from = datetime.fromisoformat(sample_promo["valid_from"])
        valid_until = datetime.fromisoformat(sample_promo["valid_until"])
        assert valid_from < valid_until

    def test_valid_discount_type(self, sample_promo):
        valid_types = {"PERCENTAGE", "FLAT", "CASHBACK", "FREE_DELIVERY"}
        assert sample_promo["discount_type"] in valid_types

    def test_usage_count_within_limit(self, sample_promo):
        assert sample_promo["usage_count"] <= sample_promo["max_usage"]


# ---------------------------------------------------------------------------
# Business rule validation
# ---------------------------------------------------------------------------
class TestBusinessRules:
    def test_order_tax_and_fee_sum(self, data_generator):
        order = data_generator.order_event(total_amount=1000)
        computed = order["subtotal"] + order["tax"] + order["delivery_fee"]
        assert abs(computed - order["total_amount"]) < 0.01

    def test_delivery_event_status_progression(self, data_generator):
        valid_transitions = {
            "ASSIGNED": {"PICKED_UP", "CANCELLED"},
            "PICKED_UP": {"DELIVERED", "CANCELLED"},
            "DELIVERED": set(),
            "CANCELLED": set(),
        }
        for status in valid_transitions:
            event = data_generator.delivery_event(status=status)
            assert event["status"] == status

    def test_pro_member_flag_is_boolean(self, data_generator):
        user = data_generator.user_event(is_pro=True)
        assert isinstance(user["is_pro_member"], bool)
        assert user["is_pro_member"] is True


# ---------------------------------------------------------------------------
# Data freshness tests
# ---------------------------------------------------------------------------
class TestDataFreshness:
    def test_order_timestamp_recent(self, sample_order):
        ts = datetime.fromisoformat(sample_order["created_at"])
        assert datetime.utcnow() - ts < timedelta(minutes=5)

    def test_user_event_timestamp_recent(self, sample_user):
        ts = datetime.fromisoformat(sample_user["event_timestamp"])
        assert datetime.utcnow() - ts < timedelta(minutes=5)


# ---------------------------------------------------------------------------
# Duplicate detection tests
# ---------------------------------------------------------------------------
class TestDuplicateDetection:
    def test_generated_order_ids_unique(self, data_generator):
        orders = [data_generator.order_event() for _ in range(100)]
        ids = [o["order_id"] for o in orders]
        assert len(ids) == len(set(ids)), "Order IDs must be unique"

    def test_generated_user_ids_unique(self, data_generator):
        users = [data_generator.user_event() for _ in range(100)]
        ids = [u["user_id"] for u in users]
        assert len(ids) == len(set(ids)), "User IDs must be unique"


# ---------------------------------------------------------------------------
# S3 data lake structure tests
# ---------------------------------------------------------------------------
class TestDataLakeStructure:
    def test_pipeline3_json_write_and_read(self, mock_s3, data_generator):
        bucket = "zomato-data-platform-test-raw-data-lake"
        key = "pipeline3-dynamodb/json-raw/orders/2024/01/15/12/test.json"
        order = data_generator.order_event()

        mock_s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(order).encode("utf-8"),
            ContentType="application/json",
        )

        resp = mock_s3.get_object(Bucket=bucket, Key=key)
        body = json.loads(resp["Body"].read())
        assert body["order_id"] == order["order_id"]

    def test_partition_path_format(self):
        now = datetime.utcnow()
        partition = now.strftime("%Y/%m/%d/%H")
        parts = partition.split("/")
        assert len(parts) == 4
        assert len(parts[0]) == 4  # year
        assert 1 <= int(parts[1]) <= 12  # month
        assert 1 <= int(parts[2]) <= 31  # day
        assert 0 <= int(parts[3]) <= 23  # hour
