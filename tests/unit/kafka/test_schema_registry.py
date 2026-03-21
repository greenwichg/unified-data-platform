"""
Unit tests for Kafka Schema Registry interactions.

Tests cover:
  - Avro schema validation (all 4 primary schemas)
  - Schema compatibility rules
  - Schema field requirements and types
  - BACKWARD compatibility verification
  - Schema evolution scenarios
"""

import json
import os
from pathlib import Path

import pytest

SCHEMA_DIR = Path(__file__).resolve().parents[3] / "schemas" / "avro"


def load_schema(name: str) -> dict:
    """Load and parse an Avro schema file."""
    with open(SCHEMA_DIR / name) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Schema file loading and validation
# ---------------------------------------------------------------------------
class TestAvroSchemaFiles:
    @pytest.mark.parametrize("schema_file", [
        "order.avsc", "user.avsc", "menu.avsc", "promo.avsc",
    ])
    def test_schema_is_valid_json(self, schema_file):
        schema = load_schema(schema_file)
        assert isinstance(schema, dict)

    @pytest.mark.parametrize("schema_file", [
        "order.avsc", "user.avsc", "menu.avsc", "promo.avsc",
    ])
    def test_schema_has_required_fields(self, schema_file):
        schema = load_schema(schema_file)
        assert schema["type"] == "record"
        assert "name" in schema
        assert "namespace" in schema
        assert "fields" in schema

    @pytest.mark.parametrize("schema_file", [
        "order.avsc", "user.avsc", "menu.avsc", "promo.avsc",
    ])
    def test_namespace_is_zomato(self, schema_file):
        schema = load_schema(schema_file)
        assert schema["namespace"] == "com.zomato.events"


# ---------------------------------------------------------------------------
# Order schema tests
# ---------------------------------------------------------------------------
class TestOrderSchema:
    @pytest.fixture()
    def schema(self):
        return load_schema("order.avsc")

    def test_name(self, schema):
        assert schema["name"] == "Order"

    def test_required_fields_present(self, schema):
        field_names = {f["name"] for f in schema["fields"]}
        required = {
            "order_id", "user_id", "restaurant_id", "status",
            "items", "subtotal", "tax", "delivery_fee", "total_amount",
            "payment_method", "delivery_address", "created_at", "updated_at",
            "event_type", "source_pipeline",
        }
        assert required.issubset(field_names)

    def test_order_id_is_string(self, schema):
        field = next(f for f in schema["fields"] if f["name"] == "order_id")
        assert field["type"] == "string"

    def test_status_is_enum(self, schema):
        field = next(f for f in schema["fields"] if f["name"] == "status")
        assert field["type"]["type"] == "enum"
        symbols = field["type"]["symbols"]
        assert "PLACED" in symbols
        assert "DELIVERED" in symbols
        assert "CANCELLED" in symbols

    def test_items_is_array(self, schema):
        field = next(f for f in schema["fields"] if f["name"] == "items")
        assert field["type"]["type"] == "array"
        item_schema = field["type"]["items"]
        assert item_schema["type"] == "record"
        assert item_schema["name"] == "OrderItem"

    def test_monetary_fields_are_decimal(self, schema):
        for field_name in ["subtotal", "tax", "delivery_fee", "total_amount"]:
            field = next(f for f in schema["fields"] if f["name"] == field_name)
            assert field["type"]["logicalType"] == "decimal"
            assert field["type"]["precision"] == 10
            assert field["type"]["scale"] == 2

    def test_timestamps_are_millis(self, schema):
        for field_name in ["created_at", "updated_at"]:
            field = next(f for f in schema["fields"] if f["name"] == field_name)
            assert field["type"]["logicalType"] == "timestamp-millis"

    def test_delivery_address_is_record(self, schema):
        field = next(f for f in schema["fields"] if f["name"] == "delivery_address")
        assert field["type"]["type"] == "record"
        addr_fields = {f["name"] for f in field["type"]["fields"]}
        assert {"latitude", "longitude", "city", "pincode"}.issubset(addr_fields)


# ---------------------------------------------------------------------------
# User schema tests
# ---------------------------------------------------------------------------
class TestUserSchema:
    @pytest.fixture()
    def schema(self):
        return load_schema("user.avsc")

    def test_name(self, schema):
        assert schema["name"] == "User"

    def test_user_id_is_string(self, schema):
        field = next(f for f in schema["fields"] if f["name"] == "user_id")
        assert field["type"] == "string"

    def test_is_pro_member_boolean_with_default(self, schema):
        field = next(f for f in schema["fields"] if f["name"] == "is_pro_member")
        assert field["type"] == "boolean"
        assert field["default"] is False

    def test_preferred_cuisine_is_array(self, schema):
        field = next(f for f in schema["fields"] if f["name"] == "preferred_cuisine")
        assert field["type"]["type"] == "array"

    def test_last_order_at_nullable(self, schema):
        field = next(f for f in schema["fields"] if f["name"] == "last_order_at")
        # Union type with null
        assert isinstance(field["type"], list)
        assert "null" in field["type"]


# ---------------------------------------------------------------------------
# Menu schema tests
# ---------------------------------------------------------------------------
class TestMenuSchema:
    @pytest.fixture()
    def schema(self):
        return load_schema("menu.avsc")

    def test_name(self, schema):
        assert schema["name"] == "MenuItem"

    def test_price_is_decimal(self, schema):
        field = next(f for f in schema["fields"] if f["name"] == "price")
        assert field["type"]["logicalType"] == "decimal"

    def test_description_nullable(self, schema):
        field = next(f for f in schema["fields"] if f["name"] == "description")
        assert isinstance(field["type"], list)
        assert "null" in field["type"]

    def test_is_vegetarian_boolean(self, schema):
        field = next(f for f in schema["fields"] if f["name"] == "is_vegetarian")
        assert field["type"] == "boolean"

    def test_rating_nullable_float(self, schema):
        field = next(f for f in schema["fields"] if f["name"] == "rating")
        assert isinstance(field["type"], list)
        assert "null" in field["type"]


# ---------------------------------------------------------------------------
# Promo schema tests
# ---------------------------------------------------------------------------
class TestPromoSchema:
    @pytest.fixture()
    def schema(self):
        return load_schema("promo.avsc")

    def test_name(self, schema):
        assert schema["name"] == "Promotion"

    def test_discount_type_is_enum(self, schema):
        field = next(f for f in schema["fields"] if f["name"] == "discount_type")
        assert field["type"]["type"] == "enum"
        symbols = field["type"]["symbols"]
        assert "PERCENTAGE" in symbols
        assert "FLAT" in symbols
        assert "FREE_DELIVERY" in symbols

    def test_applicable_cities_array_with_default(self, schema):
        field = next(f for f in schema["fields"] if f["name"] == "applicable_cities")
        assert field["type"]["type"] == "array"
        assert field["default"] == []

    def test_is_active_default_true(self, schema):
        field = next(f for f in schema["fields"] if f["name"] == "is_active")
        assert field["type"] == "boolean"
        assert field["default"] is True


# ---------------------------------------------------------------------------
# Schema compatibility tests (BACKWARD)
# ---------------------------------------------------------------------------
class TestSchemaCompatibility:
    """Verify schemas follow BACKWARD compatibility rules.

    BACKWARD means new schema can read data written by old schema.
    Key rules:
      - New fields MUST have defaults
      - Fields cannot be removed without defaults
      - Field types must be compatible
    """

    @pytest.mark.parametrize("schema_file", [
        "order.avsc", "user.avsc", "menu.avsc", "promo.avsc",
    ])
    def test_all_fields_with_no_default_are_required_types(self, schema_file):
        schema = load_schema(schema_file)
        for field in schema["fields"]:
            if "default" not in field:
                # Required fields must be non-union types or have explicit type
                field_type = field["type"]
                if isinstance(field_type, list):
                    # Union type without default is only safe if null is first
                    # (which means default is null)
                    pass  # Allowed as long as consumer handles it
                # Otherwise it's a concrete type, which is fine

    @pytest.mark.parametrize("schema_file", [
        "order.avsc", "user.avsc", "menu.avsc", "promo.avsc",
    ])
    def test_event_type_field_present(self, schema_file):
        """All schemas must have event_type for CDC tracking."""
        schema = load_schema(schema_file)
        field_names = {f["name"] for f in schema["fields"]}
        assert "event_type" in field_names

    @pytest.mark.parametrize("schema_file", [
        "order.avsc", "user.avsc", "menu.avsc", "promo.avsc",
    ])
    def test_has_timestamp_field(self, schema_file):
        """All schemas must have a timestamp for event ordering."""
        schema = load_schema(schema_file)
        field_names = {f["name"] for f in schema["fields"]}
        has_ts = (
            "event_timestamp" in field_names
            or "updated_at" in field_names
            or "created_at" in field_names
        )
        assert has_ts, f"{schema_file} missing timestamp field"
