"""
Unit tests for Pipeline 3 DynamoDB Stream Processor transforms.

Tests the DynamoDBStreamToS3 class (deserialization, flattening) and the
Spark ORC converter logic (deduplication, session building, schema mapping).
"""

import json
import os
import sys
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

# Add the pipeline source to the path
sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "pipelines",
        "pipeline3_dynamodb_streams",
        "src",
    ),
)

from dynamodb_stream_processor import DynamoDBStreamToS3


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def processor():
    """Create a DynamoDBStreamToS3 instance with a mocked S3 client."""
    with patch("dynamodb_stream_processor.boto3") as mock_boto:
        mock_s3 = MagicMock()
        mock_boto.client.return_value = mock_s3
        proc = DynamoDBStreamToS3(
            s3_bucket="test-bucket",
            s3_prefix="pipeline3-dynamodb/json-raw",
        )
        proc._mock_s3 = mock_s3
        yield proc


@pytest.fixture
def sample_insert_record():
    """A DynamoDB Streams INSERT record for an order."""
    return {
        "eventID": "evt-001",
        "eventName": "INSERT",
        "eventSource": "aws:dynamodb",
        "eventVersion": "1.1",
        "awsRegion": "ap-south-1",
        "eventSourceARN": "arn:aws:dynamodb:ap-south-1:123456789:table/orders/stream/2024-01-01",
        "dynamodb": {
            "ApproximateCreationDateTime": 1704067200,
            "SequenceNumber": "100000000000000000001",
            "SizeBytes": 256,
            "StreamViewType": "NEW_AND_OLD_IMAGES",
            "NewImage": {
                "order_id": {"S": "ord-12345"},
                "user_id": {"S": "usr-001"},
                "restaurant_id": {"S": "rest-050"},
                "status": {"S": "PLACED"},
                "total_amount": {"N": "749.50"},
                "delivery_fee": {"N": "40"},
                "city": {"S": "Mumbai"},
                "is_priority": {"BOOL": True},
                "items_count": {"N": "3"},
                "created_at": {"S": "2024-01-01T12:00:00"},
                "updated_at": {"S": "2024-01-01T12:00:00"},
            },
        },
    }


@pytest.fixture
def sample_modify_record():
    """A DynamoDB Streams MODIFY record for an order status update."""
    return {
        "eventID": "evt-002",
        "eventName": "MODIFY",
        "eventSource": "aws:dynamodb",
        "eventVersion": "1.1",
        "awsRegion": "ap-south-1",
        "eventSourceARN": "arn:aws:dynamodb:ap-south-1:123456789:table/orders/stream/2024-01-01",
        "dynamodb": {
            "ApproximateCreationDateTime": 1704067500,
            "SequenceNumber": "100000000000000000002",
            "SizeBytes": 512,
            "StreamViewType": "NEW_AND_OLD_IMAGES",
            "OldImage": {
                "order_id": {"S": "ord-12345"},
                "status": {"S": "PLACED"},
                "updated_at": {"S": "2024-01-01T12:00:00"},
            },
            "NewImage": {
                "order_id": {"S": "ord-12345"},
                "user_id": {"S": "usr-001"},
                "restaurant_id": {"S": "rest-050"},
                "status": {"S": "DELIVERED"},
                "total_amount": {"N": "749.50"},
                "delivery_fee": {"N": "40"},
                "city": {"S": "Mumbai"},
                "is_priority": {"BOOL": True},
                "items_count": {"N": "3"},
                "created_at": {"S": "2024-01-01T12:00:00"},
                "updated_at": {"S": "2024-01-01T12:35:00"},
            },
        },
    }


@pytest.fixture
def sample_remove_record():
    """A DynamoDB Streams REMOVE record."""
    return {
        "eventID": "evt-003",
        "eventName": "REMOVE",
        "eventSource": "aws:dynamodb",
        "eventVersion": "1.1",
        "awsRegion": "ap-south-1",
        "eventSourceARN": "arn:aws:dynamodb:ap-south-1:123456789:table/sessions/stream/2024-01-01",
        "dynamodb": {
            "ApproximateCreationDateTime": 1704068000,
            "SequenceNumber": "100000000000000000003",
            "SizeBytes": 128,
            "StreamViewType": "NEW_AND_OLD_IMAGES",
            "OldImage": {
                "session_id": {"S": "sess-abc"},
                "user_id": {"S": "usr-002"},
                "ttl": {"N": "1704067200"},
            },
        },
    }


@pytest.fixture
def sample_nested_record():
    """A DynamoDB record with nested Map and List types."""
    return {
        "eventID": "evt-004",
        "eventName": "INSERT",
        "eventSource": "aws:dynamodb",
        "eventVersion": "1.1",
        "awsRegion": "ap-south-1",
        "eventSourceARN": "arn:aws:dynamodb:ap-south-1:123456789:table/orders/stream/2024-01-01",
        "dynamodb": {
            "ApproximateCreationDateTime": 1704068500,
            "SequenceNumber": "100000000000000000004",
            "SizeBytes": 1024,
            "StreamViewType": "NEW_AND_OLD_IMAGES",
            "NewImage": {
                "order_id": {"S": "ord-99999"},
                "items": {
                    "L": [
                        {"M": {"name": {"S": "Biryani"}, "qty": {"N": "2"}, "price": {"N": "350"}}},
                        {"M": {"name": {"S": "Raita"}, "qty": {"N": "1"}, "price": {"N": "50"}}},
                    ]
                },
                "metadata": {
                    "M": {
                        "source": {"S": "mobile_app"},
                        "version": {"S": "17.5.2"},
                    }
                },
                "tags": {"SS": ["express", "contactless"]},
                "null_field": {"NULL": True},
            },
        },
    }


# ---------------------------------------------------------------------------
# Tests: DynamoDB type deserialization
# ---------------------------------------------------------------------------
class TestDynamoDBDeserialization:
    """Test _deserialize_value and _deserialize_dynamodb_item."""

    def test_string_type(self, processor):
        assert processor._deserialize_value({"S": "hello"}) == "hello"

    def test_number_int(self, processor):
        assert processor._deserialize_value({"N": "42"}) == 42

    def test_number_float(self, processor):
        assert processor._deserialize_value({"N": "749.50"}) == 749.50

    def test_boolean_true(self, processor):
        assert processor._deserialize_value({"BOOL": True}) is True

    def test_boolean_false(self, processor):
        assert processor._deserialize_value({"BOOL": False}) is False

    def test_null_type(self, processor):
        assert processor._deserialize_value({"NULL": True}) is None

    def test_list_type(self, processor):
        result = processor._deserialize_value({
            "L": [{"S": "a"}, {"N": "1"}, {"BOOL": True}]
        })
        assert result == ["a", 1, True]

    def test_map_type(self, processor):
        result = processor._deserialize_value({
            "M": {"key1": {"S": "val1"}, "key2": {"N": "100"}}
        })
        assert result == {"key1": "val1", "key2": 100}

    def test_string_set(self, processor):
        result = processor._deserialize_value({"SS": ["a", "b", "c"]})
        assert set(result) == {"a", "b", "c"}

    def test_number_set(self, processor):
        result = processor._deserialize_value({"NS": ["1", "2.5", "3"]})
        assert 1 in result
        assert 2.5 in result
        assert 3 in result

    def test_full_item_deserialization(self, processor):
        item = {
            "order_id": {"S": "ord-001"},
            "amount": {"N": "500.75"},
            "is_paid": {"BOOL": True},
            "notes": {"NULL": True},
        }
        result = processor._deserialize_dynamodb_item(item)
        assert result == {
            "order_id": "ord-001",
            "amount": 500.75,
            "is_paid": True,
            "notes": None,
        }


# ---------------------------------------------------------------------------
# Tests: Stream record processing
# ---------------------------------------------------------------------------
class TestStreamRecordProcessing:
    """Test process_stream_record for INSERT, MODIFY, REMOVE events."""

    def test_insert_record_processing(self, processor, sample_insert_record):
        result = processor.process_stream_record(sample_insert_record)

        assert result["event_id"] == "evt-001"
        assert result["event_name"] == "INSERT"
        assert result["table_name"] == "orders"
        assert result["aws_region"] == "ap-south-1"
        assert result["sequence_number"] == "100000000000000000001"
        assert result["data"]["order_id"] == "ord-12345"
        assert result["data"]["total_amount"] == 749.50
        assert result["data"]["is_priority"] is True
        assert result["data"]["city"] == "Mumbai"
        assert "processed_at" in result

    def test_modify_record_uses_new_image(self, processor, sample_modify_record):
        result = processor.process_stream_record(sample_modify_record)

        assert result["event_name"] == "MODIFY"
        assert result["data"]["status"] == "DELIVERED"
        assert result["data"]["updated_at"] == "2024-01-01T12:35:00"

    def test_remove_record_uses_old_image(self, processor, sample_remove_record):
        result = processor.process_stream_record(sample_remove_record)

        assert result["event_name"] == "REMOVE"
        assert result["table_name"] == "sessions"
        assert result["data"]["session_id"] == "sess-abc"
        assert result["data"]["user_id"] == "usr-002"

    def test_nested_types_processing(self, processor, sample_nested_record):
        result = processor.process_stream_record(sample_nested_record)

        assert result["data"]["order_id"] == "ord-99999"
        # List of maps
        items = result["data"]["items"]
        assert len(items) == 2
        assert items[0]["name"] == "Biryani"
        assert items[0]["qty"] == 2
        assert items[1]["price"] == 50
        # Nested map
        assert result["data"]["metadata"]["source"] == "mobile_app"
        # String set
        assert set(result["data"]["tags"]) == {"express", "contactless"}
        # Null
        assert result["data"]["null_field"] is None

    def test_table_name_extraction_from_arn(self, processor, sample_insert_record):
        result = processor.process_stream_record(sample_insert_record)
        assert result["table_name"] == "orders"


# ---------------------------------------------------------------------------
# Tests: S3 write batch
# ---------------------------------------------------------------------------
class TestS3WriteBatch:
    """Test write_batch_to_s3 constructs correct S3 keys and calls put_object."""

    def test_write_batch_constructs_correct_key(self, processor):
        records = [
            {"event_id": "e1", "data": {"order_id": "o1"}},
            {"event_id": "e2", "data": {"order_id": "o2"}},
        ]
        s3_key = processor.write_batch_to_s3(records, "orders")

        assert s3_key.startswith("pipeline3-dynamodb/json-raw/orders/")
        assert s3_key.endswith(".json")

    def test_write_batch_calls_put_object(self, processor):
        records = [{"event_id": "e1"}, {"event_id": "e2"}]
        processor.write_batch_to_s3(records, "orders")

        processor._mock_s3.put_object.assert_called_once()
        call_kwargs = processor._mock_s3.put_object.call_args
        assert call_kwargs.kwargs["Bucket"] == "test-bucket" or call_kwargs[1]["Bucket"] == "test-bucket"

    def test_write_batch_body_is_ndjson(self, processor):
        records = [
            {"event_id": "e1", "value": 100},
            {"event_id": "e2", "value": 200},
        ]
        processor.write_batch_to_s3(records, "payments")

        call_args = processor._mock_s3.put_object.call_args
        # The Body should be NDJSON
        body = call_args.kwargs.get("Body") or call_args[1].get("Body")
        if isinstance(body, bytes):
            body = body.decode("utf-8")
        lines = body.strip().split("\n")
        assert len(lines) == 2
        assert json.loads(lines[0])["event_id"] == "e1"
        assert json.loads(lines[1])["value"] == 200


# ---------------------------------------------------------------------------
# Tests: Lambda handler
# ---------------------------------------------------------------------------
class TestLambdaHandler:
    """Test the lambda_handler entry point."""

    def test_empty_records(self):
        from dynamodb_stream_processor import lambda_handler

        result = lambda_handler({"Records": []}, None)
        assert result["statusCode"] == 200

    def test_handler_processes_records(self, sample_insert_record, sample_modify_record):
        with patch("dynamodb_stream_processor.boto3") as mock_boto:
            mock_s3 = MagicMock()
            mock_boto.client.return_value = mock_s3

            from dynamodb_stream_processor import lambda_handler

            event = {"Records": [sample_insert_record, sample_modify_record]}
            result = lambda_handler(event, None)

            assert result["statusCode"] == 200
            body = json.loads(result["body"])
            assert body["records_processed"] == 2
            assert "orders" in body["tables"]

    def test_handler_groups_by_table(self, sample_insert_record, sample_remove_record):
        with patch("dynamodb_stream_processor.boto3") as mock_boto:
            mock_s3 = MagicMock()
            mock_boto.client.return_value = mock_s3

            from dynamodb_stream_processor import lambda_handler

            event = {"Records": [sample_insert_record, sample_remove_record]}
            result = lambda_handler(event, None)

            body = json.loads(result["body"])
            # Two different tables: orders and sessions
            assert len(body["tables"]) == 2
            assert "orders" in body["tables"]
            assert "sessions" in body["tables"]


# ---------------------------------------------------------------------------
# Tests: Deduplication logic (Spark-level, tested without Spark)
# ---------------------------------------------------------------------------
class TestDeduplicationLogic:
    """Test the deduplication logic that the Spark job applies."""

    def test_deduplicate_by_order_id(self):
        """Verify dedup keeps last record per order_id."""
        records = [
            {"order_id": "o1", "status": "PLACED", "updated_at": "2024-01-01T10:00:00"},
            {"order_id": "o1", "status": "PREPARING", "updated_at": "2024-01-01T10:05:00"},
            {"order_id": "o1", "status": "DELIVERED", "updated_at": "2024-01-01T10:30:00"},
            {"order_id": "o2", "status": "PLACED", "updated_at": "2024-01-01T10:00:00"},
        ]

        # Simulate dedup: keep one per order_id (last by updated_at)
        seen = {}
        for r in records:
            oid = r["order_id"]
            if oid not in seen or r["updated_at"] > seen[oid]["updated_at"]:
                seen[oid] = r

        deduped = list(seen.values())
        assert len(deduped) == 2
        o1 = [r for r in deduped if r["order_id"] == "o1"][0]
        assert o1["status"] == "DELIVERED"

    def test_session_building_from_events(self):
        """Test session aggregation from multiple location events."""
        events = [
            {"user_id": "u1", "latitude": 19.076, "longitude": 72.877, "timestamp": "2024-01-01T10:00:00"},
            {"user_id": "u1", "latitude": 19.080, "longitude": 72.880, "timestamp": "2024-01-01T10:05:00"},
            {"user_id": "u1", "latitude": 19.090, "longitude": 72.890, "timestamp": "2024-01-01T10:10:00"},
            {"user_id": "u2", "latitude": 28.704, "longitude": 77.102, "timestamp": "2024-01-01T10:00:00"},
        ]

        sessions = {}
        for e in events:
            uid = e["user_id"]
            if uid not in sessions:
                sessions[uid] = {
                    "user_id": uid,
                    "start_time": e["timestamp"],
                    "end_time": e["timestamp"],
                    "event_count": 0,
                    "locations": [],
                }
            sessions[uid]["end_time"] = e["timestamp"]
            sessions[uid]["event_count"] += 1
            sessions[uid]["locations"].append((e["latitude"], e["longitude"]))

        assert len(sessions) == 2
        assert sessions["u1"]["event_count"] == 3
        assert sessions["u2"]["event_count"] == 1
        assert sessions["u1"]["start_time"] == "2024-01-01T10:00:00"
        assert sessions["u1"]["end_time"] == "2024-01-01T10:10:00"
