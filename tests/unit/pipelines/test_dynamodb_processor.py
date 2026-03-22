"""
Unit tests for Pipeline 3 - DynamoDB Streams Processor.

Tests cover:
  - DynamoDB type deserialization (S, N, BOOL, NULL, L, M, SS, NS, B)
  - Stream record parsing and flattening
  - Table name extraction from ARN
  - Deduplication logic in the Lambda handler
  - Session/batch building from grouped records
  - S3 key generation with time-based partitioning
  - Lambda handler integration
"""

import json
import os
import sys
import uuid
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "platform",
        "pipelines",
        "pipeline3_dynamodb_streams",
        "src",
    ),
)

from dynamodb_stream_processor import DynamoDBStreamToS3, lambda_handler


# ---------------------------------------------------------------------------
# DynamoDB type deserialization tests
# ---------------------------------------------------------------------------
class TestDeserializeDynamoDBTypes:
    @pytest.fixture()
    def processor(self):
        with patch("dynamodb_stream_processor.boto3"):
            return DynamoDBStreamToS3(s3_bucket="test-bucket")

    def test_string_type(self, processor):
        assert processor._deserialize_value({"S": "hello"}) == "hello"

    def test_integer_type(self, processor):
        assert processor._deserialize_value({"N": "42"}) == 42

    def test_float_type(self, processor):
        assert processor._deserialize_value({"N": "3.14"}) == 3.14

    def test_boolean_true(self, processor):
        assert processor._deserialize_value({"BOOL": True}) is True

    def test_boolean_false(self, processor):
        assert processor._deserialize_value({"BOOL": False}) is False

    def test_null_type(self, processor):
        assert processor._deserialize_value({"NULL": True}) is None

    def test_list_type(self, processor):
        result = processor._deserialize_value(
            {"L": [{"S": "a"}, {"N": "1"}, {"BOOL": True}]}
        )
        assert result == ["a", 1, True]

    def test_map_type(self, processor):
        result = processor._deserialize_value(
            {"M": {"name": {"S": "test"}, "count": {"N": "5"}}}
        )
        assert result == {"name": "test", "count": 5}

    def test_string_set_type(self, processor):
        result = processor._deserialize_value({"SS": ["a", "b", "c"]})
        assert set(result) == {"a", "b", "c"}

    def test_number_set_type_integers(self, processor):
        result = processor._deserialize_value({"NS": ["1", "2", "3"]})
        assert result == [1, 2, 3]

    def test_number_set_type_floats(self, processor):
        result = processor._deserialize_value({"NS": ["1.1", "2.2"]})
        assert result == [1.1, 2.2]

    def test_binary_type(self, processor):
        result = processor._deserialize_value({"B": "dGVzdA=="})
        assert result == "dGVzdA=="

    def test_nested_map_in_list(self, processor):
        result = processor._deserialize_value(
            {"L": [{"M": {"key": {"S": "value"}}}]}
        )
        assert result == [{"key": "value"}]

    def test_unknown_type_returns_string(self, processor):
        result = processor._deserialize_value({"UNKNOWN_TYPE": "something"})
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# Item deserialization tests
# ---------------------------------------------------------------------------
class TestDeserializeItem:
    @pytest.fixture()
    def processor(self):
        with patch("dynamodb_stream_processor.boto3"):
            return DynamoDBStreamToS3(s3_bucket="test-bucket")

    def test_full_item(self, processor):
        item = {
            "order_id": {"S": "ord_123"},
            "total_amount": {"N": "750.50"},
            "is_delivered": {"BOOL": True},
            "items": {"L": [{"S": "biryani"}, {"S": "raita"}]},
        }
        result = processor._deserialize_dynamodb_item(item)
        assert result == {
            "order_id": "ord_123",
            "total_amount": 750.50,
            "is_delivered": True,
            "items": ["biryani", "raita"],
        }

    def test_empty_item(self, processor):
        result = processor._deserialize_dynamodb_item({})
        assert result == {}


# ---------------------------------------------------------------------------
# Stream record processing tests
# ---------------------------------------------------------------------------
class TestProcessStreamRecord:
    @pytest.fixture()
    def processor(self):
        with patch("dynamodb_stream_processor.boto3"):
            return DynamoDBStreamToS3(s3_bucket="test-bucket")

    def _make_record(self, table="orders", event_name="INSERT", data=None):
        if data is None:
            data = {"order_id": {"S": "ord_test"}, "status": {"S": "PLACED"}}
        return {
            "eventID": str(uuid.uuid4()),
            "eventName": event_name,
            "eventVersion": "1.1",
            "eventSource": "aws:dynamodb",
            "awsRegion": "us-east-1",
            "eventSourceARN": f"arn:aws:dynamodb:us-east-1:123456789012:table/{table}/stream/2024-01-01",
            "dynamodb": {
                "ApproximateCreationDateTime": 1700000000,
                "Keys": {"order_id": {"S": "ord_test"}},
                "NewImage": data,
                "SequenceNumber": "111",
                "SizeBytes": 256,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
        }

    def test_table_name_extraction(self, processor):
        record = self._make_record(table="orders")
        result = processor.process_stream_record(record)
        assert result["table_name"] == "orders"

    def test_event_name_preserved(self, processor):
        record = self._make_record(event_name="MODIFY")
        result = processor.process_stream_record(record)
        assert result["event_name"] == "MODIFY"

    def test_event_id_present(self, processor):
        record = self._make_record()
        result = processor.process_stream_record(record)
        assert result["event_id"] is not None

    def test_sequence_number(self, processor):
        record = self._make_record()
        result = processor.process_stream_record(record)
        assert result["sequence_number"] == "111"

    def test_data_deserialized(self, processor):
        data = {"order_id": {"S": "ord_abc"}, "total": {"N": "500"}}
        record = self._make_record(data=data)
        result = processor.process_stream_record(record)
        assert result["data"]["order_id"] == "ord_abc"
        assert result["data"]["total"] == 500

    def test_processed_at_timestamp(self, processor):
        record = self._make_record()
        result = processor.process_stream_record(record)
        assert "processed_at" in result
        # Should be a valid ISO timestamp
        datetime.fromisoformat(result["processed_at"])

    def test_delete_event_uses_old_image(self, processor):
        record = self._make_record(event_name="REMOVE")
        del record["dynamodb"]["NewImage"]
        record["dynamodb"]["OldImage"] = {"order_id": {"S": "ord_deleted"}}
        result = processor.process_stream_record(record)
        assert result["data"]["order_id"] == "ord_deleted"

    def test_region_captured(self, processor):
        record = self._make_record()
        result = processor.process_stream_record(record)
        assert result["aws_region"] == "us-east-1"

    def test_table_name_unknown_if_no_arn(self, processor):
        record = self._make_record()
        record["eventSourceARN"] = ""
        result = processor.process_stream_record(record)
        assert result["table_name"] == "unknown"


# ---------------------------------------------------------------------------
# S3 write tests
# ---------------------------------------------------------------------------
class TestWriteBatchToS3:
    @pytest.fixture()
    def processor(self):
        with patch("dynamodb_stream_processor.boto3") as mock_boto:
            proc = DynamoDBStreamToS3(s3_bucket="test-bucket")
            proc.s3_client = MagicMock()
            return proc

    def test_s3_key_contains_table_name(self, processor):
        records = [{"event_id": "1", "data": {}}]
        key = processor.write_batch_to_s3(records, "orders")
        assert "orders" in key

    def test_s3_key_contains_partition_path(self, processor):
        records = [{"event_id": "1", "data": {}}]
        key = processor.write_batch_to_s3(records, "orders")
        # Should contain YYYY/MM/DD/HH pattern
        parts = key.split("orders/")[1].split("/")
        assert len(parts) >= 4

    def test_s3_key_uses_configured_prefix(self, processor):
        records = [{"event_id": "1", "data": {}}]
        key = processor.write_batch_to_s3(records, "orders")
        assert key.startswith("pipeline3-dynamodb/json-raw/")

    def test_put_object_called(self, processor):
        records = [{"event_id": "1", "data": {"k": "v"}}]
        processor.write_batch_to_s3(records, "orders")
        processor.s3_client.put_object.assert_called_once()

    def test_body_is_ndjson(self, processor):
        records = [{"id": "1"}, {"id": "2"}]
        processor.write_batch_to_s3(records, "orders")

        call_kwargs = processor.s3_client.put_object.call_args[1]
        body = call_kwargs["Body"].decode("utf-8")
        lines = body.strip().split("\n")
        assert len(lines) == 2
        for line in lines:
            json.loads(line)  # each line is valid JSON

    def test_content_type_json(self, processor):
        records = [{"id": "1"}]
        processor.write_batch_to_s3(records, "orders")
        call_kwargs = processor.s3_client.put_object.call_args[1]
        assert call_kwargs["ContentType"] == "application/json"


# ---------------------------------------------------------------------------
# Lambda handler tests
# ---------------------------------------------------------------------------
class TestLambdaHandler:
    @patch("dynamodb_stream_processor.boto3")
    def test_empty_event(self, mock_boto):
        result = lambda_handler({"Records": []}, None)
        assert result["statusCode"] == 200
        assert "No records" in result["body"]

    @patch("dynamodb_stream_processor.boto3")
    def test_no_records_key(self, mock_boto):
        result = lambda_handler({}, None)
        assert result["statusCode"] == 200

    @patch("dynamodb_stream_processor.DynamoDBStreamToS3.write_batch_to_s3")
    @patch("dynamodb_stream_processor.boto3")
    def test_records_grouped_by_table(self, mock_boto, mock_write):
        mock_write.return_value = "test/key.json"
        event = {
            "Records": [
                {
                    "eventID": "1",
                    "eventName": "INSERT",
                    "eventSource": "aws:dynamodb",
                    "awsRegion": "us-east-1",
                    "eventSourceARN": "arn:aws:dynamodb:us-east-1:123:table/orders/stream/2024",
                    "dynamodb": {
                        "ApproximateCreationDateTime": 1700000000,
                        "NewImage": {"order_id": {"S": "ord_1"}},
                        "SequenceNumber": "1",
                        "SizeBytes": 100,
                        "StreamViewType": "NEW_AND_OLD_IMAGES",
                    },
                },
                {
                    "eventID": "2",
                    "eventName": "INSERT",
                    "eventSource": "aws:dynamodb",
                    "awsRegion": "us-east-1",
                    "eventSourceARN": "arn:aws:dynamodb:us-east-1:123:table/payments/stream/2024",
                    "dynamodb": {
                        "ApproximateCreationDateTime": 1700000000,
                        "NewImage": {"payment_id": {"S": "pay_1"}},
                        "SequenceNumber": "2",
                        "SizeBytes": 100,
                        "StreamViewType": "NEW_AND_OLD_IMAGES",
                    },
                },
            ]
        }

        result = lambda_handler(event, None)
        body = json.loads(result["body"])

        assert body["records_processed"] == 2
        assert set(body["tables"]) == {"orders", "payments"}
        assert mock_write.call_count == 2

    @patch("dynamodb_stream_processor.DynamoDBStreamToS3.write_batch_to_s3")
    @patch("dynamodb_stream_processor.boto3")
    def test_multiple_records_same_table_batched(self, mock_boto, mock_write):
        mock_write.return_value = "test/key.json"
        records = []
        for i in range(5):
            records.append({
                "eventID": str(i),
                "eventName": "INSERT",
                "eventSource": "aws:dynamodb",
                "awsRegion": "us-east-1",
                "eventSourceARN": "arn:aws:dynamodb:us-east-1:123:table/orders/stream/2024",
                "dynamodb": {
                    "ApproximateCreationDateTime": 1700000000,
                    "NewImage": {"order_id": {"S": f"ord_{i}"}},
                    "SequenceNumber": str(i),
                    "SizeBytes": 100,
                    "StreamViewType": "NEW_AND_OLD_IMAGES",
                },
            })

        result = lambda_handler({"Records": records}, None)
        body = json.loads(result["body"])
        assert body["records_processed"] == 5
        # All 5 records in a single table -> single write call
        assert mock_write.call_count == 1
