"""
Integration test for Pipeline 3 - DynamoDB Streams to S3 JSON.

Tests the full flow of:
  1. Processing DynamoDB stream records
  2. Writing JSON batches to S3
  3. Verifying S3 object content and structure
  4. Handling multiple tables in a single Lambda invocation

Requires: moto (for S3 mocking) or testcontainers MinIO.
"""

import json
import os
import sys
from datetime import datetime
from unittest.mock import patch

import pytest

sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "pipelines",
        "pipeline3_dynamodb_streams",
        "src",
    ),
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture()
def dynamodb_processor(mock_s3):
    """Create a DynamoDBStreamToS3 processor pointed at mock S3."""
    from dynamodb_stream_processor import DynamoDBStreamToS3

    with patch("dynamodb_stream_processor.boto3") as mock_boto:
        mock_boto.client.return_value = mock_s3
        proc = DynamoDBStreamToS3(s3_bucket="zomato-data-platform-test-raw-data-lake")
        proc.s3_client = mock_s3
        return proc


def make_stream_event(table_name: str, records: list[dict]) -> dict:
    """Build a DynamoDB Streams Lambda event."""
    return {
        "Records": [
            {
                "eventID": f"evt_{i}",
                "eventName": "INSERT",
                "eventVersion": "1.1",
                "eventSource": "aws:dynamodb",
                "awsRegion": "us-east-1",
                "eventSourceARN": f"arn:aws:dynamodb:us-east-1:123456789012:table/{table_name}/stream/2024-01-01",
                "dynamodb": {
                    "ApproximateCreationDateTime": int(datetime.utcnow().timestamp()),
                    "Keys": {list(record.keys())[0]: {"S": str(list(record.values())[0])}},
                    "NewImage": {
                        k: {"S": str(v)} if isinstance(v, str) else {"N": str(v)}
                        for k, v in record.items()
                    },
                    "SequenceNumber": str(1000 + i),
                    "SizeBytes": 256,
                    "StreamViewType": "NEW_AND_OLD_IMAGES",
                },
            }
            for i, record in enumerate(records)
        ]
    }


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------
class TestDynamoDBToS3Integration:
    def test_process_and_write_single_record(self, dynamodb_processor, mock_s3):
        record = {
            "eventID": "evt_1",
            "eventName": "INSERT",
            "eventVersion": "1.1",
            "eventSource": "aws:dynamodb",
            "awsRegion": "us-east-1",
            "eventSourceARN": "arn:aws:dynamodb:us-east-1:123:table/orders/stream/2024",
            "dynamodb": {
                "ApproximateCreationDateTime": 1700000000,
                "NewImage": {
                    "order_id": {"S": "ord_001"},
                    "total_amount": {"N": "750"},
                    "city": {"S": "Mumbai"},
                },
                "SequenceNumber": "111",
                "SizeBytes": 128,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
        }

        processed = dynamodb_processor.process_stream_record(record)
        s3_key = dynamodb_processor.write_batch_to_s3([processed], "orders")

        # Verify the object was written
        resp = mock_s3.get_object(
            Bucket="zomato-data-platform-test-raw-data-lake", Key=s3_key
        )
        body = resp["Body"].read().decode("utf-8")
        data = json.loads(body)

        assert data["table_name"] == "orders"
        assert data["data"]["order_id"] == "ord_001"
        assert data["data"]["total_amount"] == 750
        assert data["data"]["city"] == "Mumbai"

    def test_batch_write_multiple_records(self, dynamodb_processor, mock_s3):
        records = []
        for i in range(10):
            rec = {
                "eventID": f"evt_{i}",
                "eventName": "INSERT",
                "eventSource": "aws:dynamodb",
                "awsRegion": "us-east-1",
                "eventSourceARN": "arn:aws:dynamodb:us-east-1:123:table/orders/stream/2024",
                "dynamodb": {
                    "ApproximateCreationDateTime": 1700000000,
                    "NewImage": {"order_id": {"S": f"ord_{i:03d}"}},
                    "SequenceNumber": str(i),
                    "SizeBytes": 64,
                    "StreamViewType": "NEW_AND_OLD_IMAGES",
                },
            }
            records.append(dynamodb_processor.process_stream_record(rec))

        s3_key = dynamodb_processor.write_batch_to_s3(records, "orders")

        resp = mock_s3.get_object(
            Bucket="zomato-data-platform-test-raw-data-lake", Key=s3_key
        )
        body = resp["Body"].read().decode("utf-8")
        lines = body.strip().split("\n")
        assert len(lines) == 10

        # Verify each line is valid JSON
        for line in lines:
            parsed = json.loads(line)
            assert "event_id" in parsed
            assert "data" in parsed

    def test_s3_key_partitioning(self, dynamodb_processor, mock_s3):
        record = {
            "eventID": "evt_1",
            "eventName": "INSERT",
            "eventSource": "aws:dynamodb",
            "awsRegion": "us-east-1",
            "eventSourceARN": "arn:aws:dynamodb:us-east-1:123:table/payments/stream/2024",
            "dynamodb": {
                "ApproximateCreationDateTime": 1700000000,
                "NewImage": {"payment_id": {"S": "pay_001"}},
                "SequenceNumber": "1",
                "SizeBytes": 64,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
        }
        processed = dynamodb_processor.process_stream_record(record)
        s3_key = dynamodb_processor.write_batch_to_s3([processed], "payments")

        assert "pipeline3-dynamodb/json-raw/" in s3_key
        assert "payments/" in s3_key
        # Verify YYYY/MM/DD/HH partitioning
        now = datetime.utcnow()
        expected_prefix = now.strftime("%Y/%m/%d/%H")
        assert expected_prefix in s3_key

    def test_lambda_handler_multi_table(self, mock_s3):
        """Test the full Lambda handler with records from multiple tables."""
        from dynamodb_stream_processor import lambda_handler

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
                        "NewImage": {"order_id": {"S": "ord_001"}},
                        "SequenceNumber": "1",
                        "SizeBytes": 64,
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
                        "NewImage": {"payment_id": {"S": "pay_001"}},
                        "SequenceNumber": "2",
                        "SizeBytes": 64,
                        "StreamViewType": "NEW_AND_OLD_IMAGES",
                    },
                },
            ]
        }

        with patch.dict(os.environ, {"S3_BUCKET": "zomato-data-platform-test-raw-data-lake"}):
            with patch("dynamodb_stream_processor.boto3") as mock_boto:
                mock_boto.client.return_value = mock_s3
                result = lambda_handler(event, None)

        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["records_processed"] == 2
        assert set(body["tables"]) == {"orders", "payments"}
        assert len(body["s3_keys"]) == 2
