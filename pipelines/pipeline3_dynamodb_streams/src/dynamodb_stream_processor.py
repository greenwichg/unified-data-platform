"""
Pipeline 3 - DynamoDB Streams → S3 JSON → Spark (EMR) → ORC → S3

Processes DynamoDB Streams events via Lambda/Kinesis, stages as JSON in S3,
then runs Spark jobs on EMR to convert to ORC format for the data lake.

ECS Multi-AZ deployment for high availability.
"""

import json
import logging
import os
from datetime import datetime

import boto3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("pipeline3_dynamodb_streams")


class DynamoDBStreamToS3:
    """Processes DynamoDB Stream records and writes to S3 as JSON."""

    def __init__(self, s3_bucket: str, s3_prefix: str = "pipeline3-dynamodb/json-raw"):
        self.s3_client = boto3.client("s3")
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix

    def process_stream_record(self, record: dict) -> dict:
        """Transform a DynamoDB stream record into a flat JSON structure."""
        event_name = record.get("eventName", "UNKNOWN")
        dynamodb_record = record.get("dynamodb", {})

        # Extract the new image (or old image for DELETE)
        image = dynamodb_record.get("NewImage", dynamodb_record.get("OldImage", {}))

        # Deserialize DynamoDB types to plain values
        flat_record = {
            "event_id": record.get("eventID"),
            "event_name": event_name,
            "event_source": record.get("eventSource"),
            "event_version": record.get("eventVersion"),
            "aws_region": record.get("awsRegion"),
            "table_name": record.get("eventSourceARN", "").split("/")[1]
            if "/" in record.get("eventSourceARN", "")
            else "unknown",
            "approximate_creation_time": dynamodb_record.get(
                "ApproximateCreationDateTime"
            ),
            "sequence_number": dynamodb_record.get("SequenceNumber"),
            "size_bytes": dynamodb_record.get("SizeBytes"),
            "stream_view_type": dynamodb_record.get("StreamViewType"),
            "processed_at": datetime.utcnow().isoformat(),
        }

        # Flatten DynamoDB record attributes
        flat_record["data"] = self._deserialize_dynamodb_item(image)

        return flat_record

    def _deserialize_dynamodb_item(self, item: dict) -> dict:
        """Convert DynamoDB typed attributes to plain Python types."""
        result = {}
        for key, value in item.items():
            result[key] = self._deserialize_value(value)
        return result

    def _deserialize_value(self, value: dict) -> object:
        """Deserialize a single DynamoDB attribute value."""
        if "S" in value:
            return value["S"]
        if "N" in value:
            num = value["N"]
            return int(num) if "." not in num else float(num)
        if "BOOL" in value:
            return value["BOOL"]
        if "NULL" in value:
            return None
        if "L" in value:
            return [self._deserialize_value(item) for item in value["L"]]
        if "M" in value:
            return self._deserialize_dynamodb_item(value["M"])
        if "SS" in value:
            return list(value["SS"])
        if "NS" in value:
            return [float(n) if "." in n else int(n) for n in value["NS"]]
        if "B" in value:
            return value["B"]
        return str(value)

    def write_batch_to_s3(self, records: list[dict], table_name: str) -> str:
        """Write a batch of processed records to S3 as newline-delimited JSON."""
        now = datetime.utcnow()
        partition = now.strftime("%Y/%m/%d/%H")
        timestamp = now.strftime("%Y%m%d_%H%M%S_%f")
        s3_key = f"{self.s3_prefix}/{table_name}/{partition}/{timestamp}.json"

        body = "\n".join(json.dumps(record) for record in records)

        self.s3_client.put_object(
            Bucket=self.s3_bucket,
            Key=s3_key,
            Body=body.encode("utf-8"),
            ContentType="application/json",
        )

        logger.info(
            "Wrote %d records to s3://%s/%s", len(records), self.s3_bucket, s3_key
        )
        return s3_key


def lambda_handler(event: dict, context: object) -> dict:
    """AWS Lambda handler for DynamoDB Streams events.

    Triggered by DynamoDB Streams, processes records, and writes to S3.
    """
    s3_bucket = os.environ.get("S3_BUCKET", "zomato-data-platform-dev-raw-data-lake")
    processor = DynamoDBStreamToS3(s3_bucket)

    records = event.get("Records", [])
    if not records:
        return {"statusCode": 200, "body": "No records to process"}

    # Group records by source table
    table_records: dict[str, list[dict]] = {}
    for record in records:
        processed = processor.process_stream_record(record)
        table_name = processed["table_name"]
        table_records.setdefault(table_name, []).append(processed)

    # Write each table's records to S3
    s3_keys = []
    for table_name, table_recs in table_records.items():
        s3_key = processor.write_batch_to_s3(table_recs, table_name)
        s3_keys.append(s3_key)

    logger.info("Processed %d records from %d tables", len(records), len(table_records))

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "records_processed": len(records),
                "tables": list(table_records.keys()),
                "s3_keys": s3_keys,
            }
        ),
    }
