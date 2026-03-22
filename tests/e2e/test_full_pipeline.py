"""
End-to-end test orchestrating all 4 pipelines of the Zomato data platform.

Validates the complete data flow:
  Pipeline 1: Aurora MySQL -> Spark JDBC -> Iceberg/ORC -> S3 - batch ETL
  Pipeline 2: Aurora MySQL -> Debezium -> Kafka -> Flink -> Iceberg/S3 - CDC
  Pipeline 3: DynamoDB -> Streams -> S3 JSON -> Spark (EMR) -> ORC - streams
  Pipeline 4: App Events -> Kafka (2 MSK clusters) -> Flink -> S3 + Druid - realtime

This test uses mocks for external services (Spark, Flink, Druid)
but exercises the full Python orchestration logic for each pipeline.
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Add all pipeline source directories
REPO_ROOT = Path(__file__).resolve().parents[2]
for pipeline_src in [
    "platform/pipelines/pipeline1_batch_etl/src",
    "platform/pipelines/pipeline2_cdc/src",
    "platform/pipelines/pipeline3_dynamodb_streams/src",
    "platform/pipelines/pipeline4_realtime_events/src",
]:
    sys.path.insert(0, str(REPO_ROOT / pipeline_src))


# ---------------------------------------------------------------------------
# Pipeline 1: Batch ETL end-to-end
# ---------------------------------------------------------------------------
class TestPipeline1E2E:
    """Full batch ETL flow: config -> Spark JDBC read -> quality checks -> Iceberg/ORC write."""

    @patch("batch_etl.SparkSession")
    def test_full_batch_etl_flow(self, mock_spark_cls, tmp_path):
        from batch_etl import TableConfig, get_last_imported_value, run_batch_etl

        # Mock Spark session and DataFrame
        mock_spark = MagicMock()
        mock_spark_cls.builder.appName.return_value = mock_spark_cls.builder
        mock_spark_cls.builder.config.return_value = mock_spark_cls.builder
        mock_spark_cls.builder.enableHiveSupport.return_value = mock_spark_cls.builder
        mock_spark_cls.builder.getOrCreate.return_value = mock_spark

        mock_df = MagicMock()
        mock_df.count.return_value = 100
        mock_df.head.return_value = [MagicMock()]
        mock_df.filter.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.distinct.return_value = mock_df
        mock_df.withColumn.return_value = mock_df
        mock_df.columns = ["order_id", "user_id", "created_at", "updated_at"]
        mock_spark.read.jdbc.return_value = mock_df
        mock_spark.createDataFrame.return_value = mock_df

        # Mock bounds query
        mock_bounds_row = MagicMock()
        mock_bounds_row.__getitem__ = lambda self, key: 1 if key == "min_val" else 1000
        mock_df.collect.return_value = [mock_bounds_row]

        state_file = str(tmp_path / "state.json")

        tables = [
            TableConfig(table="orders", partition_column="order_id", primary_key="order_id",
                        null_check_columns=["order_id"]),
            TableConfig(table="users", partition_column="user_id", primary_key="user_id",
                        null_check_columns=["user_id"]),
        ]

        results = run_batch_etl(
            jdbc_url="jdbc:mysql://aurora:3306/zomato",
            s3_bucket="zomato-e2e-test-bucket",
            state_file=state_file,
            tables=tables,
        )

        # All tables should succeed
        assert "orders" in results["success"]
        assert "users" in results["success"]
        assert results["failed"] == []

        # State should be saved for all tables
        for table in ["orders", "users"]:
            val = get_last_imported_value(table, state_file)
            assert val is not None

    def test_incremental_import_uses_saved_state(self, tmp_path):
        from batch_etl import get_last_imported_value, save_import_state

        state_file = str(tmp_path / "state.json")

        # Pre-populate state
        save_import_state("orders", "2024-01-15T06:00:00", state_file)

        # Verify state was saved and can be retrieved
        val = get_last_imported_value("orders", state_file)
        assert val == "2024-01-15T06:00:00"

        # Tables without state should return None
        val = get_last_imported_value("users", state_file)
        assert val is None


# ---------------------------------------------------------------------------
# Pipeline 2: CDC end-to-end
# ---------------------------------------------------------------------------
class TestPipeline2E2E:
    """Full CDC flow: config generation -> SQL template resolution -> file output."""

    def test_full_cdc_config_generation(self, tmp_path):
        from flink_cdc_processor import generate_flink_job_config, write_flink_job_config

        config = generate_flink_job_config(
            kafka_bootstrap="b-1.msk-cluster.kafka.us-east-1.amazonaws.com:9098,b-2.msk-cluster.kafka.us-east-1.amazonaws.com:9098,b-3.msk-cluster.kafka.us-east-1.amazonaws.com:9098",
            schema_registry_url="https://glue.us-east-1.amazonaws.com",
            s3_bucket="zomato-e2e-test-bucket",
            checkpoint_dir="s3://checkpoints/pipeline2",
        )

        output_path = str(tmp_path / "flink_config.json")
        write_flink_job_config(config, output_path)

        # Verify the config file
        with open(output_path) as f:
            loaded = json.load(f)

        assert loaded["job_name"] == "zomato-cdc-to-iceberg"
        assert loaded["parallelism"] == 32

        # All SQL statements should have placeholders resolved
        for key, stmt in loaded["sql_statements"].items():
            assert "{kafka_bootstrap}" not in stmt
            assert "{schema_registry_url}" not in stmt
            assert "{s3_bucket}" not in stmt

        # Verify MSK source SQL is well-formed
        source_sql = loaded["sql_statements"]["create_kafka_source"]
        assert "b-1.msk-cluster.kafka.us-east-1.amazonaws.com:9098,b-2.msk-cluster.kafka.us-east-1.amazonaws.com:9098,b-3.msk-cluster.kafka.us-east-1.amazonaws.com:9098" in source_sql
        assert "https://glue.us-east-1.amazonaws.com" in source_sql

        # Verify Iceberg sink references correct bucket
        sink_sql = loaded["sql_statements"]["create_iceberg_orders_sink"]
        assert "zomato-e2e-test-bucket" in sink_sql


# ---------------------------------------------------------------------------
# Pipeline 3: DynamoDB Streams end-to-end
# ---------------------------------------------------------------------------
class TestPipeline3E2E:
    """Full DynamoDB Streams flow: stream records -> process -> S3 write."""

    def test_full_stream_processing_flow(self, mock_s3, data_generator):
        from dynamodb_stream_processor import DynamoDBStreamToS3

        with patch("dynamodb_stream_processor.boto3") as mock_boto:
            mock_boto.client.return_value = mock_s3
            processor = DynamoDBStreamToS3(
                s3_bucket="zomato-data-platform-test-raw-data-lake"
            )
            processor.s3_client = mock_s3

        # Process records from multiple tables
        tables_data = {
            "orders": [
                {"order_id": {"S": "ord_001"}, "total_amount": {"N": "750"}, "city": {"S": "Mumbai"}},
                {"order_id": {"S": "ord_002"}, "total_amount": {"N": "500"}, "city": {"S": "Delhi"}},
            ],
            "payments": [
                {"payment_id": {"S": "pay_001"}, "amount": {"N": "750"}, "status": {"S": "SUCCESS"}},
            ],
        }

        all_s3_keys = []
        for table_name, records_data in tables_data.items():
            processed_records = []
            for data in records_data:
                stream_rec = data_generator.dynamodb_stream_record(
                    table_name=table_name,
                    data=data,
                )
                processed = processor.process_stream_record(stream_rec)
                processed_records.append(processed)

            s3_key = processor.write_batch_to_s3(processed_records, table_name)
            all_s3_keys.append(s3_key)

        # Verify all writes
        assert len(all_s3_keys) == 2

        for s3_key in all_s3_keys:
            resp = mock_s3.get_object(
                Bucket="zomato-data-platform-test-raw-data-lake",
                Key=s3_key,
            )
            body = resp["Body"].read().decode("utf-8")
            for line in body.strip().split("\n"):
                record = json.loads(line)
                assert "event_id" in record
                assert "data" in record
                assert "table_name" in record


# ---------------------------------------------------------------------------
# Pipeline 4: Realtime Events end-to-end
# ---------------------------------------------------------------------------
class TestPipeline4E2E:
    """Full realtime flow: event creation -> config generation -> Druid spec."""

    def test_full_realtime_config_generation(self, tmp_path):
        from flink_realtime_processor import (
            generate_druid_ingestion_spec,
            generate_flink_job_config,
        )

        # Generate Flink config
        flink_config = generate_flink_job_config(
            kafka_bootstrap="b-1.msk-cluster.kafka.us-east-1.amazonaws.com:9098",
            kafka_bootstrap_2="b-2.msk-cluster.kafka.us-east-1.amazonaws.com:9098",
            s3_bucket="zomato-e2e-test-bucket",
            checkpoint_dir="s3://checkpoints/pipeline4",
        )

        assert flink_config["job_name"] == "zomato-realtime-events"
        assert flink_config["parallelism"] == 64

        # All SQL statements resolved
        for key, stmt in flink_config["sql_statements"].items():
            assert "{" not in stmt or "'" in stmt, f"Unresolved placeholder in {key}"

        # Generate Druid spec
        druid_spec = generate_druid_ingestion_spec(
            kafka_bootstrap="b-2.msk-cluster.kafka.us-east-1.amazonaws.com:9098",
            datasource="zomato_e2e_test_events",
        )

        assert druid_spec["type"] == "kafka"
        assert druid_spec["spec"]["dataSchema"]["dataSource"] == "zomato_e2e_test_events"
        assert druid_spec["spec"]["ioConfig"]["topic"] == "druid-ingestion-events"

    @patch("event_producer.Producer")
    def test_event_production_and_routing(self, mock_producer_cls):
        from event_producer import (
            EventType,
            ZomatoEvent,
            ZomatoEventProducer,
            create_sample_order_event,
        )

        mock_instance = MagicMock()
        mock_producer_cls.return_value = mock_instance

        producer = ZomatoEventProducer("b-1.msk-cluster.kafka.us-east-1.amazonaws.com:9098")

        # Produce events of different types
        events = [
            create_sample_order_event("usr_001", "Mumbai"),
            ZomatoEvent(event_type=EventType.USER_LOGIN.value, user_id="usr_002"),
            ZomatoEvent(event_type=EventType.MENU_VIEWED.value, user_id="usr_003"),
            ZomatoEvent(event_type=EventType.PROMO_APPLIED.value, user_id="usr_004"),
        ]

        producer.send_batch(events)

        # Verify correct topic routing
        calls = mock_instance.produce.call_args_list
        assert len(calls) == 4

        topics = [call[1]["topic"] for call in calls]
        assert topics[0] == "orders"
        assert topics[1] == "users"
        assert topics[2] == "menu"
        assert topics[3] == "promo"

        mock_instance.flush.assert_called_once()


# ---------------------------------------------------------------------------
# Cross-pipeline integration
# ---------------------------------------------------------------------------
class TestCrossPipelineIntegration:
    """Verify data flows correctly between pipelines."""

    def test_pipeline3_output_matches_spark_schema(self, data_generator):
        """Pipeline 3 Lambda output must match the Spark ORC converter input schema."""
        from dynamodb_stream_processor import DynamoDBStreamToS3

        with patch("dynamodb_stream_processor.boto3"):
            processor = DynamoDBStreamToS3(s3_bucket="test-bucket")

        record = data_generator.dynamodb_stream_record(
            table_name="orders",
            data={
                "order_id": {"S": "ord_cross_test"},
                "total_amount": {"N": "999.50"},
                "city": {"S": "Pune"},
            },
        )
        processed = processor.process_stream_record(record)

        # Verify the processed record has the fields expected by spark_orc_converter
        assert "event_id" in processed
        assert "event_name" in processed
        assert "table_name" in processed
        assert "sequence_number" in processed
        assert "data" in processed
        assert isinstance(processed["data"], dict)
        assert processed["data"]["order_id"] == "ord_cross_test"
        assert processed["data"]["total_amount"] == 999.50

    def test_all_pipelines_reference_consistent_s3_paths(self):
        """All pipelines should use consistent S3 path prefixes."""
        from flink_cdc_processor import generate_flink_job_config as gen_p2
        from flink_realtime_processor import generate_flink_job_config as gen_p4

        bucket = "zomato-consistency-test"

        p2_config = gen_p2("k:9098", "https://glue.us-east-1.amazonaws.com", bucket, "s3://cp/p2")
        p4_config = gen_p4("k:9098", "k2:9098", bucket, "s3://cp/p4")

        # Pipeline 2 uses iceberg path
        assert f"s3://{bucket}/pipeline2-cdc/iceberg" in p2_config["iceberg"]["warehouse"]

        # Pipeline 4 SQL references the bucket
        s3_stmt = p4_config["sql_statements"]["create_s3_orc_sink"]
        assert bucket in s3_stmt
