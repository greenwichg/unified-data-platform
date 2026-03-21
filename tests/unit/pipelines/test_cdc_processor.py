"""
Unit tests for Pipeline 2 - CDC Processor (Kafka Avro -> Flink -> Iceberg).

Tests cover:
  - Flink SQL statement generation and template substitution
  - Schema evolution compatibility in Flink SQL definitions
  - Kafka consumer configuration (offset management, group IDs)
  - Avro-Confluent format configuration
  - Iceberg sink configuration (upsert, ORC, partitioning)
  - Job config generation and file output
"""

import json
import os
import sys
import tempfile

import pytest

sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "pipelines",
        "pipeline2_cdc",
        "src",
    ),
)

from flink_cdc_processor import (
    FLINK_SQL_STATEMENTS,
    generate_flink_job_config,
    write_flink_job_config,
)


# ---------------------------------------------------------------------------
# Flink SQL statement tests
# ---------------------------------------------------------------------------
class TestFlinkSQLStatements:
    """Validate that all Flink SQL templates are well-formed."""

    def test_all_expected_statements_present(self):
        expected_keys = {
            "create_kafka_source",
            "create_kafka_users_source",
            "create_kafka_menu_source",
            "create_iceberg_orders_sink",
            "create_iceberg_users_sink",
            "insert_orders",
            "insert_users",
            "complex_event_order_velocity",
        }
        assert expected_keys.issubset(set(FLINK_SQL_STATEMENTS.keys()))

    def test_kafka_source_uses_avro_confluent_format(self):
        stmt = FLINK_SQL_STATEMENTS["create_kafka_source"]
        assert "'format' = 'avro-confluent'" in stmt

    def test_kafka_source_contains_schema_registry_placeholder(self):
        stmt = FLINK_SQL_STATEMENTS["create_kafka_source"]
        assert "{schema_registry_url}" in stmt

    def test_kafka_source_contains_bootstrap_placeholder(self):
        stmt = FLINK_SQL_STATEMENTS["create_kafka_source"]
        assert "{kafka_bootstrap}" in stmt

    def test_kafka_source_watermark_defined(self):
        stmt = FLINK_SQL_STATEMENTS["create_kafka_source"]
        assert "WATERMARK FOR" in stmt
        assert "INTERVAL '5' SECOND" in stmt

    def test_kafka_source_group_id_set(self):
        stmt = FLINK_SQL_STATEMENTS["create_kafka_source"]
        assert "'properties.group.id'" in stmt
        assert "flink-cdc-orders" in stmt

    def test_kafka_users_source_group_id(self):
        stmt = FLINK_SQL_STATEMENTS["create_kafka_users_source"]
        assert "flink-cdc-users" in stmt

    def test_kafka_menu_source_group_id(self):
        stmt = FLINK_SQL_STATEMENTS["create_kafka_menu_source"]
        assert "flink-cdc-menu" in stmt

    def test_kafka_source_startup_mode_earliest(self):
        for key in ["create_kafka_source", "create_kafka_users_source", "create_kafka_menu_source"]:
            stmt = FLINK_SQL_STATEMENTS[key]
            assert "'scan.startup.mode' = 'earliest-offset'" in stmt, (
                f"{key} should use earliest-offset for CDC completeness"
            )

    def test_iceberg_orders_sink_upsert_enabled(self):
        stmt = FLINK_SQL_STATEMENTS["create_iceberg_orders_sink"]
        assert "'write.upsert.enabled' = 'true'" in stmt

    def test_iceberg_orders_sink_orc_format(self):
        stmt = FLINK_SQL_STATEMENTS["create_iceberg_orders_sink"]
        assert "'write.format.default' = 'orc'" in stmt

    def test_iceberg_orders_sink_snappy_compression(self):
        stmt = FLINK_SQL_STATEMENTS["create_iceberg_orders_sink"]
        assert "'write.orc.compress' = 'SNAPPY'" in stmt

    def test_iceberg_orders_sink_primary_key(self):
        stmt = FLINK_SQL_STATEMENTS["create_iceberg_orders_sink"]
        assert "PRIMARY KEY (order_id) NOT ENFORCED" in stmt

    def test_iceberg_orders_partitioned_by_date(self):
        stmt = FLINK_SQL_STATEMENTS["create_iceberg_orders_sink"]
        assert "PARTITIONED BY (dt)" in stmt

    def test_iceberg_format_version_2(self):
        stmt = FLINK_SQL_STATEMENTS["create_iceberg_orders_sink"]
        assert "'format-version' = '2'" in stmt

    def test_iceberg_metadata_cleanup_enabled(self):
        stmt = FLINK_SQL_STATEMENTS["create_iceberg_orders_sink"]
        assert "'write.metadata.delete-after-commit.enabled' = 'true'" in stmt

    def test_insert_orders_adds_processing_time(self):
        stmt = FLINK_SQL_STATEMENTS["insert_orders"]
        assert "CURRENT_TIMESTAMP AS processing_time" in stmt

    def test_insert_orders_adds_date_partition(self):
        stmt = FLINK_SQL_STATEMENTS["insert_orders"]
        assert "DATE_FORMAT(updated_at, 'yyyy-MM-dd') AS dt" in stmt

    def test_order_velocity_tumble_window(self):
        stmt = FLINK_SQL_STATEMENTS["complex_event_order_velocity"]
        assert "TUMBLE(updated_at, INTERVAL '5' MINUTE)" in stmt
        assert "TUMBLE_START" in stmt
        assert "TUMBLE_END" in stmt

    def test_order_velocity_filters_on_op_type(self):
        stmt = FLINK_SQL_STATEMENTS["complex_event_order_velocity"]
        assert "op_type IN ('INSERT', 'UPDATE')" in stmt


# ---------------------------------------------------------------------------
# Schema evolution tests
# ---------------------------------------------------------------------------
class TestSchemaEvolution:
    """Verify that CDC source schemas handle evolution gracefully."""

    def test_orders_source_has_op_type_column(self):
        stmt = FLINK_SQL_STATEMENTS["create_kafka_source"]
        assert "op_type STRING" in stmt

    def test_users_source_has_nullable_fields(self):
        """Verify that user CDC source handles boolean/long types properly."""
        stmt = FLINK_SQL_STATEMENTS["create_kafka_users_source"]
        assert "is_pro_member BOOLEAN" in stmt
        assert "total_orders BIGINT" in stmt

    def test_menu_source_has_numeric_types(self):
        stmt = FLINK_SQL_STATEMENTS["create_kafka_menu_source"]
        assert "price DECIMAL(10, 2)" in stmt
        assert "rating FLOAT" in stmt
        assert "preparation_time_mins INT" in stmt


# ---------------------------------------------------------------------------
# Job config generation tests
# ---------------------------------------------------------------------------
class TestGenerateFlinkJobConfig:
    KAFKA_BOOTSTRAP = "kafka-1:9092,kafka-2:9092"
    SCHEMA_REGISTRY = "http://schema-registry:8081"
    S3_BUCKET = "zomato-test-bucket"
    CHECKPOINT_DIR = "s3://checkpoints/pipeline2"

    def test_job_name(self):
        config = generate_flink_job_config(
            self.KAFKA_BOOTSTRAP, self.SCHEMA_REGISTRY, self.S3_BUCKET, self.CHECKPOINT_DIR
        )
        assert config["job_name"] == "zomato-cdc-to-iceberg"

    def test_parallelism(self):
        config = generate_flink_job_config(
            self.KAFKA_BOOTSTRAP, self.SCHEMA_REGISTRY, self.S3_BUCKET, self.CHECKPOINT_DIR
        )
        assert config["parallelism"] == 32

    def test_checkpoint_config(self):
        config = generate_flink_job_config(
            self.KAFKA_BOOTSTRAP, self.SCHEMA_REGISTRY, self.S3_BUCKET, self.CHECKPOINT_DIR
        )
        assert config["checkpoint_interval_ms"] == 60000
        assert config["checkpoint_dir"] == self.CHECKPOINT_DIR
        assert config["min_pause_between_checkpoints_ms"] == 30000

    def test_state_backend_rocksdb(self):
        config = generate_flink_job_config(
            self.KAFKA_BOOTSTRAP, self.SCHEMA_REGISTRY, self.S3_BUCKET, self.CHECKPOINT_DIR
        )
        assert config["state_backend"] == "rocksdb"

    def test_restart_strategy(self):
        config = generate_flink_job_config(
            self.KAFKA_BOOTSTRAP, self.SCHEMA_REGISTRY, self.S3_BUCKET, self.CHECKPOINT_DIR
        )
        rs = config["restart_strategy"]
        assert rs["type"] == "fixed-delay"
        assert rs["attempts"] == 10
        assert rs["delay_ms"] == 30000

    def test_kafka_consumer_config(self):
        config = generate_flink_job_config(
            self.KAFKA_BOOTSTRAP, self.SCHEMA_REGISTRY, self.S3_BUCKET, self.CHECKPOINT_DIR
        )
        consumer = config["kafka"]["consumer_config"]
        assert consumer["auto.offset.reset"] == "earliest"
        assert consumer["enable.auto.commit"] == "false"
        assert int(consumer["max.poll.records"]) == 10000

    def test_iceberg_config(self):
        config = generate_flink_job_config(
            self.KAFKA_BOOTSTRAP, self.SCHEMA_REGISTRY, self.S3_BUCKET, self.CHECKPOINT_DIR
        )
        iceberg = config["iceberg"]
        assert iceberg["warehouse"] == f"s3://{self.S3_BUCKET}/pipeline2-cdc/iceberg"
        assert iceberg["write_format"] == "orc"
        assert iceberg["upsert_enabled"] is True

    def test_sql_statements_placeholders_resolved(self):
        config = generate_flink_job_config(
            self.KAFKA_BOOTSTRAP, self.SCHEMA_REGISTRY, self.S3_BUCKET, self.CHECKPOINT_DIR
        )
        for key, stmt in config["sql_statements"].items():
            assert "{kafka_bootstrap}" not in stmt, f"Unresolved placeholder in {key}"
            assert "{schema_registry_url}" not in stmt, f"Unresolved placeholder in {key}"
            assert "{s3_bucket}" not in stmt, f"Unresolved placeholder in {key}"

    def test_sql_statements_contain_actual_values(self):
        config = generate_flink_job_config(
            self.KAFKA_BOOTSTRAP, self.SCHEMA_REGISTRY, self.S3_BUCKET, self.CHECKPOINT_DIR
        )
        source_stmt = config["sql_statements"]["create_kafka_source"]
        assert self.KAFKA_BOOTSTRAP in source_stmt
        assert self.SCHEMA_REGISTRY in source_stmt


# ---------------------------------------------------------------------------
# Config file output tests
# ---------------------------------------------------------------------------
class TestWriteFlinkJobConfig:
    def test_writes_valid_json(self, tmp_path):
        config = {"job_name": "test", "parallelism": 4}
        output = str(tmp_path / "config.json")
        write_flink_job_config(config, output)

        with open(output) as f:
            loaded = json.load(f)
        assert loaded == config

    def test_creates_parent_directories(self, tmp_path):
        config = {"job_name": "test"}
        output = str(tmp_path / "a" / "b" / "config.json")
        write_flink_job_config(config, output)
        assert os.path.exists(output)

    def test_output_is_indented(self, tmp_path):
        config = {"job_name": "test", "parallelism": 4}
        output = str(tmp_path / "config.json")
        write_flink_job_config(config, output)

        with open(output) as f:
            content = f.read()
        assert "  " in content  # indented
