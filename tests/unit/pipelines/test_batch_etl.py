"""
Unit tests for Pipeline 1 - Batch ETL (Aurora MySQL -> Sqoop -> S3 ORC).

Tests cover:
  - SqoopConfig dataclass creation and defaults
  - Sqoop command generation with and without incremental mode
  - S3 target path generation with time-based partitioning
  - ORC conversion Hive query construction
  - Import state management (save/load)
  - Batch orchestration result tracking
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "pipelines",
        "pipeline1_batch_etl",
        "src",
    ),
)

from batch_etl import (
    SQOOP_TABLES,
    SqoopConfig,
    build_sqoop_command,
    convert_to_orc_with_hive,
    get_last_imported_value,
    run_batch_etl,
    run_sqoop_import,
    save_import_state,
)


# ---------------------------------------------------------------------------
# SqoopConfig tests
# ---------------------------------------------------------------------------
class TestSqoopConfig:
    def test_default_values(self):
        config = SqoopConfig(table="orders", split_by="order_id")
        assert config.num_mappers == 8
        assert config.incremental_column == "updated_at"
        assert config.check_column == "updated_at"
        assert config.merge_key == "id"

    def test_custom_values(self):
        config = SqoopConfig(
            table="payments",
            split_by="payment_id",
            num_mappers=12,
            incremental_column="modified_at",
            check_column="modified_at",
            merge_key="payment_id",
        )
        assert config.table == "payments"
        assert config.split_by == "payment_id"
        assert config.num_mappers == 12
        assert config.merge_key == "payment_id"

    def test_predefined_tables_count(self):
        assert len(SQOOP_TABLES) == 6

    def test_predefined_orders_config(self):
        orders_cfg = next(c for c in SQOOP_TABLES if c.table == "orders")
        assert orders_cfg.split_by == "order_id"
        assert orders_cfg.num_mappers == 16

    def test_predefined_tables_all_have_split_by(self):
        for cfg in SQOOP_TABLES:
            assert cfg.split_by, f"Table {cfg.table} missing split_by"
            assert cfg.num_mappers > 0, f"Table {cfg.table} has invalid num_mappers"


# ---------------------------------------------------------------------------
# Sqoop command generation tests
# ---------------------------------------------------------------------------
class TestBuildSqoopCommand:
    JDBC_URL = "jdbc:mysql://aurora-test:3306/zomato"
    S3_TARGET = "s3://test-bucket"

    def test_basic_command_structure(self):
        config = SqoopConfig(table="orders", split_by="order_id", num_mappers=16)
        cmd = build_sqoop_command(config, self.JDBC_URL, self.S3_TARGET)

        assert cmd[0] == "sqoop"
        assert cmd[1] == "import"
        assert "--connect" in cmd
        assert self.JDBC_URL in cmd
        assert "--table" in cmd
        assert "orders" in cmd

    def test_orc_format_flags(self):
        config = SqoopConfig(table="orders", split_by="order_id")
        cmd = build_sqoop_command(config, self.JDBC_URL, self.S3_TARGET)

        assert "--as-orcfile" in cmd
        assert "--compress" in cmd
        codec_idx = cmd.index("--compression-codec")
        assert cmd[codec_idx + 1] == "org.apache.hadoop.io.compress.SnappyCodec"

    def test_num_mappers_included(self):
        config = SqoopConfig(table="orders", split_by="order_id", num_mappers=16)
        cmd = build_sqoop_command(config, self.JDBC_URL, self.S3_TARGET)

        idx = cmd.index("--num-mappers")
        assert cmd[idx + 1] == "16"

    def test_split_by_column(self):
        config = SqoopConfig(table="users", split_by="user_id")
        cmd = build_sqoop_command(config, self.JDBC_URL, self.S3_TARGET)

        idx = cmd.index("--split-by")
        assert cmd[idx + 1] == "user_id"

    def test_s3_target_path_includes_table_and_partition(self):
        config = SqoopConfig(table="orders", split_by="order_id")
        cmd = build_sqoop_command(config, self.JDBC_URL, self.S3_TARGET)

        idx = cmd.index("--target-dir")
        target = cmd[idx + 1]
        assert target.startswith(f"{self.S3_TARGET}/pipeline1-batch-etl/sqoop-output/orders/")
        # Verify time-based partition pattern YYYY/MM/DD/HH
        parts = target.split("orders/")[1].split("/")
        assert len(parts) == 4
        assert len(parts[0]) == 4  # year
        assert len(parts[1]) == 2  # month
        assert len(parts[2]) == 2  # day
        assert len(parts[3]) == 2  # hour

    def test_hive_import_settings(self):
        config = SqoopConfig(table="orders", split_by="order_id")
        cmd = build_sqoop_command(config, self.JDBC_URL, self.S3_TARGET)

        assert "--hive-import" in cmd
        assert "--hive-overwrite" in cmd
        idx = cmd.index("--hive-database")
        assert cmd[idx + 1] == "zomato_raw"
        idx = cmd.index("--hive-table")
        assert cmd[idx + 1] == "orders"

    def test_null_handling(self):
        config = SqoopConfig(table="orders", split_by="order_id")
        cmd = build_sqoop_command(config, self.JDBC_URL, self.S3_TARGET)

        idx = cmd.index("--null-string")
        assert cmd[idx + 1] == "\\\\N"
        idx = cmd.index("--null-non-string")
        assert cmd[idx + 1] == "\\\\N"

    def test_no_incremental_flags_without_last_value(self):
        config = SqoopConfig(table="orders", split_by="order_id")
        cmd = build_sqoop_command(config, self.JDBC_URL, self.S3_TARGET)

        assert "--incremental" not in cmd
        assert "--check-column" not in cmd
        assert "--last-value" not in cmd
        assert "--merge-key" not in cmd

    def test_incremental_flags_with_last_value(self):
        config = SqoopConfig(
            table="orders",
            split_by="order_id",
            check_column="updated_at",
            merge_key="id",
        )
        last_val = "2024-01-15T12:00:00"
        cmd = build_sqoop_command(config, self.JDBC_URL, self.S3_TARGET, last_value=last_val)

        assert "--incremental" in cmd
        idx = cmd.index("--incremental")
        assert cmd[idx + 1] == "lastmodified"

        idx = cmd.index("--check-column")
        assert cmd[idx + 1] == "updated_at"

        idx = cmd.index("--last-value")
        assert cmd[idx + 1] == last_val

        idx = cmd.index("--merge-key")
        assert cmd[idx + 1] == "id"

    def test_map_column_java_for_timestamps(self):
        config = SqoopConfig(table="orders", split_by="order_id")
        cmd = build_sqoop_command(config, self.JDBC_URL, self.S3_TARGET)

        idx = cmd.index("--map-column-java")
        assert "created_at=String" in cmd[idx + 1]
        assert "updated_at=String" in cmd[idx + 1]


# ---------------------------------------------------------------------------
# Import state tests
# ---------------------------------------------------------------------------
class TestImportState:
    def test_get_last_value_nonexistent_file(self, tmp_path):
        result = get_last_imported_value("orders", str(tmp_path / "missing.json"))
        assert result is None

    def test_save_and_load_state(self, tmp_state_file):
        save_import_state("orders", "2024-01-15T12:00:00", tmp_state_file)
        val = get_last_imported_value("orders", tmp_state_file)
        assert val == "2024-01-15T12:00:00"

    def test_save_preserves_other_tables(self, tmp_state_file):
        save_import_state("orders", "2024-01-15T12:00:00", tmp_state_file)
        save_import_state("users", "2024-01-15T13:00:00", tmp_state_file)

        assert get_last_imported_value("orders", tmp_state_file) == "2024-01-15T12:00:00"
        assert get_last_imported_value("users", tmp_state_file) == "2024-01-15T13:00:00"

    def test_save_overwrites_same_table(self, tmp_state_file):
        save_import_state("orders", "2024-01-15T12:00:00", tmp_state_file)
        save_import_state("orders", "2024-01-15T18:00:00", tmp_state_file)

        val = get_last_imported_value("orders", tmp_state_file)
        assert val == "2024-01-15T18:00:00"

    def test_state_file_contains_imported_at(self, tmp_state_file):
        save_import_state("orders", "2024-01-15T12:00:00", tmp_state_file)

        with open(tmp_state_file) as f:
            state = json.load(f)
        assert "imported_at" in state["orders"]

    def test_save_creates_parent_directories(self, tmp_path):
        deep_path = str(tmp_path / "a" / "b" / "c" / "state.json")
        save_import_state("orders", "2024-01-15T12:00:00", deep_path)
        assert Path(deep_path).exists()

    def test_get_missing_table_returns_none(self, tmp_state_file):
        save_import_state("orders", "2024-01-15T12:00:00", tmp_state_file)
        assert get_last_imported_value("nonexistent", tmp_state_file) is None


# ---------------------------------------------------------------------------
# Sqoop import execution tests
# ---------------------------------------------------------------------------
class TestRunSqoopImport:
    @patch("batch_etl.subprocess.run")
    def test_successful_import(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        config = SqoopConfig(table="orders", split_by="order_id")
        result = run_sqoop_import(config, "jdbc:mysql://localhost/test", "s3://bucket")
        assert result is True

    @patch("batch_etl.subprocess.run")
    def test_failed_import(self, mock_run):
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="Connection refused")
        config = SqoopConfig(table="orders", split_by="order_id")
        result = run_sqoop_import(config, "jdbc:mysql://localhost/test", "s3://bucket")
        assert result is False

    @patch("batch_etl.subprocess.run", side_effect=FileNotFoundError("sqoop not found"))
    def test_sqoop_binary_missing(self, mock_run):
        config = SqoopConfig(table="orders", split_by="order_id")
        result = run_sqoop_import(config, "jdbc:mysql://localhost/test", "s3://bucket")
        assert result is False

    @patch("batch_etl.subprocess.run", side_effect=__import__("subprocess").TimeoutExpired(cmd="sqoop", timeout=3600))
    def test_timeout(self, mock_run):
        config = SqoopConfig(table="orders", split_by="order_id")
        result = run_sqoop_import(config, "jdbc:mysql://localhost/test", "s3://bucket")
        assert result is False


# ---------------------------------------------------------------------------
# ORC conversion tests
# ---------------------------------------------------------------------------
class TestConvertToORC:
    @patch("batch_etl.subprocess.run")
    def test_successful_conversion(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0)
        result = convert_to_orc_with_hive("orders", "s3://raw/orders", "s3://orc/orders")
        assert result is True

    @patch("batch_etl.subprocess.run")
    def test_failed_conversion(self, mock_run):
        mock_run.return_value = MagicMock(returncode=1, stderr="Table not found")
        result = convert_to_orc_with_hive("orders", "s3://raw/orders", "s3://orc/orders")
        assert result is False

    @patch("batch_etl.subprocess.run")
    def test_hive_command_contains_table(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0)
        convert_to_orc_with_hive("orders", "s3://raw/orders", "s3://orc/orders")

        call_args = mock_run.call_args[0][0]
        assert call_args[0] == "hive"
        assert call_args[1] == "-e"
        assert "zomato_processed.orders" in call_args[2]
        assert "zomato_raw.orders" in call_args[2]


# ---------------------------------------------------------------------------
# Batch ETL orchestration tests
# ---------------------------------------------------------------------------
class TestRunBatchETL:
    @patch("batch_etl.convert_to_orc_with_hive", return_value=True)
    @patch("batch_etl.run_sqoop_import", return_value=True)
    def test_all_tables_succeed(self, mock_sqoop, mock_orc, tmp_state_file):
        tables = [
            SqoopConfig(table="orders", split_by="order_id"),
            SqoopConfig(table="users", split_by="user_id"),
        ]
        results = run_batch_etl("jdbc:mysql://localhost/test", "test-bucket", tmp_state_file, tables)

        assert results["success"] == ["orders", "users"]
        assert results["failed"] == []
        assert "start_time" in results
        assert "end_time" in results

    @patch("batch_etl.convert_to_orc_with_hive", return_value=True)
    @patch("batch_etl.run_sqoop_import", side_effect=[True, False])
    def test_partial_failure(self, mock_sqoop, mock_orc, tmp_state_file):
        tables = [
            SqoopConfig(table="orders", split_by="order_id"),
            SqoopConfig(table="users", split_by="user_id"),
        ]
        results = run_batch_etl("jdbc:mysql://localhost/test", "test-bucket", tmp_state_file, tables)

        assert results["success"] == ["orders"]
        assert results["failed"] == ["users"]

    @patch("batch_etl.convert_to_orc_with_hive", return_value=True)
    @patch("batch_etl.run_sqoop_import", return_value=True)
    def test_state_saved_on_success(self, mock_sqoop, mock_orc, tmp_state_file):
        tables = [SqoopConfig(table="orders", split_by="order_id")]
        run_batch_etl("jdbc:mysql://localhost/test", "test-bucket", tmp_state_file, tables)

        val = get_last_imported_value("orders", tmp_state_file)
        assert val is not None
