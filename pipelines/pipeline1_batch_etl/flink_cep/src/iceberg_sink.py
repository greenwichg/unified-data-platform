"""
PyFlink equivalent of IcebergSink.java.

Writes detected CEP pattern results (fraud alerts, cancellation anomaly alerts)
to Apache Iceberg tables on S3 via the PyFlink Table API.

Uses the AWS Glue catalog instead of Hive (catalog-impl = GlueCatalog).

Converted from: com.zomato.pipeline1.sinks.IcebergSink
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from pyflink.common import Row, Types
from pyflink.datastream import DataStream
from pyflink.table import StreamTableEnvironment, Schema

if TYPE_CHECKING:
    from fraud_detection_pattern import FraudAlert
    from order_anomaly_pattern import CancellationAnomalyAlert

logger = logging.getLogger(__name__)

DATE_FMT = "%Y-%m-%d"


class IcebergSink:
    """Manages Iceberg catalog registration, table DDL, and sink wiring."""

    def __init__(self, s3_bucket: str, catalog_name: str = "zomato_iceberg") -> None:
        self._warehouse_path: str = f"s3://{s3_bucket}/pipeline1-batch-etl/iceberg"
        self._catalog_name: str = catalog_name

    # ------------------------------------------------------------------
    # Catalog & table DDL
    # ------------------------------------------------------------------

    def register_catalog(self, table_env: StreamTableEnvironment) -> None:
        """Register the Iceberg catalog backed by AWS Glue in *table_env*."""
        ddl = (
            f"CREATE CATALOG {self._catalog_name} WITH ("
            f"  'type' = 'iceberg',"
            f"  'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',"
            f"  'warehouse' = '{self._warehouse_path}',"
            f"  'property-version' = '1'"
            f")"
        )
        table_env.execute_sql(ddl)
        table_env.execute_sql(f"USE CATALOG {self._catalog_name}")
        table_env.execute_sql("CREATE DATABASE IF NOT EXISTS cep_alerts")
        table_env.execute_sql("USE cep_alerts")
        logger.info("Iceberg catalog registered: warehouse=%s", self._warehouse_path)

    def create_fraud_alerts_table(self, table_env: StreamTableEnvironment) -> None:
        """Create the ``fraud_alerts`` Iceberg table if it does not exist."""
        ddl = (
            "CREATE TABLE IF NOT EXISTS fraud_alerts ("
            "  user_id STRING,"
            "  order_ids STRING,"
            "  distinct_addresses STRING,"
            "  num_addresses INT,"
            "  num_orders INT,"
            "  alert_type STRING,"
            "  severity STRING,"
            "  detected_at TIMESTAMP(3),"
            "  window_start TIMESTAMP(3),"
            "  window_end TIMESTAMP(3),"
            "  dt STRING,"
            "  PRIMARY KEY (user_id, detected_at) NOT ENFORCED"
            ") PARTITIONED BY (dt) WITH ("
            "  'format-version' = '2',"
            "  'write.format.default' = 'orc',"
            "  'write.orc.compress' = 'SNAPPY',"
            "  'write.upsert.enabled' = 'true',"
            "  'write.metadata.delete-after-commit.enabled' = 'true',"
            "  'write.metadata.previous-versions-max' = '10'"
            ")"
        )
        table_env.execute_sql(ddl)
        logger.info("Created Iceberg table: fraud_alerts")

    def create_cancellation_anomalies_table(self, table_env: StreamTableEnvironment) -> None:
        """Create the ``cancellation_anomalies`` Iceberg table if it does not exist."""
        ddl = (
            "CREATE TABLE IF NOT EXISTS cancellation_anomalies ("
            "  restaurant_id STRING,"
            "  cancellation_count BIGINT,"
            "  baseline_count DOUBLE,"
            "  spike_ratio DOUBLE,"
            "  severity STRING,"
            "  detected_at TIMESTAMP(3),"
            "  window_start TIMESTAMP(3),"
            "  window_end TIMESTAMP(3),"
            "  dt STRING,"
            "  PRIMARY KEY (restaurant_id, detected_at) NOT ENFORCED"
            ") PARTITIONED BY (dt) WITH ("
            "  'format-version' = '2',"
            "  'write.format.default' = 'orc',"
            "  'write.orc.compress' = 'SNAPPY',"
            "  'write.upsert.enabled' = 'true',"
            "  'write.metadata.delete-after-commit.enabled' = 'true',"
            "  'write.metadata.previous-versions-max' = '10'"
            ")"
        )
        table_env.execute_sql(ddl)
        logger.info("Created Iceberg table: cancellation_anomalies")

    # ------------------------------------------------------------------
    # Sink writers
    # ------------------------------------------------------------------

    def write_fraud_alerts(
        self,
        table_env: StreamTableEnvironment,
        fraud_alerts: DataStream,
    ) -> None:
        """Write fraud alerts to the Iceberg ``fraud_alerts`` table."""

        def _to_row(alert: "FraudAlert") -> Row:
            detected = datetime.fromtimestamp(alert.detected_at / 1000.0, tz=timezone.utc)
            window_start = datetime.fromtimestamp(alert.window_start_ms / 1000.0, tz=timezone.utc)
            window_end = datetime.fromtimestamp(alert.window_end_ms / 1000.0, tz=timezone.utc)
            return Row(
                alert.user_id,
                ",".join(alert.order_ids),
                ",".join(sorted(alert.distinct_addresses)),
                len(alert.distinct_addresses),
                len(alert.order_ids),
                alert.alert_type,
                alert.severity,
                detected.replace(tzinfo=None),   # TIMESTAMP(3) expects naive
                window_start.replace(tzinfo=None),
                window_end.replace(tzinfo=None),
                detected.strftime(DATE_FMT),
            )

        rows: DataStream = fraud_alerts.map(_to_row)

        schema = (
            Schema.new_builder()
            .column("f0", "STRING")
            .column("f1", "STRING")
            .column("f2", "STRING")
            .column("f3", "INT")
            .column("f4", "INT")
            .column("f5", "STRING")
            .column("f6", "STRING")
            .column("f7", "TIMESTAMP(3)")
            .column("f8", "TIMESTAMP(3)")
            .column("f9", "TIMESTAMP(3)")
            .column("f10", "STRING")
            .build()
        )

        table = table_env.from_data_stream(rows, schema)
        table_env.create_temporary_view("tmp_fraud_alerts", table)

        table_env.execute_sql(
            "INSERT INTO fraud_alerts "
            "SELECT f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10 "
            "FROM tmp_fraud_alerts"
        )
        logger.info("Fraud alerts sink configured for Iceberg table: fraud_alerts")

    def write_cancellation_anomalies(
        self,
        table_env: StreamTableEnvironment,
        anomaly_alerts: DataStream,
    ) -> None:
        """Write cancellation anomaly alerts to the Iceberg ``cancellation_anomalies`` table."""

        def _to_row(alert: "CancellationAnomalyAlert") -> Row:
            detected = datetime.fromtimestamp(alert.detected_at / 1000.0, tz=timezone.utc)
            window_start = datetime.fromtimestamp(alert.window_start_ms / 1000.0, tz=timezone.utc)
            window_end = datetime.fromtimestamp(alert.window_end_ms / 1000.0, tz=timezone.utc)
            return Row(
                alert.restaurant_id,
                alert.cancellation_count,
                alert.baseline_count,
                alert.spike_ratio,
                alert.severity,
                detected.replace(tzinfo=None),
                window_start.replace(tzinfo=None),
                window_end.replace(tzinfo=None),
                detected.strftime(DATE_FMT),
            )

        rows: DataStream = anomaly_alerts.map(_to_row)

        schema = (
            Schema.new_builder()
            .column("f0", "STRING")
            .column("f1", "BIGINT")
            .column("f2", "DOUBLE")
            .column("f3", "DOUBLE")
            .column("f4", "STRING")
            .column("f5", "TIMESTAMP(3)")
            .column("f6", "TIMESTAMP(3)")
            .column("f7", "TIMESTAMP(3)")
            .column("f8", "STRING")
            .build()
        )

        table = table_env.from_data_stream(rows, schema)
        table_env.create_temporary_view("tmp_cancellation_anomalies", table)

        table_env.execute_sql(
            "INSERT INTO cancellation_anomalies "
            "SELECT f0, f1, f2, f3, f4, f5, f6, f7, f8 "
            "FROM tmp_cancellation_anomalies"
        )
        logger.info(
            "Cancellation anomalies sink configured for Iceberg table: cancellation_anomalies"
        )

    # ------------------------------------------------------------------
    # Convenience: set up everything
    # ------------------------------------------------------------------

    def setup_and_write(
        self,
        table_env: StreamTableEnvironment,
        fraud_alerts: DataStream,
        cancellation_alerts: DataStream,
    ) -> None:
        """Initialize catalog, create tables, and wire up both sinks."""
        self.register_catalog(table_env)
        self.create_fraud_alerts_table(table_env)
        self.create_cancellation_anomalies_table(table_env)
        self.write_fraud_alerts(table_env, fraud_alerts)
        self.write_cancellation_anomalies(table_env, cancellation_alerts)
        logger.info("All Iceberg sinks initialized for Pipeline 1 CEP alerts")
