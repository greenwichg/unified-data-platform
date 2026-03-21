package com.zomato.pipeline1.sinks;

import com.zomato.pipeline1.patterns.FraudDetectionPattern.FraudAlert;
import com.zomato.pipeline1.patterns.OrderAnomalyPattern.CancellationAnomalyAlert;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/**
 * Writes detected CEP pattern results (fraud alerts, cancellation anomaly alerts)
 * to Apache Iceberg tables on S3.
 *
 * <p>Uses Flink's Table API with the Iceberg catalog to create and insert into
 * partitioned, upsert-enabled Iceberg tables stored in ORC format.
 */
public class IcebergSink implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(IcebergSink.class);

    private static final DateTimeFormatter DATE_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);

    private final String warehousePath;
    private final String catalogName;

    public IcebergSink(String s3Bucket) {
        this.warehousePath = "s3://" + s3Bucket + "/pipeline1-batch-etl/iceberg";
        this.catalogName = "zomato_iceberg";
    }

    public IcebergSink(String s3Bucket, String catalogName) {
        this.warehousePath = "s3://" + s3Bucket + "/pipeline1-batch-etl/iceberg";
        this.catalogName = catalogName;
    }

    // -----------------------------------------------------------------------
    // Catalog & table DDL
    // -----------------------------------------------------------------------

    /**
     * Registers the Iceberg catalog in the given table environment.
     */
    public void registerCatalog(StreamTableEnvironment tableEnv) {
        String ddl = String.format(
                "CREATE CATALOG %s WITH ("
                        + "'type' = 'iceberg',"
                        + "'catalog-type' = 'hive',"
                        + "'warehouse' = '%s',"
                        + "'property-version' = '1'"
                        + ")",
                catalogName, warehousePath);

        tableEnv.executeSql(ddl);
        tableEnv.executeSql("USE CATALOG " + catalogName);
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS cep_alerts");
        tableEnv.executeSql("USE cep_alerts");
        LOG.info("Iceberg catalog registered: warehouse={}", warehousePath);
    }

    /**
     * Creates the fraud_alerts Iceberg table if it does not exist.
     */
    public void createFraudAlertsTable(StreamTableEnvironment tableEnv) {
        String ddl = "CREATE TABLE IF NOT EXISTS fraud_alerts ("
                + "  user_id STRING,"
                + "  order_ids STRING,"
                + "  distinct_addresses STRING,"
                + "  num_addresses INT,"
                + "  num_orders INT,"
                + "  alert_type STRING,"
                + "  severity STRING,"
                + "  detected_at TIMESTAMP(3),"
                + "  window_start TIMESTAMP(3),"
                + "  window_end TIMESTAMP(3),"
                + "  dt STRING,"
                + "  PRIMARY KEY (user_id, detected_at) NOT ENFORCED"
                + ") PARTITIONED BY (dt) WITH ("
                + "  'format-version' = '2',"
                + "  'write.format.default' = 'orc',"
                + "  'write.orc.compress' = 'SNAPPY',"
                + "  'write.upsert.enabled' = 'true',"
                + "  'write.metadata.delete-after-commit.enabled' = 'true',"
                + "  'write.metadata.previous-versions-max' = '10'"
                + ")";
        tableEnv.executeSql(ddl);
        LOG.info("Created Iceberg table: fraud_alerts");
    }

    /**
     * Creates the cancellation_anomalies Iceberg table if it does not exist.
     */
    public void createCancellationAnomaliesTable(StreamTableEnvironment tableEnv) {
        String ddl = "CREATE TABLE IF NOT EXISTS cancellation_anomalies ("
                + "  restaurant_id STRING,"
                + "  cancellation_count BIGINT,"
                + "  baseline_count DOUBLE,"
                + "  spike_ratio DOUBLE,"
                + "  severity STRING,"
                + "  detected_at TIMESTAMP(3),"
                + "  window_start TIMESTAMP(3),"
                + "  window_end TIMESTAMP(3),"
                + "  dt STRING,"
                + "  PRIMARY KEY (restaurant_id, detected_at) NOT ENFORCED"
                + ") PARTITIONED BY (dt) WITH ("
                + "  'format-version' = '2',"
                + "  'write.format.default' = 'orc',"
                + "  'write.orc.compress' = 'SNAPPY',"
                + "  'write.upsert.enabled' = 'true',"
                + "  'write.metadata.delete-after-commit.enabled' = 'true',"
                + "  'write.metadata.previous-versions-max' = '10'"
                + ")";
        tableEnv.executeSql(ddl);
        LOG.info("Created Iceberg table: cancellation_anomalies");
    }

    // -----------------------------------------------------------------------
    // Sink writers
    // -----------------------------------------------------------------------

    /**
     * Writes fraud alerts to the Iceberg fraud_alerts table.
     */
    public void writeFraudAlerts(StreamTableEnvironment tableEnv,
                                 DataStream<FraudAlert> fraudAlerts) {
        DataStream<Row> rows = fraudAlerts.map(alert -> {
            java.time.Instant detected = Instant.ofEpochMilli(alert.getDetectedAt());
            java.time.Instant windowStart = Instant.ofEpochMilli(alert.getWindowStartMs());
            java.time.Instant windowEnd = Instant.ofEpochMilli(alert.getWindowEndMs());

            return Row.of(
                    alert.getUserId(),
                    String.join(",", alert.getOrderIds()),
                    String.join(",", alert.getDistinctAddresses()),
                    alert.getDistinctAddresses().size(),
                    alert.getOrderIds().size(),
                    alert.getAlertType(),
                    alert.getSeverity(),
                    java.time.LocalDateTime.ofInstant(detected, ZoneOffset.UTC),
                    java.time.LocalDateTime.ofInstant(windowStart, ZoneOffset.UTC),
                    java.time.LocalDateTime.ofInstant(windowEnd, ZoneOffset.UTC),
                    DATE_FMT.format(detected)
            );
        });

        Table table = tableEnv.fromDataStream(rows, Schema.newBuilder()
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
                .build());

        tableEnv.createTemporaryView("tmp_fraud_alerts", table);

        tableEnv.executeSql(
                "INSERT INTO fraud_alerts "
                        + "SELECT f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10 "
                        + "FROM tmp_fraud_alerts"
        );

        LOG.info("Fraud alerts sink configured for Iceberg table: fraud_alerts");
    }

    /**
     * Writes cancellation anomaly alerts to the Iceberg cancellation_anomalies table.
     */
    public void writeCancellationAnomalies(StreamTableEnvironment tableEnv,
                                           DataStream<CancellationAnomalyAlert> anomalyAlerts) {
        DataStream<Row> rows = anomalyAlerts.map(alert -> {
            Instant detected = Instant.ofEpochMilli(alert.getDetectedAt());
            Instant windowStart = Instant.ofEpochMilli(alert.getWindowStartMs());
            Instant windowEnd = Instant.ofEpochMilli(alert.getWindowEndMs());

            return Row.of(
                    alert.getRestaurantId(),
                    alert.getCancellationCount(),
                    alert.getBaselineCount(),
                    alert.getSpikeRatio(),
                    alert.getSeverity(),
                    java.time.LocalDateTime.ofInstant(detected, ZoneOffset.UTC),
                    java.time.LocalDateTime.ofInstant(windowStart, ZoneOffset.UTC),
                    java.time.LocalDateTime.ofInstant(windowEnd, ZoneOffset.UTC),
                    DATE_FMT.format(detected)
            );
        });

        Table table = tableEnv.fromDataStream(rows, Schema.newBuilder()
                .column("f0", "STRING")
                .column("f1", "BIGINT")
                .column("f2", "DOUBLE")
                .column("f3", "DOUBLE")
                .column("f4", "STRING")
                .column("f5", "TIMESTAMP(3)")
                .column("f6", "TIMESTAMP(3)")
                .column("f7", "TIMESTAMP(3)")
                .column("f8", "STRING")
                .build());

        tableEnv.createTemporaryView("tmp_cancellation_anomalies", table);

        tableEnv.executeSql(
                "INSERT INTO cancellation_anomalies "
                        + "SELECT f0, f1, f2, f3, f4, f5, f6, f7, f8 "
                        + "FROM tmp_cancellation_anomalies"
        );

        LOG.info("Cancellation anomalies sink configured for Iceberg table: cancellation_anomalies");
    }

    // -----------------------------------------------------------------------
    // Convenience: set up everything
    // -----------------------------------------------------------------------

    /**
     * Initializes the Iceberg catalog, creates tables, and wires up both sinks.
     */
    public void setupAndWrite(StreamTableEnvironment tableEnv,
                              DataStream<FraudAlert> fraudAlerts,
                              DataStream<CancellationAnomalyAlert> cancellationAlerts) {
        registerCatalog(tableEnv);
        createFraudAlertsTable(tableEnv);
        createCancellationAnomaliesTable(tableEnv);
        writeFraudAlerts(tableEnv, fraudAlerts);
        writeCancellationAnomalies(tableEnv, cancellationAlerts);

        LOG.info("All Iceberg sinks initialized for Pipeline 1 CEP alerts");
    }
}
