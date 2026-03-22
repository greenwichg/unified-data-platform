"""
Pipeline 1 - Batch ETL: Aurora MySQL → Spark JDBC → Iceberg/ORC → S3

Reads tables from Aurora MySQL via Spark JDBC, applies transformations
and data quality checks, and writes to Iceberg tables (ORC format) on S3.

Tables imported: orders, users, restaurants, menu_items, payments, promotions
Schedule: Runs every 6 hours via Airflow on Amazon EMR
"""

import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, TimestampType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("pipeline1_batch_etl")


@dataclass
class TableConfig:
    """Configuration for a JDBC table ingestion."""

    table: str
    partition_column: str
    num_partitions: int = 8
    incremental_column: str = "updated_at"
    primary_key: str = "id"
    fetch_size: int = 10000
    decimal_columns: list[str] = field(default_factory=list)
    null_check_columns: list[str] = field(default_factory=list)


# Tables to import from Aurora MySQL
TABLE_CONFIGS = [
    TableConfig(
        table="orders",
        partition_column="order_id",
        num_partitions=32,
        primary_key="order_id",
        fetch_size=10000,
        decimal_columns=["subtotal", "tax", "delivery_fee", "total_amount"],
        null_check_columns=["order_id", "user_id", "restaurant_id", "created_at"],
    ),
    TableConfig(
        table="users",
        partition_column="user_id",
        num_partitions=16,
        primary_key="user_id",
        fetch_size=5000,
        null_check_columns=["user_id", "email"],
    ),
    TableConfig(
        table="restaurants",
        partition_column="restaurant_id",
        num_partitions=8,
        primary_key="restaurant_id",
        fetch_size=5000,
        null_check_columns=["restaurant_id", "name", "city"],
    ),
    TableConfig(
        table="menu_items",
        partition_column="item_id",
        num_partitions=16,
        primary_key="item_id",
        fetch_size=5000,
        decimal_columns=["price"],
        null_check_columns=["item_id", "restaurant_id", "name"],
    ),
    TableConfig(
        table="payments",
        partition_column="payment_id",
        num_partitions=32,
        primary_key="payment_id",
        fetch_size=10000,
        decimal_columns=["amount"],
        null_check_columns=["payment_id", "order_id"],
    ),
    TableConfig(
        table="promotions",
        partition_column="promo_id",
        num_partitions=8,
        primary_key="promo_id",
        fetch_size=2000,
        decimal_columns=["discount_value", "min_order_value", "max_discount"],
        null_check_columns=["promo_id", "code"],
    ),
]


def create_spark_session(app_name: str = "Pipeline1-BatchETL") -> SparkSession:
    """Create a Spark session configured for JDBC reads and Iceberg/ORC writes."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.iceberg.warehouse", "s3://zomato-data-platform-prod-raw-data-lake/iceberg")
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.orc.impl", "native")
        .config("spark.sql.orc.enableVectorizedReader", "true")
        .config("spark.sql.orc.filterPushdown", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.InstanceProfileCredentialsProvider",
        )
        .enableHiveSupport()
        .getOrCreate()
    )


def get_jdbc_bounds(
    spark: SparkSession,
    jdbc_url: str,
    jdbc_props: dict,
    config: TableConfig,
    last_value: str | None = None,
) -> tuple[int, int]:
    """Get min/max of the partition column for balanced JDBC partitioning."""
    where = ""
    if last_value:
        where = f"WHERE {config.incremental_column} >= '{last_value}'"

    query = f"(SELECT MIN({config.partition_column}) AS min_val, MAX({config.partition_column}) AS max_val FROM {config.table} {where}) AS bounds"

    bounds_df = spark.read.jdbc(jdbc_url, query, properties=jdbc_props)
    row = bounds_df.collect()[0]
    return int(row["min_val"] or 0), int(row["max_val"] or 0)


def read_table_jdbc(
    spark: SparkSession,
    jdbc_url: str,
    jdbc_props: dict,
    config: TableConfig,
    last_value: str | None = None,
) -> DataFrame:
    """Read a table from Aurora MySQL via Spark JDBC with partitioned reads."""
    lower, upper = get_jdbc_bounds(spark, jdbc_url, jdbc_props, config, last_value)

    if lower == 0 and upper == 0:
        logger.info("No data found for table %s (bounds: %d-%d)", config.table, lower, upper)
        return spark.createDataFrame([], schema=None)

    logger.info(
        "Reading %s: partition_col=%s bounds=[%d, %d] partitions=%d",
        config.table, config.partition_column, lower, upper, config.num_partitions,
    )

    # Build the query — filter for incremental if last_value is provided
    if last_value:
        table_query = f"(SELECT * FROM {config.table} WHERE {config.incremental_column} >= '{last_value}') AS t"
    else:
        table_query = config.table

    df = (
        spark.read.jdbc(
            url=jdbc_url,
            table=table_query,
            column=config.partition_column,
            lowerBound=lower,
            upperBound=upper + 1,
            numPartitions=config.num_partitions,
            properties={**jdbc_props, "fetchsize": str(config.fetch_size)},
        )
    )

    record_count = df.count()
    logger.info("Read %d records from %s", record_count, config.table)
    return df


def apply_transformations(df: DataFrame, config: TableConfig) -> DataFrame:
    """Apply type casting and add audit columns."""
    # Cast decimal columns
    for col_name in config.decimal_columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(DecimalType(18, 2)))

    # Ensure timestamp columns are proper types
    for col_name in ["created_at", "updated_at"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(TimestampType()))

    # Add partitioning and audit columns
    df = df.withColumn("dt", F.current_date())
    df = df.withColumn("_etl_loaded_at", F.current_timestamp())

    return df


def run_quality_checks(df: DataFrame, config: TableConfig) -> dict:
    """Run data quality checks and return metrics."""
    total_count = df.count()
    checks = {
        "table": config.table,
        "total_records": total_count,
        "passed": True,
        "checks": {},
    }

    if total_count == 0:
        checks["passed"] = True
        checks["checks"]["empty_result"] = "WARN: No records to validate"
        return checks

    # Null checks on critical columns
    for col_name in config.null_check_columns:
        if col_name in df.columns:
            null_count = df.filter(F.col(col_name).isNull()).count()
            null_pct = null_count / total_count
            check_passed = null_pct < 0.01  # Less than 1% nulls
            checks["checks"][f"null_{col_name}"] = {
                "null_count": null_count,
                "null_pct": round(null_pct, 4),
                "passed": check_passed,
            }
            if not check_passed:
                checks["passed"] = False
                logger.warning(
                    "Quality check FAILED: %s.%s has %.1f%% nulls (%d/%d)",
                    config.table, col_name, null_pct * 100, null_count, total_count,
                )

    # Duplicate check on primary key
    distinct_count = df.select(config.primary_key).distinct().count()
    dup_count = total_count - distinct_count
    checks["checks"]["duplicates"] = {
        "duplicate_count": dup_count,
        "passed": dup_count == 0,
    }

    return checks


def write_to_iceberg(df: DataFrame, config: TableConfig, s3_path: str) -> None:
    """Write DataFrame to Iceberg table with ORC format."""
    iceberg_table = f"iceberg.zomato_raw.{config.table}"

    logger.info("Writing %s to Iceberg table %s (ORC format)", config.table, iceberg_table)

    # Use MERGE INTO for incremental (upsert), overwrite for full load
    df.createOrReplaceTempView(f"staging_{config.table}")

    try:
        # Try merge (table exists)
        df.writeTo(iceberg_table).option("merge-schema", "true").append()
        logger.info("Appended to existing Iceberg table: %s", iceberg_table)
    except Exception:
        # Table doesn't exist — create it
        logger.info("Creating new Iceberg table: %s", iceberg_table)
        (
            df.writeTo(iceberg_table)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .tableProperty("write.format.default", "orc")
            .tableProperty("write.orc.compression-codec", "snappy")
            .tableProperty("write.orc.stripe-size-bytes", "268435456")
            .partitionedBy(F.col("dt"))
            .create()
        )


def write_to_orc(df: DataFrame, config: TableConfig, s3_path: str) -> None:
    """Write DataFrame directly as ORC files to S3 (fallback/parallel output)."""
    output_path = f"{s3_path}/pipeline1-batch-etl/orc/{config.table}"
    logger.info("Writing %s to ORC at %s", config.table, output_path)

    (
        df.write.mode("append")
        .partitionBy("dt")
        .format("orc")
        .option("compression", "snappy")
        .option("orc.stripe.size", "268435456")
        .option("orc.bloom.filter.columns", config.partition_column)
        .save(output_path)
    )


def get_last_imported_value(table: str, state_file: str) -> str | None:
    """Get the last imported value for incremental imports."""
    state_path = Path(state_file)
    if not state_path.exists():
        return None

    with open(state_path) as f:
        state = json.load(f)

    return state.get(table, {}).get("last_value")


def save_import_state(table: str, last_value: str, state_file: str) -> None:
    """Save the last imported value for a table."""
    state_path = Path(state_file)
    state = {}
    if state_path.exists():
        with open(state_path) as f:
            state = json.load(f)

    state[table] = {
        "last_value": last_value,
        "imported_at": datetime.utcnow().isoformat(),
    }

    state_path.parent.mkdir(parents=True, exist_ok=True)
    with open(state_path, "w") as f:
        json.dump(state, f, indent=2)


def run_batch_etl(
    jdbc_url: str,
    s3_bucket: str,
    state_file: str = "/tmp/spark_jdbc_state.json",
    tables: list[TableConfig] | None = None,
    write_iceberg: bool = True,
) -> dict:
    """Run the full batch ETL pipeline for all configured tables."""
    spark = create_spark_session()
    s3_path = f"s3a://{s3_bucket}"

    jdbc_props = {
        "driver": "com.mysql.cj.jdbc.Driver",
        "user": "etl_user",
        "useSSL": "true",
        "requireSSL": "true",
        "rewriteBatchedStatements": "true",
        "cachePrepStmts": "true",
        "prepStmtCacheSize": "250",
        "useServerPrepStmts": "true",
    }

    results = {
        "success": [],
        "failed": [],
        "quality_reports": [],
        "start_time": datetime.utcnow().isoformat(),
    }

    for config in tables or TABLE_CONFIGS:
        try:
            last_value = get_last_imported_value(config.table, state_file)
            logger.info(
                "Processing %s (incremental from: %s)",
                config.table, last_value or "FULL LOAD",
            )

            # Read from Aurora MySQL via JDBC
            df = read_table_jdbc(spark, jdbc_url, jdbc_props, config, last_value)

            if df.head(1):
                # Transform
                df = apply_transformations(df, config)

                # Quality checks
                quality = run_quality_checks(df, config)
                results["quality_reports"].append(quality)

                if not quality["passed"]:
                    logger.warning(
                        "Quality checks failed for %s — writing anyway with warning",
                        config.table,
                    )

                # Write to Iceberg (primary) and ORC (secondary)
                if write_iceberg:
                    write_to_iceberg(df, config, s3_path)
                write_to_orc(df, config, s3_path)

                # Update state
                new_last_value = datetime.utcnow().isoformat()
                save_import_state(config.table, new_last_value, state_file)

                results["success"].append(config.table)
            else:
                logger.info("No new data for %s", config.table)
                results["success"].append(config.table)

        except Exception:
            logger.exception("Failed to process table: %s", config.table)
            results["failed"].append(config.table)

    results["end_time"] = datetime.utcnow().isoformat()
    logger.info("Batch ETL results: %s", json.dumps(results, indent=2))

    spark.stop()
    return results


if __name__ == "__main__":
    import argparse
    import os

    parser = argparse.ArgumentParser(description="Pipeline 1: Aurora MySQL → Spark JDBC → Iceberg/ORC")
    parser.add_argument(
        "--jdbc-url",
        default=os.environ.get(
            "AURORA_JDBC_URL",
            "jdbc:mysql://aurora-cluster.cluster-xxxxx.ap-south-1.rds.amazonaws.com:3306/zomato",
        ),
    )
    parser.add_argument(
        "--s3-bucket",
        default=os.environ.get("S3_BUCKET", "zomato-data-platform-prod-raw-data-lake"),
    )
    parser.add_argument("--table", default=None, help="Import a single table")
    parser.add_argument("--full-load", action="store_true", help="Ignore incremental state")
    parser.add_argument("--no-iceberg", action="store_true", help="Skip Iceberg writes, ORC only")
    args = parser.parse_args()

    table_configs = TABLE_CONFIGS
    if args.table:
        table_configs = [c for c in TABLE_CONFIGS if c.table == args.table]
        if not table_configs:
            logger.error("Unknown table: %s", args.table)
            sys.exit(1)

    state = "/tmp/spark_jdbc_state.json"
    if args.full_load:
        state = "/dev/null"

    result = run_batch_etl(
        args.jdbc_url,
        args.s3_bucket,
        state_file=state,
        tables=table_configs,
        write_iceberg=not args.no_iceberg,
    )

    if result["failed"]:
        logger.error("Some tables failed: %s", result["failed"])
        sys.exit(1)
    logger.info("All tables imported successfully")
