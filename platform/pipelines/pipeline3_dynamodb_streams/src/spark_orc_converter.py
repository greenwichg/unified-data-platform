"""
Pipeline 3 - Spark Job: S3 JSON → ORC → S3 (Data Lake)

Runs on Amazon EMR. Reads raw JSON events from DynamoDB Streams,
applies transformations, and writes optimized ORC files to the data lake.
"""

import logging
import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pipeline3_spark_orc")


def create_spark_session(app_name: str = "DynamoDB-Stream-to-ORC") -> SparkSession:
    """Create a Spark session configured for S3 and ORC output."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.orc.impl", "native")
        .config("spark.sql.orc.enableVectorizedReader", "true")
        .config("spark.sql.orc.filterPushdown", "true")
        .config("spark.sql.hive.convertMetastoreOrc", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.InstanceProfileCredentialsProvider",
        )
        .enableHiveSupport()
        .getOrCreate()
    )


# Schema for the JSON records written by the Lambda processor
STREAM_RECORD_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_name", StringType(), True),
        StructField("event_source", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("approximate_creation_time", LongType(), True),
        StructField("sequence_number", StringType(), True),
        StructField("size_bytes", LongType(), True),
        StructField("processed_at", StringType(), True),
        StructField("data", MapType(StringType(), StringType()), True),
    ]
)


def process_orders_table(spark: SparkSession, input_path: str, output_path: str) -> int:
    """Process orders table records from JSON to ORC."""
    logger.info("Processing orders from: %s", input_path)

    df = spark.read.schema(STREAM_RECORD_SCHEMA).json(input_path)

    orders_df = (
        df.filter(F.col("table_name") == "orders")
        .select(
            F.col("event_name").alias("cdc_operation"),
            F.col("data.order_id").alias("order_id"),
            F.col("data.user_id").alias("user_id"),
            F.col("data.restaurant_id").alias("restaurant_id"),
            F.col("data.status").alias("status"),
            F.col("data.total_amount").cast(DoubleType()).alias("total_amount"),
            F.col("data.delivery_fee").cast(DoubleType()).alias("delivery_fee"),
            F.col("data.payment_method").alias("payment_method"),
            F.col("data.city").alias("city"),
            F.col("data.latitude").cast(DoubleType()).alias("latitude"),
            F.col("data.longitude").cast(DoubleType()).alias("longitude"),
            F.col("data.created_at").alias("created_at"),
            F.col("data.updated_at").alias("updated_at"),
            F.col("processed_at"),
        )
        .withColumn("dt", F.to_date(F.col("processed_at")))
        .withColumn("hour", F.hour(F.col("processed_at")))
    )

    # Deduplicate by order_id, keeping latest update
    orders_deduped = orders_df.dropDuplicates(["order_id"])

    record_count = orders_deduped.count()
    logger.info("Writing %d deduplicated order records to ORC", record_count)

    orders_deduped.write.mode("append").partitionBy("dt", "hour").format("orc").option(
        "compression", "snappy"
    ).save(f"{output_path}/orders")

    return record_count


def process_payments_table(
    spark: SparkSession, input_path: str, output_path: str
) -> int:
    """Process payments table records from JSON to ORC."""
    logger.info("Processing payments from: %s", input_path)

    df = spark.read.schema(STREAM_RECORD_SCHEMA).json(input_path)

    payments_df = (
        df.filter(F.col("table_name") == "payments")
        .select(
            F.col("event_name").alias("cdc_operation"),
            F.col("data.payment_id").alias("payment_id"),
            F.col("data.order_id").alias("order_id"),
            F.col("data.amount").cast(DoubleType()).alias("amount"),
            F.col("data.payment_method").alias("payment_method"),
            F.col("data.payment_status").alias("payment_status"),
            F.col("data.transaction_id").alias("transaction_id"),
            F.col("processed_at"),
        )
        .withColumn("dt", F.to_date(F.col("processed_at")))
    )

    record_count = payments_df.count()
    logger.info("Writing %d payment records to ORC", record_count)

    payments_df.write.mode("append").partitionBy("dt").format("orc").option(
        "compression", "snappy"
    ).save(f"{output_path}/payments")

    return record_count


def process_user_locations_table(
    spark: SparkSession, input_path: str, output_path: str
) -> int:
    """Process user_locations table records from JSON to ORC."""
    logger.info("Processing user locations from: %s", input_path)

    df = spark.read.schema(STREAM_RECORD_SCHEMA).json(input_path)

    locations_df = (
        df.filter(F.col("table_name").contains("location"))
        .select(
            F.col("data.user_id").alias("user_id"),
            F.col("data.latitude").cast(DoubleType()).alias("latitude"),
            F.col("data.longitude").cast(DoubleType()).alias("longitude"),
            F.col("data.timestamp").alias("event_timestamp"),
            F.col("processed_at"),
        )
        .withColumn("dt", F.to_date(F.col("processed_at")))
        .withColumn("hour", F.hour(F.col("processed_at")))
    )

    record_count = locations_df.count()
    logger.info("Writing %d location records to ORC", record_count)

    locations_df.write.mode("append").partitionBy("dt", "hour").format("orc").option(
        "compression", "snappy"
    ).save(f"{output_path}/user_locations")

    return record_count


def run_spark_job(
    s3_raw_bucket: str,
    s3_output_bucket: str,
    processing_date: str | None = None,
) -> dict:
    """Main entry point for the Spark ORC conversion job."""
    spark = create_spark_session()

    if processing_date is None:
        processing_date = (datetime.utcnow() - timedelta(hours=1)).strftime("%Y/%m/%d/%H")

    input_path = f"s3a://{s3_raw_bucket}/pipeline3-dynamodb/json-raw/*/{processing_date}"
    output_path = f"s3a://{s3_output_bucket}/pipeline3-dynamodb/orc"

    logger.info("Input path: %s", input_path)
    logger.info("Output path: %s", output_path)

    results = {
        "processing_date": processing_date,
        "start_time": datetime.utcnow().isoformat(),
    }

    try:
        results["orders_count"] = process_orders_table(spark, input_path, output_path)
        results["payments_count"] = process_payments_table(spark, input_path, output_path)
        results["locations_count"] = process_user_locations_table(
            spark, input_path, output_path
        )
        results["status"] = "SUCCESS"
    except Exception:
        logger.exception("Spark job failed")
        results["status"] = "FAILED"
        raise
    finally:
        results["end_time"] = datetime.utcnow().isoformat()
        logger.info("Job results: %s", results)
        spark.stop()

    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="DynamoDB Streams → ORC conversion")
    parser.add_argument("--raw-bucket", required=True, help="S3 bucket with raw JSON")
    parser.add_argument("--output-bucket", required=True, help="S3 bucket for ORC output")
    parser.add_argument("--date", default=None, help="Processing date (YYYY/MM/DD/HH)")
    args = parser.parse_args()

    result = run_spark_job(args.raw_bucket, args.output_bucket, args.date)

    if result["status"] != "SUCCESS":
        sys.exit(1)
