"""
Pipeline 3 - Spark Job: S3 JSON → ORC → S3 (Data Lake)

Runs on Amazon EMR. Reads raw JSON events from DynamoDB Streams,
applies transformations and deduplication, and writes optimized ORC
files to the data lake.

DynamoDB tables processed:
  - zomato-user-sessions      → user session activity and page views
  - zomato-delivery-tracking  → real-time delivery partner GPS and status
  - zomato-restaurant-inventory → restaurant menu availability and stock
  - zomato-user-preferences   → user dietary filters and favorites
"""

import logging
import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
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


# Schema for the JSON records written by the DynamoDB Streams processor
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


def process_user_sessions(spark: SparkSession, input_path: str, output_path: str) -> int:
    """Process zomato-user-sessions records from JSON to ORC."""
    logger.info("Processing user sessions from: %s", input_path)

    df = spark.read.schema(STREAM_RECORD_SCHEMA).json(input_path)

    sessions_df = (
        df.filter(F.col("table_name") == "zomato-user-sessions")
        .select(
            F.col("event_name").alias("cdc_operation"),
            F.col("data.user_id").alias("user_id"),
            F.col("data.session_id").alias("session_id"),
            F.col("data.device_type").alias("device_type"),
            F.col("data.app_version").alias("app_version"),
            F.col("data.pages_viewed").cast(IntegerType()).alias("pages_viewed"),
            F.col("data.actions_count").cast(IntegerType()).alias("actions_count"),
            F.col("data.city").alias("city"),
            F.col("data.started_at").alias("started_at"),
            F.col("data.ended_at").alias("ended_at"),
            F.col("data.is_active").cast(BooleanType()).alias("is_active"),
            F.col("data.session_duration_secs").cast(LongType()).alias("session_duration_secs"),
            F.col("processed_at"),
        )
        .withColumn("dt", F.to_date(F.col("processed_at")))
        .withColumn("hour", F.hour(F.col("processed_at")))
    )

    # Deduplicate by session_id keeping latest event
    sessions_deduped = sessions_df.dropDuplicates(["session_id"])

    record_count = sessions_deduped.count()
    logger.info("Writing %d deduplicated user session records to ORC", record_count)

    sessions_deduped.write.mode("append").partitionBy("dt", "hour").format("orc").option(
        "compression", "snappy"
    ).save(f"{output_path}/user_sessions")

    return record_count


def process_delivery_tracking(spark: SparkSession, input_path: str, output_path: str) -> int:
    """Process zomato-delivery-tracking records from JSON to ORC."""
    logger.info("Processing delivery tracking from: %s", input_path)

    df = spark.read.schema(STREAM_RECORD_SCHEMA).json(input_path)

    tracking_df = (
        df.filter(F.col("table_name") == "zomato-delivery-tracking")
        .select(
            F.col("event_name").alias("cdc_operation"),
            F.col("data.delivery_id").alias("delivery_id"),
            F.col("data.order_id").alias("order_id"),
            F.col("data.partner_id").alias("partner_id"),
            F.col("data.latitude").cast(DoubleType()).alias("latitude"),
            F.col("data.longitude").cast(DoubleType()).alias("longitude"),
            F.col("data.status").alias("status"),
            F.col("data.estimated_arrival").alias("estimated_arrival"),
            F.col("data.distance_km").cast(DoubleType()).alias("distance_km"),
            F.col("data.last_updated").alias("last_updated"),
            F.col("processed_at"),
        )
        .withColumn("dt", F.to_date(F.col("processed_at")))
        .withColumn("hour", F.hour(F.col("processed_at")))
    )

    # Deduplicate by delivery_id keeping latest GPS ping
    tracking_deduped = tracking_df.dropDuplicates(["delivery_id"])

    record_count = tracking_deduped.count()
    logger.info("Writing %d delivery tracking records to ORC", record_count)

    tracking_deduped.write.mode("append").partitionBy("dt", "hour").format("orc").option(
        "compression", "snappy"
    ).save(f"{output_path}/delivery_tracking")

    return record_count


def process_restaurant_inventory(
    spark: SparkSession, input_path: str, output_path: str
) -> int:
    """Process zomato-restaurant-inventory records from JSON to ORC."""
    logger.info("Processing restaurant inventory from: %s", input_path)

    df = spark.read.schema(STREAM_RECORD_SCHEMA).json(input_path)

    inventory_df = (
        df.filter(F.col("table_name") == "zomato-restaurant-inventory")
        .select(
            F.col("event_name").alias("cdc_operation"),
            F.col("data.restaurant_id").alias("restaurant_id"),
            F.col("data.item_id").alias("item_id"),
            F.col("data.item_name").alias("item_name"),
            F.col("data.is_available").cast(BooleanType()).alias("is_available"),
            F.col("data.stock_count").cast(IntegerType()).alias("stock_count"),
            F.col("data.preparation_time_mins").cast(IntegerType()).alias("preparation_time_mins"),
            F.col("data.last_updated").alias("last_updated"),
            F.col("processed_at"),
        )
        .withColumn("dt", F.to_date(F.col("processed_at")))
    )

    # Deduplicate by (restaurant_id, item_id) keeping latest availability
    inventory_deduped = inventory_df.dropDuplicates(["restaurant_id", "item_id"])

    record_count = inventory_deduped.count()
    logger.info("Writing %d restaurant inventory records to ORC", record_count)

    inventory_deduped.write.mode("append").partitionBy("dt").format("orc").option(
        "compression", "snappy"
    ).save(f"{output_path}/restaurant_inventory")

    return record_count


def process_user_preferences(
    spark: SparkSession, input_path: str, output_path: str
) -> int:
    """Process zomato-user-preferences records from JSON to ORC."""
    logger.info("Processing user preferences from: %s", input_path)

    df = spark.read.schema(STREAM_RECORD_SCHEMA).json(input_path)

    prefs_df = (
        df.filter(F.col("table_name") == "zomato-user-preferences")
        .select(
            F.col("event_name").alias("cdc_operation"),
            F.col("data.user_id").alias("user_id"),
            F.col("data.preferred_cuisines").alias("preferred_cuisines"),
            F.col("data.dietary_filters").alias("dietary_filters"),
            F.col("data.favorite_restaurants").alias("favorite_restaurants"),
            F.col("data.price_range").alias("price_range"),
            F.col("data.notifications_enabled").cast(BooleanType()).alias("notifications_enabled"),
            F.col("data.last_updated").alias("last_updated"),
            F.col("processed_at"),
        )
        .withColumn("dt", F.to_date(F.col("processed_at")))
    )

    # Deduplicate by user_id keeping latest preferences
    prefs_deduped = prefs_df.dropDuplicates(["user_id"])

    record_count = prefs_deduped.count()
    logger.info("Writing %d user preference records to ORC", record_count)

    prefs_deduped.write.mode("append").partitionBy("dt").format("orc").option(
        "compression", "snappy"
    ).save(f"{output_path}/user_preferences")

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
        results["user_sessions_count"] = process_user_sessions(spark, input_path, output_path)
        results["delivery_tracking_count"] = process_delivery_tracking(spark, input_path, output_path)
        results["restaurant_inventory_count"] = process_restaurant_inventory(
            spark, input_path, output_path
        )
        results["user_preferences_count"] = process_user_preferences(
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
