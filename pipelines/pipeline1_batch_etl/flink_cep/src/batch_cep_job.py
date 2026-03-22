"""
PyFlink equivalent of BatchCEPJob.java.

Pipeline 1 - Batch CEP Job

Reads order events from Kafka (MSK with IAM auth), applies fraud detection
and cancellation anomaly detection patterns, then writes pattern matches
to Apache Iceberg tables on S3 in ORC format.

Converted from: com.zomato.pipeline1.BatchCEPJob

Usage::

    python batch_cep_job.py
"""

from __future__ import annotations

import json
import logging
import os
import sys

from pyflink.common import (
    Duration,
    WatermarkStrategy,
    Types,
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import (
    StreamExecutionEnvironment,
    CheckpointingMode,
    ExternalizedCheckpointCleanup,
)
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
from pyflink.table import StreamTableEnvironment

from events import OrderEvent
from fraud_detection_pattern import FraudAlert, apply as apply_fraud_detection
from order_anomaly_pattern import CancellationAnomalyAlert, apply as apply_order_anomaly
from iceberg_sink import IcebergSink

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default configuration values (overridden by env vars or system properties)
# ---------------------------------------------------------------------------

DEFAULT_KAFKA_BOOTSTRAP: str = (
    "b-1.msk-zomato-data.kafka.ap-south-1.amazonaws.com:9098,"
    "b-2.msk-zomato-data.kafka.ap-south-1.amazonaws.com:9098,"
    "b-3.msk-zomato-data.kafka.ap-south-1.amazonaws.com:9098"
)
DEFAULT_KAFKA_TOPIC: str = "orders"
DEFAULT_CONSUMER_GROUP: str = "pipeline1-batch-cep"
DEFAULT_S3_BUCKET: str = "zomato-data-platform-prod-raw-data-lake"
DEFAULT_CHECKPOINT_DIR: str = (
    "s3://zomato-data-platform-prod-checkpoints/flink/pipeline1"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_config(key: str, default: str) -> str:
    """Resolve a configuration value.

    Checks (in order):
      1. ``-D<key>=<value>`` style system property passed via command-line (not
         natively available in Python, so we fall back to env vars only).
      2. Environment variable derived from *key* by upper-casing and replacing
         dots with underscores.
      3. The supplied *default*.
    """
    env_key: str = key.upper().replace(".", "_")
    env_val: str | None = os.environ.get(env_key)
    if env_val:
        return env_val
    return default


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    logger.info("Starting Pipeline 1 - Batch CEP Job")

    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------
    kafka_bootstrap: str = _get_config("kafka.bootstrap.servers", DEFAULT_KAFKA_BOOTSTRAP)
    kafka_topic: str = _get_config("kafka.topic", DEFAULT_KAFKA_TOPIC)
    consumer_group: str = _get_config("kafka.consumer.group", DEFAULT_CONSUMER_GROUP)
    s3_bucket: str = _get_config("s3.bucket", DEFAULT_S3_BUCKET)
    checkpoint_dir: str = _get_config("checkpoint.dir", DEFAULT_CHECKPOINT_DIR)
    parallelism: int = int(_get_config("job.parallelism", "32"))

    # ------------------------------------------------------------------
    # Execution environment
    # ------------------------------------------------------------------
    env: StreamExecutionEnvironment = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(parallelism)

    # Checkpointing: exactly-once with RocksDB state backend
    env.enable_checkpointing(60_000, CheckpointingMode.EXACTLY_ONCE)
    cp_config = env.get_checkpoint_config()
    cp_config.set_min_pause_between_checkpoints(30_000)
    cp_config.set_checkpoint_timeout(600_000)
    cp_config.set_max_concurrent_checkpoints(1)
    cp_config.set_tolerable_checkpoint_failure_number(3)
    cp_config.set_externalized_checkpoint_cleanup(
        ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
    )
    cp_config.set_checkpoint_storage_dir(checkpoint_dir)

    # RocksDB state backend
    env.get_config().set_string("state.backend", "rocksdb")

    # Restart strategy: fixed-delay (10 attempts, 30 s delay)
    env.set_restart_strategy(
        env.get_config()  # type: ignore[arg-type]
    )
    # Use configuration-based restart strategy
    env.get_config().set_string("restart-strategy", "fixed-delay")
    env.get_config().set_string("restart-strategy.fixed-delay.attempts", "10")
    env.get_config().set_string("restart-strategy.fixed-delay.delay", "30s")

    table_env: StreamTableEnvironment = StreamTableEnvironment.create(env)

    # ------------------------------------------------------------------
    # Kafka source
    # ------------------------------------------------------------------
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(kafka_bootstrap)
        .set_topics(kafka_topic)
        .set_group_id(consumer_group)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .set_property("security.protocol", "SASL_SSL")
        .set_property("sasl.mechanism", "AWS_MSK_IAM")
        .set_property(
            "sasl.jaas.config",
            "software.amazon.msk.auth.iam.IAMLoginModule required;",
        )
        .set_property(
            "sasl.client.callback.handler.class",
            "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
        )
        .set_property("fetch.min.bytes", "1048576")
        .set_property("fetch.max.wait.ms", "500")
        .set_property("max.poll.records", "10000")
        .build()
    )

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(30))
        .with_timestamp_assigner(_OrderTimestampAssigner())
        .with_idleness(Duration.of_minutes(1))
    )

    # Read JSON strings from Kafka, then deserialize to OrderEvent
    raw_stream = env.from_source(
        kafka_source, watermark_strategy, "kafka-orders-source"
    ).uid("kafka-orders-source").name("Kafka Orders Source")

    order_stream = raw_stream.map(_deserialize_order_event).uid(
        "json-to-order-event"
    ).name("JSON to OrderEvent")

    logger.info(
        "Kafka source configured: bootstrap=%s, topic=%s, group=%s",
        kafka_bootstrap,
        kafka_topic,
        consumer_group,
    )

    # ------------------------------------------------------------------
    # Apply CEP patterns
    # ------------------------------------------------------------------

    # Pattern 1: Fraud detection (multi-address orders)
    fraud_alerts = apply_fraud_detection(order_stream).uid(
        "fraud-detection-cep"
    ).name("Fraud Detection CEP")

    # Pattern 2: Cancellation anomaly (spike detection)
    cancellation_alerts = apply_order_anomaly(order_stream).uid(
        "cancellation-anomaly-cep"
    ).name("Cancellation Anomaly CEP")

    # Log alert counts for monitoring
    fraud_alerts.map(
        lambda alert: (logger.info("FRAUD_ALERT: %s", alert), alert)[1]
    ).uid("fraud-alert-logger").name("Fraud Alert Logger")

    cancellation_alerts.map(
        lambda alert: (logger.info("CANCELLATION_ANOMALY: %s", alert), alert)[1]
    ).uid("cancellation-alert-logger").name("Cancellation Alert Logger")

    # ------------------------------------------------------------------
    # Iceberg sinks
    # ------------------------------------------------------------------
    iceberg_sink = IcebergSink(s3_bucket)
    iceberg_sink.setup_and_write(table_env, fraud_alerts, cancellation_alerts)

    logger.info(
        "Iceberg sinks configured: warehouse=s3://%s/pipeline1-batch-etl/iceberg",
        s3_bucket,
    )

    # ------------------------------------------------------------------
    # Execute
    # ------------------------------------------------------------------
    env.execute("Zomato Pipeline 1 - Batch CEP Job")


# ---------------------------------------------------------------------------
# Deserialization helpers
# ---------------------------------------------------------------------------

class _OrderTimestampAssigner:
    """Extracts event-time timestamps from JSON strings for watermark assignment.

    Used *before* full deserialization so that the watermark strategy can work
    on the raw Kafka source.  Falls back to current processing time if the
    JSON cannot be parsed.
    """

    def extract_timestamp(self, value: str, record_timestamp: int) -> int:
        try:
            data = json.loads(value)
            return int(data.get("updatedAt") or data.get("updated_at") or record_timestamp)
        except Exception:
            return record_timestamp


def _deserialize_order_event(raw_json: str) -> OrderEvent:
    """Deserialize a JSON string into an ``OrderEvent``."""
    data = json.loads(raw_json)
    return OrderEvent.from_dict(data)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()
