"""
Pipeline 4 - Flink Real-time Job: Kafka -> Flink SQL -> S3 (ORC) + Druid

Main entry point for the real-time event processing pipeline.
Reads app events from Kafka, applies CEP (order spike detection),
and sinks to both S3 (ORC) and Druid (via secondary Kafka topic).

Druid handles: 20B events/week, 8M queries/week, millisecond response.

Usage::

    python realtime_job.py
"""

from __future__ import annotations

import json
import logging
import os
import sys

from events import KAFKA_SOURCE_DDL
from s3_orc_sink import S3_SINK_DDL, INSERT_S3_SQL
from druid_sink import DRUID_SINK_DDL, INSERT_DRUID_SQL, generate_druid_ingestion_spec
from order_spike_detection import (
    ORDER_VELOCITY_VIEW_SQL,
    SPIKE_DETECTION_VIEW_SQL,
    ALERTS_SINK_DDL,
    INSERT_ALERTS_SQL,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("pipeline4_flink_realtime")


# ---------------------------------------------------------------------------
# All SQL statements in execution order
# ---------------------------------------------------------------------------

FLINK_SQL = {
    "create_kafka_events_source": KAFKA_SOURCE_DDL,
    "create_s3_orc_sink": S3_SINK_DDL,
    "create_druid_sink": DRUID_SINK_DDL,
    "insert_to_s3": INSERT_S3_SQL,
    "insert_to_druid": INSERT_DRUID_SQL,
    "create_order_velocity_view": ORDER_VELOCITY_VIEW_SQL,
    "create_spike_detection": SPIKE_DETECTION_VIEW_SQL,
    "create_alerts_sink": ALERTS_SINK_DDL,
    "insert_alerts": INSERT_ALERTS_SQL,
}


# ---------------------------------------------------------------------------
# Job config generator
# ---------------------------------------------------------------------------

def generate_flink_job_config(
    kafka_bootstrap: str,
    kafka_bootstrap_2: str,
    s3_bucket: str,
    checkpoint_dir: str,
) -> dict:
    """Generate the Flink job config for Pipeline 4."""
    return {
        "job_name": "zomato-realtime-events",
        "parallelism": 64,
        "checkpoint_interval_ms": 30000,
        "checkpoint_dir": checkpoint_dir,
        "state_backend": "rocksdb",
        "restart_strategy": {
            "type": "exponential-delay",
            "initial_backoff_ms": 1000,
            "max_backoff_ms": 60000,
            "backoff_multiplier": 2.0,
            "jitter_factor": 0.1,
        },
        "sql_statements": {
            key: stmt.format(
                kafka_bootstrap=kafka_bootstrap,
                kafka_bootstrap_2=kafka_bootstrap_2,
                s3_bucket=s3_bucket,
            )
            for key, stmt in FLINK_SQL.items()
        },
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main() -> None:
    required = ["MSK_BOOTSTRAP", "MSK_BOOTSTRAP_2", "S3_BUCKET"]
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        print(
            f"ERROR: Required environment variables not set: {', '.join(missing)}",
            file=sys.stderr,
        )
        sys.exit(1)

    flink_config = generate_flink_job_config(
        kafka_bootstrap=os.environ["MSK_BOOTSTRAP"],
        kafka_bootstrap_2=os.environ["MSK_BOOTSTRAP_2"],
        s3_bucket=os.environ["S3_BUCKET"],
        checkpoint_dir=os.environ.get(
            "CHECKPOINT_DIR",
            f"s3://{os.environ['S3_BUCKET'].replace('raw-data-lake', 'checkpoints')}/flink/pipeline4",
        ),
    )

    output_dir = os.environ.get("CONFIG_OUTPUT_DIR", "/tmp/pipeline4")
    os.makedirs(output_dir, exist_ok=True)

    with open(f"{output_dir}/flink_job_config.json", "w") as f:
        json.dump(flink_config, f, indent=2)

    druid_spec = generate_druid_ingestion_spec(
        kafka_bootstrap=os.environ["MSK_BOOTSTRAP_2"],
    )
    with open(f"{output_dir}/druid_ingestion_spec.json", "w") as f:
        json.dump(druid_spec, f, indent=2)

    logger.info("Pipeline 4 configs generated in %s", output_dir)


if __name__ == "__main__":
    main()
