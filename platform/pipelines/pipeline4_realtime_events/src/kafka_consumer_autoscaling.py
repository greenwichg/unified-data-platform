"""
Pipeline 4 - EC2 Auto-Scaling Kafka Consumer -> Druid Feeder

Consumes enriched events from the druid-ingestion-events topic on MSK Cluster 2,
batches them, and pushes to Apache Druid via its HTTP indexing service API.

Designed to run on an EC2 Auto Scaling Group. Scaling is driven by
consumer lag CloudWatch metrics published by this process.

Architecture: Flink → MSK Cluster 2 (druid-ingestion-events) → [EC2 ASG Consumer Fleet] → Druid
"""

import json
import logging
import os
import signal
import sys
import threading
import time
import urllib.request
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import boto3
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("pipeline4_kafka_consumer_autoscaling")


@dataclass
class ConsumerConfig:
    """Configuration for the Kafka consumer fleet."""

    # MSK Cluster 2 — receives enriched events written by Flink
    source_bootstrap_servers: str = field(default_factory=lambda: os.environ["MSK_BOOTSTRAP_2"])
    source_topic: str = "druid-ingestion-events"
    consumer_group: str = "ec2-druid-feeder"

    # Druid indexing service endpoint
    druid_indexer_url: str = field(
        default_factory=lambda: os.environ.get(
            "DRUID_INDEXER_URL", "http://druid-router.zomato-data.internal:8888"
        )
    )
    druid_datasource: str = "zomato_realtime_events"

    batch_size: int = 5000
    flush_interval_ms: int = 1000
    poll_timeout_seconds: float = 1.0
    max_poll_records: int = 10000
    cloudwatch_namespace: str = "zomato-data-platform/pipeline4-realtime"
    cloudwatch_metric_interval_seconds: int = 30
    instance_id: str = field(default_factory=lambda: _get_instance_id())


def _get_instance_id() -> str:
    """Retrieve the EC2 instance ID from IMDS v2."""
    try:
        token_req = urllib.request.Request(
            "http://169.254.169.254/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "300"},
            method="PUT",
        )
        token = urllib.request.urlopen(token_req, timeout=2).read().decode()
        id_req = urllib.request.Request(
            "http://169.254.169.254/latest/meta-data/instance-id",
            headers={"X-aws-ec2-metadata-token": token},
        )
        return urllib.request.urlopen(id_req, timeout=2).read().decode()
    except Exception:
        return os.getenv("HOSTNAME", "unknown")


class ConsumerLagMonitor:
    """Publishes consumer lag metrics to CloudWatch for ASG scaling decisions."""

    def __init__(self, config: ConsumerConfig):
        self.config = config
        self.cloudwatch = boto3.client("cloudwatch", region_name="ap-south-1")
        self.admin_client = AdminClient(
            {
                "bootstrap.servers": config.source_bootstrap_servers,
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "AWS_MSK_IAM",
                "sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
                "sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
            }
        )
        self._running = False
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the background metric publishing thread."""
        self._running = True
        self._thread = threading.Thread(target=self._publish_loop, daemon=True)
        self._thread.start()
        logger.info("Consumer lag monitor started")

    def stop(self) -> None:
        """Stop the background metric publishing thread."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=10)
        logger.info("Consumer lag monitor stopped")

    def _publish_loop(self) -> None:
        """Periodically compute and publish consumer lag to CloudWatch."""
        while self._running:
            try:
                self._publish_metrics()
            except Exception:
                logger.exception("Failed to publish consumer lag metrics")
            time.sleep(self.config.cloudwatch_metric_interval_seconds)

    def _publish_metrics(self) -> None:
        """Compute consumer group lag and publish to CloudWatch."""
        try:
            consumer = Consumer(
                {
                    "bootstrap.servers": self.config.source_bootstrap_servers,
                    "group.id": self.config.consumer_group,
                    "enable.auto.commit": False,
                    "security.protocol": "SASL_SSL",
                    "sasl.mechanism": "AWS_MSK_IAM",
                    "sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
                    "sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
                }
            )

            metadata = consumer.list_topics(self.config.source_topic, timeout=10)
            total_lag = 0

            if self.config.source_topic in metadata.topics:
                partitions = metadata.topics[self.config.source_topic].partitions
                for partition_id in partitions:
                    from confluent_kafka import TopicPartition

                    tp = TopicPartition(self.config.source_topic, partition_id)
                    committed = consumer.committed([tp], timeout=10)
                    committed_offset = committed[0].offset if committed[0].offset >= 0 else 0
                    low, high = consumer.get_watermark_offsets(tp, timeout=10)
                    total_lag += max(0, high - committed_offset)

            consumer.close()

            metric_data = [
                {
                    "MetricName": "ConsumerGroupLag",
                    "Dimensions": [
                        {"Name": "ConsumerGroup", "Value": self.config.consumer_group},
                        {"Name": "Topic", "Value": self.config.source_topic},
                        {"Name": "InstanceId", "Value": self.config.instance_id},
                    ],
                    "Value": total_lag,
                    "Unit": "Count",
                    "Timestamp": datetime.now(timezone.utc),
                },
            ]

            self.cloudwatch.put_metric_data(
                Namespace=self.config.cloudwatch_namespace,
                MetricData=metric_data,
            )

            logger.info("Published lag metric: total_lag=%d (topic=%s)", total_lag, self.config.source_topic)

        except Exception:
            logger.exception("Error computing consumer lag")
            raise


class DruidFeederConsumer:
    """
    Kafka consumer that reads enriched events from MSK Cluster 2
    (druid-ingestion-events) and pushes batches to Druid via its
    HTTP indexing service API.

    Handles graceful shutdown via SIGTERM/SIGINT for clean ASG lifecycle.
    """

    def __init__(self, config: ConsumerConfig):
        self.config = config
        self._running = False
        self._batch: list[dict[str, Any]] = []
        self._last_flush_time = time.monotonic()
        self._stats = defaultdict(int)

        self.consumer = Consumer(
            {
                "bootstrap.servers": config.source_bootstrap_servers,
                "group.id": config.consumer_group,
                "auto.offset.reset": "latest",
                "enable.auto.commit": True,
                "auto.commit.interval.ms": 5000,
                "session.timeout.ms": 30000,
                "heartbeat.interval.ms": 10000,
                "max.poll.interval.ms": 300000,
                "fetch.min.bytes": 1048576,
                "fetch.wait.max.ms": 500,
                "max.partition.fetch.bytes": 10485760,
                "statistics.interval.ms": 30000,
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "AWS_MSK_IAM",
                "sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
                "sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
            }
        )

        self.lag_monitor = ConsumerLagMonitor(config)

    def _push_to_druid(self, batch: list[dict[str, Any]]) -> None:
        """Push a batch of events to Druid via the HTTP indexing service API."""
        payload = json.dumps(batch, default=str).encode("utf-8")
        url = (
            f"{self.config.druid_indexer_url}/druid/v2/datasources/"
            f"{self.config.druid_datasource}/insertEvents"
        )
        req = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                status = resp.status
                if status not in (200, 201):
                    raise RuntimeError(f"Druid returned HTTP {status}")
            self._stats["druid_batches_pushed"] += 1
            logger.info("Pushed %d events to Druid datasource %s", len(batch), self.config.druid_datasource)
        except Exception:
            self._stats["druid_errors"] += 1
            logger.exception("Failed to push batch to Druid (%d events)", len(batch))
            raise

    def _flush_batch(self) -> None:
        """Flush the accumulated batch to Druid."""
        if not self._batch:
            return

        batch_size = len(self._batch)
        logger.info("Flushing batch of %d events to Druid", batch_size)

        self._push_to_druid(self._batch)
        self._batch.clear()
        self._last_flush_time = time.monotonic()

    def _should_flush(self) -> bool:
        """Determine whether to flush the current batch."""
        if len(self._batch) >= self.config.batch_size:
            return True
        elapsed_ms = (time.monotonic() - self._last_flush_time) * 1000
        if self._batch and elapsed_ms >= self.config.flush_interval_ms:
            return True
        return False

    def _log_stats(self) -> None:
        """Log processing statistics."""
        logger.info(
            "Consumer stats: consumed=%d, druid_batches=%d, druid_errors=%d, parse_errors=%d",
            self._stats["consumed"],
            self._stats["druid_batches_pushed"],
            self._stats["druid_errors"],
            self._stats["parse_errors"],
        )

    def start(self) -> None:
        """Start consuming from MSK Cluster 2 and feeding Druid."""
        self._running = True

        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

        logger.info(
            "Starting Druid feeder consumer: source_topic=%s (Cluster 2), druid_url=%s, instance=%s",
            self.config.source_topic,
            self.config.druid_indexer_url,
            self.config.instance_id,
        )

        self.consumer.subscribe([self.config.source_topic])
        self.lag_monitor.start()

        stats_interval = 60
        last_stats_time = time.monotonic()

        try:
            while self._running:
                msg = self.consumer.poll(timeout=self.config.poll_timeout_seconds)

                if msg is None:
                    if self._should_flush():
                        self._flush_batch()
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Consumer error: %s", msg.error())
                    self._stats["consume_errors"] += 1
                    continue

                try:
                    event = json.loads(msg.value().decode("utf-8"))
                    self._batch.append(event)
                    self._stats["consumed"] += 1
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    self._stats["parse_errors"] += 1
                    logger.warning(
                        "Failed to parse message from %s partition %d offset %d: %s",
                        msg.topic(),
                        msg.partition(),
                        msg.offset(),
                        e,
                    )
                    continue

                if self._should_flush():
                    self._flush_batch()

                if time.monotonic() - last_stats_time >= stats_interval:
                    self._log_stats()
                    last_stats_time = time.monotonic()

        except KafkaException as e:
            logger.exception("Fatal Kafka error: %s", e)
            sys.exit(1)
        finally:
            self._shutdown()

    def _handle_shutdown(self, signum: int, frame: Any) -> None:
        """Handle SIGTERM/SIGINT for graceful shutdown during ASG scale-in."""
        sig_name = signal.Signals(signum).name
        logger.info("Received %s, initiating graceful shutdown", sig_name)
        self._running = False

    def _shutdown(self) -> None:
        """Perform graceful shutdown: flush pending data and close connections."""
        logger.info("Shutting down consumer...")
        self._flush_batch()
        self.lag_monitor.stop()
        self.consumer.close()
        self._log_stats()
        logger.info("Consumer shutdown complete")


def main() -> None:
    """Entry point for the EC2 auto-scaling Kafka consumer."""
    config = ConsumerConfig(
        source_bootstrap_servers=os.getenv(
            "MSK_BOOTSTRAP_2",
            "b-1.zomato-msk-rt.xxxxx.c2.kafka.ap-south-1.amazonaws.com:9098,b-2.zomato-msk-rt.xxxxx.c2.kafka.ap-south-1.amazonaws.com:9098,b-3.zomato-msk-rt.xxxxx.c2.kafka.ap-south-1.amazonaws.com:9098",
        ),
        source_topic=os.getenv("SOURCE_TOPIC", "druid-ingestion-events"),
        druid_indexer_url=os.getenv("DRUID_INDEXER_URL", "http://druid-router.zomato-data.internal:8888"),
        druid_datasource=os.getenv("DRUID_DATASOURCE", "zomato_realtime_events"),
        consumer_group=os.getenv("CONSUMER_GROUP", "ec2-druid-feeder"),
        batch_size=int(os.getenv("BATCH_SIZE", "5000")),
        flush_interval_ms=int(os.getenv("FLUSH_INTERVAL_MS", "1000")),
    )

    consumer = DruidFeederConsumer(config)
    consumer.start()


if __name__ == "__main__":
    main()
