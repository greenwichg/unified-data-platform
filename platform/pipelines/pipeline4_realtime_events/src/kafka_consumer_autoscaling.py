"""
Pipeline 4 - EC2 Auto-Scaling Kafka Consumer -> Druid Feeder

Consumes events from multiple Kafka topics on the primary cluster,
performs lightweight enrichment and batching, then produces to the
druid-ingestion-events topic on the secondary Kafka cluster.

Designed to run on an EC2 Auto Scaling Group. Scaling is driven by
consumer lag CloudWatch metrics published by this process.

Architecture: Kafka Cluster 1 -> [EC2 ASG Consumer Fleet] -> Kafka Cluster 2 -> Druid
"""

import json
import logging
import os
import signal
import sys
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import boto3
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("pipeline4_kafka_consumer_autoscaling")


@dataclass
class ConsumerConfig:
    """Configuration for the Kafka consumer fleet."""

    source_bootstrap_servers: str = field(default_factory=lambda: os.environ["MSK_BOOTSTRAP"])
    target_bootstrap_servers: str = field(default_factory=lambda: os.environ["MSK_BOOTSTRAP_2"])
    source_topics: list[str] = field(
        default_factory=lambda: ["orders", "users", "menu", "promo"]
    )
    target_topic: str = "druid-ingestion-events"
    consumer_group: str = "ec2-druid-feeder"
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
        import urllib.request

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

            total_lag = 0
            topic_lags: dict[str, int] = {}

            for topic in self.config.source_topics:
                metadata = consumer.list_topics(topic, timeout=10)
                if topic not in metadata.topics:
                    continue

                partitions = metadata.topics[topic].partitions
                topic_lag = 0

                for partition_id in partitions:
                    from confluent_kafka import TopicPartition

                    tp = TopicPartition(topic, partition_id)

                    # Get committed offset
                    committed = consumer.committed([tp], timeout=10)
                    committed_offset = committed[0].offset if committed[0].offset >= 0 else 0

                    # Get high watermark
                    low, high = consumer.get_watermark_offsets(tp, timeout=10)
                    partition_lag = max(0, high - committed_offset)
                    topic_lag += partition_lag

                topic_lags[topic] = topic_lag
                total_lag += topic_lag

            consumer.close()

            # Publish metrics to CloudWatch
            metric_data = [
                {
                    "MetricName": "ConsumerGroupLag",
                    "Dimensions": [
                        {"Name": "ConsumerGroup", "Value": self.config.consumer_group},
                        {"Name": "InstanceId", "Value": self.config.instance_id},
                    ],
                    "Value": total_lag,
                    "Unit": "Count",
                    "Timestamp": datetime.now(timezone.utc),
                },
            ]

            for topic, lag in topic_lags.items():
                metric_data.append(
                    {
                        "MetricName": "ConsumerTopicLag",
                        "Dimensions": [
                            {"Name": "ConsumerGroup", "Value": self.config.consumer_group},
                            {"Name": "Topic", "Value": topic},
                        ],
                        "Value": lag,
                        "Unit": "Count",
                        "Timestamp": datetime.now(timezone.utc),
                    }
                )

            self.cloudwatch.put_metric_data(
                Namespace=self.config.cloudwatch_namespace,
                MetricData=metric_data,
            )

            logger.info(
                "Published lag metrics: total_lag=%d, topic_lags=%s",
                total_lag,
                topic_lags,
            )

        except Exception:
            logger.exception("Error computing consumer lag")
            raise


class DruidFeederConsumer:
    """
    Kafka consumer that reads from source topics, enriches events,
    and produces to the Druid ingestion topic.

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

        self.producer = Producer(
            {
                "bootstrap.servers": config.target_bootstrap_servers,
                "acks": "all",
                "retries": 5,
                "retry.backoff.ms": 200,
                "batch.size": 65536,
                "linger.ms": 10,
                "buffer.memory": 67108864,
                "compression.type": "lz4",
                "max.in.flight.requests.per.connection": 5,
                "enable.idempotence": True,
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "AWS_MSK_IAM",
                "sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
                "sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
            }
        )

        self.lag_monitor = ConsumerLagMonitor(config)

    def _enrich_event(self, raw_event: dict[str, Any], source_topic: str) -> dict[str, Any]:
        """
        Enrich the raw Kafka event with metadata needed by Druid.
        Normalizes the event structure across different source topics.
        """
        enriched = {
            "event_id": raw_event.get("event_id") or raw_event.get("order_id") or raw_event.get("id"),
            "event_type": raw_event.get("event_type", source_topic),
            "source": source_topic,
            "user_id": raw_event.get("user_id"),
            "session_id": raw_event.get("session_id"),
            "event_timestamp": raw_event.get("event_timestamp")
            or raw_event.get("order_timestamp")
            or raw_event.get("updated_at")
            or datetime.now(timezone.utc).isoformat(),
            "city": raw_event.get("city"),
            "order_id": raw_event.get("order_id"),
            "restaurant_id": raw_event.get("restaurant_id"),
            "total_amount": raw_event.get("total_amount"),
            "delivery_time_minutes": raw_event.get("delivery_time_minutes"),
            "page_name": raw_event.get("page_name"),
            "action": raw_event.get("action"),
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "ingestion_instance": self.config.instance_id,
        }

        # Preserve nested structures for Druid flattenSpec
        if "device" in raw_event:
            enriched["device"] = raw_event["device"]
        if "location" in raw_event:
            enriched["location"] = raw_event["location"]
        if "properties" in raw_event:
            enriched["properties"] = raw_event["properties"]

        return enriched

    def _delivery_callback(self, err, msg) -> None:
        """Callback for produced message delivery confirmation."""
        if err is not None:
            self._stats["produce_errors"] += 1
            logger.error("Message delivery failed: %s", err)
        else:
            self._stats["produced"] += 1

    def _flush_batch(self) -> None:
        """Flush the accumulated batch to the target Kafka topic."""
        if not self._batch:
            return

        batch_size = len(self._batch)
        logger.info("Flushing batch of %d events to %s", batch_size, self.config.target_topic)

        for event in self._batch:
            try:
                key = event.get("user_id", "").encode("utf-8") if event.get("user_id") else None
                value = json.dumps(event, default=str).encode("utf-8")
                self.producer.produce(
                    topic=self.config.target_topic,
                    key=key,
                    value=value,
                    callback=self._delivery_callback,
                )
            except BufferError:
                logger.warning("Producer buffer full, flushing and retrying")
                self.producer.flush(timeout=10)
                self.producer.produce(
                    topic=self.config.target_topic,
                    key=key,
                    value=value,
                    callback=self._delivery_callback,
                )

        self.producer.flush(timeout=30)
        self._stats["batches_flushed"] += 1
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
            "Consumer stats: consumed=%d, produced=%d, errors=%d, batches=%d",
            self._stats["consumed"],
            self._stats["produced"],
            self._stats["produce_errors"],
            self._stats["batches_flushed"],
        )

    def start(self) -> None:
        """Start consuming and producing events."""
        self._running = True

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

        logger.info(
            "Starting Druid feeder consumer: source_topics=%s, target_topic=%s, instance=%s",
            self.config.source_topics,
            self.config.target_topic,
            self.config.instance_id,
        )

        self.consumer.subscribe(self.config.source_topics)
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
                    raw_event = json.loads(msg.value().decode("utf-8"))
                    source_topic = msg.topic()
                    enriched = self._enrich_event(raw_event, source_topic)
                    self._batch.append(enriched)
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

                # Periodically log stats
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
        self.producer.flush(timeout=30)
        self._log_stats()
        logger.info("Consumer shutdown complete")


def main() -> None:
    """Entry point for the EC2 auto-scaling Kafka consumer."""
    config = ConsumerConfig(
        source_bootstrap_servers=os.getenv(
            "MSK_BOOTSTRAP",
            "b-1.zomato-msk.xxxxx.c2.kafka.ap-south-1.amazonaws.com:9098,b-2.zomato-msk.xxxxx.c2.kafka.ap-south-1.amazonaws.com:9098,b-3.zomato-msk.xxxxx.c2.kafka.ap-south-1.amazonaws.com:9098",
        ),
        target_bootstrap_servers=os.getenv(
            "MSK_BOOTSTRAP_2",
            "b-1.zomato-msk-rt.xxxxx.c2.kafka.ap-south-1.amazonaws.com:9098,b-2.zomato-msk-rt.xxxxx.c2.kafka.ap-south-1.amazonaws.com:9098,b-3.zomato-msk-rt.xxxxx.c2.kafka.ap-south-1.amazonaws.com:9098",
        ),
        source_topics=os.getenv("SOURCE_TOPICS", "orders,users,menu,promo").split(","),
        target_topic=os.getenv("TARGET_TOPIC", "druid-ingestion-events"),
        consumer_group=os.getenv("CONSUMER_GROUP", "ec2-druid-feeder"),
        batch_size=int(os.getenv("BATCH_SIZE", "5000")),
        flush_interval_ms=int(os.getenv("FLUSH_INTERVAL_MS", "1000")),
    )

    consumer = DruidFeederConsumer(config)
    consumer.start()


if __name__ == "__main__":
    main()
