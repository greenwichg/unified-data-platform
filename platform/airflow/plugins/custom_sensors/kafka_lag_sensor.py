"""
Custom Airflow Sensor: MSK Consumer Group Lag Sensor

Waits for an Amazon MSK consumer group's total lag to drop below a configured
threshold. Useful for gating tasks that depend on near-real-time data
being fully consumed (e.g., before running Athena queries over Iceberg
tables populated by Flink consumers).

Connects to Amazon MSK using IAM authentication or mTLS depending on
cluster configuration.

Example usage:
    wait_for_lag = KafkaLagSensor(
        task_id="wait_for_cdc_lag",
        bootstrap_servers="b-1.msk-cluster.xxxx.kafka.ap-south-1.amazonaws.com:9098",
        consumer_group="flink-cdc-processor",
        max_lag=10000,
        topics=["orders", "users"],
    )
"""

import logging
from typing import Optional, Sequence

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context

logger = logging.getLogger(__name__)


class KafkaLagSensor(BaseSensorOperator):
    """
    Sensor that waits for MSK consumer group lag to drop below a threshold.

    :param bootstrap_servers: Amazon MSK bootstrap server addresses.
    :param consumer_group: Consumer group ID to monitor.
    :param max_lag: Maximum acceptable total lag across all partitions.
    :param topics: If set, only monitor lag for these specific topics.
        If None, monitors all topics the consumer group is subscribed to.
    :param per_partition_max_lag: If set, also check that no single
        partition exceeds this lag. Default None (only total lag checked).
    :param msk_config: Optional dict of additional MSK client configuration
        (e.g., security.protocol, sasl.mechanism for IAM auth).
    """

    template_fields: Sequence[str] = ("bootstrap_servers", "consumer_group")
    ui_color = "#b8d4e3"

    def __init__(
        self,
        bootstrap_servers: str,
        consumer_group: str,
        max_lag: int = 10000,
        topics: Optional[list] = None,
        per_partition_max_lag: Optional[int] = None,
        msk_config: Optional[dict] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bootstrap_servers = bootstrap_servers
        self.consumer_group = consumer_group
        self.max_lag = max_lag
        self.topics = topics
        self.per_partition_max_lag = per_partition_max_lag
        self.msk_config = msk_config or {}

    def _get_consumer_lag(self) -> dict:
        """
        Query Amazon MSK for consumer group lag using confluent_kafka AdminClient.

        Supports MSK IAM authentication via additional msk_config parameters.

        Returns a dict with:
            - total_lag: sum of lag across all monitored partitions
            - partition_lags: dict of (topic, partition) -> lag
            - error: error message if any
        """
        from confluent_kafka.admin import AdminClient
        from confluent_kafka import Consumer, TopicPartition

        result = {"total_lag": 0, "partition_lags": {}, "error": None}

        # Base MSK client configuration
        base_config = {"bootstrap.servers": self.bootstrap_servers}
        base_config.update(self.msk_config)

        try:
            admin = AdminClient(base_config)

            # Create a temporary consumer to fetch committed offsets
            consumer_config = {
                **base_config,
                "group.id": self.consumer_group,
                "enable.auto.commit": False,
            }
            consumer = Consumer(consumer_config)

            # Get metadata to find topic partitions
            metadata = admin.list_topics(timeout=10)

            topic_partitions = []
            for topic_name, topic_meta in metadata.topics.items():
                # Filter to monitored topics if specified
                if self.topics and topic_name not in self.topics:
                    continue
                # Skip internal topics
                if topic_name.startswith("_") or topic_name.startswith("__"):
                    continue

                for partition_id in topic_meta.partitions:
                    topic_partitions.append(
                        TopicPartition(topic_name, partition_id)
                    )

            if not topic_partitions:
                logger.warning("No topic partitions found for monitoring.")
                consumer.close()
                return result

            # Get committed offsets for the consumer group
            committed = consumer.committed(topic_partitions, timeout=10)

            # Get end offsets (high watermarks)
            total_lag = 0
            partition_lags = {}

            for tp in committed:
                if tp.offset < 0:
                    # No committed offset yet - skip
                    continue

                # Get the high watermark for this partition
                lo, hi = consumer.get_watermark_offsets(
                    tp, timeout=10, cached=False
                )

                lag = max(0, hi - tp.offset)
                partition_lags[(tp.topic, tp.partition)] = lag
                total_lag += lag

            result["total_lag"] = total_lag
            result["partition_lags"] = partition_lags

            consumer.close()

        except Exception as e:
            logger.error("Error fetching MSK consumer lag: %s", str(e))
            result["error"] = str(e)

        return result

    def poke(self, context: Context) -> bool:
        """
        Check if consumer group lag is below the threshold.
        """
        logger.info(
            "Checking MSK lag for group '%s' (max_lag=%d, topics=%s)",
            self.consumer_group,
            self.max_lag,
            self.topics or "all",
        )

        lag_info = self._get_consumer_lag()

        if lag_info["error"]:
            logger.warning(
                "Error fetching lag, will retry: %s", lag_info["error"]
            )
            return False

        total_lag = lag_info["total_lag"]
        partition_lags = lag_info["partition_lags"]

        logger.info(
            "Consumer group '%s' total lag: %d (threshold: %d, partitions: %d)",
            self.consumer_group,
            total_lag,
            self.max_lag,
            len(partition_lags),
        )

        # Check total lag
        if total_lag > self.max_lag:
            logger.info(
                "Total lag %d exceeds threshold %d. Waiting...",
                total_lag,
                self.max_lag,
            )
            return False

        # Check per-partition lag if configured
        if self.per_partition_max_lag:
            hot_partitions = {
                f"{tp[0]}-{tp[1]}": lag
                for tp, lag in partition_lags.items()
                if lag > self.per_partition_max_lag
            }

            if hot_partitions:
                logger.info(
                    "Partitions exceeding per-partition threshold %d: %s",
                    self.per_partition_max_lag,
                    hot_partitions,
                )
                return False

        logger.info(
            "Consumer group '%s' lag is within threshold. Proceeding.",
            self.consumer_group,
        )
        return True
