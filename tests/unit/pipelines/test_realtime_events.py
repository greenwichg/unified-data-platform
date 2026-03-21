"""
Unit tests for Pipeline 4 - Realtime Events (Kafka -> Flink -> S3 + Druid).

Tests cover:
  - Event producer configuration and topic routing
  - ZomatoEvent dataclass defaults
  - EventType enum completeness
  - Flink SQL windowing definitions
  - Druid ingestion spec generation
  - Flink job config for realtime pipeline
  - Spike detection thresholds
"""

import json
import os
import sys
import uuid
from dataclasses import asdict
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "pipelines",
        "pipeline4_realtime_events",
        "src",
    ),
)


# ---------------------------------------------------------------------------
# Event producer tests
# ---------------------------------------------------------------------------
class TestEventType:
    def test_import(self):
        from event_producer import EventType
        assert hasattr(EventType, "ORDER_PLACED")

    def test_all_event_types_defined(self):
        from event_producer import EventType
        expected = {
            "ORDER_PLACED", "ORDER_UPDATED", "ORDER_CANCELLED", "ORDER_DELIVERED",
            "USER_SIGNUP", "USER_LOGIN", "USER_LOCATION_UPDATE",
            "MENU_VIEWED", "MENU_ITEM_ADDED", "RESTAURANT_SEARCH",
            "PROMO_APPLIED", "PROMO_REDEEMED",
            "PAYMENT_INITIATED", "PAYMENT_COMPLETED",
            "DELIVERY_ASSIGNED", "DELIVERY_PICKED_UP", "DELIVERY_COMPLETED",
            "APP_OPENED", "PAGE_VIEWED", "RATING_SUBMITTED",
        }
        actual = {e.name for e in EventType}
        assert expected == actual


class TestEventTopicMapping:
    def test_order_events_route_to_orders_topic(self):
        from event_producer import EVENT_TOPIC_MAPPING, EventType
        order_types = [
            EventType.ORDER_PLACED, EventType.ORDER_UPDATED,
            EventType.ORDER_CANCELLED, EventType.ORDER_DELIVERED,
        ]
        for et in order_types:
            assert EVENT_TOPIC_MAPPING[et] == "orders", f"{et} should route to orders"

    def test_user_events_route_to_users_topic(self):
        from event_producer import EVENT_TOPIC_MAPPING, EventType
        assert EVENT_TOPIC_MAPPING[EventType.USER_SIGNUP] == "users"
        assert EVENT_TOPIC_MAPPING[EventType.USER_LOGIN] == "users"

    def test_menu_events_route_to_menu_topic(self):
        from event_producer import EVENT_TOPIC_MAPPING, EventType
        assert EVENT_TOPIC_MAPPING[EventType.MENU_VIEWED] == "menu"
        assert EVENT_TOPIC_MAPPING[EventType.RESTAURANT_SEARCH] == "menu"

    def test_promo_events_route_to_promo_topic(self):
        from event_producer import EVENT_TOPIC_MAPPING, EventType
        assert EVENT_TOPIC_MAPPING[EventType.PROMO_APPLIED] == "promo"
        assert EVENT_TOPIC_MAPPING[EventType.PROMO_REDEEMED] == "promo"

    def test_generic_events_route_to_topics(self):
        from event_producer import EVENT_TOPIC_MAPPING, EventType
        assert EVENT_TOPIC_MAPPING[EventType.APP_OPENED] == "topics"
        assert EVENT_TOPIC_MAPPING[EventType.PAGE_VIEWED] == "topics"

    def test_all_event_types_have_mapping(self):
        from event_producer import EVENT_TOPIC_MAPPING, EventType
        for et in EventType:
            assert et in EVENT_TOPIC_MAPPING, f"{et} missing from topic mapping"


class TestZomatoEvent:
    def test_default_event_id_generated(self):
        from event_producer import ZomatoEvent
        e = ZomatoEvent()
        assert e.event_id is not None
        uuid.UUID(e.event_id)  # validates it's a UUID

    def test_default_timestamp_set(self):
        from event_producer import ZomatoEvent
        e = ZomatoEvent()
        assert e.timestamp is not None

    def test_to_dict(self):
        from event_producer import ZomatoEvent
        e = ZomatoEvent(event_type="ORDER_PLACED", user_id="usr_123")
        d = asdict(e)
        assert d["event_type"] == "ORDER_PLACED"
        assert d["user_id"] == "usr_123"
        assert "event_id" in d

    def test_create_sample_order_event(self):
        from event_producer import create_sample_order_event
        event = create_sample_order_event("usr_001", "Delhi")
        assert event.event_type == "ORDER_PLACED"
        assert event.user_id == "usr_001"
        assert event.location["city"] == "Delhi"
        assert event.properties["total_amount"] == 750.0
        assert len(event.properties["items"]) > 0


class TestZomatoEventProducer:
    @patch("event_producer.Producer")
    def test_producer_config_idempotent(self, mock_producer_cls):
        from event_producer import ZomatoEventProducer
        producer = ZomatoEventProducer("localhost:9092")
        config = producer.producer_config
        assert config["enable.idempotence"] is True
        assert config["acks"] == "all"
        assert config["compression.type"] == "lz4"

    @patch("event_producer.Producer")
    def test_send_event_routes_to_correct_topic(self, mock_producer_cls):
        from event_producer import ZomatoEvent, ZomatoEventProducer
        mock_instance = MagicMock()
        mock_producer_cls.return_value = mock_instance

        producer = ZomatoEventProducer("localhost:9092")
        event = ZomatoEvent(event_type="ORDER_PLACED", user_id="usr_1")
        producer.send_event(event)

        mock_instance.produce.assert_called_once()
        call_kwargs = mock_instance.produce.call_args[1]
        assert call_kwargs["topic"] == "orders"

    @patch("event_producer.Producer")
    def test_send_event_unknown_type_goes_to_topics(self, mock_producer_cls):
        from event_producer import ZomatoEvent, ZomatoEventProducer
        mock_instance = MagicMock()
        mock_producer_cls.return_value = mock_instance

        producer = ZomatoEventProducer("localhost:9092")
        event = ZomatoEvent(event_type="TOTALLY_UNKNOWN", user_id="usr_1")
        producer.send_event(event)

        call_kwargs = mock_instance.produce.call_args[1]
        assert call_kwargs["topic"] == "topics"

    @patch("event_producer.Producer")
    def test_send_batch_flushes(self, mock_producer_cls):
        from event_producer import ZomatoEvent, ZomatoEventProducer
        mock_instance = MagicMock()
        mock_producer_cls.return_value = mock_instance

        producer = ZomatoEventProducer("localhost:9092")
        events = [
            ZomatoEvent(event_type="ORDER_PLACED", user_id="usr_1"),
            ZomatoEvent(event_type="USER_LOGIN", user_id="usr_2"),
        ]
        producer.send_batch(events)
        mock_instance.flush.assert_called_once()

    @patch("event_producer.Producer")
    def test_delivery_callback_increments_count(self, mock_producer_cls):
        from event_producer import ZomatoEventProducer
        mock_instance = MagicMock()
        mock_producer_cls.return_value = mock_instance

        producer = ZomatoEventProducer("localhost:9092")
        producer._delivery_callback(None, MagicMock())
        assert producer._delivery_count == 1

    @patch("event_producer.Producer")
    def test_delivery_callback_error(self, mock_producer_cls):
        from event_producer import ZomatoEventProducer
        mock_instance = MagicMock()
        mock_producer_cls.return_value = mock_instance

        producer = ZomatoEventProducer("localhost:9092")
        producer._delivery_callback(Exception("fail"), MagicMock())
        assert producer._error_count == 1


# ---------------------------------------------------------------------------
# Flink realtime processor tests
# ---------------------------------------------------------------------------
class TestFlinkRealtimeSQL:
    def test_import(self):
        from flink_realtime_processor import FLINK_SQL
        assert len(FLINK_SQL) > 0

    def test_kafka_events_source_uses_json_format(self):
        from flink_realtime_processor import FLINK_SQL
        stmt = FLINK_SQL["create_kafka_events_source"]
        assert "'format' = 'json'" in stmt

    def test_kafka_source_subscribes_all_topics(self):
        from flink_realtime_processor import FLINK_SQL
        stmt = FLINK_SQL["create_kafka_events_source"]
        assert "orders;users;menu;promo;topics" in stmt

    def test_kafka_source_watermark_10_seconds(self):
        from flink_realtime_processor import FLINK_SQL
        stmt = FLINK_SQL["create_kafka_events_source"]
        assert "INTERVAL '10' SECOND" in stmt

    def test_s3_sink_orc_format(self):
        from flink_realtime_processor import FLINK_SQL
        stmt = FLINK_SQL["create_s3_orc_sink"]
        assert "'format' = 'orc'" in stmt

    def test_s3_sink_partitioned_by_dt_and_hour(self):
        from flink_realtime_processor import FLINK_SQL
        stmt = FLINK_SQL["create_s3_orc_sink"]
        assert "PARTITIONED BY (dt, `hour`)" in stmt

    def test_s3_sink_auto_compaction(self):
        from flink_realtime_processor import FLINK_SQL
        stmt = FLINK_SQL["create_s3_orc_sink"]
        assert "'auto-compaction' = 'true'" in stmt

    def test_s3_sink_rolling_policy(self):
        from flink_realtime_processor import FLINK_SQL
        stmt = FLINK_SQL["create_s3_orc_sink"]
        assert "'sink.rolling-policy.file-size' = '256MB'" in stmt

    def test_druid_sink_round_robin_partitioner(self):
        from flink_realtime_processor import FLINK_SQL
        stmt = FLINK_SQL["create_druid_sink"]
        assert "'sink.partitioner' = 'round-robin'" in stmt

    def test_tumble_window_5_minutes(self):
        from flink_realtime_processor import FLINK_SQL
        stmt = FLINK_SQL["create_order_velocity_view"]
        assert "TUMBLE(`timestamp`, INTERVAL '5' MINUTE)" in stmt

    def test_spike_detection_thresholds(self):
        from flink_realtime_processor import FLINK_SQL
        stmt = FLINK_SQL["create_spike_detection"]
        assert "order_count > 1000" in stmt
        assert "'CRITICAL'" in stmt
        assert "order_count > 500" in stmt
        assert "'WARNING'" in stmt

    def test_alerts_written_back_to_kafka(self):
        from flink_realtime_processor import FLINK_SQL
        stmt = FLINK_SQL["create_alerts_sink"]
        assert "'topic' = 'order-spike-alerts'" in stmt


# ---------------------------------------------------------------------------
# Druid ingestion spec tests
# ---------------------------------------------------------------------------
class TestDruidIngestionSpec:
    def test_generate_spec_structure(self):
        from flink_realtime_processor import generate_druid_ingestion_spec
        spec = generate_druid_ingestion_spec("kafka:9092")
        assert spec["type"] == "kafka"
        assert "spec" in spec
        assert "dataSchema" in spec["spec"]
        assert "ioConfig" in spec["spec"]
        assert "tuningConfig" in spec["spec"]

    def test_datasource_name(self):
        from flink_realtime_processor import generate_druid_ingestion_spec
        spec = generate_druid_ingestion_spec("kafka:9092", datasource="test_ds")
        assert spec["spec"]["dataSchema"]["dataSource"] == "test_ds"

    def test_default_datasource(self):
        from flink_realtime_processor import generate_druid_ingestion_spec
        spec = generate_druid_ingestion_spec("kafka:9092")
        assert spec["spec"]["dataSchema"]["dataSource"] == "zomato_realtime_events"

    def test_timestamp_spec(self):
        from flink_realtime_processor import generate_druid_ingestion_spec
        spec = generate_druid_ingestion_spec("kafka:9092")
        ts = spec["spec"]["dataSchema"]["timestampSpec"]
        assert ts["column"] == "event_timestamp"
        assert ts["format"] == "iso"

    def test_dimensions(self):
        from flink_realtime_processor import generate_druid_ingestion_spec
        spec = generate_druid_ingestion_spec("kafka:9092")
        dims = spec["spec"]["dataSchema"]["dimensionsSpec"]["dimensions"]
        assert "event_type" in dims
        assert "user_id" in dims
        assert "city" in dims

    def test_metrics_include_revenue(self):
        from flink_realtime_processor import generate_druid_ingestion_spec
        spec = generate_druid_ingestion_spec("kafka:9092")
        metrics = spec["spec"]["dataSchema"]["metricsSpec"]
        metric_names = {m["name"] for m in metrics}
        assert "total_revenue" in metric_names
        assert "count" in metric_names
        assert "unique_users" in metric_names

    def test_hll_sketch_for_unique_users(self):
        from flink_realtime_processor import generate_druid_ingestion_spec
        spec = generate_druid_ingestion_spec("kafka:9092")
        metrics = spec["spec"]["dataSchema"]["metricsSpec"]
        hll = next(m for m in metrics if m["name"] == "unique_users")
        assert hll["type"] == "HLLSketchBuild"
        assert hll["lgK"] == 12

    def test_granularity_rollup_enabled(self):
        from flink_realtime_processor import generate_druid_ingestion_spec
        spec = generate_druid_ingestion_spec("kafka:9092")
        gran = spec["spec"]["dataSchema"]["granularitySpec"]
        assert gran["rollup"] is True
        assert gran["segmentGranularity"] == "HOUR"
        assert gran["queryGranularity"] == "MINUTE"

    def test_tuning_config(self):
        from flink_realtime_processor import generate_druid_ingestion_spec
        spec = generate_druid_ingestion_spec("kafka:9092")
        tuning = spec["spec"]["tuningConfig"]
        assert tuning["taskCount"] == 8
        assert tuning["replicas"] == 2
        assert tuning["maxRowsInMemory"] == 1000000


# ---------------------------------------------------------------------------
# Flink job config generation tests
# ---------------------------------------------------------------------------
class TestFlinkJobConfig:
    def test_job_name(self):
        from flink_realtime_processor import generate_flink_job_config
        config = generate_flink_job_config("k1:9092", "k2:9092", "bucket", "s3://cp")
        assert config["job_name"] == "zomato-realtime-events"

    def test_parallelism(self):
        from flink_realtime_processor import generate_flink_job_config
        config = generate_flink_job_config("k1:9092", "k2:9092", "bucket", "s3://cp")
        assert config["parallelism"] == 64

    def test_exponential_delay_restart(self):
        from flink_realtime_processor import generate_flink_job_config
        config = generate_flink_job_config("k1:9092", "k2:9092", "bucket", "s3://cp")
        rs = config["restart_strategy"]
        assert rs["type"] == "exponential-delay"
        assert rs["backoff_multiplier"] == 2.0

    def test_placeholders_resolved(self):
        from flink_realtime_processor import generate_flink_job_config
        config = generate_flink_job_config("k1:9092", "k2:9092", "bucket", "s3://cp")
        for key, stmt in config["sql_statements"].items():
            assert "{kafka_bootstrap}" not in stmt, f"Unresolved in {key}"
            assert "{kafka_bootstrap_2}" not in stmt, f"Unresolved in {key}"
            assert "{s3_bucket}" not in stmt, f"Unresolved in {key}"
