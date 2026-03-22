"""
Pipeline 4 - Event definitions and Flink SQL source table.

Defines the app event schema consumed from Kafka and the Flink SQL DDL
for the source table used by the real-time processing pipeline.

Event sources: orders, users, menu, promo, topics (generic)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional


# ---------------------------------------------------------------------------
# Event dataclass
# ---------------------------------------------------------------------------

@dataclass
class AppEvent:
    """Represents an app event consumed from Kafka by the Flink pipeline."""

    event_id: str = ""
    event_type: str = ""
    source: str = ""
    user_id: str = ""
    session_id: str = ""
    timestamp: str = ""
    properties: Dict[str, str] = field(default_factory=dict)
    device: Dict[str, str] = field(default_factory=dict)
    location: Dict[str, str] = field(default_factory=dict)

    @property
    def city(self) -> str:
        return self.location.get("city", "")

    @property
    def latitude(self) -> Optional[float]:
        val = self.location.get("latitude")
        return float(val) if val is not None else None

    @property
    def longitude(self) -> Optional[float]:
        val = self.location.get("longitude")
        return float(val) if val is not None else None

    @property
    def platform(self) -> str:
        return self.device.get("platform", "")

    @property
    def order_id(self) -> str:
        return self.properties.get("order_id", "")

    @property
    def total_amount(self) -> Optional[float]:
        val = self.properties.get("total_amount")
        return float(val) if val is not None else None


# ---------------------------------------------------------------------------
# Flink SQL: Kafka source table DDL
# ---------------------------------------------------------------------------

KAFKA_SOURCE_DDL = """
    CREATE TABLE app_events (
        event_id STRING,
        event_type STRING,
        source STRING,
        user_id STRING,
        session_id STRING,
        `timestamp` TIMESTAMP(3),
        properties MAP<STRING, STRING>,
        device MAP<STRING, STRING>,
        location MAP<STRING, STRING>,
        WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '10' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'orders;users;menu;promo;topics',
        'properties.bootstrap.servers' = '{kafka_bootstrap}',
        'properties.group.id' = 'flink-realtime-events',
        'properties.security.protocol' = 'SASL_SSL',
        'properties.sasl.mechanism' = 'AWS_MSK_IAM',
        'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required;',
        'properties.sasl.client.callback.handler.class' = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler',
        'format' = 'json',
        'json.timestamp-format.standard' = 'ISO-8601',
        'scan.startup.mode' = 'latest-offset'
    );
"""
