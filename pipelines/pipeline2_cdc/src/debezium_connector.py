"""
Pipeline 2 - CDC: Aurora MySQL → Debezium → Kafka → Flink → Iceberg/S3

Manages Debezium MySQL connectors for Change Data Capture.
Captures INSERT/UPDATE/DELETE events from Aurora MySQL and publishes
to Kafka topics in Avro format.

Topics: menu, promo, orders, users
"""

import json
import logging
import time

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("pipeline2_cdc")


# Debezium connector configurations for each MySQL table/topic
CONNECTOR_CONFIGS = {
    "orders": {
        "name": "zomato-orders-cdc",
        "config": {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "tasks.max": "3",
            "database.hostname": "${AURORA_HOST}",
            "database.port": "3306",
            "database.user": "${DB_USER}",
            "database.password": "${DB_PASSWORD}",
            "database.server.id": "1001",
            "database.server.name": "zomato-aurora",
            "database.include.list": "zomato",
            "table.include.list": "zomato.orders",
            "topic.prefix": "zomato-cdc",
            "schema.history.internal.kafka.bootstrap.servers": "${MSK_BOOTSTRAP}",
            "schema.history.internal.kafka.topic": "schema-changes.orders",
            "schema.history.internal.kafka.security.protocol": "SASL_SSL",
            "schema.history.internal.kafka.sasl.mechanism": "AWS_MSK_IAM",
            "schema.history.internal.kafka.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
            "schema.history.internal.kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
            "key.converter": "com.amazonaws.services.schemaregistry.kafkaconnect.AWSKafkaAvroConverter",
            "key.converter.region": "${AWS_REGION}",
            "key.converter.registry.name": "${GLUE_REGISTRY_NAME}",
            "key.converter.schemaAutoRegistrationEnabled": "true",
            "value.converter": "com.amazonaws.services.schemaregistry.kafkaconnect.AWSKafkaAvroConverter",
            "value.converter.region": "${AWS_REGION}",
            "value.converter.registry.name": "${GLUE_REGISTRY_NAME}",
            "value.converter.schemaAutoRegistrationEnabled": "true",
            "transforms": "unwrap,route",
            "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
            "transforms.unwrap.drop.tombstones": "false",
            "transforms.unwrap.delete.handling.mode": "rewrite",
            "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.route.regex": ".*\\.(.+)",
            "transforms.route.replacement": "orders",
            "snapshot.mode": "initial",
            "decimal.handling.mode": "string",
            "time.precision.mode": "connect",
            "tombstones.on.delete": "true",
            "provide.transaction.metadata": "true",
            "signal.data.collection": "zomato.debezium_signal",
        },
    },
    "users": {
        "name": "zomato-users-cdc",
        "config": {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "tasks.max": "2",
            "database.hostname": "${AURORA_HOST}",
            "database.port": "3306",
            "database.user": "${DB_USER}",
            "database.password": "${DB_PASSWORD}",
            "database.server.id": "1002",
            "database.server.name": "zomato-aurora",
            "database.include.list": "zomato",
            "table.include.list": "zomato.users",
            "topic.prefix": "zomato-cdc",
            "schema.history.internal.kafka.bootstrap.servers": "${MSK_BOOTSTRAP}",
            "schema.history.internal.kafka.topic": "schema-changes.users",
            "schema.history.internal.kafka.security.protocol": "SASL_SSL",
            "schema.history.internal.kafka.sasl.mechanism": "AWS_MSK_IAM",
            "schema.history.internal.kafka.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
            "schema.history.internal.kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
            "key.converter": "com.amazonaws.services.schemaregistry.kafkaconnect.AWSKafkaAvroConverter",
            "key.converter.region": "${AWS_REGION}",
            "key.converter.registry.name": "${GLUE_REGISTRY_NAME}",
            "key.converter.schemaAutoRegistrationEnabled": "true",
            "value.converter": "com.amazonaws.services.schemaregistry.kafkaconnect.AWSKafkaAvroConverter",
            "value.converter.region": "${AWS_REGION}",
            "value.converter.registry.name": "${GLUE_REGISTRY_NAME}",
            "value.converter.schemaAutoRegistrationEnabled": "true",
            "transforms": "unwrap,route",
            "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
            "transforms.unwrap.drop.tombstones": "false",
            "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.route.regex": ".*\\.(.+)",
            "transforms.route.replacement": "users",
            "snapshot.mode": "initial",
        },
    },
    "menu": {
        "name": "zomato-menu-cdc",
        "config": {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "tasks.max": "2",
            "database.hostname": "${AURORA_HOST}",
            "database.port": "3306",
            "database.user": "${DB_USER}",
            "database.password": "${DB_PASSWORD}",
            "database.server.id": "1003",
            "database.server.name": "zomato-aurora",
            "database.include.list": "zomato",
            "table.include.list": "zomato.menu_items",
            "topic.prefix": "zomato-cdc",
            "schema.history.internal.kafka.bootstrap.servers": "${MSK_BOOTSTRAP}",
            "schema.history.internal.kafka.topic": "schema-changes.menu",
            "schema.history.internal.kafka.security.protocol": "SASL_SSL",
            "schema.history.internal.kafka.sasl.mechanism": "AWS_MSK_IAM",
            "schema.history.internal.kafka.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
            "schema.history.internal.kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
            "key.converter": "com.amazonaws.services.schemaregistry.kafkaconnect.AWSKafkaAvroConverter",
            "key.converter.region": "${AWS_REGION}",
            "key.converter.registry.name": "${GLUE_REGISTRY_NAME}",
            "key.converter.schemaAutoRegistrationEnabled": "true",
            "value.converter": "com.amazonaws.services.schemaregistry.kafkaconnect.AWSKafkaAvroConverter",
            "value.converter.region": "${AWS_REGION}",
            "value.converter.registry.name": "${GLUE_REGISTRY_NAME}",
            "value.converter.schemaAutoRegistrationEnabled": "true",
            "transforms": "unwrap,route",
            "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
            "transforms.unwrap.drop.tombstones": "false",
            "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.route.regex": ".*\\.(.+)",
            "transforms.route.replacement": "menu",
            "snapshot.mode": "initial",
        },
    },
    "promo": {
        "name": "zomato-promo-cdc",
        "config": {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "tasks.max": "1",
            "database.hostname": "${AURORA_HOST}",
            "database.port": "3306",
            "database.user": "${DB_USER}",
            "database.password": "${DB_PASSWORD}",
            "database.server.id": "1004",
            "database.server.name": "zomato-aurora",
            "database.include.list": "zomato",
            "table.include.list": "zomato.promotions",
            "topic.prefix": "zomato-cdc",
            "schema.history.internal.kafka.bootstrap.servers": "${MSK_BOOTSTRAP}",
            "schema.history.internal.kafka.topic": "schema-changes.promo",
            "schema.history.internal.kafka.security.protocol": "SASL_SSL",
            "schema.history.internal.kafka.sasl.mechanism": "AWS_MSK_IAM",
            "schema.history.internal.kafka.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
            "schema.history.internal.kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
            "key.converter": "com.amazonaws.services.schemaregistry.kafkaconnect.AWSKafkaAvroConverter",
            "key.converter.region": "${AWS_REGION}",
            "key.converter.registry.name": "${GLUE_REGISTRY_NAME}",
            "key.converter.schemaAutoRegistrationEnabled": "true",
            "value.converter": "com.amazonaws.services.schemaregistry.kafkaconnect.AWSKafkaAvroConverter",
            "value.converter.region": "${AWS_REGION}",
            "value.converter.registry.name": "${GLUE_REGISTRY_NAME}",
            "value.converter.schemaAutoRegistrationEnabled": "true",
            "transforms": "unwrap,route",
            "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
            "transforms.unwrap.drop.tombstones": "false",
            "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.route.regex": ".*\\.(.+)",
            "transforms.route.replacement": "promo",
            "snapshot.mode": "initial",
        },
    },
}


class DebeziumManager:
    """Manages Debezium connectors via Kafka Connect REST API."""

    def __init__(self, connect_url: str):
        self.connect_url = connect_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

    def list_connectors(self) -> list[str]:
        """List all registered connectors."""
        resp = self.session.get(f"{self.connect_url}/connectors")
        resp.raise_for_status()
        return resp.json()

    def get_connector_status(self, name: str) -> dict:
        """Get the status of a specific connector."""
        resp = self.session.get(f"{self.connect_url}/connectors/{name}/status")
        resp.raise_for_status()
        return resp.json()

    def create_connector(self, connector_config: dict) -> dict:
        """Create or update a Debezium connector."""
        name = connector_config["name"]
        logger.info("Creating connector: %s", name)

        resp = self.session.put(
            f"{self.connect_url}/connectors/{name}/config",
            data=json.dumps(connector_config["config"]),
        )
        resp.raise_for_status()
        logger.info("Connector %s created/updated successfully", name)
        return resp.json()

    def delete_connector(self, name: str) -> None:
        """Delete a connector."""
        resp = self.session.delete(f"{self.connect_url}/connectors/{name}")
        resp.raise_for_status()
        logger.info("Connector %s deleted", name)

    def restart_connector(self, name: str) -> None:
        """Restart a connector."""
        resp = self.session.post(f"{self.connect_url}/connectors/{name}/restart")
        resp.raise_for_status()
        logger.info("Connector %s restarted", name)

    def pause_connector(self, name: str) -> None:
        """Pause a connector."""
        resp = self.session.put(f"{self.connect_url}/connectors/{name}/pause")
        resp.raise_for_status()
        logger.info("Connector %s paused", name)

    def resume_connector(self, name: str) -> None:
        """Resume a paused connector."""
        resp = self.session.put(f"{self.connect_url}/connectors/{name}/resume")
        resp.raise_for_status()
        logger.info("Connector %s resumed", name)

    def wait_for_healthy(self, name: str, timeout: int = 120) -> bool:
        """Wait for a connector to become healthy."""
        start = time.time()
        while time.time() - start < timeout:
            try:
                status = self.get_connector_status(name)
                connector_state = status["connector"]["state"]
                task_states = [t["state"] for t in status.get("tasks", [])]

                if connector_state == "RUNNING" and all(
                    s == "RUNNING" for s in task_states
                ):
                    logger.info("Connector %s is healthy", name)
                    return True

                if connector_state == "FAILED" or "FAILED" in task_states:
                    logger.error("Connector %s is in FAILED state", name)
                    return False

            except requests.RequestException as e:
                logger.warning("Error checking connector status: %s", e)

            time.sleep(5)

        logger.error("Timeout waiting for connector %s to become healthy", name)
        return False


def resolve_config(config: dict, env_vars: dict) -> dict:
    """Resolve environment variable placeholders in connector config."""
    resolved = {}
    for key, value in config.items():
        if isinstance(value, str):
            for env_key, env_value in env_vars.items():
                value = value.replace(f"${{{env_key}}}", env_value)
        resolved[key] = value
    return resolved


def deploy_all_connectors(
    connect_url: str,
    env_vars: dict,
) -> dict:
    """Deploy all CDC connectors."""
    manager = DebeziumManager(connect_url)
    results = {"deployed": [], "failed": []}

    for topic, connector_config in CONNECTOR_CONFIGS.items():
        try:
            resolved = {
                "name": connector_config["name"],
                "config": resolve_config(connector_config["config"], env_vars),
            }
            manager.create_connector(resolved)

            if manager.wait_for_healthy(connector_config["name"]):
                results["deployed"].append(topic)
            else:
                results["failed"].append(topic)

        except Exception:
            logger.exception("Failed to deploy connector for %s", topic)
            results["failed"].append(topic)

    logger.info("Deployment results: %s", json.dumps(results, indent=2))
    return results


if __name__ == "__main__":
    import os

    env = {
        "AURORA_HOST": os.environ.get("AURORA_HOST", "localhost"),
        "DB_USER": os.environ.get("DB_USER", "debezium"),
        "DB_PASSWORD": os.environ.get("DB_PASSWORD", ""),
        "MSK_BOOTSTRAP": os.environ.get("MSK_BOOTSTRAP", "b-1.zomato-msk.xxxxx.c2.kafka.ap-south-1.amazonaws.com:9098,b-2.zomato-msk.xxxxx.c2.kafka.ap-south-1.amazonaws.com:9098,b-3.zomato-msk.xxxxx.c2.kafka.ap-south-1.amazonaws.com:9098"),
        "AWS_REGION": os.environ.get("AWS_REGION", "ap-south-1"),
        "GLUE_REGISTRY_NAME": os.environ.get("GLUE_REGISTRY_NAME", "zomato-schema-registry"),
    }

    connect = os.environ.get("KAFKA_CONNECT_URL", "http://localhost:8083")
    deploy_all_connectors(connect, env)
