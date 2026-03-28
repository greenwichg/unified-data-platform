#!/bin/bash
###############################################################################
# Register Debezium MySQL CDC connectors via Kafka Connect REST API
#
# Runs automatically as part of docker-compose (connect-init service).
# Waits for Kafka Connect to be ready, then registers one connector per table.
#
# Topics produced:
#   orders  <- zomato.orders
#   users   <- zomato.users
#   menu    <- zomato.menu_items
#   promo   <- zomato.promotions
###############################################################################

set -euo pipefail

CONNECT_URL="${KAFKA_CONNECT_URL:-http://kafka-connect:8083}"
MYSQL_HOST="${MYSQL_HOST:-mysql}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-rootpass}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka-1:29092}"

# ---------------------------------------------------------------------------
# Wait for Kafka Connect to be ready
# ---------------------------------------------------------------------------
echo "Waiting for Kafka Connect at $CONNECT_URL ..."
for i in $(seq 1 30); do
    if curl -sf "$CONNECT_URL/connectors" > /dev/null 2>&1; then
        echo "Kafka Connect is ready."
        break
    fi
    echo "  attempt $i/30 — not ready yet, retrying in 5s..."
    sleep 5
    if [ "$i" -eq 30 ]; then
        echo "ERROR: Kafka Connect did not become ready in time."
        exit 1
    fi
done

# ---------------------------------------------------------------------------
# Helper: register or update a connector
# ---------------------------------------------------------------------------
register_connector() {
    local name="$1"
    local config="$2"

    echo ""
    echo "Registering connector: $name"

    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
        -X PUT "$CONNECT_URL/connectors/$name/config" \
        -H "Content-Type: application/json" \
        -d "$config")

    if [[ "$HTTP_CODE" == "200" || "$HTTP_CODE" == "201" ]]; then
        echo "  ✓ $name registered (HTTP $HTTP_CODE)"
    else
        echo "  ✗ $name failed (HTTP $HTTP_CODE)"
        exit 1
    fi
}

# ---------------------------------------------------------------------------
# Common connector settings (local dev uses JSON, not Avro)
# In production, swap key/value.converter to AWS Glue Avro converter.
# ---------------------------------------------------------------------------
COMMON_CONFIG=$(cat <<EOF
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "$MYSQL_HOST",
    "database.port": "$MYSQL_PORT",
    "database.user": "$MYSQL_USER",
    "database.password": "$MYSQL_PASSWORD",
    "database.include.list": "zomato",
    "topic.prefix": "zomato",
    "schema.history.internal.kafka.bootstrap.servers": "$KAFKA_BOOTSTRAP",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "transforms": "unwrap,route",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.route.regex": "zomato\\.zomato\\.(.*)",
    "transforms.route.replacement": "\$1",
    "snapshot.mode": "initial",
    "decimal.handling.mode": "string",
    "time.precision.mode": "connect",
    "tombstones.on.delete": "true",
    "signal.data.collection": "zomato.debezium_signal"
EOF
)

# ---------------------------------------------------------------------------
# Register connectors
# ---------------------------------------------------------------------------

# orders connector
register_connector "zomato-orders-cdc" "{
    $COMMON_CONFIG,
    \"tasks.max\": \"1\",
    \"database.server.id\": \"1001\",
    \"table.include.list\": \"zomato.orders\",
    \"schema.history.internal.kafka.topic\": \"schema-changes.orders\"
}"

# users connector
register_connector "zomato-users-cdc" "{
    $COMMON_CONFIG,
    \"tasks.max\": \"1\",
    \"database.server.id\": \"1002\",
    \"table.include.list\": \"zomato.users\",
    \"schema.history.internal.kafka.topic\": \"schema-changes.users\"
}"

# menu connector (watches menu_items table, routes to 'menu' topic)
register_connector "zomato-menu-cdc" "{
    $COMMON_CONFIG,
    \"tasks.max\": \"1\",
    \"database.server.id\": \"1003\",
    \"table.include.list\": \"zomato.menu_items\",
    \"schema.history.internal.kafka.topic\": \"schema-changes.menu\",
    \"transforms.route.regex\": \"zomato\\\\.zomato\\\\.(.*)\",
    \"transforms.route.replacement\": \"menu\"
}"

# promo connector (watches promotions table, routes to 'promo' topic)
register_connector "zomato-promo-cdc" "{
    $COMMON_CONFIG,
    \"tasks.max\": \"1\",
    \"database.server.id\": \"1004\",
    \"table.include.list\": \"zomato.promotions\",
    \"schema.history.internal.kafka.topic\": \"schema-changes.promo\",
    \"transforms.route.regex\": \"zomato\\\\.zomato\\\\.(.*)\",
    \"transforms.route.replacement\": \"promo\"
}"

echo ""
echo "All connectors registered. Current state:"
curl -s "$CONNECT_URL/connectors?expand=status" | \
    python3 -c "
import sys, json
data = json.load(sys.stdin)
for name, info in data.items():
    state = info['status']['connector']['state']
    print(f'  {name}: {state}')
" 2>/dev/null || curl -s "$CONNECT_URL/connectors"

echo ""
echo "CDC connectors are running. Changes to MySQL will now stream to Kafka."
