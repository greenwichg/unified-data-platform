#!/usr/bin/env python3
"""
Real-time Data Producer - Zomato Data Platform
===============================================
Continuously produces realistic live traffic into all three sources:

  - Aurora MySQL  → feeds Pipeline 1 (Spark batch) + Pipeline 2 (Debezium CDC)
  - DynamoDB      → feeds Pipeline 3 (DynamoDB Streams)
  - Kafka topics  → feeds Pipeline 4 (real-time events)

Each tick generates:
  - New orders (MySQL + DynamoDB + Kafka)
  - Order status progressions on existing orders (MySQL + DynamoDB + Kafka)
  - Payments for confirmed orders (MySQL + DynamoDB + Kafka)
  - User location updates (DynamoDB + Kafka)
  - Occasional new user registrations (MySQL + Kafka)

Usage:
    # Produce to all targets at default rate (5 events/sec)
    python produce_realtime.py

    # Custom rate and duration
    python produce_realtime.py --rate 23 --duration 3600

    # Single target
    python produce_realtime.py --target mysql
    python produce_realtime.py --target dynamodb
    python produce_realtime.py --target kafka

Environment variables (same as seed_data.py):
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB
    DYNAMODB_ENDPOINT, AWS_REGION, DYNAMODB_TABLE_PREFIX
    KAFKA_BOOTSTRAP
"""

import argparse
import json
import logging
import os
import random
import signal
import sys
import time
from collections import deque
from datetime import datetime, timedelta

# Reuse all reference data and generators from seed_data
sys.path.insert(0, os.path.dirname(__file__))
from seed_data import (
    CITIES,
    CITY_COORDS,
    FIRST_NAMES,
    LAST_NAMES,
    MENU_ITEMS,
    PAYMENT_METHODS,
    DYNAMODB_ENDPOINT,
    DYNAMODB_TABLE_PREFIX,
    AWS_REGION,
    KAFKA_BOOTSTRAP,
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DB,
    generate_users,
    generate_restaurants,
    generate_menu_items,
    generate_promotions,
    fmt,
    jitter,
    uid,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("produce_realtime")

# ---------------------------------------------------------------------------
# Order lifecycle: valid state transitions
# ---------------------------------------------------------------------------
ORDER_TRANSITIONS = {
    "PLACED":     ["CONFIRMED", "CANCELLED"],
    "CONFIRMED":  ["PREPARING", "CANCELLED"],
    "PREPARING":  ["READY"],
    "READY":      ["PICKED_UP"],
    "PICKED_UP":  ["DELIVERING"],
    "DELIVERING": ["DELIVERED"],
    "DELIVERED":  [],
    "CANCELLED":  [],
}

TERMINAL_STATUSES = {"DELIVERED", "CANCELLED"}


# ---------------------------------------------------------------------------
# Shared in-memory state (tracks live orders for status progression)
# ---------------------------------------------------------------------------
class ProducerState:
    """Tracks active orders so we can advance them through their lifecycle."""

    def __init__(self, max_active=500):
        self.active_orders: deque = deque(maxlen=max_active)
        self.user_ids: list[str] = []
        self.restaurant_ids: list[str] = []
        self.stats = {"mysql": 0, "dynamodb": 0, "kafka": 0}

    def add_order(self, order: dict):
        self.active_orders.append(order)

    def pop_progressable(self) -> list[dict]:
        """Return orders eligible for a status transition."""
        progressable = [
            o for o in self.active_orders
            if o["status"] not in TERMINAL_STATUSES
        ]
        return random.sample(progressable, min(len(progressable), 3))


# ---------------------------------------------------------------------------
# Event generators
# ---------------------------------------------------------------------------
def new_order_event(state: ProducerState) -> dict:
    user_id = random.choice(state.user_ids) if state.user_ids else uid("usr_")
    restaurant_id = random.choice(state.restaurant_ids) if state.restaurant_ids else uid("rest_")
    city = random.choice(CITIES)
    lat, lon = CITY_COORDS[city]
    lat, lon = jitter(lat, lon)
    subtotal = round(random.uniform(150, 1200), 2)
    tax = round(subtotal * 0.05, 2)
    delivery_fee = random.choice([0.0, 20.0, 30.0, 40.0, 49.0])
    now = datetime.utcnow()
    return {
        "order_id": uid("ord_"),
        "user_id": user_id,
        "restaurant_id": restaurant_id,
        "status": "PLACED",
        "subtotal": subtotal,
        "tax": tax,
        "delivery_fee": delivery_fee,
        "total_amount": round(subtotal + tax + delivery_fee, 2),
        "payment_method": random.choice(PAYMENT_METHODS),
        "delivery_latitude": lat,
        "delivery_longitude": lon,
        "delivery_address": f"{random.randint(1, 999)}, Sector {random.randint(1, 50)}, {city}",
        "city": city,
        "pincode": str(random.randint(100000, 999999)),
        "estimated_delivery_mins": random.randint(25, 60),
        "actual_delivery_mins": None,
        "created_at": fmt(now),
        "updated_at": fmt(now),
    }


def status_update_event(order: dict) -> dict:
    """Advance an order to its next status."""
    next_statuses = ORDER_TRANSITIONS.get(order["status"], [])
    if not next_statuses:
        return order
    new_status = next_statuses[0]  # always follow the happy path
    if new_status == "CANCELLED" and random.random() > 0.1:
        new_status = next_statuses[0] if len(next_statuses) == 1 else next_statuses[1]
    now = datetime.utcnow()
    updated = {**order, "status": new_status, "updated_at": fmt(now)}
    if new_status == "DELIVERED":
        updated["actual_delivery_mins"] = order["estimated_delivery_mins"] + random.randint(-5, 15)
    return updated


def payment_event(order: dict) -> dict:
    now = datetime.utcnow()
    return {
        "payment_id": uid("pay_"),
        "order_id": order["order_id"],
        "amount": order["total_amount"],
        "payment_method": order["payment_method"],
        "payment_status": "COMPLETED" if order["status"] not in ("PLACED", "CANCELLED") else "INITIATED",
        "transaction_id": uid("txn_"),
        "gateway_response": json.dumps({"code": "00", "message": "Success", "bank_ref": uid()}),
        "created_at": fmt(now),
        "updated_at": fmt(now),
    }


def user_location_event(user_id: str) -> dict:
    city = random.choice(CITIES)
    lat, lon = CITY_COORDS[city]
    lat, lon = jitter(lat, lon, km=5.0)
    now = datetime.utcnow()
    return {
        "user_id": user_id,
        "timestamp": now.isoformat(),
        "latitude": str(round(lat, 6)),
        "longitude": str(round(lon, 6)),
        "city": city,
        "accuracy_meters": str(random.randint(5, 50)),
        "speed_kmh": str(round(random.uniform(0, 60), 1)),
        "ttl": int((now + timedelta(days=7)).timestamp()),
    }


def new_user_event() -> dict:
    city = random.choice(CITIES)
    name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
    uid_ = uid("usr_")
    now = datetime.utcnow()
    return {
        "user_id": uid_,
        "name": name,
        "email": f"{name.lower().replace(' ', '.')}.{uid_[-4:]}@example.com",
        "phone": f"+91{random.randint(7000000000, 9999999999)}",
        "city": city,
        "signup_date": fmt(now),
        "is_pro_member": False,
        "preferred_cuisine": random.sample(["North Indian", "Chinese", "South Indian"], k=2),
        "total_orders": 0,
        "average_order_value": 0.0,
        "last_order_at": None,
        "created_at": fmt(now),
        "updated_at": fmt(now),
    }


# ---------------------------------------------------------------------------
# MySQL writer
# ---------------------------------------------------------------------------
class MySQLWriter:
    def __init__(self):
        import pymysql
        self.conn = pymysql.connect(
            host=MYSQL_HOST, port=MYSQL_PORT,
            user=MYSQL_USER, password=MYSQL_PASSWORD,
            database=MYSQL_DB, charset="utf8mb4",
            autocommit=True,
        )
        log.info("MySQL connected.")

    def write_order(self, order: dict):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO orders
                    (order_id, user_id, restaurant_id, status, subtotal, tax,
                     delivery_fee, total_amount, payment_method, delivery_latitude,
                     delivery_longitude, delivery_address, city, pincode,
                     estimated_delivery_mins, actual_delivery_mins, created_at, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                    status=VALUES(status),
                    actual_delivery_mins=VALUES(actual_delivery_mins),
                    updated_at=VALUES(updated_at)
            """, (
                order["order_id"], order["user_id"], order["restaurant_id"],
                order["status"], order["subtotal"], order["tax"],
                order["delivery_fee"], order["total_amount"], order["payment_method"],
                order["delivery_latitude"], order["delivery_longitude"],
                order["delivery_address"], order["city"], order["pincode"],
                order["estimated_delivery_mins"], order["actual_delivery_mins"],
                order["created_at"], order["updated_at"],
            ))

    def write_payment(self, payment: dict):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT IGNORE INTO payments
                    (payment_id, order_id, amount, payment_method, payment_status,
                     transaction_id, gateway_response, created_at, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                payment["payment_id"], payment["order_id"], payment["amount"],
                payment["payment_method"], payment["payment_status"],
                payment["transaction_id"], payment["gateway_response"],
                payment["created_at"], payment["updated_at"],
            ))

    def write_user(self, user: dict):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT IGNORE INTO users
                    (user_id, name, email, phone, city, signup_date, is_pro_member,
                     preferred_cuisine, total_orders, average_order_value,
                     last_order_at, created_at, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                user["user_id"], user["name"], user["email"], user["phone"],
                user["city"], user["signup_date"], user["is_pro_member"],
                json.dumps(user["preferred_cuisine"]), user["total_orders"],
                user["average_order_value"], user["last_order_at"],
                user["created_at"], user["updated_at"],
            ))

    def close(self):
        self.conn.close()


# ---------------------------------------------------------------------------
# DynamoDB writer
# ---------------------------------------------------------------------------
class DynamoDBWriter:
    def __init__(self):
        import boto3
        kwargs = {"region_name": AWS_REGION}
        if DYNAMODB_ENDPOINT:
            kwargs["endpoint_url"] = DYNAMODB_ENDPOINT
        ddb = boto3.resource("dynamodb", **kwargs)
        self.orders_table = ddb.Table(f"{DYNAMODB_TABLE_PREFIX}-orders")
        self.payments_table = ddb.Table(f"{DYNAMODB_TABLE_PREFIX}-payments")
        self.locations_table = ddb.Table(f"{DYNAMODB_TABLE_PREFIX}-user-locations")
        log.info("DynamoDB connected (prefix: %s).", DYNAMODB_TABLE_PREFIX)

    def write_order(self, order: dict):
        self.orders_table.put_item(Item={
            "order_id": order["order_id"],
            "created_at": order["created_at"],
            "user_id": order["user_id"],
            "restaurant_id": order["restaurant_id"],
            "status": order["status"],
            "total_amount": str(order["total_amount"]),
            "payment_method": order["payment_method"],
            "city": order["city"],
            "estimated_delivery_mins": order["estimated_delivery_mins"],
            "updated_at": order["updated_at"],
        })

    def write_payment(self, payment: dict):
        self.payments_table.put_item(Item={
            "payment_id": payment["payment_id"],
            "order_id": payment["order_id"],
            "amount": str(payment["amount"]),
            "payment_method": payment["payment_method"],
            "payment_status": payment["payment_status"],
            "transaction_id": payment["transaction_id"],
            "created_at": payment["created_at"],
            "updated_at": payment["updated_at"],
        })

    def write_location(self, location: dict):
        self.locations_table.put_item(Item=location)


# ---------------------------------------------------------------------------
# Kafka writer
# ---------------------------------------------------------------------------
class KafkaWriter:
    def __init__(self):
        from confluent_kafka import Producer
        self.producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
        log.info("Kafka connected (%s).", KAFKA_BOOTSTRAP)

    def _produce(self, topic: str, key: str, payload: dict):
        self.producer.produce(
            topic,
            key=key.encode(),
            value=json.dumps({**payload, "event_timestamp": datetime.utcnow().isoformat()}).encode(),
        )
        self.producer.poll(0)

    def write_order(self, order: dict):
        self._produce("orders", order["order_id"], {**order, "event_type": "ORDER_EVENT", "source": "pipeline4"})

    def write_payment(self, payment: dict):
        self._produce("orders", payment["order_id"], {**payment, "event_type": "PAYMENT_EVENT", "source": "pipeline4"})

    def write_user(self, user: dict):
        self._produce("users", user["user_id"], {**user, "event_type": "USER_EVENT", "source": "pipeline4"})

    def write_location(self, location: dict):
        self._produce("topics", location["user_id"], {**location, "event_type": "LOCATION_EVENT", "source": "pipeline4"})

    def flush(self):
        self.producer.flush(timeout=5)


# ---------------------------------------------------------------------------
# Main produce loop
# ---------------------------------------------------------------------------
def run(targets: list[str], rate: int, duration: int | None):
    state = ProducerState()

    # Bootstrap state with seed users/restaurants so orders have valid FKs
    log.info("Bootstrapping reference data...")
    users = generate_users(50)
    restaurants = generate_restaurants(20)
    state.user_ids = [u["user_id"] for u in users]
    state.restaurant_ids = [r["restaurant_id"] for r in restaurants]

    # Initialise writers
    mysql = MySQLWriter() if "mysql" in targets else None
    dynamo = DynamoDBWriter() if "dynamodb" in targets else None
    kafka = KafkaWriter() if "kafka" in targets else None

    # Seed reference data into MySQL so FK constraints hold
    if mysql:
        from seed_data import generate_menu_items as _gen_menu, generate_promotions as _gen_promos
        log.info("Seeding reference users/restaurants into MySQL...")
        for u in users:
            try:
                mysql.write_user(u)
            except Exception:
                pass
        menu_items = _gen_menu(restaurants)
        promos = _gen_promos()
        with mysql.conn.cursor() as cur:
            for r in restaurants:
                cur.execute("""
                    INSERT IGNORE INTO restaurants
                        (restaurant_id, name, city, area, cuisine_types, rating,
                         total_reviews, avg_cost_for_two, is_active, latitude, longitude,
                         created_at, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    r["restaurant_id"], r["name"], r["city"], r["area"],
                    json.dumps(r["cuisine_types"]), r["rating"], r["total_reviews"],
                    r["avg_cost_for_two"], r["is_active"], r["latitude"], r["longitude"],
                    r["created_at"], r["updated_at"],
                ))
            for m in menu_items:
                cur.execute("""
                    INSERT IGNORE INTO menu_items
                        (item_id, restaurant_id, name, description, category, cuisine_type,
                         price, is_vegetarian, is_available, preparation_time_mins,
                         rating, total_orders, image_url, created_at, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    m["item_id"], m["restaurant_id"], m["name"], m["description"],
                    m["category"], m["cuisine_type"], m["price"], m["is_vegetarian"],
                    m["is_available"], m["preparation_time_mins"], m["rating"],
                    m["total_orders"], m["image_url"], m["created_at"], m["updated_at"],
                ))
        log.info("Reference data seeded.")

    interval = 1.0 / rate
    start = time.time()
    tick = 0

    log.info("Producing at %d events/sec to: %s (duration: %s)",
             rate, targets, f"{duration}s" if duration else "∞")
    log.info("Press Ctrl+C to stop.\n")

    def _shutdown(sig, frame):
        log.info("Shutting down...")
        if kafka:
            kafka.flush()
        if mysql:
            mysql.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    while True:
        tick_start = time.time()
        tick += 1

        try:
            # --- New order (every tick) ---
            order = new_order_event(state)
            state.add_order(order)

            if mysql:
                mysql.write_order(order)
                state.stats["mysql"] += 1
            if dynamo:
                dynamo.write_order(order)
                state.stats["dynamodb"] += 1
            if kafka:
                kafka.write_order(order)
                state.stats["kafka"] += 1

            # --- Payment for new order ---
            payment = payment_event(order)
            if mysql:
                mysql.write_payment(payment)
            if dynamo:
                dynamo.write_payment(payment)
            if kafka:
                kafka.write_payment(payment)

            # --- Advance 1-3 existing orders through lifecycle ---
            for active_order in state.pop_progressable():
                updated = status_update_event(active_order)
                active_order.update(updated)

                if mysql:
                    mysql.write_order(updated)
                if dynamo:
                    dynamo.write_order(updated)
                if kafka:
                    kafka.write_order(updated)

            # --- User location update (every 3 ticks) ---
            if tick % 3 == 0 and state.user_ids:
                location = user_location_event(random.choice(state.user_ids))
                if dynamo:
                    dynamo.write_location(location)
                if kafka:
                    kafka.write_location(location)

            # --- New user registration (every 20 ticks) ---
            if tick % 20 == 0:
                new_user = new_user_event()
                state.user_ids.append(new_user["user_id"])
                if mysql:
                    mysql.write_user(new_user)
                if kafka:
                    kafka.write_user(new_user)

            # --- Flush Kafka every 10 ticks ---
            if kafka and tick % 10 == 0:
                kafka.flush()

            # --- Stats every 60 ticks ---
            if tick % 60 == 0:
                elapsed = time.time() - start
                log.info(
                    "tick=%d | elapsed=%.0fs | mysql=%d dynamo=%d kafka=%d | active_orders=%d",
                    tick, elapsed,
                    state.stats["mysql"], state.stats["dynamodb"], state.stats["kafka"],
                    len(state.active_orders),
                )

        except Exception as e:
            log.warning("tick=%d error: %s", tick, e)

        # Duration check
        if duration and (time.time() - start) >= duration:
            log.info("Duration reached. Stopping.")
            break

        # Rate limiting
        elapsed_tick = time.time() - tick_start
        sleep_for = interval - elapsed_tick
        if sleep_for > 0:
            time.sleep(sleep_for)

    if kafka:
        kafka.flush()
    if mysql:
        mysql.close()
    log.info("Done. Total ticks: %d", tick)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Real-time data producer for Zomato Data Platform")
    parser.add_argument(
        "--target",
        choices=["mysql", "dynamodb", "kafka", "all"],
        default="all",
    )
    parser.add_argument(
        "--rate", type=int, default=5,
        help="Events per second (default: 5, use 23 to simulate 2M orders/day)",
    )
    parser.add_argument(
        "--duration", type=int, default=None,
        help="How long to run in seconds (default: run forever)",
    )
    args = parser.parse_args()

    targets = ["mysql", "dynamodb", "kafka"] if args.target == "all" else [args.target]
    run(targets, args.rate, args.duration)


if __name__ == "__main__":
    main()
