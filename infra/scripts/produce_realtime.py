#!/usr/bin/env python3
"""
Real-time Data Producer - Zomato Data Platform
===============================================
Continuously produces realistic live traffic into all three pipeline sources:

  - Aurora MySQL  -> feeds Pipeline 1 (Spark batch) + Pipeline 2 (Debezium CDC)
  - DynamoDB      -> feeds Pipeline 3 (DynamoDB Streams)
  - Kafka topics  -> feeds Pipeline 4 (real-time events)

Event mix per tick:
  - 1 new order          (MySQL + DynamoDB + Kafka)
  - 1 payment            (MySQL + DynamoDB + Kafka)
  - 1-3 status updates   (MySQL + DynamoDB + Kafka)  -- advances active orders
  - 1 user location      (DynamoDB + Kafka)           -- every 3 ticks
  - 1 new user           (MySQL + Kafka)              -- every 20 ticks

Usage:
    python produce_realtime.py                          # all targets, 5 events/sec
    python produce_realtime.py --rate 23                # ~2M orders/day
    python produce_realtime.py --rate 10 --duration 600 # 10/sec for 10 minutes
    python produce_realtime.py --target kafka            # Kafka only

Environment variables (same as seed_data.py):
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB
    DYNAMODB_ENDPOINT, AWS_REGION, DYNAMODB_TABLE_PREFIX
    KAFKA_BOOTSTRAP
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import signal
import sys
import time
from collections import deque
from datetime import datetime, timedelta

from seed_helpers import (
    CITIES,
    CITY_COORDS,
    CUISINES,
    FIRST_NAMES,
    LAST_NAMES,
    ORDER_TRANSITIONS,
    PAYMENT_METHODS,
    TERMINAL_STATUSES,
    DynamoDBWriter,
    KafkaWriter,
    MySQLWriter,
    fmt,
    generate_menu_items,
    generate_promotions,
    generate_restaurants,
    generate_users,
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
# In-memory state: tracks active orders for lifecycle progression
# ---------------------------------------------------------------------------
class ProducerState:
    """Holds active orders and reference IDs so the producer can generate
    realistic events with valid foreign-key relationships."""

    def __init__(self, max_active: int = 500) -> None:
        self.active_orders: deque[dict] = deque(maxlen=max_active)
        self.user_ids: list[str] = []
        self.restaurant_ids: list[str] = []
        self.counts = {"orders": 0, "payments": 0, "updates": 0, "locations": 0, "users": 0}

    def add_order(self, order: dict) -> None:
        self.active_orders.append(order)

    def get_progressable(self, max_count: int = 3) -> list[dict]:
        """Return up to *max_count* orders eligible for a status transition."""
        eligible = [o for o in self.active_orders if o["status"] not in TERMINAL_STATUSES]
        return random.sample(eligible, min(len(eligible), max_count))


# ---------------------------------------------------------------------------
# Event generators
# ---------------------------------------------------------------------------
def new_order_event(state: ProducerState) -> dict:
    """Generate a brand-new order in PLACED status."""
    user_id = random.choice(state.user_ids) if state.user_ids else uid("usr_")
    restaurant_id = random.choice(state.restaurant_ids) if state.restaurant_ids else uid("rest_")
    city = random.choice(CITIES)
    lat, lon = jitter(*CITY_COORDS[city])
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


def advance_order(order: dict) -> dict:
    """Move an order to the next state in its lifecycle.

    Follows the happy path (PLACED -> CONFIRMED -> ... -> DELIVERED) with a
    ~10 % chance of cancellation at cancellable states.
    """
    next_statuses = ORDER_TRANSITIONS.get(order["status"], [])
    if not next_statuses:
        return order

    # 90% happy path, 10% cancel (only if CANCELLED is a valid next state)
    if "CANCELLED" in next_statuses and random.random() < 0.1:
        new_status = "CANCELLED"
    else:
        new_status = next_statuses[0]

    now = datetime.utcnow()
    updated = {**order, "status": new_status, "updated_at": fmt(now)}
    if new_status == "DELIVERED":
        updated["actual_delivery_mins"] = order["estimated_delivery_mins"] + random.randint(-5, 15)
    return updated


def payment_event(order: dict) -> dict:
    """Generate a payment record for the given order."""
    now = datetime.utcnow()
    if order["status"] in ("PLACED", "CANCELLED"):
        pstatus = "INITIATED"
    else:
        pstatus = "COMPLETED"
    return {
        "payment_id": uid("pay_"),
        "order_id": order["order_id"],
        "amount": order["total_amount"],
        "payment_method": order["payment_method"],
        "payment_status": pstatus,
        "transaction_id": uid("txn_"),
        "gateway_response": json.dumps({"code": "00", "message": "Success", "bank_ref": uid()}),
        "created_at": fmt(now),
        "updated_at": fmt(now),
    }


def user_location_event(user_id: str) -> dict:
    """Generate a GPS location ping for a user."""
    city = random.choice(CITIES)
    lat, lon = jitter(*CITY_COORDS[city], km=5.0)
    now = datetime.utcnow()
    return {
        "user_id": user_id,
        "timestamp": now.isoformat(),
        "latitude": str(lat),
        "longitude": str(lon),
        "city": city,
        "accuracy_meters": str(random.randint(5, 50)),
        "speed_kmh": str(round(random.uniform(0, 60), 1)),
        "ttl": int((now + timedelta(days=7)).timestamp()),
    }


def new_user_event() -> dict:
    """Generate a new user registration event."""
    city = random.choice(CITIES)
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    user_id = uid("usr_")
    now = datetime.utcnow()
    return {
        "user_id": user_id,
        "name": f"{first} {last}",
        "email": f"{first.lower()}.{last.lower()}.{user_id[-4:]}@example.com",
        "phone": f"+91{random.randint(7000000000, 9999999999)}",
        "city": city,
        "signup_date": fmt(now),
        "is_pro_member": False,
        "preferred_cuisine": random.sample(CUISINES, k=2),
        "total_orders": 0,
        "average_order_value": 0.0,
        "last_order_at": None,
        "created_at": fmt(now),
        "updated_at": fmt(now),
    }


# ---------------------------------------------------------------------------
# Bootstrap: seed reference data into MySQL for FK constraints
# ---------------------------------------------------------------------------
def bootstrap_mysql(
    mysql: MySQLWriter,
    users: list[dict],
    restaurants: list[dict],
) -> None:
    """Insert reference users, restaurants, menu items, and promotions into MySQL
    so that streamed orders have valid foreign keys."""
    log.info("Bootstrapping MySQL with reference data...")
    for u in users:
        try:
            mysql.write_user(u)
        except Exception:
            pass  # duplicate on re-run

    for r in restaurants:
        mysql.write_restaurant(r)

    menu_items = generate_menu_items(restaurants)
    for m in menu_items:
        mysql.write_menu_item(m)

    promotions = generate_promotions()
    for pr in promotions:
        mysql.write_promotion(pr)

    log.info("Bootstrap complete (%d users, %d restaurants, %d menu items, %d promos).",
             len(users), len(restaurants), len(menu_items), len(promotions))


# ---------------------------------------------------------------------------
# Main produce loop
# ---------------------------------------------------------------------------
def run(targets: list[str], rate: int, duration: int | None) -> None:
    state = ProducerState()

    # Generate reference data
    log.info("Generating reference data...")
    users = generate_users(50)
    restaurants = generate_restaurants(20)
    state.user_ids = [u["user_id"] for u in users]
    state.restaurant_ids = [r["restaurant_id"] for r in restaurants]

    # Initialise writers
    mysql = MySQLWriter() if "mysql" in targets else None
    dynamo = DynamoDBWriter() if "dynamodb" in targets else None
    kafka = KafkaWriter() if "kafka" in targets else None

    if mysql:
        bootstrap_mysql(mysql, users, restaurants)

    interval = 1.0 / rate
    start = time.time()
    tick = 0

    log.info(
        "Producing at %d events/sec to %s (duration: %s). Press Ctrl+C to stop.",
        rate, targets, f"{duration}s" if duration else "forever",
    )

    def _shutdown(sig: int, frame: object) -> None:
        log.info("Shutting down (signal %d)...", sig)
        if kafka:
            kafka.flush()
        if mysql:
            mysql.close()
        elapsed = time.time() - start
        log.info("Final stats after %.0fs: %s", elapsed, state.counts)
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    while True:
        tick_start = time.time()
        tick += 1

        try:
            # 1. New order
            order = new_order_event(state)
            state.add_order(order)
            state.counts["orders"] += 1

            if mysql:
                mysql.write_order(order)
            if dynamo:
                dynamo.write_order(order)
            if kafka:
                kafka.write_order(order)

            # 2. Payment for new order
            payment = payment_event(order)
            state.counts["payments"] += 1

            if mysql:
                mysql.write_payment(payment)
            if dynamo:
                dynamo.write_payment(payment)
            if kafka:
                kafka.write_payment(payment)

            # 3. Advance 1-3 existing orders through lifecycle
            for active_order in state.get_progressable():
                updated = advance_order(active_order)
                active_order.update(updated)
                state.counts["updates"] += 1

                if mysql:
                    mysql.write_order(updated)
                if dynamo:
                    dynamo.write_order(updated)
                if kafka:
                    kafka.write_order(updated)

            # 4. User location update (every 3 ticks)
            if tick % 3 == 0 and state.user_ids:
                location = user_location_event(random.choice(state.user_ids))
                state.counts["locations"] += 1
                if dynamo:
                    dynamo.write_location(location)
                if kafka:
                    kafka.write_location(location)

            # 5. New user registration (every 20 ticks)
            if tick % 20 == 0:
                user = new_user_event()
                state.user_ids.append(user["user_id"])
                state.counts["users"] += 1
                if mysql:
                    mysql.write_user(user)
                if kafka:
                    kafka.write_user(user)

            # Flush Kafka buffer periodically
            if kafka and tick % 10 == 0:
                kafka.flush()

            # Log progress every 60 ticks
            if tick % 60 == 0:
                elapsed = time.time() - start
                active = sum(1 for o in state.active_orders if o["status"] not in TERMINAL_STATUSES)
                log.info(
                    "tick=%-6d elapsed=%-5.0fs %s active_orders=%d",
                    tick, elapsed, state.counts, active,
                )

        except Exception:
            log.exception("Error at tick=%d", tick)

        # Duration check
        if duration and (time.time() - start) >= duration:
            log.info("Duration reached (%ds). Stopping.", duration)
            break

        # Rate limiting
        sleep_for = interval - (time.time() - tick_start)
        if sleep_for > 0:
            time.sleep(sleep_for)

    # Cleanup
    if kafka:
        kafka.flush()
    if mysql:
        mysql.close()
    log.info("Done. Total ticks: %d | Stats: %s", tick, state.counts)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Real-time data producer for Zomato Data Platform")
    parser.add_argument("--target", choices=["mysql", "dynamodb", "kafka", "all"], default="all")
    parser.add_argument("--rate", type=int, default=5,
                        help="Events per second (default: 5, use 23 for ~2M orders/day)")
    parser.add_argument("--duration", type=int, default=None,
                        help="Run duration in seconds (default: run forever)")
    args = parser.parse_args()

    targets = ["mysql", "dynamodb", "kafka"] if args.target == "all" else [args.target]
    run(targets, args.rate, args.duration)


if __name__ == "__main__":
    main()
