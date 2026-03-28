#!/usr/bin/env python3
"""
Seed Data Generator - Zomato Data Platform
==========================================
One-time bulk load of realistic data into all three pipeline sources:

  - Aurora MySQL  (users, restaurants, menu_items, orders, payments, promotions)
  - DynamoDB      (orders, payments, user_locations)
  - Kafka Topics  (orders, users, menu, promo)

Usage:
    python seed_data.py                            # seed everything
    python seed_data.py --target mysql             # MySQL only
    python seed_data.py --target dynamodb          # DynamoDB only
    python seed_data.py --target kafka             # Kafka only
    python seed_data.py --users 100 --orders 500   # custom volume

Environment variables:
    MYSQL_HOST / MYSQL_PORT / MYSQL_USER / MYSQL_PASSWORD / MYSQL_DB
    DYNAMODB_ENDPOINT / AWS_REGION / DYNAMODB_TABLE_PREFIX
    KAFKA_BOOTSTRAP
"""

from __future__ import annotations

import argparse
import logging
import random

from seed_helpers import (
    DYNAMODB_TABLE_PREFIX,
    DynamoDBWriter,
    KafkaWriter,
    MySQLWriter,
    generate_ddb_user_locations,
    generate_menu_items,
    generate_orders,
    generate_payments,
    generate_promotions,
    generate_restaurants,
    generate_users,
    to_ddb_orders,
    to_ddb_payments,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("seed_data")


# ---------------------------------------------------------------------------
# Seeders
# ---------------------------------------------------------------------------
def seed_mysql(
    users: list[dict],
    restaurants: list[dict],
    menu_items: list[dict],
    orders: list[dict],
    payments: list[dict],
    promotions: list[dict],
) -> None:
    try:
        writer = MySQLWriter(autocommit=False)
    except ImportError:
        log.error("pymysql not installed. Run: pip install pymysql")
        return

    log.info("Inserting %d users...", len(users))
    for u in users:
        writer.write_user(u)

    log.info("Inserting %d restaurants...", len(restaurants))
    for r in restaurants:
        writer.write_restaurant(r)

    log.info("Inserting %d menu items...", len(menu_items))
    for m in menu_items:
        writer.write_menu_item(m)

    log.info("Inserting %d orders...", len(orders))
    for o in orders:
        writer.write_order(o)

    log.info("Inserting %d payments...", len(payments))
    for p in payments:
        writer.write_payment(p)

    log.info("Inserting %d promotions...", len(promotions))
    for pr in promotions:
        writer.write_promotion(pr)

    writer.commit()
    writer.close()
    log.info("MySQL seed complete.")


def seed_dynamodb(
    ddb_orders: list[dict],
    ddb_payments: list[dict],
    ddb_locations: list[dict],
) -> None:
    writer = DynamoDBWriter()
    tables = {
        f"{DYNAMODB_TABLE_PREFIX}-orders": ddb_orders,
        f"{DYNAMODB_TABLE_PREFIX}-payments": ddb_payments,
        f"{DYNAMODB_TABLE_PREFIX}-user-locations": ddb_locations,
    }
    for table_name, records in tables.items():
        writer.batch_write(table_name, records)
    log.info("DynamoDB seed complete.")


def seed_kafka(
    users: list[dict],
    menu_items: list[dict],
    orders: list[dict],
    promotions: list[dict],
) -> None:
    try:
        writer = KafkaWriter()
    except ImportError:
        log.error("confluent-kafka not installed. Run: pip install confluent-kafka")
        return

    sample_orders = random.sample(orders, min(100, len(orders)))
    log.info("Producing %d messages to 'orders' topic...", len(sample_orders))
    for order in sample_orders:
        writer.write_order(order)

    sample_users = random.sample(users, min(50, len(users)))
    log.info("Producing %d messages to 'users' topic...", len(sample_users))
    for user in sample_users:
        writer.write_user(user)

    log.info("Producing %d messages to 'menu' topic...", len(menu_items))
    for item in menu_items:
        writer.write_menu(item)

    log.info("Producing %d messages to 'promo' topic...", len(promotions))
    for promo in promotions:
        writer.write_promo(promo)

    writer.flush()
    log.info("Kafka seed complete.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Seed data generator for Zomato Data Platform")
    parser.add_argument("--target", choices=["mysql", "dynamodb", "kafka", "all"], default="all")
    parser.add_argument("--users", type=int, default=50, help="Number of users (default: 50)")
    parser.add_argument("--restaurants", type=int, default=20, help="Number of restaurants (default: 20)")
    parser.add_argument("--orders", type=int, default=200, help="Number of orders (default: 200)")
    args = parser.parse_args()

    log.info("Generating seed data...")
    users = generate_users(args.users)
    restaurants = generate_restaurants(args.restaurants)
    menu_items = generate_menu_items(restaurants)
    orders = generate_orders(users, restaurants, args.orders)
    payments = generate_payments(orders)
    promotions = generate_promotions()

    log.info(
        "Generated: %d users, %d restaurants, %d menu items, %d orders, %d payments, %d promotions",
        len(users), len(restaurants), len(menu_items), len(orders), len(payments), len(promotions),
    )

    if args.target in ("mysql", "all"):
        log.info("[MySQL] Seeding Aurora MySQL...")
        seed_mysql(users, restaurants, menu_items, orders, payments, promotions)

    if args.target in ("dynamodb", "all"):
        log.info("[DynamoDB] Seeding DynamoDB tables...")
        ddb_orders = to_ddb_orders(orders, n=100)
        ddb_payments = to_ddb_payments(payments, n=100)
        ddb_locations = generate_ddb_user_locations(users, n=50)
        seed_dynamodb(ddb_orders, ddb_payments, ddb_locations)

    if args.target in ("kafka", "all"):
        log.info("[Kafka] Producing seed messages...")
        seed_kafka(users, menu_items, orders, promotions)

    log.info("Done.")


if __name__ == "__main__":
    main()
