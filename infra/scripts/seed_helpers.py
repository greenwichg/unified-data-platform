"""
Shared reference data, helpers, and writer classes for seed_data.py and produce_realtime.py.
"""

from __future__ import annotations

import json
import logging
import os
import random
import uuid
from datetime import datetime, timedelta
from decimal import Decimal

log = logging.getLogger("zomato_seed")

# ---------------------------------------------------------------------------
# Environment configuration
# ---------------------------------------------------------------------------
MYSQL_HOST = os.environ.get("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3306"))
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "root")
MYSQL_DB = os.environ.get("MYSQL_DB", "zomato")

DYNAMODB_ENDPOINT = os.environ.get("DYNAMODB_ENDPOINT", "http://localhost:8000")
AWS_REGION = os.environ.get("AWS_REGION", "ap-south-1")
DYNAMODB_TABLE_PREFIX = os.environ.get("DYNAMODB_TABLE_PREFIX", "zomato-data-platform-dev")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")

# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------
CITIES = ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai", "Pune"]

CITY_COORDS: dict[str, tuple[float, float]] = {
    "Mumbai":    (19.0760, 72.8777),
    "Delhi":     (28.6139, 77.2090),
    "Bangalore": (12.9716, 77.5946),
    "Hyderabad": (17.3850, 78.4867),
    "Chennai":   (13.0827, 80.2707),
    "Pune":      (18.5204, 73.8567),
}

AREAS: dict[str, list[str]] = {
    "Mumbai":    ["Bandra", "Andheri", "Juhu", "Colaba", "Lower Parel", "Powai", "Worli"],
    "Delhi":     ["Connaught Place", "Hauz Khas", "Saket", "Dwarka", "Lajpat Nagar", "Karol Bagh"],
    "Bangalore": ["Koramangala", "Indiranagar", "HSR Layout", "Whitefield", "Jayanagar", "MG Road"],
    "Hyderabad": ["Banjara Hills", "Jubilee Hills", "Madhapur", "Kukatpally", "Gachibowli"],
    "Chennai":   ["T. Nagar", "Adyar", "Anna Nagar", "Velachery", "OMR", "Mylapore"],
    "Pune":      ["Koregaon Park", "Viman Nagar", "Baner", "Hinjewadi", "Kothrud", "Aundh"],
}

CUISINES = [
    "North Indian", "South Indian", "Chinese", "Italian",
    "Mexican", "Thai", "Fast Food", "Biryani",
]

PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "Wallet", "Cash on Delivery"]

ORDER_STATUSES = [
    "PLACED", "CONFIRMED", "PREPARING", "READY",
    "PICKED_UP", "DELIVERING", "DELIVERED", "CANCELLED",
]

FIRST_NAMES = [
    "Aarav", "Vivaan", "Aditya", "Rohit", "Priya", "Pooja", "Sneha", "Rahul",
    "Anjali", "Kavya", "Arjun", "Neha", "Siddharth", "Ritika", "Manish",
    "Divya", "Karthik", "Meera", "Varun", "Ishaan",
]

LAST_NAMES = [
    "Sharma", "Verma", "Patel", "Gupta", "Singh", "Kumar", "Mehta", "Joshi",
    "Nair", "Reddy", "Iyer", "Agarwal", "Chatterjee", "Das", "Mishra",
    "Pillai", "Saxena", "Bhat", "Kulkarni", "Kapoor",
]

RESTAURANT_NAMES = [
    "Spice Garden", "Biryani House", "The Curry Co.", "Punjab Da Dhaba",
    "Dosa Plaza", "Wok & Roll", "Pizza Palace", "Burger Barn",
    "Tandoor Tales", "Masala Magic", "The Noodle Bar", "Saffron Kitchen",
    "Chaat Corner", "Kebab Kingdom", "Tiffin Express", "Rasoi",
    "Madras Cafe", "Dragon Bowl", "Pasta Point", "Bombay Bites",
]

# (name, base_price, is_vegetarian, prep_time_mins)
MENU_CATALOG: dict[str, list[tuple[str, float, bool, int]]] = {
    "North Indian": [
        ("Butter Chicken", 320.0, False, 30),
        ("Dal Makhani", 220.0, True, 25),
        ("Paneer Tikka", 280.0, True, 20),
        ("Chicken Biryani", 350.0, False, 35),
        ("Naan", 40.0, True, 10),
        ("Chole Bhature", 160.0, True, 15),
    ],
    "South Indian": [
        ("Masala Dosa", 120.0, True, 15),
        ("Idli Sambar", 80.0, True, 10),
        ("Vada", 60.0, True, 10),
        ("Chicken Chettinad", 310.0, False, 30),
        ("Filter Coffee", 40.0, True, 5),
        ("Uttapam", 100.0, True, 12),
    ],
    "Chinese": [
        ("Hakka Noodles", 180.0, True, 20),
        ("Chilli Chicken", 260.0, False, 25),
        ("Fried Rice", 160.0, True, 15),
        ("Manchurian", 200.0, True, 20),
        ("Spring Rolls", 140.0, True, 15),
    ],
    "Fast Food": [
        ("Veg Burger", 120.0, True, 10),
        ("Chicken Burger", 160.0, False, 12),
        ("French Fries", 80.0, True, 8),
        ("Pasta Arrabbiata", 200.0, True, 15),
        ("Chocolate Shake", 100.0, True, 5),
    ],
}

PROMO_TEMPLATES = [
    ("WELCOME50",  "PERCENTAGE",    50.0, 200.0, 100.0, 5000),
    ("FLAT100",    "FLAT",         100.0, 500.0, None,  3000),
    ("FREEDEL",    "FREE_DELIVERY", 30.0, 300.0, None,  10000),
    ("SAVE20",     "PERCENTAGE",    20.0, 400.0, 150.0, 8000),
    ("CASHBACK30", "CASHBACK",      30.0, 350.0, 80.0,  4000),
    ("WEEKEND25",  "PERCENTAGE",    25.0, 450.0, 120.0, 6000),
    ("NEWUSER75",  "PERCENTAGE",    75.0, 0.0,   200.0, 1000),
    ("LUNCH15",    "PERCENTAGE",    15.0, 250.0, 60.0,  12000),
    ("FLAT150",    "FLAT",         150.0, 700.0, None,  2000),
    ("MONSOON40",  "PERCENTAGE",    40.0, 300.0, 130.0, 7000),
]

# Order lifecycle state machine
ORDER_TRANSITIONS: dict[str, list[str]] = {
    "PLACED":     ["CONFIRMED", "CANCELLED"],
    "CONFIRMED":  ["PREPARING", "CANCELLED"],
    "PREPARING":  ["READY"],
    "READY":      ["PICKED_UP"],
    "PICKED_UP":  ["DELIVERING"],
    "DELIVERING": ["DELIVERED"],
    "DELIVERED":  [],
    "CANCELLED":  [],
}

TERMINAL_STATUSES = frozenset({"DELIVERED", "CANCELLED"})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def uid(prefix: str = "") -> str:
    return f"{prefix}{uuid.uuid4().hex[:12]}"


def rand_dt(days_back: int = 30) -> datetime:
    """Random datetime within the last *days_back* days."""
    offset_minutes = random.randint(0, days_back * 24 * 60)
    return datetime.utcnow() - timedelta(minutes=offset_minutes)


def fmt(dt: datetime) -> str:
    """Format datetime for MySQL."""
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def jitter(lat: float, lon: float, km: float = 2.0) -> tuple[float, float]:
    """Add random geographic jitter within *km* radius."""
    delta = km / 111.0  # ~1 degree latitude ≈ 111 km
    return (
        round(lat + random.uniform(-delta, delta), 6),
        round(lon + random.uniform(-delta, delta), 6),
    )


# ---------------------------------------------------------------------------
# Data generation
# ---------------------------------------------------------------------------
def generate_users(n: int = 50) -> list[dict]:
    users: list[dict] = []
    for _ in range(n):
        city = random.choice(CITIES)
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        name = f"{first} {last}"
        user_id = uid("usr_")
        signup = rand_dt(365)
        last_order = rand_dt(30) if random.random() > 0.2 else None
        users.append({
            "user_id": user_id,
            "name": name,
            "email": f"{first.lower()}.{last.lower()}.{user_id[-4:]}@example.com",
            "phone": f"+91{random.randint(7000000000, 9999999999)}",
            "city": city,
            "signup_date": fmt(signup),
            "is_pro_member": random.random() < 0.25,
            "preferred_cuisine": random.sample(CUISINES, k=random.randint(1, 3)),
            "total_orders": random.randint(0, 200),
            "average_order_value": round(random.uniform(200, 900), 2),
            "last_order_at": fmt(last_order) if last_order else None,
            "created_at": fmt(signup),
            "updated_at": fmt(rand_dt(7)),
        })
    return users


def generate_restaurants(n: int = 20) -> list[dict]:
    restaurants: list[dict] = []
    for i in range(n):
        city = random.choice(CITIES)
        lat, lon = jitter(*CITY_COORDS[city])
        cuisine = random.sample(CUISINES, k=random.randint(1, 3))
        created = rand_dt(730)
        base_name = RESTAURANT_NAMES[i % len(RESTAURANT_NAMES)]
        suffix = f" {i // len(RESTAURANT_NAMES) + 1}" if i >= len(RESTAURANT_NAMES) else ""
        restaurants.append({
            "restaurant_id": uid("rest_"),
            "name": f"{base_name}{suffix}",
            "city": city,
            "area": random.choice(AREAS.get(city, ["Sector 1"])),
            "cuisine_types": cuisine,
            "rating": round(random.uniform(3.0, 5.0), 1),
            "total_reviews": random.randint(50, 5000),
            "avg_cost_for_two": round(random.uniform(200, 1200), 2),
            "is_active": random.random() > 0.05,
            "latitude": lat,
            "longitude": lon,
            "created_at": fmt(created),
            "updated_at": fmt(rand_dt(30)),
        })
    return restaurants


def generate_menu_items(restaurants: list[dict]) -> list[dict]:
    items: list[dict] = []
    for restaurant in restaurants:
        cuisine_key = next(
            (c for c in restaurant["cuisine_types"] if c in MENU_CATALOG),
            "Fast Food",
        )
        candidates = MENU_CATALOG[cuisine_key]
        count = min(len(candidates), random.randint(3, 5))
        for name, base_price, is_veg, prep_time in random.sample(candidates, k=count):
            price = round(base_price * random.uniform(0.9, 1.2), 2)
            created = rand_dt(365)
            items.append({
                "item_id": uid("item_"),
                "restaurant_id": restaurant["restaurant_id"],
                "name": name,
                "description": f"Delicious {name} prepared fresh to order",
                "category": "Main Course" if base_price > 100 else "Sides & Drinks",
                "cuisine_type": cuisine_key,
                "price": price,
                "is_vegetarian": is_veg,
                "is_available": random.random() > 0.1,
                "preparation_time_mins": max(5, prep_time + random.randint(-5, 10)),
                "rating": round(random.uniform(3.5, 5.0), 1),
                "total_orders": random.randint(10, 3000),
                "image_url": f"https://img.zomato.com/menu/{uid()}.jpg",
                "created_at": fmt(created),
                "updated_at": fmt(rand_dt(60)),
            })
    return items


def generate_orders(
    users: list[dict],
    restaurants: list[dict],
    n: int = 200,
) -> list[dict]:
    status_weights = [5, 5, 10, 5, 5, 10, 55, 5]
    orders: list[dict] = []
    for _ in range(n):
        user = random.choice(users)
        restaurant = random.choice(restaurants)
        city = user["city"]
        lat, lon = jitter(*CITY_COORDS[city])
        subtotal = round(random.uniform(150, 1200), 2)
        tax = round(subtotal * 0.05, 2)
        delivery_fee = random.choice([0.0, 20.0, 30.0, 40.0, 49.0])
        total = round(subtotal + tax + delivery_fee, 2)
        status = random.choices(ORDER_STATUSES, weights=status_weights)[0]
        created = rand_dt(30)
        est_mins = random.randint(25, 60)
        actual_mins = est_mins + random.randint(-10, 20) if status == "DELIVERED" else None
        orders.append({
            "order_id": uid("ord_"),
            "user_id": user["user_id"],
            "restaurant_id": restaurant["restaurant_id"],
            "status": status,
            "subtotal": subtotal,
            "tax": tax,
            "delivery_fee": delivery_fee,
            "total_amount": total,
            "payment_method": random.choice(PAYMENT_METHODS),
            "delivery_latitude": lat,
            "delivery_longitude": lon,
            "delivery_address": f"{random.randint(1, 999)}, {random.choice(AREAS.get(city, ['Sector 1']))}, {city}",
            "city": city,
            "pincode": str(random.randint(100000, 999999)),
            "estimated_delivery_mins": est_mins,
            "actual_delivery_mins": actual_mins,
            "created_at": fmt(created),
            "updated_at": fmt(created + timedelta(minutes=random.randint(1, 60))),
        })
    return orders


def generate_payments(orders: list[dict]) -> list[dict]:
    payments: list[dict] = []
    for order in orders:
        if order["status"] == "CANCELLED" and random.random() > 0.3:
            pstatus = "REFUNDED"
        elif order["status"] in ("DELIVERED", "PICKED_UP", "DELIVERING"):
            pstatus = "COMPLETED"
        elif order["status"] == "PLACED":
            pstatus = random.choice(["INITIATED", "PROCESSING"])
        else:
            pstatus = "COMPLETED"
        created = datetime.strptime(order["created_at"], "%Y-%m-%d %H:%M:%S")
        payments.append({
            "payment_id": uid("pay_"),
            "order_id": order["order_id"],
            "amount": order["total_amount"],
            "payment_method": order["payment_method"],
            "payment_status": pstatus,
            "transaction_id": uid("txn_"),
            "gateway_response": json.dumps({"code": "00", "message": "Success", "bank_ref": uid()}),
            "created_at": fmt(created),
            "updated_at": fmt(created + timedelta(seconds=random.randint(1, 30))),
        })
    return payments


def generate_promotions() -> list[dict]:
    promos: list[dict] = []
    now = datetime.utcnow()
    for code, dtype, dvalue, min_order, max_disc, max_use in PROMO_TEMPLATES:
        valid_from = now - timedelta(days=random.randint(1, 30))
        valid_until = now + timedelta(days=random.randint(7, 60))
        unit = "%" if dtype == "PERCENTAGE" else " INR"
        promos.append({
            "promo_id": uid("promo_"),
            "promo_code": code,
            "description": f"{dvalue}{unit} off on orders above {min_order}",
            "discount_type": dtype,
            "discount_value": dvalue,
            "min_order_value": min_order,
            "max_discount": max_disc,
            "valid_from": fmt(valid_from),
            "valid_until": fmt(valid_until),
            "applicable_restaurants": json.dumps([]),
            "applicable_cities": json.dumps(random.sample(CITIES, k=random.randint(2, 6))),
            "usage_count": random.randint(0, max_use // 2),
            "max_usage": max_use,
            "is_active": True,
            "created_at": fmt(valid_from),
            "updated_at": fmt(valid_from),
        })
    return promos


# ---------------------------------------------------------------------------
# DynamoDB record converters
# ---------------------------------------------------------------------------
def to_ddb_orders(orders: list[dict], n: int = 100) -> list[dict]:
    records: list[dict] = []
    for order in random.sample(orders, min(n, len(orders))):
        records.append({
            "order_id": order["order_id"],
            "created_at": order["created_at"],
            "user_id": order["user_id"],
            "restaurant_id": order["restaurant_id"],
            "status": order["status"],
            "total_amount": str(order["total_amount"]),
            "payment_method": order["payment_method"],
            "city": order["city"],
            "delivery_address": order["delivery_address"],
            "estimated_delivery_mins": order["estimated_delivery_mins"],
            "updated_at": order["updated_at"],
        })
    return records


def to_ddb_payments(payments: list[dict], n: int = 100) -> list[dict]:
    records: list[dict] = []
    for payment in random.sample(payments, min(n, len(payments))):
        records.append({
            "payment_id": payment["payment_id"],
            "order_id": payment["order_id"],
            "amount": str(payment["amount"]),
            "payment_method": payment["payment_method"],
            "payment_status": payment["payment_status"],
            "transaction_id": payment["transaction_id"],
            "created_at": payment["created_at"],
            "updated_at": payment["updated_at"],
        })
    return records


def generate_ddb_user_locations(users: list[dict], n: int = 50) -> list[dict]:
    records: list[dict] = []
    for user in random.sample(users, min(n, len(users))):
        lat, lon = jitter(*CITY_COORDS[user["city"]], km=5.0)
        ts = rand_dt(1)
        records.append({
            "user_id": user["user_id"],
            "timestamp": ts.isoformat(),
            "latitude": str(lat),
            "longitude": str(lon),
            "city": user["city"],
            "accuracy_meters": str(random.randint(5, 50)),
            "speed_kmh": str(round(random.uniform(0, 60), 1)),
            "ttl": int((ts + timedelta(days=7)).timestamp()),
        })
    return records


# ---------------------------------------------------------------------------
# Writer: MySQL
# ---------------------------------------------------------------------------
class MySQLWriter:
    """Writes seed/streaming data to Aurora MySQL."""

    def __init__(self, autocommit: bool = True):
        import pymysql

        self.conn = pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB,
            charset="utf8mb4",
            autocommit=autocommit,
        )
        log.info("MySQL connected (%s:%d/%s).", MYSQL_HOST, MYSQL_PORT, MYSQL_DB)

    # --- single-row upserts (for streaming) ---

    def write_order(self, order: dict) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO orders
                       (order_id, user_id, restaurant_id, status, subtotal, tax,
                        delivery_fee, total_amount, payment_method, delivery_latitude,
                        delivery_longitude, delivery_address, city, pincode,
                        estimated_delivery_mins, actual_delivery_mins, created_at, updated_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON DUPLICATE KEY UPDATE
                       status=VALUES(status),
                       actual_delivery_mins=VALUES(actual_delivery_mins),
                       updated_at=VALUES(updated_at)""",
                (
                    order["order_id"], order["user_id"], order["restaurant_id"],
                    order["status"], order["subtotal"], order["tax"],
                    order["delivery_fee"], order["total_amount"], order["payment_method"],
                    order["delivery_latitude"], order["delivery_longitude"],
                    order["delivery_address"], order["city"], order["pincode"],
                    order["estimated_delivery_mins"], order["actual_delivery_mins"],
                    order["created_at"], order["updated_at"],
                ),
            )

    def write_payment(self, payment: dict) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT IGNORE INTO payments
                       (payment_id, order_id, amount, payment_method, payment_status,
                        transaction_id, gateway_response, created_at, updated_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    payment["payment_id"], payment["order_id"], payment["amount"],
                    payment["payment_method"], payment["payment_status"],
                    payment["transaction_id"], payment["gateway_response"],
                    payment["created_at"], payment["updated_at"],
                ),
            )

    def write_user(self, user: dict) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT IGNORE INTO users
                       (user_id, name, email, phone, city, signup_date, is_pro_member,
                        preferred_cuisine, total_orders, average_order_value,
                        last_order_at, created_at, updated_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    user["user_id"], user["name"], user["email"], user["phone"],
                    user["city"], user["signup_date"], user["is_pro_member"],
                    json.dumps(user["preferred_cuisine"]),
                    user["total_orders"], user["average_order_value"],
                    user["last_order_at"], user["created_at"], user["updated_at"],
                ),
            )

    # --- batch inserts (for seed) ---

    def write_restaurant(self, r: dict) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT IGNORE INTO restaurants
                       (restaurant_id, name, city, area, cuisine_types, rating,
                        total_reviews, avg_cost_for_two, is_active, latitude, longitude,
                        created_at, updated_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    r["restaurant_id"], r["name"], r["city"], r["area"],
                    json.dumps(r["cuisine_types"]), r["rating"], r["total_reviews"],
                    r["avg_cost_for_two"], r["is_active"], r["latitude"], r["longitude"],
                    r["created_at"], r["updated_at"],
                ),
            )

    def write_menu_item(self, m: dict) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT IGNORE INTO menu_items
                       (item_id, restaurant_id, name, description, category, cuisine_type,
                        price, is_vegetarian, is_available, preparation_time_mins,
                        rating, total_orders, image_url, created_at, updated_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    m["item_id"], m["restaurant_id"], m["name"], m["description"],
                    m["category"], m["cuisine_type"], m["price"], m["is_vegetarian"],
                    m["is_available"], m["preparation_time_mins"], m["rating"],
                    m["total_orders"], m["image_url"], m["created_at"], m["updated_at"],
                ),
            )

    def write_promotion(self, pr: dict) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT IGNORE INTO promotions
                       (promo_id, promo_code, description, discount_type, discount_value,
                        min_order_value, max_discount, valid_from, valid_until,
                        applicable_restaurants, applicable_cities, usage_count,
                        max_usage, is_active, created_at, updated_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    pr["promo_id"], pr["promo_code"], pr["description"],
                    pr["discount_type"], pr["discount_value"], pr["min_order_value"],
                    pr["max_discount"], pr["valid_from"], pr["valid_until"],
                    pr["applicable_restaurants"], pr["applicable_cities"],
                    pr["usage_count"], pr["max_usage"], pr["is_active"],
                    pr["created_at"], pr["updated_at"],
                ),
            )

    def commit(self) -> None:
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


# ---------------------------------------------------------------------------
# Writer: DynamoDB
# ---------------------------------------------------------------------------
class DynamoDBWriter:
    """Writes seed/streaming data to DynamoDB tables."""

    def __init__(self) -> None:
        import boto3

        kwargs: dict = {"region_name": AWS_REGION}
        if DYNAMODB_ENDPOINT:
            kwargs["endpoint_url"] = DYNAMODB_ENDPOINT
        ddb = boto3.resource("dynamodb", **kwargs)
        self.orders_table = ddb.Table(f"{DYNAMODB_TABLE_PREFIX}-orders")
        self.payments_table = ddb.Table(f"{DYNAMODB_TABLE_PREFIX}-payments")
        self.locations_table = ddb.Table(f"{DYNAMODB_TABLE_PREFIX}-user-locations")
        log.info("DynamoDB connected (prefix=%s).", DYNAMODB_TABLE_PREFIX)

    def write_order(self, order: dict) -> None:
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

    def write_payment(self, payment: dict) -> None:
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

    def write_location(self, location: dict) -> None:
        self.locations_table.put_item(Item=location)

    def batch_write(self, table_name: str, records: list[dict]) -> None:
        from botocore.exceptions import ClientError

        import boto3
        kwargs: dict = {"region_name": AWS_REGION}
        if DYNAMODB_ENDPOINT:
            kwargs["endpoint_url"] = DYNAMODB_ENDPOINT
        ddb = boto3.resource("dynamodb", **kwargs)
        try:
            table = ddb.Table(table_name)
            table.load()
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                log.warning("Table '%s' not found, skipping.", table_name)
                return
            raise
        with table.batch_writer() as batch:
            for record in records:
                batch.put_item(Item=record)
        log.info("Wrote %d records to %s.", len(records), table_name)


# ---------------------------------------------------------------------------
# Writer: Kafka
# ---------------------------------------------------------------------------
class KafkaWriter:
    """Produces events to Kafka / MSK topics."""

    def __init__(self) -> None:
        from confluent_kafka import Producer

        self.producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
        log.info("Kafka connected (%s).", KAFKA_BOOTSTRAP)

    def _produce(self, topic: str, key: str, payload: dict) -> None:
        enriched = {**payload, "event_timestamp": datetime.utcnow().isoformat()}
        self.producer.produce(
            topic,
            key=key.encode(),
            value=json.dumps(enriched).encode(),
        )
        self.producer.poll(0)

    def write_order(self, order: dict) -> None:
        self._produce("orders", order["order_id"], {**order, "event_type": "ORDER_EVENT", "source": "pipeline4"})

    def write_payment(self, payment: dict) -> None:
        self._produce("orders", payment["order_id"], {**payment, "event_type": "PAYMENT_EVENT", "source": "pipeline4"})

    def write_user(self, user: dict) -> None:
        self._produce("users", user["user_id"], {**user, "event_type": "USER_EVENT", "source": "pipeline4"})

    def write_location(self, location: dict) -> None:
        self._produce("topics", location["user_id"], {**location, "event_type": "LOCATION_EVENT", "source": "pipeline4"})

    def write_menu(self, item: dict) -> None:
        self._produce("menu", item["item_id"], {**item, "event_type": "MENU_EVENT", "source": "pipeline4"})

    def write_promo(self, promo: dict) -> None:
        self._produce("promo", promo["promo_id"], {**promo, "event_type": "PROMO_EVENT", "source": "pipeline4"})

    def flush(self, timeout: float = 10) -> None:
        self.producer.flush(timeout=timeout)
