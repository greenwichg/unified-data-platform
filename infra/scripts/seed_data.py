#!/usr/bin/env python3
"""
Seed Data Generator - Zomato Data Platform
==========================================
Generates realistic seed data for:
  - Aurora MySQL  (users, restaurants, menu_items, orders, payments, promotions)
  - DynamoDB      (orders, payments, user_locations)
  - Kafka Topics  (orders, users, menu, promo)

Usage:
    # Seed everything (default: localhost endpoints)
    python seed_data.py

    # Seed individual targets
    python seed_data.py --target mysql
    python seed_data.py --target dynamodb
    python seed_data.py --target kafka

Environment variables:
    MYSQL_HOST        (default: localhost)
    MYSQL_PORT        (default: 3306)
    MYSQL_USER        (default: root)
    MYSQL_PASSWORD    (default: root)
    MYSQL_DB          (default: zomato)

    DYNAMODB_ENDPOINT (default: http://localhost:8000)
    AWS_REGION        (default: ap-south-1)
    AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (for local: use any value)
    DYNAMODB_TABLE_PREFIX (default: zomato-data-platform-dev)

    KAFKA_BOOTSTRAP   (default: localhost:9092)
"""

import argparse
import json
import os
import random
import uuid
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Config from environment
# ---------------------------------------------------------------------------
MYSQL_HOST = os.environ.get("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", 3306))
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

CITY_COORDS = {
    "Mumbai":    (19.0760, 72.8777),
    "Delhi":     (28.6139, 77.2090),
    "Bangalore": (12.9716, 77.5946),
    "Hyderabad": (17.3850, 78.4867),
    "Chennai":   (13.0827, 80.2707),
    "Pune":      (18.5204, 73.8567),
}

CUISINES = ["North Indian", "South Indian", "Chinese", "Italian", "Mexican", "Thai", "Fast Food", "Biryani"]

PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "Wallet", "Cash on Delivery"]

ORDER_STATUSES = ["PLACED", "CONFIRMED", "PREPARING", "READY", "PICKED_UP", "DELIVERING", "DELIVERED", "CANCELLED"]

FIRST_NAMES = ["Aarav", "Vivaan", "Aditya", "Rohit", "Priya", "Pooja", "Sneha", "Rahul",
               "Anjali", "Kavya", "Arjun", "Neha", "Siddharth", "Ritika", "Manish"]

LAST_NAMES = ["Sharma", "Verma", "Patel", "Gupta", "Singh", "Kumar", "Mehta", "Joshi",
              "Nair", "Reddy", "Iyer", "Agarwal", "Chatterjee", "Das", "Mishra"]

RESTAURANT_NAMES = [
    "Spice Garden", "Biryani House", "The Curry Co.", "Punjab Da Dhaba",
    "Dosa Plaza", "Wok & Roll", "Pizza Palace", "Burger Barn",
    "Tandoor Tales", "Masala Magic", "The Noodle Bar", "Saffron Kitchen",
    "Chaat Corner", "Kebab Kingdom", "Tiffin Express",
]

MENU_ITEMS = {
    "North Indian": [
        ("Butter Chicken", 320.0, False, 30),
        ("Dal Makhani", 220.0, True, 25),
        ("Paneer Tikka", 280.0, True, 20),
        ("Chicken Biryani", 350.0, False, 35),
        ("Naan", 40.0, True, 10),
    ],
    "South Indian": [
        ("Masala Dosa", 120.0, True, 15),
        ("Idli Sambar", 80.0, True, 10),
        ("Vada", 60.0, True, 10),
        ("Chicken Chettinad", 310.0, False, 30),
        ("Filter Coffee", 40.0, True, 5),
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

PROMO_DATA = [
    ("WELCOME50", "PERCENTAGE", 50.0, 200.0, 100.0, 5000),
    ("FLAT100", "FLAT", 100.0, 500.0, None, 3000),
    ("FREEDEL", "FREE_DELIVERY", 30.0, 300.0, None, 10000),
    ("SAVE20", "PERCENTAGE", 20.0, 400.0, 150.0, 8000),
    ("CASHBACK30", "CASHBACK", 30.0, 350.0, 80.0, 4000),
    ("WEEKEND25", "PERCENTAGE", 25.0, 450.0, 120.0, 6000),
    ("NEWUSER75", "PERCENTAGE", 75.0, 0.0, 200.0, 1000),
    ("LUNCH15", "PERCENTAGE", 15.0, 250.0, 60.0, 12000),
    ("FLAT150", "FLAT", 150.0, 700.0, None, 2000),
    ("MONSOON40", "PERCENTAGE", 40.0, 300.0, 130.0, 7000),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def uid(prefix=""):
    return f"{prefix}{uuid.uuid4().hex[:12]}"


def rand_dt(days_back=30):
    """Random datetime within the last N days."""
    offset = random.randint(0, days_back * 24 * 60)
    return datetime.utcnow() - timedelta(minutes=offset)


def fmt(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def jitter(lat, lon, km=2.0):
    delta = km / 111.0
    return round(lat + random.uniform(-delta, delta), 6), round(lon + random.uniform(-delta, delta), 6)


# ---------------------------------------------------------------------------
# Data generation
# ---------------------------------------------------------------------------
def generate_users(n=50):
    users = []
    for _ in range(n):
        city = random.choice(CITIES)
        name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
        uid_ = uid("usr_")
        signup = rand_dt(365)
        last_order = rand_dt(30) if random.random() > 0.2 else None
        users.append({
            "user_id": uid_,
            "name": name,
            "email": f"{name.lower().replace(' ', '.')}.{uid_[-4:]}@example.com",
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


def generate_restaurants(n=20):
    restaurants = []
    for i in range(n):
        city = random.choice(CITIES)
        lat, lon = CITY_COORDS[city]
        lat, lon = jitter(lat, lon)
        cuisine = random.sample(CUISINES, k=random.randint(1, 3))
        created = rand_dt(730)
        restaurants.append({
            "restaurant_id": uid("rest_"),
            "name": RESTAURANT_NAMES[i % len(RESTAURANT_NAMES)] + (f" {i // len(RESTAURANT_NAMES) + 1}" if i >= len(RESTAURANT_NAMES) else ""),
            "city": city,
            "area": f"Area {random.randint(1, 20)}",
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


def generate_menu_items(restaurants):
    items = []
    for restaurant in restaurants:
        cuisine_key = next((c for c in restaurant["cuisine_types"] if c in MENU_ITEMS), "Fast Food")
        candidates = MENU_ITEMS[cuisine_key]
        for name, base_price, is_veg, prep_time in random.sample(candidates, k=min(len(candidates), random.randint(3, 5))):
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
                "preparation_time_mins": prep_time + random.randint(-5, 10),
                "rating": round(random.uniform(3.5, 5.0), 1),
                "total_orders": random.randint(10, 3000),
                "image_url": f"https://img.zomato.com/menu/{uid()}.jpg",
                "created_at": fmt(created),
                "updated_at": fmt(rand_dt(60)),
            })
    return items


def generate_orders(users, restaurants, n=200):
    orders = []
    for _ in range(n):
        user = random.choice(users)
        restaurant = random.choice(restaurants)
        city = user["city"]
        lat, lon = CITY_COORDS[city]
        lat, lon = jitter(lat, lon)
        subtotal = round(random.uniform(150, 1200), 2)
        tax = round(subtotal * 0.05, 2)
        delivery_fee = random.choice([0.0, 20.0, 30.0, 40.0, 49.0])
        total = round(subtotal + tax + delivery_fee, 2)
        status = random.choices(ORDER_STATUSES, weights=[5, 5, 10, 5, 5, 10, 55, 5])[0]
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
            "delivery_address": f"{random.randint(1, 999)}, Sector {random.randint(1, 50)}, {city}",
            "city": city,
            "pincode": str(random.randint(100000, 999999)),
            "estimated_delivery_mins": est_mins,
            "actual_delivery_mins": actual_mins,
            "created_at": fmt(created),
            "updated_at": fmt(created + timedelta(minutes=random.randint(1, 60))),
        })
    return orders


def generate_payments(orders):
    payments = []
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


def generate_promotions():
    promos = []
    now = datetime.utcnow()
    for code, dtype, dvalue, min_order, max_disc, max_use in PROMO_DATA:
        valid_from = now - timedelta(days=random.randint(1, 30))
        valid_until = now + timedelta(days=random.randint(7, 60))
        promos.append({
            "promo_id": uid("promo_"),
            "promo_code": code,
            "description": f"{dvalue}{'%' if dtype == 'PERCENTAGE' else ' INR'} off on orders above {min_order}",
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
# DynamoDB records
# ---------------------------------------------------------------------------
def to_ddb_orders(orders, n=100):
    """Convert a sample of orders to DynamoDB format."""
    records = []
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


def to_ddb_payments(payments, n=100):
    records = []
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


def generate_ddb_user_locations(users, n=50):
    records = []
    for user in random.sample(users, min(n, len(users))):
        lat, lon = CITY_COORDS[user["city"]]
        lat, lon = jitter(lat, lon, km=5.0)
        ts = rand_dt(1)
        records.append({
            "user_id": user["user_id"],
            "timestamp": ts.isoformat(),
            "latitude": str(round(lat, 6)),
            "longitude": str(round(lon, 6)),
            "city": user["city"],
            "accuracy_meters": str(random.randint(5, 50)),
            "speed_kmh": str(round(random.uniform(0, 60), 1)),
            "ttl": int((ts + timedelta(days=7)).timestamp()),
        })
    return records


# ---------------------------------------------------------------------------
# MySQL seeding
# ---------------------------------------------------------------------------
def seed_mysql(users, restaurants, menu_items, orders, payments, promotions):
    try:
        import pymysql
    except ImportError:
        print("  [!] pymysql not installed. Run: pip install pymysql")
        print("  [!] Falling back to writing SQL file: infra/scripts/mysql-init/02-seed.sql")
        _write_seed_sql(users, restaurants, menu_items, orders, payments, promotions)
        return

    conn = pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        user=MYSQL_USER, password=MYSQL_PASSWORD,
        database=MYSQL_DB, charset="utf8mb4",
        autocommit=False,
    )
    try:
        with conn.cursor() as cur:
            print(f"  Inserting {len(users)} users...")
            for u in users:
                cur.execute("""
                    INSERT IGNORE INTO users
                        (user_id, name, email, phone, city, signup_date, is_pro_member,
                         preferred_cuisine, total_orders, average_order_value, last_order_at,
                         created_at, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    u["user_id"], u["name"], u["email"], u["phone"], u["city"],
                    u["signup_date"], u["is_pro_member"],
                    json.dumps(u["preferred_cuisine"]),
                    u["total_orders"], u["average_order_value"],
                    u["last_order_at"], u["created_at"], u["updated_at"],
                ))

            print(f"  Inserting {len(restaurants)} restaurants...")
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

            print(f"  Inserting {len(menu_items)} menu items...")
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

            print(f"  Inserting {len(orders)} orders...")
            for o in orders:
                cur.execute("""
                    INSERT IGNORE INTO orders
                        (order_id, user_id, restaurant_id, status, subtotal, tax,
                         delivery_fee, total_amount, payment_method, delivery_latitude,
                         delivery_longitude, delivery_address, city, pincode,
                         estimated_delivery_mins, actual_delivery_mins,
                         created_at, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    o["order_id"], o["user_id"], o["restaurant_id"], o["status"],
                    o["subtotal"], o["tax"], o["delivery_fee"], o["total_amount"],
                    o["payment_method"], o["delivery_latitude"], o["delivery_longitude"],
                    o["delivery_address"], o["city"], o["pincode"],
                    o["estimated_delivery_mins"], o["actual_delivery_mins"],
                    o["created_at"], o["updated_at"],
                ))

            print(f"  Inserting {len(payments)} payments...")
            for p in payments:
                cur.execute("""
                    INSERT IGNORE INTO payments
                        (payment_id, order_id, amount, payment_method, payment_status,
                         transaction_id, gateway_response, created_at, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    p["payment_id"], p["order_id"], p["amount"], p["payment_method"],
                    p["payment_status"], p["transaction_id"], p["gateway_response"],
                    p["created_at"], p["updated_at"],
                ))

            print(f"  Inserting {len(promotions)} promotions...")
            for pr in promotions:
                cur.execute("""
                    INSERT IGNORE INTO promotions
                        (promo_id, promo_code, description, discount_type, discount_value,
                         min_order_value, max_discount, valid_from, valid_until,
                         applicable_restaurants, applicable_cities, usage_count,
                         max_usage, is_active, created_at, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    pr["promo_id"], pr["promo_code"], pr["description"],
                    pr["discount_type"], pr["discount_value"], pr["min_order_value"],
                    pr["max_discount"], pr["valid_from"], pr["valid_until"],
                    pr["applicable_restaurants"], pr["applicable_cities"],
                    pr["usage_count"], pr["max_usage"], pr["is_active"],
                    pr["created_at"], pr["updated_at"],
                ))

        conn.commit()
        print("  MySQL seed complete.")
    finally:
        conn.close()


def _write_seed_sql(users, restaurants, menu_items, orders, payments, promotions):
    """Fallback: write a SQL file loadable by docker-compose mysql-init."""
    lines = ["-- Auto-generated seed data\n", "USE zomato;\n\n"]

    for u in users:
        lines.append(
            f"INSERT IGNORE INTO users (user_id,name,email,phone,city,signup_date,is_pro_member,"
            f"preferred_cuisine,total_orders,average_order_value,last_order_at,created_at,updated_at) VALUES ("
            f"{_sq(u['user_id'])},{_sq(u['name'])},{_sq(u['email'])},{_sq(u['phone'])},{_sq(u['city'])},"
            f"{_sq(u['signup_date'])},{int(u['is_pro_member'])},{_sq(json.dumps(u['preferred_cuisine']))},"
            f"{u['total_orders']},{u['average_order_value']},{_sq(u['last_order_at'])if u['last_order_at'] else 'NULL'},"
            f"{_sq(u['created_at'])},{_sq(u['updated_at'])});\n"
        )

    for r in restaurants:
        lines.append(
            f"INSERT IGNORE INTO restaurants (restaurant_id,name,city,area,cuisine_types,rating,"
            f"total_reviews,avg_cost_for_two,is_active,latitude,longitude,created_at,updated_at) VALUES ("
            f"{_sq(r['restaurant_id'])},{_sq(r['name'])},{_sq(r['city'])},{_sq(r['area'])},"
            f"{_sq(json.dumps(r['cuisine_types']))},{r['rating']},{r['total_reviews']},"
            f"{r['avg_cost_for_two']},{int(r['is_active'])},{r['latitude']},{r['longitude']},"
            f"{_sq(r['created_at'])},{_sq(r['updated_at'])});\n"
        )

    for m in menu_items:
        lines.append(
            f"INSERT IGNORE INTO menu_items (item_id,restaurant_id,name,description,category,cuisine_type,"
            f"price,is_vegetarian,is_available,preparation_time_mins,rating,total_orders,image_url,"
            f"created_at,updated_at) VALUES ("
            f"{_sq(m['item_id'])},{_sq(m['restaurant_id'])},{_sq(m['name'])},{_sq(m['description'])},"
            f"{_sq(m['category'])},{_sq(m['cuisine_type'])},{m['price']},{int(m['is_vegetarian'])},"
            f"{int(m['is_available'])},{m['preparation_time_mins']},{m['rating']},{m['total_orders']},"
            f"{_sq(m['image_url'])},{_sq(m['created_at'])},{_sq(m['updated_at'])});\n"
        )

    for o in orders:
        lines.append(
            f"INSERT IGNORE INTO orders (order_id,user_id,restaurant_id,status,subtotal,tax,"
            f"delivery_fee,total_amount,payment_method,delivery_latitude,delivery_longitude,"
            f"delivery_address,city,pincode,estimated_delivery_mins,actual_delivery_mins,"
            f"created_at,updated_at) VALUES ("
            f"{_sq(o['order_id'])},{_sq(o['user_id'])},{_sq(o['restaurant_id'])},{_sq(o['status'])},"
            f"{o['subtotal']},{o['tax']},{o['delivery_fee']},{o['total_amount']},"
            f"{_sq(o['payment_method'])},{o['delivery_latitude']},{o['delivery_longitude']},"
            f"{_sq(o['delivery_address'])},{_sq(o['city'])},{_sq(o['pincode'])},"
            f"{o['estimated_delivery_mins']},{'NULL' if o['actual_delivery_mins'] is None else o['actual_delivery_mins']},"
            f"{_sq(o['created_at'])},{_sq(o['updated_at'])});\n"
        )

    for p in payments:
        lines.append(
            f"INSERT IGNORE INTO payments (payment_id,order_id,amount,payment_method,payment_status,"
            f"transaction_id,gateway_response,created_at,updated_at) VALUES ("
            f"{_sq(p['payment_id'])},{_sq(p['order_id'])},{p['amount']},{_sq(p['payment_method'])},"
            f"{_sq(p['payment_status'])},{_sq(p['transaction_id'])},{_sq(p['gateway_response'])},"
            f"{_sq(p['created_at'])},{_sq(p['updated_at'])});\n"
        )

    for pr in promotions:
        lines.append(
            f"INSERT IGNORE INTO promotions (promo_id,promo_code,description,discount_type,discount_value,"
            f"min_order_value,max_discount,valid_from,valid_until,applicable_restaurants,applicable_cities,"
            f"usage_count,max_usage,is_active,created_at,updated_at) VALUES ("
            f"{_sq(pr['promo_id'])},{_sq(pr['promo_code'])},{_sq(pr['description'])},"
            f"{_sq(pr['discount_type'])},{pr['discount_value']},{pr['min_order_value']},"
            f"{'NULL' if pr['max_discount'] is None else pr['max_discount']},"
            f"{_sq(pr['valid_from'])},{_sq(pr['valid_until'])},"
            f"{_sq(pr['applicable_restaurants'])},{_sq(pr['applicable_cities'])},"
            f"{pr['usage_count']},{pr['max_usage']},{int(pr['is_active'])},"
            f"{_sq(pr['created_at'])},{_sq(pr['updated_at'])});\n"
        )

    out_path = os.path.join(os.path.dirname(__file__), "mysql-init", "02-seed.sql")
    with open(out_path, "w") as f:
        f.writelines(lines)
    print(f"  SQL seed file written to: {out_path}")


def _sq(value):
    """Single-quote escape for SQL fallback."""
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


# ---------------------------------------------------------------------------
# DynamoDB seeding
# ---------------------------------------------------------------------------
def seed_dynamodb(ddb_orders, ddb_payments, ddb_user_locations):
    import boto3
    from botocore.exceptions import ClientError

    ddb = boto3.resource(
        "dynamodb",
        region_name=AWS_REGION,
        endpoint_url=DYNAMODB_ENDPOINT,
    )

    tables = {
        f"{DYNAMODB_TABLE_PREFIX}-orders": ddb_orders,
        f"{DYNAMODB_TABLE_PREFIX}-payments": ddb_payments,
        f"{DYNAMODB_TABLE_PREFIX}-user-locations": ddb_user_locations,
    }

    for table_name, records in tables.items():
        try:
            table = ddb.Table(table_name)
            table.load()
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                print(f"  [!] Table '{table_name}' not found — skipping.")
                continue
            raise

        print(f"  Writing {len(records)} records to {table_name}...")
        with table.batch_writer() as batch:
            for record in records:
                batch.put_item(Item=record)

    print("  DynamoDB seed complete.")


# ---------------------------------------------------------------------------
# Kafka seeding
# ---------------------------------------------------------------------------
def seed_kafka(users, restaurants, menu_items, orders, promotions):
    try:
        from confluent_kafka import Producer
    except ImportError:
        print("  [!] confluent-kafka not installed. Run: pip install confluent-kafka")
        return

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    def delivery_report(err, msg):
        if err:
            print(f"  [!] Kafka delivery failed: {err}")

    def produce(topic, key, payload):
        producer.produce(
            topic,
            key=key.encode("utf-8"),
            value=json.dumps(payload).encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)

    # orders topic — 100 order events
    print("  Producing 100 messages to 'orders' topic...")
    for order in random.sample(orders, min(100, len(orders))):
        produce("orders", order["order_id"], {**order, "event_type": "INSERT", "source": "pipeline4"})

    # users topic — 50 user events
    print("  Producing 50 messages to 'users' topic...")
    for user in random.sample(users, min(50, len(users))):
        produce("users", user["user_id"], {**user, "event_type": "INSERT", "source": "pipeline4"})

    # menu topic — all menu items
    print(f"  Producing {len(menu_items)} messages to 'menu' topic...")
    for item in menu_items:
        produce("menu", item["item_id"], {**item, "event_type": "INSERT", "source": "pipeline4"})

    # promo topic — all promotions
    print(f"  Producing {len(promotions)} messages to 'promo' topic...")
    for promo in promotions:
        produce("promo", promo["promo_id"], {**promo, "event_type": "INSERT", "source": "pipeline4"})

    producer.flush()
    print("  Kafka seed complete.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Seed data generator for Zomato Data Platform")
    parser.add_argument(
        "--target",
        choices=["mysql", "dynamodb", "kafka", "all"],
        default="all",
        help="Which system to seed (default: all)",
    )
    parser.add_argument("--users", type=int, default=50, help="Number of users to generate")
    parser.add_argument("--restaurants", type=int, default=20, help="Number of restaurants to generate")
    parser.add_argument("--orders", type=int, default=200, help="Number of orders to generate")
    args = parser.parse_args()

    print("Generating seed data...")
    users = generate_users(args.users)
    restaurants = generate_restaurants(args.restaurants)
    menu_items = generate_menu_items(restaurants)
    orders = generate_orders(users, restaurants, args.orders)
    payments = generate_payments(orders)
    promotions = generate_promotions()

    print(f"  {len(users)} users, {len(restaurants)} restaurants, {len(menu_items)} menu items")
    print(f"  {len(orders)} orders, {len(payments)} payments, {len(promotions)} promotions")

    if args.target in ("mysql", "all"):
        print("\n[MySQL] Seeding Aurora MySQL...")
        seed_mysql(users, restaurants, menu_items, orders, payments, promotions)

    if args.target in ("dynamodb", "all"):
        print("\n[DynamoDB] Seeding DynamoDB tables...")
        ddb_orders = to_ddb_orders(orders, n=100)
        ddb_payments = to_ddb_payments(payments, n=100)
        ddb_locations = generate_ddb_user_locations(users, n=50)
        seed_dynamodb(ddb_orders, ddb_payments, ddb_locations)

    if args.target in ("kafka", "all"):
        print("\n[Kafka] Producing seed messages...")
        seed_kafka(users, restaurants, menu_items, orders, promotions)

    print("\nDone.")


if __name__ == "__main__":
    main()
