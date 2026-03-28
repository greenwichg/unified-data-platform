# Data Stores Reference

This document describes what each data store holds, the schema for every
table/topic, and which pipelines read from or write to it.

---

## 1. Aurora MySQL

**Role:** System-of-record for all transactional data.
**Source for:** Pipeline 1 (Spark batch JDBC) and Pipeline 2 (Debezium CDC).

### Tables

#### `users`
Registered Zomato customers.

| Column | Type | Description |
|--------|------|-------------|
| `user_id` | VARCHAR(36) PK | Unique user identifier |
| `name` | VARCHAR(255) | Full name |
| `email` | VARCHAR(255) UNIQUE | Email address |
| `phone` | VARCHAR(20) | Mobile number |
| `city` | VARCHAR(100) | Home city |
| `signup_date` | DATETIME | Account creation date |
| `is_pro_member` | BOOLEAN | Zomato Pro subscription flag |
| `preferred_cuisine` | JSON | List of preferred cuisine types |
| `total_orders` | BIGINT | Lifetime order count |
| `average_order_value` | DECIMAL(10,2) | Lifetime AOV in INR |
| `last_order_at` | DATETIME | Timestamp of most recent order |
| `created_at` | DATETIME | Row creation timestamp |
| `updated_at` | DATETIME | Row last-updated timestamp |

Indexes: `city`, `updated_at`

---

#### `restaurants`
Partner restaurants listed on the platform.

| Column | Type | Description |
|--------|------|-------------|
| `restaurant_id` | VARCHAR(36) PK | Unique restaurant identifier |
| `name` | VARCHAR(255) | Restaurant name |
| `city` | VARCHAR(100) | City of operation |
| `area` | VARCHAR(255) | Locality / neighbourhood |
| `cuisine_types` | JSON | List of cuisine categories served |
| `rating` | FLOAT | Average customer rating (0–5) |
| `total_reviews` | INT | Total review count |
| `avg_cost_for_two` | DECIMAL(10,2) | Average cost for two diners (INR) |
| `is_active` | BOOLEAN | Whether restaurant is currently accepting orders |
| `latitude` | DOUBLE | GPS latitude |
| `longitude` | DOUBLE | GPS longitude |
| `created_at` | DATETIME | Row creation timestamp |
| `updated_at` | DATETIME | Row last-updated timestamp |

Indexes: `city`, `updated_at`

---

#### `menu_items`
Individual dishes offered by each restaurant.

| Column | Type | Description |
|--------|------|-------------|
| `item_id` | VARCHAR(36) PK | Unique menu item identifier |
| `restaurant_id` | VARCHAR(36) FK | Parent restaurant |
| `name` | VARCHAR(255) | Dish name |
| `description` | TEXT | Short description |
| `category` | VARCHAR(100) | e.g. Main Course, Sides & Drinks |
| `cuisine_type` | VARCHAR(100) | Cuisine category |
| `price` | DECIMAL(10,2) | Price in INR |
| `is_vegetarian` | BOOLEAN | Vegetarian flag |
| `is_available` | BOOLEAN | Currently available for ordering |
| `preparation_time_mins` | INT | Estimated prep time |
| `rating` | FLOAT | Item-level rating |
| `total_orders` | BIGINT | Number of times ordered |
| `image_url` | VARCHAR(500) | CDN URL for item image |
| `created_at` | DATETIME | Row creation timestamp |
| `updated_at` | DATETIME | Row last-updated timestamp |

Indexes: `restaurant_id`, `updated_at`

---

#### `orders`
Every order placed on the platform.

| Column | Type | Description |
|--------|------|-------------|
| `order_id` | VARCHAR(36) PK | Unique order identifier |
| `user_id` | VARCHAR(36) FK | Placing customer |
| `restaurant_id` | VARCHAR(36) FK | Fulfilling restaurant |
| `status` | ENUM | `PLACED → CONFIRMED → PREPARING → READY → PICKED_UP → DELIVERING → DELIVERED \| CANCELLED` |
| `subtotal` | DECIMAL(10,2) | Items total before tax/fees (INR) |
| `tax` | DECIMAL(10,2) | GST (5% of subtotal) |
| `delivery_fee` | DECIMAL(10,2) | Delivery charge |
| `total_amount` | DECIMAL(10,2) | Grand total (INR) |
| `payment_method` | VARCHAR(50) | UPI / Credit Card / Debit Card / Wallet / COD |
| `delivery_latitude` | DOUBLE | Drop-off GPS latitude |
| `delivery_longitude` | DOUBLE | Drop-off GPS longitude |
| `delivery_address` | TEXT | Human-readable delivery address |
| `city` | VARCHAR(100) | Delivery city |
| `pincode` | VARCHAR(10) | PIN code |
| `estimated_delivery_mins` | INT | ETA at order placement |
| `actual_delivery_mins` | INT | Actual time taken (populated on DELIVERED) |
| `created_at` | DATETIME | Order placement timestamp |
| `updated_at` | DATETIME | Last status-change timestamp |

Indexes: `user_id`, `restaurant_id`, `status`, `city`, `created_at`, `updated_at`

---

#### `payments`
Payment record for each order.

| Column | Type | Description |
|--------|------|-------------|
| `payment_id` | VARCHAR(36) PK | Unique payment identifier |
| `order_id` | VARCHAR(36) FK | Associated order |
| `amount` | DECIMAL(10,2) | Amount charged (INR) |
| `payment_method` | VARCHAR(50) | Payment instrument used |
| `payment_status` | ENUM | `INITIATED → PROCESSING → COMPLETED \| FAILED \| REFUNDED` |
| `transaction_id` | VARCHAR(100) | Gateway transaction reference |
| `gateway_response` | JSON | Raw gateway response payload |
| `created_at` | DATETIME | Payment initiation timestamp |
| `updated_at` | DATETIME | Last status-change timestamp |

Indexes: `order_id`, `updated_at`

---

#### `promotions`
Discount and cashback offers available on the platform.

| Column | Type | Description |
|--------|------|-------------|
| `promo_id` | VARCHAR(36) PK | Unique promo identifier |
| `promo_code` | VARCHAR(50) UNIQUE | Human-readable promo code (e.g. `WELCOME50`) |
| `description` | TEXT | Customer-facing description |
| `discount_type` | ENUM | `PERCENTAGE \| FLAT \| CASHBACK \| FREE_DELIVERY` |
| `discount_value` | DECIMAL(10,2) | Percentage or flat amount |
| `min_order_value` | DECIMAL(10,2) | Minimum cart value to apply promo |
| `max_discount` | DECIMAL(10,2) | Cap on discount amount (NULL = uncapped) |
| `valid_from` | DATETIME | Promo start date |
| `valid_until` | DATETIME | Promo expiry date |
| `applicable_restaurants` | JSON | List of eligible restaurant IDs (empty = all) |
| `applicable_cities` | JSON | List of eligible cities |
| `usage_count` | BIGINT | Current redemption count |
| `max_usage` | BIGINT | Maximum allowed redemptions |
| `is_active` | BOOLEAN | Whether promo is currently active |
| `created_at` | DATETIME | Row creation timestamp |
| `updated_at` | DATETIME | Row last-updated timestamp |

Indexes: `promo_code`, `is_active`, `updated_at`

---

#### `debezium_signal` *(internal)*
Used by Debezium to trigger incremental snapshots. Not a business table.

| Column | Type | Description |
|--------|------|-------------|
| `id` | VARCHAR(42) PK | Signal identifier |
| `type` | VARCHAR(32) | Signal type (e.g. `execute-snapshot`) |
| `data` | VARCHAR(2048) | Signal payload |

---

## 2. DynamoDB

**Role:** Low-latency operational store for real-time order tracking and user location.
**Source for:** Pipeline 3 (DynamoDB Streams → S3 → Spark EMR).

All 3 tables have **DynamoDB Streams enabled** (`NEW_AND_OLD_IMAGES`) so every
insert/update is automatically captured and forwarded to Pipeline 3.

### Tables

#### `zomato-data-platform-{env}-orders`
Real-time order state for fast lookups (e.g. order tracking page).

| Attribute | Key | Type | Description |
|-----------|-----|------|-------------|
| `order_id` | PK (Hash) | String | Unique order identifier |
| `created_at` | SK (Range) | String | Order placement timestamp |
| `user_id` | — | String | Placing customer |
| `restaurant_id` | — | String | Fulfilling restaurant |
| `status` | — | String | Current order status |
| `total_amount` | — | String | Grand total (INR, stored as string) |
| `payment_method` | — | String | Payment instrument |
| `city` | — | String | Delivery city |
| `delivery_address` | — | String | Drop-off address |
| `estimated_delivery_mins` | — | Number | ETA in minutes |
| `updated_at` | — | String | Last status-change timestamp |

GSIs:
- `user-orders-index` — `user_id` (PK) + `created_at` (SK): fetch all orders for a user
- `restaurant-orders-index` — `restaurant_id` (PK) + `created_at` (SK): fetch all orders for a restaurant

---

#### `zomato-data-platform-{env}-payments`
Payment status for real-time payment tracking.

| Attribute | Key | Type | Description |
|-----------|-----|------|-------------|
| `payment_id` | PK (Hash) | String | Unique payment identifier |
| `order_id` | SK (Range) | String | Associated order |
| `amount` | — | String | Amount charged (INR, stored as string) |
| `payment_method` | — | String | Payment instrument |
| `payment_status` | — | String | `INITIATED / COMPLETED / REFUNDED` |
| `transaction_id` | — | String | Gateway transaction reference |
| `created_at` | — | String | Payment initiation timestamp |
| `updated_at` | — | String | Last status-change timestamp |

---

#### `zomato-data-platform-{env}-user-locations`
Live GPS pings from the Zomato mobile app.

| Attribute | Key | Type | Description |
|-----------|-----|------|-------------|
| `user_id` | PK (Hash) | String | User sending the ping |
| `timestamp` | SK (Range) | String | ISO-8601 timestamp of the ping |
| `latitude` | — | String | GPS latitude |
| `longitude` | — | String | GPS longitude |
| `city` | — | String | Detected city |
| `accuracy_meters` | — | String | GPS accuracy |
| `speed_kmh` | — | String | Device speed at time of ping |
| `ttl` | — | Number | Unix epoch expiry (auto-deleted after 7 days) |

TTL is enabled — old location pings expire automatically after 7 days.

---

## 3. Kafka (Amazon MSK)

**Role:** Event streaming backbone for Pipeline 2 (CDC) and Pipeline 4 (real-time events).
**Partitions:** 64 per topic | **Replication factor:** 3 | **Retention:** 7 days

### Topics

#### `orders`
All order lifecycle events, payment events, and delivery updates.

| Event type | Produced by | Description |
|------------|-------------|-------------|
| `ORDER_EVENT` | seed / producer | New order placed or status updated |
| `PAYMENT_EVENT` | seed / producer | Payment initiated or completed |
| `ORDER_PLACED` | Pipeline 4 app | Customer places a new order |
| `ORDER_UPDATED` | Pipeline 4 app | Order status changes |
| `ORDER_CANCELLED` | Pipeline 4 app | Order cancelled |
| `ORDER_DELIVERED` | Pipeline 4 app | Order delivered |
| `PAYMENT_INITIATED` | Pipeline 4 app | Payment gateway call started |
| `PAYMENT_COMPLETED` | Pipeline 4 app | Payment settled |
| `DELIVERY_ASSIGNED` | Pipeline 4 app | Delivery partner assigned |
| `DELIVERY_PICKED_UP` | Pipeline 4 app | Partner picked up the order |
| `DELIVERY_COMPLETED` | Pipeline 4 app | Delivery confirmed |

Also consumed by: Debezium CDC (Pipeline 2), Flink (Pipeline 4), Druid ingestion.

---

#### `users`
User account events and real-time location updates.

| Event type | Produced by | Description |
|------------|-------------|-------------|
| `USER_EVENT` | seed / producer | User profile created or updated |
| `USER_LOCATION_UPDATE` | seed / producer | GPS ping from mobile app |
| `USER_SIGNUP` | Pipeline 4 app | New account registered |
| `USER_LOGIN` | Pipeline 4 app | User logged in |
| `RATING_SUBMITTED` | Pipeline 4 app | User submitted a restaurant/order rating |
| `APP_OPENED` | Pipeline 4 app | App session started |
| `PAGE_VIEWED` | Pipeline 4 app | Screen/page navigation event |

---

#### `menu`
Menu catalogue events — views, additions, and restaurant searches.

| Event type | Produced by | Description |
|------------|-------------|-------------|
| `MENU_EVENT` | seed / producer | Menu item availability update |
| `MENU_VIEWED` | Pipeline 4 app | User browsed a restaurant menu |
| `MENU_ITEM_ADDED` | Pipeline 4 app | Item added to cart |
| `RESTAURANT_SEARCH` | Pipeline 4 app | User searched for a restaurant |

---

#### `promo`
Promotion redemption events.

| Event type | Produced by | Description |
|------------|-------------|-------------|
| `PROMO_EVENT` | seed / producer | Promo details broadcast / refresh |
| `PROMO_APPLIED` | Pipeline 4 app | User applied a promo code at checkout |
| `PROMO_REDEEMED` | Pipeline 4 app | Promo successfully redeemed on payment |

---

### Internal / Infrastructure Topics

| Topic | Purpose |
|-------|---------|
| `debezium-configs` | Debezium connector configuration storage |
| `debezium-offsets` | Debezium CDC offset tracking |
| `debezium-status` | Debezium connector health status |
| `schema-changes.orders` | MySQL binlog schema change events for orders |
| `schema-changes.users` | MySQL binlog schema change events for users |
| `schema-changes.menu` | MySQL binlog schema change events for menu |
| `schema-changes.promo` | MySQL binlog schema change events for promo |
| `druid-ingestion-events` | Aggregated events forwarded to Druid for real-time OLAP |
| `order-spike-alerts` | CEP alerts generated by Flink when order volume spikes |

---

## Summary

| Store | Tables / Topics | Primary use |
|-------|----------------|-------------|
| Aurora MySQL | `users`, `restaurants`, `menu_items`, `orders`, `payments`, `promotions` | Transactional system-of-record |
| DynamoDB | `orders`, `payments`, `user-locations` | Real-time order tracking and location |
| Kafka (MSK) | `orders`, `users`, `menu`, `promo` | Event streaming for CDC and real-time pipelines |
